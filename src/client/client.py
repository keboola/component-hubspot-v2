import logging

import json
from json import JSONDecodeError
from typing import Optional, Dict, Generator, List

import requests
from keboola.http_client import HttpClient
from hubspot import HubSpot
from hubspot.crm.associations import BatchInputPublicObjectId, ApiException
from urllib3.util.retry import Retry

BASE_URL = "https://api.hubapi.com/"

ENDPOINT_CAMPAIGNS_BY_ID = "email/public/v1/campaigns/by-id"
ENDPOINT_CAMPAIGNS = "/email/public/v1/campaigns/"
ENDPOINTS_CONTACT_LISTS = "contacts/v1/lists/"
ENDPOINT_FORMS = "marketing/v3/forms/"
ENDPOINT_EMAIL_EVENTS = 'email/public/v1/events'
ENDPOINT_EMAIL_STATISTICS = 'marketing-emails/v1/emails/with-statistics'

PAGE_MAX_SIZE = 100
DEFAULT_V1_LIMIT = 1000
BATCH_LIMIT = 100

MAX_RETRIES = 5
MAX_TIMEOUT = 10

EVENT_TYPES = ["DEFERRED", "CLICK", "DROPPED", "DELIVERED", "PROCESSED", "OPEN", "BOUNCE", "SENT"]


class HubspotClientException(Exception):
    pass


class HubspotClient(HttpClient):
    def __init__(self, access_token):
        retry = Retry(
            total=3,
            backoff_factor=0.3,
            status_forcelist=(429, 500, 502, 504),
        )
        self.client_v3 = HubSpot(access_token=access_token, retry=retry)
        auth_header = {'Authorization': f'Bearer {access_token}'}
        super().__init__(BASE_URL, auth_header=auth_header, status_forcelist=(429, 500, 502, 504, 524))

    def get_crm_object_properties(self, object_type: str) -> List:
        return self.client_v3.crm.properties.core_api.get_all(object_type=object_type).to_dict().get("results")

    def get_contacts(self, properties: Dict, archived: bool = False) -> Generator:
        return self._paginate_v3_object(self.client_v3.crm.contacts.basic_api, properties=properties, archived=archived)

    def get_companies(self, properties: Dict, archived: bool = False) -> Generator:
        return self._paginate_v3_object(self.client_v3.crm.companies.basic_api, properties=properties,
                                        archived=archived)

    def get_deals(self, properties: Dict, archived: bool = False) -> Generator:
        return self._paginate_v3_object(self.client_v3.crm.deals.basic_api, properties=properties, archived=archived)

    def get_line_items(self, properties: Dict, archived: bool = False) -> Generator:
        return self._paginate_v3_object(self.client_v3.crm.line_items.basic_api, properties=properties,
                                        archived=archived)

    def get_deal_pipelines(self) -> List:
        return self.client_v3.crm.pipelines.pipelines_api.get_all(object_type="deals").to_dict().get("results")

    def get_ticket_pipelines(self) -> List:
        return self.client_v3.crm.pipelines.pipelines_api.get_all(object_type="tickets").to_dict().get("results")

    def get_products(self, properties: Dict, archived: bool = False) -> Generator:
        return self._paginate_v3_object(self.client_v3.crm.products.basic_api, properties=properties, archived=archived)

    def get_quotes(self, properties: Dict, archived: bool = False) -> Generator:
        return self._paginate_v3_object(self.client_v3.crm.quotes.basic_api, properties=properties, archived=archived)

    def get_owners(self) -> Generator:
        return self._paginate_v3_object(self.client_v3.crm.owners.owners_api)

    def get_tickets(self, properties: Dict, archived: bool = False) -> Generator:
        return self._paginate_v3_object(self.client_v3.crm.tickets.basic_api, properties=properties, archived=archived)

    def get_engagements_notes(self, properties: Dict, archived: bool = False) -> Generator:
        return self._paginate_v3_object(self.client_v3.crm.objects.notes.basic_api, properties=properties,
                                        archived=archived)

    def get_engagements_calls(self, properties: Dict, archived: bool = False) -> Generator:
        return self._paginate_v3_object(self.client_v3.crm.objects.calls.basic_api, properties=properties,
                                        archived=archived)

    def get_engagements_tasks(self, properties: Dict, archived: bool = False) -> Generator:
        return self._paginate_v3_object(self.client_v3.crm.objects.tasks.basic_api, properties=properties,
                                        archived=archived)

    def get_engagements_meetings(self, properties: Dict, archived: bool = False) -> Generator:
        return self._paginate_v3_object(self.client_v3.crm.objects.meetings.basic_api, properties=properties,
                                        archived=archived)

    def get_engagements_emails(self, properties: Dict, archived: bool = False) -> Generator:
        return self._paginate_v3_object(self.client_v3.crm.objects.emails.basic_api, properties=properties,
                                        archived=archived)

    def get_campaigns(self) -> Generator:
        for campaign_page in self._get_paged_result_pages(ENDPOINT_CAMPAIGNS_BY_ID, {}, 'campaigns'):
            for campaign in campaign_page:
                yield [self.get_campaign_details(campaign.get('id'))]

    def get_campaign_details(self, campaign_id: str) -> Dict:
        try:
            req = self.get_raw(f"{ENDPOINT_CAMPAIGNS}/{campaign_id}", timeout=MAX_TIMEOUT)
        except ConnectionError as exc:
            raise HubspotClientException(f"Connection to Hubspot failed due :{exc}") from exc
        self._check_http_result(req, "campaigns")
        return req.json()

    def get_contact_lists(self) -> Generator:
        yield from self._get_paged_result_pages(ENDPOINTS_CONTACT_LISTS, {}, 'lists')

    def get_email_statistics(self, updated_since: Optional[int] = None) -> Generator:
        parameters = {"updated__gte": updated_since} if updated_since else {}
        yield from self._get_paged_result_pages(ENDPOINT_EMAIL_STATISTICS, parameters, 'objects')

    def get_email_events(self) -> Generator:
        for event in EVENT_TYPES:
            yield from self._get_paged_result_pages(ENDPOINT_EMAIL_EVENTS, {"eventType": event}, 'events')

    def get_forms(self) -> Generator:
        yield from self._get_paged_result_pages_v3(ENDPOINT_FORMS, {})

    def get_associations(self, object_id_generator: Generator, from_object_type: str, to_object_type: str) -> None:
        batch_inputs = self._format_batch_inputs(object_id_generator)
        for input_chuck in self.divide_chunks(batch_inputs, BATCH_LIMIT):
            batch_input_chunk = BatchInputPublicObjectId(inputs=input_chuck)
            response = self.client_v3.crm.associations.batch_api.read(from_object_type=from_object_type,
                                                                      to_object_type=to_object_type,
                                                                      batch_input_public_object_id=batch_input_chunk)
            yield response.results

    @staticmethod
    def _format_batch_inputs(object_ids):
        return [{"id": object_id} for object_id in object_ids]

    @staticmethod
    def divide_chunks(list_to_divide, list_len):
        for i in range(0, len(list_to_divide), list_len):
            yield list_to_divide[i:i + list_len]

    @staticmethod
    def _paginate_v3_object(api_object, **kwargs) -> Generator:
        after = None
        while True:
            page = api_object.get_page(after=after, limit=PAGE_MAX_SIZE, **kwargs)
            yield page.results
            if page.paging is None:
                break
            after = page.paging.next.after

    def _get_paged_result_pages(self, endpoint: str, parameters: Dict, res_obj_name: str, offset: str = None,
                                limit: int = DEFAULT_V1_LIMIT) -> Generator:
        has_more = True
        while has_more:
            parameters['offset'] = offset
            parameters['limit'] = limit
            data = []

            try:
                req = self.get_raw(endpoint, params=parameters, timeout=MAX_TIMEOUT)
            except ConnectionError as exc:
                raise HubspotClientException(f"Connection to Hubspot failed due :{exc}") from exc
            self._check_http_result(req, endpoint)
            req_response = self._parse_response_text(req, endpoint, parameters)
            if req_response.get('hasMore'):
                has_more = True
                offset = req_response['offset']
            else:
                has_more = False
            if req_response.get(res_obj_name):
                data = req_response[res_obj_name]
            else:
                logging.debug(f'Empty response {req_response}')

            yield data

    def _check_http_result(self, response: requests.Response, endpoint: str) -> None:
        reason = self._decode_response_reason(response.reason)
        if 400 <= response.status_code < 600:
            raise HubspotClientException(f'Request to {endpoint} failed {response.status_code} Error : {reason}')

    @staticmethod
    def _decode_response_reason(reason: str) -> str:
        if isinstance(reason, bytes):
            # We attempt to decode utf-8 first because some servers choose to localize their reason strings.
            # If the string isn't utf-8, we fall back to iso-8859-1 for all other encodings.
            try:
                reason = reason.decode('utf-8')
            except UnicodeDecodeError:
                reason = reason.decode('iso-8859-1')
        else:
            reason = reason
        return reason

    @staticmethod
    def _parse_response_text(response: requests.Response, endpoint: str, parameters: Dict) -> Dict:
        try:
            return response.json()
        except JSONDecodeError as e:
            raise HubspotClientException(f'The HS API response is invalid. endpoint: {endpoint}, '
                                         f'parameters: {parameters}. '
                                         f'' f'Status: {response.status_code}. '
                                         f'' f'Response: {response.text[:250]}... {e}') from e

    def _get_paged_result_pages_v3(self, endpoint: str, parameters: Dict, limit: int = 100):
        has_more = True
        while has_more:
            parameters['limit'] = limit
            req_response = self.get_raw(endpoint, params=parameters, timeout=MAX_TIMEOUT)

            self._check_http_result(req_response, endpoint)
            response, parameters, has_more = self._process_v3_response(req_response, parameters)

            results = []
            if response.get('results'):
                results = response['results']
            else:
                logging.debug(f'Empty response {response}')

            yield results

    @staticmethod
    def _process_v3_response(req_response: requests.Response, parameters: Dict):
        resp_text = str.encode(req_response.text, 'utf-8')
        response = json.loads(resp_text)

        if response.get('paging', {}).get('next', {}).get('after'):
            has_more = True
            after = response['paging']['next']['after']
            parameters['after'] = after
        else:
            has_more = False

        return response, parameters, has_more
