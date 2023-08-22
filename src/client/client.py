import json
import logging
from json import JSONDecodeError
from typing import Dict, Generator, Iterator, List, Optional

import requests
from hubspot import HubSpot
from hubspot.crm import (companies, contacts, deals, line_items, owners,
                         pipelines, products, properties, quotes, tickets)
from hubspot.crm.associations import BatchInputPublicObjectId
from hubspot.crm.objects import calls, emails, meetings, notes, tasks
from keboola.http_client import HttpClient
from urllib3.util.retry import Retry as urlibRetry

BASE_URL = "https://api.hubapi.com/"

ENDPOINT_CAMPAIGNS_BY_ID = "email/public/v1/campaigns/by-id"
ENDPOINT_CAMPAIGNS = "/email/public/v1/campaigns/"
ENDPOINTS_CONTACT_LISTS = "contacts/v1/lists/"
ENDPOINT_FORMS = "marketing/v3/forms/"
ENDPOINT_EMAIL_EVENTS = 'email/public/v1/events'
ENDPOINT_EMAIL_STATISTICS = 'marketing-emails/v1/emails/with-statistics'

PAGE_MAX_SIZE = 100
PAGE_WITH_HISTORY_MAX_SIZE = 50
DEFAULT_V1_LIMIT = 1000
BATCH_LIMIT = 100

MAX_RETRIES = 5
MAX_TIMEOUT = 10
DEFAULT_BACKOFF = 0.3
EVENT_TYPES = ["DEFERRED", "CLICK", "DROPPED", "DELIVERED", "PROCESSED", "OPEN", "BOUNCE", "SENT"]


class HubspotClientException(Exception):
    pass


class HubspotClient(HttpClient):
    def __init__(self, access_token):
        retry_settings = urlibRetry(
            total=MAX_RETRIES,
            status=MAX_RETRIES,
            backoff_factor=DEFAULT_BACKOFF,
            allowed_methods=frozenset({"HEAD", "GET", "PUT", "POST"}),
            status_forcelist=(429, 500, 502, 504),
        )
        self.client_v3 = HubSpot(access_token=access_token, retry=retry_settings)
        auth_header = {'Authorization': f'Bearer {access_token}'}
        super().__init__(BASE_URL, auth_header=auth_header, status_forcelist=(429, 500, 502, 504, 524))

    def get_crm_object_properties(self, object_type: str) -> List:
        try:
            return self.client_v3.crm.properties.core_api.get_all(object_type=object_type).to_dict().get("results")
        except properties.exceptions.ApiException as exc:
            self._raise_exception_from_status_code(exc.status, object_type, exc.body)

    def get_contacts(self, object_properties: List, incremental: bool = False, archived: bool = False,
                     since_date: str = None, since_property: str = "lastmodifieddate",
                     properties_with_history: Optional[List] = None) -> Generator:
        return self._fetch_object_data(properties=object_properties,
                                       properties_with_history=properties_with_history,
                                       endpoint_name="contact",
                                       search_request_object=contacts.PublicObjectSearchRequest,
                                       search_api=self.client_v3.crm.contacts.search_api.do_search,
                                       basic_api=self.client_v3.crm.contacts.basic_api,
                                       exception=contacts.ApiException,
                                       incremental=incremental,
                                       archived=archived,
                                       since_date=since_date,
                                       since_property=since_property)

    def get_companies(self, object_properties: List, incremental: bool = False, archived: bool = False,
                      since_date: str = None, since_property: str = "hs_lastmodifieddate",
                      properties_with_history: Optional[List] = None) -> Generator:
        return self._fetch_object_data(properties=object_properties,
                                       properties_with_history=properties_with_history,
                                       endpoint_name="company",
                                       search_request_object=companies.PublicObjectSearchRequest,
                                       search_api=self.client_v3.crm.companies.search_api.do_search,
                                       basic_api=self.client_v3.crm.companies.basic_api,
                                       exception=companies.ApiException,
                                       incremental=incremental,
                                       archived=archived,
                                       since_date=since_date,
                                       since_property=since_property)

    def get_deals(self, object_properties: List, incremental: bool = False, archived: bool = False,
                  since_date: str = None, since_property: str = "hs_lastmodifieddate",
                  properties_with_history: Optional[List] = None) -> Generator:
        return self._fetch_object_data(properties=object_properties,
                                       properties_with_history=properties_with_history,
                                       endpoint_name="deal",
                                       search_request_object=deals.PublicObjectSearchRequest,
                                       search_api=self.client_v3.crm.deals.search_api.do_search,
                                       basic_api=self.client_v3.crm.deals.basic_api,
                                       exception=deals.ApiException,
                                       incremental=incremental,
                                       archived=archived,
                                       since_date=since_date,
                                       since_property=since_property)

    def get_line_items(self, object_properties: List, incremental: bool = False, archived: bool = False,
                       since_date: str = None, since_property: str = "hs_lastmodifieddate",
                       properties_with_history: Optional[List] = None) -> Generator:
        return self._fetch_object_data(properties=object_properties,
                                       properties_with_history=properties_with_history,
                                       endpoint_name="line_item",
                                       search_request_object=line_items.PublicObjectSearchRequest,
                                       search_api=self.client_v3.crm.line_items.search_api.do_search,
                                       basic_api=self.client_v3.crm.line_items.basic_api,
                                       exception=line_items.ApiException,
                                       incremental=incremental,
                                       archived=archived,
                                       since_date=since_date,
                                       since_property=since_property)

    def get_products(self, object_properties: List, incremental: bool = False, archived: bool = False,
                     since_date: str = None, since_property: str = "hs_lastmodifieddate",
                     properties_with_history: Optional[List] = None) -> Generator:
        return self._fetch_object_data(properties=object_properties,
                                       properties_with_history=properties_with_history,
                                       endpoint_name="product",
                                       search_request_object=products.PublicObjectSearchRequest,
                                       search_api=self.client_v3.crm.products.search_api.do_search,
                                       basic_api=self.client_v3.crm.products.basic_api,
                                       exception=products.ApiException,
                                       incremental=incremental,
                                       archived=archived,
                                       since_date=since_date,
                                       since_property=since_property)

    def get_quotes(self, object_properties: List, incremental: bool = False, archived: bool = False,
                   since_date: str = None, since_property: str = "hs_lastmodifieddate",
                   properties_with_history: Optional[List] = None) -> Generator:
        if archived:
            logging.info("Cannot fetch archived objects of type quote, it is not yet supported")
            archived = False
        return self._fetch_object_data(properties=object_properties,
                                       properties_with_history=properties_with_history,
                                       endpoint_name="quote",
                                       search_request_object=quotes.PublicObjectSearchRequest,
                                       search_api=self.client_v3.crm.quotes.search_api.do_search,
                                       basic_api=self.client_v3.crm.quotes.basic_api,
                                       exception=quotes.ApiException,
                                       incremental=incremental,
                                       archived=archived,
                                       since_date=since_date,
                                       since_property=since_property)

    def get_tickets(self, object_properties: List, incremental: bool = False, archived: bool = False,
                    since_date: str = None, since_property: str = "hs_lastmodifieddate",
                    properties_with_history: Optional[List] = None) -> Generator:
        return self._fetch_object_data(properties=object_properties,
                                       properties_with_history=properties_with_history,
                                       endpoint_name="ticket",
                                       search_request_object=tickets.PublicObjectSearchRequest,
                                       search_api=self.client_v3.crm.quotes.search_api.do_search,
                                       basic_api=self.client_v3.crm.tickets.basic_api,
                                       exception=tickets.ApiException,
                                       incremental=incremental,
                                       archived=archived,
                                       since_date=since_date,
                                       since_property=since_property)

    def get_owners(self) -> Generator:
        return self._paginate_v3_object(self.client_v3.crm.owners.owners_api, "owner", exception=owners.ApiException)

    def get_deal_pipelines(self) -> List:
        try:
            return self.client_v3.crm.pipelines.pipelines_api.get_all(object_type="deals").to_dict().get("results")
        except pipelines.exceptions.ApiException as exc:
            self._raise_exception_from_status_code(exc.status, "pipelines", exc.body)

    def get_ticket_pipelines(self) -> List:
        try:
            return self.client_v3.crm.pipelines.pipelines_api.get_all(object_type="tickets").to_dict().get("results")
        except pipelines.exceptions.ApiException as exc:
            self._raise_exception_from_status_code(exc.status, "pipelines", exc.body)

    def get_engagements_notes(self, object_properties: List, incremental: bool = False, archived: bool = False,
                              since_date: str = None, since_property: str = "hs_lastmodifieddate",
                              properties_with_history: Optional[List] = None) -> Generator:
        if archived:
            logging.info("Cannot fetch archived objects of type 'note', it is not yet supported")
            archived = False
        return self._fetch_object_data(properties=object_properties,
                                       properties_with_history=properties_with_history,
                                       endpoint_name="note",
                                       search_request_object=notes.PublicObjectSearchRequest,
                                       search_api=self.client_v3.crm.objects.notes.search_api.do_search,
                                       basic_api=self.client_v3.crm.objects.notes.basic_api,
                                       exception=notes.ApiException,
                                       incremental=incremental,
                                       archived=archived,
                                       since_date=since_date,
                                       since_property=since_property)

    def get_engagements_calls(self, object_properties: List, incremental: bool = False, archived: bool = False,
                              since_date: str = None, since_property: str = "hs_lastmodifieddate",
                              properties_with_history: Optional[List] = None) -> Generator:
        if archived:
            logging.info("Cannot fetch archived objects of type 'call', it is not yet supported")
            archived = False
        return self._fetch_object_data(properties=object_properties,
                                       properties_with_history=properties_with_history,
                                       endpoint_name="call",
                                       search_request_object=calls.PublicObjectSearchRequest,
                                       search_api=self.client_v3.crm.objects.calls.search_api.do_search,
                                       basic_api=self.client_v3.crm.objects.calls.basic_api,
                                       exception=calls.ApiException,
                                       incremental=incremental,
                                       archived=archived,
                                       since_date=since_date,
                                       since_property=since_property)

    def get_engagements_tasks(self, object_properties: List, incremental: bool = False, archived: bool = False,
                              since_date: str = None, since_property: str = "hs_lastmodifieddate",
                              properties_with_history: Optional[List] = None) -> Generator:
        if archived:
            logging.info("Cannot fetch archived objects of type 'tasks', it is not yet supported")
            archived = False
        return self._fetch_object_data(properties=object_properties,
                                       properties_with_history=properties_with_history,
                                       endpoint_name="task",
                                       search_request_object=tasks.PublicObjectSearchRequest,
                                       search_api=self.client_v3.crm.objects.tasks.search_api.do_search,
                                       basic_api=self.client_v3.crm.objects.tasks.basic_api,
                                       exception=tasks.ApiException,
                                       incremental=incremental,
                                       archived=archived,
                                       since_date=since_date,
                                       since_property=since_property)

    def get_engagements_meetings(self, object_properties: List, incremental: bool = False, archived: bool = False,
                                 since_date: str = None, since_property: str = "hs_lastmodifieddate",
                                 properties_with_history: Optional[List] = None) -> Generator:
        if archived:
            logging.info("Cannot fetch archived objects of type 'meeting', it is not yet supported")
            archived = False
        return self._fetch_object_data(properties=object_properties,
                                       properties_with_history=properties_with_history,
                                       endpoint_name="meeting",
                                       search_request_object=meetings.PublicObjectSearchRequest,
                                       search_api=self.client_v3.crm.objects.meetings.search_api.do_search,
                                       basic_api=self.client_v3.crm.objects.meetings.basic_api,
                                       exception=meetings.ApiException,
                                       incremental=incremental,
                                       archived=archived,
                                       since_date=since_date,
                                       since_property=since_property)

    def get_engagements_emails(self, object_properties: List, incremental: bool = False, archived: bool = False,
                               since_date: str = None, since_property: str = "hs_lastmodifieddate",
                               properties_with_history: Optional[List] = None) -> Generator:
        if archived:
            logging.info("Cannot fetch archived objects of type 'email', it is not yet supported")
            archived = False
        return self._fetch_object_data(properties=object_properties,
                                       properties_with_history=properties_with_history,
                                       endpoint_name="email",
                                       search_request_object=emails.PublicObjectSearchRequest,
                                       search_api=self.client_v3.crm.objects.emails.search_api.do_search,
                                       basic_api=self.client_v3.crm.objects.emails.basic_api,
                                       exception=emails.ApiException,
                                       incremental=incremental,
                                       archived=archived,
                                       since_date=since_date,
                                       since_property=since_property)

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

    def get_email_events(self, email_events: List) -> Generator:
        if not email_events:
            email_events = EVENT_TYPES
        logging.info(f"Fetching email Events for events : {email_events}")
        for event in email_events:
            yield from self._get_paged_result_pages(ENDPOINT_EMAIL_EVENTS, {"eventType": event}, 'events')

    def get_forms(self) -> Generator:
        yield from self._get_paged_result_pages_v3(ENDPOINT_FORMS, {})

    def get_associations(self, object_id_generator: Iterator, from_object_type: str, to_object_type: str) -> Dict:
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

    def _fetch_object_data(self, properties: List, endpoint_name: str, exception, basic_api, search_api,
                           search_request_object, since_date: str, since_property: str, incremental: bool = False,
                           archived: bool = False, properties_with_history: Optional[List] = None):
        if incremental:
            filter_groups = [{"filters": [{"value": since_date, "propertyName": since_property, "operator": "GTE"}]}]
            sorts = [{"propertyName": since_property, "direction": "DESCENDING"}]
            search_request = search_request_object(filter_groups=filter_groups,
                                                   sorts=sorts,
                                                   properties=properties,
                                                   limit=BATCH_LIMIT,
                                                   after=0)
            return self._paginate_v3_object_search(search_api,
                                                   endpoint_name,
                                                   search_request=search_request,
                                                   exception=exception)
        elif not incremental:
            return self._paginate_v3_object(basic_api,
                                            endpoint_name,
                                            exception=exception,
                                            properties=properties,
                                            properties_with_history=properties_with_history,
                                            archived=archived)

    def _paginate_v3_object(self, api_object, endpoint_name, exception, **kwargs) -> Generator:
        after = None
        while True:
            page = self._get_page_result(api_object, endpoint_name, after, exception, **kwargs)
            yield page.results
            if page.paging is None:
                break
            after = page.paging.next.after

    def _get_page_result(self, api_object, endpoint_name, after, exception, **kwargs):
        try:
            return api_object.get_page(after=after, limit=PAGE_WITH_HISTORY_MAX_SIZE, **kwargs)
        except exception as exc:
            self._raise_exception_from_status_code(exc.status, endpoint_name, exc.body)

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
        self._raise_exception_from_status_code(response.status_code, endpoint, reason)

    @staticmethod
    def _raise_exception_from_status_code(status_code: int, endpoint: str, reason: str = ""):
        if status_code == 401:
            raise HubspotClientException(f'Unauthorized request, please make sure your credentials are valid. '
                                         f'\n {reason}')
        if status_code == 403:
            raise HubspotClientException(f'Unauthorized request, please make sure your credentials contain the correct '
                                         f'scopes. The request failed during endpoint "{endpoint}", '
                                         f'make sure you have the read access scope for this endpoint. '
                                         f'\n {reason}')
        elif 400 <= status_code < 600:
            raise HubspotClientException(f'Request to "{endpoint}" failed {status_code} Error : {reason}')

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

    def _get_paged_result_pages_v3(self, endpoint: str, parameters: Dict, limit: int = PAGE_MAX_SIZE):
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

    def _paginate_v3_object_search(self, search_callable, endpoint_name, search_request, exception):
        while True:
            page = self._get_search_result(search_callable, endpoint_name, search_request, exception)
            yield page.results
            if page.paging is None:
                break
            search_request.after = page.paging.next.after

    def _get_search_result(self, search_callable, endpoint_name, search_request, exception):
        try:
            return search_callable(public_object_search_request=search_request)
        except exception as exc:
            self._raise_exception_from_status_code(exc.status, endpoint_name, exc.body)
