import copy
import csv
import datetime
import json
import logging
from os import path
from typing import Callable, List, Union

import dateparser
from keboola.component.base import ComponentBase, sync_action
from keboola.component.dao import SupportedDataTypes
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement
from keboola.component.table_schema import FieldSchema, TableSchema
from keboola.csvwriter import ElasticDictWriter

from client import HubspotClient, HubspotClientException
from configuration import Configuration, FetchMode, ObjectProperties
from json_parser import FlattenJsonParser, DEFAULT_MAX_PARSE_DEPTH
from table_handler import TableHandler

DEFAULT_DATE_FROM = "1990-01-01"

COLUMN_NAME_SWAP = {"contact_list": {"listId": "id"}}


class Component(ComponentBase):

    def __init__(self):
        super().__init__()
        self.endpoint_func_mapping = {
            "campaign": self.get_campaigns,
            "contact": self.get_contacts,
            "company": self.get_companies,
            "deal": self.get_deals,
            "line_item": self.get_line_items,
            "deal_line_item": self.get_line_items,
            "quote": self.get_quotes,
            "product": self.get_products,
            "owner": self.get_owners,
            "ticket": self.get_tickets,
            "contact_list": self.get_contact_lists,
            "email_event": self.get_email_events,
            "form": self.get_forms,
            "pipeline": self.get_pipelines,
            "note": self.get_notes,
            "call": self.get_calls,
            "task": self.get_tasks,
            "meeting": self.get_meetings,
            "email": self.get_emails,
            "email_statistic": self.get_email_statistics
        }
        self.client: HubspotClient
        self._configuration: Configuration
        self.state: dict = {}
        self._table_handler_cache: dict = {}

    def run(self):
        self._init_configuration()
        self.validate_associations()

        self.state = self.get_state_file()
        self.state["last_run"] = self._parse_date("now")

        self._init_client()

        for endpoint_name in self._configuration.endpoints.enabled:
            self.process_endpoint(endpoint_name)
        self._close_table_handlers()

        for association in self._configuration.associations:
            self.process_association(association)
        self._close_table_handlers()
        self.write_state_file(self.state)

    def _init_configuration(self):
        self.validate_configuration_parameters(
            Configuration.get_dataclass_required_parameters())
        self._configuration: Configuration = Configuration.load_from_dict(
            self.configuration.parameters)

    def _init_client(self):
        self.client = HubspotClient(
            access_token=self._configuration.pswd_private_app_token)

    @property
    def since_fetch_date(self) -> int:
        since_fetch_date: int = 0
        if self._configuration.fetch_settings.fetch_mode == FetchMode.INCREMENTAL_FETCH:
            since_fetch_date: int = int(self._parse_date(
                self._configuration.fetch_settings.date_from))

        return since_fetch_date

    @property
    def override_parser_depth(self):
        return self._configuration.override_parser_depth or DEFAULT_MAX_PARSE_DEPTH

    def process_endpoint(self, endpoint_name: str):
        try:
            self.endpoint_func_mapping[endpoint_name]()
        except HubspotClientException as e:
            raise UserException(e) from e

    def get_contacts(self) -> None:
        self._process_basic_crm_object("contact", self.client.get_contacts)

    def get_companies(self) -> None:
        self._process_basic_crm_object("company", self.client.get_companies)

    def get_deals(self) -> None:
        self._process_basic_crm_object("deal", self.client.get_deals)

    def get_line_items(self) -> None:
        self._process_basic_crm_object("line_item", self.client.get_line_items)

    def get_products(self) -> None:
        self._process_basic_crm_object("product", self.client.get_products)

    def get_quotes(self) -> None:
        self._process_basic_crm_object("quote", self.client.get_quotes)

    def get_notes(self):
        self._process_basic_crm_object(
            "note", self.client.get_engagements_notes)

    def get_calls(self):
        self._process_basic_crm_object(
            "call", self.client.get_engagements_calls)

    def get_tasks(self):
        self._process_basic_crm_object(
            "task", self.client.get_engagements_tasks)

    def get_meetings(self):
        self._process_basic_crm_object(
            "meeting", self.client.get_engagements_meetings)

    def get_emails(self):
        self._process_basic_crm_object(
            "email", self.client.get_engagements_emails)

    def get_tickets(self) -> None:
        self._process_basic_crm_object("ticket", self.client.get_tickets)

    def get_campaigns(self) -> None:
        self._process_endpoint_with_custom_schema(
            "campaign", self.client.get_campaigns)

    def get_contact_lists(self) -> None:
        self._process_endpoint_with_custom_schema(
            "contact_list", self.client.get_contact_lists)

    def get_forms(self) -> None:
        self._process_endpoint_with_custom_schema(
            "form", self.client.get_forms)

    def get_email_events(self) -> None:
        email_events = self._configuration.additional_properties.email_event_types
        self._process_endpoint_with_custom_schema("email_event", self.client.get_email_events,
                                                  email_events=email_events)

    def get_email_statistics(self) -> None:
        updated_since_timestamp = self.since_fetch_date
        self._process_endpoint_with_custom_schema("email_statistic", self.client.get_email_statistics,
                                                  updated_since=updated_since_timestamp)

    def get_owners(self) -> None:
        table_schema = self.get_table_schema_by_name("owner")
        self._init_table_handler("owner", table_schema)

        for page in self.client.get_owners():
            for item in page:
                c = item.to_dict()
                self._table_handler_cache["owner"].writerow(c)

    def get_pipelines(self) -> None:
        pipeline_schema = self.get_table_schema_by_name("pipeline")
        self._init_table_handler("pipeline", pipeline_schema)

        pipeline_stage_schema = self.get_table_schema_by_name("pipeline_stage")
        self._init_table_handler("pipeline_stage", pipeline_stage_schema)

        self._get_specific_pipeline(self.client.get_deal_pipelines)
        self._get_specific_pipeline(self.client.get_ticket_pipelines)

    def _get_specific_pipeline(self, pipeline_generator: Callable) -> None:
        parser = FlattenJsonParser(
            max_parsing_depth=self.override_parser_depth)
        for ticket_pipeline in pipeline_generator():
            stages = ticket_pipeline.pop("stages")
            pipeline_id = ticket_pipeline.get("id")
            self._table_handler_cache["pipeline"].writerow(ticket_pipeline)
            for stage in stages:
                parsed_stage = parser.parse_row(stage)
                self._table_handler_cache["pipeline_stage"].writerow(
                    {"pipeline_id": pipeline_id, **parsed_stage})

    def _process_basic_crm_object(self, object_name: str, data_generator: Callable) -> None:
        self._log_crm_object_fetching_message(object_name)

        additional_property_columns = self._get_additional_properties_to_fetch(
            object_name)

        table_schema = TableSchema(name=object_name, primary_keys=[
                                   "id"], fields=additional_property_columns)
        self._init_table_handler(object_name, table_schema)

        incremental_fetch_mode = self._configuration.fetch_settings.fetch_mode != FetchMode.FULL_FETCH

        archived = self._configuration.fetch_settings.archived

        extra_arguments = {"object_properties": table_schema.field_names,
                           "archived": archived,
                           "incremental": incremental_fetch_mode,
                           "since_date": self.since_fetch_date}

        if self._configuration.additional_properties.fetch_property_history:
            custom_props_str = getattr(
                self._configuration.additional_properties, f"{object_name}_property_history")
            properties_with_history = self._parse_properties(custom_props_str)
            extra_arguments["properties_with_history"] = properties_with_history
            self._init_property_history_table_handler()

        # If fetching archived also fetch non-archived objects
        if archived:
            self.fetch_and_write_to_table(
                object_name, data_generator, extra_arguments)
        extra_arguments["archived"] = False
        self.fetch_and_write_to_table(
            object_name, data_generator, extra_arguments)

    def _log_crm_object_fetching_message(self, object_name):
        logging_message = f"Downloading data of object {object_name}. "
        incremental_fetch_mode = self._configuration.fetch_settings.fetch_mode != FetchMode.FULL_FETCH
        if incremental_fetch_mode:
            logging_message = f"{logging_message}Fetching data incrementally, from the millisecond timestamp " \
                              f"{self.since_fetch_date}: " \
                              f"in UTC : {self._timestamp_to_datetime(self.since_fetch_date)}."
        else:
            logging_message = f"{logging_message} Fetching all data as Full Fetching mode is selected. "
            if self._configuration.fetch_settings.archived:
                logging_message = f"{logging_message}Fetching archived data."
        logging_message = f"{logging_message}Fetching " \
                          f"{self._configuration.additional_properties.object_properties.value} object properties"
        logging.info(logging_message)

    def _get_additional_properties_to_fetch(self, object_name) -> List[FieldSchema]:
        if self._configuration.additional_properties.object_properties == ObjectProperties.ALL:
            columns_with_properties = self.get_all_object_columns_with_properties(
                object_name)
        elif self._configuration.additional_properties.object_properties == ObjectProperties.CUSTOM:
            columns_with_properties = self.get_specified_object_columns_with_properties(
                object_name)
        else:
            columns_with_properties = []

        # It is necessary to add id column if not present as it is not part of the object properties
        columns_with_properties = self._add_base_fields_to_field_schema_list(
            columns_with_properties)
        return columns_with_properties

    def get_all_object_columns_with_properties(self, object_name: str) -> List[FieldSchema]:
        obj_prop = self.client.get_crm_object_properties(object_name)
        return self._generate_field_schemas_from_properties(obj_prop)

    def get_specified_object_columns_with_properties(self, object_name: str) -> List[FieldSchema]:
        custom_props_str = getattr(
            self._configuration.additional_properties, f"{object_name}_properties")
        custom_props = self._parse_properties(custom_props_str)
        obj_prop = self.client.get_crm_object_properties(object_name)
        classified_object_properties = [
            obj_prop for obj_prop in obj_prop if obj_prop.get("name") in custom_props]
        return self._generate_field_schemas_from_properties(classified_object_properties)

    def _generate_field_schemas_from_properties(self, column_properties: List) -> List[FieldSchema]:
        columns = []
        for column_property in column_properties:
            keboola_type = self._convert_hubspot_type_to_keboola_base_type(
                column_property.get("type"))
            columns.append(FieldSchema(name=column_property.get("name"),
                                       base_type=keboola_type,
                                       description=column_property.get("description")))
        return columns

    def fetch_and_write_to_table(self, object_name: str, data_generator: Callable, data_generator_kwargs) -> None:
        for page in data_generator(**data_generator_kwargs):
            for item in page:
                c = item.to_dict()
                properties = {}

                if "properties" in c:
                    properties = c.pop("properties")

                if "associations" in c:
                    c.pop("associations")

                properties_with_history = None
                if "properties_with_history" in c:
                    properties_with_history = c.pop("properties_with_history")

                if properties_with_history and self._configuration.additional_properties.fetch_property_history:
                    property_history = self._process_property_history(object_name,
                                                                      c.get(
                                                                          "id"),
                                                                      properties_with_history)
                    self._table_handler_cache["property_history"].writerows(
                        property_history)

                self._table_handler_cache[object_name].writerow(
                    {**c, **properties})

    def _process_endpoint_with_custom_schema(self, schema_name: str, data_generator: Callable, **kwargs) -> None:
        logging.info(f"Downloading all {schema_name.replace('_', ' ')}s")
        schema = self.get_table_schema_by_name(schema_name)

        self._init_table_handler(schema_name, schema)

        parser = FlattenJsonParser(
            max_parsing_depth=self.override_parser_depth)

        for page in data_generator(**kwargs):
            parsed_data = parser.parse_data(page)
            self._table_handler_cache[schema_name].writerows(parsed_data)

    def _init_property_history_table_handler(self):
        table_schema = self.get_table_schema_by_name("property_history")
        self._init_table_handler("property_history", table_schema)

    def _init_table_handler(self, handler_name, table_schema):
        if handler_name not in self._table_handler_cache:
            incremental = self._configuration.destination_settings.load_mode != "full_load"

            table_definition = self.create_out_table_definition_from_schema(
                table_schema, incremental=incremental)

            all_columns = self._add_columns_from_state_to_column_list(handler_name,
                                                                      copy.copy(table_definition.columns))

            table_definition.columns = all_columns

            writer = ElasticDictWriter(
                table_definition.full_path, table_definition.columns)
            self._table_handler_cache[handler_name] = TableHandler(
                table_definition, writer)

    def _add_columns_from_state_to_column_list(self, object_name: str, column_list) -> List:
        columns_in_state = self.state.get(object_name, [])
        all_columns = columns_in_state + column_list
        all_columns = list(dict.fromkeys(all_columns))
        return all_columns

    def _close_table_handlers(self):
        for table_handler_name in self._table_handler_cache:
            self._close_table_handler(table_handler_name)
        self._table_handler_cache = {}

    def _close_table_handler(self, table_handler_name: str):
        table_handler = self._table_handler_cache[table_handler_name]

        table_handler.close_writer()
        final_field_names = table_handler.writer_fields
        table_handler.table_definition.columns = final_field_names
        self.state[table_handler_name] = final_field_names

        table_handler.swap_column_names_in_table_definition(
            COLUMN_NAME_SWAP.get(table_handler_name, {}))

        prev_run_cols = self.state.get(table_handler_name, [])
        table_handler.redefine_table_column_metadata(prev_run_cols)
        self.write_manifest(table_handler.table_definition)

    def process_association(self, association):
        try:
            self.fetch_associations(
                association.from_object.value, association.to_object.value)
        except HubspotClientException as e:
            raise UserException(e) from e

    def fetch_associations(self, from_object_type: str, to_object_type: str, id_name: str = 'id'):
        logging.info(
            f"Fetching associations from {from_object_type} to {to_object_type}")
        object_id_generator = self._get_object_ids(
            f"{from_object_type}.csv", id_name)
        association_schema = self.get_table_schema_by_name("association")
        association_schema.name = f"{from_object_type}_to_{to_object_type}_association"

        self._init_table_handler(association_schema.name, association_schema)

        for page in self.client.get_associations(object_id_generator, from_object_type=from_object_type,
                                                 to_object_type=to_object_type):
            parsed_page = self._parse_association(
                page, from_object_type, to_object_type)
            self._table_handler_cache[association_schema.name].writerows(
                parsed_page)

    def _get_object_ids(self, file_name: str, id_name: str):
        table_path = path.join(self.tables_out_path, file_name)
        metadata_path = f"{table_path}.manifest"
        with open(metadata_path) as manifest_file:
            manifest = json.loads(manifest_file.read())

        with open(table_path) as infile:
            reader = csv.DictReader(infile, fieldnames=manifest["columns"])
            for line in reader:
                yield line.get(id_name)

    @staticmethod
    def _parse_association(raw_data: List, from_object_type: str, to_object_type: str):
        parsed_data = []
        for associations in raw_data:
            from_id = associations._from.id  # noqa
            parsed_data.extend({"from_id": from_id, "to_id": association_to.id, "from_object_type": from_object_type,
                                "to_object_type": to_object_type} for association_to in associations.to)

        return parsed_data

    def _parse_date(self, date_to_parse: str) -> int:
        if date_to_parse.lower() in {"last", "lastrun", "last run"}:
            state = self.get_state_file()
            # remove 1 hour / 3600000ms so there is no issue if data is being downloaded at the same time an object is
            # being inserted/ being updated
            return int(state.get("last_run", int(dateparser.parse(DEFAULT_DATE_FROM).timestamp() * 1000))) - 3600000
        try:
            parsed_timestamp = int(dateparser.parse(
                date_to_parse).timestamp() * 1000)
        except (AttributeError, TypeError) as err:
            raise UserException(f"Failed to parse date {date_to_parse}, make sure the date is either in YYYY-MM-DD "
                                f"format or relative date i.e. 5 days ago, 1 month ago, yesterday, etc.") from err
        return parsed_timestamp

    def validate_associations(self) -> None:
        fetching_endpoints = [endpoint_name for endpoint_name, fetch_endpoint in
                              vars(self._configuration.endpoints).items() if
                              fetch_endpoint]

        endpoints_in_associations = [
            association.from_object for association in self._configuration.associations]
        for endpoint in endpoints_in_associations:
            if endpoint not in fetching_endpoints:
                raise UserException(f"All objects for which associations should be fetched must be present "
                                    f"in the selected endpoints to be downloaded. The object '{endpoint}' "
                                    f"is not specified in the objects to fetch : '{fetching_endpoints}.")

    @staticmethod
    def _parse_properties(properties: Union[str, List]) -> List:
        if isinstance(properties, str):
            return [p.strip() for p in properties.split(",")] if properties else []
        elif len(properties) == 1:
            # in case the user saves the config when the comma separated list is still in the properties
            # the "item1,item2" becomes ["item1,item2"] and this must be parsed
            if properties[0].count(',') >= 1:
                return [p.strip() for p in properties[0].split(",")]
        else:
            return properties

    @staticmethod
    def _timestamp_to_datetime(time_in_millis: int) -> str:
        return str(datetime.datetime.fromtimestamp(time_in_millis / 1000.0, tz=datetime.timezone.utc))

    @staticmethod
    def _process_property_history(hs_object_name, hs_object_id, properties_with_history):
        history = []
        if not properties_with_history:
            properties_with_history = {}
        for property_history in properties_with_history:
            for history_event in properties_with_history[property_history]:
                parsed_history_event = {"hs_object": hs_object_name,
                                        "hs_object_id": hs_object_id,
                                        "hs_object_property_name": property_history,
                                        "source_id": history_event.source_id,
                                        "source_label": history_event.source_label,
                                        "source_type": history_event.source_type,
                                        "updated_by_user_id": history_event.updated_by_user_id,
                                        "value": history_event.value,
                                        "timestamp": history_event.timestamp}
                history.append(parsed_history_event)

        return history

    @staticmethod
    def _convert_hubspot_type_to_keboola_base_type(hubspot_type: str) -> SupportedDataTypes:
        type_conversions = {"number": SupportedDataTypes.NUMERIC,
                            "string": SupportedDataTypes.STRING,
                            "datetime": SupportedDataTypes.TIMESTAMP,
                            "date": SupportedDataTypes.DATE,
                            "enumeration": SupportedDataTypes.STRING,
                            "bool": SupportedDataTypes.BOOLEAN,
                            "phone_number": SupportedDataTypes.STRING,
                            "json": SupportedDataTypes.STRING}
        # TODO FIX JSON PARSING FOR CRM OBJECTS
        return type_conversions.get(hubspot_type, SupportedDataTypes.STRING)

    def _add_base_fields_to_field_schema_list(self, columns: List[FieldSchema]) -> List[FieldSchema]:
        for base_column in ["archived_at", "archived", "created_at", "updated_at", "id"]:
            self.insert_base_column(columns, base_column)
        return columns

    @staticmethod
    def insert_base_column(columns, column_name):
        column_exists = any(column_schema.name ==
                            column_name for column_schema in columns)
        if not column_exists:
            columns.insert(0, FieldSchema(
                name=column_name, description="", base_type=SupportedDataTypes.STRING))

    def _fetch_object_properties(self, object_name: str) -> List[SelectElement]:
        self._init_configuration()
        self._init_client()
        obj_prop = self.client.get_crm_object_properties(object_name)
        return [SelectElement(value=prop['name'], label=prop["label"]) for prop in obj_prop]

    @sync_action('loadContactProperties')
    def load_contact_properties(self) -> List[SelectElement]:
        return self._fetch_object_properties("contact")

    @sync_action('loadCompanyProperties')
    def load_company_properties(self) -> List[SelectElement]:
        return self._fetch_object_properties("company")

    @sync_action('loadDealProperties')
    def load_deal_properties(self) -> List[SelectElement]:
        return self._fetch_object_properties("deal")

    @sync_action('loadLineItemProperties')
    def load_line_item_properties(self) -> List[SelectElement]:
        return self._fetch_object_properties("line_item")

    @sync_action('loadProductProperties')
    def load_product_properties(self) -> List[SelectElement]:
        return self._fetch_object_properties("product")

    @sync_action('loadTicketProperties')
    def load_ticket_properties(self) -> List[SelectElement]:
        return self._fetch_object_properties("ticket")

    @sync_action('loadQuoteProperties')
    def load_quote_properties(self) -> List[SelectElement]:
        return self._fetch_object_properties("quote")

    @sync_action('loadCallProperties')
    def load_call_properties(self) -> List[SelectElement]:
        return self._fetch_object_properties("call")

    @sync_action('loadEmailProperties')
    def load_email_properties(self) -> List[SelectElement]:
        return self._fetch_object_properties("email")

    @sync_action('loadMeetingProperties')
    def load_meeting_properties(self) -> List[SelectElement]:
        return self._fetch_object_properties("meeting")

    @sync_action('loadNoteProperties')
    def load_note_properties(self) -> List[SelectElement]:
        return self._fetch_object_properties("note")

    @sync_action('loadTaskProperties')
    def load_task_properties(self) -> List[SelectElement]:
        return self._fetch_object_properties("task")


if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
