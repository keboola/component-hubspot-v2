import csv
import logging
import dateparser
from os import path
import json
import copy
import hashlib
import warnings
import datetime

from typing import Callable, List

from keboola.component.base import ComponentBase
from keboola.csvwriter import ElasticDictWriter
from keboola.component.exceptions import UserException
from keboola.component.dao import SupportedDataTypes, TableDefinition
from keboola.component.table_schema import TableSchema, FieldSchema

from json_parser import FlattenJsonParser
from client import HubspotClient, HubspotClientException

KEY_ACCESS_TOKEN = "#private_app_token"

KEY_ENDPOINT = "endpoints"

KEY_ADDITIONAL_PROPERTIES = "additional_properties"
KEY_OBJECT_PROPERTIES = "object_properties"  # base, all, custom
KEY_EMAIL_EVENT_TYPES = "email_event_types"

KEY_ASSOCIATIONS = "associations"
KEY_ASSOCIATION_FROM_OBJECT = "from_object"
KEY_ASSOCIATION_TO_OBJECT = "to_object"

KEY_FETCH_SETTINGS = "fetch_settings"
KEY_FETCH_MODE = "fetch_mode"
KEY_DATE_FROM = "date_from"
KEY_ARCHIVED = "archived"

KEY_DESTINATION = "destination_settings"
KEY_LOAD_MODE = "load_mode"

DEFAULT_LOAD_MODE = "incremental_load"
DEFAULT_FETCH_MODE = "full_fetch"
DEFAULT_OBJECT_PROPERTIES = "base"

REQUIRED_PARAMETERS = [KEY_ACCESS_TOKEN, KEY_ENDPOINT, KEY_DESTINATION]
REQUIRED_IMAGE_PARS = []

ENDPOINT_LIST = ["campaign", "contact", "company", "deal", "deal_line_item", "quote", "product", "owner",
                 "ticket", "contact_list", "email_event", "form", "pipeline", "note", "call", "task",
                 "meeting", "email", "email_statistic"]

COLUMN_NAME_SWAP = {"contact_list": {"listId": "id"}}

DEFAULT_DATE_FROM = "1990-01-01"

# Ignore dateparser warnings regarding pytz
warnings.filterwarnings(
    "ignore",
    message="The localize method is no longer necessary, as this time zone supports the fold attribute",
)


class Component(ComponentBase):

    def __init__(self):
        self.client = None
        self.state = {}
        self.endpoint_func_mapping = {
            "campaign": self.get_campaigns,
            "contact": self.get_contacts,
            "company": self.get_companies,
            "deal": self.get_deals,
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

        self.incremental = True
        self.fetch_archived_objects = False
        self.since_fetch_date = ""
        self.incremental_fetch_mode = False
        self.object_properties_mode = None
        super().__init__()

    def run(self):
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)

        self.state = self.get_state_file()

        params = self.configuration.parameters
        access_token = params.get(KEY_ACCESS_TOKEN)

        endpoints_to_fetch = params.get(KEY_ENDPOINT, [])
        associations_to_fetch = params.get(KEY_ASSOCIATIONS, [])

        additional_properties = params.get(KEY_ADDITIONAL_PROPERTIES, [])
        object_properties_mode = additional_properties.get(KEY_OBJECT_PROPERTIES, DEFAULT_OBJECT_PROPERTIES)
        self.object_properties_mode = object_properties_mode

        fetch_settings = params.get(KEY_FETCH_SETTINGS, [])
        fetch_mode = fetch_settings.get(KEY_FETCH_MODE, DEFAULT_FETCH_MODE)
        date_from = fetch_settings.get(KEY_DATE_FROM, DEFAULT_DATE_FROM)
        self.fetch_archived_objects = fetch_settings.get(KEY_ARCHIVED, False)
        self.incremental_fetch_mode = fetch_mode != "full_fetch"
        self.since_fetch_date: int = int(self._parse_date(date_from))
        self.state["last_run"] = self._parse_date("now")

        destination_settings = params.get(KEY_DESTINATION, {})
        load_mode = destination_settings.get(KEY_LOAD_MODE, DEFAULT_LOAD_MODE)
        self.incremental = load_mode != "full_load"

        self.validate_associations(endpoints_to_fetch, associations_to_fetch)

        self.client = HubspotClient(access_token=access_token)

        for endpoint in endpoints_to_fetch:
            if endpoints_to_fetch[endpoint]:
                try:
                    self.endpoint_func_mapping[endpoint]()
                except HubspotClientException as e:
                    raise UserException(e) from e

        for association in associations_to_fetch:
            object_from = association.get(KEY_ASSOCIATION_FROM_OBJECT)
            object_to = association.get(KEY_ASSOCIATION_TO_OBJECT)
            try:
                self.fetch_associations(object_from, object_to)
            except HubspotClientException as e:
                raise UserException(e) from e

        self.write_state_file(self.state)

    def get_contacts(self) -> None:
        self.fetch_and_save_crm_object("contact", self.client.get_contacts)

    def get_companies(self) -> None:
        self.fetch_and_save_crm_object("company", self.client.get_companies)

    def get_deals(self) -> None:
        self.fetch_and_save_crm_object("deal", self.client.get_deals)

    def get_line_items(self) -> None:
        self.fetch_and_save_crm_object("line_item", self.client.get_line_items)

    def get_products(self) -> None:
        self.fetch_and_save_crm_object("product", self.client.get_products)

    def get_quotes(self) -> None:
        self.fetch_and_save_crm_object("quote", self.client.get_quotes)

    def get_notes(self):
        self.fetch_and_save_crm_object("note", self.client.get_engagements_notes)

    def get_calls(self):
        self.fetch_and_save_crm_object("call", self.client.get_engagements_calls)

    def get_tasks(self):
        self.fetch_and_save_crm_object("task", self.client.get_engagements_tasks)

    def get_meetings(self):
        self.fetch_and_save_crm_object("meeting", self.client.get_engagements_meetings)

    def get_emails(self):
        self.fetch_and_save_crm_object("email", self.client.get_engagements_emails)

    def get_campaigns(self) -> None:
        self.fetch_and_write_endpoint_with_custom_schema("campaign", self.client.get_campaigns)

    def get_contact_lists(self) -> None:
        self.fetch_and_write_endpoint_with_custom_schema("contact_list", self.client.get_contact_lists)

    def get_forms(self) -> None:
        self.fetch_and_write_endpoint_with_custom_schema("form", self.client.get_forms)

    def get_email_events(self) -> None:
        additional_props = self.configuration.parameters.get(KEY_ADDITIONAL_PROPERTIES, {})
        email_events = additional_props.get(KEY_EMAIL_EVENT_TYPES, [])
        self.fetch_and_write_endpoint_with_custom_schema("email_event", self.client.get_email_events,
                                                         email_events=email_events)

    def get_pipelines(self) -> None:
        pipeline_schema = self.get_table_schema_by_name("pipeline")
        pipeline_table = self.create_out_table_definition_from_schema(pipeline_schema, incremental=self.incremental)
        pipeline_table.columns = self.get_deduplicated_list_of_columns("pipeline", pipeline_schema)
        pipeline_writer = ElasticDictWriter(pipeline_table.full_path, pipeline_table.columns)

        pipeline_stage_schema = self.get_table_schema_by_name("pipeline_stage")
        pipeline_stage_table = self.create_out_table_definition_from_schema(pipeline_stage_schema,
                                                                            incremental=self.incremental)
        pipeline_stage_table.columns = self.get_deduplicated_list_of_columns("pipeline_stage", pipeline_stage_schema)
        pipeline_stage_writer = ElasticDictWriter(pipeline_stage_table.full_path, pipeline_stage_table.columns)

        deal_pipelines = self.client.get_deal_pipelines()
        ticket_pipelines = self.client.get_ticket_pipelines()

        parser = FlattenJsonParser()

        for deal_pipeline in deal_pipelines:
            stages = deal_pipeline.pop("stages")
            pipeline_id = deal_pipeline.get("id")
            pipeline_writer.writerow(deal_pipeline)
            for stage in stages:
                parsed_stage = parser.parse_row(stage)
                pipeline_stage_writer.writerow({"pipeline_id": pipeline_id, **parsed_stage})

        for ticket_pipeline in ticket_pipelines:
            stages = ticket_pipeline.pop("stages")
            pipeline_id = ticket_pipeline.get("id")
            pipeline_writer.writerow(ticket_pipeline)
            for stage in stages:
                parsed_stage = parser.parse_row(stage)
                pipeline_stage_writer.writerow({"pipeline_id": pipeline_id, **parsed_stage})

        pipeline_writer.close()
        pipeline_stage_writer.close()

        self.state["pipeline"] = copy.deepcopy(pipeline_writer.fieldnames)
        self.state["pipeline_stage"] = copy.deepcopy(pipeline_stage_writer.fieldnames)

        pipeline_table.columns = pipeline_writer.fieldnames
        pipeline_table = self._remove_saved_metadata(pipeline_table, "pipeline")
        self.write_manifest(pipeline_table)

        pipeline_stage_table.columns = pipeline_stage_writer.fieldnames
        pipeline_stage_table = self._remove_saved_metadata(pipeline_stage_table, "pipeline_stage")
        self.write_manifest(pipeline_stage_table)

    def get_owners(self) -> None:
        table_schema = self.get_table_schema_by_name("owner")
        table_definition = self.create_out_table_definition_from_schema(table_schema, incremental=self.incremental)
        table_definition.columns = self.get_deduplicated_list_of_columns("owner", table_schema)
        writer = ElasticDictWriter(table_definition.full_path, table_definition.columns)
        for page in self.client.get_owners():
            for item in page:
                c = item.to_dict()
                writer.writerow(c)
        writer.close()
        self.state["owner"] = copy.deepcopy(writer.fieldnames)
        table_definition.columns = writer.fieldnames
        table_definition = self._remove_saved_metadata(table_definition, "owner")
        self.write_manifest(table_definition)

    def get_tickets(self) -> None:
        self.fetch_and_save_crm_object("ticket", self.client.get_tickets)

    def fetch_and_save_crm_object(self, object_name: str, data_generator: Callable) -> None:
        logging_message = self._generate_crm_object_fetching_message(object_name)
        logging.info(logging_message)

        columns = self._get_additional_properties_to_fetch(object_name)
        table_schema = TableSchema(name=object_name, primary_keys=["id"], fields=columns)

        table_definition = self.create_out_table_definition_from_schema(table_schema, incremental=self.incremental)

        table_definition.columns = self.get_deduplicated_list_of_columns(object_name, table_schema)

        extra_arguments = {"properties": table_definition.columns, "archived": self.fetch_archived_objects,
                           "incremental": self.incremental_fetch_mode, "since_date": self.since_fetch_date}

        self.fetch_and_write_to_table(object_name, table_definition, data_generator, extra_arguments)

    def _generate_crm_object_fetching_message(self, object_name):
        logging_message = f"Downloading data of object {object_name}. "
        if self.incremental_fetch_mode:
            logging_message = f"{logging_message}Fetching data incrementally, from the millisecond timestamp " \
                              f"{self.since_fetch_date}: in UTC : {self._timestamp_to_datetime(self.since_fetch_date)}."
        else:
            logging_message = f"{logging_message} Fetching all data as Full Fetching mode is selected. "
            if self.fetch_archived_objects:
                logging_message = f"{logging_message}Fetching archived data."
        logging_message = f"{logging_message}Fetching {self.object_properties_mode} object properties"
        return logging_message

    @staticmethod
    def _timestamp_to_datetime(time_in_millis: int) -> str:
        return str(datetime.datetime.fromtimestamp(time_in_millis / 1000.0, tz=datetime.timezone.utc))

    def _get_additional_properties_to_fetch(self, object_name) -> List[FieldSchema]:
        if self.object_properties_mode == "all":
            obj_prop = self.client.get_crm_object_properties(object_name)
            columns = self._generate_field_schemas_from_properties(obj_prop)
        elif self.object_properties_mode == "custom":
            additional_props = self.configuration.parameters.get(KEY_ADDITIONAL_PROPERTIES, {})
            custom_props_str = additional_props.get(f"{object_name}_properties", "")
            custom_props = self._parse_properties(custom_props_str)
            obj_prop = self.client.get_crm_object_properties(object_name)
            classified_object_properties = [obj_prop for obj_prop in obj_prop if obj_prop.get("name") in custom_props]
            columns = self._generate_field_schemas_from_properties(classified_object_properties)
        else:
            columns = []

        # It is necessary to add id column if not present as it is not part of the object properties
        columns = self._add_id_to_columns_if_not_present(columns)
        return columns

    @staticmethod
    def _add_id_to_columns_if_not_present(columns: List[FieldSchema]) -> List[FieldSchema]:
        id_exists = any(column_schema.name == "id" for column_schema in columns)
        if not id_exists:
            columns.append(FieldSchema(name="id", description="", base_type=SupportedDataTypes.STRING))
        return columns

    def fetch_and_write_to_table(self, object_name: str, table_definition: TableDefinition, data_generator: Callable,
                                 data_generator_kwargs) -> None:
        writer = ElasticDictWriter(table_definition.full_path, table_definition.columns)
        for page in data_generator(**data_generator_kwargs):
            for item in page:
                c = item.to_dict()
                writer.writerow({"id": c["id"], **(c["properties"])})
        writer.close()
        self.state[object_name] = copy.deepcopy(writer.fieldnames)
        table_definition = self._normalize_column_names(writer.fieldnames, table_definition)
        self.write_manifest(table_definition)

    def get_deduplicated_list_of_columns(self, object_name: str, table_schema: TableSchema) -> List:
        columns_in_state = self.state.get(object_name, [])
        all_columns = columns_in_state + table_schema.field_names
        all_columns = list(dict.fromkeys(all_columns))
        return all_columns

    def get_email_statistics(self) -> None:
        updated_since_timestamp = self.since_fetch_date
        self.fetch_and_write_endpoint_with_custom_schema("email_statistic", self.client.get_email_statistics,
                                                         updated_since=updated_since_timestamp)

    def fetch_and_write_endpoint_with_custom_schema(self, schema_name: str, data_generator: Callable, **kwargs) -> None:
        logging.info(f"Downloading all {schema_name.replace('_', ' ')}s")
        schema = self.get_table_schema_by_name(schema_name)
        table = self.create_out_table_definition_from_schema(schema, incremental=self.incremental)
        table.columns = self.get_deduplicated_list_of_columns(schema_name, schema)
        writer = ElasticDictWriter(table.full_path, table.columns)

        parser = FlattenJsonParser()

        for res in data_generator(**kwargs):
            parsed_data = parser.parse_data(res)
            writer.writerows(parsed_data)

        writer.close()
        self.state[schema_name] = copy.deepcopy(writer.fieldnames)
        table.columns = writer.fieldnames
        table = self._update_column_names(schema_name, copy.deepcopy(writer.fieldnames), table)
        table = self._normalize_column_names(table.columns, table)
        table = self._remove_saved_metadata(table, schema_name)
        self.write_manifest(table)

    def fetch_associations(self, from_object_type: str, to_object_type: str, id_name: str = 'id'):
        logging.info(f"Fetching associations from {from_object_type} to {to_object_type}")
        object_id_generator = self._get_object_ids(f"{from_object_type}.csv", id_name)
        association_schema = self.get_table_schema_by_name("association")
        association_schema.name = f"{from_object_type}_to_{to_object_type}_association"
        table = self.create_out_table_definition_from_schema(association_schema)
        writer = ElasticDictWriter(table.full_path, fieldnames=association_schema.field_names)
        for page in self.client.get_associations(object_id_generator,
                                                 from_object_type=from_object_type, to_object_type=to_object_type):
            parsed_page = self._parse_association(page, from_object_type, to_object_type)
            writer.writerows(parsed_page)

        writer.close()
        self.write_manifest(table)

    @staticmethod
    def _parse_association(raw_data: List, from_object_type: str, to_object_type: str):
        parsed_data = []
        for associations in raw_data:
            from_id = associations._from.id  # noqa
            for association_to in associations.to:
                parsed_data.append(
                    {"from_id": from_id,
                     "to_id": association_to.id,
                     "from_object_type": from_object_type,
                     "to_object_type": to_object_type})
        return parsed_data

    def _get_object_ids(self, file_name: str, id_name: str):
        table_path = path.join(self.tables_out_path, file_name)
        metadata_path = f"{table_path}.manifest"
        with open(metadata_path) as manifest_file:
            manifest = json.loads(manifest_file.read())

        with open(table_path) as infile:
            reader = csv.DictReader(infile, fieldnames=manifest["columns"])
            for line in reader:
                yield line.get(id_name)

    def _generate_field_schemas_from_properties(self, column_properties: List) -> List[FieldSchema]:
        columns = []
        for column_property in column_properties:
            keboola_type = self._convert_hubspot_type_to_keboola_base_type(column_property.get("type"))
            columns.append(FieldSchema(name=column_property.get("name"),
                                       base_type=keboola_type,
                                       description=column_property.get("description")))
        return columns

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
        return type_conversions[hubspot_type]

    @staticmethod
    def _parse_properties(properties: str) -> List:
        return [p.strip() for p in properties.split(",")] if properties else []

    def _parse_date(self, date_to_parse: str) -> int:
        if date_to_parse.lower() in {"last", "lastrun", "last run"}:
            state = self.get_state_file()
            # remove 1 hour / 3600000ms so there is no issue if data is being downloaded at the same time an object is
            # being inserted/ being updated
            return int(state.get("last_run", int(dateparser.parse(DEFAULT_DATE_FROM).timestamp() * 1000))) - 3600000
        try:
            parsed_timestamp = int(dateparser.parse(date_to_parse).timestamp() * 1000)
        except (AttributeError, TypeError) as err:
            raise UserException(f"Failed to parse date {date_to_parse}, make sure the date is either in YYYY-MM-DD "
                                f"format or relative date i.e. 5 days ago, 1 month ago, yesterday, etc.") from err
        return parsed_timestamp

    def _normalize_column_names(self, column_names: List, table_definition: TableDefinition) -> TableDefinition:
        """
        Function to make the columns conform to KBC, max length of column is 64.
        """

        key_map = {}
        for i, column_name in enumerate(column_names):
            if len(column_name) >= 64:
                hashed = self._hash_column(column_name)
                key_map[column_name] = f"{column_name[:30]}_{hashed}"
                column_names[i] = f"{column_name[:30]}_{hashed}"
        table_definition.table_metadata.column_metadata = {key_map.get(k, k): v for (k, v) in
                                                           table_definition.table_metadata.column_metadata.items()}
        return table_definition

    @staticmethod
    def _hash_column(columns_name: str) -> str:
        return hashlib.md5(columns_name.encode('utf-8')).hexdigest()

    @staticmethod
    def _update_column_names(table_name: str, column_names: List,
                             table_definition: TableDefinition) -> TableDefinition:

        column_swaps = COLUMN_NAME_SWAP.get(table_name, {})
        key_map = {}
        for i, column_name in enumerate(column_names):
            if column_name in list(column_swaps.keys()):
                key_map[column_name] = column_swaps[column_name]
                column_names[i] = column_swaps[column_name]
        table_definition.table_metadata.column_metadata = {key_map.get(k, k): v for (k, v) in
                                                           table_definition.table_metadata.column_metadata.items()}
        primary_keys = table_definition.primary_key
        new_primary_keys = []
        for primary_key in primary_keys:
            if primary_key in list(column_swaps.keys()):
                new_primary_keys.append(column_swaps[primary_key])
            else:
                new_primary_keys.append(primary_key)
        table_definition.primary_key = new_primary_keys

        columns = table_definition.columns
        new_columns = []
        for column in columns:
            if column in list(column_swaps.keys()):
                new_columns.append(column_swaps[column])
            else:
                new_columns.append(column)
        table_definition.columns = new_columns

        return table_definition

    @staticmethod
    def validate_associations(endpoints_to_fetch, associations_to_fetch):
        endpoints_in_associations = [association.get(KEY_ASSOCIATION_FROM_OBJECT) for association in
                                     associations_to_fetch]
        for endpoint in endpoints_in_associations:
            if not endpoints_to_fetch[endpoint]:
                raise UserException(f"All objects for which associations should be fetched must be present "
                                    f"in the selected endpoints to be downloaded. The object '{endpoint}' "
                                    f"is not specified in the objects to fetch : '{endpoints_to_fetch}.")

    def _remove_saved_metadata(self, table_definition, table_name):
        new_metadata = {}
        for col_name in table_definition.table_metadata.column_metadata:
            if col_name not in self.state.get(table_name, []):
                new_metadata[col_name] = table_definition.table_metadata.column_metadata[col_name]
        table_definition.table_metadata.column_metadata = new_metadata
        return table_definition


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
