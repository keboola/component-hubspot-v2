import csv
import logging
import dateparser
from os import path
import json
import hashlib

from typing import Callable, List

from keboola.component.base import ComponentBase
from keboola.csvwriter import ElasticDictWriter
from keboola.component.exceptions import UserException
from keboola.component.dao import SupportedDataTypes, TableDefinition
from keboola.component.table_schema import TableSchema, FieldSchema

from json_parser import FlattenJsonParser
from client import HubspotClient

KEY_ACCESS_TOKEN = "#private_app_token"
KEY_ENDPOINT = "endpoints"
KEY_ASSOCIATIONS = "associations"
KEY_DESTINATION = "destination"
KEY_LOAD_TYPE = "load_type"

KEY_ARCHIVED = "archived"
KEY_OBJECT_PROPERTIES = "object_properties"  # base, all, custom

KEY_EMAIL_METRICS_SINCE = "email_metrics_since"

REQUIRED_PARAMETERS = [KEY_ACCESS_TOKEN]
REQUIRED_IMAGE_PARS = []

ENDPOINT_LIST = ["campaign", "contact", "company", "deal", "deal_line_item", "quote", "product", "owner",
                 "ticket", "contact_list", "email_event", "form", "pipeline", "note", "call", "task",
                 "meeting", "email", "email_statistic"]

COLUMN_NAME_SWAP = {"contact_list": {"listId": "id"}}


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
        super().__init__()

    def run(self):
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)

        params = self.configuration.parameters
        access_token = params.get(KEY_ACCESS_TOKEN)

        self.state = self.get_state_file()

        destination_settings = params.get(KEY_DESTINATION, {})
        load_type = destination_settings.get(KEY_LOAD_TYPE, "incremental_load")
        self.incremental = load_type != "full_load"

        self.client = HubspotClient(access_token=access_token)

        endpoints_to_fetch = params.get(KEY_ENDPOINT)
        associations_to_fetch = params.get(KEY_ASSOCIATIONS, [])
        self.validate_associations(endpoints_to_fetch, associations_to_fetch)

        for endpoint in endpoints_to_fetch:
            if endpoint in ENDPOINT_LIST:
                self.endpoint_func_mapping[endpoint]()
            else:
                raise UserException(f"Endpoint : {endpoint} is not valid")

        for association in associations_to_fetch:
            object_from = association.get("object_from")
            object_to = association.get("object_to")
            self.fetch_associations(object_from, object_to)

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
        self.fetch_and_write_endpoint_with_custom_schema("email_event", self.client.get_email_events)

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

        self.state["pipeline"] = pipeline_writer.fieldnames
        self.state["pipeline_stage"] = pipeline_stage_writer.fieldnames

        pipeline_table.columns = pipeline_writer.fieldnames
        self.write_manifest(pipeline_table)

        pipeline_stage_table.columns = pipeline_stage_writer.fieldnames
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
        self.state["owner"] = writer.fieldnames
        table_definition.columns = writer.fieldnames
        self.write_manifest(table_definition)

    def get_tickets(self) -> None:
        self.fetch_and_save_crm_object("ticket", self.client.get_tickets)

    def fetch_and_save_crm_object(self, object_name: str, data_generator: Callable) -> None:
        logging.info(f"Downloading all data of object {object_name}")
        get_archived = self.configuration.parameters.get(KEY_ARCHIVED, False)
        columns = self._get_additional_properties_to_fetch(object_name)
        table_schema = TableSchema(name=object_name, primary_keys=["id"], fields=columns)
        table_definition = self.create_out_table_definition_from_schema(table_schema, incremental=self.incremental)
        table_definition.columns = self.get_deduplicated_list_of_columns(object_name, table_schema)
        extra_arguments = {"properties": table_definition.columns, "archived": get_archived}
        self.fetch_and_write_to_table(object_name, table_definition, data_generator, extra_arguments)

    def _get_additional_properties_to_fetch(self, object_name) -> List[FieldSchema]:
        object_properties_mode = self.configuration.parameters.get(KEY_OBJECT_PROPERTIES, "all")

        if object_properties_mode == "all":
            obj_prop = self.client.get_crm_object_properties(object_name)
            columns = self._generate_field_schemas_from_properties(obj_prop)
        elif object_properties_mode == "custom":
            custom_props_str = self.configuration.parameters.get(f"{object_name}_properties", "")
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
            columns.append(FieldSchema(name="id", base_type=SupportedDataTypes.STRING))
        return columns

    def fetch_and_write_to_table(self, object_name: str, table_definition: TableDefinition, data_generator: Callable,
                                 data_generator_kwargs) -> None:
        writer = ElasticDictWriter(table_definition.full_path, table_definition.columns)
        for page in data_generator(**data_generator_kwargs):
            for item in page:
                c = item.to_dict()
                writer.writerow({"id": c["id"], **(c["properties"])})
        writer.close()
        self.state[object_name] = writer.fieldnames
        table_definition = self._normalize_column_names(writer.fieldnames, table_definition)
        self.write_manifest(table_definition)

    def get_deduplicated_list_of_columns(self, object_name: str, table_schema: TableSchema) -> List:
        columns_in_state = self.state.get(object_name, [])
        all_columns = columns_in_state + table_schema.field_names
        all_columns = list(dict.fromkeys(all_columns))
        return all_columns

    def get_email_statistics(self) -> None:
        email_metrics_since_raw = self.configuration.parameters.get(KEY_EMAIL_METRICS_SINCE)
        email_metrics_since = self._parse_date(email_metrics_since_raw)
        self.fetch_and_write_endpoint_with_custom_schema("email_statistic", self.client.get_email_statistics,
                                                         updated_since=email_metrics_since)

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
        self.state[schema_name] = writer.fieldnames
        table.columns = writer.fieldnames
        table = self._update_column_names(schema_name, writer.fieldnames, table)
        table = self._normalize_column_names(writer.fieldnames, table)
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
            return state.get("last_run", "1997-01-01")
        try:
            parsed_timestamp = int(dateparser.parse(date_to_parse).timestamp() * 1000)
        except (AttributeError, TypeError) as err:
            raise UserException(f"Failed to parse date {date_to_parse}") from err
        self.state["last_run"] = parsed_timestamp
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

        return table_definition

    @staticmethod
    def validate_associations(endpoints_to_fetch, associations_to_fetch):
        endpoints_in_associations = [association.get("object_from") for association in associations_to_fetch]
        for endpoint in endpoints_in_associations:
            if endpoint not in endpoints_to_fetch:
                raise UserException(f"All objects for which associations should be fetched must be present "
                                    f"in the selected endpoints to be downloaded. The object '{endpoint}' "
                                    f"is not specified in the objects to fetch : '{endpoints_to_fetch}.")


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
