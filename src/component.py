import logging
import dateparser

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
KEY_DESTINATION = "destination"
KEY_LOAD_TYPE = "load_type"
KEY_CONTACT_PROPERTIES = "contact_properties"

KEY_EMAIL_METRICS_SINCE = "email_metrics_since"

REQUIRED_PARAMETERS = [KEY_ACCESS_TOKEN]
REQUIRED_IMAGE_PARS = []

ENDPOINT_LIST = ["campaigns", "contacts", "companies", "deals", "deal_line_items", "quotes", "products", "owners",
                 "tickets", "contact_lists", "email_events", "forms", "pipelines", "engagements", "email_statistics"]


class Component(ComponentBase):

    def __init__(self):
        self.client = None
        self.state = {}
        self.endpoint_func_mapping = {
            "campaigns": self.get_campaigns,
            "contacts": self.get_contacts,
            "companies": self.get_companies,
            "deals": self.get_deals,
            "deal_line_items": self.get_line_items,
            "quotes": self.get_quotes,
            "products": self.get_products,
            "owners": self.get_owners,
            "tickets": self.get_tickets,
            "contact_lists": self.get_contact_lists,
            "email_events": self.get_email_events,
            "forms": self.get_forms,
            "pipelines": self.get_pipelines,
            "engagements": self.get_engagements,
            "email_statistics": self.get_email_statistics
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
        for endpoint in endpoints_to_fetch:
            if endpoint in ENDPOINT_LIST:
                self.endpoint_func_mapping[endpoint]()
            else:
                raise UserException(f"Endpoint : {endpoint} is not valid")
        self.write_state_file(self.state)

    def get_contacts(self) -> None:
        self.fetch_and_save_crm_object("contact", self.client.get_contacts)

    def get_companies(self) -> None:
        self.fetch_and_save_crm_object("company", self.client.get_companies)

    def get_associations(self) -> None:
        self.fetch_and_save_crm_object("association", self.client.get_associations)

    def get_deals(self) -> None:
        self.fetch_and_save_crm_object("deals", self.client.get_deals)

    def get_line_items(self) -> None:
        self.fetch_and_save_crm_object("line_item", self.client.get_line_items)

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

        pipeline_table.columns = pipeline_writer.fieldnames
        self.write_manifest(pipeline_table)
        self.state["pipeline"] = pipeline_writer.fieldnames

        pipeline_stage_table.columns = pipeline_stage_writer.fieldnames
        self.write_manifest(pipeline_stage_table)
        self.state["pipeline_stage"] = pipeline_stage_writer.fieldnames

    def get_products(self) -> None:
        self.fetch_and_save_crm_object("product", self.client.get_products)

    def get_quotes(self) -> None:
        self.fetch_and_save_crm_object("quote", self.client.get_quotes)

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
        table_definition.columns = writer.fieldnames
        self.write_manifest(table_definition)
        self.state["owner"] = writer.fieldnames

    def get_tickets(self) -> None:
        self.fetch_and_save_crm_object("ticket", self.client.get_tickets)

    def fetch_and_save_crm_object(self, object_name: str, data_generator: Callable) -> None:
        logging.info(f"Downloading all data of object {object_name}")
        object_properties = self.client.get_crm_object_properties(object_name)
        columns = self._generate_field_schemas_from_properties(object_properties)
        table_schema = TableSchema(name=object_name, primary_keys=["id"], fields=columns)
        table_definition = self.create_out_table_definition_from_schema(table_schema, incremental=self.incremental)
        table_definition.columns = self.get_deduplicated_list_of_columns(object_name, table_schema)
        self.write_to_table(object_name, table_definition, data_generator,
                            {"properties": table_definition.columns})

    def write_to_table(self, object_name: str, table_definition: TableDefinition, data_generator: Callable,
                       data_generator_kwargs) -> None:
        writer = ElasticDictWriter(table_definition.full_path, table_definition.columns)
        for page in data_generator(**data_generator_kwargs):
            for item in page:
                c = item.to_dict()
                writer.writerow({"id": c["id"], **(c["properties"])})
        writer.close()
        table_definition = self._normalize_column_names(writer.fieldnames, table_definition)
        self.write_manifest(table_definition)
        self.state[object_name] = writer.fieldnames

    def get_deduplicated_list_of_columns(self, object_name: str, table_schema: TableSchema) -> List:
        columns_in_state = self.state.get(object_name, [])
        all_columns = columns_in_state + table_schema.field_names
        all_columns = list(dict.fromkeys(all_columns))
        return all_columns

    def get_engagements(self) -> None:
        logging.info("Downloading all Engagements")
        logging.info("Downloading all Engagements : Notes ")
        self.fetch_and_save_crm_object("notes", self.client.get_engagements_notes)
        logging.info("Downloading all Engagements : Calls")
        self.fetch_and_save_crm_object("calls", self.client.get_engagements_calls)
        logging.info("Downloading all Engagements : Tasks")
        self.fetch_and_save_crm_object("tasks", self.client.get_engagements_tasks)
        logging.info("Downloading all Engagements : Meetings")
        self.fetch_and_save_crm_object("meetings", self.client.get_engagements_meetings)
        logging.info("Downloading all Engagements : Emails")
        self.fetch_and_save_crm_object("emails", self.client.get_engagements_emails)

    def get_campaigns(self) -> None:
        self.fetch_and_write_endpoint_with_custom_schema("campaign", self.client.get_campaigns)

    def get_contact_lists(self) -> None:
        self.fetch_and_write_endpoint_with_custom_schema("contact_list", self.client.get_contact_lists)

    def get_forms(self) -> None:
        self.fetch_and_write_endpoint_with_custom_schema("form", self.client.get_forms)

    def get_email_events(self) -> None:
        self.fetch_and_write_endpoint_with_custom_schema("email_event", self.client.get_email_events)

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
        table.columns = writer.fieldnames
        self.write_manifest(table)
        self.state[schema_name] = writer.fieldnames

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

    @staticmethod
    def _normalize_column_names(column_names: List, table_definition: TableDefinition) -> TableDefinition:
        """
        Function to make the columns conform to KBC
        """

        key_map = {}
        for i, column_name in enumerate(column_names):
            if len(column_name) >= 64:
                key_map[column_name] = column_name[:63]
                column_names[i] = column_name[:63]
        table_definition.table_metadata.column_metadata = {key_map.get(k, k): v for (k, v) in
                                                           table_definition.table_metadata.column_metadata.items()}
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
