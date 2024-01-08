import dataclasses
import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Union, Optional

import dataconf


class ConfigurationBase:
    @staticmethod
    def _convert_private_value(value: str):
        return value.replace('"#', '"pswd_')

    @staticmethod
    def _convert_private_value_inv(value: str):
        if value and value.startswith("pswd_"):
            return value.replace("pswd_", "#", 1)
        else:
            return value

    @classmethod
    def load_from_dict(cls, configuration: dict):
        """
        Initialize the configuration dataclass object from dictionary.
        Args:
            configuration: Dictionary loaded from json configuration.

        Returns:

        """
        json_conf = json.dumps(configuration)
        json_conf = ConfigurationBase._convert_private_value(json_conf)
        return dataconf.loads(json_conf, cls, ignore_unexpected=True)

    @classmethod
    def get_dataclass_required_parameters(cls) -> list[str]:
        """
        Return list of required parameters based on the dataclass definition (no default value)
        Returns: list[str]

        """
        return [cls._convert_private_value_inv(f.name)
                for f in dataclasses.fields(cls)
                if f.default == dataclasses.MISSING
                and f.default_factory == dataclasses.MISSING]


@dataclass
class Endpoints(ConfigurationBase):
    campaign: bool = False
    contact: bool = False
    company: bool = False
    deal: bool = False
    line_item: bool = False
    quote: bool = False
    product: bool = False
    owner: bool = False
    ticket: bool = False
    contact_list: bool = False
    email_event: bool = False
    form: bool = False
    pipeline: bool = False
    note: bool = False
    call: bool = False
    task: bool = False
    meeting: bool = False
    email: bool = False
    email_statistic: bool = False

    @property
    def enabled(self):
        return [endpoint_name for endpoint_name, endpoint_is_enabled in vars(self).items() if endpoint_is_enabled]


class ObjectProperties(str, Enum):
    ALL = "all"
    BASE = "base"
    CUSTOM = "custom"


@dataclass
class AdditionalProperties(ConfigurationBase):
    fetch_property_history: bool = False
    email_event_types: Union[str, list[str]] = field(default_factory=list)
    contact_properties: Union[str, list[str]] = field(default_factory=list)
    contact_property_history: Union[str, list[str]] = field(default_factory=list)
    company_properties: Union[str, list[str]] = field(default_factory=list)
    company_property_history: Union[str, list[str]] = field(default_factory=list)
    deal_properties: Union[str, list[str]] = field(default_factory=list)
    deal_property_history: Union[str, list[str]] = field(default_factory=list)
    line_item_properties: Union[str, list[str]] = field(default_factory=list)
    line_item_property_history: Union[str, list[str]] = field(default_factory=list)
    product_properties: Union[str, list[str]] = field(default_factory=list)
    product_property_history: Union[str, list[str]] = field(default_factory=list)
    ticket_properties: Union[str, list[str]] = field(default_factory=list)
    ticket_property_history: Union[str, list[str]] = field(default_factory=list)
    quote_properties: Union[str, list[str]] = field(default_factory=list)
    quote_property_history: Union[str, list[str]] = field(default_factory=list)
    call_properties: Union[str, list[str]] = field(default_factory=list)
    call_property_history: Union[str, list[str]] = field(default_factory=list)
    email_properties: Union[str, list[str]] = field(default_factory=list)
    email_property_history: Union[str, list[str]] = field(default_factory=list)
    meeting_properties: Union[str, list[str]] = field(default_factory=list)
    meeting_property_history: Union[str, list[str]] = field(default_factory=list)
    note_properties: Union[str, list[str]] = field(default_factory=list)
    note_property_history: Union[str, list[str]] = field(default_factory=list)
    task_properties: Union[str, list[str]] = field(default_factory=list)
    task_property_history: Union[str, list[str]] = field(default_factory=list)
    object_properties: ObjectProperties = ObjectProperties.BASE


class HubspotObject(str, Enum):
    CAMPAIGN = "campaign"
    CONTACT = "contact"
    COMPANY = "company"
    DEAL = "deal"
    QUOTE = "quote"
    PRODUCT = "product"
    OWNER = "owner"
    TICKET = "ticket"
    FORM = "form"
    PIPELINE = "pipeline"
    NOTE = "note"
    CALL = "call"
    TASK = "task"
    MEETING = "meeting"
    EMAIL = "email"


@dataclass
class Association(ConfigurationBase):
    from_object: HubspotObject
    to_object: HubspotObject


class FetchMode(str, Enum):
    FULL_FETCH = "full_fetch"
    INCREMENTAL_FETCH = "incremental_fetch"


@dataclass
class FetchSettings(ConfigurationBase):
    archived: bool = False
    fetch_mode: FetchMode = FetchMode.FULL_FETCH
    date_from: str = "yesterday"


class LoadMode(str, Enum):
    FULL_LOAD = "full_load"
    INCREMENTAL_LOAD = "incremental_load"


@dataclass
class DestinationSettings(ConfigurationBase):
    load_mode: LoadMode = LoadMode.INCREMENTAL_LOAD


@dataclass
class Configuration(ConfigurationBase):
    pswd_private_app_token: str
    endpoints: Endpoints
    additional_properties: AdditionalProperties
    associations: list[Association]
    fetch_settings: FetchSettings
    destination_settings: DestinationSettings
    override_parser_depth: Optional[int] = None
