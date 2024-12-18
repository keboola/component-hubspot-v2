# Hubspot Extractor

The HubSpot CRM helps companies grow traffic, convert leads, get insights to close more deals, etc.

This component uses the HubSpot API to extract data of the CRM and Marketing objects from Hubspot

**Table of contents:**

[TOC]

## Prerequisites

You need to create
a [Private App](https://developers.hubspot.com/docs/api/migrate-an-api-key-integration-to-a-private-app)
in your account and enable all following scopes:

* crm.lists.read
* crm.objects.companies.read
* crm.objects.contacts.read
* crm.objects.deals.read
* crm.objects.line_items.read
* crm.objects.marketing_events.read
* crm.objects.owners.read
* crm.objects.quotes.read
* crm.schemas.contacts.read
* crm.schemas.companies.read
* crm.schemas.custom.read
* crm.schemas.deals.read
* crm.schemas.line_items.read
* crm.schemas.quotes.read
* e-commerce
* tickets
* timeline
* forms
* content
* sales-email-read

## Supported Endpoints

The following endpoints are downloadable via this component :

* Campaigns
* Companies
* Contacts
* Contact Lists
* Deals
* Line Items
* Email Events
* Marketing Email Statistics
* Forms
* Engagements :
    * Calls
    * Emails
    * Notes
    * Meetings
    * Tasks
* Owners
* Pipelines
* Products
* Quotes
* Tickets

If you need more endpoints, please submit your request to
[ideas.keboola.com](https://ideas.keboola.com/)

#### Custom Objects
Custom object is available for enterprise licence. You can specify the custom object type you want to download using the `custom_object_types` input.


## Additional Endpoint Options

### Additional properties

Additional properties are available for fetching for the following CRM objects :

* Companies
* Contacts
* Deals
* Line Items
* Calls
* Emails
* Notes
* Meetings
* Tasks
* Products
* Quotes
* Tickets
* Custom Objects (only all properties)

You can select which properties you want to fetch for each of the above CRM object using the **Property Fetch Mode**
by selecting, **all**, **base**, or **custom**. If you select **all**, then all available properties for each object
will be downloaded.
If you select **base**, then only the base properties are downloaded.
If you select **custom**, then you can specify a string of a comma separated list of properties you wish to download for
each object using the "{{object_name}}_properties" input.


### Email event types

When downloading email events you can specify a comma separated list of the following event
types : ["DEFERRED","CLICK","DROPPED","DELIVERED","PROCESSED","OPEN","BOUNCE","SENT"]

## Configuration

* Private App Token (#private_app_token) : str : Your PAT, see how to create one in the prerequisites
  section
* Endpoints (endpoints) : boolean : value indicating whether the endpoint should be fetched
    * Campaign (campaign) : bool
    * Contact (contact) : bool
    * Company (company) : bool
    * Custom Object (custom_object) : bool
    * Deal (deal) : bool
    * Line Item (line_item) : bool
    * Quote (quote) : bool
    * Product (product) : bool
    * Owner (owner) : bool
    * Ticket (ticket) : bool
    * Contact List (contact_list) : bool
    * Email Event (email_event) : bool
    * Form (form) : bool
    * Pipeline (pipeline) : bool
    * Note (note) : bool
    * Call (call) : bool
    * Task (task) : bool
    * Meeting (meeting) : bool
    * Email (email) : bool
    * Email Statistic (email_statistic) : bool
* Additional Properties (additional_properties) :
    * Fetch Property History (fetch_property_history) : bool
    * Email Event Types (email_event_types) : Union[str, list[str]]
    * Contact Properties (contact_properties) : Union[str, list[str]]
    * Contact Property History (contact_property_history) : Union[str, list[str]]
    * Company Properties (company_properties) : Union[str, list[str]]
    * Company Property History (company_property_history) : Union[str, list[str]]
    * Deal Properties (deal_properties) : Union[str, list[str]]
    * Deal Property History (deal_property_history) : Union[str, list[str]]
    * Line Item Properties (line_item_properties) : Union[str, list[str]]
    * Line Item Property History (line_item_property_history) : Union[str, list[str]]
    * Product Properties (product_properties) : Union[str, list[str]]
    * Product Property History (product_property_history) : Union[str, list[str]]
    * Ticket Properties (ticket_properties) : Union[str, list[str]]
    * Ticket Property History (ticket_property_history) : Union[str, list[str]]
    * Quote Properties (quote_properties) : Union[str, list[str]]
    * Quote Property History (quote_property_history) : Union[str, list[str]]
    * Call Properties (call_properties) : Union[str, list[str]]
    * Call Property History (call_property_history) : Union[str, list[str]]
    * Email Properties (email_properties) : Union[str, list[str]]
    * Email Property History (email_property_history) : Union[str, list[str]]
    * Meeting Properties (meeting_properties) : Union[str, list[str]]
    * Meeting Property History (meeting_property_history) : Union[str, list[str]]
    * Note Properties (note_properties) : Union[str, list[str]]
    * Note Property History (note_property_history) : Union[str, list[str]]
    * Task Properties (task_properties) : Union[str, list[str]]
    * Task Property History (task_property_history) : Union[str, list[str]]
    * Object Properties (object_properties) : object_properties
    * Custom Object Types  (custom_object_types) : Union[str, list[str]]
* Associations (associations) : list
* Fetch Settings (fetch_settings) :
    * Archived (archived) : bool
    * Fetch Mode (fetch_mode) : fetch_mode
    * Date From (date_from) : str
* Destination Settings (destination_settings) :
    * Load Mode (load_mode) : load_mode
* Override Parser Depth (override_parser_depth) : Optional[int]

### Object Properties (object_properties) values

    * all
    * base
    * custom

### Fetch Mode (fetch_mode) values

    * full_fetch
    * incremental_fetch

### Load Mode (load_mode) values

    * full_load
    * incremental_load

### Sample Configuration

```json
{
  "parameters": {
    "#private_app_token": "YOUR_PAT",
    "endpoints": {
      "contact": true,
      "pipeline": true,
      "form": true,
      "task": true,
      "deal_line_item": true,
      "deal": true,
      "ticket": true,
      "email_statistic": true,
      "product": true,
      "campaign": true,
      "note": true,
      "owner": true,
      "email_event": true,
      "meeting": true,
      "quote": true,
      "email": true,
      "company": true,
      "contact_list": true,
      "call": true,
      "custom_object": true
    },
    "associations": [
      {
        "to_object": "meeting",
        "from_object": "contact"
      }
    ],
    "fetch_settings": {
      "archived": false,
      "fetch_mode": "full_fetch"
    },
    "destination_settings": {
      "load_mode": "incremental_load"
    },
    "additional_properties": {
      "custom_object_types": ["my_custom_object_type"],
      "object_properties": "custom",
      "email_event_types": [
        "DEFERRED",
        "CLICK",
        "DROPPED",
        "DELIVERED",
        "PROCESSED",
        "OPEN",
        "BOUNCE",
        "SENT"
      ],
      "note_properties": "hs_object_id, hs_body_preview, hs_engagement_source, hs_created_by_user_id, hs_modified_by, hs_engagement_source_id, hs_user_ids_of_all_owners",
      "product_properties": "amount, description, discount, hs_sku, hs_url, hubspot_owner_id, name, price, quantity, recurringbillingfrequency, tax",
      "ticket_properties": "hs_object_id, hs_created_by_user_id, closed_date, created_by, createdate, hs_lastactivitydate, hs_pipeline, hs_resolution, hs_ticket_id, hs_ticket_priority",
      "deal_properties": "point_of_contact, product_of_interest, dealname, amount, dealstage, pipeline, closedate, hs_lastmodifieddate,hs_createdate, createdate, hs_deal_stage_probability, hs_deal_stage_probability_shadow, hs_object_id, amount_in_home_currency, days_to_close, hs_exchange_rate, hs_forecast_amount, hs_forecast_probability, hs_is_closed, hs_is_closed_won, hs_is_deal_split, hs_mrr, hs_projected_amount, hs_projected_amount_in_home_currency",
      "company_hidden": "true",
      "task_properties": "hs_object_id, hs_task_subject, hs_task_type, hs_body_preview, hs_created_by_user_id, hs_engagement_source, hs_task_for_object_type, hs_task_last_contact_outreach, hs_task_last_sales_activity_timestamp, hs_task_priority, hs_task_send_default_reminder, hs_task_status",
      "contact_properties": "hs_facebookid, hs_linkedinid, ip_city, ip_country, ip_country_code, newsletter_opt_in, firstname, linkedin_profile, lastname, email, mobilephone, phone, city, country, region, jobtitle, company, website, numemployees, industry, associatedcompanyid, hs_lead_status, lastmodifieddate, source, hs_email_optout, twitterhandle, lead_type, hubspot_owner_id, notes_last_updated, hs_analytics_source, opt_in, createdate, hs_twitterid, lifecyclestage",
      "call_properties": "hs_call_title, hs_call_status, hs_createdate, hs_lastmodifieddate, hs_object_id, hs_body_preview, hs_created_by, hs_created_by_user_id, hs_engagement_source, hubspot_owner_idm, hs_user_ids_of_all_owners",
      "line_item_properties": "name, price, quantity, createdate, hs_object_id, amount, hs_acv, hs_arr, hs_created_by_user_id, hs_margin, hs_margin_acv, hs_margin_arr, hs_margin_mrr, hs_margin_tcv, hs_mrr, hs_position_on_quote, hs_pre_discount_amount, hs_total_discount, hs_updated_by_user_id",
      "company_properties": "about_us, name, phone, facebook_company_page, city, country, website, industry, annualrevenue, linkedin_company_page, hs_lastmodifieddate, hubspot_owner_id, notes_last_updated, description, createdate, numberofemployees, hs_lead_status, founded_year, twitterhandle, linkedinbio",
      "email_properties": "hubspot_owner_id, hubspot_team_id, hubspot_owner_assigneddate, hs_created_by, hs_createdate, hs_email_bcc_email, hs_email_bcc_firstname, hs_email_bcc_lastname, hs_email_cc_email, hs_email_cc_firstname, hs_email_cc_lastname, hs_email_from_email, hs_email_from_firstname, hs_email_from_lastname, hs_email_headers",
      "quote_properties": "hs_quote_number, hs_object_id, hs_allowed_payment_methods, hs_collect_billing_address, hs_created_by_user_id, hs_language, hs_locale, hs_payment_status, hs_quote_total_preference, hs_template_type, hs_test_mode, hs_sender_firstname, hs_title, hs_expiration_date, hs_quote_amount, hs_status",
      "meeting_properties": "hubspot_owner_id, hs_user_ids_of_all_owners, hs_meeting_start_time,hs_meeting_end_time, hs_body_preview_is_truncated, hs_created_by, hs_created_by_user_id, hs_engagement_source, hs_createdate, hs_lastmodifieddate, hs_object_id, hs_timestamp"
    }
  },
  "action": "run"
}

```

## Output

The component outputs each of the crm objects into their separate table. E.g. Contact data is saved into contact.csv

Development
-----------

If required, change local data folder (the `CUSTOM_FOLDER` placeholder) path to
your custom path in the `docker-compose.yml` file:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Clone this repository, init the workspace and run the component with following
command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
git clone https://bitbucket.org/kds_consulting_team/kds-team.ex-hubspot-v2/src/master/ kds-team.ex-hubspot-v2
cd kds-team.ex-hubspot-v2
docker-compose build
docker-compose run --rm dev
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the test suite and lint check using this command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
docker-compose run --rm test
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Integration
===========

For information about deployment and integration with KBC, please refer to the
[deployment section of developers
documentation](https://developers.keboola.com/extend/component/deployment/)
