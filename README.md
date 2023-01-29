Hubspot Extractor
=============

The HubSpot CRM helps companies grow traffic, convert leads, get insights to close more deals, etc.

This component uses the HubSpot API to extract data of the CRM and Marketing objects from Hubspot

**Table of contents:**

[TOC]

Prerequisites
=============
You need to create a [Private App](https://developers.hubspot.com/docs/api/migrate-an-api-key-integration-to-a-private-app) 
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

Supported Endpoints
===================

The following endpoints are downloadable via this component :

* Campaigns
* Companies
* Contacts
* Contact Lists
* Deals
* Deal Line Items
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

Additional Endpoint Options
===================

## Additional properties
Additional properties are available for fetching for the following CRM objects :

* Companies
* Contacts
* Deals
* Deal Line Items
* Calls
* Emails
* Notes
* Meetings
* Tasks
* Products
* Quotes
* Tickets

You can select which properties you want to fetch for each of the above CRM object using the **Property Fetch Mode**
by selecting, **all**, **base**, or **custom**. If you select **all**, then all available properties for each object will be downloaded. 
If you select **base**, then only the base properties are downloaded. 
If you select **custom**, then you can specify a string of a comma separated list of properties you wish to download for each object using the "{{object_name}}_properties" input.

## Email event types

When downloading email events you can specify a comma separated list of the following event types : ["DEFERRED","CLICK","DROPPED","DELIVERED","PROCESSED","OPEN","BOUNCE","SENT"]


Configuration
=============

Param 1
-------

Param 2
-------

Output
=============

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
