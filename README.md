# hubble-rbac-controller

## Manual set up of the google integration

The google integration is using the G Suite Admin SDK. A google account has been set up to allow us to test the integration. The setup involves a manual process that we document here if you ever need to repeat it (e.g. against another account):

You need to ensure that the G-Suite admin module allows API calls.
See this guide for more details: https://developers.google.com/admin-sdk/directory/v1/guides/prerequisites  

The calls are authorized via a service account that has been set up manually by following this guide:
https://developers.google.com/admin-sdk/directory/v1/guides/delegation
You'll need to generate credentials for the service account and download those credentials to a local json file and use that to authenticate calls to the API.

In order to be able to set AWS_SAML properties on users, a custom schema needs to be defined that allows you to set the property.
Run the script `scripts/gsuite_create_schema.py` to create the custom schema.

