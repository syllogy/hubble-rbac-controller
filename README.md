# hubble-rbac-controller

## Manual set up of the google integration

The google integration is using the G Suite Admin SDK, and you need to ensure that the G-Suite admin module allows API calls.
See this guide for more details: https://developers.google.com/admin-sdk/directory/v1/guides/prerequisites  

The calls are authorized via a service account that has been set up manually by following this guide:
https://developers.google.com/admin-sdk/directory/v1/guides/delegation

