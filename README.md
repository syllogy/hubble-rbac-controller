# hubble-rbac-controller

## Description
This repo contains the implementation of a kubernetes controller called hubble-rbac-controller.
The repo https://github.com/lunarway/lunar-way-hubble-rbac-controller contains the actual deployment of the controller at Lunar.
The controller controls HubbleRbac custom resources. A HubbleRbac custom resource declares the users, roles and databases the Hubble platform consists of. They are maintained here: https://github.com/lunarway/hubble-access

## Code structure

To build the code:
```
$ make code/compile 
```


The code structure is loosely based on the onion architecture (also known as hexagonical architecture or clean architecture).
There are 3 main layers: core, infrastructure, kubernetes

### Core
*Located at:* `internal/core` \
*Description:* Contains all the internal logic, there are no infrastructure dependencies, all tests are unit tests.
To run unit tests:
```
$ make test/unit 
```

### Infrastructure
*Located at:* `internal/infrastructure` \
*Description:* contains all infrastructure code, most tests are integration tests.
To run integration tests:
```
$ docker-compose up -d
$ make test/integration
$ docker-compose down 
```

### Kubernetes
*Located at:* `pkg/` \
*Description:* Contains the kubernetes controller and everything related to kubernetes.      
To run the controller locally, define all the env variables needed by the configuration (defined in pgk/configuration/configuration.go) and run:
```
$ make run/local
```


To release a new version, create a git release tag and push it to github.

## Manual set up of the google integration

The google integration is using the G Suite Admin SDK. A google account has been set up to allow us to test the integration. The setup involves a manual process that we document here if you ever need to repeat it (e.g. against another account):

You need to ensure that the G-Suite admin module allows API calls.
See this guide for more details: https://developers.google.com/admin-sdk/directory/v1/guides/prerequisites  

The calls are authorized via a service account that has been set up manually by following this guide:
https://developers.google.com/admin-sdk/directory/v1/guides/delegation
You'll need to generate credentials for the service account and download those credentials to a local json file and use that to authenticate calls to the API.

In order to be able to set AWS_SAML properties on users, a custom schema needs to be defined that allows you to set the property.
Run the script `scripts/gsuite_create_schema.py` to create the custom schema.

