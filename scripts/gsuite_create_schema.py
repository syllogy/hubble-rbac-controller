
##################################################################################################
# Use this script to generate the custom schema used to set the AWS roles
##################################################################################################


from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client.service_account import ServiceAccountCredentials


CLIENT_SECRET = '/Users/jimmyrasmussen/Downloads/gsuite-test-6ad32b5ed2e9.json'  # the credentials downloaded from the GCP Console
ADMIN_USER = 'jwr@chatjing.com'  # The admin user used by the service account
SCHEMA_USER = 'jwr@chatjing.com'  # The user for which the custom schema will be set
SCHEMA_NAME = 'AWS_SAML'  # the name of the schema we want to work with

SCOPES = ['https://www.googleapis.com/auth/admin.directory.userschema',  # to create the schema
          'https://www.googleapis.com/auth/admin.directory.user', ]  # to manage users

credentials = ServiceAccountCredentials.from_json_keyfile_name(CLIENT_SECRET, scopes=SCOPES)
delegated_admin = credentials.create_delegated(ADMIN_USER)
admin_http_auth = delegated_admin.authorize(Http())

admin_sdk = build('admin', 'directory_v1', http=admin_http_auth)

# schema_insert = admin_sdk.schemas().delete(customerId='my_customer',schemaKey='AWS_SAML').execute()

if SCHEMA_NAME not in unique_schemas:
    schema_insert_params = {
        'customerId': 'my_customer',
        'body': {
            'schemaName': SCHEMA_NAME,
            'displayName': 'AWS_SAML',
            'fields': [
                {
                    'fieldName': 'IAM_Role',
                    'fieldType': 'STRING',
                    'displayName': 'Role',
                    'multiValued': True,
                },
                {
                    'fieldName': 'SessionDuration',
                    'fieldType': 'INT64',
                    'displayName': 'SessionDuration',
                    'multiValued': False,
                }
            ]
        },
    }
    schema_insert = admin_sdk.schemas().insert(**schema_insert_params).execute()

