package google

import (
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	admin "google.golang.org/api/admin/directory/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)


type Client struct {
	service      *admin.Service
	awsAccountId string
}

func NewGoogleClient(jsonKey []byte, principalEmail string, awsAccountId string) (*Client, error) {

	service, err := createDirectoryService(jsonKey, principalEmail)

	if err != nil {
		return nil, err
	}

	return &Client{
		service:      service,
		awsAccountId: awsAccountId,
	}, nil
}

// Build and returns an Admin SDK Directory service object authorized with
// the service accounts that act on behalf of the given user.
// Args:
//    user_email: The email of the user. Needs permissions to access the Admin APIs.
// Returns:
//    Admin SDK directory service object.
func createDirectoryService(jsonKey []byte, userEmail string) (*admin.Service, error) {
	ctx := context.Background()

	config, err := google.JWTConfigFromJSON(jsonKey, admin.AdminDirectoryUserScope)
	if err != nil {
		return nil, fmt.Errorf("JWTConfigFromJSON: %v", err)
	}
	config.Subject = userEmail

	ts := config.TokenSource(ctx)

	srv, err := admin.NewService(ctx, option.WithTokenSource(ts))
	if err != nil {
		return nil, fmt.Errorf("NewService: %v", err)
	}
	return srv, nil
}


func (client *Client) Roles(userId string) ([]string, error) {
	googleUser, err := client.service.Users.Get(userId).Projection("full").Do()

	if err != nil {
		return nil, err
	}

	var awsRoles AwsRolesCustomSchemaDTO
	err = json.Unmarshal(googleUser.CustomSchemas["AWS_SAML"], &awsRoles)

	if err != nil {
		return nil, err
	}

	var result []string

	for _, x := range awsRoles.Roles {
		result = append(result, x.Value)
	}

	return result, err
}

func (client *Client) UpdateRoles(userId string, roles []string) error {
	currentRoles, err := client.get(userId)

	if err != nil {
		return err
	}

	currentRoles = currentRoles.Distinct()

	desiredRoles := client.createDTO(roles)

	for _, r := range currentRoles.Roles {
		if !r.isManaged(client.awsAccountId) {
			desiredRoles.Roles = append(desiredRoles.Roles, r)
		}
	}

	return client.update(userId, desiredRoles)
}

func (client *Client) get(userKey string) (AwsRolesCustomSchemaDTO, error) {

	var result AwsRolesCustomSchemaDTO

	user, err := client.service.Users.Get(userKey).Projection("full").Do()
	if err != nil {
		return result, fmt.Errorf("unable to retrieve user: %w", err)
	}

	err = json.Unmarshal(user.CustomSchemas["AWS_SAML"], &result)

	if err != nil {
		//this property might not have been set if the user has not yet been setup with AWS.
		//In that case we return an empty DTO to the client
		log.WithError(err).Warn("unable to load the AWS_SAML property on the user")
		return result, nil
	}

	return result, nil
}

func (client *Client) update(userKey string, awsRoles AwsRolesCustomSchemaDTO) error {

	user, err := client.service.Users.Get(userKey).Projection("full").Do()
	if err != nil {
		return fmt.Errorf("unable to retrieve user: %w", err)
	}

	jsonRaw, err := json.Marshal(awsRoles)
	if err != nil {
		return fmt.Errorf("unable to marshal AwsRoles: %w", err)
	}

	if user.CustomSchemas == nil {
		user.CustomSchemas = make(map[string]googleapi.RawMessage)
	}
	user.CustomSchemas["AWS_SAML"] = jsonRaw

	_, err = client.service.Users.Update(userKey, user).Do()
	if err != nil {
		return fmt.Errorf("unable to update user: %w", err)
	}

	return nil
}

func (client *Client) createDTO(roles []string) AwsRolesCustomSchemaDTO {

	var awsRoles []AwsRoleCustomSchemaDTO

	for _, role := range roles {
		awsRole := AwsRoleCustomSchemaDTO{
			Type:  "work",
			Value: fmt.Sprintf("arn:aws:iam::%s:role/hubble-rbac/%s,arn:aws:iam::%s:saml-provider/GoogleApps", client.awsAccountId, role, client.awsAccountId),
		}
		awsRoles = append(awsRoles, awsRole)
	}
	return AwsRolesCustomSchemaDTO{
		Roles:           awsRoles,
		SessionDuration: 14400,
	}
}

func (client *Client) Users() ([]User, error) {
	var result []User

	response, err := client.service.Users.
		List().
		Projection("full").
		MaxResults(500).
		Do()

	if err != nil {
		return nil, err
	}

	client.mapResponse(response, result)

	if response.NextPageToken != "" {
		err := client.fetchNextPage(response.NextPageToken, result)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

func (client *Client) fetchNextPage(pageToken string, result []User) error {
	response, err := client.service.Users.
		List().
		Projection("full").
		PageToken(pageToken).
		MaxResults(500).
		Do()

	if err != nil {
		return err
	}

	client.mapResponse(response, result)

	if response.NextPageToken != "" {
		return client.fetchNextPage(response.NextPageToken, result)
	}

	return nil
}

func (client *Client) mapResponse(response *admin.Users, result []User) {
	for _, u := range response.Users {
		user := User{
			Id:    u.Id,
			email: u.PrimaryEmail,
		}
		result = append(result, user)
	}
}
