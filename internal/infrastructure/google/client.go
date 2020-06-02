package google

import (
	"encoding/json"
	"fmt"
	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	admin "google.golang.org/api/admin/directory/v1"
	"google.golang.org/api/option"
)



type AwsRole struct {
	Type string `json:"type"`
	Value string `json:"value"`
}

type AwsRoles struct {
	Roles []AwsRole `json:"Role"`
	SessionDuration int `json:"SessionDuration"`
}

type User struct {
	Id string
	email string
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


type Client struct {
	service *admin.Service
}

func NewGoogleClient(jsonKey []byte, principalEmail string) (*Client, error) {

	service, err := createDirectoryService(jsonKey, principalEmail)

	if err != nil {
		return nil, err
	}

	return &Client{service:service}, nil
}

func (client *Client) update(userKey string, awsRoles AwsRoles) error {

	x, err := client.service.Users.Get(userKey).Projection("full").Do()
	if err != nil {
		return fmt.Errorf("unable to retrieve users: %w", err)
	}

	jsonRaw, err := json.Marshal(awsRoles)
	if err != nil {
		return fmt.Errorf("unable to marshal AwsRoles: %w", err)
	}

	x.CustomSchemas["AWS"] = jsonRaw

	_, err = client.service.Users.Update(userKey, x).Do()
	if err != nil {
		return fmt.Errorf("unable to update user: %w", err)
	}

	return nil
}

func (client *Client) createAwsRoles(roles []string) AwsRoles {

	var awsRoles []AwsRole

	for _, role := range roles {
		awsRole := AwsRole{
			Type:  "work",
			Value: role,
		}
		awsRoles = append(awsRoles, awsRole)
	}
	return AwsRoles{
		Roles:awsRoles,
		SessionDuration:14400,
	}
}

func (client *Client) Users() ([]User, error) {

	response, err := client.service.Users.
		List().
		Customer("my_customer"). //TODO: what should this be??
		Projection("full").
		MaxResults(500). //TODO: add support for more than 500 users!!
		Do()

	if err != nil {
		return nil, err
	}

	if len(response.Users) == 500 {
		return nil, fmt.Errorf("too many users, no more than 500 are supported")
	}

	var result []User

	for _,u := range response.Users {
		user := User{
			Id:    u.Id,
			email: u.PrimaryEmail,
		}
		result = append(result, user)
	}

	return result, nil
}

func (client *Client) UpdateRoles(userId string, roles []string) error {
	return client.update(userId, client.createAwsRoles(roles))
}

func (client *Client) Roles(userId string) ([]string, error) {
	googleUser, err := client.service.Users.Get(userId).Projection("full").Do()

	if err != nil {
		return nil, err
	}

	var awsRoles AwsRoles
	err = json.Unmarshal(googleUser.CustomSchemas["AWS"], &awsRoles)

	if err != nil {
		return nil, err
	}

	var result []string

	for _,x := range awsRoles.Roles {
		result = append(result, x.Value)
	}

	return result, err
}
