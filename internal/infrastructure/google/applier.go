package google

import (
	"fmt"
	googleCore "github.com/lunarway/hubble-rbac-controller/internal/core/google"
)

type Applier interface {
	Apply(model googleCore.Model) error
}

type ApplierImpl struct {
	client *Client
}

func NewApplier(client *Client) *ApplierImpl {
	return &ApplierImpl{client: client}
}

func (applier *ApplierImpl) userByEmail(users []User, email string) *User {
	for _,user := range users {
		if user.email == email {
			return &user
		}
	}
	return nil
}

func (applier *ApplierImpl) Apply(model googleCore.Model) error {

	googleUsers, err := applier.client.Users()

	if err != nil {
		return fmt.Errorf("Unable to retrieve users: %w", err)
	}

	for _,user := range model.Users {
		googleUser := applier.userByEmail(googleUsers, user.Email)

		if googleUser != nil {
			err := applier.client.UpdateRoles(googleUser.Id, user.AssignedTo())

			if err != nil {
				return fmt.Errorf("Unable to update roles: %w", err)
			}
		} else {
			return fmt.Errorf("user %s doesn't exist", user.Email)
		}
	}

	return nil
}