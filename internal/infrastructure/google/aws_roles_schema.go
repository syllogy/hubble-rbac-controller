package google

import (
	"fmt"
	"strings"
)

type AwsRoleCustomSchemaDTO struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

type AwsRolesCustomSchemaDTO struct {
	Roles           []AwsRoleCustomSchemaDTO `json:"IAM_Role"`
	SessionDuration int                      `json:"SessionDuration"`
}

func (r AwsRolesCustomSchemaDTO) Distinct() AwsRolesCustomSchemaDTO {
	distinctRoles := make(map[string]AwsRoleCustomSchemaDTO)

	for _, role := range r.Roles {
		id := fmt.Sprintf("%s + %s", role.Type, role.Value)
		distinctRoles[id] = role
	}

	var roles []AwsRoleCustomSchemaDTO
	for _, role := range distinctRoles {
		roles = append(roles, role)
	}

	return AwsRolesCustomSchemaDTO{
		Roles:           roles,
		SessionDuration: r.SessionDuration,
	}
}

func (r AwsRoleCustomSchemaDTO) isManaged(accountId string) bool {
	return strings.Contains(r.Value, "/hubble-rbac/") && strings.Contains(r.Value, accountId)
}
