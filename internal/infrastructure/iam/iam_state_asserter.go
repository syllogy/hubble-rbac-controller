package iam

import (
	"github.com/stretchr/testify/assert"
	"sort"
)

type IAMState struct {
	Roles map[string][]string
}

func FetchIAMState(client *Client) IAMState {
	roles, _ := client.ListRoles()

	result := make(map[string][]string)

	for _, role := range roles {
		policies := []string{}

		attachedPolicies, _ := client.ListManagedAttachedPolicies(role)

		for _, policy := range attachedPolicies {
			policies = append(policies, *policy.PolicyName)
		}

		attachedPolicies, _ = client.ListUnmanagedAttachedPolicies(role)
		for _, policy := range attachedPolicies {
			policies = append(policies, *policy.PolicyName)
		}

		result[*role.RoleName] = policies
	}

	return IAMState{
		Roles: result,
	}
}

func AssertState(assert *assert.Assertions, actual IAMState, expected IAMState, message string) {

	for roleName, expectedPolicies := range expected.Roles {
		actualPolicies, ok := actual.Roles[roleName]
		assert.True(ok, message)
		sort.Strings(actualPolicies)
		sort.Strings(expectedPolicies)
		assert.Equal(actualPolicies, expectedPolicies, message)
	}

	for roleName, actualPolicies := range actual.Roles {
		expectedPolicies, ok := expected.Roles[roleName]
		assert.True(ok, message)
		sort.Strings(expectedPolicies)
		sort.Strings(actualPolicies)
		assert.Equal(expectedPolicies, actualPolicies, message)
	}
}
