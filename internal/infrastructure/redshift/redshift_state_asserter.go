package redshift

import (
	"github.com/stretchr/testify/assert"
	"sort"
)

type RedshiftState struct {
	Users            []string
	Groups           []string
	GroupMemberships map[string][]string
	Grants           map[string][]string
}

func NewRedshiftState() RedshiftState {
	return RedshiftState{
		Users:            []string{},
		Groups:           []string{},
		GroupMemberships: map[string][]string{},
		Grants:           map[string][]string{},
	}
}

func FetchState(client *Client) RedshiftState {

	dbGroups, _ := client.Groups()
	dbUsers, _ := client.Users()

	result := NewRedshiftState()

	for _,group := range dbGroups {
		result.Groups = append(result.Groups, group)
	}

	for _,user := range dbUsers {
		result.Users = append(result.Users, user)
	}

	for _,username := range dbUsers {
		dbUserGroups, _ := client.PartOf(username)
		result.GroupMemberships[username] = dbUserGroups
	}

	for _,groupName := range dbGroups {
		dbSchemas, _ := client.Grants(groupName)
		result.Grants[groupName] = dbSchemas
	}

	return result
}

func AssertState(assert *assert.Assertions, actual RedshiftState, expected RedshiftState, message string) {

	sort.Strings(expected.Users)
	sort.Strings(actual.Users)
	assert.Equal(expected.Users, actual.Users, message)

	sort.Strings(expected.Groups)
	sort.Strings(actual.Groups)
	assert.Equal(expected.Groups, actual.Groups, message)

	for user, expectedGroups := range expected.GroupMemberships {
		actualGroups, ok := actual.GroupMemberships[user]
		assert.True(ok)
		sort.Strings(actualGroups)
		sort.Strings(expectedGroups)
		assert.Equal(expectedGroups, actualGroups, message)
	}

	for user, actualGroups := range actual.GroupMemberships {
		expectedGroups, ok := expected.GroupMemberships[user]
		assert.True(ok, message)
		sort.Strings(expectedGroups)
		sort.Strings(actualGroups)
		assert.Equal(expectedGroups, actualGroups, message)
	}

	for group, expectedSchemas := range expected.Grants {
		actualSchemas, ok := actual.Grants[group]
		assert.True(ok, message)
		sort.Strings(actualSchemas)
		sort.Strings(expectedSchemas)
		assert.Equal(expectedSchemas, actualSchemas, message)
	}

	for group, actualSchemas := range actual.Grants {
		expectedSchemas, ok := expected.Grants[group]
		assert.True(ok)
		sort.Strings(expectedSchemas)
		sort.Strings(actualSchemas)
		assert.Equal(expectedSchemas, actualSchemas, message)
	}

}
