//+build integration

package redshift


//YOU MUST RUN docker-compose up PRIOR TO RUNNING THIS TEST

import (
	"github.com/lunarway/hubble-rbac-controller/internal/core/utils"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestClient_CreateUser(t *testing.T) {

	assert := assert.New(t)

	schema := "public"
	groupName := "bianalyst"
	username := strings.ToLower(utils.GenerateRandomString(10))

	client, _ := NewClient("lunarway","lunarway","localhost","lunarway", "disable", 5432, false)

	err := client.CreateGroup(groupName)
	assert.NoError(err)

	err = client.CreateUser(username)
	assert.NoError(err)

	users, err := client.Users()
	assert.NoError(err)
	assert.Contains(users, username)

	err = client.AddUserToGroup(username, groupName)
	assert.NoError(err)

	groups, err := client.PartOf(username)
	assert.NoError(err)
	assert.Contains(groups, groupName)

	err = client.Grant(groupName, schema)
	assert.NoError(err)

	grants, err := client.Grants(groupName)
	assert.NoError(err)
	assert.Contains(grants, schema)

	err = client.Revoke(groupName, schema)
	assert.NoError(err)

	err = client.RemoveUserFromGroup(username, groupName)
	assert.NoError(err)

	err = client.DeleteGroup(groupName)
	assert.NoError(err)

	err = client.DeleteUser(username)
	assert.NoError(err)
}

