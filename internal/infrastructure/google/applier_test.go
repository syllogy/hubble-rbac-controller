package google

import (
	googleCore "github.com/lunarway/hubble-rbac-controller/internal/core/google"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"testing"
)

func failOnError(err error) {
	if err != nil {
		panic(err)
	}
}

var ServiceAccountFilePath = "/Users/jimmyrasmussen/Downloads/gsuite-test-6ad32b5ed2e9.json"

func TestApplier_SingleRole(t *testing.T) {

	assert := assert.New(t)

	jsonCredentials, err := ioutil.ReadFile(ServiceAccountFilePath)
	assert.NoError(err)

	client, err := NewGoogleClient(jsonCredentials, "jwr@chatjing.com")
	failOnError(err)

	email := "jwr@chatjing.com"

	applier := NewApplier(client)

	model := googleCore.Model{}
	user := model.DeclareUser(email)
	user.Assign("BiAnalyst")

	err = applier.Apply(model)
	assert.NoError(err)

	googleUsers, err := client.Users()
	failOnError(err)

	jwr := applier.userByEmail(googleUsers, email)
	assert.NotNil(jwr)

	googleRoles, err := client.Roles(jwr.Id)
	failOnError(err)
	assert.Equal([]string{"BiAnalyst"}, googleRoles)
}
