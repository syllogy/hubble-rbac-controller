//+build xxxx

package google

import (
	googleCore "github.com/lunarway/hubble-rbac-controller/internal/core/google"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
)

func failOnError(err error) {
	if err != nil {
		panic(err)
	}
}

var ServiceAccountFilePath = os.Getenv("GOOGLE_CREDENTIALS_FILE_PATH")

func TestApplier_SingleRole(t *testing.T) {

	assert := assert.New(t)

	jsonCredentials, err := ioutil.ReadFile(ServiceAccountFilePath)
	assert.NoError(err)

	client, err := NewGoogleClient(jsonCredentials, "jwr@chatjing.com", "478824949770")
	failOnError(err)

	email := "jwr@chatjing.com"

	applier := NewApplier(client)

	model := googleCore.Model{}
	user := model.DeclareUser(email)
	user.Assign("GoogleBiAnalyst")

	err = applier.Apply(model)
	assert.NoError(err)

	googleUsers, err := client.Users()
	failOnError(err)

	jwr := applier.userByEmail(googleUsers, email)
	assert.NotNil(jwr)

	googleRoles, err := client.Roles(jwr.Id)
	failOnError(err)
	assert.Equal([]string{"arn:aws:iam::478824949770:role/hubble-rbac/GoogleBiAnalyst,arn:aws:iam::478824949770:saml-provider/GoogleApps"}, googleRoles)
}
