package google

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"testing"
)

func Test_XXX(t *testing.T) {
	jsonCredentials, err := ioutil.ReadFile("/Users/jimmyrasmussen/Downloads/archived/discovery-hubble-d43a9ce51251.json")
	assert.NoError(t, err)
	client, err := NewGoogleClient(jsonCredentials, "discovery-hubble@lunar.app", "899945594626")
	assert.NoError(t, err)
	users, err := client.Users()
	assert.NoError(t, err)
	fmt.Println(len(users))
}
