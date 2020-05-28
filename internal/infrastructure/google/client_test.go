package google

import (
	"io/ioutil"
	"log"
	"testing"
)

var ServiceAccountFilePath = "/Users/jimmyrasmussen/Downloads/gsuite-test-6ad32b5ed2e9.json"

func Test_Temp(t *testing.T) {

	jsonCredentials, err := ioutil.ReadFile(ServiceAccountFilePath)
	if err != nil {
		log.Fatalf("Unable to retrieve users in domain: %v", err)
	}

	client, err := NewGoogleClient(jsonCredentials, "jwr@chatjing.com")

	if err != nil {
		panic(err)
	}

	roles := make(map[string][]string)
	roles["jwr@chatjing.com"] = []string{"a", "b"}

	err = client.SetAll(roles)
	if err != nil {
		log.Fatalf("Unable to retrieve users in domain: %v", err)
	}
}
