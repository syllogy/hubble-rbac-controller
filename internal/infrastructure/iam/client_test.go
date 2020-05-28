//+build integration

package iam

//YOU MUST RUN docker-compose up PRIOR TO RUNNING THIS TEST

import (
	"testing"
)

func TestClient_ListPolicies(t *testing.T) {

	session := LocalStackSessionFactory{}.CreateSession()
	iamClient := New(session)
	iamClient.ListPolicies()
}
