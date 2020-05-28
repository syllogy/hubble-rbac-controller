//+build integration

package redshift


//YOU MUST RUN docker-compose up PRIOR TO RUNNING THIS TEST

import (
	"testing"
)

func TestClient_CreateUser(t *testing.T) {

	redshiftClient, _ := NewClient("lunarway","lunarway","localhost:5432","lunarway")
	redshiftClient.CreateUser("jwr", "bianalyst", "bianalyst")
}

