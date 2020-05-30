//+build integration

package iam

import (
	iamCore "github.com/lunarway/hubble-rbac-controller/internal/core/iam"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

//YOU MUST RUN docker-compose up PRIOR TO RUNNING THIS TEST


type EventRecorder struct {
	events []ApplyEventType
}

func (e *EventRecorder) Handle(eventType ApplyEventType, name string) {
	e.events = append(e.events, eventType)
}

func (e *EventRecorder) count(eventType ApplyEventType) int {
	result := 0
	for _, event := range e.events {
		if event == eventType {
			result += 1
		}
	}
	return result
}

const accountId = "478824949770"
const region = "eu-west-1"

func failOnError(err error) {
	if err != nil {
		panic(err)
	}
}

func init() {
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
}

func setUp() {
	session := LocalStackSessionFactory{}.CreateSession()
	iamClient := New(session)
	applier := NewApplier(iamClient, accountId, region, &EventRecorder{})

	roles, err := iamClient.ListRoles()
	failOnError(err)

	for _, role := range roles {
		err = applier.deleteRole(role)
		failOnError(err)
	}

	policies, err := iamClient.ListPolicies()
	failOnError(err)

	for _, policy := range policies {
		err = iamClient.DeletePolicy(policy)
		failOnError(err)
	}
}

func TestApplier_NoRoles(t *testing.T) {

	setUp()

	assert := assert.New(t)

	eventRecorder := EventRecorder{}
	session := LocalStackSessionFactory{}.CreateSession()
	iamClient := New(session)
	applier := NewApplier(iamClient, accountId, region, &eventRecorder)

	err := applier.Apply(iamCore.Model{Roles:[]*iamCore.AwsRole{}})
	assert.NoError(err)

	roles, err := iamClient.ListRoles()
	failOnError(err)
	assert.Empty(roles)

	policies, err := iamClient.ListPolicies()
	failOnError(err)
	assert.Empty(policies)

	assert.Equal(0, len(eventRecorder.events))
}

func TestApplier_SingleRole(t *testing.T) {

	setUp()

	assert := assert.New(t)

	eventRecorder := EventRecorder{}
	session := LocalStackSessionFactory{}.CreateSession()
	iamClient := New(session)
	applier := NewApplier(iamClient, accountId, region, &eventRecorder)

	err := applier.Apply(iamCore.Model{Roles:[]*iamCore.AwsRole{
		{
			Name:                  "BiAnalyst",
			DatabaseLoginPolicies: []*iamCore.DatabaseLoginPolicy{
				{
					Email:            "jwr@lunar.app",
					DatabaseUsername: "jwr_bianalyst",
					Databases:        []*iamCore.Database{
						{
							ClusterIdentifier: "dev",
							Name:              "jwr",
						},
					},
				},
			},
		},
	}})

	assert.NoError(err)

	roles, err := iamClient.ListRoles()
	failOnError(err)
	assert.Equal(1, len(roles))

	policies, err := iamClient.ListPolicies()
	failOnError(err)
	assert.Equal(1, len(policies))

	assert.Equal(1, eventRecorder.count(RoleCreated))
	assert.Equal(1, eventRecorder.count(PolicyCreated))
}

func TestApplier_SingleRoleTwoDatabases(t *testing.T) {

	setUp()

	assert := assert.New(t)

	eventRecorder := EventRecorder{}
	session := LocalStackSessionFactory{}.CreateSession()
	iamClient := New(session)
	applier := NewApplier(iamClient, accountId, region, &eventRecorder)

	err := applier.Apply(iamCore.Model{Roles:[]*iamCore.AwsRole{
		{
			Name:                  "BiAnalyst",
			DatabaseLoginPolicies: []*iamCore.DatabaseLoginPolicy{
				{
					Email:            "jwr@lunar.app",
					DatabaseUsername: "jwr_bianalyst",
					Databases:        []*iamCore.Database{
						{
							ClusterIdentifier: "dev",
							Name:              "jwr",
						},
						{
							ClusterIdentifier: "hubble-unstable",
							Name:              "prod",
						},
					},
				},
			},
		},
	}})

	assert.NoError(err)

	roles, err := iamClient.ListRoles()
	failOnError(err)
	assert.Equal(1, len(roles))

	policies, err := iamClient.ListPolicies()
	failOnError(err)
	assert.Equal(1, len(policies))

	assert.Equal(1, eventRecorder.count(RoleCreated))
	assert.Equal(1, eventRecorder.count(PolicyCreated))
}

func TestApplier_SingleRoleTwoUsers(t *testing.T) {

	setUp()

	assert := assert.New(t)

	eventRecorder := EventRecorder{}
	session := LocalStackSessionFactory{}.CreateSession()
	iamClient := New(session)
	applier := NewApplier(iamClient, accountId, region, &eventRecorder)

	err := applier.Apply(iamCore.Model{Roles:[]*iamCore.AwsRole{
		{
			Name:                  "BiAnalyst",
			DatabaseLoginPolicies: []*iamCore.DatabaseLoginPolicy{
				{
					Email:            "jwr@lunar.app",
					DatabaseUsername: "jwr_bianalyst",
					Databases:        []*iamCore.Database{
						{
							ClusterIdentifier: "dev",
							Name:              "jwr",
						},
					},
				},
				{
					Email:            "nra@lunar.app",
					DatabaseUsername: "nra_bianalyst",
					Databases:        []*iamCore.Database{
						{
							ClusterIdentifier: "dev",
							Name:              "nra",
						},
					},
				},
			},
		},
	}})

	assert.NoError(err)

	roles, err := iamClient.ListRoles()
	failOnError(err)
	assert.Equal(1, len(roles))

	policies, err := iamClient.ListPolicies()
	failOnError(err)
	assert.Equal(2, len(policies))

	assert.Equal(1, eventRecorder.count(RoleCreated))
	assert.Equal(2, eventRecorder.count(PolicyCreated))
}


func TestApplier_SingleRoleAddAnotherDatabase(t *testing.T) {

	setUp()

	assert := assert.New(t)

	eventRecorder := EventRecorder{}
	session := LocalStackSessionFactory{}.CreateSession()
	iamClient := New(session)
	applier := NewApplier(iamClient, accountId, region, &eventRecorder)

	err := applier.Apply(iamCore.Model{Roles:[]*iamCore.AwsRole{
		{
			Name:                  "BiAnalyst",
			DatabaseLoginPolicies: []*iamCore.DatabaseLoginPolicy{
				{
					Email:            "jwr@lunar.app",
					DatabaseUsername: "jwr_bianalyst",
					Databases:        []*iamCore.Database{
						{
							ClusterIdentifier: "dev",
							Name:              "jwr",
						},
					},
				},
			},
		},
	}})

	assert.NoError(err)

	err = applier.Apply(iamCore.Model{Roles:[]*iamCore.AwsRole{
		{
			Name:                  "BiAnalyst",
			DatabaseLoginPolicies: []*iamCore.DatabaseLoginPolicy{
				{
					Email:            "jwr@lunar.app",
					DatabaseUsername: "jwr_bianalyst",
					Databases:        []*iamCore.Database{
						{
							ClusterIdentifier: "dev",
							Name:              "jwr",
						},
						{
							ClusterIdentifier: "hubble-unstable",
							Name:              "prod",
						},
					},
				},
			},
		},
	}})

	assert.NoError(err)

	roles, err := iamClient.ListRoles()
	failOnError(err)
	assert.Equal(1, len(roles))

	policies, err := iamClient.ListPolicies()
	failOnError(err)
	assert.Equal(1, len(policies))

	assert.Equal(1, eventRecorder.count(RoleCreated))
	assert.Equal(1, eventRecorder.count(PolicyCreated))
	assert.Equal(1, eventRecorder.count(PolicyUpdated))
}