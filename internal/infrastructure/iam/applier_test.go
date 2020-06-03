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

func (e *EventRecorder) Reset() {
	e.events = []ApplyEventType{}
}

type TestContext struct {
	applier *Applier
	client *Client
	eventRecorder *EventRecorder
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

func setUp() TestContext {

	session := LocalStackSessionFactory{}.CreateSession()
	iamClient := New(session)
	eventRecorder := EventRecorder{}
	applier := NewApplier(iamClient, accountId, region, &eventRecorder)

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

	eventRecorder.Reset()

	return TestContext{
		applier: applier,
		client:  iamClient,
		eventRecorder: &eventRecorder,
	}
}

func TestApplier_NoRoles(t *testing.T) {

	context := setUp()

	assert := assert.New(t)

	err := context.applier.Apply(iamCore.Model{Roles:[]*iamCore.AwsRole{}})
	assert.NoError(err)

	actual := FetchIAMState(context.client)
	expected := IAMState{}
	expected.Roles = map[string][]string{}
	AssertState(assert, actual, expected, "There are no IAM roles")

	assert.Equal(0, len(context.eventRecorder.events))
}

func TestApplier_SingleRole(t *testing.T) {

	context := setUp()

	assert := assert.New(t)

	err := context.applier.Apply(iamCore.Model{Roles:[]*iamCore.AwsRole{
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

	actual := FetchIAMState(context.client)
	expected := IAMState{}
	expected.Roles = map[string][]string{"BiAnalyst": {"jwr_bianalyst"}}
	AssertState(assert, actual, expected, "IAM role have been created")

	assert.Equal(1, context.eventRecorder.Count(RoleCreated))
	assert.Equal(1, context.eventRecorder.Count(PolicyCreated))
}

func TestApplier_SingleRoleTwoDatabases(t *testing.T) {

	context := setUp()

	assert := assert.New(t)

	err := context.applier.Apply(iamCore.Model{Roles:[]*iamCore.AwsRole{
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

	actual := FetchIAMState(context.client)
	expected := IAMState{}
	expected.Roles = map[string][]string{"BiAnalyst": {"jwr_bianalyst"}}
	AssertState(assert, actual, expected, "IAM role have been created")

	assert.Equal(1, context.eventRecorder.Count(RoleCreated))
	assert.Equal(1, context.eventRecorder.Count(PolicyCreated))
}

func TestApplier_SingleRoleTwoUsers(t *testing.T) {

	context := setUp()

	assert := assert.New(t)

	err := context.applier.Apply(iamCore.Model{Roles:[]*iamCore.AwsRole{
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

	actual := FetchIAMState(context.client)
	expected := IAMState{}
	expected.Roles = map[string][]string{"BiAnalyst": {"jwr_bianalyst", "nra_bianalyst"}}
	AssertState(assert, actual, expected, "IAM role have been created")

	assert.Equal(1, context.eventRecorder.Count(RoleCreated))
	assert.Equal(2, context.eventRecorder.Count(PolicyCreated))
}


func TestApplier_SingleRoleAddAnotherDatabase(t *testing.T) {

	context := setUp()

	assert := assert.New(t)

	err := context.applier.Apply(iamCore.Model{Roles:[]*iamCore.AwsRole{
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

	err = context.applier.Apply(iamCore.Model{Roles:[]*iamCore.AwsRole{
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

	actual := FetchIAMState(context.client)
	expected := IAMState{}
	expected.Roles = map[string][]string{"BiAnalyst": {"jwr_bianalyst"}}
	AssertState(assert, actual, expected, "IAM role have been created")

	assert.Equal(1, context.eventRecorder.Count(RoleCreated))
	assert.Equal(1, context.eventRecorder.Count(PolicyCreated))
	assert.Equal(1, context.eventRecorder.Count(PolicyUpdated))
}

func TestApplier_RemoveUserWillRemoveAccess(t *testing.T) {

	context := setUp()

	assert := assert.New(t)

	err := context.applier.Apply(iamCore.Model{Roles:[]*iamCore.AwsRole{
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

	failOnError(err)

	actual := FetchIAMState(context.client)
	expected := IAMState{}
	expected.Roles = map[string][]string{"BiAnalyst": {"jwr_bianalyst"}}
	AssertState(assert, actual, expected, "IAM role has not attached policies")

	err = context.applier.Apply(iamCore.Model{Roles:[]*iamCore.AwsRole{
		{
			Name:                  "BiAnalyst",
			DatabaseLoginPolicies: []*iamCore.DatabaseLoginPolicy{},
			},
	}})
	failOnError(err)

	actual = FetchIAMState(context.client)
	expected.Roles = map[string][]string{"BiAnalyst": {}}
	AssertState(assert, actual, expected, "IAM role has not attached policies")
}

func TestApplier_RoleWithNoUsers(t *testing.T) {

	context := setUp()

	assert := assert.New(t)

	err := context.applier.Apply(iamCore.Model{Roles:[]*iamCore.AwsRole{
		{
			Name:                  "BiAnalyst",
			DatabaseLoginPolicies: []*iamCore.DatabaseLoginPolicy{},
		},
	}})
	assert.NoError(err)

	actual := FetchIAMState(context.client)
	expected := IAMState{}
	expected.Roles = map[string][]string{"BiAnalyst": {}}
	AssertState(assert, actual, expected, "IAM role has not attached policies")
}


func TestApplier_UserWithReferencedUnmanagedPolicies(t *testing.T) {
	context := setUp()

	assert := assert.New(t)

	policyDocument := `
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::lunarway-prod-data-tmp/*"
            ]
        }
    ]
}
`
	_, err := context.client.createOrUpdatePolicy("access-to-tmp-bucket", policyDocument)
	failOnError(err)

	err = context.applier.Apply(iamCore.Model{Roles:[]*iamCore.AwsRole{
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
			Policies: []*iamCore.PolicyReference{{Arn:"arn:aws:iam::000000000000:policy/hubble-rbac/access-to-tmp-bucket"}},
		},
	}})

	assert.NoError(err)

	actual := FetchIAMState(context.client)
	expected := IAMState{}
	expected.Roles = map[string][]string{"BiAnalyst": {"jwr_bianalyst", "access-to-tmp-bucket"}}
	AssertState(assert, actual, expected, "IAM role have been created")

	err = context.applier.Apply(iamCore.Model{Roles:[]*iamCore.AwsRole{
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
			Policies: []*iamCore.PolicyReference{},
		},
	}})
	assert.NoError(err)

	actual = FetchIAMState(context.client)
	expected.Roles = map[string][]string{"BiAnalyst": {"jwr_bianalyst"}}
	AssertState(assert, actual, expected, "IAM role have been created")

}

