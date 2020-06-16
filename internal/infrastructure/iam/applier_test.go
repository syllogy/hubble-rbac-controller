//+build integration

package iam

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/iam"
	iamCore "github.com/lunarway/hubble-rbac-controller/internal/core/iam"
	"github.com/lunarway/hubble-rbac-controller/internal/infrastructure"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

//YOU MUST RUN docker-compose up PRIOR TO RUNNING THIS TEST


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

func (client *Client) createUnmanagedPolicy(name string, document string) (*iam.Policy, error) {

	c := iam.New(client.session)

	response, err := c.ListPolicies(&iam.ListPoliciesInput{
		MaxItems:aws.Int64(5000),
	})

	if err != nil {
		return nil, err
	}

	err = client.assertNotTruncated(response.IsTruncated)

	if err != nil {
		return nil, err
	}

	policies := response.Policies

	policy := client.lookupPolicy(policies, name)
	if policy != nil {
		return policy, nil
	} else {
		response, err := c.CreatePolicy(&iam.CreatePolicyInput{
			Description:    aws.String(""),
			PolicyDocument: &document,
			PolicyName:     &name,
		})

		if err != nil {
			return nil, fmt.Errorf("unable to create policy %s: %w", name, err)
		}

		return response.Policy, nil
	}
}

func setUp(t *testing.T) TestContext {

	session := LocalStackSessionFactory{}.CreateSession()
	iamClient := New(session)
	eventRecorder := EventRecorder{}
	logger := infrastructure.NewLogger(t)
	applier := NewApplier(iamClient, accountId, region, &eventRecorder, logger)

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

	context := setUp(t)

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

	context := setUp(t)

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

	context := setUp(t)

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

	context := setUp(t)

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

	context := setUp(t)

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

	context := setUp(t)

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

	context := setUp(t)

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

	context := setUp(t)

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
	_, err := context.client.createUnmanagedPolicy("access-to-tmp-bucket", policyDocument)
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
			Policies: []*iamCore.PolicyReference{{Arn:"arn:aws:iam::000000000000:policy/access-to-tmp-bucket"}},
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
	AssertState(assert, actual, expected, "policy have been detached")
}

func TestApplier_UserWithReferencedUnmanagedPoliciesCanBeDeleted(t *testing.T) {

	context := setUp(t)

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
	_, err := context.client.createUnmanagedPolicy("access-to-tmp-bucket", policyDocument)
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
			Policies: []*iamCore.PolicyReference{{Arn:"arn:aws:iam::000000000000:policy/access-to-tmp-bucket"}},
		},
	}})

	assert.NoError(err)

	actual := FetchIAMState(context.client)
	expected := IAMState{}
	expected.Roles = map[string][]string{"BiAnalyst": {"jwr_bianalyst", "access-to-tmp-bucket"}}
	AssertState(assert, actual, expected, "IAM role have been created")

	err = context.applier.Apply(iamCore.Model{Roles:[]*iamCore.AwsRole{}})
	assert.NoError(err)

	actual = FetchIAMState(context.client)
	expected.Roles = map[string][]string{}
	AssertState(assert, actual, expected, "IAM role has been deleted")
}
