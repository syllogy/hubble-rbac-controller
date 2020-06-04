//+build integration

package iam

import (
	"github.com/lunarway/hubble-rbac-controller/internal/core/utils"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"testing"
	"time"
)

//YOU MUST RUN docker-compose up PRIOR TO RUNNING THIS TEST

func init() {
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
	rand.Seed(time.Now().UnixNano())
	//log.SetFormatter(&log.JSONFormatter{PrettyPrint:true})
}




func Test_CreatePolicy_Is_Idempotent(t *testing.T) {

	assert := assert.New(t)

	session := LocalStackSessionFactory{}.CreateSession()
	iamClient := New(session)

	document := `
	{
	 "Version": "2012-10-17",
	 "Statement": [

	     {
	         "Effect": "Allow",
	         "Action": "redshift:GetClusterCredentials",
	         "Resource": [
	             "arn:aws:redshift:eu-west-1:478824949770:dbuser:dev/jwr_bianalyst",
	             "arn:aws:redshift:eu-west-1:478824949770:dbname:dev/jwr"
	         ],
	         "Condition": {
	             "StringLike": {
	                 "aws:userid": "*:jwr@lunar.app"
	             }
	         }
	     }

	 ]
	}
`

	policyName := utils.GenerateRandomString(10)
	_, err := iamClient.createOrUpdatePolicy(policyName, document)
	assert.NoError(err)

	_, err = iamClient.createOrUpdatePolicy(policyName, document)
	assert.NoError(err)
}

func Test_DeletePolicy_Is_Idempotent(t *testing.T) {

	assert := assert.New(t)

	session := LocalStackSessionFactory{}.CreateSession()
	iamClient := New(session)

	document := `
	{
	 "Version": "2012-10-17",
	 "Statement": [

	     {
	         "Effect": "Allow",
	         "Action": "redshift:GetClusterCredentials",
	         "Resource": [
	             "arn:aws:redshift:eu-west-1:478824949770:dbuser:dev/jwr_bianalyst",
	             "arn:aws:redshift:eu-west-1:478824949770:dbname:dev/jwr"
	         ],
	         "Condition": {
	             "StringLike": {
	                 "aws:userid": "*:jwr@lunar.app"
	             }
	         }
	     }

	 ]
	}
`

	policyName := utils.GenerateRandomString(10)
	policy, err := iamClient.createOrUpdatePolicy(policyName, document)
	assert.NoError(err)

	err = iamClient.DeletePolicy(policy)
	assert.NoError(err)

	err = iamClient.DeletePolicy(policy)
	assert.NoError(err)
}

func Test_AttachPolicy_Is_Idempotent(t *testing.T) {

	assert := assert.New(t)

	session := LocalStackSessionFactory{}.CreateSession()
	iamClient := New(session)

	role, err := iamClient.CreateOrUpdateLoginRole(utils.GenerateRandomString(10))
	assert.NoError(err)

	document := `
	{
	 "Version": "2012-10-17",
	 "Statement": [

	     {
	         "Effect": "Allow",
	         "Action": "redshift:GetClusterCredentials",
	         "Resource": [
	             "arn:aws:redshift:eu-west-1:478824949770:dbuser:dev/jwr_bianalyst",
	             "arn:aws:redshift:eu-west-1:478824949770:dbname:dev/jwr"
	         ],
	         "Condition": {
	             "StringLike": {
	                 "aws:userid": "*:jwr@lunar.app"
	             }
	         }
	     }

	 ]
	}
`

	policyName := utils.GenerateRandomString(10)
	policy, err := iamClient.createOrUpdatePolicy(policyName, document)
	assert.NoError(err)

	err = iamClient.attachPolicy(role, policy)
	assert.NoError(err)

	err = iamClient.attachPolicy(role, policy)
	assert.NoError(err)
}

func Test_DetachPolicy_Is_Idempotent(t *testing.T) {

	assert := assert.New(t)

	session := LocalStackSessionFactory{}.CreateSession()
	iamClient := New(session)

	role, err := iamClient.CreateOrUpdateLoginRole(utils.GenerateRandomString(10))
	assert.NoError(err)

	document := `
	{
	 "Version": "2012-10-17",
	 "Statement": [

	     {
	         "Effect": "Allow",
	         "Action": "redshift:GetClusterCredentials",
	         "Resource": [
	             "arn:aws:redshift:eu-west-1:478824949770:dbuser:dev/jwr_bianalyst",
	             "arn:aws:redshift:eu-west-1:478824949770:dbname:dev/jwr"
	         ],
	         "Condition": {
	             "StringLike": {
	                 "aws:userid": "*:jwr@lunar.app"
	             }
	         }
	     }

	 ]
	}
`

	policyName := utils.GenerateRandomString(10)
	policy, err := iamClient.createOrUpdatePolicy(policyName, document)
	assert.NoError(err)

	err = iamClient.attachPolicy(role, policy)
	assert.NoError(err)

	attachedPolicies, err := iamClient.ListManagedAttachedPolicies(role)

	err = iamClient.DetachPolicy(role, attachedPolicies[0])
	assert.NoError(err)

	err = iamClient.DetachPolicy(role, attachedPolicies[0])
	assert.NoError(err)
}

func Test_CreateLoginRole_Is_Idempotent(t *testing.T) {

	assert := assert.New(t)

	session := LocalStackSessionFactory{}.CreateSession()
	iamClient := New(session)

	roleName := utils.GenerateRandomString(10)
	_, err := iamClient.CreateOrUpdateLoginRole(roleName)
	assert.NoError(err)

	_, err = iamClient.CreateOrUpdateLoginRole(roleName)
	assert.NoError(err)
}

func Test_DeleteLoginRole_Is_Idempotent(t *testing.T) {

	assert := assert.New(t)

	session := LocalStackSessionFactory{}.CreateSession()
	iamClient := New(session)

	roleName := utils.GenerateRandomString(10)
	role, err := iamClient.CreateOrUpdateLoginRole(roleName)
	assert.NoError(err)

	err = iamClient.DeleteLoginRole(role)
	assert.NoError(err)

	err = iamClient.DeleteLoginRole(role)
	assert.NoError(err)
}