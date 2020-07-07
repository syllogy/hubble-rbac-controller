//+build integration

package service

import (
	"github.com/lunarway/hubble-rbac-controller/internal/core/hubble"
	redshiftCore "github.com/lunarway/hubble-rbac-controller/internal/core/redshift"
	"github.com/lunarway/hubble-rbac-controller/internal/infrastructure/google"
	"github.com/lunarway/hubble-rbac-controller/internal/infrastructure/iam"
	"github.com/lunarway/hubble-rbac-controller/internal/infrastructure/redshift"
	"github.com/operator-framework/operator-sdk/pkg/log/zap"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

const accountId = "478824949770"
const region = "eu-west-1"

var localhostCredentials redshift.ClusterCredentials

func init() {
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)

	localhostCredentials = redshift.ClusterCredentials{
		Username:                 "lunarway",
		Password:                 "lunarway",
		MasterDatabase:           "lunarway",
		Host:                     "localhost",
		Sslmode:                  "disable",
		Port:                     5432,
		ExternalSchemasSupported: false,
	}
}

func failOnError(err error) {
	if err != nil {
		panic(err)
	}
}

func setUp() {

	session := iam.LocalStackSessionFactory{}.CreateSession()
	iamClient := iam.New(session)

	roles, err := iamClient.ListRoles()
	failOnError(err)

	for _, role := range roles {

		attachedPolicies, err := iamClient.ListManagedAttachedPolicies(role)
		failOnError(err)

		for _, attachedPolicy := range attachedPolicies {
			err = iamClient.DetachPolicy(role, attachedPolicy)
			failOnError(err)

			err = iamClient.DeleteAttachedPolicy(attachedPolicy)
			failOnError(err)
		}

		err = iamClient.DeleteLoginRole(role)
		failOnError(err)
	}

	policies, err := iamClient.ListPolicies()
	failOnError(err)

	for _, policy := range policies {
		err = iamClient.DeletePolicy(policy)
		failOnError(err)
	}
}


func TestApplier_Apply(t *testing.T) {

	setUp()

	assert := assert.New(t)

	//logger := infrastructure.NewLogger(t)
	logger := zap.Logger()

	excludedUsers := []string{"lunarway"}
	excludedDatabases := []string{"template0", "template1", "postgres", "padb_harvest"}
	clientGroup := redshift.NewClientGroup(map[string]*redshift.ClusterCredentials{"hubble": &localhostCredentials})
	redshiftApplier := redshift.NewDagBasedApplier(clientGroup, redshiftCore.NewExclusions(excludedDatabases, excludedUsers), accountId, logger)

	googleApplier := google.NewFakeApplier()

	session := iam.LocalStackSessionFactory{}.CreateSession()
	iamClient := iam.New(session)
	iamApplier := iam.NewApplier(iamClient, accountId, region, &IamEventRecorder{logger:logger}, logger)

	redshiftExpected := redshift.NewRedshiftState()
	redshiftExpected.Users = []string{"lunarway"}
	redshiftExpected.GroupMemberships = map[string][]string{"lunarway": {}}

	iamExpected := iam.IAMState{}

	applier := NewApplier(iamApplier, googleApplier, redshiftApplier, logger)

	xxx := redshiftCore.Model{}
	xxx.DeclareCluster("hubble")
	err := redshiftApplier.Apply(xxx)
	failOnError(err)

	model := hubble.Model{}
	user := model.AddUser("jwr", "jwr@lunar.app")
	err = applier.Apply(model, false)
	failOnError(err)

	log.Info("Create database")
	database := model.AddDatabase("hubble", "prod")
	err = applier.Apply(model,false)
	failOnError(err)

	redshiftClient, err := redshift.NewClient(
		localhostCredentials.Username,
		localhostCredentials.Password,
		localhostCredentials.Host,
		"prod",
		localhostCredentials.Sslmode,
		localhostCredentials.Port,
		localhostCredentials.ExternalSchemasSupported,
	)
	failOnError(err)


	redshiftActual := redshift.FetchState(redshiftClient)
	redshift.AssertState(assert, redshiftActual, redshiftExpected, "database should be unaffected")
	iamActual := iam.FetchIAMState(iamClient)
	iam.AssertState(assert, iamActual, iamExpected, "IAM should be unaffected")

	log.Info("Create role")
	role := model.AddRole("BiAnalyst", []hubble.DataSet{"public_bi"})
	err = applier.Apply(model,false)
	failOnError(err)

	redshiftActual = redshift.FetchState(redshiftClient)
	redshift.AssertState(assert, redshiftActual, redshiftExpected, "database should be unaffected")
	iamExpected.Roles = map[string][]string{"BiAnalyst": {}}
	iamActual = iam.FetchIAMState(iamClient)
	iam.AssertState(assert, iamActual, iamExpected, "IAM role have been created")

	log.Info("Grant role access to database")
	role.GrantAccess(database)
	err = applier.Apply(model,false)
	failOnError(err)

	redshiftActual = redshift.FetchState(redshiftClient)
	redshift.AssertState(assert, redshiftActual, redshiftExpected, "database should be unaffected")
	iamActual = iam.FetchIAMState(iamClient)
	iam.AssertState(assert, iamActual, iamExpected, "IAM role have been created")

	log.Info("Assign user to role")
	user.Assign(role)
	err = applier.Apply(model,false)
	failOnError(err)

	redshiftExpected.Users = []string{"lunarway", "jwr_bianalyst"}
	redshiftExpected.Groups = []string{"bianalyst"}
	redshiftExpected.GroupMemberships = map[string][]string{"lunarway": {}, "jwr_bianalyst": {"bianalyst"}}
	redshiftExpected.Grants = map[string][]string{"bianalyst": {"public","public_bi"}}
	redshiftActual = redshift.FetchState(redshiftClient)
	redshift.AssertState(assert, redshiftActual, redshiftExpected, "user,groups and grants have been created")

	iamExpected.Roles = map[string][]string{"BiAnalyst": {"jwr_BiAnalyst"}}
	iamActual = iam.FetchIAMState(iamClient)
	iam.AssertState(assert, iamActual, iamExpected, "IAM policy for jwr has been attached to role")

	//log.Info("Revoke access")
	//role.RevokeAccess(database)
	//err = applier.Apply(model,false)
	//failOnError(err)
	//
	//redshiftExpected = redshift.NewRedshiftState()
	//redshiftExpected.Users = []string{"lunarway"}
	//redshiftExpected.GroupMemberships = map[string][]string{"lunarway": {}}
	//redshiftExpected.Groups = []string{}
	//redshiftExpected.Grants = map[string][]string{}
	//redshiftActual = redshift.FetchState(redshiftClient)
	//redshift.AssertState(assert, redshiftActual, redshiftExpected, "the user, group and grants have been removed")
	//
	//iamExpected.Roles = map[string][]string{"BiAnalyst": {}}
	//iamActual = iam.FetchIAMState(iamClient)
	//iam.AssertState(assert, iamActual, iamExpected, "IAM policy for jwr has been detached from role")
	//
	//log.Info("Unassign user from role")
	//user.Unassign(role)
	//err = applier.Apply(model,false)
	//failOnError(err)
	//
	//redshiftActual = redshift.FetchState(redshiftClient)
	//redshift.AssertState(assert, redshiftActual, redshiftExpected, "the user, group and grants have been removed")
	//iamActual = iam.FetchIAMState(iamClient)
	//iam.AssertState(assert, iamActual, iamExpected, "IAM policy for jwr is still detached from role")
}
