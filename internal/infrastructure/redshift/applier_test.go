//+build integration

package redshift

import (
	"github.com/lunarway/hubble-rbac-controller/internal/core/redshift"
	"github.com/lunarway/hubble-rbac-controller/internal/infrastructure"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

//YOU MUST RUN docker-compose up PRIOR TO RUNNING THIS TEST

var localhostCredentials ClusterCredentials

func init() {
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)

	localhostCredentials = ClusterCredentials{
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

func TestApplier_ManageResources(t *testing.T) {

	assert := assert.New(t)

	logger := infrastructure.NewLogger(t)
	excludedUsers := []string{"lunarway"}
	excludedDatabases := []string{"template0", "postgres"}

	clientGroup := NewClientGroup(map[string]*ClusterCredentials{"dev": &localhostCredentials})
	applier := NewApplier(clientGroup, redshift.NewExclusions(excludedDatabases, excludedUsers), "478824949770", logger)

	//Create empty model
	model := redshift.Model{}
	cluster := model.DeclareCluster("dev")

	err := applier.Apply(model)
	assert.NoError(err)

	//Create a database with a BI user
	biGroup := cluster.DeclareGroup("bianalyst")
	cluster.DeclareUser("jwr_bianalyst", biGroup)
	database := cluster.DeclareDatabase("jwr")
	biDatabaseGroup := database.DeclareGroup("bianalyst")
	database.DeclareUser("jwr_bianalyst")

	err = applier.Apply(model)
	assert.NoError(err)

	redshiftClient, err := clientGroup.ForDatabase(database)
	failOnError(err)

	actual := FetchState(redshiftClient)
	AssertState(assert, actual, RedshiftState{
		Users:            []string{"lunarway","jwr_bianalyst"},
		Groups:           []string{"bianalyst"},
		GroupMemberships: map[string][]string{"lunarway": {}, "jwr_bianalyst": {"bianalyst"}},
		Grants:           map[string][]string{"bianalyst": {"public"}},
	}, "")

	//Grant access to "bi"
	biDatabaseGroup.GrantSchema(&redshift.Schema{ Name: "bi" })

	err = applier.Apply(model)
	assert.NoError(err)

	actual = FetchState(redshiftClient)
	AssertState(assert, actual, RedshiftState{
		Users:            []string{"lunarway","jwr_bianalyst"},
		Groups:           []string{"bianalyst"},
		GroupMemberships: map[string][]string{"lunarway": {},"jwr_bianalyst": {"bianalyst"}},
		Grants:           map[string][]string{"bianalyst": {"public", "bi"}},
	}, "")

	//Grant access to "test"
	biDatabaseGroup.GrantSchema(&redshift.Schema{ Name: "test" })

	err = applier.Apply(model)
	assert.NoError(err)

	actual = FetchState(redshiftClient)
	AssertState(assert, actual, RedshiftState{
		Users:            []string{"lunarway","jwr_bianalyst"},
		Groups:           []string{"bianalyst"},
		GroupMemberships: map[string][]string{"lunarway": {},"jwr_bianalyst": {"bianalyst"}},
		Grants:           map[string][]string{"bianalyst": {"public", "bi", "test"}},
	}, "")

	//Add another BI user
	cluster.DeclareUser("nra_bianalyst", biGroup)
	database.DeclareUser("nra_bianalyst")

	err = applier.Apply(model)
	assert.NoError(err)

	actual = FetchState(redshiftClient)
	AssertState(assert, actual, RedshiftState{
		Users:            []string{"lunarway","jwr_bianalyst","nra_bianalyst"},
		Groups:           []string{"bianalyst"},
		GroupMemberships: map[string][]string{"lunarway": {},"jwr_bianalyst": {"bianalyst"}, "nra_bianalyst": {"bianalyst"}},
		Grants:           map[string][]string{"bianalyst": {"public", "bi", "test"}},
	}, "")

	//Add an AML user
	amlGroup := cluster.DeclareGroup("aml")
	amlDatabaseGroup := database.DeclareGroup("aml")
	amlDatabaseGroup.GrantExternalSchema(&redshift.ExternalSchema{ Name: "lwgoevents", GlueDatabaseName: "lwgoevents" })
	cluster.DeclareUser("jwr_aml", amlGroup)
	database.DeclareUser("jwr_aml")

	err = applier.Apply(model)
	assert.NoError(err)

	actual = FetchState(redshiftClient)
	AssertState(assert, actual, RedshiftState{
		Users:            []string{"lunarway","jwr_bianalyst","nra_bianalyst", "jwr_aml"},
		Groups:           []string{"bianalyst", "aml"},
		GroupMemberships: map[string][]string{"lunarway": {},"jwr_bianalyst": {"bianalyst"}, "nra_bianalyst": {"bianalyst"}, "jwr_aml": {"aml"}},
		Grants:           map[string][]string{"bianalyst": {"public", "bi", "test"}, "aml": {"public","lwgoevents"}},
	}, "")
}

func TestApplier_FailsOnExcludedUser(t *testing.T) {

	assert := assert.New(t)

	logger := infrastructure.NewLogger(t)
	excludedUsers := []string{"lunarway"}
	excludedDatabases := []string{"template0", "postgres"}

	clientGroup := NewClientGroup(map[string]*ClusterCredentials{"dev": &localhostCredentials})
	applier := NewApplier(clientGroup, redshift.NewExclusions(excludedDatabases, excludedUsers), "478824949770", logger)

	model := redshift.Model{}
	cluster := model.DeclareCluster("dev")
	biGroup := cluster.DeclareGroup("bianalyst")
	database := cluster.DeclareDatabase( "jwr")
	database.DeclareGroup("bianalyst")
	cluster.DeclareUser("lunarway", biGroup)
	database.DeclareUser("lunarway")

	err := applier.Apply(model)
	assert.Error(err)
}
