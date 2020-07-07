package redshift

import (
	"fmt"
	"github.com/lunarway/hubble-rbac-controller/internal/core/redshift"
	"github.com/lunarway/hubble-rbac-controller/internal/infrastructure"
	log "github.com/sirupsen/logrus"
	"os"
	"testing"
)

//YOU MUST RUN docker-compose up PRIOR TO RUNNING THIS TEST

var mylocalhostCredentials ClusterCredentials

func init() {
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)

	mylocalhostCredentials = ClusterCredentials{
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

func TestXxx_Fetch(t *testing.T) {

	logger := infrastructure.NewLogger(t)
	eventRecorder := EventRecorder{}
	excludedUsers := []string{"lunarway"}
	excludedSchemas := []string{}
	excludedDatabases := []string{"template0", "postgres", "template1", "lunarway"}
	clientGroup := NewClientGroup(map[string]*ClusterCredentials{"dev": &mylocalhostCredentials})
	applier := NewApplier(clientGroup, excludedDatabases, excludedUsers, excludedSchemas, &eventRecorder, "478824949770", logger)
	resolver := NewModelResolver(clientGroup, excludedUsers, excludedDatabases)

	model := redshift.Model{}
	cluster := model.DeclareCluster("dev")
	biGroup := cluster.DeclareGroup("bianalyst")
	cluster.DeclareUser("jwr_bianalyst", biGroup)
	database := cluster.DeclareDatabase("jwr")
	dbGroup := database.DeclareGroup("bianalyst")
	dbGroup.GrantSchema(&redshift.Schema{Name:"public"})
	database.DeclareUser("jwr_bianalyst")

	err := applier.Apply(model)
	failOnError(err)

	fetchedModel := redshift.Model{}
	fetchedModel.DeclareCluster("dev")
	resolver.Resolve(&fetchedModel)

	dagBuilder := redshift.NewDagBuilder()
	dag := dagBuilder.UpdateModel(model, fetchedModel)
	fmt.Println(dag.String())
}
