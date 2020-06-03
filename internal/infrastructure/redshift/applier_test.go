package redshift

import (
	"github.com/lunarway/hubble-rbac-controller/internal/core/redshift"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"os"
	"sort"
	"testing"
)

//YOU MUST RUN docker-compose up PRIOR TO RUNNING THIS TEST

type EventRecorder struct {
	events []ApplyEventType
}

func (e *EventRecorder) Handle(eventType ApplyEventType, name string) {
	log.Infof("Event %s:%s occurred", eventType.ToString(), name)
	e.events = append(e.events, eventType)
}

func (e *EventRecorder) reset() {
	e.events = []ApplyEventType{}
}

func (e *EventRecorder) countAll() int {
	return len(e.events)
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

	eventRecorder := EventRecorder{}
	unmanagedUsers := []string{"lunarway"}
	unmanagedSchemas := []string{"public"}

	clientGroup := NewClientGroup(map[string]*ClusterCredentials{"dev": &localhostCredentials})
	applier := NewApplier(clientGroup, unmanagedUsers, unmanagedSchemas, &eventRecorder, "478824949770")

	//Create empty model
	model := redshift.Model{}

	err := applier.Apply(model)
	assert.NoError(err)
	assert.Equal(0, eventRecorder.CountAll())

	//Create a database with a BI user
	cluster := model.DeclareCluster("dev")
	cluster.DeclareUser("jwr_bianalyst")
	database := cluster.DeclareDatabase("jwr")
	biGroup := database.DeclareGroup("bianalyst")
	database.DeclareUser("jwr_bianalyst", biGroup)

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

	assert.Equal(1, eventRecorder.Count(EnsureGroupExists))
	assert.Equal(0, eventRecorder.Count(EnsureSchemaExists))
	assert.Equal(1, eventRecorder.Count(EnsureUserExists))
	assert.Equal(1, eventRecorder.Count(EnsureUserIsInGroup))

	//Grant access to "bi"
	biGroup.GrantSchema(&redshift.Schema{ Name: "bi" })

	err = applier.Apply(model)
	assert.NoError(err)

	actual = FetchState(redshiftClient)
	AssertState(assert, actual, RedshiftState{
		Users:            []string{"lunarway","jwr_bianalyst"},
		Groups:           []string{"bianalyst"},
		GroupMemberships: map[string][]string{"lunarway": {},"jwr_bianalyst": {"bianalyst"}},
		Grants:           map[string][]string{"bianalyst": {"public", "bi"}},
	}, "")

	assert.Equal(1, eventRecorder.Count(EnsureSchemaExists))
	assert.Equal(1, eventRecorder.Count(EnsureAccessIsGrantedToSchema))

	//Grant access to "test"
	biGroup.GrantSchema(&redshift.Schema{ Name: "test" })

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
	cluster.DeclareUser("nra_bianalyst")
	database.DeclareUser("nra_bianalyst", biGroup)

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
	amlGroup := database.DeclareGroup("aml")
	amlGroup.GrantExternalSchema(&redshift.ExternalSchema{ Name: "lwgoevents", GlueDatabaseName: "lwgoevents" })
	cluster.DeclareUser("jwr_aml")
	database.DeclareUser("jwr_aml", amlGroup)

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

func TestApplier_FailsOnUnmanagedUser(t *testing.T) {

	assert := assert.New(t)

	eventRecorder := EventRecorder{}
	unmanagedUsers := []string{"lunarway"}
	unmanagedSchemas := []string{"public"}

	clientGroup := NewClientGroup(map[string]*ClusterCredentials{"dev": &localhostCredentials})
	applier := NewApplier(clientGroup, unmanagedUsers, unmanagedSchemas, &eventRecorder, "478824949770")

	model := redshift.Model{}
	cluster := model.DeclareCluster("dev")
	database := cluster.DeclareDatabase( "jwr")
	biGroup := database.DeclareGroup("bianalyst")
	cluster.DeclareUser("lunarway")
	database.DeclareUser("lunarway", biGroup)

	err := applier.Apply(model)
	assert.Error(err)
}

func TestApplier_FailsOnUnmanagedSchema(t *testing.T) {

	assert := assert.New(t)

	eventRecorder := EventRecorder{}
	unmanagedUsers := []string{"lunarway"}
	unmanagedSchemas := []string{"public"}

	clientGroup := NewClientGroup(map[string]*ClusterCredentials{"dev": &localhostCredentials})
	applier := NewApplier(clientGroup, unmanagedUsers, unmanagedSchemas, &eventRecorder, "478824949770")

	model := redshift.Model{}
	cluster := model.DeclareCluster("dev")
	database := cluster.DeclareDatabase("jwr")
	biGroup := database.DeclareGroup("bianalyst")
	cluster.DeclareUser("jwr_bianalyst")
	database.DeclareUser("jwr_bianalyst", biGroup)
	biGroup.GrantSchema(&redshift.Schema{ Name: "public" })

	err := applier.Apply(model)
	assert.Error(err)
}
