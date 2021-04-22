package redshift

import (
	"github.com/lunarway/hubble-rbac-controller/internal/core/redshift"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_Resolve(t *testing.T) {

	assert := assert.New(t)

	groupName := "dbtdeveloper"
	username := "jwr"
	externalSchemaName := "lwgoevents"

	clusterIdentifiers := []string{"cluster1"}
	users := []string{username}
	groups := []string{groupName}
	databases := []string{"dev"}
	grants := []string{"public", externalSchemaName}
	externalSchemas := []redshift.ExternalSchema{{Name: externalSchemaName, GlueDatabaseName: "lw-go-events"}}

	client := NewStubRedshiftClient(users, groups, databases, grants, externalSchemas)
	clientPool := newStubRedshiftClientFactory(client)
	modelResolver := NewModelResolver(clientPool, &redshift.Exclusions{})

	model, err := modelResolver.Resolve(clusterIdentifiers)

	assert.NoError(err)

	assert.Equal(1, len(model.Clusters))
	cluster := model.Clusters[0]

	assert.Equal(1, len(cluster.Databases))
	database := cluster.Databases[0]

	assert.Equal(1, len(database.Groups))
	group := database.Groups[0]

	assert.Equal(1, len(group.GrantedExternalSchemas))
	grant := group.GrantedExternalSchemas[0]

	assert.Equal(1, len(database.Users))
	user := database.Users[0]

	assert.Equal(groupName, group.Name)
	assert.Equal(username, user.Name)
	assert.Equal(externalSchemaName, grant.Name)
}
