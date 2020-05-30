package resolver

import (
	"encoding/json"
	"fmt"
	"github.com/lunarway/hubble-rbac-controller/internal/core/hubble"
	"github.com/stretchr/testify/assert"
	"testing"
)

type TestData struct {
	unstable         hubble.Database
	dev              hubble.DevDatabase
	biAnalystRole    hubble.Role
	dbtDeveloperRole hubble.Role
	biAnalyst        hubble.User
	dbtDeveloper        hubble.User
	lwgoeventsDatabase hubble.GlueDatabase
}

func generateTestData() TestData {

	unstable := hubble.Database{
		ClusterIdentifier: "hubble-unstable",
		Name:              "prod",
	}

	dev := hubble.DevDatabase{
		ClusterIdentifier: "hubble",
	}

	lwgoeventsDatabase := hubble.GlueDatabase{
		ShortName: "lwgoevents",
		Name:      "lw-go-events",
	}

	biAnalystRole := hubble.Role{
		Name:"bi_analyst",
		GrantedDatabases:[]hubble.Database{unstable},
		GrantedDevDatabases:[]hubble.DevDatabase{},
		GrantedGlueDatabases:[]hubble.GlueDatabase{},
		Acl:[]hubble.DataSet{"bi", "core"},
	}

	dbtDeveloperRole := hubble.Role{
		Name:"dbt_developer",
		GrantedDatabases:[]hubble.Database{},
		GrantedDevDatabases:[]hubble.DevDatabase{dev},
		GrantedGlueDatabases:[]hubble.GlueDatabase{lwgoeventsDatabase},
		Acl:[]hubble.DataSet{"bi", "core"},
	}

	biAnalyst := hubble.User{
		Username:"jwr",
		Email:"jwr@lunar.app",
		AssignedTo:[]hubble.Role{biAnalystRole},
	}

	dbtDeveloper := hubble.User{
		Username:"nra",
		Email:"nra@lunar.app",
		AssignedTo:[]hubble.Role{dbtDeveloperRole},
	}

	return TestData{
		unstable:         unstable,
		dev:              dev,
		biAnalystRole:    biAnalystRole,
		dbtDeveloperRole: dbtDeveloperRole,
		biAnalyst:        biAnalyst,
		dbtDeveloper:dbtDeveloper,
		lwgoeventsDatabase:lwgoeventsDatabase,
	}
}

func Test_DbtDeveloper(t *testing.T) {

	assert := assert.New(t)

	data := generateTestData()

	model := hubble.Model{
		Databases: []hubble.Database{data.unstable},
		DevDatabases: []hubble.DevDatabase{data.dev},
		GlueDatabases: []hubble.GlueDatabase{data.lwgoeventsDatabase},
		Users:         []hubble.User{data.dbtDeveloper},
		Roles:         []hubble.Role{data.dbtDeveloperRole},
	}

	resolver := Resolver{}
	resolved, _ := resolver.Resolve(model)
	b, _ := json.MarshalIndent(resolved, "", "   ")
	fmt.Printf("%s\n", b)

	dbUsername := fmt.Sprintf("%s_%s", data.dbtDeveloper.Username, data.dbtDeveloperRole.Name)

	database := resolved.RedshiftModel.LookupDatabase(data.dev.ClusterIdentifier, data.dbtDeveloper.Username)
	assert.NotNil(database, "database is registered")

	group := database.LookupGroup(data.dbtDeveloperRole.Name)
	assert.NotNil(group, "a user group with the name of the role has been registered")
	assert.Equal(dbUsername, *database.Owner, "developer is the owner of the dev database")

	dbUser := database.LookupUser(dbUsername)
	assert.NotNil(dbUser, "a user name of the role and user has been registered")

	user := resolved.GoogleModel.LookupUser(data.dbtDeveloper.Email)
	assert.NotNil(user, "google login is registered")
	assert.Equal([]string{data.dbtDeveloperRole.Name}, user.AssignedTo(), "google login has been assigned the expected role")

	role := resolved.IamModel.LookupRole(data.dbtDeveloperRole.Name)
	assert.NotNil(role, "AWS role for the role has been created")

	policy := role.LookupDatabaseLoginPolicyForUser(data.dbtDeveloper.Email)
	assert.NotNil(policy, "policy has been registered for the user")
	assert.Equal(dbUsername, policy.DatabaseUsername, "policy uses the correct username")

	access := policy.LookupDatabase(data.dev.ClusterIdentifier, data.dbtDeveloper.Username)
	assert.NotNil(access, "access has been granted for the user to the dev/[username] database")
}

func Test_BiAnalyst(t *testing.T) {

	assert := assert.New(t)

	data := generateTestData()

	model := hubble.Model{
		Databases: []hubble.Database{data.unstable},
		DevDatabases: []hubble.DevDatabase{},
		GlueDatabases: []hubble.GlueDatabase{},
		Users:         []hubble.User{data.biAnalyst},
		Roles:         []hubble.Role{data.biAnalystRole},
	}

	resolver := Resolver{}
	resolved, _ := resolver.Resolve(model)
	b, _ := json.MarshalIndent(resolved, "", "   ")
	fmt.Printf("%s\n", b)

	dbUsername := fmt.Sprintf("%s_%s", data.biAnalyst.Username, data.biAnalystRole.Name)

	database := resolved.RedshiftModel.LookupDatabase(data.unstable.ClusterIdentifier, data.unstable.Name)
	assert.NotNil(database, "database is registered")

	group := database.LookupGroup(data.biAnalystRole.Name)
	assert.NotNil(group, "a user group with the name of the role has been registered")
	assert.Contains(group.Granted(),"bi", "group has been granted access to the expected schemas")

	dbUser := database.LookupUser(dbUsername)
	assert.NotNil(dbUser, "a user name of the role and user has been registered")

	user := resolved.GoogleModel.LookupUser(data.biAnalyst.Email)
	assert.NotNil(user, "google login is registered")
	assert.Equal([]string{data.biAnalystRole.Name}, user.AssignedTo(), "google login has been assigned the expected role")

	role := resolved.IamModel.LookupRole(data.biAnalystRole.Name)
	assert.NotNil(role, "AWS role for the role has been created")

	policy := role.LookupDatabaseLoginPolicyForUser(data.biAnalyst.Email)
	assert.NotNil(policy, "policy has been registered for the user")
	assert.Equal(dbUsername, policy.DatabaseUsername, "policy uses the correct username")

	access := policy.LookupDatabase(data.unstable.ClusterIdentifier, data.unstable.Name)
	assert.NotNil(access, "access has been granted for the user to the unstable/prod database")
}
