package controllers

import (
	"fmt"
	hubblev1alpha1 "github.com/lunarway/hubble-rbac-controller/api/v1alpha1"
	"github.com/lunarway/hubble-rbac-controller/internal/core/hubble"
	"strings"
)

func buildHubbleModel(users *hubblev1alpha1.HubbleRbac) (hubble.Model, error) {

	model := hubble.Model{}

	databaseMap := make(map[string]*hubble.Database)
	devDatabaseMap := make(map[string]*hubble.DevDatabase)
	policyMap := make(map[string]*hubble.PolicyReference)
	datalakeGrantsMap := make(map[string]*hubble.GlueDatabase)
	roleMap := make(map[string]*hubble.Role)

	for _, database := range users.Spec.Databases {
		databaseMap[database.Name] = model.AddDatabase(database.Cluster, database.Database)
	}

	for _, database := range users.Spec.DevDatabases {
		devDatabaseMap[database.Name] = model.AddDevDatabase(database.Cluster)
	}

	for _, policy := range users.Spec.Policies {
		policyMap[policy.Name] = model.AddPolicyReference(policy.Arn)
	}

	for _, role := range users.Spec.Roles {
		for _, name := range role.DatalakeGrants {
			datalakeGrantsMap[name] = &hubble.GlueDatabase{
				ShortName: strings.ReplaceAll(name, "-", ""),
				Name:      name,
			}
		}
	}

	for _, role := range users.Spec.Roles {
		var acl []hubble.DataSet
		for _, name := range role.DatawarehouseGrants {
			acl = append(acl, hubble.DataSet(name))
		}

		var datalakeGrants []*hubble.GlueDatabase
		for _, name := range role.DatalakeGrants {
			datalakeGrants = append(datalakeGrants, datalakeGrantsMap[name])
		}

		var databases []*hubble.Database
		for _, name := range role.Databases {
			database, ok := databaseMap[name]
			if !ok {
				return model, fmt.Errorf("no such database: %s", name)
			}

			databases = append(databases, database)
		}

		var devDatabases []*hubble.DevDatabase
		for _, name := range role.DevDatabases {
			database, ok := devDatabaseMap[name]
			if !ok {
				return model, fmt.Errorf("no such developer database: %s", name)
			}
			devDatabases = append(devDatabases, database)
		}

		var policies []*hubble.PolicyReference
		for _, name := range role.Policies {
			policy, ok := policyMap[name]
			if !ok {
				return model, fmt.Errorf("no such policy: %s", name)
			}
			policies = append(policies, policy)
		}

		r := &hubble.Role{
			Name:                 role.Name,
			GrantedDatabases:     databases,
			GrantedDevDatabases:  devDatabases,
			GrantedGlueDatabases: datalakeGrants,
			Acl:                  acl,
			Policies:             policies,
		}

		model.Roles = append(model.Roles, r)
		roleMap[role.Name] = r
	}

	for _, user := range users.Spec.Users {
		a := model.AddUser(user.Name, user.Email)

		for _, r := range user.Roles {
			role, ok := roleMap[r]
			if !ok {
				return model, fmt.Errorf("no such role: %s", r)
			}
			a.Assign(role)
		}
	}

	return model, nil
}
