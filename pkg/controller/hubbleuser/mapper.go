package hubbleuser

import (
	"fmt"
	lunarwayv1alpha1 "github.com/lunarway/hubble-rbac-controller/pkg/apis/lunarway/v1alpha1"
	"github.com/lunarway/hubble-rbac-controller/internal/core/hubble"
)

func mapCrdsToHubbleModel(users *lunarwayv1alpha1.HubbleUserList,
	policies *lunarwayv1alpha1.HubblePolicyReferenceList,
	databases *lunarwayv1alpha1.HubbleDatabaseList,
	developerDatabases *lunarwayv1alpha1.HubbleDeveloperDatabaseList,
	roles *lunarwayv1alpha1.HubbleRoleList) (hubble.Model, error) {

	model := hubble.Model{}

	databaseMap := make(map[string]*hubble.Database)
	devDatabaseMap := make(map[string]*hubble.DevDatabase)
	policyMap := make(map[string]*hubble.PolicyReference)
	datalakeGrantsMap := make(map[string]*hubble.GlueDatabase)
	roleMap := make(map[string]*hubble.Role)

	for _,database := range databases.Items {
		databaseMap[database.Name] = model.AddDatabase(database.Spec.Cluster, database.Spec.Name)
	}

	for _,database := range developerDatabases.Items {
		devDatabaseMap[database.Name] = model.AddDevDatabase(database.Spec.Cluster)
	}

	for _, policy := range policies.Items {
		policyMap[policy.Name] = model.AddPolicyReference(policy.Spec.Arn)
	}

	for _, role := range roles.Items {
		for _, name := range role.Spec.DatalakeGrants {
			datalakeGrantsMap[name] = &hubble.GlueDatabase{
				ShortName: name,
				Name:      name,
			}
		}
	}

	for _, role := range roles.Items {
		var acl []hubble.DataSet
		for _, name := range role.Spec.DatawarehouseGrants {
			acl = append(acl, hubble.DataSet(name))
		}

		var datalakeGrants []*hubble.GlueDatabase
		for _, name := range role.Spec.DatalakeGrants {
			datalakeGrants = append(datalakeGrants, datalakeGrantsMap[name])
		}

		var databases []*hubble.Database
		for _, name := range role.Spec.Databases {
			database, ok := databaseMap[name]
			if !ok {
				return model, fmt.Errorf("no such database: %s", name)
			}

			databases = append(databases, database)
		}

		var devDatabases []*hubble.DevDatabase
		for _, name := range role.Spec.DevDatabases {
			database, ok := devDatabaseMap[name]
			if !ok {
				return model, fmt.Errorf("no such developer database: %s", name)
			}
			devDatabases = append(devDatabases, database)
		}

		var policies []*hubble.PolicyReference
		for _, name := range role.Spec.Policies {
			policy, ok := policyMap[name]
			if !ok {
				return model, fmt.Errorf("no such policy: %s", name)
			}
			policies = append(policies, policy)
		}

		r := &hubble.Role{
			Name:                 role.Spec.Name,
			GrantedDatabases:     databases,
			GrantedDevDatabases:  devDatabases,
			GrantedGlueDatabases: datalakeGrants,
			Acl:                  acl,
			Policies:             policies,
		}

		model.Roles = append(model.Roles, r)
		roleMap[role.Name] = r
	}

	for _, user := range users.Items {
		a := model.AddUser(user.Spec.Name, user.Spec.Email)

		for _,r := range user.Spec.Roles {
			a.Assign(roleMap[r])
		}
	}

	return model, nil
}

