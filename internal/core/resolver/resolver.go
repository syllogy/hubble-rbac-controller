package resolver

import (
	"fmt"
	"github.com/lunarway/hubble-rbac-controller/internal/core/google"
	"github.com/lunarway/hubble-rbac-controller/internal/core/hubble"
	"github.com/lunarway/hubble-rbac-controller/internal/core/iam"
	"github.com/lunarway/hubble-rbac-controller/internal/core/redshift"
)

type Resolver struct {

}

//transforms the given hubble model into separate models for the 3 systems we want to reconcile
func (r *Resolver) Resolve(model hubble.Model) (redshift.Model, iam.Model, google.Model, error) {

	redshiftModel:=redshift.Model{}
	iamModel:=iam.Model{}
	googleModel:=google.Model{}

	for _,db := range model.Databases {
		cluster := redshiftModel.DeclareCluster(db.ClusterIdentifier)
		cluster.DeclareDatabase(db.Name)
	}

	for _,role := range model.Roles {
		iamModel.DeclareRole(role.Name)
	}

	for _,user := range model.Users {

		googleLogin := googleModel.DeclareUser(user.Email)

		for _,role := range user.AssignedTo {

			//Allow the user to log in with the role
			googleLogin.Assign(role.Name)

			//Declare an AWS role for the given role
			iamRole := iamModel.DeclareRole(role.Name)

			userAndRoleUsername := fmt.Sprintf("%s_%s", user.Username, role.Name)

			databaseLoginPolicyForUserAndRole := iamRole.DeclareDatabaseLoginPolicyForUser(user.Email, userAndRoleUsername)

			for _,db := range role.GrantedDatabases {
				//Allow user/role to log into the database
				databaseLoginPolicyForUserAndRole.Allow(db.ClusterIdentifier, db.Name)

				cluster := redshiftModel.DeclareCluster(db.ClusterIdentifier)

				database := cluster.DeclareDatabase(db.Name)

				//Set needed grants on the user group
				group := cluster.DeclareGroup(role.Name)
				databaseGroup := database.DeclareGroup(role.Name)
				databaseGroup.GrantSchema(&redshift.Schema{ Name: "public" })
				for _,schema := range role.Acl {
					databaseGroup.GrantSchema(&redshift.Schema{ Name: string(schema) }) //TODO: is it ok to assume that there is a schema with name = dataset?
				}

				//Declare a redshift user for the user/role and add it to the group
				cluster.DeclareUser(userAndRoleUsername, group)
				database.DeclareUser(userAndRoleUsername)

				for _,glueDb := range role.GrantedGlueDatabases {
					schema := redshift.ExternalSchema{
						Name:             glueDb.ShortName,
						GlueDatabaseName: glueDb.Name,
					}
					databaseGroup.GrantExternalSchema(&schema)
				}
			}

			for _,db := range role.GrantedDevDatabases {

				//Allow user/role to log into the database
				databaseLoginPolicyForUserAndRole.Allow(db.ClusterIdentifier, user.Username)

				cluster := redshiftModel.DeclareCluster(db.ClusterIdentifier)
				database := cluster.DeclareDatabaseWithOwner(user.Username, userAndRoleUsername)

				group := cluster.DeclareGroup(role.Name)
				databaseGroup := database.DeclareGroup(role.Name)
				databaseGroup.GrantSchema(&redshift.Schema{ Name: "public" })

				//Declare a redshift user for the user/role and add it to the group
				cluster.DeclareUser(userAndRoleUsername, group)
				database.DeclareUser(userAndRoleUsername)

				for _,glueDb := range role.GrantedGlueDatabases {
					schema := redshift.ExternalSchema{
						Name:             glueDb.ShortName,
						GlueDatabaseName: glueDb.Name,
					}
					databaseGroup.GrantExternalSchema(&schema)
				}
			}

			for _,policy := range role.Policies {
				iamRole.DeclareReferencedPolicy(policy.Arn)
			}
		}
	}

	return redshiftModel, iamModel, googleModel, nil
}
