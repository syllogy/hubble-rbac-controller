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

type Model struct {
	RedshiftModel redshift.Model
	IamModel iam.Model
	GoogleModel google.Model
}

func (r *Resolver) Resolve(grant hubble.Model) (Model, error) {

	model := Model{
		RedshiftModel:redshift.Model{},
		IamModel:iam.Model{},
		GoogleModel:google.Model{},
	}

	for _,user := range grant.Users {

		googleLogin := model.GoogleModel.DeclareUser(user.Email)

		for _,role := range user.AssignedTo {

			//Allow the user to log in with the role
			googleLogin.Assign(role.Name)

			//Declare an AWS role for the given role
			iamRole := model.IamModel.DeclareRole(role.Name)

			userAndRoleUsername := fmt.Sprintf("%s_%s", user.Username, role.Name)

			databaseLoginPolicyForUserAndRole := iamRole.DeclareDatabaseLoginPolicyForUser(user.Email, userAndRoleUsername)

			setupDatabase := func(clusterIdentifier string, dbName string) {

				//Allow user/role to log into the database
				databaseLoginPolicyForUserAndRole.Allow(clusterIdentifier, dbName)

				database := model.RedshiftModel.DeclareDatabase(clusterIdentifier, dbName)

				//Set needed grants on the user group
				group := database.DeclareGroup(role.Name)
				for _,schema := range role.Acl {
					group.GrantSchema(redshift.Schema{ Name: string(schema) }) //TODO: is it ok to assume that there is a schema with name = dataset?
				}

				//Declare a redshift user for the user/role and add it to the group
				database.DeclareUser(userAndRoleUsername, group)

				for _,glueDb := range role.GrantedGlueDatabases {
					schema := redshift.ExternalSchema{
						Name:             glueDb.ShortName,
						GlueDatabaseName: glueDb.Name,
					}
					group.GrantExternalSchema(schema)
				}
			}

			for _,db := range role.GrantedDatabases {
				setupDatabase(db.ClusterIdentifier, db.Name)
			}

			for _,db := range role.GrantedDevDatabases {
				setupDatabase(db.ClusterIdentifier, user.Username)
			}
		}
	}

	return model, nil
}
