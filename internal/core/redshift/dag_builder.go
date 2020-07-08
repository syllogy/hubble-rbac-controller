package redshift

import (
	"fmt"
)

type DagBuilder struct {
	tasks []*Task
}

func NewDagBuilder() *DagBuilder {
	return &DagBuilder{}
}

func (d *DagBuilder) UpdateModel(current *Model, desired *Model) *Dag {

	for _, currentCluster := range current.Clusters {
		desiredCluster := desired.LookupCluster(currentCluster.Identifier)

		if desiredCluster == nil {
			d.DropCluster(currentCluster)
		} else {
			d.UpdateCluster(currentCluster, desiredCluster)
		}
	}

	for _, desiredCluster := range desired.Clusters {
		currentCluster := current.LookupCluster(desiredCluster.Identifier)

		if currentCluster == nil {
			d.AddCluster(desiredCluster)
		} else {
			d.UpdateCluster(currentCluster, desiredCluster)
		}
	}

	return NewDag(d.tasks)
}

func (d *DagBuilder) AddCluster(cluster *Cluster) {

	for _, desiredGroup := range cluster.Groups {
		d.CreateGroup(cluster.Identifier, desiredGroup)
	}

	for _, desiredUser := range cluster.Users {
		d.CreateUser(cluster.Identifier, desiredUser)
	}

	for _, desiredDatabase := range cluster.Databases {
		d.AddDatabase(desiredDatabase)
	}
}

func (d *DagBuilder) DropCluster(cluster *Cluster) {

	for _, currentUser := range cluster.Users {
		d.DropUser(cluster.Identifier, currentUser)
	}

	for _, currentDatabase := range cluster.Databases {
		d.DropDatabase(currentDatabase)
	}

	for _, currentGroup := range cluster.Groups {
		d.DropGroup(cluster.Identifier, currentGroup)
	}
}

func (d *DagBuilder) UpdateCluster(currentCluster *Cluster, desiredCluster *Cluster) {

	for _, currentUser := range currentCluster.Users {
		desiredUser := desiredCluster.LookupUser(currentUser.Name)

		if desiredUser == nil {
			d.DropUser(currentCluster.Identifier, currentUser)
		} else {
			d.UpdateUser(currentCluster.Identifier, currentUser, desiredUser)
		}
	}

	for _, currentGroup := range currentCluster.Groups {
		desiredGroup := desiredCluster.LookupGroup(currentGroup.Name)

		if desiredGroup == nil {
			d.DropGroup(currentCluster.Identifier, currentGroup)
		}
	}

	for _, currentDatabase := range currentCluster.Databases {
		desiredDatabase := desiredCluster.LookupDatabase(currentDatabase.Name)

		if desiredDatabase == nil {
			d.DropDatabase(currentDatabase)
		} else {
			d.UpdateDatabase(currentDatabase, desiredDatabase)
		}
	}

	for _, desiredGroup := range desiredCluster.Groups {
		currentGroup := currentCluster.LookupGroup(desiredGroup.Name)

		if currentGroup == nil {
			d.CreateGroup(currentCluster.Identifier, desiredGroup)
		}
	}

	for _, desiredUser := range desiredCluster.Users {
		currentUser := currentCluster.LookupUser(desiredUser.Name)

		if currentUser == nil {
			d.CreateUser(currentCluster.Identifier, desiredUser)
		} else {
			d.UpdateUser(currentCluster.Identifier, currentUser, desiredUser)
		}
	}

	for _, desiredDatabase := range desiredCluster.Databases {
		currentDatabase := currentCluster.LookupDatabase(desiredDatabase.Name)

		if currentDatabase == nil {
			d.AddDatabase(desiredDatabase)
		} else {
			d.UpdateDatabase(currentDatabase, desiredDatabase)
		}
	}
}

func (d *DagBuilder) AddDatabase(database *Database) {

	d.createDatabaseTask(database.ClusterIdentifier, database)

	for _, group := range database.Groups {
		d.AddDatabaseGroup(database, group)
	}
}

func (d *DagBuilder) DropDatabase(database *Database) {

	for _, group := range database.Groups {
		d.DropDatabaseGroup(database, group)
	}
}

func stringsEqual(lhs *string, rhs *string) bool {
	if lhs == nil && rhs == nil {
		return true
	}
	if lhs != nil && rhs != nil {
		return *lhs == *rhs
	}
	return false
}

func (d *DagBuilder) UpdateDatabase(currentDatabase *Database, desiredDatabase *Database) {

	if !stringsEqual(currentDatabase.Owner, desiredDatabase.Owner) {
		panic(fmt.Errorf("Owners are different!!!"))
	}

	for _, currentGroup := range currentDatabase.Groups {

		desiredGroup := desiredDatabase.LookupGroup(currentGroup.Name)

		if desiredGroup == nil {
			d.DropDatabaseGroup(currentDatabase, currentGroup)
		} else {
			d.UpdateDatabaseGroup(currentDatabase, currentGroup, desiredGroup)
		}
	}

	for _, desiredGroup := range desiredDatabase.Groups {

		currentGroup := currentDatabase.LookupGroup(desiredGroup.Name)

		if currentGroup == nil {
			d.AddDatabaseGroup(desiredDatabase, desiredGroup)
		} else {
			d.UpdateDatabaseGroup(currentDatabase, currentGroup, desiredGroup)
		}
	}
}

func (d *DagBuilder) CreateUser(clusterIdentifier string, user *User) {

	createUserTask := d.createUserTask(clusterIdentifier, user)
	addToGroupTask := d.addToGroupTask(clusterIdentifier,user, user.Role())
	addToGroupTask.dependsOn(createUserTask)

	createGroupTask := d.lookupCreateGroupTask(clusterIdentifier, user.Role().Name)
	if createGroupTask != nil {
		addToGroupTask.dependsOn(createGroupTask)
	}
}

func (d *DagBuilder) DropUser(clusterIdentifier string, user *User) {

	dropUserTask := d.dropUserTask(clusterIdentifier, user)

	for _, group := range user.MemberOf {
		removeFromGroupTask := d.removeFromGroupTask(clusterIdentifier, user, group)
		dropUserTask.dependsOn(removeFromGroupTask)
	}

}

func (d *DagBuilder) UpdateUser(clusterIdentifier string, current *User, desired *User) {

	for _, group := range current.MemberOf {
		if group.Name != desired.Role().Name {
			removeFromGroupTask := d.removeFromGroupTask(clusterIdentifier, current, group)
			dropGroupTask := d.lookupDropGroupTask(clusterIdentifier, group.Name)

			if dropGroupTask != nil {
				dropGroupTask.dependsOn(removeFromGroupTask)
			}
		}
	}

	if !current.IsMemberOf(desired.Role().Name) {
		addToGroupTask := d.addToGroupTask(clusterIdentifier, desired, desired.Role())

		createGroupTask := d.lookupCreateGroupTask(clusterIdentifier, desired.Role().Name)

		if createGroupTask != nil {
			addToGroupTask.dependsOn(createGroupTask)
		}
	}
}

func (d *DagBuilder) CreateGroup(clusterIdentifier string, group *Group) {

	createGroupTask := d.createGroupTask(clusterIdentifier, group)
	addToGroupTasks := d.lookupAddToGroupTasks(clusterIdentifier, group.Name)

	for _, addToGroupTask := range addToGroupTasks {
		addToGroupTask.dependsOn(createGroupTask)
	}
}

func (d *DagBuilder) DropGroup(clusterIdentifier string, group *Group) {

	dropGroupTask := d.dropGroupTask(clusterIdentifier, group)
	removeFromGroupTasks := d.lookupRemoveFromGroupTasks(clusterIdentifier, group.Name)

	for _, removeFromGroupTask := range removeFromGroupTasks {
		dropGroupTask.dependsOn(removeFromGroupTask)
	}
}

func (d *DagBuilder) AddDatabaseGroup(database *Database, group *DatabaseGroup) {

	createDatabaseTask := d.lookupCreateDatabaseTask(database.ClusterIdentifier, database.Name)

	for _, schema := range group.GrantedSchemas {

		grantAccessTask := d.grantAccessTask(database, schema.Name, group.Name)

		if createDatabaseTask != nil {
			grantAccessTask.dependsOn(createDatabaseTask)
		}

		createSchemaTask := d.createSchemaTask(database, schema)

		if createDatabaseTask != nil {
			createSchemaTask.dependsOn(createDatabaseTask)
		}

		grantAccessTask.dependsOn(createSchemaTask)

		createGroupTask := d.lookupCreateGroupTask(database.ClusterIdentifier, group.Name)
		if createGroupTask != nil {
			grantAccessTask.dependsOn(createGroupTask)
		}
	}

	for _, schema := range group.GrantedExternalSchemas {

		grantAccessTask := d.grantAccessTask(database, schema.Name, group.Name)

		if createDatabaseTask != nil {
			grantAccessTask.dependsOn(createDatabaseTask)
		}

		createSchemaTask := d.createExternalSchemaTask(database, schema)

		if createDatabaseTask != nil {
			createSchemaTask.dependsOn(createDatabaseTask)
		}

		grantAccessTask.dependsOn(createSchemaTask)

		createGroupTask := d.lookupCreateGroupTask(database.ClusterIdentifier, group.Name)
		if createGroupTask != nil {
			grantAccessTask.dependsOn(createGroupTask)
		}
	}
}

func (d *DagBuilder) DropDatabaseGroup(database *Database, group *DatabaseGroup) {

	for _, schema := range group.GrantedSchemas {

		if schema.Name != "xpublic" { //groups will always get access to the public schema in all database and we can't revoke it
			revokeAccessTask := d.revokeAccessTask(database, schema.Name, group.Name)

			dropGroupTask := d.lookupDropGroupTask(database.ClusterIdentifier, group.Name)
			if dropGroupTask != nil {
				dropGroupTask.dependsOn(revokeAccessTask)
			}
		}
	}

	for _, schema := range group.GrantedExternalSchemas {

		revokeAccessTask := d.revokeAccessTask(database, schema.Name, group.Name)

		dropGroupTask := d.lookupDropGroupTask(database.ClusterIdentifier, group.Name)
		if dropGroupTask != nil {
			dropGroupTask.dependsOn(revokeAccessTask)
		}
	}
}

func (d *DagBuilder) UpdateDatabaseGroup(database *Database, current *DatabaseGroup, desired *DatabaseGroup) {

	for _, schema := range current.GrantedSchemas {

		grantDesired := desired.LookupGrantedSchema(schema.Name)

		if grantDesired == nil {
			if schema.Name != "xpublic" { //groups will always get access to the public schema in all database and we can't revoke it
				revokeAccessTask := d.revokeAccessTask(database, schema.Name, current.Name)

				dropGroupTask := d.lookupDropGroupTask(database.ClusterIdentifier, current.Name)
				if dropGroupTask != nil {
					dropGroupTask.dependsOn(revokeAccessTask)
				}
			}
		}
	}

	for _, schema := range current.GrantedExternalSchemas {

		grantDesired := desired.LookupGrantedExternalSchema(schema.Name)

		if grantDesired == nil {
			revokeAccessTask := d.revokeAccessTask(database, schema.Name, current.Name)

			dropGroupTask := d.lookupDropGroupTask(database.ClusterIdentifier, current.Name)
			if dropGroupTask != nil {
				dropGroupTask.dependsOn(revokeAccessTask)
			}
		}
	}

	for _, schema := range desired.GrantedSchemas {

		grantCurrent := current.LookupGrantedSchema(schema.Name)

		if grantCurrent == nil {
			grantAccessTask := d.grantAccessTask(database, schema.Name, desired.Name)

			createSchemaTask := d.createSchemaTask(database, schema)
			grantAccessTask.dependsOn(createSchemaTask)

			createGroupTask := d.lookupCreateGroupTask(database.ClusterIdentifier, desired.Name)
			if createGroupTask != nil {
				grantAccessTask.dependsOn(createGroupTask)
			}
		}
	}

	for _, schema := range desired.GrantedExternalSchemas {

		grantCurrent := current.LookupGrantedExternalSchema(schema.Name)

		if grantCurrent == nil {

			grantAccessTask := d.grantAccessTask(database, schema.Name, desired.Name)

			createSchemaTask := d.createExternalSchemaTask(database, schema)
			grantAccessTask.dependsOn(createSchemaTask)

			createGroupTask := d.lookupCreateGroupTask(database.ClusterIdentifier, desired.Name)
			if createGroupTask != nil {
				grantAccessTask.dependsOn(createGroupTask)
			}
		}
	}
}

