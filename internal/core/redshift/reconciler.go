package redshift

import (
	"fmt"
)

// The Reconciler knows how to reconcile two different instances of the redshift.Model.
// It will run the sequence of operations needed to transform the source model to the target model.
// It is nondestructive with regards to data. This means it will never drop a database, a schema or table,
// but it will drop groups, users etc.
type Reconciler struct {
	tasks []*Task
}

func NewReconciler() *Reconciler {
	return &Reconciler{}
}

// Reconciles the two models.
// The operations need to be executed in an order that respects the dependencies between the objects
// otherwise the operations will fail. E.g. redshift will complain if one attempts to drop a group that has members or has grants on schemas.
// Instead of executing the reconciliation it returns a DAG that represents the reconciliation process as a set of tasks.
// The DAG can be executed using the SequentialDagRunner.
// By modelling the process as a DAG we decouple the interdependencies of the tasks with the execution. This allows us to optimise the execution
// independently from the task interdependencies (e.g. parallelising it). It also makes the code easier to understand and maintain because the code structure
// would otherwise be coupled to the task interdependencies (the order of the function calls in the code would have to respect the dependencies)
func (d *Reconciler) Reconcile(current *Model, desired *Model) *ReconciliationDag {

	for _, currentCluster := range current.Clusters {
		desiredCluster := desired.LookupCluster(currentCluster.Identifier)

		if desiredCluster == nil {
			d.dropCluster(currentCluster)
		} else {
			d.updateCluster(currentCluster, desiredCluster)
		}
	}

	for _, desiredCluster := range desired.Clusters {
		currentCluster := current.LookupCluster(desiredCluster.Identifier)

		if currentCluster == nil {
			d.addCluster(desiredCluster)
		} else {
			d.updateCluster(currentCluster, desiredCluster)
		}
	}

	return NewDag(d.tasks)
}

func (d *Reconciler) addCluster(cluster *Cluster) {

	for _, desiredGroup := range cluster.Groups {
		d.createGroup(cluster.Identifier, desiredGroup)
	}

	for _, desiredUser := range cluster.Users {
		d.createUser(cluster.Identifier, desiredUser)
	}

	for _, desiredDatabase := range cluster.Databases {
		d.addDatabase(desiredDatabase)
	}
}

func (d *Reconciler) dropCluster(cluster *Cluster) {

	for _, currentUser := range cluster.Users {
		d.dropUser(cluster.Identifier, currentUser)
	}

	for _, currentDatabase := range cluster.Databases {
		d.dropDatabase(currentDatabase)
	}

	for _, currentGroup := range cluster.Groups {
		d.dropGroup(cluster.Identifier, currentGroup)
	}
}

func (d *Reconciler) updateCluster(currentCluster *Cluster, desiredCluster *Cluster) {

	for _, currentUser := range currentCluster.Users {
		desiredUser := desiredCluster.LookupUser(currentUser.Name)

		if desiredUser == nil {
			d.dropUser(currentCluster.Identifier, currentUser)
		} else {
			d.updateUser(currentCluster.Identifier, currentUser, desiredUser)
		}
	}

	for _, currentGroup := range currentCluster.Groups {
		desiredGroup := desiredCluster.LookupGroup(currentGroup.Name)

		if desiredGroup == nil {
			d.dropGroup(currentCluster.Identifier, currentGroup)
		}
	}

	for _, currentDatabase := range currentCluster.Databases {
		desiredDatabase := desiredCluster.LookupDatabase(currentDatabase.Name)

		if desiredDatabase == nil {
			d.dropDatabase(currentDatabase)
		} else {
			d.updateDatabase(currentDatabase, desiredDatabase)
		}
	}

	for _, desiredGroup := range desiredCluster.Groups {
		currentGroup := currentCluster.LookupGroup(desiredGroup.Name)

		if currentGroup == nil {
			d.createGroup(currentCluster.Identifier, desiredGroup)
		}
	}

	for _, desiredUser := range desiredCluster.Users {
		currentUser := currentCluster.LookupUser(desiredUser.Name)

		if currentUser == nil {
			d.createUser(currentCluster.Identifier, desiredUser)
		} else {
			d.updateUser(currentCluster.Identifier, currentUser, desiredUser)
		}
	}

	for _, desiredDatabase := range desiredCluster.Databases {
		currentDatabase := currentCluster.LookupDatabase(desiredDatabase.Name)

		if currentDatabase == nil {
			d.addDatabase(desiredDatabase)
		} else {
			d.updateDatabase(currentDatabase, desiredDatabase)
		}
	}
}

func (d *Reconciler) addDatabase(database *Database) {

	d.createDatabaseTask(database.ClusterIdentifier, database)

	for _, group := range database.Groups {
		d.addDatabaseGroup(database, group)
	}
}

func (d *Reconciler) dropDatabase(database *Database) {

	for _, group := range database.Groups {
		d.dropDatabaseGroup(database, group)
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

func (d *Reconciler) updateDatabase(currentDatabase *Database, desiredDatabase *Database) {

	if !stringsEqual(currentDatabase.Owner, desiredDatabase.Owner) {
		panic(fmt.Errorf("Owners are different!!!"))
	}

	for _, currentGroup := range currentDatabase.Groups {

		desiredGroup := desiredDatabase.LookupGroup(currentGroup.Name)

		if desiredGroup == nil {
			d.dropDatabaseGroup(currentDatabase, currentGroup)
		} else {
			d.updateDatabaseGroup(currentDatabase, currentGroup, desiredGroup)
		}
	}

	for _, desiredGroup := range desiredDatabase.Groups {

		currentGroup := currentDatabase.LookupGroup(desiredGroup.Name)

		if currentGroup == nil {
			d.addDatabaseGroup(desiredDatabase, desiredGroup)
		} else {
			d.updateDatabaseGroup(currentDatabase, currentGroup, desiredGroup)
		}
	}
}

func (d *Reconciler) createUser(clusterIdentifier string, user *User) {

	createUserTask := d.createUserTask(clusterIdentifier, user)
	addToGroupTask := d.addToGroupTask(clusterIdentifier,user, user.Role())
	addToGroupTask.dependsOn(createUserTask)

	createGroupTask := d.lookupCreateGroupTask(clusterIdentifier, user.Role().Name)
	if createGroupTask != nil {
		addToGroupTask.dependsOn(createGroupTask)
	}
}

func (d *Reconciler) dropUser(clusterIdentifier string, user *User) {

	dropUserTask := d.dropUserTask(clusterIdentifier, user)

	for _, group := range user.MemberOf {
		removeFromGroupTask := d.removeFromGroupTask(clusterIdentifier, user, group)
		dropUserTask.dependsOn(removeFromGroupTask)
	}

}

func (d *Reconciler) updateUser(clusterIdentifier string, current *User, desired *User) {

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

func (d *Reconciler) createGroup(clusterIdentifier string, group *Group) {

	createGroupTask := d.createGroupTask(clusterIdentifier, group)
	addToGroupTasks := d.lookupAddToGroupTasks(clusterIdentifier, group.Name)

	for _, addToGroupTask := range addToGroupTasks {
		addToGroupTask.dependsOn(createGroupTask)
	}
}

func (d *Reconciler) dropGroup(clusterIdentifier string, group *Group) {

	dropGroupTask := d.dropGroupTask(clusterIdentifier, group)
	removeFromGroupTasks := d.lookupRemoveFromGroupTasks(clusterIdentifier, group.Name)

	for _, removeFromGroupTask := range removeFromGroupTasks {
		dropGroupTask.dependsOn(removeFromGroupTask)
	}
}

func (d *Reconciler) addDatabaseGroup(database *Database, group *DatabaseGroup) {

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

func (d *Reconciler) dropDatabaseGroup(database *Database, group *DatabaseGroup) {

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

func (d *Reconciler) updateDatabaseGroup(database *Database, current *DatabaseGroup, desired *DatabaseGroup) {

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
