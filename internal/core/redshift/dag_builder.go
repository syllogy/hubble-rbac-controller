package redshift


func (d *DagBuilder) UpdateModel(current Model, desired Model) {
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
}

func (d *DagBuilder) AddCluster(cluster *Cluster) {

	for _, desiredGroup := range cluster.Groups {
		d.CreateGroup(desiredGroup)
	}

	for _, desiredUser := range cluster.Users {
		d.CreateUser(desiredUser)
	}

	for _, desiredDatabase := range cluster.Databases {
		d.AddDatabase(desiredDatabase)
	}
}

func (d *DagBuilder) DropCluster(cluster *Cluster) {

	for _, currentUser := range cluster.Users {
		d.DropUser(currentUser)
	}

	for _, currentDatabase := range cluster.Databases {
		d.DropDatabase(currentDatabase)
	}

	for _, currentGroup := range cluster.Groups {
		d.DropGroup(currentGroup)
	}
}

func (d *DagBuilder) UpdateCluster(currentCluster *Cluster, desiredCluster *Cluster) {

	for _, currentUser := range currentCluster.Users {
		desiredUser := desiredCluster.LookupUser(currentUser.Name)

		if desiredUser == nil {
			d.DropUser(currentUser)
		} else {
			d.UpdateUser(currentUser, desiredUser)
		}
	}

	for _, currentGroup := range currentCluster.Groups {
		desiredGroup := desiredCluster.LookupGroup(currentGroup.Name)

		if desiredGroup == nil {
			d.DropGroup(currentGroup)
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
			d.CreateGroup(desiredGroup)
		}
	}

	for _, desiredUser := range desiredCluster.Users {
		currentUser := currentCluster.LookupUser(desiredUser.Name)

		if currentUser == nil {
			d.CreateUser(desiredUser)
		} else {
			d.UpdateUser(currentUser, desiredUser)
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

	for _, group := range database.Groups {
		d.AddDatabaseGroup(group)
	}
}

func (d *DagBuilder) DropDatabase(database *Database) {

	for _, group := range database.Groups {
		d.DropDatabaseGroup(group)
	}
}

func (d *DagBuilder) UpdateDatabase(currentDatabase *Database, desiredDatabase *Database) {

	for _, currentGroup := range currentDatabase.Groups {

		desiredGroup := desiredDatabase.LookupGroup(currentGroup.Name)

		if desiredGroup == nil {
			d.DropDatabaseGroup(currentGroup)
		} else {
			d.UpdateDatabaseGroup(currentGroup, desiredGroup)
		}
	}

	for _, desiredGroup := range desiredDatabase.Groups {

		currentGroup := currentDatabase.LookupGroup(desiredGroup.Name)

		if currentGroup == nil {
			d.AddDatabaseGroup(desiredGroup)
		} else {
			d.UpdateDatabaseGroup(currentGroup, desiredGroup)
		}
	}
}

func (d *DagBuilder) CreateUser(user *User) {

	createUserTask := d.createUserTask(user)
	addToGroupTask := d.addToGroupTask(user)
	addToGroupTask.dependsOn(createUserTask)

	createGroupTask := d.lookupCreateGroupTask(user.MemberOf.Name)
	if createGroupTask != nil {
		addToGroupTask.dependsOn(createGroupTask)
	}
}

func (d *DagBuilder) DropUser(user *User) {

	removeFromGroupTask := d.removeFromGroupTask(user)
	dropUserTask := d.dropUserTask(user)
	dropUserTask.dependsOn(removeFromGroupTask)
}

func (d *DagBuilder) UpdateUser(current *User, desired *User) {

	if current.MemberOf.Name != desired.MemberOf.Name {
		removeFromGroupTask := d.removeFromGroupTask(current)
		addToGroupTask := d.addToGroupTask(desired)
		addToGroupTask.dependsOn(removeFromGroupTask)

		dropGroupTask := d.lookupDropGroupTask(current.MemberOf.Name)

		if dropGroupTask != nil {
			dropGroupTask.dependsOn(removeFromGroupTask)
		}

		createGroupTask := d.lookupCreateGroupTask(desired.MemberOf.Name)

		if createGroupTask != nil {
			addToGroupTask.dependsOn(createGroupTask)
		}
	}
}

func (d *DagBuilder) CreateGroup(group *Group) {

	createGroupTask := d.createGroupTask(group)
	addToGroupTasks := d.lookupAddToGroupTasks(group.Name)

	for _, addToGroupTask := range addToGroupTasks {
		addToGroupTask.dependsOn(createGroupTask)
	}
}

func (d *DagBuilder) DropGroup(group *Group) {

	dropGroupTask := d.dropGroupTask(group)
	removeFromGroupTasks := d.lookupRemoveFromGroupTasks(group.Name)

	for _, removeFromGroupTask := range removeFromGroupTasks {
		dropGroupTask.dependsOn(removeFromGroupTask)
	}
}

func (d *DagBuilder) AddDatabaseGroup(group *DatabaseGroup) {

	for _, schema := range group.GrantedSchemas {

		grantAccessTask := d.grantAccessTask(&ManageAccessModel{
			schemaName: schema.Name,
			groupName:  group.Name,
		})

		createSchemaTask := d.createSchemaTask(schema)
		grantAccessTask.dependsOn(createSchemaTask)

		createGroupTask := d.lookupCreateGroupTask(group.Name)
		if createGroupTask != nil {
			grantAccessTask.dependsOn(createGroupTask)
		}
	}

	for _, schema := range group.GrantedExternalSchemas {

		grantAccessTask := d.grantAccessTask(&ManageAccessModel{
			schemaName: schema.Name,
			groupName:  group.Name,
		})

		createSchemaTask := d.createExternalSchemaTask(schema)
		grantAccessTask.dependsOn(createSchemaTask)

		createGroupTask := d.lookupCreateGroupTask(group.Name)
		if createGroupTask != nil {
			grantAccessTask.dependsOn(createGroupTask)
		}
	}
}

func (d *DagBuilder) DropDatabaseGroup(group *DatabaseGroup) {

	for _, schema := range group.GrantedSchemas {

		revokeAccessTask := d.revokeAccessTask(&ManageAccessModel{
			schemaName: schema.Name,
			groupName:  group.Name,
		})

		dropGroupTask := d.lookupDropGroupTask(group.Name)
		if dropGroupTask != nil {
			dropGroupTask.dependsOn(revokeAccessTask)
		}
	}

	for _, schema := range group.GrantedExternalSchemas {

		revokeAccessTask := d.revokeAccessTask(&ManageAccessModel{
			schemaName: schema.Name,
			groupName:  group.Name,
		})

		dropGroupTask := d.lookupDropGroupTask(group.Name)
		if dropGroupTask != nil {
			dropGroupTask.dependsOn(revokeAccessTask)
		}
	}
}

func (d *DagBuilder) UpdateDatabaseGroup(current *DatabaseGroup, desired *DatabaseGroup) {

	for _, schema := range current.GrantedSchemas {

		grantDesired := desired.LookupGrantedSchema(schema.Name)

		if grantDesired == nil {
			revokeAccessTask := d.revokeAccessTask(&ManageAccessModel{
				schemaName: schema.Name,
				groupName:  current.Name,
			})

			dropGroupTask := d.lookupDropGroupTask(current.Name)
			if dropGroupTask != nil {
				dropGroupTask.dependsOn(revokeAccessTask)
			}
		}
	}

	for _, schema := range current.GrantedExternalSchemas {

		grantDesired := desired.LookupGrantedExternalSchema(schema.Name)

		if grantDesired == nil {
			revokeAccessTask := d.revokeAccessTask(&ManageAccessModel{
				schemaName: schema.Name,
				groupName:  current.Name,
			})

			dropGroupTask := d.lookupDropGroupTask(current.Name)
			if dropGroupTask != nil {
				dropGroupTask.dependsOn(revokeAccessTask)
			}
		}
	}

	for _, schema := range desired.GrantedSchemas {

		grantCurrent := current.LookupGrantedSchema(schema.Name)

		if grantCurrent == nil {
			grantAccessTask := d.grantAccessTask(&ManageAccessModel{
				schemaName: schema.Name,
				groupName:  desired.Name,
			})

			createSchemaTask := d.createSchemaTask(schema)
			grantAccessTask.dependsOn(createSchemaTask)

			createGroupTask := d.lookupCreateGroupTask(desired.Name)
			if createGroupTask != nil {
				grantAccessTask.dependsOn(createGroupTask)
			}
		}
	}

	for _, schema := range desired.GrantedExternalSchemas {

		grantCurrent := current.LookupGrantedExternalSchema(schema.Name)

		if grantCurrent == nil {

			grantAccessTask := d.grantAccessTask(&ManageAccessModel{
				schemaName: schema.Name,
				groupName:  desired.Name,
			})

			createSchemaTask := d.createExternalSchemaTask(schema)
			grantAccessTask.dependsOn(createSchemaTask)

			createGroupTask := d.lookupCreateGroupTask(desired.Name)
			if createGroupTask != nil {
				grantAccessTask.dependsOn(createGroupTask)
			}
		}
	}
}

