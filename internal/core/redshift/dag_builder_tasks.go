package redshift

import "fmt"

func (d *DagBuilder) createTask(identifier string, taskType TaskType, model interface{}) *Task {
	task := NewTask(identifier, taskType, model)
	d.tasks = append(d.tasks, task)
	return task
}

func (d *DagBuilder) createUserTask(clusterIdentifier string, model *User) *Task {
	existing := d.lookupCreateUserTask(clusterIdentifier, model.Name)

	if existing != nil {
		return existing
	}

	return d.createTask(model.Name, CreateUser, &UserModel{ClusterIdentifier: clusterIdentifier, User: model})
}

func (d *DagBuilder) dropUserTask(clusterIdentifier string, model *User) *Task {
	existing := d.lookupDropUserTask(clusterIdentifier, model.Name)

	if existing != nil {
		return existing
	}

	return d.createTask(model.Name, DropUser, &UserModel{ClusterIdentifier: clusterIdentifier, User: model})
}

func (d *DagBuilder) createGroupTask(clusterIdentifier string, model *Group) *Task {
	existing := d.lookupCreateGroupTask(clusterIdentifier, model.Name)

	if existing != nil {
		return existing
	}

	return d.createTask(model.Name, CreateGroup, &GroupModel{
		Group:             model,
		ClusterIdentifier: clusterIdentifier,
	})
}

func (d *DagBuilder) dropGroupTask(clusterIdentifier string, model *Group) *Task {
	existing := d.lookupDropGroupTask(clusterIdentifier, model.Name)

	if existing != nil {
		return existing
	}

	return d.createTask(model.Name, DropGroup, &GroupModel{
		Group:             model,
		ClusterIdentifier: clusterIdentifier,
	})
}

func (d *DagBuilder) createSchemaTask(database *Database, model *Schema) *Task {
	existing := d.lookupCreateSchemaTask(database, model.Name)

	if existing != nil {
		return existing
	}

	return d.createTask(model.Name, CreateSchema, &SchemaModel{
		Schema:model,
		Database:database,
	})
}

func (d *DagBuilder) createExternalSchemaTask(database *Database, model *ExternalSchema) *Task {
	existing := d.lookupCreateExternalSchemaTask(database, model.Name)

	if existing != nil {
		return existing
	}

	return d.createTask(model.Name, CreateExternalSchema, &ExternalSchemaModel{
		Schema:model,
		Database:database,
	})
}

func (d *DagBuilder) createDatabaseTask(clusterIdentifier string, model *Database) *Task {
	existing := d.lookupCreateDatabaseTask(clusterIdentifier, model.Name)

	if existing != nil {
		return existing
	}

	return d.createTask(model.Name, CreateDatabase, &DatabaseModel{
		Database:model,
		ClusterIdentifier:clusterIdentifier,
	})
}

func (d *DagBuilder) grantAccessTask(database *Database, schemaName string, groupName string) *Task {
	existing := d.lookupGrantAccessTask(database,schemaName, groupName)

	if existing != nil {
		return existing
	}

	return d.createTask(fmt.Sprintf("%s->%s", groupName, schemaName), GrantAccess, &ManageAccessModel{
		GroupName:  groupName,
		SchemaName: schemaName,
		Database:   database,
	})
}

func (d *DagBuilder) revokeAccessTask(database *Database, schemaName string, groupName string) *Task {
	existing := d.lookupRevokeAccessTask(database, schemaName, groupName)

	if existing != nil {
		return existing
	}

	return d.createTask(fmt.Sprintf("%s->%s", groupName, schemaName), RevokeAccess, &ManageAccessModel{
		GroupName:  groupName,
		SchemaName: schemaName,
		Database:   database,
	})
}

func (d *DagBuilder) addToGroupTask(clusterIdentifier string, model *User, group *Group) *Task {
	existing := d.lookupAddToGroupTask(clusterIdentifier, model.Name, group.Name)

	if existing != nil {
		return existing
	}

	return d.createTask(fmt.Sprintf("%s->%s", model.Name, group.Name), AddToGroup, &ManageMembershipModel{ClusterIdentifier:clusterIdentifier, Username: model.Name, GroupName:model.Role().Name})
}

func (d *DagBuilder) removeFromGroupTask(clusterIdentifier string, model *User, group *Group) *Task {
	existing := d.lookupRemoveFromGroupTask(clusterIdentifier, model.Name, group.Name)

	if existing != nil {
		return existing
	}

	return d.createTask(fmt.Sprintf("%s->%s", model.Name, group.Name), RemoveFromGroup, &ManageMembershipModel{ClusterIdentifier:clusterIdentifier, Username: model.Name, GroupName:model.Role().Name})
}

func (d *DagBuilder) lookupCreateUserTask(clusterIdentifier string, username string) *Task {
	for _, task := range d.tasks {
		if task.taskType == CreateUser && task.model.(*UserModel).ClusterIdentifier == clusterIdentifier && task.model.(*UserModel).User.Name == username {
			return task
		}
	}
	return nil
}

func (d *DagBuilder) lookupDropUserTask(clusterIdentifier string, username string) *Task {
	for _, task := range d.tasks {
		if task.taskType == DropUser && task.model.(*UserModel).ClusterIdentifier == clusterIdentifier && task.model.(*UserModel).User.Name == username {
			return task
		}
	}
	return nil
}

func (d *DagBuilder) lookupCreateGroupTask(clusterIdentifier string, name string) *Task {
	for _, task := range d.tasks {
		if task.taskType == CreateGroup && task.model.(*GroupModel).ClusterIdentifier == clusterIdentifier && task.model.(*GroupModel).Group.Name == name {
			return task
		}
	}
	return nil
}

func (d *DagBuilder) lookupDropGroupTask(clusterIdentifier string, name string) *Task {
	for _, task := range d.tasks {
		if task.taskType == DropGroup && task.model.(*GroupModel).ClusterIdentifier == clusterIdentifier && task.model.(*GroupModel).Group.Name == name {
			return task
		}
	}
	return nil
}

func (d *DagBuilder) lookupCreateSchemaTask(database *Database,name string) *Task {
	for _, task := range d.tasks {
		if task.taskType == CreateSchema && task.model.(*SchemaModel).Database.ClusterIdentifier == database.ClusterIdentifier && task.model.(*SchemaModel).Database.Name == database.Name && task.model.(*SchemaModel).Schema.Name == name {
			return task
		}
	}
	return nil
}

func (d *DagBuilder) lookupCreateExternalSchemaTask(database *Database,name string) *Task {
	for _, task := range d.tasks {
		if task.taskType == CreateExternalSchema && task.model.(*ExternalSchemaModel).Database.ClusterIdentifier == database.ClusterIdentifier && task.model.(*ExternalSchemaModel).Database.Name == database.Name && task.model.(*ExternalSchemaModel).Schema.Name == name {
			return task
		}
	}
	return nil
}

func (d *DagBuilder) lookupCreateDatabaseTask(clusterIdentifier string, name string) *Task {
	for _, task := range d.tasks {
		if task.taskType == CreateDatabase && task.model.(*DatabaseModel).Database.ClusterIdentifier == clusterIdentifier && task.model.(*DatabaseModel).Database.Name == name {
			return task
		}
	}
	return nil
}

func (d *DagBuilder) lookupGrantAccessTask(database *Database,schemaName string, groupName string) *Task {
	for _, task := range d.tasks {
		if task.taskType == GrantAccess && task.model.(*ManageAccessModel).Database.ClusterIdentifier == database.ClusterIdentifier && task.model.(*ManageAccessModel).Database.Name == database.Name && task.model.(*ManageAccessModel).SchemaName == schemaName &&
			task.model.(*ManageAccessModel).GroupName == groupName {
			return task
		}
	}
	return nil
}

func (d *DagBuilder) lookupRevokeAccessTask(database *Database,schemaName string, groupName string) *Task {
	for _, task := range d.tasks {
		if task.taskType == RevokeAccess && task.model.(*ManageAccessModel).Database.ClusterIdentifier == database.ClusterIdentifier && task.model.(*ManageAccessModel).Database.Name == database.Name && task.model.(*ManageAccessModel).SchemaName == schemaName &&
			task.model.(*ManageAccessModel).GroupName == groupName {
			return task
		}
	}
	return nil
}

func (d *DagBuilder) lookupAddToGroupTask(clusterIdentifier string, username string, groupName string) *Task {
	for _, task := range d.tasks {
		if task.taskType == AddToGroup && task.model.(*ManageMembershipModel).ClusterIdentifier == clusterIdentifier && task.model.(*ManageMembershipModel).Username == username &&
			task.model.(*ManageMembershipModel).GroupName == groupName {
			return task
		}
	}
	return nil
}

func (d *DagBuilder) lookupAddToGroupTasks(clusterIdentifier string, groupName string) []*Task {
	var result []*Task
	for _, task := range d.tasks {
		if task.taskType == AddToGroup && task.model.(*ManageMembershipModel).ClusterIdentifier == clusterIdentifier && task.model.(*ManageMembershipModel).GroupName == groupName {
			result = append(result, task)
		}
	}
	return result
}

func (d *DagBuilder) lookupRemoveFromGroupTasks(clusterIdentifier string, groupName string) []*Task {
	var result []*Task
	for _, task := range d.tasks {
		if task.taskType == RemoveFromGroup && task.model.(*ManageMembershipModel).ClusterIdentifier == clusterIdentifier && task.model.(*ManageMembershipModel).GroupName == groupName {
			result = append(result, task)
		}
	}
	return result
}

func (d *DagBuilder) lookupRemoveFromGroupTask(clusterIdentifier string, username string, groupName string) *Task {
	for _, task := range d.tasks {
		if task.taskType == RemoveFromGroup && task.model.(*ManageMembershipModel).ClusterIdentifier == clusterIdentifier && task.model.(*ManageMembershipModel).Username == username &&
			task.model.(*ManageMembershipModel).GroupName == groupName {
			return task
		}
	}
	return nil
}

