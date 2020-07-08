package redshift

import (
	"fmt"
)

func (d *ReconciliationDagBuilder) lookupTask(task *Task) *Task {
	for _, t := range d.tasks {
		if task.taskType == t.taskType && task.model.Equals(t.model) {
			return t
		}
	}
	return nil
}

func (d *ReconciliationDagBuilder) add(task *Task) *Task {
	existing := d.lookupTask(task)

	if existing == nil {
		d.tasks = append(d.tasks, task)
		return task
	}
	return existing
}

func (d *ReconciliationDagBuilder) createUserTask(clusterIdentifier string, model *User) *Task {

	task := NewTask(model.Name, CreateUser, &UserModel{ClusterIdentifier: clusterIdentifier, User: model})
	return d.add(task)
}

func (d *ReconciliationDagBuilder) dropUserTask(clusterIdentifier string, model *User) *Task {

	task := NewTask(model.Name, DropUser, &UserModel{ClusterIdentifier: clusterIdentifier, User: model})
	return d.add(task)
}

func (d *ReconciliationDagBuilder) createGroupTask(clusterIdentifier string, model *Group) *Task {

	task := NewTask(model.Name, CreateGroup, &GroupModel{
		Group:             model,
		ClusterIdentifier: clusterIdentifier,
	})
	return d.add(task)
}

func (d *ReconciliationDagBuilder) dropGroupTask(clusterIdentifier string, model *Group) *Task {

	task := NewTask(model.Name, DropGroup, &GroupModel{
		Group:             model,
		ClusterIdentifier: clusterIdentifier,
	})
	return d.add(task)
}

func (d *ReconciliationDagBuilder) createSchemaTask(database *Database, model *Schema) *Task {

	task := NewTask(model.Name, CreateSchema, &SchemaModel{
		Schema:model,
		Database:database,
	})
	return d.add(task)
}

func (d *ReconciliationDagBuilder) createExternalSchemaTask(database *Database, model *ExternalSchema) *Task {

	task := NewTask(model.Name, CreateExternalSchema, &ExternalSchemaModel{
		Schema:model,
		Database:database,
	})
	return d.add(task)
}

func (d *ReconciliationDagBuilder) createDatabaseTask(clusterIdentifier string, model *Database) *Task {

	task := NewTask(model.Name, CreateDatabase, &DatabaseModel{
		Database:model,
		ClusterIdentifier:clusterIdentifier,
	})
	return d.add(task)
}

func (d *ReconciliationDagBuilder) grantAccessTask(database *Database, schemaName string, groupName string) *Task {

	task := NewTask(fmt.Sprintf("%s->%s", groupName, schemaName), GrantAccess, &ManageAccessModel{
		GroupName:  groupName,
		SchemaName: schemaName,
		Database:   database,
	})
	return d.add(task)
}

func (d *ReconciliationDagBuilder) revokeAccessTask(database *Database, schemaName string, groupName string) *Task {

	task := NewTask(fmt.Sprintf("%s->%s", groupName, schemaName), RevokeAccess, &ManageAccessModel{
		GroupName:  groupName,
		SchemaName: schemaName,
		Database:   database,
	})
	return d.add(task)
}

func (d *ReconciliationDagBuilder) addToGroupTask(clusterIdentifier string, model *User, group *Group) *Task {

	task := NewTask(fmt.Sprintf("%s->%s", model.Name, group.Name), AddToGroup, &ManageMembershipModel{
		ClusterIdentifier:clusterIdentifier,
		Username: model.Name,
		GroupName:model.Role().Name,
	})
	return d.add(task)
}

func (d *ReconciliationDagBuilder) removeFromGroupTask(clusterIdentifier string, model *User, group *Group) *Task {

	task := NewTask(fmt.Sprintf("%s->%s", model.Name, group.Name), RemoveFromGroup, &ManageMembershipModel{
		ClusterIdentifier:clusterIdentifier,
		Username: model.Name,
		GroupName:model.Role().Name,
	})
	return d.add(task)
}

func (d *ReconciliationDagBuilder) lookupAddToGroupTasks(clusterIdentifier string, groupName string) []*Task {
	var result []*Task
	for _, task := range d.tasks {
		if task.taskType == AddToGroup &&
			task.model.(*ManageMembershipModel).ClusterIdentifier == clusterIdentifier &&
			task.model.(*ManageMembershipModel).GroupName == groupName {
			result = append(result, task)
		}
	}
	return result
}

func (d *ReconciliationDagBuilder) lookupRemoveFromGroupTasks(clusterIdentifier string, groupName string) []*Task {
	var result []*Task
	for _, task := range d.tasks {
		if task.taskType == RemoveFromGroup &&
			task.model.(*ManageMembershipModel).ClusterIdentifier == clusterIdentifier &&
			task.model.(*ManageMembershipModel).GroupName == groupName {
			result = append(result, task)
		}
	}
	return result
}

func (d *ReconciliationDagBuilder) lookupCreateGroupTask(clusterIdentifier string, name string) *Task {
	for _, task := range d.tasks {
		if task.taskType == CreateGroup &&
			task.model.(*GroupModel).ClusterIdentifier == clusterIdentifier &&
			task.model.(*GroupModel).Group.Name == name {
			return task
		}
	}
	return nil
}

func (d *ReconciliationDagBuilder) lookupDropGroupTask(clusterIdentifier string, name string) *Task {
	for _, task := range d.tasks {
		if task.taskType == DropGroup &&
			task.model.(*GroupModel).ClusterIdentifier == clusterIdentifier &&
			task.model.(*GroupModel).Group.Name == name {
			return task
		}
	}
	return nil
}

func (d *ReconciliationDagBuilder) lookupCreateDatabaseTask(clusterIdentifier string, name string) *Task {
	for _, task := range d.tasks {
		if task.taskType == CreateDatabase &&
			task.model.(*DatabaseModel).Database.ClusterIdentifier == clusterIdentifier &&
			task.model.(*DatabaseModel).Database.Name == name {
			return task
		}
	}
	return nil
}
