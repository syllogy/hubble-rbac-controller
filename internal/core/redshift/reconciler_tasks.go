package redshift

import (
	"fmt"
)

func (d *Reconciler) lookupTask(task *Task) *Task {
	for _, t := range d.tasks {
		if task.taskType == t.taskType && task.model.Equals(t.model) {
			return t
		}
	}
	return nil
}

func (d *Reconciler) add(task *Task) *Task {
	existing := d.lookupTask(task)

	if existing == nil {
		d.tasks = append(d.tasks, task)
		return task
	}
	return existing
}

func (d *Reconciler) createUserTask(clusterIdentifier string, model *User) *Task {

	task := NewTask(model.Name, CreateUser, &UserModel{ClusterIdentifier: clusterIdentifier, User: model})
	return d.add(task)
}

func (d *Reconciler) dropUserTask(clusterIdentifier string, model *User) *Task {

	task := NewTask(model.Name, DropUser, &UserModel{ClusterIdentifier: clusterIdentifier, User: model})
	return d.add(task)
}

func (d *Reconciler) createGroupTask(clusterIdentifier string, model *Group) *Task {

	task := NewTask(model.Name, CreateGroup, &GroupModel{
		Group:             model,
		ClusterIdentifier: clusterIdentifier,
	})
	return d.add(task)
}

func (d *Reconciler) dropGroupTask(clusterIdentifier string, model *Group) *Task {

	task := NewTask(model.Name, DropGroup, &GroupModel{
		Group:             model,
		ClusterIdentifier: clusterIdentifier,
	})
	return d.add(task)
}

func (d *Reconciler) createSchemaTask(database *Database, model *Schema) *Task {

	task := NewTask(model.Name, CreateSchema, &SchemaModel{
		Schema:model,
		Database:database,
	})
	return d.add(task)
}

func (d *Reconciler) createExternalSchemaTask(database *Database, model *ExternalSchema) *Task {

	task := NewTask(model.Name, CreateExternalSchema, &ExternalSchemaModel{
		Schema:model,
		Database:database,
	})
	return d.add(task)
}

func (d *Reconciler) createDatabaseTask(clusterIdentifier string, model *Database) *Task {

	task := NewTask(model.Name, CreateDatabase, &DatabaseModel{
		Database:model,
		ClusterIdentifier:clusterIdentifier,
	})
	return d.add(task)
}

func (d *Reconciler) grantAccessTask(database *Database, schemaName string, groupName string) *Task {

	task := NewTask(fmt.Sprintf("%s->%s", groupName, schemaName), GrantAccess, &GrantsModel{
		GroupName:  groupName,
		SchemaName: schemaName,
		Database:   database,
	})
	return d.add(task)
}

func (d *Reconciler) revokeAccessTask(database *Database, schemaName string, groupName string) *Task {

	task := NewTask(fmt.Sprintf("%s->%s", groupName, schemaName), RevokeAccess, &GrantsModel{
		GroupName:  groupName,
		SchemaName: schemaName,
		Database:   database,
	})
	return d.add(task)
}

func (d *Reconciler) addToGroupTask(clusterIdentifier string, model *User, group *Group) *Task {

	task := NewTask(fmt.Sprintf("%s->%s", model.Name, group.Name), AddToGroup, &MembershipModel{
		ClusterIdentifier:clusterIdentifier,
		Username: model.Name,
		GroupName:model.Role().Name,
	})
	return d.add(task)
}

func (d *Reconciler) removeFromGroupTask(clusterIdentifier string, model *User, group *Group) *Task {

	task := NewTask(fmt.Sprintf("%s->%s", model.Name, group.Name), RemoveFromGroup, &MembershipModel{
		ClusterIdentifier:clusterIdentifier,
		Username: model.Name,
		GroupName:model.Role().Name,
	})
	return d.add(task)
}

func (d *Reconciler) lookupAddToGroupTasks(clusterIdentifier string, groupName string) []*Task {
	var result []*Task
	for _, task := range d.tasks {
		if task.taskType == AddToGroup &&
			task.model.(*MembershipModel).ClusterIdentifier == clusterIdentifier &&
			task.model.(*MembershipModel).GroupName == groupName {
			result = append(result, task)
		}
	}
	return result
}

func (d *Reconciler) lookupRemoveFromGroupTasks(clusterIdentifier string, groupName string) []*Task {
	var result []*Task
	for _, task := range d.tasks {
		if task.taskType == RemoveFromGroup &&
			task.model.(*MembershipModel).ClusterIdentifier == clusterIdentifier &&
			task.model.(*MembershipModel).GroupName == groupName {
			result = append(result, task)
		}
	}
	return result
}

func (d *Reconciler) lookupCreateGroupTask(clusterIdentifier string, name string) *Task {
	for _, task := range d.tasks {
		if task.taskType == CreateGroup &&
			task.model.(*GroupModel).ClusterIdentifier == clusterIdentifier &&
			task.model.(*GroupModel).Group.Name == name {
			return task
		}
	}
	return nil
}

func (d *Reconciler) lookupDropGroupTask(clusterIdentifier string, name string) *Task {
	for _, task := range d.tasks {
		if task.taskType == DropGroup &&
			task.model.(*GroupModel).ClusterIdentifier == clusterIdentifier &&
			task.model.(*GroupModel).Group.Name == name {
			return task
		}
	}
	return nil
}

func (d *Reconciler) lookupCreateDatabaseTask(clusterIdentifier string, name string) *Task {
	for _, task := range d.tasks {
		if task.taskType == CreateDatabase &&
			task.model.(*DatabaseModel).Database.ClusterIdentifier == clusterIdentifier &&
			task.model.(*DatabaseModel).Database.Name == name {
			return task
		}
	}
	return nil
}
