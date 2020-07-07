package redshift

import (
	"fmt"
)

type ManageAccessModel struct {
	schemaName string
	groupName string
}

type ManageMembershipModel struct {
	username string
	groupName string
}

type DagBuilder struct {
	tasks []*Task
}

type Dag struct {
	tasks []*Task
}

func NewDagBuilder() *DagBuilder {
	return &DagBuilder{}
}

func (d *DagBuilder) createTask(identifier string, taskType TaskType, model interface{}) *Task {
	task := NewTask(identifier, taskType, model)
	d.tasks = append(d.tasks, task)
	return task
}

func (d *DagBuilder) createUserTask(model *User) *Task {
	existing := d.lookupCreateUserTask(model.Name)

	if existing != nil {
		return existing
	}

	return d.createTask(model.Name, CreateUser, model)
}

func (d *DagBuilder) dropUserTask(model *User) *Task {
	existing := d.lookupDropUserTask(model.Name)

	if existing != nil {
		return existing
	}

	return d.createTask(model.Name, DropUser, model)
}

func (d *DagBuilder) createGroupTask(model *Group) *Task {
	existing := d.lookupCreateGroupTask(model.Name)

	if existing != nil {
		return existing
	}

	return d.createTask(model.Name, CreateGroup, model)
}

func (d *DagBuilder) dropGroupTask(model *Group) *Task {
	existing := d.lookupDropGroupTask(model.Name)

	if existing != nil {
		return existing
	}

	return d.createTask(model.Name, DropGroup, model)
}

func (d *DagBuilder) createSchemaTask(model *Schema) *Task {
	existing := d.lookupCreateSchemaTask(model.Name)

	if existing != nil {
		return existing
	}

	return d.createTask(model.Name, CreateSchema, model)
}

func (d *DagBuilder) createExternalSchemaTask(model *ExternalSchema) *Task {
	existing := d.lookupCreateExternalSchemaTask(model.Name)

	if existing != nil {
		return existing
	}

	return d.createTask(model.Name, CreateExternalSchema, model)
}

func (d *DagBuilder) createDatabaseTask(model *Database) *Task {
	existing := d.lookupCreateDatabaseTask(model.Name)

	if existing != nil {
		return existing
	}

	return d.createTask(model.Name, CreateDatabase, model)
}

func (d *DagBuilder) grantAccessTask(model *ManageAccessModel) *Task {
	existing := d.lookupGrantAccessTask(model.schemaName, model.groupName)

	if existing != nil {
		return existing
	}

	return d.createTask(fmt.Sprintf("%s->%s", model.groupName, model.schemaName), GrantAccess, model)
}

func (d *DagBuilder) revokeAccessTask(model *ManageAccessModel) *Task {
	existing := d.lookupRevokeAccessTask(model.schemaName, model.groupName)

	if existing != nil {
		return existing
	}

	return d.createTask(fmt.Sprintf("%s->%s", model.groupName, model.schemaName), RevokeAccess, model)
}

func (d *DagBuilder) addToGroupTask(model *User, group *Group) *Task {
	existing := d.lookupAddToGroupTask(model.Name, group.Name)

	if existing != nil {
		return existing
	}

	return d.createTask(fmt.Sprintf("%s->%s", model.Name, group.Name), AddToGroup, &ManageMembershipModel{username:model.Name, groupName:model.Role().Name})
}

func (d *DagBuilder) removeFromGroupTask(model *User, group *Group) *Task {
	existing := d.lookupRemoveFromGroupTask(model.Name, group.Name)

	if existing != nil {
		return existing
	}

	return d.createTask(fmt.Sprintf("%s->%s", model.Name, group.Name), RemoveFromGroup, &ManageMembershipModel{username:model.Name, groupName:model.Role().Name})
}

func (d *DagBuilder) lookupCreateUserTask(username string) *Task {
	for _, task := range d.tasks {
		if task.taskType == CreateUser && task.model.(*User).Name == username {
			return task
		}
	}
	return nil
}

func (d *DagBuilder) lookupDropUserTask(username string) *Task {
	for _, task := range d.tasks {
		if task.taskType == DropUser && task.model.(*User).Name == username {
			return task
		}
	}
	return nil
}

func (d *DagBuilder) lookupCreateGroupTask(name string) *Task {
	for _, task := range d.tasks {
		if task.taskType == CreateGroup && task.model.(*Group).Name == name {
			return task
		}
	}
	return nil
}

func (d *DagBuilder) lookupDropGroupTask(name string) *Task {
	for _, task := range d.tasks {
		if task.taskType == DropGroup && task.model.(*Group).Name == name {
			return task
		}
	}
	return nil
}

func (d *DagBuilder) lookupCreateSchemaTask(name string) *Task {
	for _, task := range d.tasks {
		if task.taskType == CreateSchema && task.model.(*Schema).Name == name {
			return task
		}
	}
	return nil
}

func (d *DagBuilder) lookupCreateExternalSchemaTask(name string) *Task {
	for _, task := range d.tasks {
		if task.taskType == CreateExternalSchema && task.model.(*ExternalSchema).Name == name {
			return task
		}
	}
	return nil
}

func (d *DagBuilder) lookupCreateDatabaseTask(name string) *Task {
	for _, task := range d.tasks {
		if task.taskType == CreateDatabase && task.model.(*Database).Name == name {
			return task
		}
	}
	return nil
}

func (d *DagBuilder) lookupGrantAccessTask(schemaName string, groupName string) *Task {
	for _, task := range d.tasks {
		if task.taskType == GrantAccess && task.model.(*ManageAccessModel).schemaName == schemaName &&
			task.model.(*ManageAccessModel).groupName == groupName {
			return task
		}
	}
	return nil
}

func (d *DagBuilder) lookupRevokeAccessTask(schemaName string, groupName string) *Task {
	for _, task := range d.tasks {
		if task.taskType == RevokeAccess && task.model.(*ManageAccessModel).schemaName == schemaName &&
			task.model.(*ManageAccessModel).groupName == groupName {
			return task
		}
	}
	return nil
}

func (d *DagBuilder) lookupAddToGroupTask(username string, groupName string) *Task {
	for _, task := range d.tasks {
		if task.taskType == AddToGroup && task.model.(*ManageMembershipModel).username == username &&
			task.model.(*ManageMembershipModel).groupName  == groupName {
			return task
		}
	}
	return nil
}

func (d *DagBuilder) lookupAddToGroupTasks(groupName string) []*Task {
	var result []*Task
	for _, task := range d.tasks {
		if task.taskType == AddToGroup && task.model.(*ManageMembershipModel).groupName == groupName {
			result = append(result, task)
		}
	}
	return result
}

func (d *DagBuilder) lookupRemoveFromGroupTasks(groupName string) []*Task {
	var result []*Task
	for _, task := range d.tasks {
		if task.taskType == RemoveFromGroup && task.model.(*ManageMembershipModel).groupName == groupName {
			result = append(result, task)
		}
	}
	return result
}

func (d *DagBuilder) lookupRemoveFromGroupTask(username string, groupName string) *Task {
	for _, task := range d.tasks {
		if task.taskType == RemoveFromGroup && task.model.(*ManageMembershipModel).username == username &&
			task.model.(*ManageMembershipModel).groupName == groupName {
			return task
		}
	}
	return nil
}

func (d *Dag) String() string {
	var result string

	for _, t := range d.tasks {
		result += fmt.Sprintf("%s\n", t.String())
	}

	return result
}