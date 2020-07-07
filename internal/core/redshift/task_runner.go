package redshift

import "fmt"

type TaskRunner interface {
	CreateUser(model *UserModel) error
	DropUser(model *UserModel) error
	CreateGroup(model *GroupModel) error
	DropGroup(model *GroupModel) error
	CreateSchema(model *SchemaModel) error
	CreateExternalSchema(model *ExternalSchemaModel) error
	CreateDatabase(model *DatabaseModel) error
	GrantAccess(model *ManageAccessModel) error
	RevokeAccess(model *ManageAccessModel) error
	AddToGroup(model *ManageMembershipModel) error
	RemoveFromGroup(model *ManageMembershipModel) error
}

func ExecuteTask(taskRunner TaskRunner, task *Task) error {
	switch task.taskType {
	case CreateUser:
		return taskRunner.CreateUser(task.model.(*UserModel))
	case DropUser:
		return taskRunner.DropUser(task.model.(*UserModel))
	case CreateGroup:
		return taskRunner.CreateGroup(task.model.(*GroupModel))
	case DropGroup:
		return taskRunner.DropGroup(task.model.(*GroupModel))
	case CreateDatabase:
		return taskRunner.CreateDatabase(task.model.(*DatabaseModel))
	case CreateSchema:
		return taskRunner.CreateSchema(task.model.(*SchemaModel))
	case CreateExternalSchema:
		return taskRunner.CreateExternalSchema(task.model.(*ExternalSchemaModel))
	case GrantAccess:
		return taskRunner.GrantAccess(task.model.(*ManageAccessModel))
	case RevokeAccess:
		return taskRunner.RevokeAccess(task.model.(*ManageAccessModel))
	case AddToGroup:
		return taskRunner.AddToGroup(task.model.(*ManageMembershipModel))
	case RemoveFromGroup:
		return taskRunner.RemoveFromGroup(task.model.(*ManageMembershipModel))
	default:
		return fmt.Errorf("unexpected task type: %d", int(task.taskType))
	}
	return nil
}

type TaskPrinter struct {

}

func (t *TaskPrinter) CreateUser(model *UserModel) error {
	fmt.Printf("CreateUser (%s) %s\n", model.ClusterIdentifier, model.User.Name)
	return nil
}
func (t *TaskPrinter) DropUser(model *UserModel) error {
	fmt.Printf("DropUser (%s) %s\n", model.ClusterIdentifier,model.User.Name)
	return nil
}
func (t *TaskPrinter) CreateGroup(model *GroupModel) error {
	fmt.Printf("CreateGroup (%s) %s\n", model.ClusterIdentifier,model.Group.Name)
	return nil
}
func (t *TaskPrinter) DropGroup(model *GroupModel) error {
	fmt.Printf("DropGroup (%s) %s\n", model.ClusterIdentifier,model.Group.Name)
	return nil
}
func (t *TaskPrinter) CreateSchema(model *SchemaModel) error {
	fmt.Printf("CreateSchema (%s.%s) %s\n", model.Database.ClusterIdentifier, model.Database.Name, model.Schema.Name)
	return nil
}
func (t *TaskPrinter) CreateExternalSchema(model *ExternalSchemaModel) error {
	fmt.Printf("CreateExternalSchema (%s.%s) %s\n", model.Database.ClusterIdentifier, model.Database.Name, model.Schema.Name)
	return nil
}
func (t *TaskPrinter) CreateDatabase(model *DatabaseModel) error {
	fmt.Printf("CreateDatabase %s.%s\n", model.ClusterIdentifier, model.Database.Name)
	return nil
}
func (t *TaskPrinter) GrantAccess(model *ManageAccessModel) error {
	fmt.Printf("GrantAccess (%s.%s) %s->%s\n", model.Database.ClusterIdentifier, model.Database.Name, model.GroupName, model.SchemaName)
	return nil
}
func (t *TaskPrinter) RevokeAccess(model *ManageAccessModel) error {
	fmt.Printf("RevokeAccess (%s.%s) %s->%s\n", model.Database.ClusterIdentifier, model.Database.Name, model.GroupName, model.SchemaName)
	return nil
}
func (t *TaskPrinter) AddToGroup(model *ManageMembershipModel) error {
	fmt.Printf("AddToGroup (%s) %s->%s\n", model.ClusterIdentifier, model.Username, model.GroupName)
	return nil
}
func (t *TaskPrinter) RemoveFromGroup(model *ManageMembershipModel) error {
	fmt.Printf("RemoveFromGroup (%s) %s->%s\n", model.ClusterIdentifier, model.Username, model.GroupName)
	return nil
}

