package redshift

import (
	"fmt"
	"github.com/go-logr/logr"
)

type TaskRunner interface {
	CreateUser(model *UserModel) error
	DropUser(model *UserModel) error
	CreateGroup(model *GroupModel) error
	DropGroup(model *GroupModel) error
	CreateSchema(model *SchemaModel) error
	CreateExternalSchema(model *ExternalSchemaModel) error
	CreateDatabase(model *DatabaseModel) error
	GrantAccess(model *GrantsModel) error
	RevokeAccess(model *GrantsModel) error
	AddToGroup(model *MembershipModel) error
	RemoveFromGroup(model *MembershipModel) error
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
		return taskRunner.GrantAccess(task.model.(*GrantsModel))
	case RevokeAccess:
		return taskRunner.RevokeAccess(task.model.(*GrantsModel))
	case AddToGroup:
		return taskRunner.AddToGroup(task.model.(*MembershipModel))
	case RemoveFromGroup:
		return taskRunner.RemoveFromGroup(task.model.(*MembershipModel))
	default:
		return fmt.Errorf("unexpected task type: %s", task.taskType.String())
	}
}

type TaskPrinter struct {
	logger logr.Logger
}

func NewTaskPrinter(logger logr.Logger) *TaskPrinter {
	return &TaskPrinter{logger:logger}
}

func (t *TaskPrinter) CreateUser(model *UserModel) error {
	t.logger.Info("CreateUser", "clusterIdentifier", model.ClusterIdentifier, "username", model.User.Name)
	return nil
}
func (t *TaskPrinter) DropUser(model *UserModel) error {
	t.logger.Info("DropUser", "clusterIdentifier", model.ClusterIdentifier, "username", model.User.Name)
	return nil
}
func (t *TaskPrinter) CreateGroup(model *GroupModel) error {
	t.logger.Info("CreateGroup", "clusterIdentifier", model.ClusterIdentifier, "groupName", model.Group.Name)
	return nil
}
func (t *TaskPrinter) DropGroup(model *GroupModel) error {
	t.logger.Info("DropGroup", "clusterIdentifier", model.ClusterIdentifier, "groupName", model.Group.Name)
	return nil
}
func (t *TaskPrinter) CreateSchema(model *SchemaModel) error {
	t.logger.Info("CreateSchema", "clusterIdentifier", model.Database.ClusterIdentifier, "databaseName", model.Database.Name, "schemaName", model.Schema.Name)
	return nil
}
func (t *TaskPrinter) CreateExternalSchema(model *ExternalSchemaModel) error {
	t.logger.Info("CreateExternalSchema", "clusterIdentifier", model.Database.ClusterIdentifier, "databaseName", model.Database.Name, "schemaName", model.Schema.Name)
	return nil
}
func (t *TaskPrinter) CreateDatabase(model *DatabaseModel) error {
	t.logger.Info("CreateDatabase", "clusterIdentifier", model.ClusterIdentifier, "databaseName", model.Database.Name)
	return nil
}
func (t *TaskPrinter) GrantAccess(model *GrantsModel) error {
	t.logger.Info("GrantAccess", "clusterIdentifier", model.Database.ClusterIdentifier, "databaseName", model.Database.Name, "groupName", model.GroupName, "schemaName", model.SchemaName)
	return nil
}
func (t *TaskPrinter) RevokeAccess(model *GrantsModel) error {
	t.logger.Info("RevokeAccess", "clusterIdentifier", model.Database.ClusterIdentifier, "databaseName", model.Database.Name, "groupName", model.GroupName, "schemaName", model.SchemaName)
	return nil
}
func (t *TaskPrinter) AddToGroup(model *MembershipModel) error {
	t.logger.Info("AddToGroup", "clusterIdentifier", model.ClusterIdentifier, "username", model.Username, "groupName", model.GroupName)
	return nil
}
func (t *TaskPrinter) RemoveFromGroup(model *MembershipModel) error {
	t.logger.Info("RemoveFromGroup", "clusterIdentifier", model.ClusterIdentifier, "username", model.Username, "groupName", model.GroupName)
	return nil
}

