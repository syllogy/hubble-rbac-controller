package redshift

import (
	"fmt"
)


func newCreateUserTask(clusterIdentifier string, model *User) *Task {
	return NewTask(model.Name, CreateUser, &UserModel{ClusterIdentifier: clusterIdentifier, User: model})
}

func newDropUserTask(clusterIdentifier string, model *User) *Task {
	return NewTask(model.Name, DropUser, &UserModel{ClusterIdentifier: clusterIdentifier, User: model})
}

func newCreateGroupTask(clusterIdentifier string, model *Group) *Task {
	return NewTask(model.Name, CreateGroup, &GroupModel{
		Group:             model,
		ClusterIdentifier: clusterIdentifier,
	})
}

func newDropGroupTask(clusterIdentifier string, model *Group) *Task {
	return NewTask(model.Name, DropGroup, &GroupModel{
		Group:             model,
		ClusterIdentifier: clusterIdentifier,
	})
}

func newCreateSchemaTask(database *Database, model *Schema) *Task {
	return NewTask(model.Name, CreateSchema, &SchemaModel{
		Schema:model,
		Database:database,
	})
}

func newCreateExternalSchemaTask(database *Database, model *ExternalSchema) *Task {
	return NewTask(model.Name, CreateExternalSchema, &ExternalSchemaModel{
		Schema:model,
		Database:database,
	})
}

func newCreateDatabaseTask(clusterIdentifier string, model *Database) *Task {
	return NewTask(model.Name, CreateDatabase, &DatabaseModel{
		Database:model,
		ClusterIdentifier:clusterIdentifier,
	})
}

func newGrantAccessTask(database *Database, schemaName string, groupName string) *Task {
	return NewTask(fmt.Sprintf("%s->%s", groupName, schemaName), GrantAccess, &GrantsModel{
		GroupName:  groupName,
		SchemaName: schemaName,
		Database:   database,
	})
}

func newRevokeAccessTask(database *Database, schemaName string, groupName string) *Task {
	return NewTask(fmt.Sprintf("%s->%s", groupName, schemaName), RevokeAccess, &GrantsModel{
		GroupName:  groupName,
		SchemaName: schemaName,
		Database:   database,
	})
}

func newAddToGroupTask(clusterIdentifier string, model *User, group *Group) *Task {
	return NewTask(fmt.Sprintf("%s->%s", model.Name, group.Name), AddToGroup, &MembershipModel{
		ClusterIdentifier:clusterIdentifier,
		Username: model.Name,
		GroupName:model.Role().Name,
	})
}

func newRemoveFromGroupTask(clusterIdentifier string, model *User, group *Group) *Task {
	return NewTask(fmt.Sprintf("%s->%s", model.Name, group.Name), RemoveFromGroup, &MembershipModel{
		ClusterIdentifier:clusterIdentifier,
		Username: model.Name,
		GroupName:model.Role().Name,
	})
}
