package redshift

import (
	"fmt"
	"github.com/go-logr/logr"
	"github.com/lib/pq"
	"github.com/lunarway/hubble-rbac-controller/internal/core/redshift"
	"github.com/prometheus/common/log"
)

type TaskRunnerImpl struct {
	clientPool *ClientPool
	awsAccountId string
	log logr.Logger
}

func NewTaskRunnerImpl(clientPool *ClientPool, awsAccountId string, logger logr.Logger) *TaskRunnerImpl {
	return &TaskRunnerImpl{clientPool:clientPool, awsAccountId: awsAccountId, log: logger}
}

func (t *TaskRunnerImpl) CreateUser(model *redshift.UserModel) error {
	t.log.Info(fmt.Sprintf("CreateUser (%s) %s", model.ClusterIdentifier, model.User.Name))

	client, err := t.clientPool.GetClusterClient(model.ClusterIdentifier)

	if err != nil {
		return err
	}
	err = client.CreateUser(model.User.Name)

	if err != nil {
		return fmt.Errorf("unable to create user %s in %s: %w", model.User.Name, "cluster.Identifier", err)
	}
	return nil
}

func (t *TaskRunnerImpl) DropUser(model *redshift.UserModel) error {
	t.log.Info(fmt.Sprintf("DropUser (%s) %s", model.ClusterIdentifier,model.User.Name))

	client, err := t.clientPool.GetClusterClient(model.ClusterIdentifier)

	if err != nil {
		return err
	}
	err = client.DeleteUser(model.User.Name)

	if err != nil {
		if err.(*pq.Error).Code == objectInUse {
			log.Warnf("unable to delete user %s in cluster %s because it in use. This will happen if the user is a DbtDeveloper role because it owns a database. You'll need to delete manually", model.User.Name, model.ClusterIdentifier)
		} else {
			return fmt.Errorf("unable to delete user %s in %s: %w", model.User.Name, model.ClusterIdentifier, err)
		}
	}
	return nil
}

func (t *TaskRunnerImpl) CreateGroup(model *redshift.GroupModel) error {
	t.log.Info(fmt.Sprintf("CreateGroup (%s) %s", model.ClusterIdentifier,model.Group.Name))

	client, err := t.clientPool.GetClusterClient(model.ClusterIdentifier)

	if err != nil {
		return err
	}

	err = client.CreateGroup(model.Group.Name)

	if err != nil {
		return fmt.Errorf("failed to create group %s in %s: %w", model.Group.Name, model.ClusterIdentifier, err)
	}

	return nil
}

func (t *TaskRunnerImpl) DropGroup(model *redshift.GroupModel) error {
	t.log.Info(fmt.Sprintf("DropGroup (%s) %s", model.ClusterIdentifier,model.Group.Name))

	client, err := t.clientPool.GetClusterClient(model.ClusterIdentifier)

	if err != nil {
		return err
	}
	err = client.DeleteGroup(model.Group.Name)

	if err != nil {
		return fmt.Errorf("unable to delete group %s in %s: %w", model.Group.Name, model.ClusterIdentifier, err)
	}
	return nil
}

func (t *TaskRunnerImpl) CreateSchema(model *redshift.SchemaModel) error {
	t.log.Info(fmt.Sprintf("CreateSchema (%s.%s) %s", model.Database.ClusterIdentifier, model.Database.Name, model.Schema.Name))

	client, err := t.clientPool.GetDatabaseClient(model.Database.ClusterIdentifier, model.Database.Name)

	if err != nil {
		return err
	}

	err = client.CreateSchema(model.Schema.Name)

	if err != nil {
		return fmt.Errorf("failed to create schema %s on database %s: %w", model.Schema.Name, model.Database.Identifier(), err)
	}
	return nil
}

func (t *TaskRunnerImpl) CreateExternalSchema(model *redshift.ExternalSchemaModel) error {
	t.log.Info(fmt.Sprintf("CreateExternalSchema (%s.%s) %s", model.Database.ClusterIdentifier, model.Database.Name, model.Schema.Name))

	client, err := t.clientPool.GetDatabaseClient(model.Database.ClusterIdentifier, model.Database.Name)

	if err != nil {
		return err
	}
	err = client.CreateExternalSchema(model.Schema.Name, model.Schema.GlueDatabaseName, t.awsAccountId)

	if err != nil {
		return fmt.Errorf("failed to create schema %s on database %s: %w", model.Schema.Name, model.Database.Identifier(), err)
	}
	return nil
}

func (t *TaskRunnerImpl) CreateDatabase(model *redshift.DatabaseModel) error {
	t.log.Info(fmt.Sprintf("CreateDatabase %s.%s\n", model.ClusterIdentifier, model.Database.Name))

	client, err := t.clientPool.GetClusterClient(model.ClusterIdentifier)

	if err != nil {
		return err
	}
	err = client.CreateDatabase(model.Database.Name, model.Database.Owner)

	if err != nil {
		return fmt.Errorf("failed to create database %s on cluster %s: %w", model.Database.Name, model.Database.ClusterIdentifier, err)
	}

	//for some reason the owner of the database is not owner of the public schema
	if model.Database.Owner != nil {
		databaseClient, err := t.clientPool.GetDatabaseClient(model.Database.ClusterIdentifier, model.Database.Name)

		if err != nil {
			return err
		}
		return databaseClient.SetSchemaOwner(*model.Database.Owner, "public")
	}
	return nil
}

func (t *TaskRunnerImpl) GrantAccess(model *redshift.GrantsModel) error {
	t.log.Info(fmt.Sprintf("GrantAccess (%s.%s) %s->%s", model.Database.ClusterIdentifier, model.Database.Name, model.GroupName, model.SchemaName))

	client, err := t.clientPool.GetDatabaseClient(model.Database.ClusterIdentifier, model.Database.Name)

	if err != nil {
		return err
	}
	err = client.Grant(model.GroupName, model.SchemaName)

	if err != nil {
		return fmt.Errorf("failed to grant acccess to schema %s for group %s on database %s: %w", model.SchemaName, model.GroupName, model.Database.Identifier(), err)
	}
	return nil
}

func (t *TaskRunnerImpl) RevokeAccess(model *redshift.GrantsModel) error {
	t.log.Info(fmt.Sprintf("RevokeAccess (%s.%s) %s->%s", model.Database.ClusterIdentifier, model.Database.Name, model.GroupName, model.SchemaName))

	client, err := t.clientPool.GetDatabaseClient(model.Database.ClusterIdentifier, model.Database.Name)

	if err != nil {
		return err
	}
	err = client.Revoke(model.GroupName, model.SchemaName)

	if err != nil {
		return fmt.Errorf("unable to revoke grants for group %s in cluster %s %w", model.GroupName, model.Database.ClusterIdentifier, err)
	}
	return nil
}

func (t *TaskRunnerImpl) AddToGroup(model *redshift.MembershipModel) error {
	t.log.Info(fmt.Sprintf("AddToGroup (%s) %s->%s", model.ClusterIdentifier, model.Username, model.GroupName))

	client, err := t.clientPool.GetClusterClient(model.ClusterIdentifier)

	if err != nil {
		return err
	}
	err = client.AddUserToGroup(model.Username, model.GroupName)

	if err != nil {
		return fmt.Errorf("unable to add user %s to group %s in %s: %w", model.Username, model.GroupName, model.ClusterIdentifier, err)
	}
	return nil
}

func (t *TaskRunnerImpl) RemoveFromGroup(model *redshift.MembershipModel) error {
	t.log.Info(fmt.Sprintf("RemoveFromGroup (%s) %s->%s", model.ClusterIdentifier, model.Username, model.GroupName))

	client, err := t.clientPool.GetClusterClient(model.ClusterIdentifier)

	if err != nil {
		return err
	}
	err = client.RemoveUserFromGroup(model.Username, model.GroupName)

	if err != nil {
		return fmt.Errorf("unable to remove user %s from group %s in %s: %w", model.Username, model.GroupName, model.ClusterIdentifier, err)
	}
	return nil
}

