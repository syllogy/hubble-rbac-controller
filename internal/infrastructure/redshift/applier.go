package redshift

import (
	"fmt"
	redshiftCore "github.com/lunarway/hubble-rbac-controller/internal/core/redshift"
)

type ApplyEventType int

const (
	EnsureUserExists ApplyEventType = iota
	EnsureUserDeleted
	EnsureUserIsInGroup
	EnsureUserIsNotInGroup
	EnsureGroupExists
	EnsureGroupDeleted
	EnsureSchemaExists
	EnsureAccessIsGrantedToSchema
	EnsureAccessHasBeenRevokedFromSchema
	EnsureDatabaseExists
)

func (t ApplyEventType) ToString() string {
	switch t {
	case EnsureUserExists:
		return "EnsureUserExists"
	case EnsureUserDeleted:
		return "EnsureUserDeleted"
	case EnsureUserIsInGroup:
		return "EnsureUserIsInGroup"
	case EnsureUserIsNotInGroup:
		return "EnsureUserIsNotInGroup"
	case EnsureGroupExists:
		return "EnsureGroupExists"
	case EnsureGroupDeleted:
		return "EnsureGroupDeleted"
	case EnsureSchemaExists:
		return "EnsureSchemaExists"
	case EnsureAccessIsGrantedToSchema:
		return "EnsureAccessIsGrantedToSchema"
	case EnsureAccessHasBeenRevokedFromSchema:
		return "EnsureAccessHasBeenRevokedFromSchema"
	case EnsureDatabaseExists:
		return "EnsureDatabaseExists"
	default:
		return fmt.Sprintf("%d", int(t))
	}
}

type ApplyEventLister interface {
	Handle(eventType ApplyEventType, name string)
}

/*
The redshift applier is responsible for reconciling users, groups and access grants for the list of databases
and clusters included in the model provided to the Apply() method.

As part of giving access the applier needs to ensure that the databases, schemas, users and groups referenced in
the grants exist and might therefore create those entities. However, it will never make any destructive actions that
could cause data loss, ie. it will not remove schemas or databases, but it will remove users/groups/grants.

Currently it will not "clean up" users/groups/grants for databases that have transitioned from managed to unmanaged.
This is ok, because if a database becomes unamanaged all access grants would also have been removed from the model,
which means that IAM access to the database will be removed by the IAM applier.
 */

type Applier struct {
	clientGroup     *ClientGroup
	excludedUsers   []string
	excludedSchemas []string
	eventListener   ApplyEventLister
	awsAccountId    string
}

func NewApplier(clientGroup *ClientGroup, excludedUsers []string, excludedSchemas []string, eventListener ApplyEventLister, awsAccountId string) *Applier {
	return &Applier{
		clientGroup:     clientGroup,
		excludedSchemas: excludedSchemas,
		excludedUsers:   excludedUsers,
		eventListener:   eventListener,
		awsAccountId:    awsAccountId,
	}
}

func (applier *Applier) isUserExcluded(username string) bool {
	for _,unmanagedUser := range applier.excludedUsers {
		if unmanagedUser == username {
			return true
		}
	}
	return false
}

func (applier *Applier) isSchemaExcluded(schema string) bool {
	for _, unmanagedSchema := range applier.excludedSchemas {
		if unmanagedSchema == schema {
			return true
		}
	}
	return false
}

func (applier *Applier) applyGroups(database *redshiftCore.Database) error {

	client, err := applier.clientGroup.ForDatabase(database)

	if err != nil {
		return fmt.Errorf("no client for database %s: %w", database.Identifier(), err)
	}

	existingGroups, err := client.Groups()

	if err != nil {
		return fmt.Errorf("unable to list groups for %s: %w", database.Identifier(), err)
	}

	for _, existingGroup := range existingGroups {
		if database.LookupGroup(existingGroup) == nil {
			err = client.DeleteGroup(existingGroup)
			applier.eventListener.Handle(EnsureGroupDeleted, existingGroup)

			if err != nil {
				return fmt.Errorf("unable to delete group %s for %s: %w", existingGroup, database.Identifier(), err)
			}
		}
	}

	for _, managedGroup := range database.Groups {
		err = client.CreateGroup(managedGroup.Name)
		applier.eventListener.Handle(EnsureGroupExists, managedGroup.Name)

		if err != nil {
			return fmt.Errorf("failed to create group %s in %s: %w", managedGroup.Name, database.Identifier(), err)
		}

		existingGrantedSchemas, err := client.Grants(managedGroup.Name)

		if err != nil {
			return fmt.Errorf("unable to list grants for group %s in %s: %w", managedGroup.Name, database.Identifier(), err)
		}

		for _, existingGrantedSchema := range existingGrantedSchemas {
			if managedGroup.LookupGrantedSchema(existingGrantedSchema) ==  nil &&
				managedGroup.LookupGrantedExternalSchema(existingGrantedSchema) == nil &&
				!applier.isSchemaExcluded(existingGrantedSchema) {
				err = client.Revoke(managedGroup.Name, existingGrantedSchema)
				applier.eventListener.Handle(EnsureAccessHasBeenRevokedFromSchema, fmt.Sprintf("%s->%s", managedGroup.Name, existingGrantedSchema))

				if err != nil {
					return fmt.Errorf("failed to revoke access to schema %s for group %s in %s: %w", existingGrantedSchema, managedGroup.Name, database.Identifier(), err)
				}
			}
		}

		for _, managedSchema := range managedGroup.GrantedSchemas {

			if applier.isSchemaExcluded(managedSchema.Name) {
				return fmt.Errorf("schema %s has been explicitly excluded and cannot be managed", managedSchema.Name)
			}

			err = client.CreateSchema(managedSchema.Name)
			applier.eventListener.Handle(EnsureSchemaExists, managedSchema.Name)

			if err != nil {
				return fmt.Errorf("failed to create schema %s on database %s: %w", managedGroup.Name, database.Identifier(), err)
			}

			err = client.Grant(managedGroup.Name, managedSchema.Name)
			applier.eventListener.Handle(EnsureAccessIsGrantedToSchema, fmt.Sprintf("%s->%s", managedGroup.Name, managedSchema.Name))

			if err != nil {
				return fmt.Errorf("failed to grant acccess to schema %s for group %s on database %s: %w", managedSchema.Name, managedGroup.Name, database.Identifier(), err)
			}
		}

		for _, managedSchema := range managedGroup.GrantedExternalSchemas {

			if applier.isSchemaExcluded(managedSchema.Name) {
				return fmt.Errorf("schema %s has been explicitly excluded and cannot be managed", managedSchema.Name)
			}

			err = client.CreateExternalSchema(managedSchema.Name, managedSchema.GlueDatabaseName, applier.awsAccountId)
			applier.eventListener.Handle(EnsureSchemaExists, managedSchema.Name)

			if err != nil {
				return fmt.Errorf("failed to create schema %s on database %s: %w", managedGroup.Name, database.Identifier(), err)
			}

			err = client.Grant(managedGroup.Name, managedSchema.Name)
			applier.eventListener.Handle(EnsureAccessIsGrantedToSchema, fmt.Sprintf("%s->%s", managedGroup.Name, managedSchema.Name))

			if err != nil {
				return fmt.Errorf("failed to grant acccess to schema %s for group %s on database %s: %w", managedSchema.Name, managedGroup.Name, database.Identifier(), err)
			}
		}
	}

	return nil
}

func (applier *Applier) applyUsers(database *redshiftCore.Database) error {

	client, err := applier.clientGroup.ForDatabase(database)

	if err != nil {
		return fmt.Errorf("no client for database %s: %w", database.Identifier(), err)
	}

	for _, managedUser := range database.Users {

		if applier.isUserExcluded(managedUser.Name) {
			return fmt.Errorf("user %s has been explicitly excluded and cannot be managed", managedUser.Name)
		}

		err = client.CreateUser(managedUser.Name)
		applier.eventListener.Handle(EnsureUserExists, managedUser.Name)

		if err != nil {
			return fmt.Errorf("unable to create user %s in %s: %w", managedUser.Name, database.Identifier(), err)
		}

		alreadyPartOf, err := client.PartOf(managedUser.Name)

		if err != nil {
			return fmt.Errorf("unable to list group membership for user %s in database %s: %w", managedUser.Name, database.Identifier(), err)
		}

		for _,groupName := range alreadyPartOf {
			if managedUser.MemberOf.Name != groupName {
				err = client.RemoveUserFromGroup(managedUser.Name, groupName)
				applier.eventListener.Handle(EnsureUserIsNotInGroup, fmt.Sprintf("%s->%s", managedUser.Name, groupName))

				if err != nil {
					return fmt.Errorf("unable to remove user %s from group %s in %s: %w", managedUser.Name, groupName, database.Identifier(), err)
				}
			}
		}

		err = client.AddUserToGroup(managedUser.Name, managedUser.MemberOf.Name)
		applier.eventListener.Handle(EnsureUserIsInGroup, fmt.Sprintf("%s->%s", managedUser.Name, managedUser.MemberOf.Name))

		if err != nil {
			return fmt.Errorf("unable to add user %s to group %s in %s: %w", managedUser.Name, managedUser.MemberOf.Name, database.Identifier(), err)
		}
	}

	return nil
}

func (applier *Applier) applyDatabase(database *redshiftCore.Database) error {

	client, err := applier.clientGroup.MasterDatabase(database.ClusterIdentifier)

	if err != nil {
		return err
	}

	applier.eventListener.Handle(EnsureDatabaseExists, database.Name)
	err = client.CreateDatabase(database.Name, database.Owner)

	if err != nil {
		return err
	}

	client, err = applier.clientGroup.ForDatabase(database)

	if err != nil {
		return err
	}

	//for some reason the owner of the database is not owner of the public schema
	if database.Owner != nil {
		return client.SetSchemaOwner(*database.Owner, "public")
	} else {
		return err
	}
	return err
}

func (applier *Applier) deleteUnmanagedUsers(model redshiftCore.Model, clusterIdentifier string) error {

	client, err := applier.clientGroup.MasterDatabase(clusterIdentifier)

	if err != nil {
		return fmt.Errorf("no client for cluster %s: %w", clusterIdentifier, err)
	}

	usernames, err := client.Users()

	if err != nil {
		return fmt.Errorf("unable to list usernames for cluster %s: %w", clusterIdentifier, err)
	}

	for _, username := range usernames {
		if !applier.isUserExcluded(username) && !model.LookupUser(clusterIdentifier, username) {
			err = client.DeleteUser(username)
			applier.eventListener.Handle(EnsureUserDeleted, username)

			if err != nil {
				return fmt.Errorf("unable to delete user %s in %s: %w", username, clusterIdentifier, err)
			}
		}
	}
	return nil
}

func (applier *Applier) Apply(model redshiftCore.Model) error {

	for _, clusterIdentifier := range model.ClusterIdentifiers() {
		err := applier.deleteUnmanagedUsers(model, clusterIdentifier)
		if err != nil {
			return err
		}
	}

	for _, database := range model.Databases {

		err := applier.applyDatabase(database)

		if err != nil {
			return err
		}

		err = applier.applyGroups(database)

		if err != nil {
			return err
		}

		err = applier.applyUsers(database)

		if err != nil {
			return err
		}
	}

	return nil
}
