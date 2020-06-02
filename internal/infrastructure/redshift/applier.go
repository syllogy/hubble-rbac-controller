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

type Applier struct {
	clientGroup *ClientGroup
	unmanagedUsers []string
	unmanagedSchemas []string
	eventListener ApplyEventLister
	awsAccountId string
}

func NewApplier(clientGroup *ClientGroup, unmanagedUsers []string, unmanagedSchemas []string, eventListener ApplyEventLister, awsAccountId string) *Applier {
	return &Applier{
		clientGroup: clientGroup,
		unmanagedSchemas: unmanagedSchemas,
		unmanagedUsers: unmanagedUsers,
		eventListener: eventListener,
		awsAccountId: awsAccountId,
	}
}

func (applier *Applier) isUserManaged(username string) bool {
	for _,unmanagedUser := range applier.unmanagedUsers {
		if unmanagedUser == username {
			return false
		}
	}
	return true
}

func (applier *Applier) isSchemaManaged(schema string) bool {
	for _, unmanagedSchema := range applier.unmanagedSchemas {
		if unmanagedSchema == schema {
			return false
		}
	}
	return true
}

func (applier *Applier) applyGroups(database *redshiftCore.Database) error {

	client, err := applier.clientGroup.ForDatabase(database)

	if err != nil {
		return fmt.Errorf("No client for database %s: %w", database.Identifier(), err)
	}

	existingGroups, err := client.Groups()

	if err != nil {
		return fmt.Errorf("Unable to list groups for %s: %w", database.Identifier(), err)
	}

	for _, existingGroup := range existingGroups {
		if database.LookupGroup(existingGroup) == nil {
			err = client.DeleteGroup(existingGroup)
			applier.eventListener.Handle(EnsureGroupDeleted, existingGroup)

			if err != nil {
				return fmt.Errorf("Unable to delete group %s for %s: %w", existingGroup, database.Identifier(), err)
			}
		}
	}

	for _, group := range database.Groups {
		err = client.CreateGroup(group.Name)
		applier.eventListener.Handle(EnsureGroupExists, group.Name)

		if err != nil {
			return fmt.Errorf("Failed to create group %s in %s: %w", group.Name, database.Identifier(), err)
		}

		existingGrantedSchemas, err := client.Grants(group.Name)

		if err != nil {
			return fmt.Errorf("Unable to list grants for group %s in %s: %w", group.Name, database.Identifier(), err)
		}

		for _, existingGrantedSchema := range existingGrantedSchemas {
			if group.LookupGrantedSchema(existingGrantedSchema) ==  nil &&
				group.LookupGrantedExternalSchema(existingGrantedSchema) == nil &&
				applier.isSchemaManaged(existingGrantedSchema) {
				err = client.Revoke(group.Name, existingGrantedSchema)
				applier.eventListener.Handle(EnsureAccessHasBeenRevokedFromSchema, fmt.Sprintf("%s->%s", group.Name, existingGrantedSchema))

				if err != nil {
					return fmt.Errorf("Failed to revoke access to schema %s for group %s in %s: %w", existingGrantedSchema, group.Name, database.Identifier(), err)
				}
			}
		}

		for _, schema := range group.GrantedSchemas {

			if !applier.isSchemaManaged(schema.Name) {
				return fmt.Errorf("Schema %s cannot be managed", schema.Name)
			}

			err = client.CreateSchema(schema.Name)
			applier.eventListener.Handle(EnsureSchemaExists, schema.Name)

			if err != nil {
				return fmt.Errorf("Failed to create schema %s on database %s: %w", group.Name, database.Identifier(), err)
			}

			err = client.Grant(group.Name, schema.Name)
			applier.eventListener.Handle(EnsureAccessIsGrantedToSchema, fmt.Sprintf("%s->%s", group.Name, schema.Name))

			if err != nil {
				return fmt.Errorf("Failed to grant acccess to schema %s for group %s on database %s: %w", schema.Name, group.Name, database.Identifier(), err)
			}
		}

		for _, schema := range group.GrantedExternalSchemas {

			if !applier.isSchemaManaged(schema.Name) {
				return fmt.Errorf("Schema %s cannot be managed", schema.Name)
			}

			err = client.CreateExternalSchema(schema.Name, schema.GlueDatabaseName, applier.awsAccountId)
			applier.eventListener.Handle(EnsureSchemaExists, schema.Name)

			if err != nil {
				return fmt.Errorf("Failed to create schema %s on database %s: %w", group.Name, database.Identifier(), err)
			}

			err = client.Grant(group.Name, schema.Name)
			applier.eventListener.Handle(EnsureAccessIsGrantedToSchema, fmt.Sprintf("%s->%s", group.Name, schema.Name))

			if err != nil {
				return fmt.Errorf("Failed to grant acccess to schema %s for group %s on database %s: %w", schema.Name, group.Name, database.Identifier(), err)
			}
		}
	}

	return nil
}

func (applier *Applier) applyUsers(database *redshiftCore.Database) error {

	client, err := applier.clientGroup.ForDatabase(database)

	if err != nil {
		return fmt.Errorf("No client for database %s: %w", database.Identifier(), err)
	}

	users, err := client.Users()

	if err != nil {
		return fmt.Errorf("Unable to list users for database %s: %w", database.Identifier(), err)
	}

	for _, username := range users {
		if applier.isUserManaged(username) && database.LookupUser(username) == nil {
			err = client.DeleteUser(username)
			applier.eventListener.Handle(EnsureUserDeleted, username)

			if err != nil {
				return fmt.Errorf("Unable to delete user %s in %s: %w", username, database.Identifier(), err)
			}
		}
	}

	for _,user := range database.Users {

		if !applier.isUserManaged(user.Name) {
			return fmt.Errorf("User %s cannot be managed", user.Name)
		}

		err = client.CreateUser(user.Name)
		applier.eventListener.Handle(EnsureUserExists, user.Name)

		if err != nil {
			return fmt.Errorf("Unable to create user %s in %s: %w", user.Name, database.Identifier(), err)
		}

		alreadyPartOf, err := client.PartOf(user.Name)

		if err != nil {
			return fmt.Errorf("Unable to list group membership for user %s in database %s: %w", user.Name, database.Identifier(), err)
		}

		for _,groupName := range alreadyPartOf {
			if user.Of.Name != groupName {
				err = client.RemoveUserFromGroup(user.Name, groupName)
				applier.eventListener.Handle(EnsureUserIsNotInGroup, fmt.Sprintf("%s->%s", user.Name, groupName))

				if err != nil {
					return fmt.Errorf("Unable to remove user %s from group %s in %s: %w", user.Name, groupName, database.Identifier(), err)
				}
			}
		}

		err = client.AddUserToGroup(user.Name, user.Of.Name)
		applier.eventListener.Handle(EnsureUserIsInGroup, fmt.Sprintf("%s->%s", user.Name, user.Of.Name))

		if err != nil {
			return fmt.Errorf("Unable to add user %s to group %s in %s: %w", user.Name, user.Of.Name, database.Identifier(), err)
		}
	}

	return nil
}

func (applier *Applier) applyDatabase(database *redshiftCore.Database) error {

	client, err := applier.clientGroup.MasterDatabase(database)

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

func (applier *Applier) Apply(model redshiftCore.Model) error {

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
