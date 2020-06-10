package redshift

import (
	"fmt"
	"github.com/go-logr/logr"
	"github.com/lib/pq"
	redshiftCore "github.com/lunarway/hubble-rbac-controller/internal/core/redshift"
	"github.com/prometheus/common/log"
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

Users, groups and the group memberships are defined on the cluster level in redshift, where as granting access to schemas is done on each individual database.
When users, groups and grants are bound together, the resources cannot be dropped unless the bindings are also dropped, therefore when managing the lifecycle
we need to take care that the resources are created/deleted in the right order.
E.g. one cannot drop a group unless all grants involving the group has been revoked and all members of the group have been removed
To this end, the Apply() method will create/update/delete resources in this order:

add new databases
add new groups
add new users
update groups for each database (also create any missing schemas)
update users
delete users
delete groups

notice again that there are no steps for deleting schemas or databases.

After the call to the Apply() method the clusters should contain exactly the set of databases/users/groups/grants that are in the applied model.
An exception to this rule are the provided set of excluded users. If a user has been excluded it will not be deleted even if it is not in the applied model.
Another exception are the set of databases as they will never be deleted, even if they are not mentioned in the model.

 */

type Applier struct {
	clientGroup     *ClientGroup
	excludedUsers   []string //excluded users will not be deleted, even if they are not mentioned in the applied model
	excludedSchemas []string
	excludedDatabases []string //excluded database will not have their grants managed
	eventListener   ApplyEventLister
	awsAccountId    string
	logger logr.Logger
}

func NewApplier(clientGroup *ClientGroup, excludedDatabases []string, excludedUsers []string, excludedSchemas []string, eventListener ApplyEventLister, awsAccountId string, logger logr.Logger) *Applier {
	return &Applier{
		clientGroup:     clientGroup,
		excludedDatabases: excludedDatabases,
		excludedSchemas: excludedSchemas,
		excludedUsers:   excludedUsers,
		eventListener:   eventListener,
		awsAccountId:    awsAccountId,
		logger: logger,
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

func (applier *Applier) isDatabaseExcluded(name string) bool {
	for _, excludedDatabase := range applier.excludedDatabases {
		if excludedDatabase == name {
			return true
		}
	}
	return false
}

func (applier *Applier) createDatabase(database *redshiftCore.Database) error {

	client, err := applier.clientGroup.MasterDatabase(database.ClusterIdentifier)

	if err != nil {
		return err
	}

	if database.Owner != nil {
		if applier.isUserExcluded(*database.Owner) {
			return fmt.Errorf("user %s has been explicitly excluded and cannot be managed", *database.Owner)
		}

		applier.eventListener.Handle(EnsureUserExists, database.Name)
		err := client.CreateUser(*database.Owner)

		if err != nil {
			return fmt.Errorf("unable to create user %s in %s: %w", *database.Owner, database.ClusterIdentifier, err)
		}
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

func (applier *Applier) applyGrants(database *redshiftCore.Database, managedGroup *redshiftCore.Group) error {

	groupHasAccessToDatabase := database.LookupGroup(managedGroup.Name) != nil

	client, err := applier.clientGroup.ForDatabase(database)

	if err != nil {
		return fmt.Errorf("no client for database %s: %w", database.Identifier(), err)
	}

	existingGrantedSchemas, err := client.Grants(managedGroup.Name)

	if err != nil {
		return fmt.Errorf("unable to list grants for group %s in %s: %w", managedGroup.Name, database.Identifier(), err)
	}

	for _, existingGrantedSchema := range existingGrantedSchemas {
		schemaIsGranted := groupHasAccessToDatabase && (managedGroup.LookupGrantedSchema(existingGrantedSchema) != nil ||
			managedGroup.LookupGrantedExternalSchema(existingGrantedSchema) != nil)

		if !schemaIsGranted && !applier.isSchemaExcluded(existingGrantedSchema) {
			err = client.Revoke(managedGroup.Name, existingGrantedSchema)
			applier.eventListener.Handle(EnsureAccessHasBeenRevokedFromSchema, fmt.Sprintf("%s->%s", managedGroup.Name, existingGrantedSchema))

			if err != nil {
				return fmt.Errorf("failed to revoke access to schema %s for group %s in %s: %w", existingGrantedSchema, managedGroup.Name, database.Identifier(), err)
			}
		}
	}

	if groupHasAccessToDatabase {

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


func (applier *Applier) revokeAllGrants(databaseClient *Client, group string, databaseName string) error {
	existingGrantedSchemas, err := databaseClient.Grants(group)

	if err != nil {
		return fmt.Errorf("unable to list grants for group %s in %s: %w", group, databaseName, err)
	}

	for _, existingGrantedSchema := range existingGrantedSchemas {
		if !applier.isSchemaExcluded(existingGrantedSchema) {
			err = databaseClient.Revoke(group, existingGrantedSchema)
			applier.eventListener.Handle(EnsureAccessHasBeenRevokedFromSchema, fmt.Sprintf("%s->%s", group, existingGrantedSchema))

			if err != nil {
				return fmt.Errorf("failed to revoke access to schema %s for group %s in %s: %w", existingGrantedSchema, group, databaseName, err)
			}
		}
	}
	return nil
}


func (applier *Applier) Apply(model redshiftCore.Model) error {

	err := model.Validate()

	if err != nil {
		return err
	}

	for _, cluster := range model.Clusters {

		//Ensure that all managed databases exist
		applier.logger.Info("Creating databases")
		for _, managedDatabase := range cluster.Databases {
			err := applier.createDatabase(managedDatabase)

			if err != nil {
				return fmt.Errorf("unable to create database %s in cluster %s: %w", managedDatabase.Name, cluster.Identifier, err)
			}
		}

		client, err := applier.clientGroup.MasterDatabase(cluster.Identifier)

		if err != nil {
			return fmt.Errorf("no client for cluster %s: %w", cluster.Identifier, err)
		}

		groups, err := client.Groups()
		if err != nil {
			return fmt.Errorf("unable to list groups for %s: %w", cluster.Identifier, err)
		}

		databases, err := client.Databases()

		if err != nil {
			return fmt.Errorf("unable to list databases for cluster %s: %w", cluster.Identifier, err)
		}

		//for all the databases that are unmanaged we need to make sure that no groups have dangling grants on the schemas in those databases,
		//if any do we won't be able to remove them if they ever become unmanaged
		applier.logger.Info("Revoking dangling grants in unmanaged databases")
		for _, database := range databases {
			if !applier.isDatabaseExcluded(database) && cluster.LookupDatabase(database) == nil {

				applier.logger.Info(fmt.Sprintf("Revoking dangling grants in database %s for %s", database, cluster.Identifier))
				databaseClient, err := applier.clientGroup.Database(cluster.Identifier, database)

				if err != nil {
					return fmt.Errorf("no client for database %s: %w", database, err)
				}

				for _, group := range groups {
					if err = applier.revokeAllGrants(databaseClient, group, database); err != nil {
						return fmt.Errorf("unable to revoke grants for group %s in cluster %s %w", group, cluster.Identifier, err)
					}
				}
			}
		}

		//Ensure that all managed groups exists
		applier.logger.Info("Creating groups")
		for _, managedGroup := range cluster.Groups {
			err = client.CreateGroup(managedGroup.Name)

			if err != nil {
				return fmt.Errorf("failed to create group %s in %s: %w", managedGroup.Name, cluster.Identifier, err)
			}
			applier.eventListener.Handle(EnsureGroupExists, managedGroup.Name)
		}

		//Ensure that all managed users exists
		applier.logger.Info("Creating users")
		for _, managedUser := range cluster.Users {
			if applier.isUserExcluded(managedUser.Name) {
				return fmt.Errorf("user %s has been explicitly excluded and cannot be managed", managedUser.Name)
			}

			err := client.CreateUser(managedUser.Name)

			if err != nil {
				return fmt.Errorf("unable to create user %s in %s: %w", managedUser.Name, cluster.Identifier, err)
			}

			applier.eventListener.Handle(EnsureUserExists, managedUser.Name)
		}

		applier.logger.Info("Updating grants for groups")
		groups, err = client.Groups()
		if err != nil {
			return fmt.Errorf("unable to list groups for %s: %w", cluster.Identifier, err)
		}

		for _, database := range cluster.Databases {

			databaseClient, err := applier.clientGroup.ForDatabase(database)

			if err != nil {
				return fmt.Errorf("no client for database %s: %w", database.Identifier(), err)
			}

			//Ensure that all the desired grants have been set for this group on the database
			for _, managedGroup := range cluster.Groups {
				if err = applier.applyGrants(database, managedGroup); err != nil {
					return fmt.Errorf("unable to update group %s for %s in cluster %s: %w", managedGroup.Name, database.Name, cluster.Identifier, err)
				}
			}

			//If a group has become unmanaged we need to make sure that it doesnt have dangling grants on any schemas,
			//if it does we won't be able to remove it (which happens below)
			for _, group := range groups {
				if cluster.LookupGroup(group) == nil {
					if err = applier.revokeAllGrants(databaseClient, group, database.Name); err != nil {
						return fmt.Errorf("unable to revoke grants for group %s in cluster %s %w", group, cluster.Identifier, err)
					}
				}
			}
		}

		//Ensure that users are members of the desired group
		applier.logger.Info("Updating group memberships")
		for _,managedUser := range cluster.Users {
			alreadyPartOf, err := client.PartOf(managedUser.Name)

			if err != nil {
				return fmt.Errorf("unable to list group membership for user %s in cluster %s: %w", managedUser.Name, cluster.Identifier, err)
			}

			//If the user is already part of some other group we should remove the membership
			for _,groupName := range alreadyPartOf {
				if managedUser.MemberOf.Name != groupName {
					err = client.RemoveUserFromGroup(managedUser.Name, groupName)
					applier.eventListener.Handle(EnsureUserIsNotInGroup, fmt.Sprintf("%s->%s", managedUser.Name, groupName))

					if err != nil {
						return fmt.Errorf("unable to remove user %s from group %s in %s: %w", managedUser.Name, groupName, cluster.Identifier, err)
					}
				}
			}

			err = client.AddUserToGroup(managedUser.Name, managedUser.MemberOf.Name)
			applier.eventListener.Handle(EnsureUserIsInGroup, fmt.Sprintf("%s->%s", managedUser.Name, managedUser.MemberOf.Name))

			if err != nil {
				return fmt.Errorf("unable to add user %s to group %s in %s: %w", managedUser.Name, managedUser.MemberOf.Name, cluster.Identifier, err)
			}
		}

		//We remove all users that are not part of the model from the cluster
		applier.logger.Info("Deleting unmanaged users")
		users, err := client.Users()
		if err != nil {
			return fmt.Errorf("unable to list users for %s: %w", cluster.Identifier, err)
		}

		for _, username := range users {
			if !applier.isUserExcluded(username) && cluster.LookupUser(username) == nil {
				err = client.DeleteUser(username)
				applier.eventListener.Handle(EnsureUserDeleted, username)

				if err != nil {
					if err.(*pq.Error).Code == objectInUse {
						log.Warnf("unable to delete user %s in cluster %s because it in use. This will happen if the user is a DbtDeveloper role because it owns a database. You'll need to delete manually", username, cluster.Identifier)
					} else {
						return fmt.Errorf("unable to delete user %s in %s: %w", username, cluster.Identifier, err)
					}
				}
			}
		}

		//We remove all groups that are not part of the model from the cluster
		applier.logger.Info("Deleting unmanaged groups")
		groups, err = client.Groups()
		if err != nil {
			return fmt.Errorf("unable to list groups for %s: %w", cluster.Identifier, err)
		}

		for _, groupName := range groups {
			if cluster.LookupGroup(groupName) == nil {
				err = client.DeleteGroup(groupName)
				applier.eventListener.Handle(EnsureGroupDeleted, groupName)

				if err != nil {
					return fmt.Errorf("unable to delete group %s in %s: %w", groupName, cluster.Identifier, err)
				}
			}
		}
	}

	return nil
}
