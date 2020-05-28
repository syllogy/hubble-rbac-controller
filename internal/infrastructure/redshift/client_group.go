package redshift

import (
	"errors"
	"fmt"
	"github.com/lunarway/hubble-rbac-controller/internal/core/redshift"
)

type ClusterCredentials struct {
	username string
	password string
	masterDatabase string
	host string
	sslmode string
	port int
	externalSchemasSupported bool
}

type ClientGroup struct {
	credentials map[string]*ClusterCredentials
}

func NewClientGroup(credentials map[string]*ClusterCredentials) *ClientGroup {
	return &ClientGroup{credentials: credentials}
}

func (cg ClientGroup) ForDatabase(database *redshift.Database) (*Client, error) {

	credentials, ok := cg.credentials[database.ClusterIdentifier]

	 if !ok {
		 return nil, errors.New(fmt.Sprintf("Unknown cluster with identifier %s", database.ClusterIdentifier))
	 }

	return NewClient(credentials.username, credentials.password, credentials.host, database.Name, credentials.sslmode, credentials.port, credentials.externalSchemasSupported)
}

func (cg ClientGroup) MasterDatabase(database *redshift.Database) (*Client, error) {

	credentials, ok := cg.credentials[database.ClusterIdentifier]

	if !ok {
		return nil, errors.New(fmt.Sprintf("Unknown cluster with identifier %s", database.ClusterIdentifier))
	}

	return NewClient(credentials.username, credentials.password, credentials.host, credentials.masterDatabase, credentials.sslmode, credentials.port, credentials.externalSchemasSupported)
}
