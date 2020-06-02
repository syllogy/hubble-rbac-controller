package redshift

import (
	"errors"
	"fmt"
	"github.com/lunarway/hubble-rbac-controller/internal/core/redshift"
)

type ClusterCredentials struct {
	Username                 string
	Password                 string
	MasterDatabase           string
	Host                     string
	Sslmode                  string
	Port                     int
	ExternalSchemasSupported bool
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

	return NewClient(credentials.Username, credentials.Password, credentials.Host, database.Name, credentials.Sslmode, credentials.Port, credentials.ExternalSchemasSupported)
}

func (cg ClientGroup) MasterDatabase(clusterIdentifier string) (*Client, error) {

	credentials, ok := cg.credentials[clusterIdentifier]

	if !ok {
		return nil, errors.New(fmt.Sprintf("Unknown cluster with identifier %s", clusterIdentifier))
	}

	return NewClient(credentials.Username, credentials.Password, credentials.Host, credentials.MasterDatabase, credentials.Sslmode, credentials.Port, credentials.ExternalSchemasSupported)
}
