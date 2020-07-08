package redshift

import (
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

type ClientGroup interface {
	ForDatabase(database *redshift.Database) (*Client, error)
	MasterDatabase(clusterIdentifier string) (*Client, error)
	Database(clusterIdentifier string, databaseName string) (*Client, error)
}

type ClientGroupSharedCredentials struct {
	credentials *ClusterCredentials
}

func NewClientGroup(credentials *ClusterCredentials) ClientGroup {
	return &ClientGroupSharedCredentials{credentials: credentials}
}

func (cg ClientGroupSharedCredentials) ForDatabase(database *redshift.Database) (*Client, error) {

	credentials := cg.credentials

	return NewClient(credentials.Username, credentials.Password, fmt.Sprintf(credentials.Host, database.ClusterIdentifier), database.Name, credentials.Sslmode, credentials.Port, credentials.ExternalSchemasSupported)
}

func (cg ClientGroupSharedCredentials) MasterDatabase(clusterIdentifier string) (*Client, error) {

	credentials := cg.credentials

	return NewClient(credentials.Username, credentials.Password, fmt.Sprintf(credentials.Host, clusterIdentifier), credentials.MasterDatabase, credentials.Sslmode, credentials.Port, credentials.ExternalSchemasSupported)
}

func (cg ClientGroupSharedCredentials) Database(clusterIdentifier string, databaseName string) (*Client, error) {

	credentials := cg.credentials

	return NewClient(credentials.Username, credentials.Password, fmt.Sprintf(credentials.Host, clusterIdentifier), databaseName, credentials.Sslmode, credentials.Port, credentials.ExternalSchemasSupported)
}

