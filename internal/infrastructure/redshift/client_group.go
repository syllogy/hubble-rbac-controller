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

type HostResolver func(string) string

type ClientGroup interface {
	ForDatabase(database *redshift.Database) (*Client, error)
	MasterDatabase(clusterIdentifier string) (*Client, error)
	Database(clusterIdentifier string, databaseName string) (*Client, error)
}

type ClientGroupSharedCredentials struct {
	credentials  *ClusterCredentials
	hostResolver HostResolver
}

func NewClientGroup(credentials *ClusterCredentials) *ClientGroupSharedCredentials {
	return &ClientGroupSharedCredentials{
		credentials: credentials,
		hostResolver: func(clusterIdentifier string) string {
			return fmt.Sprintf(credentials.Host, clusterIdentifier)
		},
	}
}

//A client group used to connect to a postgresql database. Only used in tests.
func NewClientGroupForTest(credentials *ClusterCredentials) *ClientGroupSharedCredentials {
	return &ClientGroupSharedCredentials{
		credentials: credentials,
		hostResolver: func(clusterIdentifier string) string {
			return credentials.Host
		},
	}
}

func (cg ClientGroupSharedCredentials) ForDatabase(database *redshift.Database) (*Client, error) {

	credentials := cg.credentials

	return NewClient(credentials.Username, credentials.Password, cg.hostResolver(database.ClusterIdentifier), database.Name, credentials.Sslmode, credentials.Port, credentials.ExternalSchemasSupported)
}

func (cg ClientGroupSharedCredentials) MasterDatabase(clusterIdentifier string) (*Client, error) {

	credentials := cg.credentials

	return NewClient(credentials.Username, credentials.Password, cg.hostResolver(clusterIdentifier), credentials.MasterDatabase, credentials.Sslmode, credentials.Port, credentials.ExternalSchemasSupported)
}

func (cg ClientGroupSharedCredentials) Database(clusterIdentifier string, databaseName string) (*Client, error) {

	credentials := cg.credentials

	return NewClient(credentials.Username, credentials.Password, cg.hostResolver(clusterIdentifier), databaseName, credentials.Sslmode, credentials.Port, credentials.ExternalSchemasSupported)
}
