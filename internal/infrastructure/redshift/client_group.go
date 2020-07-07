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

type ClientGroup interface {
	ForDatabase(database *redshift.Database) (*Client, error)
	MasterDatabase(clusterIdentifier string) (*Client, error)
	Database(clusterIdentifier string, databaseName string) (*Client, error)
}

type ClientGroupImpl struct {
	credentials map[string]*ClusterCredentials
}

func NewClientGroup(credentials map[string]*ClusterCredentials) ClientGroup {
	return &ClientGroupImpl{credentials: credentials}
}

func (cg ClientGroupImpl) ForDatabase(database *redshift.Database) (*Client, error) {

	credentials, ok := cg.credentials[database.ClusterIdentifier]

	 if !ok {
		 return nil, errors.New(fmt.Sprintf("Unknown cluster with identifier %s", database.ClusterIdentifier))
	 }

	return NewClient(credentials.Username, credentials.Password, credentials.Host, database.Name, credentials.Sslmode, credentials.Port, credentials.ExternalSchemasSupported)
}

func (cg ClientGroupImpl) MasterDatabase(clusterIdentifier string) (*Client, error) {

	credentials, ok := cg.credentials[clusterIdentifier]

	if !ok {
		return nil, errors.New(fmt.Sprintf("Unknown cluster with identifier %s", clusterIdentifier))
	}

	return NewClient(credentials.Username, credentials.Password, credentials.Host, credentials.MasterDatabase, credentials.Sslmode, credentials.Port, credentials.ExternalSchemasSupported)
}

func (cg ClientGroupImpl) Database(clusterIdentifier string, databaseName string) (*Client, error) {

	credentials, ok := cg.credentials[clusterIdentifier]

	if !ok {
		return nil, errors.New(fmt.Sprintf("Unknown cluster with identifier %s", clusterIdentifier))
	}

	return NewClient(credentials.Username, credentials.Password, credentials.Host, databaseName, credentials.Sslmode, credentials.Port, credentials.ExternalSchemasSupported)
}


type ClientGroupSharedCredentials struct {
	credentials *ClusterCredentials
}

func NewClientGroupY(credentials *ClusterCredentials) ClientGroup {
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


type ClientPool struct {
	clientGroup ClientGroup
	masterClients map[string]*Client
	clients map[string]*Client
}

func NewClientPool(clientGroup ClientGroup) *ClientPool {
	return &ClientPool{
		clientGroup: clientGroup,
		masterClients:make(map[string]*Client),
		clients:make(map[string]*Client),
		}
}

func (c *ClientPool) GetClusterClient(clusterIdentifier string) *Client {

	client, ok := c.masterClients[clusterIdentifier]

	if !ok {
		client, _ = c.clientGroup.MasterDatabase(clusterIdentifier)
		c.masterClients[clusterIdentifier] = client
	}

	return client
}

func (c *ClientPool) GetDatabaseClient(clusterIdentifier string, databaseName string) *Client {

	identifier := clusterIdentifier+"."+databaseName
	client, ok := c.clients[identifier]

	if !ok {
		client, _ = c.clientGroup.Database(clusterIdentifier, databaseName)
		c.clients[identifier] = client
	}

	return client
}

func (c *ClientPool) Close() {
	for _, client := range c.clients {
		client.Close()
	}
	for _, client := range c.masterClients {
		client.Close()
	}
}