package redshift

import "github.com/lunarway/hubble-rbac-controller/internal/core/redshift"

type stubRedshiftClientFactory struct {
	client *StubRedshiftClient
}

func newStubRedshiftClientFactory(client *StubRedshiftClient) *stubRedshiftClientFactory {
	return &stubRedshiftClientFactory{client: client}
}

func (p *stubRedshiftClientFactory) GetClusterClient(clusterIdentifier string) (RedshiftClient, error) {
	return p.client, nil
}

func (p *stubRedshiftClientFactory) GetDatabaseClient(clusterIdentifier string, databaseName string) (RedshiftClient, error) {
	return p.client, nil
}

type StubRedshiftClient struct {
	users           []string
	groups          []string
	databases       []string
	grants          []string
	externalSchemas []redshift.ExternalSchema
}

func NewStubRedshiftClient(users []string, groups []string, databases []string, schemas []string, externalSchemas []redshift.ExternalSchema) *StubRedshiftClient {
	return &StubRedshiftClient{users: users, groups: groups, databases: databases, grants: schemas, externalSchemas: externalSchemas}
}

func (c *StubRedshiftClient) Owners() ([]Row, error) {
	owner := "owner"
	var result []Row

	for _, databaseName := range c.databases {
		result = append(result, Row{Cells: []string{databaseName, owner}})
	}

	return result, nil
}
func (c *StubRedshiftClient) UsersAndGroups() ([]Row, error) {
	var result []Row

	for _, user := range c.users {
		for _, group := range c.groups {
			result = append(result, Row{Cells: []string{user, group}})
		}
	}

	return result, nil
}
func (c *StubRedshiftClient) Groups() ([]string, error) {
	return c.groups, nil
}
func (c *StubRedshiftClient) Databases() ([]string, error) {
	return c.databases, nil
}
func (c *StubRedshiftClient) ExternalSchemas() ([]redshift.ExternalSchema, error) {
	return c.externalSchemas, nil
}
func (c *StubRedshiftClient) Grants(groupName string) ([]string, error) {
	return c.grants, nil
}
