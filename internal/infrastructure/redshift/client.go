package redshift

import (
	"database/sql"
	"fmt"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/lunarway/hubble-rbac-controller/internal/core/redshift"
	"github.com/lunarway/hubble-rbac-controller/internal/core/utils"
	"net/url"
)

type Client struct {
	db                       *sql.DB
	user                     string
	externalSchemasSupported bool
}

var duplicateObjectErrorCode pq.ErrorCode = "42710"
var objectInUse pq.ErrorCode = "55006"

func NewClient(user string, password string, addr string, database string, sslmode string, port int, externalSchemasSupported bool) (*Client, error) {

	connectionString := fmt.Sprintf("sslmode=%s user=%v password=%v host=%v port=%v dbname=%v",
		sslmode,
		user,
		url.QueryEscape(password),
		addr,
		port,
		database)

	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("redshift ping error : (%v)", err)
	}
	return &Client{
		db:                       db,
		user:                     user,
		externalSchemasSupported: externalSchemasSupported,
	}, nil
}

func (c *Client) Close() {
	c.db.Close()
}

func (c *Client) bool(sql string) (bool, error) {
	rows, err := c.db.Query(sql)

	if rows != nil {
		defer rows.Close()
	}

	if err != nil {
		return false, err
	}
	var result = false

	if rows.Next() {
		err = rows.Scan(&result)

		if err != nil {
			return false, err
		}
	}
	return result, nil
}

func (c *Client) stringList(sql string) ([]string, error) {
	rows, err := c.db.Query(sql)

	if rows != nil {
		defer rows.Close()
	}

	if err != nil {
		return nil, err
	}

	result := []string{}
	for rows.Next() {
		var item string
		err = rows.Scan(&item)

		if err != nil {
			return nil, err
		}
		result = append(result, item)
	}

	return result, nil
}

type Row struct {
	Cells []string
}

func (c *Client) stringRows(sql string) ([]Row, error) {
	rows, err := c.db.Query(sql)

	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Row
	for rows.Next() {
		var key string
		var value string
		err = rows.Scan(&key, &value)

		if err != nil {
			return nil, err
		}
		result = append(result, Row{Cells: []string{key, value}})
	}

	return result, nil
}

func (c *Client) contains(list []string, item string) bool {
	for _, x := range list {
		if x == item {
			return true
		}
	}
	return false
}

func (c *Client) Groups() ([]string, error) {
	return c.stringList("SELECT groname FROM pg_group WHERE groname !~ '^pg_' and groname !~'_datashare_roles'")
}

func (c *Client) Users() ([]string, error) {
	return c.stringList("select usename from pg_user")
}

func (c *Client) Schemas() ([]string, error) {
	return c.stringList("select nspname from pg_catalog.pg_namespace WHERE nspname !~ '^pg_' AND nspname <> 'information_schema'")
}

func (c *Client) ExternalSchemas() ([]redshift.ExternalSchema, error) {

	if !c.externalSchemasSupported {
		return []redshift.ExternalSchema{}, nil
	}

	rows, err := c.stringRows("select schemaname, databasename from svv_external_schemas")
	if err != nil {
		return nil, err
	}

	var result []redshift.ExternalSchema
	for _, row := range rows {
		result = append(result, redshift.ExternalSchema{
			Name:             row.Cells[0],
			GlueDatabaseName: row.Cells[1],
		})
	}

	return result, nil
}

func (c *Client) Databases() ([]string, error) {
	return c.stringList("SELECT datname FROM pg_database")
}

func (c *Client) CreateDatabase(name string, owner *string) error {

	databases, err := c.Databases()

	if err != nil {
		return err
	}

	if c.contains(databases, name) {
		if owner != nil {
			_, err = c.db.Exec(fmt.Sprintf("ALTER DATABASE %s OWNER TO %s", name, *owner))
			return err
		}
		return nil
	}

	if owner != nil {
		_, err = c.db.Exec(fmt.Sprintf("CREATE DATABASE %s WITH OWNER=%s", name, *owner))
	} else {
		_, err = c.db.Exec(fmt.Sprintf("CREATE DATABASE %s", name))
	}

	return err
}

func (c *Client) CreateGroup(groupName string) error {

	groups, err := c.Groups()

	if err != nil {
		return err
	}

	if c.contains(groups, groupName) {
		return nil
	}

	_, err = c.db.Exec(fmt.Sprintf("CREATE GROUP %s", groupName))

	return err
}

func (c *Client) DeleteGroup(groupName string) error {

	groups, err := c.Groups()

	if err != nil {
		return err
	}

	if !c.contains(groups, groupName) {
		return nil
	}

	grants, err := c.Grants(groupName)

	if err != nil {
		return err
	}

	for _, schema := range grants {
		err = c.Revoke(groupName, schema)

		if err != nil {
			return err
		}
	}

	//The dummy user might still exist if the last call to Grants ended abruptly
	_, err = c.db.Exec(fmt.Sprintf("DROP USER IF EXISTS dummy_%s", groupName))

	if err != nil {
		return err
	}

	_, err = c.db.Exec(fmt.Sprintf("DROP GROUP %s", groupName))

	return err
}

func (c *Client) CreateSchema(name string) error {

	schemas, err := c.Schemas()

	if err != nil {
		return err
	}

	if c.contains(schemas, name) {
		return nil
	}

	_, err = c.db.Exec(fmt.Sprintf("CREATE SCHEMA %s", name))
	return err
}

func (c *Client) CreateExternalSchema(name string, externalDatabaseName string, awsAccountId string) error {

	if !c.externalSchemasSupported {
		return c.CreateSchema(name)
	}

	schemas, err := c.Schemas()

	if err != nil {
		return err
	}

	if c.contains(schemas, name) {
		return nil
	}

	sql := `
            create external schema if not exists %s
            from data catalog
            database '%s'
            iam_role 'arn:aws:iam::%s:role/redshift-datalake'
`

	_, err = c.db.Exec(fmt.Sprintf(sql, name, externalDatabaseName, awsAccountId))
	return err
}

func (c *Client) AddUserToGroup(username string, groupname string) error {
	_, err := c.db.Exec(fmt.Sprintf("ALTER GROUP %s ADD USER %s", groupname, username))
	return err
}

func (c *Client) RemoveUserFromGroup(username string, groupname string) error {
	_, err := c.db.Exec(fmt.Sprintf("ALTER GROUP %s DROP USER %s", groupname, username))
	return err
}

func (c *Client) PartOf(username string) ([]string, error) {
	sql := `
select pg_group.groname from pg_user, pg_group  where
pg_user.usesysid = ANY(pg_group.grolist) AND
usename='%s'
`

	return c.stringList(fmt.Sprintf(sql, username))
}

func (c *Client) UsersAndGroups() ([]Row, error) {
	sql := `
select pg_user.usename, pg_group.groname from pg_user, pg_group  where
pg_user.usesysid = ANY(pg_group.grolist)
`
	return c.stringRows(sql)
}

func (c *Client) Owners() ([]Row, error) {
	sql := `
SELECT d.datname as "Name",
pg_catalog.pg_get_userbyid(d.datdba) as "Owner"
FROM pg_catalog.pg_database d
`
	return c.stringRows(sql)
}

func generateRedshiftPassword() string {
	//password must contain a digit and an lowercase and uppercase character
	return utils.GenerateRandomString(10) + "x0F"
}

func (c *Client) CreateUser(username string) error {

	users, err := c.Users()

	if err != nil {
		return err
	}

	if c.contains(users, username) {
		return nil
	}

	//Password is set to a random string, it will never be used because we log in using IAM's GetClusterCredentials
	_, err = c.db.Exec(fmt.Sprintf("CREATE USER %s PASSWORD '%s'", username, generateRedshiftPassword()))
	return err
}

func (c *Client) DeleteUser(username string) error {
	_, err := c.db.Exec(fmt.Sprintf("DROP USER IF EXISTS %s", username))
	return err
}

func (c *Client) SetSchemaOwner(username string, schema string) error {
	_, err := c.db.Exec(fmt.Sprintf("ALTER SCHEMA %s OWNER TO %s", schema, username))
	return err
}

func (c *Client) Grants(groupName string) ([]string, error) {

	schemas, err := c.Schemas()

	if err != nil {
		return nil, err
	}

	//The has_schema_privilege function only works on users (not groups), therefore we need to create a dummy user
	_, err = c.db.Exec(fmt.Sprintf("CREATE USER dummy_%s PASSWORD '%s' IN GROUP %s", groupName, generateRedshiftPassword(), groupName))

	if err != nil && err.(*pq.Error).Code != duplicateObjectErrorCode {
		return nil, err
	}

	//drop the dummy user
	defer c.db.Exec(fmt.Sprintf("DROP USER IF EXISTS dummy_%s", groupName))

	var result []string

	for _, schema := range schemas {
		isGranted, err := c.bool(fmt.Sprintf("select pg_catalog.has_schema_privilege('dummy_%s', '%s', 'USAGE')", groupName, schema))

		if err != nil {
			return nil, err
		}
		if isGranted {
			result = append(result, schema)
		}
	}

	return result, nil
}

func (c *Client) Grant(groupName string, schemaName string) error {
	_, err := c.db.Exec(fmt.Sprintf("GRANT ALL ON SCHEMA %s TO GROUP %s", schemaName, groupName))

	if err != nil {
		return err
	}
	_, err = c.db.Exec(fmt.Sprintf("GRANT SELECT ON ALL TABLES IN SCHEMA %s TO GROUP %s", schemaName, groupName))

	if err != nil {
		return err
	}
	_, err = c.db.Exec(fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT SELECT ON TABLES TO GROUP %s", schemaName, groupName))

	return err
}

func (c *Client) Revoke(groupName string, schemaName string) error {

	_, err := c.db.Exec(fmt.Sprintf("REVOKE SELECT ON ALL TABLES IN SCHEMA %s FROM GROUP %s", schemaName, groupName))
	if err != nil {
		return err
	}

	_, err = c.db.Exec(fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA %s REVOKE SELECT ON TABLES FROM GROUP %s", schemaName, groupName))
	if err != nil {
		return err
	}

	_, err = c.db.Exec(fmt.Sprintf("REVOKE ALL ON SCHEMA %s FROM GROUP %s", schemaName, groupName))
	return err
}
