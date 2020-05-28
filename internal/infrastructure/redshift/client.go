package redshift

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"net/url"
)

type Client struct {
	db *sql.DB
}

func NewClient(user string, password string, addr string, database string) (*Client, error) {
	connectionString := fmt.Sprintf("postgres://%v:%v@%v/%v?sslmode=disable", user, url.QueryEscape(password), addr, database)
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, err
	}
	return &Client{
		db: db,
	}, nil
}

func (c *Client) Close() {
	c.Close()
}

func (c *Client) Grant(groupName string, schemaName string) error {
	return nil
}


func (c *Client) CreateGroup(groupName string) error {
	return nil
}

func (c *Client) CreateUser(username string, groupName string, rolename string) error {

	rows, err := c.db.Query(fmt.Sprintf("select count(*) from pg_user where usename = '%s_%s'", username, rolename))
	if err != nil {
		return err
	}
	var count = 0

	if rows.Next() {
		err = rows.Scan(&count)

		if err != nil {
			return err
		}
	}

	if count == 0 {
		//TODO: generate a random string as password, the password will never be used
		_, err := c.db.Exec(fmt.Sprintf("CREATE USER %s_%s PASSWORD 'xxxx' IN GROUP %s;", username, rolename, groupName))

		if err != nil {
			return err
		}
	}
	return nil

}
