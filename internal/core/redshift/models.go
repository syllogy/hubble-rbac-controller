package redshift

import (
	"fmt"
	"strings"
)

type DatabaseGroup struct {
	Name string
	GrantedSchemas []*Schema
	GrantedExternalSchemas []*ExternalSchema
}

type Schema struct {
	Name string
}

type ExternalSchema struct {
	Name string
	GlueDatabaseName string
}

type DatabaseUser struct {
	Name     string
}

type User struct {
	Name     string
	MemberOf []*Group
}

type Group struct {
	Name     string
}

type Cluster struct {
	Identifier string
	Users []*User
	Groups []*Group
	Databases []*Database
}

type Database struct {
	ClusterIdentifier string
	Name string
	Owner *string
	Users []*DatabaseUser
	Groups []*DatabaseGroup
}

type Model struct {
	Clusters []*Cluster
}

func (m *Model) Validate() error {
	for _,cluster := range m.Clusters {
		err := cluster.Validate()

		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Cluster) Validate() error {
	for _, database := range c.Databases {
		for _, user := range database.Users {
			if c.LookupUser(user.Name) == nil {
				return fmt.Errorf("user with name %s from database %s has not been declared on the cluster", user.Name, database.Name)
			}
		}
	}

	for _, user := range c.Users {
		if user.Role() == nil {
			return fmt.Errorf("role of user with name %s cannot be determined. User must be part of 1 and only 1 group", user.Name)
		}
	}

	return nil
}

func (m *Model) LookupCluster(identifier string) *Cluster {
	for _,cluster := range m.Clusters {
		if cluster.Identifier == identifier {
			return cluster
		}
	}
	return nil
}

func (m *Model) DeclareCluster(identifier string) *Cluster {
	existing := m.LookupCluster(identifier)
	if existing != nil {
		return existing
	}

	newCluster := &Cluster { Identifier: identifier, Databases: []*Database{}, Users: []*User{} }
	m.Clusters = append(m.Clusters, newCluster)
	return newCluster
}

func (c *Cluster) LookupUser(username string) *User {
	for _,user := range c.Users {
		if user.Name == username {
			return user
		}
	}
	return nil
}

func (c *Cluster) DeclareUser(name string, memberOf *Group) *User {
	existing := c.LookupUser(name)
	if existing != nil {
		return existing
	}

	newUser := &User{ Name: strings.ToLower(name), MemberOf: []*Group{memberOf} }
	c.Users = append(c.Users, newUser)
	return newUser
}

func (c *Cluster) LookupGroup(name string) *Group {
	for _,user := range c.Groups {
		if user.Name == strings.ToLower(name)  {
			return user
		}
	}
	return nil
}

func (c *Cluster) DeclareGroup(name string) *Group {
	existing := c.LookupGroup(name)
	if existing != nil {
		return existing
	}

	newGroup := &Group{ Name: strings.ToLower(name) }
	c.Groups = append(c.Groups, newGroup)
	return newGroup
}


func (c *Cluster) LookupDatabase(name string) *Database {
	for _,db := range c.Databases {
		if db.Name == strings.ToLower(name) {
			return db
		}
	}
	return nil
}

func (c *Cluster) DeclareDatabase(name string) *Database {
	return c.declareDatabase(name, nil)
}

func (c *Cluster) DeclareDatabaseWithOwner(name string, owner string) *Database {
	lowercased := strings.ToLower(owner)
	return c.declareDatabase(name, &lowercased)
}

func (c *Cluster) declareDatabase(name string, owner *string) *Database {
	existing := c.LookupDatabase( name)
	if existing != nil {
		return existing
	}

	newDatabase := &Database { ClusterIdentifier: c.Identifier, Name: strings.ToLower(name), Owner: owner }
	c.Databases = append(c.Databases, newDatabase)
	return newDatabase
}

func (d *Database) LookupGroup(name string) *DatabaseGroup {
	for _,group := range d.Groups {
		if group.Name == strings.ToLower(name) {
			return group
		}
	}
	return nil
}

func (d *Database) DeclareGroup(name string) *DatabaseGroup {
	existing := d.LookupGroup(name)
	if existing != nil {
		return existing
	}

	newGroup := &DatabaseGroup{ Name: strings.ToLower(name) }
	d.Groups = append(d.Groups, newGroup)
	return newGroup
}

func (d *Database) LookupUser(name string) *DatabaseUser {
	for _, user := range d.Users {
		if user.Name == strings.ToLower(name) {
			return user
		}
	}
	return nil
}

func (d *Database) DeclareUser(name string) *DatabaseUser {
	existing := d.LookupUser(name)
	if existing != nil {
		return existing
	}

	newUser := &DatabaseUser{ Name: strings.ToLower(name) }
	d.Users = append(d.Users, newUser)
	return newUser
}

func (d *Database) Identifier() string {
	return fmt.Sprintf("%s/%s", d.ClusterIdentifier, d.Name)
}

func (g *DatabaseGroup) GrantSchema(schema *Schema) {
	existing := g.LookupGrantedSchema(schema.Name)
	if existing == nil {
		g.GrantedSchemas = append(g.GrantedSchemas, schema)
	}
}

func (g *DatabaseGroup) GrantExternalSchema(schema *ExternalSchema) {
	existing := g.LookupGrantedExternalSchema(schema.Name)
	if existing == nil {
		g.GrantedExternalSchemas = append(g.GrantedExternalSchemas, schema)
	}
}


func (g *DatabaseGroup) Granted() []string {
	schemas := make([]string, 0, len(g.GrantedSchemas) + len(g.GrantedExternalSchemas))
	for _, schema := range g.GrantedSchemas {
		schemas = append(schemas, strings.ToLower(schema.Name))
	}
	for _, schema := range g.GrantedExternalSchemas {
		schemas = append(schemas, strings.ToLower(schema.Name))
	}
	return schemas
}

func (g *DatabaseGroup) LookupGrantedSchema(name string) *Schema {
	for _, schema := range g.GrantedSchemas {
		if schema.Name == strings.ToLower(name) {
			return schema
		}
	}
	return nil
}

func (g *DatabaseGroup) LookupGrantedExternalSchema(name string) *ExternalSchema {
	for _, schema := range g.GrantedExternalSchemas {
		if schema.Name  == strings.ToLower(name) {
			return schema
		}
	}
	return nil
}

func (u *User) Role() *Group {

	if len(u.MemberOf) != 1 {
		return nil
	}
	return u.MemberOf[0]
}

func (u *User) IsMemberOf(groupName string) bool {
	for _, group := range u.MemberOf {
		if group.Name == groupName {
			return true
		}
	}
	return false
}
