package redshift

import "fmt"

type Group struct {
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

type User struct {
	Name string
	Of *Group
}

type Database struct {
	ClusterIdentifier string
	Name string
	Users []*User
	Groups []*Group
}

type Model struct {
	Databases []*Database
}

func (m *Model) LookupDatabase(clusterIdentifier string, name string) *Database {
	for _,db := range m.Databases {
		if db.ClusterIdentifier == clusterIdentifier && db.Name == name {
			return db
		}
	}
	return nil
}

func (m *Model) DeclareDatabase(clusterIdentifier string, name string) *Database {
	existing := m.LookupDatabase(clusterIdentifier, name)
	if existing != nil {
		return existing
	}

	newDatabase := &Database { ClusterIdentifier: clusterIdentifier, Name: name }
	m.Databases = append(m.Databases, newDatabase)
	return newDatabase
}

func (d *Database) LookupGroup(name string) *Group {
	for _,group := range d.Groups {
		if group.Name == name {
			return group
		}
	}
	return nil
}

func (d *Database) DeclareGroup(name string) *Group {
	existing := d.LookupGroup(name)
	if existing != nil {
		return existing
	}

	newGroup := &Group { Name: name }
	d.Groups = append(d.Groups, newGroup)
	return newGroup
}

func (d *Database) LookupUser(name string) *User {
	for _, user := range d.Users {
		if user.Name == name {
			return user
		}
	}
	return nil
}

func (d *Database) DeclareUser(name string, of *Group) *User {
	existing := d.LookupUser(name)
	if existing != nil {
		return existing
	}

	newUser := &User { Name: name, Of: of }
	d.Users = append(d.Users, newUser)
	return newUser
}

func (d *Database) Identifier() string {
	return fmt.Sprintf("%s/%s", d.ClusterIdentifier, d.Name)
}

func (g *Group) GrantSchema(schema *Schema) {
	g.GrantedSchemas = append(g.GrantedSchemas, schema)
}

func (g *Group) GrantExternalSchema(schema *ExternalSchema) {
	g.GrantedExternalSchemas = append(g.GrantedExternalSchemas, schema)
}


func (g *Group) Granted() []string {
	schemas := make([]string, 0, len(g.GrantedSchemas) + len(g.GrantedExternalSchemas))
	for _, schema := range g.GrantedSchemas {
		schemas = append(schemas, schema.Name)
	}
	for _, schema := range g.GrantedExternalSchemas {
		schemas = append(schemas, schema.Name)
	}
	return schemas
}

func (g *Group) LookupGrantedSchema(name string) *Schema {
	for _, schema := range g.GrantedSchemas {
		if schema.Name == name {
			return schema
		}
	}
	return nil
}

func (g *Group) LookupGrantedExternalSchema(name string) *ExternalSchema {
	for _, schema := range g.GrantedExternalSchemas {
		if schema.Name  == name {
			return schema
		}
	}
	return nil
}
