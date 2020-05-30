package hubble

type DataSet string

type Database struct {
	ClusterIdentifier string
	Name string
}

type DevDatabase struct {
	ClusterIdentifier string
}

type GlueDatabase struct {
	ShortName string
	Name string
}

type User struct {
	Username string
	Email string
	AssignedTo []Role
}

type Role struct {
	Name string
	GrantedDatabases []Database
	GrantedDevDatabases []DevDatabase
	GrantedGlueDatabases []GlueDatabase
	Acl []DataSet
}

type Model struct {
	Databases []Database
	DevDatabases []DevDatabase
	GlueDatabases []GlueDatabase
	Users []User
	Roles []Role
}
