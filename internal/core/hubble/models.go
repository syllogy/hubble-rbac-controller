package hubble

//An identifier for a group of related tables. In redshift this corresponds to a schema.
type DataSet string

type Database struct {
	ClusterIdentifier string //the identifier of the cluster on which the database resides
	Name string
}

//A developer's own personal database.
//If a dev database has been declared for a given user and cluster, a database with a name corresponding to the user's initials will be created on the given cluster.
type DevDatabase struct {
	ClusterIdentifier string //the identifier of the cluster on which the database resides
}

//If a glue database has been declared an "external schema" will be created in redshift that points to the glue database
//A glue database can be used to query the S3 data lake from redshift/athena/etc.
type GlueDatabase struct {
	ShortName string //the name of the external schema in redshift
	Name string //the name of database in AWS Glue
}
//TODO: as we should keep the hubble model technology agnostic we should remove the "Glue" part of this type name.

type User struct {
	Username string
	Email string
	AssignedTo []*Role
}

//If a user is assigned a role it can log into that role from the terminal and access the granted resources.
type Role struct {
	Name string //the name of the role
	GrantedDatabases []*Database //the set of databases this user has access to
	GrantedDevDatabases []*DevDatabase //the set of dev databases that this user has access to
	GrantedGlueDatabases []*GlueDatabase //the set of glue databases this user has access to
	Acl []DataSet //the set of data groups this user has access to. E.g. a credit analyst should only have access to credit related data.
	Policies []*PolicyReference //the set of extra IAM policies this user has access to. Those could be policies required by the CLI's that are part of the analyst tool chain.
}

//the complete Hubble model which contains all the resources that are managed by the controller.
type Model struct {
	Databases []*Database
	DevDatabases []*DevDatabase
	Users []*User
	Roles []*Role
	Policies []*PolicyReference
}

//A reference to an unmanaged IAM policy
type PolicyReference struct {
	Arn string
}
