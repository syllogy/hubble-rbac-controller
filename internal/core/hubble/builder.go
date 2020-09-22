package hubble

func (m *Model) AddUser(username string, email string) *User {
	user := User{
		Username:   username,
		Email:      email,
		AssignedTo: []*Role{},
	}
	m.Users = append(m.Users, &user)

	return &user
}

func (m *Model) AddRole(name string, acl []DataSet) *Role {
	role := Role{
		Name:                 name,
		Acl:                  acl,
		Policies:             []*PolicyReference{},
		GrantedDatabases:     []*Database{},
		GrantedDevDatabases:  []*DevDatabase{},
		GrantedGlueDatabases: []*GlueDatabase{},
	}
	m.Roles = append(m.Roles, &role)

	return &role
}

func (m *Model) AddDatabase(clusterIdentifier string, name string) *Database {
	database := Database{
		Name:              name,
		ClusterIdentifier: clusterIdentifier,
	}
	m.Databases = append(m.Databases, &database)

	return &database
}

func (m *Model) AddDevDatabase(clusterIdentifier string) *DevDatabase {
	database := DevDatabase{
		ClusterIdentifier: clusterIdentifier,
	}
	m.DevDatabases = append(m.DevDatabases, &database)

	return &database
}

func (m *Model) AddPolicyReference(arn string) *PolicyReference {
	policy := PolicyReference{
		Arn: arn,
	}
	m.Policies = append(m.Policies, &policy)

	return &policy
}

func (r *Role) GrantAccess(database *Database) {
	r.GrantedDatabases = append(r.GrantedDatabases, database)
}

func (r *Role) RevokeAccess(database *Database) {

	var newDatabaseList []*Database

	for _, db := range r.GrantedDatabases {
		if db.Name != database.Name {
			newDatabaseList = append(newDatabaseList, db)
		}
	}

	r.GrantedDatabases = newDatabaseList
}

func (u *User) Assign(role *Role) {
	u.AssignedTo = append(u.AssignedTo, role)
}

func (u *User) Unassign(role *Role) {

	var newAssignedToList []*Role

	for _, r := range u.AssignedTo {
		if r.Name != role.Name {
			newAssignedToList = append(newAssignedToList, r)
		}
	}

	u.AssignedTo = newAssignedToList
}
