package iam

//This represents an IAM policy that allows a user to log into a set of databases using the specified database username.
type DatabaseLoginPolicy struct {
	Email            string
	DatabaseUsername string
	Databases        []*Database
}

type Database struct {
	ClusterIdentifier string
	Name              string
}

//An unmanaged policy that wo want to give to the role.
type PolicyReference struct {
	Arn string
}

//This represents an IAM role that references the set of IAM policies.
type AwsRole struct {
	Name                  string
	DatabaseLoginPolicies []*DatabaseLoginPolicy
	Policies []*PolicyReference
}

//The complete IAM model consists of a set of managed IAM roles
type Model struct {
	Roles []*AwsRole
}

func (p *DatabaseLoginPolicy) LookupDatabase(clusterIdentifier string, name string) *Database {
	for _,r := range p.Databases {
		if r.ClusterIdentifier == clusterIdentifier && r.Name == name {
			return r
		}
	}
	return nil
}

func (p *DatabaseLoginPolicy) Allow(clusterIdentifier string, name string) {

	existing := p.LookupDatabase(clusterIdentifier, name)
	if existing == nil {
		p.Databases = append(p.Databases, &Database{
			ClusterIdentifier: clusterIdentifier,
			Name:              name,
		})
	}
}


func (r *AwsRole) LookupDatabaseLoginPolicyForUser(email string) *DatabaseLoginPolicy {
	for _,p := range r.DatabaseLoginPolicies {
		if p.Email == email {
			return p
		}
	}
	return nil
}

func (r *AwsRole) LookupDatabaseLoginPolicyForUsername(username string) *DatabaseLoginPolicy {
	for _,p := range r.DatabaseLoginPolicies {
		if p.DatabaseUsername == username {
			return p
		}
	}
	return nil
}

func (r *AwsRole) DeclareDatabaseLoginPolicyForUser(email string, username string) *DatabaseLoginPolicy {
	existing := r.LookupDatabaseLoginPolicyForUser(email)
	if existing != nil {
		return existing
	}

	newPolicy := &DatabaseLoginPolicy{ Email: email, DatabaseUsername:username}
	r.DatabaseLoginPolicies = append(r.DatabaseLoginPolicies, newPolicy)
	return newPolicy
}

func (r *AwsRole) DeclareReferencedPolicy(arn string) *PolicyReference {
	existing := r.LookupReferencedPolicy(arn)
	if existing != nil {
		return existing
	}

	newPolicy := &PolicyReference{ Arn: arn}
	r.Policies = append(r.Policies, newPolicy)
	return newPolicy
}

func (r *AwsRole) LookupReferencedPolicy(arn string) *PolicyReference {
	for _,p := range r.Policies {
		if p.Arn == arn {
			return p
		}
	}
	return nil
}

func (m *Model) LookupRole(name string) *AwsRole {
	for _,r := range m.Roles {
		if r.Name == name {
			return r
		}
	}
	return nil
}

func (m *Model) DeclareRole(name string) *AwsRole {
	existing := m.LookupRole(name)
	if existing != nil {
		return existing
	}

	newRole := &AwsRole { Name: name }
	m.Roles = append(m.Roles, newRole)
	return newRole
}
