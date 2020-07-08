package redshift

type GrantsModel struct {
	Database   *Database
	SchemaName string
	GroupName  string
}

func (s *GrantsModel) Equals(rhs Equatable) bool {
	other, ok := rhs.(*GrantsModel)
	if !ok {
		return false
	}
	return s.Database.Name == other.Database.Name &&
		s.GroupName == other.GroupName &&
		s.SchemaName == other.SchemaName
}

type MembershipModel struct {
	ClusterIdentifier string
	Username          string
	GroupName         string
}

func (s *MembershipModel) Equals(rhs Equatable) bool {
	other, ok := rhs.(*MembershipModel)
	if !ok {
		return false
	}
	return s.Username == other.Username &&
		s.GroupName == other.GroupName &&
		s.ClusterIdentifier == other.ClusterIdentifier
}

type DatabaseModel struct {
	Database          *Database
	ClusterIdentifier string
}

func (s *DatabaseModel) Equals(rhs Equatable) bool {
	other, ok := rhs.(*DatabaseModel)
	if !ok {
		return false
	}
	return s.Database.Name == other.Database.Name &&
		s.ClusterIdentifier == other.ClusterIdentifier
}

type UserModel struct {
	User              *User
	ClusterIdentifier string
}

func (s *UserModel) Equals(rhs Equatable) bool {
	other, ok := rhs.(*UserModel)
	if !ok {
		return false
	}
	return s.User.Name == other.User.Name &&
		s.ClusterIdentifier == other.ClusterIdentifier
}

type GroupModel struct {
	Group             *Group
	ClusterIdentifier string
}

func (s *GroupModel) Equals(rhs Equatable) bool {
	other, ok := rhs.(*GroupModel)
	if !ok {
		return false
	}
	return s.Group.Name == other.Group.Name &&
		s.ClusterIdentifier == other.ClusterIdentifier
}

type SchemaModel struct {
	Schema   *Schema
	Database *Database
}

func (s *SchemaModel) Equals(rhs Equatable) bool {
	other, ok := rhs.(*SchemaModel)
	if !ok {
		return false
	}
	return s.Database.ClusterIdentifier == other.Database.ClusterIdentifier &&
		s.Database.Name == other.Database.Name &&
		s.Schema.Name == s.Schema.Name
}

type ExternalSchemaModel struct {
	Schema   *ExternalSchema
	Database *Database
}

func (s *ExternalSchemaModel) Equals(rhs Equatable) bool {
	other, ok := rhs.(*ExternalSchemaModel)
	if !ok {
		return false
	}
	return s.Database.ClusterIdentifier == other.Database.ClusterIdentifier &&
		s.Database.Name == other.Database.Name &&
		s.Schema.Name == s.Schema.Name
}

