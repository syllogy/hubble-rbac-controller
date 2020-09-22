package redshift

type Excluder interface {
	IsDatabaseExcluded(string) bool
	IsUserExcluded(string) bool
}

type Exclusions struct {
	excludedUsers   []string //excluded users will not be deleted, even if they are not mentioned in the applied model
	excludedDatabases []string //excluded databases will not have their grants managed
}

func NewExclusions(excludedDatabases []string, excludedUsers []string) *Exclusions {
	return &Exclusions{excludedUsers: excludedUsers, excludedDatabases: excludedDatabases}
}

func (m *Exclusions) IsUserExcluded(username string) bool {
	for _,unmanagedUser := range m.excludedUsers {
		if unmanagedUser == username {
			return true
		}
	}
	return false
}

func (m *Exclusions) IsDatabaseExcluded(name string) bool {
	for _, excludedDatabase := range m.excludedDatabases {
		if excludedDatabase == name {
			return true
		}
	}
	return false
}
