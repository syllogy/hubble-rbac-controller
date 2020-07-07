package redshift

import "github.com/lunarway/hubble-rbac-controller/internal/core/redshift"

type ModelResolver struct {
	clientGroup ClientGroup
	excludedUsers   []string //excluded users will not be deleted, even if they are not mentioned in the applied model
	excludedDatabases []string //excluded database will not have their grants managed
}

func NewModelResolver(clientGroup ClientGroup, excludedUsers []string, excludedDatabases []string) *ModelResolver {
	return &ModelResolver{clientGroup: clientGroup, excludedUsers: excludedUsers, excludedDatabases: excludedDatabases}
}


func (m *ModelResolver) isUserExcluded(username string) bool {
	for _,unmanagedUser := range m.excludedUsers {
		if unmanagedUser == username {
			return true
		}
	}
	return false
}

func (m *ModelResolver) isDatabaseExcluded(name string) bool {
	for _, excludedDatabase := range m.excludedDatabases {
		if excludedDatabase == name {
			return true
		}
	}
	return false
}

func (m *ModelResolver) Resolve(model *redshift.Model) error {

	//TODO: find a solution that does not require hardcoding of the external schemas!
	externalSchemas := map[string]string{"lwgoevents": "lw-go-events", "eventstreams": "eventstreams"}

	for _, cluster := range model.Clusters {
		c, err := m.clientGroup.MasterDatabase(cluster.Identifier)

		if err != nil {
			return err
		}

		owners, err := c.Owners()

		if err != nil {
			return err
		}

		ownersMap := make(map[string]string)
		for _, row := range owners {
			ownersMap[row.Cells[0]] = row.Cells[1]
		}

		groups, err := c.Groups()

		if err != nil {
			return err
		}

		for _, group := range groups {
			cluster.DeclareGroup(group)
		}

		usersAndGroups, err := c.UsersAndGroups()

		if err != nil {
			return err
		}

		for _, row := range usersAndGroups {
			user := row.Cells[0]
			group := row.Cells[1]
			if !m.isUserExcluded(user) {
				cluster.DeclareUser(user, cluster.LookupGroup(group))
			}
		}

		databases, err := c.Databases()

		if err != nil {
			return err
		}

		for _, databaseName := range databases {
			if !m.isDatabaseExcluded(databaseName) {

				var database *redshift.Database

				owner, _ := ownersMap[databaseName]

				if !m.isUserExcluded(owner) {
					database = cluster.DeclareDatabaseWithOwner(databaseName, owner)
				} else {
					database = cluster.DeclareDatabase(databaseName)
				}

				databaseClient, err := m.clientGroup.ForDatabase(database)

				if err != nil {
					return err
				}

				for _, row := range usersAndGroups {
					user := row.Cells[0]
					if !m.isUserExcluded(user) {
						database.DeclareUser(user)
					}
				}

				for _, group := range groups {
					databaseGroup := database.DeclareGroup(group)

					grants, err := databaseClient.Grants(group)

					if err != nil {
						return err
					}

					for _, schema := range grants {

						glueDatabase, ok := externalSchemas[schema]

						if ok {
							databaseGroup.GrantExternalSchema(&redshift.ExternalSchema{Name:schema, GlueDatabaseName:glueDatabase})
						} else {
							databaseGroup.GrantSchema(&redshift.Schema{Name:schema})
						}
					}
				}

			}
		}
	}

	return nil
}
