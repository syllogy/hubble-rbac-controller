package redshift

import (
	"github.com/lunarway/hubble-rbac-controller/internal/core/redshift"
	"golang.org/x/sync/errgroup"
)

// The ModelResolver can query the clusters and resolve the current state and return it as a redshift.Model.
type ModelResolver struct {
	clientGroup ClientGroup
	excluded *redshift.Exclusions
}

func NewModelResolver(clientGroup ClientGroup, excluded *redshift.Exclusions) *ModelResolver {
	return &ModelResolver{clientGroup: clientGroup, excluded: excluded}
}


func (m *ModelResolver) resolveCluster(clusterIdentifier string, cluster *redshift.Cluster) error {

	//TODO: find a solution that does not require hardcoding of the external schemas!
	externalSchemas := map[string]string{
		"lwgoevents": "lw-go-events",
		"eventstreams": "eventstreams",
		"intercom": "intercom",
		"googlesheets": "google-sheets"}

	clientPool := NewClientPool(m.clientGroup)

	defer clientPool.Close()

	c, err := clientPool.GetClusterClient(clusterIdentifier)

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
		if !m.excluded.IsUserExcluded(user) {
			cluster.DeclareUser(user, cluster.LookupGroup(group))
		}
	}

	databases, err := c.Databases()

	if err != nil {
		return err
	}

	for _, databaseName := range databases {
		if !m.excluded.IsDatabaseExcluded(databaseName) {

			var database *redshift.Database

			owner, _ := ownersMap[databaseName]

			if !m.excluded.IsUserExcluded(owner) {
				database = cluster.DeclareDatabaseWithOwner(databaseName, owner)
			} else {
				database = cluster.DeclareDatabase(databaseName)
			}

			databaseClient, err := clientPool.GetDatabaseClient(database.ClusterIdentifier, databaseName)

			if err != nil {
				return err
			}
			for _, row := range usersAndGroups {
				user := row.Cells[0]
				if !m.excluded.IsUserExcluded(user) {
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
	return nil
}

// Queries the given clusters for their state and builds up a model representing the current state
func (m *ModelResolver) Resolve(clusterIdentifiers []string) (*redshift.Model, error) {

	model := &redshift.Model{}

	var g errgroup.Group
	for _, clusterIdentifier := range clusterIdentifiers {
		cluster := model.DeclareCluster(clusterIdentifier)
		clusterIdentifier := clusterIdentifier // https://golang.org/doc/faq#closures_and_goroutines
		g.Go(func() error {
			return m.resolveCluster(clusterIdentifier, cluster)
		})
	}

	err := g.Wait()

	if err != nil {
		return nil, err
	}
	return model, nil
}
