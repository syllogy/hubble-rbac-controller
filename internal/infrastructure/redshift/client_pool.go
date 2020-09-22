package redshift

type ClientPool struct {
	clientGroup   ClientGroup
	masterClients map[string]*Client
	clients       map[string]*Client
}

func NewClientPool(clientGroup ClientGroup) *ClientPool {
	return &ClientPool{
		clientGroup:   clientGroup,
		masterClients: make(map[string]*Client),
		clients:       make(map[string]*Client),
	}
}

func (c *ClientPool) GetClusterClient(clusterIdentifier string) (*Client, error) {

	client, ok := c.masterClients[clusterIdentifier]

	if ok {
		return client, nil
	}
	client, err := c.clientGroup.MasterDatabase(clusterIdentifier)
	if err != nil {
		return nil, err
	}

	c.masterClients[clusterIdentifier] = client
	return client, nil
}

func (c *ClientPool) GetDatabaseClient(clusterIdentifier string, databaseName string) (*Client, error) {

	identifier := clusterIdentifier + "." + databaseName
	client, ok := c.clients[identifier]

	if ok {
		return client, nil
	}
	client, err := c.clientGroup.Database(clusterIdentifier, databaseName)
	if err != nil {
		return nil, err
	}

	c.clients[identifier] = client
	return client, nil
}

func (c *ClientPool) Close() {
	for _, client := range c.clients {
		client.Close()
	}
	for _, client := range c.masterClients {
		client.Close()
	}
}
