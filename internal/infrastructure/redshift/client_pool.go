package redshift

type ClientPool struct {
	clientGroup ClientGroup
	masterClients map[string]*Client
	clients map[string]*Client
}

func NewClientPool(clientGroup ClientGroup) *ClientPool {
	return &ClientPool{
		clientGroup: clientGroup,
		masterClients:make(map[string]*Client),
		clients:make(map[string]*Client),
	}
}

func (c *ClientPool) GetClusterClient(clusterIdentifier string) *Client {

	client, ok := c.masterClients[clusterIdentifier]

	if !ok {
		client, _ = c.clientGroup.MasterDatabase(clusterIdentifier)
		c.masterClients[clusterIdentifier] = client
	}

	return client
}

func (c *ClientPool) GetDatabaseClient(clusterIdentifier string, databaseName string) *Client {

	identifier := clusterIdentifier+"."+databaseName
	client, ok := c.clients[identifier]

	if !ok {
		client, _ = c.clientGroup.Database(clusterIdentifier, databaseName)
		c.clients[identifier] = client
	}

	return client
}

func (c *ClientPool) Close() {
	for _, client := range c.clients {
		client.Close()
	}
	for _, client := range c.masterClients {
		client.Close()
	}
}

