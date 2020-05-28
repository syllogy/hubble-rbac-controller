package redshift

import (
	"errors"
	"fmt"
)

type ClientGroup struct {
	clients map[string]*Client
}

func NewClientGroup(clients map[string]*Client) *ClientGroup {
	return &ClientGroup{clients: clients}
}

func (cg ClientGroup) OfCluster(clusterIdentifier string) (*Client, error) {
	 client, ok := cg.clients[clusterIdentifier]

	 if !ok {
		 return nil, errors.New(fmt.Sprintf("Unknown cluster with identifier %s", clusterIdentifier))
	 }

	 return client, nil
}
