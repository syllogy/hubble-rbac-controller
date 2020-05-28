package iam

type GetClusterCredentialsPolicy struct {
	email string
	username string
	rolename string
	awsAccountId string
	access []RedshiftAccess
}

func NewGetClusterCredentialsPolicy(email string, username string, rolename string, awsAccountId string) *GetClusterCredentialsPolicy {
	return &GetClusterCredentialsPolicy{email: email, username: username, rolename: rolename, awsAccountId: awsAccountId}
}

type RedshiftAccess struct {
	clusterIdentifier string
	database string
}


func (p GetClusterCredentialsPolicy) Add(clusterIdentifier string, database string) {
	p.access = append(p.access, RedshiftAccess{
		clusterIdentifier: clusterIdentifier,
		database:          database,
	})
}
