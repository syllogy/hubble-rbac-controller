package iam

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
)

type SessionFactory interface {
	CreateSession() *session.Session
}

type LocalStackSessionFactory struct {

}

type AwsSessionFactory struct {

}
func (f LocalStackSessionFactory) CreateSession() *session.Session {
	return session.Must(session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Credentials:      credentials.NewStaticCredentials("foo", "var", ""),
			Region:           aws.String(endpoints.EuWest1RegionID),
			Endpoint:         aws.String("http://localhost:4593"),
		},
	}))
}

func (f AwsSessionFactory) CreateSession() *session.Session {
	return session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
}

