package iam

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/iam"
)


type Client struct {
	session *session.Session
}

func New(session *session.Session) *Client {
	return &Client{session:session}
}

func (client *Client) ListPolicies() error {
	c := iam.New(client.session)
	maxItems := int64(500)
	prefix := "/hubble"
	result, err := c.ListPolicies(&iam.ListPoliciesInput{
		MaxItems:&maxItems,
		PathPrefix:&prefix})

	if err != nil {
		return err
	}
	fmt.Println(result.String())

	return nil
}

func (client *Client) CreatePolicy(policy *GetClusterCredentialsPolicy) {

	c := iam.New(client.session)
	description := "test"

	document := `
{
  "Version": "2012-10-17",
  "Statement": [
      {
          "Effect": "Allow",
          "Action": "redshift:GetClusterCredentials",
          "Resource": [
              "arn:aws:redshift:eu-west-1:478824949770:dbuser:hubble/dev",
              "arn:aws:redshift:eu-west-1:478824949770:dbname:hubble/jwr"
          ],
          "Condition": {
              "StringLike": {
                  "aws:userid": "*:jwr@lunar.app"
              }
          }
      }
  ]
}
`
	name := "test-policy-2"
	path := "/hubble"

	_, err := c.CreatePolicy(&iam.CreatePolicyInput{
		Description:    &description,
		Path:           &path,
		PolicyDocument: &document,
		PolicyName:     &name,
	})

	if err != nil {
		panic(err)
	}
}
