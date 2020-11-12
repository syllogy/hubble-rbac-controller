package iam

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/iam"
	log "github.com/sirupsen/logrus"
	"strings"
)

var iamPrefix = "/hubble-rbac/"

type Client struct {
	session *session.Session
}

func New(session *session.Session) *Client {
	return &Client{session: session}
}

func (client *Client) assertNotTruncated(truncated *bool) error {
	if *truncated {
		return fmt.Errorf("internal error: Response was truncated, please increase page size or implement paging")
	}
	return nil
}

func (client *Client) lookupPolicy(policies []*iam.Policy, name string) *iam.Policy {
	for _, r := range policies {
		if *r.PolicyName == name {
			return r
		}
	}
	return nil
}

func (client *Client) lookupAttachedPolicy(policies []*iam.AttachedPolicy, name string) *iam.AttachedPolicy {
	for _, r := range policies {
		if *r.PolicyName == name {
			return r
		}
	}
	return nil
}

func (client *Client) lookupAttachedPolicyByArn(policies []*iam.AttachedPolicy, arn string) *iam.AttachedPolicy {
	for _, r := range policies {
		if *r.PolicyArn == arn {
			return r
		}
	}
	return nil
}

func (client *Client) lookupRole(roles []*iam.Role, name string) *iam.Role {
	for _, r := range roles {
		if *r.RoleName == name {
			return r
		}
	}
	return nil
}

func (client *Client) ListRoles() ([]*iam.Role, error) {
	c := iam.New(client.session)
	maxItems := int64(500)
	response, err := c.ListRoles(&iam.ListRolesInput{
		MaxItems:   &maxItems,
		PathPrefix: &iamPrefix,
	})

	if err != nil {
		return nil, err
	}

	err = client.assertNotTruncated(response.IsTruncated)

	if err != nil {
		return nil, err
	}

	log.Debug(response.String())

	return response.Roles, nil
}

func (client *Client) ListPolicies() ([]*iam.Policy, error) {
	c := iam.New(client.session)
	maxItems := int64(500)
	response, err := c.ListPolicies(&iam.ListPoliciesInput{
		MaxItems:   &maxItems,
		PathPrefix: &iamPrefix,
	})

	if err != nil {
		return nil, err
	}

	log.Debug(response.String())

	err = client.assertNotTruncated(response.IsTruncated)

	if err != nil {
		return nil, err
	}

	return response.Policies, nil
}

func (client *Client) listAttachedPolicies(role *iam.Role, prefix *string) ([]*iam.AttachedPolicy, error) {

	c := iam.New(client.session)
	maxItems := int64(500)

	response, err := c.ListAttachedRolePolicies(&iam.ListAttachedRolePoliciesInput{
		MaxItems:   &maxItems,
		PathPrefix: prefix,
		RoleName:   role.RoleName,
	})

	if err != nil {
		return nil, fmt.Errorf("unable to list attached policies for %s: %w", *role.RoleName, err)
	}
	err = client.assertNotTruncated(response.IsTruncated)

	if err != nil {
		return nil, fmt.Errorf("unable to list attached policies for %s: %w", *role.RoleName, err)
	}

	log.Debug(response.String())

	return response.AttachedPolicies, nil
}

func (client *Client) ListManagedAttachedPolicies(role *iam.Role) ([]*iam.AttachedPolicy, error) {
	return client.listAttachedPolicies(role, &iamPrefix)
}

func (client *Client) ListUnmanagedAttachedPolicies(role *iam.Role) ([]*iam.AttachedPolicy, error) {

	allPolicies, err := client.listAttachedPolicies(role, nil)
	if err != nil {
		return nil, err
	}

	var result []*iam.AttachedPolicy
	for _, policy := range allPolicies {
		if !strings.Contains(*policy.PolicyArn, iamPrefix) {
			result = append(result, policy)
		}
	}
	return result, nil
}

func (client *Client) GetPolicy(policy *iam.AttachedPolicy) (*iam.Policy, error) {

	c := iam.New(client.session)

	response, err := c.GetPolicy(&iam.GetPolicyInput{
		PolicyArn: policy.PolicyArn,
	})

	if err != nil {
		return nil, err
	}

	return response.Policy, nil
}

func (client *Client) lookupPolicyByArn(arn string) (*iam.Policy, error) {

	c := iam.New(client.session)

	response, err := c.GetPolicy(&iam.GetPolicyInput{
		PolicyArn: aws.String(arn),
	})

	if err != nil {
		return nil, err
	}

	return response.Policy, nil
}

func (client *Client) GetPolicyDocuments() (map[string]string, error) {

	result := make(map[string]string)
	maxItems := int64(500)

	c := iam.New(client.session)

	localManagedPolicy := "LocalManagedPolicy"

	response, err := c.GetAccountAuthorizationDetails(&iam.GetAccountAuthorizationDetailsInput{
		Filter:   []*string{&localManagedPolicy},
		MaxItems: &maxItems,
	})

	if err != nil {
		return nil, err
	}

	err = client.assertNotTruncated(response.IsTruncated)

	if err != nil {
		return nil, err
	}

	for _, policy := range response.Policies {
		if *policy.Path == iamPrefix {
			result[*policy.PolicyName] = *policy.PolicyVersionList[0].Document
		}
	}

	return result, nil
}

func (client *Client) createOrUpdatePolicy(name string, document string) (*iam.Policy, error) {

	c := iam.New(client.session)

	policies, err := client.ListPolicies()

	if err != nil {
		return nil, fmt.Errorf("unable to list policies: %w", err)
	}

	policy := client.lookupPolicy(policies, name)
	if policy != nil {
		err = client.DeletePolicy(policy)
		if err != nil {
			return nil, fmt.Errorf("Unable to delete policy %s: %w", name, err)
		}
	}

	response, err := c.CreatePolicy(&iam.CreatePolicyInput{
		Description:    aws.String("Created by Hubble rbac controller"),
		Path:           &iamPrefix,
		PolicyDocument: &document,
		PolicyName:     &name,
	})

	if err != nil {
		return nil, fmt.Errorf("unable to create policy %s: %w", name, err)
	}

	return response.Policy, nil
}

func (client *Client) attachPolicy(role *iam.Role, policy *iam.Policy) error {

	c := iam.New(client.session)

	attachedPolicies, err := client.ListManagedAttachedPolicies(role)

	if err != nil {
		return fmt.Errorf("unable to list attached policies: %w", err)
	}

	if client.lookupAttachedPolicy(attachedPolicies, *policy.PolicyName) == nil {
		_, err := c.AttachRolePolicy(&iam.AttachRolePolicyInput{
			PolicyArn: policy.Arn,
			RoleName:  role.RoleName,
		})

		if err != nil {
			return fmt.Errorf("unable to attach policy %s to role %s: %w", *policy.PolicyName, *role.RoleName, err)
		}
	}
	attachedPolicies, err = client.ListManagedAttachedPolicies(role)

	return nil
}

func (client *Client) DetachPolicy(role *iam.Role, policy *iam.AttachedPolicy) error {

	c := iam.New(client.session)

	attachedPolicies, err := client.ListManagedAttachedPolicies(role)

	if err != nil {
		return fmt.Errorf("unable to list attached policies: %w", err)
	}

	if client.lookupAttachedPolicy(attachedPolicies, *policy.PolicyName) != nil {

		_, err := c.DetachRolePolicy(&iam.DetachRolePolicyInput{
			PolicyArn: policy.PolicyArn,
			RoleName:  role.RoleName,
		})

		if err != nil {
			return fmt.Errorf("unable to detach policy %s from role %s: %w", *policy.PolicyName, *role.RoleName, err)
		}
	}

	return nil
}

func (client *Client) DetachUnmanagedPolicy(role *iam.Role, policy *iam.AttachedPolicy) error {

	c := iam.New(client.session)

	attachedPolicies, err := client.ListUnmanagedAttachedPolicies(role)

	if err != nil {
		return fmt.Errorf("unable to list attached policies: %w", err)
	}

	if client.lookupAttachedPolicy(attachedPolicies, *policy.PolicyName) != nil {

		_, err := c.DetachRolePolicy(&iam.DetachRolePolicyInput{
			PolicyArn: policy.PolicyArn,
			RoleName:  role.RoleName,
		})

		if err != nil {
			return fmt.Errorf("unable to detach policy %s from role %s: %w", *policy.PolicyName, *role.RoleName, err)
		}
	}

	return nil
}

func (client *Client) deletePolicy(name string, arn string) error {
	c := iam.New(client.session)

	policies, err := client.ListPolicies()

	if err != nil {
		return fmt.Errorf("unable to list policies: %w", err)
	}

	existing := client.lookupPolicy(policies, name)
	if existing == nil {
		return nil
	}

	_, err = c.DeletePolicy(&iam.DeletePolicyInput{
		PolicyArn: aws.String(arn),
	})

	if err != nil {
		return fmt.Errorf("unable to delete policy %s: %w", name, err)
	}

	return nil
}

func (client *Client) DeletePolicy(policy *iam.Policy) error {
	return client.deletePolicy(*policy.PolicyName, *policy.Arn)
}

func (client *Client) DeleteAttachedPolicy(policy *iam.AttachedPolicy) error {
	return client.deletePolicy(*policy.PolicyName, *policy.PolicyArn)
}

func (client *Client) CreateOrUpdateLoginRole(name string, accountId string) (*iam.Role, error) {

	c := iam.New(client.session)

	roles, err := client.ListRoles()

	if err != nil {
		return nil, fmt.Errorf("unable to list roles: %w", err)
	}

	role := client.lookupRole(roles, name)
	if role != nil {
		err = client.DeleteLoginRole(role)
		if err != nil {
			return nil, fmt.Errorf("unable to delete login role %s: %w", name, err)
		}
	}

	var maxSessionDuration int64 = 14400

	assumeRolePolicyDocument := `
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::%s:saml-provider/GoogleApps"
      },
      "Action": "sts:AssumeRoleWithSAML",
      "Condition": {
        "StringEquals": {
          "SAML:aud": "https://signin.aws.amazon.com/saml"
        }
      }
    }
  ]
}
`

	response, err := c.CreateRole(&iam.CreateRoleInput{
		AssumeRolePolicyDocument: aws.String(strings.TrimSpace(fmt.Sprintf(assumeRolePolicyDocument, accountId))),
		Description:              aws.String("test"),
		MaxSessionDuration:       &maxSessionDuration,
		Path:                     &iamPrefix,
		RoleName:                 &name,
	})

	if err != nil {
		return nil, fmt.Errorf("unable to create login role %s: %w", name, err)
	}

	log.Debug(response.String())

	return response.Role, nil
}

func (client *Client) DeleteLoginRole(role *iam.Role) error {
	c := iam.New(client.session)

	roles, err := client.ListRoles()

	if err != nil {
		return fmt.Errorf("unable to list roles: %w", err)
	}

	existing := client.lookupRole(roles, *role.RoleName)
	if existing == nil {
		return nil
	}

	_, err = c.DeleteRole(&iam.DeleteRoleInput{RoleName: role.RoleName})

	if err != nil {
		return fmt.Errorf("unable to delete role %s: %w", *role.RoleName, err)
	}

	return nil
}
