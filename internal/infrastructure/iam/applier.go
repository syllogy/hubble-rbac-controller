package iam

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/iam"
	iamCore "github.com/lunarway/hubble-rbac-controller/internal/core/iam"
	log "github.com/sirupsen/logrus"
	"strings"
)

type ApplyEventType int

const (
	RoleUpdated ApplyEventType = iota
	RoleCreated
	RoleDeleted
	PolicyUpdated
	PolicyCreated
	PolicyDeleted
)

func (t ApplyEventType) ToString() string {
	switch t {
	case RoleUpdated:
		return "RoleUpdated"
	case RoleCreated:
		return "RoleCreated"
	case RoleDeleted:
		return "RoleDeleted"
	case PolicyUpdated:
		return "PolicyUpdated"
	case PolicyCreated:
		return "PolicyCreated"
	case PolicyDeleted:
		return "PolicyDeleted"
	default:
		return fmt.Sprintf("%d", int(t))
	}
}

type ApplyEventLister interface {
	Handle(eventType ApplyEventType, name string)
}

type Applier struct {
	accountId string
	region string
	client *Client
	eventListener ApplyEventLister
}

func NewApplier(client *Client, accountId string, region string, eventListener ApplyEventLister) *Applier {
	return &Applier{
		accountId: accountId,
		region: region,
		client:client,
		eventListener:eventListener,
	}
}

//TODO: replace all this Sprintf'ing with go templating!
func (applier *Applier) buildDatabaseLoginPolicyDocument(policy *iamCore.DatabaseLoginPolicy) string {

	var statements []string

	for _,database := range policy.Databases {

		dbUserTemplate := "arn:aws:redshift:%s:%s:dbuser:%s/%s"
		dbNameTemplate := "arn:aws:redshift:%s:%s:dbname:%s/%s"

		dbUser := fmt.Sprintf(dbUserTemplate, applier.region, applier.accountId, database.ClusterIdentifier, strings.ToLower(policy.DatabaseUsername))
		dbName := fmt.Sprintf(dbNameTemplate, applier.region, applier.accountId, database.ClusterIdentifier, database.Name)

		statementTemplate := `
	     {
	         "Effect": "Allow",
	         "Action": "redshift:GetClusterCredentials",
	         "Resource": [
	             "%s",
	             "%s"
	         ],
	         "Condition": {
	             "StringLike": {
	                 "aws:userid": "*:%s"
	             }
	         }
	     }
`
		statement := fmt.Sprintf(statementTemplate, dbUser, dbName, policy.Email)
		statements = append(statements, statement)
	}

		documentTemplate := `
	{
	 "Version": "2012-10-17",
	 "Statement": [
	     %s
	 ]
	}
`
	document := fmt.Sprintf(documentTemplate, strings.Join(statements, ","))

	return strings.TrimSpace(document)
}

func (applier *Applier) lookupRole(roles []*iam.Role, name string) *iam.Role {
	for _,r := range roles {
		if *r.RoleName == name {
			return r
		}
	}
	return nil
}

func (applier *Applier) lookupAttachedPolicy(roles []*iam.AttachedPolicy, name string) *iam.AttachedPolicy {
	for _,r := range roles {
		if *r.PolicyName == name {
			return r
		}
	}
	return nil
}

func (applier *Applier) detachAndDeletePolicy(role *iam.Role, attachedPolicy *iam.AttachedPolicy) error {

	err := applier.client.detachPolicy(role, attachedPolicy)

	if err != nil {
		return fmt.Errorf("failed detaching policy %s: %w", *attachedPolicy.PolicyName, err)
	}

	err = applier.client.DeleteAttachedPolicy(attachedPolicy)

	if err != nil {
		return fmt.Errorf("failed deleting policy %s: %w", *attachedPolicy.PolicyName, err)
	}

	return nil
}

func (applier *Applier) createAndAttachPolicy(role *iam.Role, name string, document string) error {

	policy, err := applier.client.createOrUpdatePolicy(name, document)

	if err != nil {
		return fmt.Errorf("failed creating policy %s: %w", name, err)
	}

	err = applier.client.attachPolicy(role, policy)

	if err != nil {
		return fmt.Errorf("failed attaching policy %s: %w", name, err)
	}

	return nil
}

func (applier *Applier) createRole(name string) (*iam.Role, error) {
	return applier.client.CreateOrUpdateLoginRole(name)
}

func (applier *Applier) updateRole(desiredRole *iamCore.AwsRole, currentRole *iam.Role, policyDocuments map[string]string) error {

	attachedPolicies, err := applier.client.ListAttachedPolicies(currentRole)

	if err != nil {
		return fmt.Errorf("unable to list attached policies: %w", err)
	}

	for _,desiredPolicy := range desiredRole.Policies {
		attachedPolicy := applier.client.lookupAttachedPolicyByArn(attachedPolicies, desiredPolicy.Arn)
		if attachedPolicy == nil {
			policy, err := applier.client.lookupPolicyByArn(desiredPolicy.Arn)

			if err != nil {
				return fmt.Errorf("unable to fetch policy: %w", err)
			}

			if policy == nil {
				return fmt.Errorf("referenced policy with Arn %s does not exist", desiredPolicy.Arn)
			}

			err = applier.client.attachPolicy(currentRole, policy)

			if err != nil {
				return fmt.Errorf("failed attaching policy %s: %w", desiredPolicy.Arn, err)
			}
		}
	}

	for _, desiredPolicy := range desiredRole.DatabaseLoginPolicies {

		desiredPolicyDocument := applier.buildDatabaseLoginPolicyDocument(desiredPolicy)
		policyName :=  desiredPolicy.DatabaseUsername
		attachedPolicy := applier.client.lookupAttachedPolicy(attachedPolicies,policyName)

		if attachedPolicy != nil {
			if desiredPolicyDocument == policyDocuments[policyName] {
				log.Infof("No changes detected in policy %s", policyName)
			} else {
				applier.eventListener.Handle(PolicyUpdated, policyName)
				log.Infof("Updating policy %s attached to %s", policyName, *currentRole.RoleName)

				err := applier.detachAndDeletePolicy(currentRole, attachedPolicy)
				if err != nil {
					return fmt.Errorf("unable to detach and delete policy %s: %w", *attachedPolicy.PolicyName, err)
				}

				err = applier.createAndAttachPolicy(currentRole, policyName, desiredPolicyDocument)
				if err != nil {
					return fmt.Errorf("unable to create and attach policy %s: %w", policyName, err)
				}
			}
		} else {
			applier.eventListener.Handle(PolicyCreated, policyName)
			log.Infof("Creating policy %s and attaching to %s", policyName, *currentRole.RoleName)
			err := applier.createAndAttachPolicy(currentRole, policyName, desiredPolicyDocument)

			if err != nil {
				return fmt.Errorf("unable to create and attach policy %s: %w", policyName, err)
			}
		}
	}

	for _, attachedPolicy := range attachedPolicies {
		if desiredRole.LookupDatabaseLoginPolicyForUsername(*attachedPolicy.PolicyName) == nil {
			applier.eventListener.Handle(PolicyDeleted, *attachedPolicy.PolicyName)
			log.Infof("Deleting policy %s attached to %s", *attachedPolicy.PolicyName, *currentRole.RoleName)

			err = applier.detachAndDeletePolicy(currentRole, attachedPolicy)

			if err != nil {
				return fmt.Errorf("unable to detach and delete policy %s: %w", *attachedPolicy.PolicyName, err)
			}
		}
	}

	return nil
}

func (applier *Applier) deleteRole(role *iam.Role) error {

	attachedPolicies, err := applier.client.ListAttachedPolicies(role)

	if err != nil {
		return err
	}

	for _, attachedPolicy := range attachedPolicies {
		applier.eventListener.Handle(PolicyDeleted, *attachedPolicy.PolicyName)
		log.Infof("Deleting policy %s attached to %s", *attachedPolicy.PolicyName, *role.RoleName)

		err = applier.detachAndDeletePolicy(role, attachedPolicy)

		if err != nil {
			return err
		}
	}

	return applier.client.DeleteLoginRole(role)
}

func (applier *Applier) Apply(model iamCore.Model) error {

	policyDocuments, err := applier.client.GetPolicyDocuments()

	if err != nil {
		return fmt.Errorf("unable to list policy documents: %w", err)
	}

	existingRoles, err := applier.client.ListRoles()

	if err != nil {
		return fmt.Errorf("unable to list roles: %w", err)
	}

	for _, desiredRole := range model.Roles {
		var err error

		existingRole := applier.lookupRole(existingRoles, desiredRole.Name)

		if existingRole == nil {
			applier.eventListener.Handle(RoleCreated, desiredRole.Name)
			log.Infof("Creating role %s", desiredRole.Name)
			existingRole, err = applier.createRole(desiredRole.Name)

			if err != nil {
				return fmt.Errorf("failed when creating role %s: %w", desiredRole.Name, err)
			}
		}

		applier.eventListener.Handle(RoleUpdated, desiredRole.Name)
		log.Infof("Updating role %s", desiredRole.Name)
		err = applier.updateRole(desiredRole, existingRole, policyDocuments)
		if err != nil {
			return fmt.Errorf("failed when updating role %s: %w", desiredRole.Name, err)
		}
	}

	for _, existingRole := range existingRoles {
		if model.LookupRole(*existingRole.RoleName) == nil {
			applier.eventListener.Handle(RoleDeleted, *existingRole.RoleName)
			log.Infof("Deleting role %s", *existingRole.RoleName)
			err = applier.deleteRole(existingRole)

			if err != nil {
				return fmt.Errorf("failed when deleting role %s: %w", *existingRole.RoleName, err)
			}
		}
	}

	return nil
}
