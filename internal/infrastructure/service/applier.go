package service

import (
	"encoding/json"
	"fmt"
	"github.com/lunarway/hubble-rbac-controller/internal/core/hubble"
	"github.com/lunarway/hubble-rbac-controller/internal/core/resolver"
	"github.com/lunarway/hubble-rbac-controller/internal/infrastructure/google"
	"github.com/lunarway/hubble-rbac-controller/internal/infrastructure/iam"
	"github.com/lunarway/hubble-rbac-controller/internal/infrastructure/redshift"
	log "github.com/sirupsen/logrus"
)


type RedshiftEventRecorder struct {
}

func (e *RedshiftEventRecorder) Handle(eventType redshift.ApplyEventType, name string) {
	log.Infof("Event %s:%s occurred", eventType.ToString(), name)
}

type IamEventRecorder struct {
}

func (e *IamEventRecorder) Handle(eventType iam.ApplyEventType, name string) {
	log.Infof("Event %s:%s occurred", eventType.ToString(), name)
}


type Applier struct {
	resolver *resolver.Resolver
	googleApplier *google.Applier
	redshiftApplier *redshift.Applier
	iamApplier *iam.Applier
}

func NewApplier(clientGroup *redshift.ClientGroup, iamClient *iam.Client, googleClient *google.Client, unmanagedUsers []string, awsAccountId string, awsRegion string) *Applier {

	unmanagedSchemas := []string{"public"}

	return &Applier{
		resolver: &resolver.Resolver{},
		redshiftApplier: redshift.NewApplier(clientGroup, unmanagedUsers, unmanagedSchemas, &RedshiftEventRecorder{}, awsAccountId),
		iamApplier: iam.NewApplier(iamClient, awsAccountId, awsRegion, &IamEventRecorder{}),
		googleApplier: google.NewApplier(googleClient),
	}
}

func (applier *Applier) toString(model interface{}) string {
	s, _ := json.MarshalIndent(model, "", "   ")
	return fmt.Sprintf("%s", s)
}

func (applier *Applier) Apply(model hubble.Model, dryRun bool) error {

	log.Info(fmt.Sprintf("Received hubble model:\n%s", applier.toString(model)))

	resolved, err := applier.resolver.Resolve(model)

	if err != nil {
		return err
	}

	log.Info(fmt.Sprintf("Applying redshift model:\n%s", applier.toString(resolved.RedshiftModel)))
	if !dryRun {
		err = applier.redshiftApplier.Apply(resolved.RedshiftModel)

		if err != nil {
			return err
		}
	}

	log.Info(fmt.Sprintf("Applying IAM model:\n%s", applier.toString(resolved.IamModel)))
	if !dryRun {
		err = applier.iamApplier.Apply(resolved.IamModel)

		if err != nil {
			return err
		}
	}

	log.Info(fmt.Sprintf("Applying Google model:\n%s", applier.toString(resolved.GoogleModel)))
	if !dryRun {
		err = applier.googleApplier.Apply(resolved.GoogleModel)

		if err != nil {
			return err
		}
	}

	return nil
}
