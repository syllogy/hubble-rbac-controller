package service

import (
	"github.com/go-logr/logr"
	"github.com/lunarway/hubble-rbac-controller/internal/core/hubble"
	"github.com/lunarway/hubble-rbac-controller/internal/core/resolver"
	"github.com/lunarway/hubble-rbac-controller/internal/infrastructure/google"
	"github.com/lunarway/hubble-rbac-controller/internal/infrastructure/iam"
	"github.com/lunarway/hubble-rbac-controller/internal/infrastructure/redshift"
)


type RedshiftEventRecorder struct {
	logger logr.Logger
}

func (e *RedshiftEventRecorder) Handle(eventType redshift.ApplyEventType, name string) {
	e.logger.Info("Event occurred", "eventType", eventType.ToString(), "name", name)
}

type IamEventRecorder struct {
	logger logr.Logger
}

func (e *IamEventRecorder) Handle(eventType iam.ApplyEventType, name string) {
	e.logger.Info("Event occurred", "eventType", eventType.ToString(), "name", name)
}


type Applier struct {
	resolver *resolver.Resolver
	googleApplier *google.Applier
	redshiftApplier *redshift.Applier
	iamApplier *iam.Applier
	logger logr.Logger
}

func NewApplier(clientGroup redshift.ClientGroup, iamClient *iam.Client, googleClient *google.Client, excludedUsers []string, awsAccountId string, awsRegion string, logger logr.Logger) *Applier {

	excludedSchemas := []string{"public"}
	excludedDatabases := []string{"template0", "template1", "postgres", "padb_harvest"}

	return &Applier{
		resolver: &resolver.Resolver{},
		redshiftApplier: redshift.NewApplier(clientGroup, excludedDatabases, excludedUsers, excludedSchemas, &RedshiftEventRecorder{logger:logger}, awsAccountId, logger),
		iamApplier: iam.NewApplier(iamClient, awsAccountId, awsRegion, &IamEventRecorder{logger:logger}, logger),
		googleApplier: google.NewApplier(googleClient),
		logger: logger,
	}
}

func (applier *Applier) Apply(model hubble.Model, dryRun bool) error {

	applier.logger.Info("Received hubble model", "model", model)

	resolved, err := applier.resolver.Resolve(model)

	if err != nil {
		return err
	}

	//applier.logger.Info("Applying redshift model", "model", resolved.RedshiftModel)
	//if !dryRun {
	//	err = applier.redshiftApplier.Apply(resolved.RedshiftModel)
	//
	//	if err != nil {
	//		return err
	//	}
	//}

	//applier.logger.Info("Applying IAM model", "model", resolved.IamModel)
	//if !dryRun {
	//	err = applier.iamApplier.Apply(resolved.IamModel)
	//
	//	if err != nil {
	//		return err
	//	}
	//}

	applier.logger.Info("Applying Google model", "model", resolved.GoogleModel)
	if !dryRun {
		err = applier.googleApplier.Apply(resolved.GoogleModel)

		if err != nil {
			return err
		}
	}

	return nil
}
