package service

import (
	"github.com/go-logr/logr"
	"github.com/lunarway/hubble-rbac-controller/internal/core/hubble"
	"github.com/lunarway/hubble-rbac-controller/internal/core/resolver"
	"github.com/lunarway/hubble-rbac-controller/internal/infrastructure/google"
	"github.com/lunarway/hubble-rbac-controller/internal/infrastructure/iam"
	"github.com/lunarway/hubble-rbac-controller/internal/infrastructure/redshift"
)


type IamEventRecorder struct {
	logger logr.Logger
}

func (e *IamEventRecorder) Handle(eventType iam.ApplyEventType, name string) {
	e.logger.Info("Event occurred", "eventType", eventType.ToString(), "name", name)
}

func NewIamLogger(logger logr.Logger) *IamEventRecorder {
	return &IamEventRecorder{logger:logger}
}

type Applier struct {
	resolver *resolver.Resolver
	googleApplier google.Applier
	redshiftApplier redshift.Applier
	iamApplier *iam.Applier
	logger logr.Logger
}

func NewApplier(
	iamApplier *iam.Applier,
	googleApplier google.Applier,
	redshiftApplier redshift.Applier,
	logger logr.Logger) *Applier {

	return &Applier{
		resolver: &resolver.Resolver{},
		redshiftApplier: redshiftApplier,
		iamApplier: iamApplier,
		googleApplier: googleApplier,
		logger: logger,
	}
}


func (applier *Applier) Apply(model hubble.Model, dryRun bool) error {

	applier.logger.Info("Received hubble model", "model", model)

	redshiftModel, iamModel, googleModel, err := applier.resolver.Resolve(model)

	if err != nil {
		return err
	}

	applier.logger.Info("Applying redshift model", "model", redshiftModel)
	err = applier.redshiftApplier.Apply(redshiftModel, dryRun)

	if err != nil {
		return err
	}

	applier.logger.Info("Applying IAM model", "model", iamModel)
	if !dryRun {
		err = applier.iamApplier.Apply(iamModel)

		if err != nil {
			return err
		}
	}

	applier.logger.Info("Applying Google model", "model", googleModel)
	if !dryRun {
		err = applier.googleApplier.Apply(googleModel)

		if err != nil {
			return err
		}
	}

	applier.logger.Info("All changes have been applied")

	return nil
}
