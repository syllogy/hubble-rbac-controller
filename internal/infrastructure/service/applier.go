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

func NewIamLogger(logger logr.Logger) *IamEventRecorder {
	return &IamEventRecorder{logger:logger}
}

func NewRedshiftLogger(logger logr.Logger) *RedshiftEventRecorder {
	return &RedshiftEventRecorder{logger:logger}
}

type Applier struct {
	resolver *resolver.Resolver
	googleApplier google.Applier
	redshiftApplier *redshift.Applier
	iamApplier *iam.Applier
	logger logr.Logger
}

func NewApplier(
	iamApplier *iam.Applier,
	googleApplier google.Applier,
	redshiftApplier *redshift.Applier,
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

	resolved, err := applier.resolver.Resolve(model)

	if err != nil {
		return err
	}

	applier.logger.Info("Applying redshift model", "model", resolved.RedshiftModel)
	if !dryRun {
		err = applier.redshiftApplier.Apply(resolved.RedshiftModel)

		if err != nil {
			return err
		}
	}

	applier.logger.Info("Applying IAM model", "model", resolved.IamModel)
	if !dryRun {
		err = applier.iamApplier.Apply(resolved.IamModel)

		if err != nil {
			return err
		}
	}

	applier.logger.Info("Applying Google model", "model", resolved.GoogleModel)
	if !dryRun {
		err = applier.googleApplier.Apply(resolved.GoogleModel)

		if err != nil {
			return err
		}
	}

	return nil
}
