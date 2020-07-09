package redshift

import (
	"fmt"
	"github.com/go-logr/logr"
	redshiftCore "github.com/lunarway/hubble-rbac-controller/internal/core/redshift"
)

type Applier interface {
	Apply(model redshiftCore.Model, dryRun bool) error
}

type ApplierImpl struct {
	reconcilerConfig redshiftCore.ReconcilerConfig
	clientGroup     ClientGroup
	excluded *redshiftCore.Exclusions
	awsAccountId    string
	logger logr.Logger
}

func NewApplier(clientGroup ClientGroup, excluded *redshiftCore.Exclusions, awsAccountId string, logger logr.Logger, reconcilerConfig redshiftCore.ReconcilerConfig) *ApplierImpl {
	return &ApplierImpl{
		clientGroup:     clientGroup,
		reconcilerConfig:reconcilerConfig,
		excluded: excluded,
		awsAccountId:    awsAccountId,
		logger: logger,
	}
}

func (applier *ApplierImpl) Apply(model redshiftCore.Model, dryRun bool) error {

	err := model.Validate(applier.excluded)

	if err != nil {
		return err
	}

	clientPool := NewClientPool(applier.clientGroup)

	defer clientPool.Close()

	resolver := NewModelResolver(applier.clientGroup, applier.excluded)
	var taskRunner redshiftCore.TaskRunner
	if dryRun {
		taskRunner = &redshiftCore.TaskPrinter{}
	} else {
		taskRunner = NewTaskRunnerImpl(clientPool, applier.awsAccountId, applier.logger)
	}

	dagRunner := redshiftCore.NewSequentialDagRunner(taskRunner, applier.logger)

	var clusterIdentifiers []string
	for _, cluster := range model.Clusters {
		clusterIdentifiers = append(clusterIdentifiers, cluster.Identifier)
	}

	applier.logger.Info("Fetching current model...")

	currentModel, err := resolver.Resolve(clusterIdentifiers)

	if err != nil {
		return err
	}

	applier.logger.Info("Current model fetched", "model", currentModel)

	dag := redshiftCore.Reconcile(currentModel, &model, applier.reconcilerConfig)

	applier.logger.Info("Reconciliation DAG built", "numTasks", dag.NumTasks())

	dagRunner.Run(dag)

	if len(dag.GetFailed()) > 0 {
		return fmt.Errorf("apply failed, %d tasks failed", len(dag.GetFailed()))
	}

	return nil
}

