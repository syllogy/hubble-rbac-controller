package redshift

import (
	"fmt"
	"github.com/go-logr/logr"
	"github.com/lunarway/hubble-rbac-controller/internal/core/redshift"
)

type Applier struct {
	reconcilerConfig redshift.ReconcilerConfig
	clientGroup      ClientGroup
	excluded         *redshift.Exclusions
	awsAccountId     string
	logger           logr.Logger
	externalSchemas  []string
}

func NewApplier(clientGroup ClientGroup, excluded *redshift.Exclusions, awsAccountId string, logger logr.Logger, reconcilerConfig redshift.ReconcilerConfig, externalSchemas []string) *Applier {
	return &Applier{
		clientGroup:      clientGroup,
		reconcilerConfig: reconcilerConfig,
		excluded:         excluded,
		awsAccountId:     awsAccountId,
		logger:           logger,
		externalSchemas:  externalSchemas,
	}
}

func (applier *Applier) Apply(model redshift.Model, dryRun bool) error {

	err := model.Validate(applier.excluded)

	if err != nil {
		return err
	}

	clientPool := NewClientPool(applier.clientGroup)

	defer clientPool.Close()

	resolver := NewModelResolver(applier.clientGroup, applier.excluded, applier.externalSchemas)
	var taskRunner redshift.TaskRunner
	if dryRun {
		taskRunner = redshift.NewTaskPrinter(applier.logger)
	} else {
		taskRunner = NewTaskRunnerImpl(clientPool, applier.awsAccountId, applier.logger)
	}

	dagRunner := redshift.NewSequentialDagRunner(taskRunner, applier.logger)

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

	dag := redshift.Reconcile(currentModel, &model, applier.reconcilerConfig)

	applier.logger.Info("Reconciliation DAG built", "numTasks", dag.NumTasks())

	dagRunner.Run(dag)

	if len(dag.GetFailed()) > 0 {
		return fmt.Errorf("apply failed, %d tasks failed", len(dag.GetFailed()))
	}

	return nil
}
