package redshift

import (
	"fmt"
	"github.com/go-logr/logr"
	redshiftCore "github.com/lunarway/hubble-rbac-controller/internal/core/redshift"
)

type Applier interface {
	Apply(model redshiftCore.Model) error
}

type DagBasedApplier struct {
	clientGroup     ClientGroup
	excluded *redshiftCore.Exclusions
	awsAccountId    string
	logger logr.Logger
}

func NewDagBasedApplier(clientGroup ClientGroup, excluded *redshiftCore.Exclusions, awsAccountId string, logger logr.Logger) *DagBasedApplier {
	return &DagBasedApplier{
		clientGroup:     clientGroup,
		excluded: excluded,
		awsAccountId:    awsAccountId,
		logger: logger,
	}
}


func (applier *DagBasedApplier) Apply(model redshiftCore.Model) error {

	err := model.Validate(applier.excluded)

	if err != nil {
		return err
	}

	clientPool := NewClientPool(applier.clientGroup)

	defer clientPool.Close()

	resolver := NewModelResolver(applier.clientGroup, applier.excluded)
	taskRunner := NewTaskRunnerImpl(clientPool, applier.awsAccountId, applier.logger)
	//taskRunner := &redshiftCore.TaskPrinter{}
	dagRunner := redshiftCore.NewSequentialDagRunner(taskRunner, applier.logger)

	var clusterIdentifiers []string
	for _, cluster := range model.Clusters {
		clusterIdentifiers = append(clusterIdentifiers, cluster.Identifier)
	}

	applier.logger.Info("Fetching current model..")

	currentModel, err := resolver.Resolve(clusterIdentifiers)

	if err != nil {
		return err
	}

	applier.logger.Info("Fetching current model DONE")

	applier.logger.Info("Current model", "model", currentModel)
	applier.logger.Info("Desired model", "model", model)

	dagBuilder := redshiftCore.NewReconciliationDagBuilder()
	dag := dagBuilder.Reconcile(currentModel, &model)

	applier.logger.Info("Reconciliation DAG built", "numTasks", dag.NumTasks())

	dagRunner.Run(dag)

	if len(dag.GetFailed()) > 0 {
		return fmt.Errorf("apply failed, %d tasks failed", len(dag.GetFailed()))
	}

	return nil
}

