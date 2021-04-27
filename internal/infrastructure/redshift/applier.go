package redshift

import (
	"fmt"
	"github.com/go-logr/logr"
	"github.com/lunarway/hubble-rbac-controller/internal/core/redshift"
)

type RedshiftClientFactoryAdapter struct {
	clientPool *ClientPool
}

func (f *RedshiftClientFactoryAdapter) GetClusterClient(clusterIdentifier string) (RedshiftClient, error) {
	return f.clientPool.GetClusterClient(clusterIdentifier)
}
func (f *RedshiftClientFactoryAdapter) GetDatabaseClient(clusterIdentifier string, databaseName string) (RedshiftClient, error) {
	return f.clientPool.GetDatabaseClient(clusterIdentifier, databaseName)
}

type Applier struct {
	reconcilerConfig redshift.ReconcilerConfig
	clientGroup      ClientGroup
	excluded         *redshift.Exclusions
	awsAccountId     string
	logger           logr.Logger
}

func NewApplier(clientGroup ClientGroup, excluded *redshift.Exclusions, awsAccountId string, logger logr.Logger, reconcilerConfig redshift.ReconcilerConfig) *Applier {
	return &Applier{
		clientGroup:      clientGroup,
		reconcilerConfig: reconcilerConfig,
		excluded:         excluded,
		awsAccountId:     awsAccountId,
		logger:           logger,
	}
}

func (applier *Applier) Apply(model redshift.Model, dryRun bool) error {

	err := model.Validate(applier.excluded)

	if err != nil {
		return err
	}

	clientPool := NewClientPool(applier.clientGroup)

	defer clientPool.Close()

	resolver := NewModelResolver(&RedshiftClientFactoryAdapter{clientPool: clientPool}, applier.excluded)
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

	applier.logger.Info("Current model fetched")

	dag := redshift.Reconcile(currentModel, &model, applier.reconcilerConfig)

	applier.logger.Info("Reconciliation DAG built", "numTasks", dag.NumTasks())

	dagRunner.Run(dag)

	if len(dag.GetFailed()) > 0 {
		return fmt.Errorf("apply failed, %d tasks failed", len(dag.GetFailed()))
	}

	return nil
}
