package redshift

import (
	"github.com/go-logr/logr"
)

type SequentialDagRunner struct {
	taskRunner TaskRunner
	logger     logr.Logger
}

func NewSequentialDagRunner(taskRunner TaskRunner, logger logr.Logger) *SequentialDagRunner {
	return &SequentialDagRunner{taskRunner: taskRunner, logger: logger}
}

func (d *SequentialDagRunner) Run(dag *ReconciliationDag) {

	for dag.PendingExists() {
		for _, task := range dag.GetWaiting() {
			if task.CannotRun() {
				d.logger.Info("skipping task", "task", task.String())
				task.Skip()
				continue
			}
			task.Start()
			err := ExecuteTask(d.taskRunner, task)
			if err != nil {
				task.Failed()
				d.logger.Error(err, "task failed", "task", task.String())
				continue
			}
			task.Success()
		}
	}
}

//TODO: implement a DAG runner that can parallelize the execution
