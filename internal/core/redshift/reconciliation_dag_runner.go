package redshift

import (
	"fmt"
	"github.com/go-logr/logr"
)

type SequentialDagRunner struct {
	taskRunner TaskRunner
	logger logr.Logger
}

func NewSequentialDagRunner(taskRunner TaskRunner, logger logr.Logger) *SequentialDagRunner {
	return &SequentialDagRunner{taskRunner: taskRunner, logger: logger}
}

func (d *SequentialDagRunner) Run(dag *ReconciliationDag) {

	for dag.PendingExists() {
		for _, task := range dag.GetWaiting() {
			if task.CannotRun() {
				fmt.Printf("skipping task %s\n", task.identifier)
				task.Skip()
			} else {
				task.Start()
				err := ExecuteTask(d.taskRunner, task)
				if err != nil {
					task.Failed()
					d.logger.Error(err, "task failed")
				} else {
					task.Success()
				}
			}
		}
	}
}
