package redshift

import (
	"fmt"
)

type ReconciliationDag struct {
	tasks []*Task
}

func NewDag(tasks []*Task) *ReconciliationDag {
	return &ReconciliationDag{tasks: tasks}
}

func (d *ReconciliationDag) NumTasks() int {
	return len(d.tasks)
}

func (d *ReconciliationDag) GetWaiting() []*Task {
	var result []*Task

	for _, task := range d.tasks {
		if task.IsWaiting() {
			result = append(result, task)
		}
	}
	return result
}

func (d *ReconciliationDag) GetFailed() []*Task {
	var result []*Task

	for _, task := range d.tasks {
		if task.state == Failed {
			result = append(result, task)
		}
	}
	return result
}

func (d *ReconciliationDag) PendingExists() bool {
	for _, task := range d.tasks {
		if task.state == Pending {
			return true
		}
	}
	return false
}

func (d *ReconciliationDag) String() string {
	var result string

	for _, t := range d.tasks {
		result += fmt.Sprintf("%s\n", t.String())
	}

	return result
}