package redshift

import (
	"fmt"
	"strings"
)

type TaskType int

const (
	CreateUser TaskType = iota
	DropUser
	CreateGroup
	DropGroup
	CreateSchema
	CreateExternalSchema
	CreateDatabase
	GrantAccess
	RevokeAccess
	AddToGroup
	RemoveFromGroup
)

type TaskState int

const (
	Running TaskState = iota
	Pending
	Success
	Failed
	Skipped
)

func (t TaskType) String() string {
	return [...]string{"CreateUser", "DropUser", "CreateGroup", "DropGroup", "CreateSchema",
		"CreateExternalSchema", "CreateDatabase", "GrantAccess", "RevokeAccess", "AddToGroup", "RemoveFromGroup"}[t]
}

type Equatable interface {
	Equals(other Equatable) bool
}

type Task struct {
	identifier string
	taskType   TaskType
	model      Equatable
	upStream   []*Task
	downStream []*Task
	state      TaskState
}

func NewTask(identifier string, taskType TaskType, model Equatable) *Task {
	return &Task{identifier: identifier, taskType: taskType, model: model, state: Pending}
}

func (t *Task) Skip() {
	t.state = Skipped
}

func (t *Task) Success() {
	t.state = Success
}

func (t *Task) Failed() {
	t.state = Failed
}

func (t *Task) Start() {
	t.state = Running
}

func (t *Task) isDone() bool {
	return t.state == Success || t.state == Failed || t.state == Skipped
}

func (t *Task) IsWaiting() bool {
	return t.allUpstreamDone() && t.state == Pending
}

func (t *Task) CannotRun() bool {

	for _, parent := range t.upStream {
		if parent.state == Failed || parent.state == Skipped {
			return true
		}
	}
	return false
}

func (t *Task) allUpstreamDone() bool {
	for _, parent := range t.upStream {
		if !parent.isDone() {
			return false
		}
	}
	return true
}

func (t *Task) isUpstream(other *Task) bool {
	for _, x := range t.upStream {
		if x == other {
			return true
		}
	}
	return false
}

func (t *Task) isDownstream(other *Task) bool {
	for _, x := range t.downStream {
		if x == other {
			return true
		}
	}
	return false
}

func (t *Task) dependsOn(other *Task) {

	if !t.isUpstream(other) {
		t.upStream = append(t.upStream, other)
	}

	if !other.isDownstream(t) {
		other.downStream = append(other.downStream, t)
	}
}

func (t *Task) String() string {

	var upstream []string
	var downstream []string

	for _, task := range t.upStream {
		upstream = append(upstream, fmt.Sprintf("%s(%s)", task.taskType.String(), task.identifier))
	}

	for _, task := range t.downStream {
		downstream = append(downstream, fmt.Sprintf("%s(%s)", task.taskType.String(), task.identifier))
	}

	return fmt.Sprintf("name: %s(%s), upstream: [%s], downstream: [%s]", t.taskType.String(), t.identifier, strings.Join(upstream, ","), strings.Join(downstream, ","))

}
