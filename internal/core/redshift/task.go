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

func (t TaskType) String() string {
	return [...]string{"CreateUser", "DropUser", "CreateGroup", "DropGroup", "CreateSchema",
		"CreateExternalSchema", "CreateDatabase", "GrantAccess", "RevokeAccess", "AddToGroup", "RemoveFromGroup"}[t]
}

type Task struct {
	identifier string
	taskType TaskType
	model interface{}
	upStream []*Task
	downStream []*Task
}

func NewTask(identifier string, taskType TaskType, model interface{}) *Task {
	return &Task{identifier: identifier, taskType: taskType, model: model}
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

	for _,task := range t.upStream {
		upstream = append(upstream, fmt.Sprintf("%s(%s)", task.taskType.String(), task.identifier))
	}

	for _,task := range t.downStream {
		downstream = append(downstream, fmt.Sprintf("%s(%s)", task.taskType.String(), task.identifier))
	}

	return fmt.Sprintf("name: %s(%s), upstream: [%s], downstream: [%s]", t.taskType.String(), t.identifier, strings.Join(upstream, ","), strings.Join(downstream, ","))

}