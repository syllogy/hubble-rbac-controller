package redshift

import (
	"fmt"
)

type ManageAccessModel struct {
	Database   *Database
	SchemaName string
	GroupName  string
}

type ManageMembershipModel struct {
	ClusterIdentifier string
	Username          string
	GroupName         string
}

type DatabaseModel struct {
	Database          *Database
	ClusterIdentifier string
}

type UserModel struct {
	User              *User
	ClusterIdentifier string
}

type GroupModel struct {
	Group             *Group
	ClusterIdentifier string
}

type SchemaModel struct {
	Schema   *Schema
	Database *Database
}

type ExternalSchemaModel struct {
	Schema   *ExternalSchema
	Database *Database
}

type Dag struct {
	tasks []*Task
}

func NewDag(tasks []*Task) *Dag {
	return &Dag{tasks: tasks}
}

func (d *Dag) NumTasks() int {
	return len(d.tasks)
}

func (d *Dag) GetWaiting() []*Task {
	var result []*Task

	for _, task := range d.tasks {
		if task.IsWaiting() {
			result = append(result, task)
		}
	}
	return result
}

func (d *Dag) GetFailed() []*Task {
	var result []*Task

	for _, task := range d.tasks {
		if task.state == Failed {
			result = append(result, task)
		}
	}
	return result
}

func (d *Dag) PendingExists() bool {
	for _, task := range d.tasks {
		if task.state == Pending {
			return true
		}
	}
	return false
}

func (d *Dag) String() string {
	var result string

	for _, t := range d.tasks {
		result += fmt.Sprintf("%s\n", t.String())
	}

	return result
}