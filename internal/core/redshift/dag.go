package redshift

import (
	"fmt"
)

type DagModel interface {
	Equals(other DagModel) bool
}

type ManageAccessModel struct {
	Database   *Database
	SchemaName string
	GroupName  string
}

func (s *ManageAccessModel) Equals(rhs DagModel) bool {
	other, ok := rhs.(*ManageAccessModel)
	if !ok {
		return false
	}
	return s.Database.Name == other.Database.Name &&
		s.GroupName == other.GroupName &&
		s.SchemaName == other.SchemaName
}

type ManageMembershipModel struct {
	ClusterIdentifier string
	Username          string
	GroupName         string
}

func (s *ManageMembershipModel) Equals(rhs DagModel) bool {
	other, ok := rhs.(*ManageMembershipModel)
	if !ok {
		return false
	}
	return s.Username == other.Username &&
		s.GroupName == other.GroupName &&
		s.ClusterIdentifier == other.ClusterIdentifier
}

type DatabaseModel struct {
	Database          *Database
	ClusterIdentifier string
}

func (s *DatabaseModel) Equals(rhs DagModel) bool {
	other, ok := rhs.(*DatabaseModel)
	if !ok {
		return false
	}
	return s.Database.Name == other.Database.Name &&
		s.ClusterIdentifier == other.ClusterIdentifier
}

type UserModel struct {
	User              *User
	ClusterIdentifier string
}

func (s *UserModel) Equals(rhs DagModel) bool {
	other, ok := rhs.(*UserModel)
	if !ok {
		return false
	}
	return s.User.Name == other.User.Name &&
		s.ClusterIdentifier == other.ClusterIdentifier
}

type GroupModel struct {
	Group             *Group
	ClusterIdentifier string
}

func (s *GroupModel) Equals(rhs DagModel) bool {
	other, ok := rhs.(*GroupModel)
	if !ok {
		return false
	}
	return s.Group.Name == other.Group.Name &&
		s.ClusterIdentifier == other.ClusterIdentifier
}

type SchemaModel struct {
	Schema   *Schema
	Database *Database
}

func (s *SchemaModel) Equals(rhs DagModel) bool {
	other, ok := rhs.(*SchemaModel)
	if !ok {
		return false
	}
	return s.Database.ClusterIdentifier == other.Database.ClusterIdentifier &&
		s.Database.Name == other.Database.Name &&
		s.Schema.Name == s.Schema.Name
}

type ExternalSchemaModel struct {
	Schema   *ExternalSchema
	Database *Database
}

func (s *ExternalSchemaModel) Equals(rhs DagModel) bool {
	other, ok := rhs.(*ExternalSchemaModel)
	if !ok {
		return false
	}
	return s.Database.ClusterIdentifier == other.Database.ClusterIdentifier &&
		s.Database.Name == other.Database.Name &&
		s.Schema.Name == s.Schema.Name
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