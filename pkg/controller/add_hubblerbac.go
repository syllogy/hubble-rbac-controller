package controller

import (
	"github.com/lunarway/hubble-rbac-controller/pkg/controller/hubblerbac"
)

func init() {
	// AddToManagerFuncs is a list of functions to create controllers and add them to a manager.
	AddToManagerFuncs = append(AddToManagerFuncs, hubblerbac.Add)
}
