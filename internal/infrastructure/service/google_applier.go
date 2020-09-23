package service

import googleCore "github.com/lunarway/hubble-rbac-controller/internal/core/google"

type GoogleApplier interface {
	Apply(model googleCore.Model) error
}
