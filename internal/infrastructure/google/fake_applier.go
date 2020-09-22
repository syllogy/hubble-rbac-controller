package google

import googleCore "github.com/lunarway/hubble-rbac-controller/internal/core/google"

type NoOpApplier struct {
}

func NewNoOpApplier() *NoOpApplier {
	return &NoOpApplier{}
}

func (applier *NoOpApplier) Apply(model googleCore.Model) error {
	return nil
}
