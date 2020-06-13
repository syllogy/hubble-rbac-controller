package google

import googleCore "github.com/lunarway/hubble-rbac-controller/internal/core/google"

type FakeApplier struct {

}

func NewFakeApplier() Applier {
	return &FakeApplier{}
}

func (applier *FakeApplier) Apply(model googleCore.Model) error {
	return nil
}