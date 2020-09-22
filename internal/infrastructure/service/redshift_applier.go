package service

import redshiftCore "github.com/lunarway/hubble-rbac-controller/internal/core/redshift"

type RedshiftApplier interface {
	Apply(model redshiftCore.Model, dryRun bool) error
}
