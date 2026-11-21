package deviceplugin

import (
	core "k8s.io/api/core/v1"
)

// SlicedCoresPercent reads the per-accelerator compute (SM) percent a sliced container asks
// for from its ".sliced.cores-percentage" request, defaulting to 100 (a whole accelerator's
// compute) when absent or non-positive. Slices may oversubscribe compute, so each
// slice may request up to 100%. It drives HAMi-core CUDA_DEVICE_SM_LIMIT / vcann-rt
// aicore-quota directly (already a percent, no fraction conversion).
func SlicedCoresPercent(ctr *core.Container, coresResName core.ResourceName) int {
	if q, ok := ctr.Resources.Limits[coresResName]; ok {
		if v := q.Value(); v > 0 {
			return int(v)
		}
	}
	return 100
}

// ContainerEnvDeclared reports whether the container explicitly declares an env var named
// name in its Env list, so a caller can skip injecting a default and leave that value
// authoritative. Only explicit Env entries are checked; EnvFrom-sourced variables are not.
func ContainerEnvDeclared(ctr *core.Container, name string) bool {
	for i := range ctr.Env {
		if ctr.Env[i].Name == name {
			return true
		}
	}
	return false
}
