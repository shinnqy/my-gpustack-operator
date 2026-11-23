package worker

import (
	"testing"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/systemname"
)

func TestDesiredAcceleratorCapacity(t *testing.T) {
	const key = nodefeature.AcceleratableFeatureLabelPrefix + "nvidia-a10g"
	for _, tc := range []struct {
		name       string
		tokens     string
		overcommit bool
		cores      string
	}{
		{"per-card compute budget", "4", false, "100"},
		{"compute overcommit per slice", "4", true, "400"},
		{"token pool unavailable", "0", true, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := &core.Node{
				ObjectMeta: meta.ObjectMeta{Labels: map[string]string{
					systemname.ManagedLabelKey: "true",
					key:                        "true",
					key + ".memory":            "24Gi",
				}},
				Status: core.NodeStatus{Capacity: core.ResourceList{
					"nvidia.com/gpu.sliced": qty(tc.tokens),
				}},
			}
			card := workercore.Accelerator{ID: "gpu-0"}
			card.Status.LogicalSliced.Count = 4
			group := workercore.DevicesGroup{
				ID:           "a10g",
				Manufacturer: nodefeature.ManufacturerNVIDIA,
				Accelerators: []workercore.Accelerator{card},
			}
			group.AcceleratorSlicedDetail.Logical.CoresPercentageOvercommit = tc.overcommit
			devs := &workercore.Devices{Spec: workercore.DevicesSpec{Groups: []workercore.DevicesGroup{group}}}
			want := core.ResourceList{}
			if tc.cores != "" {
				want = core.ResourceList{
					"nvidia.com/gpu.sliced.units":             qty("1600000"),
					"nvidia.com/gpu.sliced.cores-percentage":  qty(tc.cores),
					"nvidia.com/gpu.sliced.memory-percentage": qty("100"),
					"nvidia.com/gpu.sliced.memory-mib":        qty("24576"),
				}
			}
			assertResourceList(t, want, desiredAcceleratorCapacity(node, devs), "capacity")
		})
	}
}
