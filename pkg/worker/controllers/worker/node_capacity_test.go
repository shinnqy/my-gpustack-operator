package worker

import (
	"testing"

	"github.com/stretchr/testify/assert"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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

func TestBuildAcceleratorCapacityPatch(t *testing.T) {
	const (
		units = "nvidia.com/gpu.sliced.units"
		mib   = "nvidia.com/gpu.sliced.memory-mib"
	)
	mkCap := func(entries map[string]string) core.ResourceList {
		out := core.ResourceList{}
		for k, v := range entries {
			out[core.ResourceName(k)] = resource.MustParse(v)
		}
		return out
	}

	cases := []struct {
		name    string
		desired core.ResourceList
		current core.ResourceList
		want    map[string]any // nil 表示无需修改节点容量。
	}{
		{
			// Patch 中使用 Kubernetes 的标准数量字符串，6400000 会表示成 6400k。
			name:    "set on a node without the key",
			desired: mkCap(map[string]string{units: "6400000"}),
			current: mkCap(map[string]string{"cpu": "8"}),
			want:    map[string]any{units: "6400k"},
		},
		{
			name:    "no change when already equal",
			desired: mkCap(map[string]string{units: "6400000"}),
			current: mkCap(map[string]string{units: "6400000", "cpu": "8"}),
			want:    nil,
		},
		{
			name:    "update when value differs",
			desired: mkCap(map[string]string{units: "6400000"}),
			current: mkCap(map[string]string{units: "3200000"}),
			want:    map[string]any{units: "6400k"},
		},
		{
			// 从期望容量消失的受管切片资源应置为 null，以移除旧键。
			name:    "remove stale sliced keys when the model disappears",
			desired: nil,
			current: mkCap(map[string]string{units: "6400000", mib: "98304", "cpu": "8"}),
			want:    map[string]any{units: nil, mib: nil},
		},
		{
			// 最后一张提供某个分区 profile 的卡离开后，也应移除该 profile 的容量键。
			name:    "remove a stale per-profile key when the last card offering it leaves",
			desired: nil,
			current: mkCap(map[string]string{"nvidia.com/gpu.partitioned.mig-1g.10gb": "7", "cpu": "8"}),
			want:    map[string]any{"nvidia.com/gpu.partitioned.mig-1g.10gb": nil},
		},
		{
			name:    "bare .sliced and .shared device-plugin keys are left untouched",
			desired: nil,
			current: mkCap(map[string]string{"nvidia.com/gpu.sliced": "2048", "nvidia.com/gpu.shared": "10", "cpu": "8"}),
			want:    nil,
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := buildAcceleratorCapacityPatch(c.desired, c.current)
			if c.want == nil {
				assert.Nil(t, got)
				return
			}
			assert.Equal(t, c.want, got)
		})
	}
}
