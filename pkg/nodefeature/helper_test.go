package nodefeature

import (
	"testing"

	"github.com/stretchr/testify/assert"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/systemname"
)

// mergeLabels 从左到右合并给定的标签映射。
func mergeLabels(ms ...map[string]string) map[string]string {
	ret := map[string]string{}
	for _, m := range ms {
		for k, v := range m {
			ret[k] = v
		}
	}
	return ret
}

// cpuModelLabels 反映了驱动通用（CPU）节点键的 NFD cpu 源标签，此处为 AMD 系列 25 型号 1（-> "amd-25-1"）。
func cpuModelLabels() map[string]string {
	return map[string]string{
		_NFDCPUModelVendorIDLabelKey: "AMD",
		_NFDCPUModelFamilyLabelKey:   "25",
		_NFDCPUModelIDLabelKey:       "1",
	}
}

func TestConstructAcceleratableNodeLabels(t *testing.T) {
	cases := []struct {
		name     string
		groups   device.DevicesGroupList
		expected map[string]string
	}{
		{
			name:     "no groups",
			groups:   nil,
			expected: map[string]string{},
		},
		{
			name: "group without accelerators is skipped",
			groups: device.DevicesGroupList{
				{
					ID:           "tesla-t4",
					Manufacturer: "nvidia",
					Name:         "Tesla T4",
					Memory:       15360,
				},
			},
			expected: map[string]string{},
		},
		{
			name: "full group",
			groups: device.DevicesGroupList{
				{
					ID:                "tesla-t4",
					Manufacturer:      "nvidia",
					Name:              "Tesla T4", // 练习空间 -> 破折号净化
					Memory:            15360,
					Cores:             2560,
					DriverVersion:     "580.126.09",
					RuntimeVersion:    "13.0",
					ComputeCapability: "7.5",
					Family:            "Turing",
					Accelerators: []device.Accelerator{
						{ID: "GPU-0", Index: 0},
						{ID: "GPU-1", Index: 1},
						{ID: "GPU-2", Index: 2},
						{ID: "GPU-3", Index: 3},
					},
				},
			},
			expected: map[string]string{
				FeatureLabelPrefix + "acceleratable":                        "true",
				AcceleratableFeatureLabelPrefix + "nvidia":                  "true",
				AcceleratableFeatureLabelPrefix + "nvidia.driver-version":   "580.126.09",
				AcceleratableFeatureLabelPrefix + "nvidia.runtime-version":  "13.0",
				AcceleratableFeatureLabelPrefix + "nvidia-tesla-t4":         "true",
				AcceleratableFeatureLabelPrefix + "nvidia-tesla-t4.product": "Tesla-T4",
				AcceleratableFeatureLabelPrefix + "nvidia-tesla-t4.memory":  "15Gi",
				AcceleratableFeatureLabelPrefix + "nvidia-tesla-t4.cores":   "2560",
				AcceleratableFeatureLabelPrefix + "nvidia-tesla-t4.family":  "Turing",
				AcceleratableFeatureLabelPrefix + "nvidia-tesla-t4.comcap":  "7.5",
				AcceleratableFeatureLabelPrefix + "nvidia-tesla-t4.count":   "4",
			},
		},
		{
			name: "minimal group without optional fields",
			groups: device.DevicesGroupList{
				{
					ID:           "h100",
					Manufacturer: "nvidia",
					Name:         "H100",
					Memory:       81920,
					Accelerators: []device.Accelerator{
						{ID: "GPU-0", Index: 0},
					},
				},
			},
			expected: map[string]string{
				FeatureLabelPrefix + "acceleratable":                    "true",
				AcceleratableFeatureLabelPrefix + "nvidia":              "true",
				AcceleratableFeatureLabelPrefix + "nvidia-h100":         "true",
				AcceleratableFeatureLabelPrefix + "nvidia-h100.product": "H100",
				AcceleratableFeatureLabelPrefix + "nvidia-h100.memory":  "80Gi",
				AcceleratableFeatureLabelPrefix + "nvidia-h100.cores":   "0",
				AcceleratableFeatureLabelPrefix + "nvidia-h100.count":   "1",
			},
		},
		{
			name: "multiple groups, one skipped",
			groups: device.DevicesGroupList{
				{
					ID:           "tesla-t4",
					Manufacturer: "nvidia",
					Name:         "Tesla-T4",
					Memory:       15360,
					Accelerators: []device.Accelerator{
						{ID: "GPU-0", Index: 0},
					},
				},
				{
					// 跳过——没有加速器。
					ID:           "skipped",
					Manufacturer: "nvidia",
					Name:         "Skipped",
				},
				{
					ID:           "mi300x",
					Manufacturer: "amd",
					Name:         "MI300X",
					Memory:       196608,
					Accelerators: []device.Accelerator{
						{ID: "GPU-0", Index: 0},
						{ID: "GPU-1", Index: 1},
					},
				},
			},
			expected: map[string]string{
				FeatureLabelPrefix + "acceleratable":                        "true",
				AcceleratableFeatureLabelPrefix + "nvidia":                  "true",
				AcceleratableFeatureLabelPrefix + "nvidia-tesla-t4":         "true",
				AcceleratableFeatureLabelPrefix + "nvidia-tesla-t4.product": "Tesla-T4",
				AcceleratableFeatureLabelPrefix + "nvidia-tesla-t4.memory":  "15Gi",
				AcceleratableFeatureLabelPrefix + "nvidia-tesla-t4.cores":   "0",
				AcceleratableFeatureLabelPrefix + "nvidia-tesla-t4.count":   "1",
				AcceleratableFeatureLabelPrefix + "amd":                     "true",
				AcceleratableFeatureLabelPrefix + "amd-mi300x":              "true",
				AcceleratableFeatureLabelPrefix + "amd-mi300x.product":      "MI300X",
				AcceleratableFeatureLabelPrefix + "amd-mi300x.memory":       "192Gi",
				AcceleratableFeatureLabelPrefix + "amd-mi300x.cores":        "0",
				AcceleratableFeatureLabelPrefix + "amd-mi300x.count":        "2",
			},
		},
	}

	for _, cs := range cases {
		t.Run(cs.name, func(t *testing.T) {
			actual := ConstructAcceleratableNodeLabels(cs.groups)
			assert.Equal(t, cs.expected, actual, "unexpected node labels")
		})
	}
}

func TestConstructNodeCapacityLabels(t *testing.T) {
	// ConstructNodeCapacityLabels 功能已被移除：它不再将任何单位级别的容量（如 .cpu/.ram/.storage）或
	// Kueue 配置类型（如 .z-flavor/.z-queue/.z-cohort）标签附加到节点上。现在它仅输出被管理的标记以及
	// 通用（CPU）键的存在情况——即制造商标签、完整键标签以及完整键的 .count 子元素（即向上取整的 CPU
	// 容量）——因此 NodeFlavorReconciler 能够将仅使用 CPU 的节点放入资源池，并将配置类型与相同大小的节
	// 点绑定。配置类型的推导工作转移到了 ExtractNodeFlavors 中。单位规格在 InstanceType 上被设定为固
	// 定默认值，不再在此处推导。
	//
	// AMD 系列 25 型号 1 的 CPU 的 CPU 模型标签对应通用密钥“`amd-25-1`”（无操作系统/架构后缀）。该密
	// 钥派生过程的通用回退机制由 TestExtractGeneralNodeKey 处理。

	// newNode 根据给定的标签以及一个代表性的 status.capacity 值构建节点。特意填充了容量值，以证明它不
	// 会泄露到输出的标签中。
	newNode := func(labels map[string]string) *core.Node {
		if labels == nil {
			labels = map[string]string{}
		}
		labels[core.LabelOSStable] = "linux"
		labels[core.LabelArchStable] = "amd64"
		capacity := core.ResourceList{
			core.ResourceCPU:              resource.MustParse("16"),
			core.ResourceMemory:           resource.MustParse("31Gi"),
			core.ResourceEphemeralStorage: resource.MustParse("97Gi"),
		}
		return &core.Node{
			ObjectMeta: meta.ObjectMeta{Name: "node-1", Labels: labels},
			Status:     core.NodeStatus{Capacity: capacity, Allocatable: capacity},
		}
	}

	// deviceLabels 反映了在调用 ConstructNodeCapacityLabels 之前 ConstructAcceleratableNodeLabels 所
	// 设置的节点信息。输出中不得添加任何与容量相关的标签。
	deviceLabels := func(id, product, memory, accelerators string) map[string]string {
		aKey := AcceleratableFeatureLabelPrefix + "nvidia-" + id
		return map[string]string{
			NodeAcceleratableLabelKey:                  "true",
			AcceleratableFeatureLabelPrefix + "nvidia": "true",
			aKey:              "true",
			aKey + ".product": product,
			aKey + ".memory":  memory,
			aKey + ".cores":   "0",
			aKey + ".count":   accelerators,
		}
	}

	// generalPresence 代表整个通用（CPU）视图。ConstructNodeCapacityLabels 现在会输出：受管理的标记以
	// 及制造商/关键要素存在标签。
	generalPresence := func(manu, key string) map[string]string {
		return map[string]string{
			systemname.ManagedLabelKey:                 "true",
			NodeCPUOnlyLabelKey:                        "true",
			GeneralFeatureLabelPrefix + manu:           "true",
			GeneralFeatureLabelPrefix + key:            "true",
			GeneralFeatureLabelPrefix + key + ".count": "16",
		}
	}

	cases := []struct {
		name     string
		node     *core.Node
		opts     []ConstructNodeCapacityLabelsOption
		expected map[string]string
	}{
		{
			// AMD CPU 型号标签 → 通用键 "amd-25-1"。
			name:     "amd cpu-model node emits general presence only",
			node:     newNode(cpuModelLabels()),
			expected: generalPresence("amd", "amd-25-1"),
		},
		{
			// 完全没有 NFD 的 CPU 信息——通用键则回退到 "generic"。
			name:     "cpu-only node without cpu information falls back to generic",
			node:     newNode(nil),
			expected: generalPresence("generic", "generic"),
		},
		{
			// Intel CPU 型号标签 → 通用键 "intel-6-143"。
			name: "intel cpu-model node keys off intel-6-143",
			node: newNode(map[string]string{
				_NFDCPUModelVendorIDLabelKey: "Intel",
				_NFDCPUModelFamilyLabelKey:   "6",
				_NFDCPUModelIDLabelKey:       "143",
			}),
			expected: generalPresence("intel", "intel-6-143"),
		},
		{
			// cpu-name 注释作为通用键。
			name: "cpu-name annotation leads the general key",
			node: func() *core.Node {
				nd := newNode(cpuModelLabels())
				nd.Annotations = map[string]string{
					FeatureLabelPrefix + "cpu-name": "AMD EPYC 7763 64-Core Processor",
				}
				return nd
			}(),
			expected: generalPresence("amd", "amd-epyc-7763"),
		},
		{
			// 加速器标签存在，但该设备不提供任何通用容量标签——仅会显示其存在这一信息。
			name: "accelerated node still emits general presence only",
			node: newNode(
				mergeLabels(cpuModelLabels(), deviceLabels("tesla-t4", "Tesla-T4", "15Gi", "2")),
			),
			expected: func() map[string]string {
				labels := generalPresence("amd", "amd-25-1")
				delete(labels, NodeCPUOnlyLabelKey)
				return labels
			}(),
		},
		{
			// 节点上现有的 managed=true 设置将原封不动地保留。
			name: "managed label is true on node",
			node: newNode(mergeLabels(cpuModelLabels(), map[string]string{
				systemname.ManagedLabelKey: "true",
			})),
			expected: generalPresence("amd", "amd-25-1"),
		},
		{
			// 节点上设置 `managed=false` 会覆盖默认的 "true"。
			name: "managed label is false on node",
			node: newNode(mergeLabels(cpuModelLabels(), map[string]string{
				systemname.ManagedLabelKey: "false",
			})),
			expected: mergeLabels(
				generalPresence("amd", "amd-25-1"),
				map[string]string{systemname.ManagedLabelKey: "false"},
			),
		},
	}

	// forbiddenSuffixes 是每个单元/规格标签的后缀，被截断的函数在任何键下均不得再输出该后缀。
	forbiddenSuffixes := []string{".cpu", ".ram", ".storage", ".z-flavor", ".z-queue", ".z-cohort"}

	for _, cs := range cases {
		t.Run(cs.name, func(t *testing.T) {
			actual := ConstructNodeCapacityLabels(cs.node, cs.opts...)
			assert.Equal(t, cs.expected, actual, "unexpected capacity labels")
			// 不得出现任何被移除的每单位/规格标签。
			for k := range actual {
				for _, sfx := range forbiddenSuffixes {
					assert.NotContains(t, k, sfx, "removed capacity label must not be emitted")
				}
			}
		})
	}
}

func TestConstructNodeCapacityLabels_CPUOnlySelector(t *testing.T) {
	node := &core.Node{ObjectMeta: meta.ObjectMeta{Labels: map[string]string{}}}
	assert.Equal(t, "true", ConstructNodeCapacityLabels(node)[NodeCPUOnlyLabelKey])
	node.Labels[NodeAcceleratableLabelKey] = "true"
	_, found := ConstructNodeCapacityLabels(node)[NodeCPUOnlyLabelKey]
	assert.False(t, found)
}

// TestConstructNodeCapacityLabels_ManualNodeManagement 标记了节点管理-手动选项：若未启用该选项，管理标签
// 将自动注入。若启用该选项，标签仅在管理员在节点上明确设置时存在。
func TestConstructNodeCapacityLabels_ManualNodeManagement(t *testing.T) {
	base := func(labels map[string]string) *core.Node {
		return &core.Node{ObjectMeta: meta.ObjectMeta{Name: "node-1", Labels: labels}}
	}
	managed := func(node *core.Node, opts ...ConstructNodeCapacityLabelsOption) (string, bool) {
		v, ok := ConstructNodeCapacityLabels(node, opts...)[systemname.ManagedLabelKey]
		return v, ok
	}

	// Auto（默认）：managed=true 已注入。
	v, ok := managed(base(nil))
	assert.True(t, ok, "auto mode injects the managed label")
	assert.Equal(t, "true", v)

	// 说明：没有明确标签的节点不会进行自动注入。
	_, ok = managed(base(nil), WithManualNodeManagement(true))
	assert.False(t, ok, "manual mode must not auto-inject the managed label")

	// 说明：会尊重明确的管理员选择参与/退出机制。
	v, ok = managed(base(map[string]string{systemname.ManagedLabelKey: "true"}), WithManualNodeManagement(true))
	assert.True(t, ok)
	assert.Equal(t, "true", v, "explicit opt-in preserved under manual mode")

	v, ok = managed(base(map[string]string{systemname.ManagedLabelKey: "false"}), WithManualNodeManagement(true))
	assert.True(t, ok)
	assert.Equal(t, "false", v, "explicit opt-out preserved under manual mode")
}
