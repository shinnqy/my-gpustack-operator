package service

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	worker "gpustack.ai/gpustack/api/worker/v1"
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/utils/json"
	"gpustack.ai/gpustack/pkg/workergateway/manager"
)

func instTypeRes(once, remaining, capacity string) workercore.InstanceTypeResource {
	return workercore.InstanceTypeResource{
		OnceMaxRequest: resource.MustParse(once),
		Remaining:      resource.MustParse(remaining),
		Capacity:       resource.MustParse(capacity),
	}
}

func instSpecA10G() workercore.InstanceTypeSpec {
	return workercore.InstanceTypeSpec{
		GeneralGroup:     "generic",
		AcceleratorGroup: "nvidia-a10g",
		Acceleratable:    true,
		OS:               "linux",
		Arch:             "amd64",
		UnitResources:    workercore.InstanceTypeUnitResources{CPU: "4", RAM: "16Gi"},
		LocalStorage:     "100Gi",
	}
}

func instStatusGPU(acc string) workercore.InstanceTypeStatus {
	return workercore.InstanceTypeStatus{
		Phase:             "Active",
		Accelerator:       instTypeRes(acc, acc, acc),
		CPU:               instTypeRes("4", "4", "4"),
		AcceleratorShared: instTypeRes("16164772Ki", "16164772Ki", "16164772Ki"),
		AcceleratorSliced: instTypeRes("104779756Ki", "104779756Ki", "104779756Ki"),
	}
}

func newInstType(genName, name string, spec workercore.InstanceTypeSpec, status workercore.InstanceTypeStatus) *worker.InstanceType {
	return &worker.InstanceType{
		ObjectMeta: meta.ObjectMeta{
			Name:         name,
			GenerateName: genName,
		},
		Spec:   spec,
		Status: status,
	}
}

func a10gInst(name, acc string) *worker.InstanceType {
	if name == "" {
		name = "gpustack-nvidia-a10g-hcjmv"
	}
	return newInstType("gpustack-nvidia-a10g-", name, instSpecA10G(), instStatusGPU(acc))
}

type seed struct {
	cluster string
	obj     *worker.InstanceType
}

func buildState(t *testing.T, seeds ...seed) AggregatedInstanceTypeList {
	t.Helper()
	op := OpListAggregateInstanceTypes()
	for _, s := range seeds {
		require.NoError(t, op.Next(s.cluster, s.obj))
	}
	return op.Result(false)
}

// gpuDetail 构造硬件身份及切片能力，包括逻辑切片数量、超配设置和指定的物理分区 profile。
func gpuDetail(product string, logical int32, overcommit bool, profiles ...workercore.AcceleratorSlicedPhysicalDetailProfile) workercore.InstanceTypeDetail {
	var d workercore.InstanceTypeDetail
	d.Manufacturer = "nvidia"
	d.Product = product
	d.Family = "ampere"
	d.SlicedDetail.Logical.Count = logical
	d.SlicedDetail.Logical.CoresPercentageOvercommit = overcommit
	for _, p := range profiles {
		d.SlicedDetail.Physical.Profiles = append(d.SlicedDetail.Physical.Profiles, p)
		d.SlicedDetail.Physical.Count += p.Count
	}
	return d
}

// TestListAggregateInstanceTypes_GroupingIgnoresDescription 验证单个集群的管理说明变化不会将同一硬件拆成两条聚合记录。
func TestListAggregateInstanceTypes_GroupingIgnoresDescription(t *testing.T) {
	a := a10gInst("inst-a", "1")
	a.Spec.Description = "cluster A pool"
	b := a10gInst("inst-b", "1")
	b.Spec.Description = "cluster B pool"

	state := buildState(t,
		seed{cluster: "cluster-a", obj: a},
		seed{cluster: "cluster-b", obj: b},
	)

	require.Len(t, state.Items, 1, "same hardware must not split on Description")
	require.Len(t, state.Items[0].Status.Tiers, 1)
	require.Len(t, state.Items[0].Status.Tiers[0].Candidates, 2)
}

// TestListAggregateInstanceTypes_StatusJSONShape 校验返回给调用方的 JSON 层级，
// 硬件描述及整组切片汇总位于规格状态，档位和候选各自保留应有的资源字段。
func TestListAggregateInstanceTypes_StatusJSONShape(t *testing.T) {
	it := newInstType("gpustack-nvidia-a10g-", "inst-a", instSpecA10G(), workercore.InstanceTypeStatus{
		Phase:             "Active",
		Detail:            gpuDetail("NVIDIA A10G", 8, true),
		Accelerator:       instTypeRes("1", "1", "1"),
		CPU:               instTypeRes("4", "4", "4"),
		AcceleratorShared: instTypeRes("0", "0", "0"),
		AcceleratorSliced: instTypeRes("8", "8", "8"),
	})
	state := buildState(t, seed{cluster: "cluster-a", obj: it})

	data, err := json.Marshal(state.Items[0].Status)
	require.NoError(t, err)

	const want = `{
      "detail": {
        "manufacturer": "nvidia", "product": "NVIDIA A10G", "family": "ampere",
        "cache": {}, "cpu": {"cache": {}},
        "slicedDetail": {"logical": {"coresPercentageOvercommit": true, "count": 8}, "physical": {}}
      },
      "onceMaxRequest": {"accelerator": "1", "acceleratorShared": "0", "acceleratorSliced": "8", "cpu": "4"},
      "remaining": {"accelerator": "1", "acceleratorShared": "0", "acceleratorSliced": "8", "cpu": "4"},
      "tiers": [
        {
          "onceMaxRequest": {"accelerator": "1", "acceleratorShared": "0", "acceleratorSliced": "8", "cpu": "4"},
          "remaining": {"accelerator": "1", "acceleratorShared": "0", "acceleratorSliced": "8", "cpu": "4"},
          "candidates": [
            {
              "cluster": "cluster-a", "name": "inst-a", "phase": "Active",
              "accelerator": {"onceMaxRequest": "1", "remaining": "1", "capacity": "1"},
              "acceleratorShared": {"onceMaxRequest": "0", "remaining": "0", "capacity": "0"},
              "acceleratorSliced": {"onceMaxRequest": "8", "remaining": "8", "capacity": "8"},
              "acceleratorPartitioned": {"onceMaxRequest": "0", "remaining": "0", "capacity": "0"},
              "cpu": {"onceMaxRequest": "4", "remaining": "4", "capacity": "4"},
              "acceleratorSlicedDetail": {"logical": {"coresPercentageOvercommit": true, "count": 8}, "physical": {}}
            }
          ],
          "acceleratorSlicedDetail": {"logical": {"coresPercentageOvercommit": true, "count": 8}, "physical": {}}
        }
      ]
    }`

	require.JSONEq(t, want, string(data))
}

func newFlavor(name string, spec worker.InstanceTypeFlavorSpec) *worker.InstanceTypeFlavor {
	return &worker.InstanceTypeFlavor{
		ObjectMeta: meta.ObjectMeta{Name: name},
		Spec:       spec,
	}
}

func flavorSpecA10G() worker.InstanceTypeFlavorSpec {
	return worker.InstanceTypeFlavorSpec{
		AcceleratorGroup: "gpustack-nvidia-a10g",
		Acceleratable:    true,
		Manufacturer:     "nvidia",
		Product:          "NVIDIA A10G",
		Family:           "ampere",
		Memory:           "23028Mi",
	}
}

func flavorSpecTeslaT4() worker.InstanceTypeFlavorSpec {
	return worker.InstanceTypeFlavorSpec{
		AcceleratorGroup: "gpustack-nvidia-tesla-t4",
		Acceleratable:    true,
		Manufacturer:     "nvidia",
		Product:          "Tesla T4",
		Family:           "turing",
		Memory:           "15360Mi",
	}
}

func flavorSpecCPUGroup(group string) worker.InstanceTypeFlavorSpec {
	return worker.InstanceTypeFlavorSpec{
		GeneralGroup:  group,
		Acceleratable: false,
	}
}

func aggFlavorSpec(spec worker.InstanceTypeFlavorSpec, clusters ...string) AggregatedInstanceTypeFlavorSpec {
	return AggregatedInstanceTypeFlavorSpec{
		InstanceTypeFlavorSpec: spec,
		Clusters:               clusters,
	}
}

type flavorSeed struct {
	cluster string
	obj     *worker.InstanceTypeFlavor
}

func TestListAggregateInstanceTypeFlavors_Result(t *testing.T) {
	cases := []struct {
		name     string
		items    []AggregatedInstanceTypeFlavor
		sorted   bool
		expected []string
	}{
		{
			name:     "empty list + sorted",
			items:    []AggregatedInstanceTypeFlavor{},
			sorted:   true,
			expected: []string{},
		},
		{
			name: "unsorted preserves insertion order",
			items: []AggregatedInstanceTypeFlavor{
				{Name: "gpustack-cpu-only", Spec: aggFlavorSpec(flavorSpecCPUGroup("generic"))},
				{Name: "gpustack-nvidia-tesla-t4", Spec: aggFlavorSpec(flavorSpecTeslaT4())},
				{Name: "gpustack-nvidia-a10g", Spec: aggFlavorSpec(flavorSpecA10G())},
			},
			sorted:   false,
			expected: []string{"gpustack-cpu-only", "gpustack-nvidia-tesla-t4", "gpustack-nvidia-a10g"},
		},
		{
			name: "sorted puts accelerated first then name ascending",
			items: []AggregatedInstanceTypeFlavor{
				{Name: "gpustack-cpu-only", Spec: aggFlavorSpec(flavorSpecCPUGroup("generic"))},
				{Name: "gpustack-nvidia-tesla-t4", Spec: aggFlavorSpec(flavorSpecTeslaT4())},
				{Name: "gpustack-nvidia-a10g", Spec: aggFlavorSpec(flavorSpecA10G())},
			},
			sorted:   true,
			expected: []string{"gpustack-nvidia-a10g", "gpustack-nvidia-tesla-t4", "gpustack-cpu-only"},
		},
		{
			name: "sorted is deterministic within the generic group",
			items: []AggregatedInstanceTypeFlavor{
				{Name: "gpustack-cpu-intel", Spec: aggFlavorSpec(flavorSpecCPUGroup("intel"))},
				{Name: "gpustack-cpu-amd", Spec: aggFlavorSpec(flavorSpecCPUGroup("amd"))},
			},
			sorted:   true,
			expected: []string{"gpustack-cpu-amd", "gpustack-cpu-intel"},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			op := ListAggregateInstanceTypeFlavors{
				list: AggregatedInstanceTypeFlavorList{Items: c.items},
			}
			result := op.Result(c.sorted)
			actual := make([]string, len(result.Items))
			for i, item := range result.Items {
				actual[i] = item.Name
			}
			assert.Equal(t, c.expected, actual)
		})
	}
}

func buildFlavorState(t *testing.T, seeds ...flavorSeed) AggregatedInstanceTypeFlavorList {
	t.Helper()
	op := OpListAggregateInstanceTypeFlavors()
	for _, s := range seeds {
		require.NoError(t, op.Next(s.cluster, s.obj))
	}
	return op.Result(false)
}

// findFlavorItem 用完整 Spec 查找聚合项。某些规格字段不参与命名，因此单看名称可能匹配多项。
func findFlavorItem(state AggregatedInstanceTypeFlavorList, spec worker.InstanceTypeFlavorSpec) *AggregatedInstanceTypeFlavor {
	for i := range state.Items {
		if state.Items[i].Spec.InstanceTypeFlavorSpec == spec {
			return &state.Items[i]
		}
	}
	return nil
}

func TestHandleAggregatedInstanceTypeFlavor(t *testing.T) {
	t.Run("add brand-new flavor emits Added with *AggregatedInstanceTypeFlavor", func(t *testing.T) {
		h := OpHandleAggregatedInstanceTypeFlavor(buildFlavorState(t))

		evts := h.Handle(&manager.WorkerEvent{
			Type:    manager.WorkerEventAdded,
			Cluster: "cluster-a",
			Object:  newFlavor("gpustack-nvidia-a10g", flavorSpecA10G()),
		})

		require.Len(t, evts, 1)
		assert.Equal(t, manager.WorkerEventAdded, evts[0].Type)
		item, ok := evts[0].Object.(*AggregatedInstanceTypeFlavor)
		require.True(t, ok, "Added Object must be *AggregatedInstanceTypeFlavor, got %T", evts[0].Object)
		assert.Equal(t, "gpustack-nvidia-a10g", item.Name)
		assert.Equal(t, []string{"cluster-a"}, item.Spec.Clusters)
	})

	t.Run("second cluster with same spec emits Modified with sorted clusters", func(t *testing.T) {
		h := OpHandleAggregatedInstanceTypeFlavor(buildFlavorState(t,
			flavorSeed{cluster: "cluster-b", obj: newFlavor("gpustack-nvidia-a10g", flavorSpecA10G())},
		))

		evts := h.Handle(&manager.WorkerEvent{
			Type:    manager.WorkerEventAdded,
			Cluster: "cluster-a",
			Object:  newFlavor("gpustack-nvidia-a10g", flavorSpecA10G()),
		})

		require.Len(t, evts, 1)
		assert.Equal(t, manager.WorkerEventModified, evts[0].Type)
		item := findFlavorItem(h.state, flavorSpecA10G())
		require.NotNil(t, item)
		assert.Equal(t, []string{"cluster-a", "cluster-b"}, item.Spec.Clusters)
	})

	t.Run("duplicate contribution from same cluster is a no-op", func(t *testing.T) {
		h := OpHandleAggregatedInstanceTypeFlavor(buildFlavorState(t,
			flavorSeed{cluster: "cluster-a", obj: newFlavor("gpustack-nvidia-a10g", flavorSpecA10G())},
		))

		evts := h.Handle(&manager.WorkerEvent{
			Type:    manager.WorkerEventModified,
			Cluster: "cluster-a",
			Object:  newFlavor("gpustack-nvidia-a10g", flavorSpecA10G()),
		})

		assert.Empty(t, evts)
		require.Len(t, h.state.Items, 1)
		assert.Equal(t, []string{"cluster-a"}, h.state.Items[0].Spec.Clusters)
	})

	t.Run("delete one of several clusters emits Modified", func(t *testing.T) {
		h := OpHandleAggregatedInstanceTypeFlavor(buildFlavorState(t,
			flavorSeed{cluster: "cluster-a", obj: newFlavor("gpustack-nvidia-a10g", flavorSpecA10G())},
			flavorSeed{cluster: "cluster-b", obj: newFlavor("gpustack-nvidia-a10g", flavorSpecA10G())},
		))

		evts := h.Handle(&manager.WorkerEvent{
			Type:    manager.WorkerEventDeleted,
			Cluster: "cluster-a",
			Object:  newFlavor("gpustack-nvidia-a10g", flavorSpecA10G()),
		})

		require.Len(t, evts, 1)
		assert.Equal(t, manager.WorkerEventModified, evts[0].Type)
		item := findFlavorItem(h.state, flavorSpecA10G())
		require.NotNil(t, item)
		assert.Equal(t, []string{"cluster-b"}, item.Spec.Clusters)
	})

	t.Run("delete last cluster emits Deleted with the flavor name", func(t *testing.T) {
		h := OpHandleAggregatedInstanceTypeFlavor(buildFlavorState(t,
			flavorSeed{cluster: "cluster-a", obj: newFlavor("gpustack-nvidia-tesla-t4", flavorSpecTeslaT4())},
			flavorSeed{cluster: "cluster-a", obj: newFlavor("gpustack-nvidia-a10g", flavorSpecA10G())},
		))

		evts := h.Handle(&manager.WorkerEvent{
			Type:    manager.WorkerEventDeleted,
			Cluster: "cluster-a",
			Object:  newFlavor("gpustack-nvidia-a10g", flavorSpecA10G()),
		})

		require.Len(t, evts, 1)
		assert.Equal(t, manager.WorkerEventDeleted, evts[0].Type)
		deleted, ok := evts[0].Object.(*AggregatedInstanceTypeFlavor)
		require.True(t, ok)
		assert.Equal(t, "gpustack-nvidia-a10g", deleted.Name)
		assert.Equal(t, flavorSpecA10G(), deleted.Spec.InstanceTypeFlavorSpec,
			"DELETED must carry the grouping Spec so consumers can disambiguate collidable names")
		assert.Equal(t, []string{}, deleted.Spec.Clusters,
			"DELETED carries an empty, non-nil cluster list so it serializes as [] not null")
		assert.Nil(t, findFlavorItem(h.state, flavorSpecA10G()))
		assert.NotNil(t, findFlavorItem(h.state, flavorSpecTeslaT4()), "the other flavor must remain")
	})

	t.Run("deleting a non-existent contribution is a no-op", func(t *testing.T) {
		h := OpHandleAggregatedInstanceTypeFlavor(buildFlavorState(t,
			flavorSeed{cluster: "cluster-a", obj: newFlavor("gpustack-nvidia-a10g", flavorSpecA10G())},
		))

		evts := h.Handle(&manager.WorkerEvent{
			Type:    manager.WorkerEventDeleted,
			Cluster: "cluster-b",
			Object:  newFlavor("gpustack-nvidia-a10g", flavorSpecA10G()),
		})

		assert.Empty(t, evts)
		require.Len(t, h.state.Items, 1)
	})

	t.Run("Modified with DeletionTimestamp is treated as delete", func(t *testing.T) {
		h := OpHandleAggregatedInstanceTypeFlavor(buildFlavorState(t,
			flavorSeed{cluster: "cluster-a", obj: newFlavor("gpustack-nvidia-a10g", flavorSpecA10G())},
			flavorSeed{cluster: "cluster-b", obj: newFlavor("gpustack-nvidia-a10g", flavorSpecA10G())},
		))

		dying := newFlavor("gpustack-nvidia-a10g", flavorSpecA10G())
		now := meta.NewTime(time.Now())
		dying.DeletionTimestamp = &now

		evts := h.Handle(&manager.WorkerEvent{
			Type:    manager.WorkerEventModified,
			Cluster: "cluster-a",
			Object:  dying,
		})

		require.Len(t, evts, 1)
		assert.Equal(t, manager.WorkerEventModified, evts[0].Type)
		item := findFlavorItem(h.state, flavorSpecA10G())
		require.NotNil(t, item)
		assert.Equal(t, []string{"cluster-b"}, item.Spec.Clusters)
	})

	t.Run("delete-all-cluster drops the cluster from every group it contributes to", func(t *testing.T) {
		h := OpHandleAggregatedInstanceTypeFlavor(buildFlavorState(t,
			flavorSeed{cluster: "cluster-a", obj: newFlavor("gpustack-nvidia-a10g", flavorSpecA10G())},
			flavorSeed{cluster: "cluster-b", obj: newFlavor("gpustack-nvidia-a10g", flavorSpecA10G())},
			flavorSeed{cluster: "cluster-a", obj: newFlavor("gpustack-cpu-only", flavorSpecCPUGroup("generic"))},
		))

		evts := h.Handle(&manager.WorkerEvent{
			Type:    manager.WorkerEventDeleted,
			Cluster: "cluster-a",
			Object:  nil,
		})

		// A10G 组仍有 cluster-b，应产生 Modified。原来只有 cluster-a 的纯 CPU 组应产生 Deleted。
		require.Len(t, evts, 2)
		byType := map[manager.WorkerEventType]*AggregatedInstanceTypeFlavor{}
		for _, e := range evts {
			item, ok := e.Object.(*AggregatedInstanceTypeFlavor)
			require.True(t, ok)
			byType[e.Type] = item
		}
		require.NotNil(t, byType[manager.WorkerEventModified])
		assert.Equal(t, "gpustack-nvidia-a10g", byType[manager.WorkerEventModified].Name)
		assert.Equal(t, []string{"cluster-b"}, byType[manager.WorkerEventModified].Spec.Clusters)
		require.NotNil(t, byType[manager.WorkerEventDeleted])
		assert.Equal(t, "gpustack-cpu-only", byType[manager.WorkerEventDeleted].Name)
		assert.Equal(t, flavorSpecCPUGroup("generic"), byType[manager.WorkerEventDeleted].Spec.InstanceTypeFlavorSpec,
			"DELETED must carry the grouping Spec")

		assert.NotNil(t, findFlavorItem(h.state, flavorSpecA10G()))
		assert.Nil(t, findFlavorItem(h.state, flavorSpecCPUGroup("generic")))
	})

	t.Run("rejects a non-flavor object", func(t *testing.T) {
		h := OpHandleAggregatedInstanceTypeFlavor(buildFlavorState(t))
		evts := h.Handle(&manager.WorkerEvent{
			Type:    manager.WorkerEventAdded,
			Cluster: "cluster-a",
			Object:  &worker.InstanceType{},
		})
		assert.Nil(t, evts)
	})
}
