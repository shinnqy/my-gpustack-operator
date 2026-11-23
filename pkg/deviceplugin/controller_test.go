package deviceplugin

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/utils/json"
)

// pl 构造左闭右开的槽位区间 [start, start+length)。
func pl(start, length int32) workercore.AcceleratorPlacement {
	return workercore.AcceleratorPlacement{Start: start, Length: length}
}

// a100Placements 构造 A100 上四种 profile 的合法位置，模拟探测器已记录的空卡几何布局。
func a100Placements() []workercore.AcceleratorPhysicalSlicedProfile {
	return []workercore.AcceleratorPhysicalSlicedProfile{
		{
			Name: "1g.5gb", MemoryMib: 5120, ComputeSlices: 1, MemorySlices: 1, Count: 7,
			Placements: []workercore.AcceleratorPlacement{pl(0, 1), pl(1, 1), pl(2, 1), pl(3, 1), pl(4, 1), pl(5, 1), pl(6, 1)},
		},
		{
			Name: "1g.10gb", MemoryMib: 10240, ComputeSlices: 1, MemorySlices: 2, Count: 4,
			Placements: []workercore.AcceleratorPlacement{pl(0, 2), pl(2, 2), pl(4, 2), pl(6, 2)},
		},
		{
			Name: "2g.10gb", MemoryMib: 10240, ComputeSlices: 2, MemorySlices: 2, Count: 3,
			Placements: []workercore.AcceleratorPlacement{pl(0, 2), pl(2, 2), pl(4, 2)},
		},
		{
			Name: "3g.20gb", MemoryMib: 20480, ComputeSlices: 3, MemorySlices: 4, Count: 2,
			Placements: []workercore.AcceleratorPlacement{pl(0, 4), pl(4, 4)},
		},
	}
}

// physicalAndLogicalDevices 在同一节点放一张有 MIG 槽位清单的卡和一张逻辑切片卡，供账本比较两种模式。
func physicalAndLogicalDevices(nodeName string) *workercore.Devices {
	return partitionedDevices(nodeName,
		workercore.Accelerator{
			ID: "mig-0",
			Status: workercore.AcceleratorStatus{PhysicalSliced: workercore.AcceleratorPhysicalSliced{
				Profiles: a100Placements(), Count: 7,
			}},
		},
		workercore.Accelerator{
			ID: "logical-0", Index: 1,
			Status: workercore.AcceleratorStatus{LogicalSliced: workercore.AcceleratorLogicalSliced{Count: 128}},
		},
	)
}

// allocationAnnotation 按设备插件写入 Pod 的格式编码每个容器的分配记录。
func allocationAnnotation(t *testing.T, allocations PodAllocations) map[string]string {
	t.Helper()
	annoBytes, err := json.Marshal(allocations)
	require.NoError(t, err)
	return map[string]string{AllocatedAcceleratorAnnoKey: string(annoBytes)}
}

// physicalSliceAllocation 记录一个 MIG 实例使用的设备、profile 和槽位，供账本重建时合并占用区间。
func physicalSliceAllocation(group, device, profile string, placements ...workercore.AcceleratorPlacement) workercore.DevicesStatus {
	return workercore.DevicesStatus{Groups: []workercore.DevicesAllocationGroup{{
		ID:           group,
		Manufacturer: nodefeature.ManufacturerNVIDIA,
		Accelerators: []workercore.AcceleratorAllocation{{
			ID:                          device,
			Mode:                        workercore.DeviceAllocationModeSliced,
			Allocated:                   nodefeature.ResourceMaxUnits / 8,
			AllocatedPhysicalProfile:    profile,
			AllocatedPhysicalPlacements: placements,
		}},
	}}}
}

// physicalSlicePod 构建由一个工作负载容器持有 MIG 实例的 Pod。
func physicalSlicePod(t *testing.T, name, group, device, profile, node string, placements ...workercore.AcceleratorPlacement) *core.Pod {
	t.Helper()
	return &core.Pod{
		ObjectMeta: meta.ObjectMeta{
			Name:      name,
			Namespace: "default",
			UID:       types.UID(name),
			Annotations: allocationAnnotation(t, PodAllocations{
				"main": {Devices: physicalSliceAllocation(group, device, profile, placements...)},
			}),
		},
		Spec: core.PodSpec{NodeName: node},
	}
}

func acceleratorByID(t *testing.T, status *workercore.DevicesStatus, id string) workercore.AcceleratorAllocation {
	t.Helper()
	require.NotNil(t, status, "no status was written")
	for i := range status.Groups {
		for j := range status.Groups[i].Accelerators {
			if status.Groups[i].Accelerators[j].ID == id {
				return status.Groups[i].Accelerators[j]
			}
		}
	}
	t.Fatalf("accelerator %q not found in reconciled status", id)
	return workercore.AcceleratorAllocation{}
}

// remainingByProfile 读取卡上给定 profile 的剩余数量，账本没有该项时返回零。
func remainingByProfile(t *testing.T, status *workercore.DevicesStatus, id, profile string) int32 {
	t.Helper()
	acc := acceleratorByID(t, status, id)
	for _, pc := range acc.RemainingProfiles {
		if pc.Name == profile {
			return pc.Count
		}
	}
	return 0
}

// TestDevicesReconciler_PhysicalLedgerFold 验证从 Pod 注解重建的 MIG 账本。
// 覆盖空卡上限、按位置合并实例占用、重复协调和占用记录缺失。
func TestDevicesReconciler_PhysicalLedgerFold(t *testing.T) {
	const nodeName = "node-mig"
	newReconciler := func(pods ...ctrlcli.Object) *DevicesReconciler {
		objects := append([]ctrlcli.Object{physicalAndLogicalDevices(nodeName)}, pods...)
		return &DevicesReconciler{NodeName: nodeName, Client: nodeFixture(objects...)}
	}
	ledger := func(t *testing.T, rec *DevicesReconciler) *workercore.DevicesStatus {
		t.Helper()
		key := ctrlcli.ObjectKey{Name: nodeName}
		_, err := rec.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
		require.NoError(t, err)
		devices := new(workercore.Devices)
		require.NoError(t, rec.Client.Get(t.Context(), key, devices))
		return &devices.Status
	}

	t.Run("empty MIG card reports full free, logical card empty", func(t *testing.T) {
		rec := newReconciler()

		status := ledger(t, rec)

		mig := acceleratorByID(t, status, "mig-0")
		assert.Nil(t, mig.AllocatedProfiles, "empty MIG card has no allocated instances")
		assert.Equal(t, []workercore.AcceleratorProfileCount{
			{Name: "1g.10gb", Count: 4},
			{Name: "1g.5gb", Count: 7},
			{Name: "2g.10gb", Count: 3},
			{Name: "3g.20gb", Count: 2},
		}, mig.RemainingProfiles, "empty MIG card reports full per-profile ceilings")

		logical := acceleratorByID(t, status, "logical-0")
		assert.Nil(t, logical.AllocatedProfiles, "logical card carries no allocated ledger")
		assert.Nil(t, logical.RemainingProfiles, "logical card carries no free ledger")
	})

	t.Run("one 3g.20gb@slot0 folds to the worked-example ledger", func(t *testing.T) {
		pod := physicalSlicePod(t, "p-3g", "grp-0", "mig-0", "3g.20gb", nodeName, pl(0, 4))
		rec := newReconciler(pod)

		status := ledger(t, rec)

		mig := acceleratorByID(t, status, "mig-0")
		assert.Equal(t, []workercore.AcceleratorProfileCount{{Name: "3g.20gb", Count: 1}}, mig.AllocatedProfiles)
		assert.Equal(t, []workercore.AcceleratorProfileCount{
			{Name: "1g.10gb", Count: 2},
			{Name: "1g.5gb", Count: 3},
			{Name: "2g.10gb", Count: 1},
			{Name: "3g.20gb", Count: 1},
		}, mig.RemainingProfiles, "matches the spec worked example")
	})

	t.Run("two same-profile pods at different slots reconstruct occupied", func(t *testing.T) {
		p1 := physicalSlicePod(t, "p1", "grp-0", "mig-0", "1g.10gb", nodeName, pl(0, 2))
		p2 := physicalSlicePod(t, "p2", "grp-0", "mig-0", "1g.10gb", nodeName, pl(2, 2))
		rec := newReconciler(p1, p2)

		status := ledger(t, rec)

		mig := acceleratorByID(t, status, "mig-0")
		assert.Equal(t, []workercore.AcceleratorProfileCount{{Name: "1g.10gb", Count: 2}}, mig.AllocatedProfiles,
			"two instances of the same profile counted")
		assert.Equal(t, []workercore.AcceleratorProfileCount{
			{Name: "1g.10gb", Count: 2},
			{Name: "1g.5gb", Count: 3},
			{Name: "2g.10gb", Count: 1},
			{Name: "3g.20gb", Count: 1},
		}, mig.RemainingProfiles, "free reflects both occupied slots [0,2)+[2,4), not the empty ceiling")
	})

	t.Run("节点进程重启后按 Pod 注解恢复占用，删除 Pod 后归还槽位", func(t *testing.T) {
		pod := physicalSlicePod(t, "p-3g", "grp-0", "mig-0", "3g.20gb", nodeName, pl(0, 4))
		first := newReconciler(pod)
		before := ledger(t, first)
		require.EqualValues(t, 1, remainingByProfile(t, before, "mig-0", "3g.20gb"))
		restarted := &DevicesReconciler{NodeName: nodeName, Client: first.Client}
		assert.Equal(t, before, ledger(t, restarted))

		require.NoError(t, restarted.Client.Delete(t.Context(), pod))
		after := ledger(t, restarted)
		assert.Empty(t, acceleratorByID(t, after, "mig-0").AllocatedProfiles)
		assert.EqualValues(t, 2, remainingByProfile(t, after, "mig-0", "3g.20gb"))
	})

	t.Run("missing occupancy overstates free, never understates", func(t *testing.T) {
		// 记录了实例时，3g.20gb 还可分配一个。如果缺少这个实例的注解，推算值会升到空卡上限两个。
		pod := physicalSlicePod(t, "p-3g", "grp-0", "mig-0", "3g.20gb", nodeName, pl(0, 4))
		recorded := ledger(t, newReconciler(pod))
		recordedFree := remainingByProfile(t, recorded, "mig-0", "3g.20gb")

		missing := ledger(t, newReconciler())
		missingFree := remainingByProfile(t, missing, "mig-0", "3g.20gb")

		assert.GreaterOrEqual(t, missingFree, recordedFree, "missing occupancy only inflates free")
		assert.Equal(t, int32(1), recordedFree, "recorded instance leaves one 3g.20gb slot free")
		assert.Equal(t, int32(2), missingFree, "unrecorded orphan overstates free to the empty ceiling")
	})
}
