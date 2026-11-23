package worker

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/sets"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	kueuetas "sigs.k8s.io/kueue/pkg/util/tas"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/nodefeature"
)

// devicesWithRemaining 按给定的每卡余量生成设备账本。每个值对应一张卡，
// 和真实控制器先按整卡满额建账再逐笔扣除的行为一致。测试卡支持逻辑切片且未划硬件分区，也可以承接整卡请求。
func devicesWithRemaining(remaining ...int32) workercore.Devices {
	specAccels := make([]workercore.Accelerator, len(remaining))
	statusAccels := make([]workercore.AcceleratorAllocation, len(remaining))
	for i, free := range remaining {
		id := fmt.Sprintf("gpu-%d", i)
		specAccels[i] = workercore.Accelerator{
			ID: id, Index: uint32(i),
			Status: workercore.AcceleratorStatus{
				LogicalSliced: workercore.AcceleratorLogicalSliced{Count: 10},
			},
		}
		statusAccels[i] = workercore.AcceleratorAllocation{
			ID:        id,
			Index:     uint32(i),
			Remaining: free,
		}
	}
	return workercore.Devices{
		Spec: workercore.DevicesSpec{
			Groups: []workercore.DevicesGroup{{ID: "g0", Manufacturer: "nvidia", Accelerators: specAccels}},
		},
		Status: workercore.DevicesStatus{
			Groups: []workercore.DevicesAllocationGroup{{ID: "g0", Manufacturer: "nvidia", Accelerators: statusAccels}},
		},
	}
}

// oneFlavorPool 用单个 ResourceFlavor 表示测试中 PodSet 可使用的池。按各设备组的资源键记录范围，
// 使它覆盖测试的每一张卡。测试需求和池使用相同的空 flavor 名，按相等关系匹配，空名没有通配含义。
func oneFlavorPool(devices []workercore.Devices) []scopedDevices {
	pool := make([]scopedDevices, 0, len(devices))
	for i := range devices {
		// 每份 Devices 账本来自一台节点。共享请求只有在同一台节点上凑齐卡数才能通过。
		sd := scopedDevices{devices: devices[i], hostname: fmt.Sprintf("node-%d", i)}
		seen := sets.New[string]()
		for gi := range devices[i].Status.Groups {
			g := &devices[i].Status.Groups[gi]
			if key := acceleratorKeyOf(g.Manufacturer, g.ID); !seen.Has(key) {
				seen.Insert(key)
				sd.scopes = append(sd.scopes, flavorScope{acceleratorKey: key})
			}
		}
		pool = append(pool, sd)
	}
	return pool
}

// feasibilityOfOnePool 在单一资源池内执行准入判断，该池覆盖所有测试卡和所有请求。
func feasibilityOfOnePool(devices []workercore.Devices, demands []familyDemand) (kueue.CheckState, string) {
	return nodeDevicesFeasibility(oneFlavorPool(devices), nil, demands)
}

func TestNodeDevicesFeasibility(t *testing.T) {
	const (
		whole = int32(nodefeature.ResourceMaxUnits)                                     // 完全空闲的一张卡。
		half  = int32(nodefeature.ResourceMaxUnits / 2)                                 // 单卡剩余一半的单位数。
		share = int32(nodefeature.ResourceMaxUnits / nodefeature.SharedResourceMaxSize) // 一位共享持有者在一张卡上占用的单位数。
	)
	exclusive := nodefeature.ResourceFamilyExclusive
	sharedDemand := func(cardsPerContainer, containers int32) familyDemand {
		return familyDemand{
			family:      nodefeature.ResourceFamilyShared,
			cards:       cardsPerContainer * containers,
			sharedNeeds: []sharedNeed{{cards: cardsPerContainer, containers: containers}},
		}
	}
	heldCard := func(free int32, mode workercore.DeviceAllocationMode) workercore.Devices {
		ledger := devicesWithRemaining(free)
		ledger.Status.Groups[0].Accelerators[0].Mode = mode
		return ledger
	}

	cases := []struct {
		name    string
		devices []workercore.Devices
		demands []familyDemand
		want    kueue.CheckState
	}{
		{
			name:    "exclusive over a fully-sliced node is held",
			devices: []workercore.Devices{devicesWithRemaining(half, half, half, half, half, half, half, half)},
			demands: []familyDemand{{family: exclusive, cards: 5}},
			want:    kueue.CheckStateRetry,
		},
		{
			name:    "exclusive with generic headroom but no clean card is held",
			devices: []workercore.Devices{devicesWithRemaining(half, half, half, half, half, half, half, half, half, half, half, half)},
			// 十二张卡各剩一半，合计相当于六张卡，但无法取出任何一张完整的空卡。
			demands: []familyDemand{{family: exclusive, cards: 5}},
			want:    kueue.CheckStateRetry,
		},
		{
			name:    "exclusive with enough clean cards is ready",
			devices: []workercore.Devices{devicesWithRemaining(whole, whole, whole, whole, whole, half, half, half)},
			demands: []familyDemand{{family: exclusive, cards: 5}},
			want:    kueue.CheckStateReady,
		},
		{
			name:    "exclusive short by one card is held",
			devices: []workercore.Devices{devicesWithRemaining(whole, whole, whole, whole)},
			demands: []familyDemand{{family: exclusive, cards: 5}},
			want:    kueue.CheckStateRetry,
		},
		{
			name:    "exclusive demands cannot spend the same card twice",
			devices: []workercore.Devices{devicesWithRemaining(whole)},
			demands: []familyDemand{{family: exclusive, cards: 1}, {family: exclusive, cards: 1}},
			want:    kueue.CheckStateRetry,
		},
		{
			name:    "shared fits when a free owner slot remains on each requested card",
			devices: []workercore.Devices{devicesWithRemaining(share, share, 0)},
			demands: []familyDemand{sharedDemand(2, 1)},
			want:    kueue.CheckStateReady,
		},
		{
			name:    "two shared containers of one card each share one card",
			devices: []workercore.Devices{heldCard(2*share, workercore.DeviceAllocationModeShared)},
			demands: []familyDemand{sharedDemand(1, 2)},
			want:    kueue.CheckStateReady,
		},
		{
			name:    "shared holders beyond a card's free shares are held",
			devices: []workercore.Devices{devicesWithRemaining(2 * share)},
			demands: []familyDemand{sharedDemand(1, 3)},
			want:    kueue.CheckStateRetry,
		},
		{
			name:    "shared cannot count one card twice for the same container",
			devices: []workercore.Devices{devicesWithRemaining(whole)},
			demands: []familyDemand{sharedDemand(2, 1)},
			want:    kueue.CheckStateRetry,
		},
		{
			name:    "shared cannot combine two nodes for one container",
			devices: []workercore.Devices{devicesWithRemaining(whole), devicesWithRemaining(whole)},
			demands: []familyDemand{sharedDemand(2, 1)},
			want:    kueue.CheckStateRetry,
		},
		{
			name:    "shared needs a full share on a card",
			devices: []workercore.Devices{devicesWithRemaining(share - 1)},
			demands: []familyDemand{sharedDemand(1, 1)},
			want:    kueue.CheckStateRetry,
		},
		{
			name:    "shared is not placed on a card held in sliced mode",
			devices: []workercore.Devices{heldCard(half, workercore.DeviceAllocationModeSliced)},
			demands: []familyDemand{sharedDemand(1, 1)},
			want:    kueue.CheckStateRetry,
		},
		{
			name:    "shared and exclusive demands cannot spend the same card",
			devices: []workercore.Devices{devicesWithRemaining(whole)},
			demands: []familyDemand{sharedDemand(1, 1), {family: exclusive, cards: 1}},
			want:    kueue.CheckStateRetry,
		},
		{
			name:    "two shared containers can each use two cards when the freest card is picked first",
			devices: []workercore.Devices{devicesWithRemaining(share, share, 2*share)},
			demands: []familyDemand{sharedDemand(2, 2)},
			want:    kueue.CheckStateReady,
		},
		{
			name:    "feasibility aggregates whole cards across devices",
			devices: []workercore.Devices{devicesWithRemaining(whole, half), devicesWithRemaining(whole, whole, half)},
			// 两份节点账本合计有三张完整空卡。
			demands: []familyDemand{{family: exclusive, cards: 3}},
			want:    kueue.CheckStateReady,
		},
		{
			name:    "no accelerator demand is ready",
			devices: []workercore.Devices{devicesWithRemaining(half)},
			demands: nil,
			want:    kueue.CheckStateReady,
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, _ := feasibilityOfOnePool(c.devices, c.demands)
			assert.Equal(t, c.want, got)
		})
	}
}

// 同一资源池里，即使另一台节点有整卡，也不能替 Kueue 已经指定的节点通过准入。
func TestExclusiveDemandUsesAssignedNode(t *testing.T) {
	const poolName kueue.ResourceFlavorReference = "gpu-pool"
	ta := &kueuetas.TopologyAssignment{
		Levels:  []string{core.LabelHostname},
		Domains: []kueuetas.TopologyDomainAssignment{{Values: []string{"host-a"}, Count: 1}},
	}
	wl := &kueue.Workload{
		Spec: kueue.WorkloadSpec{PodSets: []kueue.PodSet{{
			Name: "main", Count: 1, Template: core.PodTemplateSpec{Spec: core.PodSpec{Containers: []core.Container{{
				Name: "main", Resources: core.ResourceRequirements{Requests: core.ResourceList{"nvidia.com/gpu": resource.MustParse("1")}},
			}}}},
		}}},
		Status: kueue.WorkloadStatus{Admission: &kueue.Admission{PodSetAssignments: []kueue.PodSetAssignment{{
			Name:    "main",
			Flavors: map[core.ResourceName]kueue.ResourceFlavorReference{"credits.gpustack.ai/nvidia": poolName},
		}}}},
	}
	devices := oneFlavorPool([]workercore.Devices{devicesWithRemaining(nodefeature.ResourceMaxUnits / 2), devicesWithRemaining(nodefeature.ResourceMaxUnits)})
	devices[0].hostname, devices[1].hostname = "host-a", "host-b"
	for i := range devices {
		devices[i].scopes[0].flavor = poolName
	}
	for _, tc := range []struct {
		host string
		want kueue.CheckState
	}{{"host-a", kueue.CheckStateRetry}, {"host-b", kueue.CheckStateReady}} {
		ta.Domains[0].Values[0] = tc.host
		wl.Status.Admission.PodSetAssignments[0].TopologyAssignment = kueuetas.V1Beta2From(ta)
		demands := parseFamilyDemands(wl)
		got, _ := nodeDevicesFeasibility(devices, nil, demands)
		assert.Equal(t, tc.want, got, tc.host)
	}
}
