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
	sliced := nodefeature.ResourceFamilySliced
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
			name:    "sliced fits on partially-used cards",
			devices: []workercore.Devices{devicesWithRemaining(half, half, half, half)},
			demands: []familyDemand{{family: sliced, cards: 3, unitsPerCard: half}},
			want:    kueue.CheckStateReady,
		},
		{
			name:    "sliced is held when the demand exceeds every card's remainder",
			devices: []workercore.Devices{devicesWithRemaining(half, half)},
			// 单个切片需要整卡的 units，各候选卡的余量都不足。
			demands: []familyDemand{{family: sliced, cards: 1, unitsPerCard: whole}},
			want:    kueue.CheckStateRetry,
		},
		{
			// 尚未由 Pod webhook 换算的切片没有 units，会排在共享请求之后试放。即使此时预算为零，也不能再次使
			// 用已处于共享模式的卡。
			name:    "a card one role shares cannot also host another role's unshaped slice",
			devices: []workercore.Devices{devicesWithRemaining(whole)},
			demands: []familyDemand{
				{family: nodefeature.ResourceFamilyShared, cards: 1, sharedNeeds: []sharedNeed{{cards: 1, containers: 1}}},
				{family: sliced, cards: 1},
			},
			want: kueue.CheckStateRetry,
		},
		{
			// 其它模式已占用的卡不能接收逻辑切片，即便它还有足够的剩余 units。
			name:    "a slice is not placed on a card held in shared mode",
			devices: []workercore.Devices{heldCard(whole-share, workercore.DeviceAllocationModeShared)},
			demands: []familyDemand{{family: sliced, cards: 1, unitsPerCard: half}},
			want:    kueue.CheckStateRetry,
		},
		{
			name:    "a slice joins a card already held in sliced mode",
			devices: []workercore.Devices{heldCard(half, workercore.DeviceAllocationModeSliced)},
			demands: []familyDemand{{family: sliced, cards: 1, unitsPerCard: half}},
			want:    kueue.CheckStateReady,
		},
		{
			// 每个切片都要占一个令牌。测试卡有十个切片位，即使剩余显存足够，也不能再接收第十一个。
			name:    "slices beyond a card's slice count are held",
			devices: []workercore.Devices{devicesWithRemaining(whole)},
			demands: []familyDemand{{family: sliced, cards: 11, unitsPerCard: whole / 20}},
			want:    kueue.CheckStateRetry,
		},
		{
			name:    "slices up to a card's slice count share it",
			devices: []workercore.Devices{devicesWithRemaining(whole)},
			demands: []familyDemand{{family: sliced, cards: 10, unitsPerCard: whole / 20}},
			want:    kueue.CheckStateReady,
		},
		{
			// 优先把切片放进剩余空间最小但能容纳它的卡，以便另一角色仍可申请完整空卡。
			name:    "a slice fills a used card before breaking into a whole one",
			devices: []workercore.Devices{devicesWithRemaining(whole, whole*2/5)},
			demands: []familyDemand{
				{family: sliced, cards: 1, unitsPerCard: whole * 3 / 10},
				{family: exclusive, cards: 1},
			},
			want: kueue.CheckStateReady,
		},
		{
			name:    "a card one role slices cannot also serve another role exclusively",
			devices: []workercore.Devices{devicesWithRemaining(whole)},
			demands: []familyDemand{
				{family: sliced, cards: 1, unitsPerCard: whole * 3 / 10},
				{family: exclusive, cards: 1},
			},
			want: kueue.CheckStateRetry,
		},
		{
			name:    "a card one role slices cannot also take another role's share",
			devices: []workercore.Devices{devicesWithRemaining(whole)},
			demands: []familyDemand{
				{family: sliced, cards: 1, unitsPerCard: whole * 3 / 10},
				{family: nodefeature.ResourceFamilyShared, cards: 1, sharedNeeds: []sharedNeed{{cards: 1, containers: 1}}},
			},
			want: kueue.CheckStateRetry,
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

// physicalCard 用于描述一个分区化的卡片，作为可行性测试用的实例：包括其分配模式、剩余的标量单元数量、
// RemainingProfiles 账本（profile → 空闲实例数量），以及其能力是否包含缓存的 Placements（账本准备信
// 号）。
type physicalCard struct {
	id                string
	mode              workercore.DeviceAllocationMode
	remaining         int32
	remainingProfiles map[string]int32
	placementsCached  bool
}

// physicalDevices 为每张测试卡同时构建 Spec 中的能力记录和 Status 中的分配记录。两者使用相同设备 ID，
// 后者包含模式、剩余 units 和 RemainingProfiles，与 collectCards 的关联方式一致。
func physicalDevices(cards ...physicalCard) workercore.Devices {
	return physicalDevicesOf("nvidia", cards...)
}

// physicalDevicesOf 为指定厂商生成与 physicalDevices 相同形状的账本。单卡按厂商自己的规格名称存储余
// 量，因此测试必须保留厂商身份，才能验证公布的资源名是否正确转换。
func physicalDevicesOf(manufacturer string, cards ...physicalCard) workercore.Devices {
	specAccels := make([]workercore.Accelerator, len(cards))
	statusAccels := make([]workercore.AcceleratorAllocation, len(cards))
	for i, c := range cards {
		prof := workercore.AcceleratorPhysicalSlicedProfile{Name: "cap", Count: 7}
		if c.placementsCached {
			prof.Placements = []workercore.AcceleratorPlacement{{Start: 0, Length: 1}}
		}
		specAccels[i] = workercore.Accelerator{
			ID: c.id, Index: uint32(i),
			Status: workercore.AcceleratorStatus{
				PhysicalSliced: workercore.AcceleratorPhysicalSliced{
					Profiles: []workercore.AcceleratorPhysicalSlicedProfile{prof},
					Count:    prof.Count,
				},
			},
		}
		rem := make([]workercore.AcceleratorProfileCount, 0, len(c.remainingProfiles))
		for name, cnt := range c.remainingProfiles {
			rem = append(rem, workercore.AcceleratorProfileCount{Name: name, Count: cnt})
		}
		statusAccels[i] = workercore.AcceleratorAllocation{
			ID: c.id, Index: uint32(i), Mode: c.mode, Remaining: c.remaining, RemainingProfiles: rem,
		}
	}
	return workercore.Devices{
		Spec: workercore.DevicesSpec{
			Groups: []workercore.DevicesGroup{{ID: "g0", Manufacturer: manufacturer, Accelerators: specAccels}},
		},
		Status: workercore.DevicesStatus{
			Groups: []workercore.DevicesAllocationGroup{{ID: "g0", Manufacturer: manufacturer, Accelerators: statusAccels}},
		},
	}
}

func TestNodeDevicesFeasibilityPartitioned(t *testing.T) {
	none := workercore.DeviceAllocationModeNone
	partitioned := workercore.DeviceAllocationModePartitioned
	exclusive := workercore.DeviceAllocationModeExclusive

	cases := []struct {
		name    string
		devices []workercore.Devices
		profile string
		cards   int32
		want    kueue.CheckState
	}{
		{
			name: "enough cards with a free placement is ready",
			// 每个实例有1个免费放置空间，因此这两个实例必须来自不同的卡。在第一个实例有剩余空间的情况下，适配过
			// 程在检查第二个实例之前就完成了——这种情况的遍历操作并未实际执行。
			devices: []workercore.Devices{physicalDevices(
				physicalCard{id: "g0", mode: none, remainingProfiles: map[string]int32{"1g.10gb": 1}, placementsCached: true},
				physicalCard{id: "g1", mode: partitioned, remainingProfiles: map[string]int32{"1g.10gb": 1}, placementsCached: true},
			)},
			profile: "1g.10gb", cards: 2, want: kueue.CheckStateReady,
		},
		{
			name: "profile full everywhere retries (not reject)",
			devices: []workercore.Devices{physicalDevices(
				physicalCard{id: "g0", mode: partitioned, remainingProfiles: map[string]int32{"1g.10gb": 0}, placementsCached: true},
			)},
			profile: "1g.10gb", cards: 1, want: kueue.CheckStateRetry,
		},
		{
			name: "no placements cached is ledger-not-ready retry",
			devices: []workercore.Devices{physicalDevices(
				physicalCard{id: "g0", mode: none, remainingProfiles: map[string]int32{}, placementsCached: false},
			)},
			profile: "1g.10gb", cards: 1, want: kueue.CheckStateRetry,
		},
		{
			name: "exclusive-held partitioned card is not a candidate",
			devices: []workercore.Devices{physicalDevices(
				physicalCard{id: "g0", mode: exclusive, remainingProfiles: map[string]int32{"1g.10gb": 7}, placementsCached: true},
			)},
			profile: "1g.10gb", cards: 1, want: kueue.CheckStateRetry,
		},
		{
			name: "short by one card retries",
			devices: []workercore.Devices{physicalDevices(
				physicalCard{id: "g0", mode: none, remainingProfiles: map[string]int32{"1g.10gb": 1}, placementsCached: true},
			)},
			profile: "1g.10gb", cards: 2, want: kueue.CheckStateRetry,
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, _ := feasibilityOfOnePool(c.devices, []familyDemand{
				{family: nodefeature.ResourceFamilyPartitioned, cards: c.cards, profile: c.profile},
			})
			assert.Equal(t, c.want, got)
		})
	}
}

// TestNodeDevicesFeasibilityPartitionedVendorSpelledLedger 在可行性检查阶段将 profile 名称的边界标记
// 出来。需求名称即作为已发布资源的键来命名该 profile，而卡片的 per-profile 账本则以制造商自己的库名
// 称作为键——若严格比对两者，则没有匹配项，可理解为完整的卡片；若对已发布的 profile 进行任何请求，在
// 有空间的池中会永远处于重试状态。最终判定仍使用已发布的名称，因为那是用户所指定的名称。
func TestNodeDevicesFeasibilityPartitionedVendorSpelledLedger(t *testing.T) {
	roomyCard := []workercore.Devices{physicalDevicesOf(nodefeature.ManufacturerTHead, physicalCard{
		id: "p0", mode: workercore.DeviceAllocationModeNone,
		remainingProfiles: map[string]int32{"4g48gb": 2}, placementsCached: true,
	})}

	got, msg := feasibilityOfOnePool(roomyCard, []familyDemand{
		{family: nodefeature.ResourceFamilyPartitioned, cards: 1, profile: "4g.48gb"},
	})
	assert.Equal(t, kueue.CheckStateReady, got, "a published profile name against a vendor-spelled ledger")
	assert.Contains(t, msg, `"4g.48gb"`, "the verdict quotes the name the user wrote")

	// 转换过程会将一种拼写转换为另一种，但并不会让所有名称都匹配。
	got, _ = feasibilityOfOnePool(roomyCard, []familyDemand{
		{family: nodefeature.ResourceFamilyPartitioned, cards: 1, profile: "8g.96gb"},
	})
	assert.Equal(t, kueue.CheckStateRetry, got, "a profile the card does not offer")
}

// TestNodeDevicesFeasibilityPartitionedCountsPlacements 说明：实例中的分区需求计数基于实例而非卡：一
// 张卡拥有多个空闲分区，该卡对应的多个复制品属于剩余分区，这与 RemainingProfiles 的报告内容以及
// placement-authoritative Allocate 的功能一致。如果按每张卡统计一个实例，那么在有空间容纳所有复制品
// 的节点上，每个复制品都将被永久拒绝在重试阶段中运行。
func TestNodeDevicesFeasibilityPartitionedCountsPlacements(t *testing.T) {
	oneRoomyCard := []workercore.Devices{physicalDevices(physicalCard{
		id: "g0", mode: workercore.DeviceAllocationModeNone,
		remainingProfiles: map[string]int32{"1g.10gb": 3}, placementsCached: true,
	})}

	for _, want := range []struct {
		instances int32
		state     kueue.CheckState
	}{
		{instances: 1, state: kueue.CheckStateReady},
		{instances: 3, state: kueue.CheckStateReady},
		{instances: 4, state: kueue.CheckStateRetry},
	} {
		got, _ := feasibilityOfOnePool(oneRoomyCard, []familyDemand{
			{family: nodefeature.ResourceFamilyPartitioned, cards: want.instances, profile: "1g.10gb"},
		})
		assert.Equal(t, want.state, got, "%d instances against one card with 3 free placements", want.instances)
	}

	// 一个工作负载的两次分区需求不得出现在相同的放置区间中。
	got, _ := feasibilityOfOnePool(oneRoomyCard, []familyDemand{
		{family: nodefeature.ResourceFamilyPartitioned, cards: 2, profile: "1g.10gb"},
		{family: nodefeature.ResourceFamilyPartitioned, cards: 2, profile: "1g.10gb", unitsPerCard: 1},
	})
	assert.Equal(t, kueue.CheckStateRetry, got, "4 instances across two demands exceed the 3 free placements")
}

// TestNodeDevicesFeasibilityPartitionedMessages 标记了“账本未准备”状态所承载的消息与真正的“配置已满”
// 状态不同，因此运维人员能够区分升级窗口与实际冲突情况，且完全没有分区机制的池也不会重复利用资源。
func TestNodeDevicesFeasibilityPartitionedMessages(t *testing.T) {
	notReady := []workercore.Devices{physicalDevices(
		physicalCard{id: "g0", mode: workercore.DeviceAllocationModeNone, placementsCached: false},
	)}
	full := []workercore.Devices{physicalDevices(
		physicalCard{id: "g0", mode: workercore.DeviceAllocationModePartitioned, remainingProfiles: map[string]int32{"1g.10gb": 0}, placementsCached: true},
	)}
	// 完全没有分区卡的池，与发布窗口是不同的概念，必须包含其专属说明。
	noPartition := []workercore.Devices{devicesWithRemaining(int32(nodefeature.ResourceMaxUnits))}

	demand := []familyDemand{{family: nodefeature.ResourceFamilyPartitioned, cards: 1, profile: "1g.10gb"}}
	_, notReadyMsg := feasibilityOfOnePool(notReady, demand)
	_, fullMsg := feasibilityOfOnePool(full, demand)
	_, noPartitionMsg := feasibilityOfOnePool(noPartition, demand)

	assert.Equal(t, partitionLedgerNotReadyMessage(nil), notReadyMsg)
	assert.NotEqual(t, notReadyMsg, fullMsg, "ledger-not-ready and profile-full messages must differ")
	assert.NotEqual(t, partitionLedgerNotReadyMessage(nil), noPartitionMsg,
		"a pool with no partitioned card must not reuse the device-manager-rollout message")
	assert.Contains(t, noPartitionMsg, "no partitioned card")
}
