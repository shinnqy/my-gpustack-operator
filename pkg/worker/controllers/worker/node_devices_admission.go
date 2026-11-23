package worker

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"

	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlbuilder "sigs.k8s.io/controller-runtime/pkg/builder"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlcontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	ctrlpredicate "sigs.k8s.io/controller-runtime/pkg/predicate"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	kueueadmissioncheck "sigs.k8s.io/kueue/pkg/util/admissioncheck"
	kueuetas "sigs.k8s.io/kueue/pkg/util/tas"
	kueueworkload "sigs.k8s.io/kueue/pkg/workload"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/controller"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/utils/slicex"
	"gpustack.ai/gpustack/pkg/utils/strconvx"
)

const (
	// _NodeDevicesControllerName 是该 operator 声明的 AdmissionCheck controllerName；
	// worker 的 Prepare() 会应用携带该名称的 AdmissionCheck 对象。
	_NodeDevicesControllerName = "worker.gpustack.ai/node-devices"
	// _NodeDevicesAdmissionCheckName 是该 AdmissionCheck 对象的名称，即
	// NodeQueueReconciler 添加到加速型 ClusterQueue 的引用。InstanceTypeReconciler
	// 负责创建队列，但从不设置该字段。
	_NodeDevicesAdmissionCheckName = "gpustack-node-devices"
	// _NodeDevicesFieldOwner 拥有本控制器修补的 AdmissionCheckState。
	_NodeDevicesFieldOwner = "gpustack-node-devices"
	// _NodeDevicesRetryAfterSeconds 用于在 Kueue 重新准入被挂起的 Workload 前进行退避，
	// 避免因短暂的单卡资源不足导致高频循环重试。
	_NodeDevicesRetryAfterSeconds = int32(30)
)

// familyDemand 表示 Workload 向其分配的资源池发起的一组关联加速器需求：包含资源族、
// 所需卡数、单卡为承载其中一张卡的需求仍须具备的空闲单位数，以及（针对切分请求）所锚定的
// profile（其他情况为空）。对于独占（exclusive）资源族，每张卡都是独立的物理卡；共享
// 需求所需的独立卡数则在 sharedNeeds 中按持有者统计。对于逻辑切片（logical-slice）与硬件
// 切分（partition）资源族，一张卡对应一个切片或实例，因为这类请求为每个容器分配一张卡，
// 多个请求可以共享同一张物理卡。
//
// 这三个量均从同一个 podset 读取。若将所有 podset 中取最大值的单卡需求与所有 podset
// 累加的卡数配对，会让小请求受限于大请求的预算——这正是它所取代的单个每 Pod 元组的问题，
// 而且该元组的模式还会被最后扫描到的键覆盖。
type familyDemand struct {
	family       nodefeature.ResourceFamily
	cards        int32
	unitsPerCard int32
	// profile 是所请求的切分 profile，采用其资源键携带的公开拼写形式——也是每条判定消息
	// 向用户反馈的拼写形式。单卡自身的账本按厂商拼写索引，因此可行性检查会在逐卡检查时进行转换，
	// 而不在本结构中额外保存一份副本。
	profile string
	// flavor 是 Kueue 为该需求所属 podset 分配的 ResourceFlavor。它将需求限定在该
	// flavor 所覆盖的卡范围内，这对包含多个 podset 的 Workload 尤为必要，而单 podset 的
	// Workload 不受影响：Kueue 按 podset 分配 flavor，因此当 Workload 中不同角色需要不同
	// 型号的加速器时，会携带两个 flavor；若针对两者的并集匹配需求，就可能被一个对应角色
	// 永远无法调度到的卡所满足。
	flavor kueue.ResourceFlavorReference
	// podSets 列出读取出该需求的 podset 名称——当多个完全相同的需求合并时会有多个名称。
	// 该字段仅用于构造判定消息，从不参与匹配：一条不指明具体是哪个角色资源不足的全局
	// “卡数不足”提示对用户没有任何可操作性。
	podSets []kueue.PodSetReference
	// sharedNeeds 保存共享需求的持有者信息：每个容器实例是一个持有者，各自需要一定数量
	// 的独立物理卡，并在每张卡上占用一份份额。DevicePlugin 独立分配每个容器的令牌，
	// 因此两个持有者可以共享同一张卡；而仅仅把所有持有者加总的卡数，无法说明其中单个持有者
	// 需要多少张不同的物理卡。
	sharedNeeds []sharedNeed
	// node 是 Kueue 拓扑感知调度为这些 Pod 分配的节点主机名；若 podset 未携带主机名级别
	// 的拓扑分配，则为空。非空的 node 会将需求限制在该节点的卡范围内：一旦准入，Pod 会
	// 通过 nodeSelector 固定到该节点，因此资源池中其他节点上的空闲卡无法承载它们。
	node string
}

// sharedNeed 表示共享需求中需要相同数量独立物理卡的容器实例个数。
type sharedNeed struct {
	cards      int32
	containers int32
}

// IMPORTANT: 该方法把原本笼统的“总共要 3 张整卡”，拆成两条精确到节点的硬指标：
//   - 1. host-a 上必须能找出 2 张完整空卡（node="host-a", cards=2）
//   - 2. host-b 上必须能找出 1 张完整空卡（node="host-b", cards=1）
//
// =======
// parseFamilyDemands 从 Workload 的 Pod 模板中读取加速器需求，为每个 (podset, family)
// 生成一个关联元组；仅当分配的 flavor、单卡需求与 profile 均一致时才跨 podset 合并，
// 因此常见的单 podset Workload 在每个资源族上恰好产出一个元组。未请求任何已知加速器的
// Workload 不产出元组。
//
// 若 Kueue 已将某个 podset 的 Pod 放置到具体节点上，则该 podset 会为每个已分配节点
// 产出一个元组，每个元组按落在该节点上的 Pod 数量计大小，从而保证每个节点都必须能够
// 承载分配给自己的那部分 podset。
func parseFamilyDemands(wl *kueue.Workload) []familyDemand {
	var demands []familyDemand
	for i := range wl.Spec.PodSets {
		ps := &wl.Spec.PodSets[i]
		flavor := assignedFlavor(wl, ps.Name)
		nodes := assignedNodes(wl, ps.Name)
		for _, d := range podSetFamilyDemands(ps, ps.Count) {
			d.flavor = flavor
			d.podSets = []kueue.PodSetReference{ps.Name}
			if len(nodes) == 0 {
				demands = mergeDemand(demands, d)
				continue
			}
			for _, n := range nodes {
				nd := podSetFamilyDemand(ps, d.family, n.count)
				nd.flavor, nd.podSets, nd.node = flavor, d.podSets, n.hostname
				demands = mergeDemand(demands, nd)
			}
		}
	}
	sortDemands(demands)
	return demands
}

// nodeShare 表示 Kueue 放置到单个节点上的某 podset 的 Pod 数量，以节点主机名标识。
type nodeShare struct {
	hostname string
	count    int32
}

// assignedNodes 读取 Kueue 拓扑感知调度为单个 podset 的 Pod 分配的节点，以及每个
// 节点分得的 Pod 数量。当 podset 没有拓扑分配结果，或者分配的最低层级不是主机名时，
// 返回空：更粗粒度的拓扑域无法指明单个节点，因此需求仍按原先方式在整个资源池范围内判断。
//
// Kueue 会在任何 AdmissionCheck 落定之前，将拓扑分配结果连同配额预留一并写入；一旦
// Workload 获得准入，其拓扑解门器会将分配的节点标签复制到每个 Pod 的 nodeSelector 中。
// 因此在执行本检查时，目标节点实际上已经确定。
func assignedNodes(wl *kueue.Workload, podSet kueue.PodSetReference) []nodeShare {
	if wl.Status.Admission == nil {
		return nil
	}
	for i := range wl.Status.Admission.PodSetAssignments {
		psa := &wl.Status.Admission.PodSetAssignments[i]
		if psa.Name != podSet {
			continue
		}
		ta := psa.TopologyAssignment
		// 最低拓扑层级是不是主机名？
		// 	- 如果最后一项是 hostname，说明 Kueue 已经拍板定死每个 Pod 要去哪台具体机器了！
		// 	- 如果最后一项不是 hostname（例如 ["zone", "rack"]，只定到了“1 号机柜”，没定具体机器；或者根本没开 TAS，ta == nil）：说明目标机器还没敲定，Pod 去这个机柜里哪台机器都行。这时没法把检查收窄到某一台机器，所以直接 return nil，退回到按整个资源池（flavor）范围检查。
		if ta == nil || !kueuetas.IsLowestLevelHostname(ta.Levels) {
			return nil
		}
		var shares []nodeShare
		for domain := range kueuetas.InternalSeqFrom(ta) {
			if len(domain.Values) == 0 || domain.Count <= 0 {
				continue
			}
			shares = append(shares, nodeShare{hostname: domain.Values[len(domain.Values)-1], count: domain.Count})
		}
		return shares
	}
	return nil
}

// assignedFlavor 返回 Kueue 为单个 podset 分配的加速器 ResourceFlavor；若 Workload
// 尚未获得准入、未列出该 podset 的分配信息，或该分配中不包含加速器 flavor（或者包含
// 多个，与没有一样无法判定），则返回空字符串。空字符串并不表示“任意 flavor”：它仅匹配
// 自身同样未限定作用域的卡集合，因此无法读取分配信息的需求不会匹配到任何卡，检查会挂起
// 并给出明确说明原因的判定结论，而不是凭空猜测。
//
// 分配表为每个受覆盖的资源映射一个 flavor，而本控制器所门控的需求始终针对加速器——
// 因此应读取以厂商积分（credits）资源为键的条目，这也是在 chart 的转换规则替换原始
// 加速器键之后 Kueue 实际记账的资源。由本 operator 构建的 ClusterQueue 恰好覆盖
// 一种资源，因此普通 Workload 只携带一条记录；按资源键而非按名称读取，能确保在出现
// 例外情况时依然选对目标。
//
// 管理员可以手工编写覆盖多种资源的队列并引用本检查。这会带来两种情形，而对第二个
// 加速器积分资源进行计数可以同时拒绝这两种情况：
//
//   - 在加速器 flavor 之外同时分配的 CPU flavor 不会固定任何加速器键，因此不覆盖
//     任何卡；若回退使用它，会导致 Workload 被挂起并误报容量不足。
//   - 同时覆盖两个厂商的积分资源。同一资源族的需求会合并不同基准键，使卡数跨越
//     两种型号，而单个 flavor 只能固定一个加速器键，无法代表任何一方。
//
// 这两种资源是映射到两个 flavor 还是同一个 flavor 并无区别，而单 flavor 情况反而
// 更危险，因为它表面上看似明确无歧义，实则覆盖的只是其中一种型号的卡。两种情况均
// 返回空字符串，在列出任何卡之前即以分配状态作为原因挂起需求——这与 flavorAcceleratorKey
// 对歧义 flavor 的处理一致，因为任意挑选一个卡集合是唯一可能朝“错误放行 Workload”
// 方向出错的结果。
func assignedFlavor(wl *kueue.Workload, podSet kueue.PodSetReference) kueue.ResourceFlavorReference {
	if wl.Status.Admission == nil {
		return ""
	}
	for i := range wl.Status.Admission.PodSetAssignments {
		psa := &wl.Status.Admission.PodSetAssignments[i]
		if psa.Name != podSet {
			continue
		}
		var credits kueue.ResourceFlavorReference
		accelerators := 0
		for res, ref := range psa.Flavors {
			if !nodefeature.IsAcceleratableCreditsResourceName(res) {
				continue
			}
			accelerators++
			credits = ref
		}
		// 必须恰好为一个，否则返回空。出现两个厂商的积分资源意味着需求合并了两种型号的卡，
		// 没有任何一个 flavor 能同时代表两者：flavor 只固定单个加速器键，因此其覆盖的卡
		// 属于单一型号。无论这两种资源引用的是不同 flavor 还是同一个 flavor 都是如此——
		// 一个资源组可能同时覆盖两者并为各自指定同一个 flavor，若将该单一引用误判为无歧义，
		// 就会在统计两种型号总卡数的同时，把可用卡范围错误地限定到其中一种型号。
		if accelerators != 1 {
			return ""
		}
		return credits
	}
	return ""
}

// podSetFamilyDemands 读取单个 podset 按资源族划分的需求。每个资源族贡献
// count ×（该 podset 各容器累加的卡数）；卡数键（独占模式为 <base>，其余为
// <base>.shared、<base>.sliced、<base>.partitioned）累加到卡数中，计数键
// （<base>.sliced.units、<base>.partitioned.units）提供单卡单位数，而按
// profile 划分的切分键则指明 profile——它自身的值是单卡维度的，因此不计入卡数。
// 除应用容器外，也会扫描 init 容器，从而同样门控仅由 init 容器发起的请求。百分比和
// MiB 子键会被忽略：Pod webhook 已经将其折算进了计数键中。
//
// 即使一个 podset 的容器同时指定了两个厂商的基准键，每个资源族仍只对应一个元组——
// Pod webhook 禁止的是跨两个资源族，而非同一资源族下的两个基准键——因此合并后的元组
// 在标识同一个资源族的同时会累加两种型号的卡数。通过本 operator 构建的队列无法触发
// 这种情况：各厂商的键会转换为各自对应的积分资源，加速型 ClusterQueue 恰好覆盖其中
// 一种，而无法分配到 flavor 的请求会停留在未预留配额状态。只有管理员编写的、同时覆盖
// 两个厂商积分资源的队列才会走到这里，此时合并元组不会携带任何 flavor——assignedFlavor
// 会拒绝在两者之间做选择，从而挂起需求，而不是用单型号的卡去拟合跨型号的卡数。
//
// count 是该需求对应的 podset Pod 数量：可以是整个 podset，也可以是 Kueue 放置到
// 单个节点上的份额。
func podSetFamilyDemands(ps *kueue.PodSet, count int32) []familyDemand {
	byFamily := make(map[nodefeature.ResourceFamily]*familyDemand)
	containers := make([]*core.Container, 0, len(ps.Template.Spec.InitContainers)+len(ps.Template.Spec.Containers))
	for ci := range ps.Template.Spec.InitContainers {
		containers = append(containers, &ps.Template.Spec.InitContainers[ci])
	}
	for ci := range ps.Template.Spec.Containers {
		containers = append(containers, &ps.Template.Spec.Containers[ci])
	}

	for _, ctr := range containers {
		for name, qty := range mergedContainerResources(ctr) {
			family := nodefeature.ResourceFamilyOf(name)
			switch family {
			case nodefeature.ResourceFamilyExclusive, nodefeature.ResourceFamilyShared,
				nodefeature.ResourceFamilySliced, nodefeature.ResourceFamilyPartitioned:
			default:
				continue
			}
			d := byFamily[family]
			if d == nil {
				d = &familyDemand{family: family}
				byFamily[family] = d
			}
			switch {
			case strings.HasSuffix(string(name), nodefeature.SlicedUnitsResourceNameSuffix),
				strings.HasSuffix(string(name), nodefeature.PartitionedUnitsResourceNameSuffix):
				// 保留该 podset 各容器中最严格的单卡需求，确保可行性检查不会按过小的切片规格放行。
				if u := clampInt32(qty.Value()); u > d.unitsPerCard {
					d.unitsPerCard = u
				}
			case isCardKey(name, family):
				d.cards += clampInt32(qty.Value())
				if family == nodefeature.ResourceFamilyShared && qty.Value() > 0 {
					d.sharedNeeds = mergeSharedNeeds(d.sharedNeeds, []sharedNeed{{cards: clampInt32(qty.Value()), containers: 1}}, 1)
				}
			default:
				// 每个 Pod 只能指定一个 profile 是一条请求约束，但 Pod webhook 是在 Pod 对象上执行
				// 校验的，而 Kueue 在任何 Pod 创建之前就已根据 Pod 模板构建出 Workload——因此指定了
				// 两个 profile 的模板仍会到达此处。资源 map 的遍历顺序是随机的，因此取字典序最小的
				// 名称而非最后遍历到的名称：这种格式无论如何都会在创建 Pod 时被拒绝，这样做可以保证
				// 判定消息在多次调谐间保持稳定，而不会在两个名称之间来回跳变。
				if profile, ok := nodefeature.PartitionedProfileOf(name); ok {
					if d.profile == "" || profile < d.profile {
						d.profile = profile
					}
				}
			}
		}
	}

	out := make([]familyDemand, 0, len(byFamily))
	for _, d := range byFamily {
		// 使用 int64 按 podset 的 Pod 数量等比放大并做饱和截断，防止精心构造的单容器请求数
		// 乘以较大的 Pod 数量后溢出回绕为较小的数值（甚至负数）。背后没有卡数请求的孤立子键，
		// 以及 Pod 数为零的 podset，其所需卡数均为零。
		d.cards = clampInt32(int64(d.cards) * int64(count))
		if d.cards <= 0 {
			continue
		}
		d.sharedNeeds = mergeSharedNeeds(nil, d.sharedNeeds, count)
		out = append(out, *d)
	}
	sortDemands(out)
	return out
}

// podSetFamilyDemand 读取 podset 中针对指定数量 Pod 的单个资源族需求。所传入的资源族
// 必定在 podSetFamilyDemands 已为该 podset 返回的结果中，因此必然存在。
func podSetFamilyDemand(ps *kueue.PodSet, family nodefeature.ResourceFamily, count int32) familyDemand {
	for _, d := range podSetFamilyDemands(ps, count) {
		if d.family == family {
			return d
		}
	}
	return familyDemand{family: family}
}

// isCardKey 判断 name 是否为该资源族的卡数键——即其值表示卡数的键。同一资源族下的
// 其他键均为单卡预算或 profile 锚点。
func isCardKey(name core.ResourceName, family nodefeature.ResourceFamily) bool {
	switch family {
	case nodefeature.ResourceFamilyExclusive:
		return true
	case nodefeature.ResourceFamilyShared:
		return strings.HasSuffix(string(name), nodefeature.SharedResourceNameSuffix)
	case nodefeature.ResourceFamilySliced:
		return strings.HasSuffix(string(name), nodefeature.SlicedResourceNameSuffix)
	case nodefeature.ResourceFamilyPartitioned:
		return strings.HasSuffix(string(name), nodefeature.PartitionedResourceNameSuffix)
	}
	return false
}

// mergedContainerResources 合并容器的 Limits 与 Requests，Requests 优先，以便每个
// 资源只读取一次。扩展资源（加速器键）通常仅设置在 limits 中，若只扫描 Requests
// 会漏掉请求，从而错误地将可行性标记为 Ready。
func mergedContainerResources(ctr *core.Container) core.ResourceList {
	if len(ctr.Resources.Limits) == 0 {
		return ctr.Resources.Requests
	}
	merged := make(core.ResourceList, len(ctr.Resources.Requests)+len(ctr.Resources.Limits))
	for name, qty := range ctr.Resources.Limits {
		merged[name] = qty
	}
	for name, qty := range ctr.Resources.Requests {
		merged[name] = qty
	}
	return merged
}

// mergeDemand 将单个 podset 的需求折入累计列表：若存在形状相同的已有条目则累加卡数，
// 否则追加新条目。单卡预算不同的两个 podset 保持独立，互不影响彼此的校验。
//
// 分配的 flavor 也是该形状的一部分。位于同一 flavor 上的两个 podset 确实会竞争同一批
// 卡，因此合并它们才能如实反映共享预算；位于不同 flavor 上的两个 podset 必须由互不
// 重叠的卡集合来满足，若将它们合并，就会让一个角色的卡去顶替另一个角色的需求——导致
// 检查在根本无法放置该请求的资源池上误报 Ready。
func mergeDemand(demands []familyDemand, d familyDemand) []familyDemand {
	for i := range demands {
		if demands[i].family == d.family &&
			demands[i].unitsPerCard == d.unitsPerCard &&
			demands[i].profile == d.profile &&
			demands[i].flavor == d.flavor &&
			demands[i].node == d.node {
			demands[i].cards = clampInt32(int64(demands[i].cards) + int64(d.cards))
			demands[i].sharedNeeds = mergeSharedNeeds(demands[i].sharedNeeds, d.sharedNeeds, 1)
			demands[i].podSets = append(demands[i].podSets, d.podSets...)
			return demands
		}
	}
	return append(demands, d)
}

// mergeSharedNeeds 返回将 add 中所有需求折入 acc 后的结果，先将 add 的容器数量乘以
// times，再按不同卡数合并为唯一条目，并按卡数从大到小排序。该函数绝不修改任何输入，
// 因为需求按值拷贝，若直接修改会导致共享同一底层切片。
func mergeSharedNeeds(acc, add []sharedNeed, times int32) []sharedNeed {
	byCards := make(map[int32]int64, len(acc)+len(add))
	for _, n := range acc {
		byCards[n.cards] += int64(n.containers)
	}
	for _, n := range add {
		byCards[n.cards] += int64(n.containers) * int64(times)
	}
	var out []sharedNeed
	for _, cards := range slices.Sorted(maps.Keys(byCards)) {
		if containers := clampInt32(byCards[cards]); containers > 0 {
			out = append(out, sharedNeed{cards: cards, containers: containers})
		}
	}
	slices.Reverse(out)
	return out
}

// sortDemands 按约束最紧优先的顺序排列需求——切分请求排在标量请求之前，随后按单卡
// 单位数降序排列——从而使 nodeDevicesFeasibility 中的贪心匹配绝不会占用更严格需求
// 所必需的卡，同时保证判定结果具有确定性。
func sortDemands(demands []familyDemand) {
	slices.SortStableFunc(demands, func(a, b familyDemand) int {
		partitionedA := a.family == nodefeature.ResourceFamilyPartitioned
		partitionedB := b.family == nodefeature.ResourceFamilyPartitioned
		if c := slicex.CompareTrueFirst(partitionedA, partitionedB); c != 0 {
			return c
		}
		if a.unitsPerCard != b.unitsPerCard {
			return cmp.Compare(b.unitsPerCard, a.unitsPerCard)
		}
		if c := cmp.Compare(a.family, b.family); c != 0 {
			return c
		}
		return cmp.Compare(a.node, b.node)
	})
}

// clampInt32 将资源数量值收窄为账本使用的 int32，并在上下界处做饱和截断而不回绕，
// 从而避免超大（或恶意构造的）请求溢出回绕为较小或负的需求量。
func clampInt32(v int64) int32 {
	switch {
	case v > math.MaxInt32:
		return math.MaxInt32
	case v < 0:
		return 0
	default:
		return int32(v)
	}
}

// cardLedger 是单张卡的视图，按加速器 ID 关联 Spec 侧的能力（该卡可服务的资源族、
// 物理切分 profile 及其缓存的放置方案）与 Status 侧的分配状态（模式、标量剩余量以及
// 按 profile 划分的剩余账本）。
type cardLedger struct {
	// manufacturer 是该卡所属厂商；由于下方的按 profile 账本使用厂商拼写形式的 profile
	// 名称作为键，而需求中携带的是公开拼写形式，因此需要保留该字段用于转换。
	manufacturer string
	// flavors 是已分配给该卡的 ResourceFlavor 集合——即选择器匹配其所在节点且 nodeLabels
	// 固定了其自身加速器键的 flavor。混合型号节点上的卡只出现一次，并附带实际覆盖它的
	// flavor 集合，因此无论有多少 flavor 覆盖到它，下方的单卡预算都只会扣减一次。
	flavors sets.Set[kueue.ResourceFlavorReference]
	// hostname 是该卡所在节点的 kubernetes.io/hostname 标签值，也是拓扑分配用来标识
	// 节点的名称。若无法读取 Node，则为空，此时任何限定节点的需求都不能使用该卡。
	hostname          string
	capability        workercore.AcceleratorStatus
	mode              workercore.DeviceAllocationMode
	remaining         int32
	remainingProfiles []workercore.AcceleratorProfileCount
	// allocatedSlices 是账本中记录的该卡已承载的逻辑切片数量。
	allocatedSlices int32
}

// coveredBy 判断卡能否服务这条需求。卡必须属于 Kueue 实际分配的 flavor；读不到分配时，
// 不能默默放宽到整个池。请求还指定了节点时，只能使用该节点上的卡。
func (c cardLedger) coveredBy(d familyDemand) bool {
	return c.flavors.Has(d.flavor) && (d.node == "" || c.hostname == d.node)
}

// flavorScope 表示单个已分配的 ResourceFlavor 对某个节点的覆盖声明：包含 flavor 本身，
// 以及其 nodeLabels 固定的加速器设备键（"<manufacturer>-<id>"）。两者共同决定它覆盖
// 节点上的哪些卡——仅凭设备键会把同型号但节点批次不同的两个 flavor 混为一谈，而仅凭
// 节点会把混合型号节点上同时携带的其他型号卡混入其中。
type flavorScope struct {
	flavor         kueue.ResourceFlavorReference
	acceleratorKey string
	// acceleratorCount 是该 flavor 通过 "<key>.count" nodeLabel 固定的该型号单节点卡数。
	// 它是 flavor 身份的一部分——名称中编码了该数量，且 Kueue 只会将 Pod 准入到该批次
	// 的节点上——但 Devices 选择器无法携带该信息（DeviceManager 刻意不在 Devices 对象的
	// 标签中添加 ".count"），因此列表查询会忽略批次信息，并在此处按卡组重新应用：
	//
	//   - 若缺失该校验，同型号的 4 卡 flavor 与 8 卡 flavor 会互相认领对方的节点，从而使
	//     4 卡节点上的空闲卡让 8 卡 flavor 的需求报告 Ready，而其 Pod 却一直处于 Pending。
	//   - 0 表示该 flavor 未固定批次，可覆盖任意卡数；负值表示它固定了一个无法解析的批次，
	//     不覆盖任何卡（见 flavorAcceleratorCount）。
	acceleratorCount int
}

// scopedDevices 是单个节点的 Devices 账本，连同其选择器匹配该节点的已分配 flavor 列表。
// 当节点携带多种型号的加速器时，一个节点会支撑多个 flavor，因此 scopes 是一个列表，具体
// 每个 scope 覆盖哪些卡将在逐卡级别根据加速器键确定。
type scopedDevices struct {
	devices workercore.Devices
	scopes  []flavorScope
	// hostname 是节点的 kubernetes.io/hostname 标签值；若无法读取 Node，则为空。
	hostname string
}

// collectCards 将所有候选节点上的加速器展平为一个列表，按加速器 ID 关联每张卡的
// Status 分配状态与 Spec 能力，并为每张卡标记覆盖它的已分配 flavor。
//
// 采用带逐卡覆盖标记的扁平单列表，而非按 flavor 分别建表，是为了保持预算全局统一：
// 被分配了同一 flavor 的两个角色会竞争相同的卡，而混合型号节点上同时服务于两个 flavor
// 的同一张卡也绝不会被重复扣减两次。
func collectCards(pool []scopedDevices) []cardLedger {
	var cards []cardLedger
	for i := range pool {
		d := &pool[i].devices
		capByID := make(map[string]workercore.AcceleratorStatus)
		for gi := range d.Spec.Groups {
			accs := d.Spec.Groups[gi].Accelerators
			for ai := range accs {
				capByID[accs[ai].ID] = accs[ai].Status
			}
		}
		for gi := range d.Status.Groups {
			g := &d.Status.Groups[gi]
			aKey := acceleratorKeyOf(g.Manufacturer, g.ID)
			// 覆盖该组内每张卡的 flavor 集合：需同时满足选择器匹配所在节点、自身的加速器键
			// 等于该卡组的型号，且其固定的节点批次等于该卡组的卡数——这三项共同构成了该
			// flavor 的完整身份语义。
			covering := sets.New[kueue.ResourceFlavorReference]()
			for _, s := range pool[i].scopes {
				if s.acceleratorKey == aKey &&
					(s.acceleratorCount == 0 || s.acceleratorCount == len(g.Accelerators)) {
					covering.Insert(s.flavor)
				}
			}
			for ai := range g.Accelerators {
				acc := &g.Accelerators[ai]
				cards = append(cards, cardLedger{
					manufacturer:      g.Manufacturer,
					flavors:           covering,
					hostname:          pool[i].hostname,
					capability:        capByID[acc.ID],
					mode:              acc.Mode,
					remaining:         acc.Remaining,
					remainingProfiles: acc.RemainingProfiles,
					allocatedSlices:   acc.AllocatedSlices,
				})
			}
		}
	}
	return cards
}

// _unreadableAcceleratorBatch 表示 flavor 声明了本 operator 无法解析的批次时的标记值。
// 该值为负数，因此没有任何卡组的长度能与之相等，此类 flavor 不会覆盖任何卡。
const _unreadableAcceleratorBatch = -1

// flavorAcceleratorCount 读取 flavor 为其自身加速器键固定的单节点卡数——即
// NodeFlavorReconciler 随裸键一同打上的 "<key>.count" nodeLabel。0 表示“任意批次”，
// 在 flavor 未固定任何键或该标签缺失时返回：未声明批次的 flavor 不应被收窄为不覆盖任何卡。
//
// 若该标签存在但其值不是正整数，则属于相反情况并采用失败关闭策略。Kueue 仍然只会将 Pod
// 准入到携带该精确标签值的节点上，而没有节点会带有无法解析的标签值——因此若在此处将
// 此类 flavor 放宽为“任意批次”，会导致其他批次的空闲卡让需求报告 Ready，而其 Pod 却
// 永远停留在 Pending。批次是 flavor 身份中 Devices 选择器唯一无法携带的部分，因此无法
// 解析的批次根本无法校验，本检查会挂起该 Workload 而不是凭空猜测。
func flavorAcceleratorCount(rf *kueue.ResourceFlavor, acceleratorKey string) int {
	if acceleratorKey == "" {
		return 0
	}
	raw, pinned := rf.Spec.NodeLabels[nodefeature.AcceleratableFeatureLabelPrefix+acceleratorKey+
		_ResourceFlavorCountLabelSuffix]
	if !pinned {
		return 0
	}
	count, err := strconvx.Atoi[int](raw)
	if err != nil || count <= 0 {
		return _unreadableAcceleratorBatch
	}
	return count
}

// acceleratorKeyOf 按照与节点标签相同的方式构建设备组的加速器键——即 "<manufacturer>-<group id>"，
// 也是 ResourceFlavor 以 "acceleratable.feature.gpustack.ai/<key>" 形式固定的键。在此处
// 直接推导而非从标签中回读，可以使卡侧逻辑不依赖节点标签是否处于最新状态。
func acceleratorKeyOf(manufacturer, groupID string) string {
	return manufacturer + "-" + groupID
}

// servesFamily 判断该卡上报的能力是否能够服务于指定资源族。逻辑切片与硬件切分是互斥的
// 卡状态，且已切分的卡不再能作为整卡使用——因此此处将无法服务某资源族的卡从该资源族的
// 候选集合中排除，与 DevicePlugin 令牌池的逻辑完全一致。否则，独占租户的 Pod 会被
// 误判为可调度到已切分的卡上，获得准入后却永远处于 Pending 状态。
func (c cardLedger) servesFamily(family nodefeature.ResourceFamily) bool {
	switch family {
	case nodefeature.ResourceFamilyPartitioned:
		return device.IsPartitioned(c.capability)
	case nodefeature.ResourceFamilySliced:
		return device.IsLogicallySliceable(c.capability)
	default:
		return device.IsWholeAcceleratorCapable(c.capability)
	}
}

// cardBudget 记录 Workload 中已检查的请求从单张卡上占用的资源，避免后续请求重复使用同一空间。
// 独占请求占用整张卡；共享请求按每个持有者占用一份所有权份额；逻辑切分请求占用其所需
// units 以及每份切片对应的一个切片令牌；分区请求占用单张卡可承载的多个放置槽位之一。
// 分区与其他类型之间不会竞争——两者对应的设备群体按能力彼此隔离——因此每张卡只需
// 一个预算即可，且承载份额或切片的卡不会服务于其他资源族，因为设备插件会将其锁定在该模式。
type cardBudget struct {
	whole      bool
	shares     int32
	units      int32
	slices     int32
	placements int32
}

// nodeDevicesFeasibility 报告当前所有请求是否能同时放置到候选设备上，并返回检查状态及
// 相应的说明信息。候选设备涵盖 Workload 所分配的各 flavor 触及的全部节点，而非单个 flavor
// 的资源池：每张卡都会记录覆盖它的 flavor，而每个请求仅匹配其所属 podset 被分配的 flavor
// 对应的卡。
//
// 空间在匹配时即被消耗，因此同一 Workload 的两个请求绝不会重复占用同一资源。返回的
// 消息来自起决定作用的请求：即第一个无法满足的请求，或在全部满足时为最后一个检查的
// 请求。资源不足始终视为瞬态情况：返回 Retry，从不返回 Reject。
//
// 判定结果是否指明角色取决于每个请求自身的来源信息；至于 Workload 的结构是否值得
// 说明角色，则由调用方而非本函数决定。
//
// 匹配过程采用贪心策略：每个请求优先占用最先找到的可用卡，且不会退回已占用的卡来让
// 后续请求放入。当各请求对应的设备集合彼此不相交或呈嵌套关系时（本 operator 构建的
// flavor 正是如此——每张卡只有一种型号，同型号的不同 flavor 按节点批次区分），该策略是
// 精确的，因为同一 Workload 的两个 flavor 不会触及同一张卡。
//
// 若管理员自定义的 flavor 选择器存在非对称重叠，覆盖大量卡的请求可能会占用某张
// 范围更窄的请求也需要的卡，从而导致后者被阻塞（尽管换一种顺序本可两者都放下）。
// 对请求与卡做二分图匹配可以解决这一问题；此处未采用的原因在于误差的方向，任何顺序
// 都不会改变这一点：请求只会占用自己允许使用的卡，因此该算法最多误报不存在的资源不足，
// 而绝不会误报不存在的可用空间。
//
// inflight 请求来自已回复 Ready 但其 Pod 尚未记入账本的 Workload。它们会先在同一预算
// 上进行匹配，从而只根据它们留下的剩余空间来评估当前 Workload。即使某个 inflight 请求
// 未能完全放下，它也会保留已占用的空间：此处无法给它的部分仍可能由设备插件分配，
// 若将其忽略就会误报已被承诺出去的空间。
func nodeDevicesFeasibility(pool []scopedDevices, inflight, demands []familyDemand) (kueue.CheckState, string) {
	message := verdictMessage(kueue.CheckStateReady, nil)
	if len(demands) == 0 {
		return kueue.CheckStateReady, message
	}

	cards := collectCards(pool)
	budgets := make([]cardBudget, len(cards))
	for _, d := range inflight {
		fitDemand(cards, budgets, d)
	}
	for _, d := range demands {
		var state kueue.CheckState
		state, message = fitDemand(cards, budgets, d)
		if state != kueue.CheckStateReady {
			if len(inflight) != 0 {
				message += _inflightClause
			}
			return state, message
		}
	}
	return kueue.CheckStateReady, message
}

// _inflightClause 附加在计入 inflight 请求后得出 Retry 判定的消息末尾，使运维人员在账本中
// 看到空闲卡时知道它已被承诺给其他负载，而非被遗漏。
const _inflightClause = "; cards promised to Workloads admitted before this one, whose Pods have not" +
	" received them yet, are counted as taken"

// fitDemand 按请求的资源族类型进行匹配，并将占用的空间记入 budgets。
func fitDemand(cards []cardLedger, budgets []cardBudget, d familyDemand) (kueue.CheckState, string) {
	switch d.family {
	case nodefeature.ResourceFamilyPartitioned:
		return fitPartitionDemand(cards, budgets, d)
	case nodefeature.ResourceFamilyShared:
		return fitSharedDemand(cards, budgets, d)
	case nodefeature.ResourceFamilySliced:
		return fitSlicedDemand(cards, budgets, d)
	default:
		return fitExclusiveDemand(cards, budgets, d)
	}
}

// withoutRoles 剥离每个请求的来源信息，使针对它们的判定文案与引入角色概念之前完全一致。
// 以副本形式返回：不会修改调用方持有的请求切片。
func withoutRoles(demands []familyDemand) []familyDemand {
	bare := make([]familyDemand, len(demands))
	for i, d := range demands {
		d.podSets = nil
		bare[i] = d
	}
	return bare
}

// fitExclusiveDemand 基于标量化的单卡剩余账本对独占请求进行准入判断：每张卡的初始值
// 为 ResourceMaxUnits 并减去各个 Pod 的已分配量，因此只要卡上存在任何分配，其 Remaining
// 就会低于整卡容量，绝无法满足独占请求。它的 cards 计数即卡数：每份都需要一张独立的卡。
// 当该资源族的设备集合中有足够数量的卡可满足时返回 Ready，否则返回 Retry。
func fitExclusiveDemand(cards []cardLedger, budgets []cardBudget, d familyDemand) (kueue.CheckState, string) {
	var fit int32
	for i := range cards {
		if budgets[i].whole || budgets[i].shares > 0 || budgets[i].slices > 0 || !cards[i].coveredBy(d) ||
			!cards[i].servesFamily(d.family) || cards[i].remaining < nodefeature.ResourceMaxUnits {
			continue
		}
		budgets[i].whole = true
		if fit++; fit >= d.cards {
			return kueue.CheckStateReady, demandVerdictMessage(kueue.CheckStateReady, d)
		}
	}
	return kueue.CheckStateRetry, demandVerdictMessage(kueue.CheckStateRetry, d)
}

// fitSharedDemand 按持有者逐个对共享请求进行准入判断。每个持有者需要在同一节点上获得
// 所需数量的独立卡（每张卡占一份所有权份额），而每张卡拥有多少空闲份额就能承载多少
// 个持有者，因为设备插件会独立分配每个容器的令牌，并按授予的每张卡扣除一份份额。
// 只有空闲卡或已处于共享模式的卡才能接受份额。
//
// 持有者按需求从大到小依次放置，每个都优先放到“仍有空闲份额的卡数最多”的节点上，
// 并优先选取该节点上空闲份额最多的卡。将最满的卡留到最后再消耗，可以为后续持有者
// 保留尽量多的可用卡，这样就能让多个 Pod 共享同一节点，而不是各自独占一张卡来评估。
func fitSharedDemand(cards []cardLedger, budgets []cardBudget, d familyDemand) (kueue.CheckState, string) {
	freeShares := func(i int) int32 {
		if budgets[i].whole || budgets[i].slices > 0 || !cards[i].coveredBy(d) {
			return 0
		}
		return cards[i].freeShares() - budgets[i].shares
	}

	for _, need := range d.sharedNeeds {
		for range need.containers {
			byNode := make(map[string][]int)
			for i := range cards {
				if freeShares(i) > 0 {
					byNode[cards[i].hostname] = append(byNode[cards[i].hostname], i)
				}
			}
			var chosen []int
			for _, host := range slices.Sorted(maps.Keys(byNode)) {
				if len(byNode[host]) > len(chosen) {
					chosen = byNode[host]
				}
			}
			if int32(len(chosen)) < need.cards {
				return kueue.CheckStateRetry, demandVerdictMessage(kueue.CheckStateRetry, d)
			}
			slices.SortStableFunc(chosen, func(a, b int) int { return cmp.Compare(freeShares(b), freeShares(a)) })
			for _, i := range chosen[:need.cards] {
				budgets[i].shares++
			}
		}
	}
	return kueue.CheckStateReady, demandVerdictMessage(kueue.CheckStateReady, d)
}

// fitSlicedDemand 按切片逐个对逻辑切分请求进行准入判断。其 cards 计数即切片数量，因为
// Pod webhook 会将每个容器的切片固定到单张卡上；一张卡能承载的切片数受其空闲 units 与
// 切片令牌数限制，因为设备插件会独立授予每个容器的令牌，并将该切片的 units 记在它落脚
// 的那张卡上。只有空闲卡或已处于切片模式的卡才能接受切片。
//
// 每个切片会放入“仍能容纳它的剩余 units 最少”的卡中，这与设备插件的装箱顺序一致：
// 优先填满已有切片的卡、最后再拆用未动过的整卡，从而将整卡留给后续更大的切片和独占请求。
//
// 卡的空闲槽位数等于其上限减去账本中已记录的切片、本 Workload 以及 inflight 请求在此
// 分配的切片后的余量，因此即使显存仍有大量剩余，装满小切片上限的卡也不会再接纳新切片。
func fitSlicedDemand(cards []cardLedger, budgets []cardBudget, d familyDemand) (kueue.CheckState, string) {
	// 在第 10 章实现此函数。
	return kueue.CheckStateRetry, "logical slice admission is pending"
}

// IMPORTANT: 算单张卡还能发几张共享门票？
// freeShares 返回在扣除当前 Workload 的任何请求前该卡还能授予的所有权份额数：除非该卡
// 能够服务整卡资源族且处于空闲或共享持有状态，否则返回 0；满足条件时则按剩余 units
// 折算，每份单位对应一个份额。
func (c cardLedger) freeShares() int32 {
	const share = nodefeature.ResourceMaxUnits / nodefeature.SharedResourceMaxSize
	// 两个一票否决的条件：
	// 	- 这张卡硬件上根本不能当整卡/共享卡用（例如已经被硬件切分成 MIG 分区了）
	// 	- 这张卡正在被独占模式或逻辑切片模式占用！DevicePlugin 严格禁止同一张卡跨模式混用，所以只有完全空闲（None）或者已经在共享模式（Shared）的卡才能接纳共享容器。
	if !c.servesFamily(nodefeature.ResourceFamilyShared) ||
		(c.mode != workercore.DeviceAllocationModeNone && c.mode != workercore.DeviceAllocationModeShared) {
		return 0
	}
	return c.remaining / share
}

// freeSliceUnits 返回在扣除当前 Workload 的任何请求前该卡还能提供给逻辑切片的 units：
// 除非该卡能够服务逻辑切片、处于空闲或切片持有状态且仍有空闲槽位，否则返回 -1；
// 满足条件时则返回其剩余 units。
func (c cardLedger) freeSliceUnits() int32 {
	if !c.servesFamily(nodefeature.ResourceFamilySliced) ||
		(c.mode != workercore.DeviceAllocationModeNone && c.mode != workercore.DeviceAllocationModeSliced) ||
		c.freeSlots() <= 0 {
		return -1
	}
	return c.remaining
}

// freeSlots 返回在扣除当前 Workload 的任何请求前该卡还能承载的逻辑切片数量。
func (c cardLedger) freeSlots() int32 {
	return device.LogicalSlotsFree(c.capability, c.allocatedSlices)
}

// fitPartitionDemand 基于单卡放置感知账本对分区请求进行准入判断：候选卡须已完成物理
// 分区、未被整卡占用（Mode 为 None 或 Partitioned），且该 profile 仍有空闲放置槽位。
// 其 cards 计数是 INSTANCE 数量——分区请求为每个 Pod 分配一张卡（规则 3），因此多副本
// Workload 会请求对应数量的实例——而单张卡可以承载多个实例，这正是 RemainingProfiles 所
// 报告的内容，也是以放置信息为准的 Allocate 将执行的行为。若按每卡仅计一个实例，即便
// 节点上有足够空间容纳所有副本，第一个之后的每个副本也会永久停留在 Retry。
//
// 对于完全没有已分区卡的资源池，以及已分区卡尚未发布缓存 Placements 的资源池（发布
// 时序偏差），都会分别给出不同的 Retry 消息（与“profile 暂时已满”区分开），以便运维
// 人员分辨这三种情况。绝不返回 Reject。
func fitPartitionDemand(cards []cardLedger, budgets []cardBudget, d familyDemand) (kueue.CheckState, string) {
	// 在第 11 章实现此函数。
	return kueue.CheckStateRetry, "partition admission is pending"
}

// NodeDevicesAdmissionReconciler 是面向 _NodeDevicesControllerName 的外部 Kueue
// AdmissionCheck 控制器：在 Kueue 预留配额后，它会读取所分配 flavor 资源池的每卡
// Devices 账本，当请求可放置时报告 Ready，当当前没有节点具备足够空闲卡时报告 Retry
// （credits 准入基于标量总量，无法感知整卡可用性）。它只做检查：从不抢占，也从不拒绝。
type NodeDevicesAdmissionReconciler struct {
	Client    ctrlcli.Client
	APIReader ctrlcli.Reader

	// readyUnobserved 按 namespace 和 name 保存本控制器已回复 Ready、但缓存中尚未体现该
	// Ready 写入结果的 Workload 快照。下一次评估会从缓存读取其他 Workload，若缓存仍停留在
	// 写入之前，就会漏计该 inflight Workload。该字段仅在串行执行的 Reconcile 中读写。
	readyUnobserved map[types.NamespacedName]*kueue.Workload
}

var _ ctrlreconcile.Reconciler = (*NodeDevicesAdmissionReconciler)(nil)

func (r *NodeDevicesAdmissionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := ctrllog.FromContext(ctx)

	wl := new(kueue.Workload)
	if err := r.Client.Get(ctx, req.NamespacedName, wl); err != nil {
		// 已删除的 Workload 在其删除事件触发时会再调谐一次；此时无需进行任何判定。
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		logger.Error(err, "fetch workload")
		return ctrl.Result{}, err
	}

	// 仅评估处于活跃状态、尚未完成且已持有配额预留的 Workload：在预留前无从确认，而在被
	// 驱逐或完成后判定已无意义。
	//
	// 驱逐状态需要单独检查，因为 Kueue 将检查重置为 Pending 与撤销配额预留分两次写入，
	// 在这两步之间，已被驱逐的 Workload 仍会显示持有预留。若用新判定覆盖该重置操作，会
	// 导致 Workload 永久卡死：在驱逐条件置位期间 Kueue 会停止重置检查，调度器在检查处于
	// Retry 时拒绝预留配额，而没有配额预留本控制器又会停止评估。只有重新预留配额才会清除
	// 驱逐条件，从而重新开启评估。
	if !kueueworkload.HasQuotaReservation(wl) || kueueworkload.IsEvicted(wl) ||
		kueueworkload.IsFinished(wl) || !kueueworkload.IsActive(wl) {
		logger.V(3).Info("skip workload not holding quota reservation, evicted, finished or deactivated")
		return ctrl.Result{}, nil
	}

	// Workload 一旦获准入，其放置位置即已确定，自身的设备分配也已从单卡账本中扣除。若再次
	// 检查，会将其自身已占用的分配量算作对自己的扣减——超过半张卡的切片会让该卡的 Remaining
	// 低于刚刚满足的请求——从而把检查结果翻转为 Retry，使健康运行中的 Workload 在重建循环中
	// 被反复驱逐。准入闸门只需在准入前把关（在卡释放前保持 Retry）；准入之后已无需再拦截，
	// 因此保持已落定的判定不变。
	if kueueworkload.IsAdmitted(wl) {
		logger.V(3).Info("skip already-admitted workload; placement settled")
		return ctrl.Result{}, nil
	}

	// 仅限本控制器负责的检查项。
	checks, err := kueueadmissioncheck.FilterForController(ctx, r.Client, wl.Status.AdmissionChecks, _NodeDevicesControllerName)
	if err != nil {
		logger.Error(err, "filter admission checks for controller")
		return ctrl.Result{}, err
	}
	if len(checks) == 0 {
		return ctrl.Result{}, nil
	}

	// IMPORTANT: 该方法把原本笼统的“总共要 3 张整卡”，拆成两条精确到节点的硬指标：
	//   - 1. host-a 上必须能找出 2 张完整空卡（node="host-a", cards=2）
	//   - 2. host-b 上必须能找出 1 张完整空卡（node="host-b", cards=1）
	demands := parseFamilyDemands(wl)

	// 判定结果中的角色从句用于指明涉及哪个角色，只有当 Workload 拥有多个需要区分的 podset
	// 时才有必要说明。这取决于 Workload 自身的结构而非解析出的请求数量，因为包含一个加速卡
	// 角色和一个纯 CPU 角色的 Workload 只会解析出单个请求，但仍需指明该角色名称。真正的
	// 单 podset Workload——即所有由普通 Pod 生成的 Workload——不存在歧义，因此其判定文案
	// 与引入角色之前保持完全一致，而不会多出提及 Kueue 默认 podset 的冗余从句。
	if len(wl.Spec.PodSets) <= 1 {
		demands = withoutRoles(demands)
	}

	// 若请求所属 podset 没有已分配的 flavor，会在列举任何卡之前被显式拦截于此。即使不拦截，
	// 匹配阶段也会拦住它——未命名的 flavor 不会覆盖任何卡——但那是间接得出的结果，而这种
	// 异常状态足够严重，必须直接说明原因而非靠推断：否则该资源池的所有加速卡 Workload 都会
	// 停在 Retry，且判定信息中毫无指向根本原因的线索。Kueue 自身的 SetQuotaReservation 会
	// 解引用其保存的 admission，因此不可能产生缺少 admission 的已预留 Workload；走到这里说明
	// 是 Kueue 之外的组件写入了 status，或 Kueue 自身的契约发生了变化。无论哪种情况，运维
	// 人员都能直接在 Workload 上看到原因，而不是面对一场莫名的全池停滞。
	if unassigned := unassignedDemands(demands); len(unassigned) > 0 {
		roles := rolesOf(unassigned)
		logger.Info("holding workload whose podset carries no assigned flavor",
			"roles", roles)
		if err := r.applyVerdict(ctx, wl, checks, kueue.CheckStateRetry, unassignedFlavorMessage(roles)); err != nil {
			return objectWriteResult(logger, err, "patch admission check state", ctrl.Result{})
		}
		return ctrl.Result{}, nil
	}

	devices, err := r.candidateDevices(ctx, wl)
	if err != nil {
		logger.Error(err, "list candidate devices")
		return ctrl.Result{}, err
	}

	// 若请求的 flavor 能够触及资源池中的节点，但无法在节点上解析出任何卡集合，则以此为由
	// 予以拦截。匹配阶段同样会拦住它——无键作用域覆盖不到任何卡——但那样运维人员面对
	// 明明有空闲卡的资源池，看到的却会是“没有节点拥有足够空闲卡”，且再怎么等待也不会
	// 恢复：真正需要修改的是 flavor 自身的标签。它与上方缺少分配时的拦截所要防止的属于
	// 同一类误报，只是发生在更后一步，因为只有在读取 flavor 之后才能发现这一原因。
	if unresolved := unresolvedDemands(devices, demands); len(unresolved) > 0 {
		roles := rolesOf(unresolved)
		logger.Info("holding workload whose assigned flavor resolves to no card population",
			"roles", roles)
		if err := r.applyVerdict(ctx, wl, checks, kueue.CheckStateRetry, unresolvedFlavorMessage(roles)); err != nil {
			return objectWriteResult(logger, err, "patch admission check state", ctrl.Result{})
		}
		return ctrl.Result{}, nil
	}

	// 账本根据绑定到各节点的 Pod 重建，已被计费的 Pod 也读取自同一份列表，因此一个 inflight
	// Workload 要么计入账本，要么计为 inflight，绝不会重复计入，也不会两处都漏计。
	charged, err := r.chargeLedgers(ctx, devices, demands)
	if err != nil {
		logger.Error(err, "rebuild ledgers from bound pods")
		return ctrl.Result{}, err
	}
	inflight, unresolvedNode, err := r.inflightDemands(ctx, wl, devices, charged)
	if err != nil {
		logger.Error(err, "collect inflight demands")
		return ctrl.Result{}, err
	}
	if unresolvedNode != "" {
		logger.Info("holding workload behind an inflight workload whose flavor resolves to no cards",
			"node", unresolvedNode)
		if err := r.applyVerdict(ctx, wl, checks, kueue.CheckStateRetry, unresolvedInflightMessage(unresolvedNode)); err != nil {
			return objectWriteResult(logger, err, "patch admission check state", ctrl.Result{})
		}
		return ctrl.Result{}, nil
	}

	state, message := nodeDevicesFeasibility(devices, inflight, demands)

	if err := r.applyVerdict(ctx, wl, checks, state, message); err != nil {
		return objectWriteResult(logger, err, "patch admission check state", ctrl.Result{})
	}
	// demandsSummary 会立即执行格式化，因此加一层日志级别门控：此逻辑会在每次 Workload 调谐时运行。
	if logger.V(2).Enabled() {
		logger.V(2).Info("evaluated node-devices admission",
			"state", state, "demands", demandsSummary(demands), "inflight", demandsSummary(inflight))
	}
	return ctrl.Result{}, nil
}

// unassignedDemands 返回其 podset 无法解析为单一加速卡 flavor 的请求。
// 其成因虽有五种表现形式——Workload 不包含 admission、admission 未指定该 podset 的分配、
// 该分配未携带 flavor、仅携带了其他资源的 flavor、或携带了多个加速卡 flavor 且无法确定
// 唯一目标——但这五种情况都会使请求无法放置，因此统一处理而不作细分。
//
// 不请求加速卡的 Workload 不会返回任何项，因此普通的纯 CPU 场景不受影响。这里返回请求
// 本身而非其角色名，是因为单 podset Workload 同样必须触发该拦截，而它的请求不带可用于
// 查找的角色名称。
func unassignedDemands(demands []familyDemand) []familyDemand {
	var unassigned []familyDemand
	for _, d := range demands {
		if d.flavor == "" {
			unassigned = append(unassigned, d)
		}
	}
	return unassigned
}

// unresolvedDemands 返回其分配的 flavor 触及了候选资源池中的节点、却无法在节点上解析出
// 任何卡集合的请求——即该 flavor 未固定加速卡键或固定了多个键，导致无法判定其对应哪种
// 型号的卡。这两种情况只有通过管理员自定义的 flavor 才会触发。
//
// “触及节点”这一限定条件将其与普通的容量不足区分开：选择器未匹配到任何节点的 flavor
// 不会产生任何作用域，此时给出容量不足判定才是正确的——资源池里确实没有卡。这里报告的
// 则是节点本身确实在资源池内，但没有任何作用域能指明节点上哪些卡属于该 flavor 的情况。
func unresolvedDemands(pool []scopedDevices, demands []familyDemand) []familyDemand {
	reached := sets.New[kueue.ResourceFlavorReference]()
	resolved := sets.New[kueue.ResourceFlavorReference]()
	for i := range pool {
		for _, s := range pool[i].scopes {
			reached.Insert(s.flavor)
			if s.acceleratorKey != "" {
				resolved.Insert(s.flavor)
			}
		}
	}

	var unresolved []familyDemand
	for _, d := range demands {
		if d.flavor != "" && reached.Has(d.flavor) && !resolved.Has(d.flavor) {
			unresolved = append(unresolved, d)
		}
	}
	return unresolved
}

// unresolvedFlavorMessage 用于解释一种永远无法通过释放容量来解除的阻塞，因此它将问题
// 指向 flavor 而非资源池：卡就在那里，阻止它们被计入的并非负载本身或集群的资源利用率。
func unresolvedFlavorMessage(roles []kueue.PodSetReference) string {
	return "the ResourceFlavor assigned" + forRoles(roles) +
		" pins no single accelerator key, so which cards of its nodes are its own cannot be" +
		" resolved; correct the flavor's node labels (will retry)"
}

// rolesOf 返回给定请求所属的 podset 列表，经排序并去重。当请求不携带来源信息时返回空，
// 这正是单 podset Workload 的判定文案能够保持与引入角色前一致的原因。
func rolesOf(demands []familyDemand) []kueue.PodSetReference {
	roles := sets.New[kueue.PodSetReference]()
	for _, d := range demands {
		roles.Insert(d.podSets...)
	}
	if roles.Len() == 0 {
		return nil
	}
	return sets.List(roles)
}

// unassignedFlavorMessage 用于解释一种无论多少空闲容量都无法解除的阻塞，因此它指出缺失了
// 什么而非什么已满：否则运维人员在一个并不缺资源的池子上看到的判定会是“没有节点拥有足够
// 空闲卡”。
func unassignedFlavorMessage(roles []kueue.PodSetReference) string {
	return "the Workload's admission does not resolve to a single accelerator flavor" + forRoles(roles) +
		", so no card population can be resolved; will retry once it does"
}

// demandsSummary 为控制器日志格式化已解析的请求；由于 familyDemand 的字段均为未导出
// 字段，默认的结构体格式化不会输出任何有效信息。
func demandsSummary(demands []familyDemand) string {
	parts := make([]string, 0, len(demands))
	for _, d := range demands {
		part := fmt.Sprintf("%s cards=%d", d.family, d.cards)
		if d.node != "" {
			part += " node=" + d.node
		}
		if d.profile != "" {
			part += " profile=" + d.profile
		}
		if d.unitsPerCard > 0 {
			part += fmt.Sprintf(" units=%d", d.unitsPerCard)
		}
		parts = append(parts, part)
	}
	return strings.Join(parts, "; ")
}

// candidateDevices 收集支撑 Kueue 分配给该 Workload 的所有 flavor 的各节点 Devices 账本，
// 通过 flavor 的 nodeLabels（即 DeviceManager 标记到每个 Devices 对象上的相同标签）进行
// 定位。非加速卡 flavor 不会匹配任何 Devices，因此遍历所有已分配的 flavor 是安全的。
// 该列表不经过缓存读取：worker manager 并不 watch Devices。
func (r *NodeDevicesAdmissionReconciler) candidateDevices(ctx context.Context, wl *kueue.Workload) ([]scopedDevices, error) {
	if wl.Status.Admission == nil {
		return nil, nil
	}
	flavorRefs := sets.New[kueue.ResourceFlavorReference]()
	for i := range wl.Status.Admission.PodSetAssignments {
		for _, ref := range wl.Status.Admission.PodSetAssignments[i].Flavors {
			flavorRefs.Insert(ref)
		}
	}

	// 每个节点对应一条记录，携带所有选择器匹配到该节点的已分配 flavor：拥有两种加速卡型号的
	// 节点会按型号分别支撑不同的 flavor，但其卡必须只列举一次，以防单卡预算被重复消耗。
	byName := make(map[string]*scopedDevices)
	var order []string
	for ref := range flavorRefs {
		rf := new(kueue.ResourceFlavor)
		if err := r.Client.Get(ctx, ctrlcli.ObjectKey{Name: string(ref)}, rf); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, err
		}
		// 空选择器会匹配所有 Devices 对象；将其跳过。
		if len(rf.Spec.NodeLabels) == 0 {
			continue
		}
		aKey := flavorAcceleratorKey(rf)
		scope := flavorScope{
			flavor:           ref,
			acceleratorKey:   aKey,
			acceleratorCount: flavorAcceleratorCount(rf, aKey),
		}
		// flavor 的 nodeLabels 中包含一个用于 Kueue 调度的 ".count" 节点批次固定标签，而
		// DeviceManager 会刻意不将其写入 Devices 对象的选择器标签中；因此需去掉该标签以便
		// List 能匹配成功——其余的真实 CPU 键、acceleratable 键、os/arch 与 managed 标签均会保留，
		// 而这些正是 NodeDevicesReconciler 与 DeviceManager 共同标记到 Devices 上的标签。
		sel := make(ctrlcli.MatchingLabels, len(rf.Spec.NodeLabels))
		for k, v := range rf.Spec.NodeLabels {
			if strings.HasSuffix(k, _ResourceFlavorCountLabelSuffix) {
				continue
			}
			sel[k] = v
		}
		list := new(workercore.DevicesList)
		if err := r.APIReader.List(ctx, list, sel); err != nil {
			return nil, err
		}
		for i := range list.Items {
			name := list.Items[i].Name
			sd, seen := byName[name]
			if !seen {
				sd = &scopedDevices{devices: list.Items[i]}
				byName[name] = sd
				order = append(order, name)
			}
			sd.scopes = append(sd.scopes, scope)
		}
	}

	// 按节点名称排序，从而确保贪心匹配及其判定结果在每次调谐中保持一致。此处首次出现顺序
	// 并不稳定：它是通过遍历 flavor 引用集合构建的（Go 会随机化遍历顺序），且每次 List
	// 返回的结果也没有顺序保证。由于匹配过程按列表顺序消耗单卡预算，不稳定的顺序可能会改变
	// 请求所消耗的合规卡。
	slices.Sort(order)
	pool := make([]scopedDevices, 0, len(order))
	for _, name := range order {
		sd := byName[name]
		hostname, err := r.nodeHostname(ctx, name)
		if err != nil {
			return nil, err
		}
		sd.hostname = hostname
		pool = append(pool, *sd)
	}
	return pool, nil
}

// nodeHostname 读取指定 Node 的 kubernetes.io/hostname 标签——Devices 对象以 Node 名称
// 命名，而拓扑分配则通过该标签指代节点，两者未必一致。若 Node 已不存在，则返回空
// hostname 而非报错：其上的卡将不再服务任何节点作用域的请求，从而拦截此类请求，避免
// 将其准入到已经离开集群的节点上。
func (r *NodeDevicesAdmissionReconciler) nodeHostname(ctx context.Context, name string) (string, error) {
	nd := new(core.Node)
	if err := r.Client.Get(ctx, ctrlcli.ObjectKey{Name: name}, nd); err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil
		}
		return "", err
	}
	return nd.Labels[core.LabelHostname], nil
}

// flavorAcceleratorKey 读取 ResourceFlavor 在其 spec.nodeLabels 中固定的加速卡设备键——
// 即 NodeFlavorReconciler 写入的 "acceleratable.feature.gpustack.ai/<key>=true" 项。
// CPU flavor 不携带此键并返回空字符串，它不会匹配任何卡组的键，因此即便其更宽泛的选择器
// 也能触及某些节点的 Devices，也不会贡献任何卡。
//
// 判定逻辑直接复用 nodefeature 自身对 flavor 标签的函数，而不在本地重写。它决定了角色
// 所对应的卡集合范围；此前本地维护的一份副本曾发生过逻辑漂移——将句点误判为元数据后缀，
// 导致合法的组 ID（device.NormalizeName 会保留 "-"、"_" 和 "."）被丢弃，进而使该
// flavor 上的所有 Workload 在卡空闲的情况下仍一直停在 Retry。
//
// nodefeature.ExtractNodeFlavors 会为每个 flavor 精确写入一个 acceleratable 键，因此
// 固定了多个键的 flavor 违反了该写入方的不变量——这也是本函数无法判定的情况，因为其
// 覆盖的卡存在真实歧义。任取其一都会把角色武断地限定到某种型号，因此一个也不取：该
// flavor 不覆盖任何卡。面对歧义输入，扣留容量是唯一不会出错的做法。
//
// 空键本身并不构成最终判定，此处也无法直接拦截 Workload：请求中仍带有 Kueue 分配的
// flavor，真正将无键作用域转化为明确阻塞原因的是 unresolvedDemands，它会检查该作用域
// 最终落入的资源池。
func flavorAcceleratorKey(rf *kueue.ResourceFlavor) string {
	keys := nodefeature.ExtractAcceleratableKeys(rf.Spec.NodeLabels)
	if len(keys) != 1 {
		return ""
	}
	return keys[0]
}

// applyVerdict 将状态写入此控制器拥有的每个检查项，但仅在状态或消息与 Workload 已携带的内容不同时才写入，这样控制器就不会在已稳定的 Workload 上重复应用（并重新触发自身）。Retry 带有固定的退避时间。
//
// 比较针对整个判定结果——状态、消息和重新入队延迟——因为任何被排除在外的字段都会被该守卫固定为最先到达的值。
//
// 消息说明了原因：判定结果会指明其原因和角色，因此同一个 Workload 的两次 Retry 判定可能除了状态之外完全不同——例如先因缺少 flavor 分配而挂起，随后因卡数不足而挂起。若仅比较状态，只要状态不变就会将第一个原因一直固定在 Workload 上，导致操作人员误以为仍在等待实际上已经到达的分配。延迟同样需要纳入比较：丢失延迟的 Retry 会导致 Kueue 立即重试，从而引发退避机制本应避免的忙等循环。
//
// 这三者都是观测结果的确定性函数，因此仅在发生变化时写入仍然能够收敛稳定。
func (r *NodeDevicesAdmissionReconciler) applyVerdict(
	ctx context.Context,
	wl *kueue.Workload,
	checks []kueue.AdmissionCheckReference,
	state kueue.CheckState,
	message string,
) error {
	desired, changed := desiredCheckStates(wl, checks, state, message)
	if !changed {
		return nil
	}
	err := kueueworkload.PatchStatus(ctx, r.Client, wl, ctrlcli.FieldOwner(_NodeDevicesFieldOwner),
		func(w *kueue.Workload) (bool, error) {
			for i := range desired {
				kueueworkload.SetAdmissionCheckState(&w.Status.AdmissionChecks, desired[i], clock.RealClock{})
			}
			return true, nil
		})
	if err != nil {
		return err
	}
	r.recordVerdict(wl, desired, state)
	return nil
}

// desiredCheckStates 生成此控制器拥有的每个检查项的判定结果，并报告其中是否有任何一项与 Workload 已携带的内容不同。
//
// 返回的是所有拥有的检查项，而非仅返回有差异的项，因为写入操作采用 server-side apply，且 admissionChecks 是以 name 为键的列表：该字段所有者之前应用过但随后省略的条目，会被视为其已放弃声明，API server 会剪除该条目中它曾拥有的字段。因此，省略已稳定的检查项会导致每当同级项发生变化时就重置它们。要么应用完整的期望集合，要么什么都不做。
//
// 将其与 apply 逻辑分离是为了使这一点可被验证：测试中使用的 fake client 未实现 apply 时的剪除语义，因此驱动 Reconcile 的测试无法区分完整载荷与部分载荷——必须直接对载荷本身进行断言。
func desiredCheckStates(
	wl *kueue.Workload,
	checks []kueue.AdmissionCheckReference,
	state kueue.CheckState,
	message string,
) ([]kueue.AdmissionCheckState, bool) {
	desired := make([]kueue.AdmissionCheckState, 0, len(checks))
	changed := false
	for _, name := range checks {
		acs := kueue.AdmissionCheckState{
			Name:    name,
			State:   state,
			Message: message,
		}
		if state == kueue.CheckStateRetry {
			acs.RequeueAfterSeconds = ptr.To(_NodeDevicesRetryAfterSeconds)
		}
		desired = append(desired, acs)

		// 比较时读取的是获取到的原始 Workload。PatchStatus 传给更新函数的对象只是一个不包含任何 status 的裸 server-side-apply 骨架，因此在其中查找检查项永远找不到，这种比较会让所有更新都直接通过。
		cur := kueueadmissioncheck.FindAdmissionCheck(wl.Status.AdmissionChecks, name)
		if cur == nil || cur.State != acs.State || cur.Message != acs.Message ||
			!ptr.Equal(cur.RequeueAfterSeconds, acs.RequeueAfterSeconds) {
			changed = true
		}
	}
	return desired, changed
}

// demandVerdictMessage 为按数量申请的设备生成准入说明。Kueue 指定节点时说明该节点的结果，
// 否则说明已分配 flavor 对应的资源池。
func demandVerdictMessage(state kueue.CheckState, d familyDemand) string {
	if d.node != "" {
		return assignedNodeMessage(state, d)
	}
	return verdictMessage(state, d.podSets)
}

// assignedNodeMessage 解释受指定节点约束的准入结果。Kueue 的拓扑调度只看节点总量，
// 看不到额度分散在哪些卡上。驱逐后若总量未变，它可能反复选择同一节点，即使另一个节点有空卡，
// 也得等指定节点的设备释放。
func assignedNodeMessage(state kueue.CheckState, d familyDemand) string {
	if state == kueue.CheckStateReady {
		return fmt.Sprintf("the node %q Kueue assigned has enough free cards to place the request%s", d.node, forRoles(d.podSets))
	}
	return fmt.Sprintf("no card on the node %q Kueue assigned can host the request%s;"+
		" Kueue's topology-aware scheduling sees only the node's summed capacity and may assign the same node again,"+
		" so this retries until that node's cards free, even while another node has room", d.node, forRoles(d.podSets))
}

func verdictMessage(state kueue.CheckState, podSets []kueue.PodSetReference) string {
	if state == kueue.CheckStateReady {
		return "the assigned flavor pool has enough free cards to place the request" + forRoles(podSets)
	}
	return "no node in the assigned flavor pool currently has enough free cards" +
		forRoles(podSets) + "; will retry as capacity frees"
}

// forRoles 将判定所涉及的 podset 渲染为末尾的 " for role \"x\"" 子句，若为空则不渲染。多角色 Workload 按角色进行判定——不同角色可能被分配到不同的 flavor 从而对应不同的加速卡——如果判定结果只提及 Workload 名称，操作人员就只能猜测 prefill/decode 部署中究竟是哪一半资源不足。未指定 podset（尚未分配任何资源）的 Workload 的表述则与引入角色概念前完全一致。
func forRoles(podSets []kueue.PodSetReference) string {
	if len(podSets) == 0 {
		return ""
	}
	names := make([]string, len(podSets))
	for i, ps := range podSets {
		names[i] = fmt.Sprintf("%q", ps)
	}
	slices.Sort(names)
	if len(names) == 1 {
		return " for role " + names[0]
	}
	return " for roles " + strings.Join(names, ", ")
}

func (r *NodeDevicesAdmissionReconciler) SetupController(_ context.Context, opts controller.SetupOptions) error {
	r.Client = opts.Manager.GetClient()
	r.APIReader = opts.Manager.GetAPIReader()

	return ctrl.NewControllerManagedBy(opts.Manager).
		Named("nodedevicesadmission").
		For(
			// 启动时的全量同步会重新评估每一个已预留配额的 Workload；一旦处于 Retry 状态的 Workload 被 Kueue 重新准入，它就会再次预留配额并接受重新检查。
			&kueue.Workload{},
			ctrlbuilder.WithPredicates(ctrlpredicate.NewPredicateFuncs(func(obj ctrlcli.Object) bool {
				wl := obj.(*kueue.Workload)
				return kueueworkload.HasQuotaReservation(wl) && len(wl.Status.AdmissionChecks) > 0
			})),
		).
		// 每次只处理一个判定。每次判定都会将此前已回复 Ready 的 Workload 计为在途（inflight）；若两个判定并发执行，双方都会漏算对方，从而为同一张卡同时回复 Ready。readyUnobserved 同样依赖这一串行保证。
		WithOptions(ctrlcontroller.Options{MaxConcurrentReconciles: 1}).
		Complete(r)
}

// NodeDevicesAdmissionCheckReconciler 将由 operator 应用的 AdmissionCheck 对象（即带有 _NodeDevicesControllerName 的对象）保持为 Active 状态。Kueue 会将引用了非 Active AdmissionCheck 的 ClusterQueue 置为非活跃状态，进而使其下的 Workload 变成 Inadmissible，因此该控制器必须报告 Active 以确保准入关卡正常运作。
type NodeDevicesAdmissionCheckReconciler struct {
	Client ctrlcli.Client
}

var _ ctrlreconcile.Reconciler = (*NodeDevicesAdmissionCheckReconciler)(nil)

func (r *NodeDevicesAdmissionCheckReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := ctrllog.FromContext(ctx)

	ac := new(kueue.AdmissionCheck)
	if err := r.Client.Get(ctx, req.NamespacedName, ac); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		logger.Error(err, "fetch admission check")
		return ctrl.Result{}, err
	}

	if ac.Spec.ControllerName != _NodeDevicesControllerName {
		logger.V(3).Info("skip admission check not owned by this controller")
		return ctrl.Result{}, nil
	}

	if kubemeta.IsConditionTrue(ac.Status.Conditions, kueue.AdmissionCheckActive) {
		return ctrl.Result{}, nil
	}

	kubemeta.SetCondition(&ac.Status.Conditions, meta.Condition{
		Type:    kueue.AdmissionCheckActive,
		Status:  meta.ConditionTrue,
		Reason:  "Ready",
		Message: "the node-devices admission check controller is running",
	})
	if err := r.Client.Status().Update(ctx, ac); err != nil {
		return objectWriteResult(logger, err, "mark admission check active", ctrl.Result{})
	}

	logger.V(2).Info("activated node-devices admission check", "name", ac.Name)
	return ctrl.Result{}, nil
}

func (r *NodeDevicesAdmissionCheckReconciler) SetupController(_ context.Context, opts controller.SetupOptions) error {
	r.Client = opts.Manager.GetClient()

	return ctrl.NewControllerManagedBy(opts.Manager).
		Named("nodedevicesadmissioncheck").
		For(
			&kueue.AdmissionCheck{},
			ctrlbuilder.WithPredicates(ctrlpredicate.NewPredicateFuncs(func(obj ctrlcli.Object) bool {
				ac := obj.(*kueue.AdmissionCheck)
				return ac.Spec.ControllerName == _NodeDevicesControllerName
			})),
		).
		Complete(r)
}
