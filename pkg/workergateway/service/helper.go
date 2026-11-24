package service

import (
	"cmp"
	"fmt"
	"slices"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"

	worker "gpustack.ai/gpustack/api/worker/v1"
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/utils/mapx"
	"gpustack.ai/gpustack/pkg/utils/slicex"
	"gpustack.ai/gpustack/pkg/workergateway/manager"
)

// instanceTypePhaseActive 表示候选规格当前可以调度。
// 汇总 OnceMaxRequest 和 Remaining 时只计入 Active 候选，其他候选仍保留在列表中。
const instanceTypePhaseActive = "Active"

// normalizeAggregatedInstanceTypeSpec 清除 Inactive、DisplayName 和 Description 后再比较规格，
// 使各集群对同一硬件的启停设置和显示文字不会拆散同一组。
func normalizeAggregatedInstanceTypeSpec(spec AggregatedInstanceTypeSpec) AggregatedInstanceTypeSpec {
	spec.Inactive = false
	spec.DisplayName = ""
	spec.Description = ""
	return spec
}

// buildAggregatedInstanceTypeName 从规格生成稳定的聚合名称，不依赖集群对象的名称或到达顺序。
// 加速器规格在名称中带上加速器组和单位 CPU，内存和本地存储的数值使用 Gi。
func buildAggregatedInstanceTypeName(spec AggregatedInstanceTypeSpec) string {
	ram := strings.TrimSuffix(spec.UnitResources.RAM, "Gi")
	localStorage := strings.TrimSuffix(spec.LocalStorage, "Gi")
	if spec.Acceleratable {
		return fmt.Sprintf("%s-%s-%s-%s-%sc-%sg-%sg",
			spec.GeneralGroup, spec.AcceleratorGroup, spec.OS, spec.Arch,
			spec.UnitResources.CPU, ram, localStorage)
	}
	return fmt.Sprintf("%s-%s-%s-%sg-%sg", spec.GeneralGroup, spec.OS, spec.Arch, ram, localStorage)
}

type ListClusterInstanceTypeFlavors struct {
	list ClusterInstanceTypeFlavorList
}

func OpListClusterInstanceTypeFlavors() *ListClusterInstanceTypeFlavors {
	return &ListClusterInstanceTypeFlavors{
		list: ClusterInstanceTypeFlavorList{
			Items: make([]ClusterInstanceTypeFlavor, 0),
		},
	}
}

func (in *ListClusterInstanceTypeFlavors) Next(cluster string, obj runtime.Object) error {
	flavor, ok := obj.(*worker.InstanceTypeFlavor)
	if !ok {
		return fmt.Errorf("object is not of type InstanceTypeFlavor")
	}

	item := ClusterInstanceTypeFlavor{
		InstanceTypeFlavor: *flavor,
		Cluster:            cluster,
	}
	item.ManagedFields = nil
	in.list.Items = append(in.list.Items, item)
	return nil
}

func (in *ListClusterInstanceTypeFlavors) Result() ClusterInstanceTypeFlavorList {
	return in.list
}

type ListAggregateInstanceTypeFlavors struct {
	list        AggregatedInstanceTypeFlavorList
	itemIndexer map[worker.InstanceTypeFlavorSpec]int
}

func OpListAggregateInstanceTypeFlavors() *ListAggregateInstanceTypeFlavors {
	return &ListAggregateInstanceTypeFlavors{
		list: AggregatedInstanceTypeFlavorList{
			Items: make([]AggregatedInstanceTypeFlavor, 0),
		},
		itemIndexer: make(map[worker.InstanceTypeFlavorSpec]int),
	}
}

// Next 按完整的 Spec 合并各集群提供的同类资源池，记录每个贡献集群。同组名称沿用集群端对象的名称。
func (in *ListAggregateInstanceTypeFlavors) Next(cluster string, obj runtime.Object) error {
	flavor, ok := obj.(*worker.InstanceTypeFlavor)
	if !ok {
		return fmt.Errorf("object is not of type InstanceTypeFlavor")
	}

	itemIndex, existed := in.itemIndexer[flavor.Spec]
	if !existed {
		itemIndex = len(in.list.Items)
		in.itemIndexer[flavor.Spec] = itemIndex
		in.list.Items = append(in.list.Items, AggregatedInstanceTypeFlavor{
			Name: flavor.Name,
			Spec: AggregatedInstanceTypeFlavorSpec{
				InstanceTypeFlavorSpec: flavor.Spec,
				Clusters:               make([]string, 0),
			},
		})
	}

	item := &in.list.Items[itemIndex]
	item.Spec.Clusters = append(item.Spec.Clusters, cluster)
	return nil
}

// Result 返回聚合后的资源池列表。需要排序时先列出带加速器的规格，同类中按名称排序。
func (in *ListAggregateInstanceTypeFlavors) Result(sorted bool) AggregatedInstanceTypeFlavorList {
	if sorted {
		slices.SortFunc(in.list.Items, func(a, b AggregatedInstanceTypeFlavor) int {
			if c := slicex.CompareTrueFirst(a.Spec.Acceleratable, b.Spec.Acceleratable); c != 0 {
				return c
			}
			return cmp.Compare(a.Name, b.Name)
		})
	}

	for i := range in.list.Items {
		sort.Strings(in.list.Items[i].Spec.Clusters)
	}
	return in.list
}

type HandleClusterInstanceTypeFlavor struct{}

func OpHandleClusterInstanceTypeFlavor() *HandleClusterInstanceTypeFlavor {
	return &HandleClusterInstanceTypeFlavor{}
}

func (in *HandleClusterInstanceTypeFlavor) Handle(evt *manager.WorkerEvent) []*manager.WorkerEvent {
	if evt.Object != nil {
		flavor, ok := evt.Object.(*worker.InstanceTypeFlavor)
		if !ok {
			return nil
		}
		if flavor.DeletionTimestamp != nil {
			evt.Type = manager.WorkerEventDeleted
		}
		clusterFlavor := &ClusterInstanceTypeFlavor{
			InstanceTypeFlavor: *flavor,
			Cluster:            evt.Cluster,
		}
		clusterFlavor.ManagedFields = nil
		evt.Object = clusterFlavor
	}
	return []*manager.WorkerEvent{evt}
}

// HandleAggregatedInstanceTypeFlavor 维护跨集群的规格目录。首个集群加入一组时产生 Added，
// 成员增加或减少时产生 Modified，最后一个集群移出时产生 Deleted。Worker 侧按完整 Spec 区分条目，
// 规格改变会先删除旧规格再添加新规格，因此可直接从事件确定组。
type HandleAggregatedInstanceTypeFlavor struct {
	state AggregatedInstanceTypeFlavorList
}

func OpHandleAggregatedInstanceTypeFlavor(state AggregatedInstanceTypeFlavorList) *HandleAggregatedInstanceTypeFlavor {
	return &HandleAggregatedInstanceTypeFlavor{
		state: state,
	}
}

// groupIndex 按完整规格查找聚合项，找不到时返回 -1。目录按硬件池分组，线性查找也避免了删除元素后还需维护索引。
func (in *HandleAggregatedInstanceTypeFlavor) groupIndex(spec worker.InstanceTypeFlavorSpec) int {
	for i := range in.state.Items {
		if in.state.Items[i].Spec.InstanceTypeFlavorSpec == spec {
			return i
		}
	}
	return -1
}

func (in *HandleAggregatedInstanceTypeFlavor) Handle(evt *manager.WorkerEvent) []*manager.WorkerEvent {
	var evts []*manager.WorkerEvent

	if evt.Type == manager.WorkerEventDeleted && evt.Object == nil {
		// 从该集群参与的每个组中移除它。原地删除空组时回退下标，保证遍历不漏项，也保留之前待发送的 Modified 事件引用。
		for i := 0; i < len(in.state.Items); i++ {
			item := &in.state.Items[i]
			kept := dropCluster(item.Spec.Clusters, evt.Cluster)
			if len(kept) == len(item.Spec.Clusters) {
				continue
			}
			if len(kept) == 0 {
				deleted := deletedFlavor(item)
				in.state.Items = append(in.state.Items[:i], in.state.Items[i+1:]...)
				i--
				evts = append(evts, &manager.WorkerEvent{
					Type:   manager.WorkerEventDeleted,
					Object: deleted,
				})
				continue
			}
			item.Spec.Clusters = kept
			evts = append(evts, &manager.WorkerEvent{
				Type:   manager.WorkerEventModified,
				Object: item,
			})
		}
		return evts
	}

	flavor, ok := evt.Object.(*worker.InstanceTypeFlavor)
	if !ok {
		return nil
	}
	if flavor.DeletionTimestamp != nil {
		evt.Type = manager.WorkerEventDeleted
	}

	if evt.Type == manager.WorkerEventDeleted {
		// 删除事件附带原对象，可以用它原先的 Spec 找到对应组。
		return in.removeCluster(evt.Cluster, flavor.Spec)
	}

	// 对新增和更新事件，确保该集群已列入相应规格组。
	return in.addCluster(evt.Cluster, flavor.Name, flavor.Spec)
}

// addCluster 添加规格组的来源集群。组的首个成员产生 Added，加入已有组产生 Modified，重复成员无需再次报告。
func (in *HandleAggregatedInstanceTypeFlavor) addCluster(cluster, name string, spec worker.InstanceTypeFlavorSpec) []*manager.WorkerEvent {
	idx := in.groupIndex(spec)
	if idx == -1 {
		in.state.Items = append(in.state.Items, AggregatedInstanceTypeFlavor{
			Name: name,
			Spec: AggregatedInstanceTypeFlavorSpec{
				InstanceTypeFlavorSpec: spec,
				Clusters:               []string{cluster},
			},
		})
		return []*manager.WorkerEvent{{
			Type:   manager.WorkerEventAdded,
			Object: &in.state.Items[len(in.state.Items)-1],
		}}
	}

	item := &in.state.Items[idx]
	for _, c := range item.Spec.Clusters {
		if c == cluster {
			return nil
		}
	}
	item.Spec.Clusters = append(item.Spec.Clusters, cluster)
	sort.Strings(item.Spec.Clusters)
	return []*manager.WorkerEvent{{
		Type:   manager.WorkerEventModified,
		Object: item,
	}}
}

// removeCluster 从规格组移除一个集群。若仍有其他成员则产生 Modified，最后一个成员离开时产生 Deleted。
func (in *HandleAggregatedInstanceTypeFlavor) removeCluster(cluster string, spec worker.InstanceTypeFlavorSpec) []*manager.WorkerEvent {
	idx := in.groupIndex(spec)
	if idx == -1 {
		return nil
	}

	item := &in.state.Items[idx]
	kept := dropCluster(item.Spec.Clusters, cluster)
	if len(kept) == len(item.Spec.Clusters) {
		return nil
	}
	if len(kept) == 0 {
		deleted := deletedFlavor(item)
		in.state.Items = append(in.state.Items[:idx], in.state.Items[idx+1:]...)
		return []*manager.WorkerEvent{{
			Type:   manager.WorkerEventDeleted,
			Object: deleted,
		}}
	}
	item.Spec.Clusters = kept
	return []*manager.WorkerEvent{{
		Type:   manager.WorkerEventModified,
		Object: item,
	}}
}

// deletedFlavor 复制被删除组的名称与完整规格，再把集群清单置为空切片。名称相同的组仍可凭规格识别，JSON 中的空清单也保持为数组。
func deletedFlavor(item *AggregatedInstanceTypeFlavor) *AggregatedInstanceTypeFlavor {
	return &AggregatedInstanceTypeFlavor{
		Name: item.Name,
		Spec: AggregatedInstanceTypeFlavorSpec{
			InstanceTypeFlavorSpec: item.Spec.InstanceTypeFlavorSpec,
			Clusters:               []string{},
		},
	}
}

// dropCluster 生成不含指定集群的新切片，保留原有顺序，同时避免与原切片共享底层数组。
func dropCluster(clusters []string, cluster string) []string {
	kept := make([]string, 0, len(clusters))
	for _, c := range clusters {
		if c != cluster {
			kept = append(kept, c)
		}
	}
	return kept
}

// compareTierByPrimary 按主资源维度从小到大排列各档。加速器规格比较整卡，纯 CPU 规格比较 CPU。
func (in *AggregatedInstanceType) compareTierByPrimary(a, b AggregatedInstanceTypeOnceMaxRequestTier) int {
	if in.Spec.Acceleratable {
		return a.OnceMaxRequest.Accelerator.Cmp(b.OnceMaxRequest.Accelerator)
	}
	return a.OnceMaxRequest.CPU.Cmp(b.OnceMaxRequest.CPU)
}

// Recompute 从各档位重算整个规格的 OnceMaxRequest 与 Remaining。先按主维度选择一个档位，
// 其完整资源组合作为单次最大请求。各档同一维度的余量则逐项累加。即使主维度为零，也能保留真实候选提供的共享或切片资源。
func (in *AggregatedInstanceType) Recompute() {
	var newOnceMaxRequest, newRemaining AggregatedInstanceTypeOverviewResource

	seeded := false
	for i := range in.Status.Tiers {
		tier := &in.Status.Tiers[i]

		// 所有维度均为零的档位不参与单次最大请求的选择。否则它可能挡住另一个整卡额度同为零、却仍有可用切片的档位。
		if !overviewResourceIsZero(tier.OnceMaxRequest) {
			wins := !seeded
			if !wins {
				if in.Spec.Acceleratable {
					wins = newOnceMaxRequest.Accelerator.Cmp(tier.OnceMaxRequest.Accelerator) < 0
				} else {
					wins = newOnceMaxRequest.CPU.Cmp(tier.OnceMaxRequest.CPU) < 0
				}
			}
			if wins {
				newOnceMaxRequest = tier.OnceMaxRequest
			}
			seeded = true
		}

		newRemaining.Accelerator.Add(tier.Remaining.Accelerator)
		newRemaining.CPU.Add(tier.Remaining.CPU)
		newRemaining.AcceleratorShared.Add(tier.Remaining.AcceleratorShared)
		newRemaining.AcceleratorSliced.Add(tier.Remaining.AcceleratorSliced)
		newRemaining.AcceleratorPartitioned = sumProfileCounts(
			newRemaining.AcceleratorPartitioned, tier.Remaining.AcceleratorPartitioned,
		)
	}

	in.Status.OnceMaxRequest = newOnceMaxRequest
	in.Status.Remaining = newRemaining

	// 从各档重新汇总整组切片能力，物理分区按 profile 名累计到 Detail.SlicedDetail。
	// 硬件身份由 adoptDetailIdentity 维护，基础计数使用独立切片，避免改到档位中的原值。
	var newSliced workercore.AcceleratorSlicedDetail
	profileIndex := make(map[string]int)
	for i := range in.Status.Tiers {
		addAcceleratorSlicedDetail(&newSliced, in.Status.Tiers[i].AcceleratorSlicedDetail, profileIndex)
	}
	in.Status.Detail.SlicedDetail = newSliced
}

// overviewResourceIsZero 判断该组合在所有维度是否都无法提供一次分配。
func overviewResourceIsZero(r AggregatedInstanceTypeOverviewResource) bool {
	return r.Accelerator.IsZero() && r.AcceleratorShared.IsZero() &&
		r.AcceleratorSliced.IsZero() && profileCountsAreZero(r.AcceleratorPartitioned) && r.CPU.IsZero()
}

// profileCountsAreZero 检查各分区 profile 是否都没有可分配实例。已支持但无空位的 profile 会保留零计数。
func profileCountsAreZero(counts []workercore.AcceleratorProfileCount) bool {
	for _, p := range counts {
		if p.Count > 0 {
			return false
		}
	}
	return true
}

// Recompute 从候选成员重算一个档位。单次最大请求取自主维度最大的同一个有效候选，所有有效候选的余量则逐维求和。对于主维度为零的情况，
// 仍从真实有效的候选初始化资源组合。
func (in *AggregatedInstanceTypeOnceMaxRequestTier) Recompute(acceleratable bool) {
	var newOnceMaxRequest, newRemaining AggregatedInstanceTypeOverviewResource

	seeded := false
	for i := range in.Candidates {
		candidate := &in.Candidates[i]

		// 只汇总 Active 候选的可调度额度。非 Active 候选仍可见，暂时贡献零额度。
		if candidate.Phase != instanceTypePhaseActive {
			continue
		}

		// 用首个 Active 候选初始化最大资源组合，使整卡额度为零的切片池也能贡献真实可用的资源。
		wins := !seeded
		if !wins {
			if acceleratable {
				wins = newOnceMaxRequest.Accelerator.Cmp(candidate.Accelerator.OnceMaxRequest) < 0
			} else {
				wins = newOnceMaxRequest.CPU.Cmp(candidate.CPU.OnceMaxRequest) < 0
			}
		}
		if wins {
			newOnceMaxRequest = AggregatedInstanceTypeOverviewResource{
				Accelerator:            candidate.Accelerator.OnceMaxRequest,
				CPU:                    candidate.CPU.OnceMaxRequest,
				AcceleratorShared:      candidate.AcceleratorShared.OnceMaxRequest,
				AcceleratorSliced:      candidate.AcceleratorSliced.OnceMaxRequest,
				AcceleratorPartitioned: capProfileCountsAtOne(candidate.AcceleratorPartitioned.RemainingProfiles),
			}
		}
		seeded = true

		newRemaining.Accelerator.Add(candidate.Accelerator.Remaining)
		newRemaining.CPU.Add(candidate.CPU.Remaining)
		newRemaining.AcceleratorShared.Add(candidate.AcceleratorShared.Remaining)
		newRemaining.AcceleratorSliced.Add(candidate.AcceleratorSliced.Remaining)
		newRemaining.AcceleratorPartitioned = sumProfileCounts(
			newRemaining.AcceleratorPartitioned, candidate.AcceleratorPartitioned.RemainingProfiles,
		)
	}

	in.OnceMaxRequest = newOnceMaxRequest
	in.Remaining = newRemaining

	// 按 profile 名累加每个候选声明的切片能力。能力清单描述硬件本身，所以也要计入暂时不可调度的候选。
	var newSliced workercore.AcceleratorSlicedDetail
	profileIndex := make(map[string]int)
	for i := range in.Candidates {
		addAcceleratorSlicedDetail(&newSliced, in.Candidates[i].AcceleratorSlicedDetail, profileIndex)
	}
	in.AcceleratorSlicedDetail = newSliced
}

// newAggregatedTier 根据 InstanceType 的当前状态初始化空档位。它暂时保存原始的单次最大值和余量，
// 添加候选后再按阶段重算。
func newAggregatedTier(instType *worker.InstanceType) AggregatedInstanceTypeOnceMaxRequestTier {
	return AggregatedInstanceTypeOnceMaxRequestTier{
		OnceMaxRequest: AggregatedInstanceTypeOverviewResource{
			Accelerator:            instType.Status.Accelerator.OnceMaxRequest,
			CPU:                    instType.Status.CPU.OnceMaxRequest,
			AcceleratorShared:      instType.Status.AcceleratorShared.OnceMaxRequest,
			AcceleratorSliced:      instType.Status.AcceleratorSliced.OnceMaxRequest,
			AcceleratorPartitioned: capProfileCountsAtOne(instType.Status.AcceleratorPartitioned.RemainingProfiles),
		},
		Remaining: AggregatedInstanceTypeOverviewResource{
			Accelerator:            instType.Status.Accelerator.Remaining,
			CPU:                    instType.Status.CPU.Remaining,
			AcceleratorShared:      instType.Status.AcceleratorShared.Remaining,
			AcceleratorSliced:      instType.Status.AcceleratorSliced.Remaining,
			AcceleratorPartitioned: sumProfileCounts(nil, instType.Status.AcceleratorPartitioned.RemainingProfiles),
		},
	}
}

// adoptDetailIdentity 从已经完成协调的候选取得硬件身份。早期对象可能还没有 Detail，后续观测应能补全，
// 同时保留独立计算的切片汇总。
func adoptDetailIdentity(status *AggregatedInstanceTypeStatus, instType *worker.InstanceType) {
	if !instType.Status.Detail.AcceleratorReady() {
		return
	}
	sliced := status.Detail.SlicedDetail
	status.Detail = instType.Status.Detail
	status.Detail.SlicedDetail = sliced
}

// newAggregatedCandidate 从一个集群的 InstanceType 构造候选，包括它观测到的切片能力，
// 供档位和整个规格逐层求和。
func newAggregatedCandidate(cluster, name string, instType *worker.InstanceType) AggregatedInstanceTypeOnceMaxRequestCandidate {
	return AggregatedInstanceTypeOnceMaxRequestCandidate{
		Cluster:                 cluster,
		Name:                    name,
		Phase:                   instType.Status.Phase,
		Accelerator:             instType.Status.Accelerator,
		CPU:                     instType.Status.CPU,
		AcceleratorShared:       instType.Status.AcceleratorShared,
		AcceleratorSliced:       instType.Status.AcceleratorSliced,
		AcceleratorPartitioned:  instType.Status.AcceleratorPartitioned,
		AcceleratorSlicedDetail: instType.Status.Detail.SlicedDetail,
	}
}

// addAcceleratorSlicedDetail 累加逻辑切片和物理分区计数，使用按名称索引的 profile 合并各项数量，
// 保留各档上报的超配能力。
func addAcceleratorSlicedDetail(dst *workercore.AcceleratorSlicedDetail, src workercore.AcceleratorSlicedDetail, profileIndex map[string]int) {
	dst.Logical.Count += src.Logical.Count
	if src.Logical.Count > 0 && src.Logical.CoresPercentageOvercommit {
		dst.Logical.CoresPercentageOvercommit = true
	}

	dst.Physical.Count += src.Physical.Count
	for _, p := range src.Physical.Profiles {
		if idx, ok := profileIndex[p.Name]; ok {
			dst.Physical.Profiles[idx].Count += p.Count
			continue
		}
		profileIndex[p.Name] = len(dst.Physical.Profiles)
		dst.Physical.Profiles = append(dst.Physical.Profiles, workercore.AcceleratorSlicedPhysicalDetailProfile{
			Name:  p.Name,
			Count: p.Count,
		})
	}
}

// sumProfileCounts 按 profile 名累加可获得实例数，按名称输出稳定结果，使不同集群以不同顺序到达时仍产生相同的清单。
func sumProfileCounts(dst, src []workercore.AcceleratorProfileCount) []workercore.AcceleratorProfileCount {
	if len(src) == 0 {
		return dst
	}
	sums := make(map[string]int32, len(dst)+len(src))
	for _, p := range dst {
		sums[p.Name] += p.Count
	}
	for _, p := range src {
		sums[p.Name] += p.Count
	}
	out := mapx.Slice(sums, func(name string, count int32) workercore.AcceleratorProfileCount {
		return workercore.AcceleratorProfileCount{Name: name, Count: count}
	})
	slices.SortFunc(out, func(a, b workercore.AcceleratorProfileCount) int { return cmp.Compare(a.Name, b.Name) })
	return out
}

// capProfileCountsAtOne 复制分区账本，并将每个 profile 的数量最多记为一。单次硬切分请求只领取一张卡上的一个实例。
func capProfileCountsAtOne(counts []workercore.AcceleratorProfileCount) []workercore.AcceleratorProfileCount {
	if len(counts) == 0 {
		return nil
	}
	out := make([]workercore.AcceleratorProfileCount, len(counts))
	for i, p := range counts {
		out[i] = workercore.AcceleratorProfileCount{Name: p.Name, Count: min(p.Count, 1)}
	}
	return out
}

type ListAggregateInstanceTypes struct {
	list            AggregatedInstanceTypeList
	itemIndexer     map[AggregatedInstanceTypeSpec]int
	itemTierIndexer []map[string]int
}

func OpListAggregateInstanceTypes() *ListAggregateInstanceTypes {
	return &ListAggregateInstanceTypes{
		list: AggregatedInstanceTypeList{
			Items: make([]AggregatedInstanceType, 0),
		},
		itemIndexer: make(map[AggregatedInstanceTypeSpec]int),
	}
}

func (in *ListAggregateInstanceTypes) Next(cluster string, obj runtime.Object) error {
	instType, ok := obj.(*worker.InstanceType)
	if !ok {
		return fmt.Errorf("object is not of type InstanceType")
	}

	// 用规范化后的 Spec 分组，使同一硬件在不同集群使用不同显示文字或启停设置时仍属同组。聚合项保留首次看到的规格内容。
	itemKey := normalizeAggregatedInstanceTypeSpec(instType.Spec)
	itemIndex, existed := in.itemIndexer[itemKey]
	if !existed {
		itemIndex = len(in.list.Items)
		in.itemIndexer[itemKey] = itemIndex
		in.itemTierIndexer = append(in.itemTierIndexer, make(map[string]int))
		item := AggregatedInstanceType{
			Name: buildAggregatedInstanceTypeName(instType.Spec),
			Spec: instType.Spec,
		}
		in.list.Items = append(in.list.Items, item)
	}

	item := &in.list.Items[itemIndex]
	tierIndexer := in.itemTierIndexer[itemIndex]

	// 从已协调的候选更新聚合项的硬件描述。
	adoptDetailIdentity(&item.Status, instType)

	tierIndexKey := instType.Status.Accelerator.OnceMaxRequest.String()
	tierIndex, existed := tierIndexer[tierIndexKey]
	if !existed {
		tierIndex = len(item.Status.Tiers)
		tierIndexer[tierIndexKey] = tierIndex
		item.Status.Tiers = append(item.Status.Tiers, newAggregatedTier(instType))
	}

	tier := &item.Status.Tiers[tierIndex]
	tier.Candidates = append(tier.Candidates, newAggregatedCandidate(cluster, instType.Name, instType))

	return nil
}

func (in *ListAggregateInstanceTypes) Result(sorted bool) AggregatedInstanceTypeList {
	if sorted {
		// 先按是否使用加速器，再按名称排列聚合项。
		slices.SortFunc(in.list.Items, func(a, b AggregatedInstanceType) int {
			if c := slicex.CompareTrueFirst(a.Spec.Acceleratable, b.Spec.Acceleratable); c != 0 {
				return c
			}
			return cmp.Compare(a.Name, b.Name)
		})
	}

	for i := range in.list.Items {
		item := &in.list.Items[i]

		// 排序前先重算各档容量，使只有非 Active 成员的档位按零可用额度参与排序。
		for j := range item.Status.Tiers {
			tier := &item.Status.Tiers[j]
			tier.Recompute(item.Spec.Acceleratable)
		}

		// 按主资源维度升序排列各档。
		slices.SortFunc(item.Status.Tiers, item.compareTierByPrimary)

		// 根据档位重算聚合项的单次最大请求。
		item.Recompute()
	}
	return in.list
}

type HandleAggregatedInstanceType struct {
	state AggregatedInstanceTypeList
}

func OpHandleAggregatedInstanceType(state AggregatedInstanceTypeList) *HandleAggregatedInstanceType {
	return &HandleAggregatedInstanceType{
		state: state,
	}
}

func (in *HandleAggregatedInstanceType) Handle(evt *manager.WorkerEvent) []*manager.WorkerEvent {
	var evts []*manager.WorkerEvent

	if evt.Type == manager.WorkerEventDeleted && evt.Object == nil {
		// 删除该集群在所有聚合规格中的成员。
		for i := 0; i < len(in.state.Items); i++ {
			item := &in.state.Items[i]
			itemChanged := false
			for j := 0; j < len(item.Status.Tiers); j++ {
				tier := &item.Status.Tiers[j]
				tierChanged := false
				// 保留其他集群的候选，移除被取消集群的候选。
				newCandidates := make([]AggregatedInstanceTypeOnceMaxRequestCandidate, 0, len(tier.Candidates))
				for k := range tier.Candidates {
					candidate := tier.Candidates[k]
					if candidate.Cluster != evt.Cluster {
						newCandidates = append(newCandidates, tier.Candidates[k])
						continue
					}
					tierChanged = true
				}
				if !tierChanged {
					continue
				}
				itemChanged = true
				if len(newCandidates) == 0 {
					item.Status.Tiers = append(item.Status.Tiers[:j], item.Status.Tiers[j+1:]...)
					j--
					continue
				}
				tier.Candidates = newCandidates

				// 按剩余候选重算档位。
				tier.Recompute(item.Spec.Acceleratable)
			}
			if !itemChanged {
				continue
			}
			if len(item.Status.Tiers) == 0 {
				itemName := item.Name
				in.state.Items = append(in.state.Items[:i], in.state.Items[i+1:]...)
				i--

				// 通知订阅者该聚合项已删除。
				evts = append(evts, &manager.WorkerEvent{
					Type:   manager.WorkerEventDeleted,
					Object: &AggregatedInstanceType{Name: itemName},
				})
				continue
			}

			// 根据保留下来的档位更新聚合项。
			item.Recompute()

			// 通知订阅者聚合项已更新。
			evts = append(evts, &manager.WorkerEvent{
				Type:   manager.WorkerEventModified,
				Object: item,
			})
		}

		return evts
	}

	instType, ok := evt.Object.(*worker.InstanceType)
	if !ok {
		return nil
	}
	if instType.DeletionTimestamp != nil {
		evt.Type = manager.WorkerEventDeleted
	}

	index := [3]int{-1, -1, -1} // 依次记录匹配到的聚合项、档位和候选位置。

	// 用规范化后的 Spec 寻找原组。仅改变显示或管理字段的成员，仍应更新同一个硬件规格。
	instTypeKey := normalizeAggregatedInstanceTypeSpec(instType.Spec)
	for i := 0; i < len(in.state.Items); i++ {
		if normalizeAggregatedInstanceTypeSpec(in.state.Items[i].Spec) != instTypeKey {
			continue
		}
		index[0] = i

		item := &in.state.Items[index[0]]

		// 从新观测到的候选更新硬件描述。删除事件不会提供更新的硬件信息。
		if evt.Type != manager.WorkerEventDeleted {
			adoptDetailIdentity(&item.Status, instType)
		}

		for j := 0; j < len(item.Status.Tiers); j++ {
			tier := &item.Status.Tiers[j]
			// 用候选原始的整卡 OnceMaxRequest 确认档位。档位汇总值会因成员阶段而变为零，不能作为永久身份。
			if tier.Candidates[0].Accelerator.OnceMaxRequest.Equal(instType.Status.Accelerator.OnceMaxRequest) {
				index[1] = j
			}
			for k := range tier.Candidates {
				if tier.Candidates[k].Cluster == evt.Cluster && tier.Candidates[k].Name == instType.Name {
					index[1] = j
					index[2] = k
					break
				}
			}
			if index[2] != -1 {
				break
			}
		}

		// 找到了同一集群、同名的候选。
		if index[2] != -1 {
			tier := &item.Status.Tiers[index[1]]

			if evt.Type == manager.WorkerEventDeleted {
				// 从原档位移除该候选。
				tier.Candidates = append(tier.Candidates[:index[2]], tier.Candidates[index[2]+1:]...)

				// 原档位失去所有候选时，一并移除该档。
				if len(tier.Candidates) == 0 {
					item.Status.Tiers = append(item.Status.Tiers[:index[1]], item.Status.Tiers[index[1]+1:]...)
				} else {
					// 用原档位保留的候选更新容量。
					tier.Recompute(item.Spec.Acceleratable)
				}

				// 聚合项已没有档位，移除它。
				if len(item.Status.Tiers) == 0 {
					itemName := item.Name
					in.state.Items = append(in.state.Items[:index[0]], in.state.Items[index[0]+1:]...)

					// 同时发布整个聚合项的删除事件。
					evts = append(evts, &manager.WorkerEvent{
						Type:   manager.WorkerEventDeleted,
						Object: &AggregatedInstanceType{Name: itemName},
					})
				} else {
					// 重算仍然存在的聚合项。
					item.Recompute()

					// 发布聚合项的变化。
					evts = append(evts, &manager.WorkerEvent{
						Type:   manager.WorkerEventModified,
						Object: item,
					})
				}

				// 被删除的候选不会移入其他档位，此处即可结束。
				return evts
			}

			candidate := newAggregatedCandidate(evt.Cluster, instType.Name, instType)

			if tier.Candidates[0].Accelerator.OnceMaxRequest.Equal(candidate.Accelerator.OnceMaxRequest) {
				// 单次最大额度没有导致换档时，在原位置更新候选。
				tier.Candidates[index[2]] = candidate

				// 重算更新后的档位。
				tier.Recompute(item.Spec.Acceleratable)
			} else {
				// 候选需要换档，先从旧档移除。
				tier.Candidates = append(tier.Candidates[:index[2]], tier.Candidates[index[2]+1:]...)

				// 旧档已经为空时移除它。
				if len(tier.Candidates) == 0 {
					item.Status.Tiers = append(item.Status.Tiers[:index[1]], item.Status.Tiers[index[1]+1:]...)
				} else {
					// 重算仍有其他候选的旧档。
					tier.Recompute(item.Spec.Acceleratable)
				}

				// 根据候选的原始整卡 OnceMaxRequest 寻找目标档位，即使某档暂时因阶段过滤显示零容量，也能识别它。
				newTierIndex := -1
				for j := 0; j < len(item.Status.Tiers); j++ {
					if item.Status.Tiers[j].Candidates[0].Accelerator.OnceMaxRequest.Equal(instType.Status.Accelerator.OnceMaxRequest) {
						newTierIndex = j
						break
					}
				}
				if newTierIndex != -1 {
					// 将候选移入已有目标档位。
					tier = &item.Status.Tiers[newTierIndex]
					tier.Candidates = append(tier.Candidates, candidate)

					// 用新的成员集合重算目标档位。
					tier.Recompute(item.Spec.Acceleratable)
				} else {
					// 缺少对应额度的档位时新建一档。
					newTier := newAggregatedTier(instType)
					newTier.Candidates = []AggregatedInstanceTypeOnceMaxRequestCandidate{candidate}
					item.Status.Tiers = append(item.Status.Tiers, newTier)

					// 对新档立即执行阶段过滤，防止迁移中的非 Active 候选增加可调度容量，并得到正确的排序值。
					item.Status.Tiers[len(item.Status.Tiers)-1].Recompute(item.Spec.Acceleratable)

					// 按主资源维度重新排序。
					slices.SortFunc(item.Status.Tiers, item.compareTierByPrimary)
				}
			}

			// 更新聚合规格的容量汇总。
			item.Recompute()

			// 发送本次更新的事件。
			evts = append(evts, &manager.WorkerEvent{
				Type:   manager.WorkerEventModified,
				Object: item,
			})

			return evts
		}

		// 档位已存在，其中还没有该候选。
		if index[1] != -1 {
			if evt.Type == manager.WorkerEventDeleted {
				// 收到的删除事件对应的候选已经不存在，无需更改。
				return evts
			}

			tier := &item.Status.Tiers[index[1]]

			tier.Candidates = append(tier.Candidates, newAggregatedCandidate(evt.Cluster, instType.Name, instType))

			// 加入候选后更新该档位。
			tier.Recompute(item.Spec.Acceleratable)

			// 重新计算聚合规格。
			item.Recompute()

			// 发送更新后的聚合项。
			evts = append(evts, &manager.WorkerEvent{
				Type:   manager.WorkerEventModified,
				Object: item,
			})

			return evts
		}

		// 聚合规格存在，但找不到事件所指的档位。
		break
	}

	if evt.Type != manager.WorkerEventDeleted {
		tier := newAggregatedTier(instType)
		tier.Candidates = []AggregatedInstanceTypeOnceMaxRequestCandidate{
			newAggregatedCandidate(evt.Cluster, instType.Name, instType),
		}

		// 同一规格首次出现这个档位。
		if index[0] != -1 {
			item := &in.state.Items[index[0]]
			item.Status.Tiers = append(item.Status.Tiers, tier)

			// 先重算新档位，使阶段过滤在排序和整体汇总之前生效。
			item.Status.Tiers[len(item.Status.Tiers)-1].Recompute(item.Spec.Acceleratable)

			// 按主资源对档位重新排序。
			slices.SortFunc(item.Status.Tiers, item.compareTierByPrimary)

			// 重算聚合规格的概览。
			item.Recompute()

			// 发布聚合规格已更新的事件。
			evts = append(evts, &manager.WorkerEvent{
				Type:   manager.WorkerEventModified,
				Object: item,
			})

			return evts
		}

		// 先重算新档，防止非 Active 首成员给新规格带来虚高的容量，并为下一层准备已汇总的切片能力。
		tier.Recompute(instType.Spec.Acceleratable)

		// 用首个候选初始化聚合项的硬件身份。若该候选尚未协调，后续事件仍能补全。其余派生字段由重算得到。
		in.state.Items = append(in.state.Items, AggregatedInstanceType{
			Name: buildAggregatedInstanceTypeName(instType.Spec),
			Spec: instType.Spec,
			Status: AggregatedInstanceTypeStatus{
				Detail: instType.Status.Detail,
				Tiers: []AggregatedInstanceTypeOnceMaxRequestTier{
					tier,
				},
			},
		})

		item := &in.state.Items[len(in.state.Items)-1]

		// 通过统一的重算路径生成单档位概览，同时把切片汇总写入聚合项的 Detail。
		item.Recompute()

		// 发布新聚合规格的 Added 事件。
		evts = append(evts, &manager.WorkerEvent{
			Type:   manager.WorkerEventAdded,
			Object: item,
		})

		return evts
	}

	return evts
}

type ListClusterInstanceTypes struct {
	list ClusterInstanceTypeList
}

func OpListClusterInstanceTypes() *ListClusterInstanceTypes {
	return &ListClusterInstanceTypes{
		list: ClusterInstanceTypeList{
			Items: make([]ClusterInstanceType, 0),
		},
	}
}

func (in *ListClusterInstanceTypes) Next(cluster string, obj runtime.Object) error {
	instType, ok := obj.(*worker.InstanceType)
	if !ok {
		return fmt.Errorf("object is not of type InstanceType")
	}

	item := ClusterInstanceType{
		InstanceType: *instType,
		Cluster:      cluster,
	}
	item.ManagedFields = nil
	in.list.Items = append(in.list.Items, item)
	return nil
}

func (in *ListClusterInstanceTypes) Result() ClusterInstanceTypeList {
	return in.list
}

type HandleClusterInstanceType struct{}

func OpHandleClusterInstanceType() *HandleClusterInstanceType {
	return &HandleClusterInstanceType{}
}

func (in *HandleClusterInstanceType) Handle(evt *manager.WorkerEvent) []*manager.WorkerEvent {
	if evt.Object != nil {
		instType, ok := evt.Object.(*worker.InstanceType)
		if !ok {
			return nil
		}
		if instType.DeletionTimestamp != nil {
			evt.Type = manager.WorkerEventDeleted
		}
		clusterInstType := &ClusterInstanceType{
			InstanceType: *instType,
			Cluster:      evt.Cluster,
		}
		clusterInstType.ManagedFields = nil
		evt.Object = clusterInstType
	}
	return []*manager.WorkerEvent{evt}
}

type ListClusterInstances struct {
	list ClusterInstanceList
}

func OpListClusterInstances() *ListClusterInstances {
	return &ListClusterInstances{
		list: ClusterInstanceList{
			Items: make([]ClusterInstance, 0),
		},
	}
}

func (in *ListClusterInstances) Next(cluster string, obj runtime.Object) error {
	inst, ok := obj.(*worker.Instance)
	if !ok {
		return fmt.Errorf("object is not of type Instance")
	}

	item := ClusterInstance{
		Instance: *inst,
		Cluster:  cluster,
	}
	item.ManagedFields = nil
	in.list.Items = append(in.list.Items, item)
	return nil
}

func (in *ListClusterInstances) Result() ClusterInstanceList {
	return in.list
}

type HandleClusterInstance struct {
	namespace string
}

func OpHandleClusterInstance(namespace string) *HandleClusterInstance {
	return &HandleClusterInstance{namespace: namespace}
}

func (in *HandleClusterInstance) Handle(evt *manager.WorkerEvent) []*manager.WorkerEvent {
	if evt.Object != nil {
		inst, ok := evt.Object.(*worker.Instance)
		if !ok {
			return nil
		}
		if in.namespace != "" && inst.Namespace != in.namespace {
			return nil
		}
		if inst.DeletionTimestamp != nil {
			evt.Type = manager.WorkerEventDeleted
		}
		clusterInst := &ClusterInstance{
			Instance: *inst,
			Cluster:  evt.Cluster,
		}
		clusterInst.ManagedFields = nil
		evt.Object = clusterInst
	}
	return []*manager.WorkerEvent{evt}
}

type ListClusterInstancePersistentVolumeTypes struct {
	list ClusterInstancePersistentVolumeTypeList
}

func OpListClusterInstancePersistentVolumeTypes() *ListClusterInstancePersistentVolumeTypes {
	return &ListClusterInstancePersistentVolumeTypes{
		list: ClusterInstancePersistentVolumeTypeList{
			Items: make([]ClusterInstancePersistentVolumeType, 0),
		},
	}
}

func (in *ListClusterInstancePersistentVolumeTypes) Next(cluster string, obj runtime.Object) error {
	volType, ok := obj.(*worker.InstancePersistentVolumeType)
	if !ok {
		return fmt.Errorf("object is not of type InstancePersistentVolumeType")
	}

	item := ClusterInstancePersistentVolumeType{
		InstancePersistentVolumeType: *volType,
		Cluster:                      cluster,
	}
	item.ManagedFields = nil
	in.list.Items = append(in.list.Items, item)
	return nil
}

func (in *ListClusterInstancePersistentVolumeTypes) Result() ClusterInstancePersistentVolumeTypeList {
	return in.list
}

type HandleClusterInstancePersistentVolumeType struct{}

func OpHandleClusterInstancePersistentVolumeType() *HandleClusterInstancePersistentVolumeType {
	return &HandleClusterInstancePersistentVolumeType{}
}

func (in *HandleClusterInstancePersistentVolumeType) Handle(evt *manager.WorkerEvent) []*manager.WorkerEvent {
	if evt.Object != nil {
		volType, ok := evt.Object.(*worker.InstancePersistentVolumeType)
		if !ok {
			return nil
		}
		if volType.DeletionTimestamp != nil {
			evt.Type = manager.WorkerEventDeleted
		}
		clusterVolType := &ClusterInstancePersistentVolumeType{
			InstancePersistentVolumeType: *volType,
			Cluster:                      evt.Cluster,
		}
		clusterVolType.ManagedFields = nil
		evt.Object = clusterVolType
	}
	return []*manager.WorkerEvent{evt}
}

type ListClusterInstancePersistentVolumes struct {
	list ClusterInstancePersistentVolumeList
}

func OpListClusterInstancePersistentVolumes() *ListClusterInstancePersistentVolumes {
	return &ListClusterInstancePersistentVolumes{}
}

func (in *ListClusterInstancePersistentVolumes) Next(cluster string, obj runtime.Object) error {
	vol, ok := obj.(*worker.InstancePersistentVolume)
	if !ok {
		return fmt.Errorf("object is not of type InstancePersistentVolume")
	}

	item := ClusterInstancePersistentVolume{
		InstancePersistentVolume: *vol,
		Cluster:                  cluster,
	}
	item.ManagedFields = nil
	in.list.Items = append(in.list.Items, item)
	return nil
}

func (in *ListClusterInstancePersistentVolumes) Result() ClusterInstancePersistentVolumeList {
	return in.list
}

type HandleClusterInstancePersistentVolume struct {
	namespace string
}

func OpHandleClusterInstancePersistentVolume(namespace string) *HandleClusterInstancePersistentVolume {
	return &HandleClusterInstancePersistentVolume{namespace: namespace}
}

func (in *HandleClusterInstancePersistentVolume) Handle(evt *manager.WorkerEvent) []*manager.WorkerEvent {
	if evt.Object != nil {
		vol, ok := evt.Object.(*worker.InstancePersistentVolume)
		if !ok {
			return nil
		}
		if in.namespace != "" && vol.Namespace != in.namespace {
			return nil
		}
		if vol.DeletionTimestamp != nil {
			evt.Type = manager.WorkerEventDeleted
		}
		clusterVol := &ClusterInstancePersistentVolume{
			InstancePersistentVolume: *vol,
			Cluster:                  evt.Cluster,
		}
		clusterVol.ManagedFields = nil
		evt.Object = clusterVol
	}
	return []*manager.WorkerEvent{evt}
}

type ListClusterInstanceImagePullSecrets struct {
	list ClusterInstanceImagePullSecretList
}

func OpListClusterInstanceImagePullSecrets() *ListClusterInstanceImagePullSecrets {
	return &ListClusterInstanceImagePullSecrets{
		list: ClusterInstanceImagePullSecretList{
			Items: make([]ClusterInstanceImagePullSecret, 0),
		},
	}
}

func (in *ListClusterInstanceImagePullSecrets) Next(cluster string, obj runtime.Object) error {
	secret, ok := obj.(*worker.InstanceImagePullSecret)
	if !ok {
		return fmt.Errorf("object is not of type InstanceImagePullSecret")
	}

	item := ClusterInstanceImagePullSecret{
		InstanceImagePullSecret: *secret,
		Cluster:                 cluster,
	}
	item.ManagedFields = nil
	in.list.Items = append(in.list.Items, item)
	return nil
}

func (in *ListClusterInstanceImagePullSecrets) Result() ClusterInstanceImagePullSecretList {
	return in.list
}

type HandleClusterInstanceImagePullSecret struct {
	namespace string
}

func OpHandleClusterInstanceImagePullSecret(namespace string) *HandleClusterInstanceImagePullSecret {
	return &HandleClusterInstanceImagePullSecret{namespace: namespace}
}

func (in *HandleClusterInstanceImagePullSecret) Handle(evt *manager.WorkerEvent) []*manager.WorkerEvent {
	if evt.Object != nil {
		secret, ok := evt.Object.(*worker.InstanceImagePullSecret)
		if !ok {
			return nil
		}
		if in.namespace != "" && secret.Namespace != in.namespace {
			return nil
		}
		if secret.DeletionTimestamp != nil {
			evt.Type = manager.WorkerEventDeleted
		}
		clusterSecret := &ClusterInstanceImagePullSecret{
			InstanceImagePullSecret: *secret,
			Cluster:                 evt.Cluster,
		}
		clusterSecret.ManagedFields = nil
		evt.Object = clusterSecret
	}
	return []*manager.WorkerEvent{evt}
}

type ListClusterInstanceSSHPublicKeys struct {
	list ClusterInstanceSSHPublicKeyList
}

func OpListClusterInstanceSSHPublicKeys() *ListClusterInstanceSSHPublicKeys {
	return &ListClusterInstanceSSHPublicKeys{
		list: ClusterInstanceSSHPublicKeyList{
			Items: make([]ClusterInstanceSSHPublicKey, 0),
		},
	}
}

func (in *ListClusterInstanceSSHPublicKeys) Next(cluster string, obj runtime.Object) error {
	key, ok := obj.(*worker.InstanceSSHPublicKey)
	if !ok {
		return fmt.Errorf("object is not of type InstanceSSHPublicKey")
	}

	item := ClusterInstanceSSHPublicKey{
		InstanceSSHPublicKey: *key,
		Cluster:              cluster,
	}
	item.ManagedFields = nil
	in.list.Items = append(in.list.Items, item)
	return nil
}

func (in *ListClusterInstanceSSHPublicKeys) Result() ClusterInstanceSSHPublicKeyList {
	return in.list
}

type HandleClusterInstanceSSHPublicKey struct {
	namespace string
}

func OpHandleClusterInstanceSSHPublicKey(namespace string) *HandleClusterInstanceSSHPublicKey {
	return &HandleClusterInstanceSSHPublicKey{namespace: namespace}
}

func (in *HandleClusterInstanceSSHPublicKey) Handle(evt *manager.WorkerEvent) []*manager.WorkerEvent {
	if evt.Object != nil {
		key, ok := evt.Object.(*worker.InstanceSSHPublicKey)
		if !ok {
			return nil
		}
		if in.namespace != "" && key.Namespace != in.namespace {
			return nil
		}
		if key.DeletionTimestamp != nil {
			evt.Type = manager.WorkerEventDeleted
		}
		clusterKey := &ClusterInstanceSSHPublicKey{
			InstanceSSHPublicKey: *key,
			Cluster:              evt.Cluster,
		}
		clusterKey.ManagedFields = nil
		evt.Object = clusterKey
	}
	return []*manager.WorkerEvent{evt}
}
