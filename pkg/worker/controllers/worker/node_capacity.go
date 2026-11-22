package worker

import (
	"context"
	"maps"
	"sort"
	"strconv"
	"strings"
	"time"

	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlbuilder "sigs.k8s.io/controller-runtime/pkg/builder"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlevent "sigs.k8s.io/controller-runtime/pkg/event"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	ctrlpredicate "sigs.k8s.io/controller-runtime/pkg/predicate"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/controller"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/systemname"
	"gpustack.ai/gpustack/pkg/utils/ctrlhandlerx"
	"gpustack.ai/gpustack/pkg/utils/json"
	"gpustack.ai/gpustack/pkg/utils/mapx"
)

// NodeCapacityReconciler 协调受管 Kubernetes Node 的 status.capacity 上的加速器计数扩展资源，
// 由 Node 的特征标签/容量变更以及同名 Devices 对象驱动：
//   - 软件切片与硬件分区是针对两组永不重叠的物理卡群的两个系列，因此各自拥有独立的计数键，
//     且分别以其自身的裸 device-plugin 令牌池是否存在作为门控。厂商的 ".sliced.*" 键仅在其
//     ".sliced" 池存在时出现，".partitioned.*" 键仅在其 ".partitioned" 池存在时出现；
//     当令牌池消失或降为 0 时，会反向 patch 移除对应系列的键。
//     参见 desiredAcceleratorCapacity 了解键集合及各自的计算方式。
//   - 每个 profile 的分区键基于运行时账本而非静态上限推导，因为不同 profile 会竞争同一批
//     物理切片：切分出一种规格会占用另一种规格所需的空间。这使得分配或释放操作即使在能力不变的
//     情况下也会改变容量值，因此 Devices watch 需要同时响应 status 侧与 spec 侧的变更。
//   - 当 kubelet 重启清空容量后，采用基于状态（level-based）的机制重新应用。
//
// ".units" 键的较大数值（每张卡 D 个）由本控制器上报，而非通过 device-plugin 的虚拟设备池上报，
// 以免给 kubelet 设备管理器带来压力。
type NodeCapacityReconciler struct {
	Client ctrlcli.Client
}

var _ ctrlreconcile.Reconciler = (*NodeCapacityReconciler)(nil)

func (r *NodeCapacityReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := ctrllog.FromContext(ctx)

	// 获取 Node。此处为只读操作（下方的容量 patch 针对新建的 Node 对象），因此跳过深拷贝。
	nd := new(core.Node)
	err := r.Client.Get(ctx, req.NamespacedName, nd, ctrlcli.UnsafeDisableDeepCopy)
	if err != nil {
		// 已删除的 Node 无需再进行协调。
		if err = ctrlcli.IgnoreNotFound(err); err != nil {
			logger.Error(err, "fetch node")
		}
		return ctrl.Result{}, err
	}

	// 若已删除则跳过。
	if nd.DeletionTimestamp != nil {
		logger.V(3).Info("skip deleted node")
		return ctrl.Result{}, nil
	}

	// 获取同名的 Devices CR（每个 Node 一个）；它提供各厂商的最大切片数与算力超卖标志。
	// 若账本缺失则视为无切片能力，".sliced.*" 键将保持反向 patch 移除状态，直到账本上报。
	devs := new(workercore.Devices)
	err = r.Client.Get(ctx, ctrlcli.ObjectKey{Name: nd.Name}, devs, ctrlcli.UnsafeDisableDeepCopy)
	if err != nil {
		if !kerrors.IsNotFound(err) {
			logger.Error(err, "fetch devices")
			return ctrl.Result{}, err
		}
		devs = nil
	}

	// 将 Node 的 ".sliced.*" 容量收敛到期望集合（对于未纳管或不支持加速的 Node 为空集，从而移除任何陈旧键）。
	capacityPatch := buildAcceleratorCapacityPatch(desiredAcceleratorCapacity(nd, devs), nd.Status.Capacity)
	if capacityPatch == nil {
		return ctrl.Result{}, nil
	}
	data := json.ShouldMarshal(map[string]any{
		"status": map[string]any{"capacity": capacityPatch},
	})
	// 对新建的 Node 对象而非 nd 执行 patch：merge-patch 响应会解码回目标对象，
	// 而 nd 是在未深拷贝的情况下读取的（直接修改它会污染共享 informer 缓存）。
	patchNode := &core.Node{ObjectMeta: meta.ObjectMeta{Name: nd.Name}}
	err = r.Client.Status().Patch(ctx, patchNode, ctrlcli.RawPatch(types.MergePatchType, data))
	if err != nil {
		logger.Error(err, "patch node accelerator capacity")
		return ctrl.Result{}, err
	}
	logger.V(2).Info("patched node accelerator capacity")
	return ctrl.Result{}, nil
}

// desiredAcceleratorCapacity 计算受管 Node 应当上报的计数扩展资源，按厂商对其同名 Devices 组
// 中上报了切片能力的型号求和（因此不支持切片的型号——例如与支持切片的 Ascend 910B 共用同一 Node
// 的 Ascend 310——不会计入任何值）。
//
// 两个系列统计互不相交的卡群，因为处于硬件分区模式的卡无法提供软件切片，反之亦然：
//   - ".sliced.units"                 = Σ logicalCards × D
//   - ".sliced.cores-percentage"      = Σ Detail.Logical.Count × 100（开启超卖时），否则为 Σ logicalCards × 100
//   - ".sliced.memory-percentage"     = Σ logicalCards × 100
//   - ".sliced.memory-mib"            = Σ logicalCards × 单卡显存 MiB
//   - ".partitioned.units"            = Σ partitionedCards × D
//   - ".partitioned.<kind>-<profile>" = Σ 该 profile 的（已分配 + 剩余）实例数
//
// 每个系列均以其自身的裸 device-plugin 令牌池是否存在作为门控，因此只有当插件确实在此 Node 上
// 提供该系列服务时，Node 才会通告该系列的计数键。门控读取 capacity 而非 allocatable：
// 在物理卡无法提供该系列服务的 Node 上 capacity 本就会降为 0；而当该系列仅仅是资源饱和或被占用时
// allocatable 也会降为 0——若以 allocatable 为门控，会导致实例运行期间计数键被删除，并在释放后重新添加。
//
// 所有卡数量均以 Devices 账本的逐卡数据作为唯一事实来源——绝不与 Node 的 ".count" 标签混用——
// 并通过完整的可加速节点键（"${manufacturer}-${group ID}"）解析到各型号，从而确保同一厂商下
// 显存（或切片数/超卖配置）不同的型号能够正确求和。若某系列没有可用的卡，则不输出对应键，
// 由 buildAcceleratorCapacityPatch 反向 patch 移除任何陈旧键。
//
// 单卡显存仍采用取整到 Gi 的有损 ".memory" 标签值，而非精确的 DevicesGroup.Memory，
// 以保证通告的 memory-mib 与 Pod Webhook 锚点一致。对于未纳管 Node 返回 nil。
func desiredAcceleratorCapacity(nd *core.Node, devs *workercore.Devices) core.ResourceList {
	// 在第 10 章中实现此函数。
	return nil
}

// acceleratorCards 将 Devices 组中的卡划分为两个永不重叠的集合：logical 统计提供软件切片的卡数，
// partitioned 统计处于硬件分区模式的卡数。logicalSlices 为所有 logical 卡的切片数之和。
// 每张卡至多属于一个集合——从而避免同一张卡同时被两个系列的容量键统计——两者均不提供的卡则不归入任何集合。
func acceleratorCards(g *workercore.DevicesGroup) (logical, logicalSlices, partitioned int64) {
	for i := range g.Accelerators {
		st := g.Accelerators[i].Status
		switch {
		case device.IsPartitioned(st):
			partitioned++
		case device.IsLogicallySliceable(st):
			logical++
			logicalSlices += int64(st.LogicalSliced.Count)
		}
	}
	return logical, logicalSlices, partitioned
}

// partitionInstancesByProfile 按 profile 名称统计一组处于分区模式的卡所能提供的硬件分区总数：
// 对这些卡求和，累计每个 profile 已分配的实例数加上仍可容纳的实例数。
//
// 这两项缺一不可。调度器在放置 Pod 时会从通告容量中减去 Node 上已有 Pod 的请求量，
// 因此若只上报仍空闲的数量，会导致每个运行中的实例被重复扣减，该键值会比实际少一个。
//
// 若某张卡的账本条目既未上报已分配实例也未上报剩余实例，说明尚无可用的账本——
// 运行时状态是根据卡缓存的放置拓扑重建的，而旧版 DeviceManager 未记录该信息——
// 此时回退到能力声明中的静态每 profile 上限。对于实际已被占用的卡，这会暂时高估可用量，
// 一旦账本上报即会收敛；而若上报 0，则会使正常工作的 Node 陷入不可调度状态。
func partitionInstancesByProfile(g *workercore.DevicesGroup, ledger map[string]*workercore.AcceleratorAllocation) map[string]int64 {
	out := make(map[string]int64)
	for i := range g.Accelerators {
		acc := &g.Accelerators[i]
		if !device.IsPartitioned(acc.Status) {
			continue
		}
		alloc := ledger[acc.ID]
		ready := device.PartitionLedgerReady(alloc)
		// 为卡提供的每个 profile 都生成条目，即使值为 0。这样当某个 profile 的空间被其他 profile 的
		// 实例占用时，其值变为 0 而非直接消失，从而在卡填充过程中仅数值变化而键本身保持存在——
		// 减少每次切分和释放时重写 Node 对象的理由。
		for k := range acc.Status.PhysicalSliced.Profiles {
			p := &acc.Status.PhysicalSliced.Profiles[k]
			if _, seen := out[p.Name]; !seen {
				out[p.Name] = 0
			}
			if !ready {
				out[p.Name] += int64(p.Count)
			}
		}
		if !ready {
			continue
		}
		for k := range alloc.AllocatedProfiles {
			out[alloc.AllocatedProfiles[k].Name] += int64(alloc.AllocatedProfiles[k].Count)
		}
		for k := range alloc.RemainingProfiles {
			out[alloc.RemainingProfiles[k].Name] += int64(alloc.RemainingProfiles[k].Count)
		}
	}
	return out
}

// devicesLedgerByGroup 按与 spec 侧相同的 "${manufacturer}-${group ID}" 键建立 Node 运行时
// 分配账本的索引，再按加速器 ID 索引。能力信息位于 Devices 的 spec，占用状态位于其 status，
// 因此按 profile 划分的容量键是两者的连接结果；直接从 spec 读取占用状态会静默返回空值。
// 键中包含厂商前缀的原因与 spec 索引相同：ConstructGroupID 会剥离厂商前缀，
// 裸 group ID 在同一 Node 上可能跨厂商冲突，从而导致误将某厂商的账本用于另一厂商的卡。
func devicesLedgerByGroup(devs *workercore.Devices) map[string]map[string]*workercore.AcceleratorAllocation {
	if devs == nil {
		return nil
	}
	out := make(map[string]map[string]*workercore.AcceleratorAllocation, len(devs.Status.Groups))
	for i := range devs.Status.Groups {
		grp := &devs.Status.Groups[i]
		accs := make(map[string]*workercore.AcceleratorAllocation, len(grp.Accelerators))
		for j := range grp.Accelerators {
			accs[grp.Accelerators[j].ID] = &grp.Accelerators[j]
		}
		out[grp.Manufacturer+"-"+grp.ID] = accs
	}
	return out
}

// ownedCapacitySuffixes 是 NodeCapacityReconciler 通过 Patch Node 管理的固定计数键后缀。
// 按 profile 划分的 ".partitioned.<kind>-<profile>" 键具有可变后缀（profile 名称），
// 改由解析匹配（见 isOwnedCapacityKey）。device-plugin 通告的裸 ".sliced"、
// ".partitioned" 与 ".shared" 键被刻意排除在外，确保它们绝不会被置空移除。
var ownedCapacitySuffixes = []string{
	nodefeature.SlicedUnitsResourceNameSuffix,
	nodefeature.SlicedCoresPercentageResourceNameSuffix,
	nodefeature.SlicedMemoryPercentageResourceNameSuffix,
	nodefeature.SlicedMemoryMibResourceNameSuffix,
	nodefeature.PartitionedUnitsResourceNameSuffix,
}

// isOwnedCapacityKey 判断 name 是否属于本控制器管理的计数键——即五个固定键或按 profile
// 划分的 ".partitioned.<kind>-<profile>" 键——而非裸 device-plugin 令牌键。
func isOwnedCapacityKey(name core.ResourceName) bool {
	// 精确识别按 profile 划分的键（已知的加速器前缀、已知的分区类型、非空 profile），
	// 而非仅匹配原始中缀，从而避免外表相似的外部扩展资源被误判为本控制器所有，并在未出现在期望集合时被置空移除。
	if _, ok := nodefeature.PartitionedProfileOf(name); ok {
		return true
	}
	return hasAcceleratorSuffix(name, ownedCapacitySuffixes...)
}

// isAcceleratorFamilyCapacityKey 判断 name 是否为本控制器必须响应的容量键：
// 任一切片系列的裸 device-plugin 令牌池（决定是否通告的存在性门控）或任一归属本控制器的计数键。
// 只有归属本控制器的键才会被 patch（见 isOwnedCapacityKey）；裸令牌池只监听而不写入。
func isAcceleratorFamilyCapacityKey(name core.ResourceName) bool {
	return isOwnedCapacityKey(name) ||
		hasAcceleratorSuffix(name,
			nodefeature.SlicedResourceNameSuffix,
			nodefeature.PartitionedResourceNameSuffix)
}

// hasAcceleratorSuffix 判断 name 是否在已知的加速器前缀之后带有指定后缀之一。
// 前缀检查可防止其他插件恰好以相同后缀结尾的外部扩展资源（如 "example.com/foo.partitioned.units"）
// 被误认为归属本控制器，并在未出现在期望集合时被置空移除。
func hasAcceleratorSuffix(name core.ResourceName, suffixes ...string) bool {
	for _, suffix := range suffixes {
		base, ok := strings.CutSuffix(string(name), suffix)
		if ok && nodefeature.IsKnownAcceleratableResourceName(core.ResourceName(base)) {
			return true
		}
	}
	return false
}

// buildAcceleratorCapacityPatch 返回将 Node 收敛到 desired 所需的 status.capacity 条目：
// 设置当前值与期望不同的每个期望计数键，并将不在 desired 中的陈旧归属键置为 null（移除）。
// 若当前容量已完全匹配则返回 nil，调用方即可跳过 patch——从而保持重复 patch 的幂等性。
// 仅输出归属本控制器的键，因此绝不会触及 kubelet 管理的容量。
func buildAcceleratorCapacityPatch(desired, current core.ResourceList) map[string]any {
	patch := make(map[string]any)
	for name, q := range desired {
		if cur, ok := current[name]; !ok || cur.Cmp(q) != 0 {
			patch[string(name)] = q.String()
		}
	}
	for name := range current {
		if !isOwnedCapacityKey(name) {
			continue
		}
		if _, ok := desired[name]; !ok {
			patch[string(name)] = nil
		}
	}
	if len(patch) == 0 {
		return nil
	}
	return patch
}

// acceleratorFamilyCapacityChanged 判断两个容量映射之间是否有本控制器关注的容量条目
// （任一系列的裸 device-plugin 令牌池，或任一归属计数键）被添加、移除或变更。
func acceleratorFamilyCapacityChanged(oldCap, newCap core.ResourceList) bool {
	for name, q := range newCap {
		if !isAcceleratorFamilyCapacityKey(name) {
			continue
		}
		if old, ok := oldCap[name]; !ok || old.Cmp(q) != 0 {
			return true
		}
	}
	for name := range oldCap {
		if !isAcceleratorFamilyCapacityKey(name) {
			continue
		}
		if _, ok := newCap[name]; !ok {
			return true
		}
	}
	return false
}

func (r *NodeCapacityReconciler) SetupController(_ context.Context, opts controller.SetupOptions) error {
	r.Client = opts.Manager.GetClient()

	dedupWindow := ctrlhandlerx.NewDedupWindow[ctrlreconcile.Request]()

	return ctrl.NewControllerManagedBy(opts.Manager).
		Named("nodecapacity").
		For(
			&core.Node{},
			ctrlbuilder.WithPredicates(
				// 在以下情况下触发对 Node 的协调：
				// - 创建。
				// - 更新时若其受管/可加速特征标签发生变化（新增/移除型号或卡数变化），或 ".sliced" 系列
				//   容量条目发生变化——包括 device-plugin 的裸 ".sliced" 令牌池出现/消失/扩缩（存在性门控），
				//   或归属本控制器的 ".sliced.*" 键发生变化（kubelet 重启清空后的自愈）。
				//   逐卡切片细节本身的变更则通过下方的 Devices watch 传入。
				ctrlpredicate.Funcs{
					DeleteFunc: func(ctrlevent.DeleteEvent) bool {
						return false
					},
					UpdateFunc: func(e ctrlevent.UpdateEvent) bool {
						oldNd, newNd := e.ObjectOld.(*core.Node), e.ObjectNew.(*core.Node)
						if newNd.DeletionTimestamp != nil {
							return false
						}
						if !mapx.EqualWithStringPrefix(oldNd.Labels, newNd.Labels,
							systemname.ManagedLabelKey,
							nodefeature.AcceleratableFeatureLabelPrefix) {
							return true
						}
						return acceleratorFamilyCapacityChanged(oldNd.Status.Capacity, newNd.Status.Capacity)
					},
				},
			),
		).
		Watches(
			// Devices 的变更可能改变期望的计数容量，而不一定会改动 Node predicate 监听的裸令牌池，
			// 因此需要将同名的 Node 加入队列。有两类变更值得关注：spec 侧的能力变更（尤其是分区模式切换，
			// 它会重新划分卡群），以及 status 侧的占用变更——由于按 profile 划分的分区键现已基于运行时账本推导，
			// 切分或释放分区必须重新 patch Node。3s 的去重窗口用于合并由此产生的突发事件。
			&workercore.Devices{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFuncWithWindow(
				3*time.Second,
				dedupWindow,
				r.enqueueNodeWhenDevicesChanged,
			),
			ctrlbuilder.WithPredicates(ctrlpredicate.Funcs{
				// 受管 Devices 的切片细节：创建/删除时整个能力出现/消失；更新时仅在 spec 切片细节变更时触发，
				// 绝不因 Status（分配）抖动或健康状态等非切片 spec 字段而触发。
				CreateFunc: func(e ctrlevent.CreateEvent) bool {
					return isManagedDevices(e.Object)
				},
				DeleteFunc: func(e ctrlevent.DeleteEvent) bool {
					return isManagedDevices(e.Object)
				},
				UpdateFunc: func(e ctrlevent.UpdateEvent) bool {
					oldDevs, newDevs := e.ObjectOld.(*workercore.Devices), e.ObjectNew.(*workercore.Devices)
					if !isManagedDevices(newDevs) {
						return false
					}
					return acceleratorDetailChanged(oldDevs, newDevs)
				},
			}),
		).
		Complete(r)
}

// isManagedDevices 判断 Devices 账本是否属于受 Operator 管理的 Node。
func isManagedDevices(obj ctrlcli.Object) bool {
	return obj.GetLabels()[systemname.ManagedLabelKey] == "true"
}

// enqueueNodeWhenDevicesChanged 将发生变更的 Devices 账本映射到与其同名的 Node
// （两者均为集群作用域且共享 Node 名称），从而在逐卡切片细节变更时协调该 Node 的 ".sliced.*" 容量。
func (r *NodeCapacityReconciler) enqueueNodeWhenDevicesChanged(
	ctx context.Context, obj ctrlcli.Object,
) []ctrlreconcile.Request {
	req := ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKey{Name: obj.GetName()}}
	ctrllog.FromContext(ctx).V(2).Info("enqueued node from devices", "request", req)
	return []ctrlreconcile.Request{req}
}

// acceleratorSignature 精确捕获用于计算期望容量的 Devices 字段：逐卡的逻辑/分区划分、
// 逻辑切片详情（数量与超卖标志）以及各 profile 的分区实例数。通过比较签名，可使 Devices watch
// 仅在可能影响容量键的变更时触发，包括卡数不变但 profile 集合发生变化的重新分区。
// 该类型必须保持可比较性（用作 maps.Equal 的值），因此将 profile 折叠为规范字符串而非 map。
type acceleratorSignature struct {
	logicalCards      int64
	partitionedCards  int64
	logicalCount      int32
	logicalOvercommit bool
	partitionProfiles string
}

// partitionProfileSignature 将组的各 profile 分区实例数渲染为规范且与顺序无关的
// "name=count;..." 字符串，使得 profile 集合或任意 profile 数量变更时重新入队 Node，
// 而相同清单比较结果相等。
func partitionProfileSignature(profiles map[string]int64) string {
	if len(profiles) == 0 {
		return ""
	}
	parts := make([]string, 0, len(profiles))
	for name, count := range profiles {
		parts = append(parts, name+"="+strconv.FormatInt(count, 10))
	}
	sort.Strings(parts)
	return strings.Join(parts, ";")
}

// acceleratorDetailChanged 判断两份 Devices 快照之间影响期望容量计算的内容是否发生变动。
// 与早期实现不同，它会同时读取 status 侧与 spec 侧：由于按 profile 划分的分区键源自运行时账本，
// 分配或释放操作会在 spec 不变的情况下改变容量值。status 侧的其他变动（如模式切换、units 波动）
// 仍映射到相同签名，因此不会触发协调。
func acceleratorDetailChanged(oldDevs, newDevs *workercore.Devices) bool {
	return !maps.Equal(acceleratorSignatures(oldDevs), acceleratorSignatures(newDevs))
}

// acceleratorSignatures 将 Devices 对象的各个组投影为其签名，并以 "${manufacturer}-${group ID}"
// 可加速键作为索引。
func acceleratorSignatures(devs *workercore.Devices) map[string]acceleratorSignature {
	if devs == nil {
		return nil
	}
	ledgerByGroup := devicesLedgerByGroup(devs)
	out := make(map[string]acceleratorSignature, len(devs.Spec.Groups))
	for i := range devs.Spec.Groups {
		g := &devs.Spec.Groups[i]
		key := g.Manufacturer + "-" + g.ID
		logicalCards, _, partitionedCards := acceleratorCards(g)
		out[key] = acceleratorSignature{
			logicalCards:      logicalCards,
			partitionedCards:  partitionedCards,
			logicalCount:      g.AcceleratorSlicedDetail.Logical.Count,
			logicalOvercommit: g.AcceleratorSlicedDetail.Logical.CoresPercentageOvercommit,
			partitionProfiles: partitionProfileSignature(partitionInstancesByProfile(g, ledgerByGroup[key])),
		}
	}
	return out
}
