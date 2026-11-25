package deviceplugin

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlbuilder "sigs.k8s.io/controller-runtime/pkg/builder"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlevent "sigs.k8s.io/controller-runtime/pkg/event"
	ctrlhandler "sigs.k8s.io/controller-runtime/pkg/handler"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	ctrlpredicate "sigs.k8s.io/controller-runtime/pkg/predicate"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"
	ctrlsource "sigs.k8s.io/controller-runtime/pkg/source"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/controller"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/kubeclientset"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/utils/json"
	"gpustack.ai/gpustack/pkg/utils/osx"
	"gpustack.ai/gpustack/pkg/utils/stringx"
)

// IMPORTANT: 监听本机 Devices 和 Pod，负责把真实占用情况重建回 Devices.Status（账本），并给各插件广播刷新通知

type (
	_DevicesNotifier struct {
		Manufacturer   string
		AllocationMode workercore.DeviceAllocationMode
		Channel        chan []string
	}
	// _ReservationKey 用于标识一个容器在进程中的分配请求。一个 Pod 可以同时持有多个请求——设备插件 API
	// 在每次 Allocate 调用中对应一个容器——因此容器名称是身份的一部分，而非有效载荷的细节。
	_ReservationKey struct {
		PodUID    types.UID
		Container string
	}
	// 预订记录是某个容器持久分配记录的进程内镜像。DeviceIDs 是 kubelet 为该容器的分配操作提供的设备
	// `ID`。那些由分配机制选择加速器本身的设备类型会返回那些`ID`无法标识的设备，因此这些`ID`无法从分
	// 配记录中重新推导出来，它们会与其分配记录一同保存。
	_Reservation struct {
		Allocated workercore.DevicesStatus
		DeviceIDs []string
	}
	// DevicesReconciler 会在 Kubernetes Node 上对 v1alpha1.Devices 对象进行协调，并监控被安排到该
	// Node 的 Pods 事件，以管理 Devices 的状态。
	DevicesReconciler struct {
		NodeName  string
		Client    ctrlcli.Client
		APIReader ctrlcli.Reader

		notifiersMutex sync.RWMutex
		notifiers      []_DevicesNotifier
		// lastLivePodUIDs 缓存了最新的广播直播 pod-UID 集合，因此在进行协调后订阅的通知器会立即获
		// 得初始数据。如果没有这个缓存，迟到的订阅者（即每个供应商在初始协调后进行的重新注册循环）
		// 可能直到下一次协调才会获得数据，并且在重新同步时也无法进行数据恢复。
		lastLivePodUIDs []string

		// 按 pod UID 和容器名称进行键控的预订记录显示，加速器设备分配了某个容器，因此在同一节点、
		// 同一准入窗口期内，同一 Pod 的 Allocate 操作可以共享相同的物理设备，无需重新选择或竞争注
		// 解缓存的传播。仅通过 Pod 进行键控会导致某个 Pod 中第一个被服务的容器掩盖后续所有容器：
		// 已预留的容器将被拒绝解析，因此每个持有活跃占用的两个容器永远无法同时被服务。
		reservationsMutex sync.RWMutex
		reservations      map[_ReservationKey]_Reservation

		// visibilityGrants 记录了哪些容器已获得 Allocate 的可见性分配，该记录以 pod UID 和容器名
		// 称作为键。它决定了两个 pod 的可见性容器有何不同：此类容器不消耗账本单位，因此也不创建预
		// 留，所以没有此记录就无法标记其已服务，而一批相同的 Pods 会将其可见性分配分配给最旧的待
		// 处理 pod——这样第二个 pod 的容器就能获得第一个 pod 的设备，进而获得第一个 pod 的分区。
		//
		// 它刻意没有被纳入预订中：一个可见性容器不包含加速器，若在其中添加条目，则会在每个加速器
		// 模式、占用情况以及设备 ID 统计中计入每个地图阅读器的数据。
		visibilityGrantsMutex sync.RWMutex
		visibilityGrants      sets.Set[_ReservationKey]

		// pendingReleases 记录了尚未完成的补偿注释写入操作，以 pod UID 和容器名称作为键，用于标记
		// 需要恢复的条目（值为 nil 则代表删除该条目）。补偿的其中一半是内存资源，不会失败。这半部
		// 分属于 I/O 资源，可能会失败。如果留下一个条目，就会让 kubelet 始终为那个从未启动的容器
		// 消耗加速器资源——这就是补偿机制存在的意义，旨在修复这一漏洞。没有其他操作会重新尝试它：
		// Pod 监视机制会在该注释发生变更时触发，而触发该机制的变化就是失败的写入操作。因此，
		// Reconcile 会不断重试这些操作，直到它们完成或对应的 pod 被移除。
		//
		// 它刻意没有被纳入预留区域：此处没有条目即代表没有要求，任何查看该地图的人都会将其视为一
		// 种预留。
		pendingReleasesMutex sync.RWMutex
		pendingReleases      map[_ReservationKey]*ContainerAllocation

		// pendingReleaseEvents 会在记录到补偿写入操作时唤醒此协调器。其他操作不会：触发该事件的唯
		// 一 Pod 变更是分配注释的更改，而原本会触发该事件的写入操作刚刚失败了——而 RequeueAfter 只
		// 能从已经看到该条记录的轮次中重新启动。由于已排队等待的唤醒操作，该操作会被缓冲并发送而
		// 不产生阻塞，一个空的通道（没有进行任何配置）会直接丢弃该操作，而下一个协调过程仍会在表
		// 中找到该条记录。
		pendingReleaseEvents chan ctrlevent.GenericEvent

		// allocateMutex 会对整个工作负载进行序列化处理。Allocate 的标识用于节点层面的
		// cross-mode-check 与预留区域，而 Allocate 的标识则用于 grant 区域。所有 per-mode
		// ResourceServers 共享同一个协调器，因此单个节点范围内的 mutex 可以使得并发的 Allocate（
		// 例如被 Kueue 批准的相同 Pods 的批次）依次处理该区域：每次写入的声明——无论是预留还是授权
		// ——在下一个标识其 pod 之前即可被看到，因此 getAllocatingPod（跳过已被占用的容器）可以将
		// 每个 Allocate 映射到不同的 pod，而不是所有 Allocate 都指向最旧的待处理 pod。对于同一加
		// 速器而言，不同模式的 Allocate 无法在 cross-mode check 与预留写入（TOCTOU）之间交错执行。
		// 该 mutex 不会在注解补丁（I/O）或触发或命名分区的厂商调用中被持有。
		allocateMutex sync.Mutex

		// kubeletPods 用于询问 kubelet 所持有的 Pod 和容器，因此分配器可以知道它服务的是哪个容器。
		// 参见 _KubeletPending。Nil 将识别工作完全交给了待处理的 Pod 启发式算法。
		kubeletPods kubeletPodLister
		// slicedAllocateGateOff 取消了对逻辑切片中无法容纳的卡片的拒绝处理，因此逻辑分片分配机制
		// 会锁定账本并进行分配，就像在拒绝机制存在之前那样。
		slicedAllocateGateOff bool
	}
)

// IMPORTANT: 让控制面的 NodeDevicesAdmissionReconciler 和本节点的 ListAndWatch 知道这张卡“已被占用”或“已归还空闲”？
// ======
// Reconcile 功能会根据节点的 `Spec` 以及安排在该节点上的 Pods 的注释信息，重新构建该节点的 Devices 状态。
// 随后，它会将生成的实时 pod-UID 集合通过进程内预留/可见性授权表进行处理，并通知 ListAndWatch 订阅者。状
// 态的重新构建是整体性的，而非增量式的：BuildDesiredStatus 会在一次遍历中根据 `Spec` 和 Pod 的注释信息重
// 新计算每个加速器模式、剩余资源、已分配配置和剩余配置。该方法只分配一次结果，刻意没有第二次单独应用的写
// 入操作，以便后续遍历能够覆盖它。
func (r *DevicesReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	nodeName := req.Name

	logger := ctrllog.FromContext(ctx).WithValues("node", nodeName)

	devs := new(workercore.Devices)
	err := r.Client.Get(ctx, req.NamespacedName, devs)
	if err != nil {
		logger.Error(err, "fetch devices")
		return ctrl.Result{RequeueAfter: time.Second}, err
	}

	podList := new(core.PodList)
	err = r.Client.List(ctx, podList,
		ctrlcli.MatchingFields{IndexingPodsByNodeName: nodeName},
		ctrlcli.UnsafeDisableDeepCopy)
	if err != nil {
		logger.Error(err, "list pods with node name")
		return ctrl.Result{RequeueAfter: time.Second}, err
	}

	// 先核对 kubelet 与 informer 的记录。两者冲突时暂停发布账本，但还要继续清理无关的预留并尝试归还设备。
	// 如果在此直接返回，一条过期记录就可能阻塞其他 Pod 的补偿写入，而失败的写入本身不会触发新的 Pod 事件。
	_, verifyErr := r.verifyKubeletAllocations(ctx, devs, podList)
	if verifyErr != nil {
		logger.Error(verifyErr, "verify device bookkeeping against kubelet")
	}
	desiredStatus, livePodUIDs := BuildDesiredStatus(logger, devs, podList)

	// 只有本轮核对通过才发布新账本。遇到冲突时保留已发布的状态，等两份记录能对上再更新。
	if verifyErr == nil && !kubemeta.DeepEqual(devs.Status, desiredStatus) {
		devs.Status = desiredStatus
		err = r.Client.Status().Update(ctx, devs)
		if err != nil {
			logger.Error(err, "update devices status")
			return ctrl.Result{}, ctrlcli.IgnoreNotFound(err)
		}
	}

	// 清理已消失 Pod 的进程内预留和可见性授权。这里复用切片目录回收器使用的存活 UID 集合。
	r.pruneReservations(livePodUIDs)
	r.pruneVisibilityGrants(livePodUIDs)
	r.retryPendingReleases(ctx, logger, podList.Items, livePodUIDs)

	// 等可独立执行的补偿处理完再报告冲突。此轮仍不能用未核实的清单向监听者公布新状态。
	if verifyErr != nil {
		return ctrl.Result{}, verifyErr
	}

	r.notifiersMutex.Lock()
	r.lastLivePodUIDs = livePodUIDs
	r.notifiersMutex.Unlock()
	r.notifyListeners()

	// 补偿写入未成功前，账本仍会显示设备被占用。Pod 监听等不到一次未成功的注解变更，
	// 因此还有待归还记录时需要主动安排下一轮。
	if r.hasPendingReleases() {
		return ctrl.Result{RequeueAfter: pendingReleaseRetryPeriod}, nil
	}
	if r.kubeletPods != nil {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	return ctrl.Result{}, nil
}

// BuildDesiredStatus 根据 Spec 和 Pod 的注解，整体重建节点的 Devices 状态：它从 Spec 中初始化每个加速器，
// 然后合并每个 Pod 的注解所指定的内容。这两个步骤作为一次操作执行——mergePodAllocations 在合并物理切片规格
// 账本的同时，也在同一过程中收集当前 pod-UID 集合，因此不会出现后续步骤对已有数据的二次单独写入。
//
// 该文件被导出为 worker 节点设备使用的 AdmissionCheck 工具，该工具会根据其读取的 Pod 列表重新构建账本，而
// Pods 的收费数据也来自同一列表。发布的 Status 则是该结果的副本。根据这一副本来判断，可以认为某个 Pod 已
// 被计入收费，而此时账本才显示其分配情况。
func BuildDesiredStatus(
	logger logr.Logger, devs *workercore.Devices, podList *core.PodList,
) (workercore.DevicesStatus, []string) {
	return mergePodAllocations(logger, devs, podList, initDesiredStatus(devs))
}

// reserveDevices 按 Pod UID 和容器名保存分配结果及 kubelet 发来的设备 ID，作为 Pod 持久注解的即时副本。
// 同一 Pod 的边车无需等待注解同步就能找到主容器的设备。身份或分配为空时跳过。
func (r *DevicesReconciler) reserveDevices(
	podUID types.UID, container string, allocated workercore.DevicesStatus, deviceIDs []string,
) {
	if podUID == "" || container == "" || len(allocated.Groups) == 0 {
		return
	}
	r.reservationsMutex.Lock()
	if r.reservations == nil {
		r.reservations = make(map[_ReservationKey]_Reservation)
	}
	r.reservations[_ReservationKey{PodUID: podUID, Container: container}] = _Reservation{
		Allocated: allocated,
		DeviceIDs: deviceIDs,
	}
	r.reservationsMutex.Unlock()
	// 预留一写入就通知 ListAndWatch。其他模式的插件可以马上把这些设备标为不可用，无需等待注解触发下一轮协调。
	r.notifyListeners()
}

// reservedDevices 返回指定容器在内存中登记的设备，以及是否找到了预留。
func (r *DevicesReconciler) reservedDevices(podUID types.UID, container string) (workercore.DevicesStatus, bool) {
	r.reservationsMutex.RLock()
	defer r.reservationsMutex.RUnlock()
	got, ok := r.reservations[_ReservationKey{PodUID: podUID, Container: container}]
	return got.Allocated, ok
}

// grantVisibility 只标记该容器已获可见性，下次并发调用即可认领别的容器。它授予的是已有设备的访问权限，
// 不记录设备用量，也无需刷新库存。缺少 UID 或容器名时跳过。
func (r *DevicesReconciler) grantVisibility(podUID types.UID, container string) {
	if podUID == "" || container == "" {
		return
	}
	r.visibilityGrantsMutex.Lock()
	if r.visibilityGrants == nil {
		r.visibilityGrants = sets.New[_ReservationKey]()
	}
	r.visibilityGrants.Insert(_ReservationKey{PodUID: podUID, Container: container})
	r.visibilityGrantsMutex.Unlock()
}

// revokeVisibility 撤销尚未产生有效响应的可见性授权。这样 kubelet 重试仍能匹配原容器，
// 而不会误认另一个 Pod 的边车。
func (r *DevicesReconciler) revokeVisibility(podUID types.UID, container string) {
	r.visibilityGrantsMutex.Lock()
	r.visibilityGrants.Delete(_ReservationKey{PodUID: podUID, Container: container})
	r.visibilityGrantsMutex.Unlock()
}

// visibilityGranted 查询该容器是否已收到可见性授权。
func (r *DevicesReconciler) visibilityGranted(podUID types.UID, container string) bool {
	r.visibilityGrantsMutex.RLock()
	defer r.visibilityGrantsMutex.RUnlock()
	return r.visibilityGrants.Has(_ReservationKey{PodUID: podUID, Container: container})
}

// pickAcceleratorOwner 从候选中排除自己，再选择名称按字典序最小的容器。正常只有一个持有者；
// 即使记录异常出现多个，内存和注解也会稳定选中同一个。
func pickAcceleratorOwner(candidates []string, self string) (string, bool) {
	var owner string
	for _, name := range candidates {
		if name == self {
			continue
		}
		if owner == "" || name < owner {
			owner = name
		}
	}
	return owner, owner != ""
}

// reservedAcceleratorDevices 返回同一 Pod 中另一个容器预留的设备及其名称。需要进一步按持有者查记录时，
// 可使用这个名称以保持一致。
func (r *DevicesReconciler) reservedAcceleratorDevices(
	podUID types.UID, self string,
) (workercore.DevicesStatus, string, bool) {
	r.reservationsMutex.RLock()
	defer r.reservationsMutex.RUnlock()
	var candidates []string
	for k := range r.reservations {
		if k.PodUID != podUID {
			continue
		}
		candidates = append(candidates, k.Container)
	}
	owner, ok := pickAcceleratorOwner(candidates, self)
	if !ok {
		return workercore.DevicesStatus{}, "", false
	}
	return r.reservations[_ReservationKey{PodUID: podUID, Container: owner}].Allocated, owner, true
}

// reservationsFor 以容器名为键，返回指定 Pod 当前的所有内存预留。
func (r *DevicesReconciler) reservationsFor(podUID types.UID) map[string]_Reservation {
	r.reservationsMutex.RLock()
	defer r.reservationsMutex.RUnlock()
	var held map[string]_Reservation
	for k, v := range r.reservations {
		if k.PodUID != podUID {
			continue
		}
		if held == nil {
			held = make(map[string]_Reservation, 1)
		}
		held[k.Container] = v
	}
	return held
}

// releaseReservation 删除指定容器的内存预留。写 Pod 注解失败时需立即归还；
// 缺少注解会使删除 Pod 的监听无法触发清理，其他模式可能一直把这张卡视作已占用。
func (r *DevicesReconciler) releaseReservation(podUID types.UID, container string) {
	r.reservationsMutex.Lock()
	delete(r.reservations, _ReservationKey{PodUID: podUID, Container: container})
	r.reservationsMutex.Unlock()
	// 与 reserveDevices 对应，回滚后立即通知各插件刷新设备健康状态，使其他模式可以再次选择此卡。
	r.notifyListeners()
}

// reservedPhysicalOccupied 按物理卡收集内存预留的分区位置。
// 上一次 Allocate 解锁前写入的预留可以补足 LivePhysicalOccupied 的注解同步延迟。
// 两者取并集判断区间是否重叠，同一区间出现两次也无妨。
func (r *DevicesReconciler) reservedPhysicalOccupied() Placements {
	r.reservationsMutex.RLock()
	defer r.reservationsMutex.RUnlock()
	occupied := make(Placements)
	allocated := make(map[Resource]map[string]int32)
	for _, v := range r.reservations {
		accumulatePhysicalOccupied(v.Allocated, occupied, allocated)
	}
	return occupied
}

// reservedLogicalOccupied 收集内存中已经预留、但注解可能还看不到的逻辑区间。
// 同批请求由此避开彼此的切片窗口。
func (r *DevicesReconciler) reservedLogicalOccupied() Placements {
	r.reservationsMutex.RLock()
	defer r.reservationsMutex.RUnlock()
	occupied := make(Placements)
	for _, v := range r.reservations {
		accumulateLogicalOccupied(v.Allocated, occupied)
	}
	return occupied
}

// reservedModeForResource 按设备组和设备 ID 返回卡在内存中已被哪种模式、哪个 Pod 占用。
// Allocate 同步写预留，即使 Devices 状态尚未更新也能检查冲突。无人持有时返回 None 和空 UID。
func (r *DevicesReconciler) reservedModeForResource(group, device string) (workercore.DeviceAllocationMode, types.UID) {
	r.reservationsMutex.RLock()
	defer r.reservationsMutex.RUnlock()
	for key, reservation := range r.reservations {
		status := reservation.Allocated
		for i := range status.Groups {
			grp := &status.Groups[i]
			if grp.ID != group {
				continue
			}
			for j := range grp.Accelerators {
				acc := &grp.Accelerators[j]
				if acc.ID == device && acc.Mode != workercore.DeviceAllocationModeNone {
					return acc.Mode, key.PodUID
				}
			}
		}
	}
	return workercore.DeviceAllocationModeNone, ""
}

// pruneReservations 根据 Reconcile 收集的存活 Pod UID 清理预留，避免设备在 Pod 删除后仍被占用。
func (r *DevicesReconciler) pruneReservations(livePodUIDs []string) {
	r.reservationsMutex.Lock()
	defer r.reservationsMutex.Unlock()
	if len(r.reservations) == 0 {
		return
	}
	live := livePodUIDSet(livePodUIDs)
	for key := range r.reservations {
		if !live.Has(key.PodUID) {
			delete(r.reservations, key)
		}
	}
}

// pruneVisibilityGrants 用同一批存活 UID 清理已消失 Pod 的可见性授权。
func (r *DevicesReconciler) pruneVisibilityGrants(livePodUIDs []string) {
	r.visibilityGrantsMutex.Lock()
	defer r.visibilityGrantsMutex.Unlock()
	if r.visibilityGrants.Len() == 0 {
		return
	}
	live := livePodUIDSet(livePodUIDs)
	for key := range r.visibilityGrants {
		if !live.Has(key.PodUID) {
			r.visibilityGrants.Delete(key)
		}
	}
}

// livePodUIDSet 将这次遍历得到的 Pod UID 转成方便查询的集合。
func livePodUIDSet(livePodUIDs []string) sets.Set[types.UID] {
	live := sets.New[types.UID]()
	for i := range livePodUIDs {
		live.Insert(types.UID(livePodUIDs[i]))
	}
	return live
}

// pendingReleaseRetryPeriod 是补偿写入失败后的等待时间。设备在写入成功前仍显示被占用，因此要较快重试，
// 同时给 I/O 留出恢复时间。
const pendingReleaseRetryPeriod = 5 * time.Second

// recordPendingRelease 记录需要由下一轮 Reconcile 重试的注解写入。prior 保存要恢复的旧分配，
// nil 表示删除该容器条目；缺少 UID 或容器名时跳过。
func (r *DevicesReconciler) recordPendingRelease(
	podUID types.UID, container string, prior *ContainerAllocation,
) {
	if podUID == "" || container == "" {
		return
	}
	r.pendingReleasesMutex.Lock()
	if r.pendingReleases == nil {
		r.pendingReleases = make(map[_ReservationKey]*ContainerAllocation, 1)
	}
	wasEmpty := len(r.pendingReleases) == 0
	r.pendingReleases[_ReservationKey{PodUID: podUID, Container: container}] = prior
	r.pendingReleasesMutex.Unlock()

	// 第一条待归还记录到来时主动唤醒控制器，因为失败的写入不会产生变更事件。此后的失败由重新排队控制间隔，
	// 避免每次失败都立即把自己唤醒。
	if !wasEmpty {
		return
	}
	select {
	case r.pendingReleaseEvents <- ctrlevent.GenericEvent{}:
	default:
	}
}

// dropPendingRelease 清除已经成功或所属 Pod 已消失的补偿请求。若同一容器又成功获得新分配，也要作废旧请求，
// 否则重试旧归还操作会撤销有效的新分配。
func (r *DevicesReconciler) dropPendingRelease(podUID types.UID, container string) {
	r.pendingReleasesMutex.Lock()
	delete(r.pendingReleases, _ReservationKey{PodUID: podUID, Container: container})
	r.pendingReleasesMutex.Unlock()
}

// hasPendingReleases 查询是否仍有需要重试的补偿写入。
func (r *DevicesReconciler) hasPendingReleases() bool {
	r.pendingReleasesMutex.RLock()
	defer r.pendingReleasesMutex.RUnlock()
	return len(r.pendingReleases) > 0
}

// pendingReleaseKeySet 只复制待处理容器的键，使后续 I/O 在锁外执行。真正写出之前再读当前值，
// 避免覆盖中途新登记的分配。
func (r *DevicesReconciler) pendingReleaseKeySet() sets.Set[_ReservationKey] {
	r.pendingReleasesMutex.RLock()
	defer r.pendingReleasesMutex.RUnlock()
	if len(r.pendingReleases) == 0 {
		return nil
	}
	return sets.KeySet(r.pendingReleases)
}

// peekPendingRelease 在准备写入时重读指定容器的补偿项。其他请求若已用新分配替代它，这里就会发现无需操作。
// 条目只在写成功后清除，失败可以留待重试，无需重复登记和唤醒。
func (r *DevicesReconciler) peekPendingRelease(
	podUID types.UID, container string,
) (*ContainerAllocation, bool) {
	r.pendingReleasesMutex.RLock()
	defer r.pendingReleasesMutex.RUnlock()
	prior, ok := r.pendingReleases[_ReservationKey{PodUID: podUID, Container: container}]
	return prior, ok
}

// retryPendingReleases 使用本轮已有的 Pod 列表重试补偿，找不到原 UID 时删除待办。
// 同名重建的 Pod 拥有新 UID，绝不能撤销它的分配。
//
// 每次写入前重读待办，防止漫长的多次 API 调用期间误撤新分配。这可以识别已经被取代的待办；
// 注解读改写本身仍可能与并发补丁竞争。
func (r *DevicesReconciler) retryPendingReleases(
	ctx context.Context, logger logr.Logger, pods []core.Pod, livePodUIDs []string,
) {
	pending := r.pendingReleaseKeySet()
	if pending.Len() == 0 {
		return
	}

	live := livePodUIDSet(livePodUIDs)
	for key := range pending {
		if !live.Has(key.PodUID) {
			r.dropPendingRelease(key.PodUID, key.Container)
			pending.Delete(key)
		}
	}

	for i := range pods {
		pod := &pods[i]
		for key := range pending {
			if key.PodUID != pod.UID {
				continue
			}
			pending.Delete(key)
			prior, ok := r.peekPendingRelease(key.PodUID, key.Container)
			if !ok {
				continue
			}
			if err := r.unpatchAllocatingPod(ctx, pod, key.Container, prior); err != nil {
				logger.Error(err, "give back a refused allocation; its accelerator keeps reading as "+
					"held until this lands or the pod is gone",
					"pod", ctrlcli.ObjectKeyFromObject(pod), "container", key.Container)
				break
			}
			r.dropPendingRelease(key.PodUID, key.Container)
			// 每轮对同一个 Pod 最多写一次。补丁不会更新 informer 已返回的共享对象，
			// 从旧副本再次写入可能把刚删除的条目加回来。剩下的待办在下轮读取新缓存后继续。
			break
		}
	}
}

const (
	// AllocatedAcceleratorAnnoKey 用于标识携带 Pod 每个容器加速器分配信息的注释，属于 PodAllocations
	// 的 JSON 数据的一部分。这是容器所包含内容的唯一持久记录——进程内的预留不会在设备管理器重启后保留，
	// 因此账本的重建、资源回收流程以及 Pod 删除的监控都依赖于这一记录。
	AllocatedAcceleratorAnnoKey       = "device.gpustack.ai/accelerator.allocated"
	_PreferredAcceleratorIDAnnoKey    = "device.gpustack.ai/accelerator.preferred-id"
	_PreferredAcceleratorIndexAnnoKey = "device.gpustack.ai/accelerator.preferred-index"
)

// _AllocationMatch 描述这次 Allocate 或选卡建议可能对应哪个待分配容器。设备插件 RPC 没有 Pod 身份，
// 因此需要从节点的候选中查找。
type _AllocationMatch struct {
	// RPC 直接提供的线索只有两项，即正在申请的资源名和 kubelet 为这个容器送来的设备 ID 数量。
	ResourceName core.ResourceName
	Quantity     resource.Quantity
	// SkipReserved 跳过已经认领的 Pod 和容器，避免多个相同请求都匹配到最早的那个。可见性调用保留这些候选，
	// 才能找回主容器有预留的 Pod。
	SkipReserved bool
	// SkipGranted 跳过已经分得可见性的容器。边车不预留新的加速器，因此要单独记授权，
	// 否则同批相同 Pod 的边车会反复匹配到最早的那个。
	SkipGranted bool
	// Feasible 使用 RPC 没有携带的信息区分候选，例如分区规格或切片单卡预算。若所有候选都不合适，
	// 仍在原候选中识别容器；选定后，再由 rejectSlicedOvercommit 等逻辑按同一份占用信息决定能否真正分配。
	Feasible func(pod *core.Pod, ctr *core.Container) bool
	// Kubelet 记录当前仍在等待这类设备的容器。能取得 kubelet 的记录时优先据此匹配；无法查询时为 nil。
	Kubelet *_KubeletPending
}

func (r *DevicesReconciler) getAllocatingPodWithRetry(
	ctx context.Context, match _AllocationMatch,
) (pod *core.Pod, ctr *core.Container, err error) {
	for i := 0; i < 5; i++ {
		pod, ctr, err = r.getAllocatingPod(ctx, match)
		if err == nil {
			return pod, ctr, nil
		}
		time.Sleep(3 * time.Second)
	}
	return nil, nil, fmt.Errorf("get allocating pod with retry: %w", err)
}

// _AllocatingCandidate 保存一个待分配容器及其所属 Pod，供 getAllocatingPod 分类和排序。
type _AllocatingCandidate struct {
	pod *core.Pod
	ctr *core.Container
}

// getAllocatingPod 为 Allocate 或选卡提示寻找对应容器。先按 kubelet 的待分配记录缩小范围，
// 再从最高优先级的非空类别中选最早进入的 Pending Pod。优先级依次为尚未认领且可满足、尚未认领但不可满足、
// 已有预留或授权。最后一种允许 kubelet 在收到响应却未写好检查点时重放原分配。
func (r *DevicesReconciler) getAllocatingPod(
	ctx context.Context, match _AllocationMatch,
) (*core.Pod, *core.Container, error) {
	podList := new(core.PodList)
	err := r.Client.List(ctx, podList,
		ctrlcli.MatchingFields{IndexingPodsByNodeName: r.NodeName},
		ctrlcli.UnsafeDisableDeepCopy)
	if err != nil {
		return nil, nil, fmt.Errorf("list pods with node name: %w", err)
	}
	if len(podList.Items) == 0 {
		return nil, nil, fmt.Errorf("no pods found with node name %s", r.NodeName)
	}

	slices.SortFunc(podList.Items, func(a, b core.Pod) int {
		return a.CreationTimestamp.Compare(b.CreationTimestamp.Time)
	})

	logger := ctrllog.FromContext(ctx)
	feasible, infeasible, claimed := r.classifyAllocatingCandidates(podList.Items, match)
	feasible, infeasible, claimed = narrowToKubeletPending(logger, match, feasible, infeasible, claimed)

	return rankAllocatingCandidate(logger, match, feasible, infeasible, claimed)
}

// narrowToKubeletPending 在每个优先级里只保留 kubelet 仍在等待的候选。kubelet 逐个接纳 Pod，
// 通常只会留下一个，即使它比其他 Pod 新也应选中。若 kubelet 的记录与缓存不相交，先拒绝并等待缓存追上，
// 避免将设备写到错误的 Pod。缓存完全没有候选时，由调用方稍后重试。
func narrowToKubeletPending(
	logger logr.Logger, match _AllocationMatch,
	feasible, infeasible, claimed []_AllocatingCandidate,
) ([]_AllocatingCandidate, []_AllocatingCandidate, []_AllocatingCandidate) {
	if match.Kubelet == nil || len(feasible)+len(infeasible)+len(claimed) == 0 {
		return feasible, infeasible, claimed
	}
	waiting := func(tier []_AllocatingCandidate) []_AllocatingCandidate {
		return slices.DeleteFunc(slices.Clone(tier), func(c _AllocatingCandidate) bool {
			return !match.Kubelet.waitsForDevice(c.pod, c.ctr)
		})
	}
	narrowedFeasible, narrowedInfeasible, narrowedClaimed := waiting(feasible), waiting(infeasible), waiting(claimed)

	switch n := len(narrowedFeasible) + len(narrowedInfeasible) + len(narrowedClaimed); {
	case n == 0:
		logger.Info("kubelet reports no pending candidate as waiting for this resource; refusing to guess the allocating container",
			"resource", match.ResourceName,
			"candidates", len(feasible)+len(infeasible)+len(claimed))
		return nil, nil, nil
	case n > 1:
		logger.Info("kubelet reports several candidates as waiting for this resource; "+
			"choosing among them by the pending-Pod heuristic",
			"resource", match.ResourceName, "candidates", n)
	}
	return narrowedFeasible, narrowedInfeasible, narrowedClaimed
}

// classifyAllocatingCandidates 按从早到晚的顺序检查本节点的 Pending Pod，将申请匹配资源的容器分为可满足、
// 不可满足、已认领三组。
func (r *DevicesReconciler) classifyAllocatingCandidates(
	pods []core.Pod, match _AllocationMatch,
) (feasible, infeasible, claimed []_AllocatingCandidate) {
	classify := func(pod *core.Pod, ctr *core.Container) {
		if !containerRequests(ctr, match.ResourceName, match.Quantity) {
			return
		}
		// 用即时的设备预留或可见性授权识别已经处理的容器。即使 informer 尚未更新，
		// 节点锁内的后续调用也能看到上一笔认领。
		if match.SkipReserved {
			if _, ok := r.reservedDevices(pod.UID, ctr.Name); ok {
				claimed = append(claimed, _AllocatingCandidate{pod, ctr})
				return
			}
		}
		if match.SkipGranted && r.visibilityGranted(pod.UID, ctr.Name) {
			claimed = append(claimed, _AllocatingCandidate{pod, ctr})
			return
		}
		if match.Feasible != nil && !match.Feasible(pod, ctr) {
			infeasible = append(infeasible, _AllocatingCandidate{pod, ctr})
			return
		}
		feasible = append(feasible, _AllocatingCandidate{pod, ctr})
	}

	for i := range pods {
		pod := &pods[i]
		if p := pod.Status.Phase; p != "" && p != core.PodPending {
			continue
		}
		for j := range pod.Spec.InitContainers {
			classify(pod, &pod.Spec.InitContainers[j])
		}
		for j := range pod.Spec.Containers {
			classify(pod, &pod.Spec.Containers[j])
		}
	}

	return feasible, infeasible, claimed
}

// rankAllocatingCandidate 优先从可满足组中选最早的容器，其次尝试不可满足组，最后才重放已有分配。
// 发生回退时记录原因。
func rankAllocatingCandidate(
	logger logr.Logger, match _AllocationMatch,
	feasible, infeasible, claimed []_AllocatingCandidate,
) (*core.Pod, *core.Container, error) {
	switch {
	case len(feasible) > 0:
		return feasible[0].pod, feasible[0].ctr, nil
	case len(infeasible) > 0:
		logger.Info("no candidate container can be served by this allocation; "+
			"resolving to the oldest one anyway rather than failing a resolvable request",
			"resource", match.ResourceName, "candidates", len(infeasible),
			"pod", ctrlcli.ObjectKeyFromObject(infeasible[0].pod), "container", infeasible[0].ctr.Name)
		return infeasible[0].pod, infeasible[0].ctr, nil
	case len(claimed) > 0:
		logger.Info("every candidate container was already answered; replaying the oldest one's "+
			"allocation, which a kubelet that lost its checkpoint is asking for again",
			"resource", match.ResourceName, "candidates", len(claimed),
			"pod", ctrlcli.ObjectKeyFromObject(claimed[0].pod), "container", claimed[0].ctr.Name)
		return claimed[0].pod, claimed[0].ctr, nil
	}

	return nil, nil, fmt.Errorf("cannot find pending pod with resource request %s=%s",
		match.ResourceName, match.Quantity.String())
}

// containerRequests 检查容器申请的资源名和数量是否正好匹配。
func containerRequests(ctr *core.Container, resName core.ResourceName, resQuantity resource.Quantity) bool {
	q, ok := ctr.Resources.Limits[resName]
	return ok && q.Equal(resQuantity)
}

type (
	// ContainerAllocation 是指设备插件为 Pod 中的一个容器所分配的资源。
	//
	// DeviceIDs 是 kubelet 为该容器分配资源所提供的设备 `ID`。这些 `ID` 是记录下来的，而非重新推导出
	// 来的，因为那些分配资源时直接选择使用加速器设备的设备家族会返回那些没有对应 `ID` 的设备。此外，
	// 只要分配过程持续进行，这些 `ID` 就必须保持为“健康”状态：kubelet 不会重新接纳那些检查点 `ID` 已
	// 离开健康状态的容器。
	ContainerAllocation struct {
		Devices   workercore.DevicesStatus `json:"devices"`
		DeviceIDs []string                 `json:"deviceIDs,omitempty"`
	}

	// PodAllocations 是 AllocatedAcceleratorAnnoKey 注释的值，以容器名称作为键。容器维度决定了第二次
	// 分配是否会覆盖第一个容器的权益，同时也决定了某个容器中的重复分配是否会覆盖其自身条目，从而避免
	// 对加速器进行重复计数。
	PodAllocations map[string]ContainerAllocation
)

// initDesiredStatus 给库存 Spec 中的每张卡建立相应的状态行。先设为 None 和整卡满额，
// 随后再从 Pod 注解扣除占用。
func initDesiredStatus(devs *workercore.Devices) workercore.DevicesStatus {
	desiredStatus := workercore.DevicesStatus{
		Groups: make([]workercore.DevicesAllocationGroup, 0, len(devs.Spec.Groups)),
	}
	for i := range devs.Spec.Groups {
		specGrp := &devs.Spec.Groups[i]
		statusGrp := workercore.DevicesAllocationGroup{
			ID:           specGrp.ID,
			Manufacturer: specGrp.Manufacturer,
			Accelerators: make([]workercore.AcceleratorAllocation, 0, len(specGrp.Accelerators)),
		}
		for j := range specGrp.Accelerators {
			acc := &specGrp.Accelerators[j]
			statusGrp.Accelerators = append(statusGrp.Accelerators, workercore.AcceleratorAllocation{
				ID:        acc.ID,
				Index:     acc.Index,
				Mode:      workercore.DeviceAllocationModeNone,
				Remaining: nodefeature.ResourceMaxUnits,
			})
		}
		desiredStatus.Groups = append(desiredStatus.Groups, statusGrp)
	}
	return desiredStatus
}

// foldStep 指明处理某个 Pod 时在哪一步失败。读取失败会漏掉这个 Pod 的占用；合并时遇到模式冲突则须报告冲突，
// 普通重建按出错步骤分别记录。
type foldStep int

const (
	// readFoldStep 表示解码单个 Pod 的分配记录。
	readFoldStep foldStep = iota
	// mergeFoldStep 表示把已解码的分配合并进账本。
	mergeFoldStep
)

// foldPodAllocations 以调用方传入的空账为起点，逐个合并 Pod 注解中的占用，在同一轮计算硬件分区的规格余量，
// 并收集供工作目录回收使用的存活 UID。
//
// onFailure 决定如何处理某个 Pod 出错。返回 true 会记录错误并继续，返回 false 会停止并交回错误。
// 普通重建要能继续回答 kubelet；缺少可读记录的卡可能因此显示空闲。需要确认设备已归还的调用方必须获知错误，
// 不能从一份漏记持有者的账本推断设备已释放。
func foldPodAllocations(
	devs *workercore.Devices,
	podList *core.PodList,
	desiredStatus workercore.DevicesStatus,
	onFailure func(step foldStep, pod *core.Pod, err error) (keepFolding bool),
) (workercore.DevicesStatus, []string, error) {
	// 两张表按物理卡收集注解中已经占用的分区区间、各规格已建的实例数。下方据此计算余量，无需访问设备。
	physicalOccupied := make(Placements)
	physicalAllocated := make(map[Resource]map[string]int32)
	livePodUIDs := make([]string, 0, len(podList.Items))

	for i := range podList.Items {
		pod := &podList.Items[i]
		// 正在删除的 Pod 也计入存活集合和账本。宽限期内容器可能还在运行，工作目录和硬件分区也可能仍被占用。
		// 已经终止的 Pod 也保留在存活集合，由 heldAllocation 决定其哪些资源已实际归还。
		livePodUIDs = append(livePodUIDs, string(pod.UID))

		podStatus, err := allocatedStatusOf(pod)
		if err != nil {
			readErr := fmt.Errorf("pod %s: read the allocation it holds: %w",
				ctrlcli.ObjectKeyFromObject(pod), err)
			if !onFailure(readFoldStep, pod, readErr) {
				return workercore.DevicesStatus{}, nil, readErr
			}
			continue
		}
		podStatus = heldAllocation(pod, podStatus)

		// 将合并器的错误交给调用方，避免漏报同一张卡被不同模式持有的冲突。
		// 允许继续的调用方沿用 applyAllocatedStatus 随错误返回的状态。
		mergedStatus, err := applyAllocatedStatus(podStatus, desiredStatus)
		if err != nil {
			mergeErr := fmt.Errorf("pod %s: merge its allocation: %w",
				ctrlcli.ObjectKeyFromObject(pod), err)
			if !onFailure(mergeFoldStep, pod, mergeErr) {
				return workercore.DevicesStatus{}, nil, mergeErr
			}
			continue
		}
		desiredStatus = mergedStatus

		accumulatePhysicalOccupied(podStatus, physicalOccupied, physicalAllocated)
	}

	// 在同一次状态重建中计算各分区规格的账本，避免再写一次 Status 时相互覆盖。
	// foldPhysicalLedger 根据注解还原的占用和探测时得到的合法区间进行计算。
	foldPhysicalLedger(devs, &desiredStatus, physicalOccupied, physicalAllocated)

	return desiredStatus, livePodUIDs, nil
}

// IMPORTANT: 严格模式账本折叠入口
//
// mergePodAllocationsStrict 执行失败即止（fail-closed）的严格重建：共享折叠过程报告的任意错误
// 都会立即中止重建并返回，从而在输入不完整时阻塞操作，避免误判放行。
func mergePodAllocationsStrict(
	devs *workercore.Devices, podList *core.PodList,
) (workercore.DevicesStatus, []string, error) {
	return foldPodAllocations(devs, podList, initDesiredStatus(devs), stopAtFoldFailure)
}

// stopAtFoldFailure 中止共享折叠并将折叠构造的错误返回给调用方，用于阻止严格重建返回因失败而残缺的账本。
func stopAtFoldFailure(_ foldStep, _ *core.Pod, _ error) bool { return false }

// mergePodAllocations 执行尽可能完整的普通重建。它的错误处理始终要求继续，
// 所以可忽略公共合并函数用于中途退出的附加结果。
func mergePodAllocations(
	logger logr.Logger, devs *workercore.Devices, podList *core.PodList, desiredStatus workercore.DevicesStatus,
) (workercore.DevicesStatus, []string) {
	mergedStatus, livePodUIDs, _ := foldPodAllocations(devs, podList, desiredStatus, absorbFoldFailure(logger))

	return mergedStatus, livePodUIDs
}

// absorbFoldFailure 记录当前 Pod 的合并错误，并让普通重建继续，以便仍能用这轮账本响应 kubelet。
func absorbFoldFailure(logger logr.Logger) func(step foldStep, pod *core.Pod, err error) bool {
	return func(step foldStep, pod *core.Pod, err error) bool {
		if step == readFoldStep {
			// 读不出分配记录时，普通重建会漏记该 Pod 的设备，实际有人使用的卡可能显示空闲。因此要明确记录风险。
			// 常见原因是旧设备管理器写入了当前版本无法读取的格式，跨此类版本升级前应先排空节点。
			logger.Error(err, "cannot read the allocation this pod holds; its cards will be "+
				"reported free while it still occupies them — drain the node before upgrading "+
				"the device manager",
				"pod", ctrlcli.ObjectKeyFromObject(pod))
			return true
		}

		logger.Error(err, "merge allocated accelerators into devices status",
			"pod", ctrlcli.ObjectKeyFromObject(pod))

		return true
	}
}

// notifyListeners 将最近一轮存活 Pod UID 广播给 ListAndWatch。协调、即时预留和归还都会触发它，
// 使其他模式及时看到设备的健康状态变化。发送不阻塞，第一次存活扫描前也不发送，
// 避免目录回收器把尚未初始化的 nil 当作没有活跃 Pod。
func (r *DevicesReconciler) notifyListeners() {
	r.notifiersMutex.Lock()
	defer r.notifiersMutex.Unlock()
	if r.lastLivePodUIDs == nil {
		return
	}
	for i := range r.notifiers {
		notifier := &r.notifiers[i]
		select {
		case notifier.Channel <- r.lastLivePodUIDs:
		default:
		}
	}
}

// IndexingPodsByNodeName 是字段索引器键 SetupController 为 Pods 注册的关键字，以 Spec.NodeName 为索引。此
// 包中每个 Pod 的查找都会通过它进行，而不是扫描整个集群的 Pods，因此无论集群包含多少其他节点，针对一个节
// 点的查找都较为便宜。
const IndexingPodsByNodeName = "pods.nodeName"

// SetupController 会注册 Pod-by-node-name 字段索引，从 KUBERNETES_NODE_NAME 环境变量中解析当前节点的名称，
// 并连接管理器的控制器：它会监控该节点的 v1alpha1.Devices 对象以及所有安排在该节点上的 Pod，这些 Pod 带有
// AllocatedAcceleratorAnnoKey 注释，每次相关变更时都会为该节点注册一个协调项。在管理器启动之前调用它一次，
// 这样它注册的字段索引就能在缓存开始同步之前就设置完成。
func (r *DevicesReconciler) SetupController(ctx context.Context, opts controller.SetupOptions) error {
	// 配置字段索引器。
	fi := opts.Manager.GetFieldIndexer()
	err := fi.IndexField(ctx, &core.Pod{}, IndexingPodsByNodeName,
		func(obj ctrlcli.Object) []string {
			if obj == nil {
				return nil
			}
			pod := obj.(*core.Pod)
			return []string{pod.Spec.NodeName}
		})
	if err != nil {
		return fmt.Errorf("index pod '%s': %w", IndexingPodsByNodeName, err)
	}

	// 在控制器监控之前以及任何分配操作能够记录之前，用于记录补偿写入的唤醒通道已设置好。
	r.pendingReleaseEvents = make(chan ctrlevent.GenericEvent, 1)

	r.NodeName = osx.Getenv("KUBERNETES_NODE_NAME")
	r.Client = opts.Manager.GetClient()
	if osx.Getenv(IdentifyByKubeletEnv) != "false" {
		r.kubeletPods = newKubeletPodLister(KubeletPodResourcesSocket)
	}
	r.slicedAllocateGateOff = osx.Getenv(SlicedAllocateGateEnv) == "false"

	return ctrl.NewControllerManagedBy(opts.Manager).
		Named("deviceplugin.manage.devices").
		For(
			&workercore.Devices{},
			ctrlbuilder.WithPredicates(
				// 对当前节点对应的 Devices 对象感兴趣。
				ctrlpredicate.NewPredicateFuncs(func(object ctrlcli.Object) bool {
					return object.GetName() == r.NodeName
				}),
			),
		).
		Watches(
			// 查看 Kubernetes Pods，并将对应的 v1.Devices 加入队列。
			&core.Pod{},
			ctrlhandler.EnqueueRequestsFromMapFunc(
				r.enqueueDevicesWhenPodChanged,
			),
			ctrlbuilder.WithPredicates(
				// 当 Pod 被安排到当前节点并且带有 AllocatedAcceleratorAnnoKey 注解时，触
				// 发协调。
				ctrlpredicate.NewPredicateFuncs(func(object ctrlcli.Object) bool {
					pod := object.(*core.Pod)
					return pod.Spec.NodeName == r.NodeName
				}),
				ctrlpredicate.Funcs{
					CreateFunc: func(e ctrlevent.CreateEvent) bool {
						return kubemeta.HasAnnotation(e.Object, AllocatedAcceleratorAnnoKey)
					},
					DeleteFunc: func(e ctrlevent.DeleteEvent) bool {
						return kubemeta.HasAnnotation(e.Object, AllocatedAcceleratorAnnoKey)
					},
					UpdateFunc: podUpdateChangesLedger,
				},
			),
		).
		WatchesRawSource(
			// 无法成功落地的补偿写入操作将从 Reconcile 重新尝试，因此记录该操作必须由协调器自
			// 行将其恢复——关于为何不进行监视，请参考 pendingReleaseEvents。
			ctrlsource.Channel(
				r.pendingReleaseEvents,
				ctrlhandler.EnqueueRequestsFromMapFunc(r.enqueueDevicesForThisNode),
			),
		).
		Complete(r)
}

// enqueueDevicesForThisNode 会将该节点的 Devices 加入队列，无论携带何种事件。它充当唤醒通道，其事件名称并
// 不属于它们自身：它们所表达的只是“该节点有工作”，即一个请求。
func (r *DevicesReconciler) enqueueDevicesForThisNode(
	context.Context, ctrlcli.Object,
) []ctrlreconcile.Request {
	return []ctrlreconcile.Request{
		{
			NamespacedName: ctrlcli.ObjectKey{
				Name: r.NodeName,
			},
		},
	}
}

func (r *DevicesReconciler) enqueueDevicesWhenPodChanged(
	ctx context.Context, obj ctrlcli.Object,
) []ctrlreconcile.Request {
	logger := ctrllog.FromContext(ctx).
		WithValues("pod", ctrlcli.ObjectKeyFromObject(obj))

	pod := obj.(*core.Pod)

	// 作为安全检查，确保 Pod 被调度到当前节点。
	if pod.Spec.NodeName != r.NodeName {
		logger.Error(nil, "pod is not scheduled to the current node")
		return nil
	}

	reqs := []ctrlreconcile.Request{
		{
			NamespacedName: ctrlcli.ObjectKey{
				Name: pod.Spec.NodeName,
			},
		},
	}
	logger.V(2).Info("enqueued devices for pod", "requests", reqs)
	return reqs
}

// getReconcileNotifier 会订阅 live-pod-UID 广播，并返回用于读取的通道以及取消订阅的函数。每个订阅者必须在
// 停止读取时调用第二个返回值：订阅的生命周期长于其读取者，而广播会在通知器互斥锁下遍历整个集合——其中一些
// 数据来自节点分配的互斥锁。
func (r *DevicesReconciler) getReconcileNotifier(
	manufacturer string, allocationMode workercore.DeviceAllocationMode,
) (<-chan []string, func()) {
	r.notifiersMutex.Lock()
	defer r.notifiersMutex.Unlock()

	channel := make(chan []string, 4)
	r.notifiers = append(r.notifiers, _DevicesNotifier{
		Manufacturer:   manufacturer,
		AllocationMode: allocationMode,
		Channel:        channel,
	})
	// 向延迟订阅者发送最新的广播内容，以确保其重新获取循环永远不会处于未订阅状态。频道已重新缓冲，因
	// 此此发送操作不会出现阻塞。
	if r.lastLivePodUIDs != nil {
		channel <- r.lastLivePodUIDs
	}
	return channel, func() {
		r.dropReconcileNotifier(channel)
	}
}

// dropReconcileNotifier 会取消由其处理的通道所标识的订阅。多次调用是安全的：已失效的通道不会产生任何影响。
func (r *DevicesReconciler) dropReconcileNotifier(channel chan []string) {
	r.notifiersMutex.Lock()
	defer r.notifiersMutex.Unlock()

	r.notifiers = slices.DeleteFunc(r.notifiers, func(n _DevicesNotifier) bool {
		return n.Channel == channel
	})
}

func (r *DevicesReconciler) getDevices(ctx context.Context) (*workercore.Devices, error) {
	devs := &workercore.Devices{
		ObjectMeta: meta.ObjectMeta{
			Name: r.NodeName,
		},
	}
	err := r.Client.Get(ctx, ctrlcli.ObjectKeyFromObject(devs), devs,
		&ctrlcli.GetOptions{
			Raw: &meta.GetOptions{
				ResourceVersion: "0",
			},
		})
	return devs, err
}

// Aggregate 会将每个容器的记录汇总到账本所消耗的 Pod 级分配中。包含两个相同加速器的容器会保留各自的条目，
// 因此加速器会为这两个容器分别计费。容器按名称顺序访问，因此结果不依赖于 map 迭代。
func (in PodAllocations) Aggregate() workercore.DevicesStatus {
	names := make([]string, 0, len(in))
	for name := range in {
		names = append(names, name)
	}
	sort.Strings(names)

	var (
		aggregated workercore.DevicesStatus
		groupIndex = make(map[string]int)
	)
	for _, name := range names {
		groups := in[name].Devices.Groups
		for i := range groups {
			src := &groups[i]
			idx, ok := groupIndex[src.ID]
			if !ok {
				idx = len(aggregated.Groups)
				groupIndex[src.ID] = idx
				aggregated.Groups = append(aggregated.Groups, workercore.DevicesAllocationGroup{
					ID:           src.ID,
					Manufacturer: src.Manufacturer,
				})
			}
			dst := &aggregated.Groups[idx]
			dst.Accelerators = append(dst.Accelerators, src.Accelerators...)
		}
	}
	return aggregated
}

// AllocatedAcceleratorsOf 读取 Pod 的每容器分配记录。未标注的 Pod 会返回一个空映射，且不会引发错误。
func AllocatedAcceleratorsOf(pod *core.Pod) (PodAllocations, error) {
	str := pod.Annotations[AllocatedAcceleratorAnnoKey]
	if str == "" {
		return nil, nil
	}
	var allocations PodAllocations
	if err := json.Unmarshal(stringx.ToBytes(&str), &allocations); err != nil {
		return nil, err
	}
	return allocations, nil
}

// AllocatedAcceleratorGroupsOf 合并 Pod 各容器的设备分配记录，供 Instance 状态报告实际获得的设备组。
// 没有记录或注解无法解析时返回 nil。
func AllocatedAcceleratorGroupsOf(pod *core.Pod) []workercore.DevicesAllocationGroup {
	allocations, err := AllocatedAcceleratorsOf(pod)
	if err != nil {
		return nil
	}
	return allocations.Aggregate().Groups
}

// patchAllocatingPod 将单个容器的分配写入 Pod 注解，同一容器重试时覆盖自己的记录。注解补丁会替换整个值，
// 因此先读取缓存注解，再补上本进程已知的其他容器预留，防止缓存滞后导致兄弟容器的分配丢失。
func (r *DevicesReconciler) patchAllocatingPod(
	ctx context.Context, pod *core.Pod, container string,
	allocatedStatus workercore.DevicesStatus, deviceIDs []string,
) error {
	allocations, err := AllocatedAcceleratorsOf(pod)
	if err != nil {
		return fmt.Errorf("read allocated accelerators: %w", err)
	}
	if allocations == nil {
		allocations = make(PodAllocations, 1)
	}
	for name, reserved := range r.reservationsFor(pod.UID) {
		if name == container {
			continue
		}
		allocations[name] = ContainerAllocation{
			Devices:   reserved.Allocated,
			DeviceIDs: reserved.DeviceIDs,
		}
	}
	allocations[container] = ContainerAllocation{
		Devices:   allocatedStatus,
		DeviceIDs: deviceIDs,
	}

	allocationsBytes, err := json.Marshal(allocations)
	if err != nil {
		return fmt.Errorf("marshal allocated accelerators: %w", err)
	}

	obj := map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]any{
				AllocatedAcceleratorAnnoKey: string(allocationsBytes),
			},
		},
	}
	objBytes, err := json.Marshal(obj)
	if err != nil {
		return fmt.Errorf("marshal object meta: %w", err)
	}

	_, err = kubeclientset.PatchWithCtrlClient(ctx, r.Client, pod, types.StrategicMergePatchType, objBytes)
	if err == nil {
		// 新分配成功写入注解后，才可以清除同一容器原先待重试的归还操作。若写入失败，
		// 旧补偿仍负责释放此前错误计入的设备。
		r.dropPendingRelease(pod.UID, container)
	}
	return err
}

// unpatchAllocatingPod 撤销指定容器本次写入的分配。若 prior 非空就还原旧条目，首次申请失败则删除该条目。
// 合并其他容器时也要用缓存注解加本进程的预留，以免整条注解被替换时带走别人的分配。
func (r *DevicesReconciler) unpatchAllocatingPod(
	ctx context.Context, pod *core.Pod, container string, prior *ContainerAllocation,
) error {
	allocations, err := AllocatedAcceleratorsOf(pod)
	if err != nil {
		return fmt.Errorf("read allocated accelerators: %w", err)
	}
	for name, reserved := range r.reservationsFor(pod.UID) {
		if name == container {
			continue
		}
		if allocations == nil {
			allocations = make(PodAllocations, 1)
		}
		allocations[name] = ContainerAllocation{
			Devices:   reserved.Allocated,
			DeviceIDs: reserved.DeviceIDs,
		}
	}
	if prior != nil {
		if allocations == nil {
			allocations = make(PodAllocations, 1)
		}
		allocations[container] = *prior
	} else {
		delete(allocations, container)
	}

	// 最后一条分配移除后，用 nil 值让战略合并补丁删除整个注解键。
	// 遗留空注解会在删除 Pod 时触发没有实际分配可清理的协调。
	var value any
	if len(allocations) > 0 {
		allocationsBytes, marshalErr := json.Marshal(allocations)
		if marshalErr != nil {
			return fmt.Errorf("marshal allocated accelerators: %w", marshalErr)
		}
		value = string(allocationsBytes)
	}

	obj := map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]any{
				AllocatedAcceleratorAnnoKey: value,
			},
		},
	}
	objBytes, err := json.Marshal(obj)
	if err != nil {
		return fmt.Errorf("marshal object meta: %w", err)
	}

	_, err = kubeclientset.PatchWithCtrlClient(ctx, r.Client, pod, types.StrategicMergePatchType, objBytes)
	if err != nil {
		// 只有写入失败可以重试，且必须重试，否则内存已归还、注解却还在扣费。解析或编码出错会重复得到同样的结果，
		// 强行覆盖无法解析的记录还可能抹掉未知的分配。
		r.recordPendingRelease(pod.UID, container, prior)
	}
	return err
}

func allocatedStatusOf(pod *core.Pod) (allocatedStatus workercore.DevicesStatus, err error) {
	allocations, err := AllocatedAcceleratorsOf(pod)
	if err != nil {
		return workercore.DevicesStatus{}, err
	}
	return allocations.Aggregate(), nil
}

func applyAllocatedStatus(allocatedStatus, remainingStatus workercore.DevicesStatus) (workercore.DevicesStatus, error) {
	if len(allocatedStatus.Groups) == 0 {
		return remainingStatus, nil
	}

	dstAcceleratorIndex := make(map[Resource]*workercore.AcceleratorAllocation)
	for i := range remainingStatus.Groups {
		dstGrp := &remainingStatus.Groups[i]
		for j := range dstGrp.Accelerators {
			dstAccelerator := &dstGrp.Accelerators[j]
			res := Resource{
				Group:  dstGrp.ID,
				Device: dstAccelerator.ID,
			}
			dstAcceleratorIndex[res] = dstAccelerator
		}
	}

	for i := range allocatedStatus.Groups {
		srcGrp := &allocatedStatus.Groups[i]
		for j := range srcGrp.Accelerators {
			srcAccelerator := &srcGrp.Accelerators[j]
			res := Resource{
				Group:  srcGrp.ID,
				Device: srcAccelerator.ID,
			}
			dstAccelerator, exists := dstAcceleratorIndex[res]
			if !exists {
				continue
			}
			if dstAccelerator.Mode != workercore.DeviceAllocationModeNone &&
				dstAccelerator.Mode != srcAccelerator.Mode {
				return remainingStatus, fmt.Errorf("conflicting allocation mode for resource %v: %v vs. %v",
					res, dstAccelerator.Mode, srcAccelerator.Mode)
			}
			dstAccelerator.Mode = srcAccelerator.Mode
			dstAccelerator.Remaining = max(dstAccelerator.Remaining-srcAccelerator.Allocated, 0)
			if srcAccelerator.Mode == workercore.DeviceAllocationModeSliced {
				dstAccelerator.AllocatedSlices++
			}
		}
	}

	return remainingStatus, nil
}

// accumulatePhysicalOccupied 将 Pod 中标注的物理切片位置信息，整理为每个加速器对应的占用区间集合以及每个
// 配置文件的实例数量，以 `(group, device)` 为键进行关联。一个 Pod 最多对应一个加速器实例（
// AllocatedPhysicalProfile + AllocatedPhysicalPlacements —— 向上传输）。通过节点上的 Pods 进行合并后，即
// 可得到每个加速器的实际占用情况，且不涉及设备访问问题。
func accumulatePhysicalOccupied(
	podStatus workercore.DevicesStatus,
	occupied Placements,
	allocated map[Resource]map[string]int32,
) {
	for i := range podStatus.Groups {
		grp := &podStatus.Groups[i]
		for j := range grp.Accelerators {
			acc := &grp.Accelerators[j]
			// 物理记录即一个规格，以及其实例所占据的间隔。单独任何一个部分都不可行。
			if acc.AllocatedPhysicalProfile == "" || len(acc.AllocatedPhysicalPlacements) == 0 {
				continue
			}
			res := Resource{Group: grp.ID, Device: acc.ID}
			occupied[res] = append(occupied[res], acc.AllocatedPhysicalPlacements...)
			if allocated[res] == nil {
				allocated[res] = make(map[string]int32)
			}
			// 一个 Pod 在其每个加速器上仅包含一个规格实例，因此应统计实例数量（每个加速器条目
			// 对应一个实例），而非其分布区间。
			allocated[res][acc.AllocatedPhysicalProfile]++
		}
	}
}

// accumulateLogicalOccupied 从 Pod 注解的 AllocatedLogicalPlacements 中按设备组和卡 ID 汇总逻辑切片区间。
// 它只读逻辑位置字段，因此物理分区不会被误计入。
func accumulateLogicalOccupied(podStatus workercore.DevicesStatus, occupied Placements) {
	for i := range podStatus.Groups {
		grp := &podStatus.Groups[i]
		for j := range grp.Accelerators {
			acc := &grp.Accelerators[j]
			if len(acc.AllocatedLogicalPlacements) == 0 {
				continue
			}
			res := Resource{Group: grp.ID, Device: acc.ID}
			occupied[res] = append(occupied[res], acc.AllocatedLogicalPlacements...)
		}
	}
}

// foldPhysicalLedger 将真实占用与探测时缓存的合法位置结合，
// 在同一个 Status 中填写 AllocatedProfiles 和 RemainingProfiles。
// 每个规格的余量等于仍不与已有分区重叠的合法位置数。状态行与 Spec 中的卡逐项对应，可以按下标读取。
// 旧设备管理器未上报位置时保留空余量集合，供准入区分「尚无账本」与「规格已满」。
func foldPhysicalLedger(
	devs *workercore.Devices,
	status *workercore.DevicesStatus,
	occupied Placements,
	allocated map[Resource]map[string]int32,
) {
	for i := range devs.Spec.Groups {
		grp := &devs.Spec.Groups[i]
		for j := range grp.Accelerators {
			acc := &grp.Accelerators[j]
			profiles := acc.Status.PhysicalSliced.Profiles
			if len(profiles) == 0 {
				continue
			}
			possible := make(map[string][]workercore.AcceleratorPlacement, len(profiles))
			for k := range profiles {
				p := &profiles[k]
				if len(p.Placements) > 0 {
					possible[p.Name] = p.Placements
				}
			}
			res := Resource{Group: grp.ID, Device: acc.ID}
			dst := &status.Groups[i].Accelerators[j]
			dst.AllocatedProfiles = device.ProfileCountSlice(allocated[res])
			dst.RemainingProfiles = device.ProfileCountSlice(device.ComputeRemainingProfiles(occupied[res], possible))
		}
	}
}

// LivePhysicalOccupied 列表按加速器资源分类，列出了该节点上 Pods 当前通过注解占用的物理切片位置——这就是账
// 本折叠层所使用的、由注解生成的占用集合。每个供应商的回收循环会将其作为归属自校验的依据，因此错误标注的
// 拥有标记不会破坏仍由正在运行的 Pod 持有的实例。结束的 Pod 仍然计入统计，这与回收循环所依据的实时集合一
// 致：其实例在 Pod 对象被销毁时消失，而非其容器退出时。它读取信息缓存（无设备 I/O）。
func (r *DevicesReconciler) LivePhysicalOccupied(ctx context.Context) (Placements, error) {
	podList := new(core.PodList)
	if err := r.Client.List(ctx, podList,
		ctrlcli.MatchingFields{IndexingPodsByNodeName: r.NodeName},
		ctrlcli.UnsafeDisableDeepCopy); err != nil {
		return nil, err
	}
	occupied := make(Placements)
	allocated := make(map[Resource]map[string]int32)
	for i := range podList.Items {
		podStatus, err := allocatedStatusOf(&podList.Items[i])
		if err != nil {
			continue
		}
		accumulatePhysicalOccupied(podStatus, occupied, allocated)
	}
	return occupied, nil
}

// LiveLogicalOccupied 从 informer 缓存读取本节点 Pod 注解中的逻辑区间。它们跨设备管理器重启保留；
// 正在删除的 Pod 继续计入，已终止 Pod 则按 heldAllocation 判断硬件是否仍被持有，使区间和设备余额一致。
func (r *DevicesReconciler) LiveLogicalOccupied(ctx context.Context) (Placements, error) {
	podList := new(core.PodList)
	if err := r.Client.List(ctx, podList,
		ctrlcli.MatchingFields{IndexingPodsByNodeName: r.NodeName},
		ctrlcli.UnsafeDisableDeepCopy); err != nil {
		return nil, err
	}
	occupied := make(Placements)
	for i := range podList.Items {
		pod := &podList.Items[i]
		podStatus, err := allocatedStatusOf(pod)
		if err != nil {
			continue
		}
		accumulateLogicalOccupied(heldAllocation(pod, podStatus), occupied)
	}
	return occupied, nil
}

// liveDeviceIDs 合并本节点内存预留和持久 Pod 注解中的设备 ID。删除尚未完成的 Pod 仍算持有。
// kubelet 重放时会逐个核对原 ID，按数量发布资源的插件必须把这些 ID 保持为 Healthy。读取只走缓存，
// 遇到损坏注解会记日志并跳过，因为遗漏的 ID 可能被误标为 Unhealthy。
func (r *DevicesReconciler) liveDeviceIDs(ctx context.Context) (sets.Set[string], error) {
	podList := new(core.PodList)
	if err := r.Client.List(ctx, podList,
		ctrlcli.MatchingFields{IndexingPodsByNodeName: r.NodeName},
		ctrlcli.UnsafeDisableDeepCopy); err != nil {
		return nil, err
	}
	held := sets.New[string]()
	for i := range podList.Items {
		pod := &podList.Items[i]
		allocations, err := AllocatedAcceleratorsOf(pod)
		if err != nil {
			ctrllog.FromContext(ctx).Error(err, "read the allocation annotation of a pod; "+
				"its device IDs may be reported unhealthy and strand the kubelet's checkpoint",
				"pod", kubemeta.GetNamespacedNameKey(pod))
			continue
		}
		for _, allocation := range allocations {
			held.Insert(allocation.DeviceIDs...)
		}
	}
	r.reservationsMutex.RLock()
	for _, reservation := range r.reservations {
		held.Insert(reservation.DeviceIDs...)
	}
	r.reservationsMutex.RUnlock()
	return held, nil
}

func preferredAcceleratorIDsOf(pod *core.Pod, devices *workercore.Devices) []string {
	if pod == nil || pod.Annotations == nil {
		return nil
	}

	if str, ok := pod.Annotations[_PreferredAcceleratorIDAnnoKey]; ok {
		seen := sets.New[string]()
		var ids []string
		for _, raw := range strings.Split(str, ",") {
			id := strings.TrimSpace(raw)
			if id == "" || seen.Has(id) {
				continue
			}
			seen.Insert(id)
			ids = append(ids, id)
		}
		return ids
	}

	if str, ok := pod.Annotations[_PreferredAcceleratorIndexAnnoKey]; ok {
		seen := sets.New[string]()
		var ids []string
		for _, raw := range strings.Split(str, ",") {
			idxStr := strings.TrimSpace(raw)
			if idxStr == "" {
				continue
			}
			for i := range devices.Spec.Groups {
				grp := &devices.Spec.Groups[i]
				for j := range grp.Accelerators {
					acc := &grp.Accelerators[j]
					if strconv.Itoa(int(acc.Index)) == idxStr && !seen.Has(acc.ID) {
						seen.Insert(acc.ID)
						ids = append(ids, acc.ID)
					}
				}
			}
		}
		return ids
	}

	return nil
}
