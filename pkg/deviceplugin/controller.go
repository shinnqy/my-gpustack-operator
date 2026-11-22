package deviceplugin

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"sync"

	"github.com/go-logr/logr"
	core "k8s.io/api/core/v1"
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
	"gpustack.ai/gpustack/pkg/kubemeta"
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

// Reconcile 功能会根据节点的 `Spec` 以及安排在该节点上的 Pods 的注释信息，重新构建该节点的 Devices 状态。
// 随后，它会将生成的实时 pod-UID 集合通过进程内预留/可见性授权表进行处理，并通知 ListAndWatch 订阅者。状
// 态的重新构建是整体性的，而非增量式的：BuildDesiredStatus 会在一次遍历中根据 `Spec` 和 Pod 的注释信息重
// 新计算每个加速器模式、剩余资源、已分配配置和剩余配置。该方法只分配一次结果，刻意没有第二次单独应用的写
// 入操作，以便后续遍历能够覆盖它。
func (r *DevicesReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// 第 8 章补上此函数的实现。
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
	// 第 8 章补上此函数的实现。
	var result0 workercore.DevicesStatus
	var result1 []string
	return result0, result1
}

const (
	// AllocatedAcceleratorAnnoKey 用于标识携带 Pod 每个容器加速器分配信息的注释，属于 PodAllocations
	// 的 JSON 数据的一部分。这是容器所包含内容的唯一持久记录——进程内的预留不会在设备管理器重启后保留，
	// 因此账本的重建、资源回收流程以及 Pod 删除的监控都依赖于这一记录。
	AllocatedAcceleratorAnnoKey       = "device.gpustack.ai/accelerator.allocated"
	_PreferredAcceleratorIDAnnoKey    = "device.gpustack.ai/accelerator.preferred-id"
	_PreferredAcceleratorIndexAnnoKey = "device.gpustack.ai/accelerator.preferred-index"
)

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

func allocatedStatusOf(pod *core.Pod) (allocatedStatus workercore.DevicesStatus, err error) {
	allocations, err := AllocatedAcceleratorsOf(pod)
	if err != nil {
		return workercore.DevicesStatus{}, err
	}
	return allocations.Aggregate(), nil
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
