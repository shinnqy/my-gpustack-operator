package worker

import (
	"context"
	"net/http"
	"time"

	apps "k8s.io/api/apps/v1"
	coordination "k8s.io/api/coordination/v1"
	core "k8s.io/api/core/v1"
	ctrlrecord "k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlbuilder "sigs.k8s.io/controller-runtime/pkg/builder"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlcontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	ctrlevent "sigs.k8s.io/controller-runtime/pkg/event"
	ctrlhandler "sigs.k8s.io/controller-runtime/pkg/handler"
	ctrlpredicate "sigs.k8s.io/controller-runtime/pkg/predicate"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/controller"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/systemmeta"
	"gpustack.ai/gpustack/pkg/utils/ctrlhandlerx"
	"gpustack.ai/gpustack/pkg/worker/kuberess"
	"gpustack.ai/gpustack/pkg/worker/kvcache"
	"gpustack.ai/gpustack/pkg/worker/kvcache/mooncake"
)

// KVCacheBackendReconciler 调谐 KVCacheBackend。
//
// 它管理该对象的生命周期，并在后续任务中一并管理为其提供服务的工作负载：
//
//   - 存活对象会附加本仓库专属的 finalizer 加以锁定，防止与调谐并发的删除操作遗留孤儿
//     资源。
//   - 删除时若 status.usedBy 仍列有使用方，则保留 finalizer。此处的阻塞无需重新入队：
//     usedBy 由使用方自身的控制器写入本对象的 status，该写入事件自然会重新触发清理流程。
//     使用定时器只会反复查询一个答案尚未改变的问题。
//   - status 在 DeepEqual 保护下整块写入，因此处于稳态的后端不会产生写入操作，也就不会
//     自身触发额外事件。
type KVCacheBackendReconciler struct {
	Client ctrlcli.Client

	// AdminHTTP 用于读取 leader 的管理接口。它自带独立超时控制，因为若 leader 接受连接后
	// 发生阻塞，没有超时将会使本次调谐一直挂起直至传输层超时。
	//
	// 该超时仅限制单次阻塞的时长，并不能隔离其他后端受其影响；这正是 kvCacheBackendConcurrency
	// 的作用，两者配合使用才能发挥效果。
	AdminHTTP *http.Client

	// Recorder 用于发布 status 字段无法承载的事件信息。磁盘层的清理事件记录在 Node 而非本
	// 后端对象上，因为需要记录该信息时后端正在被删除，记录在后端上会随对象一并销毁。
	Recorder ctrlrecord.EventRecorder

	// leaseHolders 是该调谐器在多次调谐之间保留的唯一状态，保留它是为了识别状态跃迁：
	// 租约易主与单纯续约在连续两次读取中表现为同一个对象。其零值即可直接使用，因此
	// 无需在构造路径中额外初始化。
	leaseHolders leaseHolders
}

// adminReadTimeout 限制单次读取 leader 管理接口的耗时。该超时较短：此处读取的所有数据
// 均由同一集群内的进程从内存提供，耗时更长说明 leader 已出现异常，尽快上报比苦等更有价值。
const adminReadTimeout = 5 * time.Second

// newAdminHTTPClient 构建所有管理接口读取共用的客户端。该客户端在控制器整个生命周期内
// 仅创建一次，以便在多次调谐之间复用连接池：每次调谐新建客户端会导致每次都重新连接
// 同一个 leader。
//
// 客户端会拒绝重定向而非自动跟随。对于外部后端，地址由 spec 编写者指定，默认客户端会
// 跟随其返回的 302 重定向访问本算子可达的任意主机——即以算子的网络身份发起读取，并将
// 响应摘录写入所有拥有该对象读权限的用户皆可查看的 status 中。这三次读取均针对准入校验
// 已验证地址上的固定路径，因此绝不应采信指向别处的重定向，而是直接上报携带该重定向的响应。
func newAdminHTTPClient() *http.Client {
	return &http.Client{
		Timeout: adminReadTimeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

var _ ctrlreconcile.Reconciler = (*KVCacheBackendReconciler)(nil)

func (r *KVCacheBackendReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// 在第 22 章中实现此函数。
	var result0 ctrl.Result
	var result1 error
	return result0, result1
}

// isTerminalWaitReason 判断处于等待状态的容器是否已陷入卡死而非正在启动。
//
// 被排除的两个原因属于 kubelet 自身的推进状态：容器正在创建，以及 Pod 的 init 容器
// 尚未完成。等待期间上报的其他所有原因——包括镜像拉取失败、各种退避重试——都会持续
// 存在，直到本次调谐之外的某些条件发生改变。
func isTerminalWaitReason(reason string) bool {
	switch reason {
	case "", "ContainerCreating", "PodInitializing":
		return false
	}
	return true
}

// podIsReady 报告 Pod 自身的就绪状态。对于 member 而言，就绪意味着存储客户端已挂载其
// 分段，而不仅仅是容器已启动：就绪探针连接的是入口进程在挂载完成后才会开启服务的端口。
// 若没有该探针，容器一运行该状态就会为 true，使每个 member 在启动期间都会被计为缺口。
func podIsReady(pod *core.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == core.PodReady {
			return cond.Status == core.ConditionTrue
		}
	}
	return false
}

// alignRenderedContainer 将渲染出的字段复制到运行中的容器对象，并返回是否发生变更。
// 此处逐一显式列出字段，因为未列出的字段由 API Server 填充默认值，若覆盖它们会导致
// 每次调谐都产生写入。
//
// 该函数同时服务于 leader 和 member。仅其中一方使用的字段在另一方上比较时自然相等，
// 将字段列表集中在一处维护，可以防止渲染器新增字段后只渲染却永不收敛的问题。
func alignRenderedContainer(actual *core.Container, expected core.Container) (changed bool) {
	if actual.Image != expected.Image {
		actual.Image = expected.Image
		changed = true
	}
	// 无条件比对，因为渲染器会直接解析出策略而非将其留空。此前的问题恰恰出在期望值为空：
	// API Server 会在写入时填充该字段，导致拿空值与默认值比对时工作负载不断滚动更新；
	// 而为规避该问题跳过比对又会遗留过期策略——例如已从 spec 中移除的值，或基于 spec 已
	// 不再使用的镜像标签推导出的值。如今该规则统一由 kvcache.EffectivePullPolicy 负责。
	if actual.ImagePullPolicy != expected.ImagePullPolicy {
		actual.ImagePullPolicy = expected.ImagePullPolicy
		changed = true
	}
	// 与拉取策略不同，此处可安全比对：渲染器会指定非空值，而 API Server 仅在字段为空时
	// 填充默认值。
	if actual.TerminationMessagePolicy != expected.TerminationMessagePolicy {
		actual.TerminationMessagePolicy = expected.TerminationMessagePolicy
		changed = true
	}
	if !kubemeta.DeepEqual(actual.Command, expected.Command) {
		actual.Command = expected.Command
		changed = true
	}
	if !kubemeta.DeepEqual(actual.Args, expected.Args) {
		actual.Args = expected.Args
		changed = true
	}
	if !kubemeta.DeepEqual(actual.Env, expected.Env) {
		actual.Env = expected.Env
		changed = true
	}
	if !kubemeta.DeepEqual(actual.Ports, expected.Ports) {
		actual.Ports = expected.Ports
		changed = true
	}
	if !kubemeta.DeepEqual(actual.ReadinessProbe, expected.ReadinessProbe) {
		actual.ReadinessProbe = expected.ReadinessProbe
		changed = true
	}
	if !kubemeta.DeepEqual(actual.LivenessProbe, expected.LivenessProbe) {
		actual.LivenessProbe = expected.LivenessProbe
		changed = true
	}
	// 比对整个字段值，而非仅比对 Requests。渲染器完全不设置 limits，即将其保持为零值——而
	// 仅比对渲染器所填字段恰恰会让外部注入的 limit 残留下来：过低的内存 limit 会导致容器
	// 陷入 OOM 崩溃循环，且本对象中没有任何信息能解释原因。后端自身的声明在设计上就是
	// REQUEST，因为 member 会依据同一数值计算其分段大小，低于该值的 limit 会杀死本可正常
	// 容下的 member。
	if !kubemeta.DeepEqual(actual.Resources, expected.Resources) {
		actual.Resources = expected.Resources
		changed = true
	}
	if !kubemeta.DeepEqual(actual.VolumeMounts, expected.VolumeMounts) {
		actual.VolumeMounts = expected.VolumeMounts
		changed = true
	}
	// 安全上下文按整体比对，而非逐个 capability 比对：它是作为整体渲染的，运行中的容器若
	// 通过带外方式获得了额外配置，必须予以清除。
	if !kubemeta.DeepEqual(actual.SecurityContext, expected.SecurityContext) {
		actual.SecurityContext = expected.SecurityContext
		changed = true
	}
	// 停机钩子的 argv 内部携带了缩容宽限时间，且该宽限时间可编辑。若将其排除在比对之外，
	// 该钩子只会被写入一次而不再更新：Pod 指纹会随变更移动，从而触发 member 被删除重建——
	// 但所使用的模板中钩子仍保留旧的宽限时间。其结果比不收敛更糟，因为其余一切都表现为
	// 变更已经生效。
	if !kubemeta.DeepEqual(actual.Lifecycle, expected.Lifecycle) {
		actual.Lifecycle = expected.Lifecycle
		changed = true
	}
	return changed
}

// enqueueKVCacheBackendWhenPoolChanged 将 pool 映射回其引用的所有后端。
//
// 它读取 pool 的 spec 而非后端的 usedBy，因为最关键的事件是 pool 的删除操作——而删除
// 事件会携带对象的最后已知状态，其中仍列出了这些后端。
func (r *KVCacheBackendReconciler) enqueueKVCacheBackendWhenPoolChanged(
	_ context.Context, obj ctrlcli.Object,
) []ctrlreconcile.Request {
	kvcp, ok := obj.(*workercore.KVCachePool)
	if !ok {
		return nil
	}

	reqs := make([]ctrlreconcile.Request, 0, len(kvcp.Spec.Backends))
	for _, name := range kvcp.Spec.Backends {
		reqs = append(reqs, ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKey{Name: name}})
	}
	return reqs
}

// enqueueKVCacheBackendWhenWorkloadChanged 将渲染出的对象映射回拥有它的后端。
//
// 它直接从对象自身的资源注解中读取后端名称，而非遍历其 owner references：该注解正是
// predicate 已经筛选过的依据，因此两者在构造上天然一致，且删除事件与更新事件同样
// 携带该注解。
func (r *KVCacheBackendReconciler) enqueueKVCacheBackendWhenWorkloadChanged(
	_ context.Context, obj ctrlcli.Object,
) []ctrlreconcile.Request {
	name := systemmeta.DescribeResourceNote(obj, kvcache.ResourceNoteBackend)
	if name == "" {
		return nil
	}
	return []ctrlreconcile.Request{{NamespacedName: ctrlcli.ObjectKey{Name: name}}}
}

// kvCacheBackendWorkloadPredicate 将监听范围限定为本算子自身创建的对象。若无此过滤，
// 集群中的每个 Deployment 和 Service 都会唤醒该调谐器。
//
// Namespace 校验占检查条件的一半，且这一半并非为了降噪。资源注解本质上是 annotation，
// 任何能在任意 Namespace 创建对象的人都可以写入匹配的注解——每次匹配都会触发对应后端
// 的一次调谐，即对其管理接口发起三次 HTTP 读取。渲染只会在系统 Namespace 中进行，
// 因此无论其他 Namespace 中的对象如何标注自身，都不可能是本算子的对象。
func kvCacheBackendWorkloadPredicate() ctrlpredicate.Predicate {
	return ctrlpredicate.NewPredicateFuncs(func(obj ctrlcli.Object) bool {
		return obj.GetNamespace() == kuberess.SystemNamespaceName &&
			systemmeta.MatchResource(obj, kvcache.ResourceType)
	})
}

func (r *KVCacheBackendReconciler) SetupController(_ context.Context, opts controller.SetupOptions) error {
	r.Client = opts.Manager.GetClient()
	r.Recorder = opts.Manager.GetEventRecorderFor("kvcachebackend")
	if r.AdminHTTP == nil {
		r.AdminHTTP = newAdminHTTPClient()
	}

	dedupWindow := ctrlhandlerx.NewDedupWindow[ctrlreconcile.Request]()

	return ctrl.NewControllerManagedBy(opts.Manager).
		Named("kvcachebackend").
		For(&workercore.KVCacheBackend{}).
		Watches(
			// pool 会通过将自身写入 status.usedBy 来占用该后端，并在退出时由其自身的调谐器清除
			// 该条目——因此通常后端会由自身 status 变更唤醒，此监听并无额外作用。它所兜底的是
			// pool 在未清除该条目的情况下消失的场景，否则没有任何事件能唤醒后端：该占用记录会
			// 指向一个已不存在的对象并卡住销毁流程，且再无事件触发重新检查。
			//
			// 与工作负载监听不同，此处不做去重。前者按渲染对象触发并聚合到同一后端；此处按 pool
			// 触发，而每个 pool 对其引用的每个后端仅列出一次。
			&workercore.KVCachePool{},
			ctrlhandler.EnqueueRequestsFromMapFunc(r.enqueueKVCacheBackendWhenPoolChanged),
		).
		Watches(
			// 监听 leader 的 Deployment 并将拥有它的后端入队。若无此监听，调谐器只会在后端对象
			// 自身变化时才收敛渲染出的对象，人工对 Deployment 的修改将一直保留，直到后端对象
			// 恰好发生其他变更——这并非持续调谐，而是碰巧调谐。
			&apps.Deployment{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFuncWithWindow(
				3*time.Second,
				dedupWindow,
				r.enqueueKVCacheBackendWhenWorkloadChanged,
			),
			ctrlbuilder.WithPredicates(kvCacheBackendWorkloadPredicate()),
		).
		Watches(
			// 对其前端承载已发布端点的 Service 同样如此。
			&core.Service{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFuncWithWindow(
				3*time.Second,
				dedupWindow,
				r.enqueueKVCacheBackendWhenWorkloadChanged,
			),
			ctrlbuilder.WithPredicates(kvCacheBackendWorkloadPredicate()),
		).
		Watches(
			// 以及各 member 组的 DaemonSet，节点加入或离开分组最先在此体现。
			&apps.DaemonSet{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFuncWithWindow(
				3*time.Second,
				dedupWindow,
				r.enqueueKVCacheBackendWhenWorkloadChanged,
			),
			ctrlbuilder.WithPredicates(kvCacheBackendWorkloadPredicate()),
		).
		Watches(
			// 以及 leader 自身的 Pod，用于捕获仅会写入 Pod 的那类故障。调度器会将“无可用节点”
			// 记录在 Pod 上（表现为 PodScheduled=False），而该写入不会引起 Deployment 的任何变化——
			// Deployment 自身要到十分钟后才会上报 ProgressDeadlineExceeded。若无此监听，上报
			// leader 不可调度的那次调谐就只能被动等待其他事件恰好触发。
			&core.Pod{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFuncWithWindow(
				3*time.Second,
				dedupWindow,
				r.enqueueKVCacheBackendWhenLeaderPodChanged,
			),
			ctrlbuilder.WithPredicates(kvCacheBackendLeaderPodPredicate()),
		).
		Watches(
			// 以及选主所依赖的 Lease，它是唯一能反映主备切换发生的对象。该对象由存储引擎而非
			// 本算子创建，因此不携带资源注解，映射函数必须依据名称反推出归属关系。
			//
			// 此处设置 predicate 并非出于代码整洁。在每个 leader 的整个生命周期中，租约每隔几秒
			// 就会续约一次，若不过滤监听事件，每次续约都会触发一次调谐，进而对该后端的管理接口
			// 发起三次串行读取——这相当于一个仅在每次故障切换时记录一条事件的功能，却对集群中的
			// 每个存储引擎施加持续负载。
			&coordination.Lease{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFuncWithWindow(
				3*time.Second,
				dedupWindow,
				r.enqueueKVCacheBackendWhenLeaseChanged,
			),
			ctrlbuilder.WithPredicates(ctrlpredicate.Funcs{
				// 首次出现的 Lease 用于建立基线而非上报任何事件，它所唤醒的该次调谐负责保存该基线。
				CreateFunc: func(e ctrlevent.CreateEvent) bool {
					return e.Object.GetNamespace() == kuberess.SystemNamespaceName
				},
				UpdateFunc: func(e ctrlevent.UpdateEvent) bool {
					return kvCacheBackendLeaseHolderChanged(e.ObjectOld, e.ObjectNew)
				},
				// 被删除的 Lease 表示选主已停止，而非发生主备切换。负责清除持有者记录的调谐将由后端
				// 自身的变更唤醒。
				DeleteFunc:  func(ctrlevent.DeleteEvent) bool { return false },
				GenericFunc: func(ctrlevent.GenericEvent) bool { return false },
			}),
		).
		WithOptions(ctrlcontroller.Options{
			MaxConcurrentReconciles: kvCacheBackendConcurrency,
		}).
		Complete(r)
}

// kvCacheBackendConcurrency 指定该控制器同时调谐的后端数量。
//
// 设置该并发度的原因在于，这是此处唯一会阻塞在集群外部操作上的调谐器：每个后端都会
// 按定时器重新入队，且每次调谐都会向对象指定的地址发起三次串行读取——对于外部后端，
// 该地址完全不受本算子控制。在默认的单 worker 模式下，只要有一个端点在建立连接后
// 阻塞，就会在每次调谐中占用队列直至超时，导致其他所有后端（包括正在删除的后端）
// 都在其后排队等待。
//
// 单请求超时无法替代并发控制。它仅限制单次阻塞的持续时间，无法避免其他后端被其阻塞。
//
// 取值为 4 而非更大值：该工作每次仅针对不同主机发起一次 HTTP 读取，因此目的在于防止
// 少数异常地址独占队列，而非追求高吞吐量。
const kvCacheBackendConcurrency = 4

// kvCacheBackendLeaderPodPredicate 将 Pod 的 watch 范围限制为本 operator 的 leader Pod。
//
// 它不能像另外三个 watch 那样按资源标记过滤：Pod 由 Deployment 的控制器根据模板创建，
// 而该标记位于 Deployment 上，而非模板内部。leader Pod 上确实带有身份标签——而伪造标签
// 比伪造标记更容易，因此这里的 Namespace 同样起着关键的校验作用。
func kvCacheBackendLeaderPodPredicate() ctrlpredicate.Predicate {
	return ctrlpredicate.NewPredicateFuncs(func(obj ctrlcli.Object) bool {
		return obj.GetNamespace() == kuberess.SystemNamespaceName &&
			mooncake.LeaderPodBackendName(obj.GetLabels()) != ""
	})
}

// enqueueKVCacheBackendWhenLeaderPodChanged 将 leader Pod 映射回其所属的 backend。
func (r *KVCacheBackendReconciler) enqueueKVCacheBackendWhenLeaderPodChanged(
	_ context.Context, obj ctrlcli.Object,
) []ctrlreconcile.Request {
	name := mooncake.LeaderPodBackendName(obj.GetLabels())
	if name == "" {
		return nil
	}
	return []ctrlreconcile.Request{{NamespacedName: ctrlcli.ObjectKey{Name: name}}}
}
