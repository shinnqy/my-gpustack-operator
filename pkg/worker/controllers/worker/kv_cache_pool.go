package worker

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	ctrl "sigs.k8s.io/controller-runtime"
	ctrlbuilder "sigs.k8s.io/controller-runtime/pkg/builder"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlevent "sigs.k8s.io/controller-runtime/pkg/event"
	ctrlhandler "sigs.k8s.io/controller-runtime/pkg/handler"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	ctrlpredicate "sigs.k8s.io/controller-runtime/pkg/predicate"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/controller"
	"gpustack.ai/gpustack/pkg/kubeapistatus"
)

const (
	// IndexingKVCachePoolBindingByPool 按所引用的 pool 为每个 KVCachePoolBinding 建立索引。
	// 这样“查询该 pool 的所有 Binding”就只需一次限定范围的查询，而不必列举集群中全部 Binding
	// 并在内存中过滤——每次 reconcile 的 pass 都会执行该查询，因此差别体现在每次 pass 而非仅在启动时。
	IndexingKVCachePoolBindingByPool = "kvcachepoolbindings.worker.gpustack.ai/pool"

	// IndexingKVCachePoolByBackend 按所依赖的 backend 为每个 KVCachePool 建立索引。
	//
	// 之所以需要该索引，是因为租户账本与策略文件按 master 而非按 pool 收敛：多个 pool 可能共享
	// 同一个 backend，若某次 pass 仅写入自身 pool 的租户，每次 reconcile 都会抹掉其他 pool 的
	// 租户。查找这些同属一个 backend 的兄弟 pool 同样必须通过单次查询完成，原因与上文相同——它会在
	// 两者各自的每次 pass 中执行。
	IndexingKVCachePoolByBackend = "kvcachepools.worker.gpustack.ai/backend"
)

// indexKVCachePoolBindingByPool 是 IndexingKVCachePoolBindingByPool 的字段索引提取器。
//
// 即使 Binding 引用的 pool 尚不存在，仍会对其建立索引。这是有意为之：pool 可能在之后创建，
// 该索引负责将此 Binding 带入随后的 pass 中——如果在此处丢弃，提前指定了 pool 的 Binding
// 将对其永久不可见。
func indexKVCachePoolBindingByPool(obj ctrlcli.Object) []string {
	kvcpb, ok := obj.(*workercore.KVCachePoolBinding)
	if !ok || kvcpb == nil || kvcpb.Spec.PoolRef.Name == "" {
		return nil
	}
	return []string{kvcpb.Spec.PoolRef.Name}
}

// indexKVCachePoolByBackend 是 IndexingKVCachePoolByBackend 的字段索引提取器。
//
// 尽管准入校验仅允许一个条目，它仍对 spec.backends 中的每一项建立索引，而不是只取第一项：
// 索引必须如实反映已存储的对象内容，否则在该规则收紧之前存储的对象，或由绕过 Webhook 的客户端
// 写入的对象，将在兄弟 pool 相互查找的查询中被遗漏。
func indexKVCachePoolByBackend(obj ctrlcli.Object) []string {
	kvcp, ok := obj.(*workercore.KVCachePool)
	if !ok || kvcp == nil {
		return nil
	}

	names := make([]string, 0, len(kvcp.Spec.Backends))
	for _, name := range kvcp.Spec.Backends {
		if name != "" {
			names = append(names, name)
		}
	}
	return names
}

// KVCachePool 上报的阶段，由实际观测状态而非期望配置推导得出：即使 spec 已完整，只要其
// backend 尚未发布地址，pool 的阶段就仍为 Provisioning。
const (
	KVCachePoolPhaseProvisioning = "Provisioning"
	KVCachePoolPhaseReady        = "Ready"
	KVCachePoolPhaseError        = "Error"
	KVCachePoolPhaseDeleting     = "Deleting"
)

// KVCachePool 上报的 condition 类型，每个维度对应一个。
//
// 它们均采用正向命名，与本仓库所有 condition 一致，也符合 Kubernetes API 规范：类型名表示
// 健康状态，False 表示存在故障。规范中命名了两个故障——MultiTenancyDisabled 和
// QuotaPolicyNotWritable——它们作为下面两个 condition 的 Reason 存在，而不是独立的 condition
// 类型，因为当故障发生时表现为 True 的类型，在阅读体验上会与集群中操作员看到的其余所有
// condition 完全相反。
const (
	// KVCachePoolConditionBackendResolved 在无法读取 backend 或其尚未发布可访问地址时为 False。
	KVCachePoolConditionBackendResolved kubeapistatus.ConditionType = "BackendResolved"
	// KVCachePoolConditionQuotaLedgerAvailable 在 master 未持有可写入的租户账本时为 False——
	// 此时 Reason 为 MultiTenancyDisabled，即在运行时观测到的 F5 前置条件，而非在准入阶段做假设。
	KVCachePoolConditionQuotaLedgerAvailable kubeapistatus.ConditionType = "QuotaLedgerAvailable"
	// KVCachePoolConditionQuotaPolicyWritable 在 master 拒绝写入配额时为 False——
	// 若是其自身的策略源拒绝该写入，则 Reason 为 QuotaPolicyNotWritable。
	KVCachePoolConditionQuotaPolicyWritable kubeapistatus.ConditionType = "QuotaPolicyWritable"
	// KVCachePoolConditionQuotaWithinTotal 在该 pool 下各 Binding 请求的上限之和超过 pool 自身
	// 声明的总量时为 False——Reason 为 Oversubscribed，同时列出总和与总量。False 并不代表故障，
	// 也没有组件损坏：存储层会按各自请求的比例向每个租户分配其所能提供的份额，且每个 Binding 的
	// status.effectiveQuota 会上报实际授予的数值。此处将其作为 Condition 而非直接拒绝，是因为
	// 直接拒绝会破坏“先创建 Binding 再扩容 backend”的常规操作顺序；准入阶段仅拒绝单个本身就
	// 绝对无法满足的上限请求。
	//
	// summarizeKVCachePool 读取的维度中刻意不包含该 condition，因此超额分配的 pool 不会被判定为
	// Error：False 所表示的是存储层按设计执行的按比例分配，这样命名正是为了避免误导操作员去“修复”
	// 一个本来正常工作的状态。
	KVCachePoolConditionQuotaWithinTotal kubeapistatus.ConditionType = "QuotaWithinTotal"
	// KVCachePoolConditionCapacityAllocatable 在 master 上报无可分配容量时为 False。
	// 这对应启动顺序问题：此时尚无成员完成挂载，因此所有有效配额均为零，尽管各个对象表面上看起来
	// 都配置正确，但任何写入都无法成功。
	KVCachePoolConditionCapacityAllocatable kubeapistatus.ConditionType = "CapacityAllocatable"
	// KVCachePoolConditionReleasable 仅在删除操作被阻塞保持时才为 False，且刻意不包含在
	// summarizeKVCachePool 读取的维度中：运行中的 pool 从不携带此 condition，而在该函数中，
	// 未观测到的维度会被视作 Provisioning。
	KVCachePoolConditionReleasable kubeapistatus.ConditionType = "Releasable"
)

// 规范中明确指定名称的 Reason 列表。它们被定义为常量，因为验收标准 11 会对其断言，
// 且 e2e 用例会按名称检索它们。
const (
	KVCachePoolReasonMultiTenancyDisabled   = "MultiTenancyDisabled"
	KVCachePoolReasonQuotaPolicyNotWritable = "QuotaPolicyNotWritable"
	KVCachePoolReasonOversubscribed         = "Oversubscribed"
)

// KVCacheMasterSeparatesTenants 报告服务于某 pool 的 master 是否能够隔离两个复用域。
// 这是渲染器需要回答的问题：无法隔离的 master 不会区分租户名称，若仍转发租户名，就相当于宣称
// 提供了存储层实际并不具备的隔离能力。
//
// 优先采用观测结果——即 pool 的 QuotaLedgerAvailable condition，由 reconciler 根据 master 自身的
// 响应写入，因此也涵盖了并非由本 operator 启动的外部 master。True 表示支持隔离；Reason 为
// MultiTenancyDisabled 的 False 表示不支持。其他任何读取结果（缺失、Unknown 或其他原因导致的
// False）均无法得出结论，此时对于托管 backend 则依据声明配置判断——这是精确的，因为本 operator
// 会将该参数渲染到 leader 自身的命令行中——而对于不可读的外部 backend 则保留租户信息，与在 pool
// 上报结果前阻止 Pod 调度到该 pool 的门控逻辑保持一致。
func KVCacheMasterSeparatesTenants(kvcb *workercore.KVCacheBackend, kvcp *workercore.KVCachePool) bool {
	condition := KVCachePoolConditionQuotaLedgerAvailable
	switch {
	case condition.IsTrue(kvcp):
		return true
	case condition.IsFalse(kvcp) && condition.GetReason(kvcp) == KVCachePoolReasonMultiTenancyDisabled:
		return false
	}
	if managed := kvcb.Spec.Connection.Managed; managed != nil {
		return managed.Leader.MultiTenancyEnabled()
	}
	return true
}

// KVCachePoolReconciler 负责调谐 KVCachePool。
//
// 在这一对资源中，pool 是唯一被直接调谐的对象：KVCachePoolBinding 没有独立的 reconciler，
// 所有 Binding 事件都会映射到其指定的 pool 上。这并非简化实现，而是保证每次 pass 正确性的关键——
// 租户账本与策略文件均按 master 维度维护，若在每个 Binding 的循环中各自写入，每次 pass 都会
// 覆盖其他 Binding 刚刚写入的内容。
type KVCachePoolReconciler struct {
	Client ctrlcli.Client

	// AdminHTTP 用于读写 leader 的管理接口。它与 backend reconciler 一样带有独立的超时设置，
	// 原因相同：若 master 接受连接后发生卡死，缺少超时会导致本次 reconcile 一直阻塞，直到传输层
	// 允许的上限为止。
	AdminHTTP *http.Client
}

func (r *KVCachePoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// 在第 22 章中实现此函数。
	var result0 ctrl.Result
	var result1 error
	return result0, result1
}

// kvCachePoolPredicate 决定关于 pool 自身的哪些事件值得触发一次 reconcile。
//
// 它在创建、generation（spec）变更以及对象被标记删除时触发。它忽略最终移除事件——此时已无
// 状态需要收敛——而且关键在于，它忽略本 operator 自身的写入：状态更新和 finalizer 修改既不会
// 改变 generation，也不会改变删除时间戳，若不加以过滤，每次 pass 都会触发下一次 pass，从而
// 导致循环无法收敛。
func kvCachePoolPredicate() ctrlpredicate.Predicate {
	return ctrlpredicate.Funcs{
		DeleteFunc: func(ctrlevent.DeleteEvent) bool { return false },
		UpdateFunc: func(e ctrlevent.UpdateEvent) bool {
			oldKvcp, ok := e.ObjectOld.(*workercore.KVCachePool)
			if !ok {
				return true
			}
			newKvcp, ok := e.ObjectNew.(*workercore.KVCachePool)
			if !ok {
				return true
			}
			if !oldKvcp.DeletionTimestamp.Equal(newKvcp.DeletionTimestamp) {
				return true
			}
			return oldKvcp.Generation != newKvcp.Generation
		},
	}
}

func (r *KVCachePoolReconciler) SetupController(ctx context.Context, opts controller.SetupOptions) error {
	// 配置字段索引器。两个索引均在此处而非读取处注册，因为索引必须在缓存启动前就绪，
	// 对不存在的索引执行 List 会在运行时而非编译时失败。
	fi := opts.Manager.GetFieldIndexer()
	err := fi.IndexField(ctx, &workercore.KVCachePoolBinding{},
		IndexingKVCachePoolBindingByPool, indexKVCachePoolBindingByPool)
	if err != nil {
		return fmt.Errorf("index kv cache pool binding '%s': %w", IndexingKVCachePoolBindingByPool, err)
	}
	err = fi.IndexField(ctx, &workercore.KVCachePool{},
		IndexingKVCachePoolByBackend, indexKVCachePoolByBackend)
	if err != nil {
		return fmt.Errorf("index kv cache pool '%s': %w", IndexingKVCachePoolByBackend, err)
	}

	r.Client = opts.Manager.GetClient()
	if r.AdminHTTP == nil {
		r.AdminHTTP = newAdminHTTPClient()
	}

	return ctrl.NewControllerManagedBy(opts.Manager).
		Named("kvcachepool").
		For(
			// 按 KVCachePool 自身名称对其进行调谐。
			//
			// 在创建、generation（spec）变更以及对象被标记删除时触发。绝不在最终移除时触发——此时已无
			// 状态需要收敛——也绝不在本 operator 自身的状态或 finalizer 写入时触发，这些写入既不会改变
			// generation，也不会改变删除时间戳，否则会导致每次 pass 都调度下一次 pass。
			&workercore.KVCachePool{},
			ctrlbuilder.WithPredicates(kvCachePoolPredicate()),
		).
		Watches(
			// 每个 Binding 事件都会传递到其指定的 pool。Binding 用于声明复用域和配额上限，因此其创建、
			// 编辑和删除都会改变 pool 必须写入 master 账本的内容。
			//
			// 此处与 pool 自身不同，不会过滤掉删除事件：Binding 消失意味着该租户的条目必须被移除，
			// 且没有其他事件会传达这一变化。
			&workercore.KVCachePoolBinding{},
			ctrlhandler.EnqueueRequestsFromMapFunc(r.enqueueKVCachePoolWhenBindingChanged),
		).
		Complete(r)
}

// compareKVCacheObjectReferences 对 usedBy 列表进行排序。由于单个列表中可能包含多种类型，
// 先按 Kind 排序，再按用于标识同类对象的 Namespace 和名称排序。
func compareKVCacheObjectReferences(a, b workercore.KVCacheObjectReference) int {
	if c := strings.Compare(a.Kind, b.Kind); c != 0 {
		return c
	}
	if c := strings.Compare(a.Namespace, b.Namespace); c != 0 {
		return c
	}
	return strings.Compare(a.Name, b.Name)
}

// KVCachePoolBinding 上报的 condition 类型。与 pool 一样采用正向命名。
const (
	// KVCachePoolBindingConditionDomainExclusive 在另一个 Binding 声明了相同的复用域时为 False——
	// Reason 为 DomainClaimedByMultipleBindings，并指明另一声明方。
	KVCachePoolBindingConditionDomainExclusive kubeapistatus.ConditionType = "DomainExclusive"
	// KVCachePoolBindingConditionQuotaObserved 在无法读取 master 中该复用域的数据时为 False；
	// 即使 master 从未见过该复用域，该 condition 仍为 True——这是观测到了“不存在”这一事实，
	// 而非观测失败。
	KVCachePoolBindingConditionQuotaObserved kubeapistatus.ConditionType = "QuotaObserved"
	// KVCachePoolBindingConditionQuotaGranted 在观测到的授权额度无法支持写入时为 False：
	// 包括没有账本条目、未导出数值或授权额度为零。它与 QuotaObserved 分开，因为授权为零属于
	// 一次成功的观测——两者配合即可区分“无法查看”与“查看到了，但没有可写入的配额”。
	//
	// 如果没有它，Binding 仅凭观测成功就会进入 Ready 状态，导致刚重启完 master 后几十秒内
	// 授权额度仍为零的复用域在写入必然失败的情况下被报告为 Ready。这正是 Binding 存在所要
	// 防止的误判。
	KVCachePoolBindingConditionQuotaGranted kubeapistatus.ConditionType = "QuotaGranted"
	// KVCachePoolBindingConditionReleasable 在该 Namespace 内仍有工作负载通过此 Binding 持有
	// pool 时为 False。出于与 pool 自身相同的原因，summarizeKVCachePoolBinding 读取的维度中不包含它。
	KVCachePoolBindingConditionReleasable kubeapistatus.ConditionType = "Releasable"
)

// enqueueKVCachePoolWhenBindingChanged 将 Binding 映射到其指定的 pool。
//
// pool 是集群作用域资源，因此请求仅包含名称而不含 Namespace——来自任意 Namespace 的 Binding
// 都会将同一个 pool 入队，这正是让 pool 作为唯一被调谐对象的意义所在。
//
// 即使 Binding 指定的 pool 尚不存在，仍会将其入队。随后的 pass 会发现该资源不存在并返回，
// 仅耗费一次空操作 reconcile，却能避免 pool 稍后创建时该 Binding 处于不可见状态。
func (r *KVCachePoolReconciler) enqueueKVCachePoolWhenBindingChanged(
	ctx context.Context, obj ctrlcli.Object,
) []ctrlreconcile.Request {
	logger := ctrllog.FromContext(ctx).
		WithValues("kv cache pool binding", ctrlcli.ObjectKeyFromObject(obj))

	kvcpb, ok := obj.(*workercore.KVCachePoolBinding)
	if !ok || kvcpb.Spec.PoolRef.Name == "" {
		return nil
	}

	reqs := []ctrlreconcile.Request{
		{NamespacedName: ctrlcli.ObjectKey{Name: kvcpb.Spec.PoolRef.Name}},
	}

	logger.V(2).Info("enqueued kv cache pool from binding", "requests", reqs)
	return reqs
}
