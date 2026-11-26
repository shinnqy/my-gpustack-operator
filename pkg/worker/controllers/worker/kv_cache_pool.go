package worker

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/sets"
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
	"gpustack.ai/gpustack/pkg/kubeclientset"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/systemmeta"
	"gpustack.ai/gpustack/pkg/utils/ctrlclix"
	"gpustack.ai/gpustack/pkg/worker/kvcache/mooncake"
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

// 资源池释放被推迟的两个原因。
const (
	// KVCachePoolReasonHeldByBindings 是一个尚未被撤销的授权：若根据绑定释放资源，会导致某个命名空间在
	// 不再存在的资源池中持有配额。
	KVCachePoolReasonHeldByBindings = "HeldByBindings"
	// KVCachePoolReasonLedgerNotReleased 是之后无人能看到的容量。Master 的账本没有标注出条目属于谁，因
	// 此一旦这个对象消失，集群中其他东西就不知道该如何删除它所记录的内容了。
	KVCachePoolReasonLedgerNotReleased = "LedgerNotReleased"
)

// 这一对中 usedBy 条目所代表的类型。
//
// 它们是常量，因为声明及其发布必须在相同的字符串上匹配，而这两个过程发生在不同的阶段——一个是在资源池解析其
// 后端时，另一个则是当资源池被拆除时，必须根据值重新找到入口。
const (
	KVCachePoolKind        = "KVCachePool"
	KVCachePoolBindingKind = "KVCachePoolBinding"
)

// kvCachePoolObserveInterval 会针对已确定的资源池重新执行一次检查。Master 中的数据——有效配额、使用率、可分
// 配量——保持不变，因此如果资源池仅在 Kubernetes 事件上完成协调，那么其报告的内容将基于最后一次事件发生时的状态。
const kvCachePoolObserveInterval = 30 * time.Second

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

// IMPORTANT: 【共享缓存池 kv cache】多租户配额账本与 Binding 状态收敛的入口
func (r *KVCachePoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := ctrllog.FromContext(ctx)

	// 读取本次调谐的对象。
	kvcp := new(workercore.KVCachePool)
	err := r.Client.Get(ctx, req.NamespacedName, kvcp, ctrlclix.WithoutQuorum)
	if err != nil {
		if !kerrors.IsNotFound(err) {
			logger.Error(err, "fetch kv cache pool")
			return ctrl.Result{}, err
		}
		// Pool 已删除时，Binding 仍可能存在。它们按自身 poolRef 索引，用同一个名字仍可找到。Pool 终
		// 结器已经负责处理集群外的状态，这里释放仍在等待的 Binding。
		return ctrl.Result{}, r.releaseOrphanKVCachePoolBindings(ctx, req.Name)
	}

	if kvcp.DeletionTimestamp != nil {
		return r.teardownKVCachePool(ctx, kvcp)
	}

	// 先添加终结器。登记在 master 上的条目可能比 Pool 活得更久，账本又不记录条目属于哪个 Pool，必须在对
	// 象消失前确定该清理哪些条目。
	if !systemmeta.Lock(kvcp) {
		if err = r.Client.Update(ctx, kvcp); err != nil {
			logger.Error(err, "lock kv cache pool")
			return ctrl.Result{}, err
		}
	}

	// 条件写入器需要含有 Status 的对象。复制实际 ObjectMeta 和本轮观测到的 Status，在独立副本上构建期望
	// 状态，供 syncStatus 与原对象比较。
	holder := &workercore.KVCachePool{
		ObjectMeta: *kvcp.ObjectMeta.DeepCopy(),
		Status:     *kvcp.Status.DeepCopy(),
	}
	// 每轮重新解析地址，防止后端删除后 Pool 继续发布旧端点，或把 phase 算成已解析。
	holder.Status.ClientEndpoint = ""

	kvcb, resolveErr := r.resolveKVCachePoolBackend(ctx, kvcp)
	if resolveErr != nil {
		if resolveErr.internal != nil {
			logger.Error(resolveErr.internal, "resolve kv cache pool backend")
			return ctrl.Result{}, resolveErr.internal
		}
		KVCachePoolConditionBackendResolved.False(holder, resolveErr.reason, resolveErr.message)
		// 后端已经消失时，顺便释放正等待删除的 Binding。其账本所在的 master 已不存在，再等也没有条
		// 目可删。Binding 的事件只会触发所属 Pool，而该 Pool 每轮都会在这里提前返回。
		if resolveErr.gone {
			if err = r.releaseOrphanKVCachePoolBindings(ctx, kvcp.Name); err != nil {
				return ctrl.Result{}, err
			}
		}
		// 后端启动后会把地址写到自身 status。Pool 控制器没有监听这个对象，因此定时重试。
		return r.syncKVCachePoolStatus(ctx, kvcp, r.summarizeKVCachePool(holder),
			ctrl.Result{RequeueAfter: kvCachePoolObserveInterval})
	}

	// 找到后端便立即登记占用关系，即使它还没有发布端点。这样在 Pool 等待后端启动期间，后端的清理流程也
	// 能看到这个使用方。
	if err = r.claimKVCacheBackend(ctx, kvcb, kvcp); err != nil {
		if isStaleKVCacheBackendWrite(err) {
			logger.V(1).Info("claim kv cache backend deferred, the backend changed or was deleted since it was read",
				"backend", kvcb.Name, "reason", err.Error())
			return _requeueAfterConflict, nil
		}
		logger.Error(err, "claim kv cache backend", "backend", kvcb.Name)
		return ctrl.Result{}, err
	}

	clientAddress, adminAddress := kvCacheBackendAddresses(kvcb)
	if adminAddress == "" {
		KVCachePoolConditionBackendResolved.False(holder, "EndpointsNotPublished",
			fmt.Sprintf("backend %q publishes no %q address yet, so its quota ledger cannot be "+
				"reached; no address is derived from the backend's name, because one that happened "+
				"to resolve would drive the wrong master",
				kvcb.Name, workercore.KVCacheBackendEndpointNameAdmin))
		return r.syncKVCachePoolStatus(ctx, kvcp, r.summarizeKVCachePool(holder),
			ctrl.Result{RequeueAfter: kvCachePoolObserveInterval})
	}
	KVCachePoolConditionBackendResolved.True(holder, "Resolved",
		fmt.Sprintf("backend %q is reachable", kvcb.Name))
	// Pool 只公布推理引擎连接的 Client 地址。Admin 地址允许写配额账本，Pool 是集群级对象，拥有其读权限
	// 的用户都能看到它的 status。
	holder.Status.ClientEndpoint = clientAddress

	// 同一后端上的所有 Pool 共用 master 的账本和策略文件。下列步骤须汇总所有这些 Pool。
	master, err := r.observeKVCachePoolMaster(ctx, kvcb)
	if err != nil {
		logger.Error(err, "read the pools and bindings on one backend")
		return ctrl.Result{}, err
	}
	holder.Status.Domains = master.domainsOf(kvcp)
	holder.Status.UsedBy = master.usedByOf(kvcp)
	observeKVCachePoolQuotaWithinTotal(kvcp, holder, master)

	// 在首次写外部账本前先为 Binding 添加终结器。若登记后 Binding 先被删除，系统就可能失去唯一能说明该
	// 条目归属的记录。添加终结器失败时重新入队即可。
	if err = r.lockKVCachePoolBindings(ctx, master); err != nil {
		return ctrl.Result{}, err
	}

	if err = r.syncQuotaPolicyConfigMap(ctx, kvcb, master.tenants); err != nil {
		logger.Error(err, "sync kv cache quota policy configmap")
		return ctrl.Result{}, err
	}

	admin := &mooncake.AdminClient{Address: adminAddress, HTTP: r.AdminHTTP}
	ledger := r.convergeTenantLedger(ctx, kvcb, admin, master, holder)
	observeKVCachePoolUsage(holder, ledger)

	// 若 master 因域中仍有对象而拒绝删除配额，必须再渲染一次，把仍生效的域补回种子策略。首次渲染的期望
	// 列表已排除正在释放的 Binding。若此时 leader 重启，初始化容器会复制种子文件，遗漏的策略将提前丢失。
	// 仅在有域被保留时需要这次额外渲染。
	//
	// 多个 Binding 争用的域也要保留种子中的现有配额。控制器会保留该域的账本条目，重启后的策略须与其一致。
	if len(ledger.retained) > 0 || len(master.contested) > 0 {
		tenants := withContestedTenants(master, ledger.observed)
		if len(ledger.retained) > 0 {
			tenants = withRetainedTenants(master, ledger.retained, tenants)
		}
		if err = r.syncQuotaPolicyConfigMap(ctx, kvcb, tenants); err != nil {
			logger.Error(err, "re-sync kv cache quota policy configmap with the preserved tenants")
			return ctrl.Result{}, err
		}
	}

	// 用同一次采样先写各 Binding，再写 Pool。Pool 的 phase 汇总它们的状态，避免 Pool 已显示 Ready，而各
	// Binding 仍显示上一轮的额度。
	if err = r.syncKVCachePoolBindings(ctx, kvcp, master, ledger); err != nil {
		// 写 Binding 时若发生冲突或对象已删除，相关变更会被 Binding 的监听器捕获，并触发下一轮调谐。
		return objectWriteResult(logger, err, "sync kv cache pool bindings", ctrl.Result{})
	}

	return r.syncKVCachePoolStatus(ctx, kvcp, r.summarizeKVCachePool(holder),
		ctrl.Result{RequeueAfter: kvCachePoolObserveInterval})
}

// kvCachePoolResolveError 描述本轮为何无法使用后端，区分可报告给用户的配置问题与内部读取错误。
//
// 配置问题写入 Pool 条件并更新 status，控制器的读取错误则原样返回队列重试。这样既不会为拼错的名字无限重试，
// 也不会把缓存读取故障藏在 status 中。
type kvCachePoolResolveError struct {
	reason  string
	message string
	// Gone 表明后端确实不存在，与这次未能确定下来的情况不同。这种差异决定了是否应该删除已标记为删除的
	// Binding：其账本记录存在于一个不存在的 Master 上，因此没有任何东西应归它所有，而保留终结器则会导致
	// 对象永远无法释放。资源池中指定错误的后端数量并非问题所在——某个名称下的 Master 可能完全正常运作，
	// 因此它仍会继续存在。
	gone     bool
	internal error
}

// resolveKVCachePoolBackend 读取 Pool 唯一引用的后端。
func (r *KVCachePoolReconciler) resolveKVCachePoolBackend(
	ctx context.Context, kvcp *workercore.KVCachePool,
) (*workercore.KVCacheBackend, *kvCachePoolResolveError) {
	// 准入要求恰有一个后端。调谐还必须检查实际存储的数据，对于规则启用前或绕过 webhook 写入的对象，报告
	// 错误并等待修正。
	if len(kvcp.Spec.Backends) != 1 {
		return nil, &kvCachePoolResolveError{
			reason: "BackendNotSingular",
			message: fmt.Sprintf("this pool names %d backends and exactly 1 is supported: quota is "+
				"enforced by one master's ledger", len(kvcp.Spec.Backends)),
		}
	}

	name := kvcp.Spec.Backends[0]
	kvcb := new(workercore.KVCacheBackend)
	if err := r.Client.Get(ctx, ctrlcli.ObjectKey{Name: name}, kvcb, ctrlclix.WithoutQuorum); err != nil {
		if !kerrors.IsNotFound(err) {
			return nil, &kvCachePoolResolveError{internal: err}
		}
		// 准入要求其存在，可能自那时起已被删除。基于级别的：条件规定如此，下一次检查将再次进行。
		return nil, &kvCachePoolResolveError{
			reason:  "BackendNotFound",
			message: fmt.Sprintf("backend %q does not exist", name),
			gone:    true,
		}
	}

	return kvcb, nil
}

// kvCacheBackendAddresses 按角色名称读取后端公布的 Client 和 Admin 地址。
//
// status.endpoints 以 name 为键，顺序可变。必须按名称查找，否则列表首项可能属于另一角色。没找到就返回空地址，
// 按对象名猜测可能让 Pool 连到错误的 master。
func kvCacheBackendAddresses(kvcb *workercore.KVCacheBackend) (client, admin string) {
	for i := range kvcb.Status.Endpoints {
		endpoint := &kvcb.Status.Endpoints[i]
		switch endpoint.Name {
		case workercore.KVCacheBackendEndpointNameClient:
			client = endpoint.Address
		case workercore.KVCacheBackendEndpointNameAdmin:
			admin = endpoint.Address
		}
	}
	return client, admin
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

// kvCachePoolMaster 保存本轮为同一个 master 一次性收集的 Pool、Binding 和租户信息。
//
// 这是整个资源池的通用配置，而非单个资源池的通用配置，这就是该类型存在的意义：绑定到后端的每个资源池都会共
// 享租户账本和策略文件，因此只了解自身资源池的进程会写入描述部分内容的文档，并抹除其余内容。
type kvCachePoolMaster struct {
	// tenants 就是理想的账本：每个要求上限的绑定项对应一条记录，这些记录分布在此 Master 上的每个资源池
	// 中，且顺序确定，因此同一状态的两次渲染结果在字节层面是完全相同的。
	tenants []mooncake.QuotaPolicyTenant

	// domains 按名称记录这个 master 上所有被 Binding 声明的复用域。有域名但没有请求额度时，域仍被占用，
	// 账本中则没有配额条目。
	domains map[string]kvCachePoolDomainClaim

	// registered 表示该域是否为 Master 资源池中已发布的状态。这是确保删除安全的关键：如果 Master 账本中
	// 有一条记录表明该操作员从未注册过该条目，那么该条目属于其他人——可能是外部后端为那些此处未创建的租
	// 户提供推理服务——删除该条目会导致数据丢失，且无法追踪是谁要求的。
	registered map[string]struct{}

	// 每个领域都存在不止一个绑定声明，每个声明者——包括那些从上述领域中被移除的人——都适用。这些声明被保
	// 留而非计数，因为双方都必须在自己的状态中提及对方，而计数无法说明是谁。
	contested map[string][]workercore.KVCachePoolBindingReference

	// bindings 是指此 Master 上每个资源池的每一个绑定，以资源池名称为键进行标识。这些绑定会将状态信息写
	// 入其中，而这些信息源自构建声明列表中的同一列表：第二次查询将允许针对未包含该绑定的合并账本报告在
	// 中间创建到的绑定。
	bindings map[string][]workercore.KVCachePoolBinding
}

// kvCachePoolDomainClaim 记录某个 Binding 对复用域的占用声明。
type kvCachePoolDomainClaim struct {
	pool    string
	binding workercore.KVCachePoolBindingReference
	domain  workercore.KVCachePoolBindingDomain
}

// domainsOf 根据全局主数据对资源池的注册项进行构建。
//
// 这是权威性的规定，而非建议性意见：某个条目存在是因为有绑定声明如此设定。每个条目所包含的数据量并未在此列
// 出——一个尚未被任何人挖掘的领域在报告中会保留这些数据，而不会用零值表示。
func (m *kvCachePoolMaster) domainsOf(kvcp *workercore.KVCachePool) []workercore.KVCachePoolDomain {
	names := make([]string, 0, len(m.domains))
	for name, claim := range m.domains {
		if claim.pool == kvcp.Name {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return nil
	}
	slices.Sort(names)

	domains := make([]workercore.KVCachePoolDomain, 0, len(names))
	for _, name := range names {
		claim := m.domains[name]
		domains = append(domains, workercore.KVCachePoolDomain{
			Name:      name,
			Binding:   claim.binding,
			BlockSize: claim.domain.BlockSize,
			Dtype:     claim.domain.Dtype,
		})
	}
	return domains
}

// usedByOf 列出了属于同一资源池的绑定对象，这些绑定对象来自同一个列表，而声明也是从该列表中构建的。
//
// 这是一个单范围查询，对应集群范围内对象允许查找的方向：该资源池通过索引读取其绑定，且没有任何绑定会跨命名
// 空间进行读取以查找它。这些条目带有 REAL 命名空间，与这个资源池写入后端时的命名空间不同，因为绑定属于特定
// 命名空间，而命名空间是其身份的一部分。
func (m *kvCachePoolMaster) usedByOf(kvcp *workercore.KVCachePool) []workercore.KVCacheObjectReference {
	bindings := m.bindings[kvcp.Name]
	if len(bindings) == 0 {
		return nil
	}

	usedBy := make([]workercore.KVCacheObjectReference, 0, len(bindings))
	for i := range bindings {
		// 本次即将释放的绑定项已不再属于持有者：同一项会解除锁定，因此将其列入列表会发布一个在状态
		// 更新时已不存在的持有者。而正在被删除的某项目仍会留在列表中，因为它仍然存在，其删除行为在
		// 持有者释放之前也不会发生。
		if kvCachePoolBindingIsReleasing(&bindings[i]) {
			continue
		}
		usedBy = append(usedBy, workercore.KVCacheObjectReference{
			Kind:      KVCachePoolBindingKind,
			Namespace: bindings[i].Namespace,
			Name:      bindings[i].Name,
		})
	}
	if len(usedBy) == 0 {
		return nil
	}
	// 对域排序，使相同状态产生相同列表，避免仅因 List 返回顺序变化就重写 status。
	slices.SortFunc(usedBy, compareKVCacheObjectReferences)
	return usedBy
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

// kvCachePoolBindingIsReleasing 报告称一个绑定正在被删除，且没有任何东西再被持有。
//
// 待删除且已无使用方的 Binding 不再参与期望账本，这样控制器才能先删其条目，再移除终结器。若工作负载仍持有
// Binding，则保留其占用记录和配额，让仍在进行的写入继续。
func kvCachePoolBindingIsReleasing(kvcpb *workercore.KVCachePoolBinding) bool {
	return kvcpb.DeletionTimestamp != nil && len(kvcpb.Status.UsedBy) == 0
}

// observeKVCachePoolMaster 会收集所有资源池及这些池的每一个绑定对象，并将它们集中到同一个后端。
//
// 先按后端索引找出所有兄弟 Pool，再按各 Pool 的索引查其 Binding，无须遍历命名空间。若只取当前 Pool，每次写
// 策略文件都会删掉其他 Pool 的租户。
func (r *KVCachePoolReconciler) observeKVCachePoolMaster(
	ctx context.Context, kvcb *workercore.KVCacheBackend,
) (*kvCachePoolMaster, error) {
	pools := new(workercore.KVCachePoolList)
	err := r.Client.List(ctx, pools,
		ctrlcli.MatchingFields{IndexingKVCachePoolByBackend: kvcb.Name})
	if err != nil {
		return nil, fmt.Errorf("list the pools on backend %q: %w", kvcb.Name, err)
	}

	master := &kvCachePoolMaster{
		domains:    map[string]kvCachePoolDomainClaim{},
		registered: map[string]struct{}{},
		contested:  map[string][]workercore.KVCachePoolBindingReference{},
		bindings:   map[string][]workercore.KVCachePoolBinding{},
	}
	claimants := map[string][]workercore.KVCachePoolBindingReference{}

	for i := range pools.Items {
		pool := &pools.Items[i]

		// 该索引包含 spec.backends 中的每一项条目，因此将其中两项命名为资源池的实例会同时出现在这两
		// 个条目下。resolveKVCachePoolBackend 根本不提供此类资源池——它会报告 BackendNotSingular 错
		// 误，并声称没有哪个后端可用——本次处理必须与其保持一致，否则资源池的上限限制将在两个主账本
		// 中通过两次协调操作被写入，而该资源池本身则不会在这两个账本中留下任何记录。
		//
		// 拒绝提供服务并不等于不关注它：其域名仍会进入注册列表，其绑定项仍会出现在下面的快照中，因
		// 为前一次操作留下的条目只有在其名称仍被知晓时才能被删除，而清理程序仍需执行。被注册但未被
		// 需要，正是 convergeTenantLedger 所删除的内容，对于没有后端服务的资源池来说，这也是正确的结果。
		served := len(pool.Spec.Backends) == 1

		// 这是上次资源池里的内容。在绑定之前请阅读，因为它描述了当前 Master 账本里实际存在的状况，
		// 而非应有的状态。
		for j := range pool.Status.Domains {
			master.registered[pool.Status.Domains[j].Name] = struct{}{}
		}

		bindings := new(workercore.KVCachePoolBindingList)
		err = r.Client.List(ctx, bindings,
			ctrlcli.MatchingFields{IndexingKVCachePoolBindingByPool: pool.Name})
		if err != nil {
			return nil, fmt.Errorf("list the bindings of pool %q: %w", pool.Name, err)
		}

		master.bindings[pool.Name] = bindings.Items

		for j := range bindings.Items {
			kvcpb := &bindings.Items[j]
			name := kvcpb.Spec.Domain.Name
			if name == "" {
				continue
			}
			// 本轮即使不再请求这个域，也要记录其归属。写账本可能成功而随后更新 Pool status 失败。
			// 如果只依赖上一轮 status，删除流程就可能永远找不到控制器已经创建的条目。
			master.registered[name] = struct{}{}

			if kvCachePoolBindingIsReleasing(kvcpb) || !served {
				// 占用已结束，或该 Pool 已失去有效后端。将 Binding 留在 master.bindings 以
				// 便释放终结器，但从期望域和租户集合中移除。账本差异会触发 DELETE，重新生成
				// 的策略文件也会移除该项。
				continue
			}

			ref := workercore.KVCachePoolBindingReference{
				Namespace: kvcpb.Namespace,
				Name:      kvcpb.Name,
			}
			claimants[name] = append(claimants[name], ref)
			master.domains[name] = kvCachePoolDomainClaim{
				pool:    pool.Name,
				binding: ref,
				domain:  kvcpb.Spec.Domain,
			}

			master.tenants = append(master.tenants, mooncake.QuotaPolicyTenant{
				Name:  name,
				Quota: kvcpb.Spec.Quota.Ceiling,
			})
		}
	}

	// 一个涉及两个绑定请求的领域被同时管理，其账本记录保持原样。准入拒绝第二个请求，因此导致两个绑定请
	// 求竞争一个缓存——在此选择胜者将给一个命名空间设定上限，另一个则没有。这些请求者被保留下来，以便两
	// 个绑定可以互相命名；只是运作就此停止。
	for name, refs := range claimants {
		if len(refs) > 1 {
			master.contested[name] = refs
			delete(master.domains, name)
			// 也将其从注册列表中移除，这就是“保持原样”这一说法的由来。Convergence 会删除那些已
			// 注册但非所需的条目；仅从租户名单中移除该名称，就会使其从所需列表中消失，同时仍保
			// 留在注册列表中。因此，两个绑定系统正在争论的条目，将会被那个声称不干预的进程删除—
			// —两者都会随之丢失缓存。
			delete(master.registered, name)
		}
	}
	master.tenants = slices.DeleteFunc(master.tenants, func(t mooncake.QuotaPolicyTenant) bool {
		_, contested := master.contested[t.Name]
		return contested
	})

	// 对同一批租户保持稳定排序。列表顺序一致时，重复渲染得到相同内容，无需重写 ConfigMap。
	slices.SortFunc(master.tenants, func(a, b mooncake.QuotaPolicyTenant) int {
		return strings.Compare(a.Name, b.Name)
	})

	return master, nil
}

// withContestedTenants 将一个域重新放入目标文档中，使得两个绑定实体开始对之产生争议。
//
// 有争议的分支将其从目标租户中移除，因此没有一方的上限被写入，并且故意保持其账本条目保持原样。种子代码也必
// 须执行相同的操作：它在每个容器启动时根据 Master 的自身策略被复制，因此如果文档中省略了域信息，在下一个主
// 节点重启时就会失去其明确的配额，并开始拒绝两个命名空间仍使用的缓存的写入操作——这是 API 在准入阶段就已经拒
// 绝的冲突在数据层面产生的结果。
//
// 保留 master 已持有的配额上限。域名冲突时，若采用任一方声明的数值，就等于替它赢得了本应暂缓裁决的争议。
func withContestedTenants(
	master *kvCachePoolMaster, observed []mooncake.TenantQuota,
) []mooncake.QuotaPolicyTenant {
	if len(master.contested) == 0 {
		return master.tenants
	}

	tenants := slices.Clone(master.tenants)
	for i := range observed {
		entry := &observed[i]
		if _, contested := master.contested[entry.TenantID]; !contested {
			continue
		}
		if !entry.HasExplicitPolicy {
			// 没有需要保留的内容：该领域遵循 Master 的默认设置，在此处编写相关说明会制定本次会
			// 议没有相关方要求的明确政策。
			continue
		}
		tenants = append(tenants, mooncake.QuotaPolicyTenant{
			Name: entry.TenantID,
			// 种子文件应写租户申请的配额上限。实际 grant 由 master 按当时可分配容量计算，若把它
			// 写回上限，每次重新分配都有可能永久压低上限。
			Quota: *resource.NewQuantity(entry.RequestedBytes, resource.BinarySI),
		})
	}

	slices.SortFunc(tenants, func(a, b mooncake.QuotaPolicyTenant) int {
		return strings.Compare(a.Name, b.Name)
	})
	return tenants
}

// withRetainedTenants 会将 Master 不会删除的条目重新放入目标文档中。
//
// 他们的绑定正在被移除，因此 observeKVCachePoolMaster 被排除在外——这对于账本来说是正确的，因为此次移除正是
// 本次操作想要实现的。但对于 SEED 来说则不正确，因为主节点在每次容器启动时都会按照自己的策略来覆盖它。在拒
// 绝和排空之间，该域仍然处于运行状态，如果在那个窗口期重新启动，就会连同其策略一起被移除。
//
// 上限来自绑定本身的释放，它仍存在于快照中：这是该条目所记录的数值，因此将其恢复即可重现文档内容，而非创造新的内容。
func withRetainedTenants(
	master *kvCachePoolMaster, retained []string, base []mooncake.QuotaPolicyTenant,
) []mooncake.QuotaPolicyTenant {
	tenants := slices.Clone(base)
	for _, domain := range retained {
		if slices.ContainsFunc(tenants, func(t mooncake.QuotaPolicyTenant) bool {
			return t.Name == domain
		}) {
			continue
		}
		if ceiling, ok := retainedTenantCeiling(master, domain); ok {
			tenants = append(tenants, mooncake.QuotaPolicyTenant{Name: domain, Quota: ceiling})
		}
	}

	// 采用与快照相同的顺序，因此在此重建的文档与账本更新前的文档相比，差异清晰可辨。
	slices.SortFunc(tenants, func(a, b mooncake.QuotaPolicyTenant) int {
		return strings.Compare(a.Name, b.Name)
	})
	return tenants
}

// retainedTenantCeiling 表示该域在此主节点上的所有资源池中的最高注册级别。没有绑定要求的条目被遗漏了：这里
// 没有任何内容能说明它原本想要什么。
func retainedTenantCeiling(
	master *kvCachePoolMaster, domain string,
) (ceiling resource.Quantity, found bool) {
	for _, bindings := range master.bindings {
		for i := range bindings {
			if bindings[i].Spec.Domain.Name == domain {
				return bindings[i].Spec.Quota.Ceiling, true
			}
		}
	}
	return resource.Quantity{}, false
}

// observeKVCachePoolQuotaWithinTotal 计算各 Binding 声明的配额上限之和，更新它是否超过 Pool 配额总量的条件。
//
// 合计时跳过已在释放、域名冲突或域名为空的 Binding，使本轮的条件反映与期望租户集合一致的变更。删除 Binding
// 缓解超订后，无须等下一次 master 采样即可看到结果。
//
// 一个特殊情况是 Pool 指定了多个后端。这里仍可对 Kubernetes 对象做算术检查，但没有唯一 master 可以接收这些
// ceiling。后端数量错误由后端解析条件报告，此处的合计只说明声明本身。
//
// 这里只写 Pool 的条件，不拒绝 Binding 或修改租户额度。该检查完全基于集群里的声明，master 暂时不可达时也可执行。
func observeKVCachePoolQuotaWithinTotal(
	kvcp, holder *workercore.KVCachePool, master *kvCachePoolMaster,
) {
	// 累加器沿用 total 的格式，条件消息中的合计和上限都以相同单位显示，避免把 17179869184 和 16Gi 并排比较。
	sum := resource.Quantity{Format: kvcp.Spec.Quota.Total.Format}
	excluded := sets.New[string]()
	for i := range master.bindings[kvcp.Name] {
		kvcpb := &master.bindings[kvcp.Name][i]
		name := kvcpb.Spec.Domain.Name
		if name == "" || kvCachePoolBindingIsReleasing(kvcpb) {
			continue
		}
		if _, contested := master.contested[name]; contested {
			excluded.Insert(name)

			continue
		}
		sum.Add(kvcpb.Spec.Quota.Ceiling)
	}

	total := kvcp.Spec.Quota.Total
	if sum.Cmp(total) <= 0 {
		// 消息须说明本次合计实际包含哪些 Binding。发生争用的域不写新配额，其 ceiling 也未计入总和，
		// 不能在消息中声称检查了所有 Binding。
		if excluded.Len() > 0 {
			KVCachePoolConditionQuotaWithinTotal.True(holder, "WithinTotal", fmt.Sprintf(
				"every uncontested binding's ceiling fits within the pool's declared total: those "+
					"ceilings sum to %s against a total of %s. Whether the master grants them in full "+
					"depends on its allocatable capacity, and each binding's status.effectiveQuota "+
					"reports the grant. The contested domains %s are left "+
					"out of that sum, and no ceiling is written for them at all, so this comparison "+
					"says nothing about what they asked for",
				sum.String(), total.String(), strings.Join(sets.List(excluded), ", "),
			))

			return
		}
		KVCachePoolConditionQuotaWithinTotal.True(holder, "WithinTotal",
			fmt.Sprintf("every binding's ceiling fits within the pool's declared total: the ceilings "+
				"sum to %s against a total of %s. Whether the master grants them in full depends on "+
				"its allocatable capacity, and each binding's status.effectiveQuota reports the grant",
				sum.String(), total.String()))

		return
	}

	KVCachePoolConditionQuotaWithinTotal.False(holder, KVCachePoolReasonOversubscribed,
		fmt.Sprintf("the bindings' ceilings sum to %s, over the pool's declared total of %s. "+
			"Nothing is refused and nothing is broken: the store grants every tenant a share of what "+
			"it can serve in proportion to what each asked, and each binding's "+
			"status.effectiveQuota reports the figure actually granted",
			sum.String(), total.String()))
}

// lockKVCachePoolBindings 会在此 Master 上的每个活跃绑定上调用终结器，并且必须在集群外部进行任何操作之前执行。
//
// 删除租户只有名字可用。master.registered 从各 Pool 已公布的域和存活 Binding 建立归属表。若在两者记录域名之
// 前 Binding 就被删除，控制器此后无法证明账本条目是自己创建的，也无法安全删除它。
//
// 在每池绑定阶段进行锁定太晚：策略文档和账本会先合并，因此外部写入与该阶段的冲突会导致一个没有终结器且条目
// 已存在于主表中的绑定可删除对象。
//
// 在写账本前保护同一 master 上所有 Pool 的 Binding。master.tenants 汇总了它们各自请求的配额，
// convergeTenantLedger 将逐项 PUT。只锁当前 Pool 会使其他 Pool 刚写入的条目失去归属保护。
//
// 已被标记为删除的绑定对象将被跳过，而非锁定：如果再次在 API 服务器正准备移除的对象上添加终结器，将会导致该
// 对象无法被移除。
func (r *KVCachePoolReconciler) lockKVCachePoolBindings(
	ctx context.Context, master *kvCachePoolMaster,
) error {
	logger := ctrllog.FromContext(ctx)

	for pool := range master.bindings {
		for i := range master.bindings[pool] {
			kvcpb := &master.bindings[pool][i]

			if kvcpb.DeletionTimestamp != nil || systemmeta.Lock(kvcpb) {
				continue
			}
			if err := r.Client.Update(ctx, kvcpb); err != nil {
				logger.Error(err, "lock kv cache pool binding before converging the master",
					"kv cache pool binding", ctrlcli.ObjectKeyFromObject(kvcpb))
				return err
			}
		}
	}

	return nil
}

// syncQuotaPolicyConfigMap 为同一 master 写入完整的期望配额策略。
//
// 每次都从期望租户集合生成并覆盖整份文档。master 通过 Admin API 修改的是自身可写的副本，不能把那份副本当作
// 控制器下一轮的期望状态。
//
// 渲染器一旦拒绝输入，就终止本轮。下一步将对同一批租户逐项 PUT，且不会再次校验。继续写会让账本接受种子文件
// 没有的条目，导致运行时和重启后的配额不一致。
//
// 永远重新尝试是成本最低的选择。这里的每个输入都通过同一个验证器运行的 webhook 进行处理，因此拒绝要么是围绕
// 准入机制编写的错误代码，要么就是这个操作中的漏洞；这两种情况都容易被公开指出，且都无法被解决。
func (r *KVCachePoolReconciler) syncQuotaPolicyConfigMap(
	ctx context.Context, kvcb *workercore.KVCacheBackend, tenants []mooncake.QuotaPolicyTenant,
) error {
	logger := ctrllog.FromContext(ctx)

	document, err := mooncake.RenderQuotaPolicy(tenants)
	if err != nil {
		logger.Error(err, "render the tenant quota policy",
			"backend", kvcb.Name, "tenants", len(tenants))
		return fmt.Errorf("render the tenant quota policy of backend %q: %w", kvcb.Name, err)
	}

	eCM := mooncake.RenderQuotaPolicyConfigMap(kvcb, document)
	_, err = kubeclientset.CreateWithCtrlClient(ctx, r.Client, eCM,
		kubeclientset.WithUpdateIfExisted(alignQuotaPolicyConfigMapFn(kvcb, eCM)))
	if err != nil {
		return err
	}

	logger.V(2).Info("synced the tenant quota policy", "backend", kvcb.Name, "tenants", len(tenants))
	return nil
}

// alignQuotaPolicyConfigMapFn 使已有的策略 ConfigMap 与本轮渲染结果一致。
//
// 只比较本控制器生成的文档、归属注解和所有者引用。其他组件可以添加自己的标签或注解，无需为此重写 ConfigMap。
//
// 第二个返回值为 SKIP，而非 "changed"。返回 false 且没有任何变化会在每次迭代中重写对象；返回 nil 对象则会在
// 调用者内部引发 panic。
func alignQuotaPolicyConfigMapFn(
	kvcb *workercore.KVCacheBackend, eCM *core.ConfigMap,
) func(*core.ConfigMap) (*core.ConfigMap, bool, error) {
	return func(aCM *core.ConfigMap) (*core.ConfigMap, bool, error) {
		skip := true

		if !kubemeta.DeepEqual(aCM.Data, eCM.Data) {
			aCM.Data = eCM.Data
			skip = false
		}

		if !systemmeta.EqualResourceTypeAndNotes(eCM, aCM) {
			systemmeta.SyncResourceTypeAndNotes(eCM, aCM)
			skip = false
		}

		// 如果没有这一点，ConfigMap 即使早于所有者引用创建——或者是由他人编辑的——也会持续存在，直到
		// 其所属的后端服务被删除为止；其他任何操作都无法将其删除：后端服务自身的清理过程并不知晓该对象的存在。
		if !kubemeta.IsControlledBy(aCM, kvcb) {
			kubemeta.ControlOnWithoutBlock(aCM, kvcb,
				workercore.SchemeGroupVersion.WithKind("KVCacheBackend"))
			skip = false
		}

		return aCM, skip, nil
	}
}

// convergeTenantLedger 根据所有相关 Binding 的期望，使 master 上的实际租户账本收敛。
//
// 仅在观测到的申请额度不同或没有显式策略时 PUT。只对控制器登记过且已无人申请的条目执行 DELETE。外部 master
// 的其他管理方所建的租户要原样保留。
//
// 若管理型后端声明关闭多租户，或外部 master 在首次读取时返回 UNAVAILABLE_IN_CURRENT_MODE，则没有租户条目需
// 要收敛。仍采集指标，并返回已收敛且 noLedger 为真的结果，使正在释放的 Binding 可以解除终结器。
//
// 通过条件记录 master 的响应。对方明确拒绝配置时，盲目把同一错误返回队列重试也不会改变结果，用户需先从条件中看见原因。
func (r *KVCachePoolReconciler) convergeTenantLedger(
	ctx context.Context,
	kvcb *workercore.KVCacheBackend,
	admin *mooncake.AdminClient,
	master *kvCachePoolMaster,
	holder *workercore.KVCachePool,
) kvCachePoolLedgerPass {
	logger := ctrllog.FromContext(ctx)

	// 管理型后端的多租户标志由控制器写入 leader 的启动参数。声明关闭多租户时，直接跳过只有启用账本才能回答的查询。
	if managed := kvcb.Spec.Connection.Managed; managed != nil && !managed.Leader.MultiTenancyEnabled() {
		r.reportTenantLedgerAbsent(holder)
		metrics, scraped := r.observeAllocatableCapacity(ctx, admin, holder, true)
		return kvCachePoolLedgerPass{converged: true, noLedger: true, metrics: metrics, scraped: scraped}
	}

	observed, err := admin.ListTenantQuotas(ctx)
	if err != nil {
		r.reportTenantLedgerFailure(holder, err, "reading the tenant ledger")
		logger.Error(err, "list tenant quotas")
		if errors.Is(err, mooncake.ErrMultiTenancyDisabled) {
			// 外部 master 通过实际响应说明是否支持账本。若它未启用多租户，就跳过账本写入，仍采集可用容量。
			metrics, scraped := r.observeAllocatableCapacity(ctx, admin, holder, true)
			return kvCachePoolLedgerPass{converged: true, noLedger: true, metrics: metrics, scraped: scraped}
		}
		return kvCachePoolLedgerPass{failure: err}
	}

	held := make(map[string]mooncake.TenantQuota, len(observed))
	for _, entry := range observed {
		held[entry.TenantID] = entry
	}

	desired := make(map[string]struct{}, len(master.tenants))
	for _, tenant := range master.tenants {
		desired[tenant.Name] = struct{}{}

		// Webhook 和策略渲染器共用数量校验器，已拒绝无法转成 int64 字节数的值，因此这里可以调用 Value()。
		want := tenant.Quota.Value()
		if entry, ok := held[tenant.Name]; ok && entry.HasExplicitPolicy && entry.RequestedBytes == want {
			continue
		}
		if err = admin.PutTenantQuota(ctx, tenant.Name, want); err != nil {
			r.reportTenantLedgerFailure(holder, err,
				fmt.Sprintf("writing the quota of reuse domain %q", tenant.Name))
			logger.Error(err, "put tenant quota", "tenant", tenant.Name)
			return kvCachePoolLedgerPass{failure: err, observed: observed}
		}
	}

	// 记录 master 因域中仍有对象而未能删除的条目名。正在释放的 Binding 需要知道被保留的是否是它自己的域，单有数量不够。
	var retained []string

	for _, entry := range observed {
		if _, wanted := desired[entry.TenantID]; wanted {
			continue
		}
		if _, ours := master.registered[entry.TenantID]; !ours {
			// 该租户属于这个 master 的其他使用方，当前控制器未登记它。
			logger.V(2).Info("leaving a ledger entry this operator never registered",
				"tenant", entry.TenantID)
			continue
		}
		if !entry.HasExplicitPolicy {
			// 控制器只写显式策略，删除时也要检查 HasExplicitPolicy。这与上面 PUT 时识别已有条目的规则一致。
			logger.V(2).Info("leaving a ledger entry that carries no explicit policy",
				"tenant", entry.TenantID)
			continue
		}
		if err = admin.DeleteTenantQuota(ctx, entry.TenantID); err != nil {
			if errors.Is(err, mooncake.ErrTenantNotEmpty) {
				// 域仍有对象时可以等它排空，但这次删除还未完成。把域名记为保留，防止
				// Binding 提前解除终结器，否则 master 会留下无从确认归属的条目。
				logger.V(2).Info("a reuse domain still holds objects, so its quota stays",
					"tenant", entry.TenantID)
				retained = append(retained, entry.TenantID)
				continue
			}
			r.reportTenantLedgerFailure(holder, err,
				fmt.Sprintf("removing the quota of reuse domain %q", entry.TenantID))
			logger.Error(err, "delete tenant quota", "tenant", entry.TenantID)
			return kvCachePoolLedgerPass{failure: err, retained: retained, observed: observed}
		}
	}

	KVCachePoolConditionQuotaLedgerAvailable.True(holder, "Available",
		"the master holds a tenant ledger and it is writable")
	KVCachePoolConditionQuotaPolicyWritable.True(holder, "Writable",
		fmt.Sprintf("the master's ledger holds the %d reuse domain(s) asked for on this backend, "+
			"across every pool bound to it", len(master.tenants)))

	metrics, scraped := r.observeAllocatableCapacity(ctx, admin, holder, false)
	return kvCachePoolLedgerPass{
		converged: len(retained) == 0,
		retained:  retained,
		observed:  observed,
		metrics:   metrics,
		scraped:   scraped,
	}
}

// kvCachePoolLedgerPass 是遍历 Master 账本一次后返回的内容。
//
// 这两个标志看起来相似，实则不同。Converged 显示，本次所有的 PUT 和 DELETE 操作都被接受了；而 Gates 释放绑
// 定操作意味着：在拥有该对象的对象消失之前，该条目必须从 Master 中移除，否则它所持有的容量也将无法被回收——
// 账本中没有任何标签表明该条目属于谁。Scraped 则表明解释得到了回应，但这属于另一种自身会失效的界面；一个已
// 收敛的账本和一个未收敛的账本都是正常的状态，而绑定操作则保留它们所拥有的数据。
type kvCachePoolLedgerPass struct {
	converged bool
	// noLedger 表示 master 没有租户账本。这可以来自管理型后端的显式配置，也可以来自外部 master 对账本请
	// 求的明确拒绝。此时无需写入即可完成调谐，Binding 应报告配额未强制执行。
	noLedger bool
	// 失败是账本操作错误，导致本次传递无法收敛。它区分了没有生成账本的 Master，以及根本无法读取账本的 Master。
	failure error
	// retained 指的是 Master 不会删除的条目，因为它们的领域仍包含对象。它保持原样，而非被合并到单独的收
	// 敛结构中，并且在同一轮处理中的后续失败中依然存活：一个自身领域在此处的绑定可以说明为何其删除被推
	// 迟，而不是丢弃 Master 已经给出的答案。
	retained []string
	// 观察到的账本就是本次读取时的状态。它被保存下来，以便重新渲染时能够重现该批次刻意未写入的条目——这
	// 是一个有争议的领域，其数据必须来自 Master，而非任何一方的主张者。
	observed []mooncake.TenantQuota
	metrics  mooncake.TenantQuotaMetrics
	scraped  bool
}

// reportTenantLedgerAbsent 会记录一个没有租户账本的 Master 的两种状态：要么是系统自动识别到的状态，要么是管
// 理后端声明或 Master 自身拒绝的结果。该消息仅陈述事实及其两种后果，而非指出需要切换的标记：没有账本的
// Master 是一种操作员可以定义的拓扑结构，而不仅仅是需要修复的故障；此外，在所谓的后端情况下，也拒绝进行切换。
func (r *KVCachePoolReconciler) reportTenantLedgerAbsent(holder *workercore.KVCachePool) {
	KVCachePoolConditionQuotaLedgerAvailable.False(holder,
		KVCachePoolReasonMultiTenancyDisabled,
		"the master runs without multi-tenancy: it holds no tenant ledger, so every write falls "+
			"into the default tenant and no per-tenant quota is in force. Such a master serves "+
			"exactly one reuse domain, and usage is not accounted per domain")
	KVCachePoolConditionQuotaPolicyWritable.Unknown(holder,
		KVCachePoolReasonMultiTenancyDisabled,
		"a master with no tenant ledger accepts no quota to persist, so whether its quota policy "+
			"source is writable was not observed")
}

// 这就是关键所在。多租户功能被关闭后，就变成了一个需要重新配置的 BACKEND，而该资源池无法实现收敛；无法被重
// 写的策略源则变成了一个需要修复的 MOUNT，否则配额会显示为根本不生效的状态。其他情况则都不属于这种情况，他
// 们直接说明问题，而不是借用其他名称来指代。
//
// 每个结果都会标注两个维度，这就是该报告基于维度的特点。持有者从上一次的结果状态开始，因此本次未涉及的维度
// 会保留上一次的结果——这两个问题互不冲突。如果某次操作导致策略源拒绝写入，或者根本没有看到账本，那么
// MultiTenancyDisabled 就会随之出现：summarizeKVCachePool 会按顺序扫描这些维度，因此当本次的结果证明账本存
// 在时，资源池会报告需要重新配置后端。如果排序相反，则会出现镜像状态。本次无法观察到的维度会被设置为“未知”，
// 而不是保持原样，因为“本次未观察到”和“观察到正常”并非同一含义。
func (r *KVCachePoolReconciler) reportTenantLedgerFailure(
	holder *workercore.KVCachePool, err error, doing string,
) {
	switch {
	case errors.Is(err, mooncake.ErrMultiTenancyDisabled):
		r.reportTenantLedgerAbsent(holder)
	case errors.Is(err, mooncake.ErrQuotaPolicyNotWritable):
		// 这种拒绝行为本身便是账本的回应：Master 接受了请求，找到了租户账本来记录内容，但在下方的政
		// 策来源上却失败了。若在此处将账本描述为不可使用，就会与所处理的回应相矛盾。
		KVCachePoolConditionQuotaLedgerAvailable.True(holder, "Available",
			"the master holds a tenant ledger; what it could not do was persist the policy")
		KVCachePoolConditionQuotaPolicyWritable.False(holder,
			KVCachePoolReasonQuotaPolicyNotWritable,
			fmt.Sprintf("the master could not persist %s and so did not apply it: its tenant quota "+
				"policy source is not writable. The quota is NOT in force — this master writes the "+
				"policy before applying it, so nothing was accepted. A restart will not help; the "+
				"policy source has to be made writable", doing))
	default:
		// 并非所谓的缺陷。账本是未能回应的表面，因此故障被记录在 ITS 轴上，而政策来源——只有通过得到
		// 响应的账本写入才能触及——则未被关注，也不被归咎。
		KVCachePoolConditionQuotaLedgerAvailable.False(holder, "LedgerUnreachable",
			fmt.Sprintf("%s failed: %v", doing, err))
		KVCachePoolConditionQuotaPolicyWritable.Unknown(holder, "LedgerUnreachable",
			"the master's ledger did not answer, so whether it can persist a quota policy was not "+
				"observed")
	}
}

// observeKVCachePoolUsage 计算的是该资源池的租户所持有的数据量，占已通过检查的总数据量的比例。
//
// 绝不会成为整个资源池的掌控者。一个后端服务管理着多个资源池，其公开信息会涵盖所有租户——在此发布数据会导致
// 资源池的使用量增加，因为其他人的命名空间会进行写入操作，而阅读此数据的主体绝不应支持这种情况。
//
// 当抓取操作未返回响应时，状态为“absent”，且“absent”不等于零：以 0 形式序列化的总数表明缓存为空，而实际情况
// 是无人能够查看。抓取未提及的域名对整体结果没有任何贡献，也不会阻碍总和——其自身的绑定报告说明了该空白情况，
// 由于十个域名中有一个缺失，因此资源池的总数被隐藏了，这只会掩盖其余九个域名的情况。
func observeKVCachePoolUsage(holder *workercore.KVCachePool, ledger kvCachePoolLedgerPass) {
	if !ledger.scraped {
		holder.Status.Usage = nil
		return
	}

	// 一旦抓取完成且该资源池中的至少一个域名包含在其中，则零值会被公开而非隐藏：Master 被询问后表示该域
	// 名中没有任何内容，这就是一种衡量标准。如果文档中未提及的域名，则不属于这种情况——这与 Binding 中
	// TenantNotInLedger 所保持的“不存在即为零”的区分原则相同。对于此类域名，如果资源池显示值为 0，那么
	// 在 Master 重启后，普通状态下该资源池会报告“该资源池中没有任何内容”。
	var total int64
	var resolved int
	for i := range holder.Status.Domains {
		sample, known := ledger.metrics.Tenant(holder.Status.Domains[i].Name)
		if !known {
			continue
		}
		resolved++
		if occupancy, _ := sample.Occupancy(); occupancy != nil {
			total += *occupancy
		}
	}

	// 完全没有域名的资源池，是一种情况：答案是零，而非不存在：没有提出任何要求，因此也没有缺失任何东西。
	if resolved == 0 && len(holder.Status.Domains) > 0 {
		holder.Status.Usage = nil
		return
	}

	holder.Status.Usage = &workercore.KVCachePoolUsage{Total: quantityFromBytes(&total)}
}

// observeAllocatableCapacity 用于读取 Master 需要将其分配给租户的部分。
//
// 零值是启动排序中的陷阱：没有成员已挂载，因此每个租户的有效配额为零，无法进行写入操作。然而，集群中的每个
// 对象仍看起来配置正确，该资源池写入的所有配额也都被接受了。被抓取的数据会被返回，而不是丢弃，因为每次绑定
// 时，该资源池都会从自己的租户中读取数据。无论有多少次绑定，每次绑定都会进行一次抓取，因此该操作的请求数量
// 将受绑定到该资源池的命名空间数量的影响。
func (r *KVCachePoolReconciler) observeAllocatableCapacity(
	ctx context.Context, admin *mooncake.AdminClient, holder *workercore.KVCachePool,
	noLedger bool,
) (mooncake.TenantQuotaMetrics, bool) {
	metrics, err := admin.TenantQuotaMetrics(ctx)
	if err != nil {
		KVCachePoolConditionCapacityAllocatable.False(holder, "CapacityNotObserved",
			fmt.Sprintf("the master's tenant metrics could not be read: %v", err))
		return mooncake.TenantQuotaMetrics{}, false
	}

	capacity := metrics.AllocatableCapacityBytes
	if capacity == nil && noLedger {
		// 没有租户账本的 Master 根本不会显示租户配额指标；它仅会显示全局指标，对默认租户的任何写入
		// 操作也都会受其限制。
		capacity = metrics.TotalCapacityBytes
	}

	switch {
	case capacity == nil:
		KVCachePoolConditionCapacityAllocatable.False(holder, "CapacityNotObserved",
			"the master's exposition carries no allocatable capacity")
	case *capacity == 0:
		// reason 只陈述本次观测。成员尚未挂载内存段，或 leader 重启后已经开始响应但内部视图仍在重建，
		// 都可能产生相同读数。单 master 重启时曾观测到后者约需 30 秒（30 s）。
		KVCachePoolConditionCapacityAllocatable.False(holder, "NothingToAllocate",
			"the master reports nothing to allocate, so every reuse domain's effective quota is "+
				"zero and no write can succeed. Its members have either not mounted their segments "+
				"yet, or not finished remounting them after the master restarted")
	default:
		if noLedger && metrics.AllocatableCapacityBytes == nil {
			KVCachePoolConditionCapacityAllocatable.True(holder, "Allocatable",
				fmt.Sprintf("the master holds no tenant ledger, so its whole %d bytes are the "+
					"store's global capacity, bounding every write alike", *capacity))
			break
		}
		KVCachePoolConditionCapacityAllocatable.True(holder, "Allocatable",
			fmt.Sprintf("the master has %d bytes to divide between its reuse domains", *capacity))
	}

	// 即使容量计量器缺失或为零，仍会返回。这些反映了 Master 的情况；同一文档中的每个租户系列依然如故，
	// 而一个自身数据已到位的绑定对象应当报告这些数据，而非继承资源池的判定结果。
	return metrics, true
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

const (
	// KVCachePoolBindingReasonDomainClaimedByMultipleBindings 表示多个 Binding 同时声明了同一复用域。
	KVCachePoolBindingReasonDomainClaimedByMultipleBindings = "DomainClaimedByMultipleBindings"
	// KVCachePoolBindingReasonHeldByWorkloads 是一种无法删除的绑定：它是某种授权，若根据工作负载释放它，
	// 会导致该工作负载在资源池中写入内容，而没有任何记录表明其正在被使用。
	KVCachePoolBindingReasonHeldByWorkloads = "HeldByWorkloads"
	// KVCachePoolBindingReasonLedgerNotReleased 是一个绑定关系，其领域仍持有对象，因此 Master 拒绝释放
	// 其配额。清空该领域即可释放它。
	KVCachePoolBindingReasonLedgerNotReleased = "LedgerNotReleased"
	// KVCachePoolBindingReasonLedgerRequestFailed 是一个绑定对象，其账本请求失败，其条目是否已消失尚不
	// 确定。资源池的状态反映了 Master 的报告内容。
	KVCachePoolBindingReasonLedgerRequestFailed = "LedgerRequestFailed"
)

// syncKVCachePoolBindings 从本轮同一份指标采样中提取各 Binding 自己的数值，再写回各自 status。
//
// 这里没有总结。每个数字都来自 Binding 自己的 spec.domain.name 这一单一系列，因为租户就是复用领域——如果
// Binding 将两个系列合并起来，就会将另一个命名空间的消耗量当作自己的消耗量来报告。
//
// 没有发生的抓取行为，就是无法读取主数据的失败尝试。之前的数值则保持原样，只有条件会发生变化：将其归零会将“
// 我们无法查看”变为“你未使用任何内容”，而这正是促使某人去寻找已消失的缓存的诱因。
//
// Binding 没有独立控制器，其变更会触发所引用 Pool 的调谐。因此由 Pool 的同一轮调谐决定添加或移除 Binding 终
// 结器，使它与租户账本的写入顺序保持一致。
func (r *KVCachePoolReconciler) syncKVCachePoolBindings(
	ctx context.Context,
	kvcp *workercore.KVCachePool,
	master *kvCachePoolMaster,
	ledger kvCachePoolLedgerPass,
) error {
	logger := ctrllog.FromContext(ctx)

	for i := range master.bindings[kvcp.Name] {
		kvcpb := &master.bindings[kvcp.Name][i]

		if kvCachePoolBindingIsReleasing(kvcpb) {
			if err := r.releaseKVCachePoolBinding(ctx, kvcpb, ledger); err != nil {
				return err
			}
			continue
		}

		// 已经被 lockKVCachePoolBindings 锁定，本次处理之前并未在 Master 中写入任何内容。此处采用锁
		// 定方式反而导致了缺陷：账本与策略文件在上方合并，因此中间发生崩溃时，会在 Master 中留下一
		// 个绑定项，且没有终结器来删除它。

		holder := &workercore.KVCachePoolBinding{
			ObjectMeta: *kvcpb.ObjectMeta.DeepCopy(),
			Status:     *kvcpb.Status.DeepCopy(),
		}
		r.observeKVCachePoolBinding(holder, kvcpb, master, ledger.metrics, ledger.scraped, ledger.noLedger)

		desired := summarizeKVCachePoolBinding(holder)
		if kubemeta.DeepEqual(desired, kvcpb.Status) {
			continue
		}
		kvcpb.Status = desired
		if err := r.Client.Status().Update(ctx, kvcpb); err != nil {
			// 整个流程放弃了，而不是继续处理其他元素。这里的失败几乎总是由于冲突导致的，这表明
			// 在快照之下，此 Binding 已被修改；此循环中的另一个 Binding 所生成的内容也正在被更
			// 新——因此继续处理会导致使用已知已过时的数据来生成剩余内容。冲突背后的修改本身也是
			// 一个 Binding 事件，它会重新列出和重新处理数据，其他元素则能从最新的数据中获取其数
			// 值。是延迟，而非丢失。
			return fmt.Errorf("update kv cache pool binding %s status: %w",
				ctrlcli.ObjectKeyFromObject(kvcpb), err)
		}
		logger.V(2).Info("refreshed kv cache pool binding status",
			"kv cache pool binding", ctrlcli.ObjectKeyFromObject(kvcpb), "phase", desired.Phase)
	}

	return nil
}

// observeKVCachePoolBinding 会直接填充一个 Binding 的字段。
func (r *KVCachePoolReconciler) observeKVCachePoolBinding(
	holder, kvcpb *workercore.KVCachePoolBinding,
	master *kvCachePoolMaster,
	metrics mooncake.TenantQuotaMetrics,
	scraped bool,
	noLedger bool,
) {
	domain := kvcpb.Spec.Domain.Name

	if kvcpb.DeletionTimestamp != nil {
		// 仅因某种绑定关系而触及，它仍具有效力——已释放的则永远无法达到此程度。下面的数据仍适用于此
		// 情况：工作负载仍通过这一领域进行读写操作，而无人能够实施的删除操作也不会使其配额失效。
		KVCachePoolBindingConditionReleasable.False(holder,
			KVCachePoolBindingReasonHeldByWorkloads,
			fmt.Sprintf("deletion is held: %s in this namespace still hold(s) the pool through this "+
				"binding. Releasing it would leave a workload writing into a pool nothing records it "+
				"as using", formatKVCacheConsumers(kvcpb.Status.UsedBy)))
	}

	if others := otherClaimants(master.contested[domain], kvcpb); len(others) > 0 {
		// 发生域名冲突时双方都不能管理该域，也不能上报属于另一方的额度。把另一 Binding 的命名空间和
		// 名称写入消息，便于定位冲突双方。
		KVCachePoolBindingConditionDomainExclusive.False(holder,
			KVCachePoolBindingReasonDomainClaimedByMultipleBindings,
			fmt.Sprintf("reuse domain %q is also claimed by %s, so it is managed for neither: one "+
				"ledger entry cannot carry two namespaces' quotas. Admission refuses a second claim, "+
				"so this is two creates that raced it — delete one of them", domain,
				strings.Join(others, ", ")))
		return
	}
	KVCachePoolBindingConditionDomainExclusive.True(holder, "Exclusive",
		fmt.Sprintf("reuse domain %q is claimed by this binding alone", domain))

	if noLedger {
		// 没有租户账本的 master 仍能向默认租户写入，受整个存储的容量约束。两个条件都为 True 表示
		// Binding 可用，消息须说明没有独立的租户额度或强制限制。
		holder.Status.RequestedQuota, holder.Status.EffectiveQuota = nil, nil
		holder.Status.Usage, holder.Status.OverQuota = nil, nil

		KVCachePoolBindingConditionQuotaObserved.True(holder, "NoTenantLedger",
			fmt.Sprintf("the master holds no tenant ledger, so there are no per-tenant figures for "+
				"reuse domain %q: every write falls into the default tenant, and usage is not "+
				"accounted per domain", domain))
		KVCachePoolBindingConditionQuotaGranted.True(holder, "Unenforced",
			fmt.Sprintf("the master holds no tenant ledger, so no per-tenant quota is in force for "+
				"reuse domain %q: writes are bounded only by the store's global capacity", domain))
		return
	}

	if !scraped {
		// 对象上已有的所有图形都保留不变。请查看该函数的调用方。
		KVCachePoolBindingConditionQuotaObserved.False(holder, "MasterNotScraped",
			fmt.Sprintf("the master's figures for reuse domain %q could not be read this pass; the "+
				"ones below are from the last pass that could", domain))
		return
	}

	sample, known := metrics.Tenant(domain)
	if !known {
		// Master 从未听说过这个领域。这是一个观察结果——因此轴线是真实的，数据也清晰明了，不会留下模糊不清的部分。
		holder.Status.RequestedQuota, holder.Status.EffectiveQuota = nil, nil
		holder.Status.Usage, holder.Status.OverQuota = nil, nil

		KVCachePoolBindingConditionQuotaObserved.True(holder, "TenantNotInLedger",
			fmt.Sprintf("the master reports no figures for reuse domain %q: it carries no entry for "+
				"it yet. This is not a quota of zero — no quota has been observed at all", domain))
		KVCachePoolBindingConditionQuotaGranted.False(holder, "NoLedgerEntry",
			fmt.Sprintf("the master carries no ledger entry for reuse domain %q, so nothing is "+
				"granted to it and no write can succeed", domain))
		return
	}

	holder.Status.RequestedQuota = quantityFromBytes(sample.RequestedBytes)
	holder.Status.EffectiveQuota = quantityFromBytes(sample.EffectiveBytes)
	holder.Status.OverQuota = sample.OverQuota

	occupancy, inflight := sample.Occupancy()
	holder.Status.Usage = quantityFromBytes(occupancy)

	KVCachePoolBindingConditionQuotaObserved.True(holder, "Observed",
		quotaObservedMessage(domain, sample.ExplicitPolicy, inflight))

	// 根据资源池的容量而非具体数值来解读数据：一个 Binding 报告其自身的租户，两者可能有所不同。nil 和零
	// 被分开处理，因为它们的含义不同——Master 完全不导出该序列，并不等同于 Master 没有授予任何内容。
	switch {
	case sample.EffectiveBytes == nil:
		// 已知版本都在同一循环导出租户的申请值和实际值，截断的指标文本也会被整体拒绝。这里仍须检查
		// 指针，避免空指针比较，并区分实际额度尚未观测与已经观测为零。
		KVCachePoolBindingConditionQuotaGranted.False(holder, "GrantNotExported",
			fmt.Sprintf("the master's figures for reuse domain %q carry no effective quota, so "+
				"whether anything is granted to it is unknown", domain))
	case *sample.EffectiveBytes == 0:
		// 说明所读取的内容以及原因所在，但不声称自己已看到：这仅读取了每个租户的系列数据，从未查看
		// 资源池的容量指标，因此无法判断从共享资源中未挂载的 Master 是否因相应的重新调整而归零。
		KVCachePoolBindingConditionQuotaGranted.False(holder, "ZeroGranted",
			fmt.Sprintf("the master grants reuse domain %q zero bytes, so no write can succeed. "+
				"Whether its pool has nothing to allocate, or divided what it has and left this "+
				"domain nothing, is on the pool's own CapacityAllocatable", domain))
	default:
		KVCachePoolBindingConditionQuotaGranted.True(holder, "Granted",
			fmt.Sprintf("the master grants reuse domain %q %d bytes", domain, *sample.EffectiveBytes))
	}
}

// quotaObservedMessage 说明了这些数值的含义，其具体数值会有所不同。
func quotaObservedMessage(domain string, explicit *bool, inflight bool) string {
	parts := make([]string, 0, 3)

	switch {
	case explicit != nil && !*explicit:
		// 已请求，但 Master 表示当前没有运行。任务尚未完成，或者被拒绝了——这由资源池的自身条件决定。
		parts = append(parts, fmt.Sprintf("a quota.ceiling is set, but the master reports reuse "+
			"domain %q as running without an explicit policy; see the pool's conditions", domain))
	default:
		parts = append(parts, fmt.Sprintf("the master's figures for reuse domain %q were read", domain))
	}

	if inflight {
		parts = append(parts, "usage includes writes that have not committed: this master reports one "+
			"occupancy figure charged at the start of a write, with nothing to subtract")
	}

	return strings.Join(parts, "; ")
}

// otherClaimants 返回争用同一复用域的其他 Binding 名称，排除当前正在写状态的 Binding。
func otherClaimants(
	claims []workercore.KVCachePoolBindingReference, kvcpb *workercore.KVCachePoolBinding,
) []string {
	others := make([]string, 0, len(claims))
	for _, claim := range claims {
		if claim.Namespace == kvcpb.Namespace && claim.Name == kvcpb.Name {
			continue
		}
		others = append(others, claim.Namespace+"/"+claim.Name)
	}
	slices.Sort(others)
	return others
}

// quantityFromBytes 将观察到的字节计数转换为数量状态，并保留指针所代表的区别：未观察到的内容不会被发布。
func quantityFromBytes(bytes *int64) *resource.Quantity {
	if bytes == nil {
		return nil
	}
	// BinarySI 规定，一个完整的 MiB 数值应被理解为一个数值。Master 的计数方式基于字节计数，而 DecimalSI
	// 则会生成 1073741824 而不是 1Gi。
	return resource.NewQuantity(*bytes, resource.BinarySI)
}

// summarizeKVCachePoolBinding 根据其条件来确定 Binding 的阶段，就像资源池的阶段由资源池自身决定一样：首先出
// 现故障，然后出现一个尚未被任何人关注的轴。
func summarizeKVCachePoolBinding(
	holder *workercore.KVCachePoolBinding,
) workercore.KVCachePoolBindingStatus {
	status := holder.Status

	// 删除操作在任何轴操作之前就决定了阶段，而“可释放”特意不属于其中：那种仅在移除过程中存在的轴，在每
	// 一个活跃对象上会被视为“未观察到”——“配置”。
	if holder.DeletionTimestamp != nil {
		status.Phase = KVCachePoolPhaseDeleting
		status.PhaseMessage = KVCachePoolBindingConditionReleasable.GetMessage(holder)
		return status
	}

	// QuotaGranted 出现在 QuotaObserved 之后，因此如果某次遍历无法读取 Master 数据，就会显示“无法读取”，
	// 而不是显示授予信息。
	axes := []kubeapistatus.ConditionType{
		KVCachePoolBindingConditionDomainExclusive,
		KVCachePoolBindingConditionQuotaObserved,
		KVCachePoolBindingConditionQuotaGranted,
	}

	for _, axis := range axes {
		if axis.Exists(holder) && !axis.IsTrue(holder) {
			status.Phase, status.PhaseMessage = KVCachePoolPhaseError, axis.GetMessage(holder)
			return status
		}
	}
	for _, axis := range axes {
		if !axis.Exists(holder) {
			status.Phase, status.PhaseMessage = KVCachePoolPhaseProvisioning,
				fmt.Sprintf("%s has not been observed yet", axis)
			return status
		}
	}

	status.Phase, status.PhaseMessage = KVCachePoolPhaseReady,
		KVCachePoolBindingConditionQuotaObserved.GetMessage(holder)
	return status
}

// summarizeKVCachePool 根据条件确定阶段。
//
// 任何使资源池与 Ready 状态分离的情况都是前提条件，因此直接读取这些数据，而不进行任何重新推导：在某种条件下
// 以某种方式报告的错误，在另一阶段又以另一种方式报告，这就是操作器如何对这两种情况都不信任的原因。
func (r *KVCachePoolReconciler) summarizeKVCachePool(
	holder *workercore.KVCachePool,
) workercore.KVCachePoolStatus {
	status := holder.Status

	axes := []kubeapistatus.ConditionType{
		KVCachePoolConditionBackendResolved,
		KVCachePoolConditionQuotaLedgerAvailable,
		KVCachePoolConditionQuotaPolicyWritable,
		KVCachePoolConditionCapacityAllocatable,
	}

	// 首先需在所有维度上寻找故障，只有在确认所有维度均无异常后，才会检查未观测到的维度。这种顺序很重要，
	// 因为某一故障在未被发现后就会消失——如果检查仅停留在第一个账本拒绝时，就无法得知容量读数——而按维度
	// 顺序进行扫描则会导致报告“容量尚未被观测到”，对于实际存在无账本的 Master 情况的资源池来说，这正是
	// 操作员需要理解的内容。
	//
	// MultiTenancyDisabled 是那种非 True 的读取值，并非故障：无账本的 Master 属于声明式的单租户拓扑结构，
	// 而此资源池则按照该拓扑允许的程度进行收敛。其他所有非 True 的读取值——包括同一维度上因其他原因导致
	// 的 False 值——都是故障所在。
	for _, axis := range axes {
		if axis.Exists(holder) && !axis.IsTrue(holder) &&
			axis.GetReason(holder) != KVCachePoolReasonMultiTenancyDisabled {
			status.Phase, status.PhaseMessage = KVCachePoolPhaseError, axis.GetMessage(holder)
			return status
		}
	}
	for _, axis := range axes {
		if !axis.Exists(holder) {
			status.Phase, status.PhaseMessage = KVCachePoolPhaseProvisioning,
				fmt.Sprintf("%s has not been observed yet", axis)
			return status
		}
	}

	if KVCachePoolConditionQuotaLedgerAvailable.IsFalse(holder) &&
		KVCachePoolConditionQuotaLedgerAvailable.GetReason(holder) == KVCachePoolReasonMultiTenancyDisabled {
		status.Phase, status.PhaseMessage = KVCachePoolPhaseReady,
			"the master runs without multi-tenancy: a single-tenant pool with no per-tenant quota "+
				"in force, serving exactly one reuse domain"
		return status
	}

	status.Phase, status.PhaseMessage = KVCachePoolPhaseReady,
		"the master holds every quota this pool asked for"
	return status
}

// syncKVCachePoolStatus 会整体写入状态，仅针对差异进行更新，随后以结果结束该过程。
//
// 这个保护机制可以防止循环自我迭代：此控制器的谓词会忽略其自身的状态写入操作，而已稳定的资源池则完全不会进
// 行任何写入。同样的谓词还会丢弃其他方仅涉及状态的变更，因此冲突背后的变更可能不会产生任何事件，该操作会被
// 重新排队，而不是等待发生。
func (r *KVCachePoolReconciler) syncKVCachePoolStatus(
	ctx context.Context, kvcp *workercore.KVCachePool, desired workercore.KVCachePoolStatus, result ctrl.Result,
) (ctrl.Result, error) {
	if kubemeta.DeepEqual(desired, kvcp.Status) {
		return result, nil
	}

	logger := ctrllog.FromContext(ctx)

	kvcp.Status = desired
	if err := r.Client.Status().Update(ctx, kvcp); err != nil {
		return objectWriteResult(logger, err, "update kv cache pool status", _requeueAfterConflict)
	}
	logger.V(2).Info("refreshed kv cache pool status", "phase", desired.Phase)
	return result, nil
}

// teardownKVCachePool 会在没有任何对象占用时释放资源池，当存在对象占用时则拒绝释放。
//
// 两位持有者因不同原因被拒绝，并因此给出不同的反馈。
//
// 仍引用资源池的 Binding 是一种未被撤回的授权：若释放该授权，将导致一个命名空间持有不再存在的资源池配额。这
// 种拒绝行为仅属于状态写入操作——Binding 的消失会通过监视机制触发协调操作，因此计时器只能猜测其发生时间。
//
// 一个不会放弃该资源池中已注册记录的 Master，其容量是无人能看到的：账本没有标注记录属于谁，因此一旦该对象消
// 失，集群中的其他元素便不会知道如何删除它。这种拒绝是有时间限制的，因为 Master 不是控制器所监控的对象，当
// 控制器恢复时，集群中的任何内容都不会发生变化。
//
// 先释放本轮可以放行的 Binding，再检查哪些 Binding 仍阻止删除。在正常服务时由调谐循环负责释放，Pool 一旦进
// 入删除分支就必须在这里完成这项工作。
//
// 删除资源池不会做的是持续观察。那些被绑定的对象则不会更新其数据，这是资源池被废弃的必然代价：资源池已被要
// 求停止运作，下面的条件列出了所有阻碍这一过程的绑定对象。
func (r *KVCachePoolReconciler) teardownKVCachePool(
	ctx context.Context, kvcp *workercore.KVCachePool,
) (ctrl.Result, error) {
	logger := ctrllog.FromContext(ctx)

	holder := &workercore.KVCachePool{
		ObjectMeta: *kvcp.ObjectMeta.DeepCopy(),
		Status:     *kvcp.Status.DeepCopy(),
	}
	deleting := func(result ctrl.Result) (ctrl.Result, error) {
		return r.syncKVCachePoolStatus(ctx, kvcp, deletingKVCachePoolStatus(holder), result)
	}

	bindings := new(workercore.KVCachePoolBindingList)
	err := r.Client.List(ctx, bindings,
		ctrlcli.MatchingFields{IndexingKVCachePoolBindingByPool: kvcp.Name})
	if err != nil {
		logger.Error(err, "list the bindings of a pool being deleted")
		return ctrl.Result{}, err
	}

	// 已解决，针对以下所有移除情况均如此。无客户即是一个已删除的 Master，或者一个未登记账本的 Master，
	// 而非无法访问的 Master；差异决定了是否仍有任何债务归属于它。
	admin, kvcb, unreachable, err := r.resolveKVCachePoolAdmin(ctx, kvcp, holder)
	if err != nil {
		return ctrl.Result{}, err
	}

	// 根据以下所有权规则，仅观察一次。仅在存在 Master 时才会被采用：无主客户端即指已消失或没有账本；不
	// 可访问的客户端则会将以下所有移除操作都转换为保留状态。无法进行的快照属于错误，而非能够正常进行的
	// 快照，其规则与 deleteTenantQuotas 中的账本列表条款一致——在不知道属于谁的情况下删除条目，正是该规
	// 则旨在防止的损失。
	var master *kvCachePoolMaster
	if admin != nil {
		if master, err = r.observeKVCachePoolMaster(ctx, kvcb); err != nil {
			logger.Error(err, "observe the master while tearing down a pool")
			return ctrl.Result{}, err
		}
	}

	// removable 将域名列表缩小到资源池实际可移除的域名。
	//
	// 清理范围限于本 Pool 声明的名称。同一 master 可服务多个 Pool，不能仅凭域存在于账本就删除其他 Pool
	// 的条目。涉及争用时仍要保留可归属其他 Binding 的域。
	//
	// 由无人宣称的域无法被移除，这并非疏漏：这是一种被删除的绑定力，已超出其释放范围，此时该资源池的
	// status.domains 即为集群中该条目存在的最后记录。
	removable := func(names []string) []string {
		if master == nil {
			return names
		}
		kept := make([]string, 0, len(names))
		for _, name := range names {
			if _, contested := master.contested[name]; contested {
				logger.V(2).Info("leaving a contested reuse domain to the claimant that keeps it",
					"domain", name)
				continue
			}
			if claim, claimed := master.domains[name]; claimed && claim.pool != kvcp.Name {
				logger.V(2).Info("leaving a reuse domain a sibling pool owns",
					"domain", name, "owner", claim.pool)
				continue
			}
			kept = append(kept, name)
		}
		return kept
	}

	var releasing, holding []*workercore.KVCachePoolBinding
	for i := range bindings.Items {
		kvcpb := &bindings.Items[i]
		if kvCachePoolBindingIsReleasing(kvcpb) {
			releasing = append(releasing, kvcpb)
			continue
		}
		holding = append(holding, kvcpb)
	}

	if len(releasing) > 0 {
		if unreachable {
			return deleting(ctrl.Result{RequeueAfter: kvCachePoolObserveInterval})
		}
		names := make([]string, 0, len(releasing))
		for _, kvcpb := range releasing {
			names = append(names, kvcpb.Spec.Domain.Name)
		}
		if r.deleteTenantQuotas(ctx, admin, removable(names), holder) {
			return deleting(ctrl.Result{RequeueAfter: kvCachePoolObserveInterval})
		}
		for _, kvcpb := range releasing {
			if err = r.unlockKVCachePoolBinding(ctx, kvcpb); err != nil {
				return ctrl.Result{}, err
			}
		}
	}

	if len(holding) > 0 {
		names := make([]string, 0, len(holding))
		for _, kvcpb := range holding {
			names = append(names, kvcpb.Namespace+"/"+kvcpb.Name)
		}
		KVCachePoolConditionReleasable.False(holder, KVCachePoolReasonHeldByBindings,
			fmt.Sprintf("deletion is held: %s still reference(s) this pool. A binding is a namespace's "+
				"grant, and releasing the pool under one would leave that namespace holding a quota "+
				"on a pool that no longer exists", listBoundedNames(names)))
		return deleting(ctrl.Result{})
	}

	// 无论 Bindings 没有带走的什么，在常规情况下都是空的；进入其中的，是被 Binding 删除的、已超出其释放
	// 时间的对象，使得这个资源池成为最后一个知晓其存在对象的。
	if len(kvcp.Status.Domains) > 0 {
		if unreachable {
			return deleting(ctrl.Result{RequeueAfter: kvCachePoolObserveInterval})
		}
		names := make([]string, 0, len(kvcp.Status.Domains))
		for i := range kvcp.Status.Domains {
			names = append(names, kvcp.Status.Domains[i].Name)
		}
		if r.deleteTenantQuotas(ctx, admin, removable(names), holder) {
			return deleting(ctrl.Result{RequeueAfter: kvCachePoolObserveInterval})
		}
	}

	// 渲染后的文档与这些条目保持一致，出于同样的原因：该资源池所创建的租户在所属资源池消失之前，不得被
	// 记录在任何地方。只有在后端系统本身消失时，才会被跳过，而此时 ConfigMap 也会通过所有者引用一同消失。
	//
	// 无法访问的 Master 将替代此位置。重新渲染是全文档、覆盖 Master 的，因此不阅读账本就写入内容，就会
	// 根据对后端兄弟节点所持有内容的猜测来生成种子数据——这也是为什么 deleteTenantQuotas 拒绝删除无法首先列出的条目。
	if kvcb != nil {
		if unreachable {
			return deleting(ctrl.Result{RequeueAfter: kvCachePoolObserveInterval})
		}
		held, err := r.releaseQuotaPolicyOfPool(ctx, admin, kvcb, kvcp, holder)
		if err != nil {
			return ctrl.Result{}, err
		}
		if held {
			return deleting(ctrl.Result{RequeueAfter: kvCachePoolObserveInterval})
		}
	}

	// 该声明位于条目之后、资源池的锁之前。在之后，因为当该资源池的条目仍处于 Master 状态时，后端服务已
	// 释放该条目，因此该条目可以被删除。在之前，因为先释放了自身锁的资源池会留下该声明，而集群中没有任
	// 何东西可以移除它，而且后端服务会被已消失的资源池长期保留。
	if err = r.releaseKVCachePoolBackendClaim(ctx, kvcp); err != nil {
		if isStaleKVCacheBackendWrite(err) {
			logger.V(1).Info("release kv cache backend deferred, the backend changed or was deleted since it was read",
				"reason", err.Error())
			return _requeueAfterConflict, nil
		}
		return ctrl.Result{}, err
	}

	if systemmeta.Unlock(kvcp) {
		return ctrl.Result{}, nil
	}
	if err = r.Client.Update(ctx, kvcp); err != nil {
		return ctrl.Result{}, ctrlcli.IgnoreNotFound(err)
	}
	logger.V(2).Info("released kv cache pool")
	return ctrl.Result{}, nil
}

// deletingKVCachePoolStatus 是资源池即将被释放时的状态。
//
// 此阶段的确定方式由此处决定，而非通过 summarizeKVCachePool 进行确定，因为“可释放”并非该函数的参数之一：这
// 四个参数描述的是被作为推理服务使用的资源池，而销毁操作不会重新观察到其中任何一个。如果交由摘要功能处理，
// 由绑定机制持有的资源池将报告上一次推理服务过程中遇到的所有错误。
func deletingKVCachePoolStatus(holder *workercore.KVCachePool) workercore.KVCachePoolStatus {
	status := holder.Status
	status.Phase = KVCachePoolPhaseDeleting
	status.PhaseMessage = KVCachePoolConditionReleasable.GetMessage(holder)
	return status
}

// resolveKVCachePoolAdmin 会找到正在被销毁的资源池中仍欠条目数的 Master。
//
// 一个没有客户端且无法访问的 FALSE 对象，即一个已消失的 Master，没有任何债务：后端将其账本也带走了，而一个
// 等待使用的资源池在后端消失期间也无法被删除——这是堆栈通常的消失顺序。同样的结论也适用于那些从未指定可用后
// 端、没有进行任何注册的资源池，以及那些未声明多租户功能、其 Master 也不持有任何债务对象的托管后端——这些后
// 端同样会被删除。
//
// Unreachable TRUE 是一个存在但不发布地址的后端服务。根据已写入的条件，这些条目可能仍然存在，而被留下的条目
// 则无法被回收——账本并不记录是哪个资源池创建了它。该后端服务会与客户端一起被返回，因为销毁过程需要执行两种
// 移除操作：需要地址的账本条目，以及只需要对象的渲染政策文档。对于已消失的后端服务来说，两者都返回 nil——其
// ConfigMap 也随着所有者引用一同消失了。
func (r *KVCachePoolReconciler) resolveKVCachePoolAdmin(
	ctx context.Context, kvcp, holder *workercore.KVCachePool,
) (admin *mooncake.AdminClient, backend *workercore.KVCacheBackend, unreachable bool, err error) {
	logger := ctrllog.FromContext(ctx)

	kvcb, resolveErr := r.resolveKVCachePoolBackend(ctx, kvcp)
	if resolveErr != nil {
		if resolveErr.internal != nil {
			return nil, nil, false, resolveErr.internal
		}
		// 只有被标记为“已消失”的后端才不会产生任何责任。另一种导致此解析失败的方式是
		// BackendNotSingular，而将两个后端命名为一个资源池，并非意味着其 Master 已消失——其中一个名
		// 称可能仍是一个活跃的 Master，在字段为单数状态时，或在网络 hook 触发写入操作之前，它仍持有
		// 该资源池注册的条目。当被视为已消失时，清理操作会发现客户端为 `nil`，调用账本完成结算，撤
		// 销后端声明并移除终结器：这些条目会保留下来，而账本不会记录是哪一个资源池创建了它们，因此
		// 任何东西都无法重新利用那部分资源。此处采用与下方不可访问的管理员地址相同的处理方式，因为
		// 属于同样的损失。
		if !resolveErr.gone {
			KVCachePoolConditionReleasable.False(holder, KVCachePoolReasonLedgerNotReleased,
				fmt.Sprintf("deletion is held: this pool's backend cannot be resolved (%s), so the "+
					"reuse domains it registered cannot be removed from any ledger. %s. An entry "+
					"left behind is capacity nothing can reclaim, because the ledger does not "+
					"record which pool created it",
					resolveErr.reason, resolveErr.message))
			return nil, nil, true, nil
		}
		logger.V(2).Info("tearing down a pool with no master left to ask",
			"reason", resolveErr.reason)
		return nil, nil, false, nil
	}

	// 管理型 leader 的多租户开关由控制器渲染。若开关关闭，既然运行时从不写租户账本，删除时也无需访问该
	// 账本。策略文件可以由集群现状重建，避免一个未能启动的 leader 阻塞 Pool 和后端的删除。
	if managed := kvcb.Spec.Connection.Managed; managed != nil && !managed.Leader.MultiTenancyEnabled() {
		logger.V(2).Info("tearing down a pool on a backend declared without a tenant ledger")
		return nil, kvcb, false, nil
	}

	_, adminAddress := kvCacheBackendAddresses(kvcb)
	if adminAddress == "" {
		KVCachePoolConditionReleasable.False(holder, KVCachePoolReasonLedgerNotReleased,
			fmt.Sprintf("deletion is held: backend %q publishes no %q address, so the reuse domains "+
				"registered on it cannot be removed from its ledger. An entry left behind is capacity "+
				"nothing can reclaim, because the ledger does not record which pool created it",
				kvcb.Name, workercore.KVCacheBackendEndpointNameAdmin))
		return nil, kvcb, true, nil
	}

	return &mooncake.AdminClient{Address: adminAddress, HTTP: r.AdminHTTP}, kvcb, false, nil
}

// releaseQuotaPolicyOfPool 重新渲染一个 Master 的策略文档，此时不包含该资源池。
//
// 账本并非资源池租户信息的唯一存储位置。这个 ConfigMap 由该控制器生成，但由后端服务拥有，因此当资源池被释放
// 时不会收集到任何数据——在每次容器启动时，主节点的种子容器会将其从 Master 自己的文件中复制回来。如果未经过
// 此步骤就释放资源池，则不会留下用于重复使用的条目，集群中的任何对象都不会声称拥有这些条目；重启后 Master
// 会从空索引中重新构建它们的使用情况，因此每个租户都以零使用量的配额形式重新出现：这就是一个完全健康的租户
// 状态。之后无法观察到任何差异，因此防止这种情况发生才是唯一的防御措施，而非简单的整理操作。
//
// 将集群快照和 master 的实际账本一起用于重建种子文件。期望租户列表会排除争用的域以及仍在排空的域，其中可能
// 有同后端其他 Pool 的存活条目。若仅用快照覆盖整个文件，leader 下次重启可能丢失这些配额。
func (r *KVCachePoolReconciler) releaseQuotaPolicyOfPool(
	ctx context.Context,
	admin *mooncake.AdminClient,
	kvcb *workercore.KVCacheBackend,
	kvcp, holder *workercore.KVCachePool,
) (held bool, err error) {
	logger := ctrllog.FromContext(ctx)

	master, err := r.observeKVCachePoolMaster(ctx, kvcb)
	if err != nil {
		return false, err
	}

	tenants := make([]mooncake.QuotaPolicyTenant, 0, len(master.tenants))
	kept := make(map[string]struct{}, len(master.tenants))
	for _, tenant := range master.tenants {
		// 该资源池中注册的域会保留原样；兄弟域中注册的域则会被保留。这就是账本删除所遵循的归属规则，
		// 必须如此，否则两个层面会就租户归属于谁产生分歧。地图中无人声明的域则会被保留，这是较为保守的选择。
		if claim, ok := master.domains[tenant.Name]; ok && claim.pool == kvcp.Name {
			continue
		}
		tenants = append(tenants, tenant)
		kept[tenant.Name] = struct{}{}
	}

	// 一个列表无法正确执行拆卸操作，也没有根据 Master 中内容的一半来编写文档，而 deleteTenantQuotas 的
	// 拒绝行为也是出于同样的原因。
	//
	// 开启多租户功能的 Master 是特例，根据相同规则，deleteTenantQuotas 会处理它：它不持有任何账本，因此
	// 不存在可能被隐藏的条目，文档仅从所需的租户中渲染。此处所提到的写入操作，正是资源池的终结器永远无
	// 法完成的操作——而种子会持续保留已消失的资源池中的租户。无客户端的情况与后端声明中所述相同，因此根
	// 本不会调用 Master。
	//
	// 这种等待状态属于条件及定时重试机制，而非错误。Master 并非该控制器监控的对象，因此只有在退避时间逐
	// 步增加的情况下才会进行重试，而资源池的状态也无法说明为何该问题仍然存在。
	var observed []mooncake.TenantQuota
	if admin != nil {
		observed, err = admin.ListTenantQuotas(ctx)
		if err != nil && !errors.Is(err, mooncake.ErrMultiTenancyDisabled) {
			KVCachePoolConditionReleasable.False(holder, KVCachePoolReasonLedgerNotReleased,
				fmt.Sprintf("deletion is held: the master's tenant ledger could not be read, so the "+
					"quota policy document cannot be re-rendered without guessing what sibling pools "+
					"still hold. Deletion resumes once the master answers: %v", err))
			logger.Error(err, "list tenant quotas before re-rendering the quota policy")
			return true, nil
		}
	}
	for i := range observed {
		entry := &observed[i]
		if _, already := kept[entry.TenantID]; already {
			continue
		}
		// 只有一项明确的策略被写入此文档中。在 Master 默认配置下运行的那个策略，是此操作员从未编写
		// 过的租户策略；在此处为其编写一项策略，将产生该策略所不具备的明确性——这与账本自身的 PUT 和
		// DELETE 操作所适用的机制相同。
		if !entry.HasExplicitPolicy {
			continue
		}
		// 该资源池中注册的所有内容都即将被移除，无论上述 DELETE 操作是否已处理完毕：那些仍在列表中
		// 的条目，由于正在被清除，不应被即将离开的资源池重新分配。
		if claim, ok := master.domains[entry.TenantID]; ok && claim.pool == kvcp.Name {
			continue
		}
		if kvCachePoolRegisteredBy(master, entry.TenantID, kvcp.Name) {
			continue
		}
		tenants = append(tenants, mooncake.QuotaPolicyTenant{
			Name: entry.TenantID,
			// 所要求的图形中，Master 本身并不持有任何内容，也不属于任何人的所有物：一个有争议的
			// 领域有两个所有者，且各自的边界都不是用于播种的基准，而一个释放性的绑定项进入则被
			// 复制，而非重新计算。
			Quota: *resource.NewQuantity(entry.RequestedBytes, resource.BinarySI),
		})
	}

	// 固定租户顺序，使此处重建的策略文件与正常运行时生成的内容一致。
	slices.SortFunc(tenants, func(a, b mooncake.QuotaPolicyTenant) int {
		return strings.Compare(a.Name, b.Name)
	})

	// 即使什么都没有留下，也会被保存下来，而不是被删除。一个空的文档就是当 ConfigMap 不存在时，种子容器
	// 会退回到的状态，因此这两个路径在 Master 中的状态是相同的——而删除这个控制器不拥有的对象，比本控制
	// 器所做的要严重得多。
	return false, r.syncQuotaPolicyConfigMap(ctx, kvcb, tenants)
}

// kvCachePoolRegisteredBy 报告某个名为池的 Bindings 是否包含此域。
//
// master.domains 规定：对于只有一个活跃 Claimant 的域，以及没有其他 Claimant 的域，有争议的域将从地图中删除，
// 而释放的 Binding 则不会进入该域。这两种情况都需由 re-render 来决定，因此 Binding 会直接被读取。
func kvCachePoolRegisteredBy(master *kvCachePoolMaster, domain, pool string) bool {
	for i := range master.bindings[pool] {
		if master.bindings[pool][i].Spec.Domain.Name == domain {
			return true
		}
	}
	return false
}

// deleteTenantQuotas 删除指定租户的配额条目，并报告是否仍有条目被保留。
//
// 这些名称始终只是资源池的列表，并非 Master 所包含的所有内容：一个 Master 服务多个资源池，其账本并未说明条
// 目属于谁，因此通过更宽泛的删除操作会连带删除其他资源池的租户。
//
// 仍然包含对象的域是 409 状态的第三种含义，有专门的说明：这不是错误，当域资源耗尽后，该操作即可完成；若将其
// 理解为多租户拒绝机制，则会对决定对象是否可以释放的单一操作设置错误的状态。
//
// 删除与正常收敛采用相同的显式策略检查。控制器只会写显式策略，不应删除一个沿用 master 默认值、仅名称碰巧匹
// 配本 Pool 的租户。
func (r *KVCachePoolReconciler) deleteTenantQuotas(
	ctx context.Context, admin *mooncake.AdminClient, domains []string, holder *workercore.KVCachePool,
) (held bool) {
	logger := ctrllog.FromContext(ctx)

	// 一个无客户的 Master 指的是已消失的 Master，或者是未被声明账本的 Master，而不是无法访问的 Master；
	// 对于不存在的账本，也不存在任何债务。resolveKVCachePoolAdmin 的契约已经说明了这一点；在此处而非每
	// 个调用点执行该约束，才能确保每个调用者都符合这一要求，因为调用者仅能区分 `unreachable` 这一情况，
	// 而在这个路径上它属于错误的。
	if admin == nil {
		return false
	}

	// 请仔细阅读，关于下面的门。一份清单如果无法有效执行拆卸工作，而不是盲目进行，那么无意识地删除那些
	// 包含明确政策的条目正是该门存在的目的，因此未回答的问题不应被视为许可。
	observed, err := admin.ListTenantQuotas(ctx)
	if err != nil {
		// 一个没有多租户功能的 Master 不会保存任何账本，而那种无法存在的记录，在此次清理过程中也无
		// 需删除：答案已经确定，而非未知，因此资源池会被释放，而不是等待永远不会到来的读取操作。其
		// 他所有故障则取决于这些记录是否仍然存在，以及是否会被保留。
		if errors.Is(err, mooncake.ErrMultiTenancyDisabled) {
			logger.V(2).Info("tearing down a pool against a master that holds no tenant ledger")
			return false
		}
		KVCachePoolConditionReleasable.False(holder, KVCachePoolReasonLedgerNotReleased,
			fmt.Sprintf("deletion is held: the master's tenant ledger could not be read, so whether "+
				"its entries are this operator's to remove is unknown. Deletion resumes once the "+
				"master answers: %v", err))
		logger.Error(err, "list tenant quotas while tearing down a pool")
		return true
	}
	explicit := make(map[string]bool, len(observed))
	for i := range observed {
		explicit[observed[i].TenantID] = observed[i].HasExplicitPolicy
	}

	for _, domain := range domains {
		if present, ok := explicit[domain]; !ok || !present {
			// 已经消失，或运行在 Master 的默认配置下且从未被此操作员编写过。
			logger.V(2).Info("leaving a ledger entry that carries no explicit policy of ours",
				"tenant", domain)
			continue
		}
		err := admin.DeleteTenantQuota(ctx, domain)
		if err == nil {
			continue
		}
		if errors.Is(err, mooncake.ErrTenantNotEmpty) {
			KVCachePoolConditionReleasable.False(holder, KVCachePoolReasonLedgerNotReleased,
				fmt.Sprintf("deletion is held: reuse domain %q still holds objects, so the master "+
					"refuses to remove its quota. The entry goes once the domain has drained", domain))
			return true
		}
		KVCachePoolConditionReleasable.False(holder, KVCachePoolReasonLedgerNotReleased,
			fmt.Sprintf("deletion is held: the master would not remove the quota of reuse domain "+
				"%q: %v", domain, err))
		logger.Error(err, "delete tenant quota while tearing down a pool", "tenant", domain)
		return true
	}

	return false
}

// releaseKVCachePoolBackendClaim 会将此资源池从后端服务器的 usedBy 列表中移除，前提是后端服务器仍然存在，可
// 以执行该操作。已经移除的后端服务器无需再被移除任何内容。
func (r *KVCachePoolReconciler) releaseKVCachePoolBackendClaim(
	ctx context.Context, kvcp *workercore.KVCachePool,
) error {
	kvcb, resolveErr := r.resolveKVCachePoolBackend(ctx, kvcp)
	if resolveErr != nil {
		if resolveErr.internal != nil {
			return resolveErr.internal
		}
		return nil
	}
	return r.releaseKVCacheBackend(ctx, kvcb, kvcp)
}

// releaseKVCachePoolBinding 会在其拥有的绑定对象对应的条目消失后，删除该绑定的终结器。
//
// 先让 convergeTenantLedger 删除已释放租户的条目，再移除 Binding 的终结器。如果账本尚未收敛，就继续等待。否
// 则负责记录条目归属的 Binding 会先消失，留下无法回收的额度。若 master 明确表示没有租户账本，则没有需要等待删除的条目。
//
// 在退出过程中不会写入任何状态。该对象即将停止存在，而在最后一个终结器被移除时写入的状态信息，会与 API 服务
// 器的移除过程同时发生。
func (r *KVCachePoolReconciler) releaseKVCachePoolBinding(
	ctx context.Context, kvcpb *workercore.KVCachePoolBinding, ledger kvCachePoolLedgerPass,
) error {
	logger := ctrllog.FromContext(ctx).
		WithValues("kv cache pool binding", ctrlcli.ObjectKeyFromObject(kvcpb))

	if !ledger.converged {
		domain := kvcpb.Spec.Domain.Name

		// 这个域是 Master 不会删除的。会显示一个状态，与下面的版本不同：该对象不会停止存在——它会一
		// 直等待工作负载中的对象存在——而且没有特定原因的话，它会显示为“绑定中，正在删除”，并带有空
		// 白消息，因为删除阶段会依据这种状态来生成消息。
		if slices.Contains(ledger.retained, domain) {
			logger.V(2).Info("holding a binding's release: its domain still holds objects")
			return r.holdKVCachePoolBindingRelease(ctx, kvcpb, KVCachePoolBindingReasonLedgerNotReleased,
				fmt.Sprintf("deletion is held: the master will not remove the quota of reuse domain "+
					"%q while it still holds objects. Nothing in the cluster references this binding "+
					"any more — drain the domain and the release completes on the next pass", domain))
		}

		// 能够回答该问题的 Master 中不包含任何租户账本，这种失败情况会解决该问题，而不会留下疑问；
		// 资源池自身的销毁过程也采用相同逻辑：由于多租户功能被关闭，没有账本可供条目存储，因此此次
		// 发布不会在 Master 上造成任何阻塞。相反，持有该对象会永久保留，即使后端状态不变，答案也不会改变。
		if errors.Is(ledger.failure, mooncake.ErrMultiTenancyDisabled) {
			logger.V(2).Info("releasing a binding against a master that holds no tenant ledger")
			return r.unlockKVCachePoolBinding(ctx, kvcpb)
		}

		if ledger.failure != nil {
			// 其他所有失败情况都会使该域的入口状态为“未知”，因此终结器仍被保留。被保留的兄弟节
			// 点不会处理那些该进程从未触及的操作，或是其无法获得的答案。
			logger.V(2).Info("holding a binding's release: the ledger did not converge this pass")
			return r.holdKVCachePoolBindingRelease(ctx, kvcpb,
				KVCachePoolBindingReasonLedgerRequestFailed,
				fmt.Sprintf("deletion is held: the master's tenant ledger did not answer this pass, so "+
					"whether the quota of reuse domain %q is gone cannot be established. The pool's own "+
					"conditions carry what the master said; the release retries every pass", domain))
		}

		// 如果没有失败，非空保留列表意味着删除循环执行到了最后，因此它想要删除的每一项都已被移除——
		// 包括这一项。如果保留它，就会让每个被彻底删除的兄弟节点变成一个未清理的域块。
		if len(ledger.retained) > 0 {
			logger.V(2).Info("releasing a binding whose own entry is gone, on a pass another " +
				"domain held")
			return r.unlockKVCachePoolBinding(ctx, kvcpb)
		}
	}

	return r.unlockKVCachePoolBinding(ctx, kvcpb)
}

// holdKVCachePoolBindingRelease 说明了为何一个无绑定的 Binding 仍然无法释放。
func (r *KVCachePoolReconciler) holdKVCachePoolBindingRelease(
	ctx context.Context, kvcpb *workercore.KVCachePoolBinding, reason, message string,
) error {
	holder := &workercore.KVCachePoolBinding{
		ObjectMeta: *kvcpb.ObjectMeta.DeepCopy(),
		Status:     *kvcpb.Status.DeepCopy(),
	}
	KVCachePoolBindingConditionReleasable.False(holder,
		reason, message)

	desired := summarizeKVCachePoolBinding(holder)
	if kubemeta.DeepEqual(desired, kvcpb.Status) {
		return nil
	}
	kvcpb.Status = desired
	if err := r.Client.Status().Update(ctx, kvcpb); err != nil {
		if kerrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	return nil
}

// unlockKVCachePoolBinding 会移除一个 Binding 的终结器，这最终才使其能够被释放。
//
// 它不会写入任何状态。该对象即将停止存在，对某个对象的状态写入操作，其最后一个终结器即将被移除，会与 API 服
// 务器自身的移除过程同时进行。如果未找到，说明其他人先到达了那里，这也是最终的结果。
func (r *KVCachePoolReconciler) unlockKVCachePoolBinding(
	ctx context.Context, kvcpb *workercore.KVCachePoolBinding,
) error {
	if systemmeta.Unlock(kvcpb) {
		return nil
	}

	logger := ctrllog.FromContext(ctx).
		WithValues("kv cache pool binding", ctrlcli.ObjectKeyFromObject(kvcpb))

	if err := r.Client.Update(ctx, kvcpb); err != nil {
		if kerrors.IsNotFound(err) {
			return nil
		}
		logger.Error(err, "release kv cache pool binding")
		return err
	}
	logger.V(2).Info("released kv cache pool binding")
	return nil
}

// releaseOrphanKVCachePoolBindings 用于释放不再存在的资源池的绑定。
//
// 只有已被标记为删除的绑定项才会被处理。其他未标记的绑定项则保持原样：它所指向的资源池可能稍后创建，而该绑
// 定的锁定机制将决定该资源池的销毁过程何时开始。
//
// 没有账本条目被删除，也不可能被删除。该资源池名为 backend，因此当资源池消失后，便无法确定是哪位 Master 保
// 存了该条目。资源池自身的终结器会处理这一情况，只要仍有任何 Binding 存在，它就会拒绝操作。因此，如果到达了
// 这一步，说明资源池被强制删除——而其 Bindings 中注册的内容则属于 Master 可 reclaim 的部分。
func (r *KVCachePoolReconciler) releaseOrphanKVCachePoolBindings(ctx context.Context, pool string) error {
	logger := ctrllog.FromContext(ctx)

	bindings := new(workercore.KVCachePoolBindingList)
	err := r.Client.List(ctx, bindings, ctrlcli.MatchingFields{IndexingKVCachePoolBindingByPool: pool})
	if err != nil {
		logger.Error(err, "list the bindings of an absent pool", "pool", pool)
		return err
	}

	for i := range bindings.Items {
		kvcpb := &bindings.Items[i]
		if kvcpb.DeletionTimestamp == nil {
			continue
		}
		if err = r.unlockKVCachePoolBinding(ctx, kvcpb); err != nil {
			return err
		}
	}

	return nil
}

// claimKVCacheBackend 会在后端的 usedBy 字段中记录此资源池，该字段用于记录在资源池仍使用该资源时后端自身的销毁过程。
//
// 该条目包含一个空的命名空间，这其实是一种值，而非缺失：这两个对象都属于集群范围，因此没有需要命名的命名空
// 间。这是该系列中唯一会写入数据的列表，而空字符串必须作为列表映射键存在，以便写入操作能够正常进行。
//
// 只有当列表中尚未包含应有的内容时，才会发生写入操作。这一保护机制能够防止资源池在每次迭代中重写另一个控制
// 器的对象——同时，由于后端自带的控制器也会写入相同状态，从而避免每次迭代都引发冲突。
func (r *KVCachePoolReconciler) claimKVCacheBackend(
	ctx context.Context, kvcb *workercore.KVCacheBackend, kvcp *workercore.KVCachePool,
) error {
	return r.syncKVCacheBackendClaim(ctx, kvcb, kvcp, true)
}

// releaseKVCacheBackend 会撤销该资源池的占用，因此在其资源池处理完成之后，后端实例可以被删除。
func (r *KVCachePoolReconciler) releaseKVCacheBackend(
	ctx context.Context, kvcb *workercore.KVCacheBackend, kvcp *workercore.KVCachePool,
) error {
	return r.syncKVCacheBackendClaim(ctx, kvcb, kvcp, false)
}

func (r *KVCachePoolReconciler) syncKVCacheBackendClaim(
	ctx context.Context, kvcb *workercore.KVCacheBackend, kvcp *workercore.KVCachePool, claim bool,
) error {
	ref := workercore.KVCacheObjectReference{
		Kind:      KVCachePoolKind,
		Namespace: "",
		Name:      kvcp.Name,
	}

	at := slices.IndexFunc(kvcb.Status.UsedBy,
		func(e workercore.KVCacheObjectReference) bool { return e == ref })
	if claim == (at >= 0) {
		return nil
	}

	if claim {
		kvcb.Status.UsedBy = append(kvcb.Status.UsedBy, ref)
		// 保持占用方顺序稳定，避免两个 Pool 登记顺序变化就导致后端重写 status。
		slices.SortFunc(kvcb.Status.UsedBy, compareKVCacheObjectReferences)
	} else {
		kvcb.Status.UsedBy = slices.Delete(kvcb.Status.UsedBy, at, at+1)
	}

	// 冲突会被放入队列，而非就地重试。后端의控制器也会记录此状态，因此失败方的整个后端视图到那时都已经
	// 过时了——如果仅重试列表，就会基于已移动的对象生成错误的记录。
	omitLegacyMemberListing(&kvcb.Status)
	return r.Client.Status().Update(ctx, kvcb)
}

// isStaleKVCacheBackendWrite 用于报告：该资源池对其后端进行的失败写入操作，是否仅仅意味着后端发生了变更，或
// 者在该次读取之后被删除了。这两种情况都是正常的：后端自身的控制器会记录相同的状态，且堆栈可以优先于后端被
// 移除。这两种情况都不值得在错误日志中记录，也无法通过事件来修复，因为该控制器并不监控后端。因此，调用者会
// 以简短的重新请求结束该操作，而不是出现错误，这也是为什么 objectWriteResult 不适用于此场景：它会在未找到的
// 情况下结束操作，且完全不进行重试。下一次操作会再次读取后端，从较新版本中获取已移动的数据，并将已删除的数据标记为缺失。
func isStaleKVCacheBackendWrite(err error) bool {
	return kerrors.IsConflict(err) || kerrors.IsNotFound(err)
}

// formatKVCacheConsumers 在删除被拒绝时列出占用对象，便于管理员查找应先释放的引用。
//
// 由于后端自身的限制，usedBy 没有项数量限制，由使用该对象的用户自行编写，而这条消息则承载了拒绝的提示。无限
// 制的连接会随之导致拒绝行为——删除操作会被保留，对象也不会说明原因。
func formatKVCacheConsumers(refs []workercore.KVCacheObjectReference) string {
	names := make([]string, 0, len(refs))
	for _, ref := range refs {
		names = append(names, ref.Kind+"/"+ref.Name)
	}
	return listBoundedNames(names)
}

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
