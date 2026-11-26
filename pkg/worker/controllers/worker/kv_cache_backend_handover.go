package worker

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"

	coordination "k8s.io/api/coordination/v1"
	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"

	gpustack "gpustack.ai/gpustack/api/v1"
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/worker/kuberess"
	"gpustack.ai/gpustack/pkg/worker/kvcache/mooncake"
)

// kvCacheBackendEventLeaderHandover 表示该后端用于选主的 Lease 更换了持有者。
const kvCacheBackendEventLeaderHandover = "KVCacheLeaderHandover"

// leaseHolders 记录本 operator 上次查看时每个 backend 的 Lease 所指定的副本，以便区分
// 主备切换与续约。
//
// 它仅保存在内存中，刻意不作为字段持久化。切换是状态跃迁的属性，若用字段记录，只是对
// Lease 已发布值的冗余拷贝，仅用于比对——本设计中已有三个候选字段因此被淘汰。这一取舍的
// 代价明确可见而不隐晦：若本 operator 重启期间发生切换，则不会产生 Event，因为此处未曾
// 观测到切换前的状态。Lease 自身的 leaseTransitions 字段会在重启后保留，但它是计数而非事件。
//
// 保存的值永远不会离开此 map。哪个副本持有 Lease 并非本 API 回答的问题，因此读取身份仅用于
// 比较，Event 也只说明持有者发生了转移。
//
// 其零值即可直接使用，因此未经过 SetupController 构建的 reconciler（本包中所有测试辅助
// 函数均如此）也能以与真实路径相同的方式观测切换，而不会在被测代码中走不同的分支。
type leaseHolders struct {
	mu sync.Mutex
	by map[string]string
}

// observe 记录当前持有者状态，并报告是否发生移动，这与与零值差异不同：首次看到后端建立基准，不会报告任何内
// 容，因为首次看到并不属于交接。
//
// 基线值是最后一个非空的持有者，而非最后看到的数值，因为租约在从一份副本传输到另一份副本的过程中会经过一个
// 空持有者，而这一中间读数并非任何持有者所持有。将其存储会将交接过程分为两半——一半是持有者离开，另一半是持
// 有者到达，而由于中间有空值，任何一方都没有之前的持有者可供比较，因此真正的故障切换不会报告任何内容。在相
// 反方向上跳过写入操作也无妨：一次释放操作后，同一份副本再次获取数据，两者相等且保持沉默，这正是应有的状态。
func (h *leaseHolders) observe(backend, holder string) (moved bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.by == nil {
		h.by = make(map[string]string)
	}
	previous, seen := h.by[backend]
	if holder != "" {
		h.by[backend] = holder
	}

	return seen && holder != "" && previous != holder
}

// forget 提供了一个后端，因此被删除的后端在进程生命周期内不会保留其最后一个持有者；而以相同名称重新创建的后
// 端则没有基础值，而是从过时的状态开始。
func (h *leaseHolders) forget(backend string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	delete(h.by, backend)
}

// leaderLease 只读一次选主 Lease，供后面两处观测共用。
//
// 他们对同一个对象提出了不同的问题——它是否已易主，以及是否涉及任何人的名字——每个“Get”操作都是对对象进行重复
// 遍历的过程，该过程会在每次遍历中重复执行。读取结果会被传递下去，而不是被缓存于控制器中，因此没有任何数据
// 会超过此次遍历的时间。
//
// 只有后续观测需要 Lease 时才读取它。electionBackend 为 None 时两项观测都会提前返回，无需请求 API Server。
//
// 返回错误码 NotFound，而非被展平为空的租约，因为两个读取器对此有不同的处理方式：一个会忘记其基准状态，另一
// 个则继续询问主节点是否已经准备就绪。
func (r *KVCacheBackendReconciler) leaderLease(
	ctx context.Context, kvcb *workercore.KVCacheBackend,
) (*coordination.Lease, error) {
	managed := kvcb.Spec.Connection.Managed
	if managed == nil || !managed.Leader.KubernetesElectionEnabled() {
		return nil, nil
	}

	lease := new(coordination.Lease)
	if err := r.Client.Get(ctx, ctrlcli.ObjectKey{
		Name:      mooncake.LeaderObjectName(kvcb),
		Namespace: kuberess.SystemNamespaceName,
	}, lease); err != nil {
		return nil, err
	}

	return lease, nil
}

// reportLeaderHandover 在相邻两轮之间发现 Lease 持有者发生变化时记录一个 Event。
//
// 它回答的是“服务是否刚刚移动了”这个问题，这是延迟突增后的第一个诊断问题，而用户当前所查找的任何地方都没有
// 相关答案。它无法回答“昨晚发生了多少次”：事件是在集群的事件 TTL 时间内收集的，默认为一小时，而持久性计数则
// 是 Lease 自身的 leaseTransitions。
//
// REQUIRED：该参数未指定副本。哪个实例持有租约，并非本设计的非目标要求，而是由 Kubernetes 侧在其 Pod 上添加
// 的标签来确定的。所移动的内容属于交接的属性；谁持有它则属于副本的属性，此处仅报告第一个情况。该参数不需要
// 任何上下文，因为它不会发出任何调用：调用者会读取一次租约，并将其交给两个报告者，而记录事件则通过记录器进
// 行，而非通过 API 进行。
func (r *KVCacheBackendReconciler) reportLeaderHandover(
	kvcb *workercore.KVCacheBackend, lease *coordination.Lease, leaseErr error,
) {
	managed := kvcb.Spec.Connection.Managed
	if managed == nil || !managed.Leader.KubernetesElectionEnabled() {
		return
	}

	if leaseErr != nil || lease == nil {
		// 缺失的租约是主节点尚未启动时的正常状态。忽略最后一个持有者，就会使得间隔后的首次活动无法
		// 被识别为交接行为。
		if leaseErr == nil || kerrors.IsNotFound(leaseErr) {
			r.leaseHolders.forget(kvcb.Name)
		}
		return
	}

	if !r.leaseHolders.observe(kvcb.Name, leaseHolderIdentity(lease)) {
		return
	}

	// 正常情况而非警告。高可用性后端每次滚动更新都会产生警告，因此在这里的警告会出现在所有常规操作中，
	// 也会让读者难以过滤掉那些包含非计划情况的序列。
	var transitions int32
	if lease.Spec.LeaseTransitions != nil {
		transitions = *lease.Spec.LeaseTransitions
	}
	r.recordNormal(kvcb, kvCacheBackendEventLeaderHandover,
		"the store's leader moved to another replica; the lease has recorded %d handovers in total, "+
			"and the cache it serves is whatever the new leader could restore", transitions)
}

// reportElectionObserved 用于回答是否实际进行着选举，适用于那些需要该信息的后端。
//
// 它存在的意义在于，有些镜像过于老旧，无法参与选举。在特定的存储版本以下，Kubernetes 领导层后端并不存在，而
// 绑定在此类镜像上的后端会显示选举标志、启动、从单个副本中提供服务，并报告为“准备就绪”——但实际上没有任何东
// 西在参与选举。从镜像标签中读取版本信息并非一种验证方式：标签只是一个名称，而用户最常使用的标签则是一种摘
// 要或本地重建的结果。
//
// 从 Kubernetes 侧可以看到的是选举的结果。有持有者的租约意味着选举发生了。没有持有者的租约，在准备就绪的主
// 节点下，则意味着选举没有发生。
//
// REQUIRED：消息首先说明所观察到的内容，然后按此顺序说明产生这些内容的因素，从不提及原因。无持有者的租赁协
// 议也类似于缺失的角色绑定和尚未开始的过程，而提及图像的消息在这两种情况下都是错误的。
func (r *KVCacheBackendReconciler) reportElectionObserved(
	ctx context.Context, kvcb, holder *workercore.KVCacheBackend,
	lease *coordination.Lease, leaseErr error,
) {
	managed := kvcb.Spec.Connection.Managed
	if managed == nil || !managed.Leader.KubernetesElectionEnabled() {
		// 由于选举功能被禁用，没有需要观察的 Lease 数据。请删除之前的判断结果，因为状态是上一个观察
		// 结果的副本，否则会显得过时。
		holder.Status.Conditions = slices.DeleteFunc(holder.Status.Conditions,
			func(c gpustack.Condition) bool {
				return c.Type == string(KVCacheBackendConditionElectionObserved)
			})
		return
	}

	leaseName := mooncake.LeaderObjectName(kvcb)

	switch {
	case leaseErr != nil && !kerrors.IsNotFound(leaseErr):
		KVCacheBackendConditionElectionObserved.Unknown(holder, "LeaseUnreadable", fmt.Sprintf(
			"the lease %q could not be read: %v", leaseName, clipFaultDetail(leaseErr.Error()),
		))

	case leaseErr == nil && lease != nil && leaseHolderIdentity(lease) != "":
		KVCacheBackendConditionElectionObserved.True(holder, "Electing", fmt.Sprintf(
			"the lease %q names a holder, so an election has taken place", leaseName,
		))

	case !r.leaderPodIsReady(ctx, kvcb):
		// 同样的观察结果，其中没有相关信息。一个尚未准备就绪的主节点没有机会开展活动，因此这种缺失
		// 并不能说明它是否能够做到。
		KVCacheBackendConditionElectionObserved.Unknown(holder, "LeaderStarting", fmt.Sprintf(
			"no replica of the leader is ready yet, so the lease %q says nothing about whether an "+
				"election can happen", leaseName,
		))

	default:
		KVCacheBackendConditionElectionObserved.False(holder, "NoHolder", fmt.Sprintf(
			"a replica of the leader is ready while the lease %q names no holder. A store image "+
				"built without the Kubernetes leadership backend, a role binding for it that has not "+
				"been applied, and a process that is serving without having finished its first "+
				"campaign all produce this", leaseName,
		))
	}
}

// enqueueKVCacheBackendWhenLeaseChanged 将 Lease 映射回通过它进行选主的 backend。
//
// 该 Lease 既没有资源标记，也没有 owner reference，因为它由存储层而非本 operator 创建——
// 唯一的关联是名称，即 backend 自身名称加上 leader 后缀。在读回对应的 backend 之前，这只是
// 一个猜测，本函数正是为此而读回验证：若其他组件恰巧使用了符合该命名模式的 Lease，则不会
// 匹配到任何 backend，也不会入队任何请求。
func (r *KVCacheBackendReconciler) enqueueKVCacheBackendWhenLeaseChanged(
	ctx context.Context, obj ctrlcli.Object,
) []ctrlreconcile.Request {
	name, ok := strings.CutSuffix(obj.GetName(), mooncake.LeaderObjectNameSuffix)
	if !ok || name == "" {
		return nil
	}

	kvcb := new(workercore.KVCacheBackend)
	if err := r.Client.Get(ctx, ctrlcli.ObjectKey{Name: name}, kvcb); err != nil {
		return nil
	}
	if kvcb.Spec.Connection.Managed == nil ||
		!kvcb.Spec.Connection.Managed.Leader.KubernetesElectionEnabled() {
		return nil
	}

	return []ctrlreconcile.Request{{NamespacedName: ctrlcli.ObjectKey{Name: name}}}
}

// kvCacheBackendLeaseHolderChanged 仅放行可能属于主备切换的事件。
//
// 这是关键的负载过滤，而非单纯降噪。在每个 leader 的生命周期内，Lease 每隔几秒就会续约一次，
// 每次更新若进入 reconciler，都会使该 backend 对其管理接口发起三次串行 HTTP 读取——因此
// 此处若不加过滤，稳态集群就会对其中每个存储实例造成持续负载。
//
// 仅比较 holder 而非其他字段，因为只有它在切换时会变化：renewTime 在每次续约时都会更新，
// 而 acquireTime 随 holder 一起变化。
func kvCacheBackendLeaseHolderChanged(old, updated ctrlcli.Object) bool {
	if updated.GetNamespace() != kuberess.SystemNamespaceName {
		return false
	}

	oldLease, ok := old.(*coordination.Lease)
	if !ok {
		return false
	}
	newLease, ok := updated.(*coordination.Lease)
	if !ok {
		return false
	}

	return leaseHolderIdentity(oldLease) != leaseHolderIdentity(newLease)
}

func leaseHolderIdentity(lease *coordination.Lease) string {
	if lease.Spec.HolderIdentity == nil {
		return ""
	}
	return *lease.Spec.HolderIdentity
}

// recordNormal 针对 obj 发布一个普通事件，采用与 recordWarning 相同的 nil-记录保护机制，原因也相同。
func (r *KVCacheBackendReconciler) recordNormal(
	obj ctrlcli.Object, reason, format string, args ...any,
) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(obj, core.EventTypeNormal, reason, format, args...)
}
