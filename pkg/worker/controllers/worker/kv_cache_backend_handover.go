package worker

import (
	"context"
	"strings"
	"sync"

	coordination "k8s.io/api/coordination/v1"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/worker/kuberess"
	"gpustack.ai/gpustack/pkg/worker/kvcache/mooncake"
)

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
