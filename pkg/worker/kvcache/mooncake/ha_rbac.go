package mooncake

import (
	core "k8s.io/api/core/v1"
	rbac "k8s.io/api/rbac/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/systemmeta"
	"gpustack.ai/gpustack/pkg/worker/kuberess"
	"gpustack.ai/gpustack/pkg/worker/kvcache"
)

// MemberRBACObjectName 是后端在高可用环境下运行的账户名称。
//
// 按 BACKEND 计算，而非按组计算：每个 BACKEND 组读取相同的 Lease，且这种访问权限是此账户所允许的唯一权限。
func MemberRBACObjectName(kvcb *workercore.KVCacheBackend) string {
	return kvcb.Name + "-member"
}

// leaderNeedsAPIAccess 用于报告是否正在进行 Kubernetes 选举。它不受副本数量的影响：一个副本也会参与竞选，因
// 此增加副本数量不会改变第一个主节点的模板。
//
// 如果没有选举，任何角色都没有持有代币的理由。在高可用架构下，主节点通过 Lease 机制进行选举，而明确指向
// Lease 的成员会读取该值。该成员账户会针对两个地址值进行渲染。该谓词被命名是因为有五个渲染操作会引用它。
func leaderNeedsAPIAccess(leader workercore.KVCacheBackendLeader) bool {
	return leader.KubernetesElectionEnabled()
}

// leaderServiceAccountName 是主节点 Pod 运行的账户名称，在无高可用配置的情况下为空。
//
// “Empty”是 API 服务器所识别的 "default" 状态，在此功能存在之前，Pod 就是采用这种状态运行的。无条件为账户命
// 名显然也是另一种常见做法，但这是错误的：该账户仅在高可用环境下才会被显示，因此 Pod 在非高可用环境下为账户
// 命名则永远无法获得令牌。
func leaderServiceAccountName(kvcb *workercore.KVCacheBackend) string {
	if !leaderNeedsAPIAccess(kvcb.Spec.Connection.Managed.Leader) {
		return ""
	}

	return LeaderObjectName(kvcb)
}

// memberServiceAccountName 对于成员组来说是相同的。参见 leaderServiceAccountName。
func memberServiceAccountName(kvcb *workercore.KVCacheBackend) string {
	if !leaderNeedsAPIAccess(kvcb.Spec.Connection.Managed.Leader) {
		return ""
	}

	return MemberRBACObjectName(kvcb)
}

// HARBAC 保存某个角色参与选举需要的授权对象。启用时各字段都有值，关闭时均为 nil。
//
// 他们一起旅行，因为部分集合是此操作器无法观察到的一种失败状态：没有绑定关系的账户，或者没有角色的绑定关系，
// 会以“无人”身份进行认证，而无论是 API 服务器还是存储服务都没有以能指出缺失对象的明确方式说明这一点。
type HARBAC struct {
	ServiceAccount *core.ServiceAccount
	Role           *rbac.Role
	RoleBinding    *rbac.RoleBinding
}

// 需要报告一下这个后端是否真的需要该访问权限。
func (r HARBAC) Wanted() bool { return r.ServiceAccount != nil }

// RenderLeaderRBAC 会展示主节点赢得选举所需具备的条件，或根本不会展示任何内容。
//
// 要求：此处每个动词都应依据商店的 C++ 侧调用代码来理解，而非其 Go 包装器提供的内容。两者存在差异，这种差异
// 实际上是一种过度授权：包装器导出了 K8sLeaseWatchHolder 并发出一个集合监视任务，但 C++ 代码中没有任何地方
// 调用它——K8s 控制器通过定时重新读取该任务来执行监视操作。因此，由包装器表面产生的授权包含 `watch` 功能，而
// 实际上没有地方使用它。
func RenderLeaderRBAC(kvcb *workercore.KVCacheBackend) HARBAC {
	return renderHARBAC(kvcb, LeaderObjectName(kvcb), leaderResourceNoteRole, []rbac.PolicyRule{
		{
			APIGroups: []string{"coordination.k8s.io"},
			Resources: []string{"leases"},
			// LeaseLock 只需三种操作权限，即读取现有 Lease、在记录不存在时创建它，以及在续约时更新它。
			Verbs: []string{"create", "get", "update"},
		},
		{
			APIGroups: []string{""},
			Resources: []string{"pods"},
			// 主节点在赢得后会对其自身的 Pod 进行标记，这是存储系统主动进行的操作。该操作器既不
			// 会请求它，也不会读取它，也无法拒绝它：主节点始终保留的 Pod 身份标识符合其中三个条
			// 件之一，而开启 HA 功能则满足第三个条件。不执行该操作并不会停止其行为——它会在主节
			// 点的生命周期内，每秒记录一次警告信息。
			Verbs: []string{"patch"},
		},
	})
}

// RenderMemberRBAC 会渲染该账户作为明确针对 Lease 需求的成员所需的内容，或什么都不显示。
//
// 一个针对 Lease 的成员在连接时读取持有者信息，并再次读取以参与选举。它从不拥有领导权，因此此账户的范围比主
// 节点的要小。默认的 Service 地址不会使用此账户，但两个地址值都会接收它。
//
// 有限制：该读取操作属于投票类型。Kubernetes 控制器的 WaitForViewChange 函数每 200ms 重新读取一次持有者数据，
// 而非持续监控，因此每个涉及 Lease 的成员每秒会对 API 服务器进行约五次读取操作，且这一成本会随涉及 Lease 的
// 成员数量增加而上升。
func RenderMemberRBAC(kvcb *workercore.KVCacheBackend) HARBAC {
	return renderHARBAC(kvcb, MemberRBACObjectName(kvcb), memberResourceNoteRole, []rbac.PolicyRule{
		{
			APIGroups: []string{"coordination.k8s.io"},
			Resources: []string{"leases"},
			Verbs:     []string{"get"},
		},
	})
}

// renderHARBAC 在由后端拥有的共享系统命名空间中构建一个角色的三个对象。
//
// 有限制：其调用者所传递的规则中不存在 resourceNames。RBAC 不会将其应用于 `create` 操作，而仅限制某些动词的
// 使用，会使得一个后端中的 Lease 与另一个后端的 Lease 被孤立，实际上并未实现此效果。这些授权在租约层面是全
// 命名空间范围的，而这里每个账户都位于该操作员所拥有的一个命名空间中的某个信任域中。
func renderHARBAC(
	kvcb *workercore.KVCacheBackend, name, noteRole string, rules []rbac.PolicyRule,
) HARBAC {
	if !leaderNeedsAPIAccess(kvcb.Spec.Connection.Managed.Leader) {
		return HARBAC{}
	}

	// 该标识将每个由后端渲染的对象与对应角色自身的组件值关联起来。这些标识是依据与工作负载相同的常量生
	// 成的，无需重新表述，因为这三个对象是通过与 Deployment 和 DaemonSets 相同的方式被发现的。
	labels := map[string]string{
		labelKeyName:      labelValueName,
		labelKeyInstance:  kvcb.Name,
		labelKeyComponent: noteRole,
		labelKeyPartOf:    labelValuePartOf,
	}
	objectMeta := func() meta.ObjectMeta {
		return meta.ObjectMeta{
			Name:      name,
			Namespace: kuberess.SystemNamespaceName,
			Labels:    labels,
		}
	}

	sa := &core.ServiceAccount{ObjectMeta: objectMeta()}
	role := &rbac.Role{ObjectMeta: objectMeta(), Rules: rules}
	binding := &rbac.RoleBinding{
		ObjectMeta: objectMeta(),
		RoleRef: rbac.RoleRef{
			APIGroup: rbac.GroupName,
			Kind:     "Role",
			Name:     name,
		},
		Subjects: []rbac.Subject{
			{
				Kind:      rbac.ServiceAccountKind,
				Name:      name,
				Namespace: kuberess.SystemNamespaceName,
			},
		},
	}

	for _, obj := range []kubemeta.MetaObject{sa, role, binding} {
		systemmeta.NoteResource(obj, kvcache.ResourceType, map[string]string{
			kvcache.ResourceNoteBackend: kvcb.Name,
			"role":                      noteRole,
		})
		kubemeta.ControlOnWithoutBlock(obj, kvcb,
			workercore.SchemeGroupVersion.WithKind("KVCacheBackend"))
	}

	return HARBAC{ServiceAccount: sa, Role: role, RoleBinding: binding}
}
