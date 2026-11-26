package mooncake

import (
	"fmt"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/worker/kuberess"
)

const (
	// LeaderRPCPort 是 leader 提供 RPC 服务的端口，也是引擎客户端连接的端口。
	// 这里将其固定而未沿用构件自身的默认值，是因为 Service 和对外发布的端点
	// 必须明确指定端口号，若默认值发生变动会导致这些配置静默漂移。
	LeaderRPCPort = 50051
	// LeaderMetricsPort 是 leader 同时提供 Prometheus 指标暴露和 HTTP 管理 API
	// 的端口。单端口承载双接口是构件本身的设计，而非此处的简化处理，这也是对外
	// 发布的管理端点与指标抓取目标使用同一地址的原因。
	LeaderMetricsPort = 9003

	// LeaderPodNameEnv 和 LeaderPodNamespaceEnv 是在 Kubernetes 选举模式下
	// 渲染出的 argv 所引用的环境变量。运行该 argv 的 Workload 必须通过
	// downward API 定义这两个变量，引用才能正确解析。
	//
	// 它们沿用本仓库自身的命名，而非该标志文档中写明的默认来源 POD_NAME / POD_NAMESPACE。
	// 这不会带来任何缺失：相关标志均会显式渲染，因此构件永远不会回退去自行读取
	// 默认环境变量，而且本仓库中的其他所有组件也均采用这种拼写方式。
	LeaderPodNameEnv      = "KUBERNETES_POD_NAME"
	LeaderPodNamespaceEnv = "KUBERNETES_POD_NAMESPACE"
	// LeaderPodIPEnv 是第三个环境变量，与前两个一样，仅在启用 Kubernetes 选举时
	// 定义，因为只有选举逻辑会读取它。其具体作用参见 -rpc_address 标志。
	LeaderPodIPEnv = "KUBERNETES_POD_IP"

	// LeaderKVLeaseTTL 表示缓存对象受保护免于淘汰的时长，用于替代构件自身的 10 秒默认值。
	//
	// 租约在对象被读取时授予，而非写入时：完成写入时授予的是零时长租约，因此尚未被
	// 读取过的对象可立即被淘汰。因此该值限制的是最近读取集合（RECENTLY READ）的大小，
	// 而非写入速率；淘汰机制会直接跳过受保护对象，即使找不到其他可淘汰对象也不会回退选择它们。
	//
	// 10 秒过短，一个引擎副本刚读取的块，在下一个副本请求之前就可能再次变为可淘汰状态，
	// 而共享缓存存在的意义正是提供这种跨副本复用。5 分钟足以覆盖该需求，且只要在该窗口内
	// 读取的数据集未超出存储容量就不会产生额外开销；若超出，则说明存储容量低于自身工作集，
	// 而非该配置值导致的问题。
	//
	// 它采用时长字符串格式，因为该标志支持 ms、s、m 和 h 后缀。
	LeaderKVLeaseTTL = "5m"
)

// leaderAllocationStrategies 将该 API 对分配策略的命名规则映射到具体对象上。这是两个术语交汇的地方：由于该
// 组中的每个枚举类型都采用 CamelCase 命名方式，因此该 API 使用 CamelCase；而标志则采用 snake_case 命名方式。
// 此外，该表的另一份副本也体现了这种差异。
//
// 它并非总是接受旗帜所接受的每一个值——具体原因可参考 API 类型说明。
var leaderAllocationStrategies = map[string]string{
	"Random":         "random",
	"FreeRatioFirst": "free_ratio_first",
}

// RenderLeaderFlags 会将后端设置为其主节点进程运行的 argv 环境。
//
// 这是一个纯函数，具有确定性的执行顺序，因此无需集群即可测试整个标志面，且渲染后的 Deployment 差异与上一个
// 版本能够清晰对比。
//
// 需要整个对象的信息，而不仅仅是主节点信息，因为选举标志指出了该后端所选举的 Lease 名称，而这个名称就是该对象的身份。
//
// 它不生成的部件与它所生成的部件一样具有承重作用：
//
//   - 点对点元数据平面没有外部元数据存储，因而不生成任何元数据标志，也无需设置 -etcd_endpoints。
//   - 无 -port。该选项已废弃，应改用 -rpc_port。
//   - 该工件没有使用其默认标志。该规范未涉及的标志也缺失了，因此改变上游的默认设置会表现为需要调查的行为变
//     化，而非我们默默重新指定的值。
//
// `-`default_kv_lease_ttl 是一个独立于所有其他标志的标识，在双向情况下均如此：它从没有任何字段生成，其数值
// 由此处设定，而非原始对象中的数值；也是唯一允许通过处理而覆盖的渲染标识。其渲染位置说明了原因。
func RenderLeaderFlags(kvcb *workercore.KVCacheBackend) []string {
	leader := kvcb.Spec.Connection.Managed.Leader

	flags := []string{
		fmt.Sprintf("-rpc_port=%d", LeaderRPCPort),
		fmt.Sprintf("-metrics_port=%d", LeaderMetricsPort),
	}

	// 无条件渲染，不来自任何领域，且未针对穿透操作进行保留。这里的其他标志都是其一；这个则不是，而两方
	// 面的特性都是关键。
	//
	// 之所以能显示出来，是因为该资产的默认设置会保护一个缓存对象长达十秒，这一时间短于一个推理引擎副本
	// 读取一个数据块与下一个副本请求该数据块之间的间隔——因此，默认设置实际上会消耗共享缓存的复用功能，
	// 而共享缓存的存在正是为了提供这种功能。它仍然可访问，因为适合部署的数值取决于其读取模式和存储大小，
	// 而这些都不是该 API 所描述的，所以这里的一个单一数值只能作为一个更好的起点，而永远不是答案。
	//
	// 有两个机制使得覆盖操作能够生效，且两者都需要被持有。关键项不在主节点的派生列表中，因此准入机制会
	// 在 extraArgs 中接受它，而不会将其作为冲突来拒绝。此外，该工件会解析“最后一方获胜”的重复标志，而通
	// 过操作则会在这一行之后追加——因此操作者的条目就是真正生效的条目，无需对 extraArgs 进行任何检查。
	flags = append(flags, "-default_kv_lease_ttl="+LeaderKVLeaseTTL)

	// 当 electionBackend 为 Kubernetes 时，选举结果会作为一个组呈现，包括一个副本的情况。将组拆分是一种
	// 部分渲染方式：-enable_ha 选项在没有连接字符串的情况下会在启动时生效；而没有 -enable_ha 的连接字符
	// 串则会被接受并忽略，同时会发出警告。
	//
	// ElectionBackend 当前接受 Kubernetes 或 `None` 值。Kubernetes 值用于识别此后端类型；未来的值则需要
	// 使用独立的标志进行标识。
	//
	// -ha_backend_connstring 的格式为 namespace/lease-name。后端对象是集群级的，渲染的资源共用一个命名
	// 空间，使用 LeaderObjectName 为各后端派生独立 Lease。否则两个后端可能在同一份 Lease 上竞争同一领导权。
	//
	// REQUIRED: -rpc_address 属于该组，尽管它看起来像是一个绑定设置。该组件将其合并为 "
	// rpc_address:rpc_port"，并以该字符串进行宣传，该字符串既作为选举的标识，也作为写入 Lease 中的地址，
	// 适用于那些明确选择 Lease 寻址的成员。默认设置为 0.0.0.0 后，每个副本都属于同一个标识，每个遵循
	// Lease 的成员都会获得一个可以解析回自身的地址。Pod IP 是每个副本独有的唯一值，可以从另一个 Pod 访
	// 问到，并且也能正确绑定，因为该组件将其相同的字符串传递给其 RPC 服务器。
	if leaderNeedsAPIAccess(leader) {
		flags = append(flags,
			leaderElectionFlag,
			"-ha_backend_type=k8s",
			fmt.Sprintf("-ha_backend_connstring=%s/%s",
				kuberess.SystemNamespaceName, LeaderObjectName(kvcb)),
			fmt.Sprintf("-rpc_address=$(%s)", LeaderPodIPEnv),
			// 该组件自身的默认值是所有部署共享的固定字符串，因此保持原样就不会产生冲突。它负责
			// 控制存储自身的命名空间机制，而非选举机制，因此是从身份中衍生出来的，而非来自 Lease。
			"-cluster_id="+LeaderObjectName(kvcb))
	}

	// 未设置的策略不会产生任何结果，只能靠猜测：CRD 架构将此字段设为默认值，因此空值表示该对象从未经过审核。
	if mapped, ok := leaderAllocationStrategies[leader.AllocationStrategy]; ok {
		flags = append(flags, "-allocation_strategy="+mapped)
	}

	// 仅在被请求时渲染。显式设置 -enable_multi_tenants=false 将作为该资产的默认设置，并可能导致所有不希
	// 望此设置的后端在命令行中出现问题。
	//
	// 把启用多租户的开关和策略连接器 URI 一起传入。master 初始化文件连接器时会拒绝默认的空 URI，单独启
	// 用开关会让进程启动失败。-tenant_quota_connector_type 默认为 file，工作负载也只提供这一种来源。
	if leader.MultiTenancyEnabled() {
		flags = append(flags,
			"-enable_multi_tenants=true",
			"-tenant_quota_connector_uri="+QuotaPolicyFilePath)
	}

	// 磁盘层的主节点部分，它没有独立的字段：一个声明了 localDisks 条目的成员组就能激活该层，因此主节点
	// 的标志是从这些成员声明中衍生出来的。只有当某个组声明了此选项时才会显示，就像这里的其他所有标志一
	// 样——该元素的默认值为 false，所以显式的 -enable_offload=false 会重新设定默认值，同时也会让那些从未
	// 需要该层的后端系统的命令行参数发生变化。
	//
	// 一并生成这两个标志。-offload_on_evict 选择延迟模式，只有该模式受存储侧保护。准入也会限制透传参数，
	// 避免其中一个标志被单独设置。
	if memberHasDiskTier(kvcb) {
		flags = append(flags, "-enable_offload=true", "-offload_on_evict=true")
	}

	// Pod 自身的身份仅在选举过程中显示，因为选举是唯一的读取者：该组件利用此身份将获胜的 Pod 标记为主节
	// 点，且仅在 KubernetesLease 后端下如此。它位于此处而非选举组中，因此选举后端的`argv`值与无条件显示时保持一致。
	//
	// 无条件模式并非免费。该组件仅接受来自 0.3.12 版本的两种标志，而另一个未知的标志在启动时会导致系统
	// 崩溃，因此所有较旧的图像在推理服务启动前就会退出——包括那些由单个主节点运行的图像也不例外。
	if leaderNeedsAPIAccess(leader) {
		flags = append(flags,
			fmt.Sprintf("-pod_name=$(%s)", LeaderPodNameEnv),
			fmt.Sprintf("-pod_namespace=$(%s)", LeaderPodNamespaceEnv))
	}

	// 最后按用户声明的顺序追加透传参数，保证相同 spec 生成相同 argv。准入会拒绝与字段派生标志同名的参数。
	// -default_kv_lease_ttl 没有对应的 spec 字段，允许显式覆盖。每项保留自己的短横线，作为一个完整参数传入。
	flags = append(flags, leader.ExtraArgs...)

	return flags
}

// memberHasDiskTier 用于报告此后端是否存在任何成员组声明了本地磁盘层，这是主节点的卸载标志所依据的事实。
func memberHasDiskTier(kvcb *workercore.KVCacheBackend) bool {
	for i := range kvcb.Spec.Connection.Managed.Members {
		if len(kvcb.Spec.Connection.Managed.Members[i].LocalDisks) > 0 {
			return true
		}
	}
	return false
}
