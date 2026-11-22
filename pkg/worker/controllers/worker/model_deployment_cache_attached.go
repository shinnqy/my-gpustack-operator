package worker

import (
	"context"
	"fmt"

	core "k8s.io/api/core/v1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubeapistatus"
)

// ModelDeploymentConditionCacheAttached 上报缓存是否被观测到实际生效，
// 这与是否配置了缓存是两个不同的问题。
//
// 参数被接受并不能说明任何问题：在已发布的 store 上实测显示，`--enable_kv_events=true` 会被
// 接受，启动日志也会回显 `enable_kv_events=1`，但 `GET /kv_events/status` 仍返回
// `{"enabled":false}`，且 socket 从未绑定。同一项目中另一个未声明的开关则会直接报错失败。
// 无法从一个开关的失败模式推断另一个开关的行为，因此该 condition 必须在引擎下游判断，
// 绝不依据渲染结果或日志行。
const ModelDeploymentConditionCacheAttached kubeapistatus.ConditionType = "CacheAttached"

const (
	modelDeploymentReasonCacheActive            = "CacheActive"
	modelDeploymentReasonCacheOperationsFailing = "CacheOperationsFailing"
	modelDeploymentReasonCacheNotApplicable     = "NotApplicable"
	modelDeploymentReasonUnmanaged              = "Unmanaged"
	modelDeploymentReasonCacheNotAttributable   = "CacheNotAttributable"
	modelDeploymentReasonNoReplicaReady         = "NoReplicaReady"
	modelDeploymentReasonNoObservationAvailable = "NoObservationAvailable"
)

// ModelDeploymentCacheReading 表示单个 replica 自身对其 KV cache 客户端的观测报告。
//
// 共有三个枚举值，且第三个并非否定状态。我们对三种受支持引擎的实际暴露指标进行了测量，
// 在没有任何流量之前，没有任何引擎会发布表明“connector 已初始化”的指标：vLLM 的五个
// `vllm:mooncake_store_operation_*` 指标族均带标签，仅在非空数据的观测路径上才会调用
// `.labels()`；vllm-ascend 根本未声明任何自有 Prometheus 指标；SGLang 的存储计数器也以
// 相同方式带标签，其 `hicache_host_*` gauge 虽在启动时出现，但仅能证明 host 层已开启。因此
// “无报告”与“已连接但空闲”无法区分，若将前者判为未连接，会在最常见的稳态下产生误报。
//
// 因此本类型刻意设计为无法表达无流量时的“已初始化”读数：如果用一个替身去报告引擎
// 并不提供的信号，会使 Unknown 路径无法测试，且围绕它的所有测试都将是在测试并不存在的信号。
type ModelDeploymentCacheReading int

const (
	// ModelDeploymentCacheUnreadable 表示完全没有观测报告：endpoint 未响应，或者响应了但
	// 未携带任何 store 指标族。其对应 Unknown，绝不是 False。
	ModelDeploymentCacheUnreadable ModelDeploymentCacheReading = iota
	// ModelDeploymentCacheFailing 表示报告的 store 操作无一成功。这是正向证据而非信号缺失：
	// 这些指标族带有 status 标签，实测取值为 "ok"、"error" 和 "partial_failure"，因此只有
	// 尝试过但失败的 replica 才会发布其他情况下不会出现的时间序列。
	ModelDeploymentCacheFailing
	// ModelDeploymentCacheActive 表示报告了成功的 store 操作。
	ModelDeploymentCacheActive
)

// ModelDeploymentCacheScraper 读取单个 replica 自身对其 KV cache 客户端的观测报告。
//
// 它作为 reconciler 接收的接口而非直接发起的连接，因为这里真正关心的场景全是失败情况，
// 而真实的连接无法按需制造失败。
//
// 该报告按 replica 维度在 Pod 自身地址上获取，这也是整个 condition 成立的基础：reuse domain
// 按设计由同一个 Binding 上的所有 deployment 共享，因此 domain 级别的指标无法归因——
// 若同一个 Binding 上同时存在一个健康 deployment 和一个故障 deployment，健康方的写入
// 会把故障方也报告为已连接。
//
// TODO: 生产环境中目前没有任何地方为 reconciler 注入 scraper，因此每个就绪的 replica
// 都会被视为未提供报告，而依赖报告的两个判定——来自成功 replica 的 CacheActive 以及
// CacheOperationsFailing——均不可达。所有 store 操作均失败但仍在提供服务的 deployment
// 因此会上报 Unknown。测试之所以通过是因为注入了 fake，单看本文件可能会误以为该路径
// 已在运行。接入真实实现应与指标聚合本就必须构建的按 Pod 抓取路径合并，单独为本
// condition 构建第二条抓取路径会造成重复。
type ModelDeploymentCacheScraper interface {
	// ScrapeCache 读取单个 replica 的观测报告。
	//
	// 非 nil 的 error 表示完全无法读取报告，此时读数为 Unreadable。返回 Unreadable 且 error 为
	// nil 则是另一种无报告的情况：endpoint 正常响应，但未携带任何 store 指标族。
	ScrapeCache(ctx context.Context, pod *core.Pod) (ModelDeploymentCacheReading, error)
}

// observeModelDeploymentCache 将缓存连接观测结果合入 status。
//
// 基于水平触发的逻辑，每轮都会重新评估，绝不靠假设置为 True。
func (r *ModelDeploymentReconciler) observeModelDeploymentCache(
	ctx context.Context, md *workercore.ModelDeployment, pods []core.Pod,
	domain *modelDeploymentDomain, holder *workercore.ModelDeployment,
) {
	if md.Spec.KVCache == nil {
		ModelDeploymentConditionCacheAttached.True(holder, modelDeploymentReasonCacheNotApplicable,
			"no shared KV cache was requested; direct prefill/decode transfer is reported by the router contract")

		return
	}

	if replica, reason, why := modelDeploymentUnclaimableReplica(md, pods); replica != "" {
		// 本 operator 未提供任何可由该 replica 负责的配置，因此不会将该观测归因于自身行为。
		// 无论 pool 报告什么，也无论共享 domain 佐证什么：domain 级别的事实不能替代命令行无法
		// 归因的 replica。
		ModelDeploymentConditionCacheAttached.Unknown(holder, reason,
			fmt.Sprintf("%s does not report a cache client it was not shown to have: %s", replica, why))

		return
	}

	ready := modelDeploymentReadyReplicas(md, pods)
	if len(ready) == 0 {
		ModelDeploymentConditionCacheAttached.Unknown(holder, modelDeploymentReasonNoReplicaReady,
			"no replica has become ready yet, so no engine has an account to give")

		return
	}

	active, failing, unreadable := r.readModelDeploymentCache(ctx, ready)

	switch {
	case active > 0:
		ModelDeploymentConditionCacheAttached.True(holder, modelDeploymentReasonCacheActive,
			fmt.Sprintf("%d of %d ready replicas report succeeding store operations",
				active, len(ready)))

	case failing > 0:
		// 这不是信号缺失，而且这是唯一没有更近层观测者的状态。无法启动的 connector 会拖垮整个
		// replica（vLLM 在缺少配置路径时会直接报错退出），因此该失败已通过 replica 永不 Ready
		// 上报。这里的情况不同：引擎已 Ready 且正在服务，但每次尝试的 store 操作都失败。status 中
		// 没有其他字段能体现这一点，因此此处判定为 False 而非 Unknown。
		ModelDeploymentConditionCacheAttached.False(holder,
			modelDeploymentReasonCacheOperationsFailing,
			fmt.Sprintf("%d of %d ready replicas report store operations of which none succeeded; "+
				"the engine is serving without the cache", failing, len(ready)))

	case domain != nil && modelDeploymentDomainHoldsData(domain):
		// 佐证信号，仅在没有任何 replica 提供报告时使用。这是 master 自身对已落盘字节数的统计，
		// 作为缓存正常工作的证据更强，但归因能力更弱——它是按 reuse domain 统计的，而一个 domain
		// 按设计是可以共享的。
		ModelDeploymentConditionCacheAttached.True(holder, modelDeploymentReasonCacheActive,
			fmt.Sprintf("no replica gave an account, and reuse domain %q holds data; this attributes "+
				"to the domain rather than to this deployment, because one domain is shared by "+
				"every deployment on its binding", domain.KVCache.Domain.Name))

	default:
		// 判定为 Unknown 是因为其本应报告的状态已有更近层的观测者，而不仅仅是因为信号缺失。
		// 启动失败的 connector 会导致其 replica 退出，而未 Ready 的 replica 已通过
		// status.roles[].ready 上报。剩余情况是已连接但处于空闲状态的 deployment，这与未读取到
		// 指标的 deployment 无法区分——绝不能将两者混为一谈地报告为未连接。
		//
		// 缺失不等于零：Binding 上缺少 blocks/usage 表示抓取未包含该 domain，而观测到零表示
		// domain 未持有任何数据，空闲的已连接 domain 同样也不持有任何数据。
		ModelDeploymentConditionCacheAttached.Unknown(holder,
			modelDeploymentReasonNoObservationAvailable,
			fmt.Sprintf("none of the %d ready replicas gave an account of its cache client and the "+
				"reuse domain reports nothing held; an attached deployment that is idle looks "+
				"exactly like this, so it is not reported as detached", unreadable))
	}
}

// readModelDeploymentCache 抓取每个就绪 replica 并统计返回结果。
//
// 当构建 reconciler 时未提供 scraper，将所有 replica 视为不可读，而不是返回 error：该
// condition 的完整契约即未读取到信号时判定为 Unknown。
func (r *ModelDeploymentReconciler) readModelDeploymentCache(
	ctx context.Context, ready []*core.Pod,
) (active, failing, unreadable int) {
	if r.CacheScraper == nil {
		return 0, 0, len(ready)
	}

	for _, pod := range ready {
		reading, err := r.CacheScraper.ScrapeCache(ctx, pod)
		if err != nil {
			unreadable++

			continue
		}
		switch reading {
		case ModelDeploymentCacheActive:
			active++
		case ModelDeploymentCacheFailing:
			failing++
		case ModelDeploymentCacheUnreadable:
			unreadable++
		}
	}

	return active, failing, unreadable
}

// modelDeploymentUnclaimableReplica 找出一个属于已声明角色但其运行命令行无法由本
// operator 负责的 replica，并返回机器可读的 reason 和消息。它同时拒绝运行非本 operator
// 编写命令行的 replica，以及命令行无法归因的 replica，因为共享 domain 不得用于佐证这两者。
func modelDeploymentUnclaimableReplica(
	md *workercore.ModelDeployment, pods []core.Pod,
) (string, string, string) {
	declared := make(map[string]bool, len(md.Spec.Roles))
	for i := range md.Spec.Roles {
		declared[md.Spec.Roles[i].Name] = true
	}

	for _, replica := range modelDeploymentGroupPodsByReplica(pods) {
		// 某些引擎渲染的辅助 head 节点由本 operator 构建，但不携带任何相关 annotations，因此仅
		// 检查 spec 中声明的角色。
		if !declared[replica.Role] {
			continue
		}

		execution, why := modelDeploymentDeployedReplicaExecution(replica.Members)
		switch execution {
		case modelDeploymentExecutionTakeover:
			return modelDeploymentPodDescription(replica.Members[0]),
				modelDeploymentReasonUnmanaged,
				"it runs a command line this operator contributed no arguments to, so it was " +
					"configured with no cache client and is not reported on one"
		case modelDeploymentExecutionUnreadable:
			return modelDeploymentPodDescription(replica.Members[0]),
				modelDeploymentReasonCacheNotAttributable, why
		}
	}

	return "", "", ""
}

// modelDeploymentReadyReplicas 筛选出可向引擎发起查询的 replica：完整、就绪、未处于退出
// 过程中，并由每个 replica 中负责响应的那个成员代表。正在终止的 replica 仍可能响应，
// 其报告反映的是即将退出的进程状态。
//
// 仅向 leader 查询，因为只有 leader 提供接收该查询的 API。多成员 replica 运行着横跨各成员
// 的单一引擎，并仅在 0 号成员暴露接口；其余成员仅承担 rank，不会响应。向它们查询并不会
// 得到不同的缓存报告——只会得到空结果，而这些无响应结果会计入不可读或失败统计，从而导致
// 健康的多成员 deployment 的缓存 condition 因与缓存毫无关系的理由变成 False。
//
// 就绪的含义与已发布的 replica 计数保持一致，这也是为何按整个 replica 而非仅按 leader
// 评估。只有当一个 replica 的所有成员都就绪时，该 replica 才算就绪，此 condition 旁边的
// role status 也是这样统计的；否则，当 leader 已 Ready 而同级成员缺失或仍在启动时，
// 此 condition 对缓存的判断就会与零个就绪数的状态并存，让读者无从判断该相信哪一个。
// 没有机制强制引擎的 leader 在集群组建完成前保持未就绪——readiness probe 由 deployment
// 编写者定义——因此这种一致性必须在此处保证，而不能靠假设。
//
// 不再在 spec 中声明的角色的 replica 不会被查询。其规模未知，没有基准数可用来校验成员
// 数量；而且 deployment 停止声明的 Pod 正处于下线途中，不再属于 deployment 需要汇报
// 缓存状态的范围。
//
// 不携带成员标签的 Pod 即视为 leader，这使得该过滤逻辑兼容此前单成员形态：单成员
// replica 的 Pod 没有此类标签，默认按 0 号成员处理，只要该 Pod 存在即视为完整，因此
// 最终查询的 Pod 集合与之前完全一致。
func modelDeploymentReadyReplicas(md *workercore.ModelDeployment, pods []core.Pod) []*core.Pod {
	declared := make(map[string]bool, len(md.Spec.Roles))
	for i := range md.Spec.Roles {
		declared[md.Spec.Roles[i].Name] = true
	}

	// 分组会剔除已处于退出过程中的成员，因此失去成员的 replica 在此处会少于其声明总数，
	// 并与不完整的 replica 一同被过滤掉。
	//
	// 完整性依据 replica 自身的组总数读取。角色声明的规模描述的是即将构建的 replica，
	// 若角色规模被修改，使用新规模就会把该角色所有健康的旧 replica 都排除在可查询引擎集合
	// 之外——从而把配置变更变成静默的缓存观测盲区。
	ready := make([]*core.Pod, 0, len(pods))
	for _, replica := range modelDeploymentGroupPodsByReplica(pods) {
		if !declared[replica.Role] {
			continue
		}
		if !modelDeploymentDeployedReplicaShape(replica).modelDeploymentReplicaIsWhole() {
			continue
		}
		// 仅抓取已证实受管的 replica：上述检查已拒绝对其余 replica 的处理。
		if execution, _ := modelDeploymentDeployedReplicaExecution(replica.Members); execution !=
			modelDeploymentExecutionManaged {
			continue
		}

		var leader *core.Pod
		whole := true
		for _, member := range replica.Members {
			if !podIsReady(member) {
				whole = false

				break
			}
			if modelDeploymentPodMemberIndex(member) == modelDeploymentLeaderMemberIndex {
				leader = member
			}
		}
		if whole && leader != nil {
			ready = append(ready, leader)
		}
	}

	return ready
}

// modelDeploymentDomainHoldsData 上报 master 对 reuse domain 的统计中是否显示持有任何数据。
//
// 两个指标中的 nil 都不能等同于零。两者在 Binding 上都是带 omitempty 的指针，因此缺失
// 表示抓取未包含该 domain；若将其视作零，会把每个未被抓取的 pool 都变成确认未连接的报告。
func modelDeploymentDomainHoldsData(domain *modelDeploymentDomain) bool {
	if domain.KVCache == nil {
		return false
	}
	if domain.Blocks != nil && *domain.Blocks > 0 {
		return true
	}

	return domain.Usage != nil && !domain.Usage.IsZero()
}
