package worker

import (
	"context"
	"fmt"
	"slices"

	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	kueuepodconst "sigs.k8s.io/kueue/pkg/controller/jobs/pod/constants"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/systemname"
	"gpustack.ai/gpustack/pkg/utils/strconvx"
	"gpustack.ai/gpustack/pkg/utils/stringx"
)

// 我们不使用自有 label 携带角色名称，这是有意设计而非疏漏。
// 每个副本上的 app.kubernetes.io/component 已经保存了角色名称：它既是 Service
// 选择器所匹配的 label，也是 modelDeploymentPodRole 在状态中将 Pod 归属到对应角色
// 时读取的字段。若再增加第二个可供选择的载体，就会出现由两个函数写入的同一问题的
// 两个答案；它们目前或许一致，但一旦发生漂移，出问题的往往是故障路径上无人读取
// 的那一个，其表现就是 Service 没有端点，或者角色报告无就绪副本。
//
// 当前项目域内有一个键携带角色类型：modelDeploymentLabelKeyRoleKind。
// 这与上述情况不同，也不会削弱上述原则。类型是仅包含三个值的封闭集合，副本前端
// 组件借此区分 prefiller 与 decoder；而名称是用户自由定义的字符串，只有当前
// 部署自身的对象才会匹配它。因此类型键没有第二个写入方会导致漂移——它与状态回显
// 同样派生自 ModelDeploymentEffectiveRoleKind——而若另设名称键则会与
// app.kubernetes.io/component 重叠。
//
// 新增该键属于增量扩展，因此可以安全添加；而移除已经对外发布的键则会破坏依赖它进行
// 选择的调用方。

const (
	// modelDeploymentPodGroupNamePrefix 标记由本 operator 派生而非原样采用的组名。
	// 它沿用了 nodefeature.FormatLocalQueueName 处理过长 ClusterQueue 名称时的回退
	// 前缀，刻意采用相同拼写以便在各处保持一致的可辨识形态。
	//
	// 此处并未直接调用该函数：它的处理对象是 ClusterQueue 名称，在此复用会让组标识
	// 看起来像队列标识。两者共享的是命名形态，而非派生逻辑。
	modelDeploymentPodGroupNamePrefix = "gpustack-fnv64-"

	// modelDeploymentReplicaOrdinalLabel 记录副本渲染时的序号，它是收敛器所依赖的
	// 唯一单副本标识：组名由其派生，spec-hash 涵盖该序号，缩容时也依据它决定保留
	// 哪些副本。
	//
	// 该键基于 systemname.LabelPrefix 拼接而非硬编码拼写，因为该前缀是本项目声明
	// label 域名的唯一位置；若另写一份拼写，日后即使漂移也不会立即报错。
	modelDeploymentReplicaOrdinalLabel = "modeldeployment." + systemname.LabelPrefix + "pod-ordinal"

	// modelDeploymentMemberIndexLabel 记录当前 Pod 是所属副本中的第几个成员（从零开始计数）。
	// 上方的序号 label 指明属于哪个副本，此 label 指明属于该副本中的哪个成员，两者
	// 共同唯一确定部署中的一个 Pod。
	//
	// 它作为 label 而非渲染器写入的环境变量存在，这样才能让每个副本只需维护一份 Pod
	// 模板。容器通过指向该键的 fieldRef 读取自身 rank，从而使每个成员的容器规约完全
	// 一致而实际取值各异——若渲染器直接将索引写入 env，同一副本内两个成员的哈希就会
	// 不同，导致每个成员都被判定为待滚动更新。
	//
	// 这也使 leader 可被 Service 选择器选中。Service 选择器无法表达“名称以 -m0
	// 结尾的 Pod”，因此若仅代理提供 API 服务的成员，就必须让成员索引可被匹配，且
	// 索引 0 按构造即为 leader，而非通过选举产生。
	modelDeploymentMemberIndexLabel = "modeldeployment." + systemname.LabelPrefix + "member-index"

	// modelDeploymentLeaderMemberIndex 是每个副本都具备的成员索引，也是角色 Service
	// 唯一代理的成员。规模为 1 的副本本身即为 leader；规模大于 1 时，其余成员不提供 API。
	modelDeploymentLeaderMemberIndex = 0

	// modelDeploymentLabelKeyEndpointEligible 标记可被 Router 或普通 Service 选中的端点
	// 所属 Pod：仅在提供服务的成员上为 "true"（leader 服务模式的角色仅为 leader，
	// External-DP 形态的角色为所有携带 rank 的成员），其余情况下均不设置。不设置即表示
	// 不具备资格，因此不存在易被误读的 "false" 值。
	//
	// 该 label 由调谐器在运行时写入，绝不参与模板渲染：若放入模板，它会成为 spec-hash
	// 的一部分，摘除它以取消资格时反而会触发副本滚动重建，违背排空时保留该副本的初衷。
	// 上方的 Pod 自身身份 label 由模板渲染，而此 label 则是调谐器对 Pod 当前状态的
	// 电平式（level-based）判定结果。
	//
	// 与上方所有 label 一样，它也基于 systemname.LabelPrefix 构建，因为 Service
	// 选择器必须能在同一域名空间下匹配到它。
	modelDeploymentLabelKeyEndpointEligible = "modeldeployment." + systemname.LabelPrefix + "endpoint-eligible"

	// modelDeploymentEndpointEligibleValue 是资格键唯一携带的值。label 选择器本质上是
	// 一组等值条件的合取，因此该条件只看是否存在；该值之所以存在是因为选择器需要一个
	// 匹配值，而 "true" 明确表达了存在即有效的语义。
	modelDeploymentEndpointEligibleValue = "true"

	// modelDeploymentLabelKeyAPIAnswering 标记按其创建时所用模式的路由规则响应部署
	// HTTP API 的副本 Pod。
	//
	// 它表示路由归属，而非引擎资格判定。上方的资格键表示该成员的端点可提供服务；而
	// 此键表示该成员归属于哪个 Service 之后。两者之所以不同，原因只有一个：正在变更中
	// 的角色可能同时存在按不同命令模式构建的副本，每种模式都保留其原有的路由方式。
	// 接管模式（take-over）的副本无论本 operator 能否校验其引擎，均按旧规则响应请求；
	// 而托管副本则按资格规则响应请求，该规则已由上方的资格键承载。
	//
	// 该 label 由调谐器在运行时写入且绝不参与模板渲染，因此不计入 spec-hash：开启或
	// 关闭某个成员的路由绝不应导致正在提供服务的副本发生滚动重建。
	modelDeploymentLabelKeyAPIAnswering = "modeldeployment." + systemname.LabelPrefix + "api-answering"

	// modelDeploymentAPIAnsweringValue 是响应标记键唯一携带的值，原因与资格键相同。
	modelDeploymentAPIAnsweringValue = "true"

	// modelDeploymentTrueValue 是本包中布尔真值的统一拼写：既用于调谐器写入的
	// annotation，也用于 operator 读取的环境变量。
	//
	// 只定义一处是因为该拼写本身是同一概念，且包内有多处需要读取它：若在别处再次
	// 手写拼写，就会成为“true”在此处含义的第二个定义，日后两者可能出现偏差。
	modelDeploymentTrueValue = "true"
)

// ModelDeploymentPodGroupMeta 是单个副本的 Pod 所携带的 Kueue 组元数据。
//
// LABELS 与 ANNOTATIONS 必须一并返回，任何调用方都不得只应用其中一部分。成员
// 归属 label 用于将 Pod 纳入组内，而总数 annotation 用于告知 Kueue 需要等待的 Pod
// 数量；若 Pod 只带前者不带后者，就会加入一个规模未知的组，随后 Kueue 会拒绝
// 组装 Workload，且报错既不指明 Pod 也不指明字段。这种故障表现极其隐蔽，因此
// 两者必须作为整体一同传递。
type ModelDeploymentPodGroupMeta struct {
	// Labels 写入 Pod 的 metadata.labels。
	Labels map[string]string

	// Annotations 写入 Pod 的 metadata.annotations。
	Annotations map[string]string
}

// modelDeploymentPodGroupSpec 是部署调度视图中单个 role 的条目：包含 role、用于索引该 role
// 在调度视图中份额的名称，以及该 role 声明的总数。
//
// 该名称是一个关联键，而非查找对象的途径。Pod 的 Kueue 分组属于单个副本自身，由
// modelDeploymentReplicaGroupName 按 (role, ordinal) 派生，因此没有 Pod 会在其成员标签中
// 携带该名称。仍然使用此结构的路径（按组名索引 role 的 Workload 的状态路径）通过该名称
// 进行关联；这是按副本拆分后遗留的耦合，将由后续的状态重构工作进一步细化。
type modelDeploymentPodGroupSpec struct {
	// Role 是分组键：每个条目仅对应一个 role 而不属于其他任何 role，下方的总数即该 role
	// 声明的副本数量。
	Role string

	// InstanceType 仅作携带之用，不作为索引键。它是该 role 自身的实例类型，保存在此处以便所有
	// 提及 cluster queue 的消息和条件都能准确指明该 role 调度进入的队列：ClusterQueue 以
	// InstanceType 命名，若改用 role 名称则会把运维人员引向一个不存在的对象。
	InstanceType string

	// Name 是结构体注释中所述的按 role 划分的关联键。它在同一 role 内保持稳定，在不同 role
	// 之间互不相同，这正是关联键所需的全部性质。
	Name string

	// TotalCount 是当前 role 声明的副本数，不包含其他任何 role。
	TotalCount int32
}

// modelDeploymentPodGroups 将部署的各个 role 作为调度条目返回，每个 role 对应一项并保持
// roles 的原始顺序。
//
// 该函数是纯函数：相同的部署输入总是产生相同的结果，不依赖客户端也不依赖时钟。
//
// 分组键是 role，而不是 instanceType。Kueue 将每个副本作为独立单元进行准入，而 role 级
// 视图所要承载的一切——按 role 计数、按 role 分配 flavor、按 role 统计状态——仍然挂载在
// 该分组之上。若改用 instanceType 作为键，会把使用同一 instanceType 的两个 role 合并到
// 同一个条目中，其计数变化将无法从总数上体现：例如将 prefill 2 / decode 2 调整为
// prefill 1 / decode 3 时，总数依然是 4。
//
// 返回顺序与 roles 的顺序一致，因此对未变更的 spec 执行两次遍历会在相同位置返回相同的名称。
// map 的迭代顺序无法保证这一点，而这些名称需要作为状态 map 的键。
func modelDeploymentPodGroups(md *workercore.ModelDeployment) []modelDeploymentPodGroupSpec {
	groups := make([]modelDeploymentPodGroupSpec, 0, len(md.Spec.Roles))
	for i := range md.Spec.Roles {
		role := &md.Spec.Roles[i]
		groups = append(groups, modelDeploymentPodGroupSpec{
			Role:         role.Name,
			InstanceType: role.InstanceType,
			TotalCount:   role.Replicas,
			Name:         modelDeploymentPodGroupNameOf(md, role.Name),
		})
	}

	return groups
}

// modelDeploymentRoleSize 返回该 role 单个副本所包含的 Pod 数量。
//
// 该函数以 1 为下限，而非直接信任字段值。schema 默认将该字段设为 1，因此经过 API server
// 的对象至少都会携带该值；但在内存中构建的 role（所有测试和部分调用方都会这样做）其字段为
// 零值。一旦 0 传入 total-count 注解，就会声明一个成员数为 0 的分组，导致 Kueue 完全无法
// 为其组装 Workload：副本将无限期处于阻塞状态，且没有任何信息指明原因。
func modelDeploymentRoleSize(role *workercore.ModelDeploymentRole) int {
	if role.ReplicaSize < 1 {
		return 1
	}

	return int(role.ReplicaSize)
}

// modelDeploymentAnsweringShape 标识一个副本中哪些成员能够响应在途的读请求。
//
// 这是供两个掌握不同事实的调用方共用的单一枚举。端点可用性判断从 role 读取形状，而下线
// 排空逻辑则从成员 Pod 读取，因为 spec 已移除某 role 时，其遗留副本已没有对应的 role 可读。
// 两者共用这套词汇和下方函数，使规则只需编写一次，且任一调用方都不会曲解另一方对形状的定义。
type modelDeploymentAnsweringShape int

const (
	// modelDeploymentAnsweringUnknown 表示当前 operator 无法确定的响应形状。它不会由推导
	// 产生，仅出现在排空逻辑无法自洽时，并使排空流程保持挂起。
	modelDeploymentAnsweringUnknown modelDeploymentAnsweringShape = iota
	// modelDeploymentAnsweringAll 表示 External-DP 副本：每个成员都拥有自己的 rank 并运行
	// 各自的引擎监听端口，因此所有成员均响应请求。
	modelDeploymentAnsweringAll
	// modelDeploymentAnsweringLeader 表示由 leader 提供服务的跨多 Pod 副本：follower 仅加入
	// 集合通信而不提供指标端点，因此只有 leader 响应请求。
	modelDeploymentAnsweringLeader
	// modelDeploymentAnsweringSole 表示仅包含单个 Pod 的副本，该 Pod 本身即为其 leader。
	modelDeploymentAnsweringSole
)

// modelDeploymentAnsweringShapeOf 根据唯一决定响应形状的两个事实推导出对应的形状。
//
// 该推导永不返回 UNKNOWN，因为传入的两个条件均为确定值：尚未确定这两个条件的调用方不会
// 调用本函数。因无法获知形状而挂起的排空逻辑会给出自身的原因，该原因说明的是成员间的
// 不一致，而非某个无人能读取的形状。
func modelDeploymentAnsweringShapeOf(externalDP bool, size int) modelDeploymentAnsweringShape {
	if externalDP {
		return modelDeploymentAnsweringAll
	}
	if size <= 1 {
		return modelDeploymentAnsweringSole
	}

	return modelDeploymentAnsweringLeader
}

// modelDeploymentReplicaRunsCommand 返回副本的每个成员是否都使用本 operator 编写的
// 命令行启动。
//
// 若副本内成员状态不一致，或成员不包含可供读取的引擎容器，则将其排除：此时其运行模式
// 尚未确定，询问方绝不能被告知该副本处于受管状态。
func modelDeploymentReplicaRunsCommand(members []*core.Pod) bool {
	if len(members) == 0 {
		return false
	}
	for _, member := range members {
		managed, ok := modelDeploymentMemberExecution(member)
		if !ok || !managed {
			return false
		}
	}

	return true
}

// modelDeploymentAnyMemberRunsCommand 返回代表某 role 的成员中是否至少有一个是由本
// operator 构建的。处于过渡期的 role 可能同时存在两种模式的副本，而准入控制仍属于还在
// 提供服务的受管副本，因此只要有一个成员满足即可。
func modelDeploymentAnyMemberRunsCommand(members []*core.Pod) bool {
	for _, member := range members {
		if managed, ok := modelDeploymentMemberExecution(member); ok && managed {
			return true
		}
	}

	return false
}

// modelDeploymentDeployedAnsweringShape 根据副本成员当前正在运行的实际状态，而非 spec
// 当前声明的 role，推导出响应形状。
//
// 这在副本替换期间至关重要。当一个 role 从 4 成员改为 1 成员，或从 External 数据并行
// 模式改为 leader 服务模式时，其旧副本仍按构建时的形状继续提供服务。若直接读取期望的 role，
// 在 spec 改为 1 成员的瞬间就会撤回 4 成员副本的 follower，或在 spec 改为 leader 模式的
// 瞬间暴露整个 rank 组的所有成员——而这些旧副本此时仍在正常服务且尚未被任何新副本替换。
//
// 总成员数取自成员自身声明的值，并行度则直接从它们自身的命令行中读取。两者反映的都是眼前
// 这个副本的既成事实，而不是可能已经改变的 role 期望。
func modelDeploymentDeployedAnsweringShape(
	view modelDeploymentReplicaView, engine string,
) modelDeploymentAnsweringShape {
	total, reason := modelDeploymentReplicaDeclaredTotal(view.Members)
	if reason != "" || total <= 0 {
		// 成员对自身状态存在分歧的副本无法确定形状。保持等待是如实的做法，且保持现状可以保留
		// 它已携带的成员关系。
		return modelDeploymentAnsweringUnknown
	}

	externalDP, known := modelDeploymentMembersExternalDP(view.Members, engine)
	if !known {
		// 读取失败并不意味着内部数据并行。保持等待可保留副本已有的成员关系，避免基于猜测撤回
		// follower。
		return modelDeploymentAnsweringUnknown
	}

	return modelDeploymentAnsweringShapeOf(externalDP, total)
}

// modelDeploymentMembersExternalDP 根据副本成员正在运行的命令行，判断其是否采用
// 数据并行负载均衡。
//
// 该函数读取成员而非 role，原因与形状推导相同：role 描述的是下一个副本将如何构建，而
// 这些成员代表的是当前正在运行的状态。
//
// 当检查过受管成员但其中没有任何一个能成功读取时，known 为 false，因此结果并非确定的
// "leader-served"。非本 operator 构建的成员不携带受管的负载均衡配置，这属于确定的结论。
func modelDeploymentMembersExternalDP(members []*core.Pod, engine string) (externalDP, known bool) {
	consulted := false
	for _, member := range members {
		managed, readable := modelDeploymentMemberExecution(member)
		if !readable || !managed {
			continue
		}
		consulted = true
		container, found := modelDeploymentDrainContainerOf(member, modelDeploymentMainContainerName)
		if !found {
			continue
		}
		// ARGV 由 Command 和 Args 共同组成，因为引擎实际就是以这两者的拼接启动的。受管副本的
		// 并行配置作为额外参数声明并渲染到命令行上，因此若只读取 Args，会把所有副本都误判为
		// leader-served，进而暴露 External-DP 分组中的 follower。
		argv := slices.Concat(container.Command, container.Args)
		reading, err := scanModelDeploymentParallelism(engine, argv,
			modelDeploymentDeclaredEnv(container.Env))
		if err != nil {
			continue
		}
		shape, _ := modelDeploymentLoadBalance(engine,
			modelDeploymentRoleKindFromUpstream(member.Labels[modelDeploymentLabelKeyRoleKind]),
			modelDeploymentDeclaredEnv(container.Env), reading.declared)

		return shape != workercore.ModelDeploymentLoadBalanceInternal &&
			shape != workercore.ModelDeploymentLoadBalanceUnknown, true
	}

	return false, !consulted
}

// modelDeploymentDeclaredEnv 将容器环境变量还原为并行度扫描所需的声明形式。
//
// 仅读取字面量写明值的变量。由 Kubernetes 在运行时注入的值并非本 operator 可依赖的声明，
// 若仅凭变量名就视作声明来读取，任何携带该变量名的 Pod 都会改变外界对其所属副本的判定。
func modelDeploymentDeclaredEnv(env []core.EnvVar) []workercore.ModelDeploymentEnvVar {
	declared := make([]workercore.ModelDeploymentEnvVar, 0, len(env))
	for _, variable := range env {
		if variable.ValueFrom != nil {
			continue
		}
		declared = append(declared, workercore.ModelDeploymentEnvVar{
			Name: variable.Name, Value: variable.Value,
		})
	}

	return declared
}

// modelDeploymentAnsweringMemberLeader 判断一个 Pod 是否为 leader 服务型副本中负责
// 响应的成员。
//
// 这里直接读取标签而不经过 modelDeploymentPodMemberIndex，这正是本函数的意义所在。
// 该辅助函数会将缺失或无法解析的标签默认回退为 leader 索引，这对于尚未引入标签的单成员
// Pod 是正确的，但在此处会造成灾难性后果：一个缺少 member-index 标签的多成员副本会把
// 每个成员都判作 leader，从而重演该协议原本因读取所有成员而犯下的错误。
func modelDeploymentAnsweringMemberLeader(pod *core.Pod) bool {
	index, ok := pod.Labels[modelDeploymentMemberIndexLabel]

	return ok && index == strconvx.Itoa(modelDeploymentLeaderMemberIndex)
}

// modelDeploymentAnsweringMembers 返回代表该副本响应的成员集合，或无法确定集合的原因。
//
// 每次失败都会附带明确形状名称并进入挂起状态，以便运维人员查看挂起的预留资源时，能够
// 区分是无法识别 leader 的副本，还是无法读取形状的副本。
func modelDeploymentAnsweringMembers(
	shape modelDeploymentAnsweringShape, members []*core.Pod,
) ([]*core.Pod, string) {
	if shape == modelDeploymentAnsweringUnknown {
		return nil, "the replica's shape could not be established, so no member can answer for it"
	}

	// External-DP 副本和单成员副本的响应集合都是全体成员，且单成员副本自身即为 leader，因此
	// 这两种形状都会返回全部成员。这也使单成员副本的行为与直接遍历成员集合的排空逻辑完全一致。
	if shape == modelDeploymentAnsweringAll || shape == modelDeploymentAnsweringSole {
		return members, ""
	}

	leaders := make([]*core.Pod, 0, 1)
	for _, member := range members {
		if modelDeploymentAnsweringMemberLeader(member) {
			leaders = append(leaders, member)
		}
	}

	switch len(leaders) {
	case 1:
		return leaders, ""
	case 0:
		return nil, "a leader-served replica of several Pods has no member carrying the leader index, " +
			"so no member can answer for it"
	default:
		return nil, fmt.Sprintf(
			"a leader-served replica of several Pods has %d members carrying the leader index, "+
				"so no member can answer for it", len(leaders),
		)
	}
}

// IMPORTANT: 为指定 (role, ordinal, member) 生成全套 Kueue 组标签与注解（pod-group-name、ordinal、member-index、total-count、role-hash=role.Name、pod-group-serving=true 及可选拓扑层级）。
//
// ModelDeploymentPodGroup 返回某个 role 单个副本中单个成员的分组元数据：即位于指定
// ordinal 的副本中指定索引位置的成员。
//
// 该函数是纯函数：相同的部署、role、ordinal 和 member 总是产生相同的结果，不依赖客户端
// 也不依赖时钟。每个副本都加入属于自己的 Kueue Pod 分组——由其自身成员构成、对应一个
// Workload、作为一次整体准入——这使得扩缩容或替换仅作用于该副本自身，而不必等待所有
// 兄弟副本达成一致后 Kueue 才开始组装 Workload。
//
// 在这套元数据中，ordinal 是区分同一 role 下不同副本的唯一依据：组名由它派生，ordinal
// 标签也以纯数字直接记录它，从而使 Pod 的成员归属与身份标识分别面向 Kueue 和收敛器
// 表达同一件事。组名永远不会被反向解析——需要回读的只有标签，组名对读取方而言只需保证
// 唯一性即可。
//
// member index 不会进入组名，这正是分组设计的本意：同一副本的成员共享同一个组名，因为
// 它们构成一次联合准入，而区分各成员的标签 Kueue 并不会读取。若将索引混入组名，每个成员
// 都会变成各自独立的单 Pod 分组，这正是本设计要避免的形态。
//
// 绝不设置快速准入注解，且其缺失状态是通过断言验证而非假设。Kueue 的快速路径会取分组中
// 第一个可调度的 Pod，将该 PodSet 的 Count 设为整个分组的总数并直接返回。总数为 1 时
// 它毫无收益；总数大于 1 时则会造成严重错误，仅凭一个成员可调度就放行整个多成员分组，这
// 与总数所表达的同生共死语义完全背道而驰。相关测试明确命名了该注解并断言其不存在。
func ModelDeploymentPodGroup(
	md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole, ordinal, member int,
) ModelDeploymentPodGroupMeta {
	labels := map[string]string{
		kueuepodconst.GroupNameLabel:       modelDeploymentReplicaGroupName(md, role.Name, ordinal),
		modelDeploymentReplicaOrdinalLabel: strconvx.Itoa(ordinal),
	}
	// 无论副本规模大小，每个成员都会写入 member index 标签。负责对外响应 API 的是索引为 0 的 Pod
	// （每个副本一个成员，规模为 1 时即该副本唯一的 Pod），而标签选择器只能做合取匹配，因此只能通过
	// 所有成员共同携带的标签来选出该集合。若在单成员副本上省略索引，该集合就只能表达为“没有索引或
	// 索引为 0”，这是标签选择器无法表达的析取条件。把选择器限定到多成员角色也行不通：服务发现按
	// kind 而非 role 过滤，不同规模的两个 server 角色共用同一选择器，索引为 0 的条件会让其中的单成员
	// 角色全部落空。
	//
	// 写入该标签的代价是集群内所有部署触发一次滚动重建，这是经过权衡后主动接受的代价：写入标签会改变
	// 单成员副本的指纹，包含此改动的版本会将其逐一重建一次，固定的渲染摘要也会按自身基线流程同步更新。
	// 该标签记录的不再是只有一个可能取值的索引，而是使可响应集合能够被选择的唯一事实，这正是权衡改变的原因。
	labels[modelDeploymentMemberIndexLabel] = strconvx.Itoa(member)

	annotations := map[string]string{
		// 总数即单个副本的规模，调整副本数绝不会改变它：每个分组仅归属于当前副本，因此增减副本只会
		// 增减整组，而不会改动任何运行中成员携带的总数。这使扩缩容可以按单副本逐个增删，在规模大于 1
		// 时同样成立——按旧规模创建的成员在副本重建前保留旧总数，两个总数可以共存，不会改写运行中的副本。
		kueuepodconst.GroupTotalCountAnnotation: strconvx.Itoa(modelDeploymentRoleSize(role)),
		// role-hash 是承载核心语义的注解而非装饰。Kueue 存在该注解时按原样读取，否则会对 Pod spec 的
		// 形态（容器、nodeSelector、affinity、tolerations）计算不透明摘要，导致 PodSet 名称与运维
		// 人员声明的角色无关。按角色划分的 flavor 分配和状态汇总都依靠该名称将 Workload 的 PodSet
		// 关联到角色，使用摘要会在不报错的情况下破坏关联。直接写入角色自身名称就能在构造上保证 PodSet
		// 身份与角色一致，这也是准入阶段校验角色名符合 Kueue PodSetReference 规则且唯一的原因。
		//
		// 该值始终保持为角色名，绝不拼入 ordinal。Kueue 依据它对 PodSet 分组，状态逻辑也靠它归属 Pod，
		// 混入 ordinal 会让每个副本变成独立的 PodSet 身份，破坏该注解存在的关联作用。
		kueuepodconst.RoleHashAnnotation: role.Name,
		// 推理部署不会自然结束。缺少该注解时 Kueue 会套用批处理（BATCH）语义：Pod 进入 Succeeded 后
		// 会被视作可回收并提前归还配额，而此时部署本应持续提供服务。
		//
		// 退场路径需要为此付出代价。Kueue 将 serving 分组视为永不结束，因此不会主动释放挂在组内 Pod
		// 上的 finalizer，只有删除对应 Workload 才会释放。deleteModelDeploymentGroupWorkload 负责支付
		// 这笔代价，若保留该注解却移除该调用就会造成 Workload 泄漏。
		kueuepodconst.GroupServingAnnotationKey: kueuepodconst.GroupServingAnnotationValue,
	}
	if role.Topology != nil && role.Topology.RequiredLevel != "" {
		annotations[kueue.PodSetRequiredTopologyAnnotation] = role.Topology.RequiredLevel
	}

	return ModelDeploymentPodGroupMeta{
		Labels:      labels,
		Annotations: annotations,
	}
}

// modelDeploymentPodOrdinal 读取渲染副本时所赋予的 ordinal。
//
// 当标签缺失时，函数明确报告缺失而非猜测为 0：一个未携带 ordinal 标签的 Pod——无论是在
// 按副本分组机制出现之前渲染的，还是手工创建的——都不应由任何 ordinal 认领，收敛器会
// 将此类 Pod 视为过期对象，而非将其收编到一个它从未归属过的 ordinal 槽位。
func modelDeploymentPodOrdinal(pod *core.Pod) (int, bool) {
	ordinal, err := strconvx.Atoi[int](pod.Labels[modelDeploymentReplicaOrdinalLabel])
	if err != nil || ordinal < 0 {
		return 0, false
	}

	return ordinal, true
}

// modelDeploymentReplicaView 表示集群当前持有的单个副本视图：包含其所属的 role 与
// ordinal，以及落位其上的存活 Pod。
type modelDeploymentReplicaView struct {
	// Role 是成员所属的 role。
	Role string
	// Ordinal 是该副本所占用的槽位序号，仅当 Seated 为 true 时有意义。
	Ordinal int
	// Seated 对于未声明 ordinal 的 Pod 为 false。此类 Pod 不属于任何可命名的副本，因此
	// 各自独立构成一个视图——这与副本尚未引入成员概念前的形态一致，从而使基于这些视图统计
	// 的各项数值不会受其干扰而发生偏移。
	Seated bool
	// Members 是该副本上的存活 Pod，保持输入列表中的原有顺序。
	Members []*core.Pod
}

// modelDeploymentGroupPodsByReplica 将存活的 Pod 按所属副本分组，跳过已经在退出的 Pod。
//
// 设立该函数是为了让所有按副本统计的指标都共用同一套分组逻辑。就绪度、配额预留以及分组
// 完整性都是针对副本的统计；当一个副本仅含单个 Pod 时，它们都是直接对 Pod 计数。若在
// 各个位置分别重构分组逻辑，不同统计方对"什么是副本"的认知就会产生分歧——而且这种分歧
// 是静默的，因为无论按哪种方式统计，返回的都是一个看似合理的普通数字。
//
// 没有 ordinal 的 Pod 会作为独立视图，以输入位置而非名称或 UID 作为分组键。后两者看似
// 顺理成章但均不可行：分组键必须在不依赖特定字段已设置的前提下区分这些 Pod，而一旦某
// 字段为空就会发生键冲突的方案，会把同一 role 下所有未落位的 Pod 合并成单个副本。
func modelDeploymentGroupPodsByReplica(pods []core.Pod) []modelDeploymentReplicaView {
	return modelDeploymentGroupPods(pods, true)
}

// modelDeploymentGroupPodsIncludingDeparting 对所有 Pod 进行分组，包括正在退出的 Pod。
//
// 该函数用于统计正在离开的副本数据。正在被回收的副本，其所有成员都处于退出过程中，因此
// 会跳过退出中 Pod 的分组函数在面对这类状态时正好会返回 0——从而导致在资源池拆解某 role
// 时，抢占报告显示 "0 replicas"，或输出 "they are all terminating" 的消息却没有任何
// 可计数对象。
func modelDeploymentGroupPodsIncludingDeparting(pods []core.Pod) []modelDeploymentReplicaView {
	return modelDeploymentGroupPods(pods, false)
}

func modelDeploymentGroupPods(pods []core.Pod, skipDeparting bool) []modelDeploymentReplicaView {
	type key struct {
		role     string
		ordinal  int
		standing int // 已落位时为 0；否则为该 Pod 在列表中的位置加 1
	}

	at := make(map[key]int, len(pods))
	views := make([]modelDeploymentReplicaView, 0, len(pods))
	for i := range pods {
		pod := &pods[i]
		if skipDeparting && pod.DeletionTimestamp != nil {
			continue
		}

		k := key{role: modelDeploymentPodRole(pod)}
		view := modelDeploymentReplicaView{Role: k.role}
		if ordinal, ok := modelDeploymentPodOrdinal(pod); ok {
			k.ordinal, view.Ordinal, view.Seated = ordinal, ordinal, true
		} else {
			k.standing = i + 1
		}

		if idx, seen := at[k]; seen {
			views[idx].Members = append(views[idx].Members, pod)

			continue
		}
		view.Members = []*core.Pod{pod}
		at[k] = len(views)
		views = append(views, view)
	}

	return views
}

// modelDeploymentPodDescription 根据 Pod 自身的形态来命名：当副本仅含一个 Pod 时称为
// replica，否则称为 replica 的 member。
//
// 它读取 Pod 自身的分组总数而非 spec，因为消息的生成对象是一个已存在的 Pod，而 spec 可能
// 已经变更为其他形态——同时也是因为早于多成员副本机制创建的 Pod，无论 spec 现在如何
// 定义，它本身就是一个完整副本。member-index 标签已无法区分这两者：无论副本大小如何，
// 每个成员都会写入该标签，因此读取它会将所有单 Pod 副本都称作 member。分组总数才是能够
// 区分两种形态的值，而完全不带总数的 Pod 则按完整副本处理，这正符合该总数引入前旧 Pod
// 的实际情况。
func modelDeploymentPodDescription(pod *core.Pod) string {
	total, err := strconvx.Atoi[int](pod.Annotations[kueuepodconst.GroupTotalCountAnnotation])
	if err == nil && total > 1 {
		return "member " + pod.Name
	}

	return "replica " + pod.Name
}

// modelDeploymentPodMemberIndex 读取渲染 Pod 时所赋予的副本内成员索引。
//
// 当标签缺失时，函数默认返回 leader 索引而非报告缺失，这与上方 ordinal 的处理恰恰相反，
// 且这种不对称是有意为之的。缺少 ordinal 意味着该 Pod 不属于收敛器可命名的任何副本，
// 因此猜测一个 ordinal 会将其错误地收编到它从未占用过的槽位。而缺少 member index 的
// 含义则窄得多：如今每个成员都会写入该标签，因此它的缺失仅标志该 Pod 是在此规则生效前
// 渲染的，而当时未带该标签的副本全部都是单成员副本——其唯一 Pod 本身就是 leader。将其
// 视作 leader 正符合该标签本应表达的含义；反之，若将这类 Pod 视为不可归位对象，在规则
// 上线的那一轮调谐中就会触发所有存量部署发生滚动更新。
func modelDeploymentPodMemberIndex(pod *core.Pod) int {
	member, err := strconvx.Atoi[int](pod.Labels[modelDeploymentMemberIndexLabel])
	if err != nil || member < 0 {
		return modelDeploymentLeaderMemberIndex
	}

	return member
}

// deleteModelDeploymentGroupWorkload 删除 Kueue 为该部署分组组装的 Workload，从而
// 释放 Kueue 挂载在每个副本上的 finalizer。
//
// 这是清理流程的唯一出口，而非一种优化。该分组带有服务中注解，因此 Kueue 永远不会将其
// 判定为已完成，也不会自行清理其 Pod 的 finalizer；删除 Workload 是触发释放的唯一途径。
// 由于该 Workload 的属主正是被它锁住的这些 Pod，且属主引用未设置 controller 标记，垃圾
// 回收也无法触及它——仅删除副本会让部署一直卡在 Deleting 状态且任何地方都不报错。
//
// 未找到对象亦视为成功。未达到声明总数的分组根本不会生成 Workload，而在前一次调谐已删除
// Workload 之后再次执行清理时也不会查到任何对象；这两种情况都已达到期望状态。
func (r *ModelDeploymentReconciler) deleteModelDeploymentGroupWorkload(
	ctx context.Context, md *workercore.ModelDeployment, pods []core.Pod,
) error {
	wls, err := r.findModelDeploymentGroupWorkloads(ctx, md, pods)
	if err != nil {
		return err
	}

	// 必须遍历删除拥有这些 Pod 的每一个 Workload，而不仅仅是第一个。当部署中不同 role 位于
	// 不同的 instanceType 上时，每个分组都会对应一个 Workload，各自持有所属副本上 Kueue 的
	// finalizer；只删除其中一个只会释放一个分组，其余分组将完全无法退出。
	for _, wl := range wls {
		if err = r.Client.Delete(ctx, wl); err != nil && !kerrors.IsNotFound(err) {
			return fmt.Errorf("delete workload %s: %w", wl.Name, err)
		}
	}

	return nil
}

// releaseModelDeploymentStrandedWorkloads 在副本处于退出过程中且其所在组已无存活成员时，删除该副本的 Workload。
//
// 之所以存在此逻辑，是因为非本 operator 发起的退出没有机制去删除 Workload。
// 本 operator 驱动的每条路径（如拆解、角色消失、缩容、滚动替换）都会删除其移除副本对应的 Workload。
// 而由人工操作、节点驱空（node drain）或 Pod 驱逐移除的副本则没有此类路径，且服务组的 finalizer 只能通过删除其 Workload 来释放。
// 若无此处理，Pod 会卡在 finalizer 阻塞的 Terminating 状态，其 Workload 会继续保持已准入状态并占用配额，序号（ordinal）会保持被占用，创建门控（create gate）也永远不会为之放行：从而无法创建替代副本，必须人工介入才能恢复。
//
// 两种终止阶段（terminal phase）都无法自行恢复，且失败方式各不相同，这也是将修复逻辑放在此处而非创建门控的原因。
// 因超出宽限期（grace period）而被终止的容器会进入 Failed 状态，Kueue 将其计为 INACTIVE，因此在其旁创建的替代副本会使组内成员数超出上限，Kueue 会清理掉该已死亡成员——这种情况本可以留给创建门控处理。
// 而收到 SIGTERM 后正常退出的容器会进入 Succeeded 状态，Kueue 仍将其计为 ACTIVE，于是同一个替代副本反而成了多余成员，导致 Kueue 转而反复删除该替代副本。
// 删除 Workload 是唯一能同时解决这两种情况的办法。
//
// 存活成员的存在保证了这一操作的安全性。Kueue 对 Workload 被删除的响应是停止整个组，因此此处仅删除不再拥有任何存活 Pod 的 Workload；
// 只要组内仍有成员在提供服务，即使其同组副本正在排空退出，也会保留该 Workload。
// 本 operator 渲染的形态是每组一个副本，这使得两种判断在此处恰好一致，但条件仍按成员而非数量编写，以确保即便将来存在多成员组，也不会被此路径错误停止。
//
// 按 deleteModelDeploymentGroupWorkload 的语义约定，目标不存在即视为成功：若前一轮处理已经删除了该 Workload，后续执行未找到目标时，这正是期望的状态。
func (r *ModelDeploymentReconciler) releaseModelDeploymentStrandedWorkloads(
	ctx context.Context, md *workercore.ModelDeployment, pods []core.Pod,
) error {
	departing := make([]core.Pod, 0, len(pods))
	standing := sets.New[types.UID]()
	for i := range pods {
		if pods[i].DeletionTimestamp != nil {
			departing = append(departing, pods[i])

			continue
		}
		standing.Insert(pods[i].UID)
	}
	if len(departing) == 0 {
		return nil
	}

	wls, err := r.findModelDeploymentGroupWorkloads(ctx, md, departing)
	if err != nil {
		return err
	}

	for _, wl := range wls {
		if modelDeploymentWorkloadOwnsAny(wl, standing) {
			continue
		}
		if err = r.Client.Delete(ctx, wl); err != nil && !kerrors.IsNotFound(err) {
			return fmt.Errorf("delete stranded workload %s: %w", wl.Name, err)
		}
	}

	return nil
}

// modelDeploymentReplicaGroupName 推导单个副本（即指定角色在指定序号下的副本）的 Kueue 组名。
//
// 该名称始终采用哈希形式，这是有意为之的设计选择而非权宜之计。Kueue 会原样使用组名作为其 Workload 名称，
// 而 Workload 是 Kubernetes 对象名（小写 RFC 1123 子域名），因此组名受限于该字符集，可读组合名称带来的好处在此无从体现：
// 字符集不允许的分隔符无法用于可读形式，而允许的分隔符（连字符）又无法可靠拆分，况且本来也不需要反向解析。
// 哈希输入包含 Namespace、Name、Role 和 Ordinal：虽然组名仅在同一 Namespace 内与其他 Pod 比较，
// 但不同 Namespace 中同名的两个部署绝不能被识别为同一个组。
//
// 该名称仅保证唯一，而非可解析，这正是关键所在：没有任何逻辑会从中反推序号。
// 序号通过独立的 label 传递，该名称只需向读取方保证唯一性即可——这也是为何其推导方式不随部署声明的角色数量变化：
// 第一个角色的副本无论作为唯一角色还是多个角色之一，其组名生成方式完全一致。
func modelDeploymentReplicaGroupName(
	md *workercore.ModelDeployment, role string, ordinal int,
) string {
	return modelDeploymentPodGroupNamePrefix + stringx.SumByFNV64a(
		md.Namespace, "/", md.Name, "/", role, "/", strconvx.Itoa(ordinal),
	)
}

// modelDeploymentReplicaServiceName 是发布单个副本各成员的 headless Service 名称，也是所有这些成员共同使用的子域名（subdomain）。
//
// 与上方的 Kueue 组名不同，此名称采用可读形式而非哈希形式，原因在于它的使用对象不同。
// 组名只用于内部比对，从不对外展示；而此名称是调试集合通信（collective）时排查人员需要输入的地址组成部分，
// 也是引擎日志在某个 rank 无法连接 leader 时输出的内容。
// 可读性的代价是长度预算限制，因为 Service 名称是最多 63 个字符的 DNS-1035 标签。
// 此处不负责强制校验：该预算会在准入（admission）阶段按角色声明数量所能产生的最长名称进行检查，
// 从而在准入时直接拒绝成员无法命名的部署，而不是在渲染成对象后被 API server 逐个拒绝。
//
// 仅当副本规模大于 1 时才存在该 Service。单成员副本没有任何需要寻址的对端，为其渲染 Service 只会为每个副本白白创建无人使用的对象。
func modelDeploymentReplicaServiceName(
	md *workercore.ModelDeployment, role string, ordinal int,
) string {
	return md.Name + "-" + role + "-r" + strconvx.Itoa(ordinal)
}

// modelDeploymentMemberName 是单个副本中某一个成员的名称，同时也是它的主机名（hostname），
// 因此构成了同组其他成员解析其地址所需 DNS 记录的前半部分。
//
// 成员索引（member index）放在末尾以便识别 leader，且从零开始计数，从而在构造上保证零号成员始终是每个副本的 leader。
// 无需选举也无需持久化存储该决策：只要知道部署名、角色和序号，任何读取方都能无需查询集群直接写出 leader 地址，
// 这使得容器在组内任何成员启动之前就能获得该地址。
//
// 该名称永远不会被反向解析。序号和成员索引各自通过独立的 label 传递，此字符串只需提供唯一性和可识别性，而不承担结构解析职责。
func modelDeploymentMemberName(
	md *workercore.ModelDeployment, role string, ordinal, member int,
) string {
	return modelDeploymentReplicaServiceName(md, role, ordinal) + "-m" + strconvx.Itoa(member)
}

// modelDeploymentPodGroupNameOf 是状态路径仍需使用的、以组名为键（group-name-keyed）的形态下的角色名称：
// 一个按角色划分的关联键（join key），其推导方式与副本组名相同但不含序号，且不会被任何 Pod 携带。
//
// 与 modelDeploymentReplicaGroupName 同理，它始终采用哈希形式，而且还有另一个前者不涉及的原因：
// 若推导形态随角色数量变化，当第二个角色加入时就会改变第一个角色的键名；对于使用方而言，一个会漂移的键远不如一开始就不透明的键。
func modelDeploymentPodGroupNameOf(md *workercore.ModelDeployment, role string) string {
	return modelDeploymentPodGroupNamePrefix +
		stringx.SumByFNV64a(md.Namespace, "/", md.Name, "/", role)
}
