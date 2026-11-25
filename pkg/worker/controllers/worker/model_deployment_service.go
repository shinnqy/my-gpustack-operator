package worker

import (
	"maps"
	"slices"
	"strings"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	kueuepodconst "sigs.k8s.io/kueue/pkg/controller/jobs/pod/constants"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/systemmeta"
	"gpustack.ai/gpustack/pkg/utils/strconvx"
	"gpustack.ai/gpustack/pkg/worker/kvcache/inject"
)

// renderModelDeploymentServices 渲染一个 deployment 拥有的所有 Service：对外访问的
// 全局 Service，以及每个角色对应的 Service。
//
// 全局 Service 排在第一位，该顺序是契约的一部分：调用方按名称进行垃圾回收，status
// 也据此读取其端点。
//
// manufacturers 按角色名保存各角色的加速器厂商，缺失条目视作“未确认为 Ascend”：
// 仅当 Service 后端副本会发布 KV 事件时才会渲染 KV 事件端口，而该决定由连接器做出。
func renderModelDeploymentServices(
	md *workercore.ModelDeployment, manufacturers map[string]string, pods []core.Pod,
) []*core.Service {
	svcs := make([]*core.Service, 0, len(md.Spec.Roles)+1)
	svcs = append(svcs, renderModelDeploymentService(md, pods))
	for i := range md.Spec.Roles {
		role := &md.Spec.Roles[i]
		standing := modelDeploymentReplicaServiceOrdinals(pods, md, role)
		svcs = append(svcs,
			renderModelDeploymentRoleService(md, role, manufacturers,
				modelDeploymentStandingMembers(pods, role, standing),
				modelDeploymentNeedsDeployedRouting(pods, md, role)))

		// 每个副本对应一个 headless Service，且仅当副本存在需要对端寻址的成员时才创建。它负责
		// 在 DNS 中发布各成员的名称，因此单成员副本无需此类 Service——强行渲染只会在集群中
		// 为无消费者场景增加多余的对象。
		//
		// 它们在此列表中排在角色自身 Service 之后，这一点很关键，因为调用方会通过将此列表
		// 与现有对象对比来执行垃圾回收。只要副本仍处于期望状态，或其已部署成员仍需要
		// 对端记录，就会保留其 Service。
		//
		// 当期望规模表明某个序号的副本将拥有对端，或该序号下仍有成员正在运行时，就会为其分配
		// 一个 Service。将角色从四成员编辑为单成员时，原四成员副本在替换副本构建期间仍会
		// 继续提供服务；若此时删除其 Service，就会抽走运行中集合通信的对端记录：成员之间
		// 无法互相解析，而无法解析对端的集合通信不会显式失败，而是会直接挂起。因此保留
		// 策略基于已观测到的成员存在状态，并在最后一个此类成员离开后的一轮协调中结束。
		ordinals := standing
		for ordinal := range int(role.Replicas) {
			if modelDeploymentRoleSize(role) == 1 {
				break
			}
			ordinals[ordinal] = true
		}
		for _, ordinal := range slices.Sorted(maps.Keys(ordinals)) {
			svcs = append(svcs, renderModelDeploymentReplicaService(md, role, ordinal))
		}
	}

	return svcs
}

// modelDeploymentReplicaServiceOrdinals 返回其运行中成员仍需要保留对端记录的副本序号，
// 无论当前期望规模是否还会渲染这些记录。
//
// 正在终止中的成员同样计入。该 Service 发布未就绪地址正是为了使成员在就绪前就能找到
// 对端，而在即将退出的成员仍在运行时删除记录，会在下一刻造成同样的记录丢失。
//
// 判断依据是成员携带的组总数（group total），而非成员索引：成员索引在任何规模下都会
// 写入，因此存在索引只能说明 Pod 已被渲染，并不能说明它有对端成员。
func modelDeploymentReplicaServiceOrdinals(
	pods []core.Pod, md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole,
) map[int]bool {
	ordinals := make(map[int]bool)
	for i := range pods {
		pod := &pods[i]
		if !modelDeploymentOwns(pod, md) || modelDeploymentPodRole(pod) != role.Name {
			continue
		}
		total, err := strconvx.Atoi[int](pod.Annotations[kueuepodconst.GroupTotalCountAnnotation])
		if err != nil || total <= 1 {
			// 只有一个成员的副本没有需要对端寻址的目标。
			continue
		}
		if ordinal, ok := modelDeploymentPodOrdinal(pod); ok {
			ordinals[ordinal] = true
		}
	}

	return ordinals
}

// renderModelDeploymentReplicaService 渲染用于发布单个副本所有成员的 headless Service，
// 该 Service 也是每个成员所指定的 subdomain。
//
// 它采用 headless 模式，因为核心需求是每 Pod 记录而非虚拟地址。ClusterIP 会在成员间
// 做负载均衡，而这正是分布式 rank 绝对不能发生的行为：集合通信的成员必须按名称逐个
// 寻址对端，解析为“任意成员”的地址对所有成员而言都无法使用。
//
// 它基于副本序号进行选择，确保某副本的 Service 只发布该副本的成员而不包含其他成员。
// 仅使用角色的身份标签会导致每个副本的 Service 都指向该角色的所有成员，从而使各成员
// 将对端解析到错误副本的 Pod 上——跨实例边界建立的集合通信不会直接失败，而是会给出
// 错误的推理结果。
//
// 它会发布未就绪的成员。若未设置 publishNotReadyAddresses，成员记录在 kubelet 判定其
// 就绪前不会存在，而 rank 在加入集合通信之前无法就绪，加入集合通信又依赖该记录来发现
// 对端：整个组会在自身的就绪状态上发生死锁。这与门控机制遇到的循环依赖相同，也采用
// 相同的方式解决——不等待就绪。
func renderModelDeploymentReplicaService(
	md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole, ordinal int,
) *core.Service {
	svc := renderModelDeploymentServiceFor(
		md, role, modelDeploymentReplicaServiceName(md, role.Name, ordinal),
	)

	svc.Spec.ClusterIP = core.ClusterIPNone
	svc.Spec.PublishNotReadyAddresses = true
	svc.Spec.Selector[modelDeploymentReplicaOrdinalLabel] = strconvx.Itoa(ordinal)

	return svc
}

// renderModelDeploymentService 渲染用于访问 deployment 的唯一 Service。
//
// 这刻意不同于 instance.go 的做法。convertServiceFromInstance 为每个 Pod 渲染一个
// NodePort Service，这对于 Instance（用户直连的单台可寻址开发机）是合适的。但在单个
// 地址背后挂接 N 个可互换副本则是另一种形态：按 Pod 创建 NodePort 会发布 N 个地址且
// 它们之间没有负载均衡，同时每个副本都会消耗一个节点端口。
//
// 它仍然只指向第一个角色，并且在引入 P/D 角色后依然保持如此，而不是另作选择。单角色
// 设计将此决定留给了本设计；而本设计的 Non-Goals 通过不在此处做路由来回应：这里没有
// 任何机制在 prefiller 和 decoder 之间引导请求，因此选出哪个角色作为入口都没有意义。
// 选择所有角色的 Service 比随机选择更糟——它会将请求轮询分发给分别配置为生产者和
// 消费者的进程，产生静默错误，而这正是本设计引入 kind 字段所要避免的问题。按角色
// 可寻址是本任务新增的能力；真正的统一入口需要路由器，属于后续设计。
// modelDeploymentEffectiveRoleForEligibility 返回集群中当前存续副本所看到的
// 角色视图。
//
// 门控关注的是正在提供服务的副本，而非 spec 当前声明的角色。一个正在提供服务、但被
// 转为自管（take-over）模式的角色，在旧有受管副本仍在运行时并未失去对其组转发能力的
// 观测：这些副本的命令行仍由本 operator 生成。只要仍有此类副本存续，就清除期望命令行，
// 从而避免在角色切换的那一轮协调中撤回门控以及由门控收窄的选择器。
func modelDeploymentEffectiveRoleForEligibility(
	role *workercore.ModelDeploymentRole, members []*core.Pod,
) *workercore.ModelDeploymentRole {
	if !modelDeploymentAnyMemberRunsCommand(members) {
		return role
	}
	effective := *role
	effective.Command = nil

	return &effective
}

// modelDeploymentStandingMembers 返回某个角色中仍占有一席的副本的存活成员，门控和
// 成员规则均基于这些成员来判定。
func modelDeploymentStandingMembers(
	pods []core.Pod, role *workercore.ModelDeploymentRole, standing map[int]bool,
) []*core.Pod {
	members := make([]*core.Pod, 0, len(standing))
	for _, view := range modelDeploymentGroupPods(pods, true) {
		if modelDeploymentPodRole(view.Members[0]) != role.Name || !standing[view.Ordinal] {
			continue
		}
		members = append(members, view.Members...)
	}

	return members
}

func renderModelDeploymentService(
	md *workercore.ModelDeployment, pods []core.Pod,
) *core.Service {
	front := modelDeploymentFrontRole(md)
	svc := renderModelDeploymentServiceFor(md, front, md.Name)
	modelDeploymentFrontLeadersOnly(svc, front,
		modelDeploymentRoleHasMultiMemberReplica(pods, md, front))
	modelDeploymentSelectAnsweringMembers(svc, modelDeploymentNeedsDeployedRouting(pods, md, front))
	// 仅检查入口角色自身的存续成员：Service 只路由到该角色，因此兄弟角色的受管 Pod
	// 或已在退出的 Pod 不能为其解除自管模式下的拒绝策略。
	var frontStanding []*core.Pod
	for _, view := range modelDeploymentGroupPods(pods, true) {
		if modelDeploymentPodRole(view.Members[0]) == front.Name {
			frontStanding = append(frontStanding, view.Members...)
		}
	}
	if modelDeploymentEligibilitySelectorActive(md,
		modelDeploymentEffectiveRoleForEligibility(front, frontStanding), nil,
		modelDeploymentEligibilityDecided(md)) {
		modelDeploymentSelectEligibleEndpoints(svc, md, front)
	}

	return svc
}

// modelDeploymentFrontRole 是 deployment 自身地址所属的角色：即全局 Service 所路由、
// 以及对外发布端点所描述的那个副本组。
//
// ENCODE 角色永远不会作为入口角色。编码器将媒体转换为供 prefiller 使用的 embedding，
// 自身不响应任何外部请求，因此将入口指向编码器会把流量发送到完全不处理请求的进程——
// 而且其端点仍可解析并响应健康检查，这正是此类故障呈现静默失败的原因。因此无论编码器
// 在列表中的位置是首个、中间还是末尾，都会被跳过。
//
// 没有任何其他角色的 deployment 同样无法响应任何请求，准入控制会拒绝创建此类资源。
// 在这种情况下返回第一个角色，是为了让未能被准入拒绝拦截到的 spec 仍能渲染出一个地址，
// 而不是完全为空。
func modelDeploymentFrontRole(md *workercore.ModelDeployment) *workercore.ModelDeploymentRole {
	for i := range md.Spec.Roles {
		if ModelDeploymentEffectiveRoleKind(&md.Spec.Roles[i]) != workercore.ModelDeploymentRoleKindEncode {
			return &md.Spec.Roles[i]
		}
	}

	return &md.Spec.Roles[0]
}

// modelDeploymentFrontLeadersOnly 将提供 OpenAI API 的 Service 收窄到每个副本中唯一
// 负责响应 API 的那个成员。
//
// 每个成员都带有角色的身份标签——这是必须的，因为 status、收敛器以及其他所有选择器
// 都靠这些标签将 Pod 归属到对应角色——因此按单 Pod 副本编写的选择器会选中全部 size
// 个成员。当 size 大于 1 时，非 leader 成员完全不提供 API 服务，普通轮询会导致
// (size-1)/size 的请求失败，且成功与失败交错出现，而不是让 Service 显式不可用。
//
// 当 size 为 1 时，选择器保持完全原样，而不仅仅是等价形式：单成员副本的 Pod 不携带
// 成员索引标签，若此时加上该筛选条件将匹配不到任何 Pod，导致所有运行中 deployment
// 的 Service 端点全部清空。
//
// 当角色处于编辑过渡期时，筛选条件基于当前存续状态而非期望规模。一个从四成员编辑为
// 单成员的副本仍有四个成员在运行，仅按新规模计算的选择器会去掉 leader 条件并面向全部
// 四个成员——从而让原本正常工作的 deployment 重新出现上述故障。因此，只要期望规模
// 大于 1 或角色的任意实际成员携带成员索引，就应用该条件，直到最后一个此类成员退出后
// 才移除。
func modelDeploymentFrontLeadersOnly(
	svc *core.Service, role *workercore.ModelDeploymentRole, multiMemberStanding bool,
) {
	if modelDeploymentRoleSize(role) == 1 && !multiMemberStanding {
		return
	}

	svc.Spec.Selector[modelDeploymentMemberIndexLabel] = strconvx.Itoa(modelDeploymentLeaderMemberIndex)
}

// modelDeploymentNeedsDeployedRouting 报告期望路由规则是否与运行中的成员不同。
// 从首次编辑起即使用观测到的成员状态，包括替换成员尚未出现之前。
func modelDeploymentNeedsDeployedRouting(
	pods []core.Pod, md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole,
) bool {
	desiredManaged := len(role.Command) == 0
	desiredExternal := desiredManaged && modelDeploymentRoleExternalDP(md, role)
	for i := range pods {
		pod := &pods[i]
		if !modelDeploymentOwns(pod, md) || modelDeploymentPodRole(pod) != role.Name {
			continue
		}
		if pod.DeletionTimestamp != nil {
			continue
		}
		managed, readable := modelDeploymentMemberExecution(pod)
		if !readable {
			continue
		}
		if managed != desiredManaged {
			return true
		}
		if !managed {
			continue
		}
		// 无法读取并行度信息的成员不能反映其构建时所采用的路由方式。
		if external, known := modelDeploymentMembersExternalDP(
			[]*core.Pod{pod}, md.Spec.Engine.Name,
		); known && external != desiredExternal {
			return true
		}
	}

	return false
}

// modelDeploymentSelectAnsweringMembers 将 HTTP Service 收窄到其所属副本按构建时
// 模式能够响应请求的成员。
//
// 当运行中的模式或响应形态与期望配置不同时启用该条件；一旦所有成员都遵循期望的路由
// 规则，即移除该条件。
func modelDeploymentSelectAnsweringMembers(
	svc *core.Service, mixed bool,
) {
	if !mixed {
		return
	}

	svc.Spec.Selector[modelDeploymentLabelKeyAPIAnswering] = modelDeploymentAPIAnsweringValue
	delete(svc.Spec.Selector, modelDeploymentLabelKeyEndpointEligible)
	delete(svc.Spec.Selector, modelDeploymentMemberIndexLabel)
}

// modelDeploymentRoleHasMultiMemberReplica 报告角色的任意实际成员是否仍属于按多成员
// 构建的副本（这正是它作为“成员”而非完整副本的依据）。
func modelDeploymentRoleHasMultiMemberReplica(
	pods []core.Pod, md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole,
) bool {
	for i := range pods {
		pod := &pods[i]
		if !modelDeploymentOwns(pod, md) || modelDeploymentPodRole(pod) != role.Name {
			continue
		}
		if total, err := strconvx.Atoi[int](pod.Annotations[kueuepodconst.GroupTotalCountAnnotation]); err == nil &&
			total > 1 {
			return true
		}
	}

	return false
}

// modelDeploymentGroupForwardActivatable 报告该角色的组转发能力是否可能达到 Verified
// 或 NotApplicable——即激活准入资格（eligibility）的两种状态。对于 operator 尚无
// 观测依据的形态（矩阵中未包含的引擎），按构造逻辑恒返回 Unsupported 且永远无法激活，
// 因此在其 Service 上附加资格选择器会导致端点永久为空：这些 Service 渲染时不带该
// 条件，从而保留 deployment 首次启用前的原有路由。
//
// 当 operator 对某个 P/D 角色所在的配对存在观测依据时，该角色即可激活。检查针对
// operator 仍知晓的路由发起，因为门控关注的是正在服务的副本：当受管成员仍存续时
// 改由用户接管命令行的角色并未失去 operator 所知晓的路由，而配对另一侧被用户接管
// 也不会移除当前角色的屏障——资格判定对该配对返回 Unsupported，从而保持组原有的
// 资格而非将其撤回。
func modelDeploymentGroupForwardActivatable(
	md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole,
) bool {
	if _, known := modelDeploymentGroupForwardEngines[md.Spec.Engine.Name]; !known {
		return false
	}
	if len(role.Command) > 0 {
		return false
	}
	kind := ModelDeploymentEffectiveRoleKind(role)
	if kind != workercore.ModelDeploymentRoleKindPrefill &&
		kind != workercore.ModelDeploymentRoleKindDecode {
		return true
	}
	unsupported, _ := modelDeploymentGroupForwardPairSupport(modelDeploymentPairableDeployment(md))

	return !unsupported
}

// modelDeploymentRoleJudged 报告 operator 是否在稳定语义下控制该角色的准入资格：
// 在读取任何观测结果之前该要求即已生效，因此引用该条件的选择器不会在门控做出决定时
// 发生跳变。
//
// operator 无法询问的角色不纳入评判。它的路由是 operator 所不知晓的，因此不存在
// 需要验证资格的组，也无需附加要求。
//
// 受保护的类别只要 operator 能够询问就会纳入评判，且无论是否已有结论，其要求从首次
// 启用起即生效。受保护类别与纳入评判的类别是两个不同的问题：operator 无法询问的
// 受保护类别仍保留该要求，原生选择器依然要求具备资格，只是当前没有评判依据来决定它。
//
// 无需观测即可由 operator 给出答案的组同样纳入评判：单成员副本没有跨成员路径，而
// 编码器不输出 token。
//
// 受管配对在其探测路径可用时纳入评判，此时该配对自身的观测结果决定的是组是否合格，
// 而非要求是否生效。探测路径不可用会使受保护的配对处于未评判状态。
//
// 下方 switch 中的配对分支不会被执行到。未受保护的配对即命令行被用户接管的配对，
// 而此类角色不可激活，因此第一个分支已经处理完毕。
func modelDeploymentRoleJudged(
	md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole,
) bool {
	if !modelDeploymentGroupForwardActivatable(md, role) {
		return false
	}
	if modelDeploymentProtectedRole(md, role) {
		return true
	}
	switch ModelDeploymentEffectiveRoleKind(role) {
	case workercore.ModelDeploymentRoleKindPrefill, workercore.ModelDeploymentRoleKindDecode:
		return false
	case workercore.ModelDeploymentRoleKindEncode:
		return true
	}

	return modelDeploymentRoleSize(role) <= 1
}

// modelDeploymentPairableDeployment 返回用于门控配对支持检查的 deployment 视图：
// 其 Prefill 和 Decode 角色视为携带 operator 自身的命令行。
//
// 该副本仅用于这一项决策，绝不会传递给资格判定、探测、渲染或调用方的 spec。
// 检查逻辑读取这个清除了 P/D 命令行的副本，从而针对 operator 配置的配对而非用户
// 接管状态进行询问。门控关注的是正在服务的副本：当受管成员仍存续时被接管命令行的
// 角色并未失去 operator 所知晓的路由。
func modelDeploymentPairableDeployment(md *workercore.ModelDeployment) *workercore.ModelDeployment {
	pairable := md.DeepCopy()
	for i := range pairable.Spec.Roles {
		switch ModelDeploymentEffectiveRoleKind(&pairable.Spec.Roles[i]) {
		case workercore.ModelDeploymentRoleKindPrefill, workercore.ModelDeploymentRoleKindDecode:
			pairable.Spec.Roles[i].Command = nil
		}
	}

	return pairable
}

// modelDeploymentEligibilityDecided 报告某一轮协调是否已对整组谓词给出明确结论：
// EndpointEligibility Condition 为 True（合格——收窄选择器并由标签放行）或 False
// （已撤销——收窄选择器并由标签撤回）。已确定的 Condition 是 operator 自身的行为，
// 因此据此执行动作（包括作用于早于门控机制的遗留路由）不属于需要回溯的回退问题。
// Unknown 或缺失的 Condition 表示尚未做出任何决定，基于它的全新渲染绝不会收窄选择器。
func modelDeploymentEligibilityDecided(md *workercore.ModelDeployment) bool {
	for i := range md.Status.Conditions {
		condition := &md.Status.Conditions[i]
		if condition.Type == string(ModelDeploymentConditionEndpointEligibility) {
			return condition.Status == meta.ConditionTrue || condition.Status == meta.ConditionFalse
		}
	}

	return false
}

// modelDeploymentGroupProtection 定义单个组的选择器治理方式。
type modelDeploymentGroupProtection int

const (
	// modelDeploymentGroupUnprotected 表示 operator 为其保留现有行为的组。
	modelDeploymentGroupUnprotected modelDeploymentGroupProtection = iota
	// modelDeploymentGroupProtected 表示一个受管组，其选择范围受组自身资格的收窄限制，
	// 且在恢复时需等待对该组自身的新一轮观测。
	modelDeploymentGroupProtected
)

// modelDeploymentGroupProtectionOf 返回单个组的选择策略类型。
//
// 类别由形态辅助函数决定，而不是通过匹配 reason 字符串：
//
//   - 受管的受保护组（多成员普通副本、External DP 副本，或 Prefill/Decode 副本）按组自身的资格状态收窄端点；
//   - 不支持的自定义路由或能力矩阵之外的引擎保留现有的兼容性例外处理，因为本 operator 完全无法判断该路由；
//   - 普通单成员服务保留现有行为（包括其观测兼容性），因为不存在需要保护的组。
//
// 角色按有效角色（即现有存活成员实际运行的角色）读取：接管了自身命令行的角色不再是 spec 中仍命名的受管角色，除非仍有存活成员在运行 operator 的命令。
func modelDeploymentGroupProtectionOf(
	md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole, view modelDeploymentReplicaView,
) modelDeploymentGroupProtection {
	effective := *role
	if modelDeploymentReplicaRunsCommand(view.Members) {
		effective.Command = nil
	}
	if len(effective.Command) > 0 {
		return modelDeploymentGroupUnprotected
	}
	if _, known := modelDeploymentGroupForwardEngines[md.Spec.Engine.Name]; !known {
		return modelDeploymentGroupUnprotected
	}
	kind := ModelDeploymentEffectiveRoleKind(&effective)
	if kind == workercore.ModelDeploymentRoleKindPrefill || kind == workercore.ModelDeploymentRoleKindDecode {
		return modelDeploymentGroupProtected
	}
	if modelDeploymentRoleExternalDP(md, &effective) {
		return modelDeploymentGroupProtected
	}
	if len(view.Members) > 1 {
		return modelDeploymentGroupProtected
	}

	return modelDeploymentGroupUnprotected
}

// modelDeploymentProtectedRole 报告角色的各个组是否按自身的资格状态收窄端点。
//
// 这是角色侧的判断结果，依据角色自身形态读取：Prefill 或 Decode 角色、External DP 角色，或成员共享多个席位的角色。组侧的判断还会读取现有存活成员的状态；除命令行被接管的角色外，两者对所有角色都一致。
func modelDeploymentProtectedRole(
	md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole,
) bool {
	// 接管了自身命令行的角色不再是 spec 中仍命名的受管角色，而兼容性例外正是该类别的意义所在：收窄一个无人能判断的路由会导致一个端点也选不出来。
	if len(role.Command) > 0 {
		return false
	}
	kind := ModelDeploymentEffectiveRoleKind(role)
	if kind == workercore.ModelDeploymentRoleKindPrefill || kind == workercore.ModelDeploymentRoleKindDecode {
		return true
	}
	if modelDeploymentRoleExternalDP(md, role) {
		return true
	}

	return role.ReplicaSize > 1
}

// modelDeploymentEligibilitySelectorActive 是判断某个暴露面是否指定了 endpoint-eligible 端点的唯一决策点。
//
// 兼容性例外优先处理，因为它是唯一永不收窄端点的类别：能力矩阵之外的引擎或接管了自身命令行的角色，不存在本 operator 可进行的观测；若暴露面要求筛选它永远无法授予资格的端点，将导致选不出任何端点。
//
// 受管的受保护类别从首次启用起即生效，包括尚未作出决定的轮次。不带该要求的暴露面完全不会读取资格标签，否则从未获得过资格的组将仅凭就绪状态继续提供服务。
//
// 其余类别保留现有的三部分条件：静态部分（形态本身必须能够激活）、激活部分（必须有某个轮次记录了明确的谓词结果——True（已授予资格）或 False（已撤销）——因为在任何轮次给出结果前就收窄会选出零个端点，使 deployment 现有的路由中断），以及保持部分（一旦暴露面带有该选择器条件，只要形态仍可激活，线上选择器就会保留它）。
func modelDeploymentEligibilitySelectorActive(
	md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole,
	liveSelector map[string]string, passDecided bool,
) bool {
	if _, known := modelDeploymentGroupForwardEngines[md.Spec.Engine.Name]; !known {
		return false
	}
	if len(role.Command) > 0 {
		return false
	}
	if modelDeploymentProtectedRole(md, role) {
		return true
	}
	if !modelDeploymentGroupForwardActivatable(md, role) {
		return false
	}
	if passDecided || modelDeploymentEligibilityDecided(md) {
		return true
	}

	return liveSelector != nil &&
		liveSelector[modelDeploymentLabelKeyEndpointEligible] == modelDeploymentEndpointEligibleValue
}

// modelDeploymentEligibilityActiveAnnotation 在混合 Service 使用成员关系期间保留门控的激活状态。
const modelDeploymentEligibilityActiveAnnotation = "modeldeployment.gpustack.ai/eligibility-active"

// modelDeploymentSelectEligibleEndpoints 选择具备资格的 HTTP 端点。
// External 角色对所有 rank 提供服务；Internal 角色保留其 leader 限制。
// Headless 对等 Service 从不使用端点资格筛选。
func modelDeploymentSelectEligibleEndpoints(
	svc *core.Service, md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole,
) {
	// 在第 21 章实现此函数。
}

// renderModelDeploymentRoleService 渲染前置代理单个角色的 Service。
//
// P/D 部署需要它，因为 decoder 必须能作为 decoder 被访问：两个角色是有意区分的不同配置，因此解析到“任意副本”的地址对两者而言都不是有效地址。当前规范中尚无调用方——目前还没有 router——而这正是现在就渲染它的原因：将来调用它的配对规范应该能直接看到已就绪的地址，而不必为了获取它们再去修改该对象的结构。
//
// 名称格式为 <deployment>-<role>；Service 名称是上限 63 个字符的 DNS-1035 标签，而普通对象名称最长可达 253 个字符。
//
// 这是一类新的失败场景，而非继承自旧有逻辑——此注释的早期版本曾有相反说法，那是错误的。deployment 级 Service 只会在 deployment 自身名称过长时失败；而这里的 Service 会因为 40 字符的 deployment 搭配 30 字符的 role 而失败，尽管两者单独来看都合法。决定这一点的是可达输入集合的变化，而不是失败形式是否常见。
//
// 因此 webhook 会在准入阶段拒绝这种组合（validateModelDeploymentRoleServiceNames），以便在错误信息中同时指出两部分。否则 deployment 的 Pod 侧会正常工作，但按角色的 Service 在每次创建时都会被拒绝，reconciler 将无限重试，而根因却远离出错对象、藏在两个对象之外的字段里。
func renderModelDeploymentRoleService(
	md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole,
	manufacturers map[string]string, standingMembers []*core.Pod, mixedCommandMode bool,
) *core.Service {
	svc := renderModelDeploymentServiceFor(md, role, md.Name+"-"+role.Name)
	modelDeploymentFrontLeadersOnly(svc, role, len(standingMembers) > 0)
	modelDeploymentSelectAnsweringMembers(svc, mixedCommandMode)
	if modelDeploymentEligibilitySelectorActive(md,
		modelDeploymentEffectiveRoleForEligibility(role, standingMembers), nil,
		modelDeploymentEligibilityDecided(md)) {
		modelDeploymentSelectEligibleEndpoints(svc, md, role)
	}
	if modelDeploymentPublishesKVEvents(md, role, manufacturers[role.Name]) {
		for _, port := range inject.KVEventsPorts() {
			svc.Spec.Ports = append(svc.Spec.Ports, core.ServicePort{
				Name: port.Name, Protocol: port.Protocol, Port: port.ContainerPort,
				TargetPort: intstr.FromInt32(port.ContainerPort),
			})
		}
	}

	return svc
}

// renderModelDeploymentServiceFor 是两个 Service 共用的结构：一个前置代理单个角色副本的 ClusterIP Service，使用该角色的端口，归属于 deployment。
//
// port 和 targetPort 是两个不同的数值。port 是调用方连接的端口，保持在声明端口或默认端口上；targetPort 是副本实际监听的端口，当角色未声明端口时，它会随角色自身的 --port 参数移动。
func renderModelDeploymentServiceFor(
	md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole, name string,
) *core.Service {
	port := modelDeploymentServicePort(role)
	target := ModelDeploymentRoleServingPort(md, role)

	svc := &core.Service{
		ObjectMeta: meta.ObjectMeta{
			Name:      name,
			Namespace: md.Namespace,
			Labels:    modelDeploymentSelectorLabels(md, role),
		},
		Spec: core.ServiceSpec{
			Type: core.ServiceTypeClusterIP,
			// selector 仅携带身份标识。与 Deployment 不同，Service 的 selector 是可变的，但修改它会使此前代理的所有副本成为孤儿——因此任何可能因 spec 更新而改变的标签都不会放入其中，入口标签尤其如此。
			//
			// 它同时指定了 deployment 和角色。如果仅按角色匹配，同一个 namespace 下各自运行 "decode" 角色的两个 deployment 将会共享端点。
			Selector: modelDeploymentSelectorLabels(md, role),
			Ports: []core.ServicePort{{
				Name:       port.Name,
				Protocol:   port.Protocol,
				Port:       port.ContainerPort,
				TargetPort: intstr.FromInt32(target),
			}},
		},
	}

	// 此处保留的 selector 仅包含角色身份，它会选中每个副本的所有成员。随后由各个调用方按用途收窄范围：两个 ClusterIP Service 仅代理 leader（modelDeploymentFrontLeadersOnly），而副本的 headless Service 则收窄到对应副本。若在这里就做决定，会导致三者之一拿到错误的端点。

	systemmeta.NoteResource(svc, ModelDeploymentResourceType, map[string]string{
		ModelDeploymentResourceNoteRole: role.Name,
	})
	kubemeta.ControlOnWithoutBlock(svc, md, workercore.SchemeGroupVersionKind("ModelDeployment"))

	return svc
}

// modelDeploymentServicePort 返回 deployment 对外发布的端口：即角色声明的第一个端口条目，或所有受支持引擎的 OpenAI 兼容服务器监听的默认端口。它同时是 Service 自身的端口和对外发布端点的端口；副本实际监听的端口由 modelDeploymentTargetPort 决定。
//
// 它读取与副本相同的渲染结果而非直接读取原始字段，从而避免 Service 与其背后的容器指定不一致的端口。
func modelDeploymentServicePort(role *workercore.ModelDeploymentRole) core.ContainerPort {
	if len(role.Ports) == 0 {
		return core.ContainerPort{
			Name:          modelDeploymentDefaultPortName,
			Protocol:      core.ProtocolTCP,
			ContainerPort: modelDeploymentDefaultPort,
		}
	}

	return modelDeploymentContainerPorts(role)[0]
}

// modelDeploymentTargetPort 返回角色副本提供服务的容器端口：即其 Service 指向的端口、门控评估的端口以及 router 连接的端口。
//
// 未声明端口的角色会使用其自身的 --port（包括引擎识别为该参数的任何写法）来改变目标端口。operator 仅在角色未传入 --port 时才自动填充，因此该参数就是引擎实际监听的端口；如果目标端口仍留在默认值上，所有请求都会发往一个无人监听的端口。
//
// 已声明的端口不会被移动。operator 会将声明的端口渲染到引擎的 --port 中，且准入检查会拒绝角色自身 --port 与之不一致的配置。
//
// 直连 decoder 的目标端口也不会被移动，因为 fronted 表示其路由代理占用了服务端口：此时角色的 --port 指定的是代理背后的引擎端口，而非 Pod 的对外入口。
func modelDeploymentTargetPort(
	engine string, role *workercore.ModelDeploymentRole, fronted bool,
) core.ContainerPort {
	port := modelDeploymentServicePort(role)
	if len(role.Ports) > 0 || fronted {
		return port
	}
	if listen, ok := ModelDeploymentRoleListenPort(engine, role); ok {
		port.ContainerPort = listen
	}

	return port
}

// ModelDeploymentRoleListenPort 返回角色传入的最后一个 --port 参数值（支持引擎识别为该参数的任何写法）；若未传入或无法解析，则返回 false。引擎的基础 argv 和连接器参数都不包含监听标志，因此只能从角色自身的参数中获取。
func ModelDeploymentRoleListenPort(engine string, role *workercore.ModelDeploymentRole) (int32, bool) {
	port, err := modelDeploymentCommandPort(engine, ModelDeploymentRoleArgs(role))

	return port, err == nil
}

// ModelDeploymentRoleFrontedByProxy 报告角色是否为由路由代理占用服务端口的直连 decoder。这完全由 spec 决定：在驱动它的 router 下，每个 decoder 都会渲染该代理，而接管了自身命令行的角色永远不会。
func ModelDeploymentRoleFrontedByProxy(
	md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole,
) bool {
	return len(role.Command) == 0 && modelDeploymentFrontsDecodeWithSidecar(md, role)
}

// ModelDeploymentRoleServingPort 返回角色 Service 所指向的目标端口。
func ModelDeploymentRoleServingPort(
	md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole,
) int32 {
	return modelDeploymentTargetPort(
		md.Spec.Engine.Name, role, ModelDeploymentRoleFrontedByProxy(md, role),
	).ContainerPort
}

// ModelDeploymentRoleServingProtocol 返回角色 Service 对外发布的协议。
func ModelDeploymentRoleServingProtocol(role *workercore.ModelDeploymentRole) core.Protocol {
	return modelDeploymentServicePort(role).Protocol
}

// ModelDeploymentRoleServingScheme 返回角色引擎端点对外发布的协议方案（scheme）。
func ModelDeploymentRoleServingScheme(engine string, role *workercore.ModelDeploymentRole) core.URIScheme {
	scheme, _ := modelDeploymentEngineTransport(engine, ModelDeploymentRoleArgs(role))

	return scheme
}

// modelDeploymentEndpoint 返回所有副本共同对外提供服务的唯一地址。
//
// 该地址是推导出来的，而非从 Service 回读：集群内 DNS 名称由 Service 的名称和 namespace 决定——两者均由本 operator 指定——而 ClusterIP 既不是调用方使用的地址，在重建后也不稳定。
//
// 协议方案必须与端口从同一个角色读取：一个通过 TLS 提供服务的角色若对外发布为 http://，客户端将无法使用该地址，而其背后的副本却会被使用 TLS 的探针判定为 Ready。这会导致两套事实彼此脱节，而门控的存在正是为了防止这种情况。入口角色同时决定两者，因为该 Service 代理的就是这个角色。
//
// 它是从角色自身的参数中读取，而非从渲染后的命令行读取（本函数无法构造完整命令行）。这一做法之所以成立，全赖 operator 自身添加的参数不包含任何监听或 TLS 标志，这一点由测试断言保证，而非在此处假定。
func modelDeploymentEndpoint(md *workercore.ModelDeployment) string {
	role := modelDeploymentFrontRole(md)
	port := modelDeploymentServicePort(role)

	// 此处仅需要协议方案，刻意丢弃另一个返回值并非疏漏。另一个返回值报告探针是否可以评估该监听器，这与客户端应连接什么地址是两个不同的问题：修改了 host 的角色仍然在该地址背后提供服务，要求客户端证书的角色也仍然使用 TLS 提供服务。这两种情况都不是错误，若将其视为错误反而会拒绝发布正确的地址。
	//
	// 即使角色修改了引擎端口，此处使用的仍是对外发布的端口：Service 的 targetPort 会跟随引擎端口变化，因此调用方原先连接的地址可以继续正常工作。
	scheme, _ := modelDeploymentEngineTransport(md.Spec.Engine.Name, ModelDeploymentRoleArgs(role))

	return strings.ToLower(string(scheme)) + "://" +
		md.Name + "." + md.Namespace + ".svc:" + strconvx.Itoa(int(port.ContainerPort))
}

// 拼出 http://<md>-<role>.<ns>.svc:<port>
func modelDeploymentRoleEndpoint(
	md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole,
) string {
	port := modelDeploymentServicePort(role)
	scheme, _ := modelDeploymentEngineTransport(md.Spec.Engine.Name, ModelDeploymentRoleArgs(role))

	return strings.ToLower(string(scheme)) + "://" + md.Name + "-" + role.Name + "." +
		md.Namespace + ".svc:" + strconvx.Itoa(int(port.ContainerPort))
}

// alignModelDeploymentService 将渲染后的 Service 合并到观测到的 Service 上，并报告是否发生了变更。
//
// 这里采用对齐合并而非整体替换，因为 Service 承载了并非由 operator 写入且绝不能丢弃的状态——尤其是已分配的 ClusterIP，所有解析过它的客户端仍在使用该地址。
func alignModelDeploymentService(actual, expected *core.Service) (changed bool) {
	key := modelDeploymentEligibilityActiveAnnotation
	if actual.Annotations[key] != expected.Annotations[key] {
		if expected.Annotations[key] == "" {
			delete(actual.Annotations, key)
		} else {
			if actual.Annotations == nil {
				actual.Annotations = map[string]string{}
			}
			actual.Annotations[key] = expected.Annotations[key]
		}
		changed = true
	}
	if !maps.Equal(actual.Spec.Selector, expected.Spec.Selector) {
		actual.Spec.Selector = expected.Spec.Selector
		changed = true
	}

	if !equalModelDeploymentServicePorts(actual.Spec.Ports, expected.Spec.Ports) {
		actual.Spec.Ports = expected.Spec.Ports
		changed = true
	}

	// Type 也会被收敛，且在本控制器开始 watch Service 后这一路径变为可达：外部对 NodePort 或 LoadBalancer 的带外修改现在会唤醒本 reconciler，若不处理 type，就意味着唤醒后观测到了漂移却唯独放任它不纠正。deployment 的入口按设计为 ClusterIP——将每个副本集暴露在节点端口上，不是本 operator 会替用户做的决定。
	//
	// 端口会随 type 一同直接替换而非仅作比较：从 NodePort 切回时必须丢弃 API server 分配的 `nodePort`，而上文的端口比较刻意忽略了该字段，因此无法察觉这一差异。
	if actual.Spec.Type != expected.Spec.Type {
		actual.Spec.Type = expected.Spec.Type
		actual.Spec.Ports = expected.Spec.Ports
		changed = true
	}

	// 收敛该字段是因为它可变且起关键支撑作用。副本的 headless Service 必须在 kubelet 判定成员就绪之前发布它们，原因在于：成员只有先加入集合通信组才能变为就绪，而找到对端需要依赖这些 DNS 记录；若记录本身还要等待就绪，组就会自我死锁。带外关闭该字段正好会破坏这一点，表现为所有对象看起来都正常，但副本始终无法开始服务。
	//
	// clusterIP 不会在此一并收敛，这种不对称性源于 API 本身而非刻意取舍：它在创建后不可变，在此写入只会导致更新失败，而无法修复任何问题。
	if actual.Spec.PublishNotReadyAddresses != expected.Spec.PublishNotReadyAddresses {
		actual.Spec.PublishNotReadyAddresses = expected.Spec.PublishNotReadyAddresses
		changed = true
	}

	return changed
}

// equalModelDeploymentServicePorts 仅比较本 operator 渲染的端口字段。Service 的端口包含 API server 填充的默认值，若比较整个切片，每次协调都会判定存在差异并无限重写对象。
func equalModelDeploymentServicePorts(actual, expected []core.ServicePort) bool {
	if len(actual) != len(expected) {
		return false
	}
	for i := range expected {
		if actual[i].Name != expected[i].Name ||
			actual[i].Port != expected[i].Port ||
			actual[i].Protocol != expected[i].Protocol ||
			actual[i].TargetPort != expected[i].TargetPort {
			return false
		}
	}

	return true
}
