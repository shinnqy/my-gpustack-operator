// 整组健康资格判定：以 instance 粒度而非单个成员粒度决定副本端点是否可被选中的谓词。
//
// 仅当整组都满足条件时，副本才会被准入选路。单个成员就绪并不代表整组可用：
// 某个成员正常而对等成员宕机时仍无法提供服务，因为整组构成同一条前向路径。
// 本文件将该路径拆分为五个具名检查项（leg）并保留三值判定结果，因为两种未通过状态
// 的行为必须不同，而布尔值会将它们混为一谈。
//
// 三种判定结果对应三种不同的处置语义。FAILED 表示明确的健康故障，可立即撤销资格。
// UNKNOWN 表示本 operator 无法完成的观测，其行为为保持现状（hold）：既不会单独触发撤销，
// 也不会恢复资格。VERIFIED 表示已评估且通过。若将 FAILED 与 UNKNOWN 合并，
// 要么会因缺失观测而将健康的部署黑洞化，要么会在规范禁止的弱信号下授予资格。
//
// group-forward 检查项目前对部分形态已可观测；对于不可观测的形态，本文件会说明
// 具体受阻原因，而非笼统认定无法观测。非分离式多 rank 副本通过绑定本 operator 自有
// 标识符的单 token 请求进行验证，具体见 probe 文件。单成员副本没有跨成员路径需要验证，
// 因此不适用。分离式副本在各引擎上均因各自实测原因保持不支持状态，因为在这类引擎上
// 单次请求都无法跨越不同 role。
package worker

import (
	"context"
	"slices"
	"strings"

	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/utils/strconvx"
)

// modelDeploymentLegVerdict 表示单个检查项的判定结果。
type modelDeploymentLegVerdict int

const (
	// modelDeploymentLegVerified 表示已完成评估且通过的检查项。
	modelDeploymentLegVerified modelDeploymentLegVerdict = iota

	// modelDeploymentLegFailed 表示明确的健康故障。它会触发资格撤销，且无论组谓词的
	// 其他部分是否可验证都会撤销，因为已知损坏的组无需更多证据。
	modelDeploymentLegFailed

	// modelDeploymentLegUnknown 表示本 operator 无法评估的检查项，其行为为保持现状。
	modelDeploymentLegUnknown
)

// modelDeploymentQualificationLegName 表示整组谓词中的某一项条件。它按名称传递，
// 以便处于保持状态的 instance 能准确报告是哪一项未通过验证，而非笼统提示存在未验证项。
type modelDeploymentQualificationLegName string

const (
	modelDeploymentLegMemberSetComplete    modelDeploymentQualificationLegName = "MemberSetComplete"
	modelDeploymentLegMembersReady         modelDeploymentQualificationLegName = "MembersReady"
	modelDeploymentLegGroupForward         modelDeploymentQualificationLegName = "GroupForward"
	modelDeploymentLegNoPlannedRetirement  modelDeploymentQualificationLegName = "NoPlannedRetirement"
	modelDeploymentLegNoPendingReplacement modelDeploymentQualificationLegName = "NoPendingReplacement"
)

// modelDeploymentQualificationLeg 表示一个具名条件及其判定结果。
type modelDeploymentQualificationLeg struct {
	Name    modelDeploymentQualificationLegName
	Verdict modelDeploymentLegVerdict
	Reason  string
}

// modelDeploymentMembershipGeneration 精确标识某次验证所针对的一组 Pod。
//
// 它是实时推导的，从不持久化存储。若需要持久化 generation，就必须引入写入方、
// 存储位置和排序规则，而其中任何一环都可能在重启或漏写时导致旧验证继续作用于
// 已不存在的组。直接从成员 UID 集合推导可彻底避免这一问题：成员一旦替换，
// UID 集合即发生变化，先前的验证自然不再适用于新集合。
//
// 它是顺序无关的，而直接拼接字符串做不到这一点。成员列表来自 Pod 列表查询，
// 以不同顺序返回相同 Pod 的查询结果描述的仍是同一个组；若 generation 对顺序敏感，
// 就会将其误判为新成员集合，从而无故将健康的部署置于保持状态。
type modelDeploymentMembershipGeneration string

// modelDeploymentGenerationOf 推导副本当前成员集合的 generation。
func modelDeploymentGenerationOf(view modelDeploymentReplicaView) modelDeploymentMembershipGeneration {
	uids := make([]string, 0, len(view.Members))
	for _, member := range view.Members {
		uids = append(uids, string(member.UID))
	}
	slices.Sort(uids)

	return modelDeploymentMembershipGeneration(strings.Join(uids, "/"))
}

// modelDeploymentPendingReplacement 记录本轮协调已判定为过时的 role 与 ordinal，
// 从而让资格判定拒绝恢复正在淘汰中的副本。它直接读取同一轮中已计算好的 rollout 结果，
// 从不根据渲染结果重新计算，因为关于“该副本是否正在被替换”的判断必须保持唯一一致。
type modelDeploymentPendingReplacement struct {
	// ordinals 按 role 名称为第一层键、副本 ordinal 为第二层键进行索引。
	ordinals map[string]map[int]bool
}

// modelDeploymentPending 报告指定的副本是否已被标记为待替换。
func (p modelDeploymentPendingReplacement) modelDeploymentPending(role string, ordinal int) bool {
	if p.ordinals == nil {
		return false
	}

	return p.ordinals[role][ordinal]
}

// modelDeploymentGroupForwardState 表示引擎级 group-forward 观测可能给出的结论。
// 本 operator 目前产生其中 NotApplicable 和 Unsupported 两种状态，其余中间状态
// 用于为后续接入实际观测预留准确的语义位置。
type modelDeploymentGroupForwardState string

const (
	// modelDeploymentGroupForwardNotApplicable 表示完全没有跨成员路径的副本，
	// 因此无需观测。它不同于 Unsupported，若将两者混淆会导致所有普通单成员部署永久处于保持状态。
	modelDeploymentGroupForwardNotApplicable modelDeploymentGroupForwardState = "NotApplicable"

	modelDeploymentGroupForwardVerified modelDeploymentGroupForwardState = "Verified"

	modelDeploymentGroupForwardFailed modelDeploymentGroupForwardState = "Failed"

	// modelDeploymentGroupForwardUnknown 表示无法完成观测或观测结果不够新鲜、不足以支撑判定的状态。
	modelDeploymentGroupForwardUnknown modelDeploymentGroupForwardState = "Unknown"

	// modelDeploymentGroupForwardUnsupported 表示不存在引擎级 group-forward 观测手段的部署形态。
	modelDeploymentGroupForwardUnsupported modelDeploymentGroupForwardState = "Unsupported"
)

// modelDeploymentGroupForward 表示单个副本的 group-forward 观测证据。
type modelDeploymentGroupForward struct {
	State       modelDeploymentGroupForwardState
	Reason      string
	Generation  modelDeploymentMembershipGeneration
	Observation string
}

// modelDeploymentInstanceQualification 表示单个副本的整组健康资格判定结果。
type modelDeploymentInstanceQualification struct {
	// View 是当前进行资格判定的副本视图，调用方可通过它访问其涵盖的成员。
	View modelDeploymentReplicaView
	// Generation 是评估下列检查项时所针对的成员集合标识。
	Generation modelDeploymentMembershipGeneration
	// GroupForward 保留完整结构而不展平为普通检查项，因为激活资格由它决定，
	// 且 status 需要报告具体是哪种状态导致了保持。
	GroupForward modelDeploymentGroupForward
	// PairAttempt 是在通过有向配对探测验证该副本时记录的探测尝试信息。
	// 保留该字段以便读取方无需重新推导即可区分代理形态与覆盖 rank 形态的结论，
	// 并能读取该结论所绑定的请求信息。
	PairAttempt *modelDeploymentGroupForwardPairAttempt
	// Observed 表示是否已有任意成员实际报告了就绪状态。本轮刚创建的副本尚未报告，
	// 完全由这类副本组成的部署尚未产生任何资格观测结果——即 status 中所称的 NotObserved，
	// 不应将其重述为保持状态。
	Observed bool
	Legs     []modelDeploymentQualificationLeg
}

// Leg 读取指定名称的检查项。
func (q modelDeploymentInstanceQualification) Leg(
	name modelDeploymentQualificationLegName,
) modelDeploymentQualificationLeg {
	for _, leg := range q.Legs {
		if leg.Name == name {
			return leg
		}
	}

	return modelDeploymentQualificationLeg{
		Name:    name,
		Verdict: modelDeploymentLegUnknown,
		Reason:  "the leg was not evaluated",
	}
}

// modelDeploymentGroupState 表示组健康资格所处的状态。
//
// 这里的命名与 status 对外发布的名称保持一致，因为本 operator 对每个组只报告一种状态，
// 为同一概念维护两套词汇没有任何益处。
type modelDeploymentGroupState string

const (
	// modelDeploymentGroupStateEligible 表示所有检查项均已通过验证的组状态。
	modelDeploymentGroupStateEligible modelDeploymentGroupState = "Eligible"
	// modelDeploymentGroupStateWithdrawn 表示因明确故障而被撤销资格的组状态。
	modelDeploymentGroupStateWithdrawn modelDeploymentGroupState = "Withdrawn"
	// modelDeploymentGroupStateAwaitingMembers 表示成员集合尚未建立完整的组状态。
	modelDeploymentGroupStateAwaitingMembers modelDeploymentGroupState = "AwaitingMembers"
	// modelDeploymentGroupStateAwaitingReady 表示成员尚未全部达到 Ready=True 的组状态。
	modelDeploymentGroupStateAwaitingReady modelDeploymentGroupState = "AwaitingReady"
	// modelDeploymentGroupStateAwaitingProbe 表示前向路径尚未完成观测的组状态。
	modelDeploymentGroupStateAwaitingProbe modelDeploymentGroupState = "AwaitingProbe"
	// modelDeploymentGroupStateUnsupported 表示不存在可用观测手段的部署形态状态。
	modelDeploymentGroupStateUnsupported modelDeploymentGroupState = "Unsupported"
	// modelDeploymentGroupStateUnknown 表示因本 operator 无法归类原因而处于保持状态的组状态。
	modelDeploymentGroupStateUnknown modelDeploymentGroupState = "Unknown"
)

// modelDeploymentQualificationLegOrder 是检查项的评估顺序，也是处于保持状态的组报告
// 阻塞检查项时的顺序。
//
// 该顺序即实际评估顺序，因此第一个未通过验证的检查项正是该组获得资格前
// 必须先发生改变的条件。
var modelDeploymentQualificationLegOrder = []modelDeploymentQualificationLegName{
	modelDeploymentLegMemberSetComplete,
	modelDeploymentLegMembersReady,
	modelDeploymentLegGroupForward,
	modelDeploymentLegNoPlannedRetirement,
	modelDeploymentLegNoPendingReplacement,
}

// MembersReady 报告组内所有成员是否均报告为 Ready=True。
//
// 这是针对成员就绪问题的故障关闭（fail-closed）回答。只有 Ready=True 才是肯定答案，
// 因此只要有成员尚未报告就绪状态，或报告结果不是 True，整组就不安全。
// 该检查项本身仍保持三值语义，因为仍在启动中的组应处于保持而非撤销状态，
// 但无论哪种情况，该组当前都不予选路。
func (q modelDeploymentInstanceQualification) MembersReady() bool {
	return q.Leg(modelDeploymentLegMembersReady).Verdict == modelDeploymentLegVerified
}

// GroupState 对组进行状态分类。
//
// 阻塞检查项只选择一次，因此状态与原因绝不会在“是什么阻塞了该组”上产生分歧。
// 优先级顺序为：撤销高于不支持形态，不支持形态高于保持；这与 status 中已发布的
// 优先级一致，确保此处的分类与 status 中的展示不会对两个事实的先后关系产生冲突。
func (q modelDeploymentInstanceQualification) GroupState() modelDeploymentGroupState {
	if q.Eligible() {
		return modelDeploymentGroupStateEligible
	}
	if q.HasFailure() {
		return modelDeploymentGroupStateWithdrawn
	}
	if q.GroupForward.State == modelDeploymentGroupForwardUnsupported {
		return modelDeploymentGroupStateUnsupported
	}
	for _, name := range modelDeploymentQualificationLegOrder {
		if q.Leg(name).Verdict == modelDeploymentLegVerified {
			continue
		}
		switch name {
		case modelDeploymentLegMemberSetComplete:
			return modelDeploymentGroupStateAwaitingMembers
		case modelDeploymentLegMembersReady:
			return modelDeploymentGroupStateAwaitingReady
		case modelDeploymentLegGroupForward:
			return modelDeploymentGroupStateAwaitingProbe
		}

		// 由第一个未通过验证的检查项决定状态名称，因此因淘汰或替换检查项而被保持的组，
		// 会报告本 operator 尚未进一步细分的通用状态。
		return modelDeploymentGroupStateUnknown
	}

	return modelDeploymentGroupStateUnknown
}

// Reason 说明阻塞该组的具体事实，或声明该组已具备资格。
//
// 它对应的检查项与 GroupState 一致，避免出现状态提示等待某项条件而原因却指向
// 另一项条件的不一致情况。
func (q modelDeploymentInstanceQualification) Reason() string {
	if q.Eligible() {
		// 代理返回的响应并不能证明各个 rank 均已覆盖或 KV 传输正常。
		if q.PairAttempt != nil && q.PairAttempt.Shape == modelDeploymentGroupForwardPairShapeViaRoutingProxy {
			return "routing proxy returned the expected content with the probe request ID while naming " +
				"the prefiller; individual rank coverage and KV transfer remain unproven"
		}

		return "every leg of the group verified"
	}
	failed := q.HasFailure()
	for _, name := range modelDeploymentQualificationLegOrder {
		leg := q.Leg(name)
		if failed && leg.Verdict == modelDeploymentLegFailed {
			return leg.Reason
		}
	}
	if q.GroupForward.State == modelDeploymentGroupForwardUnsupported {
		return q.GroupForward.Reason
	}
	for _, name := range modelDeploymentQualificationLegOrder {
		if leg := q.Leg(name); leg.Verdict != modelDeploymentLegVerified {
			return leg.Reason
		}
	}

	return q.GroupForward.Reason
}

// HasFailure 报告是否存在明确的健康故障。该信号会无条件触发资格撤销，
// 且刻意不依赖组谓词本身是否可验证：已知损坏的组无需再要求其具备可验证性。
func (q modelDeploymentInstanceQualification) HasFailure() bool {
	for _, leg := range q.Legs {
		if leg.Verdict == modelDeploymentLegFailed {
			return true
		}
	}

	return false
}

// Eligible 是写入资格状态时唯一询问的条件。只要存在任意 UNKNOWN 检查项即保持现状，
// 因此处于保持状态的 instance 绝不会因为某个检查项被跳过而恢复资格。
func (q modelDeploymentInstanceQualification) Eligible() bool {
	for _, leg := range q.Legs {
		if leg.Verdict != modelDeploymentLegVerified {
			return false
		}
	}

	return true
}

// Conclusive 报告谓词是否已对该副本得出明确结论，而非留下悬而未决的问题。
// 它用于区分被撤销的副本与被保持的副本：被撤销的副本已明确不具备资格且将保持该状态，
// 而被保持的副本在缺失的观测到位后仍可能获得资格。
//
// 只有结论明确的副本才能计入对外发布的合格计数。若把被保持副本当前的 false 当作最终结果
// 计入统计，会导致上一轮发布的数字在下一轮被迫收回，也会将实际处于未观测状态的组误报为
// 完全不合格。
func (q modelDeploymentInstanceQualification) Conclusive() bool {
	for _, leg := range q.Legs {
		if leg.Verdict == modelDeploymentLegUnknown {
			return false
		}
	}

	return true
}

// HeldLegs 按稳定顺序列出未达到 Verified 的检查项名称，从而确保在组状态未变时，
// 连续两轮协调报告的原因不会来回变化。
func (q modelDeploymentInstanceQualification) HeldLegs() []modelDeploymentQualificationLegName {
	held := make([]modelDeploymentQualificationLegName, 0, len(q.Legs))
	for _, leg := range q.Legs {
		if leg.Verdict != modelDeploymentLegVerified {
			held = append(held, leg.Name)
		}
	}
	slices.Sort(held)

	return held
}

// Activated 报告是否允许恢复该副本的资格。资格撤回从来不受此条件限制。
//
// 该准入门槛要求的是明确的肯定结论，而非否定结论的缺失。仅当 group-forward 检查项为
// Verified，或副本为 NotApplicable（即不存在需要验证的组）时才允许恢复。Unknown 状态
// 会阻止恢复：这意味着探测无法解析或根本未能询问成员，若将其视为放行许可，就会在毫无
// 依据的情况下恢复端点——而这正是该检查项要防止的失效模式。若把条件写成“非 Unsupported”，
// 一旦观测可能失败就会落入同样的问题。
//
// 设置该门槛是因为对于探针无法触达的部署形态，group-forward 检查项会返回 Unsupported。
// 在缺乏该观测能力时，reconciler 不得将端点重新放回选路集合，否则就相当于在低于规范要求的
// 弱信号下授予了资格；但它仍可以移除端点，因为所有健康故障信号都指向同一撤回方向。
func (q modelDeploymentInstanceQualification) Activated() bool {
	return q.GroupForward.State == modelDeploymentGroupForwardVerified ||
		q.GroupForward.State == modelDeploymentGroupForwardNotApplicable
}

// qualifyModelDeploymentInstances 为每个副本评估一次整组健康谓词。
//
// 先评估低开销检查项、最后执行实时观测，这一顺序具有实质意义而非单纯偏好。
// 缺少成员或存在未就绪成员的副本已由 informer 缓存中的事实判定失效，因此既无需付出
// 实时观测开销，更重要的是其明确故障绝不会因恰巧缺少观测而被弱化为保持状态。
//
// 有向配对传输通道是可选的。Prefill 或 Decode 组以配对形式提供服务，若未提供该传输通道，
// 本 operator 就完全无法观测配对状态，因此这类组会直接返回 Unsupported，而不是被问及
// 无法回答的问题。需要获得配对判定结果的调用方必须接入该传输通道，未接入的调用方则会
// 按普通组形态得到相应的默认判定结果。
func qualifyModelDeploymentInstances(
	ctx context.Context, md *workercore.ModelDeployment, pods []core.Pod,
	pending modelDeploymentPendingReplacement, fetch modelDeploymentGroupForwardFetch,
	pairFetch ...modelDeploymentGroupForwardPairFetch,
) []modelDeploymentInstanceQualification {
	roles := make(map[string]*workercore.ModelDeploymentRole, len(md.Spec.Roles))
	for i := range md.Spec.Roles {
		roles[md.Spec.Roles[i].Name] = &md.Spec.Roles[i]
	}

	views := modelDeploymentGroupPodsByReplica(pods)
	locals := make([]modelDeploymentReplicaLocal, 0, len(views))
	for _, view := range views {
		role := roles[view.Role]
		if role == nil {
			// 属于 spec 中已不再提及的 role 的副本正在退出；它不具备任何资格，
			// 为其报告阻塞检查项只会给出无人可采取行动的无效原因。
			continue
		}

		locals = append(locals, newModelDeploymentReplicaLocal(view, role))
	}

	answers := modelDeploymentGroupForwardAnswers(
		ctx, md, locals, fetch, modelDeploymentPairTransport(pairFetch),
	)

	qualifications := make([]modelDeploymentInstanceQualification, 0, len(locals))
	for i, local := range locals {
		qualifications = append(qualifications, local.modelDeploymentQualification(
			md, pending, answers[i],
		))
	}

	return qualifications
}

// modelDeploymentPairTransport 从可选参数中读取有向配对传输通道。
//
// 缺少传输通道代表未接入该观测能力，而不是允许绕过它继续询问配对：
// 调用方未传入该参数，即表明本 operator 没有观测配对的途径。
func modelDeploymentPairTransport(
	pairFetch []modelDeploymentGroupForwardPairFetch,
) modelDeploymentGroupForwardPairFetch {
	if len(pairFetch) == 0 {
		return nil
	}

	return pairFetch[0]
}

// modelDeploymentReplicaLocal 保存单个副本判定结果中无需实时观测的部分。
//
// 本地检查项放在此处而非直接放进最终资格结果中，是因为在发起任何观测前必须先了解
// 这些状态：只有成员完整且均已就绪的两个副本才能参与配对探测，不满足的副本根本不会被探测。
type modelDeploymentReplicaLocal struct {
	// view 是这些检查项所属的副本视图。
	view modelDeploymentReplicaView
	// role 是该副本所属的角色。
	role *workercore.ModelDeploymentRole
	// membersComplete 和 membersReady 是两个成员判定项。
	membersComplete modelDeploymentQualificationLeg
	membersReady    modelDeploymentQualificationLeg
}

// newModelDeploymentReplicaLocal 确定单个副本的两个成员判定项。
//
// 完整性依据成员自身声明的组总数来衡量，而非角色声明的大小，从而使因角色被修改而脱离当前配置的副本仍保持完整。
// 成员无法确立形态时保持待定；数量低于其声明总数的副本仍判定为失败。
func newModelDeploymentReplicaLocal(
	view modelDeploymentReplicaView, role *workercore.ModelDeploymentRole,
) modelDeploymentReplicaLocal {
	shape := modelDeploymentDeployedReplicaShape(view)
	membersComplete := modelDeploymentQualificationLeg{
		Name:    modelDeploymentLegMemberSetComplete,
		Verdict: shape.modelDeploymentReplicaLegVerdict(),
		Reason:  modelDeploymentReplicaHoldsReason(view, shape),
	}

	unready, unobserved := 0, 0
	for _, member := range view.Members {
		switch modelDeploymentMemberReadiness(member) {
		case modelDeploymentLegVerified:
		case modelDeploymentLegFailed:
			unready++
		default:
			unobserved++
		}
	}
	membersReady := modelDeploymentQualificationLeg{
		Name:    modelDeploymentLegMembersReady,
		Verdict: modelDeploymentLegVerified,
	}
	switch {
	case unobserved == len(view.Members) && len(view.Members) > 0:
		// 尚未观测到任何状态，这并非故障。本轮刚创建的副本完全没有 PodReady 条件，
		// 若将其解读为“未就绪”，会在健康部署刚创建时就误判其异常——并且会在创建后的下一轮
		// 导致上报结果发生变化，而未变更的 spec 不应触发这种状态写入。
		membersReady.Verdict = modelDeploymentLegUnknown
		membersReady.Reason = "no member's readiness has been observed yet"
	case unready == 0 && unobserved > 0:
		// 单个成员就绪不代表整个组就绪。当副本中一个成员已就绪而另一个同级成员仍处于启动窗口期内时，
		// 尚未完成该判定项所要求的观测，因此保持待定：若将其解读为“所有成员均报告就绪”，
		// 就会基于不完整的结果授予资格。
		membersReady.Verdict = modelDeploymentLegUnknown
		membersReady.Reason = strconvx.Itoa(unobserved) + " of " +
			strconvx.Itoa(len(view.Members)) + " members are still starting"
	case unready == 0:
		membersReady.Reason = "every member reports ready"
	default:
		membersReady.Verdict = modelDeploymentLegFailed
		membersReady.Reason = strconvx.Itoa(unready) + " of " +
			strconvx.Itoa(len(view.Members)) + " members report not ready"
	}

	return modelDeploymentReplicaLocal{
		view:            view,
		role:            role,
		membersComplete: membersComplete,
		membersReady:    membersReady,
	}
}

// localReady 报告副本是否完整且其所有成员均报告 Ready=True。
//
// 这是进行配对探测的前提条件，比普通组的条件更严格：一个配对同时代表两个副本，
// 因此未处于服务状态的副本绝不能成为配对的一方。
func (l modelDeploymentReplicaLocal) localReady() bool {
	return l.membersComplete.Verdict == modelDeploymentLegVerified &&
		l.membersReady.Verdict == modelDeploymentLegVerified
}

// localFault 返回阻止对该副本进行探测的成员判定项名称。
func (l modelDeploymentReplicaLocal) localFault() string {
	if l.membersComplete.Verdict != modelDeploymentLegVerified {
		return l.membersComplete.Reason
	}

	return l.membersReady.Reason
}

// modelDeploymentQualification 根据副本的本地判定项及其观测结果组装整个组的判定结论。
//
// 判定项的顺序保持不变，因为读取判定项的顺序就是状态读取方看到它们的顺序。
func (l modelDeploymentReplicaLocal) modelDeploymentQualification(
	md *workercore.ModelDeployment, pending modelDeploymentPendingReplacement,
	answer modelDeploymentGroupForwardAnswer,
) modelDeploymentInstanceQualification {
	qualification := modelDeploymentInstanceQualification{
		View:         l.view,
		Generation:   modelDeploymentGenerationOf(l.view),
		GroupForward: answer.forward,
		PairAttempt:  answer.attempt,
	}
	for _, member := range l.view.Members {
		if modelDeploymentMemberReadiness(member) != modelDeploymentLegUnknown {
			qualification.Observed = true

			break
		}
	}

	// 完整性检查与就绪性检查并不冗余。缺少已声明成员的副本可能其现有成员全部报告就绪，
	// 但 Kueue 并未为其组装 Workload，也未准入任何资源。这种情况属于成员缺失（仅残留部分 Pod），
	// 而非部分正常工作。
	qualification.Legs = append(qualification.Legs, l.membersComplete)
	qualification.Legs = append(qualification.Legs, l.membersReady)

	qualification.Legs = append(qualification.Legs, modelDeploymentQualificationLeg{
		Name:    modelDeploymentLegGroupForward,
		Verdict: groupForwardVerdict(answer.forward.State),
		Reason:  answer.forward.Reason,
	})

	qualification.Legs = append(qualification.Legs, modelDeploymentQualificationLeg{
		Name:    modelDeploymentLegNoPlannedRetirement,
		Verdict: retirementVerdict(md, l.view),
		Reason:  retirementReason(md, l.view),
	})

	replacing := modelDeploymentQualificationLeg{
		Name:    modelDeploymentLegNoPendingReplacement,
		Verdict: modelDeploymentLegVerified,
		Reason:  "the replica is not condemned for replacement by this pass",
	}
	if l.view.Seated && pending.modelDeploymentPending(l.view.Role, l.view.Ordinal) {
		replacing.Verdict = modelDeploymentLegFailed
		replacing.Reason = "the replica is condemned for replacement by this pass"
	}
	qualification.Legs = append(qualification.Legs, replacing)

	return qualification
}

// modelDeploymentGroupForwardAnswer 表示单个副本的观测结果及其背后依据。
type modelDeploymentGroupForwardAnswer struct {
	// forward 为观测结果本身。
	forward modelDeploymentGroupForward
	// attempt 为验证该副本的有向配对尝试记录（当通过已验证的配对完成验证时存在）。
	attempt *modelDeploymentGroupForwardPairAttempt
}

// modelDeploymentGroupForwardAnswers 获取每个副本的观测结果。
//
// 普通角色逐一探测，而 Prefill 和 Decode 角色则成对探测，因为配对中的任何一方都无法单独提供服务：
// prefiller 计算 KV 而 decoder 消费 KV，因此一个返回预期响应的请求即可同时作为双方的有效证据。
func modelDeploymentGroupForwardAnswers(
	ctx context.Context, md *workercore.ModelDeployment,
	locals []modelDeploymentReplicaLocal,
	fetch modelDeploymentGroupForwardFetch,
	pairFetch modelDeploymentGroupForwardPairFetch,
) []modelDeploymentGroupForwardAnswer {
	answers := make([]modelDeploymentGroupForwardAnswer, len(locals))

	var prefills, decodes []int
	for i, local := range locals {
		switch ModelDeploymentEffectiveRoleKind(local.role) {
		case workercore.ModelDeploymentRoleKindPrefill:
			prefills = append(prefills, i)

			continue
		case workercore.ModelDeploymentRoleKindDecode:
			decodes = append(decodes, i)

			continue
		}

		// 已被明确判定为失败的成员判定项所否决的副本无需进行实时探测：发送请求只会消耗推理引擎的 token，
		// 却无法改变故障结论。此时仍会以无传输方式调用观测逻辑：不依赖网络的结论（单成员副本为
		// NotApplicable，未验证的形态为 Unsupported）保持不变，仅探测项返回缺少证据。
		transport := fetch
		if local.membersComplete.Verdict == modelDeploymentLegFailed ||
			local.membersReady.Verdict == modelDeploymentLegFailed {
			transport = nil
		}
		answers[i] = modelDeploymentGroupForwardAnswer{
			forward: observeModelDeploymentGroupForward(ctx, md, local.role, local.view, transport),
		}
	}

	for i, answer := range modelDeploymentPairAnswers(ctx, md, locals, prefills, decodes, pairFetch) {
		answers[i] = answer
	}

	return answers
}

// modelDeploymentPairAnswers 获取同一部署中 Prefill 与 Decode 副本的探测结果。
//
// 两个角色的配置会一次性联合判定，因为配对涉及双方角色：若其中某个角色的路由本 operator 无法识别，
// 则该角色无法与任何对端配对，其对端角色亦然。
func modelDeploymentPairAnswers(
	ctx context.Context, md *workercore.ModelDeployment,
	locals []modelDeploymentReplicaLocal,
	prefills, decodes []int,
	fetch modelDeploymentGroupForwardPairFetch,
) map[int]modelDeploymentGroupForwardAnswer {
	answers := map[int]modelDeploymentGroupForwardAnswer{}
	if len(prefills) == 0 && len(decodes) == 0 {
		return answers
	}

	unsupported, unsupportedReason := modelDeploymentGroupForwardPairSupport(md)

	// 候选列表包含可以进行探测的副本。所有非候选副本均已有确定结果：要么该形态无需观测，
	// 要么副本尚未达到可探测的就绪状态，要么传输通道未连接。
	candidates := func(group []int) []int {
		ready := make([]int, 0, len(group))
		for _, i := range group {
			local := locals[i]
			switch {
			case unsupported:
				answers[i] = modelDeploymentGroupForwardAnswer{
					forward: modelDeploymentGroupForwardUnsupportedFor(unsupportedReason),
				}
			case !local.localReady():
				answers[i] = modelDeploymentGroupForwardAnswer{
					forward: modelDeploymentGroupForwardUnknownFor(
						"the group is not complete and ready, so the pair was not asked: " +
							local.localFault(),
					),
				}
			case fetch == nil:
				answers[i] = modelDeploymentGroupForwardAnswer{
					forward: modelDeploymentGroupForwardUnsupportedFor(
						"no directed pair transport is wired, so this operator has no observation " +
							"of a prefill-decode pair",
					),
				}
			default:
				ready = append(ready, i)
			}
		}

		return ready
	}
	readyPrefills := candidates(prefills)
	readyDecodes := candidates(decodes)

	// 缺少对端成员的配对无法成对，因此没有就绪对端副本的实例将保持待定，并注明所缺失的对象。
	if len(readyPrefills) == 0 || len(readyDecodes) == 0 {
		for _, i := range append(append([]int{}, readyPrefills...), readyDecodes...) {
			answers[i] = modelDeploymentGroupForwardAnswer{
				forward: modelDeploymentGroupForwardUnknownFor(
					"no ready replica of the other role is available to complete the pair",
				),
			}
		}

		return answers
	}

	// 按固定顺序发起探测，以确保每一轮都以相同顺序探测相同的配对。
	slices.SortStableFunc(readyPrefills, func(a, b int) int {
		return locals[a].view.Ordinal - locals[b].view.Ordinal
	})
	slices.SortStableFunc(readyDecodes, func(a, b int) int {
		return locals[a].view.Ordinal - locals[b].view.Ordinal
	})

	verified := map[int]bool{}
	for _, prefill := range readyPrefills {
		for _, decode := range readyDecodes {
			if verified[prefill] && verified[decode] {
				// 双方均已不再需要此次配对探测，因此再次探测既不会改变任何一方的结果，还会白白消耗推理引擎的 token。
				continue
			}

			result := observeModelDeploymentGroupForwardPair(
				ctx, md, locals[prefill].view, locals[decode].view, fetch,
			)

			// 配对双方独立汇总结果，因为驱动会分别给出各自的结果。已验证的结果绝不会被后续失败的配对伙伴覆盖，
			// 且验证该副本的那次尝试记录会作为依据保留在旁。
			if !verified[prefill] {
				answers[prefill] = modelDeploymentGroupForwardAnswer{forward: result.Prefill}
			}
			if !verified[decode] {
				answers[decode] = modelDeploymentGroupForwardAnswer{forward: result.Decode}
			}
			if result.Prefill.State == modelDeploymentGroupForwardVerified {
				verified[prefill] = true
				answers[prefill] = modelDeploymentGroupForwardAnswer{
					forward: result.Prefill, attempt: &result.Attempt,
				}
			}
			if result.Decode.State == modelDeploymentGroupForwardVerified {
				verified[decode] = true
				answers[decode] = modelDeploymentGroupForwardAnswer{
					forward: result.Decode, attempt: &result.Attempt,
				}
			}
		}
	}

	return answers
}

// modelDeploymentGroupForwardPairSupport 报告本 operator 是否无法对该部署形态对应的有向配对
// 提供已验证的观测支持，以及不支持的原因。
//
// 这些检查精确且具名，因为维护一份通用的引擎支持表会导致引擎差异在两处重复判定，
// 而配对驱动本身已经决定了其中大部分差异。
//
// 第一个返回值表示不支持，以便调用处代码更符合其语义。
func modelDeploymentGroupForwardPairSupport(
	md *workercore.ModelDeployment,
) (bool, string) {
	// 引擎支持情况由配对驱动自身决定，驱动也会以相同方式说明。
	if _, known := modelDeploymentGroupForwardPairEngines[md.Spec.Engine.Name]; !known {
		return true, "engine " + md.Spec.Engine.Name +
			" has no group-forward observation this operator has verified"
	}

	// 接管了自身命令行的角色所提供的路由本 operator 无法获知，而配对无法向未知路由发起探测。
	// 此检查覆盖配对的双方，因为无法配对的一方不会因为对端可配对而变得可配对。
	for i := range md.Spec.Roles {
		role := &md.Spec.Roles[i]
		switch ModelDeploymentEffectiveRoleKind(role) {
		case workercore.ModelDeploymentRoleKindPrefill, workercore.ModelDeploymentRoleKindDecode:
		default:
			continue
		}
		if len(role.Command) > 0 {
			return true, "role " + role.Name +
				" took over its command line, so the route its pair would be asked at is one this " +
				"operator does not know"
		}
	}

	// vLLM 配对仅能通过路由代理进行观测，因为 connector 握手时通过来自 prefiller 自身传输注册表的
	// engine id 来指定 prefiller，而本 operator 并不读取该注册表。
	if md.Spec.Engine.Name == workercore.ModelDeploymentEngineVLLM {
		if decodeRole := modelDeploymentGroupForwardPairDecodeRole(md); decodeRole != nil &&
			!ModelDeploymentRoleFrontedByProxy(md, decodeRole) {
			return true, "the prefiller's engine id is held in the prefiller's own transfer registry " +
				"and this operator does not read that registry, so a vLLM pair that no routing " +
				"proxy drives cannot be observed"
		}
	}

	return false, ""
}

// modelDeploymentMemberReadiness 将单个成员的就绪状态读取为三种结论，而非 podIsReady 返回的两种。
//
// 条件缺失并不等同于否定结论。刚创建的 Pod 完全没有 PodReady 条件，而 podIsReady 会将其判为未就绪。
// 若用于判断组是否健康，这种解读存在双重错误：它会仅仅因为部署刚创建就判定其异常，
// 并且会在创建后的下一轮导致结果变动，而未变更的 spec 不应触发此类状态写入。
// 只有存在且为 False 的条件才是真实的观测结果，应如实上报。
//
// Unknown 条件同样属于缺少证据。Kubernetes 会在合理场景下写入 PodReady=Unknown
// （如 kubelet 正在重启、Node 失联、驱逐正在进行），此时成员的健康状况未知；
// 将其视为 Failed 会报告集群从未观测到的确定性故障。只有 False 才是确定性结论。
//
// 仍处于启动窗口期内的容器也不属于故障。kubelet 尚未标记为 Started 的运行中容器，
// 意味着其 startupProbe 尚未成功——大模型在编译计算图时可能会长期处于此状态；
// 若将其判为 Failed，就会因为启动较慢而撤销副本资格，进而打断启动时间预算本应保护的编译过程。
// 处于终态等待原因的容器则恰恰相反：即便尚未产生任何就绪结果，也应直接作为确定性故障上报，
// 因为已明确损坏的组无需更多证据。
func modelDeploymentMemberReadiness(pod *core.Pod) modelDeploymentLegVerdict {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == core.PodReady && condition.Status == core.ConditionTrue {
			return modelDeploymentLegVerified
		}
	}

	for _, statuses := range [][]core.ContainerStatus{
		pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses,
	} {
		for _, status := range statuses {
			if status.State.Waiting != nil &&
				isTerminalWaitReason(status.State.Waiting.Reason) {
				return modelDeploymentLegFailed
			}
		}
	}

	if pod.Status.Phase == core.PodRunning {
		for _, status := range pod.Status.ContainerStatuses {
			if status.State.Running != nil && (status.Started == nil || !*status.Started) {
				return modelDeploymentLegUnknown
			}
		}
	}

	for _, condition := range pod.Status.Conditions {
		if condition.Type == core.PodReady {
			if condition.Status == core.ConditionFalse {
				return modelDeploymentLegFailed
			}

			return modelDeploymentLegUnknown
		}
	}

	return modelDeploymentLegUnknown
}

// groupForwardVerdict 将观测自身的状态映射为判定项的结论。Unsupported 的观测结果映射为
// UNKNOWN 而非 FAILED：此时并未发现组存在任何异常，若报告 Failed 则相当于宣称了本 operator 无法确认的健康故障。
func groupForwardVerdict(state modelDeploymentGroupForwardState) modelDeploymentLegVerdict {
	switch state {
	case modelDeploymentGroupForwardNotApplicable, modelDeploymentGroupForwardVerified:
		return modelDeploymentLegVerified
	case modelDeploymentGroupForwardFailed:
		return modelDeploymentLegFailed
	default:
		return modelDeploymentLegUnknown
	}
}

// retirementVerdict 报告该副本是否已有计划中的退服安排。此处仅读取预留信息而绝不写入，
// 且仅用于说明计划中的退出不属于组故障：健康状态撤销与成员替换会独立于任何预留执行，
// 因此预留后续的任何逻辑都无法让不健康的组继续保持合格资格。
func retirementVerdict(
	md *workercore.ModelDeployment, view modelDeploymentReplicaView,
) modelDeploymentLegVerdict {
	if retirementTargets(md, view) {
		return modelDeploymentLegFailed
	}

	return modelDeploymentLegVerified
}

// retirementReason 说明占用该副本的预留名称，或指出不存在此类预留。
func retirementReason(
	md *workercore.ModelDeployment, view modelDeploymentReplicaView,
) string {
	if !retirementTargets(md, view) {
		return "no retirement reservation claims this replica"
	}

	return "a retirement reservation names this replica"
}

// retirementTargets 报告部署的单个预留是否指定了该副本。处于任何状态的预留都会认定其占用该副本，
// 因为挂起或已完成的操作仍然是对该副本做出的决定，而非没有决定。
func retirementTargets(
	md *workercore.ModelDeployment, view modelDeploymentReplicaView,
) bool {
	reservation := md.Status.Retirement
	if reservation == nil {
		return false
	}
	if reservation.RoleName != view.Role {
		return false
	}

	return view.Seated && int(reservation.ReplicaOrdinal) == view.Ordinal
}

// modelDeploymentQualificationsByMember 按所覆盖每个成员的 UID 建立资格判定结果索引，
// 从而使按 Pod 执行的资格写入逻辑能够直接获取对应副本的结果，无需再次分组。
func modelDeploymentQualificationsByMember(
	qualifications []modelDeploymentInstanceQualification,
) map[types.UID]modelDeploymentInstanceQualification {
	byMember := make(map[types.UID]modelDeploymentInstanceQualification)
	for _, qualification := range qualifications {
		for _, member := range qualification.View.Members {
			byMember[member.UID] = qualification
		}
	}

	return byMember
}

// modelDeploymentRoleQualified 统计每个角色中整组通过资格判定的副本数量。
//
// 只要该角色存在任意尚未定论的副本，计数即为 nil，这是传输格式用来区分“未观测到的列表”与“已观测到的空列表”的方式。
// 拥有三个合格副本和一个待定副本的角色不会上报三个合格端点：它不会上报无法担保的数值，
// 因为看到数字 3 的读取方会误以为该角色状态明确，而实际上第四个副本尚不明确。
//
// 明确不合格的副本仍计入已定论范围。它代表对问题的确定回答，因此一个包含两个副本（一个合格、一个被撤销资格）
// 的角色会上报 1；而一个所有副本均已明确且无一合格的角色会上报显式的 0——列表完整且为空，
// 这正是副本资格被撤销后的表现。每个经由该谓词评估的角色都会发布计数，因此没有合格副本的角色会报告 0，
// 而非从状态中消失。
func modelDeploymentRoleQualified(
	qualifications []modelDeploymentInstanceQualification,
) map[string]*int32 {
	qualified := make(map[string]int32)
	seen := make(map[string]bool)
	conclusive := make(map[string]bool)
	for _, qualification := range qualifications {
		role := qualification.View.Role
		if !seen[role] {
			seen[role] = true
			conclusive[role] = true
		}
		// 该标志会对所有副本取交集，因此无论有多少同级副本合格，只要有一个未定论的副本就会使整个角色降级为未定论。
		// 若改为在第一个已定论副本上设置该标志，则会发布不完整的计数并掩盖尚未解决的悬而未决状态。
		conclusive[role] = conclusive[role] && qualification.Conclusive()
		// 副本只有在每个判定项都通过验证时才具备资格，这本身就意味着结论已定，因此此处无需额外检查。
		if qualification.Eligible() {
			qualified[role]++
		}
	}

	counts := make(map[string]*int32, len(seen))
	for role := range seen {
		if !conclusive[role] {
			continue
		}

		value := qualified[role]
		counts[role] = &value
	}

	return counts
}
