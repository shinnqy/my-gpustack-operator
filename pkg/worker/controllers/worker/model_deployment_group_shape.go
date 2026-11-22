package worker

import (
	core "k8s.io/api/core/v1"
	kueuepodconst "sigs.k8s.io/kueue/pkg/controller/jobs/pod/constants"

	"gpustack.ai/gpustack/pkg/utils/strconvx"
)

// 副本的实际部署形态根据其成员携带的组总数和成员索引读取，绝不从角色声明的大小推断。

// modelDeploymentReplicaShapeState 表示副本成员对其形态的确立程度。
type modelDeploymentReplicaShapeState int

const (
	// modelDeploymentReplicaShapeUnreadable 表示成员无法确立有效形态：成员声明的总数不一致、
	// 席位索引重复、越界或无法解析，或者多成员组缺少席位索引信息。
	modelDeploymentReplicaShapeUnreadable modelDeploymentReplicaShapeState = iota
	// modelDeploymentReplicaShapeIncomplete 表示形态可读取，但现有成员数量少于其声明的总数。
	modelDeploymentReplicaShapeIncomplete
	// modelDeploymentReplicaShapeComplete 表示形态可读取且副本成员已全部就位。
	modelDeploymentReplicaShapeComplete
)

// modelDeploymentReplicaShape 表示单个副本的实际部署规模及其成员对该形态的确立状态。
type modelDeploymentReplicaShape struct {
	// Total 是成员声明的组大小。仅当形态不可读时为 0。
	Total int
	// State 表示成员所确立的形态状态。
	State modelDeploymentReplicaShapeState
	// Reason 说明存在的冲突或成员缺口。该文本会用于阻断原因展示，因此需写明实际读取到的
	// 信息与预期信息。
	Reason string
}

// modelDeploymentReplicaIsWhole 返回成员是否已填满其声明的组总数。
func (s modelDeploymentReplicaShape) modelDeploymentReplicaIsWhole() bool {
	return s.State == modelDeploymentReplicaShapeComplete
}

// modelDeploymentReplicaLegVerdict 返回该形态所对应的链路判定结果。
//
// 成员数少于声明总数的副本说明存在成员缺失，属于故障。而彼此矛盾的成员无法描述有效形态，
// 应判定为 Unknown 而非故障：本 operator 尚无法确定其任何状态，也不能断定其已损坏。
func (s modelDeploymentReplicaShape) modelDeploymentReplicaLegVerdict() modelDeploymentLegVerdict {
	switch s.State {
	case modelDeploymentReplicaShapeComplete:
		return modelDeploymentLegVerified
	case modelDeploymentReplicaShapeIncomplete:
		return modelDeploymentLegFailed
	default:
		return modelDeploymentLegUnknown
	}
}

// modelDeploymentDeployedReplicaShape 从单个副本自身的成员读取其形态。该函数为纯函数。
func modelDeploymentDeployedReplicaShape(view modelDeploymentReplicaView) modelDeploymentReplicaShape {
	if len(view.Members) == 0 {
		return modelDeploymentReplicaShape{
			State:  modelDeploymentReplicaShapeUnreadable,
			Reason: "the replica holds no members, so it has no deployed shape to read",
		}
	}

	total, reason := modelDeploymentReplicaDeclaredTotal(view.Members)
	if reason != "" {
		return modelDeploymentReplicaShape{State: modelDeploymentReplicaShapeUnreadable, Reason: reason}
	}

	if reason = modelDeploymentReplicaSeatReason(view.Members, total); reason != "" {
		return modelDeploymentReplicaShape{State: modelDeploymentReplicaShapeUnreadable, Reason: reason}
	}

	if held := len(view.Members); held != total {
		return modelDeploymentReplicaShape{
			Total: total,
			State: modelDeploymentReplicaShapeIncomplete,
			Reason: "the replica holds " + strconvx.Itoa(held) + " of " + strconvx.Itoa(total) +
				" members it declared, so it is short of the group it was built at",
		}
	}

	return modelDeploymentReplicaShape{Total: total, State: modelDeploymentReplicaShapeComplete}
}

// modelDeploymentReplicaHoldsReason 说明副本的当前成员与实际部署总数之间的对应情况。
// 不可读的形态本身已足够说明问题，因为给出成员计数反而会成为成员信息所不支持的断言。
func modelDeploymentReplicaHoldsReason(
	view modelDeploymentReplicaView, shape modelDeploymentReplicaShape,
) string {
	if shape.State == modelDeploymentReplicaShapeUnreadable {
		return shape.Reason
	}

	return "the replica holds " + strconvx.Itoa(len(view.Members)) + " of " +
		strconvx.Itoa(shape.Total) + " declared members"
}

// modelDeploymentReplicaDeclaredTotal 返回所有成员一致声明的组大小，或不一致的原因。
//
// 未声明总数的成员视为单成员副本：在引入该 annotation 之前创建的 Pod 即如此，
// 且 modelDeploymentPodDescription 一贯按此方式读取。已声明总数的成员必须遵守该值，
// 因此将两者混在同一组内会产生自相矛盾。
func modelDeploymentReplicaDeclaredTotal(members []*core.Pod) (int, string) {
	total := 0
	for _, member := range members {
		declared, ok := modelDeploymentMemberDeclaredTotal(member)
		if !ok {
			return 0, "member " + member.Name + " declares a group total that is not a replica size, " +
				"so the replica's shape could not be established"
		}

		if total == 0 {
			total = declared

			continue
		}
		if total != declared {
			return 0, "the replica's members declare group totals of " + strconvx.Itoa(total) +
				" and " + strconvx.Itoa(declared) + ", so its shape could not be established"
		}
	}

	return total, ""
}

// modelDeploymentMemberDeclaredTotal 读取单个成员自身的组总数，若值不可能用于构建副本
// 则返回 false。
//
// 缺失该值不视为失败，而是按单成员副本读取；存在该值但不是有效规模时才视为失败。
// 前者对应引入该 annotation 之前创建的 Pod，后者则是本 operator 无法据此执行的无效声明。
func modelDeploymentMemberDeclaredTotal(member *core.Pod) (int, bool) {
	raw, present := member.Annotations[kueuepodconst.GroupTotalCountAnnotation]
	if !present {
		return 1, true
	}

	total, err := strconvx.Atoi[int](raw)
	if err != nil || total < 1 {
		return 0, false
	}

	return total, true
}

// modelDeploymentReplicaSeatReason 返回副本成员无法按席位区分的原因；若可区分则返回 ""。
//
// 仅对可确认的历史遗留单成员副本接受无席位信息的情况：渲染逻辑一贯会为每个成员写入
// 席位标签，因此不存在由本系统生成却缺少席位标签的多成员组。若存在席位标签但无法解析，
// 则属于缺陷，而非上述历史遗留情况。
func modelDeploymentReplicaSeatReason(members []*core.Pod, total int) string {
	seats := make(map[int]string, len(members))
	claimed, invalid := 0, 0
	for _, member := range members {
		index, state := modelDeploymentMemberSeat(member)
		switch state {
		case modelDeploymentSeatInvalid:
			invalid++

			continue
		case modelDeploymentSeatAbsent:

			continue
		}

		claimed++
		if holder, taken := seats[index]; taken {
			return "members " + holder + " and " + member.Name + " both claim seat " +
				strconvx.Itoa(index) + " of a replica declaring " + strconvx.Itoa(total) +
				" members, so the replica cannot be told apart from itself"
		}
		seats[index] = member.Name

		if index >= total {
			return "member " + member.Name + " claims seat " + strconvx.Itoa(index) +
				" of a replica declaring only " + strconvx.Itoa(total) +
				" members, so the replica cannot be told apart from itself"
		}
	}

	if invalid > 0 {
		return strconvx.Itoa(invalid) + " of " + strconvx.Itoa(len(members)) +
			" members declare a member index that is not one, so the replica cannot be told apart " +
			"from itself"
	}
	if claimed == 0 {
		if total == 1 && len(members) == 1 {
			return ""
		}

		return "no member declares which seat it holds, and a replica of " + strconvx.Itoa(total) +
			" members is a shape whose seats were always written, so the replica cannot be told " +
			"apart from itself"
	}
	if claimed != len(members) {
		return strconvx.Itoa(claimed) + " of " + strconvx.Itoa(len(members)) +
			" members declare which seat they hold and the rest do not, so the replica cannot be " +
			"told apart from itself"
	}

	return ""
}

// modelDeploymentSeatState 表示成员席位标签所携带的状态：有效席位、完全缺失或无效席位声明。
//
// 不使用 bool 类型，因为“缺失”与“无效”是两种不同的事实，不能混为一谈。缺失表明
// 该 Pod 创建于引入此标签之前；无效则表明存在本 operator 无法使用的错误声明。
type modelDeploymentSeatState int

const (
	modelDeploymentSeatValid modelDeploymentSeatState = iota
	modelDeploymentSeatAbsent
	modelDeploymentSeatInvalid
)

// modelDeploymentMemberSeat 读取成员所声明的席位索引。
//
// 它不同于 modelDeploymentPodMemberIndex，后者回答的是“引擎将该成员视作哪个索引”，
// 并在标签缺失时默认回退到主节点索引。该默认值在彼处正确但在本处有误：若在此处使用，
// 会使历史遗留副本中的每个成员都变成主节点的重复项。
func modelDeploymentMemberSeat(member *core.Pod) (int, modelDeploymentSeatState) {
	raw, present := member.Labels[modelDeploymentMemberIndexLabel]
	if !present {
		return 0, modelDeploymentSeatAbsent
	}

	index, err := strconvx.Atoi[int](raw)
	if err != nil || index < 0 {
		return 0, modelDeploymentSeatInvalid
	}

	return index, modelDeploymentSeatValid
}

// modelDeploymentReplicaExecution 表示构建副本当前所运行命令行的主体。
//
// 这是一个针对运行中 Pod 的判断。当前声明的 role 描述的是即将构建的副本，
// 与仍在提供服务的副本无关。
type modelDeploymentReplicaExecution int

const (
	// modelDeploymentExecutionUnreadable 表示无法归属命令行来源的副本，
	// 因为其中有成员未包含可供读取的引擎容器。
	modelDeploymentExecutionUnreadable modelDeploymentReplicaExecution = iota
	// modelDeploymentExecutionManaged 表示运行由本 operator 构建的命令行的副本。
	modelDeploymentExecutionManaged
	// modelDeploymentExecutionTakeover 表示运行由 role 完整提供命令行的副本，
	// 本 operator 未向其添加任何参数、环境变量或 connector。
	modelDeploymentExecutionTakeover
)

// modelDeploymentDeployedReplicaExecution 根据副本自身成员报告当前所运行命令行的构建方。它是纯函数。
//
// 判断依据是服务端口 annotation，与 group-forward 探针所依赖的相同：渲染器仅为运行
// operator 自有命令行的 role 写入该 annotation；而非 drain hook，因为 Elastic 实现会清除
// 其渲染的每个 Ray worker 上的 drain hook。
func modelDeploymentDeployedReplicaExecution(members []*core.Pod) (modelDeploymentReplicaExecution, string) {
	if len(members) == 0 {
		return modelDeploymentExecutionUnreadable,
			"the replica holds no members, so it has no running command line to read"
	}

	// 必须检查每一个成员。没有引擎容器的成员视为未读取，跳过它会导致
	// 仅凭恰好可读的成员来归属整个副本。
	built := false
	for _, member := range members {
		managed, ok := modelDeploymentMemberExecution(member)
		if !ok {
			return modelDeploymentExecutionUnreadable, "member " + member.Name + " carries no engine " +
				"container to read, so the replica's execution could not be established"
		}
		if member != members[0] && managed != built {
			return modelDeploymentExecutionUnreadable, "the replica's members disagree on who built " +
				"their command line, so its execution could not be established"
		}

		built = managed
	}

	if built {
		return modelDeploymentExecutionManaged, ""
	}

	return modelDeploymentExecutionTakeover, ""
}

// modelDeploymentMemberExecution 读取单个成员的命令行是否由本 operator 构建，
// 若成员不包含可供读取的引擎容器则返回 false。
func modelDeploymentMemberExecution(member *core.Pod) (managed, ok bool) {
	if _, found := modelDeploymentDrainContainerOf(member, modelDeploymentMainContainerName); !found {
		return false, false
	}

	return member.Annotations["prometheus.io/port"] != "", true
}
