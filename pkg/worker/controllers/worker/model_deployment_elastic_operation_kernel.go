package worker

import (
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// 期望宽度、Kueue 准入数、Ray 注册数和实际服务宽度各自回答不同的问题。
// 四个 Pod 通过准入时，Ray 可能尚未注册四个成员。每一层都独立记录数值或缺失的原因。

// elasticLayer 保存独立观测到的一层数据。
//
// Known 用来区分确实测得的零和未能取得的数值。前者能参与决策，后者须等待证据。
type elasticLayer struct {
	Known  bool
	Value  int
	Reason string
}

// knownLayer 构造带有确定观测值的一层。
func knownLayer(value int) elasticLayer { return elasticLayer{Known: true, Value: value} }

// unknownLayer 构造尚无可靠读数的一层，并记录原因。
func unknownLayer(reason string) elasticLayer { return elasticLayer{Reason: reason} }

// elasticFacts 汇集状态机可以使用的观测结果。它只包含数据，客户端、时钟和网络访问留给调用方。
// 相同记录与观测总会得出相同决定，测试可以直接核对决定。
type elasticFacts struct {
	// DeploymentUID 指明这些观测属于哪一个部署，防止把其他部署的数据用于当前操作。
	DeploymentUID types.UID
	// Generation 记录采集这些观测时的 spec 版本。
	Generation int64
	// Desired、Admitted、Ray 和 Effective 分别记录期望宽度、获准成员数、
	// Ray 注册情况和已证明的服务宽度。
	Desired   elasticLayer
	Admitted  elasticLayer
	Ray       elasticLayer
	Effective elasticLayer
	// AdmittedAllocation 记录真正获得完整加速卡分配的成员数。Pod 通过准入后，仍可能在等待分卡。
	AdmittedAllocation elasticLayer
	// WithdrawalConfirmed 表示服务侧已确认相关实例从流量分发中摘除。该事实必须由服务侧提供。
	WithdrawalConfirmed bool
	// NativeComplete 表示引擎已完成变更，并且达到本次目标宽度。
	NativeComplete bool
	// ScalingKnown 表示是否取得了引擎的应答，Scaling 表示它是否正在调整宽度。
	// 已有明确拒绝的命令只有在引擎确认空闲后才可重发，读不到标志时继续等待。
	ScalingKnown bool
	Scaling      bool
	// ForwardProven 表示引擎报告的新宽度下确实处理过推理请求，供状态机核实该宽度已经提供服务。
	ForwardProven bool
	// ActorFree 逐个记录已捕获的 worker UID 是否仍有 Ray actor。只要还有 actor，就要保留相应的 Pod。
	ActorFree map[types.UID]bool
	// ActorMappingKnown 表示本次读取是否获得了可用的 actor 到 Pod 映射。
	// 读取失败时无法据此判断哪些成员已空闲。
	ActorMappingKnown bool
	// ReleaseObserved 表示已看到这些成员的设备记账和调度配额都归还，删除请求本身只能证明已开始释放。
	ReleaseObserved bool
}

// elasticAction 表示状态机为当前证据选择的下一步动作。
type elasticAction string

const (
	// elasticActionHold 保留现场，并写明还缺哪项证据。重启后尚未收齐观测时通常会得到这个动作。
	elasticActionHold elasticAction = "Hold"
	// elasticActionRequestScale 指示首次发送调整宽度的命令。引擎明确拒绝后，
	// 由 elasticActionRetryScale 在记录允许的次数内重发。
	elasticActionRequestScale elasticAction = "RequestScale"
	// elasticActionRetryScale 在已记录明确拒绝且引擎确认空闲时允许重发。状态机决定是否还有额度，
	// 调用方根据时钟判断退避间隔是否已过。
	elasticActionRetryScale elasticAction = "RetryScale"
	// elasticActionAwaitNative 等待引擎完成宽度变更。
	elasticActionAwaitNative elasticAction = "AwaitNative"
	// elasticActionAwaitWithdrawal 等待服务侧确认摘除流量。
	elasticActionAwaitWithdrawal elasticAction = "AwaitWithdrawal"
	// elasticActionAwaitForward 等待真实推理请求在新宽度下得到处理。
	elasticActionAwaitForward elasticAction = "AwaitForward"
	// elasticActionRelease 释放已捕获且已经证实没有 actor 的 worker。
	elasticActionRelease elasticAction = "Release"
	// elasticActionComplete 结束这次宽度变更。
	elasticActionComplete elasticAction = "Complete"
)

// elasticDecision 同时给出下一步动作、对应的操作状态以及原因。
type elasticDecision struct {
	Action elasticAction
	State  elasticOperationState
	Reason string
}

// elasticHold 集中构造暂停决定，使所有等待路径都保留所处阶段并说明为什么要等。
func elasticHold(state elasticOperationState, format string, args ...any) elasticDecision {
	return elasticDecision{Action: elasticActionHold, State: state, Reason: fmt.Sprintf(format, args...)}
}

// 引擎明确拒绝后最多尝试五次。第 n 次拒绝到下一次尝试之间至少等待 min(15s × 2^(n−1), 4m)，
// 从一次调谐周期开始逐步翻倍。第五次明确拒绝后终止操作，报告引擎给出的原因。
const (
	elasticScaleMaxAttempts = 5
	elasticScaleRetryBase   = 15 * time.Second
	elasticScaleRetryCap    = 4 * time.Minute
)

// // [IMPORTANT: elastic EP]“纯函数大脑”做裁决
//
// decideElasticOperation 是弹性宽度状态机的决策入口。
//
// 这个纯函数只读取操作记录和本轮事实。调用方负责观测、计时并执行决定，再把新结果带回。
// 重启后同样可以用持久记录和新证据重建下一步。
//
// 按依赖顺序核对证据。部署或 generation 不符、容量尚未获准或落实、Ray 无法印证、
// 成员的 actor 状态未知或资源尚未确认归还时，都必须停在相应步骤。已经发出的命令单独进入观测路径。
func decideElasticOperation(op *elasticOperation, facts elasticFacts) elasticDecision {
	if err := op.validate(); err != nil {
		return elasticDecision{
			Action: elasticActionHold,
			State:  op.State,
			Reason: err.Error(),
		}
	}

	// 首先核对部署 UID，别的部署即使有完整数据也无法说明本次操作的结果。
	if facts.DeploymentUID != op.DeploymentUID {
		return elasticHold(op.State,
			"the facts are about deployment %s rather than %s", facts.DeploymentUID, op.DeploymentUID)
	}

	// spec 的 generation 已改变时，保留尚未解决的操作并进入重建流程。旧意图不能自动套用到新版本。
	if facts.Generation != op.Generation {
		return elasticHold(op.State,
			"the deployment is at generation %d while this operation is bound to %d",
			facts.Generation, op.Generation)
	}

	// 命令发出后先观察。调用超时、未收到答复或回执无法解析时都可能已经生效。
	// 只有持久记录中保留了引擎的明确拒绝，后续决策才会在有界重试与终止之间选择。
	//
	// 以操作阶段判断命令是否已发出，同时校验表示同一事实的布尔字段。两者矛盾时不能据此重新发送命令。
	if op.SentState() {
		return decideAfterCommand(op, facts)
	}

	return decideBeforeCommand(op, facts)
}

// decideBeforeCommand 决定何时可以首次请求引擎变更。扩容所需容量必须先准备好。
//
// 扩容前分别检查 Kueue 准入、实际整卡分配和 Ray 注册数是否达到目标。缺少任何一层就等待，
// 避免引擎收到集群尚无能力兑现的请求。
//
// 缩容要移出已有成员，无需为目标宽度申请新增配额。命令仍须对应记录里的目标，并先取得服务摘除的证明。
func decideBeforeCommand(op *elasticOperation, facts elasticFacts) elasticDecision {
	// 只有读到了 spec 期望的宽度，并且它与操作记录中的目标相同，才能按该记录向引擎发命令。
	if !facts.Desired.Known {
		return elasticHold(op.State, "the desired width is unknown: %s", facts.Desired.Reason)
	}
	if facts.Desired.Value != op.Width.Target {
		return elasticHold(op.State, "the spec now wants width %d rather than %d",
			facts.Desired.Value, op.Width.Target)
	}

	// 缩容前先摘除服务流量并等待确认，避免引擎减少成员时仍有新请求被路由过来。
	if !op.Width.Upward() {
		if !facts.WithdrawalConfirmed {
			return elasticDecision{
				Action: elasticActionAwaitWithdrawal,
				State:  op.State,
				Reason: "the serving side has not confirmed the narrowing withdrew",
			}
		}

		return elasticDecision{
			Action: elasticActionRequestScale,
			State:  elasticStateCommandSent,
			Reason: fmt.Sprintf("the engine was not yet asked to move from %d to %d",
				op.Width.Old, op.Width.Target),
		}
	}

	if !facts.Admitted.Known {
		return elasticHold(op.State, "admitted width is unknown: %s", facts.Admitted.Reason)
	}
	if facts.Admitted.Value < op.Width.Target {
		return elasticHold(op.State, "only %d workers are admitted and %d are wanted",
			facts.Admitted.Value, op.Width.Target)
	}
	if !facts.AdmittedAllocation.Known {
		return elasticHold(op.State, "admitted allocation is unknown: %s", facts.AdmittedAllocation.Reason)
	}
	if facts.AdmittedAllocation.Value < op.Width.Target {
		return elasticHold(op.State, "only %d workers hold an allocation and %d are wanted",
			facts.AdmittedAllocation.Value, op.Width.Target)
	}
	if !facts.Ray.Known {
		return elasticHold(op.State, "the ray world is unknown: %s", facts.Ray.Reason)
	}
	if facts.Ray.Value < op.Width.Target {
		return elasticHold(op.State, "the ray world holds %d members and %d are wanted",
			facts.Ray.Value, op.Width.Target)
	}

	return elasticDecision{
		Action: elasticActionRequestScale,
		State:  elasticStateCommandSent,
		Reason: fmt.Sprintf("the engine was not yet asked to move from %d to %d",
			op.Width.Old, op.Width.Target),
	}
}

// decideAfterCommand 为已经发出命令的操作选择下一步。它检查生效或退场证据，
// 只有已记录的明确拒绝才进入专门的重试判断。
func decideAfterCommand(op *elasticOperation, facts elasticFacts) elasticDecision {
	if op.Width.Upward() {
		return decideUpwardAfterCommand(op, facts)
	}

	// 已到 Released 时，成员退场的前置检查已完成。此后只需观察资源是否归还，避免再次进入删除流程。
	if op.State == elasticStateReleased {
		return elasticReleaseComplete(op, facts)
	}

	return decideDownwardAfterCommand(op, facts)
}

// elasticScaleRefusalOutcome 将已记录的拒绝转为重试或最终放弃。先检查原命令是否已生效，再判断拒绝，
// 避免重发实际已成功的命令。必须确认引擎空闲并且还有尝试额度，具体重发时间由调用方判定。
func elasticScaleRefusalOutcome(op *elasticOperation, facts elasticFacts) (elasticDecision, bool) {
	if op.ScaleLastError == "" {
		return elasticDecision{}, false
	}
	if !facts.ScalingKnown {
		return elasticHold(op.State,
			"the engine did not answer whether it is scaling, so the refused command is not re-issued on a guess; the recorded refusal: %s",
			op.ScaleLastError), true
	}
	if facts.Scaling {
		return elasticHold(op.State,
			"the engine is still scaling, so the refused command waits for it to settle; the recorded refusal: %s",
			op.ScaleLastError), true
	}
	if max(op.ScaleAttempts, 1) < elasticScaleMaxAttempts {
		return elasticDecision{
			Action: elasticActionRetryScale,
			State:  op.State,
			Reason: fmt.Sprintf("the engine refused the scale command to width %d; the recorded refusal: %s",
				op.Width.Target, op.ScaleLastError),
		}, true
	}
	return elasticHold(elasticStateAbandoned, "%s", elasticScaleRefusalMessage(op)), true
}

// elasticScaleRefusalMessage 在最终报错中展示旧宽度、目标宽度、已耗尽的尝试次数和引擎最后一次拒绝的原文。
func elasticScaleRefusalMessage(op *elasticOperation) string {
	return fmt.Sprintf(
		"the scale command from width %d to %d was refused %d times and its retry bound is spent; the last refusal: %s",
		op.Width.Old, op.Width.Target, max(op.ScaleAttempts, 1), op.ScaleLastError,
	)
}

// decideUpwardAfterCommand 等待引擎完成扩容，并核实目标宽度上的真实推理。扩容无需移除成员，
// 证据齐全就能完成。
func decideUpwardAfterCommand(op *elasticOperation, facts elasticFacts) elasticDecision {
	if !facts.NativeComplete {
		if decision, refused := elasticScaleRefusalOutcome(op, facts); refused {
			return decision
		}
		return elasticDecision{
			Action: elasticActionAwaitNative,
			State:  elasticStateAwaitingWidth,
			Reason: "the engine has not reported the new width yet",
		}
	}
	if !facts.Effective.Known {
		return elasticHold(elasticStateNativeDone, "the effective width is unknown: %s", facts.Effective.Reason)
	}
	if facts.Effective.Value != op.Width.Target {
		return elasticHold(elasticStateNativeDone, "the engine reports width %d rather than %d",
			facts.Effective.Value, op.Width.Target)
	}
	// 引擎报告新宽度后，还要用在该宽度下实际完成的前向推理证明服务可用。缩容也使用同一要求。
	if !facts.ForwardProven {
		return elasticDecision{
			Action: elasticActionAwaitForward,
			State:  elasticStateNativeDone,
			Reason: "no request has been served at the new width yet",
		}
	}

	return elasticDecision{
		Action: elasticActionComplete,
		State:  elasticStateCompleted,
		Reason: fmt.Sprintf("the engine serves at width %d", op.Width.Target),
	}
}

// decideDownwardAfterCommand 按顺序核实引擎完成、目标宽度、真实推理、服务流量已摘除，
// 以及待退场成员均无 actor。全部满足后才允许释放。
func decideDownwardAfterCommand(op *elasticOperation, facts elasticFacts) elasticDecision {
	if !facts.NativeComplete {
		if decision, refused := elasticScaleRefusalOutcome(op, facts); refused {
			return decision
		}
		return elasticDecision{
			Action: elasticActionAwaitNative,
			State:  elasticStateAwaitingWidth,
			Reason: "the engine has not reported the new width yet",
		}
	}
	if !facts.Effective.Known {
		return elasticHold(elasticStateNativeDone, "the effective width is unknown: %s", facts.Effective.Reason)
	}
	if facts.Effective.Value != op.Width.Target {
		return elasticHold(elasticStateNativeDone, "the engine reports width %d rather than %d",
			facts.Effective.Value, op.Width.Target)
	}
	// 在引擎报告的宽度上执行真实请求，才有证据表明新的并行组能够提供服务。
	if !facts.ForwardProven {
		return elasticDecision{
			Action: elasticActionAwaitForward,
			State:  elasticStateNativeDone,
			Reason: "no request has been served at the new width yet",
		}
	}
	if !facts.WithdrawalConfirmed {
		return elasticDecision{
			Action: elasticActionAwaitWithdrawal,
			State:  elasticStateWithdrawn,
			Reason: "the serving side has not confirmed the narrowing withdrew",
		}
	}

	if reason := elasticRetirementRefusal(op, facts); reason != "" {
		return elasticHold(elasticStateWithdrawn, "%s", reason)
	}

	return elasticDecision{
		Action: elasticActionRelease,
		State:  elasticStateReleased,
		Reason: fmt.Sprintf("%d captured workers are proven actor-free and may be released",
			len(op.retractableWorkers())),
	}
}

// elasticRetirementRefusal 检查捕获的成员是否都可退场，若有一个不满足条件就返回原因。
//
// 先确认 actor 到 Pod 的映射可读，再逐个检查。映射缺失时不能认定成员无 actor，
// 否则可能删掉仍承载并行组状态的 Pod。
func elasticRetirementRefusal(op *elasticOperation, facts elasticFacts) string {
	if !facts.ActorMappingKnown {
		return "the runtime's actor mapping could not be read, so no worker is proven actor-free"
	}

	for _, worker := range op.retractableWorkers() {
		if worker.UID == op.Master.UID {
			return fmt.Sprintf("worker %s is the master and is never retirable", worker.Name)
		}
		actorFree, known := facts.ActorFree[worker.UID]
		if !known {
			return fmt.Sprintf("worker %s has no actor evidence, because nothing mapped it", worker.Name)
		}
		if !actorFree {
			return fmt.Sprintf("worker %s still holds an actor", worker.Name)
		}
	}

	return ""
}

// elasticReleaseComplete 是最后一步，等待实际看到资源归还。
//
// Pod 消失只能证明删除已经发生。需要账本与队列配额共同确认资源已释放，才能将操作标为完成。
func elasticReleaseComplete(_ *elasticOperation, facts elasticFacts) elasticDecision {
	if !facts.ReleaseObserved {
		return elasticHold(elasticStateReleased,
			"the captured workers have not been seen released from the ledger and the quota")
	}

	return elasticDecision{
		Action: elasticActionComplete,
		State:  elasticStateCompleted,
		Reason: "the narrowing is released, observed and complete",
	}
}
