// 退场预留（retirement reservation）：唯一允许删除副本的协议，也是在其运行期间
// 阻止其他所有路径触碰该副本的门控。
//
// 一条预留是对单个副本的持久化决策。它由准入方写入 status，并由本文件负责
// 推进直至结束。本协议中绝不能出错的所有判断均以三值结果和 hold 表达：对于可能
// 仍在提供服务的副本，绝不会仅凭一次未能到达的观测结果就将其删除。
//
// 两个独立决策，而非合二为一。Hold 是拦截谓词：无论哪条路径请求，本轮调谐中
// 只要删除目标指向被捕获成员的 UID 就必须拒绝。DeletesAllowed 则是协议自身
// 提交删除的许可。二者分开是因为它们回答的是不同的问题，若合为一个标志，status 中
// 将无法区分这两种情况：因副本被预留而遭拒的路径，看起来会与协议自身正在持有
// 的路径完全一致，运维人员查看 status 时将无法分辨究竟发生了哪一种情况。
//
// 当不存在任何预留时，此处逻辑一概不执行。计划为空、Hold 为 false，下方的每条
// 删除路径都按本文件出现之前的既有方式运行。这一性质必须在每条路径上切实成立
// 而非口头宣称，因为正是它保证向控制器引入该协议时的安全性。
//
// 各状态是水平状态（level），而非单向步骤。每次调谐都会从持久化状态重新进入，
// 并决定保持、推进或中止，因此即便某轮调谐从未获知此前发生过什么，也能做出与
// 知情轮次相同的决策。没有任何操作是因为上一轮说过要做才执行的，控制器重启后
// 也只是重新进入当前状态而非重新决策，这正是持久化 phaseStartedAt 的意义所在。
package worker

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"time"

	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	kubeapistatus "gpustack.ai/gpustack/pkg/kubeapistatus"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/utils/strconvx"
)

// 协议的预算时间已固化。每个阶段会在全局截止时间与从 phaseStartedAt 起算的
// 自身预算时间二者中的较早者结束，从而确保没有阶段会超出操作总时限，控制器重启
// 也不会延长任何阶段。
//
// 这些预算不同于 PreStop hook 中的预算。PreStop hook 自带 settle 与 exit 预留时间
// （modelDeploymentDrainSettleSeconds 与 modelDeploymentDrainExitSeconds），在收到
// SIGTERM 时运行于容器内部；而这里的预算运行在控制器中，针对的是尚未触碰的副本。
// 两者属于完全独立的机制，将其中任一数值误读为另一方的数值都会导致静默错误，
// 因此它们在此处单独声明，而非从 hook 处借用。
const (
	// modelDeploymentRetirementWithdrawalBudget 是分为两步执行的单个摘除（withdrawal）阶段
	// 的预算：先将目标移出流量选择，随后读取确认其不再服务任何请求的响应。之所以共用
	// 一个预算，是因为规范仅定义了一个，若拆成两个会导致组在未受流量的状态多停留
	// 一倍时间，却并未带来任何额外的新决策。
	modelDeploymentRetirementWithdrawalBudget = 30 * time.Second

	// modelDeploymentRetirementDrainBudget 是协议在所有成员仍保持运行的情况下，
	// 等待引擎自身在途请求队列降至零的最长时间。
	modelDeploymentRetirementDrainBudget = 240 * time.Second

	modelDeploymentRetirementDisaggregatedGraceWindow = 60 * time.Second

	// modelDeploymentRetirementSettleBudget 是协议在执行删除后等待加速器与配额
	// 释放的最长时间；这属于状态收敛过程，而非一次决策。
	modelDeploymentRetirementSettleBudget = 30 * time.Second

	// modelDeploymentRetirementOverallBudget 限定整个退场操作的总体时限。
	modelDeploymentRetirementOverallBudget = 300 * time.Second
)

// modelDeploymentRetirementRetryAnnotation 是重试已中止预留的显式指令。它是显式指令而非定时器：
// 中止状态在收到再次请求之前始终保持终止，从而避免无法排空的副本在每次调谐时被反复删除。
//
// 注解值格式为 roleName:replicaOrdinal:opaqueRetryToken。token 按契约保持不透明：跨部署无法
// 证明其来源，因此不解析出处，只要出现新字符串即视为新的重试请求。
const modelDeploymentRetirementRetryAnnotation = "modeldeployment.gpustack.ai/retirement-retry"

// modelDeploymentRetirementEncodeWithdrawn 是 encoder 副本在等待 Draining 窗口耗尽期间携带的原因文案。
const modelDeploymentRetirementEncodeWithdrawn = "the target runs the Encode role, which exports no " +
	"in-flight queue this protocol can read; it is withdrawn from discovery and leaving on the window"

// modelDeploymentRetirementPlan 表示单轮调谐对当前唯一在途预留所评估出的结果。
//
// 零值计划代表无预留计划，也是没有操作在运行时所有删除路径看到的状态：
// Hold 为 false，DeletesAllowed 为 true，没有任何对象被拦截。
type modelDeploymentRetirementPlan struct {
	// Reservation 是本轮调谐结束时的状态，将随其余 status 一并持久化。无论是当前
	// 没有任何操作，还是刚完成的操作刚刚被清除，该字段都为 nil，二者通过 Clear
	// 字段加以区分。
	Reservation *workercore.ModelDeploymentRetirementStatus

	// Target 是预留所指定的副本，按当前实际存在的 Pod 解析得到。
	Target modelDeploymentReplicaView

	// held 是预留所声明的成员 UID 集合。它与 Target 分开存放，使调用路径无需重建分组即可查询单个
	// Pod 是否被持有，而这正是每个删除点实际需要判断的问题。
	held sets.Set[types.UID]

	// Hold 表示本轮协调中是否必须拒绝针对被持有 UID 的删除操作。
	Hold bool

	// DeletesAllowed 是协议自身允许提交删除的权限标志。在 Deleting 和 Settling 阶段为 true，而在
	// 协议仍处于撤出或排空阶段时为 false。
	DeletesAllowed bool

	// Clear 表示已完成的操作已结束，应当移除该预留。
	Clear bool

	// InFlight 表示操作仍在运行中，因此需要再次进入协调流程以推进它。对于仅在等待队列清空的
	// 副本，没有其他事件会唤醒该部署。
	InFlight bool

	// Deletes 是协议本身在本轮需要删除的目标。在所有其他状态下均为空，包括 DeletesAllowed 为
	// true 但删除请求已在先前轮次发出的情况。
	Deletes []core.Pod
}

// holds 报告某 Pod 是否为当前操作的目标之一。
func (p modelDeploymentRetirementPlan) holds(uid types.UID) bool {
	return p.Hold && p.held.Has(uid)
}

// names 报告预留是否按名称声明了该副本。
//
// 它针对的是槽位而非成员，这一区分在滚动更新时至关重要。协议按 UID 持有单个 Pod，因此如果
// 滚动更新淘汰某个副本，它只会删除未被持有的成员——将按副本的决策变成局部删除，导致
// 淘汰操作面对一个其他成员已被滚动更新删掉的副本。被要求对已预留副本执行操作的滚动更新
// 必须将其整体跳过。
func (p modelDeploymentRetirementPlan) names(role string, ordinal int) bool {
	return p.InFlight && p.Reservation != nil && p.Reservation.RoleName == role &&
		int(p.Reservation.ReplicaOrdinal) == ordinal
}

// planModelDeploymentRetirement 以电平方式评估当前预留，并返回本轮应当持久化的状态以及允许
// 执行的操作。
//
// 无预留的轮次既不是错误也不属于被阻止状态。它会返回零值计划，所有调用方都会将其视为
// “照常继续执行”。
func (r *ModelDeploymentReconciler) planModelDeploymentRetirement(
	ctx context.Context, md *workercore.ModelDeployment, pods []core.Pod,
) *modelDeploymentRetirementPlan {
	reservation := md.Status.Retirement
	if reservation == nil {
		return &modelDeploymentRetirementPlan{DeletesAllowed: true}
	}

	plan := &modelDeploymentRetirementPlan{
		Reservation:    reservation.DeepCopy(),
		DeletesAllowed: true,
		InFlight:       true,
	}
	plan.Target, plan.held = modelDeploymentRetirementTarget(reservation, pods)

	// 已完成的操作直接清理，且这是本轮对其执行的唯一动作。
	// 不会把预留作为墓碑留在对象上，因为滞留的预留会持续拒绝健康谓词并占住空槽位。
	if reservation.State == workercore.ModelDeploymentRetirementStateCompleted {
		plan.Clear = true
		plan.InFlight = false

		return plan
	}

	// 进入 Deleting 及之后阶段，目标缺失意味着删除已经落地而非异常丢失。这些阶段本身就在等待目标
	// 消失，若把目标缺失当成外部终止，就会中止刚刚完成的操作并错误记录一次并未发生的保留。
	if reservation.State != workercore.ModelDeploymentRetirementStateAdmitted &&
		reservation.State != workercore.ModelDeploymentRetirementStateDisqualified &&
		reservation.State != workercore.ModelDeploymentRetirementStateWithdrawing &&
		reservation.State != workercore.ModelDeploymentRetirementStateDraining &&
		reservation.State != workercore.ModelDeploymentRetirementStateAborted {
		// 从 Deleting 起保持 Hold。操作已提交删除并依赖待删分组继续受本协议管辖，整个 settle 期间
		// 其他路径不得介入——尤其是常规收敛逻辑在同一轮也能看到这些 Pod，若放开 Hold，多余副本清理
		// 规则会把目标连同其他不再声明的副本一起收走。退场按成员推进删除，而常规收敛按副本决策，两者
		// 正好在此阶段冲突。
		plan.Hold = true
		// 透传调用方 context：已取消的调谐未读取服务端状态，绝不能改用新的 background context
		// 继续推进删除后的观测。
		r.advanceModelDeploymentRetirementPastDeletion(ctx, md, plan)

		return plan
	}

	// 目标已经消失，或已进入非删除途径唯一可能的终态（被 kubelet 驱逐或引擎自行退出，终态 Pod
	// 不会再运行）。此时已无流量可撤出、无队列可排空，继续持有只会让中止操作挂在一个已死的 Pod 上。
	// 因此记录实际原因并释放目标，交由常规终态 Pod 路径完成后续清理。
	//
	// 终态沿用 Aborted 并通过 reason 说明原因（冻结状态集中没有单独枚举），同时解除 Hold，
	// 因为已无法保留的目标不应继续被持有。
	if plan.held.Len() == 0 || modelDeploymentRetirementTargetTerminal(plan) {
		plan.Reservation.State = workercore.ModelDeploymentRetirementStateAborted
		plan.Reservation.Reason = "the target was terminated outside this operator; " +
			"nothing could be retained"
		plan.Hold = false
		plan.InFlight = false

		return plan
	}

	// 从此处开始操作持有目标。在提交删除之前的所有状态（含 Aborted）均保持 Hold：中止操作的本意
	// 就是刻意保留容量，若后续缩容把被保留的副本直接删掉，保留语义便不复存在。
	plan.Hold = true

	// 若 spec 重新声明了该 ordinal 且同 UID 成员均健康，则收编而非继续退场。处于 Aborted 状态时
	// 扩容再次包含该 ordinal 意味着副本仍被需要，取消操作并交由常规收敛保留现有 Pod 即可。
	// 同 ordinal 上出现不同 UID 属于新分组，已在上方按外部终止处理，不会错误收编替代副本。
	if adopted, retry := r.adoptsModelDeploymentRetirement(ctx, md, plan, pods); adopted {
		plan.Clear = true
		plan.Hold = false
		plan.InFlight = false

		return plan
	} else if retry {
		plan.InFlight = true

		return plan
	}

	// 重试指令是显式操作，必须在进入状态机之前先消费，使从 Aborted 发起的重试回到 Admitted，
	// 而不是直接重跑先前中止的那一步。
	if r.consumeModelDeploymentRetry(ctx, md, plan) {
		// 消费逻辑会按固定顺序先写状态再清注解，本轮直接返回并在下一轮重新进入，使顺序可观测而非假设。
		plan.InFlight = true

		return plan
	}

	switch reservation.State {
	case workercore.ModelDeploymentRetirementStateAborted:
		// 中止状态在收到新意图前保持终态，防止无法排空的副本在每次调谐时反复触发删除。
		plan.InFlight = false

	case workercore.ModelDeploymentRetirementStateAdmitted:
		r.advanceModelDeploymentRetirementToDisqualified(ctx, md, plan)

	case workercore.ModelDeploymentRetirementStateDisqualified:
		r.advanceModelDeploymentRetirementToWithdrawing(ctx, md, plan)

	case workercore.ModelDeploymentRetirementStateWithdrawing:
		r.advanceModelDeploymentRetirementToDraining(ctx, md, plan)

	case workercore.ModelDeploymentRetirementStateDraining:
		r.advanceModelDeploymentRetirementToDeleting(ctx, md, plan)

	case workercore.ModelDeploymentRetirementStateDeleting:
		r.advanceModelDeploymentRetirementToSettling(ctx, md, plan)

	case workercore.ModelDeploymentRetirementStateSettling:
		r.advanceModelDeploymentRetirementToCompleted(ctx, md, plan)
	}

	return plan
}

// advanceModelDeploymentRetirementPastDeletion 驱动 Deleting 及之后的状态，这些状态以目标消失为前提。
//
// 进入时 Hold 已经生效，确保删除进行期间协议外部的路径不会插手该目标。
func (r *ModelDeploymentReconciler) advanceModelDeploymentRetirementPastDeletion(
	ctx context.Context, md *workercore.ModelDeployment, plan *modelDeploymentRetirementPlan,
) {
	if err := ctx.Err(); err != nil {
		plan.Clear = false
		plan.Reservation.Reason = fmt.Sprintf("the pass was cancelled, so the release is not observed: %v", err)

		return
	}
	switch plan.Reservation.State {
	case workercore.ModelDeploymentRetirementStateDeleting:
		r.advanceModelDeploymentRetirementToSettling(ctx, md, plan)
	case workercore.ModelDeploymentRetirementStateSettling:
		r.advanceModelDeploymentRetirementToCompleted(ctx, md, plan)
	}
}

// commitModelDeploymentRetirement 发出操作对应的删除请求，它是此控制器中唯一凭协议自身权限
// 删除副本的位置。
//
// 每次删除都携带其 UID 作为前置条件，以防止同名替代 Pod 被误删。预留正是按 UID 持有
// 成员，才能强制执行这一约束；若删除时不带该前置条件，UID 集合就只是建议性的——如果在
// 制定计划与发起调用之间原目标被删且同名替代 Pod 已创建，删除请求会在替代 Pod 上成功，
// 从而在原 Pod 仍在运行时就错误报告操作已完成。
//
// Workload 随 Pod 一并作为同一决策删除，原因与本文件其他各处相同：它是释放 Kueue 在组内
// 每个成员上所持 finalizer 的关键，若只删 Pod 而不删 Workload，副本将永远停留在
// Terminating 状态。
func (r *ModelDeploymentReconciler) commitModelDeploymentRetirement(
	ctx context.Context, md *workercore.ModelDeployment, plan *modelDeploymentRetirementPlan,
	pods []core.Pod,
) error {
	logger := ctrllog.FromContext(ctx)
	if !plan.DeletesAllowed || len(plan.Deletes) == 0 {
		return nil
	}

	for i := range plan.Deletes {
		pod := &plan.Deletes[i]
		uid := pod.UID
		if err := r.Client.Delete(ctx, pod,
			ctrlcli.Preconditions{UID: &uid}); err != nil && !kerrors.IsNotFound(err) {
			return fmt.Errorf("delete retired replica %s: %w", pod.Name, err)
		}
		logger.Info("deleting retired replica member", "pod", pod.Name, "state", plan.Reservation.State)
	}

	return r.commitModelDeploymentRetirementWorkload(ctx, md, plan, pods)
}

// commitModelDeploymentRetirementWorkload 删除原本会保留被淘汰成员 finalizer 的 Workload，
// 这是删除流程中更为微妙的另一半。
//
// Workload 是总开关且作用域为整个组。Kueue 会挂起受管组中被删除的 Pod，直到该组的
// Workload 被删除，而这里的组对应的是角色的服务集合，而非单个副本：一个角色的四个副本
// 可以共享同一个 Workload。如果为了淘汰一个副本而删除它，会同时释放所有四个副本的
// finalizer，导致整套部署随之被连带下线。上面的 Pod 删除是逐成员且安全的；而此处的
// 删除并非如此，这正是它必须先做判断再决定是否发出的原因。
//
// 因此，仅当 Workload 在目标之外不拥有任何成员时才将其删除。若一个副本本身就是整个组，
// 它下线时其 Workload 也一并删除，因为之后该组已空无一物。若一个副本只是多个副本之一，
// 则保留其 Workload：该组仍在提供服务，其余成员继续在描述它们的 Workload 下保持
// finalizer，而被淘汰的成员会在下次不包含它地重新编排该组时被释放。
//
// 该判断基于部署中当前存活的 Pod，而非 Workload 自身的状态，从而避免将无法从自身对象
// 读取成员信息的 Workload 误判为不描述其他成员。无法证明整个组都属于目标，就意味着不执行
// 删除。
func (r *ModelDeploymentReconciler) commitModelDeploymentRetirementWorkload(
	ctx context.Context, md *workercore.ModelDeployment, plan *modelDeploymentRetirementPlan,
	pods []core.Pod,
) error {
	workloads, err := r.findModelDeploymentGroupWorkloads(ctx, md, plan.Deletes)
	if err != nil {
		return err
	}

	target := sets.New[types.UID]()
	for i := range plan.Deletes {
		target.Insert(plan.Deletes[i].UID)
	}
	outsiders := sets.New[types.UID]()
	for i := range pods {
		if !target.Has(pods[i].UID) {
			outsiders.Insert(pods[i].UID)
		}
	}

	for _, workload := range workloads {
		if modelDeploymentWorkloadOwnsAny(workload, outsiders) {
			continue
		}

		uid := workload.UID
		if err = r.Client.Delete(ctx, workload,
			ctrlcli.Preconditions{UID: &uid}); err != nil && !kerrors.IsNotFound(err) {
			return fmt.Errorf("delete retired replica's workload %s: %w", workload.Name, err)
		}
	}

	return nil
}

// persistModelDeploymentRetirement 写入本轮达到的状态；如果本轮达到的状态与已有状态一致，
// 则不执行写入。
//
// 它是独立的一次写入，而不是在本轮状态同步前直接修改对象字段，原因是状态同步会比较派生
// 状态与已观测状态以决定是否写入。如果就地赋值该字段，在执行比较时它就会等于自身，导致
// 操作进度永远无法到达 API server，每一轮都会退回到第一轮所处的初始状态。未发生任何
// 变更的轮次不会写入，这正是防止控制器在每个 Pod 事件上持续重写对象的原因。
//
// 无预留的轮次从不写入：两边均为 nil，比较结果即已足够。
func (r *ModelDeploymentReconciler) persistModelDeploymentRetirement(
	ctx context.Context, md *workercore.ModelDeployment,
	previous *workercore.ModelDeploymentRetirementStatus, plan *modelDeploymentRetirementPlan,
) error {
	next := plan.Reservation
	if plan.Clear {
		next = nil
	}

	return r.writeModelDeploymentRetirement(ctx, md, previous, next)
}

// writeModelDeploymentRetirement 仅持久化预留字段，不修改其他内容。
//
// 三个早期淘汰写入操作具有相同的形态，若不共用同一个写入器，就会共享同一种失败模式：
// 全量 status 写入会把整个带类型的 status——包括对象恰好携带的所有字段——发回 API server，
// 而服务端会对所有字段进行校验。由旧版本写入的已存储对象可能在本操作从不触碰的字段中
// 携带当前 schema 不接受的枚举值，进而导致本意仅为“将操作推进一个阶段”的写入因不相干
// 的内容而被拒绝。仅 patch retirement 字段则只发送本操作声明的内容。
//
// 该 patch 采用乐观并发控制：它携带本轮读取到的 resourceVersion，因此在写入期间已发生
// 变更的存储对象会返回 Conflict，而非被静默覆盖。无操作时完全不写入；清除预留时则会
// 显式写入 null，因为省略该字段的 patch 会保留原有值。
//
// 对象会根据服务端响应更新，使同一轮中后续的写入携带本次产生的版本，而非本轮开始时的
// 版本。
func (r *ModelDeploymentReconciler) writeModelDeploymentRetirement(
	ctx context.Context, md *workercore.ModelDeployment,
	previous, next *workercore.ModelDeploymentRetirementStatus,
) error {
	if kubemeta.DeepEqual(previous, next) {
		return nil
	}

	// base 是服务端最后一次持有的对象状态，这就是调用方将新旧两侧都传入的原因。如果在调用方
	// 赋完新值之后再拷贝 base，base 中就已经包含了新值，两者之间的 diff 将为空。
	base := md.DeepCopy()
	base.Status.Retirement = previous
	candidate := base.DeepCopy()
	candidate.Status.Retirement = next

	optimistic := ctrlcli.MergeFromWithOptions(base, ctrlcli.MergeFromWithOptimisticLock{})
	if err := r.Client.Status().Patch(ctx, candidate, optimistic); err != nil {
		return err
	}

	// patch 会将服务端响应解码到 candidate 中，因此本次写入产生的版本即为本轮下一次写入所
	// 携带的版本。这里不重新读回对象，因为那会要求客户端执行一次本写入器原本不依赖的 Get。
	//
	// 只有在服务端接受之后才将 candidate 拷贝回原对象。写入失败时，调用方的对象仍描述
	// 服务端当前持有的状态，本轮其余逻辑将据此判断令牌是否已被消耗以及是否允许执行删除。
	md.ResourceVersion = candidate.ResourceVersion
	md.Status = candidate.Status

	return nil
}

// modelDeploymentRetirementTarget 按 UID 将预留解析为当前存活的目标副本。
//
// 必须按 UID 而非仅按 (role, ordinal) 解析：操作期间该槽位可能被滚动更新或人工重建填充，
// 若仅按 ordinal 查找就会误指替代副本并将其删除。UID 唯一标识单个对象，已被替换的目标在此解析为空，
// 进而按外部终止处理而不会静默转移目标。
func modelDeploymentRetirementTarget(
	reservation *workercore.ModelDeploymentRetirementStatus, pods []core.Pod,
) (modelDeploymentReplicaView, sets.Set[types.UID]) {
	claimed := sets.New[string](reservation.TargetMemberUIDs...)
	held := sets.New[types.UID]()

	view := modelDeploymentReplicaView{Role: reservation.RoleName, Ordinal: int(reservation.ReplicaOrdinal)}
	for i := range pods {
		pod := &pods[i]
		if pod.DeletionTimestamp != nil || !claimed.Has(string(pod.UID)) {
			continue
		}

		// UID 集合是权威依据，同时从标签填充视图以便协议判断槽位与完整性；即使成员标签与预留不一致，
		// 只要 UID 命中依然会被持有保护。
		if ordinal, ok := modelDeploymentPodOrdinal(pod); ok {
			view.Seated = true
			view.Ordinal = ordinal
		}
		view.Members = append(view.Members, pod)
		held.Insert(pod.UID)
	}

	return view, held
}

// advanceModelDeploymentRetirementToDisqualified 将操作从 Admitted 推进到 Disqualified，等待目标离开端点选择集。
//
// 此处只观测资格标签是否已移除，而不直接修改标签：常规收敛中的健康谓词 NoPlannedRetirement 分支
// 会对被预留副本自动摘除该标签，避免同一标签键出现两个写入方。
func (r *ModelDeploymentReconciler) advanceModelDeploymentRetirementToDisqualified(
	_ context.Context, _ *workercore.ModelDeployment, plan *modelDeploymentRetirementPlan,
) {
	// 不在此处重写阶段开始时间，因为目标仍携带资格标签时会多次轮询 Admitted，重写会不断重置预算。
	now := r.modelDeploymentNow()
	if modelDeploymentRetirementBudgetSpent(plan.Reservation, modelDeploymentRetirementWithdrawalBudget, now) {
		r.abortModelDeploymentRetirement(plan, now,
			"the withdrawal budget was exhausted before the target left selection")

		return
	}

	for _, member := range plan.Target.Members {
		if member.Labels[modelDeploymentLabelKeyEndpointEligible] == modelDeploymentEndpointEligibleValue {
			plan.Reservation.Reason = "the target is still in endpoint selection"

			return
		}
	}

	plan.Reservation.State = workercore.ModelDeploymentRetirementStateDisqualified
	plan.Reservation.Reason = "the target has left endpoint selection"
}

// advanceModelDeploymentRetirementToWithdrawing 将操作从 Disqualified 推进到 Withdrawing，
// 等待路由器的实际服务观测确认目标不再承接流量。
//
// Withdrawing 与 Disqualified 共享同一摘除阶段预算，沿用既有 phaseStartedAt。
func (r *ModelDeploymentReconciler) advanceModelDeploymentRetirementToWithdrawing(
	ctx context.Context, md *workercore.ModelDeployment, plan *modelDeploymentRetirementPlan,
) {
	now := r.modelDeploymentNow()
	if modelDeploymentRetirementBudgetSpent(plan.Reservation, modelDeploymentRetirementWithdrawalBudget, now) {
		r.abortModelDeploymentRetirement(plan, now,
			"the withdrawal budget was exhausted before the target stopped serving")

		return
	}

	residual, held := r.observeModelDeploymentRetirementResidual(ctx, md, plan.held)
	if held {
		plan.Reservation.Reason = residual

		return
	}

	plan.Reservation.State = workercore.ModelDeploymentRetirementStateWithdrawing
	plan.Reservation.Reason = "the target is out of selection and nothing serves it"
}

// advanceModelDeploymentRetirementToDraining 将操作推进到 Draining，所有成员保持运行并开始读取引擎队列。
func (r *ModelDeploymentReconciler) advanceModelDeploymentRetirementToDraining(
	_ context.Context, _ *workercore.ModelDeployment, plan *modelDeploymentRetirementPlan,
) {
	now := r.modelDeploymentNow()
	plan.Reservation.PhaseStartedAt = meta.NewTime(now)

	if modelDeploymentRetirementBudgetSpent(plan.Reservation, modelDeploymentRetirementDrainBudget, now) {
		r.abortModelDeploymentRetirement(plan, now,
			"the drain budget was exhausted before the target left deletion")

		return
	}

	plan.Reservation.State = workercore.ModelDeploymentRetirementStateDraining
	plan.Reservation.Reason = "reading the target's in-flight queues with every member retained"
}

// advanceModelDeploymentRetirementToDeleting 读取各成员队列，确认全部空闲后提交删除。
// 这是协议唯一授权删除目标的入口。
//
// encoder 副本不依据语言模型队列指标判定（编码器不导出此类队列），而是等待完整的 Draining 窗口后进入删除；
// 窗口耗尽后仍走同一释放证据捕获流程，捕获失败在总截止时间到达时通过 holdModelDeploymentRetirementBeforeDelete 中止。
func (r *ModelDeploymentReconciler) advanceModelDeploymentRetirementToDeleting(
	ctx context.Context, md *workercore.ModelDeployment, plan *modelDeploymentRetirementPlan,
) {
	now := r.modelDeploymentNow()
	// 只读取一次，保证窗口门控与下方 Deleting 原因对同一目标保持一致判断。
	encode := modelDeploymentRetirementReplicaIsEncode(plan.Target.Members)
	if encode {
		// 对 encoder 而言窗口本身即是门控，预算耗尽时放行删除而非中止。
		if !modelDeploymentRetirementBudgetSpent(plan.Reservation, modelDeploymentRetirementDrainBudget, now) {
			plan.Reservation.Reason = modelDeploymentRetirementEncodeWithdrawn

			return
		}
	} else {
		if modelDeploymentRetirementBudgetSpent(plan.Reservation, modelDeploymentRetirementDrainBudget, now) {
			r.abortModelDeploymentRetirement(plan, now,
				"the drain budget was exhausted before the target's queues reached zero")

			return
		}

		if reason, drained := r.observeModelDeploymentRetirementDrained(ctx, md, plan); !drained {
			plan.Reservation.Reason = reason

			return
		}
	}

	// 必须在授权删除前捕获并持久化释放证据。这是成员、分配记录与 Workload 全部尚存的最后时刻：
	// 一旦删除就无法从剩余 Pod 重建原占有信息。任一步失败都阻止删除，保留现场证据。
	record, err := r.captureModelDeploymentRetirementRelease(ctx, md, plan)
	if err != nil {
		r.holdModelDeploymentRetirementBeforeDelete(plan, now, encode, fmt.Sprintf(
			"the target's release cannot be recorded, so nothing is deleted: %v", err,
		))

		return
	}
	if err := r.persistModelDeploymentRetirementRelease(ctx, md, record); err != nil {
		r.holdModelDeploymentRetirementBeforeDelete(plan, now, encode, fmt.Sprintf(
			"the target's release could not be written, so nothing is deleted: %v", err,
		))

		return
	}
	// 写入后立即回读校验身份绑定，避免提交删除后才发现记录未落盘或不属于当前操作。
	if _, err := r.loadModelDeploymentRetirementRelease(md, plan); err != nil {
		r.holdModelDeploymentRetirementBeforeDelete(plan, now, encode, fmt.Sprintf(
			"the target's release was not recorded for this operation, so nothing is deleted: %v", err,
		))

		return
	}

	reason := "the target's queues are empty; deleting every member together"
	if encode {
		reason = "the target runs the Encode role and its withdrawal window is spent; " +
			"deleting every member together"
	}
	plan.Reservation.State = workercore.ModelDeploymentRetirementStateDeleting
	plan.Reservation.PhaseStartedAt = meta.NewTime(now)
	plan.Reservation.Reason = reason
	plan.DeletesAllowed = true
	plan.Deletes = modelDeploymentRetirementDeleteSet(plan.Target)
}

// holdModelDeploymentRetirementBeforeDelete 在释放证据捕获失败时阻止删除并保留现场；
// 对于已完成等待窗口的 encoder 副本，超过总截止时间后转为中止，避免永久停留在 Draining。
func (r *ModelDeploymentReconciler) holdModelDeploymentRetirementBeforeDelete(
	plan *modelDeploymentRetirementPlan, now time.Time, encode bool, reason string,
) {
	if encode && !now.Before(plan.Reservation.Deadline.Time) {
		r.abortModelDeploymentRetirement(plan, now, reason)

		return
	}

	plan.Reservation.Reason = reason
	plan.DeletesAllowed = false
	plan.Deletes = nil
}

// advanceModelDeploymentRetirementToSettling 观测删除是否已在 API server 上真正生效。
//
// 服务组成员在对应 Workload 删除前会被 Kueue finalizer 留驻在服务端，仅靠发起 Delete 不代表容量释放。
// 当捕获的所有成员 UID 均从服务端消失后，状态进入 Settling。
func (r *ModelDeploymentReconciler) advanceModelDeploymentRetirementToSettling(
	ctx context.Context, md *workercore.ModelDeployment, plan *modelDeploymentRetirementPlan,
) {
	now := r.modelDeploymentNow()
	plan.DeletesAllowed = true

	// held 集合会跳过已标记删除的对象，不能用来判定目标消失；需直接基于捕获 UID 重新查询服务端，
	// 仅在成员全部消失后再进入 Settling 观测账本与配额释放。
	reason, absent := r.observeModelDeploymentRetirementTargetsGone(ctx, md, plan)
	if !absent {
		plan.Reservation.Reason = reason
		plan.Deletes = modelDeploymentRetirementDeleteSet(plan.Target)

		return
	}

	plan.Reservation.State = workercore.ModelDeploymentRetirementStateSettling
	plan.Reservation.PhaseStartedAt = meta.NewTime(now)
	plan.Reservation.Reason = "the target has left; observing the accelerator and quota release"
}

// advanceModelDeploymentRetirementToCompleted 在观测到加速器与配额释放后结束退场操作。
//
// Deleting 及之后阶段绝不中止：目标已提交删除，无法保留成员、Workload 或容量，超时只更新原因并继续观测。
func (r *ModelDeploymentReconciler) advanceModelDeploymentRetirementToCompleted(
	ctx context.Context, md *workercore.ModelDeployment, plan *modelDeploymentRetirementPlan,
) {
	now := r.modelDeploymentNow()

	// 先观测是否已释放，再检查超时预算：即便释放完成得较晚，只要实际已释放就应正常完成。
	reason, released := r.observeModelDeploymentRetirementReleased(ctx, md, plan)
	if !released {
		if modelDeploymentRetirementBudgetSpent(plan.Reservation, modelDeploymentRetirementSettleBudget, now) {
			plan.Reservation.Reason = fmt.Sprintf(
				"the settle budget was exhausted and the release has not been observed: %s", reason,
			)
		} else {
			plan.Reservation.Reason = reason
		}

		return
	}

	plan.Reservation.State = workercore.ModelDeploymentRetirementStateCompleted
	plan.Reservation.Reason = "the target is deleted and its accelerator and quota release were observed"
	plan.Clear = true
	plan.InFlight = false
	plan.Hold = false
}

// abortModelDeploymentRetirement 结束删除前阶段因预算耗尽而触发的中止。
//
// 仅从删除前步骤进入，此时目标成员、Workload 与容量均完好保留；保留原 phaseStartedAt 便于排障与重试审计。
func (r *ModelDeploymentReconciler) abortModelDeploymentRetirement(
	plan *modelDeploymentRetirementPlan, now time.Time, reason string,
) {
	plan.Reservation.State = workercore.ModelDeploymentRetirementStateAborted
	plan.Reservation.Reason = reason + "; members, workload and capacity are retained"
	plan.Reservation.Deadline = meta.NewTime(now)
	plan.InFlight = false
}

// modelDeploymentRetirementBudgetSpent 报告当前阶段是否已耗尽时间预算。
//
// 阶段终点取阶段预算终点与整体 Deadline 中的较早者；相等即视为耗尽。
func modelDeploymentRetirementBudgetSpent(
	reservation *workercore.ModelDeploymentRetirementStatus, budget time.Duration, now time.Time,
) bool {
	phaseEnd := reservation.PhaseStartedAt.Add(budget)
	overallEnd := reservation.Deadline.Time
	if overallEnd.Before(phaseEnd) {
		phaseEnd = overallEnd
	}

	return !now.Before(phaseEnd)
}

// modelDeploymentRetirementTargetTerminal 报告预留当前持有的全部成员是否均已进入终态。
func modelDeploymentRetirementTargetTerminal(plan *modelDeploymentRetirementPlan) bool {
	if plan.held.Len() == 0 {
		return true
	}

	for _, member := range plan.Target.Members {
		if member.Status.Phase != core.PodFailed && member.Status.Phase != core.PodSucceeded {
			return false
		}
	}

	return true
}

// modelDeploymentRetirementDeleteSet 返回协议作为整体一次性删除的 Pod 集合（同一副本的所有成员同生共死）。
func modelDeploymentRetirementDeleteSet(view modelDeploymentReplicaView) []core.Pod {
	deletes := make([]core.Pod, 0, len(view.Members))
	for _, member := range view.Members {
		deletes = append(deletes, *member)
	}

	return deletes
}

// modelDeploymentNow 读取时钟；时钟作为字段而非直接调用 time.Now，使测试可以将时间预算
// （例如预留的各个阶段或弹性重试的退避时间）设定在任意时刻，而不必实际等待时间流逝。
func (r *ModelDeploymentReconciler) modelDeploymentNow() time.Time {
	if r.clock == nil {
		return time.Now()
	}

	return r.clock()
}

// observeModelDeploymentRetirementResidual 检查是否仍有路由器宣称正在服务目标的任一成员，
// 以及该问题当前是否可被可靠观测。
//
// 未配置路由器的部署无法观测直连 Service 上已有长连接是否静默，因此保守保持阻塞。
// 配置路由器时复用统一观测收集与聚合规则，并逐成员核对 Bound 与 RecentDispatch，防止漏检刚分发的请求。
func (r *ModelDeploymentReconciler) observeModelDeploymentRetirementResidual(
	ctx context.Context, md *workercore.ModelDeployment, held sets.Set[types.UID],
) (string, bool) {
	if md.Spec.Router == nil {
		return "this deployment declares no router, so the established direct-Service connections " +
			"this target's service may still hold cannot be observed; the operation holds", true
	}
	ctx, cancel := r.modelDeploymentRouterObservationContext(ctx, md)
	defer cancel()

	pods, err := r.listModelDeploymentPods(ctx, md)
	if err != nil {
		return "the deployment's pods could not be listed: " + err.Error(), true
	}
	live := make([]*core.Pod, 0, len(pods))
	for i := range pods {
		live = append(live, &pods[i])
	}

	observations, failure := r.collectModelDeploymentRouterObservations(ctx, md, live)
	if failure != "" {
		return failure, true
	}

	answer := aggregateModelDeploymentServing(true, observations, modelDeploymentServingFreshness)
	switch answer.State {
	case workercore.ModelDeploymentServingStateUnknown:
		return answer.Reason, true
	case workercore.ModelDeploymentServingStateNotConverged:
		return answer.Reason, true
	}

	serving := sets.New[types.UID]()
	// 分发日志（RecentDispatch）仅用于检查目标是否刚刚收到过分发，不计入服务副本数；
	// 只要命中被持有目标，就继续等待，不影响无关存活副本。
	dispatched := sets.New[types.UID]()
	for _, observation := range observations {
		for _, endpoint := range observation.Bound {
			if endpoint.Serving {
				serving.Insert(endpoint.PodUID)
			}
		}
		for _, endpoint := range observation.RecentDispatch {
			dispatched.Insert(endpoint.PodUID)
		}
	}

	residual := sets.New[types.UID]()
	for uid := range serving {
		if held.Has(uid) {
			residual.Insert(uid)
		}
	}
	for uid := range dispatched {
		if held.Has(uid) {
			residual.Insert(uid)
		}
	}
	if residual.Len() == 0 {
		return "", false
	}

	names := make([]string, 0, residual.Len())
	for uid := range residual {
		names = append(names, string(uid))
	}
	slices.Sort(names)

	return strconvx.Itoa(len(names)) + " target members are still served: " +
		strings.Join(names, ", "), true
}

// observeModelDeploymentRetirementDrained 检查目标所有需要作答的成员是否连续两次被观测为空闲。
//
// 不要求原 role 仍存在于 spec 中（删除角色本身就是常见退场场景），形态直接由目标成员自身命令行判定。
// 分离形态先等待交接宽限期（modelDeploymentRetirementDisaggregatedGraceWindow），再连续读取两次
// 排空状态并对每个预期指标逐项验零，避免单次瞬时读零导致误删在途流量。
func (r *ModelDeploymentReconciler) observeModelDeploymentRetirementDrained(
	ctx context.Context, md *workercore.ModelDeployment, plan *modelDeploymentRetirementPlan,
) (string, bool) {
	role := modelDeploymentRetirementRole(md, plan)
	externalDP, renderReason := modelDeploymentRetirementRenderedExternalDP(md, plan.Target.Members)
	if renderReason != "" {
		return renderReason, false
	}
	if len(plan.Target.Members) == 0 {
		return "the replica has no members to read", false
	}
	if !externalDP.known {
		return "a member carries no engine container to read, so the replica's own command line could " +
			"not be established", false
	}
	if disaggregated, reason := modelDeploymentRetirementDisaggregated(md, plan.Target.Members); reason != "" {
		return reason, false
	} else if disaggregated && (plan.Reservation.PhaseStartedAt.IsZero() ||
		r.modelDeploymentNow().Sub(plan.Reservation.PhaseStartedAt.Time) < modelDeploymentRetirementDisaggregatedGraceWindow) {
		return fmt.Sprintf(
			"the %s disaggregated handoff grace window is in progress because a disaggregated replica has no verifiable decoder-side release",
			modelDeploymentRetirementDisaggregatedGraceWindow,
		), false
	}

	answering, reason := r.modelDeploymentRetirementDrainMembers(md, plan)
	if reason != "" {
		return reason, false
	}

	reader := r.modelDeploymentDrainReaderOf()
	for _, member := range answering {
		target := modelDeploymentRetirementDrainTarget(md, role, plan, member)
		// 同一轮连续读取两次，两次均需完整且所有预期队列指标为零，避免两次请求间隙的偶然零值误放行。
		for read := 1; read <= 2; read++ {
			answer, err := reader.Drain(ctx, target)
			if err != nil {
				return fmt.Sprintf("member %s could not be read: %v", member.Name, err), false
			}
			if reason, idle := modelDeploymentDrainMemberIdle(member, target, answer); !idle {
				return reason, false
			}
		}
	}

	return "", true
}

// modelDeploymentRetirementRenderedExternalDP 从各成员实际渲染的命令行读取副本是否采用外部 DP 负载均衡，
// 以确定排空时应读取全部成员还是仅读取 leader。
func modelDeploymentRetirementRenderedExternalDP(
	md *workercore.ModelDeployment, members []*core.Pod,
) (modelDeploymentRetirementCommand, string) {
	var answer modelDeploymentRetirementCommand
	var missing bool
	for _, member := range members {
		externalDP, known := modelDeploymentRetirementMemberExternalDP(md, member)
		if !known {
			missing = true

			continue
		}
		if answer.known && answer.answer != externalDP {
			return modelDeploymentRetirementCommand{}, "the replica's members disagree on whether it " +
				"is disaggregated, so its shape could not be established"
		}
		answer = modelDeploymentRetirementCommand{answer: externalDP, known: true}
	}
	// 只要有任一成员无法读取，整个副本就视为不可读，防止依据部分成员误判。
	answer.known = answer.known && !missing

	return answer, ""
}

// modelDeploymentRetirementCommand 记录成员命令行给出的布尔判断以及是否全部成员均成功读取。
type modelDeploymentRetirementCommand struct {
	answer bool
	known  bool
}

// modelDeploymentRetirementAnsweringShape 根据实际成员 Pod 判定副本的响应形态。
func modelDeploymentRetirementAnsweringShape(
	md *workercore.ModelDeployment, members []*core.Pod,
) (modelDeploymentAnsweringShape, string) {
	externalDP, size, reason := modelDeploymentRetirementReplicaFacts(md, members)
	if reason != "" {
		return modelDeploymentAnsweringUnknown, reason
	}

	return modelDeploymentAnsweringShapeOf(externalDP, size), ""
}

// modelDeploymentRetirementReplicaFacts 从存活成员中提取推导响应形态所需的两个事实（外部 DP 与规模）。
func modelDeploymentRetirementReplicaFacts(
	md *workercore.ModelDeployment, members []*core.Pod,
) (externalDP bool, size int, reason string) {
	size = len(members)
	if size == 0 {
		return false, 0, "the replica has no members to read"
	}

	command, cmdReason := modelDeploymentRetirementRenderedExternalDP(md, members)
	if cmdReason != "" {
		return false, 0, cmdReason
	}
	if !command.known {
		return false, 0, "a member carries no engine container to read, so the replica's own command " +
			"line could not be established"
	}

	shape := modelDeploymentDeployedReplicaShape(modelDeploymentReplicaView{Members: members})
	if !shape.modelDeploymentReplicaIsWhole() {
		return false, 0, shape.Reason
	}

	return command.answer, size, ""
}

// modelDeploymentRetirementMemberExternalDP 读取单个成员的负载均衡形态；缺少主容器时 known 返回 false。
func modelDeploymentRetirementMemberExternalDP(
	md *workercore.ModelDeployment, member *core.Pod,
) (externalDP, known bool) {
	container, found := modelDeploymentDrainContainerOf(member, modelDeploymentMainContainerName)
	if !found {
		return false, false
	}

	reading, err := scanModelDeploymentParallelism(modelDeploymentRetirementEngine(md),
		container.Command, modelDeploymentContainerEnv(container))
	if err != nil {
		return false, true
	}
	shape, _ := modelDeploymentLoadBalance(modelDeploymentRetirementEngine(md),
		modelDeploymentRoleKindFromUpstream(member.Labels[modelDeploymentLabelKeyRoleKind]),
		modelDeploymentContainerEnv(container), reading.declared)
	return shape == workercore.ModelDeploymentLoadBalanceExternal ||
		shape == workercore.ModelDeploymentLoadBalanceHybrid ||
		shape == workercore.ModelDeploymentLoadBalanceMultiPort, true
}

func modelDeploymentRetirementDisaggregated(md *workercore.ModelDeployment, members []*core.Pod) (bool, string) {
	engine := modelDeploymentRetirementEngine(md)
	var answer modelDeploymentRetirementCommand
	for _, member := range members {
		kind := strings.ToLower(member.Labels[modelDeploymentLabelKeyRoleKind])
		if kind == "" && md != nil {
			roleName := modelDeploymentPodRole(member)
			for i := range md.Spec.Roles {
				if md.Spec.Roles[i].Name == roleName {
					if k, ok := ModelDeploymentRoleUpstreamKind(
						ModelDeploymentEffectiveRoleKind(&md.Spec.Roles[i]),
					); ok {
						kind = k
					}
					break
				}
			}
		}
		container, found := modelDeploymentDrainContainerOf(member, modelDeploymentMainContainerName)
		if !found {
			return false, "a member has no engine container to classify"
		}
		reading, err := scanModelDeploymentParallelism(engine,
			container.Command, modelDeploymentContainerEnv(container))
		if err != nil {
			return false, "the member's parallelism could not be read: " + err.Error()
		}
		shape, reason := modelDeploymentLoadBalance(engine,
			modelDeploymentRoleKindFromUpstream(kind),
			modelDeploymentContainerEnv(container), reading.declared)
		if shape == workercore.ModelDeploymentLoadBalanceUnknown {
			return false, reason
		}
		disaggregated := kind == "prefill" || kind == "decode" ||
			modelDeploymentDrainSGLangDisaggregationMode(container.Command) != "" ||
			shape == workercore.ModelDeploymentLoadBalanceExternal ||
			shape == workercore.ModelDeploymentLoadBalanceHybrid ||
			shape == workercore.ModelDeploymentLoadBalanceMultiPort
		if answer.known && answer.answer != disaggregated {
			return false, "the replica's members disagree on whether it is disaggregated, so its shape could not be established"
		}
		answer = modelDeploymentRetirementCommand{answer: disaggregated, known: true}
	}
	return answer.answer, ""
}

func modelDeploymentContainerEnv(container *core.Container) []workercore.ModelDeploymentEnvVar {
	env := make([]workercore.ModelDeploymentEnvVar, 0, len(container.Env))
	for _, entry := range container.Env {
		env = append(env, workercore.ModelDeploymentEnvVar{Name: entry.Name, Value: entry.Value})
	}
	return env
}

// modelDeploymentRetirementDrainMembers 返回排空阶段需要采样观测的成员列表（不缩小预留绑定的删除 UID 集合）。
func (r *ModelDeploymentReconciler) modelDeploymentRetirementDrainMembers(
	md *workercore.ModelDeployment,
	plan *modelDeploymentRetirementPlan,
) ([]*core.Pod, string) {
	shape, reason := modelDeploymentRetirementAnsweringShape(md, plan.Target.Members)
	if reason != "" {
		return nil, reason
	}

	return modelDeploymentAnsweringMembers(shape, plan.Target.Members)
}

// modelDeploymentRetirementReplicaIsEncode 根据成员 Pod 自身携带的角色类型标签判断副本是否为 encoder。
// 全部成员均标记为 encode 才返回 true，成员不一致时走常规队列路径以保证故障安全。
func modelDeploymentRetirementReplicaIsEncode(members []*core.Pod) bool {
	if len(members) == 0 {
		return false
	}

	upstream, ok := ModelDeploymentRoleUpstreamKind(workercore.ModelDeploymentRoleKindEncode)
	if !ok {
		return false
	}
	for _, member := range members {
		if member.Labels[modelDeploymentLabelKeyRoleKind] != upstream {
			return false
		}
	}

	return true
}

// modelDeploymentRetirementDrainTarget 构造针对单个成员的排空观测目标，显式固定容器名、引擎与部署 UID。
func modelDeploymentRetirementDrainTarget(
	md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole,
	plan *modelDeploymentRetirementPlan, member *core.Pod,
) modelDeploymentDrainTarget {
	return modelDeploymentDrainTarget{
		PodUID:        member.UID,
		Container:     modelDeploymentMainContainerName,
		Role:          modelDeploymentRetirementRoleName(role, plan),
		Engine:        modelDeploymentRetirementEngine(md),
		DeploymentUID: md.UID,
	}
}

// modelDeploymentDrainMemberIdle 依据排空读取结果逐项校验单成员是否真正空闲，不满足时返回可读原因。
func modelDeploymentDrainMemberIdle(
	member *core.Pod, target modelDeploymentDrainTarget, answer modelDeploymentDrainAnswer,
) (string, bool) {
	switch answer.State {
	case modelDeploymentDrainUnsupported:
		return "member " + member.Name + " reports no in-flight measurement: " + answer.Reason, false
	case modelDeploymentDrainUnknown:
		return "member " + member.Name + " is unobserved: " + answer.Reason, false
	case modelDeploymentDrainBusy:
		return "member " + member.Name + " still holds " +
			modelDeploymentDrainSeriesTotal(answer.Series), false
	}

	if !answer.Complete {
		return "member " + member.Name + " returned an incomplete envelope", false
	}
	if missing := modelDeploymentDrainMissingSeries(target.Engine, answer.Series); len(missing) > 0 {
		return "member " + member.Name + " is missing " + strings.Join(missing, ", "), false
	}
	if invalid := modelDeploymentDrainUnusableSeries(target.Engine, answer.Series); len(invalid) > 0 {
		return "member " + member.Name + " reported unusable activity in " +
			strings.Join(invalid, ", "), false
	}
	if busy := modelDeploymentDrainActiveSeries(target.Engine, answer.Series); len(busy) > 0 {
		return "member " + member.Name + " still holds activity in " + strings.Join(busy, ", "), false
	}

	return "", true
}

// modelDeploymentDrainActivityValue 报告一个 gauge 数值是否可被解读为有效活动量。
//
// 在途工作数量是一个非负有限数，任何其他值都是本协议无法使用的结果：NaN 与所有边界比较
// 均为 false，因此它会绕过零值检测，却并不表示任何实际测量；而无穷大或负计数值则是
// 只反映采集异常而非队列状态的带符号数据行。解析器与协议共用这同一个谓词，使在报文边界
// 被拒绝的值在判定处也以完全相同的方式被拒绝。
func modelDeploymentDrainActivityValue(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0) && value >= 0
}

// modelDeploymentDrainUnusableSeries 列出预期指标中数值非有效活动量（NaN/Inf/负数）的指标名。
func modelDeploymentDrainUnusableSeries(engine string, series map[string]float64) []string {
	expected, known := modelDeploymentInFlightMetrics[engine]
	if !known {
		return []string{"a measurement for engine " + engine}
	}

	unusable := make([]string, 0, len(expected))
	for _, metric := range expected {
		if value, present := series[metric]; present && !modelDeploymentDrainActivityValue(value) {
			unusable = append(unusable, metric)
		}
	}
	sort.Strings(unusable)

	return unusable
}

// modelDeploymentDrainActiveSeries 列出预期指标中数值不为零的指标名。
func modelDeploymentDrainActiveSeries(engine string, series map[string]float64) []string {
	expected, known := modelDeploymentInFlightMetrics[engine]
	if !known {
		return []string{"a measurement for engine " + engine}
	}

	active := make([]string, 0, len(expected))
	for _, metric := range expected {
		if value := series[metric]; value != 0 {
			active = append(active, metric)
		}
	}
	sort.Strings(active)

	return active
}

// modelDeploymentDrainMissingSeries 列出本次采样缺失的预期在途指标名，预期清单直接复用全局映射表。
func modelDeploymentDrainMissingSeries(engine string, series map[string]float64) []string {
	expected, known := modelDeploymentInFlightMetrics[engine]
	if !known {
		return []string{"a measurement for engine " + engine}
	}

	missing := make([]string, 0, len(expected))
	for _, metric := range expected {
		if _, present := series[metric]; !present {
			missing = append(missing, metric)
		}
	}

	return missing
}

// modelDeploymentDrainSeriesTotal 汇总采样到的在途请求总数，供阻塞原因文案展示。
func modelDeploymentDrainSeriesTotal(series map[string]float64) string {
	total := 0.0
	for _, value := range series {
		total += value
	}

	return fmt.Sprintf("%.0f", total) + " in flight"
}

// modelDeploymentRetirementEngine 返回目标副本对应的引擎名；缺省时回退为 vLLM。
func modelDeploymentRetirementEngine(md *workercore.ModelDeployment) string {
	if md.Spec.Engine.Name == "" {
		return workercore.ModelDeploymentEngineVLLM
	}

	return md.Spec.Engine.Name
}

// modelDeploymentRetirementRoleName 返回目标所属角色名；即使 spec 已删除该角色，预留中仍保留其名称。
func modelDeploymentRetirementRoleName(
	role *workercore.ModelDeploymentRole, plan *modelDeploymentRetirementPlan,
) string {
	if role != nil {
		return role.Name
	}

	return plan.Reservation.RoleName
}

// modelDeploymentRetirementRole 在当前 spec 中查找预留指向的角色；已被删除则返回 nil。
func modelDeploymentRetirementRole(
	md *workercore.ModelDeployment, plan *modelDeploymentRetirementPlan,
) *workercore.ModelDeploymentRole {
	for i := range md.Spec.Roles {
		if md.Spec.Roles[i].Name == plan.Reservation.RoleName {
			return &md.Spec.Roles[i]
		}
	}

	return nil
}

// adoptsModelDeploymentRetirement 判断在 spec 重新声明相同且健康未变的副本时，能否取消删除前预留并收编该副本。
//
// 必须同时满足三个条件：spec 重新包含该 ordinal、存活成员与预留 UID 集合完全一致、且除预留本身外其余
// 健康资格维度全部通过。恢复端点资格标签时若中途出错则回滚已写标签并要求重试。
func (r *ModelDeploymentReconciler) adoptsModelDeploymentRetirement(
	ctx context.Context, md *workercore.ModelDeployment, plan *modelDeploymentRetirementPlan,
	pods []core.Pod,
) (bool, bool) {
	if (plan.Reservation.State != workercore.ModelDeploymentRetirementStateAdmitted &&
		plan.Reservation.State != workercore.ModelDeploymentRetirementStateDisqualified &&
		plan.Reservation.State != workercore.ModelDeploymentRetirementStateWithdrawing &&
		plan.Reservation.State != workercore.ModelDeploymentRetirementStateDraining &&
		plan.Reservation.State != workercore.ModelDeploymentRetirementStateAborted) ||
		plan.held.Len() == 0 {
		return false, false
	}
	if plan.Reservation.State != workercore.ModelDeploymentRetirementStateAborted &&
		md.Generation < plan.Reservation.ObservedGeneration {
		return false, false
	}

	role := modelDeploymentRetirementRole(md, plan)
	if role == nil || int(role.Replicas) <= int(plan.Reservation.ReplicaOrdinal) {
		return false, false
	}
	if plan.held.Len() != len(plan.Reservation.TargetMemberUIDs) ||
		len(plan.Target.Members) != len(plan.Reservation.TargetMemberUIDs) {
		return false, false
	}
	if len(modelDeploymentRetirementReplicaMembers(
		pods, plan.Reservation.RoleName, int(plan.Reservation.ReplicaOrdinal),
	)) != len(plan.Reservation.TargetMemberUIDs) {
		return false, false
	}

	members := make([]core.Pod, 0, len(plan.Target.Members))
	for _, member := range plan.Target.Members {
		members = append(members, *member)
	}
	fetch := r.groupForwardFetch
	if fetch == nil {
		fetch = defaultGroupForwardFetch
	}
	qualifications := qualifyModelDeploymentInstances(
		ctx, md, members, modelDeploymentPendingReplacement{}, fetch,
	)
	for _, qualification := range qualifications {
		if qualification.View.Role == plan.Reservation.RoleName &&
			qualification.View.Ordinal == int(plan.Reservation.ReplicaOrdinal) {
			if !modelDeploymentRetirementAdoptable(qualification) {
				return false, false
			}
			var patched []*core.Pod
			for _, member := range plan.Target.Members {
				if member.Labels[modelDeploymentLabelKeyEndpointEligible] != modelDeploymentEndpointEligibleValue {
					base := member.DeepCopy()
					candidate := member.DeepCopy()
					if candidate.Labels == nil {
						candidate.Labels = map[string]string{}
					}
					candidate.Labels[modelDeploymentLabelKeyEndpointEligible] = modelDeploymentEndpointEligibleValue
					if r.Client != nil {
						if err := r.Client.Patch(ctx, candidate, ctrlcli.MergeFrom(base)); err != nil {
							ctrllog.FromContext(ctx).Error(
								err, "failed to restore endpoint eligibility on adopted retirement member",
								"pod", member.Name,
							)
							for _, restored := range patched {
								withdrawn := restored.DeepCopy()
								delete(withdrawn.Labels, modelDeploymentLabelKeyEndpointEligible)
								if err := r.Client.Patch(ctx, withdrawn, ctrlcli.MergeFrom(restored.DeepCopy())); err != nil {
									ctrllog.FromContext(ctx).Error(
										err, "failed to roll back endpoint eligibility", "pod", restored.Name,
									)
								}
								delete(restored.Labels, modelDeploymentLabelKeyEndpointEligible)
							}
							return false, true
						}
					}
					if member.Labels == nil {
						member.Labels = map[string]string{}
					}
					member.Labels[modelDeploymentLabelKeyEndpointEligible] = modelDeploymentEndpointEligibleValue
					patched = append(patched, member)
				}
			}
			if r.Recorder != nil {
				r.Recorder.Event(md, core.EventTypeNormal, "RetirementAdopted",
					"the declared replica was healthy and its retirement reservation was cancelled")
			}
			return true, false
		}
	}

	return false, false
}

// modelDeploymentRetirementAdoptable 判断被保留副本在忽略 NoPlannedRetirement 这一预留项本身外，
// 其余全部健康资格维度是否均已验证通过。
func modelDeploymentRetirementAdoptable(qualification modelDeploymentInstanceQualification) bool {
	for _, leg := range qualification.Legs {
		if leg.Name == modelDeploymentLegNoPlannedRetirement {
			continue
		}
		if leg.Verdict != modelDeploymentLegVerified {
			return false
		}
	}

	return true
}

// consumeModelDeploymentRetry 处理重试注解，并严格保证执行顺序：
//  1. 若 token 与已消费 token 相同则视为重放，直接清理注解且不改状态；
//  2. 否则先将新 token 与回退至 Admitted 的状态一同持久化到服务端；
//  3. 仅在状态落盘成功后再清理重试注解。
//
// 这样即使在第 2 步与第 3 步之间崩溃，下一轮会命中分支 1 仅清理注解，绝不会丢失指令或重复触发重试。
func (r *ModelDeploymentReconciler) consumeModelDeploymentRetry(
	ctx context.Context, md *workercore.ModelDeployment, plan *modelDeploymentRetirementPlan,
) bool {
	logger := ctrllog.FromContext(ctx)

	directive, present := md.Annotations[modelDeploymentRetirementRetryAnnotation]
	if !present {
		return false
	}
	roleName, ordinal, token, ok := parseModelDeploymentRetirementRetry(directive)
	if !ok {
		logger.Info("refusing a malformed retirement retry directive", "directive", directive)

		return false
	}
	if roleName != plan.Reservation.RoleName || ordinal != int(plan.Reservation.ReplicaOrdinal) {
		logger.Info("refusing a retirement retry directive that names another replica",
			"directive", directive)

		return false
	}

	// 重放去重必须在状态判断之前执行，确保崩溃恢复后即使状态已切回 Admitted 也能把残留注解清掉。
	if plan.Reservation.LastConsumedRetryToken == token {
		if err := r.clearModelDeploymentRetirementRetry(ctx, md); err != nil {
			logger.Error(err, "clear a consumed retirement retry directive")
		}

		return false
	}

	// 仅在操作处于 Aborted 状态时接受未消费的重试指令。
	if plan.Reservation.State != workercore.ModelDeploymentRetirementStateAborted {
		return false
	}

	now := r.modelDeploymentNow()
	reservation := plan.Reservation.DeepCopy()
	reservation.LastConsumedRetryToken = token
	reservation.State = workercore.ModelDeploymentRetirementStateAdmitted
	reservation.Reason = "retry directive accepted"
	reservation.StartedAt = meta.NewTime(now)
	reservation.Deadline = meta.NewTime(now.Add(modelDeploymentRetirementOverallBudget))
	reservation.PhaseStartedAt = meta.NewTime(now)

	// 关键持久化点：写入成功后 token 即已生效，后续清理注解只是收尾动作。
	if err := r.writeModelDeploymentRetirement(ctx, md, md.Status.Retirement, reservation); err != nil {
		logger.Error(err, "persist a consumed retirement retry token")

		return false
	}

	if err := r.clearModelDeploymentRetirementRetry(ctx, md); err != nil {
		logger.Error(err, "clear a consumed retirement retry directive")
	}

	plan.Reservation = reservation
	plan.Hold = true
	plan.InFlight = true
	plan.Deletes = nil
	plan.DeletesAllowed = false

	return true
}

// clearModelDeploymentRetirementRetry 使用带乐观锁的 merge patch 清除重试注解，防止误删并发写入的新指令。
func (r *ModelDeploymentReconciler) clearModelDeploymentRetirementRetry(
	ctx context.Context, md *workercore.ModelDeployment,
) error {
	if _, present := md.Annotations[modelDeploymentRetirementRetryAnnotation]; !present {
		return nil
	}

	patched := md.DeepCopy()
	delete(patched.Annotations, modelDeploymentRetirementRetryAnnotation)
	if err := r.Client.Patch(ctx, patched, ctrlcli.MergeFromWithOptions(
		md, ctrlcli.MergeFromWithOptimisticLock{},
	)); err != nil {
		return fmt.Errorf("clear the retirement retry directive: %w", err)
	}
	md.Annotations = patched.Annotations

	return nil
}

// parseModelDeploymentRetirementRetry 严格解析三段式重试指令（roleName:ordinal:token），格式非法直接拒绝。
func parseModelDeploymentRetirementRetry(directive string) (string, int, string, bool) {
	parts := strings.Split(directive, ":")
	if len(parts) != 3 {
		return "", 0, "", false
	}

	ordinal, err := strconvx.Atoi[int](parts[1])
	if err != nil || ordinal < 0 {
		return "", 0, "", false
	}
	if parts[0] == "" || parts[2] == "" {
		return "", 0, "", false
	}

	return parts[0], ordinal, parts[2], true
}

// errModelDeploymentReservationUnnameable 表示目标无法被标识，因此没有任何预留可以指定它。
// 它会阻止移除而非放行，因为确认移除安全的唯一方式就是拥有可供核验的身份标识。
var errModelDeploymentReservationUnnameable = errors.New(
	"the target has no uid, so a reservation cannot name it",
)

// refuseModelDeploymentRemoval 是准入环节：当某个执行路径即将删除不再需要的副本时，它
// 决定是否由淘汰协议接管此次删除。
//
// 它总是阻止直接删除，这就是它的全部职责。移除意图要么启动协议（当没有操作正在进行时
// 作为首个意图），要么因已有操作持有一个副本而被拒绝；无论哪种情况，删除都不会在本轮
// 发生。它唯一绝不做的事情就是直接删除。
//
// 为何必须由协议接管删除。移除副本的方式是删除其 Pod 以及 Kueue 用来挂起其 finalizer 的
// Workload。这是一种瞬间切断：引擎收到信号，正在处理的请求会在中途被拒绝，且无法观测
// 目标当时实际在做什么。协议存在的意义就是在切断前完成撤出与排空，因此即将执行删除的
// 路径必须先将决策权交给它。若没有这一环节，预留对象就没有写入方，2 缩 1 的缩容会像
// 以前一样立即删除，也就永远无法得知目标原本正在服务什么请求。
//
// 一次只允许一个操作，第二个意图会被拒绝而非排队。通信结构中每个部署只保存一个预留，
// 若两个操作同时撤出，就相当于对同一个问题给出两个无法合并的答案。因此，只要已有预留
// 在进行中，其他所有移除意图都会被阻止，并且是在后续轮次重试而非记录到队列中：等待
// 不会丢失任何状态，因为操作结束时移除意图依然存在，下一轮就会准入它。这正是其
// 电平触发的特性——被阻止的轮次并非失败的轮次，当前被阻塞的移除会在协议完成后继续推进。
//
// 未声明序数（ordinal）的 Pod 不予准入，因为通信结构通过 (role, ordinal) 指定目标，
// 不属于任何副本的 Pod 无从命名。同时也不应阻止它，因为一个没有对应预留可以最终清除
// 的阻塞，会导致没有任何逻辑能再移除该副本。它应由滚动更新负责清理，无槽位的 Pod
// 一向也是在那里被移除的。
func (r *ModelDeploymentReconciler) refuseModelDeploymentRemoval(
	ctx context.Context, md *workercore.ModelDeployment, plan *modelDeploymentRetirementPlan,
	pod *core.Pod, pods, members []core.Pod,
) bool {
	logger := ctrllog.FromContext(ctx)
	role := modelDeploymentPodRole(pod)
	ordinal, seated := modelDeploymentPodOrdinal(pod)

	// 在判断 Pod 是否有槽位之前，先拒绝已被预留的 Pod。预留按 UID 指定成员，而序数标签是
	// 可变的：一旦被剥离，Pod 就不再是通信结构可命名的副本，但操作依然持有这同一个 Pod。
	// 若走下方无槽位的返回分支，它会将其交给按角色删除的路径。此检查范围很窄，仅拒绝预留
	// 实际持有的 Pod。
	if plan.holds(pod.UID) {
		logger.V(3).Info("holding a removal of a pod the reservation already holds",
			"pod", pod.Name, "role", role, "ordinal", ordinal, "seated", seated,
			"state", plan.Reservation.State)

		return true
	}

	if !seated {
		return false
	}

	if plan.Reservation == nil || plan.Reservation.RoleName != role ||
		int(plan.Reservation.ReplicaOrdinal) != ordinal {
		podPointers := make([]*core.Pod, 0, len(pods))
		for i := range pods {
			podPointers = append(podPointers, &pods[i])
		}
		members := make([]*core.Pod, 0, 1)
		for _, member := range modelDeploymentReplicaMembers(podPointers, ordinal) {
			if modelDeploymentPodRole(member) == role {
				members = append(members, member)
			}
		}
		neverScheduled := len(members) > 0
		for i := range members {
			if !modelDeploymentReplicaNeverScheduled(members[i]) {
				neverScheduled = false
				break
			}
		}
		if neverScheduled {
			workloads, err := r.findModelDeploymentGroupWorkloads(ctx, md, modelDeploymentPodValues(members))
			if err != nil {
				logger.Error(err, "find workloads for never-scheduled retirement candidate", "pod", pod.Name)
			}
			admitted := false
			for _, workload := range workloads {
				if kubeapistatus.ConditionType(kueue.WorkloadAdmitted).IsTrue(workload) {
					admitted = true
					break
				}
			}
			// 发生瞬时列举错误时，保守地回退到标准淘汰准入流程，而不是在准入状态未确认的情况下
			// 绕过淘汰协议。
			if err == nil && !admitted {
				return false
			}
		}
	}

	// 在协议运行期间以及保留目标期间，移除操作均会被拒绝。InFlight 仅涵盖前者：中止操作会
	// 清除 InFlight 但仍保留目标，此时第二个意图就会覆盖被保留的预留。Hold 恰好在有目标
	// 被保留时为 true，因此两者结合即可拒绝冲突意图，同时又不会冻结合法替换该预留的出口。
	if plan.InFlight || plan.Hold {
		logger.V(3).Info("holding a removal while a retirement reservation is in flight or retained",
			"pod", pod.Name, "role", role, "ordinal", ordinal,
			"state", plan.Reservation.State, "retained", plan.Hold)

		return true
	}

	if err := r.admitModelDeploymentRetirement(ctx, md, plan, pod, pods, members); err != nil {
		// 未能写入的预留应视为阻止而非允许删除。控制器随后会再次进入协调，因为无论如何计划中
		// 都已设置了触发重试的重新入队；若此时改为直接发出删除，就会造成本协议旨在防止的突然
		// 切断。
		logger.Error(err, "admit a retirement reservation", "pod", pod.Name)

		return true
	}

	return true
}

func modelDeploymentReplicaNeverScheduled(pod *core.Pod) bool {
	return pod.Spec.NodeName == "" && pod.Status.PodIP == "" && pod.Status.Phase == core.PodPending &&
		pod.Labels[modelDeploymentLabelKeyEndpointEligible] != modelDeploymentEndpointEligibleValue
}

// admitModelDeploymentRetirement 为某个移除路径想要下线的副本写入预留。
//
// 成员集合由调用方收窄，这也是整副本淘汰（retirement）与成员裁剪（shed）之间的区别。
// 当 spec 不再声明某个序数时，整个副本都会被淘汰，因此成员就是该副本的全部成员。
// 冗余规则的决策粒度更细：它只裁剪某个仍被需要的序数下的单个成员，若在那里绑定整个
// 序数，就会把当前渲染结果仍然描述的成员删掉——而这恰恰是冗余规则旨在保留的成员。
// 掌握更窄成员集的调用方会直接传入该集合，通信结构中的成员 UID 正是用来表达这一点。
//
// 它绑定的是副本当前实际的成员，而非 spec 所描述的成员。成员 UID 是后续删除前置条件
// 校验的依据，而 Workload UID 是释放持有各成员的 finalizer 的关键，因此两者均在准入时
// 读取而非后续重新计算：否则在准入与删除之间重新填充进槽位的 Pod 就会顶替被预留成员
// 遭到删除。
//
// 执行准入的这一轮不删除任何内容，这是调用方配合的部分：该函数返回后，调用方保持阻止
// 删除，并且由于计划中现在已携带预留，同一轮中的其他后续移除意图都会被上方分支拒绝。
func (r *ModelDeploymentReconciler) admitModelDeploymentRetirement(
	ctx context.Context, md *workercore.ModelDeployment, plan *modelDeploymentRetirementPlan,
	pod *core.Pod, pods, members []core.Pod,
) error {
	role := modelDeploymentPodRole(pod)
	ordinal, _ := modelDeploymentPodOrdinal(pod)
	now := r.modelDeploymentNow()

	if members == nil {
		members = modelDeploymentRetirementReplicaMembers(pods, role, ordinal)
	}
	workloadUID := ""
	// 未达到声明总数的副本根本不会编排产生 Workload，这与组 Workload 的所有其他读取方
	// 本就容忍的缺失状态是一致的。绑定空字符串而非凭空构造一个 UID，可避免准入在 Kueue
	// 尚未完成编排的组上失败。
	if len(members) > 0 {
		workloads, err := r.findModelDeploymentGroupWorkloads(ctx, md, members)
		if err != nil {
			return err
		}
		if len(workloads) > 0 {
			workloadUID = string(workloads[0].UID)
		}
	}

	// 没有 UID 的成员无法被指定，而绑定空字符串的预留会命中部署中的每一个 Pod：因为目标是
	// 按 UID 解析的，未命名的目标会解析为整个列表，协议进而会将其全部删除。集群总是在本
	// 控制器见到 Pod 之前就分配好 UID，因此这主要针对测试夹具以及防御性检查，而非生产环境
	// 会走到的分支——正因如此，该分支选择阻止删除而非放行删除。若在此处失效开放，就会因为
	// 缺少身份标识而误删整个部署。
	uids := make([]string, 0, len(members))
	for _, member := range members {
		if member.UID == "" {
			return errModelDeploymentReservationUnnameable
		}
		uids = append(uids, string(member.UID))
	}
	slices.Sort(uids)

	reservation := &workercore.ModelDeploymentRetirementStatus{
		RoleName:           role,
		ReplicaOrdinal:     int32(ordinal),
		ObservedGeneration: md.Generation,
		TargetMemberUIDs:   uids,
		TargetWorkloadUID:  workloadUID,
		State:              workercore.ModelDeploymentRetirementStateAdmitted,
		Reason:             "admitted for a replica this pass no longer declares",
		StartedAt:          meta.NewTime(now),
		Deadline:           meta.NewTime(now.Add(modelDeploymentRetirementOverallBudget)),
		PhaseStartedAt:     meta.NewTime(now),
	}

	if err := r.writeModelDeploymentRetirement(ctx, md, md.Status.Retirement, reservation); err != nil {
		return fmt.Errorf("persist an admitted retirement reservation: %w", err)
	}

	plan.Reservation = reservation
	plan.Hold = true
	plan.InFlight = true
	plan.Target = modelDeploymentReplicaView{Role: role, Ordinal: ordinal, Seated: true}
	plan.held = sets.New[types.UID]()
	for _, member := range members {
		plan.held.Insert(member.UID)
		plan.Target.Members = append(plan.Target.Members, &member)
	}

	return nil
}

// modelDeploymentRetirementReplicaMembers 返回落座于单个副本上的存活 Pod，通过节点树自身
// 的分组逻辑解析，确保准入与协议后续的目标解析对单个副本的边界认定一致。
func modelDeploymentRetirementReplicaMembers(
	pods []core.Pod, role string, ordinal int,
) []core.Pod {
	members := make([]core.Pod, 0, len(pods))
	for _, view := range modelDeploymentGroupPodsByReplica(pods) {
		if view.Role != role || !view.Seated || view.Ordinal != ordinal {
			continue
		}
		for _, member := range view.Members {
			members = append(members, *member)
		}
	}

	return members
}

// modelDeploymentShedMembers 将待裁剪集合转换为准入绑定所需的值切片形式。由于冗余规则使用
// Pod 指针列表，而预留绑定的是值，因此在此处统一完成转换，避免在每个调用点重复处理。
func modelDeploymentShedMembers(shed []*core.Pod) []core.Pod {
	members := make([]core.Pod, 0, len(shed))
	for _, pod := range shed {
		members = append(members, *pod)
	}

	return members
}

// modelDeploymentRetirementShedSeat 报告待裁剪集合是否包含协议可命名的副本，并返回其首个
// 成员的序数。即使数量超出预期，待裁剪集合仍可能为空——挑选是基于槽位以及当前渲染结果
// 进行的，无法拆分出的集合不会产出任何成员——而空集合不属于需要移交的移除操作。
func modelDeploymentRetirementShedSeat(shed []*core.Pod) (int, bool) {
	if len(shed) == 0 {
		return 0, false
	}

	return modelDeploymentPodOrdinal(shed[0])
}
