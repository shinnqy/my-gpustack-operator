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
	// 在第 19 章中实现此函数。
	var result0 *modelDeploymentRetirementPlan
	return result0
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

// modelDeploymentNow 读取时钟；时钟作为字段而非直接调用 time.Now，使测试可以将时间预算
// （例如预留的各个阶段或弹性重试的退避时间）设定在任意时刻，而不必实际等待时间流逝。
func (r *ModelDeploymentReconciler) modelDeploymentNow() time.Time {
	if r.clock == nil {
		return time.Now()
	}

	return r.clock()
}

func modelDeploymentContainerEnv(container *core.Container) []workercore.ModelDeploymentEnvVar {
	env := make([]workercore.ModelDeploymentEnvVar, 0, len(container.Env))
	for _, entry := range container.Env {
		env = append(env, workercore.ModelDeploymentEnvVar{Name: entry.Name, Value: entry.Value})
	}
	return env
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
