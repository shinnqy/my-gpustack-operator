package worker

import (
	"fmt"
	"strings"

	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubeapistatus"
	"gpustack.ai/gpustack/pkg/utils/strconvx"
)

// ModelDeploymentConditionReplicasUpToDate 报告运行中的副本是否与当前收敛逻辑为其渲染的
// 结果一致。
//
// 它并非仅针对对象 spec 的陈述，如果那样定义会让消费方无法理解。它所比较的哈希还涵盖
// 从 Binding 合成的 KVCache 连接器，因此只要该连接器无法再解析，即使仍携带当前 spec 的
// 副本也会与渲染结果产生差异——这正是本条件存在所要报告的状态。
//
// 设立该条件的原因在于，滚动更新存在一种其他所有条件都描述正确却未提及的状态。当连接
// 无法解析时，哈希与期望不一致的副本会被保留在原地，因为重建它会导致仅因存储短暂抖动
// 就重建所有部署的每一个副本——而这一保护机制会在存储不可用期间推迟无关的修改。
//
// 在推迟变更期间，对象给出的状态虽然准确，指向的却不是用户关注的主体：唯一为 false 的
// 条件提到了一个指标无法读取的复用域，但用户无法从这句话推导出“你正在等待的修改正排队
// 阻塞在它后面”。本条件正是打通这一推导的路径，完整解决了由此带来的可诊断性成本——
// 被推迟的变更并未丢失，因此此处无需任何恢复逻辑。
const ModelDeploymentConditionReplicasUpToDate kubeapistatus.ConditionType = "ReplicasUpToDate"

const (
	modelDeploymentReasonUpToDate           = "UpToDate"
	modelDeploymentReasonRolloutInProgress  = "RolloutInProgress"
	modelDeploymentReasonRolloutHeldByCache = "RolloutHeldByCache"

	// modelDeploymentReasonRolloutHeldByWeights 是另一种原因导致的相同阻塞状态：替换所需
	// 的模型权重受阻，因此滚动更新原本要删除的副本会继续保留。它单独作为一个 reason，因为
	// 修复方向在于 WeightsReady，且完全不使用 KVCache 的部署也会进入该状态。
	modelDeploymentReasonRolloutHeldByWeights = "RolloutHeldByWeights"

	// modelDeploymentReasonReplacementInProgress 表示本轮调谐中所有存活副本均与渲染结果一致，
	// 但副本数量低于声明值时的状态：某个副本自行离开了（例如 Node 排空、Kueue 回收配额、
	// kubelet 驱逐或手动删除，本轮无法区分具体原因），且当前正在替换该副本。
	//
	// 将其作为独立 Reason 的原因在于读者关心的问题与滚动更新（rollout）恰恰相反。
	// rollout 回答的是“我的修改是否已经生效”；而 replacement 回答的是“在未修改任何配置的情况下，
	// 为什么容量发生了波动”。若把前者当作后者来报告，会误导运维人员去比对并未改动的 spec；
	// 而如果什么都不报告——即该 Reason 所替代的旧做法，返回一个与稳态完全相同的 UpToDate——
	// 则会掩盖部署因外部原因而以低于声明副本数提供服务的这一窗口期。
	modelDeploymentReasonReplacementInProgress = "ReplacementInProgress"

	// modelDeploymentReasonRolloutNotObserved 是本轮未能核实任何副本时给出的结论。
	// 它使用 Unknown 而不是省略该 condition，因为另一种做法（保留已存储的原值）会在刚好
	// 删除了所有副本的那些调谐轮次中继续维持一个权威的 True 状态。
	modelDeploymentReasonRolloutNotObserved = "RolloutNotObserved"
)

// modelDeploymentRollout 记录单次收敛过程对携带旧版 spec 的副本所作出的决策。
//
// 它在调谐决策时即时记录，而非事后根据 Pod 重新推导。期望哈希值属于本轮执行渲染的
// 产物且不会被持久化保存，因此事后回推意味着需要重新渲染一次再作比较——而在唯一关键的
// 时刻（即本轮运行期间 spec 再次被修改时），这回答的其实是一个完全不同的问题。
type modelDeploymentRollout struct {
	// accounted 统计本轮能够确认并担保的副本数量：要么读取了已有副本的哈希值，要么根据
	// 刚刚完成的渲染结果新建了副本。
	//
	// 它用于区分“没有副本过期”和“没有检查任何副本”，否则在该记录的其他字段中这两者
	// 都会表现为零。调谐在写入 status 时无法担保任何副本的情况比想象中更常见：例如销毁流程，
	// 以及 rollout 删除旧副本与创建新副本之间的轮次——此时名称仍被正在终止的副本占用，
	// 因此新建操作也无法落地。
	//
	// 新建操作计入其中，是因为刚刚下发的 Pod 直接由本轮自身的期望状态渲染而来，这比哈希
	// 比较是更强而非更弱的保证。若不计入新建操作，将导致部署的首轮调谐无法给出结论，且
	// 第二轮调谐会为无人改动的 spec 写入状态变更，而本控制器不会这样做。
	accounted int
	// outdated 统计渲染后的 spec 哈希值与期望值不一致的现有副本数量。
	outdated int
	// held 统计因无法解析 KVCache 连接而被保留在原位的过期副本数量。该值永远不会大于 outdated。
	held int
	// heldByWeights 统计因替换副本所需的模型权重受阻而被保留在原位的过期副本数量。
	// 同一角色的副本只会计入 held 或此处之一，不会重复计数，因此两者之和永远不会大于 outdated。
	heldByWeights int
	// olderRenderVersion 统计渲染版本兼容部署下限但严格低于 modelDeploymentRenderVersion 的当前副本数量。
	olderRenderVersion int
}

// observeModelDeploymentRollout 将单轮调谐的 rollout 决策汇总写入 status。
//
// 无法担保任何副本的记录会报告 Unknown，而不会原封不动地保留已存储的原值。
// 这一区别正是关键所在：保留原值会沿用上一轮有效调谐写入的结果，在稳定部署后该值为
// 权威的 True；随后销毁流程删除所有副本且不再担保任何副本，如果不更新，该对象就会在
// 没有任何副本存在的情况下继续声称所有副本都与渲染结果一致。
//
// 共有两种情况会进入此分支：销毁流程，以及 rollout 删除旧副本后、因名称仍被占用而尚未
// 创建新副本的中间轮次。这两种时刻恰恰是副本最不符合最新状态的时刻，因此陈旧结论正好在
// 它被读取的状态下出错。Unknown 与 CacheAttached 在无法采集读数时采用的形式一致。
//
// 本轮决策与观测计数共同决定最终结果，检查顺序即原因的优先级排序。
// 被挂起的 rollout 优先级最高，因为在存储恢复之前无法采取任何其他操作；rollout 优先于
// 副本替换，因为当两者同时发生时 rollout 是总因——它是副本离开的原因，触发该变更的
// 读者需要看到它的提示信息；当所有存活副本均为最新版本但数量仍不足时，才归结为无法用
// spec 变更解释的副本替换。
func observeModelDeploymentRollout(
	holder, md *workercore.ModelDeployment, pods []core.Pod,
	wlByReplica map[types.UID]*kueue.Workload, rollout *modelDeploymentRollout,
) {
	// 该判断放在记录对象本身而非调用点处，因为“未担保任何副本”是本轮检查结果自身的属性，
	// 否则每个调用方都必须自行记住这一条件。
	if rollout == nil || rollout.accounted == 0 {
		ModelDeploymentConditionReplicasUpToDate.Unknown(holder, modelDeploymentReasonRolloutNotObserved,
			"this pass accounted for no replica: it compared no hash and created none, so whether the "+
				"replicas match the current render was not established either way")

		return
	}

	missing, missingWhere := modelDeploymentReplicasMissing(md, pods, wlByReplica)

	switch {
	case rollout.held > 0:
		// 这里不会声称有变更正在等待生效，因为它无法确认这一点。在存储中断期间，期望渲染结果
		// 不包含连接器，因此无论是否有人编辑过部署，每个已挂载副本的哈希值都会不同——一次普通的
		// 存储闪断就会使池中的所有部署进入此状态。指出一个用户可能根本没有作出的变更，出错的概率
		// 远大于正确的概率，因此消息以条件方式陈述后果，并将读者指向存储状态。
		//
		// 同时这里指明的是具体影响范围而非笼统的“spec 编辑”，后者比本保护机制覆盖得更宽。
		// 该保护机制阻断的是对“已存在”副本的变更：在中断期间扩容新增的序号副本仍会创建
		// （不带连接器），因为尚未存在的副本也无法获得尚不存在的地址。真正等待的是会改变
		// 运行中副本渲染 Pod 的编辑——并且是整体等待，因为此时已没有任何组结构层面的编辑能绕过该保护。
		ModelDeploymentConditionReplicasUpToDate.False(holder, modelDeploymentReasonRolloutHeldByCache, fmt.Sprintf(
			"%d of %d replicas differ from what this pass rendered and were left in place: the KV "+
				"cache connection could not be resolved, and recreating them on that alone would "+
				"rebuild every replica whenever the store blinks. Until it resolves, an edit that "+
				"changes a running replica's rendered Pod waits with them -- withheld rather than "+
				"dropped, and it rolls out once the connection returns",
			rollout.held, rollout.outdated,
		))
	case rollout.heldByWeights > 0:
		// 与上一种相同的挂起逻辑但原因不同，而原因正是读者后续操作的依据：如果消息提及缓存，
		// 就会把未声明缓存的部署误导到一个它从未使用过的存储上。
		//
		// 这两个计数器按角色而非按轮次互斥：一个角色可能因缓存而挂起，同时另一个角色因权重而挂起。
		// 该轮次会优先报告上面的缓存情况（缓存挂起的优先级高于一切），而 WeightsReady 仍会指明权重侧的问题。
		ModelDeploymentConditionReplicasUpToDate.False(holder, modelDeploymentReasonRolloutHeldByWeights, fmt.Sprintf(
			"%d of %d replicas differ from what this pass rendered and were left in place: the model "+
				"weights their replacements need are blocked, so deleting them would only remove "+
				"serving replicas. WeightsReady says what blocks them. Until it is True, an edit that "+
				"changes a running replica's rendered Pod waits with them -- withheld rather than "+
				"dropped, and it rolls out once the weights are available",
			rollout.heldByWeights, rollout.outdated,
		))
	case rollout.outdated > 0:
		// 并非一次性全部删除。rollout 每轮每个角色只轮换一个副本，且绝不会在存在副本缺口
		// 或本轮已清理多余副本的情况下同时执行，因此存在差异的副本数量并不等于本轮下发的删除数——
		// 若消息按后者来表述，就相当于声称执行了尚未发生的删除。本轮最多只为每个角色删除一个副本；
		// 观察到被删副本消失的下一轮才会创建其替代副本，如果读者被告知替换已完成，就会停止继续观察。
		ModelDeploymentConditionReplicasUpToDate.False(holder, modelDeploymentReasonRolloutInProgress, fmt.Sprintf(
			"%d replicas differ from what this pass rendered and turn over one per role per pass: "+
				"this pass deleted at most one replica per role, and the pass that finds it gone "+
				"creates the replacement", rollout.outdated,
		))
	case missing > 0 && modelDeploymentRolloutWasInProgress(holder):
		// rollout 造成的副本空缺仍属于 rollout 本身。删除最后一个过期副本的轮次之后，不再有
		// 过期副本残留，且替代副本尚未创建，因此紧随其后的轮次会看到数量不足但无配置差异——
		// 仅从当前状态来看，这与副本自行离开无法区分。若将其判为副本自行离开，就会在运维人员
		// 刚发起的 rollout 中途告诉他们“spec 未发生任何变化”，从而误导他们把 spec 与自身作对比。
		//
		// 上一轮的结论是唯一的传递载体。reconcile 本身无记忆，而本可通过哈希证明来源的那个副本
		// 恰好已经消失。该状态会自动清除：一旦替代副本创建出来，调谐就会进入下方分支并写入
		// 新的结论。
		ModelDeploymentConditionReplicasUpToDate.False(holder, modelDeploymentReasonRolloutInProgress, fmt.Sprintf(
			"%d of the declared replicas are being replaced by the rollout in flight: %s. The last "+
				"replica built from the earlier spec has gone, and the pass that finds it gone "+
				"creates its replacement",
			missing, strings.Join(missingWhere, "; "),
		))
	case missing > 0:
		// 本轮比较的所有副本均无差异，因此该缺口并非本轮可见的 rollout 所致，消息必须明确
		// 说明这一点，因为另外两种解读都会误导读者：RolloutInProgress 会让读者去排查可能并未
		// 改变的 spec，而 UpToDate 在部署以低于声明数量提供服务时读起来却完全像正常稳态。
		//
		// 该结论仅基于实际比较过的内容，而非 spec 本身。早期措辞曾说“spec 未作任何修改”，
		// 仿佛这一维度能够知晓此事；其实不能——创建尚未落地的扩容操作到达此处时与副本离开
		// 看起来完全一样，因为本可区分两者的那个副本正处于缺失状态。准确的表述应严格基于
		// 可观测事实，modelDeploymentReplicasMissing 下方的分支完整记录了这一边界。
		//
		// 消息到此为止，不再猜测副本离开的原因（早期措辞曾这样做）。“副本自行离开”是对本维度
		// 从未观测到的原因作出的断言，且在一种既非罕见也非运维人员操作所致的情况下是错误的：
		// Kueue 抢占会驱逐副本，同时保留被撤回预留的 Workload，于是该缺口到达此处时看起来
		// 完全像副本自行离开。在发生抢占期间告诉运维人员“没有任何操作作用于其部署”，会误导
		// 他们去错误的方向排查原因。配额 condition 会报告该原因，本消息应指向它而非与其冲突。
		ModelDeploymentConditionReplicasUpToDate.False(holder, modelDeploymentReasonReplacementInProgress, fmt.Sprintf(
			"%d of the declared replicas are missing: %s. Every replica this pass could compare "+
				"matched what it rendered, so no rollout is in flight. What removed them is not "+
				"this condition's to say -- a preemption reports itself on the quota condition -- "+
				"and each replacement is created once its ordinal holds no member",
			missing, strings.Join(missingWhere, "; "),
		))
	default:
		if rollout.olderRenderVersion > 0 {
			ModelDeploymentConditionReplicasUpToDate.True(holder, modelDeploymentReasonUpToDate, fmt.Sprintf(
				"every replica matches the declared spec (%d running on an older compatible render version; "+
					"current render version is %d, set annotation %s=%d to roll them)",
				rollout.olderRenderVersion, modelDeploymentRenderVersion,
				modelDeploymentMinRenderVersionAnnotation, modelDeploymentRenderVersion,
			))

			return
		}
		ModelDeploymentConditionReplicasUpToDate.True(holder, modelDeploymentReasonUpToDate,
			"every replica matches what this pass rendered")
	}
}

// modelDeploymentRolloutWasInProgress 读取上一轮调谐写入的结论；该结论仍在 holder 上，
// 因为正在构建的 status 是从当前观测状态复制而来，且本维度尚未写入新值。
//
// 该函数的存在是因为 rollout 的最后一步与非预期的副本离开会产生完全相同的可观测现象：
// 副本不足且无配置差异。集群对象上没有任何信息能将两者区分开——证据已随被删副本一同
// 消失——因此只能根据本控制器上一轮的判定结果而非集群状态来回答这个问题。
func modelDeploymentRolloutWasInProgress(holder *workercore.ModelDeployment) bool {
	for i := range holder.Status.Conditions {
		if holder.Status.Conditions[i].Type == string(ModelDeploymentConditionReplicasUpToDate) {
			return holder.Status.Conditions[i].Reason == modelDeploymentReasonRolloutInProgress
		}
	}

	return false
}

// modelDeploymentReplicasMissing 衡量部署当前副本数与各角色声明数量之间的差距，并标出每个
// 空缺槽位，使最终结论能够精确指向具体副本，而非笼统的角色级缺口数量。
//
// 它能区分的信息：哪个角色的哪些序号处于空缺状态。槽位按序号标签命名，与收敛器创建和移除
// 副本所用的标识相同，因此“role prefill replica 1”能精确指出缺失的槽位，而非仅给出角色的总缺口数。
//
// 它无法区分的信息（任何单轮调谐都无法区分）：空缺槽位此前是否曾被填充过。证据已随副本
// （即其 Pod 以及所有 workload 解析所依赖的归属关系）一同消失，因此副本离开留下的槽位与
// spec 扩容新增的槽位在观测上完全一致。在此交汇的情况包括：创建尚未落地的扩容、自行离开的
// 最大序号副本、以及被手动删除的 workload，它们在观测上都表现为：旁边存在仍有 workload 的副本，
// 而自身是一个空缺槽位。
//
// 对无法区分情况的分类方式：采用角色级代理条件“该角色的任意副本（包括正在离开的副本）背后
// 仍关联着 workload”。当该条件成立时，空缺槽位被视为副本离开——因此上述扩容窗口会报告
// replacement 而非初始组装，只有上一轮的结论（modelDeploymentRolloutWasInProgress）能将其
// 升级判定为 rollout。当该条件不成立时，说明该角色仍在组装首批副本，其缺口会被完全排除：
// 初始创建不属于对任何对象的替换，若计入其中，会导致每个部署的首几轮调谐都被报为 replacement。
//
// 空缺槽位按序号从低到高填充，且数量严格等于缺口数，这与收敛器创建副本的计算逻辑一致：
// 携带无序号副本的角色仍会将该副本计入总数以抵扣缺口，但不会为其占用任何序号槽位。
//
// 被缩容移除的角色会自动排除。清理其 Pod 的轮次会同时删除其 workload，因此它会自动退出
// 该统计，绝不会被误判为 replacement。
func modelDeploymentReplicasMissing(
	md *workercore.ModelDeployment, pods []core.Pod,
	wlByReplica map[types.UID]*kueue.Workload,
) (int, []string) {
	// live 统计的是副本数而非 Pod 数，且必须如此：下方的缺口计算需要用声明数减去它，而声明数
	// 的单位是副本。如果统计 Pod 数，对于每个副本包含多个成员的角色，short 会变成负数，
	// 导致该分支永远不会触发，真正缺失的副本也就无法被指明。当每个副本只有一个成员时，
	// 两种统计结果相同，这也是之前一直将其当作 Pod 计数却未暴露问题的原因。
	//
	// 无论副本是否完整，只要存在即计为存活。这与配额 condition 的判定相反，但针对的是不同
	// 问题：此处判断的是“槽位是否为空”，而容纳着半成品副本的槽位并非空槽位。副本不完整的
	// 问题应由 rollout 修复，并由负责该维度的 condition 报告。
	live := make(map[string]int, len(md.Spec.Roles))
	occupied := make(map[string]map[int]bool, len(md.Spec.Roles))
	for _, view := range modelDeploymentGroupPodsByReplica(pods) {
		live[view.Role]++
		if !view.Seated {
			continue
		}
		if occupied[view.Role] == nil {
			occupied[view.Role] = make(map[int]bool)
		}
		occupied[view.Role][view.Ordinal] = true
	}

	missing := 0
	var where []string
	for i := range md.Spec.Roles {
		role := &md.Spec.Roles[i]
		declared := int(role.Replicas)
		short := declared - live[role.Name]
		if short <= 0 || !modelDeploymentRoleEverAssembled(role.Name, pods, wlByReplica) {
			continue
		}

		var ordinals []int
		for ordinal := 0; ordinal < declared && len(ordinals) < short; ordinal++ {
			if !occupied[role.Name][ordinal] {
				ordinals = append(ordinals, ordinal)
			}
		}
		missing += short
		where = append(where, modelDeploymentNameReplicas(role.Name, ordinals))
	}

	return missing, where
}

// modelDeploymentRoleEverAssembled 报告该角色的任意副本（包括正在离开的副本）背后是否仍有 workload。
//
// 它是一个代理指标而非历史记录，其局限性正如缺失计数注释中所述的边界：它只说明该角色此前
// 完成过组装，并不代表那个空缺槽位曾经存在过。正在离开的副本也会计入，因为持有该副本的
// workload 仍是可观测的归属关系，而且在替换期间收敛器将其删除的瞬间，正是该代理指标绝对
// 不能翻转状态的时刻。
func modelDeploymentRoleEverAssembled(
	role string, pods []core.Pod, wlByReplica map[types.UID]*kueue.Workload,
) bool {
	for i := range pods {
		if modelDeploymentPodRole(&pods[i]) == role && wlByReplica[pods[i].UID] != nil {
			return true
		}
	}

	return false
}

// modelDeploymentNameReplicas 将单个角色的空缺槽位渲染为结论片段：
// "role prefill replica 1"、"role prefill replicas 1, 2"。
func modelDeploymentNameReplicas(role string, ordinals []int) string {
	digits := make([]string, 0, len(ordinals))
	for _, ordinal := range ordinals {
		digits = append(digits, strconvx.Itoa(ordinal))
	}
	noun := "replica"
	if len(ordinals) > 1 {
		noun = "replicas"
	}

	return fmt.Sprintf("role %s %s %s", role, noun, strings.Join(digits, ", "))
}
