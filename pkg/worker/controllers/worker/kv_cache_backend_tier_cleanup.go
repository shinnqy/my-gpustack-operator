package worker

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/utils/stringx"
	"gpustack.ai/gpustack/pkg/worker/kuberess"
)

// kvCacheBackendTierCleanupDeadline 定义了删除操作等待节点层级被清空的时间限制。
//
// 测量标准是从该节点自身的清理 Pod 开始，而非从对象的删除时间戳开始，也不是从本次操作中最旧的 Pod 开始。以
// 删除时间来测量并不有效：此步骤之前的所有情况都是无限制的，因为清理过程会持续进行，直到消费者再次命名后端，
// 直到渲染的工作负载终止——从集群层面来看，仅就这一等待时间而言，当一个节点无法访问时，一个对象就会被保留十
// 二分钟。如果以时间来衡量，那么清理过程就会提前结束，报告每个节点都被废弃，并删除它刚刚创建的 Pods，而实际
// 上它从未尝试过。
//
// 从每个节点自己的清理 Pod 的创建时间开始计算 5m。创建暂时失败的 Pod 会在下一轮才出现，不能用较早节点的时间
// 提前宣告它超时。
//
// 使用计数传递方式反而更糟糕：内存中的计数器在每次控制器重启时都会重置，而被阻塞的节点会永远保持对象处于打开状态。
//
// 因此，Pods 就相当于一个时钟，只要它们未被重新创建，这个时钟就会持续存在。这就是为什么
// ensureKVCacheBackendTierCleanupPod 会让一个失败的 Pod 保持原状，而不进行重建；也是为什么那种根本不会创建
// Pod 的路径——即无法解析的图像——会放弃操作，而不是将其标记为待处理。由 Pods 构成的时钟，一旦被重新启动，就
// 不再属于时钟了。
//
// 这可以很短，因为清理 Pod 会运行成员们运行的镜像，而该镜像已经存在于这个群组所覆盖的每一个节点上，因此 Pod
// 与其工作之间没有需要拉取的依赖。
const kvCacheBackendTierCleanupDeadline = 5 * time.Minute

// 层级清理的事件原因。这些记录会保存在 NODE 上，而非后端系统上：后端系统正在被删除，其相关事件也会随之丢失，
// 而剩余的数据和节点则会与其一同被移除。
const (
	kvCacheBackendEventTierAbandoned     = "KVCacheTierNotCleaned"
	kvCacheBackendEventTierShared        = "KVCacheTierSharedPath"
	kvCacheBackendEventTierPartlyEmptied = "KVCacheTierPartlyEmptied"
)

// cleanKVCacheBackendTier 会清空被删除后端中的磁盘层，且适用于所声明的成员组所覆盖的每个节点。
//
// 当没有剩余待处理事项时，它就会报告完成，这包括放弃操作：超过截止时间后，剩余的节点仍保留其内容，删除操作
// 也会继续进行，因为等待永远不会响应的节点的终结器会留下一个无法被删除的对象。放弃操作并非无声无息——每个保
// 留内容的节点都会收到警告事件和记录的错误信息。
//
// 它在渲染后的工作负载消失之后，以及锁定解除之前运行。这种顺序的两种情况都至关重要：如果成员仍在运行，存储
// 数据就会写入正在被清空的文件目录中；而一旦锁定解除，该对象就会消失，此时根本不会有任何操作进行。
func (r *KVCacheBackendReconciler) cleanKVCacheBackendTier(
	ctx context.Context, kvcb *workercore.KVCacheBackend,
) (done bool, err error) {
	logger := ctrllog.FromContext(ctx)

	member := kvCacheBackendCleanableTier(kvcb)
	if member == nil {
		return true, nil
	}
	path := filepath.Clean(member.LocalDisks[0].Path)

	nodes := new(core.NodeList)
	if err = r.Client.List(ctx, nodes, ctrlcli.MatchingLabels(member.NodeSelector)); err != nil {
		return false, fmt.Errorf("list nodes carrying the disk tier: %w", err)
	}

	// 不再存在的节点不会被等待，也不会被报告。其本地磁盘随之丢失，因此没有需要清理的内容。
	//
	// 假设，因为这种假设会悄然失败：缺少 Node 对象意味着缺少磁盘。当机器仍在运行时被删除的 Node，会留下
	// 本次操作未提及的内容。这里没有任何方法可以找到这样的节点。
	if len(nodes.Items) == 0 {
		return true, nil
	}

	// 一次性读取整个遍历过程的数据，而非每个节点单独读取。这些读取数据来自信息器缓存，因此并非涉及多次
	// 往返操作；而是每个节点都会复制集群中的每个后端数据，在节点循环过程中，每次销毁时每个节点都会针对
	// 每个后端进行复制。一次快照意味着本次遍历中每个节点都需与同一组数据进行比较。
	others := new(workercore.KVCacheBackendList)
	if err = r.Client.List(ctx, others); err != nil {
		return false, fmt.Errorf("list kv cache backends sharing the disk tier: %w", err)
	}

	var pending int
	for i := range nodes.Items {
		node := &nodes.Items[i]

		// 同一目录中的另一个后端层级，就是那种做起来比跳过更糟糕的情况：清空该目录会同时删除该后端
		// 的实时数据。没有任何规则禁止两个后端使用同一个路径，因此这里会进行检查，而不是被忽略。这
		// 是一次检查，而非锁定，这种差异值得提及，而不是由用户自行发现。完全关闭窗口需要对该路径进
		// 行原子性操作，而这一设计变化并未引入这种要求。
		//
		// 该检查所涵盖的是 Pod 的整个生命周期，而非其运行瞬间的情况，此评论的早期版本就犯了这个错误：
		// 它将窗口描述为位于此读取操作与 Pod 的`rm`操作之间，而实际上共享可以在`rm`操作仍在进行时随
		// 时开始。每次迭代重新运行检查，并不足以单独实现已经开始的删除操作——Pod 是由之前的迭代构建
		// 的，没有任何因素会改变这一决定——因此跳过该节点会导致此分支的删除操作仅停留在未完成状态，
		// 而另一个后端现在仍持有该目录。因此，Pod 是被终止了，而非仅仅未被创建。
		//
		// 仍然被处理的操作时间，比两次遍历之间的间隔要长。`rm` 操作从共享开始的那一刻一直持续到检查
		// 器发现它为止，随后又持续到 Pod 的终止过程，因为删除操作只是要求 kubelet 停止工作，而不是
		// 真正移除任何内容。整个这个过程就是第二个事件存在的原因：另一个后端的数据被部分移除，与一
		// 个被弃置的目录相比，这是不同的情况。
		if holder := kvCacheBackendTierSharedWith(kvcb, others, path, node); holder != "" {
			stopped, serr := r.stopKVCacheBackendTierCleanupPod(ctx, kvcb, node)
			if serr != nil {
				return false, serr
			}
			r.recordWarning(node, kvCacheBackendEventTierShared,
				"%s was not emptied for deleted KVCacheBackend %q: KVCacheBackend %q declares an "+
					"overlapping path on this node and emptying it would remove that backend's data",
				path, kvcb.Name, holder)
			if stopped {
				r.recordWarning(node, kvCacheBackendEventTierPartlyEmptied,
					"%s may hold a partial result: emptying it for deleted KVCacheBackend %q was "+
						"already running when KVCacheBackend %q was found to declare an overlapping "+
						"path, and was stopped -- the removal ends as that cleanup pod terminates, "+
						"not at once, so check that backend's data on this node",
					path, kvcb.Name, holder)
			}
			logger.Info("skipped a disk tier shared with another backend",
				"node", node.Name, "path", path, "sharedWith", holder, "stoppedRemoval", stopped)
			continue
		}

		finished, pod, cerr := r.ensureKVCacheBackendTierCleanupPod(ctx, kvcb, member, path, node)
		if cerr != nil {
			return false, cerr
		}
		if finished {
			continue
		}

		// 各节点的 5m 超时都从自己的 Pod 创建时间计起。创建失败后重新入队可能产生较晚的 Pod，使用最
		// 早创建时间会误报这些节点已经超时。
		//
		// 没有 Pod 意味着没有时钟，也无需报告任何内容：这个节点正在等待另一个 Pod 被构建，这是另一
		// 个版本中过时的 Pod 被清除后的状态。
		if pod == nil || pod.CreationTimestamp.IsZero() ||
			time.Since(pod.CreationTimestamp.Time) <= kvCacheBackendTierCleanupDeadline {
			pending++
			continue
		}

		// 消息中只说明了已知的信息，即该目录并非确实为空——而不是说它仍然包含数据，这一点我们无法确
		// 认。这种差异并非迂回的：在截止日期仍未完成的 Pod 也从未运行过，最有可能的原因是该目录根本
		// 不在该节点上，而“仍然包含数据”是一种错误的断言。这个阶段被如此命名，是为了让阅读该事件的人能区分两者。
		//
		// 该消息不包含已过去的时间，仅包含其跨越的截止时间。当其他节点仍处于待处理状态时，该操作会
		// 每隔几秒执行一次，因此每次执行时都会再次触发此分支——而 client-go 的重复操作则被合并为一次
		// 事件，其计数仅在消息保持不变时有效。精确的年龄使得每次重复操作都成为一个独立的事件，正是
		// 通过这种方式，一个被废弃的节点会转化为一百个这样的节点。
		r.recordWarning(node, kvCacheBackendEventTierAbandoned,
			"%s was not confirmed empty for deleted KVCacheBackend %q: its cleanup was still %s "+
				"after %s and the deletion was not held open for it; a Pod that never left pending "+
				"usually means the directory does not exist on this node",
			path, kvcb.Name, strings.ToLower(string(pod.Status.Phase)),
			kvCacheBackendTierCleanupDeadline)
		// 日志中保留了精确的年龄信息，而该事件故意没有这么做：日志行没有通过文本进行去重处理，因此
		// 精度在此处并不重要。
		logger.Error(nil, "gave up emptying a disk tier",
			"node", node.Name, "path", path, "phase", pod.Status.Phase,
			"age", time.Since(pod.CreationTimestamp.Time),
			"deadline", kvCacheBackendTierCleanupDeadline)
	}

	if pending > 0 {
		return false, nil
	}

	// 此时每个节点都已清空、跳过或报告问题，可回收本轮清理用的 Pod，避免它们滞留在控制器的命名空间等待所有者回收。
	//
	// 代价是，失败的 Pod 的日志也会随之丢失，而这种情况正是节点保留内容的原因。剩下的就是事件本身，因此
	// 它提到了路径、后端和截止日期，而不仅仅是说明某件事被跳过了。
	//
	// 回收清理 Pod 失败时记录日志，仍允许调用者释放已经完成清理的后端。如果此处返回错误，下一轮会为已成
	// 功回收的 Pod 重建对象并重置计时，导致无法清理的节点反复获得新的期限。
	//
	// 记录它们是很安全的，因为这些 Pods 带有 ownerReference 指向后端：当对象被移除后，API 服务器会将其
	// 删除，因此这种删除操作决定了何时进行收集，而非是否进行收集。关于此内容及为何不会产生阻塞，请参考
	// kvCacheBackendTierCleanupPod 文档。
	if cerr := r.deleteKVCacheBackendTierCleanupPods(ctx, kvcb); cerr != nil {
		logger.Error(cerr, "collect the disk tier cleanup pods of a finished teardown",
			"backend", kvcb.Name)
	}

	return true, nil
}

// stopKVCacheBackendTierCleanupPod 会在单个节点上结束该后端清理操作，并报告是否存在正在进行的删除操作。
//
// 这就是在某一轮操作开始时做出的决策如何导致之前一轮操作被取消的结果。共享路径检查在每轮操作中都会执行，但
// `rm` 在自身开始和结束之间不会询问任何内容——因此如果没有这一机制，对于已经进行到一半的删除操作，检查会拒绝
// 开始，表现为拒绝行为，同时也会视为完成。
//
// 事件描述观测到的状态。即使清理 Pod 已在终止，也要报告该节点上的其他后端数据可能已部分删除。控制器若在删除
// Pod 后、发布事件前重启，下次仍可补报。消息不含时间或轮次，重复事件可合并计数。
//
// UID 被检查了两次，原因也不同。标签决定了这个通过 Get 获取的 Pod 是否属于我们——其名称源自后端和节点，因此
// 会在不同版本中重复出现。删除时的前提条件则决定了它是否仍属于我们：在 Get 和删除之间，这个 Pod 可以完成终
// 止过程，下一个版本的清理 Pod 可以使用相同的派生名称；而仅通过名称进行删除则会导致其终止。被拒绝的前提条件
// 会作为冲突返回，意味着现在有其他对象的存在。
//
// 结束操作并非瞬间完成——Pod 会按照其自身预算完成终止，而`rm`也会随之执行——因此这限制了删除操作，而非完全取
// 消它。这与放弃路径所采取的策略相同，原因也相同：部分为空目录是一种成本，而无人能阻止的针对其他后端实时数
// 据的删除操作则是一种损失。
func (r *KVCacheBackendReconciler) stopKVCacheBackendTierCleanupPod(
	ctx context.Context, kvcb *workercore.KVCacheBackend, node *core.Node,
) (stopped bool, err error) {
	key := ctrlcli.ObjectKey{
		Name:      kvCacheBackendTierCleanupPodName(kvcb, node),
		Namespace: kuberess.SystemNamespaceName,
	}
	pod := new(core.Pod)
	if err = r.Client.Get(ctx, key, pod); err != nil {
		return false, ctrlcli.IgnoreNotFound(err)
	}
	if pod.Labels[kvCacheBackendTierCleanupUIDLabel] != string(kvcb.UID) {
		return false, nil
	}

	// 已经进行中，并已报告如此。删除操作不会重新发布：它不会改变任何事情，当前状态指的是移除行为，而非谁结束了它。
	if pod.DeletionTimestamp != nil {
		return true, nil
	}

	uid := pod.UID
	err = r.Client.Delete(ctx, pod, &ctrlcli.DeleteOptions{
		Preconditions: &meta.Preconditions{UID: &uid},
	})
	if err != nil {
		if kerrors.IsNotFound(err) || kerrors.IsConflict(err) {
			return false, nil
		}
		return false, fmt.Errorf("stop the disk tier cleanup pod on a shared path: %w", err)
	}

	return true, nil
}

// kvCacheBackendTierCleanupPods 列出了属于某个后端的清理 Pods。
func (r *KVCacheBackendReconciler) kvCacheBackendTierCleanupPods(
	ctx context.Context, kvcb *workercore.KVCacheBackend,
) ([]core.Pod, error) {
	pods := new(core.PodList)
	err := r.Client.List(ctx, pods,
		ctrlcli.InNamespace(kuberess.SystemNamespaceName),
		ctrlcli.MatchingLabels(map[string]string{
			kvCacheBackendTierCleanupLabel:    kvcb.Name,
			kvCacheBackendTierCleanupUIDLabel: string(kvcb.UID),
		}))
	if err != nil {
		return nil, fmt.Errorf("list the disk tier cleanup pods: %w", err)
	}
	return pods.Items, nil
}

// kvCacheBackendCleanableTier 返回了该后端被要求清空的磁盘层，以及声明此层的群组中的节点选择器。
//
// 准入机制只允许一个成员组声明 localDisks，因此只有一层和一个选择器，而非多层级。循环仍会针对每个成员组执行，
// 因为其他地方制定的规则并非该函数能够读取的信息。它返回的是整个成员组，而非仅一层，因为清理操作必须针对该
// 组所运行的镜像进行。一个成员组可以覆盖整个后端镜像，而清理 Pod 的承诺——即它与其工作之间不会进行拉取操作——
// 仅适用于该成员组已拉取的镜像。
func kvCacheBackendCleanableTier(
	kvcb *workercore.KVCacheBackend,
) *workercore.KVCacheBackendMember {
	if kvcb.Spec.Connection.Managed == nil {
		return nil
	}
	for i := range kvcb.Spec.Connection.Managed.Members {
		member := &kvcb.Spec.Connection.Managed.Members[i]
		if len(member.LocalDisks) == 0 || !member.LocalDisks[0].CleanAfterDelete {
			continue
		}
		if strings.TrimSpace(member.LocalDisks[0].Path) == "" {
			continue
		}
		return member
	}
	return nil
}

// kvCacheBackendTierSharedWith 返回另一个会因清空该节点目录而丢失磁盘层数据的后端名称，未发现重叠时返回空字符串。
//
// 重叠而非相等：嵌套在另一个层级中的层级会被清空外层目录而移除，就像相同的路径一样，这种移除是必然的。
//
// 它采用后端处理，而不是直接读取，因此一次快照可以覆盖遍历中的所有节点。
func kvCacheBackendTierSharedWith(
	kvcb *workercore.KVCacheBackend, others *workercore.KVCacheBackendList, path string, node *core.Node,
) string {
	for i := range others.Items {
		other := &others.Items[i]
		if other.Name == kvcb.Name || other.Spec.Connection.Managed == nil {
			continue
		}
		// 自我删除且被要求清空其层级的后端服务，并不属于此路径上的有效声明：其自身的清理操作会移除
		// 相同的内容。没有这种操作的话，两个共享同一目录且被一起删除的后端服务会互相看到对方被删除
		// 的内容，各自跳过，而两位管理员要求移除的内容则仅以“共享路径”事件的形式保留下来。
		//
		// 两个部分都是必需的。删除后端时如果不设置 cleanAfterDelete，就是故意保留其内容——这就是默认
		// 设置的含义——因此清空目录会破坏那些由所有者要求保留的数据，这比避免的死锁情况更糟糕。
		if other.DeletionTimestamp != nil && kvCacheBackendCleanableTier(other) != nil {
			continue
		}
		for j := range other.Spec.Connection.Managed.Members {
			member := &other.Spec.Connection.Managed.Members[j]
			if len(member.LocalDisks) == 0 {
				continue
			}
			if !kvCacheBackendPathsOverlap(path, filepath.Clean(member.LocalDisks[0].Path)) {
				continue
			}
			// 另一个后端只有在其自身群组选择它时才能到达此节点。
			if !kvCacheBackendNodeSelected(member.NodeSelector, node.Labels) {
				continue
			}
			return other.Name
		}
	}
	return ""
}

// kvCacheBackendNodeSelected 判断节点是否匹配指定成员组的 nodeSelector。
//
// 沿用 kubelet 对 spec.nodeSelector 的匹配规则。节点必须包含选择器的每个键值对，空选择器匹配所有节点。把空
// 值语义写在这里，便于直接核对。
func kvCacheBackendNodeSelected(selector, nodeLabels map[string]string) bool {
	for k, v := range selector {
		if nodeLabels[k] != v {
			return false
		}
	}
	return true
}

// kvCacheBackendPathsOverlap 判断清空任一目录是否会同时删除另一个目录的内容。
func kvCacheBackendPathsOverlap(a, b string) bool {
	if a == b {
		return true
	}
	return strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}

// ensureKVCacheBackendTierCleanupPod 负责管理某个节点的清理 Pod，并报告该节点是否已完成清理。
//
// Pod 包含明确的 nodeName 属性，且没有容忍逻辑，这是有意为之的：绕过调度器使其能够运行在被封锁或受污染节点
// 上，这也使得真正消失的节点会立即失败，而不是停留在待处理状态直到截止日期。
func (r *KVCacheBackendReconciler) ensureKVCacheBackendTierCleanupPod(
	ctx context.Context,
	kvcb *workercore.KVCacheBackend,
	member *workercore.KVCacheBackendMember,
	path string,
	node *core.Node,
) (finished bool, pod *core.Pod, err error) {
	// 在图像解析之前会先读取现有的 Pod，顺序至关重要。该图像仅用于创建 Pod，因此先解析它，就能让在多次
	// 迭代中发生变化的设置反映出一个已运行 Pod 的节点：如果清理成功，则会被报告为被放弃；如果仍在运行，
	// 则会被因非其截止时间的原因而放弃。
	key := ctrlcli.ObjectKey{
		Name:      kvCacheBackendTierCleanupPodName(kvcb, node),
		Namespace: kuberess.SystemNamespaceName,
	}
	pod = new(core.Pod)
	err = r.Client.Get(ctx, key, pod)
	switch {
	case err == nil:
		// 不同代后端可复用同一个名字，Get 只按名称查询，所以还要比较 UID。旧清理 Pod 若已成功，可能
		// 被误当成本次清理完成；若仍在其他阶段，旧创建时间可能使新后端刚开始就被判定超时。
		if pod.Labels[kvCacheBackendTierCleanupUIDLabel] != string(kvcb.UID) {
			if derr := r.Client.Delete(ctx, pod); derr != nil && !kerrors.IsNotFound(derr) {
				return false, nil, fmt.Errorf("delete a disk tier cleanup pod of another "+
					"incarnation: %w", derr)
			}
			return false, nil, nil
		}
		if pod.Status.Phase == core.PodSucceeded {
			return true, pod, nil
		}
		// 其他所有阶段仍处于运行中，且 Pod 永远不会被删除以便重新启动。重新启动是 kubelet 的职责：
		// 该 Pod 请求了 RestartPolicyOnFailure 策略，因此即使是短暂的故障——例如挂载暂时不可用，或节
		// 点在运行过程中重启——也会通过延迟重试机制重新启动。根据实时 kubelet 的监控数据来看：每次容
		// 器失败都会使 Pod 保持 Running 状态，且重新启动次数逐渐增加，根本不会达到 Failed 状态。
		//
		// 删除一个 Pod，然后下一次迭代重新创建它，这将导致任务截止日期的失效，因为截止日期就是该
		// Pod 的创建时间戳：每次迭代都会创建一个新的 Pod，这会抹去已过去的时间，过期的状态永远不会
		// 成为现实，而且一个永远无法被清理的节点会永远保持后端处于开放状态。
		return false, pod, nil
	case !kerrors.IsNotFound(err):
		return false, nil, fmt.Errorf("get the disk tier cleanup pod: %w", err)
	}

	// 是 GROUP 的镜像，而非后端服务。一个组可以拥有自己的镜像，而在这里运行整个后端的镜像则会导致某个原
	// 本没有该镜像的节点被覆盖——而这正是这个 Pod 不应该经历的等待情况。
	image := member.Image
	if image == "" {
		if image, err = resolveKVCacheBackendImage(ctx, kvcb); err != nil {
			// 放弃这里，不再等待。没有镜像就无法运行，而在这个分支上也不存在任何 Pod，因此也没
			// 有时钟：被报告为待处理状态，这个节点将等待一个永远不会开始的截止日期，因为该截止
			// 日期是以从未创建的 Pod 为基准计算的。那么，终结器会保持后端服务开放状态，直到操作
			// 员没有镜像为止，这就是那个没人能删除的对象，而此功能的存在就是为了防止这种情况发生。
			//
			// 因此，在相同的事件下，节点在截止日期过后会采用相同的退出方式，消息中将图像称为原因，而非节点。
			r.recordWarning(node, kvCacheBackendEventTierAbandoned,
				"%s still holds data from deleted KVCacheBackend %q: no image could be resolved to "+
					"empty it, and the deletion was not held open for it (%v)",
				path, kvcb.Name, err)
			ctrllog.FromContext(ctx).Error(err, "gave up emptying a disk tier with no image to run",
				"node", node.Name, "path", path)
			return true, nil, nil
		}
	}

	pod = kvCacheBackendTierCleanupPod(kvcb, key, image, path, node)
	if err = r.Client.Create(ctx, pod); err != nil && !kerrors.IsAlreadyExists(err) {
		return false, nil, fmt.Errorf("create the disk tier cleanup pod: %w", err)
	}
	// 刚刚创建。保持原样返回，因此调用者根据当前所在的 Pod 来计算此节点的时间，而不是根据最初创建的那个 Pod 来计算。
	return false, pod, nil
}

// kvCacheBackendTierCleanupPod 为一个节点生成清理磁盘层的 Pod。
//
// 它会移除目录中的内容，但不会删除目录本身。无论是谁创建了该节点，都为其指定了所有者，而该操作并未进行选择；
// 该节点可能还挂载了文件系统；删除它则会导致以上三件事全部失效。
func kvCacheBackendTierCleanupPod(
	kvcb *workercore.KVCacheBackend, key ctrlcli.ObjectKey, image, path string, node *core.Node,
) *core.Pod {
	pod := &core.Pod{
		ObjectMeta: meta.ObjectMeta{
			Name:      key.Name,
			Namespace: key.Namespace,
			Labels: map[string]string{
				kvCacheBackendTierCleanupLabel:    kvcb.Name,
				kvCacheBackendTierCleanupUIDLabel: string(kvcb.UID),
			},
			// 由后端管理，因此即使此控制器无法访问，API 服务器仍会收集这些 Pods。销毁过程会在其
			// 完成路径上自行删除它们，但此路径并非唯一的删除途径：被强制删除的对象、被清除的终
			// 结器，或者一个在创建 Pod 与收集之间停止的控制器，否则它们会留在操作系统的命名空间
			// 中，没有任何东西可以移除它们。
			//
			// blockOwnerDeletion 未被设置。设置该值将使这些 Pods 能够保留后端自身的删除操作，而
			// 这正是整个功能的存在目的所无法避免的缺陷——这也是它们没有包含最终化器的原因。
			OwnerReferences: []meta.OwnerReference{
				{
					APIVersion: workercore.GroupVersion.String(),
					Kind:       "KVCacheBackend",
					Name:       kvcb.Name,
					UID:        kvcb.UID,
				},
			},
		},
		Spec: core.PodSpec{
			NodeName: node.Name,
			// OnFailure，因此 kubelet 会原样重试容器，而 Pod 会持续存在直至其失败。这就是让创建
			// 时间戳成为一个有效截止时间的原因：另一种方案——永不——以及此控制器会删除失败的 Pod，
			// 这样下一次迭代就会创建新的 Pod——每次迭代都会重置时间，因此放弃的截止时间便不会触发。
			RestartPolicy: core.RestartPolicyOnFailure,
			// 与成员们带来的相同凭证。没有这些凭证，私有存储镜像会让这个 Pod 处于待处理状态，直
			// 到截止日期，然后该节点会被报告为被遗弃，而原因与节点本身无关。
			ImagePullSecrets: kvcb.Spec.ImagePullSecrets,
			Containers: []core.Container{
				{
					Name:  "clean",
					Image: image,
					// glob 不会覆盖 dotfiles，因此存储中自己的隐藏条目会以独立名称存在。
					// 该目录本身永远不会作为 rm 的参数。
					//
					// 最后的命令就是判决结果，它只是一个测试，而非正式报告。之前的版本
					// 以 `ls -A /tier | wc -l` 结束，无论计算多少，最终都会返回零，因此
					// 部分移除操作会达到 PodSucceeded 状态，并被记录为已清理节点。这正
					// 是该功能旨在让无法实现的失败情况：放弃必须表现得非常明显，而一个
					// 无法出错的管道则会保持沉默。
					//
					// 仅计数功能在无法读取的目录上仍不会失败，此评论的早期版本却声称可
					// 以。《ls》会将错误输出到标准错误流，不会输出到标准输出流；`wc -l`
					// 会打印出 0，因此测试通过——即内容从未被修改、被记录为空的节点。因
					// 此，可读性是通过其独立命令来测试的，此时 `set -e` 可以依据其状态
					// 进行处理。这与成员调查中的条目 `-1` 是同一问题，也是同一解决方案：
					// 计数功能无法包含以计数为特征的失败情况。
					//
					// `rm -rf`与`|| true`一起使用也是不被允许的，而且其实也不需要这样：
					// `-f`已经会忽略不存在的操作数，而不匹配的 glob 就会按照这种方式展
					// 开。以`sh`为基准计算：这三个 glob 在空目录下执行`set -e`时会返回零。
					Command: []string{
						"sh", "-c",
						`set -e; rm -rf -- /tier/* /tier/.[!.]* /tier/..?*; ` +
							`ls -A /tier >/dev/null 2>&1; ` +
							`[ "$(ls -A /tier | wc -l)" -eq 0 ]`,
					},
					VolumeMounts: []core.VolumeMount{
						{Name: "tier", MountPath: "/tier"},
					},
				},
			},
			Volumes: []core.Volume{
				{
					Name: "tier",
					VolumeSource: core.VolumeSource{
						HostPath: &core.HostPathVolumeSource{
							Path: path,
							// 使用 `Directory` 而不是 DirectoryOrCreate，因为创建路径会留下管
							// 理员从未要求的空目录——这也是为什么这个操作器无法首先准备层级的原因。
							//
							// 代价在于，不存在的路径并非简单的“不操作”，此评论的早期版本也这么
							// 说过。kubelet 未能通过类型检查，Pod 处于 FailedMount 状态等待中，
							// 且节点在报告之前需要等待整个截止时间。Pod 上没有任何内容能区分其
							// 与其他待处理状态：FailedMount 是一个事件，而非一种状态，因此“放
							// 弃”事件只是对这一阶段的命名，而非表明节点仍持有其无法看到的数据。
							Type: ptr.To(core.HostPathDirectory),
						},
					},
				},
			},
		},
	}
	// 非强制锁定。仓库的终结器标记了需要操作员注意其消失的对象，而这个 Pod 则恰恰相反：它仅存在一次卸载
	// 过程，而会移除其终结器的对象是后端服务，稍后会被删除。如果处于锁定状态，它作为无法被删除的 Pod 会
	// 持续存在，直到其所有者死亡——这正是整个功能设计要避免的失败情况，只是更底层的问题。
	return pod
}

// kvCacheBackendTierCleanupLabel 会找到某个后端下的清理 Pods，而 kvCacheBackendTierCleanupUIDLabel 则会将其
// 限制为某个 INCARNATION 下的清理项。
//
// 后端重建后可能沿用名称，先前遗留的 Pod 也会匹配同一标签。清理超时按 Pod 创建时间计算，须同时比对后端 UID，
// 避免新后端首次清理就被旧时间戳判定为超时。
const (
	kvCacheBackendTierCleanupLabel    = "kvcache.gpustack.ai/tier-cleanup-of"
	kvCacheBackendTierCleanupUIDLabel = "kvcache.gpustack.ai/tier-cleanup-uid"
)

// kvCacheBackendTierCleanupPodNameLimit 是 API Server 接受的 Pod 名称长度上限。
//
// 与实时服务器进行对比，而不是回顾过去，因为脑海中想到的数字是错误的：一个 Pod 名称可以作为 DNS 子域被验证，
// 因此为 253，而不是 DNS 标签中的 63。在 v1.36.1 版本中通过服务器端模拟测试验证，253 被接受，而 254 被拒绝。
const kvCacheBackendTierCleanupPodNameLimit = 253

// kvCacheBackendTierCleanupPodName 为指定节点生成清理 Pod 名称。
//
// 无论是名称还是构建方式都低于限制阈值。节点名称与 KVCacheBackend 名称都属于 DNS 子域，因此二者都可以单独使
// 用整个 253 范围；而通过拼接它们构建的名称在那些拥有较长名称的集群中则会被拒绝。哈希值同时包含这两种身份，
// 因此对于执行 `kubectl get pods` 的操作者来说，可识别的前缀是存在的；只要不符合要求，就可以取消该前缀的使
// 用，而不会造成两个节点或两个后端名称的冲突。
//
// 前缀会被完全删除，而不是被截断。KVCacheBackend 这个名称可能包含点号，如果只截取其中的一个点，就会留下以"-
// "开头的内存段，而 API 服务器会像拒绝过长的名称一样拒绝它——用错误位置的点号来替换需要长名称的缺陷，这其实是一种浪费。
//
// 出错可不是表面问题。被拒绝的 Create 操作会在每次清理过程中失败，而终结器会留下无法删除的后端残留，这正是
// 该功能存在的意义——防止这种情况发生。
func kvCacheBackendTierCleanupPodName(kvcb *workercore.KVCacheBackend, node *core.Node) string {
	// 分隔符很重要：哈希函数会按从前往后的顺序写入其参数，因此没有分隔符（"a"、"bc"）和（"ab"、"c"）会
	// 构成同一个 Pod。任一名称中都不能出现 `/`。
	suffix := "-tier-clean-" + stringx.SumByFNV64a(kvcb.Name, "/", node.Name)
	if len(kvcb.Name)+len(suffix) <= kvCacheBackendTierCleanupPodNameLimit {
		return kvcb.Name + suffix
	}
	return "kvcache" + suffix
}

// deleteKVCacheBackendTierCleanupPods 删除当前后端为清理磁盘层创建的 Pod。
func (r *KVCacheBackendReconciler) deleteKVCacheBackendTierCleanupPods(
	ctx context.Context, kvcb *workercore.KVCacheBackend,
) error {
	pods, err := r.kvCacheBackendTierCleanupPods(ctx, kvcb)
	if err != nil {
		return err
	}
	for i := range pods {
		// 超时后连正在运行的清理 Pod 也必须删除。终结器一旦释放，新后端就可能占用并写入同一目录。若
		// 允许旧 Pod 的 rm 继续运行，就会删掉新数据，仅依赖所有者引用的异步回收无法限定这一时间窗口。
		//
		// 这两种成本并非同等大小。中断操作意味着目录被部分清空，这是“放弃事件”中已经报告的，也是有
		// 人要求移除的内容。不中断操作则意味着销毁没人要求移除的数据。因此，移除操作在此结束，该事
		// 件显示清理工作当时仍在进行中。
		//
		// 简单的删除操作，因为首先没有需要释放的内容，这些 Pods 没有最终处理程序。关于为何不能如此，
		// 请参考 kvCacheBackendTierCleanupPod。
		if err = r.Client.Delete(ctx, &pods[i]); err != nil && !kerrors.IsNotFound(err) {
			return fmt.Errorf("delete the disk tier cleanup pod: %w", err)
		}
	}
	return nil
}
