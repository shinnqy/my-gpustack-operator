package worker

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/worker/kuberess"
	"gpustack.ai/gpustack/pkg/worker/kvcache/mooncake"
)

// 当某个后端层的层级目录在后端启动时已有内容时，会记录 kvCacheBackendEventTierReused 事件。
const kvCacheBackendEventTierReused = "KVCacheTierNotEmpty"

// reportKVCacheBackendTierReuse 会一次性决定此后端是起始于一个非空目录，并记录其是否继续存在。
//
// 它只报告内容，没有其他行为：它不会清空目录，不会在其中写入标记，也不会阻止后端启动。重新使用路径是合理的
// 行为——有意恢复某个层级则更为明显——因此，唯一合理的做法就是明确指出这一点。
//
// 为什么是条件，而不仅仅是事件。问题是“当这个后端启动时目录是空的吗？”在第一次写入之后，集群上的任何内容都
// 无法回答这个问题：成员自己的数据与其他人的数据无法区分。因此，无法像其他状态那样根据观测到的状态来重新计
// 算，而这两种替代方案都更糟糕：
//
//   - 实测发现，不能用“尚未挂载任何内存段”作为观测前提。勘察结果可读到首个内存段挂载之间只有几秒，调谐可能
//     错过。反过来，若一个内存段也挂载不了，该前提会一直成立，使事件反复出现。
//   - 在目录中写标记文件可以长期记录结果，但会未经请求改动管理员的目录。
//
// 因此，该条件仅被编写一次便不再进行任何操作，而 `Exists` 则负责确保这一点。这是一段有意为之的、非基于级别
// 的状态，这种状态在其他情况下会每次重新计算，而它的存在是因为其记录内容不再具有可观测性。
func (r *KVCacheBackendReconciler) reportKVCacheBackendTierReuse(
	ctx context.Context, kvcb, holder *workercore.KVCacheBackend,
) {
	if kvcb.Spec.Connection.Managed == nil {
		return
	}
	// 已经决定好了。不会再重新审视，因此在这个后端数据上重新启动的成员无法实现一次全新的开始，也无法避
	// 免重复出现的问题。
	if KVCacheBackendConditionTierWasEmpty.Exists(holder) {
		return
	}

	logger := ctrllog.FromContext(ctx)

	var wanted, surveyed int
	reused := map[string]int{}

	for group := range kvcb.Spec.Connection.Managed.Members {
		if len(kvcb.Spec.Connection.Managed.Members[group].LocalDisks) == 0 {
			continue
		}

		// 按期望节点数判断是否收齐报告。尚未创建、正在调度或正在拉取镜像的 Pod 还不在列表里，单数已
		// 有 Pod 会过早判定完成。使用 DaemonSet 的期望数可以沿用它对选择器和可放置节点的判断。
		ds := new(apps.DaemonSet)
		err := r.Client.Get(ctx, ctrlcli.ObjectKey{
			Name:      mooncake.MemberObjectName(kvcb, group),
			Namespace: kuberess.SystemNamespaceName,
		}, ds)
		if err != nil {
			// 尚未渲染，或无法阅读。无论哪种情况，当前都无法解决该问题，而解决它将会是永久性的。
			if !kerrors.IsNotFound(err) {
				logger.Error(err, "read the member daemonset to size the disk tier survey",
					"group", group)
			}
			return
		}
		wanted += int(ds.Status.DesiredNumberScheduled)

		pods := new(core.PodList)
		err = r.Client.List(ctx, pods,
			ctrlcli.InNamespace(kuberess.SystemNamespaceName),
			ctrlcli.MatchingLabels(mooncake.MemberSelectorLabels(kvcb, group)))
		if err != nil {
			logger.Error(err, "list member pods to read the disk tier survey", "group", group)
			return
		}

		for i := range pods.Items {
			pod := &pods.Items[i]
			// 一个替换 Pod 无法回答关于过去的问题。该调查在自身成员之前进行，因此节点上的第一个
			// Pod 会报告该层级在后端写入内容之前的情况。后来重新创建的 Pod——可能是节点重启、驱
			// 逐或镜像变更——会再次进行相同的调查，但所针对的层级已被当前后端填充，其记录会被视
			// 为其他人的遗留内容。无论哪种情况都无法作为证据，因此未被计入任何统计中，也不会被
			// 算作未回答的节点，否则会永远阻碍判断结果。
			if !kvCacheBackendTierSurveyInScope(kvcb, pod) {
				continue
			}
			entries, ok := kvCacheBackendTierSurveyReading(pod)
			if !ok {
				continue
			}
			surveyed++
			if entries > 0 {
				reused[pod.Spec.NodeName] = entries
			}
		}
	}

	// 这两个版本不需要使用相同的证据，对它们进行同等对待才使得部分调查变得危险。一个节点报告有内容时，
	// 无论其他节点怎么说，都会判定“该层级为空”，因此针对当前数据发出的警告也会生效。“该层级为空”这一结
	// 论会适用于未回复的节点，因此会等待所有成员 Pod 都完成写入操作——如果有一个 Pod 仍在拉取数据，那么
	// 基于某个含有内容的节点，就会导致无法得出明确的结论；而“一次写入”规则则使这种状态永久化。
	//
	// 代价是，那种在单个节点上永远无法成功的调查——比如没有外壳的图像、无法读取的挂载点——会将状态标记为“
	// 不存在”而非“错误”。这才是正确的做法：标记为“不存在”意味着问题未被解答，这是事实，而且有文档说明，读者不必猜测。
	if len(reused) == 0 {
		if wanted == 0 || surveyed != wanted {
			return
		}
		KVCacheBackendConditionTierWasEmpty.True(holder, "Empty",
			fmt.Sprintf("the disk tier was empty on all %d node(s) carrying it when this backend "+
				"started", wanted))
		return
	}

	nodes := make([]string, 0, len(reused))
	for node := range reused {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)

	parts := make([]string, 0, len(nodes))
	for _, node := range nodes {
		parts = append(parts, fmt.Sprintf("%s: %d", node, reused[node]))
	}
	message := fmt.Sprintf("the disk tier already held entries when this backend started (%s); "+
		"nothing was removed, and a key written here can read back as content the previous backend "+
		"left", strings.Join(parts, ", "))

	KVCacheBackendConditionTierWasEmpty.False(holder, "PreexistingContent", message)
	// 在满足条件时仅触发一次事件，在过渡过程中触发，而非每次迭代都触发：条件是读者后续会发现的，而事件
	// 则是当前正在观看的人能感知到的内容。需要像处理清理工作一样进行保护，因为如果没有记录器，控制器在
	// 此时会陷入恐慌。
	r.recordWarning(kvcb, kvCacheBackendEventTierReused, "%s", message)
}

// kvCacheBackendTierSurveyWindow 表示在后端创建后，其成员 Pods 仍被视为第一代，因此仍能记录该层级在运行前的状态。
//
// 目前没有明确的标识。没有任何记录表明哪个 Pod 是第一个出现在某个节点上的，也没有状态字段能表明存储系统首次
// 能够写入的时刻，因此一个在后端系统初始化后很久才创建的 Pod，与在初始滚动更新期间创建的 Pod 之间的区别，仅
// 通过经过的时间来判定，除此之外没有其他依据。
//
// 选择这个方案并非因为其精确性，而是因为其在失败时的表现。时间太短，滚动更新过程缓慢——包括调度和冷启动图像
// 拉取——导致该问题不存在，这确实说明了问题未被解决。时间太长，则会被认为窗口内存在被替换的 Pod，而这正是为
// 了阻止这种情况而存在的错误指控。因此，这种方案对滚动更新来说过于宽松，且时间还远短于后端系统的运行周期。
const kvCacheBackendTierSurveyWindow = 30 * time.Minute

// kvCacheBackendTierSurveyInScope 报告了成员 Pod 的调查是否仍将该层级描述为当前后端所拥有的，而非当前后端之后所改变的。
func kvCacheBackendTierSurveyInScope(kvcb *workercore.KVCacheBackend, pod *core.Pod) bool {
	if kvcb.CreationTimestamp.IsZero() || pod.CreationTimestamp.IsZero() {
		return false
	}
	return pod.CreationTimestamp.Sub(kvcb.CreationTimestamp.Time) <= kvCacheBackendTierSurveyWindow
}

// kvCacheBackendTierSurveyReading 读取单个成员的目录勘察结果，并检查它是否可信。
//
// 勘察会输出目录条目数及对照计数。镜像缺少 shell、目录不可读或命令不存在时，第一个数字可能为空，看起来与空
// 目录一样。运行中容器的根目录总有内容，因此对照计数为零表示勘察失败，不能据此报告磁盘层干净。
//
// 负计数是另一种失败情况，控制机制无法处理它。控制机制仅表明调查已进行，却未说明该层级是否可被识别。容器用
// 户无法列出其内容的目录会产生一个健康的控制结果，同时计数为零，因此调查明确报告为 -1，在此被拒绝。两种失败
// 情况，两个信号，因为一种信号无法同时涵盖两者。
func kvCacheBackendTierSurveyReading(pod *core.Pod) (entries int, ok bool) {
	var message string
	for i := range pod.Status.InitContainerStatuses {
		status := &pod.Status.InitContainerStatuses[i]
		if status.Name != mooncake.MemberLocalDiskSurveyContainerName {
			continue
		}
		if status.State.Terminated == nil {
			return 0, false
		}
		message = status.State.Terminated.Message
		break
	}
	if message == "" {
		return 0, false
	}

	var entriesSeen, control string
	for field := range strings.SplitSeq(strings.TrimSpace(message), " ") {
		key, value, found := strings.Cut(field, "=")
		if !found {
			continue
		}
		switch key {
		case "entries":
			entriesSeen = value
		case "control":
			control = value
		}
	}

	controlCount, err := strconv.Atoi(strings.TrimSpace(control))
	if err != nil || controlCount == 0 {
		return 0, false
	}
	entries, err = strconv.Atoi(strings.TrimSpace(entriesSeen))
	if err != nil || entries < 0 {
		return 0, false
	}
	return entries, true
}
