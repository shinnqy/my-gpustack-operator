package worker

import (
	"context"
	"slices"

	core "k8s.io/api/core/v1"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

const (
	// modelPlacementPreferenceWeight 是指向持有某摘要（digest）的节点集合的偏好调度项权重。
	// 该权重是评估该偏好策略时使用的基准值；TAS 仅对各项得分进行相对比较，因此携带单一调度项的
	// Pod 在任何权重下的放置结果都相同，而多个调度项之间则具有同等的权重。
	modelPlacementPreferenceWeight = 100

	// modelPlacementMaxNodes 限制单个调度项中列出的主机名数量上限，防止添加至 Pod 及其 Workload
	// 的偏好规则体积随集群规模线性膨胀。
	modelPlacementMaxNodes = 16

	// nodeModelStoreConditionReady 是插件在正常服务其所在节点期间保持为 True 的状态条件名称。
	nodeModelStoreConditionReady = "Ready"
)

// placementPreference 为挂载这些权重的 Pod 返回节点放置偏好：当使用节点交付摘要且已就绪可
// 挂载时，返回持有该摘要的节点；当使用镜像制品时，返回 kubelet 上报持有该镜像的节点；对于
// 其他交付方式、解析受阻或没有任何节点持有的引用，返回 nil。
//
// 该方法会遍历所有 NodeModelStore，因此仅在需要创建 Pod 的调谐轮次中统一计算一次：既避免
// 不创建任何 Pod 的调谐轮次承担遍历开销，又保证同一副本的所有成员获得完全相同的调度项——
// Kueue 需要这一点，因为它基于其中一个成员来构建整个副本的 PodSet。
func (w *modelArtifactWeights) placementPreference(ctx context.Context, cli ctrlcli.Reader) *core.PreferredSchedulingTerm {
	if w == nil || w.Blocked || w.Render == nil {
		return nil
	}

	switch w.Render.Delivery {
	case workercore.ModelDeploymentModelDeliveryNode:
		return modelPlacementPreference(ctx, cli, w.Render.ManifestDigest)
	case workercore.ModelDeploymentModelDeliveryImage:
		return modelPlacementImagePreference(ctx, cli, w.Render.ImageReference)
	}
	return nil
}

// modelPlacementImagePreference 返回一个偏好节点亲和性调度项，列出 kubelet 上报持有指定
// 镜像制品引用的节点；若无节点持有则返回 nil。
//
// 它始终只是偏好（preference）而非硬性过滤条件（filter），原因与摘要偏好相同：Kueue 的拓扑
// 感知调度（TAS）仅将该调度项作为已满足 Pod 调度条件的节点之间的加分项，因此即使条目有误或
// 过期，最多也只是导致在另一个节点上多拉取一次镜像。而且过期条目在实际中很可能出现：信息来源
// 是 kubelet 的 Node.status.images，该列表受 --node-status-max-images（默认 50）上限限制，
// 运行镜像较多的节点可能根本不会列出目标镜像，而被垃圾回收的镜像也会在下一次上报前短暂残留。
// 软亲和性的设计正使这种不精确变得可以接受：未命中最多只是损失本轮的局部性收益。此处不读取
// 镜像大小——CRI 上报的大小是压缩后的镜像体积，远低于实际磁盘占用，且容量问题本就不在此考虑。
//
// 节点列表先按主机名排序再截断上限，因为 Node.status.images 不包含镜像的当前引用状态或最近
// 使用时间等信号：按名称排序是唯一仅依赖候选节点集合本身的确定性排序方式，从而确保基于同一快照
// 构建的同一副本成员携带完全一致的节点列表。
func modelPlacementImagePreference(ctx context.Context, cli ctrlcli.Reader, reference string) *core.PreferredSchedulingTerm {
	logger := ctrllog.FromContext(ctx)

	nodes := new(core.NodeList)
	if err := cli.List(ctx, nodes); err != nil {
		logger.Error(err, "list nodes for an image placement preference; none is added")
		return nil
	}

	var hostnames []string
	for i := range nodes.Items {
		node := &nodes.Items[i]
		holds := slices.ContainsFunc(node.Status.Images, func(img core.ContainerImage) bool {
			return slices.Contains(img.Names, reference)
		})
		if !holds {
			continue
		}
		hostname := node.Labels[core.LabelHostname]
		if hostname == "" {
			continue
		}
		hostnames = append(hostnames, hostname)
	}
	if len(hostnames) == 0 {
		return nil
	}
	slices.Sort(hostnames)
	if len(hostnames) > modelPlacementMaxNodes {
		hostnames = hostnames[:modelPlacementMaxNodes]
	}

	return &core.PreferredSchedulingTerm{
		Weight: modelPlacementPreferenceWeight,
		Preference: core.NodeSelectorTerm{MatchExpressions: []core.NodeSelectorRequirement{
			{Key: core.LabelHostname, Operator: core.NodeSelectorOpIn, Values: hostnames},
		}},
	}
}

// modelPlacementPreference 返回列出已就绪可供挂载的 digest 所在节点的节点亲和性偏好项（preferred node-affinity term）；
// 若无节点满足条件则返回 nil。
//
// 该项仅作为偏好（PREFERENCE），绝非过滤条件（FILTER）。Kueue 的拓扑感知调度（TAS）会在已满足 Pod 要求的
// 节点中将该项作为节点评分依据，因此错误或过时的条目最多只会导致在另一节点上多一次下载。同理，读取失败时
// 返回空（无偏好项）而非返回 error：若因无法计算偏好而阻塞 Pod 调度，就相当于把文件位置置于算力之上。
//
// 节点仅在同时满足以下三个条件时才计入：其 NodeModelStore 将该 digest 列为 Ready、该对象的 Ready condition
// 为 True，且其 CSINode 当前列出了该插件的 driver。第三个条件是必需的，因为对象本身不会标明何时过时：
// 插件离开节点后，其对象和不再变化的 Ready condition 仍会保留，而 kubelet 会从 CSINode 中移除该 driver。
//
// 偏好项中的值为节点的 kubernetes.io/hostname 标签而非节点名称：TAS 按该标签索引节点，且部分云厂商会将其
// 设为与 Node 名称不同的值。
func modelPlacementPreference(ctx context.Context, cli ctrlcli.Reader, digest string) *core.PreferredSchedulingTerm {
	// 在第 18 章中实现此函数。
	return nil
}

// injectModelPlacementPreference 将 term 追加到 Pod 的首选节点亲和性（preferred node affinity）中，并保留
// 原有的所有条目。
//
// 请求 Kueue 首选拓扑层级的 Pod 不会注入该偏好：经实测，在启用 TASBalancedPlacement（与 chart 默认配置一致）
// 时，这类 PodSet 会完全丢失其节点亲和性评分，因此注入该 term 只会声明一个无人响应的偏好。
func injectModelPlacementPreference(pod *core.Pod, term *core.PreferredSchedulingTerm) {
	if term == nil {
		return
	}
	if _, ok := pod.Annotations[kueue.PodSetPreferredTopologyAnnotation]; ok {
		return
	}
	if pod.Spec.Affinity == nil {
		pod.Spec.Affinity = &core.Affinity{}
	}
	if pod.Spec.Affinity.NodeAffinity == nil {
		pod.Spec.Affinity.NodeAffinity = &core.NodeAffinity{}
	}
	na := pod.Spec.Affinity.NodeAffinity
	na.PreferredDuringSchedulingIgnoredDuringExecution = append(na.PreferredDuringSchedulingIgnoredDuringExecution, *term.DeepCopy())
}
