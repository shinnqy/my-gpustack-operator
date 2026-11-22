package exporter

import (
	"context"
	"fmt"

	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"

	"gpustack.ai/gpustack/pkg/deviceplugin"
	"gpustack.ai/gpustack/pkg/utils/osx"
)

// 导出报告说明此 Device Manager 是否为发布节点按 Instance 划分数据的那个。
//
// 一个承载两个供应商的节点运行两个设备管理器，两者都能看到其所有实例：如果没有规则限制，它们会从两个抓取
// 目标中发布相同的数据系列。在查询时使用 max by(...)进行去重也不奏效——这两个样本是独立的，因此 max 会产生
// 向上的偏差，而不是在相同的副本之间做出选择。
//
// 规则是，该节点中处于就绪状态的 Device Manager 容器中的制造商按顺序进行导出。这是一种完全有序的操作，不
// 需要共享状态或租约：每个容器都监视其节点的容器，当当前持有者不再处于就绪状态时，角色会自动移交。在就绪
// 状态发生变化期间，两个容器可能会短暂地同时被视为导出者，这会在两个目标之间产生重复操作，但会在一段时间
// 内自行解决。
//
// 加速器不受此限制：不同制造商的 Device `ID`是互不重复的，因此每个 Device Manager 都会发布自己的 ID，绝不
// 会冲突。
func (p *Poller) exporting(ctx context.Context) (bool, error) {
	podName := osx.Getenv("KUBERNETES_POD_NAME")
	if podName == "" {
		return false, fmt.Errorf("environment variable KUBERNETES_POD_NAME is not set")
	}
	// 空命名空间并非“此 Pod 的命名空间”，而是所有命名空间：在这种情况下，选举将把此 Pod 与恰好运行在
	// 该节点的运营商其他安装环境中的设备管理器进行比较，并可能选择其中的某个。
	podNamespace := osx.Getenv("KUBERNETES_POD_NAMESPACE")
	if podNamespace == "" {
		return false, fmt.Errorf("environment variable KUBERNETES_POD_NAMESPACE is not set")
	}

	podList := &core.PodList{}
	err := p.reader.List(ctx, podList,
		ctrlcli.InNamespace(podNamespace),
		ctrlcli.MatchingLabels{deviceplugin.ComponentLabelKey: deviceplugin.DeviceManagerComponent},
		ctrlcli.MatchingFieldsSelector{
			Selector: fields.OneTermEqualSelector(deviceplugin.IndexingPodsByNodeName, p.nodeName),
		},
	)
	if err != nil {
		return false, fmt.Errorf("failed to list device manager pods on node %s: %w", p.nodeName, err)
	}

	// 从标签上读取此 pod 的制造商信息，而非通过 --manufacturer 标志来读取：标签是其他 pod 进行比较的
	// 依据，一个进程无法以其同行无法识别的名称作为出口者。
	var mine string
	first := ""
	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.Name == podName {
			mine = pod.Labels[deviceplugin.ManufacturerLabelKey]
		}
		if pod.DeletionTimestamp != nil || !deviceplugin.IsPodReady(pod) {
			continue
		}
		// 没有说明服务哪个制造商的 Pod 无法获得资格。空白标签在真实标签之前被排序，因此将其视为赢
		// 家会导致节点完全没有出口器：带标签的 Pod 会遵循该节点，而该节点也无法将自己识别为赢家。
		manufacturer := pod.Labels[deviceplugin.ManufacturerLabelKey]
		if manufacturer == "" {
			continue
		}
		if first == "" || manufacturer < first {
			first = manufacturer
		}
	}

	// 尚未准备就绪，或尚未放入缓存中：只要任一节点已准备就绪，该节点就会导出数据。而未准备就绪的节点
	// 自然也不会被抓取。
	return mine != "" && mine == first, nil
}
