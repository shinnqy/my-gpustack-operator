package apistatus

import (
	"k8s.io/utils/ptr"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"gpustack.ai/gpustack/pkg/kubeapistatus"
	"gpustack.ai/gpustack/pkg/utils/slicex"
)

const (
	// ClusterQueueConditionActive 表示全局队列已就绪，可以接纳工作负载。
	ClusterQueueConditionActive kubeapistatus.ConditionType = "Active"
	// ClusterQueueConditionTopologyReady 表示各受管 flavor 都启用了拓扑感知调度，
	// 各自选择的节点互不重叠，引用的 Topology 已存在，且总配额与物理容量相符。
	ClusterQueueConditionTopologyReady kubeapistatus.ConditionType = "TopologyReady"
)

// clusterQueueStatusPaths 按 Active 条件生成队列阶段。
//
//	条件      条件值    显示阶段      判定结果
//	Active    Unknown   Activating    过渡中
//	Active    False     Inactive      已中断
//	Active    True      Active        已完成
var clusterQueueStatusPaths = kubeapistatus.NewSummarizer(
	[][]kubeapistatus.ConditionType{
		{
			ClusterQueueConditionActive,
		},
	},
)

// GetSummaryOfClusterQueue 优先按 StopPolicy 生成阶段和说明，队列未暂停时再读取 Conditions。
//
// 使用 HoldAndDrain 时，只要还有已预留或已准入的工作负载，就显示 Draining；排空后显示 Inactive。
// Hold 直接显示 Inactive。
func GetSummaryOfClusterQueue(cq *kueue.ClusterQueue) (phase, phaseMessage string) {
	switch ptr.Deref(cq.Spec.StopPolicy, kueue.None) {
	case kueue.HoldAndDrain:
		if clusterQueueHasReserved(cq) {
			return "Draining", "cluster queue is draining admitted workloads"
		}
		return "Inactive", "cluster queue is held and drained"
	case kueue.Hold:
		return "Inactive", "cluster queue is held"
	}
	if ClusterQueueConditionTopologyReady.IsFalse(cq) {
		return "Inactive", ClusterQueueConditionTopologyReady.GetMessage(cq)
	}
	return clusterQueueStatusPaths.GetSummary(&cq.Status)
}

// clusterQueueHasReserved 检查队列中是否还有已预留或已准入的资源。
// 它与 worker 控制器的 hasReserved 使用相同的判断，分别实现以避免包循环依赖。
func clusterQueueHasReserved(cq *kueue.ClusterQueue) bool {
	if cq.Status.ReservingWorkloads != 0 || cq.Status.AdmittedWorkloads != 0 {
		return true
	}
	return slicex.Any(cq.Status.FlavorsReservation, func(i int) bool {
		return slicex.Any(cq.Status.FlavorsReservation[i].Resources, func(j int) bool {
			return !cq.Status.FlavorsReservation[i].Resources[j].Total.IsZero() ||
				!cq.Status.FlavorsReservation[i].Resources[j].Borrowed.IsZero()
		})
	})
}
