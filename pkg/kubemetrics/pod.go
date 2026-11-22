package kubemetrics

import (
	core "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	worker "gpustack.ai/gpustack/api/worker/v1"
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

// HasStartedContainer 返回 Pod 中是否曾有任何容器启动过。
//
// 这是两个采集入口在读取任何指标前共用的前置检查。若 Instance 没有任何容器启动过，
// 则其资源消耗在逻辑上即为零而无需实际测量；此时再去请求 kubelet、metrics API
// 和各个 DeviceManager，不仅会白白发出三次请求，还可能把其他租户的数据误当作它的。
//
// 判断条件是“已启动”，而非“已就绪”。Pod 可能正处于就绪探针失败、sidecar 未就绪
// 或已开始终止的状态，而其主容器仍持有加速卡显存；此时上报零值等于捏造空闲状态。
// 基于同样原因，容器留下的任何运行痕迹都算数——包括重启、先前终止、正在运行的 init 容器
// 或临时容器——因为这些状态只会放行检查，而放行方向永远不会凭空编造指标。
//
// nil Pod 视为没有任何容器启动过。这对应调用方持有 Instance 但尚未拥有对应 Pod 的情况：
// 例如已停止、未调度，或仅残留上一代 Pod。它与尚未启动的 Pod 返回相同的未通过结果，
// 从而使该检查保持单一语义。
func HasStartedContainer(pod *core.Pod) bool {
	if pod == nil {
		return false
	}
	for _, statuses := range [][]core.ContainerStatus{
		pod.Status.InitContainerStatuses,
		pod.Status.ContainerStatuses,
		pod.Status.EphemeralContainerStatuses,
	} {
		for i := range statuses {
			if containerStarted(&statuses[i]) {
				return true
			}
		}
	}
	return false
}

// containerStarted 返回单个容器状态是否表明该容器曾运行过（无论当前是否在运行）：
// 已退出的容器曾持有过资源，而等待重启的容器此前也已运行过。
func containerStarted(cs *core.ContainerStatus) bool {
	switch {
	case cs.State.Running != nil, cs.State.Terminated != nil:
		return true
	case cs.RestartCount > 0:
		return true
	case cs.LastTerminationState.Terminated != nil, cs.LastTerminationState.Running != nil:
		return true
	}
	return ptr.Deref(cs.Started, false)
}

// NewUnstartedSample 返回没有任何容器启动过的 Instance 的采样样本：包含声明的总量上限，
// 且所有实测指标均显式置为零。
//
// 此处的零代表实测结果而非默认值。由于没有任何容器启动，不存在任何可能消耗资源的实体——
// 这正是显式零值所表达的语义，也是该路径完全不执行采集的原因。
//
// 若存在 Pod，总量取自 Pod 的容器 limits；否则取自 Instance 自身的 spec.resources。
// 两者对同一资源声明的结果一致，因为都经过相同的取整逻辑。若调用方两者皆无
// （即未声明资源的 Instance），则总量为 0，其语义与未设置限制的容器相同，均表示“未声明上限”。
func NewUnstartedSample(
	pod *core.Pod,
	resources *workercore.InstanceResources,
) *worker.InstanceMetricsSample {
	var sample *worker.InstanceMetricsSample
	switch {
	case pod != nil:
		sample = NewSample(pod)
	case resources != nil:
		sample = NewSampleFromResources(*resources)
	default:
		sample = NewSampleFromResources(workercore.InstanceResources{})
	}

	sample.CPUUsedMilliCores = ptr.To[uint64](0)
	sample.MemoryUsedMiB = ptr.To[uint64](0)
	sample.StorageUsedMiB = ptr.To[uint64](0)
	return sample
}
