package deviceplugin

import (
	core "k8s.io/api/core/v1"
	ctrlevent "sigs.k8s.io/controller-runtime/pkg/event"

	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/utils/mapx"
)

// podTerminated 报告该 Pod 是否处于终止阶段。kubelet 在每个容器停止后仅设置一次该值，之后不会重新启动容器，
// 并返回 Pod 的设备信息。
func podTerminated(pod *core.Pod) bool {
	return pod.Status.Phase == core.PodSucceeded || pod.Status.Phase == core.PodFailed
}

// podUpdateChangesLedger 报告一个 Pod 的更新是否改变了账本重建所依据的数据：其分配记录已变更，删除操作已
// 开始或移动，或者已进入最终阶段但仍保留记录。
func podUpdateChangesLedger(e ctrlevent.UpdateEvent) bool {
	oldPod, newPod := e.ObjectOld, e.ObjectNew
	if kubemeta.HasAnnotation(newPod, AllocatedAcceleratorAnnoKey) &&
		podTerminated(newPod.(*core.Pod)) && !podTerminated(oldPod.(*core.Pod)) {
		return true
	}
	if newPod.GetDeletionTimestamp() == nil {
		return !mapx.EqualWithKey(oldPod.GetAnnotations(), newPod.GetAnnotations(), AllocatedAcceleratorAnnoKey)
	}
	if kubemeta.HasAnnotation(oldPod, AllocatedAcceleratorAnnoKey) {
		if oldPod.GetDeletionTimestamp() == nil {
			return true
		}
		return !oldPod.GetDeletionTimestamp().Equal(newPod.GetDeletionTimestamp())
	}
	return false
}
