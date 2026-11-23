package deviceplugin

import (
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	ctrlevent "sigs.k8s.io/controller-runtime/pkg/event"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/utils/mapx"
	"gpustack.ai/gpustack/pkg/utils/osx"
)

// LedgerReleaseTerminatedPodsEnv 设为 false 时，Succeeded 或 Failed 的 Pod 会持续占账，
// 直到该 Pod 对象被删除。发布账本的设备管理器和重建账本做准入的 worker 都在启动时读取一次此开关。
const LedgerReleaseTerminatedPodsEnv = "GPUSTACK_LEDGER_RELEASE_TERMINATED_PODS"

// terminatedPodsReleased 表示终止的 Pod 能否释放其逻辑分配。使用变量以便测试替换。
var terminatedPodsReleased = func() bool { return releaseTerminatedPodsAtStartup }

var releaseTerminatedPodsAtStartup = osx.Getenv(LedgerReleaseTerminatedPodsEnv) != "false"

// statefulSlicedManufacturers 列出通过驱动对象实现逻辑切片的厂商。相应回收循环等 Pod 对象删除后才销毁实例，
// 即使容器已退出也不能提前重分。此集合须与为逻辑切片启动 RunReclaimLoop 的厂商一致。
var statefulSlicedManufacturers = sets.New(nodefeature.ManufacturerMetaX, nodefeature.ManufacturerCambricon)

// podTerminated 报告该 Pod 是否处于终止阶段。kubelet 在每个容器停止后仅设置一次该值，之后不会重新启动容器，
// 并返回 Pod 的设备信息。
func podTerminated(pod *core.Pod) bool {
	return pod.Status.Phase == core.PodSucceeded || pod.Status.Phase == core.PodFailed
}

// heldAllocation 从 Pod 的持久记录中筛出仍占用的部分。进入终止阶段后，可以归还随容器退出而解除隔离的整卡、
// 共享卡和逻辑切片。硬件分区以及有状态厂商创建的切片仍占用至 Pod 对象删除。其他阶段继续全额记账，
// 关闭提前释放开关时也继续全额记账。
func heldAllocation(pod *core.Pod, status workercore.DevicesStatus) workercore.DevicesStatus {
	if !podTerminated(pod) || !terminatedPodsReleased() {
		return status
	}
	held := workercore.DevicesStatus{Groups: make([]workercore.DevicesAllocationGroup, 0, len(status.Groups))}
	for i := range status.Groups {
		grp := status.Groups[i]
		grp.Accelerators = nil
		for _, acc := range status.Groups[i].Accelerators {
			if heldAfterTermination(grp.Manufacturer, &acc) {
				grp.Accelerators = append(grp.Accelerators, acc)
			}
		}
		if len(grp.Accelerators) != 0 {
			held.Groups = append(held.Groups, grp)
		}
	}
	return held
}

// heldAfterTermination 判断 Pod 进入终止阶段后，指定设备是否仍被其持有。只要记录中有物理分区信息，
// 就按分区处理。
func heldAfterTermination(manufacturer string, acc *workercore.AcceleratorAllocation) bool {
	if acc.AllocatedPhysicalProfile != "" || len(acc.AllocatedPhysicalPlacements) != 0 {
		return true
	}
	switch acc.Mode {
	case workercore.DeviceAllocationModeExclusive, workercore.DeviceAllocationModeShared:
		return false
	case workercore.DeviceAllocationModeSliced:
		return statefulSlicedManufacturers.Has(manufacturer)
	default:
		return true
	}
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
