package apistatus

import (
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	"gpustack.ai/gpustack/pkg/kubeapistatus"
)

// podStatusPaths 按调度、初始化、就绪的顺序解释 Pod 条件。三列值依次为 Unknown、False 和 True 时的阶段。\n//\n//\tPodScheduled     Scheduling    Pending           Scheduled\n//\tPodInitialized   Initializing  InitializeFailed  Initialized\n//\tPodReady         Starting      NotReady          Ready\n//\n// 初始化和就绪条件还会根据 reason 细分，例如启动期间暂未就绪的容器仍显示 Starting。
var podStatusPaths = kubeapistatus.NewSummarizer(
	[][]core.PodConditionType{
		{
			core.PodScheduled,
			core.PodInitialized,
			core.PodReady,
		},
	},
	func(d kubeapistatus.Decision[core.PodConditionType]) {
		d.Make(core.PodInitialized,
			func(st meta.ConditionStatus, reason string) (string, string, kubeapistatus.StatusScore) {
				switch st {
				case meta.ConditionUnknown:
					return "Initializing", "", kubeapistatus.StatusTransitioning
				case meta.ConditionFalse:
					if reason == "ContainersNotInitialized" {
						message := "One or more init containers are not initialized, " +
							"they may be pulling images or performing other startup operations, " +
							"please check events for more details."
						return "Initializing", message, kubeapistatus.StatusTransitioning
					}
					return "InitializeFailed", "", kubeapistatus.StatusInterrupted
				}
				return "Initialized", "", kubeapistatus.StatusDone
			})
		d.Make(core.PodScheduled,
			func(st meta.ConditionStatus, reason string) (string, string, kubeapistatus.StatusScore) {
				switch st {
				case meta.ConditionUnknown:
					return "Scheduling", "", kubeapistatus.StatusTransitioning
				case meta.ConditionFalse:
					return "Pending", "", kubeapistatus.StatusTransitioning
				}
				return "Scheduled", "", kubeapistatus.StatusDone
			})
		d.Make(core.PodReady,
			func(st meta.ConditionStatus, reason string) (string, string, kubeapistatus.StatusScore) {
				switch st {
				case meta.ConditionUnknown:
					return "Starting", "", kubeapistatus.StatusTransitioning
				case meta.ConditionFalse:
					if reason == "ContainersNotReady" {
						message := "One or more containers are not ready, " +
							"they may be pulling images or performing other startup operations, " +
							"please check events for more details."
						return "Starting", message, kubeapistatus.StatusTransitioning
					}
					return "NotReady", "", kubeapistatus.StatusInterrupted
				}
				return "Ready", "", kubeapistatus.StatusDone
			})
	},
)

// GetSummaryOfPod 根据 Pod 状态返回阶段及原因。失败的 Pod 保留终态和消息。若消息为空则使用 Reason。
// 初始化或启动期间优先显示容器实际等待的原因。遇到拉取镜像失败时返回 NotReady，让使用者知道需要处理镜像。
func GetSummaryOfPod(podStatus *core.PodStatus) (phase, phaseMessage string) {
	if podStatus.Phase == core.PodFailed {
		phaseMessage = podStatus.Message
		if phaseMessage == "" {
			phaseMessage = podStatus.Reason
		}
		return string(core.PodFailed), phaseMessage
	}
	phase, phaseMessage = podStatusPaths.GetSummary(podStatus)
	if phase != "Initializing" && phase != "Starting" {
		return phase, phaseMessage
	}
	waiting := getFirstWaitingContainerState(podStatus)
	if waiting == nil {
		return phase, phaseMessage
	}
	if waiting.Message != "" {
		phaseMessage = waiting.Message
	}
	if isImagePullWaitingReason(waiting.Reason) {
		phase = "NotReady"
	}
	return phase, phaseMessage
}

// getFirstWaitingContainerState 先查看初始化容器，再查看业务容器。优先返回镜像拉取错误，否则返回遇到的第一个等待状态。
func getFirstWaitingContainerState(podStatus *core.PodStatus) *core.ContainerStateWaiting {
	var first *core.ContainerStateWaiting
	for _, statuses := range [2][]core.ContainerStatus{podStatus.InitContainerStatuses, podStatus.ContainerStatuses} {
		for i := range statuses {
			waiting := statuses[i].State.Waiting
			if waiting == nil {
				continue
			}
			if isImagePullWaitingReason(waiting.Reason) {
				return waiting
			}
			if first == nil {
				first = waiting
			}
		}
	}
	return first
}

// isImagePullWaitingReason 判断等待原因是否表示无法拉取或使用指定名称的镜像。
func isImagePullWaitingReason(reason string) bool {
	switch reason {
	case "ErrImagePull", "ImagePullBackOff", "InvalidImageName":
		return true
	}
	return false
}
