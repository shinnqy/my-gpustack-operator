package deviceplugin

import (
	core "k8s.io/api/core/v1"
)

// chart 和 Instance 控制器打在 Pod 上的标签，多个组件都会回读这些标签。
// 它们集中定义在此处（紧邻 Pod 索引与分配信息读取器），因为契约一旦存在第二份副本就会产生漂移：
// 若在 chart 中重命名了某标签而只更新了其中一份副本，读取另一份副本的组件就会静默失效、匹配不到任何对象——
// 既不会触发编译错误，也不会让各自固定了副本的包测试失败。
const (
	// ComponentLabelKey 标识 Pod 所属的组件名称，DeviceManagerComponent 则是 chart 为每个厂商的 device manager DaemonSet 打上的值。
	ComponentLabelKey      = "app.kubernetes.io/component"
	DeviceManagerComponent = "device-manager"

	// ManufacturerLabelKey 记录 device manager Pod 所对应的厂商。chart 为每个厂商创建一个 DaemonSet 并为其打上该标签。
	ManufacturerLabelKey = "gpustack.ai/manufacturer"

	// InstancePartOfLabelKey 记录底层 Instance 的 UID，由 Instance 控制器打在 Pod 上。
	InstancePartOfLabelKey = "app.kubernetes.io/part-of"
)

// IsPodReady 报告 Pod 是否带有状态为 true 的 Ready 条件。
func IsPodReady(pod *core.Pod) bool {
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == core.PodReady {
			return pod.Status.Conditions[i].Status == core.ConditionTrue
		}
	}
	return false
}
