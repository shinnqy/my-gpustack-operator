package deviceplugin

import (
	"context"

	klog "k8s.io/klog/v2"
	deviceplugin "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

// IMPORTANT: 通用加速器插件服务，把跨模式共享的 DevicesReconciler 和分配锁串起来；具体怎么给容器塞设备则回调各厂商实现的 Responder。

// ResourceServer 通过设备插件 gRPC API 将每一种（制造商、分配模式）组合对传递给 kubelet：它会声明该系列的
// 令牌，回应分配提示，并执行分配操作。每种模式对应一个实例，且每个实例共享一个 DevicesReconciler——正是这
// 个组件使得节点级分配互斥锁以及各加速器跨模式的一致性得以实现，而非仅局限于单一模式内。Responder 负责处
// 理与供应商相关的响应内容。当 `Allocate` 参数未设置时，它会直接拒绝该请求。
type ResourceServer struct {
	deviceplugin.UnimplementedDevicePluginServer

	Logger         klog.Logger
	Manufacturer   string
	AllocationMode workercore.DeviceAllocationMode
	Reconciler     *DevicesReconciler
	Responder      ContainerAllocateResponder

	serving
}

// 只要上下文允许，Start 会将此服务器的资源提供给 kubelet，每当 kubelet 取走这些资源时，服务会重新建立。
//
// 它会阻塞，直到 ctx 被取消或调用 Stop 指令，只有在无法建立任何代次时才会报告错误。在已运行的服务器上调用
// 它则毫无效果，只会阻塞直到 ctx 完成，因此重试的主管无法让两个服务器共享一个套接字。
func (s *ResourceServer) Start(ctx context.Context, kubeSocket string) error {
	// 第 8 章补上此函数的实现。
	var result0 error
	return result0
}

// Stop 会停止 gRPC 服务器并结束推理服务循环，在 Start 执行完毕后才会返回。此时没有该服务器的代次需要处理
// 套接字，这允许分配器在该路径上替换服务器。这是一个无操作函数，会被记录，适用于从未启动的服务器。正在进
// 行的分配会被放弃，而非被清空：kubelet 会重试那些未能获得响应的分配，而该服务器所做的预留也会在其 Pods
// 消失后由协调器进行清理。
//
// 稍后的启动操作在之前的启动操作返回后可以再次执行——单个启动保护机制在启动操作离开时会被解除，因此，一个
// 启动操作同时执行自己的停止操作会被拒绝，无法允许在同一个套接字上放置第二个服务器。
func (s *ResourceServer) Stop() {
	s.serving.Stop(s.Logger)
}
