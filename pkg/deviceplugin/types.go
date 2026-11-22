package deviceplugin

import (
	"context"

	deviceplugin "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

// 下面的第一个块对 k8s.io/kubelet 的 device-plugin v1beta1 API 类型进行了别名处理，并进行了重新导出，因此
// 制造商的分配器可以在不导入 kubelet 包本身的情况下，为它构建的 gRPC 负载命名。这些别名保留了 kubelet 原
// 有的名称：该块的全部意义在于它实现了该 API 的 1:1 映射，而重命名的映射则是一个更差的映射。
//
// 这个镜子的价格问题在于"Device"在这里指代了三样不同的东西——其中两项是 kubelet 的，一项是此软件包自己的——
// 因此读者必须知道各自指的是什么。
//
//   - 该设备是 kubelet 所 advertised 的设备：ListAndWatch 发布的 `{ID, Health, Topology}` 记录中提及的。
//     其 ID 是设备 ID（一个令牌字符串），而非我们所理解的 Device ID。
//   - DeviceSpec 是 kubelet 的宿主设备节点规范：/dev 路径是运行时要求注入到容器中的内容。
//   - Device, on Resource，是指令牌所指向物理设备的 ID。这是该包自己的词汇表，而非 kubelet 的词汇表——关于
//     Device/Accelerator/Resource 的层级结构，可参考 pkg/device 包的文档。
type (
	Options                   = deviceplugin.DevicePluginOptions
	Empty                     = deviceplugin.Empty
	ListAndWatchResponse      = deviceplugin.ListAndWatchResponse
	AllocateRequest           = deviceplugin.AllocateRequest
	AllocateResponse          = deviceplugin.AllocateResponse
	ContainerAllocateResponse = deviceplugin.ContainerAllocateResponse
	Mount                     = deviceplugin.Mount
	DeviceSpec                = deviceplugin.DeviceSpec
	// 服务器是每个设备插件 gRPC 服务器（其中包括 ResourceServer）向其调用者提供的生命周期契约，与
	// kubelet API 接口无关，该服务器还实现了该接口。
	Server interface {
		// Start 启动 gRPC 服务器并向 kubelet 注册，在上下文取消或出错时结束。取消时会停止服务并撤
		// 销注册。
		Start(ctx context.Context, kubeSocket string) error
		// Stop 命令会停止 gRPC 服务器，并从 kubelet 中取消注册它。此方法应非阻塞，并立即返回结果。
		Stop()
	}
)
