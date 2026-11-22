// Package rdma 拥有 RDMA 设备插件服务器：这四种分配模式系列使得节点的 RDMA 接口可分配。它就是厂商打包形式
// 所描述的精简版 device.Allocator——它构建这些服务器，赋予它们统一的生命周期，并针对 kubelet 的套接字启动
// 每个服务器。这些服务器本身以及它们生成的每个响应都存储在 pkg/deviceplugin 中，同时也在它们读取的协调器
// 清单中。
package rdma

// IMPORTANT: 为什么单独拆出来，不归到 nvidia/ 或 ascend/ 下面？
// 因为 RDMA 网卡属于整台服务器，不管机器上插的是 NVIDIA 还是昇腾，都统一由它来把 RDMA 网卡（PF/VF）挂载进容器。

import (
	"context"

	klog "k8s.io/klog/v2"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/devicemanager/controllers"
	"gpustack.ai/gpustack/pkg/deviceplugin"
	"gpustack.ai/gpustack/pkg/utils/gox"
)

// FamilyName 指定了此分配器所服务的资源家族。网络接口属于节点，而非制造商，因此这是一个家族名称，其中分配
// 器包含制造商的标识。它永远不会进入被检测到的制造商循环，而这一点是 RDMA 所不具备的。
const FamilyName = "rdma"

// 新版本返回 RDMA 分配器：每种分配模式家族对应一个设备插件服务器，控制标志保持不变，每个服务器都使用该家
// 族所拥有的资源名称。这些开关对于厂商的家族来说具有特定含义——被丢弃的家族根本不会在 kubelet 中注册任何资
// 源——而 `Exclusive ungated` 则如厂商侧那样运作。
//
// 构建过程不会失败，因为这里每个模式都有自己的资源名称：构造函数 NewRDMAServer 拒绝使用没有模式的模式，而
// 上述集合也从未提供过模式。被构造函数拒绝的模式会被记录并保留未服务状态，而不是终止启动过程。因此，即使
// 集合的范围被扩大到超过 GetRDMAResourceName 所指定的范围，这个分配器仍会报告成功，而 kubelet 却从未看到
// 那个资源。目前没有任何情况发生这种情况，如果真有这种情况，那只会是与服务器相关的模式数量，这是测试所验
// 证的。
func New(opts device.AllocatorOptions) device.Allocator {
	logger := opts.Logger.WithName(FamilyName)

	modes := []workercore.DeviceAllocationMode{
		workercore.DeviceAllocationModeExclusive,
	}
	if !opts.NoShared {
		modes = append(modes, workercore.DeviceAllocationModeShared)
	}
	// 没有“Sliced”条目，也没有需要关闭的 NoSliced 开关：RDMA 系列不提供“Sliced”键，因此没有需要关闭该
	// 开关的地方。在此提供该模式会导致服务器的资源名称为空，而 newServers 无论如何都会丢弃这种状态。
	if !opts.NoPartitioned {
		modes = append(modes, workercore.DeviceAllocationModePartitioned)
	}

	return &aggregated{
		logger:     logger,
		servers:    newServers(logger, modes),
		modes:      modes,
		kubeSocket: opts.KubeSocket,
	}
}

// newServers 根据每种模式构建一台服务器，并一次性处理协调器的问题：这些服务器读取相同的节点清单，如果构造
// 函数拒绝包含无资源名称的模式，则会在此处被报告，而非被忽略。
func newServers(logger klog.Logger, modes []workercore.DeviceAllocationMode) []deviceplugin.Server {
	reconciler := controllers.Get[*deviceplugin.DevicesReconciler]()

	servers := make([]deviceplugin.Server, 0, len(modes))
	for _, mode := range modes {
		server, err := deviceplugin.NewRDMAServer(logger, mode, reconciler)
		if err != nil {
			logger.Error(err, "serving no resource for the allocation mode", "mode", mode.String())
			continue
		}
		servers = append(servers, server)
	}
	return servers
}

type aggregated struct {
	logger  klog.Logger
	servers []deviceplugin.Server
	// modes 会携带每台服务器的分配模式。这些服务器属于 pkg/deviceplugin 在其 Server 接口背后的非导出
	// 类型，因此这里无法询问服务器所服务的模式。这个切片是唯一的访问途径，测试也基于它进行断言。针对
	// 服务器的长度限制，也是构造函数拒绝的模式之一：newServers 会跳过这种模式，导致服务器的长度比这个
	// 切片短，这是“所有模式都被服务”与“某个模式被静默丢弃”之间的唯一区别。
	modes      []workercore.DeviceAllocationMode
	kubeSocket string
	// 生命周期拥有服务器运行的上下文，因此停止此分配器会终止所有服务器。
	lifecycle gox.Lifecycle
}

func (*aggregated) Name() string {
	return FamilyName
}

func (in *aggregated) Start(ctx context.Context) error {
	in.logger.Info("starting")

	tasks := make([]func(context.Context) error, 0, len(in.servers))
	for i := range in.servers {
		srv := in.servers[i]
		tasks = append(tasks, func(ctx context.Context) error {
			return srv.Start(ctx, in.kubeSocket)
		})
	}

	return in.lifecycle.Start(ctx, tasks...)
}

func (in *aggregated) Stop() {
	in.logger.Info("stopping")

	in.lifecycle.Stop()
}
