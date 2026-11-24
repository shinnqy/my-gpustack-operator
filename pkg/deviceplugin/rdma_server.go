package deviceplugin

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc"
	core "k8s.io/api/core/v1"
	klog "k8s.io/klog/v2"
	deviceplugin "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/utils/stringx"
	"gpustack.ai/gpustack/pkg/utils/waitx"
)

// rdmaServer 通过设备插件 gRPC API 提供一种分配模式的 RDMA 端点服务，该服务由相同的协调器库存和相同的推理
// 服务生命周期驱动，与加速器服务器保持一致。它自身不持有任何分配状态：所有响应都从
// Devices.spec.interfaces[]中重新计算，因此这两个 pooled 体系结构使用相同的 HCAs，不会互相减少，而且检测
// 器后来修正的一个事实无法在基于旧版本的响应中保留。
type rdmaServer struct {
	deviceplugin.UnimplementedDevicePluginServer

	Logger         klog.Logger
	AllocationMode workercore.DeviceAllocationMode
	ResourceName   core.ResourceName
	Reconciler     *DevicesReconciler

	serving
}

// NewRDMAServer 返回了宣传某一分配模式 RDMA 端点的设备插件服务器，该服务器由协调器的清单驱动。它拒绝任何
// 没有自身 RDMA 资源键的分配模式——可见性分配会指定由同一 Pod 下的另一个容器持有的端点，且不会写入任何
// RDMA 分配记录来回应该请求，因此该模式未被服务，而非部分服务——因此调用者无法构建一个会向 kubelet 注册空
// 资源名称的服务器。
func NewRDMAServer(
	logger klog.Logger,
	mode workercore.DeviceAllocationMode,
	reconciler *DevicesReconciler,
) (Server, error) {
	name := nodefeature.GetRDMAResourceName(mode)
	if name == "" {
		return nil, fmt.Errorf("allocation mode %s serves no RDMA resource", mode)
	}
	return &rdmaServer{
		Logger:         logger.WithName(strings.ToLower(mode.String())),
		AllocationMode: mode,
		ResourceName:   name,
		Reconciler:     reconciler,
	}, nil
}

// rdmaFamilyName 是套接字基础名称的第一个组成部分，也是通知器的厂商标签。它用于标识 RDMA 资源家族，而目前
// 没有厂商使用这一名称——因为加速器套接字的第一个组成部分总是厂商名称，这与检测器报告的情况一致。因此，
// RDMA 套接字无法与厂商服务器产生冲突，整端点、共享端点和 VF 三种模式各有自己的套接字。
const rdmaFamilyName = "rdma"

// servingOptions 定义了此服务器所提供的内容：响应 gRPC 调用的插件，即此服务器本身。其注册的资源。以及基于
// 名称与 kubelet 的套接字。
func (s *rdmaServer) servingOptions() servingOptions {
	return servingOptions{
		Plugin:       s,
		ResourceName: s.ResourceName,
		SocketName:   stringx.Join(".", rdmaFamilyName, strings.ToLower(s.AllocationMode.String()), "sock"),
	}
}

// 开始将插件、套接字和资源选项名称传递给 kubelet，只要 ctx 允许。
func (s *rdmaServer) Start(ctx context.Context, kubeSocket string) error {
	return s.serving.Start(ctx, kubeSocket, s.servingOptions(), s.Logger)
}

// Stop 会停止 gRPC 服务器并结束推理服务循环。
func (s *rdmaServer) Stop() {
	s.serving.Stop(s.Logger)
}

// GetDevicePluginOptions 返回将用于与设备管理器通信的选项。没有指定的优先分配策略：此服务器在其令牌之间没
// 有表现出任何偏好，因此 kubelet 可以自由选择。
func (s *rdmaServer) GetDevicePluginOptions(context.Context, *Empty) (*Options, error) {
	return &Options{}, nil
}

// ListAndWatch 返回一个 Devices 列表的流。每当某个设备状态发生变化或该设备消失时，ListAndWatch 就会返回新
// 的列表。
func (s *rdmaServer) ListAndWatch(_ *Empty, srv grpc.ServerStreamingServer[ListAndWatchResponse]) error {
	// 就像加速器服务器那样，在 ListAndWatch 开始时注册，在退出时则取消订阅。kubelet 每次注册都会开启
	// 一个新的流，而超过该流时间的订阅会在进程生命周期内，每次重启 kubelet 时积累一份。
	notifier, release := s.Reconciler.getReconcileNotifier(rdmaFamilyName, s.AllocationMode)
	defer release()

	ctx := srv.Context()

	s.Logger.Info("sending initial list and watch response")
	if err := waitx.RetryUntilSuccess(ctx, 2*time.Second, true, func(ctx context.Context) error {
		resp, err := s.getListAndWatchResponse(ctx)
		if err != nil {
			// 被返回而非被吞掉。此轮轮询以零返回结束，因此日志记录和流程中断会将其视为成功，
			// 因为并未发送任何内容，而流将继续进入监视循环，此时 kubelet 根本不持有设备列表：
			// 直到稍后发生一次协调操作触发通知器时，该资源才被视为零。
			s.Logger.Error(err, "get initial list and watch response, retry later")
			return err
		}
		if err = srv.Send(resp); err != nil {
			// 出于相同原因再次尝试。如果某个流无法接受第一个响应，说明某个 kubelet 已停止读取，
			// 此时该轮询在流的上下文结束时终止。
			s.Logger.Error(err, "send initial list and watch response, retry later")
			return err
		}
		return nil
	}); err != nil {
		s.Logger.Error(err, "initial list and watch")
		return err
	}

	s.Logger.Info("watching for device updates")
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-notifier:
			resp, err := s.getListAndWatchResponse(ctx)
			if err != nil {
				s.Logger.Error(err, "get list and watch response on update")
				return err
			}
			if err := srv.Send(resp); err != nil {
				s.Logger.Error(err, "send list and watch response")
				return err
			}
			s.Logger.Info("sent list and watch response")
		}
	}
}

// getListAndWatchResponse 根据当前接口清单发布各端点的 token 和 NUMA 提示。链路明确失败时，
// token 保留原 ID，健康状态改为 Unhealthy，供 kubelet 停止向新容器分配。
func (s *rdmaServer) getListAndWatchResponse(ctx context.Context) (*ListAndWatchResponse, error) {
	devs, err := s.Reconciler.getDevices(ctx)
	if err != nil {
		return nil, err
	}

	resp := &ListAndWatchResponse{}
	for i := range devs.Spec.Interfaces {
		iface := &devs.Spec.Interfaces[i]
		for _, ep := range rdmaEndpoints(iface, s.AllocationMode) {
			// 链接裁决通过标记的方式将端点节点的令牌保留在新 Pods 中，而非将其移除：删除所发
			// 布的 ID 会使 kubelet 对所管理的容器进行的检查点分配失效。完全没有裁决并不意味着
			// 失败——此处达到的端点节点拥有受限的 RDMA 设备，因此缺失的链接记录只是通过其他途
			// 径到达的未验证情况。
			health := deviceplugin.Healthy
			if ep.Link != nil && ep.Link.State == workercore.DeviceInterfaceLinkStateFailed {
				health = deviceplugin.Unhealthy
			}
			for _, id := range rdmaDeviceIDs(ep.Resource, s.AllocationMode) {
				// 该分区家族还包含 NUMA 提示，而加速器分区资源池则没有：一个 RDMA 分区令
				// 牌仅对应一个虚拟函数，该函数可以遵循亲和性提示。而加速器分区令牌则完全
				// 不指向任何加速器。
				resp.Devices = append(resp.Devices, &deviceplugin.Device{
					ID:       id,
					Health:   health,
					Topology: ep.Topology,
				})
			}
		}
	}
	return resp, nil
}

// rdmaDeviceIDs 返回该模式下一个端点所宣传的可互换令牌。
//
// 故意不使用 Resource.DeviceIDs。该方法共享的分支以加速器账本单位作为标记——该单位由全局分母除以加速器份额
// 数得到——因为一个加速器令牌必须表明其属于哪个加速器，并且会忽略调用者传递的任何大小信息。RDMA 令牌没有这
// 样的数值：分配路径将令牌映射到其指定的端点，且不会读取其索引，并且按端点进行去重处理，因此一个端点上的
// 第二个令牌就是同一事物的第二个占用。
//
// 通过该方法路由 RDMA 家族，实际上同时犯了两个错误。它使得所显示的计数与文件所要求的 RDMA 加速器份额计数
// 一致，并且将类似单位偏移的索引发布到了 RDMA 账本中不存在的地方。
func rdmaDeviceIDs(res Resource, mode workercore.DeviceAllocationMode) []string {
	size := rdmaPoolSizeOf(mode)
	prefix := res.String() + ":"

	ids := make([]string, 0, size)
	for i := int32(0); i < size; i++ {
		ids = append(ids, prefix+padIndex(uint64(i)))
	}
	return ids
}

// rdmaPoolSizeOf 返回一个端点在以下模式下的令牌数量：共享家族的并发上限，以及那些令牌本身就是完整实体的家
// 族中的一个令牌。
//
// 这个上限是 RDMA 自己的数值，而非它过去所借鉴的加速器数值。这个常数决定了加速器可以被分成多少个所有者，
// 这涉及到加速器的情况。而关于 HCA 的情况则没有提及，使用相同的符号使得两者看起来像是一个需要协同发展的事
// 实。
func rdmaPoolSizeOf(mode workercore.DeviceAllocationMode) int32 {
	switch mode {
	case workercore.DeviceAllocationModeShared:
		return int32(nodefeature.RDMAEndpointPoolSize)
	default:
		return 1
	}
}
