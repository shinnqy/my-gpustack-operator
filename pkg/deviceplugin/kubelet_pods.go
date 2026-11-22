package deviceplugin

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	podresources "k8s.io/kubelet/pkg/apis/podresources/v1"

	"gpustack.ai/gpustack/pkg/utils/osx"
)

// 辅助读取本机 kubelet 视角下的 Pod 状态

const (
	// KubeletPodResourcesSocket 是 kubelet 提供其 Pod-Resources API 的接口：它所管理的 Pod，以及每个
	// 容器已分配的设备。Device Manager 的 DaemonSet 会以只读方式挂载其目录。
	KubeletPodResourcesSocket = "/var/lib/kubelet/pod-resources/kubelet.sock"
	// 当设置为 "false" 时，IdentifyByKubeletEnv 会关闭 kubelet 查找功能：每次分配后，它将仅通过
	// pending-Pod 启发式方法来识别其容器，就像在查找功能存在之前那样。
	IdentifyByKubeletEnv = "GPUSTACK_DEVICE_PLUGIN_IDENTIFY_BY_KUBELET"
	// kubeletPodResourcesTimeout 设定了查找的边界。kubelet 在请求分配时会被阻塞，因此无法快速响应的查
	// 找操作会拒绝分配，而不会无限期阻塞。
	kubeletPodResourcesTimeout = 2 * time.Second
)

// kubeletPodLister 返回 kubelet 当前持有的容器，以及每个容器所分配的设备信息。
type kubeletPodLister func(ctx context.Context) ([]*podresources.PodResources, error)

// newKubeletPodLister 返回一个列表器，该列表器通过 pod-resources 套接字向 kubelet 请求数据。每次调用时都
// 会建立连接：在调用之间保持的连接将持续存在，即使 kubelet 重启并替换了套接字，这些连接也不会失效。
func newKubeletPodLister(socket string) kubeletPodLister {
	return func(ctx context.Context) ([]*podresources.PodResources, error) {
		cli, err := grpc.NewClient("unix://"+socket,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return nil, fmt.Errorf("dial kubelet pod-resources socket: %w", err)
		}
		defer osx.Close(cli)

		ctx, cancel := context.WithTimeout(ctx, kubeletPodResourcesTimeout)
		defer cancel()
		resp, err := podresources.NewPodResourcesListerClient(cli).List(ctx, &podresources.ListPodResourcesRequest{})
		if err != nil {
			return nil, fmt.Errorf("list kubelet pod resources: %w", err)
		}
		return resp.GetPodResources(), nil
	}
}
