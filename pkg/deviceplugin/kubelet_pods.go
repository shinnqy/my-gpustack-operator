package deviceplugin

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	podresources "k8s.io/kubelet/pkg/apis/podresources/v1"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

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

// _KubeletPending 用 kubelet 自己的记录回答设备插件请求缺少的容器身份问题。kubelet 先把 Pod 纳入管理，
// 再逐个准入，并在 Allocate 返回后才登记该容器的设备。因此调用发生时，要找的是已被 kubelet 接收、
// 却还没拿到这类设备的容器。只看创建时间无法得知到达 kubelet 的顺序，而时间戳本身也可能相同。
type _KubeletPending struct {
	// pods 为 kubelet 接管的每个 Pod 保存容器状态。true 表示该容器已经持有目标资源，false 表示还没有。
	pods map[types.NamespacedName]map[string]bool
}

// kubeletPending 查询哪些容器还在等待 resName。查询关闭或 kubelet 无法应答时返回 nil，
// 由调用方通过本地候选推断。
func (r *DevicesReconciler) kubeletPending(ctx context.Context, resName core.ResourceName) *_KubeletPending {
	if r.kubeletPods == nil {
		return nil
	}
	listed, err := r.kubeletPods(ctx)
	if err != nil {
		ctrllog.FromContext(ctx).Info("kubelet pod resources unavailable; identifying the allocating container "+
			"by the pending-Pod heuristic", "resource", resName, "reason", err.Error())
		return nil
	}

	return kubeletPendingFrom(listed, resName)
}

func kubeletPendingFrom(listed []*podresources.PodResources, resName core.ResourceName) *_KubeletPending {
	pending := &_KubeletPending{pods: make(map[types.NamespacedName]map[string]bool, len(listed))}
	for _, p := range listed {
		ctrs := make(map[string]bool, len(p.GetContainers()))
		for _, c := range p.GetContainers() {
			held := false
			for _, d := range c.GetDevices() {
				if d.GetResourceName() == string(resName) && len(d.GetDeviceIds()) > 0 {
					held = true
					break
				}
			}
			ctrs[c.GetName()] = held
		}
		pending.pods[types.NamespacedName{Namespace: p.GetNamespace(), Name: p.GetName()}] = ctrs
	}
	return pending
}

// waitsForDevice 检查 kubelet 是否持有该 Pod，而且该容器尚无目标设备或未被列出。
// PodResources 可能省略不可重启的 init 容器，因此无法仅凭缺少这一行就排除它。
func (k *_KubeletPending) waitsForDevice(pod *core.Pod, ctr *core.Container) bool {
	ctrs, ok := k.pods[types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}]
	if !ok {
		return false
	}
	held, reported := ctrs[ctr.Name]
	return !reported || !held
}
