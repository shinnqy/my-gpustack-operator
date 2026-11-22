package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"k8s.io/client-go/rest"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/system"
	"gpustack.ai/gpustack/pkg/utils/json"
)

const (
	// nodeModelStoreKubeletRefresh 是重新读取节点 kubelet 阈值的时间间隔。这些阈值仅在 kubelet 重启时才会变化，且读取发现未变化时不会触发写入。
	nodeModelStoreKubeletRefresh = 30 * time.Minute
	// nodeModelStoreKubeletRetry 是未能读取到节点 kubelet 时重试的等待时间：短暂的不可达会在下一次重试时恢复，而不必等到下一次常规刷新。
	nodeModelStoreKubeletRetry = time.Minute
	// nodeModelStoreKubeletTimeout 限制单次读取的超时时间，使 kubelet 无响应的节点
	// 对其 spec 中其他字段的延迟不超过该时长。
	nodeModelStoreKubeletTimeout = 5 * time.Second
)

// kubeletReading 表示节点最近一次成功读取到的 kubelet 阈值。
type kubeletReading struct {
	thresholds *workercore.NodeModelStoreKubelet
	readTime   time.Time
}

// kubeletThresholds 返回用于节点 spec 的 kubelet 阈值：若距上次读取尚未超过刷新周期则返回上次读数，
// 否则执行新的读取。读取失败时会保留上次读数，或在 worker 重启后保留对象中已携带的值，
// 从而避免 kubelet 短暂无响应导致节点上限在 kubelet 默认值与实际值之间来回跳变；同时返回 false，
// 以便调用方在早于刷新周期时重试。thresholds 为 nil 表示从未成功读取过。
func (r *NodeModelStoreReconciler) kubeletThresholds(
	ctx context.Context, node string, current *workercore.NodeModelStoreKubelet,
) (thresholds *workercore.NodeModelStoreKubelet, read bool) {
	r.kubeletMu.Lock()
	last, ok := r.kubelet[node]
	r.kubeletMu.Unlock()
	if ok && r.now().Sub(last.readTime) < nodeModelStoreKubeletRefresh {
		return last.thresholds, true
	}

	thresholds, err := r.readKubelet(ctx, node)
	if err != nil {
		ctrllog.FromContext(ctx).Info("the node's effective kubelet configuration could not be read; "+
			"its model cache keeps the last reading or kubelet's defaults", "node", node, "error", err.Error())
		if ok {
			return last.thresholds, false
		}
		return current, false
	}

	r.kubeletMu.Lock()
	defer r.kubeletMu.Unlock()
	if r.kubelet == nil {
		r.kubelet = map[string]kubeletReading{}
	}
	r.kubelet[node] = kubeletReading{thresholds: thresholds, readTime: r.now()}

	return thresholds, true
}

// forgetKubelet 在节点对应的对象被移除后丢弃该节点的读数。
func (r *NodeModelStoreReconciler) forgetKubelet(node string) {
	r.kubeletMu.Lock()
	defer r.kubeletMu.Unlock()
	delete(r.kubelet, node)
}

func (r *NodeModelStoreReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// readKubelet 在 nodeModelStoreKubeletTimeout 时间内读取节点的阈值。
func (r *NodeModelStoreReconciler) readKubelet(ctx context.Context, node string) (*workercore.NodeModelStoreKubelet, error) {
	ctx, cancel := context.WithTimeout(ctx, nodeModelStoreKubeletTimeout)
	defer cancel()
	if r.ReadKubelet != nil {
		return r.ReadKubelet(ctx, node)
	}

	cli := system.LoopbackKubeClient.Get()
	if cli == nil {
		return nil, errors.New("no loopback client")
	}

	return readKubeletConfigz(ctx, cli.CoreV1().RESTClient(), node)
}

// readKubeletConfigz 通过 API server 的节点代理读取节点生效的 kubelet 配置。
// configz 反映的是 kubelet 实际执行的配置，即命令行标志、配置文件与 drop-in 片段合并后的结果。
// 只要启用了 kubelet 的调试处理器（默认启用），该接口即可提供服务。
func readKubeletConfigz(ctx context.Context, core rest.Interface, node string) (*workercore.NodeModelStoreKubelet, error) {
	raw, err := core.Get().
		Resource("nodes").Name(node).SubResource("proxy").Suffix("configz").
		DoRaw(ctx)
	if err != nil {
		return nil, fmt.Errorf("read the kubelet configz of node %s: %w", node, err)
	}

	return decodeKubeletConfigz(raw)
}

// decodeKubeletConfigz 从 configz 响应中提取模型缓存上限所需的阈值。
func decodeKubeletConfigz(raw []byte) (*workercore.NodeModelStoreKubelet, error) {
	var z struct {
		KubeletConfig *struct {
			EvictionHard                map[string]string `json:"evictionHard"`
			ImageGCHighThresholdPercent *int32            `json:"imageGCHighThresholdPercent"`
		} `json:"kubeletconfig"`
	}
	if err := json.Unmarshal(raw, &z); err != nil {
		return nil, fmt.Errorf("decode the kubelet configz: %w", err)
	}
	if z.KubeletConfig == nil {
		return nil, errors.New("decode the kubelet configz: no kubeletconfig in the answer")
	}

	return &workercore.NodeModelStoreKubelet{
		NodefsAvailable:             z.KubeletConfig.EvictionHard["nodefs.available"],
		ImagefsAvailable:            z.KubeletConfig.EvictionHard["imagefs.available"],
		ImageGCHighThresholdPercent: z.KubeletConfig.ImageGCHighThresholdPercent,
	}, nil
}
