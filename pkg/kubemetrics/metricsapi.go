package kubemetrics

import (
	"context"
	"fmt"

	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	"gpustack.ai/gpustack/pkg/system"
	"gpustack.ai/gpustack/pkg/utils/json"
)

// fetchPodUsageFromMetricsAPI 通过通用 REST 客户端（不依赖 k8s.io/metrics）从
// metrics.k8s.io API 读取 Pod 的 CPU 和内存用量，单位分别为 nanocores 和字节。
//
// 这是 kubelet 之后的降级数据源：它不包含存储指标，且在没有 metrics adapter 的集群中
// 完全不可用。cpu 为 nil 且 error 为 nil 即表示该接口未提供服务，调用方必须将其与
// 真实失败区分开来。
//
// 若 adapter 提供了采样时间戳也会一并返回，以便调用方丢弃早于当前 Pod 创建时间、
// 实属同名旧实例的条目。对于无时间戳的采样，该字段为 nil，此时不适用时间检查：
// adapter 自身未标注时间的条目，我们也无法判断其时间。
func fetchPodUsageFromMetricsAPI(ctx context.Context, pod *core.Pod) (*uint64, *uint64, *meta.Time, error) {
	raw, err := system.LoopbackKubeClient.Get().CoreV1().RESTClient().Get().
		AbsPath("/apis/metrics.k8s.io/v1beta1/namespaces/" + pod.Namespace + "/pods/" + pod.Name).
		DoRaw(ctx)
	if err != nil {
		if kerrors.IsNotFound(err) {
			return nil, nil, nil, nil
		}
		return nil, nil, nil, fmt.Errorf("failed to get pod metrics of %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	return parsePodMetricsUsage(raw)
}

// parsePodMetricsUsage 解析 metrics.k8s.io 的 PodMetrics 结构并汇总各容器用量：
// CPU 单位为 nanocores，内存单位为字节。
func parsePodMetricsUsage(raw []byte) (*uint64, *uint64, *meta.Time, error) {
	var podMetrics struct {
		Timestamp  meta.Time `json:"timestamp"`
		Containers []struct {
			Usage map[core.ResourceName]resource.Quantity `json:"usage"`
		} `json:"containers"`
	}
	if err := json.Unmarshal(raw, &podMetrics); err != nil {
		return nil, nil, nil, fmt.Errorf("failed to decode pod metrics: %w", err)
	}

	// 不含容器的响应没有携带任何采样数据：将其报告为未提供服务，而非真正的零用量。
	if len(podMetrics.Containers) == 0 {
		return nil, nil, nil, nil
	}

	// 任意 adapter 都可能提供 metrics.k8s.io 服务，因此数值可能为负或异常；
	// 若不经检查直接转换为无符号整数，负数会变成 EB 级别的“当前用量”。
	var cpu, memory uint64
	for i := range podMetrics.Containers {
		if q, ok := podMetrics.Containers[i].Usage[core.ResourceCPU]; ok {
			cpu += nonNegative(q.ScaledValue(resource.Nano))
		}
		if q, ok := podMetrics.Containers[i].Usage[core.ResourceMemory]; ok {
			memory += nonNegative(q.Value())
		}
	}
	ts := &podMetrics.Timestamp
	if ts.IsZero() {
		ts = nil
	}
	return &cpu, &memory, ts, nil
}

// nonNegative 将有符号采样值截断为非负的无符号值。
func nonNegative(v int64) uint64 {
	if v < 0 {
		return 0
	}
	return uint64(v)
}
