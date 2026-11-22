package kubemetrics

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	kubeletstats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"

	worker "gpustack.ai/gpustack/api/worker/v1"
	"gpustack.ai/gpustack/pkg/system"
	"gpustack.ai/gpustack/pkg/utils/json"
)

// DefaultMaxAge 是指没有自主采样策略的调用者所缓存的节点读取结果可接受的最大年龄——这种由请求驱动的采样方
// 式将每个节点的读取次数限制在单次。
//
// 在固定周期进行采样的调用者必须设置为 0，而不是其周期值。其轮次开始时间恰好间隔一个周期，而条目在轮间中
// 点被标记，因此条目始终比周期值年轻——它会在每隔一轮读取源数据，并在该轮之间重新发布内容，其频率仅为所发
// 布的半倍。
const DefaultMaxAge = 15 * time.Second

// FetchPodSample 返回一个 Instance Pod 的利用率样本：包括其声明的总利用率，以及节点 kubelet 测量的使用情
// 况。
//
// 当 kubelet 无法响应时——可能是无法访问，或者响应时未携带 Pod，这种情况发生在重启后其统计提供器重新获取数
// 据时——使用情况会降级到 metrics.k8s.io API，该 API 提供 CPU 和内存数据，但不提供存储数据。总数据量在任何
// 情况下都保持不变，因为它们来自“实例”自身的声明，而非测量结果。
//
// kubelet 的读取结果是节点级的，因此 maxAge 表示调用者仍接受该节点的读取结果保留多久。关于其含义，请参考
// fetchPodStatsFromKubelet。
//
// 返回的错误信息会列出所有失败的来源及其原因，因此 API 调用方可以原样将消息交付给其客户端。
func FetchPodSample(
	ctx context.Context,
	nodeName string,
	pod *core.Pod,
	maxAge time.Duration,
) (*worker.InstanceMetricsSample, error) {
	pods, err := fetchPodStatsFromKubelet(ctx, nodeName, maxAge)
	if err == nil {
		if ps := podStatsOf(pods, pod); ps != nil {
			return newSampleFromPodStats(pod, ps), nil
		}
		// 缓存的读取结果可能早于容器本身的状态。在断定 kubelet 不知道该信息之前，请重新读取一次：
		// 基于过时的读取结果进行降级操作会消耗存储空间，甚至当指标 API 仍保留旧数据时，可能会将容
		// 器视为已消失。
		if maxAge > 0 {
			if fresh, ferr := refetchPodStatsFromKubelet(ctx, nodeName, maxAge); ferr == nil {
				if ps := podStatsOf(fresh, pod); ps != nil {
					return newSampleFromPodStats(pod, ps), nil
				}
			}
		}
		// kubelet 已返回响应，但其中没有该 Pod 的信息。此时也走降级路径，避免给 CPU 和内存数据标
		// 记一个实际从未采集过的时间。
		err = fmt.Errorf("node %s reports no stats for the pod", nodeName)
	}

	cpu, memory, ts, ferr := fetchPodUsageFromMetricsAPI(ctx, pod)
	// 这个指标条目是在该 Pod 存在之前测量的：它属于同一名称的先前版本——不应使用它。
	previousIncarnation := ferr == nil && cpu != nil && ts != nil && ts.Time.Before(pod.CreationTimestamp.Time)

	// 说明具体原因：“指标 API 失败”、“在此处无法访问”以及“仅知道之前存在名为此的 Pod”是三种不同的操作
	// 行为。
	switch {
	case ferr != nil:
		return nil, fmt.Errorf(
			"failed to read the usage of instance pod %s/%s from the kubelet (%w) and from the metrics API (%w)",
			pod.Namespace, pod.Name, err, ferr,
		)
	case previousIncarnation:
		return nil, fmt.Errorf(
			"failed to read the usage of instance pod %s/%s from the kubelet (%w); "+
				"the metrics API only knows a previous incarnation of this pod name",
			pod.Namespace, pod.Name, err,
		)
	case cpu == nil:
		return nil, fmt.Errorf(
			"failed to read the usage of instance pod %s/%s from the kubelet (%w); "+
				"the metrics.k8s.io API is not served in this cluster",
			pod.Namespace, pod.Name, err,
		)
	}

	// 回退方案没有存储数据，因此 StorageUsedMiB 字段保持为空。
	sample := NewSample(pod)
	sample.CPUUsedMilliCores = nanoCoresToMilliCores(cpu)
	sample.MemoryUsedMiB = bytesToMiB(memory)
	if ts != nil {
		sample.Timestamp = *ts
	}
	return sample, nil
}

// FetchPodSamples 根据 pod UID 返回每个 pod 的一个使用情况样本。
//
// 这是 FetchPodSample 的节点级对应实现，用于让调用者一次性采样节点的每个 Instance。它获取的是 Pod，因为样
// 本的汇总数据源自其容器限制，而 kubelet 的输出并不包含这些限制信息。此外，它根据 UID 对结果进行键化处理，
// 因为样本本身没有独立的身份标识。
//
// 两者在处理 kubelet 无法处理的 Pod 问题上存在差异。FetchPodSample 会将该 Pod 降级为 metrics.k8s.io 格式，
// 因为其调用者曾询问过该 Pod 的问题，必须给出答案。而在这里，该 Pod 则被完全忽略：报告整个节点的 Exporter
// 没有对象可以回应单个 Pod 的问题，而对每个 Pod 进行降级操作则会导致每个周期中每个 Pod 多产生一次请求。只
// 有整个节点范围内的读取失败才属于错误，因为那会同时使所有 Pod 失去其数据。
func FetchPodSamples(
	ctx context.Context,
	nodeName string,
	pods []*core.Pod,
	maxAge time.Duration,
) (map[types.UID]*worker.InstanceMetricsSample, error) {
	stats, err := fetchPodStatsFromKubelet(ctx, nodeName, maxAge)
	if err != nil {
		return nil, err
	}

	samples := make(map[types.UID]*worker.InstanceMetricsSample, len(pods))
	for _, pod := range pods {
		ps := podStatsOf(stats, pod)
		if ps == nil {
			continue
		}
		samples[pod.UID] = newSampleFromPodStats(pod, ps)
	}
	return samples, nil
}

// readTimeout 设定了单个节点读取的超时限制。该读取操作被特意与启动它的调用者分离——其他节点正在等待该读取
// 结果，其中某个节点放弃时，不应导致该读取操作被取消——因此它具有自己的截止时间。
const readTimeout = 10 * time.Second

// fetchPodStatsFromKubelet 返回了给定节点 kubelet 通过 API-server 节点代理测量的每个 Pod 统计信息。
//
// 通过代理进行操作意味着不需要进行 kubelet 地址解析，也不需要进行 kubelet TLS 处理，同时会保留调用者的
// ServiceAccount 令牌在其原处：直接读取 kubelet 会将该令牌交给一个在大多数发行版上无法验证其服务证书的对
// 等方。
//
// 读取结果是整个节点的，因此如果询问某个节点的多个 Pod，否则每个 Pod 都会重新读取相同的数据。有两点可以避
// 免这种情况：在 maxAge 之前成功读取的数据会从进程全局缓存中获取。对于某个节点的并发缺失情况，会合并到一
// 次正在进行的读取中。当 maxAge 为 0 或更低时，就会绕过缓存——但不会发生数据合并，因为数据合并没有任何成本
// 且总是正确的。无论哪种情况，数据的新鲜度都能被保留，因为样本会被标记为 kubelet 自身的测量时间，而不是读
// 取时间。失败数据则永远不会被缓存。
//
// 当其他人的阅读正在进行时，等待的调用者仍会继续工作，只要其自身上下文允许。
//
// 返回的切片在 maxAge 时间窗口内与其他调用者共享：可以读取它，但不可写入。
func fetchPodStatsFromKubelet(
	ctx context.Context,
	nodeName string,
	maxAge time.Duration,
) ([]kubeletstats.PodStats, error) {
	return podStatsFromKubelet(ctx, nodeName, maxAge, true)
}

// refetchPodStatsFromKubelet 会读取过期的缓存数据，这些数据是调用者之前发现无法使用的——即该 Pod 在出现之
// 前就存在的数据——它会像其他读取数据一样，在 maxAge 时间范围内缓存所读取的数据。如果不替换缓存数据，过期
// 的条目将在其有效时间内继续存在，因此每个调用者都会重复进行同样的额外读取操作。
func refetchPodStatsFromKubelet(
	ctx context.Context,
	nodeName string,
	maxAge time.Duration,
) ([]kubeletstats.PodStats, error) {
	return podStatsFromKubelet(ctx, nodeName, maxAge, false)
}

// podStatsFromKubelet 执行读取操作，可选择先从缓存中提供数据。是否查询缓存以及结果是否被缓存是两个不同的
// 问题：强制重新读取会跳过查找过程，但仍会保存所读取的内容。
func podStatsFromKubelet(
	ctx context.Context,
	nodeName string,
	maxAge time.Duration,
	useCache bool,
) ([]kubeletstats.PodStats, error) {
	if useCache {
		if pods, ok := podStatsCache.load(nodeName, maxAge); ok {
			return pods, nil
		}
	}

	ch := podStatsFlight.DoChan(nodeName, func() (any, error) {
		// 调用者可能会出现在主节点存储读取结果与执行操作之间的空白期，从而被遗忘，否则就会重新读
		// 取已缓存的内容。
		if useCache {
			if pods, ok := podStatsCache.load(nodeName, maxAge); ok {
				return pods, nil
			}
		}
		readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), readTimeout)
		defer cancel()
		return getPodStats(readCtx, nodeName, maxAge)
	})

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("failed to get kubelet stats summary of node %s: %w", nodeName, ctx.Err())
	case r := <-ch:
		if r.Err != nil {
			return nil, r.Err
		}
		return r.Val.([]kubeletstats.PodStats), nil
	}
}

// getPodStats 会执行一次节点代理读取操作，并在成功时将其缓存起来。
func getPodStats(
	ctx context.Context,
	nodeName string,
	maxAge time.Duration,
) ([]kubeletstats.PodStats, error) {
	raw, err := system.LoopbackKubeClient.Get().CoreV1().RESTClient().Get().
		Resource("nodes").Name(nodeName).SubResource("proxy").
		Suffix("stats", "summary").
		DoRaw(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get kubelet stats summary of node %s: %w", nodeName, err)
	}

	summary := &kubeletstats.Summary{}
	if err = json.Unmarshal(raw, summary); err != nil {
		return nil, fmt.Errorf("failed to decode kubelet stats summary of node %s: %w", nodeName, err)
	}

	podStatsCache.store(nodeName, summary.Pods, maxAge)
	return summary.Pods, nil
}

// podStatsOf 返回给定 Pod 的读取数据条目。如果属于节点级读取，则返回 `nil`——例如，kubelet 的统计提供器尚
// 未获取该 Pod 的数据。
//
// 该匹配包含 Pod UID：被删除后重新创建的 Pod 会保留其命名空间和名称，绝不得提供之前版本的内容。
func podStatsOf(pods []kubeletstats.PodStats, pod *core.Pod) *kubeletstats.PodStats {
	for i := range pods {
		ref := &pods[i].PodRef
		if ref.Namespace == pod.Namespace && ref.Name == pod.Name && ref.UID == string(pod.UID) {
			return &pods[i]
		}
	}
	return nil
}

// maxCachedNodes 定义了同时保留的节点输出数量上限。仅通过年龄扫描无法达到此限制：一个客户端在单个窗口内遍
// 历大型集群中的每个 Instance，就会读取所有节点，每个条目都是该节点上所有 Pod 的完整解析摘要。超过此数量
// 后，最旧的节点会优先被处理——因为它们无论如何都是即将过期的。
const maxCachedNodes = 64

// cacheRetention 指的是无论谁请求，条目都会被保留的时间长度。这种清理机制并非按照调用者的 maxAge 来设定：
// 如果调用者只接受非常新的数据，那么其他调用者仍然可以获取仍然完好的条目。无论采用哪种方式，都不会提供过
// 时的数据——加载过程会将每个条目与其调用者所指定的有效期进行比较，因此这仅用于决定哪些条目值得保留，而
// maxCachedNodes 则用于限制内存容量。
const cacheRetention = 5 * time.Minute

// podStatsCache 保存了每个节点最新的成功读取数据，因此一个节点上的多个 pod 会共享一个节点代理请求，而非各
// 自单独请求。
var podStatsCache = podStatsStore{entries: map[string]podStatsEntry{}}

// podStatsFlight 会同时处理每个运行中的节点的一次读取操作。仅就缓存而言，它就能将稳定的轮询负载减少一半：
// 一个客户端同时查询节点的每个 Instance，就会同时被拒绝和读取，每次都发生在特定的时间窗口内。
var podStatsFlight singleflight.Group

type podStatsEntry struct {
	pods      []kubeletstats.PodStats
	fetchedAt time.Time
}

type podStatsStore struct {
	mu      sync.RWMutex
	entries map[string]podStatsEntry
}

// 当节点年龄小于 maxAge 时，load 函数会返回该节点的缓存读取值。
func (c *podStatsStore) load(nodeName string, maxAge time.Duration) ([]kubeletstats.PodStats, bool) {
	if maxAge <= 0 {
		return nil, false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	e, ok := c.entries[nodeName]
	if !ok || time.Since(e.fetchedAt) >= maxAge {
		return nil, false
	}
	return e.pods, true
}

// store 会保存节点的读取结果，在超过 cacheRetention 时删除所有条目，然后保留剩余的最旧数据，直到最多保留
// maxCachedNodes 个节点。如果没有进行清理操作，映射表在进程运行期间会按每个节点存储一次读取结果。如果没有
// 设置限制，大集群中的一次读取操作会一次性获取所有数据。
func (c *podStatsStore) store(nodeName string, pods []kubeletstats.PodStats, maxAge time.Duration) {
	if maxAge <= 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for name, e := range c.entries {
		if now.Sub(e.fetchedAt) >= cacheRetention {
			delete(c.entries, name)
		}
	}
	c.entries[nodeName] = podStatsEntry{pods: pods, fetchedAt: now}
	c.evictOldest()
}

// evictOldest 会按最旧的顺序移除条目，直到存储量重新达到 maxCachedNodes 值。调用者持有写锁。
func (c *podStatsStore) evictOldest() {
	for len(c.entries) > maxCachedNodes {
		var oldestName string
		var oldestAt time.Time
		for name, e := range c.entries {
			if oldestName == "" || e.fetchedAt.Before(oldestAt) {
				oldestName, oldestAt = name, e.fetchedAt
			}
		}
		delete(c.entries, oldestName)
	}
}
