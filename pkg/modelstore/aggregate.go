package modelstore

import (
	"sort"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

// model-manager 插件的 Pod（按 chart 中的标签定义）及其安全端口的名称。
const (
	PluginComponent      = "model-manager"
	PluginSecurePortName = "https"
	// DefaultPluginSecurePort 是 chart 中的默认安全端口，用于端口未命名的 Pod。
	DefaultPluginSecurePort = 32444
)

// Aggregate 表示某份模型内容在各节点上的分布情况：包括按状态统计的节点数以及下载中节点的进度。
// 每个节点均持有或正在下载完整副本。
type Aggregate struct {
	Ready, Downloading, Failed int32
	// DownloadedBytes 汇总所有下载中节点已持有的字节数。
	DownloadedBytes int64
	// FailureReasons 按失败原因统计失败节点数量。
	FailureReasons map[string]int32

	// fractions 汇总每个下载中节点已下载量占其总大小的比例之和。
	fractions float64
}

// NodeEntries 从包含指定 digest 的存储中提取各节点对应的条目，并按节点名称索引返回。
func NodeEntries(stores []workercore.NodeModelStore, digest string) map[string]workercore.NodeModelStoreModel {
	entries := map[string]workercore.NodeModelStoreModel{}
	for i := range stores {
		for _, m := range stores[i].Status.Models {
			if m.Digest == digest {
				entries[stores[i].Name] = m
			}
		}
	}

	return entries
}

// AggregateEntries 统计同一份内容在各节点上的条目状态与进度。
func AggregateEntries(entries map[string]workercore.NodeModelStoreModel) Aggregate {
	a := Aggregate{FailureReasons: map[string]int32{}}
	for _, m := range entries {
		switch m.State {
		case workercore.NodeModelStoreModelStateReady:
			a.Ready++
		case workercore.NodeModelStoreModelStateFailed:
			a.Failed++
			a.FailureReasons[m.Reason]++
		case workercore.NodeModelStoreModelStateDownloading:
			a.Downloading++
			a.DownloadedBytes += m.DownloadedBytes
			if m.SizeBytes > 0 {
				a.fractions += min(float64(m.DownloadedBytes)/float64(m.SizeBytes), 1)
			}
		}
	}

	return a
}

// DownloadingPercent 返回下载中节点的平均进度（向下取整的整数百分比），若当前无节点在下载则返回 false。
// 就绪节点仅计入状态统计而不参与均值计算，以免新开始下载的节点拉低已完成副本的平均值。
func (a Aggregate) DownloadingPercent() (int32, bool) {
	if a.Downloading == 0 {
		return 0, false
	}

	return int32(a.fractions / float64(a.Downloading) * 100), true // nolint: gosec // within 0..100.
}

// SteppedPercent 是将 DownloadingPercent 向下取整到 5 的倍数（存储状态更新的步长）的结果；
// 若当前无节点在下载则返回 nil。
func (a Aggregate) SteppedPercent() *int32 {
	p, ok := a.DownloadingPercent()
	if !ok {
		return nil
	}
	p -= p % 5

	return &p
}

// Reasons 返回按名称排序的失败原因及其计数列表。
func (a Aggregate) Reasons() []string {
	reasons := make([]string, 0, len(a.FailureReasons))
	for r := range a.FailureReasons {
		reasons = append(reasons, r)
	}
	sort.Strings(reasons)

	return reasons
}
