// Package metrics 定义节点模型缓存的 Prometheus 指标。其标签绝不携带 Namespace、制品名、仓库名
// 或 Pod 名：这些均属于租户标识，而节点的指标会跨租户被读取。
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// 挂载结果分类。
const (
	MountHit          = "hit"
	MountMaterialized = "materialized"
	MountDenied       = "denied"
	MountPending      = "pending"
)

var (
	// DownloadBytes 按来源统计已接收的模型文件字节数。
	DownloadBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gpustack_model_manager_download_bytes_total",
		Help: "Bytes of model files received, by source.",
	}, []string{"source"})

	// Mounts 按结果统计挂载调用次数。
	Mounts = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gpustack_model_manager_mounts_total",
		Help: "Mount calls, by result: hit (published content), materialized (published by this call's " +
			"materialization), denied (authorization refused) or pending (materializing or backing off).",
	}, []string{"result"})

	// Materializations 按结果统计已结束的物化尝试次数：published 或具体失败原因。
	Materializations = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gpustack_model_manager_materializations_total",
		Help: "Finished materialization attempts, by result: published or the failure's reason.",
	}, []string{"result"})

	// PublishDuration 记录一次尝试从开始到发布所耗费的时长。
	PublishDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "gpustack_model_manager_publish_duration_seconds",
		Help:    "Seconds from a materialization's start to its publication.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 16),
	})

	// GCRemovedBytes 统计垃圾回收释放的字节数。
	GCRemovedBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "gpustack_model_manager_gc_removed_bytes_total",
		Help: "Bytes of model content removed by collection.",
	})

	// StoredBytes 和 CapacityBytes 分别记录缓存占用的字节数及其所在文件系统的总容量。
	StoredBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "gpustack_model_manager_stored_bytes",
		Help: "Bytes the node's published trees and partial downloads occupy.",
	})
	CapacityBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "gpustack_model_manager_capacity_bytes",
		Help: "Size of the filesystem holding the node's model cache.",
	})
)

// Collectors 返回上述所有指标，用于统一注册。
func Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		DownloadBytes, Mounts, Materializations, PublishDuration, GCRemovedBytes, StoredBytes, CapacityBytes,
	}
}
