// Package kubemetrics 读取 Instance 在 Kubernetes 侧的资源利用率：从其底层 Pod 读取声明的
// 总量上限，从节点 kubelet 统计信息读取实测用量，并通过 API Server 节点代理获取 kubelet 的
// stats summary。
//
// 它是上报这些指标的两个入口——Instance 的 "metrics" 子资源与 DeviceManager 的
// Prometheus 导出器——共用的唯一实现，从而确保两者绝不会因单位或取整步骤产生偏差。
// 加速卡的实测指标来自 DeviceManager 自身的监控快照，不属于本包职责；除 Pod 级指标外，
// 本包还统一了两个入口必须保持一致的两项判定：Pod 是否有任何容器已启动，以及切分的加速卡
// 分片获得了多少配额。
package kubemetrics

import (
	"time"

	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeletstats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"

	worker "gpustack.ai/gpustack/api/worker/v1"
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/utils/mathx"
	"gpustack.ai/gpustack/pkg/utils/quantityx"
)

// NewSample 返回仅包含 Instance 声明信息（CPU、内存和存储上限）的采样样本，
// 附带当前时间戳，且所有实测指标均为空。
//
// 导出该函数是为了支持包内调用方无法完成的一件事：在构建 Pod 的控制器旁验证
// 控制器写入的内容与本函数读回的上限一致。
func NewSample(pod *core.Pod) *worker.InstanceMetricsSample {
	sample := &worker.InstanceMetricsSample{Timestamp: meta.Now()}
	setTotals(sample, pod)
	return sample
}

// NewSampleFromResources 返回仅包含 Instance 在 spec.resources 中声明的 CPU、内存和存储上限的
// 采样样本，附带当前时间戳，且所有实测指标均为空。当调用方持有 Instance 但尚无底层 Pod 时，
// 可走这条仅计算总量的路径：在控制器尚未渲染出 Pod 之前，没有容器 limits 可读，
// 但已声明的总量依然是值得上报的事实。
//
// 对于同一份 spec.resources，它与根据控制器渲染出的 Pod 调用 NewSample 所生成的总量
// 完全一致：两者使用同一套取整算法。
func NewSampleFromResources(resources workercore.InstanceResources) *worker.InstanceMetricsSample {
	sample := &worker.InstanceMetricsSample{Timestamp: meta.Now()}
	setTotalsFromMilliCoresAndBytes(
		sample,
		resources.CPU.ScaledValue(resource.Milli),
		resources.RAM.Value(),
		resources.LocalStorage.Value(),
	)
	return sample
}

// newSampleFromPodStats 返回完整采样样本：包含 Instance 声明的总量以及 kubelet 为底层 Pod
// 测得的用量。允许任意统计字段为 nil（例如当 kubelet 的统计提供方刚接管某 Pod 时会省略这些字段），
// 此时对应的 Used 指标保持缺失。
func newSampleFromPodStats(pod *core.Pod, ps *kubeletstats.PodStats) *worker.InstanceMetricsSample {
	sample := NewSample(pod)

	// kubelet 为每个统计块单独记录时间戳，部分条目可能只有内存而无 CPU。
	// 将样本时间戳设为实际存在的时间中最旧的一个，使读取方看到的指标时效性不会高于
	// 其背后最陈旧的采样；若没有任何带时间戳的块，则保留 NewSample 记录的读取时间。
	if measured := oldestMeasurement(ps); !measured.IsZero() {
		sample.Timestamp = meta.Time{Time: measured}
	}

	if ps.CPU != nil {
		sample.CPUUsedMilliCores = nanoCoresToMilliCores(ps.CPU.UsageNanoCores)
	}
	if ps.Memory != nil {
		sample.MemoryUsedMiB = bytesToMiB(ps.Memory.WorkingSetBytes)
	}
	// 使用 Pod 级别的临时存储汇总值而非容器可写层：kubelet 驱逐依据的是汇总值
	// （可写层加日志加本地 emptyDir 卷），因此它是唯一可与 StorageTotalMiB 对比的分子。
	// 当 Instance 写满其工作区卷时，应显示为接近满载而非接近空闲。
	if ps.EphemeralStorage != nil {
		sample.StorageUsedMiB = bytesToMiB(ps.EphemeralStorage.UsedBytes)
	}

	return sample
}

// oldestMeasurement 返回当前存在的统计块中最早的测量时间；若均未携带时间戳，则返回零值时间。
func oldestMeasurement(ps *kubeletstats.PodStats) time.Time {
	var oldest time.Time
	consider := func(t meta.Time) {
		if t.IsZero() {
			return
		}
		if oldest.IsZero() || t.Time.Before(oldest) {
			oldest = t.Time
		}
	}

	if ps.CPU != nil {
		consider(ps.CPU.Time)
	}
	if ps.Memory != nil {
		consider(ps.Memory.Time)
	}
	if ps.EphemeralStorage != nil {
		consider(ps.EphemeralStorage.Time)
	}
	return oldest
}

// setTotals 从底层 Pod 的容器 limits 中填充样本声明的资源上限；Instance 控制器会从
// Instance 的 spec.resources 原样写入 limits（仅 requests 会按超卖比例缩放）。
// Init 容器不计入：它们不会与应用容器同时运行，计入会导致上限虚高到实际无法达到的值。
//
// 若 Pod 对某资源未声明 limit，则其总量为 0，表示“未声明上限”；计算百分比的调用方
// 无论如何都必须防范除零。
func setTotals(sample *worker.InstanceMetricsSample, pod *core.Pod) {
	var cpuMilliCores, memoryBytes, storageBytes int64
	for i := range pod.Spec.Containers {
		limits := pod.Spec.Containers[i].Resources.Limits
		if q, ok := limits[core.ResourceCPU]; ok {
			cpuMilliCores += q.ScaledValue(resource.Milli)
		}
		if q, ok := limits[core.ResourceMemory]; ok {
			memoryBytes += q.Value()
		}
		if q, ok := limits[core.ResourceEphemeralStorage]; ok {
			storageBytes += q.Value()
		}
	}

	setTotalsFromMilliCoresAndBytes(sample, cpuMilliCores, memoryBytes, storageBytes)
}

// setTotalsFromMilliCoresAndBytes 应用所有总量路径共用的取整规则：
// CPU 保持原值不变，内存和存储向上取整到最近的 MiB。
func setTotalsFromMilliCoresAndBytes(sample *worker.InstanceMetricsSample, cpuMilliCores, memoryBytes, storageBytes int64) {
	sample.CPUTotalMilliCores = uint64(cpuMilliCores)
	sample.MemoryTotalMiB = mathx.CeilDiv(uint64(memoryBytes), uint64(quantityx.Mi))
	sample.StorageTotalMiB = mathx.CeilDiv(uint64(storageBytes), uint64(quantityx.Mi))
}

// bytesToMiB 将可选的字节数转换为 MiB，并保留缺失状态（nil）。
// 商向上取整以确保数据源测得的任何用量都保持可见：空闲 Instance 的工作集通常小于 1 MiB，
// 若直接截断，会把已测得的用量显示为完全没有使用。
func bytesToMiB(bytes *uint64) *uint64 {
	if bytes == nil {
		return nil
	}
	mib := mathx.CeilDiv(*bytes, uint64(quantityx.Mi))
	return &mib
}

// nanoCoresToMilliCores 将可选的 nanocore 数值转换为毫核（milli-cores），保留缺失状态（nil），
// 并基于与 bytesToMiB 相同的原因向上取整：空闲 Instance 的消耗远低于 1 毫核，
// 且百分比计算并不需要更细的单位。
func nanoCoresToMilliCores(nanoCores *uint64) *uint64 {
	if nanoCores == nil {
		return nil
	}
	milliCores := mathx.CeilDiv(*nanoCores, uint64(1_000_000))
	return &milliCores
}
