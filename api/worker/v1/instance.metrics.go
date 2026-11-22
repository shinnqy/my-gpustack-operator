// SPDX-FileCopyrightText: 2026 GPUStack, Inc.
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// InstanceMetrics 是 Instance 用于读取当前资源利用率的子资源，
// 提供底层 Kubernetes Pod 的 CPU、内存、存储使用量以及所分配加速器指标的实时采样。
//
// CPU、内存和存储数据在请求时从节点 kubelet 实时读取；
// 加速器数据来自 device manager 的最新快照，若超过若干个监控周期未更新则会被丢弃。
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type InstanceMetrics struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Sample 是该 Instance 当前的资源利用率采样。
	// 当对应的数据源不可用时，其中的可选字段将缺省。
	Sample InstanceMetricsSample `json:"sample" protobuf:"bytes,2,opt,name=sample"`
}

var _ runtime.Object = (*InstanceMetrics)(nil)

// InstanceMetricsSample 是 Instance 的单次资源利用率采样点。
//
// 每个数值都是按同一单位上报的 Total/Used 配对之一，以便调用方无需读取 Instance 的 spec
// 即可通过单次采样直接计算出利用率百分比：CPU 单位为毫核（milli-cores），内存和存储单位为 MiB。
// 数据源本身以更精细的单位计量——kubelet 使用纳核（nanocores）和字节，厂商设备库使用 MiB——
// 最终以较粗的单位为准，因为计算百分比无需更高的精度。
//
// Total 来自 Instance 自身的声明，因此始终有值；Used 则是实测值，
// 在其数据源不可用时会缺省。每次单位换算均向上取整，
// 因此低于一个单位的实测用量会记为 1，而 0 表示完全没有使用。
type InstanceMetricsSample struct {
	// Timestamp 表示 kubelet 测量 CPU、内存和存储数据的时间点。
	Timestamp meta.Time `json:"timestamp" protobuf:"bytes,1,opt,name=timestamp"`

	// CPUTotalMilliCores 是 Instance 对应 Pod 的 CPU 上限（单位为毫核）。
	CPUTotalMilliCores uint64 `json:"cpuTotalMilliCores" protobuf:"varint,2,opt,name=cpuTotalMilliCores"`

	// CPUUsedMilliCores 是 Instance 对应 Pod 在 kubelet 采样窗口内的平均 CPU 使用量（单位为毫核）。
	CPUUsedMilliCores *uint64 `json:"cpuUsedMilliCores,omitempty" protobuf:"varint,3,opt,name=cpuUsedMilliCores"`

	// MemoryTotalMiB 是 Instance 对应 Pod 的内存上限（单位为 MiB）。
	MemoryTotalMiB uint64 `json:"memoryTotalMiB" protobuf:"varint,4,opt,name=memoryTotalMiB"`

	// MemoryUsedMiB 是 Instance 对应 Pod 的工作集（working set）内存使用量（单位为 MiB）。
	MemoryUsedMiB *uint64 `json:"memoryUsedMiB,omitempty" protobuf:"varint,5,opt,name=memoryUsedMiB"`

	// StorageTotalMiB 是 Instance 对应 Pod 的临时存储上限（单位为 MiB）。
	StorageTotalMiB uint64 `json:"storageTotalMiB" protobuf:"varint,6,opt,name=storageTotalMiB"`

	// StorageUsedMiB 是 Instance 对应 Pod 的临时存储使用量（单位为 MiB）。
	//
	// 这是 kubelet 执行驱逐时所依据的 Pod 级汇总用量——包括容器的可写层、日志以及本地 emptyDir 卷——
	// 因此它与 StorageTotalMiB 所报告的上限采用同一口径。当数据来自不提供存储指标的
	// metrics.k8s.io 回退路径时，该字段缺省。
	StorageUsedMiB *uint64 `json:"storageUsedMiB,omitempty" protobuf:"varint,7,opt,name=storageUsedMiB"`

	// Accelerators 保存分配给该 Instance 的加速器指标，以其获授资源的标识符为键——
	// 可以是整张设备或其中的硬件分区；当 Instance 未分配加速器或 device manager 不可达时，该字段缺省。
	//
	// +listType=map
	// +listMapKey=id
	Accelerators []InstanceAcceleratorMetrics `json:"accelerators,omitempty" protobuf:"bytes,8,rep,name=accelerators"`
}

// InstanceAcceleratorMetrics 表示 Instance 持有的单个加速器，按 Instance 自身的口径上报：
// 它在该加速器上获授的配额，以及实测使用了多少获授资源。
//
// 所有模式都上报相同的字段且具有相同的语义。持有整张设备的 Instance 直接读取该设备的指标，
// 因为整张设备即为其获授资源；持有切分份额（逻辑切片或硬件分区）的 Instance 则读取该份额的配额与用量。
// 因此，无论底层采用何种分配方式，调用方在查询“该 Instance 使用了多少显存、距离上限还有多少”时
// 只需读取相同的两个字段，完全无需感知分配细节。Mode 仅用于向需要按模式分组的调用方说明分配方式，
// 除此之外不会改变任何字段结构。
//
// Total 来自资源分配结果，只要分配过程能确定该值就会存在。Used 则是实测数值，
// 当当前节点无法测量时该字段将缺省（绝不返回 0）：例如厂商未提供按进程的设备查询接口、
// 驱动返回 NOT_SUPPORTED，或设备上的进程无法归因到对应容器。字段缺省表示“此处无法测量”，
// 具体原因由 Device Manager 的能力指标（capability gauge）指明；而 0 表示已测量且处于空闲状态。
// 因此，若切分份额的用量无法测量，将不上报任何用量，而不会回退上报整卡用量（因整卡数据包含了卡上其他租户的用量）。
type InstanceAcceleratorMetrics struct {
	// ID 是该 Instance 所持有加速器的全局唯一标识符。
	//
	// 在 Partitioned 模式下，该值是分区自身的标识符（例如 MIG UUID，而非父卡的 UUID），
	// 因为分区才是 Instance 实际获授的资源，下方所有指标也均针对该分区。
	// 这也避免了同一 Instance 持有同一张卡上的两个分区时，因共用卡级标识符而合并为一条记录。
	ID string `json:"id" protobuf:"bytes,1,opt,name=id"`

	// Mode 表示该加速器分配给 Instance 的方式：整卡独占、逻辑切片或硬件分区。
	// 它不会改变下方指标的含义，仅标识其背后的分配机制，以便调用方按此分组或过滤。
	//
	// 该字段携带的是模式的名称——"Exclusive"、"Sliced"、"Partitioned"——而非枚举的传输值，
	// 因为名称是对指标读取方唯一直观的形式，且该字段在 Device Manager 的 Prometheus 指标中
	// 也采用同样的名称。调用方绝无需在两者之间进行转换。
	Mode string `json:"mode" protobuf:"bytes,9,opt,name=mode"`

	// MemoryTotalMiB 是 Instance 在该加速器上获授的内存（单位为 MiB）：
	// 持有整张设备时为设备自身内存，持有逻辑切片时为分配时计入的配额，
	// 持有硬件分区时为该分区自身的内存。
	//
	// 逻辑切片的配额由分配所计费的单位折算回 MiB——与准入阶段计算额度时采用相同的内存基准——
	// 因此该分母绝不会与实际获授量不一致。当无法确定获授大小时该字段缺省：
	// 例如驱动未公开分区几何规格，或设备的容量信息尚未同步到当前采样中。
	MemoryTotalMiB *uint64 `json:"memoryTotalMiB,omitempty" protobuf:"varint,2,opt,name=memoryTotalMiB"`

	// MemoryUsedMiB 是实测该 Instance 在其获授资源中所占用的内存（单位为 MiB）。
	//
	// 该值直接测量自硬件而非账面记录，因此可能会超出 MemoryTotalMiB 且不会被截断：
	// 截断会把存在泄漏的配额伪装成严格受控的状态。遇到超额时应将其视为需要排查的异常现象，
	// 而非违规铁证——配额单位换算的向下取整以及驱动层的记账开销也可能导致数值略微超出。
	MemoryUsedMiB *uint64 `json:"memoryUsedMiB,omitempty" protobuf:"varint,3,opt,name=memoryUsedMiB"`

	// MemoryUtilizationPercent 是 MemoryUsedMiB 占 MemoryTotalMiB 的百分比。显式提供该字段而非交由调用方计算，
	// 是为了确保百分比与其来源字段始终一致；当其中任一字段缺失时，该字段也不存在。
	MemoryUtilizationPercent *uint32 `json:"memoryUtilizationPercent,omitempty" protobuf:"varint,4,opt,name=memoryUtilizationPercent"`

	// CoresUtilizationPercent 是测得的 Instance 实际使用的自身算力配额百分比：独占整张设备时为设备利用率，
	// 持有逻辑切片时为测得的卡占用比例除以强制上限。例如，上限为整卡五分之一的切片跑满该五分之一配额时，读数为 100 而非 20。
	//
	// 该分母定义会带来两个有意保留的后果。首先，其值可能超过 100，原因与显存指标对相同：如果 shim 层允许切片在卡空闲时
	// 突发超出上限，则说明该上限并未被强制执行，而截断处理恰好会掩盖这一情况。其次，在上限较小时粒度较粗——厂商对整卡的
	// 度量精度为整数百分比，因此 5% 的上限在此处只能产生 20 的倍数。
	//
	// 硬件分区下该字段缺失：目前尚无厂商提供按分区统计的算力数据。
	CoresUtilizationPercent *uint32 `json:"coresUtilizationPercent,omitempty" protobuf:"varint,5,opt,name=coresUtilizationPercent"` // nolint: lll

	// TemperatureCelsius 是加速器的温度（单位：摄氏度）。
	//
	// 在所有模式下均为整卡维度：切分出的份额没有独立温度，整张设备的温度是唯一能真实反映 Instance 硬件热状态的指标。
	TemperatureCelsius *uint32 `json:"temperatureCelsius,omitempty" protobuf:"varint,6,opt,name=temperatureCelsius"`

	// PowerUsageWatts 是加速器的功耗（单位：瓦特）。
	// 在所有模式下均为整卡维度，原因与 TemperatureCelsius 相同。
	PowerUsageWatts *uint32 `json:"powerUsageWatts,omitempty" protobuf:"varint,7,opt,name=powerUsageWatts"`

	// Unhealthy 表示加速器是否处于不健康状态。
	// 在所有模式下均为整卡维度：一旦整卡异常，其上的所有份额都会受到影响。
	Unhealthy *bool `json:"unhealthy,omitempty" protobuf:"varint,8,opt,name=unhealthy"`
}
