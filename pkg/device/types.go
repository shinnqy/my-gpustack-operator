package device

import (
	"time"

	klog "k8s.io/klog/v2"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

// 设备身份与拓扑元数据。
type (
	// AcceleratorAllocation 描述运行时分配的模式、额度以及硬件分区按规格记录的账本。
	AcceleratorAllocation = workercore.AcceleratorAllocation
	// AcceleratorStatus 保存设备健康和切分能力等观测状态。
	AcceleratorStatus = workercore.AcceleratorStatus
)

// 设备指标数据结构。
type (
	// AcceleratorMetrics 保存单卡的显存、核心使用率、温度与功耗等指标。
	AcceleratorMetrics struct {
		// ID 是设备的全局唯一标识。
		ID string `json:"id" yaml:"id"`
		// Memory 是设备总显存，单位为 MiB。
		Memory uint64 `json:"memory" yaml:"memory"`
		// MemoryUsage 是已用显存，单位为 MiB。
		MemoryUsage uint64 `json:"memoryUsage" yaml:"memoryUsage"`
		// MemoryUtilization 是显存使用百分比，范围为 0 至 100。
		MemoryUtilization uint32 `json:"memoryUtilization" yaml:"memoryUtilization"`
		// CoresUtilization 是核心使用百分比，范围为 0 至 100。
		CoresUtilization uint32 `json:"coresUtilization" yaml:"coresUtilization"`
		// Temperature 是摄氏温度。
		Temperature uint32 `json:"temperature" yaml:"temperature"`
		// PowerUsage 是功耗，单位为瓦。
		PowerUsage uint32 `json:"powerUsage" yaml:"powerUsage"`
		// Unhealthy 为真表示设备报告异常。
		Unhealthy bool `json:"unhealthy" yaml:"unhealthy"`
	}
	// MetricsGroup 表示一组设备指标。
	MetricsGroup struct {
		// Manufacturer 是设备厂商名称。
		Manufacturer string `json:"manufacturer" yaml:"manufacturer"`
		// Timestamp 是采集这组指标的时间。
		Timestamp time.Time `json:"timestamp" yaml:"timestamp"`
		// Accelerators 是组内各加速器的指标列表。
		Accelerators []AcceleratorMetrics `json:"accelerators" yaml:"accelerators"`
	}
	// MetricsGroupList 表示设备指标组列表。
	MetricsGroupList = []MetricsGroup
)

// 进程级设备用量含有宿主机进程信息，对外发布前按 Pod 和容器汇总。
type (
	// AcceleratorProcessReason 记录进程指标不可用的原因。字符串会随缺失值作为标签发布，
	// 使调用方能够区分无法测量与设备空闲。
	AcceleratorProcessReason string
)

const (
	// AcceleratorProcessReasonNone 表示接口已给出有效答复。
	AcceleratorProcessReasonNone AcceleratorProcessReason = ""
	// AcceleratorProcessReasonUnsupported 表示驱动对该设备不提供此查询。
	AcceleratorProcessReasonUnsupported AcceleratorProcessReason = "unsupported"
	// AcceleratorProcessReasonPermission 表示当前进程缺少执行查询的权限。
	AcceleratorProcessReasonPermission AcceleratorProcessReason = "permission"
	// AcceleratorProcessReasonDriverError 表示本次驱动调用失败，下一轮仍可重试。
	AcceleratorProcessReasonDriverError AcceleratorProcessReason = "transient_driver_error"
	// AcceleratorProcessReasonTruncated 表示驱动所需缓冲区超出读取上限，本轮进程列表不完整。
	AcceleratorProcessReasonTruncated AcceleratorProcessReason = "truncated"
)

// 设备身份与拓扑元数据。
type (
	// Topology 保存设备的 NUMA 节点和 PCI 拓扑等信息。
	Topology = workercore.DeviceTopology
	// Ethernet 保存以太网接口名称和 PCI 信息。
	Ethernet = workercore.DeviceEthernet
	// Fabric 表示设备所在的高速互连域，包括域的标识与设备在域内的地址。
	Fabric = workercore.DeviceFabric
	// AcceleratorPhysicalSlicedProfile 表示一种硬件分区规格，例如 NVIDIA MIG profile。
	AcceleratorPhysicalSlicedProfile = workercore.AcceleratorPhysicalSlicedProfile
	// AcceleratorLogicalSliced 表示单卡支持的软件逻辑切分能力。
	AcceleratorLogicalSliced = workercore.AcceleratorLogicalSliced
	// AcceleratorPhysicalSliced 表示单卡支持的硬件分区能力。
	AcceleratorPhysicalSliced = workercore.AcceleratorPhysicalSliced
	// AcceleratorSlicedDetail 保存设备组汇总后的切分能力。
	AcceleratorSlicedDetail = workercore.AcceleratorSlicedDetail
	// AcceleratorSlicedPhysicalDetailProfile 保存设备组按硬切规格汇总的数量和大小。
	AcceleratorSlicedPhysicalDetailProfile = workercore.AcceleratorSlicedPhysicalDetailProfile
	// AcceleratorPlacement 描述设备上连续的一段落位，单位由承载该字段的接口决定。
	AcceleratorPlacement = workercore.AcceleratorPlacement
	// Accelerator 保存单卡的身份、拓扑、特性和状态。
	Accelerator = workercore.Accelerator
	// DevicesGroup 表示型号和显存容量相同的一组设备。
	DevicesGroup = workercore.DevicesGroup
	// DevicesGroupList 是设备组清单。
	DevicesGroupList = []DevicesGroup
)

type (
	// DetectorOptions 是创建厂商探测器所需的选项。
	DetectorOptions struct {
		Logger klog.Logger
	}

	// Detector 负责从厂商管理库读取硬件清单。成功返回空清单表示本轮没有可报告的卡。返回错误表示本轮无法测量，
	// 外层探测循环需要保留上一次成功的清单。
	Detector interface {
		// Name 返回本探测器负责的厂商名称。
		Name() string

		// DetectAccelerator 执行一次枚举，并返回设备分组。PCI 信息不可用或不可靠时，noPciCheck 允许跳过 PCI 预检查。
		DetectAccelerator(noPciCheck bool) (DevicesGroupList, error)
	}
)
