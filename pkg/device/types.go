package device

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"
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

	// AcceleratorProcess 是制造商报告存在加速器的一个进程。
	//
	// 这些数值只是参考值，因为无法测量其中一项的厂商不应被理解为测量了零：厂商的库中设有专门用于此目
	// 的的标记，而作为数字使用的那个标记，就是该领域所能容纳的最大数值。
	AcceleratorProcess struct {
		// PID 是进程的 HOST pid，正如厂商库所报告的那样。容器化进程的名称由主机看到的 pid 决定，
		// 而非进程自身看到的 pid。
		PID uint32
		// MemoryBytes 是指该进程所持有的设备内存大小，单位为字节——采用厂商的本地单位，而非 MiB，
		// 因此在任何转换之前会先对子 MiB 行进行求和。如果厂商报告了数值占位符而非具体数字，则值为
		// 空。
		MemoryBytes *uint64
		// CoresPercent 表示整个加速器在测量该处理过程中所使用的计算资源比例，范围在 [0,100] 之间。
		//
		// NIL 表示不可测量，这与上述内存的情况相同。而表示“已测量且空闲”的适配器则必须通过指向零
		// 的指针来表明这一点。这一区别不能由用户自行决定：在某个库中，样本列表中没有的进程处于空
		// 闲状态，因为该库仅报告非零样本。在另一个库中，某行包含一个哨兵值，因为该硬件版本根本无
		// 法测量占用情况。两者都以同一字段的形式呈现，因此适配器是唯一能确定其属于哪一类的地方。
		CoresPercent *uint32
	}

	// AcceleratorProcesses 是每个加速器下的进程行数据，以及为什么某个入口点根本没有产生任何行数据。
	//
	// 这两个原因是相互独立的：驱动程序通常占用进程内存，同时拒绝使用进程资源。将它们合并为一个原因则
	// 无法完整反映其功能。没有设定原因的“进程”列表为空，就代表了处于空闲状态的设备状态。
	AcceleratorProcesses struct {
		// ID 是这些行所属加速器的唯一标识符，与 AcceleratorMetrics 相同的标识符。
		ID string
		// MemoryReason 表示没有行拥有可用内存，当读取到内存时该字段为空。
		MemoryReason AcceleratorProcessReason
		// CoresReason 是为什么没有一行能承载可用利用率，在读取时为空。
		CoresReason AcceleratorProcessReason
		// “Processes”指设备所报告的每个进程的一行数据。
		Processes []AcceleratorProcess
	}

	// AcceleratorProcessesGroup 表示每个加速器在一次遍历中对应的一个制造商的进程行。
	AcceleratorProcessesGroup struct {
		// Manufacturer 是设备制造商的名称。
		Manufacturer string
		// 时间戳是指这些行被收集的时间。
		Timestamp time.Time
		// Accelerators 是该组中每个加速器行的列表。
		Accelerators []AcceleratorProcesses
	}
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

	// 接口代表工人之一的网络接口，包括其总线坐标和 RDMA 链接状态。
	Interface = workercore.DeviceInterface

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

		// MonitorAccelerator 会监控系统中的加速器，并返回设备指标组的列表。
		//
		// 如果 noPciCheck 为 true，在监控加速器时，检测器将跳过 PCI 检查。这对于那些无法获取或不
		// 可靠的 PCI 信息的平台非常有用。
		MonitorAccelerator(noPciCheck bool) (MetricsGroupList, error)
	}

	// AcceleratorProcessDetector 是 Detector 的可选配套组件，仅由其库能够识别设备所连接的进程及其数量
	// 的生产商实现。
	//
	// 这是故意不作为 Detector 的一部分的。无法响应的检测器则不会实现它，这样就能保持那九种实现和所有
	// 测试结果不变，同时“该制造商不提供针对每个进程的查询”也成为一种编译时事实，而非必须在运行时返回
	// 错误的方法。
	AcceleratorProcessDetector interface {
		// MonitorAcceleratorProcesses 返回由 deviceIDs 指定的加速器的每个进程数据行，以厂商自己的
		// 单位表示，并保留厂商的语义。
		//
		// 只查询那些加速器，结果中会包含每个加速器的条目，而其他类型则没有。调用者指定这些设备，
		// 因为“每个进程查询”仅指设备上的特定分配情况：查询其余内容将消耗厂商和/proc 系统资源，而
		// 这些资源的数量无人能确定，而且会让某个未被划分的卡上的进程无法测量其数据，对任何人都没
		// 有好处。
		//
		// 如果 noPciCheck 为 true，检测器将跳过 PCI 检查，这与两种检测器方法的行为一致。
		MonitorAcceleratorProcesses(
			noPciCheck bool, deviceIDs sets.Set[string],
		) (AcceleratorProcessesGroup, error)
	}
)

// 用于硬件分区使用的结构。
//
// 硬件分区是以该分区的自身设备句柄来测量的，而非以父卡上的句柄来测量。其地址由分配记录中的标识符确定，而
// 非由哪个分区恰好包含我们的进程来决定。正因如此，一个空闲分区可以报告为零：没有进程的分区仍然可以被命名，
// 而基于进程的查找则无法找到任何内容，只能发布其不存在的状态。
//
// 因此，这些根本不携带进程 ID，请求也不包含租户身份：哪个 Pod 和容器拥有分区，取决于调用者的加入情况，而
// Pod 列表也处于这种状态。
type (
	// AcceleratorPartitionRequest 指定了一个用于读取的硬件分区，该分配信息记录了以下内容：父加速器及
	// 该分区的自身标识符，以及规格文件。
	AcceleratorPartitionRequest struct {
		// DeviceID 是 PARENT 加速器的唯一标识符，它根据卡片所读取的源对请求进行分组。
		DeviceID string
		// ID 是分区自身的标识符，由分配机制记录下来，也是分区被引用的唯一对象。如果是在设备插件记
		// 录之前进行的分配，则为空，此时会以“无”作为回应并说明原因，而非通过推导得到：推导意味着
		// 将下方的规格名称转换为驱动程序配置文件 ID，这需要查阅制造商的整个配置文件目录——一个分区
		// 制造商有 17 个 ID，另一个有 85 个——每个卡片、每个周期，都是为了恢复分配器免费保存的内容。
		//
		// 这是一种寻找分区的方法，并不能证明哪个分区已被授予。这些标识符基于名称，源自父分区和实
		// 例自身的身份，因此破坏一个分区并在同一位置创建另一个分区，会返回原来的那个分区。
		ID string
		// Profile 是分区规格的名称，例如 "1g.10gb"。该名称用于 echo 命令的响应以及日志以指定授权
		// 对象，而非用于解决任何问题。
		Profile string
		// “Placements”指的是分区在父系统中的占用区域，以制造商自己的划分单位来计算——即当今每个分
		// 区制造商都对应一个区域。其运作原因与“Profile”相同。
		Placements []AcceleratorPlacement
	}
	// AcceleratorPartition 是某个被请求的分区的答案。它反映了请求中的三个识别字段，因此调用者无需依赖
	// 回复的顺序即可将其匹配起来。
	//
	// 无论是否能够从中读取到任何内容，每个请求都会得到回应：包含“空值”以及原因的回应会使无法读取的分
	// 区保持为空闲状态。而若省略了回应，在调用者报告测量设备时就会变为如此。
	AcceleratorPartition struct {
		// DeviceID，规格和放置位置均为请求自身内容，保持不变。
		DeviceID   string
		Profile    string
		Placements []AcceleratorPlacement

		// ID 是分区自身的唯一标识符——一个 MIG UUID，而非父卡片的。它决定了分区被报告的内容，因为
		// 持有者获得的 것은分区而非卡片。当无法访问分区的句柄时，该值即为空，这与导致下方两个值变
		// 为空的失败情况相同。
		ID string

		// MemoryTotalBytes 是该分区自身的内存容量，其句柄以厂商的本地单位进行报告。该数值来自驱动
		// 程序，而非规格名称，因为名称存在四舍五入：一个 "1g.10gb" 分区包含 9856 MiB，而非 10240。
		MemoryTotalBytes *uint64
		// MemoryUsedBytes 是指该分区自身句柄中存储的设备内存大小，以厂商的本地单位表示。如果数值
		// 无法读取，则设为空，原因见下方说明。
		MemoryUsedBytes *uint64

		// CoresPercent 是分区自身的计算利用率，其句柄会报告该值。
		//
		// 所有那些库不提供按分区计算的功能的制造商都处于空白状态，而大多数厂商都是如此：NVML 在分
		// 区卡上既无法提供按进程计算的结果，也无法提供按实例计算的结果。而 Hygon 却能在分区自身接
		// 口上提供此类结果——在运行 matmul 循环的实例上，该指标达到 95%，而同一卡上另外三个处于空
		// 闲状态的实例则显示为 0——因此，这个领域是存在的，而非所谓的“缺失”是普遍现象。
		CoresPercent *uint32

		// MemoryReason 决定了这两个内存数值在读取时均为 null 或空。
		MemoryReason AcceleratorProcessReason
		// CoresReason 是 CoresPercent 在读取时变为 nil 或空的原因。
		CoresReason AcceleratorProcessReason
	}
	// AcceleratorPartitionsGroup 是某制造商在一次处理中针对每个分区给出的答案。
	AcceleratorPartitionsGroup struct {
		// Manufacturer 是设备制造商的名称。
		Manufacturer string
		// 时间戳是指分区被读取的时间。
		Timestamp time.Time
		// Partitions 每次请求返回一个答案，顺序随机。
		Partitions []AcceleratorPartition
	}
	// AcceleratorPartitionDetector 是 Detector 的可选配套组件，仅由那些拥有硬件分区自身句柄的库文件制
	// 造商实现。它与 AcceleratorProcessDetector 是分开的，因为两者关注的问题不同：前者关注哪些进程持
	// 有卡片，而后者关注某个硬件分区包含什么内容。制造商可以单独使用其中一个组件，而不需要同时使用另
	// 一个。
	AcceleratorPartitionDetector interface {
		// MonitorAcceleratorPartitions 会读取由请求指定的分区，并通过其分配记录中的标识符来引用每
		// 个分区的句柄。
		//
		// 结果会针对每个请求提供答案，而针对其他请求则不会。如果 noPciCheck 为 true，检测器将跳过
		// PCI 检查，这与检测器方法的行为一致。
		MonitorAcceleratorPartitions(
			noPciCheck bool, requests []AcceleratorPartitionRequest,
		) (AcceleratorPartitionsGroup, error)
	}
)

type (
	// AllocatorOptions 表示配置分配器的选项。
	AllocatorOptions struct {
		Logger        klog.Logger
		KubeSocket    string
		NoShared      bool
		NoSliced      bool
		NoPartitioned bool
	}
	// Allocator 是一个用于将设备分配给容器的接口。
	Allocator interface {
		// Name 返回该接口所分配的设备类型名称。
		Name() string

		// Start 启动分配器，并为其准备分配准备。
		//
		// 调用此方法后，分配器应已准备好分配设备。
		Start(ctx context.Context) error

		// Stop 会停止分配器的工作，并释放其持有的所有资源。调用此方法后，不应再使用分配器进行分配。
		Stop()
	}
)
