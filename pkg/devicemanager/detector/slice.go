package detector

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/ptr"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/devicemanager/procattr"
	"gpustack.ai/gpustack/pkg/deviceplugin"
	"gpustack.ai/gpustack/pkg/system"
	"gpustack.ai/gpustack/pkg/utils/osx"
)

// MonitorSliceSchemaVersion 是切片部分模式的版本，而 MonitorSliceSchemaVersionMin 则是该消费者仍会读取的
// 最早版本。
//
// 该版本的存在是为了让消费者能够区分不受支持硬件与版本差异。如果没有这个版本，使用较旧 Device Manager 的
// 较新消费者会看到一个有效的、新的快照，其中没有切片数据，他们无法判断是该节点的加速器无法测量，还是生产
// 者只是提前使用了该功能——将前者视为后者进行报告，正是本部分旨在防止的“空闲状态下读取缺失数据”错误。
//
// 消费者接受 RANGE 格式，而不是仅接受自己的版本，因为这两种格式运行在不同的 Pods 中，且升级时间也不同：在
// 滚动更新期间，新的工作进程会读取旧的 Device Manager 文件。如果拒绝整个部分，就会丢弃旧生产者用来报告新
// 型号的那些数据，而这些数据其实并非他们所编写。因此，当消费者能够将其视为独立版本时，每个版本都会被添加
// 进来。而超出范围之外的版本则毫无意义——这确实是对当前版本无法理解的部分的诚实描述。
//
// 版本 1 包含每个进程的使用情况、设备诊断信息以及硬件分区信息。
const (
	MonitorSliceSchemaVersion    = 1
	MonitorSliceSchemaVersionMin = 1
)

// KnownMonitorSliceSchema 报告某个部分的 schema 版本是否为当前构建所读取的版本。
func KnownMonitorSliceSchema(version int) bool {
	return version >= MonitorSliceSchemaVersionMin && version <= MonitorSliceSchemaVersion
}

const (
	// maxSliceUsages 定义了该部分可容纳的记录数量，因此如果节点中的 Pods、`容器` 或设备数量不符合预期，
	// 就无法无限制地扩展快照。其数量比真实节点高出数个数量级：在最大化的节点配置下，每个 Pod 的每一个
	// 容器都包含其所有加速器。
	maxSliceUsages = 4096
	// maxSliceDevices 定义了诊断项，该节点所包含的每个加速器对应一个条目。
	maxSliceDevices = 256
)

const (
	// sliceReasonInvalidData 表示厂商自己的数据行与其来源设备不符——即数值超过卡片的物理容量——因此该数
	// 据被拒绝，而非作为数字发布以供计算，亦不会被静默置零。
	sliceReasonInvalidData device.AcceleratorProcessReason = "invalid_data"
	// sliceReasonBounded 表示为了将区间保持在边界内，设备中的记录被丢弃了。丢弃整卡而非部分记录，才能
	// 避免缺失记录被读取为测量零值。
	sliceReasonBounded device.AcceleratorProcessReason = "bounded"
)

const SliceReasonVersionSkew device.AcceleratorProcessReason = "version_skew"

// MonitorSliceSection 是指该节点在本轮测量中每个加速器在 Instance 下的份额，以（Pod UID、容器名称、设备
// ID）为键。
//
// 这是独立的一段数据，而非 device.AcceleratorMetrics 内部的一个字段。该字段保持其原样：即某一张卡的读数。
// 这种按进程聚合的原始数据行不会出现在此处——它们指的是主机进程，而隐私边界和该字段的大小限制也取决于它们
// 保持为内部数据。
//
// 消费者不应将记录的缺失视为使用量的缺失。只有当容器中包含进程时，才会生成记录。如果容器中没有任何进程，
// 在 Devices 报告中会显示为已测量但未使用——即为零。Figures 会一次性做出该决定，因此两个指标的表现一致。
type MonitorSliceSection struct {
	// SchemaVersion 为写入该部分的生产者所定的 MonitorSliceSchemaVersion 版本。不了解该版本的消费者必
	// 须报告所有缺失的切片数据，并说明因版本差异导致的现象，而不是将该部分视为空。而了解该版本的消费
	// 者则可以读取该版本所包含的记录，并报告其余的缺失内容。
	SchemaVersion int `json:"schemaVersion"`
	// Truncated 报告称达到了限制阈值，部分设备完全被丢弃。这些设备的相关信息存储在 Devices 中，因此被
	// 丢弃的设备会被标记为不存在，而非空闲状态。
	Truncated bool `json:"truncated,omitempty"`
	// Usages 中保存了每条记录的对应信息，每条记录对应一个（Pod UID、容器名称、设备 ID）。
	Usages []SliceUsage `json:"usages"`
	// Devices 中每个加速器对应一个条目，无论是否有相关记录生成，都包含该加速器的信息。此处未列出的加
	// 速器则完全未被涵盖。
	Devices []SliceDeviceDiagnostics `json:"devices"`
	// Partitions 存储了每个硬件分区中 Instance 所包含的记录，这些记录在分区自身的设备句柄上读取。它属
	// 于一个列表，而非 `Usages` 中的一个字段，因为该列表来自不同的来源，能够独立于该来源的故障而保留
	// 下来——详见图例。
	Partitions []SlicePartition `json:"partitions,omitempty"`
}

// SliceUsage 是指一个 Instance 容器在某一加速器中的使用比例。
//
// 这两个数值是指针，因为“缺失”和“零”指的是不同的状态：缺失意味着在此处无法测量该数值，而零则意味着已进行
// 测量，且容器处于空闲状态。
type SliceUsage struct {
	// 制造商、PodUID、容器和 DeviceID 是关键参数。其中，容器指的是容器的名称，而非其运行时 ID：消费者
	// 加入的分配资源会按容器名称进行记录，而运行时 ID——即其背后的进程 ID——则保留在节点上。
	Manufacturer string `json:"manufacturer"`
	PodUID       string `json:"podUID"`
	Container    string `json:"container"`
	DeviceID     string `json:"deviceID"`

	// MemoryUsedMiB 是指该容器进程在设备上的内存占用量，以厂商的本地单位计算后统一转换。如果制造商未
	// 提供可用的数值，则此项为空白。
	MemoryUsedMiB *uint64 `json:"memoryUsedMiB,omitempty"`
	// CoresUtilizationPercent 是指整个卡片中该容器进程上次测量到的使用率。如果制造商未提供每进程的使
	// 用数据，则该值为空。
	CoresUtilizationPercent *uint32 `json:"coresUtilizationPercent,omitempty"`
}

// SlicePartition 是某个 Instance 容器的硬件分区，属于一个加速器，以该分区的自身设备句柄为测量标准。
//
// 分区并未从父卡的进程中聚合而来，这就是它属于记录而非 SliceUsage 的原因：该分区由分配记录的内容来命名——
// 即一个规格和一种放置方式——因此，一个空闲分区仍然可以被找到并报告为零，而另一个租户在兄弟分区上拥有的进
// 程也无法破坏它。
type SlicePartition struct {
	// 制造商、PodUID、容器和 DeviceID 是关键参数，而 SliceUsage 也携带这些参数。DeviceID 属于父加速器，
	// 它也是分配记录以及消费者加入的依据。
	Manufacturer string `json:"manufacturer"`
	PodUID       string `json:"podUID"`
	Container    string `json:"container"`
	DeviceID     string `json:"deviceID"`

	// ID 是该分区的自身标识符，其句柄会显示此值。消费者以该标识符来报告分区，而非上述的 DeviceID，因
	// 为该标识符代表了分区的归属者——而且，由同一个 Instance 所持有的同一卡片上的两个分区，在卡片的标
	// 识符下只算作一个条目，在他们自己的标识符下则算作两个。如果无法访问句柄，则值为空。
	ID string `json:"id,omitempty"`

	// MemoryTotalMiB 和 MemoryUsedMiB 分别表示该分区的容量以及实际占用量，这些值既可以通过处理该句柄
	// 读取，也可以一次性转换得到。容量来自驱动程序，而非规格名称，因为名称存在四舍五入：一个 "1g.10gb
	// " 分区包含 9856 MiB，而非 10240。如果无法读取句柄，则这些值不存在。当可以读取且分区处于空闲状态
	// 时，这些值为零。
	MemoryTotalMiB *uint64 `json:"memoryTotalMiB,omitempty"`
	MemoryUsedMiB  *uint64 `json:"memoryUsedMiB,omitempty"`

	// CoresPercent 是指分区自身的计算利用率，通过其句柄可以读取。对于没有按分区显示计算数据的库函数，
	// 大多数厂商都不会提供此值。请参考 device.AcceleratorPartition.CoresPercent。
	CoresPercent *uint32 `json:"coresPercent,omitempty"`

	// MemoryReason 说明了为何上述内存数据不存在，在生成时为空。CoresReason 说明了为何 CoresPercent 不
	// 存在，在读取时同样为空。
	MemoryReason device.AcceleratorProcessReason `json:"memoryReason,omitempty"`
	CoresReason  device.AcceleratorProcessReason `json:"coresReason,omitempty"`
}

// SliceDeviceDiagnostics 用于解释某个加速器所生成的样本：为什么某个数值缺失，以及该过程看到了什么内容。如
// 果没有这些计数数据，仅基于汇总数据的部分会造成与真正空闲设备相同的低估情况，而这是该功能无法承受的混乱
// 状态。
type SliceDeviceDiagnostics struct {
	Manufacturer string `json:"manufacturer"`
	DeviceID     string `json:"deviceID"`

	// MemoryReason 和 CoresReason 用于说明为何该数值在设备的所有记录中均缺失，且在产生该数值时为空。
	// 它们是分开的，因为驱动程序通常负责处理内存，同时拒绝进程使用。
	MemoryReason device.AcceleratorProcessReason `json:"memoryReason,omitempty"`
	CoresReason  device.AcceleratorProcessReason `json:"coresReason,omitempty"`

	// RowsReturned 表示制造商为该设备返回的每个进程行数。
	RowsReturned uint32 `json:"rowsReturned"`
	// RowsAttributed 表示其中有多少行被解析为 Instance 的 Pod 和容器。
	RowsAttributed uint32 `json:"rowsAttributed"`
	// RowsNonInstance 表示节点运行的、支持没有 Instance 的 Pod 中的行数。这些行是真实的，不属于任何人
	// 的份额，因此删除它们仍能使设备可测量。
	RowsNonInstance uint32 `json:"rowsNonInstance"`
	// RowsUnreadable 表示完全无法从 /proc 读取的行数，或读取时其身份发生变化的行数：退出、权限问题、
	// 不可读、僵尸状态、不稳定、不可见。
	RowsUnreadable uint32 `json:"rowsUnreadable"`
	// RowsAmbiguous 表示有多少行数据被读取，但无法被合并为一个已知的 Pod 和容器：一个主机进程、两个
	// Pod 祖先、节点索引中未包含的 Pod 或容器，以及代表他人使用的设备处理过程。
	RowsAmbiguous uint32 `json:"rowsAmbiguous"`
	// ReadsTruncated 表示该设备的多少个入口点无法提供完整的行列表。被截断的列表被误判为完整的列表，会
	// 导致现有的进程变成不存在的进程，因此会显示为空而非部分内容。
	ReadsTruncated uint32 `json:"readsTruncated"`
}

// SliceFigures 是指消费者针对某个（Pod、`容器`、`设备`）所发布的数据：每个数据要么有测量值——可能为零——要
// 么不存在。
type SliceFigures struct {
	MemoryUsedMiB *uint64

	// CoresUtilizationPercent 是所有模式下的总资源利用率，但在“分区模式”下则属于该分区的专属值——该值
	// 通过分区的句柄读取，该句柄对父资源一无所知。消费者将前者用于限制持有者的上限，而保留后者不变，
	// 因为分区没有计算请求，因此没有需要限制的上限。
	CoresUtilizationPercent *uint32

	// ID 和 MemoryTotalMiB 描述的是所测量的内容，而非测量所使用的工具，它们仅适用于生产者知晓的情况：
	// 硬件分区拥有自己的标识符和容量，这些信息均可通过其标识号获取。对于其他模式而言，这些参数则不存
	// 在，其身份由加速器决定，而配额则由消费者从分配信息中读取。
	ID             string
	MemoryTotalMiB *uint64
}

// Figures 会解析一个容器在某一设备上的数据，并报告该部分是否能够适用于该设备。
//
// 它包含“缺失与零”的决策，因此两个使用界面都会将其视为相同：对于该部分未携带任何答案的设备，该设备在该容
// 器中的每个测量值均回答为零。因为对于在测量设备上没有进行任何处理的容器，该设备也没有进行任何处理。
//
// 分区的数据量根据每个进程查询独立解析，这是有意为之，而非上述规则中的疏漏。这些数据源自其自身来源——即分
// 区的设备句柄，因此制造商对每个进程的查询失败与它们无关：一行数据无法用于分配整卡的内存和核心资源，也不
// 应将其与分区关联起来。在支持 MIG 的卡上，每个进程的查询正是无法回答此类问题的查询。因此，仅分区记录本身
// 就足以构成答案。
//
// 分区记录会覆盖其自身键的每进程计数，而不会与其合并。这两个计数分别用于两个不同的句柄上的相同内存，因此“
// 字段混合”计数会输出一个无法重新计算的数值。分区自身的句柄是这两个计数中更精确、更可靠的那个，也是唯一能
// 在兄弟租户不可归属的进程中存活下来的计数方式。
func (s *MonitorSliceSection) Figures(
	manufacturer, podUID, container, deviceID string,
) (SliceFigures, bool) {
	if s == nil {
		return SliceFigures{}, false
	}
	if !KnownMonitorSliceSchema(s.SchemaVersion) {
		// 该 API 按设计规定没有“原因”字段，因此该界面在显示偏差时仅会每次报告一次，而不会保持沉默：
		// 操作员将控制台的空数据与导出器的 version_skew 指标进行比较，两者都反映了同一信息，且都
		// 不是对硬件的猜测。
		logger.V(2).Info("a device manager wrote a slice section this build cannot read,"+
			" reporting no slice figures", "reason", SliceReasonVersionSkew,
			"sectionVersion", s.SchemaVersion, "supported", MonitorSliceSchemaVersion,
			"manufacturer", manufacturer, "device", deviceID)
		return SliceFigures{}, false
	}

	figures := SliceFigures{}
	if p := slices.IndexFunc(s.Partitions, func(p SlicePartition) bool {
		return p.Manufacturer == manufacturer && p.PodUID == podUID &&
			p.Container == container && p.DeviceID == deviceID
	}); p >= 0 {
		figures.ID = s.Partitions[p].ID
		figures.MemoryTotalMiB = s.Partitions[p].MemoryTotalMiB
		figures.MemoryUsedMiB = s.Partitions[p].MemoryUsedMiB
		// 分区的计算行为已针对该分区明确说明，因此会按原样执行：对于不发起计算请求的模式，消费者
		// 相对于上限的重新表述操作将不会产生任何效果。在那些库中不包含分区级别数据的所有厂商产品
		// 上，该操作均不存在。
		figures.CoresUtilizationPercent = s.Partitions[p].CoresPercent
		return figures, true
	}

	i := slices.IndexFunc(s.Devices, func(d SliceDeviceDiagnostics) bool {
		return d.Manufacturer == manufacturer && d.DeviceID == deviceID
	})
	if i < 0 {
		return figures, false
	}
	diag := s.Devices[i]

	j := slices.IndexFunc(s.Usages, func(u SliceUsage) bool {
		return u.Manufacturer == manufacturer && u.PodUID == podUID &&
			u.Container == container && u.DeviceID == deviceID
	})
	if j >= 0 {
		figures.MemoryUsedMiB = s.Usages[j].MemoryUsedMiB
		figures.CoresUtilizationPercent = s.Usages[j].CoresUtilizationPercent
		return figures, true
	}

	if diag.MemoryReason == device.AcceleratorProcessReasonNone {
		figures.MemoryUsedMiB = ptr.To[uint64](0)
	}
	if diag.CoresReason == device.AcceleratorProcessReasonNone {
		figures.CoresUtilizationPercent = ptr.To[uint32](0)
	}
	return figures, true
}

// containerNames 将某个 Pod 的 UID 及其对应的容器运行时 `ID` 映射到该容器的名称。厂商相关的行会对应到运行
// 时 ID，且分配情况按名称进行记录，因此关联操作在此处进行——即在节点上，因为两者信息均已知——只有名称会传递。
type containerNames map[string]map[string]string

// collectSlices 执行单个监控周期的进程级处理，并返回快照的切片部分。如果处理没有产生任何结果，则返回
// `nil`。
//
// nil 并非一个空区间，这种差异很重要：空区间表示已测量该节点但未找到任何内容，而 nil 则表示本次测量失败。
// 因此，失败的 Pod 列表——这是第一个计时器可能遇到的状况，在读取器开始工作之前——会返回 nil，下一次计时则能
// 恢复正常。
//
// 这里的原始行仅作为本地值存在。它们无法进入快照，因为它们从未出现在其中：该返回的片段仅包含 per-(Pod、
// `容器`、`设备`) 的聚合数据，没有其他内容，因此删除它们可不是可以忽略的步骤。
func (d *Detector) collectSlices(
	ctx context.Context, metricsGrpList device.MetricsGroupList,
) *MonitorSliceSection {
	// 如果库无法回答任何问题，说明该开发者没有实现任何接口。当没有任何接口被实现时，就没有什么可读取
	// 的内容，且 Pods 也根本没有被列出。
	providers := make(map[string]device.AcceleratorProcessDetector, len(d.detectors))
	partitioners := make(map[string]device.AcceleratorPartitionDetector, len(d.detectors))
	for i := range d.detectors {
		if pd, ok := d.detectors[i].(device.AcceleratorProcessDetector); ok {
			providers[d.detectors[i].Name()] = pd
		}
		if pd, ok := d.detectors[i].(device.AcceleratorPartitionDetector); ok {
			partitioners[d.detectors[i].Name()] = pd
		}
	}
	if (len(providers) == 0 && len(partitioners) == 0) || d.podLister == nil {
		return nil
	}

	pods, err := d.podLister(ctx)
	if err != nil {
		// 读取失败，因此这个标记毫无意义——这与知道没有内容被雕刻出来不同。两者都指向没有该部分的
		// 区域，且必须保持不同的路径：失败的读取会生成一个“EMPTY”区域，这将表明该节点已被测量并发
		// 现为空。
		logger.V(2).Error(err, "listing this node's pods, reporting no slice figures")
		return nil
	}

	// 只有某些容器在雕刻模式下所包含的设备值得查询：整台设备的分配没有需要报告的片段，而处于空闲状态
	// 的可分割卡片应该不消耗任何成本。
	targets := carvedAcceleratorsOf(pods)
	if len(targets) == 0 {
		logger.V(4).Info("no carved accelerator allocation on this node, no slice figures to take")
		return nil
	}

	var procGroups []device.AcceleratorProcessesGroup
	for manufacturer, deviceIDs := range targets {
		pd, ok := providers[manufacturer]
		if !ok || d.procResolver == nil {
			continue
		}

		logger := logger.V(3).WithValues("manufacturer", manufacturer)
		grp, err := pd.MonitorAcceleratorProcesses(d.noPCICheck, deviceIDs)
		if err != nil {
			logger.Error(err, "monitor accelerator processes")
			continue
		}
		if len(grp.Accelerators) == 0 {
			continue
		}
		procGroups = append(procGroups, grp)
	}

	// 分区通过其独立句柄被读取，其租户身份在读取前即可确定，无需在之后通过进程进行解析。因此，此步骤
	// 完全不需要 /proc，而由 Instance 持有的分区是否包含任何运行中的进程，也会被报告出来。
	partitions := d.readPartitions(pods, partitioners)

	var section *MonitorSliceSection
	if len(procGroups) > 0 {
		index, names := podIndexOf(pods)
		section = buildSliceSection(procGroups, metricsGrpList, names,
			func(pids []uint32) map[uint32]procattr.Result {
				return d.procResolver.Resolve(index, pids)
			})
	}
	return withPartitions(section, partitions, cardMemoryOf(metricsGrpList))
}

// partitionTarget 是 Instance 容器所持有的一个硬件分区，根据分配记录，这是制造商的诉求，而租户的回应则属
// 于其所有。
type partitionTarget struct {
	podUID    string
	container string
	request   device.AcceleratorPartitionRequest
}

// readPartitions 会要求每个供应商提供该节点所管理的每个分区对应的处理程序信息，并返回以租户为键的记录。
//
// 租户来自分配，而非答案，这就是此版本与按进程分配版本之间的本质区别：询问的是关于几何形状的问题，因此答
// 案将归属于注释中指明负责该几何形状的容器，而不会归属于其他任何人。
func (d *Detector) readPartitions(
	pods []core.Pod, partitioners map[string]device.AcceleratorPartitionDetector,
) []partitionRow {
	if len(partitioners) == 0 {
		return nil
	}

	var rows []partitionRow
	for manufacturer, targets := range partitionTargetsOf(pods) {
		pd, ok := partitioners[manufacturer]
		if !ok {
			continue
		}

		logger := logger.V(3).WithValues("manufacturer", manufacturer)
		requests := make([]device.AcceleratorPartitionRequest, 0, len(targets))
		for i := range targets {
			requests = append(requests, targets[i].request)
		}
		grp, err := pd.MonitorAcceleratorPartitions(d.noPCICheck, requests)
		if err != nil {
			logger.Error(err, "monitor accelerator partitions")
			continue
		}

		for i := range targets {
			j := slices.IndexFunc(grp.Partitions, func(p device.AcceleratorPartition) bool {
				return p.DeviceID == targets[i].request.DeviceID &&
					p.Profile == targets[i].request.Profile &&
					slices.Equal(p.Placements, targets[i].request.Placements)
			})
			if j < 0 {
				// 该接口承诺每次请求都能提供响应，因此出现缺失情况属于制造商适配器缺陷，
				// 而非硬件问题。它被丢弃了，而不是被标记为“无明确原因缺失”，因为原因属于
				// 我们，而非节点本身——而任何看不到相关记录的消费者，最终也会认为数据缺失。
				logger.Info("a manufacturer answered no partition for a request, reporting nothing for it",
					"device", targets[i].request.DeviceID, "profile", targets[i].request.Profile)
				continue
			}
			rows = append(rows, partitionRow{
				manufacturer: manufacturer,
				podUID:       targets[i].podUID,
				container:    targets[i].container,
				partition:    grp.Partitions[j],
			})
		}
	}
	return rows
}

// partitionTargetsOf 根据厂商不同，返回该节点 Pods 所包含的所有硬件分区。
//
// 一个分区会被记录在 Pod 自身的分配注释中，作为规格名称以及它在父加速器上的位置（AllocatedPhysicalProfile
// / AllocatedPhysicalPlacements）。如果缺少其中任一记录，则不是该 Pod 可以命名的分区：这两个记录共同决定
// 了同一卡片上不同租户的分区差异，因此不完整的记录会被跳过，而不会进行松散匹配。
//
// 两个容器所要求的分区份额被取消，涉及两个容器。其中一个分区只有一个租户，因此名为“两个”的记录中至少有一
// 个是错误的，但此处无法判断是哪一个：同时回应会占用一个租户的内存供另一个使用，这正是整个拒绝流程存在的
// 错误原因。这是消费者自己加入设备时也会遇到的拒绝情况，因为只有一个 Pod 被划分为两个容器，而此情况仅出现
// 在本次操作中，因为只有本次操作能跨越 Pods 进行交互。
func partitionTargetsOf(pods []core.Pod) map[string][]partitionTarget {
	targets := make(map[string][]partitionTarget)
	for i := range pods {
		podUID := string(pods[i].UID)
		if podUID == "" {
			continue
		}
		allocations, err := deviceplugin.AllocatedAcceleratorsOf(&pods[i])
		if err != nil {
			// 已记录下在每进程迭代中读取相同注释的位置。
			continue
		}

		for container, allocation := range allocations {
			for _, grp := range allocation.Devices.Groups {
				for _, accelerator := range grp.Accelerators {
					if accelerator.Mode != workercore.DeviceAllocationModePartitioned ||
						accelerator.AllocatedPhysicalProfile == "" ||
						len(accelerator.AllocatedPhysicalPlacements) == 0 {
						continue
					}
					targets[grp.Manufacturer] = append(targets[grp.Manufacturer], partitionTarget{
						podUID:    podUID,
						container: container,
						request: device.AcceleratorPartitionRequest{
							DeviceID: accelerator.ID,
							ID:       accelerator.AllocatedPhysicalID,
							Profile:  accelerator.AllocatedPhysicalProfile,
							// device.AcceleratorPlacement 是已记录类型的别名，
							// 因此这些位置会按记录时的状态进行移动，任何东西
							// 都无法在途中修改它们。
							Placements: accelerator.AllocatedPhysicalPlacements,
						},
					})
				}
			}
		}
	}
	for manufacturer, claimed := range targets {
		if kept := singlyClaimedPartitions(manufacturer, claimed); len(kept) > 0 {
			targets[manufacturer] = kept
		} else {
			delete(targets, manufacturer)
		}
	}
	return targets
}

// singlyClaimedPartitions 会确保分区恰好被一个容器所声明。
func singlyClaimedPartitions(manufacturer string, claimed []partitionTarget) []partitionTarget {
	claimants := make(map[string]int, len(claimed))
	key := func(t partitionTarget) string {
		return t.request.DeviceID + "\x00" + t.request.Profile + "\x00" +
			fmt.Sprint(t.request.Placements)
	}
	for _, target := range claimed {
		claimants[key(target)]++
	}

	kept := make([]partitionTarget, 0, len(claimed))
	for _, target := range claimed {
		if claimants[key(target)] > 1 {
			logger.Info("one hardware partition is claimed by more than one container, reporting nothing for it",
				"manufacturer", manufacturer, "device", target.request.DeviceID,
				"profile", target.request.Profile, "pod", target.podUID,
				"container", target.container)
			continue
		}
		kept = append(kept, target)
	}
	return kept
}

// partitionRow 是某一家制造商针对一个分区及其所属租户的回应。这是未经转换和 withPartitions 合理性检查前的
// 原始读取数据。
type partitionRow struct {
	manufacturer string
	podUID       string
	container    string
	partition    device.AcceleratorPartition
}

// withPartitions 将分区读取内容折叠到该部分中，将每个内存数值统一转换为 MiB 单位，并删除与父卡片冲突的一
// 项。
//
// 当每个进程的查询结果为空时，该部分就会被创建。其原因与创建收费项的原因相同：一个分区的数据本身就是答案，
// 而在支持 MIG 的卡上，每个进程的查询是那个无法给出答案的查询。
func withPartitions(
	section *MonitorSliceSection, rows []partitionRow, cardMemory map[string]uint64,
) *MonitorSliceSection {
	if len(rows) == 0 {
		return section
	}
	if section == nil {
		section = &MonitorSliceSection{SchemaVersion: MonitorSliceSchemaVersion}
	}

	for _, row := range rows {
		if len(section.Partitions) >= maxSliceUsages {
			section.Truncated = true
			break
		}
		partition := SlicePartition{
			Manufacturer: row.manufacturer,
			PodUID:       row.podUID,
			Container:    row.container,
			DeviceID:     row.partition.DeviceID,
			ID:           row.partition.ID,
			MemoryReason: row.partition.MemoryReason,
			CoresReason:  row.partition.CoresReason,
		}
		// 保持原样执行。它没有完整的卡片形象可供与下文的记忆对进行校验——百分比不能超过卡片本身的
		// 大小——而且它来自分区自己的控制接口，因此提供该服务的制造商已经对其进行了规划。
		if row.partition.CoresPercent != nil {
			partition.CoresPercent = ptr.To(*row.partition.CoresPercent)
		}
		if row.partition.MemoryUsedBytes != nil && row.partition.MemoryTotalBytes != nil {
			var (
				totalMiB      = convertUsageToMiB(*row.partition.MemoryTotalBytes)
				usedMiB       = convertUsageToMiB(*row.partition.MemoryUsedBytes)
				cardMemoryMiB = cardMemory[row.manufacturer+"/"+row.partition.DeviceID]
			)
			switch {
			case cardMemoryMiB > 0 && (totalMiB > cardMemoryMiB || usedMiB > cardMemoryMiB):
				// 如果分区的大小超过或包含其被雕刻出来的卡片所承载的内容，那么这一读数就
				// 不可信，就像每个进程的过载容量总和一样。两者都是如此：它们从一个把手上
				// 脱落，因此其中任何一个不可能的情况都会削弱该读数的可信度，而不仅仅是该
				// 领域的问题。
				logger.V(2).Info("a partition exceeds its whole accelerator, dropping the figures",
					"manufacturer", row.manufacturer, "device", row.partition.DeviceID,
					"pod", row.podUID, "container", row.container,
					"totalMiB", totalMiB, "usedMiB", usedMiB, "cardMiB", cardMemoryMiB)
				partition.MemoryReason = sliceReasonInvalidData
			default:
				partition.MemoryTotalMiB = ptr.To(totalMiB)
				partition.MemoryUsedMiB = ptr.To(usedMiB)
			}
		}
		section.Partitions = append(section.Partitions, partition)
	}

	slices.SortFunc(section.Partitions, func(a, b SlicePartition) int {
		return strings.Compare(
			a.Manufacturer+"\x00"+a.DeviceID+"\x00"+a.PodUID+"\x00"+a.Container,
			b.Manufacturer+"\x00"+b.DeviceID+"\x00"+b.PodUID+"\x00"+b.Container,
		)
	})
	return section
}

// carvedAcceleratorsOf 返回该节点 Pods 在雕刻分配模式下的加速器，按制造商进行键序排列。
//
// 两种划分模式均被计算在内。由制造商负责其自身处理器的分区，通过分区通过量来衡量，其记录会优先于其他记录。
// 但那些在硬件层面进行划分且不对每个分区进行查询的制造商，则只有进程路径，没有其他内容——因此仅使用软模式
// 进行限制，这些卡片将不会被查询。其他所有模式都是整台设备级的分配。而“共享”模式既不属于划分模式，也不属
// 于整台设备模式，因此没有配额或切片数量可供参考。
//
// 分配信息从 Pod 自身的注解中读取，该注解是由设备插件写入的、适用于每个容器的持久性记录，因此除了 Pod 列
// 表所需的读取开销外，不会产生其他读取成本。
func carvedAcceleratorsOf(pods []core.Pod) map[string]sets.Set[string] {
	targets := make(map[string]sets.Set[string])
	for i := range pods {
		allocations, err := deviceplugin.AllocatedAcceleratorsOf(&pods[i])
		if err != nil {
			// 无法读取注释的 Pod 表示此处没有设备。它也不被视为空容器：它所包含的内容完全未知，
			// 其同属节点的设备名称仍会被查询。
			logger.V(2).Error(err, "reading a pod's allocated accelerators",
				"pod", pods[i].Namespace+"/"+pods[i].Name)
			continue
		}

		for _, allocation := range allocations {
			for _, grp := range allocation.Devices.Groups {
				for _, accelerator := range grp.Accelerators {
					if !carvedAllocationMode(accelerator.Mode) {
						continue
					}
					if targets[grp.Manufacturer] == nil {
						targets[grp.Manufacturer] = sets.New[string]()
					}
					targets[grp.Manufacturer].Insert(accelerator.ID)
				}
			}
		}
	}
	return targets
}

// carvedAllocationMode 用于指示分配是占用加速器的一部分，还是占用整个加速器。
func carvedAllocationMode(mode workercore.DeviceAllocationMode) bool {
	switch mode {
	case workercore.DeviceAllocationModeSliced, workercore.DeviceAllocationModePartitioned:
		return true
	}
	return false
}

// listNodePods 通过设备插件为其运行的 informer 列出该节点的 Pods，因此该操作只需进行缓存查找，而非每次都
// 向 API 服务器发送 List 请求。这与指标导出器进行的操作相同（pkg/devicemanager/exporter/poller.go）。
func (d *Detector) listNodePods(ctx context.Context) ([]core.Pod, error) {
	reader := system.LoopbackCtrlClient.Get()
	if reader == nil {
		return nil, errors.New("loopback controller client is not configured yet")
	}
	ndName := osx.Getenv("KUBERNETES_NODE_NAME")
	if ndName == "" {
		return nil, errors.New("environment variable KUBERNETES_NODE_NAME is not set")
	}

	podList := &core.PodList{}
	err := reader.List(ctx, podList,
		ctrlcli.MatchingFieldsSelector{
			Selector: fields.OneTermEqualSelector(deviceplugin.IndexingPodsByNodeName, ndName),
		},
	)
	if err != nil {
		return nil, err
	}
	return podList.Items, nil
}

// buildSliceSection 会将每个制造商的每进程行数据汇总到该部分中。
//
// resolve 是指分配一个设备的进程 ID，并覆盖节点的 Pod 索引。它按设备进行，而非针对节点单独处理：出现在两
// 个设备上的进程 ID 将分别独立分配，而解析器所给出的“厂商从该读者无法看到的命名空间中报告了 ID”这一结论，
// 仅在一个设备的行集中才有意义。
func buildSliceSection(
	procGroups []device.AcceleratorProcessesGroup,
	metricsGrpList device.MetricsGroupList,
	names containerNames,
	resolve func(pids []uint32) map[uint32]procattr.Result,
) *MonitorSliceSection {
	cardMemory := cardMemoryOf(metricsGrpList)

	section := &MonitorSliceSection{SchemaVersion: MonitorSliceSchemaVersion}
	for _, procGrp := range procGroups {
		manufacturer := procGrp.Manufacturer
		for _, procs := range procGrp.Accelerators {
			usages, diag := aggregateAcceleratorProcesses(
				manufacturer, procs, cardMemory[manufacturer+"/"+procs.ID],
				names, resolve,
			)

			// 该限制会删除整个设备，而不会仅删除其中的部分记录：如果某个设备中的某条记录被删
			// 除，该部分仍被报告为已测量值，其结果将显示为测量零。
			switch {
			case len(section.Devices) >= maxSliceDevices:
				section.Truncated = true
				continue
			case len(section.Usages)+len(usages) > maxSliceUsages:
				section.Truncated = true
				diag.MemoryReason = sliceReasonBounded
				diag.CoresReason = sliceReasonBounded
				usages = nil
			}
			section.Usages = append(section.Usages, usages...)
			section.Devices = append(section.Devices, diag)
		}
	}
	if len(section.Devices) == 0 {
		return nil
	}

	slices.SortFunc(section.Usages, func(a, b SliceUsage) int {
		return strings.Compare(
			a.Manufacturer+"\x00"+a.DeviceID+"\x00"+a.PodUID+"\x00"+a.Container,
			b.Manufacturer+"\x00"+b.DeviceID+"\x00"+b.PodUID+"\x00"+b.Container,
		)
	})
	slices.SortFunc(section.Devices, func(a, b SliceDeviceDiagnostics) int {
		return strings.Compare(a.Manufacturer+"\x00"+a.DeviceID, b.Manufacturer+"\x00"+b.DeviceID)
	})
	return section
}

// cardMemoryOf 根据制造商和设备 ID，从同一监控周期中索引加速器自身的容量。正是这一机制使得超过物理容量的
// 数值被识别为无效数据，而非可发布的数值，无论是针对厂商的表格还是用于账本数据。
func cardMemoryOf(metricsGrpList device.MetricsGroupList) map[string]uint64 {
	cardMemory := make(map[string]uint64)
	for _, grp := range metricsGrpList {
		for _, am := range grp.Accelerators {
			cardMemory[grp.Manufacturer+"/"+am.ID] = am.Memory
		}
	}
	return cardMemory
}

// aggregateAcceleratorProcesses 会将单个加速器中的每进程行数据转换为针对（Pod、`容器`）的记录以及解释这些
// 数据的诊断信息。此处不采用 Pod 索引：它由 resolve 的关闭行为决定，因此节点运行的 Pods 数量是在一次遍历
// 中确定的，而非每次设备中确定。
func aggregateAcceleratorProcesses(
	manufacturer string,
	procs device.AcceleratorProcesses,
	cardMemoryMiB uint64,
	names containerNames,
	resolve func(pids []uint32) map[uint32]procattr.Result,
) ([]SliceUsage, SliceDeviceDiagnostics) {
	acc := newSliceAccumulator(manufacturer, procs, cardMemoryMiB)

	pids := make([]uint32, 0, len(procs.Processes))
	for i := range procs.Processes {
		pids = append(pids, procs.Processes[i].PID)
	}
	results := resolve(pids)

	for i := range procs.Processes {
		row := &procs.Processes[i]
		result := results[row.PID]
		switch result.Outcome {
		case procattr.OutcomeExcluded:
			acc.exclude()
		case procattr.OutcomeAttributed:
			// 名为 Pod 状态中未包含的容器无法作为键，若进行猜测则会导致错误分配。
			name := names[result.Identity.PodUID][result.Identity.ContainerID]
			if name == "" {
				acc.poison(device.AcceleratorProcessReason(procattr.ReasonUnknownContainer))
				continue
			}
			acc.add(sliceKey{podUID: result.Identity.PodUID, container: name}, row)
		default:
			// 这一行属于当前无法命名的对象。此设备上的每一个数值都与之相关：缺少一个项的数值
			// 总和并非较小的数值，而是错误的数值。
			acc.poison(device.AcceleratorProcessReason(result.Reason))
		}
	}

	return acc.emit()
}

// sliceKey 是设备内一条记录的标识：即哪个容器中的哪个 Pod。
type sliceKey struct {
	podUID    string
	container string
}

// sliceEntry 是厂商原生单位下，某个设备中某个容器当前的累计值。
type sliceEntry struct {
	// memoryBytes 是容器中各行数据的总和，而 memoryUnusable 则记录其中有一行没有数值——因此总和是部分
	// 值，部分值会被标记为“缺失”而非具体数值。
	memoryBytes    uint64
	memoryUnusable bool
	// coresPercent 是容器每进程利用率的总和，以卡上总百分比表示。而 coresUnusable 则表明其中某一行没
	// 有数值——由于内存数值的设定方式，这一总和是部分有效的，因此被标记为“不存在”。
	//
	// 此处，没有数据的行不会被视为空闲进程。如果适配器库仅报告非零样本，则会将指针指向零，并标记为“空
	// 闲”。当 nil 达到此状态时，即为硬件无法测量的行，对此不添加任何内容会显示容器可能超过的卡容量比
	// 例。
	coresPercent  uint64
	coresUnusable bool
}

// sliceAccumulator 用于构建设备的记录。这是 SliceUsage 产生的唯一方式，而`emit`则是其唯一的退出点——因此“
// 无法归属的行会导致该设备上的所有数据消失”这一规则是自动生效的，而非要求每个调用者记得检查一个标志。
type sliceAccumulator struct {
	manufacturer  string
	deviceID      string
	cardMemoryMiB uint64

	memoryReason device.AcceleratorProcessReason
	coresReason  device.AcceleratorProcessReason
	poisoned     device.AcceleratorProcessReason

	diag    SliceDeviceDiagnostics
	order   []sliceKey
	entries map[sliceKey]*sliceEntry
}

func newSliceAccumulator(
	manufacturer string, procs device.AcceleratorProcesses, cardMemoryMiB uint64,
) *sliceAccumulator {
	acc := &sliceAccumulator{
		manufacturer:  manufacturer,
		deviceID:      procs.ID,
		cardMemoryMiB: cardMemoryMiB,
		memoryReason:  procs.MemoryReason,
		coresReason:   procs.CoresReason,
		entries:       make(map[sliceKey]*sliceEntry),
		diag: SliceDeviceDiagnostics{
			Manufacturer: manufacturer,
			DeviceID:     procs.ID,
			RowsReturned: uint32(len(procs.Processes)),
		},
	}
	for _, reason := range []device.AcceleratorProcessReason{procs.MemoryReason, procs.CoresReason} {
		if reason == device.AcceleratorProcessReasonTruncated {
			acc.diag.ReadsTruncated++
		}
	}
	return acc
}

// 将带有属性的一行添加到其容器的总和中。
func (a *sliceAccumulator) add(key sliceKey, row *device.AcceleratorProcess) {
	a.diag.RowsAttributed++

	entry, ok := a.entries[key]
	if !ok {
		entry = &sliceEntry{}
		a.entries[key] = entry
		a.order = append(a.order, key)
	}

	if row.MemoryBytes == nil {
		entry.memoryUnusable = true
	} else {
		entry.memoryBytes += *row.MemoryBytes
	}
	if row.CoresPercent == nil {
		entry.coresUnusable = true
	} else {
		entry.coresPercent += uint64(*row.CoresPercent)
	}
}

// 排除那些属于 Pod 的记录行，即该节点所运行的、不支持 Instance 的节点。
func (a *sliceAccumulator) exclude() {
	a.diag.RowsNonInstance++
}

// poison 记录了无法归属的行，并携带设备上的所有数据。第一个原因被保留：这是读者应该采取行动的那个原因，而
// 后续的行通常都是同样的拒绝行为重复出现。
func (a *sliceAccumulator) poison(reason device.AcceleratorProcessReason) {
	if unreadableAttributionReason(procattr.Reason(reason)) {
		a.diag.RowsUnreadable++
	} else {
		a.diag.RowsAmbiguous++
	}
	if a.poisoned == device.AcceleratorProcessReasonNone {
		a.poisoned = reason
	}
}

// emit 会生成设备的记录和诊断信息。这是累积器的唯一出口：被污染的设备不会产生任何记录，其被污染的原因也会
// 出现在两个数值上。
func (a *sliceAccumulator) emit() ([]SliceUsage, SliceDeviceDiagnostics) {
	if a.poisoned != device.AcceleratorProcessReasonNone {
		a.diag.MemoryReason = a.poisoned
		a.diag.CoresReason = a.poisoned
		return nil, a.diag
	}
	a.diag.MemoryReason = a.memoryReason
	a.diag.CoresReason = a.coresReason

	// 厂商自己的数据行可能会与其来源的卡片内容相冲突。汇总过去的物理容量并非值得发布的过载数据——超出
	// 配额的部分则值得发布且保持不变——这是一份无法被设备上的任何容器所信任的数据。
	var deviceMemoryMiB uint64
	for _, key := range a.order {
		deviceMemoryMiB += convertUsageToMiB(a.entries[key].memoryBytes)
	}
	if a.cardMemoryMiB > 0 && deviceMemoryMiB > a.cardMemoryMiB {
		a.diag.MemoryReason = sliceReasonInvalidData
	}

	usages := make([]SliceUsage, 0, len(a.order))
	for _, key := range a.order {
		entry := a.entries[key]
		usage := SliceUsage{
			Manufacturer: a.manufacturer,
			PodUID:       key.podUID,
			Container:    key.container,
			DeviceID:     a.deviceID,
		}
		if a.diag.MemoryReason == device.AcceleratorProcessReasonNone && !entry.memoryUnusable {
			usage.MemoryUsedMiB = ptr.To(convertUsageToMiB(entry.memoryBytes))
		}
		if a.diag.CoresReason == device.AcceleratorProcessReasonNone && !entry.coresUnusable {
			// 该数值以整个卡片为分母，因此超过 100 的各进程样本重叠总和被保留在分母中，而不会
			// 以无意义的百分比形式呈现。
			usage.CoresUtilizationPercent = ptr.To(uint32(min(entry.coresPercent, 100)))
		}
		usages = append(usages, usage)
	}
	if len(usages) == 0 {
		return nil, a.diag
	}
	slices.SortFunc(usages, func(x, y SliceUsage) int {
		return strings.Compare(x.PodUID+"\x00"+x.Container, y.PodUID+"\x00"+y.Container)
	})
	return usages, a.diag
}

// convertUsageToMiB 会在最后将本地字节总量一次性转换为 MiB，并且会保持非零的总量不变：该转换采用向下取整
// 方式，因此真正包含几百个 KiB 的容器会被误报为不包含任何数据。
func convertUsageToMiB(bytes uint64) uint64 {
	mib := device.ConvertBytesToMiB(bytes)
	if mib == 0 && bytes > 0 {
		return 1
	}
	return mib
}

// unreadableAttributionReason 用于指示拒绝是源于 /proc 未能响应，还是源于其响应的内容。这两种情况被分别统
// 计，因为它们的含义不同：第一种指的是竞争或部署失败，第二种指的是真正不属于 Instance 负责的内容。
func unreadableAttributionReason(reason procattr.Reason) bool {
	switch reason {
	case procattr.ReasonExited, procattr.ReasonPermission, procattr.ReasonUnreadable,
		procattr.ReasonZombie, procattr.ReasonUnstable, procattr.ReasonInvisible:
		return true
	}
	return false
}

// podIndexOf 根据此节点的 Pods 构建归属索引及容器名称的关联。
//
// 每个 Pod 都承载着实例：属于某个 Pod 且不依赖任何 Instance 的进程可以安全移除。而属于某个 Pod 且无人知晓
// 的 Pod 则使其设备无法被测量——因此，识别节点中的普通 Pods 才能确保设备可被测量。
func podIndexOf(pods []core.Pod) (procattr.PodIndex, containerNames) {
	index := make(procattr.PodIndex, len(pods))
	names := make(containerNames, len(pods))
	for i := range pods {
		pod := &pods[i]
		uid := string(pod.UID)
		if uid == "" {
			continue
		}

		containers := sets.New[string]()
		for _, statuses := range [][]core.ContainerStatus{
			pod.Status.InitContainerStatuses,
			pod.Status.ContainerStatuses,
			pod.Status.EphemeralContainerStatuses,
		} {
			for j := range statuses {
				id := containerIDOf(statuses[j].ContainerID)
				if id == "" {
					continue
				}
				containers.Insert(id)
				if names[uid] == nil {
					names[uid] = make(map[string]string)
				}
				names[uid][id] = statuses[j].Name
			}
		}

		index[uid] = procattr.Pod{Instance: isInstancePod(pod), Containers: containers}
	}
	return index, names
}

// containerIDOf 会去掉 "<runtime>://" 协议，因为 cgroup 路径仅显示容器 ID，两者需要直接比较。因此，Pod 状
// 态报告会包含容器 ID。
func containerIDOf(id string) string {
	if _, rest, ok := strings.Cut(id, "://"); ok {
		return rest
	}
	return id
}

// isInstancePod 用于报告某个 Pod 是否支持一个 Instance。
//
// 这反映了 pkg/devicemanager/exporter/poller.go 中 instanceOf 的逻辑，该逻辑对指标导出器也解决了同样的问
// 题，且在该文件中未被导出：控制器 ownerReference 会指定 Instance 的名称，而标签中的部分内容会重复其 UID，
// 这两种情况都会被检查，因为指标子资源会根据该标签解析出 Pod。任何对其中一项进行更改的人，都应该同时更改
// 另一项。
func isInstancePod(pod *core.Pod) bool {
	ref := meta.GetControllerOf(pod)
	if ref == nil || ref.Kind != _InstanceKind ||
		schema.FromAPIVersionAndKind(ref.APIVersion, ref.Kind).Group != workercore.GroupName {
		return false
	}
	return pod.Labels[deviceplugin.InstancePartOfLabelKey] == string(ref.UID)
}

// _InstanceKind 是 Instance Pod 所携带的所属类型。
const _InstanceKind = "Instance"
