package exporter

import (
	"github.com/prometheus/client_golang/prometheus"

	worker "gpustack.ai/gpustack/api/worker/v1"
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/devicemanager/detector"
	"gpustack.ai/gpustack/pkg/utils/strconvx"
)

// 这些暴露的单位有意偏离了 Prometheus 的约定单位——即字节、核心数、比率等基础单位。它们实际上模仿了
// Instance 指标 API 字段的对应字段，因此两个展示相同数据的界面绝不会因四舍五入而产生差异。每个名称中都包
// 含该单位，因此这里没有歧义，只是不符合常规表达方式。
const (
	// `_Namespace` 用于标识此收集器所发布的每个家族。
	_Namespace = "gpustack_instance_"
	// _CollectorNamespace 前缀会将收集器的报告标注在自身之上，因此在 Prometheus 中可以看到一个无声退
	// 化的导出器，而不仅仅出现在日志中。
	_CollectorNamespace = _Namespace + "metrics_collector_"
)

// _CapabilityNamespace 用于标记每个进程的探针系列。特意没有使用 _Namespace: 前缀，因为该系列完全不包含
// Instance 维度。若使用该前缀，将引导读取器按 instance_name 进行聚合，最终得到空结果，并得出该能力从未在
// Instance 级别被报告的结论，而非从未在 Instance 级别存在。
const _CapabilityNamespace = "gpustack_"

// 收集器自身的仪表盘会标注所报告的来源：节点 kubelet 负责显示 pod 级别的数据，而 Device Manager 的监控循
// 环则用于显示与加速器相关的数据。
const (
	_SourceKubelet  = "kubelet"
	_SourceSnapshot = "snapshot"
)

// 每个进程的入口点都是单独进行的，因为驱动程序通常仅用于服务进程内存，而拒绝进程使用。它们会记录每个入口
// 点产生的数据量。
const (
	_EntryPointMemory = "memory"
	_EntryPointCores  = "cores"
)

// NewCollector 返回了用于发布该节点 Instance 数据的收集器，它结合了 poller 从 kubelet 采集的数据以及监控
// 循环从设备采集的数据。
//
// Collect 会仅从内存中读取那两个内容，其他什么也不读：没有请求，没有跨来源的锁，也没有可能阻塞抓取或导致
// 失败的内容。
func NewCollector(poller *Poller, monitorSnapshot func() *detector.MonitorSnapshot) prometheus.Collector {
	// 不是“instance”：Prometheus 会添加自己的实例目标标签，在默认的 honor_labels 设置为 false 时，它
	// 会将冲突的导出标签重命名为 exported_instance——因此，按实例进行的查询会无声地按抓取目标进行分组。
	instanceLabels := []string{"namespace", "instance_name", "instance_uid", "node"}

	// 加速器数量按每个加速器计算，因此它们会保留其来源信息——通过标识符以及制造商自己的工具名称所代表
	// 的索引——以及采样该数据的制造商。
	//
	// 该模式只是一个标签，而非独立的类别，因为所有子类别在每种模式下都表示同一含义：即 `实例` 的授予
	// 及其使用情况。因此，不关心授予方式的查询会忽略该标签，而关注该标签的查询则会按该标签进行分组。
	acceleratorLabels := append(append([]string{}, instanceLabels...),
		"id", "index", "manufacturer", "mode")

	// 探针是节点驱动程序及其某个卡组的属性，因此它既不包含 Instance，也不包含分配信息：共享同一卡的两
	// 个 Instance 会共享一个答案，而给其赋予 Instance 标签则会导致该答案被发布两次。
	capabilityLabels := []string{"node", "manufacturer", "id", "entry_point", "reason"}

	return &instanceCollector{
		poller:          poller,
		monitorSnapshot: monitorSnapshot,

		cpuTotal: prometheus.NewDesc(
			_Namespace+"cpu_total_millicores",
			"The CPU limit of the Instance in milli-cores.",
			instanceLabels, nil,
		),
		cpuUsed: prometheus.NewDesc(
			_Namespace+"cpu_used_millicores",
			"The CPU usage of the Instance in milli-cores.",
			instanceLabels, nil,
		),
		memoryTotal: prometheus.NewDesc(
			_Namespace+"memory_total_mib",
			"The memory limit of the Instance in MiB.",
			instanceLabels, nil,
		),
		memoryUsed: prometheus.NewDesc(
			_Namespace+"memory_used_mib",
			"The working set memory usage of the Instance in MiB.",
			instanceLabels, nil,
		),
		storageTotal: prometheus.NewDesc(
			_Namespace+"storage_total_mib",
			"The ephemeral storage limit of the Instance in MiB.",
			instanceLabels, nil,
		),
		storageUsed: prometheus.NewDesc(
			_Namespace+"storage_used_mib",
			"The ephemeral storage usage of the Instance in MiB, "+
				"the pod-level aggregate the kubelet evicts on.",
			instanceLabels, nil,
		),

		acceleratorMemoryTotal: prometheus.NewDesc(
			_Namespace+"accelerator_memory_total_mib",
			"The memory the Instance was granted on the accelerator in MiB: the whole device, "+
				"a logical slice's quota, or a hardware partition's own capacity.",
			acceleratorLabels, nil,
		),
		acceleratorMemoryUsed: prometheus.NewDesc(
			_Namespace+"accelerator_memory_used_mib",
			"The memory the Instance was measured holding of that grant in MiB.",
			acceleratorLabels, nil,
		),
		acceleratorMemoryUtilization: prometheus.NewDesc(
			_Namespace+"accelerator_memory_utilization_percent",
			"The used memory over the granted memory, in percent.",
			acceleratorLabels, nil,
		),
		acceleratorCoresUtilization: prometheus.NewDesc(
			_Namespace+"accelerator_cores_utilization_percent",
			"How much of the Instance's own compute allowance on the accelerator it was measured "+
				"using, in percent; it may exceed 100 where a cap is not being enforced.",
			acceleratorLabels, nil,
		),
		acceleratorTemperature: prometheus.NewDesc(
			_Namespace+"accelerator_temperature_celsius",
			"The temperature of the whole accelerator allocated to the Instance in Celsius.",
			acceleratorLabels, nil,
		),
		acceleratorPowerUsage: prometheus.NewDesc(
			_Namespace+"accelerator_power_usage_watts",
			"The power usage of the whole accelerator allocated to the Instance in Watts.",
			acceleratorLabels, nil,
		),
		acceleratorUnhealthy: prometheus.NewDesc(
			_Namespace+"accelerator_unhealthy",
			"Whether the whole accelerator allocated to the Instance is unhealthy.",
			acceleratorLabels, nil,
		),

		processCapability: prometheus.NewDesc(
			_CapabilityNamespace+"accelerator_process_capability",
			"Whether the accelerator's per-process query answered on this node's driver, "+
				"per entry point, carrying the reason it did not.",
			capabilityLabels, nil,
		),

		success: prometheus.NewDesc(
			_CollectorNamespace+"success",
			"Whether the last sampling round of this source succeeded.",
			[]string{"source"}, nil,
		),
		duration: prometheus.NewDesc(
			_CollectorNamespace+"duration_seconds",
			"How long the last sampling round of this source took.",
			[]string{"source"}, nil,
		),
	}
}

type instanceCollector struct {
	poller          *Poller
	monitorSnapshot func() *detector.MonitorSnapshot

	cpuTotal     *prometheus.Desc
	cpuUsed      *prometheus.Desc
	memoryTotal  *prometheus.Desc
	memoryUsed   *prometheus.Desc
	storageTotal *prometheus.Desc
	storageUsed  *prometheus.Desc

	acceleratorMemoryTotal       *prometheus.Desc
	acceleratorMemoryUsed        *prometheus.Desc
	acceleratorMemoryUtilization *prometheus.Desc
	acceleratorCoresUtilization  *prometheus.Desc
	acceleratorTemperature       *prometheus.Desc
	acceleratorPowerUsage        *prometheus.Desc
	acceleratorUnhealthy         *prometheus.Desc

	processCapability *prometheus.Desc

	success  *prometheus.Desc
	duration *prometheus.Desc
}

func (c *instanceCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.cpuTotal
	ch <- c.cpuUsed
	ch <- c.memoryTotal
	ch <- c.memoryUsed
	ch <- c.storageTotal
	ch <- c.storageUsed
	ch <- c.acceleratorMemoryTotal
	ch <- c.acceleratorMemoryUsed
	ch <- c.acceleratorMemoryUtilization
	ch <- c.acceleratorCoresUtilization
	ch <- c.acceleratorTemperature
	ch <- c.acceleratorPowerUsage
	ch <- c.acceleratorUnhealthy
	ch <- c.processCapability
	ch <- c.success
	ch <- c.duration
}

func (c *instanceCollector) Collect(ch chan<- prometheus.Metric) {
	// 这两个来源是独立的，每个来源都独立报告其他来源所做的事情：kubelet 负责在轮询周期中提供 Pod 级别
	// 的数据，而监控循环则负责提供加速器相关的数据。监控快照在这里被读取，而不是被纳入某个轮询周期中，
	// 因此加速器相关数据与监控快照创建时的状态一样最新，而非上次轮询的结果。读取该快照只是占用该进程
	// 已经持有的指针资源。
	monitor := c.monitorSnapshot()
	monitorFresh := detector.MonitorSnapshotFresh(monitor)
	ch <- gauge(c.success, boolValue(monitorFresh), _SourceSnapshot)

	round := c.poller.LastRound()

	// 该探针负责处理此节点的驱动程序和卡件，而非 Instance，因此在这里每个设备只发布一次，而非下方每个
	// Instance 一次——如果两个 Instance 共享一个卡件，则会导致相同的标签集被重复显示两次，从而导致整个
	// 抓取失败。
	if monitorFresh {
		c.collectProcessCapability(ch, monitor.Slices, carvedAcceleratorsOfRound(round))
	}

	if round == nil {
		// 目前尚未进行任何采样。请报告此情况，而非保持沉默，以便通过“无数据”或“此节点不运行
		// Instance”来区分。
		ch <- gauge(c.success, 0, _SourceKubelet)
		return
	}

	ch <- gauge(c.success, boolValue(round.Snapshot != nil && round.Snapshot.UsageMeasured), _SourceKubelet)
	ch <- gauge(c.duration, round.Duration.Seconds(), _SourceKubelet)

	// 失败的轮次会公布其结果，但不会提供具体数据：没有这些数据，就无法得到该节点的实例列表，而这里的
	// 每个家族都由一份数据来标注。那些仅通过 kubelet 读取就失败的轮次并不属于此类——因为该轮次包含了实
	// 例及其分配情况，所以下方的加速器家族数据仍然会公布，只是没有 pod 级别的测量数据。
	if round.Snapshot == nil {
		return
	}

	for i := range round.Snapshot.Instances {
		inst := &round.Snapshot.Instances[i]
		// Pod 级别的角色来自节点的恰好一个 Device Manager——参见(*Poller)。exporting——而加速器角色
		// 则来自每一个 Device Manager：不同制造商的`ID`是不重复的，因此每个 Device Manager 负责自
		// 己的卡片，且它们永远不会冲突。
		if round.Snapshot.Exporting {
			c.collectInstance(ch, inst)
		}
		if monitorFresh {
			c.collectAccelerators(ch, inst, monitor)
		}
	}
}

// collectAccelerators 会发布每个 Instance 在其各个加速器上所持有的内容，这些内容按照 Instance 自身的规则
// 进行解析，所使用的索引与指标子资源所使用的索引一致。
//
// 这里的每一个决定在实施前就已经确定：分配的资助、生产者部门对零的采用、决议中的身份与方式。剩下的只有这
// 一规则——一个数字，没有任何内容可以表述或发布，既非样本，也非会被视为闲置或无权获得的零。
func (c *instanceCollector) collectAccelerators(
	ch chan<- prometheus.Metric,
	inst *InstanceSample,
	monitor *detector.MonitorSnapshot,
) {
	if inst.Grants == nil {
		// Pod 的分配信息无法读取，因此无法得知某张卡的数据属于谁。发布该卡的数据将要求使用该
		// Instance 的每个租户承担费用。
		return
	}

	allocated := detector.AllocatedAcceleratorMetricsOf(monitor, inst.Accelerators)
	for i := range allocated {
		// 一张卡片可以包含多个条目：一个包含两个硬件分区的 Instance 会持有两个授权项，每个授权项
		// 都有独立的身份和数值。
		for _, am := range inst.Grants.Resolve(
			monitor.Slices, allocated[i].Manufacturer, &allocated[i].Metrics,
		) {
			c.publishAccelerator(ch, inst, allocated[i].Index, allocated[i].Manufacturer, am)
		}
	}
}

// publishAccelerator 会发布一个条目的家族，每个家族仅由包含数值的条目构成：若没有数值则意味着无法描述，而
// 零值则表明处于空闲状态。
func (c *instanceCollector) publishAccelerator(
	ch chan<- prometheus.Metric,
	inst *InstanceSample,
	index uint32,
	manufacturer string,
	am worker.InstanceAcceleratorMetrics,
) {
	labels := []string{
		inst.Namespace, inst.Name, string(inst.UID), c.poller.nodeName,
		am.ID, strconvx.Itoa(index), manufacturer, am.Mode,
	}

	if v := am.MemoryTotalMiB; v != nil {
		ch <- gauge(c.acceleratorMemoryTotal, float64(*v), labels...)
	}
	if v := am.MemoryUsedMiB; v != nil {
		ch <- gauge(c.acceleratorMemoryUsed, float64(*v), labels...)
	}
	if v := am.MemoryUtilizationPercent; v != nil {
		ch <- gauge(c.acceleratorMemoryUtilization, float64(*v), labels...)
	}
	if v := am.CoresUtilizationPercent; v != nil {
		ch <- gauge(c.acceleratorCoresUtilization, float64(*v), labels...)
	}
	// 整张卡自身的读数，以所有模式发布：一张卡的一部分没有温度、没有功耗，也没有自身的生命值。
	if v := am.TemperatureCelsius; v != nil {
		ch <- gauge(c.acceleratorTemperature, float64(*v), labels...)
	}
	if v := am.PowerUsageWatts; v != nil {
		ch <- gauge(c.acceleratorPowerUsage, float64(*v), labels...)
	}
	if v := am.Unhealthy; v != nil {
		ch <- gauge(c.acceleratorUnhealthy, boolValue(*v), labels...)
	}
}

// collectSliceCapability 根据加速器和入口点来报告：每个进程查询是否在该节点的驱动程序中得到了响应——以及未
// 能响应的原因，这就能避免“无法测量”被解读为“空闲”状态。
//
// 某个未知架构的某部分没有输出任何内容：其设备列表不可信，无法代表该消费者所读取的内容。若从分配信息中推
// 导出来，则会在两个实例共享的卡上重复显示同一设备。capabilityDevice 是探针能够回答的其中一个加速器，其识
// 别方式与 gauges 标签的识别方式一致。
type capabilityDevice struct {
	manufacturer string
	id           string
}

// carvedAcceleratorsOfRound 列出了该节点在“雕刻”模式下所持有的各加速器实例，每个实例各一个。这是能力探针
// 的备用设备列表，仅用于无法依赖快照自身列表的情况——因此它是根据 Pods 记录的内容生成的，而非基于任何硬件
// 读取数据。
func carvedAcceleratorsOfRound(round *Round) []capabilityDevice {
	if round == nil || round.Snapshot == nil {
		return nil
	}

	seen := make(map[capabilityDevice]struct{})
	devices := make([]capabilityDevice, 0, len(round.Snapshot.Instances))
	for i := range round.Snapshot.Instances {
		for _, grp := range round.Snapshot.Instances[i].Accelerators {
			for _, accelerator := range grp.Accelerators {
				switch accelerator.Mode {
				case workercore.DeviceAllocationModeSliced,
					workercore.DeviceAllocationModePartitioned:
				default:
					continue
				}
				dev := capabilityDevice{manufacturer: grp.Manufacturer, id: accelerator.ID}
				if _, dup := seen[dev]; dup {
					continue
				}
				seen[dev] = struct{}{}
				devices = append(devices, dev)
			}
		}
	}
	return devices
}

func (c *instanceCollector) collectProcessCapability(
	ch chan<- prometheus.Metric,
	section *detector.MonitorSliceSection,
	carved []capabilityDevice,
) {
	if section == nil {
		return
	}
	if !detector.KnownMonitorSliceSchema(section.SchemaVersion) {
		// 此版本无法读取的某部分内容无法说明节点的驱动程序情况，因此其自身的设备列表在此处无法使
		// 用——但状态本身必须可见，因为比 Device Manager 更新的消费者所面对的硬件与无法测量的硬件
		// 并无区别，而这是该版本 schema 旨在消除的混乱情况。设备实际上来自 Instances 自身的分配，
		// 出于与下文的循环相同的去重原因，这些设备也被去重处理：一个卡片对应两个 Instances 的分配，
		// 不应出现两次相同的标签集。
		for _, dev := range carved {
			for _, entryPoint := range []string{_EntryPointMemory, _EntryPointCores} {
				ch <- gauge(c.processCapability, 0,
					c.poller.nodeName, dev.manufacturer, dev.id, entryPoint,
					string(detector.SliceReasonVersionSkew))
			}
		}
		return
	}

	// 生产者每个加速器会生成一条记录，因此这只能防止出现重复的快照：重复的标签设置会导致整个抓取失败，
	// 这比仅报告第一个答案还要糟糕。
	seen := make(map[string]struct{}, len(section.Devices))
	for _, diag := range section.Devices {
		key := diag.Manufacturer + "\x00" + diag.DeviceID
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}

		for entryPoint, reason := range map[string]device.AcceleratorProcessReason{
			_EntryPointMemory: diag.MemoryReason,
			_EntryPointCores:  diag.CoresReason,
		} {
			// 这些原因并不相互关联：“库无法满足此查询”和“本次遍历无法读取它”是两种不同的说法，
			// 而短暂无法访问的驱动程序不应被理解为关于硬件的判定。
			ch <- gauge(c.processCapability,
				boolValue(reason == device.AcceleratorProcessReasonNone),
				c.poller.nodeName, diag.Manufacturer, diag.DeviceID, entryPoint, string(reason))
		}
	}
}

func (c *instanceCollector) collectInstance(ch chan<- prometheus.Metric, inst *InstanceSample) {
	labels := []string{inst.Namespace, inst.Name, string(inst.UID), c.poller.nodeName}

	// 总数始终会被填充。已使用的数值属于测量数据，而缺失的则不会被报告，也不会被记为零，因为那样会显
	// 示为“空闲”状态。
	ch <- gauge(c.cpuTotal, float64(inst.Sample.CPUTotalMilliCores), labels...)
	ch <- gauge(c.memoryTotal, float64(inst.Sample.MemoryTotalMiB), labels...)
	ch <- gauge(c.storageTotal, float64(inst.Sample.StorageTotalMiB), labels...)

	if v := inst.Sample.CPUUsedMilliCores; v != nil {
		ch <- gauge(c.cpuUsed, float64(*v), labels...)
	}
	if v := inst.Sample.MemoryUsedMiB; v != nil {
		ch <- gauge(c.memoryUsed, float64(*v), labels...)
	}
	if v := inst.Sample.StorageUsedMiB; v != nil {
		ch <- gauge(c.storageUsed, float64(*v), labels...)
	}
}

func gauge(desc *prometheus.Desc, value float64, labels ...string) prometheus.Metric {
	return prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value, labels...)
}

func boolValue(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
