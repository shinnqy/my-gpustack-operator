package kubemetrics

import (
	"slices"
	"strings"

	core "k8s.io/api/core/v1"
	klog "k8s.io/klog/v2"
	"k8s.io/utils/ptr"

	worker "gpustack.ai/gpustack/api/worker/v1"
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/devicemanager/detector"
	"gpustack.ai/gpustack/pkg/deviceplugin"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/utils/mathx"
)

// grantLogger 记录调用方无法在结果中直接看到的一种拒绝情形：同一 Pod 的两个容器
// 分别被授予了同一个加速器，该情形自身不会产生任何指标数值。
var grantLogger = klog.Background().WithName("instance-accelerator-metrics")

// acceleratorKey 在一个 Pod 可同时持有的多个厂商之间唯一标识一次授权：设备 ID 仅在
// 各自厂商的驱动库内唯一，跨厂商则不具备唯一性。
type acceleratorKey struct {
	manufacturer string
	deviceID     string

	// partitionID 用于区分同一张卡上的两个硬件分区，它们属于两次授权且必须对应两条记录——
	// 记录列表按 id 键控，而每个分区都有自己的名称，因此二者在此可被区分。在所有其他模式下
	// 该字段均为空：逻辑切分片没有独立标识，因此同一张卡上的两个切分片会在该键上冲突，
	// 根本无法作为两条独立记录存在。
	partitionID string
}

// acceleratorGrant 表示一个 Pod 在单个加速器上获得的授权（按分配记录所载）以及
// 持有该授权的容器。采集端记录按容器建索引，因此授权与度量数据实际上是同一条记录的
// 两半，而不是两次可能不一致的独立查找。
type acceleratorGrant struct {
	container string
	mode      workercore.DeviceAllocationMode

	// partitionID 是按分配记录所载的该授权所持有的分区，在所有其他模式下为空。即使无法对
	// 分区进行度量，它仍会标识该条目，从而避免同一张卡上两个无法度量的分区混为同一个标识符。
	partitionID string

	// allocatedUnits 是本次分配按照 nodefeature.ResourceMaxUnits 定义的基准记在该卡上的
	// 归一化单卡单元数——与准入阶段计算额度所用的基于显存锚定的基准一致。逻辑切分片的
	// 显存配额正是由此折算出来的。
	allocatedUnits int32

	// coresCapPercent 是逻辑切分片所受限的整卡算力占比上限，直接读取自其实际执行方：
	// 容器自身的算力请求。它是将实测占比重新折算的基准分母，而绝不能从上方的 units 中
	// 推算，因为逻辑切分片的算力预算与显存预算是相互独立的——容器可能持有十分之一的显存
	// 与一半的算力。
	coresCapPercent uint32

	// ambiguous 标记同一个 Pod 内两个容器各自获得的重复授权——即同一张整卡或同一张卡的
	// 同一个分区。单条记录无法承载两次授权，而任选其一或求和都会上报一个实际无人获得的配额。
	ambiguous bool
}

// AcceleratorGrants 按加速器索引单个 Pod 获得的各项授权，以便上报其指标的接口能够
// 以 Instance 自身的视角来表述这些数值。
//
// 它按 Pod 构建一次并按加速器查询，因为过滤加速器列表所用的 Pod 级分配视图无法
// 说明哪个容器持有哪项授权——而采集端的记录是按容器建索引的。
//
// 它放在此处紧邻配额计算逻辑，是因为上报这些指标的两个出口都执行相同的关联操作：
// 按请求触发的 metrics 子资源，以及按抓取周期触发的 device manager exporter。两套
// 独立实现今天或许一致，但在双方各自未覆盖的边缘情况上终会偏离——例如被两个容器
// 切分的加速器、自带标识的分区，以及按上限折算的算力占比。
type AcceleratorGrants struct {
	podUID string
	grants map[acceleratorKey]acceleratorGrant
}

// NewAcceleratorGrants 按加速器索引 Pod 的分配信息，并从执行限制的容器上读取每个
// 逻辑切分片的算力上限。
func NewAcceleratorGrants(pod *core.Pod, allocations deviceplugin.PodAllocations) *AcceleratorGrants {
	grants := &AcceleratorGrants{
		podUID: string(pod.UID),
		grants: make(map[acceleratorKey]acceleratorGrant),
	}

	for container, allocation := range allocations {
		for _, grp := range allocation.Devices.Groups {
			for _, accelerator := range grp.Accelerators {
				// Visibility 模式完全不授予任何资源：它只是让一个容器能够看到同 Pod 兄弟容器所获授的
				// 设备。将其纳入索引会让这类 Pod 看起来像是两个容器争抢同一张卡，而真正持有该设备的
				// 兄弟容器已经在此被索引过了。
				if accelerator.Mode == workercore.DeviceAllocationModeVisibility {
					continue
				}

				// 分区以自身 ID 为键，因此分别持有同一张卡上两个分区的两个容器会记为两项授权，而不会被
				// 当作存在争抢的同一张卡。
				partitionID := ""
				if accelerator.Mode == workercore.DeviceAllocationModePartitioned {
					partitionID = accelerator.AllocatedPhysicalID
				}

				key := acceleratorKey{
					manufacturer: grp.Manufacturer,
					deviceID:     accelerator.ID,
					partitionID:  partitionID,
				}
				if held, ok := grants.grants[key]; ok && held.container != container {
					grantLogger.V(2).Info(
						"two containers of one pod were granted the same accelerator,"+
							" reporting no figures of its own",
						"pod", pod.Namespace+"/"+pod.Name, "manufacturer", key.manufacturer,
						"device", key.deviceID, "partition", key.partitionID,
					)
					held.ambiguous = true
					grants.grants[key] = held
					continue
				}
				grants.grants[key] = acceleratorGrant{
					container:      container,
					mode:           accelerator.Mode,
					partitionID:    partitionID,
					allocatedUnits: accelerator.Allocated,
					coresCapPercent: slicedCoresCapPercentOf(pod, container, grp.Manufacturer,
						accelerator.Mode),
				}
			}
		}
	}

	return grants
}

// Resolve 以 Instance 自身的视角上报其在单个加速器上持有的资源情况：将分配所授予的
// 资源作为总量，将采集端针对该项授权实测的结果作为已用量，并附带产生这两者的分配模式。
//
// 每项授权对应一条记录，而非每张卡一条。除 Partitioned 模式外，一张卡在所有模式下都只
// 服务于一项授权；而在 Partitioned 模式下，Instance 可能同时持有同一张卡的多个分区
// （每个容器一个），每个分区都是各自独立的授权，拥有独立的标识、容量和用量。将它们合并到
// 父级物理卡上，相当于把卡上的某个租户当作整张卡来上报。
//
// 持有整张设备的 Instance 直接读取设备自身的指标，因为整张设备就是该授权。持有切分份额的
// Instance 则读取该份额的配额与实测用量，在无法取得该份额测量值时则完全不读取任何显存或
// 算力数值——设备级指标会包含同卡上的所有其他租户，而杜绝这种替代正是整个特性的存在意义。
// 非资源用量的整卡读数（温度、功耗与健康状态）在所有模式下都会上报，因为卡上的切分份额本身
// 没有独立的这类读数。
//
// 实测数值原样来自 section，无论缺失还是为零：由 section 统一持有这一判断，确保上报
// 这些指标的两个出口表现完全一致。若 section 完全无法回答该加速器的数据（例如采集端版本
// 早于切分片上报、无法识别的 schema 或未覆盖该加速器），则将这些数值留空，这才是最诚实的
// 表达：授权本身仍已知，但没有任何来源测得其用量。
func (g *AcceleratorGrants) Resolve(
	section *detector.MonitorSliceSection,
	manufacturer string,
	card *device.AcceleratorMetrics,
) []worker.InstanceAcceleratorMetrics {
	// 按每项授权所持有的分区排序，从而使同一 Pod 的定时抓取与子资源读取每次输出的条目顺序一致。
	// 除 Partitioned 模式外，每种模式均只产生恰好一条记录。
	keys := make([]acceleratorKey, 0, 1)
	for key := range g.grants {
		if key.manufacturer == manufacturer && key.deviceID == card.ID {
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		// 加速器列表已按该 Pod 自身的分配信息过滤，因此其中不应有索引未命中的项。若确实发生
		// 未命中，说明没有任何一方能确定 Instance 在该加速器上的份额，而设备级数值并不代表该份额——
		// 因此条目仅保留该卡的标识而不包含任何度量值。
		return []worker.InstanceAcceleratorMetrics{cardEntry(card)}
	}
	slices.SortFunc(keys, func(a, b acceleratorKey) int {
		return strings.Compare(a.partitionID, b.partitionID)
	})

	entries := make([]worker.InstanceAcceleratorMetrics, 0, len(keys))
	for _, key := range keys {
		entries = append(entries, g.resolveGrant(section, manufacturer, card, g.grants[key]))
	}
	return entries
}

// resolveGrant 解析单张加速卡上的单次分配授权。
func (g *AcceleratorGrants) resolveGrant(
	section *detector.MonitorSliceSection,
	manufacturer string,
	card *device.AcceleratorMetrics,
	grant acceleratorGrant,
) worker.InstanceAcceleratorMetrics {
	entry := cardEntry(card)
	entry.Mode = grant.mode.String()

	// 即使无法采集指标，硬件分区也会自行命名：分配记录已标明具体分区，若按父卡的
	// 标识符上报，会将同一张卡的两个分区归到同一个标识符下，而以 id 为键的条目列表
	// 无法区分这种情况。
	if grant.partitionID != "" {
		entry.ID = grant.partitionID
	}
	if grant.ambiguous {
		return entry
	}

	if !carvedAllocationMode(grant.mode) {
		entry.MemoryTotalMiB = ptr.To(card.Memory)
		entry.MemoryUsedMiB = ptr.To(card.MemoryUsage)
		entry.MemoryUtilizationPercent = ptr.To(card.MemoryUtilization)
		entry.CoresUtilizationPercent = ptr.To(card.CoresUtilization)
		return entry
	}

	var figures detector.SliceFigures
	if measured, answered := section.Figures(
		manufacturer, g.podUID, grant.container, card.ID,
	); answered {
		figures = measured
	}

	// 硬件分区自行命名并确定容量：驱动直接从分区自身的句柄读取这两项信息，因此容量
	// 来自数据采集端而非分配记录，且在驱动返回命名时优先采用驱动自身的命名。
	// 逻辑切片则两者皆无，它是从当前条目所属加速卡上切分出来的配额。
	if figures.ID != "" {
		entry.ID = figures.ID
	}
	if grant.mode == workercore.DeviceAllocationModePartitioned {
		entry.MemoryTotalMiB = figures.MemoryTotalMiB
	} else {
		entry.MemoryTotalMiB = slicedMemoryTotalMiB(grant.allocatedUnits, card.Memory)
	}
	entry.MemoryUsedMiB = figures.MemoryUsedMiB
	entry.MemoryUtilizationPercent = utilizationPercent(entry.MemoryUsedMiB, entry.MemoryTotalMiB)
	entry.CoresUtilizationPercent = ownCoresUtilizationPercent(
		figures.CoresUtilizationPercent, grant.coresCapPercent,
	)
	return entry
}

// cardEntry 构造在所有模式下均来自整卡的条目基础部分：以其标识作为起点，
// 并包含共享分片自身无法单独测量的读数。
func cardEntry(card *device.AcceleratorMetrics) worker.InstanceAcceleratorMetrics {
	return worker.InstanceAcceleratorMetrics{
		ID:                 card.ID,
		TemperatureCelsius: ptr.To(card.Temperature),
		PowerUsageWatts:    ptr.To(card.PowerUsage),
		Unhealthy:          ptr.To(card.Unhealthy),
	}
}

// carvedAllocationMode 返回一次分配是否仅持有加速卡的一部分而非整卡。
// Shared 不属于切分模式：它将设备同时授予多个持有者且彼此之间没有配额限制，
// 因此设备的整卡指标就是其持有者仅有的指标。
func carvedAllocationMode(mode workercore.DeviceAllocationMode) bool {
	switch mode {
	case workercore.DeviceAllocationModeSliced, workercore.DeviceAllocationModePartitioned:
		return true
	}
	return false
}

// slicedMemoryTotalMiB 将逻辑切片分配的单位数还原为 MiB，作为生成该单位数的
// nodefeature.MemoryMibToUnits 的逆运算，使上报的总量不会与实际授予的配额不一致。
// 若记录声明超过整卡容量，则上限截断为整卡容量，这与 DevicePlugin 写入记录时的边界一致。
//
// 已授予的配额绝不会还原为零。单位数按固定比例划分，因此在大容量加速卡上足够小的切片
// 相除后可能得到零——例如在 80 GiB 卡上请求 1 MiB 会被授予 1,600,000 单位中的 19 单位，
// 还原后为 0 MiB。此处 0 并不表示极小配额，而是表示没有配额，因此可上报的最小配额为 1 MiB。
// 仅当分配未计费，或卡容量未能传递到此阶段时，才视为无配额可上报并保持缺失。
func slicedMemoryTotalMiB(units int32, cardMemoryMiB uint64) *uint64 {
	if units <= 0 || cardMemoryMiB == 0 {
		return nil
	}
	if units > nodefeature.ResourceMaxUnits {
		units = nodefeature.ResourceMaxUnits
	}
	return ptr.To(max(uint64(units)*cardMemoryMiB/nodefeature.ResourceMaxUnits, 1))
}

// utilizationPercent 将一对 Used/Total 数值表示为百分比，若任一端缺失则返回 nil。
// 该值直接根据一同发布的两个数值计算，而非取自独立来源，从而确保三者始终一致。
func utilizationPercent(usedMiB, totalMiB *uint64) *uint32 {
	if usedMiB == nil || totalMiB == nil || *totalMiB == 0 {
		return nil
	}
	return ptr.To(device.CalculateUtilization(*usedMiB, *totalMiB))
}

// ownCoresUtilizationPercent 将测得的整卡算力占比换算为占持有者自身算力配额的比例，
// 从而使同一字段在所有模式下具有一致含义：例如限制为整卡五分之一且跑满该配额的切片，
// 此处读数为 100，与跑满整卡的 Instance 完全一致。
//
// 该商向上取整，原因与本包中所有其他换算一致：厂商以整数百分比度量整卡利用率，
// 较小的算力上限会使分子粒度变粗，向下取整可能将明显忙碌的切片发布为毫无负载。
// 若上限为整卡（即切片未请求算力预算时授予的默认值），则原样透传该数值。
func ownCoresUtilizationPercent(measuredPercent *uint32, capPercent uint32) *uint32 {
	if measuredPercent == nil {
		return nil
	}
	if capPercent == 0 || capPercent >= 100 {
		return measuredPercent
	}
	return ptr.To(uint32(mathx.CeilDiv(uint64(*measuredPercent)*100, uint64(capPercent)))) // nolint: gosec
}

// slicedCoresCapPercentOf 从 Pod 某个容器获批的请求中读取分配器对其施加的算力上限。
// 若容器已不在 Pod spec 中，或未请求算力预算，则上限视为整卡——这与分配器本身的策略一致。
//
// 该函数仅针对逻辑切片返回有效计算结果。上述默认值正是原因所在：硬件分区不发起
// 算力请求，若为其读取请求值，就会把每个分区都按整卡上限处理。
func slicedCoresCapPercentOf(
	pod *core.Pod, container, manufacturer string, mode workercore.DeviceAllocationMode,
) uint32 {
	if mode != workercore.DeviceAllocationModeSliced {
		return 0
	}
	resName := nodefeature.GetAcceleratableSlicedCoresPercentageResourceName(manufacturer)

	// 遍历全部三类容器列表，因为分配记录可能属于任意类型的容器，且查找未命中时的默认值
	// 为整卡上限——如果查找范围窄于所读取的分配记录，就会发布一个从未授予过的上限。
	// 归属索引与已启动容器检查出于同样原因也遍历这三类容器。
	ctr := &core.Container{}
	for _, containers := range [][]core.Container{pod.Spec.Containers, pod.Spec.InitContainers} {
		if i := slices.IndexFunc(containers, func(c core.Container) bool {
			return c.Name == container
		}); i >= 0 {
			ctr = &containers[i]
			break
		}
	}
	if i := slices.IndexFunc(pod.Spec.EphemeralContainers, func(c core.EphemeralContainer) bool {
		return c.Name == container
	}); i >= 0 {
		ctr = (*core.Container)(&pod.Spec.EphemeralContainers[i].EphemeralContainerCommon)
	}
	return uint32(deviceplugin.SlicedCoresPercent(ctr, resName)) // nolint: gosec
}
