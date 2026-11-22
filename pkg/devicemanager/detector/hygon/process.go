package hygon

import (
	"time"

	"k8s.io/apimachinery/pkg/util/sets"

	"gpustack.ai/gpustack/binding"
	"gpustack.ai/gpustack/binding/rsmi"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/utils/loggerx"
)

// MonitorAcceleratorProcesses 实现了 device.AcceleratorProcessDetector 接口：它报告每个 DCU 由哪些主机进
// 程占用，占用的内存量，以及——根据 GFX 修订版本的定义——计算单元的使用情况，从而让 Device Manager 能够将共
// 享卡中的份额归属于持有该卡的 Instance。
//
// 此适配器及其上方的可观测路径均已在驱动程序下运行。在搭载了切片容器的 BW 卡（gfx9，DTK 25.04）运行
// matmul 运算时，库返回了持有进程的主机进程 ID、其 VRAM 使用量以及实时 cu_occupancy 值，这些数值与在
// /sys/class/kfd/kfd/proc/<pid>/下读取的内核自身数据一致。这也就解决了该适配器无法从记录的有效数据中解答
// 的问题：Hygon 上 RSMI 的计算数据是一个真实的百分比，而非 AMD GFX 版本所返回的 KFD_STATS_INVALID 哨兵值—
// —就在这个版本中。
//
// 后续的运行数据通过子资源、导出器 /monitor/snapshot 以及持有切片分配中的能力指标进行了传递，因此
// `docs/reference/instance-metrics.md` 中的“硬件状态”一栏现在标注了 Hygon 的运行情况。该运行数据还表明这
// 两个数值并非以相同的深度得到验证，页面也明确指出了这一点：内存数据与厂商的 hy-smi 以及内核的 vram_<
// gpuid> 完全一致，而除了内核之外，没有任何其他部分发布过针对每个进程的计算数据以供对比——结果显示为稳定的
// 10，而库中的数据则显示为 11。因此，应将计算数据视为衡量正确的数值，而非与内核数值相符合。
//
// 那次运行还表明，下方的三步骤结构是承载性而非防御性结构。主机范围枚举会报告 pid，并将两个数值置零——这只
// 是一个进程列表，而非测量值——因此，依赖其行的适配器会报告每个卡片持有者均未使用任何资源。只有设备级查询
// 的回答有效，且仅此一种：与之并行的按 pid 的变体则返回了 VRAM 数值，且 cu_occupancy 仍为零。
func (in *hygon) MonitorAcceleratorProcesses(
	noPciCheck bool, deviceIDs sets.Set[string],
) (_ device.AcceleratorProcessesGroup, err error) {
	grp := device.AcceleratorProcessesGroup{Manufacturer: Manufacturer}

	defer loggerx.RecoverWithStackScanner(func(s loggerx.Scanner, e error) {
		in.logger.Error(e, "failed to monitor hygon device processes")
		for s.Scan() {
			in.logger.Error(nil, s.Text())
		}
		err = e
	})

	if deviceIDs.Len() == 0 {
		return grp, nil
	}

	grp.Timestamp = time.Now()

	pciDevs := binding.GetPCIDevices([]string{_PciVendor}, nil)
	if !noPciCheck && len(pciDevs) == 0 {
		// 某事物在卡片上保留了某种分配信息，而 Hygon PCI 设备无法响应该分配。这值得告知运营商——这
		// 是过时的分配，或者卡片已离开总线——因此，每个被请求的设备都会被报告为未读取状态，而不是
		// 被丢弃，这将让消费者面临一种无法解释的缺失情况。
		in.logger.Info("no hygon pci devices found for allocated accelerators",
			"devices", deviceIDs.Len())
		grp.Accelerators = unreadAcceleratorProcesses(deviceIDs, nil)
		return grp, nil
	}

	in.init()

	cnt, ret := in.rsmi.GetDeviceCount()
	if !ret.IsSuccess() || cnt == 0 {
		in.logger.V(3).Error(ret, "failed to get device count for allocated accelerators",
			"count", cnt, "devices", deviceIDs.Len())
		grp.Accelerators = unreadAcceleratorProcesses(deviceIDs, nil)
		return grp, nil
	}

	grp.Accelerators = make([]device.AcceleratorProcesses, 0, deviceIDs.Len())
	answered := sets.New[string]()
	for i := 0; i < cnt; i++ {
		logger := in.logger.WithValues("index", i)

		dev := in.rsmi.GetDeviceHandleByIndex(i)

		// 在查询之前就解决了身份问题，因此没有分配费用的设备可以进行一次便宜的调用，且完全不会进
		// 行任何进程读取。
		uuid, ret := dev.GetUniqueId()
		if !ret.IsSuccess() {
			logger.V(3).Error(ret, "failed to get device UUID")
			continue
		}
		if !deviceIDs.Has(uuid) {
			continue
		}

		infos, infoRet := dev.GetComputeProcessInfo()

		procs := acceleratorProcessesOf(uuid, infos, infoRet)
		if procs.MemoryReason != device.AcceleratorProcessReasonNone {
			logger.V(3).Info("no per-process memory", "reason", procs.MemoryReason, "return", infoRet)
		}
		if procs.CoresReason != device.AcceleratorProcessReasonNone {
			logger.V(3).Info("no per-process compute", "reason", procs.CoresReason, "return", infoRet)
		}
		grp.Accelerators = append(grp.Accelerators, procs)
		answered.Insert(uuid)
	}

	// 由于同一原因，一个被请求的设备没有列出其处理入口——即一个失败的 UUID 调用，或者一个在分配仍将其
	// 命名为设备时已离开机器的卡片——也被报告为未读状态：因为每个被请求的设备只对应一个条目，而带有原
	// 因的条目能够解释其不存在的原因。
	grp.Accelerators = append(grp.Accelerators,
		unreadAcceleratorProcesses(deviceIDs, answered)...)

	return grp, nil
}

// unreadAcceleratorProcesses 返回每个未响应请求设备的一个条目，每个条目在两张表中都没有行，且原因属于临时
// 性质。
//
// 原因只是暂时的，并非故意为之：这些路径上没有任何内容可以证明 RSMI 确实服务于查询，因此探针功能不应因为
// 一次失败的测试就得出它不服务的结论。计算能力在这里与内存情况一致，而不同于那些库中从未出现过计算相关数
// 据的制造商：RSMI 的 cu_occupancy 是该适配器在其他地方读取的、每个进程真实的数值，因此未读取设备上的缺失
// 与内存的暂时缺失相同，并非库的固有属性。
func unreadAcceleratorProcesses(
	deviceIDs, answered sets.Set[string],
) []device.AcceleratorProcesses {
	unread := make([]device.AcceleratorProcesses, 0, deviceIDs.Len()-answered.Len())
	for _, id := range sets.List(deviceIDs.Difference(answered)) {
		unread = append(unread, device.AcceleratorProcesses{
			ID:           id,
			MemoryReason: device.AcceleratorProcessReasonDriverError,
			CoresReason:  device.AcceleratorProcessReasonDriverError,
		})
	}
	return unread
}

// acceleratorProcessesOf 将某设备的 RSMI 计算-处理结果转换为聚合器所消耗的行数据，使用 RSMI 自己的单位进
// 行表示，且不会扁平化其语义。
//
// 内存和算力来自同一查询，因此 MemoryReason 和 CoresReason 属于同一分类。返回结果如下：在两种情况下，该调
// 用都无法响应的设备均不支持（或存在其他适用原因）。而成功的调用在两种情况下均不会返回任何结果——即使每一
// 行都包含算力相关的标记。CuOccupancyAvailable 是每行的具体数据，而非每台设备的数据：在 GFX 版本上无法测
// 量占用率的进程，并不会导致该设备对查询的响应为拒绝，因此通过该检查的失败行会显示内存信息，且
// CoresPercent 字段会缺失，这与 NVIDIA 适配器对内存读取值为 NVML 的行的处理方式相同。
func acceleratorProcessesOf(id string, infos []rsmi.ProcessInfo, infoRet rsmi.Return) device.AcceleratorProcesses {
	reason := processQueryReason(infoRet)
	procs := device.AcceleratorProcesses{
		ID:           id,
		MemoryReason: reason,
		CoresReason:  reason,
	}

	procs.Processes = make([]device.AcceleratorProcess, 0, len(infos))
	if procs.MemoryReason != device.AcceleratorProcessReasonNone {
		return procs
	}
	for i := range infos {
		row := device.AcceleratorProcess{
			PID:         infos[i].Process_id,
			MemoryBytes: &infos[i].Vram_usage,
		}
		if infos[i].CuOccupancyAvailable() {
			row.CoresPercent = &infos[i].Cu_occupancy
		}
		procs.Processes = append(procs.Processes, row)
	}
	return procs
}

// processQueryReason 类用于分类 RSMI 的计算过程查询所回答的内容。
//
// 成功的查询没有返回任何行并不等同于拒绝：RSMI 只处理包含该设备的查询，因此空结果就是设备处于空闲状态的体
// 现。
func processQueryReason(ret rsmi.Return) device.AcceleratorProcessReason {
	switch {
	case ret.IsSuccess():
		return device.AcceleratorProcessReasonNone
	case ret.IsAPIUnavailable(), ret == rsmi.STATUS_NOT_SUPPORTED:
		return device.AcceleratorProcessReasonUnsupported
	case ret == rsmi.STATUS_PERMISSION:
		return device.AcceleratorProcessReasonPermission
	case ret == rsmi.STATUS_INSUFFICIENT_SIZE:
		return device.AcceleratorProcessReasonTruncated
	}
	return device.AcceleratorProcessReasonDriverError
}
