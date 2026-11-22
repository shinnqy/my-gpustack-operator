package amd

import (
	"time"

	"k8s.io/apimachinery/pkg/util/sets"

	"gpustack.ai/gpustack/binding"
	"gpustack.ai/gpustack/binding/rsmi"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/utils/loggerx"
)

// MonitorAcceleratorProcesses 实现了 device.AcceleratorProcessDetector 接口：它报告每个主机进程持有哪块
// GPU、占用多少内存，以及——根据 GFX 修订版本的定义——计算单元的使用情况，从而让 Device Manager 能够将共享
// 卡的份额分配给持有该卡的 Instance。
//
// 这是该后端中唯一一个不经过 AMD SMI 的查询，原因是其缺陷源于 ROCm 7.2.0 版本，而非某种偏好设置。AMD SMI
// 自身的设备级处理列表会忽略所分配的处理器处理程序，并响应驱动程序所知晓的所有计算处理程序，因此通过它进
// 行的读取操作会作用于机器中的每一块显卡。而用于确定处理程序实际占用哪些显卡的入口点——
// amdsmi_get_gpu_compute_process_gpus——会返回 `INVAL` 作为活跃进程 ID，尽管该符号的解析结果及其签名与已安
// 装的头文件以及 AMD 的公开文档均一致。ROCm SMI 则针对每个设备回答相同的问题，这既是该功能的需求，也是
// `rocm-smi` 在该栈上采用的方式。
//
// 这样可以读到该进程在这张卡上的显存用量。AMD SMI 接口给出进程总量，它的头文件也说明这个总量不等于单卡显
// 存。
func (in *amd) MonitorAcceleratorProcesses(
	noPciCheck bool, deviceIDs sets.Set[string],
) (_ device.AcceleratorProcessesGroup, err error) {
	grp := device.AcceleratorProcessesGroup{Manufacturer: Manufacturer}

	defer loggerx.RecoverWithStackScanner(func(s loggerx.Scanner, e error) {
		in.logger.Error(e, "failed to monitor amd device processes")
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
		// 某些设备在卡上保留了过时的分配信息，而 AMD PCI 设备无法响应。这值得向运营商说明——要么是
		// 过时的分配，要么是已离开总线的设备，因此每个被请求的设备都被报告为未读取状态，而不是被
		// 丢弃，否则消费者会面临一种无法解释的状况。
		in.logger.Info("no amd pci devices found for allocated accelerators",
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

		// 在查询之前就确定了身份，因此没有分配费用的设备可以进行一次廉价的调用，且完全不需要进行
		// 进程读取。这两个库对此达成了一致：在双卡主机上，ROCm SMI 的唯一 ID 与 AMD SMI 报告的 ID
		// 一一对应，该 ID 被识别为加速器 ID，这也使得该查询能够按设备 ID 进行响应。
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
// 原因属于暂时性现象，而非故意为之：这些路径上没有任何内容可以证明该库无法满足查询需求，因此探针功能不应
// 因一次失败的调用就得出该库无法工作的结论。Compute 与内存存在相同的成因，因为两者都源自同一查询：ROCm
// SMI 的 cu_occupancy 是一个真实的每进程数值，该适配器能够读取到硬件所测量的数值，因此未读取设备上的该数
// 值缺失，与内存的缺失情况相同，属于暂时性现象。
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

// acceleratorProcessesOf 将某设备的计算-处理结果转换为聚合器所消耗的行数据，使用库自己的单位进行计量，且
// 不会扁平化其语义。
//
// 内存和算力都来自同一查询，因此 MemoryReason 和 CoresReason 属于同一分类，且对应相同的返回值。算力标记是
// 一个逐行数据，而非按设备划分的数据：一种无法衡量占用率的 GFX 版本会导致该行的算力缺失，而内存——以及设备
// 其他行的数据——则保持测量结果不变。在 gfx1101 上观察到这一现象，因为每行都包含该标记，且每行仍包含内存数
// 据。
func acceleratorProcessesOf(
	id string, infos []rsmi.ProcessInfo, infoRet rsmi.Return,
) device.AcceleratorProcesses {
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

// processQueryReason 类用于分类计算-处理查询所回答的内容。
//
// 成功的查询没有返回任何行并不等同于拒绝：库只报告处理了包含该设备的查询，因此空结果就是设备处于空闲状态
// 的体现。
func processQueryReason(ret rsmi.Return) device.AcceleratorProcessReason {
	switch {
	case ret.IsSuccess():
		return device.AcceleratorProcessReasonNone
	case ret.IsAPIUnavailable(), ret == rsmi.STATUS_NOT_SUPPORTED:
		return device.AcceleratorProcessReasonUnsupported
	case ret == rsmi.STATUS_PERMISSION:
		return device.AcceleratorProcessReasonPermission
	case ret == rsmi.STATUS_INSUFFICIENT_SIZE:
		// 绑定器报告称，有一行列表因尺寸不足无法完成，因此没有返回其可容纳的部分。
		return device.AcceleratorProcessReasonTruncated
	}
	return device.AcceleratorProcessReasonDriverError
}
