package iluvatar

import (
	"time"

	"k8s.io/apimachinery/pkg/util/sets"

	"gpustack.ai/gpustack/binding"
	"gpustack.ai/gpustack/binding/ixml"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/utils/loggerx"
)

// MonitorAcceleratorProcesses 实现了 device.AcceleratorProcessDetector 接口：它报告每个主机进程持有哪块
// GPU 以及该 GPU 的内存大小，从而 Device Manager 能够将共享卡中的份额分配给持有该卡的 Instance。
//
// 硬件验证结果：在编写此适配器时，没有可用的 Iluvatar GPU。以下的转换仅针对 process_test.go 中记录的 IXML
// 有效载荷进行。
func (in *iluvatar) MonitorAcceleratorProcesses(
	noPciCheck bool, deviceIDs sets.Set[string],
) (_ device.AcceleratorProcessesGroup, err error) {
	grp := device.AcceleratorProcessesGroup{Manufacturer: Manufacturer}

	defer loggerx.RecoverWithStackScanner(func(s loggerx.Scanner, e error) {
		in.logger.Error(e, "failed to monitor iluvatar device processes")
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
		// 某些设备在卡上保留了过时的分配信息，Iluvatar PCI 设备对此无响应。这值得告知运营商——要么
		// 是过时的分配，要么是已离开总线的卡，因此每个被请求的设备都被报告为未读取状态，而非被丢
		// 弃，否则消费者将面临无法解释的空白状态。
		in.logger.Info("no iluvatar pci devices found for allocated accelerators",
			"devices", deviceIDs.Len())
		grp.Accelerators = unreadAcceleratorProcesses(deviceIDs, nil)
		return grp, nil
	}

	in.init()

	cnt, ret := in.ixml.DeviceGetCount()
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

		dev, ret := in.ixml.DeviceGetHandleByIndex(i)
		if !ret.IsSuccess() {
			logger.V(3).Error(ret, "failed to get device handle")
			continue
		}

		// 在查询之前就解决了身份问题，因此没有分配费用的设备可以进行一次便宜的调用，且完全不会进
		// 行任何进程读取。
		uuid, ret := dev.GetUUID()
		if !ret.IsSuccess() {
			logger.V(3).Error(ret, "failed to get device UUID")
			continue
		}
		if !deviceIDs.Has(uuid) {
			continue
		}

		infos, infoRet := dev.GetComputeRunningProcesses()

		procs := acceleratorProcessesOf(uuid, infos, infoRet)
		if procs.MemoryReason != device.AcceleratorProcessReasonNone {
			logger.V(3).Info("no per-process memory", "reason", procs.MemoryReason, "return", infoRet)
		}
		grp.Accelerators = append(grp.Accelerators, procs)
		answered.Insert(uuid)
	}

	// 由于同一原因，一个被请求的设备没有列出其处理柄——即一个处理柄或 UUID 调用失败，或者一张卡片在分
	// 配仍将其命名为设备时已离开机器——也被报告为未读取状态：因为每个被请求的设备只对应一个承诺项，而
	// 带有原因的条目能够使其缺失变得合理。
	grp.Accelerators = append(grp.Accelerators,
		unreadAcceleratorProcesses(deviceIDs, answered)...)

	return grp, nil
}

// unreadAcceleratorProcesses 返回每个未响应请求设备的一个条目，每个条目没有行数据，原因是内存中的临时问题，
// 且不在计算环境中得到支持。
//
// 内存原因属于临时性的，而非故意不支持：这些路径上没有任何内容可以证明 IXML 会处理该查询，因此通过一次失
// 败的测试无法得出它不会处理的结论。在 Iluvatar 的其他地方一样，计算功能在这里也未被支持，因为根本没有携
// 带进程级使用情况的 IXML 查询，这是库的属性，而非本次测试的结果。
func unreadAcceleratorProcesses(
	deviceIDs, answered sets.Set[string],
) []device.AcceleratorProcesses {
	unread := make([]device.AcceleratorProcesses, 0, deviceIDs.Len()-answered.Len())
	for _, id := range sets.List(deviceIDs.Difference(answered)) {
		unread = append(unread, device.AcceleratorProcesses{
			ID:           id,
			MemoryReason: device.AcceleratorProcessReasonDriverError,
			CoresReason:  device.AcceleratorProcessReasonUnsupported,
		})
	}
	return unread
}

// acceleratorProcessesOf 将某设备的 IXML 计算过程结果转换为聚合器所消耗的行数据，使用 IXML 自己的单位进行
// 表示，且不会扁平化其语义。
//
// CoresReason 始终不受支持：IIXML 根本不提供进程级别的利用率查询功能，因此在该硬件上进程级计算不仅为零，
// 甚至无法使用——这个适配器绝不能声称有此类数据。
func acceleratorProcessesOf(id string, infos []ixml.ProcessInfo_v1, infoRet ixml.Return) device.AcceleratorProcesses {
	procs := device.AcceleratorProcesses{
		ID:           id,
		MemoryReason: processQueryReason(infoRet),
		CoresReason:  device.AcceleratorProcessReasonUnsupported,
	}

	procs.Processes = make([]device.AcceleratorProcess, 0, len(infos))
	if procs.MemoryReason != device.AcceleratorProcessReasonNone {
		return procs
	}
	for i := range infos {
		procs.Processes = append(procs.Processes, device.AcceleratorProcess{
			PID:         infos[i].Pid,
			MemoryBytes: &infos[i].UsedGpuMemory,
		})
	}
	return procs
}

// processQueryReason 类用于分类 IXML 的计算运行进程查询所回答的内容。
//
// 成功的查询没有返回任何行并不等同于拒绝：IXML 只处理那些包含设备的查询，因此空结果就是设备处于空闲状态的
// 体现。
func processQueryReason(ret ixml.Return) device.AcceleratorProcessReason {
	switch {
	case ret.IsSuccess(), ret == ixml.ERROR_NOT_FOUND:
		return device.AcceleratorProcessReasonNone
	case ret.IsAPIUnavailable(), ret == ixml.ERROR_NOT_SUPPORTED:
		return device.AcceleratorProcessReasonUnsupported
	case ret == ixml.ERROR_NO_PERMISSION:
		return device.AcceleratorProcessReasonPermission
	case ret == ixml.ERROR_INSUFFICIENT_SIZE:
		return device.AcceleratorProcessReasonTruncated
	}
	return device.AcceleratorProcessReasonDriverError
}
