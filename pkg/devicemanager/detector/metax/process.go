package metax

import (
	"time"

	"k8s.io/apimachinery/pkg/util/sets"

	"gpustack.ai/gpustack/binding"
	"gpustack.ai/gpustack/binding/mxsml"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/utils/loggerx"
)

// MonitorAcceleratorProcesses 实现了 device.AcceleratorProcessDetector 接口：它报告每个主机进程持有哪块
// GPU 以及该 GPU 的内存大小，从而 Device Manager 能够将共享卡中的份额分配给持有该卡的 Instance。
//
// 通过硬件与真实分配进程及 mx-smi --show-process 进行验证。第一次硬件运行中发现此处传递的行匹配 ID 为
// DeviceInfo.GpuId（物理 ID），而 ProcessGpuInfo.GpuId 是一个序列索引，因此每一行都被静默丢弃，每张卡都被
// 识别为空闲状态。关于修复方法，请参考 acceleratorProcessesOf 的文档。
func (in *metax) MonitorAcceleratorProcesses(
	noPciCheck bool, deviceIDs sets.Set[string],
) (_ device.AcceleratorProcessesGroup, err error) {
	grp := device.AcceleratorProcessesGroup{Manufacturer: Manufacturer}

	defer loggerx.RecoverWithStackScanner(func(s loggerx.Scanner, e error) {
		in.logger.Error(e, "failed to monitor metax device processes")
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
		// 某些设备在卡上保留了过时的分配信息，而 MetaX PCI 设备无法对此作出响应。这值得向运营商说
		// 明——要么是过时的分配信息，要么是已离开总线的卡，因此每个被请求的设备都被报告为未读取状
		// 态，而非被丢弃，否则消费者将面临无法解释的空白状态。
		in.logger.Info("no metax pci devices found for allocated accelerators",
			"devices", deviceIDs.Len())
		grp.Accelerators = unreadAcceleratorProcesses(deviceIDs, nil)
		return grp, nil
	}

	in.init()

	cnt := in.mxsml.GetDeviceCount()
	if cnt == 0 {
		in.logger.V(3).Info("no metax devices found for allocated accelerators",
			"devices", deviceIDs.Len())
		grp.Accelerators = unreadAcceleratorProcesses(deviceIDs, nil)
		return grp, nil
	}

	grp.Accelerators = make([]device.AcceleratorProcesses, 0, deviceIDs.Len())
	answered := sets.New[string]()
	for i := 0; i < cnt; i++ {
		logger := in.logger.WithValues("index", i)

		dev := in.mxsml.GetDeviceHandleByIndex(i)

		info, ret := dev.GetInfo()
		if !ret.IsSuccess() {
			logger.V(3).Error(ret, "failed to get device info")
			continue
		}
		if mxsml.DeviceVirtualizationMode(info.Mode) == mxsml.Virtualization_Mode_Vf {
			continue
		}

		// 在查询之前就解决了身份问题，因此没有分配费用的设备可以进行一次便宜的调用，且完全不会进
		// 行任何进程读取。
		uuid := info.GetUUID()
		if !deviceIDs.Has(uuid) {
			continue
		}

		rows, rowsRet := dev.GetProcessInfo()

		procs := acceleratorProcessesOf(uuid, uint32(i), rows, rowsRet)
		if procs.MemoryReason != device.AcceleratorProcessReasonNone {
			logger.V(3).Info("no per-process memory", "reason", procs.MemoryReason, "return", rowsRet)
		}
		grp.Accelerators = append(grp.Accelerators, procs)
		answered.Insert(uuid)
	}

	// 由于同一原因，一个被请求的设备没有列出其处理入口——即一个失败的设备信息调用，或者一个在分配仍将
	// 其命名为设备的卡片——被报告为未读：因为每个被请求的设备只对应一个条目，而带有原因的条目能够解释
	// 其不存在的原因。
	grp.Accelerators = append(grp.Accelerators,
		unreadAcceleratorProcesses(deviceIDs, answered)...)

	return grp, nil
}

// unreadAcceleratorProcesses 返回每个未响应请求设备的一个条目，每个条目没有行数据，原因是内存中的临时问题，
// 且不在计算环境中得到支持。
//
// 内存原因属于临时性的，而非故意不支持：这些路径上没有任何内容可以证明 MXSML 能够满足查询需求，因此通过一
// 次失败的测试无法得出它无法满足查询的结论。在 MetaX 的其他地方一样，计算功能在这里也未被支持，因为 MXSML
// 根本没有针对每个进程的使用情况进行查询，这是库本身的属性，而非本次测试的结果。
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

// acceleratorProcessesOf 将单个设备的 MXSML 每设备处理结果转换为聚合器所消耗的行数据，使用 MXSML 自己的单
// 位制，且不会扁平化其语义。
//
// 每一行都列出了该进程所使用的每个 GPU，而不仅仅是这一台。因此，匹配 gpuID 的行会被读取。对于该进程来说，
// 没有针对此设备记录的进程则不会被报告。gpuID 是设备的序列索引（即 GetDeviceHandleByIndex 中的 i），与
// ProcessGpuInfo.GpuId 所采用的 ID 空间相同——这一点已通过硬件确认，与 DeviceInfo.GpuId 不同，后者是物理
// ID（与 sysfs 中的 phy_gpu_id 一致），且永远不会与进程行的数据匹配。
//
// CoresReason 始终不受支持：MXSML 根本没有针对每个进程的利用率查询功能，因此在该硬件上，进程级计算不仅为
// 零，甚至根本不可用——这个适配器绝不应声称有此类数据。
func acceleratorProcessesOf(
	id string, gpuID uint32, rows []mxsml.ProcessInfo_v3, rowsRet mxsml.Return,
) device.AcceleratorProcesses {
	procs := device.AcceleratorProcesses{
		ID:           id,
		MemoryReason: processQueryReason(rowsRet),
		CoresReason:  device.AcceleratorProcessReasonUnsupported,
	}

	procs.Processes = make([]device.AcceleratorProcess, 0, len(rows))
	if procs.MemoryReason != device.AcceleratorProcessReasonNone {
		return procs
	}
	for i := range rows {
		n := min(int(rows[i].GpuNumber), len(rows[i].ProcessGpuInfo))
		for j := 0; j < n; j++ {
			if rows[i].ProcessGpuInfo[j].GpuId != gpuID {
				continue
			}
			procs.Processes = append(procs.Processes, device.AcceleratorProcess{
				PID:         rows[i].ProcessId,
				MemoryBytes: &rows[i].ProcessGpuInfo[j].GpuMemoryUsage,
			})
			break
		}
	}
	return procs
}

// processQueryReason 类用于分类 MXSML 针对每个设备的查询响应内容。
//
// 成功的查询没有返回任何行并不等同于拒绝：MXSML 只处理包含该设备的查询，因此空结果就是设备处于空闲状态的
// 体现。
func processQueryReason(ret mxsml.Return) device.AcceleratorProcessReason {
	switch {
	case ret.IsSuccess():
		return device.AcceleratorProcessReasonNone
	case ret.IsAPIUnavailable(), ret == mxsml.OperationNotSupport:
		return device.AcceleratorProcessReasonUnsupported
	case ret == mxsml.PermissionDenied:
		return device.AcceleratorProcessReasonPermission
	case ret == mxsml.InsufficientSize:
		return device.AcceleratorProcessReasonTruncated
	}
	return device.AcceleratorProcessReasonDriverError
}
