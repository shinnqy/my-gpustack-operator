package ascend

import (
	"time"

	"k8s.io/apimachinery/pkg/util/sets"

	"gpustack.ai/gpustack/binding"
	"gpustack.ai/gpustack/binding/dcmi"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/utils/loggerx"
)

// MonitorAcceleratorProcesses 实现了 device.AcceleratorProcessDetector 接口：它报告每个 NPU 由哪些主机进
// 程占用，以及占用了多少内存，从而让 Device Manager 能够将共享芯片的份额分配给持有该芯片的 Instance。
//
// 仅限内存使用。DCMI 在 dcmi_computing_resource 中携带了按 vNPU 计算的数值，但无论是 vendored 还是驱动程
// 序自己的头文件中，都没有任何函数会处理该结构。同时 libdcmi.so 也不导出此类查询，因此进程级计算不仅在这
// 个硬件上为零，甚至根本不可用——这是该适配器从未声称过的一个数值。
//
// 该库通过（卡片 ID、设备 ID）对来识别设备，而不是使用系统其他键所采用的标识符，因此下面的枚举过程与两个
// 检测器方法使用的“卡片-然后索引”遍历方式相同。在查询之前，vdie 身份会在每个句柄上被解析。
func (in *ascend) MonitorAcceleratorProcesses(
	noPciCheck bool, deviceIDs sets.Set[string],
) (_ device.AcceleratorProcessesGroup, err error) {
	grp := device.AcceleratorProcessesGroup{Manufacturer: Manufacturer}

	defer loggerx.RecoverWithStackScanner(func(s loggerx.Scanner, e error) {
		in.logger.Error(e, "failed to monitor ascend device processes")
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
		// 某些设备在芯片上保留了过时的分配资源，而 Ascend PCI 设备无法响应这些资源。这值得向运营
		// 商说明——要么是过时的分配资源，要么是已离开总线的卡片，因此每个被请求的设备都会被报告为
		// 未读取状态，而不是被丢弃，否则消费者会面临一种无法解释的状况。
		in.logger.Info("no ascend pci devices found for allocated accelerators",
			"devices", deviceIDs.Len())
		grp.Accelerators = unreadAcceleratorProcesses(deviceIDs, nil)
		return grp, nil
	}

	in.init()

	_, cardList, ret := in.dcmi.GetCardList()
	if !ret.IsSuccess() || len(cardList) == 0 {
		in.logger.V(3).Error(ret, "failed to list cards for allocated accelerators",
			"cards", len(cardList), "devices", deviceIDs.Len())
		grp.Accelerators = unreadAcceleratorProcesses(deviceIDs, nil)
		return grp, nil
	}

	grp.Accelerators = make([]device.AcceleratorProcesses, 0, deviceIDs.Len())
	answered := sets.New[string]()
	for _, card := range cardList {
		logger := in.logger.WithValues("card", card)

		cnt, ret := in.dcmi.GetDeviceNumInCard(card)
		if !ret.IsSuccess() {
			logger.V(3).Error(ret, "failed to get device count in card")
			continue
		}

		for i := int32(0); i < cnt; i++ {
			logger := logger.WithValues("index", i)

			dev := in.dcmi.GetDeviceHandleByCardAndIndex(card, i)

			if typ, ret := dev.GetType(); ret.IsSuccess() && typ != dcmi.NPU_TYPE {
				continue
			}

			// 在查询之前就解决了身份问题，因此没有分配费用的设备可以进行一次便宜的调用，且完
			// 全不会进行任何进程读取。
			uuid, ret := deviceUUID(dev)
			if !ret.IsSuccess() {
				logger.V(3).Error(ret, "failed to get device vdie info")
				continue
			}
			if !deviceIDs.Has(uuid) {
				continue
			}

			rows, rowsRet := dev.GetProcessMemoryUsage()

			procs := acceleratorProcessesOf(uuid, rows, rowsRet)
			if procs.MemoryReason != device.AcceleratorProcessReasonNone {
				logger.V(3).Info("no per-process memory", "reason", procs.MemoryReason, "return", rowsRet)
			}
			grp.Accelerators = append(grp.Accelerators, procs)
			answered.Insert(uuid)
		}
	}

	// 对于某个被请求的设备，其未列出的处理项被报告为未读——这可能是因为某个卡片、类型或 VDI 调用失败，
	// 或者某个芯片在分配名称仍保留其状态时已离开设备——原因相同：每个被请求的设备只能有一个记录项，而
	// 带有原因的记录项能够解释其不存在的原因。
	grp.Accelerators = append(grp.Accelerators,
		unreadAcceleratorProcesses(deviceIDs, answered)...)

	return grp, nil
}

// unreadAcceleratorProcesses 返回每个未响应请求设备的一个条目，每个条目在内存中仅包含一行数据及一个临时原
// 因。
//
// 内存原因属于临时性的，而非有意为之的支撑：这里没有任何内容反驳驱动程序能够处理该查询，因此探针功能也不
// 应由此得出它无法处理。这仅表明本次遍历无法读取这些设备。在 Ascend 的其他地方，计算功能同样没有得到支持，
// 因为这是库的属性而非本次遍历的属性。
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

// acceleratorProcessesOf 将某设备每进程内存的 DCMI 值，转换为聚合器所使用的单位——字节——形式，且不会对其语
// 义进行扁平化处理。
//
// 进程 ID 不为正的行不是进程：该字段为有符号类型，因此负的 ID 表示该库包含该读取内容无法解析的内容。删除
// 该行会导致整卡的所有进程都显示出来。因此，对于该样本，整卡的内存数据也会缺失，原因与列表被截断的情况相
// 同。
func acceleratorProcessesOf(
	id string, rows []dcmi.ProcMemInfo, rowsRet dcmi.Return,
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
		if rows[i].Id <= 0 {
			procs.MemoryReason = device.AcceleratorProcessReasonDriverError
			procs.Processes = procs.Processes[:0]
			return procs
		}
		procs.Processes = append(procs.Processes, device.AcceleratorProcess{
			PID:         uint32(rows[i].Id),
			MemoryBytes: &rows[i].Mem_usage,
		})
	}
	return procs
}

// processQueryReason 类用于分类 DCMI 的进程级内存查询所回答的内容。
//
// 成功的查询没有返回行数据并不等同于拒绝：库只报告设备上的内存占用情况，因此空的结果就是设备处于空闲状态
// 的体现。
func processQueryReason(ret dcmi.Return) device.AcceleratorProcessReason {
	switch {
	case ret.IsSuccess():
		return device.AcceleratorProcessReasonNone
	case ret.IsAPIUnavailable(),
		ret == dcmi.ERROR_NOT_SUPPORT, ret == dcmi.ERROR_NOT_SUPPORT_IN_CONTAINER:
		return device.AcceleratorProcessReasonUnsupported
	case ret == dcmi.ERROR_OPER_NOT_PERMITTED:
		return device.AcceleratorProcessReasonPermission
	case ret == dcmi.ERROR_LIST_TRUNCATED:
		// 绑定器报告称，某个设备拥有的进程数量超过了单个读取器能接受的列表长度，因此无法返回那些
		// 真正符合要求的行，因为库中的进程数量只是一个仅用于输出的参数，无法确定缓冲区的边界。
		return device.AcceleratorProcessReasonTruncated
	}
	return device.AcceleratorProcessReasonDriverError
}
