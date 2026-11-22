package cambricon

import (
	"time"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/ptr"

	"gpustack.ai/gpustack/binding"
	"gpustack.ai/gpustack/binding/cndev"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/utils/loggerx"
)

// kibShift 将该库所报告的每进程内存的 KiB 单位转换为接口所采用的字节单位。这是一种位移操作，而非除法操作，
// 因为转换方向是相反的：该字段属于更大的单位，因此 KiB 的数值必须增加才能满足要求。
const kibShift = 10

// MonitorAcceleratorProcesses 实现了 device.AcceleratorProcessDetector 接口：它报告每个 MLU 由哪些主机进
// 程占用以及占用的比例，从而 Device Manager 能够将共享卡中的份额归因于持有该卡的 Instance。
//
// 它再次枚举了设备，而不是跟随 MonitorAccelerator 进行操作，后者将原始行数据——即主机进程 ID——排除在所有传
// 输的数据类型之外。因此，两次操作之间仅间隔几毫秒。这里没有任何数据是从卡片自身数据中提取出来的，因此消
// 费者无法察觉到这一差异。
//
// 硬件验证情况：在编写此适配器时，尚未找到 Cambricon MLU。以下的转换仅针对 process_test.go 中记录的 CNDev
// 有效载荷进行。
func (in *cambricon) MonitorAcceleratorProcesses(
	noPciCheck bool, deviceIDs sets.Set[string],
) (_ device.AcceleratorProcessesGroup, err error) {
	grp := device.AcceleratorProcessesGroup{Manufacturer: Manufacturer}

	defer loggerx.RecoverWithStackScanner(func(s loggerx.Scanner, e error) {
		in.logger.Error(e, "failed to monitor cambricon device processes")
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
		// 某些设备在卡上保留了过时的分配信息，而 Cambricon PCI 设备无法响应这些分配。这值得向运营
		// 商说明——要么是过时的分配，要么是已离开总线的卡，因此每个被请求的设备都被报告为未读取状
		// 态，而不是被丢弃，否则消费者会面临一种无法解释的状况。
		in.logger.Info("no cambricon pci devices found for allocated accelerators",
			"devices", deviceIDs.Len())
		grp.Accelerators = unreadAcceleratorProcesses(deviceIDs, nil)
		return grp, nil
	}

	in.init()

	cnt, ret := in.cndev.GetDeviceCount()
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

		dev, ret := in.cndev.GetDeviceHandleByIndex(i)
		if !ret.IsSuccess() {
			logger.V(3).Error(ret, "failed to get device handle by index")
			continue
		}

		// 在两次查询之前就解决了身份问题，因此没有分配费用的设备可以进行一次便宜的调用，且完全不
		// 需要进行任何进程读取。
		uuid, ret := dev.GetUUID()
		if !ret.IsSuccess() {
			logger.V(3).Error(ret, "failed to get device UUID")
			continue
		}
		if !deviceIDs.Has(uuid) {
			continue
		}

		// 在任何内容被解释之前，这两个查询都是针对同一个处理对象的，因此这两个来源所描述的瞬间与
		// 两个库调用所能达到的精度是相同的。
		infos, infoRet := dev.GetProcessInfo()
		utils, utilRet := dev.GetProcessUtilization()

		procs := acceleratorProcessesOf(uuid, infos, infoRet, utils, utilRet)
		if procs.MemoryReason != device.AcceleratorProcessReasonNone {
			logger.V(3).Info("no per-process memory", "reason", procs.MemoryReason, "return", infoRet)
		}
		if procs.CoresReason != device.AcceleratorProcessReasonNone {
			logger.V(3).Info("no per-process utilization", "reason", procs.CoresReason, "return", utilRet)
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

// unreadAcceleratorProcesses 返回每个未响应请求设备的一个条目，每个条目在两张表中都没有行，且原因属于临时
// 性质。
//
// 原因只是暂时的，并非故意为之：这些路径上没有任何内容可以证明 CNDev 无法服务于任何查询，因此能力探针不应
// 因一次失败的测试就得出它无法服务的结论。计算能力在这里与内存情况一致，而不同于那些其库根本不涉及计算数
// 据的制造商：CNDev 的进程级利用率是此适配器在其他地方所读取的真实数据，因此在未读取设备上的缺失现象，与
// 内存的暂时差异相同，并非该库的特性。
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

// acceleratorProcessesOf 将某设备的两个原始 CNDev 响应转换为聚合器所消耗的行数据，使用 CNDev 自己的单位进
// 行计量，且不会扁平化其语义。
//
// 这些行是该查询执行时的进程列表。当查询未执行时，它们就是利用率行中对应的 PID，因此仅处理其中一项查询的
// 驱动程序仍能显示其处理的内容。反之，仅出现在利用率行中的 PID 会被删除：GetProcessInfo 和
// GetProcessUtilization 是查询设备当前进程表的两个独立查询，因此进程列表中未出现的 PID 可能已退出，若对不
// 再属于该设备的设备进行统计，则会显示无持有者的数据。
func acceleratorProcessesOf(
	id string,
	infos []cndev.ProcessInfo,
	infoRet cndev.Return,
	utils []cndev.ProcessUtilization,
	utilRet cndev.Return,
) device.AcceleratorProcesses {
	procs := device.AcceleratorProcesses{
		ID:           id,
		MemoryReason: processQueryReason(infoRet),
		CoresReason:  processQueryReason(utilRet),
	}

	cores := coresByPID(utils)
	if procs.CoresReason != device.AcceleratorProcessReasonNone {
		cores = nil
	}

	pids := make([]uint32, 0, max(len(infos), len(utils)))
	memory := make(map[uint32]*uint64, len(infos))
	if procs.MemoryReason == device.AcceleratorProcessReasonNone {
		for i := range infos {
			pid := infos[i].Pid
			if _, seen := memory[pid]; seen {
				continue
			}
			// 库报告中将该数值以 KiB 形式呈现，而该字段所代表的单位是字节。绑定过程故意以库自
			// 己的单位进行转换，因此这就是转换的地点——这是一次乘法操作，而非转换为 MiB，因为
			// 聚合器会直接对行进行求和，然后进行一次转换。这样，一个包含几百个 KiB 的容器就不
			// 会变成零。
			memory[pid] = ptr.To(infos[i].PhysicalMemoryUsed << kibShift)
			pids = append(pids, pid)
		}
	} else {
		for pid := range cores {
			pids = append(pids, pid)
		}
	}

	procs.Processes = make([]device.AcceleratorProcess, 0, len(pids))
	for _, pid := range pids {
		row := device.AcceleratorProcess{PID: pid, MemoryBytes: memory[pid]}
		if procs.CoresReason == device.AcceleratorProcessReasonNone {
			// 该查询返回的 pid 属于 IDLE 进程，并非不可测量进程：此库仅在采样期间处于忙碌状态
			// 的进程时会输出样本。因此，该值为零被明确标注出来，而非保留为 nil，导致聚合器将
			// 其解读为“此行无法被测量”，进而可能导致整个容器数据全部失效。
			percent := uint32(0)
			if sampled, ok := cores[pid]; ok {
				percent = sampled
			}
			row.CoresPercent = &percent
		}
		procs.Processes = append(procs.Processes, row)
	}
	return procs
}

// coresByPID 记录了每个进程使用的 MLU（IPU）份额，每个进程对应一行数据，如 CNDev 所报告的那样——这里没有按
// 时间戳排列的样本缓冲区可供合并，与某些其他厂商库中的利用率查询不同。
func coresByPID(utils []cndev.ProcessUtilization) map[uint32]uint32 {
	cores := make(map[uint32]uint32, len(utils))
	for i := range utils {
		cores[utils[i].Pid] = utils[i].IpuUtil
	}
	return cores
}

// processQueryReason 类用于分类 CNDev 的每个进程查询中，哪一种查询得到了回应。
//
// 成功的查询没有返回任何行并不属于拒绝：CNDev 仅处理持有该设备的查询，因此空结果就是设备处于空闲状态的体
// 现。
func processQueryReason(ret cndev.Return) device.AcceleratorProcessReason {
	switch {
	case ret.IsSuccess():
		return device.AcceleratorProcessReasonNone
	case ret.IsAPIUnavailable(), ret == cndev.ERROR_NOT_SUPPORTED:
		return device.AcceleratorProcessReasonUnsupported
	case ret == cndev.ERROR_NO_PERMISSION:
		return device.AcceleratorProcessReasonPermission
	case ret == cndev.ERROR_INSUFFICIENT_SPACE:
		return device.AcceleratorProcessReasonTruncated
	}
	return device.AcceleratorProcessReasonDriverError
}
