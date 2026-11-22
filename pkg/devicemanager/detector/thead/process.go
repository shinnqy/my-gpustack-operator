package thead

import (
	"time"

	"k8s.io/apimachinery/pkg/util/sets"

	"gpustack.ai/gpustack/binding"
	"gpustack.ai/gpustack/binding/hgml"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/utils/loggerx"
)

// MonitorAcceleratorProcesses 实现了 device.AcceleratorProcessDetector 接口：它报告每个 PPU 由哪些主机进
// 程占用，占用的内存量以及计算资源量，从而让 Device Manager 能够将共享卡中的份额分配给持有该卡的 Instance。
//
// 它再次枚举了设备，而不是跟随 MonitorAccelerator 进行操作，后者将原始行数据——即主机进程 ID——排除在所有传
// 输的数据类型之外。因此，两次操作之间仅间隔几毫秒。这里没有任何数据是从卡片自身数据中提取出来的，因此消
// 费者无法察觉到这一差异。
func (in *thead) MonitorAcceleratorProcesses(
	noPciCheck bool, deviceIDs sets.Set[string],
) (_ device.AcceleratorProcessesGroup, err error) {
	grp := device.AcceleratorProcessesGroup{Manufacturer: Manufacturer}

	defer loggerx.RecoverWithStackScanner(func(s loggerx.Scanner, e error) {
		in.logger.Error(e, "failed to monitor thead device processes")
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
		// 某些设备在卡上保留了过时的分配信息，而 THead PCI 设备无法响应。这值得向运营商说明——即过
		// 时的分配，或者已离开总线的卡，因此每个被请求的设备都被报告为未读取状态，而不是被丢弃，
		// 否则消费者会面临一种无法解释的缺失情况。
		in.logger.Info("no thead pci devices found for allocated accelerators",
			"devices", deviceIDs.Len())
		grp.Accelerators = unreadAcceleratorProcesses(deviceIDs, nil)
		return grp, nil
	}

	in.init()

	cnt, ret := in.hgml.DeviceGetCount()
	if !ret.IsSuccess() || cnt == 0 {
		in.logger.V(3).Error(ret, "failed to count devices for allocated accelerators",
			"count", cnt, "devices", deviceIDs.Len())
		grp.Accelerators = unreadAcceleratorProcesses(deviceIDs, nil)
		return grp, nil
	}

	grp.Accelerators = make([]device.AcceleratorProcesses, 0, deviceIDs.Len())
	answered := sets.New[string]()
	for i := 0; i < cnt; i++ {
		logger := in.logger.WithValues("index", i)

		dev, ret := in.hgml.DeviceGetHandleByIndex(i)
		if !ret.IsSuccess() {
			logger.V(3).Error(ret, "failed to get device handle")
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
		infos, infoRet := dev.GetComputeRunningProcesses()
		// 要求获取驾驶员仍持有的每一个样本，而不仅仅是那些从记忆中的时间戳开始的样本：该库会保留
		// 几秒钟的这些数据，而每个进程最新的一个样本才是有价值的报告对象。因此，在多个时间戳之间
		// 移动光标毫无意义，甚至会导致错过的一个时间戳变成空白。
		samples, sampleRet := dev.GetProcessUtilization(0)

		procs := acceleratorProcessesOf(uuid, infos, infoRet, samples, sampleRet)
		if procs.MemoryReason != device.AcceleratorProcessReasonNone {
			logger.V(3).Info("no per-process memory", "reason", procs.MemoryReason, "return", infoRet)
		}
		if procs.CoresReason != device.AcceleratorProcessReasonNone {
			logger.V(3).Info("no per-process utilization", "reason", procs.CoresReason, "return", sampleRet)
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
// 原因只是暂时的，并非故意为之：这里没有任何内容反驳驱动程序会处理该查询，因此探针功能不应由此得出它不处
// 理的结论。这仅仅表明本次遍历无法读取这些设备。
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

// acceleratorProcessesOf 函数将某款设备的两个原始 HGML 答案转换为聚合器所消耗的行数据，使用 HGML 自己的单
// 位进行表示，且不会扁平化其语义。
//
// 这些行是该查询执行时的进程列表。当查询未执行时，这些 PID 就是利用样本所命名的进程，因此仅处理其中一项查
// 询的驱动程序仍能报告其实际服务内容。反之，仅出现在样本中的 PID 会被移除：样本缓冲区会持续运行几秒，因此
// 这样的 PID 属于已退出的进程，若将其利用率报告给不再使用的设备，则会显示出一个没有持有者的数值。
func acceleratorProcessesOf(
	id string,
	infos []hgml.ProcessInfo,
	infoRet hgml.Return,
	samples []hgml.ProcessUtilizationSample,
	sampleRet hgml.Return,
) device.AcceleratorProcesses {
	procs := device.AcceleratorProcesses{
		ID:           id,
		MemoryReason: processQueryReason(infoRet),
		CoresReason:  processQueryReason(sampleRet),
	}

	utilization := newestUtilizationByPID(samples)
	if procs.CoresReason != device.AcceleratorProcessReasonNone {
		utilization = nil
	}

	pids := make([]uint32, 0, max(len(infos), len(samples)))
	memory := make(map[uint32]*uint64, len(infos))
	if procs.MemoryReason == device.AcceleratorProcessReasonNone {
		for i := range infos {
			pid := infos[i].Pid
			if _, seen := memory[pid]; seen {
				continue
			}
			// 驱动无法管理这部分显存时会返回一个特殊值。把这个值当作无符号数读，会得到类型能
			// 表示的最大值，因此将它记录为无法测量。
			var used *uint64
			if infos[i].UsedGpuMemoryAvailable() {
				used = &infos[i].UsedGpuMemory
			}
			memory[pid] = used
			pids = append(pids, pid)
		}
	} else {
		for pid := range utilization {
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
			if sampled, ok := utilization[pid]; ok {
				percent = sampled
			}
			row.CoresPercent = &percent
		}
		procs.Processes = append(procs.Processes, row)
	}
	return procs
}

// newestUtilizationByPID 保存了每个进程最新的样本。
//
// 该库在每次采样周期中，每个进程会生成一个样本，且仅针对那个在该周期中未处于空闲状态的进程。因此，一个进
// 程可能会多次出现，具有不同的时间戳，而处于空闲状态的进程则完全不会出现。对某个进程的样本进行求和时，会
// 将一个进程的活动视为多个进程的活动。最新的描述正是当前的情况——这也是 PPU 切片插件的计算控制器所执行的操
// 作。
func newestUtilizationByPID(samples []hgml.ProcessUtilizationSample) map[uint32]uint32 {
	type sample struct {
		timestamp uint64
		smUtil    uint32
	}

	newest := make(map[uint32]sample, len(samples))
	for i := range samples {
		if kept, ok := newest[samples[i].Pid]; ok && kept.timestamp > samples[i].TimeStamp {
			continue
		}
		newest[samples[i].Pid] = sample{timestamp: samples[i].TimeStamp, smUtil: samples[i].SmUtil}
	}

	utilization := make(map[uint32]uint32, len(newest))
	for pid, s := range newest {
		utilization[pid] = s.smUtil
	}
	return utilization
}

// processQueryReason 类用于分类每个进程查询所回答的内容。
//
// 成功的查询没有返回任何行，并不等同于拒绝：库只报告处理了那些包含设备的记录，并且只返回了非空闲的样本，
// 因此空结果就是空闲设备的体现。ERROR_NOT_FOUND——即窗口内完全没有样本——也是同一情况，将其视为缺失会掩盖真
// 实的零值。
func processQueryReason(ret hgml.Return) device.AcceleratorProcessReason {
	switch {
	case ret.IsSuccess(), ret == hgml.ERROR_NOT_FOUND:
		return device.AcceleratorProcessReasonNone
	case ret == hgml.ERROR_NOT_SUPPORTED, ret.IsAPIUnavailable():
		return device.AcceleratorProcessReasonUnsupported
	case ret == hgml.ERROR_NO_PERMISSION:
		return device.AcceleratorProcessReasonPermission
	case ret == hgml.ERROR_INSUFFICIENT_SIZE:
		// 绑定器报告称，有一行列表因尺寸不足无法完成，因此没有返回其可容纳的部分。
		return device.AcceleratorProcessReasonTruncated
	}
	return device.AcceleratorProcessReasonDriverError
}
