package nvidia

import (
	"time"

	"gpustack.ai/gpustack/binding"
	"gpustack.ai/gpustack/binding/nvml"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/utils/loggerx"
)

// MonitorAcceleratorPartitions 实现了 device.AcceleratorPartitionDetector 接口：它会在分区自己的设备句柄
// 上报告每个请求的 MIG 分区所包含的内容。
//
// 通过将记录的规格和位置与驱动程序枚举的实例进行匹配来找到分区——而不是通过哪个实例包含我们的进程。这种差
// 异正是其存在的原因：空闲分区不包含任何进程，而先查找进程的做法则无法找到句柄，并会报告不存在的情况，而
// 实际上为零。
//
// 不会报告计算利用率。NVML 在支持 MIG 的卡上不提供每个进程的利用率，也不提供每个实例的利用率，因此每个答
// 案都带有不支持的原因——即没有明确的成因，而是以“无”来表示，因为并未测量到数值。
func (in *nvidia) MonitorAcceleratorPartitions(
	noPciCheck bool, requests []device.AcceleratorPartitionRequest,
) (_ device.AcceleratorPartitionsGroup, err error) {
	grp := device.AcceleratorPartitionsGroup{Manufacturer: Manufacturer}

	defer loggerx.RecoverWithStackScanner(func(s loggerx.Scanner, e error) {
		in.logger.Error(e, "failed to monitor nvidia device partitions")
		for s.Scan() {
			in.logger.Error(nil, s.Text())
		}
		err = e
	})

	if len(requests) == 0 {
		return grp, nil
	}

	grp.Timestamp = time.Now()

	pciDevs := binding.GetPCIDevices([]string{_PciVendor}, nil)
	if !noPciCheck && len(pciDevs) == 0 {
		// 分配名称指的是卡上某个分区，而非 NVIDIA PCI 设备负责处理。每个请求仍会被处理——带有原因
		// 的回应使得其缺失变得合理。
		in.logger.Info("no nvidia pci devices found for allocated partitions",
			"partitions", len(requests))
		grp.Partitions = unreadAcceleratorPartitions(
			requests, device.AcceleratorProcessReasonDriverError,
		)
		return grp, nil
	}

	in.init()

	cnt, ret := in.nvml.DeviceGetCount()
	if !ret.IsSuccess() || cnt == 0 {
		in.logger.V(3).Error(ret, "failed to count devices for allocated partitions",
			"count", cnt, "partitions", len(requests))
		grp.Partitions = unreadAcceleratorPartitions(
			requests, device.AcceleratorProcessReasonDriverError,
		)
		return grp, nil
	}

	byCard := partitionRequestsByCard(requests)
	grp.Partitions = make([]device.AcceleratorPartition, 0, len(requests))
	for i := 0; i < cnt; i++ {
		logger := in.logger.WithValues("index", i)

		dev, ret := in.nvml.DeviceGetHandleByIndex(i)
		if !ret.IsSuccess() {
			logger.V(3).Error(ret, "failed to get device handle")
			continue
		}

		// 身份识别首先得到解决，因此没有分区处理的卡片只需进行一次便宜的调用，完全不需要 MIG 枚举。
		uuid, ret := dev.GetUUID()
		if !ret.IsSuccess() {
			logger.V(3).Error(ret, "failed to get device UUID")
			continue
		}
		cardRequests, ok := byCard[uuid]
		if !ok {
			continue
		}
		delete(byCard, uuid)

		partitions := resolvePartitions(cardRequests, in.migPartitionSource(dev))
		for j := range partitions {
			if partitions[j].MemoryReason != device.AcceleratorProcessReasonNone {
				logger.V(3).Info("no partition memory", "device", uuid,
					"profile", partitions[j].Profile, "reason", partitions[j].MemoryReason)
			}
		}
		grp.Partitions = append(grp.Partitions, partitions...)
	}

	// 一个请求，其卡片没有列出对应的处理句柄——即失败的句柄或 UUID 调用，或者卡片已离开机器，而分配操
	// 作仍指向其上的某个分区。
	for _, cardRequests := range byCard {
		grp.Partitions = append(grp.Partitions, unreadAcceleratorPartitions(
			cardRequests, device.AcceleratorProcessReasonDriverError,
		)...)
	}

	return grp, nil
}

// partitionRequestsByCard 根据每个父加速器名称对请求进行分组，因此一张卡会被计数一次，无论该节点包含其多
// 少个分区。
func partitionRequestsByCard(
	requests []device.AcceleratorPartitionRequest,
) map[string][]device.AcceleratorPartitionRequest {
	byCard := make(map[string][]device.AcceleratorPartitionRequest)
	for _, req := range requests {
		byCard[req.DeviceID] = append(byCard[req.DeviceID], req)
	}
	return byCard
}

// unreadAcceleratorPartitions 会对所有请求作出无具体数据及给定理由的回应，适用于那些根本无法到达分区句柄
// 的路径。
//
// 调用者在此处传递的原因是暂时的，而非不受支持，只要驱动程序能够处理查询，这一假设就不成立：能力探针不应
// 因为无法访问卡片而得出“此节点无法执行”的结论。
func unreadAcceleratorPartitions(
	requests []device.AcceleratorPartitionRequest, reason device.AcceleratorProcessReason,
) []device.AcceleratorPartition {
	unread := make([]device.AcceleratorPartition, 0, len(requests))
	for _, req := range requests {
		unread = append(unread, device.AcceleratorPartition{
			DeviceID:     req.DeviceID,
			Profile:      req.Profile,
			Placements:   req.Placements,
			MemoryReason: reason,
			CoresReason:  device.AcceleratorProcessReasonUnsupported,
		})
	}
	return unread
}

// migPartition 是指单个 GPU 实例自身的 MIG 设备句柄所报告的内容：即消费者将其识别为的身份，以及该句柄所测
// 量的内存对。
type migPartition struct {
	id         string
	totalBytes uint64
	usedBytes  uint64
}

// migPartitionSource 是解析过程的面向驱动器的部分。每个读取操作都是一个闭包，因此匹配过程本身无需依赖硬件
// 且可进行单元测试，正如该包的 MIG 规格定义所要求的那样。
//
// 每个闭包在答案之外还会报告一个原因，而不是错误，因为该原因会被作为标签发布，用以说明它导致的缺失情况。
type migPartitionSource struct {
	// migEnabled 报告该卡是否进行了分区。
	migEnabled func() (bool, device.AcceleratorProcessReason)
	// partitionByID 读取名为该分配的分区。布尔值表示该卡片是否包含该标识符下的分区。
	partitionByID func(id string) (migPartition, device.AcceleratorProcessReason, bool)
}

// resolvePartitions 会按照请求的顺序，响应来自一张卡片的每一个请求。
//
// 其身份和内存数据均来自对分区自身句柄的读取，因此一个分区要么被完整报告——包括名称、大小和测量数据——要么
// 被报告为无数据状态并附带原因。这里没有需要独立解决的内容：分区关于自身的所有信息都来自同一个句柄，半个
// 答案则意味着一项无法测量的授予。
func resolvePartitions(
	requests []device.AcceleratorPartitionRequest, src migPartitionSource,
) []device.AcceleratorPartition {
	if enabled, reason := src.migEnabled(); !enabled {
		return unreadAcceleratorPartitions(requests, reason)
	}

	partitions := make([]device.AcceleratorPartition, 0, len(requests))
	for _, req := range requests {
		partition := device.AcceleratorPartition{
			DeviceID:   req.DeviceID,
			Profile:    req.Profile,
			Placements: req.Placements,
			// 在支持 MIG 的卡上，NVML 不会显示每个进程的使用情况，也不显示每个实例的使用情况，
			// 因此每个回答中都没有该数据的具体说明。
			CoresReason: device.AcceleratorProcessReasonUnsupported,
		}

		if req.ID == "" {
			// 无需处理。属于临时状态而非无支持：该节点的驱动程序能够完美满足查询需求，该记录
			// 早于该字段，而此 Pod 的下一次分配也会包含该记录。
			partition.MemoryReason = device.AcceleratorProcessReasonDriverError
			partitions = append(partitions, partition)
			continue
		}

		read, reason, found := src.partitionByID(req.ID)
		if !found && reason == device.AcceleratorProcessReasonNone {
			// 该卡片在该标识符下没有分区：要么在实时分配下被销毁，要么在卡片不再拥有分区的分
			// 配记录下被删除。如果缺失，则兄弟分区的数据会占用另一租户的内存。
			reason = device.AcceleratorProcessReasonDriverError
		}
		if found && reason == device.AcceleratorProcessReasonNone {
			partition.ID = read.id
			partition.MemoryTotalBytes = &read.totalBytes
			partition.MemoryUsedBytes = &read.usedBytes
		}
		partition.MemoryReason = reason
		partitions = append(partitions, partition)
	}
	return partitions
}

// migPartitionSource 将解析的两次读取操作绑定到一张卡的 NVML 句柄上。
//
// MIG 设备处理器在每次卡片遍历和每次访问中最多被遍历一次，仅在需要使用的首次读取时才会进行遍历。这种遍历
// 会消耗每个分区槽位的调用成本，如果某张卡片询问没有分区，则不会产生相关费用。
func (in *nvidia) migPartitionSource(dev nvml.Device) migPartitionSource {
	var (
		handles      map[string]nvml.MigDevice
		handleReason device.AcceleratorProcessReason
		walked       bool
	)
	walk := func() {
		if !walked {
			handles, handleReason = migDeviceHandles(dev)
			walked = true
		}
	}
	// readPartition 是读取分区全部答案的来源。
	readPartition := func(mig nvml.MigDevice) (migPartition, device.AcceleratorProcessReason) {
		// 首先读取身份信息：一个无人能命名的分区，消费者无法报告其任何信息，无论其内存中如何描述，
		// 因此在此处失败会导致整个答案无法给出，而非在父卡片的标识符下发布数据。
		uuid, ret := mig.GetUUID()
		if !ret.IsSuccess() {
			return migPartition{}, partitionQueryReason(ret)
		}

		memHandler := mig.GetMemoryInfoV()
		memInfo, ret := memHandler.V2()
		if !ret.IsSuccess() {
			memInfo, ret = memHandler.V1()
			if !ret.IsSuccess() {
				return migPartition{}, partitionQueryReason(ret)
			}
		}
		return migPartition{
			id:         uuid,
			totalBytes: memInfo.Total,
			usedBytes:  memInfo.Used,
		}, device.AcceleratorProcessReasonNone
	}

	return migPartitionSource{
		migEnabled: func() (bool, device.AcceleratorProcessReason) {
			current, _, ret := dev.GetMigMode()
			if !ret.IsSuccess() {
				return false, partitionQueryReason(ret)
			}
			// MIG 模式关闭的卡片不会持有任何分区用于读取，且在没有模式更改的情况下也不会开始
			// 持有分区：此节点无法回答的内容属于“不支持”，而非故障。
			if current != nvml.DEVICE_MIG_ENABLE {
				return false, device.AcceleratorProcessReasonUnsupported
			}
			return true, device.AcceleratorProcessReasonNone
		},

		partitionByID: func(id string) (migPartition, device.AcceleratorProcessReason, bool) {
			walk()
			if handleReason != device.AcceleratorProcessReasonNone {
				return migPartition{}, handleReason, false
			}
			mig, ok := handles[id]
			if !ok {
				return migPartition{}, device.AcceleratorProcessReasonNone, false
			}
			read, reason := readPartition(mig)
			return read, reason, true
		},
	}
}

// migDeviceHandles 将卡片的实时分区标识符与对应地址的 MIG 设备句柄进行映射，该句柄即为分区自身内存的读取
// 地址。
//
// 无法读取标识符的句柄将被跳过，而不会导致卡片失败：另一个分区的信息仍然值得阅读，而当前这个分区则报告了
// 缺失情况及其原因。
func migDeviceHandles(dev nvml.Device) (map[string]nvml.MigDevice, device.AcceleratorProcessReason) {
	count, ret := dev.GetMaxMigDeviceCount()
	if !ret.IsSuccess() {
		return nil, partitionQueryReason(ret)
	}

	handles := make(map[string]nvml.MigDevice, count)
	// 一个分区无法同时响应两个句柄，因此被重复声明的标识符无法被任何句柄指向——无论还有多少句柄声称该
	// 标识符，它都将保持不可指向状态。仅删除该映射条目，则第三个声索者就可以重新注册那个被第二个声索
	// 者排除的标识符。
	ambiguous := make(map[string]struct{})
	for i := 0; i < count; i++ {
		mig, ret := dev.GetMigDeviceHandleByIndex(i)
		if !ret.IsSuccess() || mig == nil {
			continue
		}
		uuid, ret := mig.GetUUID()
		if !ret.IsSuccess() || uuid == "" {
			continue
		}
		if _, dup := ambiguous[uuid]; dup {
			continue
		}
		if _, dup := handles[uuid]; dup {
			// 拒绝使用任意一种手柄操作分区，会导致数据被错误读取。
			delete(handles, uuid)
			ambiguous[uuid] = struct{}{}
			continue
		}
		handles[uuid] = mig
	}
	return handles, device.AcceleratorProcessReasonNone
}

// partitionQueryReason 类用于分类分区读取所返回的内容。其分类逻辑与按进程分类一致——即会拒绝，除非是可能清
// 除的情况，因为这两种情况都被作为标签来解释，说明存在缺失的情况。
func partitionQueryReason(ret nvml.Return) device.AcceleratorProcessReason {
	switch {
	case ret.IsSuccess():
		return device.AcceleratorProcessReasonNone
	case ret == nvml.ERROR_NOT_SUPPORTED, ret.IsAPIUnavailable(), ret == nvml.ERROR_FUNCTION_NOT_FOUND:
		return device.AcceleratorProcessReasonUnsupported
	case ret == nvml.ERROR_NO_PERMISSION:
		return device.AcceleratorProcessReasonPermission
	}
	return device.AcceleratorProcessReasonDriverError
}
