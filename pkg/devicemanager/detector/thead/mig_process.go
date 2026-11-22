// SPDX-FileCopyrightText: 2026 GPUStack, Inc.
// SPDX-License-Identifier: Apache-2.0

package thead

import (
	"time"

	"gpustack.ai/gpustack/binding"
	"gpustack.ai/gpustack/binding/hgml"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/utils/loggerx"
)

// MonitorAcceleratorPartitions 实现了 device.AcceleratorPartitionDetector 接口：它会报告 PPU 的哪个硬件分
// 区包含内容，因此被授予该分区的 Instance 可以读取该分区的容量和使用情况，而不是父卡组的。
//
// 分区由记录下来的标识符所指代，仅此而已，其他分区制造商则并非如此。另一种方法是将记录下来的规格名称转换
// 为驱动程序规格 ID，枚举该规格的实例并匹配记录下来的位置——但这需要遍历整个规格目录，而此库仅枚举了 85 个
// ID，而 NVML 则只枚举了 17 个。分配器在创建或采用分区时已经读取了该标识符，因此这种遍历方式需要每张卡、
// 每台显示器周期内数百次驱动程序调用来恢复已存储的信息。
//
// 其后果很明确，而非隐藏：由旧版 Device Manager 分配的分区，其注释中没有标识符，在 Pod 重新分配之前，会被
// 视为不存在并给出原因。这种“不存在”是实话——父卡片的内容属于所有租户，将其报告为当前 Instance 的内容，不
// 如什么都不报好。
//
// 不像其他分区制造商那样，不会报告计算利用率：没有针对每个分区的计算查询设置，因此每个结果都带有不支持的
// 原因——即没有明确的成因，也没有被测量为零的情况。
func (in *thead) MonitorAcceleratorPartitions(
	noPciCheck bool, requests []device.AcceleratorPartitionRequest,
) (_ device.AcceleratorPartitionsGroup, err error) {
	grp := device.AcceleratorPartitionsGroup{Manufacturer: Manufacturer}

	defer loggerx.RecoverWithStackScanner(func(s loggerx.Scanner, e error) {
		in.logger.Error(e, "failed to monitor thead device partitions")
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
		// 分配名称指的是卡上某个分区，没有 THead PCI 设备可以响应。每个请求仍然会被响应——带有原因
		// 的响应能够解释其缺失的原因。
		in.logger.Info("no thead pci devices found for allocated partitions",
			"partitions", len(requests))
		grp.Partitions = unreadAcceleratorPartitions(
			requests, device.AcceleratorProcessReasonDriverError,
		)
		return grp, nil
	}

	in.init()

	cnt, ret := in.hgml.DeviceGetCount()
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

		dev, ret := in.hgml.DeviceGetHandleByIndex(i)
		if !ret.IsSuccess() {
			logger.V(3).Error(ret, "failed to get device handle")
			continue
		}

		// 身份首先被解析，因此没有分区划分的卡片只需一次便宜的调用，完全不需要进行分区枚举。
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

// migPartition 是指一个分区自身的 MIG 设备句柄所报告的内容：即消费者将其识别为的身份，以及该句柄所测量的
// 内存对。
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
			DeviceID:    req.DeviceID,
			Profile:     req.Profile,
			Placements:  req.Placements,
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

// migPartitionSource 将解析的两次读取操作绑定到一张卡片的 HGML 句柄上。
//
// MIG 设备句柄最多在每次卡片遍历和每次读取时处理一次，仅在需要处理的首次读取时执行。每次分区槽位的一次调
// 用操作会消耗成本，而所有请求均为空卡片则不会产生相关费用。
func (in *thead) migPartitionSource(dev hgml.Device) migPartitionSource {
	var (
		handles      map[string]hgml.MigDevice
		handleReason device.AcceleratorProcessReason
		walked       bool
	)

	return migPartitionSource{
		migEnabled: func() (bool, device.AcceleratorProcessReason) {
			current, _, ret := dev.GetMigMode()
			if !ret.IsSuccess() {
				return false, partitionQueryReason(ret)
			}
			// MIG 模式关闭的卡片不会持有任何分区用于读取，且在没有模式更改的情况下也不会开始
			// 持有分区：此节点无法回答的内容属于“不支持”，而非故障。
			if current != hgml.DEVICE_MIG_ENABLE {
				return false, device.AcceleratorProcessReasonUnsupported
			}
			return true, device.AcceleratorProcessReasonNone
		},

		partitionByID: func(id string) (migPartition, device.AcceleratorProcessReason, bool) {
			if !walked {
				handles, handleReason = migDeviceHandles(dev)
				walked = true
			}
			if handleReason != device.AcceleratorProcessReasonNone {
				return migPartition{}, handleReason, false
			}
			mig, ok := handles[id]
			if !ok {
				return migPartition{}, device.AcceleratorProcessReasonNone, false
			}

			memHandler := mig.GetMemoryInfoV()
			memInfo, ret := memHandler.V2()
			if !ret.IsSuccess() {
				memInfo, ret = memHandler.V1()
				if !ret.IsSuccess() {
					return migPartition{}, partitionQueryReason(ret), true
				}
			}
			return migPartition{
				id:         id,
				totalBytes: memInfo.Total,
				usedBytes:  memInfo.Used,
			}, device.AcceleratorProcessReasonNone, true
		},
	}
}

// migDeviceHandles 将卡片的实时分区标识符与对应地址的 MIG 设备句柄进行映射，该句柄即为分区自身内存的读取
// 地址。
//
// 无法读取标识符的句柄将被跳过，而不会导致卡片失败：另一个分区的信息仍然值得阅读，而当前这个分区则报告了
// 缺失情况及其原因。
func migDeviceHandles(dev hgml.Device) (map[string]hgml.MigDevice, device.AcceleratorProcessReason) {
	count, ret := dev.GetMaxMigDeviceCount()
	if !ret.IsSuccess() {
		return nil, partitionQueryReason(ret)
	}

	handles := make(map[string]hgml.MigDevice, count)
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

// partitionQueryReason 类用于分类分区读取所返回的内容。其分类逻辑与按进程分类一致——即会拒绝，除非是可能清
// 除的情况，因为这两种情况都被作为标签来解释，说明存在缺失的情况。
func partitionQueryReason(ret hgml.Return) device.AcceleratorProcessReason {
	switch {
	case ret.IsSuccess():
		return device.AcceleratorProcessReasonNone
	case ret == hgml.ERROR_NOT_SUPPORTED, ret.IsAPIUnavailable(), ret == hgml.ERROR_FUNCTION_NOT_FOUND:
		return device.AcceleratorProcessReasonUnsupported
	case ret == hgml.ERROR_NO_PERMISSION:
		return device.AcceleratorProcessReasonPermission
	}
	return device.AcceleratorProcessReasonDriverError
}
