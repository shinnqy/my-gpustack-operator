// SPDX-FileCopyrightText: 2026 GPUStack, Inc.
// SPDX-License-Identifier: Apache-2.0

package dmi

// Device 可表示物理 DCU 或划出的 MIG 实例，二者共享内存和利用率等查询接口。
type Device struct {
	handle dmiDevice
	lib    *DMI
}

// GpuInstance 是一个 GPU 实例的句柄：它代表一张显卡的内存和计算资源作为一个整体被分离出来，随后会在其中创
// 建计算实例。
type GpuInstance struct {
	handle dmiGpuInstance
	lib    *DMI
}

// ComputeInstance 是计算实例的句柄，即工作负载实际运行的载体。每个实例对应 exactly 一个 MIG 设备。
type ComputeInstance struct {
	handle dmiComputeInstance
	lib    *DMI
}

// maxInstancesPerQuery 限制两种数组查询的缓冲区大小。当前卡有四个 GPU 切片，
// 这里保留额外空间以兼容更细的切分。
const maxInstancesPerQuery = 32

// GetIndex 报告设备的枚举索引。
func (d Device) GetIndex() (uint32, Return) {
	var index uint32
	ret := nvmlDeviceGetIndex(d.handle, &index)
	return index, ret
}

// GetMemoryInfo 报告总内存、空闲内存和已使用内存。
//
// 对于 MIG 设备句柄，它会使用该实例自身的内存，而不是卡内存，这使其可以作为每个实例的实体来使用。
func (d Device) GetMemoryInfo() (Memory, Return) {
	var mem Memory
	ret := nvmlDeviceGetMemoryInfo(d.handle, &mem)
	return mem, ret
}

// GetUtilizationRates 函数以百分比形式报告计算与内存利用率。
//
// 这是厂商头部文件未声明的一个入口点，尽管共享对象会导出它。包装器会声明 NVML 的签名。对于 MIG 设备句柄的
// 询问，它会报告该实例自身的利用率：在拥有四个实例的卡上，其中一个运行了 matmul 循环，该实例的响应率为
// 95%，而其另外三个处于空闲状态的实例响应率为 0%。因此，它是每个实例计算量的来源，也是唯一能衡量计算量的
// 地方——此 API 上没有其他地方可以测量计算量。
func (d Device) GetUtilizationRates() (Utilization, Return) {
	var util Utilization
	ret := nvmlDeviceGetUtilizationRates(d.handle, &util)
	return util, ret
}

// GetGpuInstanceProfileInfoBySliceCount 按切片数量查询规格。厂商参数是切片数枚举的索引，0 表示一片，
// 3 表示四片。真正的 profile ID 在返回结构中。
func (d Device) GetGpuInstanceProfileInfoBySliceCount(sliceCount uint32) (GpuInstanceProfileInfo, Return) {
	var info GpuInstanceProfileInfo
	if sliceCount == 0 || sliceCount > GPU_INSTANCE_PROFILE_COUNT {
		return info, ERROR_INVALID_ARGUMENT
	}
	ret := nvmlDeviceGetGpuInstanceProfileInfo(d.handle, sliceCount-1, &info)
	return info, ret
}

// GetGpuInstancePossiblePlacements 枚举空卡上该规格允许的起点和长度。分配时还要另外排除已经占用的区间。
func (d Device) GetGpuInstancePossiblePlacements(profileID uint32) ([]GpuInstancePlacement, Return) {
	placements := make([]GpuInstancePlacement, maxInstancesPerQuery)
	count := uint32(len(placements))
	ret := nvmlDeviceGetGpuInstancePossiblePlacements(d.handle, profileID, &placements[0], &count)
	if !ret.IsSuccess() {
		return nil, ret
	}
	if count > uint32(len(placements)) {
		return nil, ERROR_INSUFFICIENT_SIZE
	}
	return placements[:count], ret
}

// GetGpuInstances 返回卡上当前存在的某个规格的 GPU 实例。
//
// 它根据规格 ID 进行筛选，因此想要获取卡片上所有实例的调用者，必须针对该规格每次询问一次卡片提供的实例。
// 如果只询问一个 ID 并将答案理解为“卡片的实例”，就会遗漏不同宽度的所有实例。
func (d Device) GetGpuInstances(profileID uint32) ([]GpuInstance, Return) {
	handles := make([]dmiGpuInstance, maxInstancesPerQuery)
	count := uint32(len(handles))
	ret := nvmlDeviceGetGpuInstances(d.handle, profileID, &handles[0], &count)
	if !ret.IsSuccess() {
		return nil, ret
	}
	if count > uint32(len(handles)) {
		return nil, ERROR_INSUFFICIENT_SIZE
	}
	out := make([]GpuInstance, count)
	for i := range out {
		out[i] = GpuInstance{handle: handles[i], lib: d.lib}
	}
	return out, ret
}

// CreateGpuInstanceWithPlacement 会根据所选的放置方式创建规格实例。
//
// 这是分配器使用的形式。让驱动程序自行选择会导致实例的位置不可预测，而调用者在重启后匹配的位置正是其重建
// 所有权所依据的内容。
func (d Device) CreateGpuInstanceWithPlacement(profileID uint32, placement GpuInstancePlacement) (GpuInstance, Return) {
	var handle dmiGpuInstance
	ret := nvmlDeviceCreateGpuInstanceWithPlacement(d.handle, profileID, &placement, &handle)
	return GpuInstance{handle: handle, lib: d.lib}, ret
}

// GetMaxMigDeviceCount 报告了此卡同时能容纳多少个 MIG 设备。
func (d Device) GetMaxMigDeviceCount() (uint32, Return) {
	var count uint32
	ret := nvmlDeviceGetMaxMigDeviceCount(d.handle, &count)
	return count, ret
}

// GetGpuInstanceID 报告了 MIG 设备句柄属于哪个 GPU 实例。
func (d Device) GetGpuInstanceID() (uint32, Return) {
	var id uint32
	ret := nvmlDeviceGetGpuInstanceId(d.handle, &id)
	return id, ret
}

// GetComputeInstanceID 报告了 MIG 设备句柄对应的计算实例。
func (d Device) GetComputeInstanceID() (uint32, Return) {
	var id uint32
	ret := nvmlDeviceGetComputeInstanceId(d.handle, &id)
	return id, ret
}

// 如果 GetMigDeviceHandleByIndex 返回的是属于该卡的全局索引下的 MIG 设备，则该函数会返回该设备。
//
// 该索引并非按卡片划分的，尽管调用操作会涉及一张卡片：它会对 NODE 上的每一台 MIG 设备进行编号。在测试主机
// 上，卡片 0 的四个实例分别位于索引 0 到 3 之间，卡片 1 的单个实例位于索引 4，卡片 2 的实例位于索引 5。每
// 个实例仅在被单独询问时响应，当被询问其他卡片时则返回 NOT_FOUND。因此，调用者若使用
// 0..GetMaxMigDeviceCount 对每张卡片进行扫描，将只能找到第一张卡片的所有实例，而其余卡片则没有。建议使用
// MigDevices，因为它能准确覆盖这种特性所实际需要的扫描范围。
func (d Device) GetMigDeviceHandleByIndex(globalIndex uint32) (Device, Return) {
	var handle dmiDevice
	ret := nvmlDeviceGetMigDeviceHandleByIndex(d.handle, globalIndex, &handle)
	return Device{handle: handle, lib: d.lib}, ret
}

// MigDevices 返回从该卡中提取出的所有 MIG 设备，解决了在 GetMigDeviceHandleByIndex 中提到的全局索引问题。
//
// 扫描过程受节点自身容量的限制——即设备数量乘以每张卡的最大数量——因为这就是共享索引空间的最大范围。一旦找
// 到该卡的最大容量，扫描就会提前停止，因此一个完整的节点所花费的成本不会超过其包含的实例数量。属于其他卡
// 的指数会返回“不存在”的代码并被跳过。其他情况则会终止扫描并返回结果，因此驱动程序错误永远不会被报告为实
// 例数量少于实际数量的卡。
func (d Device) MigDevices() ([]Device, Return) {
	perCard, ret := d.GetMaxMigDeviceCount()
	if !ret.IsSuccess() {
		return nil, ret
	}
	if perCard == 0 {
		return nil, SUCCESS
	}

	cards, ret := d.lib.GetDeviceCount()
	if !ret.IsSuccess() {
		return nil, ret
	}

	out := make([]Device, 0, perCard)
	for idx := uint32(0); idx < perCard*cards && uint32(len(out)) < perCard; idx++ {
		md, r := d.GetMigDeviceHandleByIndex(idx)
		if r.IsSuccess() {
			out = append(out, md)
			continue
		}
		if r.ReportsAbsent() {
			continue
		}
		return nil, r
	}
	return out, SUCCESS
}

// GetInfo 会报告实例的 ID、其规格 ID 以及它在卡片上的位置。
func (g GpuInstance) GetInfo() (GpuInstanceInfo, Return) {
	var info GpuInstanceInfo
	ret := nvmlGpuInstanceGetInfo(g.handle, &info)
	return info, ret
}

// GetComputeInstanceProfileInfoBySliceCount 返回该 GPU 实例中占据 sliceCount 个切片的计算实例规格信息。
//
// 切片计数索引与其 GPU 实例对应的索引相同，这也是其名称如此明确的原因。详见
// GetGpuInstanceProfileInfoBySliceCount。engProfile 用于选择推理引擎规格，而厂商仅定义了一种规格，即
// SHARED。
func (g GpuInstance) GetComputeInstanceProfileInfoBySliceCount(
	sliceCount, engProfile uint32,
) (ComputeInstanceProfileInfo, Return) {
	var info ComputeInstanceProfileInfo
	if sliceCount == 0 || sliceCount > COMPUTE_INSTANCE_PROFILE_COUNT {
		return info, ERROR_INVALID_ARGUMENT
	}
	ret := nvmlGpuInstanceGetComputeInstanceProfileInfo(g.handle, sliceCount-1, engProfile, &info)
	return info, ret
}

// GetComputeInstances 返回该 GPU 实例中属于某个规格的计算实例。与其 GPU 实例版本类似，它也通过规格 ID 进
// 行筛选，且每个规格仅需调用一次。
func (g GpuInstance) GetComputeInstances(profileID uint32) ([]ComputeInstance, Return) {
	handles := make([]dmiComputeInstance, maxInstancesPerQuery)
	count := uint32(len(handles))
	ret := nvmlGpuInstanceGetComputeInstances(g.handle, profileID, &handles[0], &count)
	if !ret.IsSuccess() {
		return nil, ret
	}
	if count > uint32(len(handles)) {
		return nil, ERROR_INSUFFICIENT_SIZE
	}
	out := make([]ComputeInstance, count)
	for i := range out {
		out[i] = ComputeInstance{handle: handles[i], lib: g.lib}
	}
	return out, ret
}

// CreateComputeInstance 会在该 GPU 实例中创建对应规格的计算实例。
func (g GpuInstance) CreateComputeInstance(profileID uint32) (ComputeInstance, Return) {
	var handle dmiComputeInstance
	ret := nvmlGpuInstanceCreateComputeInstance(g.handle, profileID, &handle)
	return ComputeInstance{handle: handle, lib: g.lib}, ret
}

// Destroy 会删除 GPU 实例。
//
// 当实例仍持有计算实例时，该操作会失败：拆卸顺序被强制为先处理计算实例。当工作负载占用实例时也会失败，即
// 驱动程序拒绝从正在运行的进程下取出卡片，而非允许通过重试来修复问题。
func (g GpuInstance) Destroy() Return {
	return nvmlGpuInstanceDestroy(g.handle)
}

// GetInfo 报告计算实例的 ID、其规格 ID，以及其在 GPU 实例切片范围内的位置。
func (c ComputeInstance) GetInfo() (ComputeInstanceInfo, Return) {
	var info ComputeInstanceInfo
	ret := nvmlComputeInstanceGetInfo(c.handle, &info)
	return info, ret
}

// Destroy 会删除计算实例。当工作负载正在使用它时，该操作会失败。
func (c ComputeInstance) Destroy() Return {
	return nvmlComputeInstanceDestroy(c.handle)
}
