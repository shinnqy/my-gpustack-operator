package hgml

import (
	"fmt"
	"runtime"
	"strings"
	"unsafe"

	"gpustack.ai/gpustack/binding"
)

// DeviceGetCount 返回当前管理库能够枚举的设备数。
func (l *HGML) DeviceGetCount() (int, Return) {
	var deviceCount uint32
	if l.so.Lookup("hgmlDeviceGetCount_v2") == nil {
		ret := hgmlDeviceGetCount_v2(&deviceCount)
		return int(deviceCount), ret
	}
	if l.so.Lookup("hgmlDeviceGetCount") == nil {
		ret := hgmlDeviceGetCount(&deviceCount)
		return int(deviceCount), ret
	}
	return 0, ERROR_FUNCTION_NOT_FOUND
}

// DeviceGetHandleByIndex 按索引取得设备句柄。
func (l *HGML) DeviceGetHandleByIndex(index int) (Device, Return) {
	var handle hgmlDevice
	if l.so.Lookup("hgmlDeviceGetHandleByIndex_v2") == nil {
		ret := hgmlDeviceGetHandleByIndex_v2(uint32(index), &handle)
		return Device{handle: handle, so: l.so}, ret
	}
	if l.so.Lookup("hgmlDeviceGetHandleByIndex") == nil {
		ret := hgmlDeviceGetHandleByIndex(uint32(index), &handle)
		return Device{handle: handle, so: l.so}, ret
	}
	return Device{}, ERROR_FUNCTION_NOT_FOUND
}

// DeviceGetHandleByUUID 用于获取具有指定 UUID 的设备的句柄。
func (l *HGML) DeviceGetHandleByUUID(uuid string) (Device, Return) {
	if l.so.Lookup("hgmlDeviceGetHandleByUUID") != nil {
		return Device{}, ERROR_FUNCTION_NOT_FOUND
	}

	var handle hgmlDevice
	ret := hgmlDeviceGetHandleByUUID(uuid+string(rune(0)), &handle)
	return Device{handle: handle, so: l.so}, ret
}

type Device struct {
	handle hgmlDevice
	so     binding.Library
}

// GetHggcComputeCapability 读取 HGGC 计算能力的主、次版本号。
func (l Device) GetHggcComputeCapability() (int32, int32, Return) {
	if l.so.Lookup("hgmlDeviceGetHggcComputeCapability") != nil {
		return 0, 0, ERROR_FUNCTION_NOT_FOUND
	}

	var major, minor int32
	ret := hgmlDeviceGetHggcComputeCapability(l.handle, &major, &minor)
	return major, minor, ret
}

// GetPciInfoV 返回可分别读取不同版本 PCI 信息的查询对象。
func (l Device) GetPciInfoV() PciInfoHandler {
	return PciInfoHandler(l)
}

type PciInfoHandler Device

func (info PciInfo) GetBusId() string {
	return fmt.Sprintf("%04x:%02x:%02x.0", info.Domain, info.Bus, info.Device)
}

func (l PciInfoHandler) V2() (PciInfo, Return) {
	if l.so.Lookup("hgmlDeviceGetPciInfo_v2") != nil {
		return PciInfo{}, ERROR_FUNCTION_NOT_FOUND
	}

	var pciInfo PciInfo
	ret := hgmlDeviceGetPciInfo_v2(l.handle, &pciInfo)
	return pciInfo, ret
}

func (l PciInfoHandler) V1() (PciInfo, Return) {
	if l.so.Lookup("hgmlDeviceGetPciInfo") != nil {
		return PciInfo{}, ERROR_FUNCTION_NOT_FOUND
	}

	var pciInfo PciInfo
	ret := hgmlDeviceGetPciInfo(l.handle, &pciInfo)
	return pciInfo, ret
}

// GetTemperature 函数用于获取设备的温度信息。
func (l Device) GetTemperature() (uint32, Return) {
	if l.so.Lookup("hgmlDeviceGetTemperature") != nil {
		return 0, ERROR_FUNCTION_NOT_FOUND
	}

	var temp uint32
	ret := hgmlDeviceGetTemperature(l.handle, TEMPERATURE_GPU, &temp)
	return temp, ret
}

// GetPowerUsage 函数以毫瓦为单位获取设备的当前功耗。
func (l Device) GetPowerUsage() (uint32, Return) {
	if l.so.Lookup("hgmlDeviceGetPowerUsage") != nil {
		return 0, ERROR_FUNCTION_NOT_FOUND
	}

	var power uint32
	ret := hgmlDeviceGetPowerUsage(l.handle, &power)
	return power, ret
}

// GetMinorNumber 读取设备在操作系统中的次设备号。
func (l Device) GetMinorNumber() (uint32, Return) {
	if l.so.Lookup("hgmlDeviceGetMinorNumber") != nil {
		return 0, ERROR_FUNCTION_NOT_FOUND
	}

	var minorNumber uint32
	ret := hgmlDeviceGetMinorNumber(l.handle, &minorNumber)
	return minorNumber, ret
}

// GetName 返回设备名称，通常包含 GPU 型号。
func (l Device) GetName() (string, Return) {
	if l.so.Lookup("hgmlDeviceGetMinorNumber") != nil {
		return "", ERROR_FUNCTION_NOT_FOUND
	}

	name := make([]byte, DEVICE_NAME_V2_BUFFER_SIZE)
	ret := hgmlDeviceGetName(l.handle, &name[0], DEVICE_NAME_V2_BUFFER_SIZE)
	return string(name[:clen(name)]), ret
}

// GetUUID 返回跨系统识别这台设备的 UUID。
func (l Device) GetUUID() (string, Return) {
	if l.so.Lookup("hgmlDeviceGetUUID") != nil {
		return "", ERROR_FUNCTION_NOT_FOUND
	}

	uuid := make([]byte, DEVICE_UUID_V2_BUFFER_SIZE)
	ret := hgmlDeviceGetUUID(l.handle, &uuid[0], DEVICE_UUID_V2_BUFFER_SIZE)
	return string(uuid[:clen(uuid)]), ret
}

// GetUtilizationRates 会获取设备的当前利用率，包括 GPU 和内存的利用率，并返回一个包含相关信息的
// Utilization 结构体。
func (l Device) GetUtilizationRates() (Utilization, Return) {
	if l.so.Lookup("hgmlDeviceGetUtilizationRates") != nil {
		return Utilization{}, ERROR_FUNCTION_NOT_FOUND
	}

	var utilization Utilization
	ret := hgmlDeviceGetUtilizationRates(l.handle, &utilization)
	return utilization, ret
}

// GetNumGpuCores 读取设备的 GPU 核心数。
func (l Device) GetNumGpuCores() (uint32, Return) {
	if l.so.Lookup("hgmlDeviceGetNumGpuCores") != nil {
		return 0, ERROR_FUNCTION_NOT_FOUND
	}

	var numCores uint32
	ret := hgmlDeviceGetNumGpuCores(l.handle, &numCores)
	return numCores, ret
}

// GetMemoryInfoV 提供不同版本的显存查询，返回总量、已用量与空闲量。
func (l Device) GetMemoryInfoV() MemoryInfoHandler {
	return MemoryInfoHandler(l)
}

type MemoryInfoHandler Device

func (l MemoryInfoHandler) V1() (Memory_v2, Return) {
	if l.so.Lookup("hgmlDeviceGetMemoryInfo") != nil {
		return Memory_v2{}, ERROR_FUNCTION_NOT_FOUND
	}

	var memory Memory
	ret := hgmlDeviceGetMemoryInfo(l.handle, &memory)
	return Memory_v2{
		Total: memory.Total,
		Used:  memory.Used,
		Free:  memory.Free,
	}, ret
}

func (l MemoryInfoHandler) V2() (Memory_v2, Return) {
	if l.so.Lookup("hgmlDeviceGetMemoryInfo_v2") != nil {
		return Memory_v2{}, ERROR_FUNCTION_NOT_FOUND
	}

	var memory Memory_v2
	memory.Version = STRUCT_VERSION(memory, 2)
	ret := hgmlDeviceGetMemoryInfo_v2(l.handle, &memory)
	return memory, ret
}

// GetMemoryErrorCounter 按错误类型、ECC 计数器和内存位置查询错误次数。
func (l Device) GetMemoryErrorCounter(errorType MemoryErrorType, counterType EccCounterType, locationType MemoryLocation) (uint64, Return) {
	if l.so.Lookup("hgmlDeviceGetMemoryErrorCounter") != nil {
		return 0, ERROR_FUNCTION_NOT_FOUND
	}

	var count uint64
	ret := hgmlDeviceGetMemoryErrorCounter(l.handle, errorType, counterType, locationType, &count)
	return count, ret
}

// GetTopologyCommonAncestor 用于获取当前设备与另一个指定设备在 GPU 拓扑中的共同祖先，并返回该共同祖先的拓
// 扑层级。
func (l Device) GetTopologyCommonAncestor(device2 Device) (GpuTopologyLevel, Return) {
	if l.so.Lookup("hgmlDeviceGetTopologyCommonAncestor") != nil {
		return TOPOLOGY_SYSTEM, ERROR_FUNCTION_NOT_FOUND
	}

	var pathInfo GpuTopologyLevel
	ret := hgmlDeviceGetTopologyCommonAncestor(l.handle, device2.handle, &pathInfo)
	return pathInfo, ret
}

// GetFieldValues 会获取设备的指定字段的值，并返回一个包含对应值的 FieldValue 结构体列表。
func (l Device) GetFieldValues(values []FieldValue) Return {
	if l.so.Lookup("hgmlDeviceGetFieldValues") != nil {
		return ERROR_FUNCTION_NOT_FOUND
	}

	valuesCount := len(values)
	return hgmlDeviceGetFieldValues(l.handle, int32(valuesCount), &values[0])
}

// GetMigMode 同时读取当前和待生效的 MIG 模式。
func (l Device) GetMigMode() (uint32, uint32, Return) {
	if l.so.Lookup("hgmlDeviceGetMigMode") != nil {
		return 0, 0, ERROR_FUNCTION_NOT_FOUND
	}

	var currentMode, pendingMode uint32
	ret := hgmlDeviceGetMigMode(l.handle, &currentMode, &pendingMode)
	return currentMode, pendingMode, ret
}

type (
	GpuInstance struct {
		device               hgmlDevice
		gpuInstanceId        uint32
		gpuInstanceProfileId uint32
		gpuInstancePlacement GpuInstancePlacement
		gpuInstance          hgmlGpuInstance
		so                   binding.Library
	}

	GpuInstanceInfo struct {
		Id        uint32
		ProfileId uint32
		Placement GpuInstancePlacement
	}
)

// GetMaxMigDeviceCount 用于获取该设备上可创建的 MIG 设备的最大数量，并返回该数量以及表示操作成功或失败的
// 返回值。
func (l Device) GetMaxMigDeviceCount() (int, Return) {
	if l.so.Lookup("hgmlDeviceGetMaxMigDeviceCount") != nil {
		return 0, ERROR_FUNCTION_NOT_FOUND
	}

	var count uint32
	ret := hgmlDeviceGetMaxMigDeviceCount(l.handle, &count)
	return int(count), ret
}

type MigDevice interface {
	// GetUUID 用于获取设备的通用唯一标识符（UUID），这是一个字符串，能够在所有系统中唯一识别该设备。
	GetUUID() (string, Return)
	// GetMemoryInfoV 用于获取设备的内存信息，包括总内存、已使用内存和空闲内存，并返回一个处理程序，可
	// 用于访问不同版本的内存信息。
	GetMemoryInfoV() MemoryInfoHandler
	// GetMemoryErrorCounter 根据指定的错误类型、ECC 计数器类型及内存位置，获取设备的内存错误计数，并
	// 将错误计数作为 uint64 值返回。
	GetMemoryErrorCounter(MemoryErrorType, EccCounterType, MemoryLocation) (uint64, Return)
	// GetTopologyCommonAncestor 用于获取当前设备与另一个指定设备在 GPU 拓扑中的共同祖先，并返回该共同
	// 祖先的拓扑层级。
	GetTopologyCommonAncestor(Device) (GpuTopologyLevel, Return)
	// GetFieldValues 会获取设备的指定字段的值，并返回一个包含对应值的 FieldValue 结构体列表。
	GetFieldValues([]FieldValue) Return
	// GetGpuInstance 会获取与设备关联的 GPU 实例，返回该 GPU 实例以及表示操作成功或失败的返回值。
	GetGpuInstance() (GpuInstance, Return)
	// GetGpuInstanceId 返回拥有此 MIG 设备句柄的 GPU 实例的 ID。与 GetGpuInstance 不同，它不会解析完
	// 整的实例（这需要父设备句柄），因此可以直接在 MIG 设备句柄上调用。
	GetGpuInstanceId() (uint32, Return)
	// GetComputeInstanceId 返回该 MIG 设备句柄所属 GPU 实例中的计算实例 ID。该值与 GetGpuInstanceId
	// 和 GetUUID 一起，通过同一句柄来识别身份字符串所指向的计算实例。
	GetComputeInstanceId() (uint32, Return)
	// GetComputeRunningProcesses 返回该 MIG 设备上正在运行的每个计算进程的一行数据，其查询条件与同名
	// 的全设备查询相同。这是唯一一个针对单个分区进行回答的查询：即针对整个加速器，父设备的查询结果。
	GetComputeRunningProcesses() ([]ProcessInfo, Return)
	// GpmQueryDeviceSupportV 用于获取设备上的 GPU 性能指标（GPM）支持信息，返回一个处理器，可用于访问
	// 不同版本的 GPM 支持信息。
	GpmQueryDeviceSupportV() GpmSupportHandler
	// GpmMetricsGetV 用于获取设备的性能指标，以进行 GPU 性能监控（GPM），并返回一个处理程序，可用于访
	// 问不同版本的 GPM 指标信息。
	GpmMetricsGetV() GpmMetricsGetHandler
}

type _MigDevice struct {
	Device
}

// GetMigDeviceHandleByIndex 根据索引获取 MIG 设备的句柄，返回 MIG 设备句柄以及表示操作成功或失败的返回值。
func (l Device) GetMigDeviceHandleByIndex(index int) (MigDevice, Return) {
	if l.so.Lookup("hgmlDeviceGetMigDeviceHandleByIndex") != nil {
		return nil, ERROR_FUNCTION_NOT_FOUND
	}

	var migDevice hgmlDevice
	ret := hgmlDeviceGetMigDeviceHandleByIndex(l.handle, uint32(index), &migDevice)
	return _MigDevice{Device{handle: migDevice, so: l.so}}, ret
}

// CreateGpuInstanceWithPlacement 根据指定的 GPU 实例规格和放置方式在设备上创建 GPU 实例，并返回所创建的
// GPU 实例以及表示操作成功或失败的返回值。
func (l Device) CreateGpuInstanceWithPlacement(info *GpuInstanceProfileInfo, placement *GpuInstancePlacement) (GpuInstance, Return) {
	if info == nil {
		return GpuInstance{}, ERROR_INVALID_ARGUMENT
	}

	if l.so.Lookup("hgmlDeviceCreateGpuInstanceWithPlacement") != nil {
		return GpuInstance{}, ERROR_FUNCTION_NOT_FOUND
	}
	var gpuInstance hgmlGpuInstance
	ret := hgmlDeviceCreateGpuInstanceWithPlacement(l.handle, info.Id, placement, &gpuInstance)
	if !ret.IsSuccess() {
		return GpuInstance{}, ret
	}

	if l.so.Lookup("hgmlGpuInstanceGetInfo") != nil {
		return GpuInstance{}, ERROR_FUNCTION_NOT_FOUND
	}
	var gpuInstanceInfo hgmlGpuInstanceInfo
	ret = hgmlGpuInstanceGetInfo(gpuInstance, &gpuInstanceInfo)
	if !ret.IsSuccess() {
		return GpuInstance{}, ret
	}

	return GpuInstance{
		device:               l.handle,
		gpuInstanceId:        gpuInstanceInfo.Id,
		gpuInstanceProfileId: gpuInstanceInfo.ProfileId,
		gpuInstancePlacement: gpuInstanceInfo.Placement,
		gpuInstance:          gpuInstance,
		so:                   l.so,
	}, ret
}

// GetGpuInstanceId 用于获取拥有该设备句柄的 GPU 实例的 ID，同时返回该 ID 以及表示操作成功或失败的返回值。
// 该函数在 MIG 设备句柄上有效，用于识别所属的 GPU 实例。
func (l Device) GetGpuInstanceId() (uint32, Return) {
	if l.so.Lookup("hgmlDeviceGetGpuInstanceId") != nil {
		return 0, ERROR_FUNCTION_NOT_FOUND
	}
	var gpuInstanceId uint32
	ret := hgmlDeviceGetGpuInstanceId(l.handle, &gpuInstanceId)
	return gpuInstanceId, ret
}

// GetComputeInstanceId 用于获取该设备句柄所属于计算实例的 ID，该实例位于其所属的 GPU 实例中。该函数会返回
// ID 以及表示操作成功或失败的返回值。该函数在 MIG 设备句柄上有效，而 MIG 设备句柄同时包含被赋予的身份字符
// 串、容器所关联的 GPU 实例 ID，以及该身份地址所关联的 GPU 和计算实例的 ID。因此，从该句柄中读取这些信息
// 可以确保它们描述的是同一个分区。每个 GPU 实例的 ID 都是唯一的，且当实例被销毁后会被重新分配。
func (l Device) GetComputeInstanceId() (uint32, Return) {
	if l.so.Lookup("hgmlDeviceGetComputeInstanceId") != nil {
		return 0, ERROR_FUNCTION_NOT_FOUND
	}
	var computeInstanceId uint32
	ret := hgmlDeviceGetComputeInstanceId(l.handle, &computeInstanceId)
	return computeInstanceId, ret
}

// GetGpuInstance 会获取与设备关联的 GPU 实例，返回该 GPU 实例以及表示操作成功或失败的返回值。
func (l Device) GetGpuInstance() (GpuInstance, Return) {
	if l.so.Lookup("hgmlDeviceGetGpuInstanceId") != nil {
		return GpuInstance{}, ERROR_FUNCTION_NOT_FOUND
	}
	var gpuInstanceId uint32
	ret := hgmlDeviceGetGpuInstanceId(l.handle, &gpuInstanceId)
	if !ret.IsSuccess() {
		return GpuInstance{}, ret
	}

	if l.so.Lookup("hgmlDeviceGetGpuInstanceById") != nil {
		return GpuInstance{}, ERROR_FUNCTION_NOT_FOUND
	}
	var gpuInstance hgmlGpuInstance
	ret = hgmlDeviceGetGpuInstanceById(l.handle, gpuInstanceId, &gpuInstance)
	if !ret.IsSuccess() {
		return GpuInstance{}, ret
	}

	if l.so.Lookup("hgmlGpuInstanceGetInfo") != nil {
		return GpuInstance{}, ERROR_FUNCTION_NOT_FOUND
	}
	var gpuInstanceInfo hgmlGpuInstanceInfo
	ret = hgmlGpuInstanceGetInfo(gpuInstance, &gpuInstanceInfo)
	if !ret.IsSuccess() {
		return GpuInstance{}, ret
	}

	return GpuInstance{
		device:               l.handle,
		gpuInstanceId:        gpuInstanceId,
		gpuInstanceProfileId: gpuInstanceInfo.ProfileId,
		gpuInstancePlacement: gpuInstanceInfo.Placement,
		gpuInstance:          gpuInstance,
		so:                   l.so,
	}, ret
}

// GetInfo 用于获取关于 GPU 实例的信息，包括其 ID、规格 ID 和放置位置。
func (l GpuInstance) GetInfo() GpuInstanceInfo {
	return GpuInstanceInfo{
		Id:        l.gpuInstanceId,
		ProfileId: l.gpuInstanceProfileId,
		Placement: l.gpuInstancePlacement,
	}
}

type GpuInstanceProfileInfoHandler GpuInstance

func (l GpuInstanceProfileInfoHandler) V1() (GpuInstanceProfileInfo_v3, Return) {
	if l.so.Lookup("hgmlDeviceGetGpuInstanceProfileInfo") != nil {
		return GpuInstanceProfileInfo_v3{}, ERROR_FUNCTION_NOT_FOUND
	}

	var info GpuInstanceProfileInfo
	ret := hgmlDeviceGetGpuInstanceProfileInfo(l.device, l.gpuInstanceProfileId, &info)
	return GpuInstanceProfileInfo_v3{
		Id:                  info.Id,
		SliceCount:          info.SliceCount,
		InstanceCount:       info.InstanceCount,
		MultiprocessorCount: info.MultiprocessorCount,
		CopyEngineCount:     info.CopyEngineCount,
		DecoderCount:        info.DecoderCount,
		EncoderCount:        info.EncoderCount,
		JpegCount:           info.JpegCount,
		OfaCount:            info.OfaCount,
		MemorySizeMB:        info.MemorySizeMB,
	}, ret
}

func (l GpuInstanceProfileInfoHandler) V2() (GpuInstanceProfileInfo_v3, Return) {
	if l.so.Lookup("hgmlDeviceGetGpuInstanceProfileInfoV") != nil {
		return GpuInstanceProfileInfo_v3{}, ERROR_FUNCTION_NOT_FOUND
	}

	var info GpuInstanceProfileInfo_v2
	info.Version = STRUCT_VERSION(info, 2)
	ret := hgmlDeviceGetGpuInstanceProfileInfoV(l.device, l.gpuInstanceProfileId, &info)
	return GpuInstanceProfileInfo_v3{
		Version:             info.Version,
		Id:                  info.Id,
		SliceCount:          info.SliceCount,
		InstanceCount:       info.InstanceCount,
		MultiprocessorCount: info.MultiprocessorCount,
		CopyEngineCount:     info.CopyEngineCount,
		DecoderCount:        info.DecoderCount,
		EncoderCount:        info.EncoderCount,
		JpegCount:           info.JpegCount,
		OfaCount:            info.OfaCount,
		MemorySizeMB:        info.MemorySizeMB,
		Name:                info.Name,
	}, ret
}

func (l GpuInstanceProfileInfoHandler) V3() (GpuInstanceProfileInfo_v3, Return) {
	if l.so.Lookup("hgmlDeviceGetGpuInstanceProfileInfoV") != nil {
		return GpuInstanceProfileInfo_v3{}, ERROR_FUNCTION_NOT_FOUND
	}

	var info GpuInstanceProfileInfo_v3
	info.Version = STRUCT_VERSION(info, 3)
	ret := hgmlDeviceGetGpuInstanceProfileInfoV(l.device, l.gpuInstanceProfileId, (*GpuInstanceProfileInfo_v2)(unsafe.Pointer(&info)))
	return info, ret
}

// GetGpuInstanceProfileInfo 按 profileId 在物理卡上查询规格，无需先创建 GPU 实例。先读 V2 的公共字段，
// 再尝试 V3 的 Capabilities，最后按需回退旧接口。
// V2 和 V3 结构均为 152 字节，但从第三个字段起布局不同。部分库会按缓冲区大小填入 V2 布局，
// 即使调用方传入 V3 并收到成功。只有两次读取的公共字段一致，才采用 V3 独有的能力位，
// 避免把拼错偏移的字节当作规格名称或切片数。
func (l Device) GetGpuInstanceProfileInfo(profileId uint32) (GpuInstanceProfileInfo_v3, Return) {
	h := GpuInstanceProfileInfoHandler{
		device:               l.handle,
		gpuInstanceProfileId: profileId,
		so:                   l.so,
	}
	if info, ret := h.V2(); ret.IsSuccess() {
		if v3, v3ret := h.V3(); v3ret.IsSuccess() && describesSameProfile(info, v3) {
			info.Capabilities = v3.Capabilities
		}
		return info, ret
	}
	if info, ret := h.V3(); ret.IsSuccess() {
		return info, ret
	}
	return h.V1()
}

// describesSameProfile 比较两次读取的所有公共字段，确认 V3 缓冲区确实按 V3 布局填写。比较名称时只读到 NUL，
// 忽略末尾未写入的字节。
func describesSameProfile(a, b GpuInstanceProfileInfo_v3) bool {
	return a.Id == b.Id &&
		a.SliceCount == b.SliceCount &&
		a.InstanceCount == b.InstanceCount &&
		a.MemorySizeMB == b.MemorySizeMB &&
		a.GetName() == b.GetName()
}

// GetName 在 C 字符数组的边界内读取规格名。旧版结构没有名称。若名称带有 MIG 前缀便去掉，以便组成资源名。
// HGML 是否返回该前缀尚需真机确认。
func (info *GpuInstanceProfileInfo_v3) GetName() string {
	return strings.TrimPrefix(binding.GoStringN(info.Name[:]), "MIG ")
}

// CreateComputeInstance 根据指定的规格信息，创建与 GPU 实例相关的计算实例。
func (l GpuInstance) CreateComputeInstance(info *ComputeInstanceProfileInfo) (ComputeInstance, Return) {
	if info == nil {
		return ComputeInstance{}, ERROR_INVALID_ARGUMENT
	}

	if l.so.Lookup("hgmlGpuInstanceCreateComputeInstance") != nil {
		return ComputeInstance{}, ERROR_FUNCTION_NOT_FOUND
	}
	var computeInstance hgmlComputeInstance
	ret := hgmlGpuInstanceCreateComputeInstance(l.gpuInstance, info.Id, &computeInstance)
	if !ret.IsSuccess() {
		return ComputeInstance{}, ret
	}

	var computeInstanceInfo hgmlComputeInstanceInfo
	if l.so.Lookup("hgmlComputeInstanceGetInfo_v2") == nil {
		ret = hgmlComputeInstanceGetInfo_v2(computeInstance, &computeInstanceInfo)
	} else {
		ret = hgmlComputeInstanceGetInfo(computeInstance, &computeInstanceInfo)
	}
	if !ret.IsSuccess() {
		return ComputeInstance{}, ret
	}

	return ComputeInstance{
		GpuInstance:              l,
		computeInstanceId:        computeInstanceInfo.Id,
		computeInstanceProfileId: computeInstanceInfo.ProfileId,
		computeInstancePlacement: computeInstanceInfo.Placement,
		computeInstance:          computeInstance,
	}, ret
}

type (
	ComputeInstance struct {
		GpuInstance
		computeInstanceId        uint32
		computeInstanceProfileId uint32
		computeInstancePlacement ComputeInstancePlacement
		computeInstance          hgmlComputeInstance
	}
	ComputeInstanceInfo struct {
		Id        uint32
		ProfileId uint32
		Placement ComputeInstancePlacement
	}
)

// GetInfo 用于获取计算实例的信息，包括其 ID、规格 ID 和放置位置。
func (l ComputeInstance) GetInfo() ComputeInstanceInfo {
	return ComputeInstanceInfo{
		Id:        l.computeInstanceId,
		ProfileId: l.computeInstanceProfileId,
		Placement: l.computeInstancePlacement,
	}
}

// `Destroy` 会销毁 GPU 实例。当该实例仍持有计算实例或活跃进程时，它会返回 ERROR_IN_USE，因此调用者应首先
// 销毁计算实例（使用 GetComputeInstances，然后调用 ComputeInstance.Destroy），并在 IN_USE 限制下重新尝试
// 操作。
func (l GpuInstance) Destroy() Return {
	if l.so.Lookup("hgmlGpuInstanceDestroy") != nil {
		return ERROR_FUNCTION_NOT_FOUND
	}
	return hgmlGpuInstanceDestroy(l.gpuInstance)
}

// Destroy 会销毁计算实例。当实例仍有活跃进程时，它会返回 ERROR_IN_USE，因此调用者会在 IN_USE 限制下重新尝
// 试。
func (l ComputeInstance) Destroy() Return {
	if l.so.Lookup("hgmlComputeInstanceDestroy") != nil {
		return ERROR_FUNCTION_NOT_FOUND
	}
	return hgmlComputeInstanceDestroy(l.computeInstance)
}

// gpuInstancePossiblePlacementsCall 对应签名相同的基础和 _v2 入口。优先用 _v2，老驱动可回退到基础入口。
type gpuInstancePossiblePlacementsCall func(hgmlDevice, uint32, *GpuInstancePlacement, *uint32) Return

func (l Device) gpuInstancePossiblePlacements(symbol string, call gpuInstancePossiblePlacementsCall, profileId uint32) ([]GpuInstancePlacement, Return) {
	if l.so.Lookup(symbol) != nil {
		return nil, ERROR_FUNCTION_NOT_FOUND
	}
	var count uint32
	if ret := call(l.handle, profileId, nil, &count); !ret.IsSuccess() {
		return nil, ret
	}
	if count == 0 {
		return nil, SUCCESS
	}
	placements := make([]GpuInstancePlacement, count)
	if ret := call(l.handle, profileId, &placements[0], &count); !ret.IsSuccess() {
		return nil, ret
	}
	// 驱动写回实际条数，调用方掌握缓冲区容量。切片前必须检查边界。
	count = min(count, uint32(len(placements)))
	return placements[:count], SUCCESS
}

// GetGpuInstancePossiblePlacements 查询指定规格在设备上的合法落位，用内存切片的起点和长度表示。
// 先取得所需容量，再填充数组，并校验返回条数。
func (l Device) GetGpuInstancePossiblePlacements(profileId uint32) ([]GpuInstancePlacement, Return) {
	placements, ret := l.gpuInstancePossiblePlacements(
		"hgmlDeviceGetGpuInstancePossiblePlacements_v2", hgmlDeviceGetGpuInstancePossiblePlacements_v2, profileId,
	)
	if ret == ERROR_FUNCTION_NOT_FOUND {
		return l.gpuInstancePossiblePlacements(
			"hgmlDeviceGetGpuInstancePossiblePlacements", hgmlDeviceGetGpuInstancePossiblePlacements, profileId,
		)
	}
	return placements, ret
}

// GetGpuInstances 返回该设备上指定规格的实时 GPU 实例。会读取每个句柄的信息，因此返回的 GpuInstance 会包
// 含其被占用的位置——即 `GetInfo().Placement`，账本正是利用该值来构建被占用区间的。
//
// 结果缓冲区的大小由规格中的合法放置数量（即每张卡实例的上限）决定，该数量通过
// GetGpuInstancePossiblePlacements 函数获取，该函数会传入创建时的 profileId。不得通过
// GetGpuInstanceProfileInfo 函数来设置大小（profileId）：该函数使用的是规格的 ENUM INDEX 索引（
// 0..GPU_INSTANCE_PROFILE_COUNT-1），而非创建 ID。这两种索引在真实硬件上有所不同，因此若用创建 ID 进行设
// 置会失败，导致所有活跃实例被清除——悄无声息地清空占用账本，从而在分配时造成重复预订位置，并隐藏实例以供
// 回收。
//
// 这种尺寸设定是固定的上限，而非猜测：possible-placements 查询会报告规格的每一种合法放置方式，而非当前可
// 用的放置方式。同一个规格的两个实时实例无法共享同一内存切片位置——因此实时实例数量永远无法超过放置实例数
// 量。等于缓冲区的出数，即表示一个完全分区的卡片，而非被截断的读取操作，因此不会被视为错误。
//
// 没有合法的位置信息，则无法存在任何实例。因此，空的放置列表会报告为空的实时集合，而不会进行进一步探测。
// 失败的放置查询会被作为失败处理，永远不会得出该结论。
func (l Device) GetGpuInstances(profileId uint32) ([]GpuInstance, Return) {
	if l.so.Lookup("hgmlDeviceGetGpuInstances") != nil {
		return nil, ERROR_FUNCTION_NOT_FOUND
	}
	placements, ret := l.GetGpuInstancePossiblePlacements(profileId)
	if !ret.IsSuccess() {
		return nil, ret
	}
	if len(placements) == 0 {
		return nil, SUCCESS
	}

	if l.so.Lookup("hgmlGpuInstanceGetInfo") != nil {
		return nil, ERROR_FUNCTION_NOT_FOUND
	}
	handles := make([]hgmlGpuInstance, len(placements))
	// count 是输入/输出缓冲区容量参数（输入：数组大小，输出：写入数量），因此将其初始化为分配的句柄数
	// 量——切勿将其设为 0，因为这会要求库填充一个零容量缓冲区，并可能导致错误或空集合的返回，此时会强
	// 制执行输入大小的限制。
	count := uint32(len(handles))
	if ret := hgmlDeviceGetGpuInstances(l.handle, profileId, &handles[0], &count); !ret.IsSuccess() {
		return nil, ret
	}
	// 通过所分配的缓冲区限制了库的外计数：如果库报告成功但计数较高，否则会超出分配的句柄索引范围，导
	// 致进程中断。
	count = min(count, uint32(len(handles)))
	instances := make([]GpuInstance, 0, count)
	for i := uint32(0); i < count; i++ {
		var info hgmlGpuInstanceInfo
		if ret := hgmlGpuInstanceGetInfo(handles[i], &info); !ret.IsSuccess() {
			return nil, ret
		}
		instances = append(instances, GpuInstance{
			device:               l.handle,
			gpuInstanceId:        info.Id,
			gpuInstanceProfileId: info.ProfileId,
			gpuInstancePlacement: info.Placement,
			gpuInstance:          handles[i],
			so:                   l.so,
		})
	}
	return instances, SUCCESS
}

// GetComputeInstanceProfileInfo 会检查该 GPU 实例上由规格枚举索引 ciProfileIndex (
// 0..COMPUTE_INSTANCE_PROFILE_COUNT-1) 和 ciEngProfileId 所标识的计算实例规格，无需先存在计算实例即可执行。
// 该函数在 CreateComputeInstance 之前被调用，用于获取构建出的计算实例的规格信息——其 ID 就是其他计算实例调
// 用时使用的创建 ID。
func (l GpuInstance) GetComputeInstanceProfileInfo(ciProfileIndex, ciEngProfileId uint32) (ComputeInstanceProfileInfo, Return) {
	if l.so.Lookup("hgmlGpuInstanceGetComputeInstanceProfileInfo") != nil {
		return ComputeInstanceProfileInfo{}, ERROR_FUNCTION_NOT_FOUND
	}
	var info ComputeInstanceProfileInfo
	ret := hgmlGpuInstanceGetComputeInstanceProfileInfo(l.gpuInstance, ciProfileIndex, ciEngProfileId, &info)
	return info, ret
}

// GetComputeInstances 返回了当前 GPU 实例上某个计算实例规格的实时计算实例，并读取每个句柄的信息以填充返回
// 的 ComputeInstance 值。Reclaim 利用此功能在实例被销毁前销毁 GPU 实例的计算实例。
//
// ciProfileIndex 是一个规格枚举索引（0..COMPUTE_INSTANCE_PROFILE_COUNT-1），即 profile-info 探针所占据的
// 空间。库自身的枚举调用会使用探针在 profile 记录中报告的创建 ID，这是一个不同的空间——头部对这两个参数进
// 行了区分——因此会传递查询到的 ID，而非索引值。直接传递索引仅会在两个空间重合的情况下生效，否则会使得某个
// 活跃计算实例无法被找到：其 GPU 实例的销毁操作将被判定为永远处于忙碌状态，导致该分区及其在节点生命周期内
// 的位置被废弃。
//
// 缓冲区的大小由规格中的 InstanceCount 决定，该值通过唯一的 `SHARED 推理引擎规格` 进行查询——
// COMPUTE_INSTANCE_ENGINE_PROFILE_COUNT 为 1——因此这种枚举不会遗漏任何计算实例，无论其由谁创建。等于该缓
// 冲大小的出数，即代表一个填满至规格上限的 GPU 实例，而非被截断的读取结果。
func (l GpuInstance) GetComputeInstances(ciProfileIndex uint32) ([]ComputeInstance, Return) {
	if l.so.Lookup("hgmlGpuInstanceGetComputeInstances") != nil {
		return nil, ERROR_FUNCTION_NOT_FOUND
	}
	profileInfo, ret := l.GetComputeInstanceProfileInfo(ciProfileIndex, COMPUTE_INSTANCE_ENGINE_PROFILE_SHARED)
	if !ret.IsSuccess() {
		return nil, ret
	}
	if profileInfo.InstanceCount == 0 {
		return nil, SUCCESS
	}

	handles := make([]hgmlComputeInstance, profileInfo.InstanceCount)
	// count 是输入/输出缓冲区容量参数。将其初始化为分配的句柄数量（参见 GetGpuInstances），这样库就永
	// 远不会被传递一个零容量缓冲区。
	count := uint32(len(handles))
	if ret := hgmlGpuInstanceGetComputeInstances(
		l.gpuInstance, profileInfo.Id, &handles[0], &count,
	); !ret.IsSuccess() {
		return nil, ret
	}
	// 通过所分配的缓冲区限制库的外计数（参见 GetGpuInstances）。
	count = min(count, uint32(len(handles)))
	instances := make([]ComputeInstance, 0, count)
	for i := uint32(0); i < count; i++ {
		var info hgmlComputeInstanceInfo
		if l.so.Lookup("hgmlComputeInstanceGetInfo_v2") == nil {
			ret = hgmlComputeInstanceGetInfo_v2(handles[i], &info)
		} else {
			ret = hgmlComputeInstanceGetInfo(handles[i], &info)
		}
		if !ret.IsSuccess() {
			return nil, ret
		}
		instances = append(instances, ComputeInstance{
			GpuInstance:              l,
			computeInstanceId:        info.Id,
			computeInstanceProfileId: info.ProfileId,
			computeInstancePlacement: info.Placement,
			computeInstance:          handles[i],
		})
	}
	return instances, SUCCESS
}

// maxProcessQueryAttempts 限制了计数后填充的读取操作。驱动程序报告的行数在探针阶段和填充阶段之间会发生变
// 化，因为进程会不断出现和消失，所以如果填充结果不足，驱动程序会按照其要求的大小重新尝试。这个限制决定了
// 重试的终止条件，当达到该限制时，会报告不足的情况，而不是上次填充的缓冲区：将被截断的列表当作完整的列表
// 处理，会将存在的进程转化为不存在的进程。
const maxProcessQueryAttempts = 3

// maxProcessRows 限制了每个进程读取会分配的行数。该数量由驱动程序提供，如果驱动程序的 ABI 与生成该驱动程
// 序的头文件不匹配，或者驱动程序本身存在缺陷，那么所报告的行数将与任何进程列表无关。若进行分配，整个进程
// 将在尝试限制机制生效之前崩溃。没有任何真实节点能达到如此多的行数：在单个加速器上，有数千个进程，这远远
// 超过了单个节点自身的进程表所能容纳的数量。超过上限的数量将被视为无法完成的读取操作，就像驱动程序所管理
// 的缓冲区容量超出限制一样。
const maxProcessRows uint32 = 4096

// invalidInstanceId 为 HGML_INVALID_GPU_INSTANCE_ID (hgml.h)，这是库本身为那些不适用的 GPU 或计算实例 ID
// 所写入的值。生成的常量不会导出该值。
const invalidInstanceId uint32 = 0xFFFFFFFF

// readProcessRows 会执行逐进程查询的“先统计再填充”协议，直至完成，仅返回完整的列表。填充操作会在没有行需
// 要统计时调用，此时使用指定大小的缓冲区进行填充，并通过统计结果报告驱动程序已写入或当前需要的行数。
//
// 返回的行数据与驱动程序所写的内容完全一致，且使用驱动程序自己的单位进行表示。
func readProcessRows[T any](fill func(count *uint32, rows []T) Return) ([]T, Return) {
	// 没有缓冲区的探针可以显示驱动程序持有的行数。当遇到零尺寸缓冲区时，驱动程序会使用短缓冲区代码进
	// 行响应，这表明探针运行正常，而非故障。
	var count uint32
	if ret := fill(&count, nil); !ret.IsSuccess() && ret != ERROR_INSUFFICIENT_SIZE {
		return nil, ret
	}

	for range maxProcessQueryAttempts {
		if count == 0 {
			return nil, SUCCESS
		}
		// 拒绝计数，进程列表可以包含其他内容而非分配它。
		if count > maxProcessRows {
			return nil, ERROR_INSUFFICIENT_SIZE
		}

		rows := make([]T, count)
		written := count
		ret := fill(&written, rows)
		switch {
		case ret == ERROR_INSUFFICIENT_SIZE:
			// 列表在探针和填充之间增长。以驾驶员当前要求的尺寸进行重试。为了使重试能够取得进
			// 展，增长必须严格遵循特定标准，因此重复我们刚才尝试过的尺寸的驾驶员会被跳过，而
			// 不是再次尝试。
			if written > count {
				count = written
			} else {
				count++
			}
		case !ret.IsSuccess():
			return nil, ret
		default:
			// 遵循我们设定的缓冲区大小，因此报告写入行数超过给定数量的驱动程序不会引发范围超
			// 出 panic 错误。
			if written > count {
				written = count
			}
			return rows[:written], ret
		}
	}

	return nil, ERROR_INSUFFICIENT_SIZE
}

// UsedGpuMemoryAvailable 报告 UsedGpuMemory 是否包含字节计数。
//
// 当驱动程序无法管理其需要测量的内存时，库会将 VALUE_NOT_AVAILABLE 写入该字段。这是一个标记，而非数字：该
// 字段为无符号类型，因此作为数字读取时，它代表最大的可表示内存数值。若调用者对其进行求和或比较，则会将不
// 可测量的进程视为饱和状态。
func (p ProcessInfo) UsedGpuMemoryAvailable() bool {
	var notAvailable int64 = VALUE_NOT_AVAILABLE
	return p.UsedGpuMemory != uint64(notAvailable)
}

// GetComputeRunningProcesses 返回该设备上正在运行的每个计算进程的一行数据，其中 UsedGpuMemory 以字节为单
// 位，符合库端的报告方式——关于进程内存无法测量时写入的哨兵值，请参考 UsedGpuMemoryAvailable 字段。该值永
// 远不会被重新设置为零。
//
// Pids 是宿主 Pids：容器化进程以宿主看到的 pid 命名，而非其自身看到的 pid，因此这里不会发生任何转换。
//
// 最新的布局更受青睐，旧布局则按顺序排列被接受，这样缺少版本标识的驱动程序会出现故障，而不是退化。较旧的
// 布局所包含的字段较少：v1 布局完全没有实例 ID，因此这些 ID 会作为库自身的无效实例值出现，而不是作为实例
// 0。
func (l Device) GetComputeRunningProcesses() ([]ProcessInfo, Return) {
	switch {
	case l.so.Lookup("hgmlDeviceGetComputeRunningProcesses_v3") == nil:
		return readProcessRows(func(count *uint32, rows []ProcessInfo) Return {
			if len(rows) == 0 {
				return hgmlDeviceGetComputeRunningProcesses_v3(l.handle, count, nil)
			}
			return hgmlDeviceGetComputeRunningProcesses_v3(l.handle, count, &rows[0])
		})
	case l.so.Lookup("hgmlDeviceGetComputeRunningProcesses_v2") == nil:
		return readProcessRows(func(count *uint32, rows []ProcessInfo) Return {
			if len(rows) == 0 {
				return hgmlDeviceGetComputeRunningProcesses_v2(l.handle, count, nil)
			}
			// v2 和 v3 布局包含相同的字段，但它们属于不同的结构体类型，因此驱动程序会填充 v2
			// 缓冲区，字段按名称进行复制，而非通过字节进行重新解释。
			buf := make([]ProcessInfo_v2, len(rows))
			ret := hgmlDeviceGetComputeRunningProcesses_v2(l.handle, count, &buf[0])
			if !ret.IsSuccess() {
				return ret
			}
			for i := range buf[:min(int(*count), len(buf))] {
				rows[i] = ProcessInfo{
					Pid:               buf[i].Pid,
					UsedGpuMemory:     buf[i].UsedGpuMemory,
					GpuInstanceId:     buf[i].GpuInstanceId,
					ComputeInstanceId: buf[i].ComputeInstanceId,
				}
			}
			return ret
		})
	case l.so.Lookup("hgmlDeviceGetComputeRunningProcesses") == nil:
		return readProcessRows(func(count *uint32, rows []ProcessInfo) Return {
			if len(rows) == 0 {
				return hgmlDeviceGetComputeRunningProcesses(l.handle, count, nil)
			}
			buf := make([]ProcessInfo_v1, len(rows))
			ret := hgmlDeviceGetComputeRunningProcesses(l.handle, count, &buf[0])
			if !ret.IsSuccess() {
				return ret
			}
			for i := range buf[:min(int(*count), len(buf))] {
				rows[i] = ProcessInfo{
					Pid:               buf[i].Pid,
					UsedGpuMemory:     buf[i].UsedGpuMemory,
					GpuInstanceId:     invalidInstanceId,
					ComputeInstanceId: invalidInstanceId,
				}
			}
			return ret
		})
	}
	return nil, ERROR_FUNCTION_NOT_FOUND
}

// GetProcessUtilization 返回了库从 lastSeenTimeStamp 开始所记录的每个进程利用率样本数据，
// lastSeenTimeStamp 是一个以微秒为单位的 CPU 时间戳。如果请求值为零，则表示驱动程序仍保留所有样本数据。
// SmUtil、MemUtil、EncUtil 和 DecUtil 是驱动程序所报告的百分比形式。
//
// 这些样本以原始形式返回，因为将其合并会破坏其原本的意义。该库会针对每个样本周期、每个进程生成一行数据，
// 仅针对该周期中利用率不为零的进程。因此，一个进程可能会以不同的时间戳出现多次，而整个期间处于空闲状态的
// 进程则根本不会出现在数据中。由调用者决定某个进程的数据行数。
//
// 结果中不存在的 pid 并非零值 pid，因此当遇到 ERROR_NOT_FOUND 时——由于 lastSeenTimeStamp 对应的样本数据完
// 全缺失——会直接返回该值，而非空列表。
//
// 两个符号都包含此查询，其中较新的符号更受青睐。两者均通过它们共有的样本字段进行报告。较新的布局下的 JPEG
// 和 OFA 利用率完全无法通过此方法获取，这并非因为数值为零，而是因为根本不存在该数据。
//
// 当前符号并非有效功能，此方法会回退而非依赖偏好设置。以 PPU 驱动程序为基准：
// hgmlDeviceGetProcessesUtilizationInfo 被导出，对每一次调用均返回 NOT_SUPPORTED 结果，而与其同期的旧版
// hgmlDeviceGetProcessUtilization 则能满足查询需求——这也是厂商自带的 ppu-smi 所绑定使用的功能。如果选择使
// 用新符号并报告其拒绝行为，那么所有针对每个进程的利用率数据都将缺失，且会以硬件名称作为原因，而实际上该
// 功能在库中只是一个空实现。
//
// 回退机制仅在入口点被拒绝时才会触发——即较新的符号拒绝查询或该结构版本被拒绝的情况——绝不会在临时故障时发
// 生，这也不涉及其他符号的情况，且会导致每一次此类故障引发两次调用。
func (l Device) GetProcessUtilization(lastSeenTimeStamp uint64) ([]ProcessUtilizationSample, Return) {
	if l.so.Lookup("hgmlDeviceGetProcessesUtilizationInfo") == nil {
		rows, ret := l.processUtilizationV2(lastSeenTimeStamp)
		if !entryPointRefused(ret) {
			return rows, ret
		}
		if l.so.Lookup("hgmlDeviceGetProcessUtilization") != nil {
			// 没有可以依靠的，所以拒绝就是答案。
			return rows, ret
		}
		return l.processUtilizationV1(lastSeenTimeStamp)
	}
	if l.so.Lookup("hgmlDeviceGetProcessUtilization") == nil {
		return l.processUtilizationV1(lastSeenTimeStamp)
	}
	return nil, ERROR_FUNCTION_NOT_FOUND
}

// entryPointRefused 用于指示“THIS SYMBOL”是否不会处理该查询，而非该尝试失败。只有前一种情况才值得尝试使用
// 其他符号。
//
// 整个 API 无法使用，这并非本集合的组成部分，尽管看起来像是其超集：一个未加载的库、一个未运行的驱动程序，
// 以及两者之间的版本不匹配，这些都是与另一个符号所属于的堆栈相关的事实。尝试使用它只会导致第二次调用以同
// 样的方式失败，更糟糕的是，还会用第二次调用的错误替换可处理的整个堆栈错误，而读者随后就会去调查这个错误。
func entryPointRefused(ret Return) bool {
	switch ret {
	case ERROR_NOT_SUPPORTED, ERROR_ARGUMENT_VERSION_MISMATCH, ERROR_FUNCTION_NOT_FOUND:
		return true
	}
	return false
}

// processUtilizationV2 通过带版本的符号读取查询，该符号的结构体内部包含计数信息，而非放在旁边。
func (l Device) processUtilizationV2(
	lastSeenTimeStamp uint64,
) ([]ProcessUtilizationSample, Return) {
	return readProcessRows(func(count *uint32, rows []ProcessUtilizationSample) Return {
		// 空数组就是该符号被用来进行计数时的形式。
		buf := make([]ProcessUtilizationInfo_v1, len(rows))
		info := ProcessesUtilizationInfo{
			ProcessSamplesCount: *count,
			LastSeenTimeStamp:   lastSeenTimeStamp,
		}
		info.Version = STRUCT_VERSION(info, 1)
		if len(rows) > 0 {
			// 传递给 C 的结构体中包含一个指向这个 Go 数组的指针，除非该数组被固定（pinned），
			// 否则 cgo 指针规则禁止这种用法：检查器会直接拒绝该调用，这是合理的——因为未固定的
			// 数组在驱动程序持有其地址期间可以被移动。通过为调用过程固定数组长度，才能使这种
			// 布局合法，且数组必须保留在 Go 内存中，因为调用者会从其中读取数据。
			var pinner runtime.Pinner
			pinner.Pin(&buf[0])
			defer pinner.Unpin()

			info.ProcUtilArray = &buf[0]
		}
		ret := hgmlDeviceGetProcessesUtilizationInfo(l.handle, &info)
		*count = info.ProcessSamplesCount
		if !ret.IsSuccess() || len(rows) == 0 {
			return ret
		}
		for i := range buf[:min(int(info.ProcessSamplesCount), len(buf))] {
			rows[i] = ProcessUtilizationSample{
				Pid:       buf[i].Pid,
				TimeStamp: buf[i].TimeStamp,
				SmUtil:    buf[i].SmUtil,
				MemUtil:   buf[i].MemUtil,
				EncUtil:   buf[i].EncUtil,
				DecUtil:   buf[i].DecUtil,
			}
		}
		return ret
	})
}

// processUtilizationV1 通过较旧的符号读取查询，该符号接受行数组和计数作为两个参数。这是厂商自身工具所依赖
// 的版本。
func (l Device) processUtilizationV1(
	lastSeenTimeStamp uint64,
) ([]ProcessUtilizationSample, Return) {
	return readProcessRows(func(count *uint32, rows []ProcessUtilizationSample) Return {
		if len(rows) == 0 {
			return hgmlDeviceGetProcessUtilization(l.handle, nil, count, lastSeenTimeStamp)
		}
		return hgmlDeviceGetProcessUtilization(l.handle, &rows[0], count, lastSeenTimeStamp)
	})
}
