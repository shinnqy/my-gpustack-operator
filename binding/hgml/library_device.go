package hgml

import "C"
import (
	"fmt"
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
)

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
