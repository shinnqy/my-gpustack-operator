package nvml

import "C"
import (
	"fmt"
	"strings"
	"unsafe"

	"gpustack.ai/gpustack/binding"
)

// DeviceGetCount 返回当前管理库能够枚举的设备数。
func (l *NVML) DeviceGetCount() (int, Return) {
	var deviceCount uint32
	if l.so.Lookup("nvmlDeviceGetCount_v2") == nil {
		ret := nvmlDeviceGetCount_v2(&deviceCount)
		return int(deviceCount), ret
	}
	if l.so.Lookup("nvmlDeviceGetCount") == nil {
		ret := nvmlDeviceGetCount(&deviceCount)
		return int(deviceCount), ret
	}
	return 0, ERROR_FUNCTION_NOT_FOUND
}

// DeviceGetHandleByIndex 按索引取得设备句柄。
func (l *NVML) DeviceGetHandleByIndex(index int) (Device, Return) {
	var handle nvmlDevice
	if l.so.Lookup("nvmlDeviceGetHandleByIndex_v2") == nil {
		ret := nvmlDeviceGetHandleByIndex_v2(uint32(index), &handle)
		return Device{handle: handle, so: l.so}, ret
	}
	if l.so.Lookup("nvmlDeviceGetHandleByIndex") == nil {
		ret := nvmlDeviceGetHandleByIndex(uint32(index), &handle)
		return Device{handle: handle, so: l.so}, ret
	}
	return Device{}, ERROR_FUNCTION_NOT_FOUND
}

type Device struct {
	handle nvmlDevice
	so     binding.Library
}

// GetCudaComputeCapability 返回设备计算能力的主、次版本号。
func (l Device) GetCudaComputeCapability() (int32, int32, Return) {
	if l.so.Lookup("nvmlDeviceGetCudaComputeCapability") != nil {
		return 0, 0, ERROR_FUNCTION_NOT_FOUND
	}

	var major, minor int32
	ret := nvmlDeviceGetCudaComputeCapability(l.handle, &major, &minor)
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
	if l.so.Lookup("nvmlDeviceGetPciInfo_v2") != nil {
		return PciInfo{}, ERROR_FUNCTION_NOT_FOUND
	}

	var pciInfo PciInfo
	ret := nvmlDeviceGetPciInfo_v2(l.handle, &pciInfo)
	return pciInfo, ret
}

func (l PciInfoHandler) V1() (PciInfo, Return) {
	if l.so.Lookup("nvmlDeviceGetPciInfo") != nil {
		return PciInfo{}, ERROR_FUNCTION_NOT_FOUND
	}

	var pciInfo PciInfo
	ret := nvmlDeviceGetPciInfo(l.handle, &pciInfo)
	return pciInfo, ret
}

// GetMinorNumber 读取设备在操作系统中的次设备号。
func (l Device) GetMinorNumber() (uint32, Return) {
	if l.so.Lookup("nvmlDeviceGetMinorNumber") != nil {
		return 0, ERROR_FUNCTION_NOT_FOUND
	}

	var minorNumber uint32
	ret := nvmlDeviceGetMinorNumber(l.handle, &minorNumber)
	return minorNumber, ret
}

// GetName 返回设备名称，通常包含 GPU 型号。
func (l Device) GetName() (string, Return) {
	if l.so.Lookup("nvmlDeviceGetName") != nil {
		return "", ERROR_FUNCTION_NOT_FOUND
	}

	name := make([]byte, DEVICE_NAME_V2_BUFFER_SIZE)
	ret := nvmlDeviceGetName(l.handle, &name[0], DEVICE_NAME_V2_BUFFER_SIZE)
	return string(name[:clen(name)]), ret
}

// GetUUID 返回跨系统识别这台设备的 UUID。
func (l Device) GetUUID() (string, Return) {
	// 判断已加载的库是否导出某个符号，调用前须先 Load
	if l.so.Lookup("nvmlDeviceGetUUID") != nil {
		return "", ERROR_FUNCTION_NOT_FOUND
	}

	uuid := make([]byte, DEVICE_UUID_V2_BUFFER_SIZE)
	// IMPORTANT: 这里把 Go 的句柄、字节指针和长度换成对应的 C 类型，再调用 C.nvmlDeviceGetUUID
	ret := nvmlDeviceGetUUID(l.handle, &uuid[0], DEVICE_UUID_V2_BUFFER_SIZE)
	// C 字符串用零字节结束。`clen` 找到第一个零字节，`uuid[:clen(uuid)]` 只取真正的文字。
	return string(uuid[:clen(uuid)]), ret
}

// GetNumGpuCores 读取设备的 GPU 核心数。
func (l Device) GetNumGpuCores() (uint32, Return) {
	if l.so.Lookup("nvmlDeviceGetNumGpuCores") != nil {
		return 0, ERROR_FUNCTION_NOT_FOUND
	}

	var numCores uint32
	ret := nvmlDeviceGetNumGpuCores(l.handle, &numCores)
	return numCores, ret
}

// GetMemoryBusWidth 以位为单位返回显存位宽。探测器用它区分 HBM 与可能因 ECC 扣减可用容量的 GDDR，
// 查询失败时保留驱动报告的容量。
func (l Device) GetMemoryBusWidth() (uint32, Return) {
	if l.so.Lookup("nvmlDeviceGetMemoryBusWidth") != nil {
		return 0, ERROR_FUNCTION_NOT_FOUND
	}

	var busWidth uint32
	ret := nvmlDeviceGetMemoryBusWidth(l.handle, &busWidth)
	return busWidth, ret
}

// GetMemoryInfoV 提供不同版本的显存查询，返回总量、已用量与空闲量。
func (l Device) GetMemoryInfoV() MemoryInfoHandler {
	return MemoryInfoHandler(l)
}

type MemoryInfoHandler Device

func (l MemoryInfoHandler) V1() (Memory_v2, Return) {
	if l.so.Lookup("nvmlDeviceGetMemoryInfo") != nil {
		return Memory_v2{}, ERROR_FUNCTION_NOT_FOUND
	}

	var memory Memory
	ret := nvmlDeviceGetMemoryInfo(l.handle, &memory)
	return Memory_v2{
		Total: memory.Total,
		Free:  memory.Free,
		Used:  memory.Used,
	}, ret
}

func (l MemoryInfoHandler) V2() (Memory_v2, Return) {
	if l.so.Lookup("nvmlDeviceGetMemoryInfo_v2") != nil {
		return Memory_v2{}, ERROR_FUNCTION_NOT_FOUND
	}

	var memory Memory_v2
	memory.Version = STRUCT_VERSION(memory, 2)
	ret := nvmlDeviceGetMemoryInfo_v2(l.handle, &memory)
	return memory, ret
}

// GetEccMode 同时读取当前和待生效的 ECC 模式。
func (l Device) GetEccMode() (EnableState, EnableState, Return) {
	if l.so.Lookup("nvmlDeviceGetEccMode") != nil {
		return FEATURE_DISABLED, FEATURE_DISABLED, ERROR_FUNCTION_NOT_FOUND
	}

	var current, pending EnableState
	ret := nvmlDeviceGetEccMode(l.handle, &current, &pending)
	return current, pending, ret
}

// GetMemoryErrorCounter 按错误类型、ECC 计数器和内存位置查询错误次数。
func (l Device) GetMemoryErrorCounter(errorType MemoryErrorType, counterType EccCounterType, locationType MemoryLocation) (uint64, Return) {
	if l.so.Lookup("nvmlDeviceGetMemoryErrorCounter") != nil {
		return 0, ERROR_FUNCTION_NOT_FOUND
	}

	var count uint64
	ret := nvmlDeviceGetMemoryErrorCounter(l.handle, errorType, counterType, locationType, &count)
	return count, ret
}

// GetGpuFabricInfoV 提供不同版本的 GPU 互连域信息查询。
func (l Device) GetGpuFabricInfoV() GpuFabricInfoHandler {
	return GpuFabricInfoHandler(l)
}

type GpuFabricInfoHandler Device

func (l GpuFabricInfoHandler) V1() (GpuFabricInfo, Return) {
	if l.so.Lookup("nvmlDeviceGetGpuFabricInfo") != nil {
		return GpuFabricInfo{}, ERROR_FUNCTION_NOT_FOUND
	}

	var gpuFabricInfo GpuFabricInfo
	ret := nvmlDeviceGetGpuFabricInfo(l.handle, &gpuFabricInfo)
	return gpuFabricInfo, ret
}

// NVML 把版本化入口命名为 nvmlDeviceGetGpuFabricInfoV。_v2 是结构版本宏的后缀。按函数的真实导出名探测，
// 避免所有驱动都错误回退到 V1。
func (l GpuFabricInfoHandler) V2() (GpuFabricInfo_v2, Return) {
	if l.so.Lookup("nvmlDeviceGetGpuFabricInfoV") != nil {
		return GpuFabricInfo_v2{}, ERROR_FUNCTION_NOT_FOUND
	}

	var info GpuFabricInfo_v2
	info.Version = STRUCT_VERSION(info, 2)
	ret := nvmlDeviceGetGpuFabricInfoV(l.handle, (*GpuFabricInfoV)(unsafe.Pointer(&info)))
	return info, ret
}

// GetMigMode 同时读取当前和待生效的 MIG 模式。
func (l Device) GetMigMode() (uint32, uint32, Return) {
	if l.so.Lookup("nvmlDeviceGetMigMode") != nil {
		return 0, 0, ERROR_FUNCTION_NOT_FOUND
	}

	var currentMode, pendingMode uint32
	ret := nvmlDeviceGetMigMode(l.handle, &currentMode, &pendingMode)
	return currentMode, pendingMode, ret
}

type (
	GpuInstance struct {
		device               nvmlDevice
		gpuInstanceId        uint32
		gpuInstanceProfileId uint32
		gpuInstancePlacement GpuInstancePlacement
		gpuInstance          nvmlGpuInstance
		so                   binding.Library
	}
)

type GpuInstanceProfileInfoHandler GpuInstance

func (l GpuInstanceProfileInfoHandler) V1() (GpuInstanceProfileInfo_v3, Return) {
	if l.so.Lookup("nvmlDeviceGetGpuInstanceProfileInfo") != nil {
		return GpuInstanceProfileInfo_v3{}, ERROR_FUNCTION_NOT_FOUND
	}

	var info GpuInstanceProfileInfo
	ret := nvmlDeviceGetGpuInstanceProfileInfo(l.device, l.gpuInstanceProfileId, &info)
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
	if l.so.Lookup("nvmlDeviceGetGpuInstanceProfileInfoV") != nil {
		return GpuInstanceProfileInfo_v3{}, ERROR_FUNCTION_NOT_FOUND
	}

	var info GpuInstanceProfileInfo_v2
	info.Version = STRUCT_VERSION(info, 2)
	ret := nvmlDeviceGetGpuInstanceProfileInfoV(l.device, l.gpuInstanceProfileId, &info)
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
	if l.so.Lookup("nvmlDeviceGetGpuInstanceProfileInfoV") != nil {
		return GpuInstanceProfileInfo_v3{}, ERROR_FUNCTION_NOT_FOUND
	}

	var info GpuInstanceProfileInfo_v3
	info.Version = STRUCT_VERSION(info, 3)
	ret := nvmlDeviceGetGpuInstanceProfileInfoV(l.device, l.gpuInstanceProfileId, (*GpuInstanceProfileInfo_v2)(unsafe.Pointer(&info)))
	return info, ret
}

// GetGpuInstanceProfileInfo 直接从设备查询 profileId，无需创建 MIG 实例。先以 V2 布局取得公共字段，
// 再读 V3 能力位，必要时回退到 V1。
// V2 和 V3 结构同为 152 字节，却从第三个字段起布局不同。有的驱动填入旧布局也会返回成功，
// 只有共享字段吻合时才可使用 V3 的 Capabilities。
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

// describesSameProfile 比较 V2 与 V3 的公共字段，避免将错误布局的切片数、显存或名称作为有效信息。
// 名称比较到首个 NUL 为止。
func describesSameProfile(a, b GpuInstanceProfileInfo_v3) bool {
	return a.Id == b.Id &&
		a.SliceCount == b.SliceCount &&
		a.InstanceCount == b.InstanceCount &&
		a.MemorySizeMB == b.MemorySizeMB &&
		a.GetName() == b.GetName()
}

// GetName 在 C 数组的边界内读取规范化的 MIG 规格名，例如 1g.5gb。旧版接口不提供名称。
func (info *GpuInstanceProfileInfo_v3) GetName() string {
	return strings.TrimPrefix(binding.GoStringN(info.Name[:]), "MIG ")
}

// gpuInstancePossiblePlacementsCall 描述两代可能落位查询共有的签名，允许优先用 _v2 并回退到基础 NVML 入口。
type gpuInstancePossiblePlacementsCall func(nvmlDevice, uint32, *GpuInstancePlacement, *uint32) Return

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
		"nvmlDeviceGetGpuInstancePossiblePlacements_v2", nvmlDeviceGetGpuInstancePossiblePlacements_v2, profileId,
	)
	if ret == ERROR_FUNCTION_NOT_FOUND {
		return l.gpuInstancePossiblePlacements(
			"nvmlDeviceGetGpuInstancePossiblePlacements", nvmlDeviceGetGpuInstancePossiblePlacements, profileId,
		)
	}
	return placements, ret
}
