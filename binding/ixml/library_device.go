package ixml

import (
	"fmt"

	"gpustack.ai/gpustack/binding"
)

// DeviceGetCount 返回当前管理库能够枚举的设备数。
func (l *IXML) DeviceGetCount() (int, Return) {
	if l.so.Lookup("nvmlDeviceGetCount_v2") != nil {
		return 0, ERROR_FUNCTION_NOT_FOUND
	}

	var deviceCount uint32
	ret := nvmlDeviceGetCount_v2(&deviceCount)
	return int(deviceCount), ret
}

// DeviceGetHandleByIndex 按索引取得设备句柄。
func (l *IXML) DeviceGetHandleByIndex(index int) (Device, Return) {
	if l.so.Lookup("nvmlDeviceGetHandleByIndex_v2") != nil {
		return Device{}, ERROR_FUNCTION_NOT_FOUND
	}

	var handle nvmlDevice
	ret := nvmlDeviceGetHandleByIndex_v2(uint32(index), &handle)
	return Device{handle: handle, so: l.so}, ret
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

func (info PciInfo) GetBusId() string {
	return fmt.Sprintf("%04x:%02x:%02x.0", info.Domain, info.Bus, info.Device)
}

// GetPciInfo 返回 PCI 总线、域和设备编号。
func (l Device) GetPciInfo() (PciInfo, Return) {
	if l.so.Lookup("nvmlDeviceGetPciInfo_v3") != nil {
		return PciInfo{}, ERROR_FUNCTION_NOT_FOUND
	}

	var pci PciInfo
	ret := nvmlDeviceGetPciInfo_v3(l.handle, &pci)
	return pci, ret
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

	name := make([]byte, DEVICE_NAME_BUFFER_SIZE)
	ret := nvmlDeviceGetName(l.handle, &name[0], DEVICE_NAME_BUFFER_SIZE)
	return string(name[:clen(name)]), ret
}

// GetUUID 读取设备的唯一标识。
func (l Device) GetUUID() (string, Return) {
	if l.so.Lookup("nvmlDeviceGetUUID") != nil {
		return "", ERROR_FUNCTION_NOT_FOUND
	}

	uuid := make([]byte, DEVICE_UUID_BUFFER_SIZE)
	ret := nvmlDeviceGetUUID(l.handle, &uuid[0], DEVICE_UUID_BUFFER_SIZE)
	return string(uuid[:clen(uuid)]), ret
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
		Used:  memory.Used,
		Free:  memory.Free,
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

// GetHealth 读取设备健康位掩码，各位对应驱动检测到的异常。
func (l Device) GetHealth() (uint64, Return) {
	if l.so.Lookup("ixmlDeviceGetHealth") != nil {
		return 0, ERROR_FUNCTION_NOT_FOUND
	}

	var health uint64
	ret := ixmlDeviceGetHealth(l.handle, &health)
	return health, ret
}
