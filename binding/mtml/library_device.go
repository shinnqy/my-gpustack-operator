package mtml

import (
	"fmt"

	"gpustack.ai/gpustack/binding"
)

// CountDevice 读取设备总数。
func (l *MTML) CountDevice() (int, Return) {
	if l.so.Lookup("mtmlLibraryCountDevice") != nil {
		return 0, ERROR_FUNCTION_NOT_FOUND
	}

	var deviceCount uint32
	ret := mtmlLibraryCountDevice(l.lib, &deviceCount)
	return int(deviceCount), ret
}

// InitDeviceByIndex 按设备索引初始化句柄。
func (l *MTML) InitDeviceByIndex(index int) (Device, Return) {
	if l.so.Lookup("mtmlLibraryInitDeviceByIndex") != nil {
		return Device{}, ERROR_FUNCTION_NOT_FOUND
	}

	var handle *mtmlDevice
	ret := mtmlLibraryInitDeviceByIndex(l.lib, uint32(index), &handle)
	return Device{handle: handle, so: l.so}, ret
}

type Device struct {
	handle *mtmlDevice
	so     binding.Library
}

// Free 释放设备句柄及其占用的资源。
func (l Device) Free() Return {
	if l.so.Lookup("mtmlLibraryFreeDevice") != nil {
		return ERROR_FUNCTION_NOT_FOUND
	}

	return mtmlLibraryFreeDevice(l.handle)
}

// GetName 获取设备名称。
func (l Device) GetName() (string, Return) {
	if l.so.Lookup("mtmlDeviceGetName") != nil {
		return "", ERROR_FUNCTION_NOT_FOUND
	}

	name := make([]byte, DEVICE_NAME_BUFFER_SIZE)
	ret := mtmlDeviceGetName(l.handle, &name[0], DEVICE_NAME_BUFFER_SIZE)
	return string(name[:clen(name)]), ret
}

// GetUUID 获取设备 UUID。
func (l Device) GetUUID() (string, Return) {
	if l.so.Lookup("mtmlDeviceGetUUID") != nil {
		return "", ERROR_FUNCTION_NOT_FOUND
	}

	uuid := make([]byte, DEVICE_UUID_BUFFER_SIZE)
	ret := mtmlDeviceGetUUID(l.handle, &uuid[0], DEVICE_UUID_BUFFER_SIZE)
	return string(uuid[:clen(uuid)]), ret
}

func (info PciInfo) GetBusId() string {
	return fmt.Sprintf("%04x:%02x:%02x.0", info.Segment, info.Bus, info.Device)
}

// GetPciInfo 读取 PCI 段、总线、设备和功能号。
func (l Device) GetPciInfo() (PciInfo, Return) {
	if l.so.Lookup("mtmlDeviceGetPciInfo") != nil {
		return PciInfo{}, ERROR_FUNCTION_NOT_FOUND
	}

	var pciInfo PciInfo
	ret := mtmlDeviceGetPciInfo(l.handle, &pciInfo)
	return pciInfo, ret
}

// CountGpuCores 读取设备的 GPU 核心数量。
func (l Device) CountGpuCores() (uint32, Return) {
	if l.so.Lookup("mtmlDeviceCountGpuCores") != nil {
		return 0, ERROR_FUNCTION_NOT_FOUND
	}

	var coreCount uint32
	ret := mtmlDeviceCountGpuCores(l.handle, &coreCount)
	return coreCount, ret
}

func (l Device) InitMemory() (MemoryHandler, Return) {
	if l.so.Lookup("mtmlDeviceInitMemory") != nil {
		return MemoryHandler{}, ERROR_FUNCTION_NOT_FOUND
	}

	var handle *mtmlMemory
	ret := mtmlDeviceInitMemory(l.handle, &handle)
	return MemoryHandler{handle: handle, so: l.so}, ret
}

type MemoryHandler struct {
	handle *mtmlMemory
	so     binding.Library
}

// Free 释放显存信息句柄及它持有的资源。
func (l MemoryHandler) Free() Return {
	if l.so.Lookup("mtmlDeviceFreeMemory") != nil {
		return ERROR_FUNCTION_NOT_FOUND
	}

	return mtmlDeviceFreeMemory(l.handle)
}

// GetTotal 返回以字节计的显存总量。
func (l MemoryHandler) GetTotal() (uint64, Return) {
	if l.so.Lookup("mtmlMemoryGetTotal") != nil {
		return 0, ERROR_FUNCTION_NOT_FOUND
	}

	var total uint64
	ret := mtmlMemoryGetTotal(l.handle, &total)
	return total, ret
}

// GetEccErrorCounter 按错误类型、计数器和位置查询 ECC 错误次数。
func (l MemoryHandler) GetEccErrorCounter(errorType MemoryErrorType, counterType EccCounterType, location MemoryLocation) (uint64, Return) {
	if l.so.Lookup("mtmlMemoryGetEccErrorCounter") != nil {
		return 0, ERROR_FUNCTION_NOT_FOUND
	}

	var count uint64
	ret := mtmlMemoryGetEccErrorCounter(l.handle, errorType, counterType, location, &count)
	return count, ret
}

// GetProperty 读取设备属性。
func (l Device) GetProperty() (DeviceProperty, Return) {
	if l.so.Lookup("mtmlDeviceGetProperty") != nil {
		return DeviceProperty{}, ERROR_FUNCTION_NOT_FOUND
	}

	var property DeviceProperty
	ret := mtmlDeviceGetProperty(l.handle, &property)
	return property, ret
}
