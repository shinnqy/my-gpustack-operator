package mxsml

import "C"
import (
	"unsafe"

	"gpustack.ai/gpustack/binding"
)

// GetDeviceCount 读取可用设备数。
func (l *MXSML) GetDeviceCount() int {
	if l.so.Lookup("mxSmlGetDeviceCount") != nil {
		return 0
	}

	return int(mxSmlGetDeviceCount())
}

// GetDeviceHandleByIndex 按枚举索引取得设备句柄。
func (l *MXSML) GetDeviceHandleByIndex(index int) Device {
	return Device{index: uint32(index), so: l.so}
}

type Device struct {
	index uint32
	so    binding.Library
}

// GetMemoryInfo 读取设备的显存信息。
func (l Device) GetMemoryInfo() (MemoryInfo, Return) {
	if l.so.Lookup("mxSmlGetMemoryInfo") != nil {
		return MemoryInfo{}, FunctionNotFound
	}

	var info MemoryInfo
	ret := mxSmlGetMemoryInfo(l.index, &info)
	return info, ret
}

func (info DeviceInfo) GetBusId() string {
	return C.GoString((*C.char)(unsafe.Pointer(&info.BdfId[0])))
}

func (info DeviceInfo) GetDeviceName() string {
	return C.GoString((*C.char)(unsafe.Pointer(&info.DeviceName[0])))
}

func (info DeviceInfo) GetUUID() string {
	return C.GoString((*C.char)(unsafe.Pointer(&info.Uuid[0])))
}

// GetInfo 读取设备基本属性。
func (l Device) GetInfo() (DeviceInfo, Return) {
	if l.so.Lookup("mxSmlGetDeviceInfo") != nil {
		return DeviceInfo{}, FunctionNotFound
	}

	var info DeviceInfo
	ret := mxSmlGetDeviceInfo(l.index, &info)
	return info, ret
}

// GetTotalEccErrors 按错误类型和内存位置汇总 ECC 错误。
func (l Device) GetTotalEccErrors() (EccErrorCount, Return) {
	if l.so.Lookup("mxSmlGetTotalEccErrors") != nil {
		return EccErrorCount{}, FunctionNotFound
	}

	var count EccErrorCount
	ret := mxSmlGetTotalEccErrors(l.index, &count)
	return count, ret
}
