package amdsmi

import (
	"C"
	"fmt"
	"strings"
	"unsafe"

	"gpustack.ai/gpustack/binding"
)

// GetProcessorHandles 枚举系统中 AMD GPU 的处理器句柄。驱动已经加载但没有可枚举的设备时，返回成功和空列表。
func (l *AMDSMI) GetProcessorHandles() ([]Device, Return) {
	if l.so.Lookup("amdsmi_get_socket_handles") != nil {
		return nil, STATUS_FUNCTION_NOT_FOUND
	}

	// 传入空数组指针，先向驱动查询所需的元素个数。
	var numSockets uint32
	ret := amdsmiGetSocketHandles(&numSockets, nil)
	if !ret.IsSuccess() {
		return nil, ret
	}
	// 先检查数量再分配缓冲区，避免对空切片取首元素地址，把零个设备误报成驱动故障。
	if numSockets == 0 {
		return nil, STATUS_SUCCESS
	}
	socketHandles := make([]*SocketHandle, numSockets)
	ret = amdsmiGetSocketHandles(&numSockets, &socketHandles[0])
	if !ret.IsSuccess() {
		return nil, ret
	}

	var handles []Device
	for i := uint32(0); i < numSockets; i++ {
		var numProcessors uint32
		ret = amdsmiGetProcessorHandles(socketHandles[i], &numProcessors, nil)
		if !ret.IsSuccess() {
			return nil, ret
		}
		// 同样跳过没有处理器的插槽，避免访问空缓冲区。
		if numProcessors == 0 {
			continue
		}
		processorHandles := make([]*ProcessorHandle, numProcessors)
		ret = amdsmiGetProcessorHandles(socketHandles[i], &numProcessors, &processorHandles[0])
		if !ret.IsSuccess() {
			return nil, ret
		}

		for j := uint32(0); j < numProcessors; j++ {
			handles = append(handles, Device{handle: processorHandles[j], so: l.so})
		}
	}

	return handles, ret
}

type Device struct {
	handle *ProcessorHandle
	so     binding.Library
}

func (info AsicInfo) GetMarketName() string {
	return C.GoString((*C.char)(unsafe.Pointer(&info.Market_name[0])))
}

// isUnsupportedSerial 识别厂商的无效序列号。头文件使用 0xFFFFFFFF 表示 ASIC 序列号不可读，
// 相关字段还会返回更宽的全 1 值。这些值都不能充当设备身份。
func isUnsupportedSerial(serial string) bool {
	return serial != "" && strings.Trim(serial, "f") == ""
}

// GetAsicSerial 返回小写 ASIC 序列号，仅在实际存在时去掉 0x 前缀。探测器和 ROCm 运行时用同一串字符匹配设备，
// 无条件截去两位会选不到卡。厂商返回 N/A 或全 1 哨兵值时返回空字符串。
func (info AsicInfo) GetAsicSerial() string {
	ret := strings.TrimPrefix(
		strings.ToLower(C.GoString((*C.char)(unsafe.Pointer(&info.Asic_serial[0])))), "0x",
	)
	if ret == "n/a" || isUnsupportedSerial(ret) {
		return ""
	}
	return ret
}

func (info AsicInfo) GetUniqueId() string {
	if as := info.GetAsicSerial(); as != "" {
		return "GPU-" + as
	}
	return ""
}

func (info AsicInfo) GetTargetGraphicsVersion() string {
	return fmt.Sprintf("gfx%x", info.Target_graphics_version)
}

// GetGpuAsicInfo 读取指定 GPU 的 ASIC 信息。
func (l Device) GetGpuAsicInfo() (AsicInfo, Return) {
	if l.so.Lookup("amdsmi_get_processor_handle_from_bdf") != nil {
		return AsicInfo{}, STATUS_FUNCTION_NOT_FOUND
	}

	var info AsicInfo
	ret := amdsmiGetGpuAsicInfo(l.handle, &info)
	return info, ret
}

func (info DriverInfo) GetVersion() string {
	return C.GoString((*C.char)(unsafe.Pointer(&info.Version[0])))
}

// GetGpuDriverInfo 读取指定 GPU 的驱动信息。
func (l Device) GetGpuDriverInfo() (DriverInfo, Return) {
	if l.so.Lookup("amdsmi_get_gpu_driver_info") != nil {
		return DriverInfo{}, STATUS_FUNCTION_NOT_FOUND
	}

	var info DriverInfo
	ret := amdsmiGetGpuDriverInfo(l.handle, &info)
	return info, ret
}

func (bdf Bdf) String() string {
	v := *(*uint64)(unsafe.Pointer(&bdf[0]))
	function := v & 0x7
	device := (v >> 3) & 0x1F
	bus := (v >> 8) & 0xFF
	domain := (v >> 16) & 0xFFFFFFFFFFFF
	return fmt.Sprintf("%04x:%02x:%02x.%x", domain, bus, device, function)
}

// GetGpuDeviceBdf 读取 GPU 的 PCI 总线、设备和功能号。
func (l Device) GetGpuDeviceBdf() (Bdf, Return) {
	if l.so.Lookup("amdsmi_get_gpu_device_bdf") != nil {
		return Bdf{}, STATUS_FUNCTION_NOT_FOUND
	}

	var bdf Bdf
	ret := amdsmiGetGpuDeviceBdf(l.handle, &bdf)
	return bdf, ret
}

// GetGpuMetricsInfo 用于获取指定 GPU 设备的性能指标。
func (l Device) GetGpuMetricsInfo() (GpuMetrics, Return) {
	if l.so.Lookup("amdsmi_get_gpu_metrics_info") != nil {
		return GpuMetrics{}, STATUS_FUNCTION_NOT_FOUND
	}

	var metrics GpuMetrics
	ret := amdsmiGetGpuMetricsInfo(l.handle, &metrics)
	return metrics, ret
}

// GetGpuVramUsage 读取 GPU 的显存使用信息。
func (l Device) GetGpuVramUsage() (VramUsage, Return) {
	if l.so.Lookup("amdsmi_get_gpu_vram_usage") != nil {
		return VramUsage{}, STATUS_FUNCTION_NOT_FOUND
	}

	var vramUsage VramUsage
	ret := amdsmiGetGpuVramUsage(l.handle, &vramUsage)
	return vramUsage, ret
}

// GetGpuEccCount 读取指定 GPU 硬件模块的 ECC 错误计数。
func (l Device) GetGpuEccCount(block GpuBlock) (ErrorCount, Return) {
	if l.so.Lookup("amdsmi_get_gpu_ecc_count") != nil {
		return ErrorCount{}, STATUS_FUNCTION_NOT_FOUND
	}

	var eccCount ErrorCount
	ret := amdsmiGetGpuEccCount(l.handle, block, &eccCount)
	return eccCount, ret
}

// GetPowerInfo 用于获取指定 GPU 设备的电源信息。
func (l Device) GetPowerInfo() (PowerInfo, Return) {
	if l.so.Lookup("amdsmi_get_power_info") != nil {
		return PowerInfo{}, STATUS_FUNCTION_NOT_FOUND
	}

	var powerInfo PowerInfo
	ret := amdsmiGetPowerInfo(l.handle, &powerInfo)
	return powerInfo, ret
}
