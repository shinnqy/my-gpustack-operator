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

// GetTemperature 函数用于获取设备的温度信息。
func (l Device) GetTemperature() (uint32, Return) {
	if l.so.Lookup("nvmlDeviceGetTemperature") != nil {
		return 0, ERROR_FUNCTION_NOT_FOUND
	}

	var temp uint32
	ret := nvmlDeviceGetTemperature(l.handle, TEMPERATURE_GPU, &temp)
	return temp, ret
}

// GetPowerUsage 函数以毫瓦为单位获取设备的当前功耗。
func (l Device) GetPowerUsage() (uint32, Return) {
	if l.so.Lookup("nvmlDeviceGetPowerUsage") != nil {
		return 0, ERROR_FUNCTION_NOT_FOUND
	}

	var power uint32
	ret := nvmlDeviceGetPowerUsage(l.handle, &power)
	return power, ret
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

// GetUtilizationRates 用于获取设备的当前利用率，包括 GPU 和内存的利用率。
func (l Device) GetUtilizationRates() (Utilization, Return) {
	if l.so.Lookup("nvmlDeviceGetUtilizationRates") != nil {
		return Utilization{}, ERROR_FUNCTION_NOT_FOUND
	}

	var utilization Utilization
	ret := nvmlDeviceGetUtilizationRates(l.handle, &utilization)
	return utilization, ret
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

// GetComputeRunningProcesses 会返回该设备上正在运行的每个计算进程的一行数据，其中 UsedGpuMemory 以字节为
// 单位，即库所报告的数值。
//
// Pids 是宿主 Pids：容器化进程以宿主看到的 pid 命名，而非其自身看到的 pid，因此这里不会发生任何转换。
//
// Vendored 头文件仅包含这种查询的单一、未版本化的形式，因此这些行仅包含 pid 及其内存数据，其他内容则没有。
// 该库中根本不存在针对每个进程的利用率查询，因此在这个硬件上，进程级计算不仅为零，甚至无法使用。
func (l Device) GetComputeRunningProcesses() ([]ProcessInfo_v1, Return) {
	if l.so.Lookup("nvmlDeviceGetComputeRunningProcesses") != nil {
		return nil, ERROR_FUNCTION_NOT_FOUND
	}

	return readProcessRows(func(count *uint32, rows []ProcessInfo_v1) Return {
		if len(rows) == 0 {
			return nvmlDeviceGetComputeRunningProcesses(l.handle, count, nil)
		}
		return nvmlDeviceGetComputeRunningProcesses(l.handle, count, &rows[0])
	})
}
