package mxsml

import (
	"C"
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

// GetTemperatureInfo 用于获取指定传感器的设备温度信息。
func (l Device) GetTemperatureInfo(sensors TemperatureSensors) (int32, Return) {
	if l.so.Lookup("mxSmlGetTemperatureInfo") != nil {
		return 0, FunctionNotFound
	}

	var temp int32
	ret := mxSmlGetTemperatureInfo(l.index, sensors, &temp)
	return temp, ret
}

// GetBoardPowerInfo 会获取设备的每种电源方式的电源信息。
func (l Device) GetBoardPowerInfo() ([]BoardWayElectricInfo, Return) {
	if l.so.Lookup("mxSmlGetBoardPowerInfo") != nil {
		return nil, FunctionNotFound
	}

	var count uint32
	info := make([]BoardWayElectricInfo, 1)
	ret := mxSmlGetBoardPowerInfo(l.index, &count, &info[0])
	if ret == InsufficientSize {
		info = make([]BoardWayElectricInfo, count)
		ret = mxSmlGetBoardPowerInfo(l.index, &count, &info[0])
		return info[:count], ret
	}
	return nil, ret
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

// GetIpUsage 用于获取设备指定 IP 块的使用信息。
func (l Device) GetIpUsage(ip UsageIp) (int32, Return) {
	if l.so.Lookup("mxSmlGetDeviceIpUsage") != nil {
		return 0, FunctionNotFound
	}

	var usage int32
	ret := mxSmlGetDeviceIpUsage(l.index, ip, &usage)
	return usage, ret
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

// maxProcessQueryAttempts 限制了计数后填充的读取操作。进程数量可以在计数值与填充值之间变化，因此当填充达
// 到上限时，会以更大的大小重新尝试。这个限制决定了重试的终止条件，当达到限制时，会报告不足情况，而非上次
// 填充的缓冲区：将被截断的列表当作完整的列表处理，会将存在的进程转化为不存在的进程。
const maxProcessQueryAttempts = 3

// maxProcessRows 限制了每个进程读取会分配的行数。该数量来自库文件，如果库的 ABI 与生成该库的头文件不匹配—
// —或者该库本身已损坏——则可能会报告出与任何进程列表无关的数量。若进行分配，整个进程将在尝试限制机制生效之
// 前崩溃。实际上没有任何节点能容纳如此多的行：在单个加速器上就有数千个进程，这远远超过了单个节点自身的进
// 程表所能容纳的数量。超过上限的数量将被视为无法完成的读取操作，就像库中的缓冲区容量超出限制一样。
const maxProcessRows uint32 = 4096

// sgpuDisabled 是当设备不在 sgpu 模式下时，库在 MxSml.h 中写入的 sgpu ID。旧版进程查询布局根本不包含 sgpu
// ID，因此它们的行中会显示此值，而不是 sgpu 0——后者才是真正的切片。
const sgpuDisabled int32 = -1

// processFiller 用某个设备的进程填充行（这些行永远不会为空），并通过记录写入的行数来报告。
type processFiller func(count *uint32, rows []ProcessInfo_v3) Return

// GetProcessInfo 会返回该设备上运行的每个进程的一行数据，每行包含该进程由库报告出的每个 GPU 相关条目——库
// 使用自己的单位来显示 GpuMemoryUsage，并以 GpuNumber 表示其填充的条目数量。此处没有进行任何转换或合并操
// 作。
//
// Pids 是宿主 Pids：容器化进程以宿主看到的 pid 命名，而非其自身看到的 pid，因此这里不会发生任何转换。
//
// 它使用设备级别的查询方式，而非整个主机级别的查询方式。主机级别的查询仅将缓冲区大小作为输入参数，不会返
// 回任何结果，因此无法区分完整的缓冲区与截断读取。设备级别的查询会报告其写入的行数，这正是使得截断读取可
// 被识别并能够被报告的原因。
//
// 超出库缓冲区容量的缓冲区永远不会以列表形式返回。一个写入了给定行数的填充操作可能还有更多内容需要写入，
// 因此会尝试更大的填充量。在多次重试过程中不断填充所有缓冲区的列表会被报告为尺寸不足，而不是被缩短。
func (l Device) GetProcessInfo() ([]ProcessInfo_v3, Return) {
	if l.so.Lookup("mxSmlGetNumberOfProcess") != nil {
		return nil, FunctionNotFound
	}
	fill := l.processFiller()
	if fill == nil {
		return nil, FunctionNotFound
	}

	// 该库会统计使用任何 GPU 的进程数量，这也是此情况的上限。
	var count uint32
	if ret := mxSmlGetNumberOfProcess(&count); !ret.IsSuccess() {
		return nil, ret
	}

	for range maxProcessQueryAttempts {
		if count == 0 {
			return nil, Success
		}
		// 拒绝计数，进程列表可以包含其他内容而非分配它。
		if count > maxProcessRows {
			return nil, InsufficientSize
		}

		// 超过所要求的数量的松弛行数：如果库库填满了所分配的每一行，可能还有更多内容需要写入，这
		// 是该查询唯一的截断信号。
		size := count + 1
		rows := make([]ProcessInfo_v3, size)
		written := size
		ret := fill(&written, rows)
		switch {
		case ret == InsufficientSize || written >= size:
			// 尝试更大的值。为了实现重试的进展，增长幅度必须严格，因此那些不报告我们刚刚尝试
			// 过的数值的库就超出了范围。
			if written > count {
				count = written
			} else {
				count++
			}
		case !ret.IsSuccess():
			return nil, ret
		default:
			return rows[:written], ret
		}
	}

	return nil, InsufficientSize
}

// processFiller 返回设备当前布局下安装库所包含的过程查询的填充内容。如果未包含内容，则返回 nil。较旧的布
// 局会被读取到各自的缓冲区中，并逐字段进行复制，因此缺少最新符号的驱动程序会出现性能下降而非故障，而较旧
// 布局中未包含的字段会被报告为不适用，而非零值。
func (l Device) processFiller() processFiller {
	switch {
	case l.so.Lookup("mxSmlGetSingleGpuProcess_v3") == nil:
		return func(count *uint32, rows []ProcessInfo_v3) Return {
			return mxSmlGetSingleGpuProcess_v3(l.index, count, &rows[0])
		}
	case l.so.Lookup("mxSmlGetSingleGpuProcess_v2") == nil:
		return func(count *uint32, rows []ProcessInfo_v3) Return {
			buf := make([]ProcessInfo_v2, len(rows))
			ret := mxSmlGetSingleGpuProcess_v2(l.index, count, &buf[0])
			if !ret.IsSuccess() {
				return ret
			}
			for i := range buf[:min(int(*count), len(buf))] {
				rows[i] = ProcessInfo_v3{
					ProcessId:   buf[i].ProcessId,
					ProcessName: buf[i].ProcessName,
					GpuNumber:   buf[i].GpuNumber,
				}
				for j := range buf[i].ProcessGpuInfo {
					rows[i].ProcessGpuInfo[j] = ProcessGpuInfo_v3{
						BdfId:          buf[i].ProcessGpuInfo[j].BdfId,
						GpuId:          buf[i].ProcessGpuInfo[j].GpuId,
						GpuMemoryUsage: buf[i].ProcessGpuInfo[j].GpuMemoryUsage,
						DieId:          buf[i].ProcessGpuInfo[j].DieId,
						SgpuId:         sgpuDisabled,
					}
				}
			}
			return ret
		}
	case l.so.Lookup("mxSmlGetSingleGpuProcess") == nil:
		return func(count *uint32, rows []ProcessInfo_v3) Return {
			buf := make([]ProcessInfo, len(rows))
			ret := mxSmlGetSingleGpuProcess(l.index, count, &buf[0])
			if !ret.IsSuccess() {
				return ret
			}
			for i := range buf[:min(int(*count), len(buf))] {
				rows[i] = ProcessInfo_v3{
					ProcessId:   buf[i].ProcessId,
					ProcessName: buf[i].ProcessName,
					GpuNumber:   buf[i].GpuNumber,
				}
				for j := range buf[i].ProcessGpuInfo {
					// 这种布局也不包含芯片 ID，且对于不适用的芯片，库中也没有对应的值，
					// 因此对于如此老旧的驱动程序来说，它保持为零。
					rows[i].ProcessGpuInfo[j] = ProcessGpuInfo_v3{
						BdfId:          buf[i].ProcessGpuInfo[j].BdfId,
						GpuId:          buf[i].ProcessGpuInfo[j].GpuId,
						GpuMemoryUsage: buf[i].ProcessGpuInfo[j].GpuMemoryUsage,
						SgpuId:         sgpuDisabled,
					}
				}
			}
			return ret
		}
	}
	return nil
}
