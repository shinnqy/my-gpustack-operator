package rsmi

import (
	"fmt"
	"slices"
	"strconv"

	"gpustack.ai/gpustack/binding"
)

// GetDeviceCount 读取可用设备数。
func (l *RSMI) GetDeviceCount() (int, Return) {
	if l.so.Lookup("rsmi_num_monitor_devices") != nil {
		return 0, STATUS_FUNCTION_NOT_FOUND
	}

	var count uint32
	ret := rsmiNumMonitorDevices(&count)
	return int(count), ret
}

// GetDeviceHandleByIndex 按枚举索引取得设备句柄。
func (l *RSMI) GetDeviceHandleByIndex(index int) Device {
	return Device{index: uint32(index), so: l.so}
}

type Device struct {
	index uint32
	so    binding.Library
}

// GetUniqueId 读取设备的唯一标识。
func (l Device) GetUniqueId() (string, Return) {
	if l.so.Lookup("rsmi_dev_unique_id_get") != nil {
		return "", STATUS_FUNCTION_NOT_FOUND
	}

	var id uint64
	ret := rsmiDevUniqueIdGet(l.index, &id)
	if !ret.IsSuccess() {
		return "", ret
	}
	return "GPU-" + strconv.FormatUint(id, 16), ret
}

const MAX_NAME_LENGTH = 256

// GetName 获取设备名称。
func (l Device) GetName() (string, Return) {
	if l.so.Lookup("rsmi_dev_name_get") != nil {
		return "", STATUS_FUNCTION_NOT_FOUND
	}

	name := make([]byte, MAX_NAME_LENGTH)
	ret := rsmiDevNameGet(l.index, &name[0], MAX_NAME_LENGTH)
	return string(name[:clen(name)]), ret
}

// GetTargetGraphicsVersion 读取目标图形架构版本。
func (l Device) GetTargetGraphicsVersion() (string, Return) {
	if l.so.Lookup("rsmi_dev_target_graphics_version_get") != nil {
		return "", STATUS_FUNCTION_NOT_FOUND
	}

	var version uint64
	ret := rsmiDevTargetGraphicsVersionGet(l.index, &version)
	if !ret.IsSuccess() {
		return "", ret
	}
	if version < 2000 {
		return fmt.Sprintf("gfx%d", version), ret
	}
	return fmt.Sprintf("gfx%x", version), ret
}

// GetPciId 读取 PCI 标识，格式化为包含域的 domain:bus:device.function 地址。
func (l Device) GetPciId() (string, Return) {
	if l.so.Lookup("rsmi_dev_pci_id_get") != nil {
		return "", STATUS_FUNCTION_NOT_FOUND
	}

	var id uint64
	ret := rsmiDevPciIdGet(l.index, &id)
	if !ret.IsSuccess() {
		return "", ret
	}
	// 按厂商的位布局还原完整 BDF。
	// # BDFID = ((DOMAIN & 0xFFFFFFFF) << 32) | ((Partition & 0xF) << 28) # | ((BUS & 0xFF) << 8) | (
	// (DEVICE & 0x1F) <<3 ) # | (FUNCTION & 0x7)
	domain := (id >> 32) & 0xFFFFFFFF
	bus := (id >> 8) & 0xFF
	deviceId := (id >> 3) & 0x1F
	function := id & 0x7
	return fmt.Sprintf("%04x:%02x:%02x.%x", domain, bus, deviceId, function), ret
}

// GetBusyPercent 用于获取设备的忙碌百分比。
func (l Device) GetBusyPercent() (uint32, Return) {
	if l.so.Lookup("rsmi_dev_busy_percent_get") != nil {
		return 0, STATUS_FUNCTION_NOT_FOUND
	}

	var busy uint32
	ret := rsmiDevBusyPercentGet(l.index, &busy)
	return busy, ret
}

const MAX_TEMP_SENSORS = 8

// GetTempMetric 用于获取设备的指定温度指标。它会遍历所有可用的温度传感器，并返回第一个成功的指定指标读数。
// 如果没有任何传感器提供成功的读数，则返回“未找到”状态。
func (l Device) GetTempMetric(metric TemperatureMetric) (int64, Return) {
	if l.so.Lookup("rsmi_dev_temp_metric_get") != nil {
		return 0, STATUS_FUNCTION_NOT_FOUND
	}

	for i := uint32(0); i < MAX_TEMP_SENSORS; i++ {
		var temp int64
		ret := rsmiDevTempMetricGet(l.index, i, metric, &temp)
		if ret.IsSuccess() {
			return temp, ret
		}
	}
	return 0, STATUS_NOT_FOUND
}

// GetMemoryTotal 查询指定内存类型的容量。
func (l Device) GetMemoryTotal(memType MemoryType) (uint64, Return) {
	if l.so.Lookup("rsmi_dev_memory_total_get") != nil {
		return 0, STATUS_FUNCTION_NOT_FOUND
	}

	var total uint64
	ret := rsmiDevMemoryTotalGet(l.index, memType, &total)
	return total, ret
}

// GetMemoryUsage 用于获取指定内存类型下设备的已使用内存。
func (l Device) GetMemoryUsage(memType MemoryType) (uint64, Return) {
	if l.so.Lookup("rsmi_dev_memory_usage_get") != nil {
		return 0, STATUS_FUNCTION_NOT_FOUND
	}

	var used uint64
	ret := rsmiDevMemoryUsageGet(l.index, memType, &used)
	return used, ret
}

// GetEccCount 读取指定 GPU 硬件模块的 ECC 错误计数。
func (l Device) GetEccCount(block GpuBlock) (ErrorCount, Return) {
	if l.so.Lookup("rsmi_dev_ecc_count_get") != nil {
		return ErrorCount{}, STATUS_FUNCTION_NOT_FOUND
	}

	var count ErrorCount
	ret := rsmiDevEccCountGet(l.index, block, &count)
	return count, ret
}

// GetPower 用于获取设备对于平均功耗类型的当前功耗。
func (l Device) GetPower() (uint64, Return) {
	if l.so.Lookup("rsmi_dev_power_get") == nil {
		var (
			power     uint64
			powerType = AVERAGE_POWER
		)
		ret := rsmiDevPowerGet(l.index, &power, &powerType)
		if ret.IsSuccess() {
			return power, ret
		}
	}
	if l.so.Lookup("rsmi_dev_power_ave_get") == nil {
		var power uint64
		ret := rsmiDevPowerAveGet(l.index, 0, &power)
		return power, ret
	}
	return 0, STATUS_FUNCTION_NOT_FOUND
}

// maxProcessQueryAttempts 限制了计数填充式读取操作。当进程不断出现和消失时，库所报告的行数会在探针阶段和
// 填充阶段之间变化，因此如果填充结果不足，系统会按照库所要求的规模重新尝试。这个限制值决定了重试的终止条
// 件，当该限制被耗尽时，系统会报告不足情况，而非上次填充的缓冲区：将被截断的列表当作完整的列表处理，会将
// 存在的进程转化为不存在的进程。
const maxProcessQueryAttempts = 3

// maxProcessRows 限制了每个进程读取会分配的行数。该数量来自库文件，如果库的 ABI 与生成该库的头文件不匹配—
// —或者该库本身已损坏——则可能会报告出与任何进程列表无关的数量。若进行分配，整个进程将在尝试限制机制生效之
// 前崩溃。实际上没有任何节点能容纳如此多的行：在单个加速器上就有数千个进程，这远远超过了单个节点自身的进
// 程表所能容纳的数量。超过上限的数量将被视为无法完成的读取操作，就像库中的缓冲区容量超出限制一样。
const maxProcessRows uint32 = 4096

// readProcessRows 会执行逐进程查询的“先统计再填充”协议，直至完成，仅返回完整的列表。填充操作会在没有行需
// 要统计时调用，此时使用指定大小的缓冲区进行填充，并通过统计结果报告库已写入或当前需要的行数。
//
// 返回的行数据与库中的定义完全一致，且使用库自己的单位进行表示。
func readProcessRows[T any](fill func(count *uint32, rows []T) Return) ([]T, Return) {
	// 没有缓冲区的探针可以显示库中包含的行数。
	var count uint32
	if ret := fill(&count, nil); !ret.IsSuccess() && ret != STATUS_INSUFFICIENT_SIZE {
		return nil, ret
	}

	for range maxProcessQueryAttempts {
		if count == 0 {
			return nil, STATUS_SUCCESS
		}
		// 拒绝计数，进程列表可以包含其他内容而非分配它。
		if count > maxProcessRows {
			return nil, STATUS_INSUFFICIENT_SIZE
		}

		rows := make([]T, count)
		written := count
		ret := fill(&written, rows)
		switch {
		case ret == STATUS_INSUFFICIENT_SIZE:
			// 列表在探针和填充之间增长。以该库当前要求的尺寸进行重试。增长必须严格控制，才能
			// 促使重试产生进展，因此如果库重复我们刚才尝试的尺寸，则会被跳过，而不会再次尝试。
			if written > count {
				count = written
			} else {
				count++
			}
		case !ret.IsSuccess():
			return nil, ret
		default:
			// 请遵循我们设定的缓冲区大小，这样一个报告写入行数超过其接收量的库就不会引发范围
			// 超出 panic。
			if written > count {
				written = count
			}
			return rows[:written], ret
		}
	}

	return nil, STATUS_INSUFFICIENT_SIZE
}

// kfdStatsInvalid 与 rocm_smi.h 中定义的 KFD_STATS_INVALID 相同：rsmi_process_info_t 类型的 cu_occupancy
// 字段所代表的 GFX 版本并未提供库用于测量的 debugfs 方法。该值未被生成到 const.go 文件中，因为头文件将其
// 标注在结构体字段旁边，而非库中的编号常量之中。
const kfdStatsInvalid = 0xFFFFFFFF

// CuOccupancyAvailable 报告 Cu_occupancy 是否包含百分比值。
//
// 当 GFX 版本未提供用于测量计算单元占用率的 debugfs 方法时，RSMI 会将 kfdStatsInvalid 写入该字段。这是一
// 个标记值，而非数字：该字段为无符号类型，因此作为数字读取时，它代表最大的可表示百分比值。若调用者对其进
// 行比较或平均处理，就会将无法测量的进程误判为完全占用的状态。
func (p ProcessInfo) CuOccupancyAvailable() bool {
	return p.Cu_occupancy != kfdStatsInvalid
}

// GetComputeProcessInfo 返回该设备上正在运行的每个计算进程的数据，以库自身的单位为准：Vram_usage 和
// Sdma_usage 是库所报告的值。Cu_occupancy 则是计算单元使用率的百分比，这一表述与标题中的描述一致——指的是
// 比例，而非单位数量。关于当 GFX 版本无法测量该值时的默认值 CuOccupancyAvailable，请参考相关说明。此处不
// 会进行任何转换。
//
// Pids 是宿主 Pids：容器化进程以宿主看到的 pid 命名，而非其自身看到的 pid，因此这里不会发生任何转换。
//
// 这需要三个查询，因为该库没有针对每个设备的进程列表。第一个查询会列出主机上所有的计算进程。然后对每个进
// 程 ID 进行询问，了解它使用的是哪些设备索引。只有那些答案中包含该设备索引的进程 ID，才会被进一步查询其在
// 该设备上的运行数据。必须通过这种方式来确定设备归属关系：针对每个设备的查询不会为那些根本不在该设备上的
// 进程 ID 返回任何结果，因此其答案无法用于判断该进程 ID 是否属于该设备。如果将其视为属于该设备，则会导致
// 主机上所有计算进程都被归因于该设备上的每一个设备。
//
// 结果要么是完整的，要么是错误，绝不会是部分结果：一个空的列表表示该设备没有计算进程。任何不属于该库或无
// 法处理的 pid 都会导致整个调用失败，而不是从列表中被移除。在枚举过程中被排除的 pid 也会导致调用失败，而
// 调用者后续的样本处理机制会修复这一问题——一个未知数量的缺失项的列表无法与完整的列表相区分。
func (l Device) GetComputeProcessInfo() ([]ProcessInfo, Return) {
	if l.so.Lookup("rsmi_compute_process_info_get") != nil ||
		l.so.Lookup("rsmi_compute_process_gpus_get") != nil ||
		l.so.Lookup("rsmi_compute_process_info_by_device_get") != nil {
		return nil, STATUS_FUNCTION_NOT_FOUND
	}

	hostProcs, ret := readProcessRows(func(count *uint32, rows []ProcessInfo) Return {
		if len(rows) == 0 {
			return rsmiComputeProcessInfoGet(nil, count)
		}
		return rsmiComputeProcessInfoGet(&rows[0], count)
	})
	if !ret.IsSuccess() {
		return nil, ret
	}

	var rows []ProcessInfo
	for i := range hostProcs {
		pid := hostProcs[i].Process_id

		// 此 PID 使用的设备。采用与进程列表相同的“计数-再填充”协议，因此在读取过程中生成的列表会
		// 重新尝试生成，而非仅读取一半。
		indices, ret := readProcessRows(func(count *uint32, rows []uint32) Return {
			if len(rows) == 0 {
				return rsmiComputeProcessGpusGet(pid, nil, count)
			}
			return rsmiComputeProcessGpusGet(pid, &rows[0], count)
		})
		if !ret.IsSuccess() {
			return nil, ret
		}
		if !slices.Contains(indices, l.index) {
			continue
		}

		var proc ProcessInfo
		if ret := rsmiComputeProcessInfoByDeviceGet(pid, l.index, &proc); !ret.IsSuccess() {
			return nil, ret
		}
		rows = append(rows, proc)
	}
	return rows, STATUS_SUCCESS
}
