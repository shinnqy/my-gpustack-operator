package rsmi

import (
	"fmt"
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
	//  # BDFID = ((DOMAIN & 0xFFFFFFFF) << 32) | ((Partition & 0xF) << 28)
	//    #         | ((BUS & 0xFF) << 8) | ((DEVICE & 0x1F) <<3 )
	//    #         | (FUNCTION & 0x7)
	domain := (id >> 32) & 0xFFFFFFFF
	bus := (id >> 8) & 0xFF
	deviceId := (id >> 3) & 0x1F
	function := id & 0x7
	return fmt.Sprintf("%04x:%02x:%02x.%x", domain, bus, deviceId, function), ret
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

// GetEccCount 读取指定 GPU 硬件模块的 ECC 错误计数。
func (l Device) GetEccCount(block GpuBlock) (ErrorCount, Return) {
	if l.so.Lookup("rsmi_dev_ecc_count_get") != nil {
		return ErrorCount{}, STATUS_FUNCTION_NOT_FOUND
	}

	var count ErrorCount
	ret := rsmiDevEccCountGet(l.index, block, &count)
	return count, ret
}
