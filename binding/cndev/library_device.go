package cndev

import (
	"C"
	"fmt"
	"runtime"
	"strconv"
	"unsafe"

	"gpustack.ai/gpustack/binding"
)

// GetDeviceCount 返回系统中可枚举的设备数量。
func (l *CNDev) GetDeviceCount() (int, Return) {
	if l.so.Lookup("cndevGetDeviceCount") != nil {
		return 0, ERROR_FUNCTION_NOT_FOUND
	}

	var info CardInfo
	info.Version = VERSION_6
	ret := cndevGetDeviceCount(&info)
	return int(info.Number), ret
}

// GetDeviceHandleByIndex 按枚举索引取得设备句柄。
func (l *CNDev) GetDeviceHandleByIndex(index int) (Device, Return) {
	if l.so.Lookup("cndevGetDeviceHandleByIndex") != nil {
		return Device{}, ERROR_FUNCTION_NOT_FOUND
	}

	var handle cndevDevice
	ret := cndevGetDeviceHandleByIndex(int32(index), &handle)
	return Device{handle: handle, so: l.so}, ret
}

// GetDeviceHandleByPciBusId 返回具有指定 PCI 总线 ID 的设备的句柄。
func (l *CNDev) GetDeviceHandleByPciBusId(pciBusId string) (Device, Return) {
	if l.so.Lookup("cndevGetDeviceHandleByPciBusId") != nil {
		return Device{}, ERROR_FUNCTION_NOT_FOUND
	}

	var handle cndevDevice
	ret := cndevGetDeviceHandleByPciBusId(pciBusId, &handle)
	return Device{handle: handle, so: l.so}, ret
}

type Device struct {
	handle cndevDevice
	so     binding.Library
}

func (uuid UUID) String() string {
	return "MLU-" + C.GoString((*C.char)(unsafe.Pointer(&uuid.Uuid[0])))
}

// GetCardName 返回设备名称。
func (l Device) GetCardName() (string, Return) {
	if l.so.Lookup("cndevGetCardNameStringByDevId") == nil {
		return cndevGetCardNameStringByDevId(l.handle), SUCCESS
	}

	if l.so.Lookup("cndevGetCardName") == nil {
		var cardName CardName
		cardName.Version = VERSION_6
		ret := cndevGetCardName(&cardName, l.handle)
		if !ret.IsSuccess() {
			return "", ret
		}

		cardNameId := NameEnum(cardName.Id)
		if l.so.Lookup("cndevGetCardNameString") == nil {
			return cndevGetCardNameString(cardNameId), SUCCESS
		}

		switch cardNameId {
		case DEVICE_TYPE_MLU100:
			return "MLU100", ret
		case DEVICE_TYPE_MLU270:
			return "MLU270", ret
		case DEVICE_TYPE_MLU220_M2, DEVICE_TYPE_MLU220_EDGE, DEVICE_TYPE_MLU220_EVB, DEVICE_TYPE_MLU220_M2i:
			return "MLU220", ret
		case DEVICE_TYPE_MLU290:
			return "MLU290", ret
		case DEVICE_TYPE_MLU370:
			return "MLU370", ret
		case DEVICE_TYPE_MLU365:
			return "MLU365", ret
		case DEVICE_TYPE_CE3226:
			return "CE3226", ret
		case DEVICE_TYPE_MLU590:
			return "MLU590", ret
		case DEVICE_TYPE_MLU585:
			return "MLU585", ret
		case DEVICE_TYPE_MLU580:
			return "MLU580", ret
		case DEVICE_TYPE_MLU570:
			return "MLU570", ret
		}
		return "MLU", ret
	}

	return "", ERROR_FUNCTION_NOT_FOUND
}

func (sn CardSN) String() string {
	return strconv.FormatUint(sn.Sn, 16)
}

// GetUUID 返回设备的唯一标识。
func (l Device) GetUUID() (string, Return) {
	if l.so.Lookup("cndevGetUUID") != nil {
		return "", ERROR_FUNCTION_NOT_FOUND
	}

	var uuid UUID
	uuid.Version = VERSION_6
	ret := cndevGetUUID(&uuid, l.handle)
	return uuid.String(), ret
}

// GetMemoryInfoV 提供对应版本的设备内存信息读取接口。
func (l Device) GetMemoryInfoV() MemoryHandler {
	return MemoryHandler(l)
}

type MemoryHandler Device

func (l MemoryHandler) V2() (MemoryInfoV2, Return) {
	if l.so.Lookup("cndevGetMemoryUsageV2") != nil {
		return MemoryInfoV2{}, ERROR_FUNCTION_NOT_FOUND
	}

	var memInfo MemoryInfoV2
	ret := cndevGetMemoryUsageV2(&memInfo, l.handle)
	return memInfo, ret
}

// GetVersionInfo 返回设备版本信息。
func (l Device) GetVersionInfo() (VersionInfo, Return) {
	if l.so.Lookup("cndevGetVersionInfo") != nil {
		return VersionInfo{}, ERROR_FUNCTION_NOT_FOUND
	}

	var versionInfo VersionInfo
	// Version 是输入字段。CNDev 依靠它判断调用方使用的结构体布局，留为零会返回 ERROR_UNSUPPORTED_API_VERSION。
	versionInfo.Version = VERSION_6
	ret := cndevGetVersionInfo(&versionInfo, l.handle)
	return versionInfo, ret
}

// GetCardHealthStateV 提供读取不同版本健康状态的接口。
func (l Device) GetCardHealthStateV() CardHealthStateHandler {
	return CardHealthStateHandler(l)
}

type CardHealthStateHandler Device

func (l CardHealthStateHandler) V1() (CardHealthStateV2, Return) {
	if l.so.Lookup("cndevGetCardHealthState") != nil {
		return CardHealthStateV2{}, ERROR_FUNCTION_NOT_FOUND
	}

	var healthState CardHealthState
	healthState.Version = VERSION_6
	ret := cndevGetCardHealthState(&healthState, l.handle)
	return CardHealthStateV2{
		Health:      healthState.Health,
		DeviceState: healthState.DeviceState,
		DriverState: healthState.DriverState,
	}, ret
}

func (l CardHealthStateHandler) V2() (CardHealthStateV2, Return) {
	if l.so.Lookup("cndevGetCardHealthStateV2") != nil {
		return CardHealthStateV2{}, ERROR_FUNCTION_NOT_FOUND
	}

	var healthStateV2 CardHealthStateV2
	ret := cndevGetCardHealthStateV2(&healthStateV2, l.handle)
	return healthStateV2, ret
}

// GetPCIeInfoV 提供读取不同版本 PCIe 信息的接口。
func (l Device) GetPCIeInfoV() PCIeInfoHandler {
	return PCIeInfoHandler(l)
}

type PCIeInfoHandler Device

func (info PCIeInfoV2) GetBusId() string {
	return fmt.Sprintf("%04x:%02x:%02x.%d", info.Domain, info.Bus, info.Device, info.Function)
}

func (l PCIeInfoHandler) V2() (PCIeInfoV2, Return) {
	if l.so.Lookup("cndevGetPCIeInfoV2") != nil {
		return PCIeInfoV2{}, ERROR_FUNCTION_NOT_FOUND
	}

	var pcieInfo PCIeInfoV2
	ret := cndevGetPCIeInfoV2(&pcieInfo, l.handle)
	return pcieInfo, ret
}

func (l Device) GetPowerInfo() (DevicePowerInfo, Return) {
	if l.so.Lookup("cndevGetDevicePowerInfo") != nil {
		return DevicePowerInfo{}, ERROR_FUNCTION_NOT_FOUND
	}

	var powerInfo DevicePowerInfo
	ret := cndevGetDevicePowerInfo(&powerInfo, l.handle)
	return powerInfo, ret
}

func (l Device) GetTemperatureInfo() (TemperatureInfo, Return) {
	if l.so.Lookup("cndevGetTemperatureInfo") != nil {
		return TemperatureInfo{}, ERROR_FUNCTION_NOT_FOUND
	}

	var tempInfo TemperatureInfo
	ret := cndevGetTemperatureInfo(&tempInfo, l.handle)
	return tempInfo, ret
}

func (l Device) GetUtilizationInfo() (UtilizationInfo, Return) {
	if l.so.Lookup("cndevGetDeviceUtilizationInfo") != nil {
		return UtilizationInfo{}, ERROR_FUNCTION_NOT_FOUND
	}

	var utilInfo UtilizationInfo
	utilInfo.Version = VERSION_6
	ret := cndevGetDeviceUtilizationInfo(&utilInfo, l.handle)
	return utilInfo, ret
}

// GetSMLUMode 报告设备上是否启用了 sMLU 模式。
func (l Device) GetSMLUMode() (SMLUMode, Return) {
	if l.so.Lookup("cndevGetSMLUMode") != nil {
		return SMLUMode{}, ERROR_FUNCTION_NOT_FOUND
	}

	var mode SMLUMode
	mode.Version = VERSION_6
	ret := cndevGetSMLUMode(&mode, l.handle)
	return mode, ret
}

// SetSMLUMode 可以开启或关闭设备上的 sMLU 模式。
func (l Device) SetSMLUMode(enabled bool) Return {
	if l.so.Lookup("cndevSetSMLUMode") != nil {
		return ERROR_FUNCTION_NOT_FOUND
	}

	var mode SMLUMode
	mode.Version = VERSION_6
	mode.SmluMode = uint32(FEATURE_DISABLED)
	if enabled {
		mode.SmluMode = uint32(FEATURE_ENABLED)
	}
	return cndevSetSMLUMode(&mode, l.handle)
}

// CreateSMluProfile 根据给定的配额创建一个 sMLU 规格，并返回其规格 ID。
func (l Device) CreateSMluProfile(profile SMluSet) (int32, Return) {
	if l.so.Lookup("cndevCreateSMluProfileInfo") != nil {
		return 0, ERROR_FUNCTION_NOT_FOUND
	}

	profile.Version = VERSION_6
	var profileID int32
	ret := cndevCreateSMluProfileInfo(&profile, &profileID, l.handle)
	return profileID, ret
}

// DestroySMluProfile 会使用给定的规格 ID 销毁 sMLU 规格。
func (l Device) DestroySMluProfile(profileID int32) Return {
	if l.so.Lookup("cndevDestroySMluProfileInfo") != nil {
		return ERROR_FUNCTION_NOT_FOUND
	}

	return cndevDestroySMluProfileInfo(profileID, l.handle)
}

// CreateSMluInstance 根据给定的规格 ID 创建一个 sMLU 实例，并赋予其名称。在后续的每个操作中，该实例都通过
// 名称进行访问，因此不会返回句柄。
func (l Device) CreateSMluInstance(profileID uint32, name string) Return {
	if l.so.Lookup("cndevCreateSMluInstanceByProfileId") != nil {
		return ERROR_FUNCTION_NOT_FOUND
	}

	cName, cNameAllocMap := unpackPCharString(name)
	var handle cndevMluInstance
	ret := cndevCreateSMluInstanceByProfileId(&handle, profileID, l.handle, (*byte)(unsafe.Pointer(cName)))
	runtime.KeepAlive(cNameAllocMap)
	return ret
}

// DestroySMluInstanceByName 会销毁设备上名为该名称的 sMLU 实例。
func (l Device) DestroySMluInstanceByName(name string) Return {
	if l.so.Lookup("cndevDestroySMluInstanceByInstanceName") != nil {
		return ERROR_FUNCTION_NOT_FOUND
	}

	cName, cNameAllocMap := unpackPCharString(name)
	ret := cndevDestroySMluInstanceByInstanceName(l.handle, (*byte)(unsafe.Pointer(cName)))
	runtime.KeepAlive(cNameAllocMap)
	return ret
}

// GetAllSMluInstanceInfo 返回设备上每个 sMLU 实例的信息。
func (l Device) GetAllSMluInstanceInfo() ([]SMluInfo, Return) {
	if l.so.Lookup("cndevGetAllSMluInstanceInfo") != nil {
		return nil, ERROR_FUNCTION_NOT_FOUND
	}

	// count 参数为输入/输出：没有缓冲区的探针可以报告存在的实例数量，因此会预期出现
	// INSUFFICIENT_SPACE 的情况，不会被视为硬性故障。
	var count int32
	if ret := cndevGetAllSMluInstanceInfo(&count, nil, l.handle); !ret.IsSuccess() && ret != ERROR_INSUFFICIENT_SPACE {
		return nil, ret
	}
	if count <= 0 {
		return nil, SUCCESS
	}

	infos := make([]SMluInfo, count)
	for i := range infos {
		infos[i].Version = VERSION_6
	}
	ret := cndevGetAllSMluInstanceInfo(&count, &infos[0], l.handle)
	if !ret.IsSuccess() {
		return nil, ret
	}
	// 请遵循我们设定的缓冲区大小，因此报告大于输入数量的输出数量的驱动程序不会引发“切片超出范围”的恐
	// 慌。
	if int(count) > len(infos) {
		count = int32(len(infos))
	}
	return infos[:count], ret
}

// GetInstanceName 从以 NUL 结尾的 C 字符数组中获取 sMLU 实例名称。
func (info *SMluInfo) GetInstanceName() string {
	return C.GoString((*C.char)(unsafe.Pointer(&info.InstanceName[0])))
}

// GetDevNodeName 返回由 NUL 终止的 C 字符数组中的 sMLU 实例设备节点路径。
func (info *SMluInfo) GetDevNodeName() string {
	return C.GoString((*C.char)(unsafe.Pointer(&info.DevNodeName[0])))
}

// GetSMluProfileIds 返回设备上定义的每个 sMLU 规格的 `ID`。
func (l Device) GetSMluProfileIds() ([]int32, Return) {
	if l.so.Lookup("cndevGetSMluProfileIdInfo") != nil {
		return nil, ERROR_FUNCTION_NOT_FOUND
	}

	var info SMluProfileIdInfo
	info.Version = VERSION_6
	ret := cndevGetSMluProfileIdInfo(&info, l.handle)
	if !ret.IsSuccess() {
		return nil, ret
	}
	n := int(info.Count)
	if n > len(info.ProfileId) {
		n = len(info.ProfileId)
	}
	ids := make([]int32, n)
	copy(ids, info.ProfileId[:n])
	return ids, ret
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

// readProcessRows 会执行逐进程查询的“先统计再填充”协议，直至完成，仅返回完整的列表。在“填充”操作中，如果
// 没有需要统计的行，则仅调用该操作。若有指定大小的缓冲区，则用于填充数据，并通过统计结果报告库已写入或当
// 前需要的行数。由于每个 cnDev 结构体都声明了自身的布局，因此每行版本的标记属于“填充”操作的一部分，而非此
// 循环的一部分，因此每次尝试时都会重新执行“填充”操作。
//
// 返回的行数据与库中的定义完全一致，且使用库自己的单位进行表示。
func readProcessRows[T any](fill func(count *uint32, rows []T) Return) ([]T, Return) {
	// 没有缓冲区的探针可以显示库中包含多少行数据。对于零尺寸缓冲区，它会通过“短缓冲区”代码进行响应，
	// 这表明探针运行正常，而非失败。
	var count uint32
	if ret := fill(&count, nil); !ret.IsSuccess() && ret != ERROR_INSUFFICIENT_SPACE {
		return nil, ret
	}

	for range maxProcessQueryAttempts {
		if count == 0 {
			return nil, SUCCESS
		}
		// 拒绝计数，进程列表可以包含其他内容而非分配它。
		if count > maxProcessRows {
			return nil, ERROR_INSUFFICIENT_SPACE
		}

		rows := make([]T, count)
		written := count
		ret := fill(&written, rows)
		switch {
		case ret == ERROR_INSUFFICIENT_SPACE:
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

	return nil, ERROR_INSUFFICIENT_SPACE
}

// GetProcessInfo 会返回该设备上运行的每个进程对应的一行数据，其中 PhysicalMemoryUsed 和 VirtualMemoryUsed
// 所采用的单位均为 KiB，此处没有进行单位转换。
//
// Pids 是宿主 Pids：容器化进程以宿主看到的 pid 命名，而非其自身看到的 pid，因此这里不会发生任何转换。
func (l Device) GetProcessInfo() ([]ProcessInfo, Return) {
	if l.so.Lookup("cndevGetProcessInfo") != nil {
		return nil, ERROR_FUNCTION_NOT_FOUND
	}

	return readProcessRows(func(count *uint32, rows []ProcessInfo) Return {
		if len(rows) == 0 {
			return cndevGetProcessInfo(count, nil, l.handle)
		}
		// 版本是一个输入参数：cnDev 会拒绝那些未声明调用方期望的布局的版本化结构体，且缓冲区中的
		// 每一行都包含其自己的版本标识词。
		for i := range rows {
			rows[i].Version = VERSION_6
		}
		return cndevGetProcessInfo(count, &rows[0], l.handle)
	})
}

// GetProcessUtilization 返回该设备上运行的每个进程的一行数据，其中 IpuUtil、JpuUtil、VpuDecUtil、
// VpuEncUtil 和 MemUtil 为库所报告的百分比。
//
// 库中未报告的处理过程在结果中不存在，而不是处于零状态。ERROR_NOT_SUPPORTED 则被返回为自身，而不是一个空
// 列表，因为该设备根本无法支持按进程进行的利用。
func (l Device) GetProcessUtilization() ([]ProcessUtilization, Return) {
	if l.so.Lookup("cndevGetProcessUtilization") != nil {
		return nil, ERROR_FUNCTION_NOT_FOUND
	}

	return readProcessRows(func(count *uint32, rows []ProcessUtilization) Return {
		if len(rows) == 0 {
			return cndevGetProcessUtilization(count, nil, l.handle)
		}
		for i := range rows {
			rows[i].Version = VERSION_6
		}
		return cndevGetProcessUtilization(count, &rows[0], l.handle)
	})
}
