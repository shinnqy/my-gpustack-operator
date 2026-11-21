package cndev

import "C"
import (
	"fmt"
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
