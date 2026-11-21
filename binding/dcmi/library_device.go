package dcmi

import "C"
import (
	"fmt"
	"strconv"
	"strings"
	"unsafe"
)

// GetCardList 同时返回数量和编号。V1 先枚举卡再枚举卡内设备。V2 直接枚举逻辑设备。
// 绑定层把 V2 的逻辑设备 N 表示为一张只含索引 0 的卡 N。
// 使用数量切片前检查其范围，负数或超过缓冲区长度时返回 ERROR_LIST_TRUNCATED，避免把不完整清单报告为成功。
func (l *DCMI) GetCardList() (int32, []int32, Return) {
	var (
		num  int32
		ret  Return
		list = make([]int32, MAX_CARD_NUM)
	)

	if apiVersion() == APIVersionV2 {
		ret = Return(dcmiv2GetDeviceList(&list[0], &num, int32(len(list))))
	} else {
		ret = Return(dcmiGetCardList(&num, &list[0], int32(len(list))))
	}
	if !ret.IsSuccess() {
		return 0, nil, ret
	}
	if num < 0 || num > int32(len(list)) {
		return 0, nil, ERROR_LIST_TRUNCATED
	}

	return num, list[:num], SUCCESS
}

// GetDeviceNumInCard 查询卡内设备数。V2 中每个有效逻辑设备映射为一张含一个设备的卡，
// 先用轻量查询确认卡号仍有效。
func (l *DCMI) GetDeviceNumInCard(cardId int32) (int32, Return) {
	if apiVersion() == APIVersionV2 {
		var deviceType UnitType
		if ret := Return(dcmiv2GetDeviceType(cardId, &deviceType)); !ret.IsSuccess() {
			return 0, ret
		}

		return 1, SUCCESS
	}

	var deviceNum int32
	ret := Return(dcmiGetDeviceNumInCard(cardId, &deviceNum))

	return deviceNum, ret
}

// GetDeviceHandleByCardAndIndex 以卡号和卡内索引创建 DCMI 设备句柄。
func (l *DCMI) GetDeviceHandleByCardAndIndex(cardId, deviceId int32) Device {
	return Device{cardId: cardId, deviceId: deviceId}
}

type Device struct {
	cardId   int32
	deviceId int32
}

// devID 将句柄还原为 V2 的逻辑设备编号。只有 (N, 0) 来自 V2 枚举，其他索引必须拒绝，
// 避免把一张卡的测量结果归给另一张卡。
func (l Device) devID() (int32, Return) {
	if l.deviceId != 0 {
		return 0, ERROR_INVALID_DEVICE_ID
	}

	return l.cardId, SUCCESS
}

// deviceInfo 选择当前代际的通用设备信息接口，让组合查询复用同一处分支。
func (l Device) deviceInfo(mainCmd MainCmd, subCmd uint32, buf unsafe.Pointer, size *uint32) Return {
	if apiVersion() == APIVersionV2 {
		devId, ret := l.devID()
		if !ret.IsSuccess() {
			return ret
		}

		return Return(dcmiv2GetDeviceInfo(devId, mainCmd, subCmd, buf, size))
	}

	return Return(dcmiGetDeviceInfo(l.cardId, l.deviceId, mainCmd, subCmd, buf, size))
}

// GetType 查询设备类型，例如 NPU、MCU 或 CPU。
func (l Device) GetType() (UnitType, Return) {
	var deviceType UnitType

	if apiVersion() == APIVersionV2 {
		devId, ret := l.devID()
		if !ret.IsSuccess() {
			return deviceType, ret
		}
		ret = Return(dcmiv2GetDeviceType(devId, &deviceType))

		return deviceType, ret
	}

	ret := Return(dcmiGetDeviceType(l.cardId, l.deviceId, &deviceType))

	return deviceType, ret
}

// GetChipInfoV 返回芯片信息的版本化查询接口。
func (l Device) GetChipInfoV() ChipInfoHandler {
	return ChipInfoHandler(l)
}

type ChipInfoHandler Device

// V1 读取旧版较小的芯片信息结构。调用方已经先试过 V2，无需在此重复发起同一次查询。
func (l ChipInfoHandler) V1() (ChipInfoV2, Return) {
	var info ChipInfo
	ret := Return(dcmiGetDeviceChipInfo(l.cardId, l.deviceId, &info))
	return ChipInfoV2{
		Chip_type:  info.Chip_type,
		Chip_name:  info.Chip_name,
		Chip_ver:   info.Chip_ver,
		Aicore_cnt: info.Aicore_cnt,
	}, ret
}

func (info ChipInfoV2) GetChipName() string {
	return C.GoString((*C.char)(unsafe.Pointer(&info.Chip_name[0])))
}

func (l ChipInfoHandler) V2() (ChipInfoV2, Return) {
	var info ChipInfoV2

	if apiVersion() == APIVersionV2 {
		devId, ret := Device(l).devID()
		if !ret.IsSuccess() {
			return info, ret
		}
		ret = Return(dcmiv2GetDeviceChipInfo(devId, &info))

		return info, ret
	}

	ret := Return(dcmiGetDeviceChipInfoV2(l.cardId, l.deviceId, &info))

	return info, ret
}

// GetVDieV 返回芯片裸片标识的版本化查询接口。
func (l Device) GetVDieV() VDieHandler {
	return VDieHandler(l)
}

type VDieHandler Device

func (dieId DieId) String() string {
	var parts []string
	for i := 0; i < len(dieId.Die); i++ {
		parts = append(parts, strings.ToUpper(strconv.FormatUint(uint64(dieId.Die[i]), 16)))
	}
	return strings.Join(parts, " ")
}

// V1 通过旧接口读取 SoC 的裸片标识。调用方已优先尝试过 V2，回退时只调用旧入口。
func (l VDieHandler) V1() (DieId, Return) {
	var socDie SocDie
	ret := Return(dcmiGetDeviceDie(l.cardId, l.deviceId, &socDie))
	return DieId(socDie), ret
}

// V2 优先读虚拟裸片的 ID，失败时再读厂商用作 A5 UUID 的 DDie ID。两个查询都使用 V2。
// 旧驱动仍由调用方回退到 V1。
func (l VDieHandler) V2() (DieId, Return) {
	var dieId DieId

	if apiVersion() == APIVersionV2 {
		devId, ret := Device(l).devID()
		if !ret.IsSuccess() {
			return dieId, ret
		}

		ret = Return(dcmiv2GetDeviceDieId(devId, VDIE, &dieId))
		if !ret.IsSuccess() {
			ret = Return(dcmiv2GetDeviceDieId(devId, DDIE, &dieId))
		}

		return dieId, ret
	}

	ret := Return(dcmiGetDeviceDieV2(l.cardId, l.deviceId, VDIE, &dieId))

	return dieId, ret
}

// GetHealth 返回设备健康状态。0 表示正常，1、2、3 分别表示轻微、严重和危急告警，0xFFFFFFFF 表示找不到设备。
func (l Device) GetHealth() (uint32, Return) {
	var health uint32

	if apiVersion() == APIVersionV2 {
		devId, ret := l.devID()
		if !ret.IsSuccess() {
			return 0, ret
		}
		ret = Return(dcmiv2GetDeviceHealth(devId, &health))

		return health, ret
	}

	ret := Return(dcmiGetDeviceHealth(l.cardId, l.deviceId, &health))

	return health, ret
}

// GetPhysicalID 返回物理编号。V1 先从句柄查逻辑编号，再映射为物理编号。V2 的设备索引本身就是逻辑编号，
// 只需做后一步映射。
func (l Device) GetPhysicalID() (uint32, Return) {
	var (
		logId int32
		phyId uint32
	)

	if apiVersion() == APIVersionV2 {
		devId, ret := l.devID()
		if !ret.IsSuccess() {
			return 0, ret
		}
		ret = Return(dcmiv2GetChipPhyIdByDevId(uint32(devId), &phyId))

		return phyId, ret
	}

	ret := Return(dcmiGetDeviceLogicId(&logId, l.cardId, l.deviceId))
	if ret.IsSuccess() {
		ret = Return(dcmiGetDevicePhyidFromLogicid(uint32(logId), &phyId))
	}

	return phyId, ret
}

// GetPcieInfoV 提供 PCIe 信息的版本化查询接口。
func (l Device) GetPcieInfoV() PcieInfoHandler {
	return PcieInfoHandler(l)
}

type PcieInfoHandler Device

func (info PcieInfoAll) GetBusId() string {
	return fmt.Sprintf("%04x:%02x:%02x.%01x", info.Domain, info.Bdf_busid, info.Bdf_deviceid, info.Bdf_funcid)
}

// V1 读取较小的旧版 PCIe 结构。调用方已经先尝试过 V2，回退时只使用 V1 入口。
func (l PcieInfoHandler) V1() (PcieInfoAll, Return) {
	var info PcieInfo
	ret := Return(dcmiGetDevicePcieInfo(l.cardId, l.deviceId, &info))
	return PcieInfoAll{
		Venderid:     info.Venderid,
		Subvenderid:  info.Subvenderid,
		Deviceid:     info.Deviceid,
		Subdeviceid:  info.Subdeviceid,
		Bdf_busid:    info.Bdf_busid,
		Bdf_deviceid: info.Bdf_deviceid,
		Bdf_funcid:   info.Bdf_funcid,
	}, ret
}

func (l PcieInfoHandler) V2() (PcieInfoAll, Return) {
	var info PcieInfoAll

	if apiVersion() == APIVersionV2 {
		devId, ret := Device(l).devID()
		if !ret.IsSuccess() {
			return info, ret
		}
		ret = Return(dcmiv2GetDevicePcieInfo(devId, &info))

		return info, ret
	}

	ret := Return(dcmiGetDevicePcieInfoV2(l.cardId, l.deviceId, &info))

	return info, ret
}

// GetHbmInfo 读取设备的高带宽内存 HBM 信息。
func (l Device) GetHbmInfo() (HbmInfo, Return) {
	var info HbmInfo

	if apiVersion() == APIVersionV2 {
		devId, ret := l.devID()
		if !ret.IsSuccess() {
			return info, ret
		}
		ret = Return(dcmiv2GetDeviceHbmInfo(devId, &info))

		return info, ret
	}

	ret := Return(dcmiGetDeviceHbmInfo(l.cardId, l.deviceId, &info))

	return info, ret
}

// GetMemoryInfoV 提供设备内存信息的版本化查询接口。
func (l Device) GetMemoryInfoV() MemoryHandler {
	return MemoryHandler(l)
}

type MemoryHandler Device

// V2 方法读取 memory_info_v2 结构。第二代驱动仅支持通过 HBM 接口查询内存，调用本入口会收到驱动拒绝。
func (l MemoryHandler) V2() (GetMemoryInfo, Return) {
	var info MemoryInfo
	ret := Return(dcmiGetDeviceMemoryInfoV2(l.cardId, l.deviceId, &info))
	return GetMemoryInfo{
		Memory_size: info.Size,
		Freq:        info.Freq,
		Utiliza:     info.Utiliza,
	}, ret
}

// GetEccInfo 读取指定设备内存类型的 ECC 纠错信息。
func (l Device) GetEccInfo(deviceType DeviceType) (EccInfo, Return) {
	var info EccInfo

	if apiVersion() == APIVersionV2 {
		devId, ret := l.devID()
		if !ret.IsSuccess() {
			return info, ret
		}
		ret = Return(dcmiv2GetDeviceEccInfo(devId, deviceType, &info))

		return info, ret
	}

	ret := Return(dcmiGetDeviceEccInfo(l.cardId, l.deviceId, deviceType, &info))

	return info, ret
}

// GetSuperPodInfo 读取设备所属超节点、服务器和产品类型。A5 根据产品类型选择 HCCL 拓扑描述。
// 两代驱动共用 deviceInfo 分派，返回结构中不包含 Go 指针。
func (l Device) GetSuperPodInfo() (SpodInfo, Return) {
	var info SpodInfo
	size := uint32(unsafe.Sizeof(info))
	if ret := l.deviceInfo(MAIN_CMD_CHIP_INF, CINF_SUB_CMD_GET_SPOD_INFO, unsafe.Pointer(&info), &size); !ret.IsSuccess() {
		return SpodInfo{}, ret
	}

	return info, SUCCESS
}

// GetMainboardId 查询主板 ID，以区分 A5 芯片的单卡、四芯片载板和训练底板。只有 V2 提供该查询，
// V1 会返回符号不可用。
func (l Device) GetMainboardId() (uint32, Return) {
	devId, ret := l.devID()
	if !ret.IsSuccess() {
		return 0, ret
	}

	var mainboardId uint32
	ret = Return(dcmiv2GetMainboardId(devId, &mainboardId))

	return mainboardId, ret
}

// eidNumMax 是每个 URMA 设备能返回的 EID 数量上限，也是 GetEidList 为驱动预留的缓冲区容量。
const eidNumMax = 32

// GetUrmaDeviceCount 返回设备上的 URMA 功能实体数量，供 GetEidList 按索引遍历。该 UB 能力仅由 V2 驱动提供。
func (l Device) GetUrmaDeviceCount() (uint32, Return) {
	devId, ret := l.devID()
	if !ret.IsSuccess() {
		return 0, ret
	}

	var count uint32
	ret = Return(dcmiv2GetUrmaDeviceCnt(devId, &count))

	return count, ret
}

// GetEidList 按 URMA 索引读取设备在 UB 网络上的 16 字节端点标识。标识内还编码了裸片、端口等信息，
// 绑定层保留原始字节。数量参数传入容量、返回实际写入条数，超出容量时拒绝读取。此接口仅适用于 V2。
func (l Device) GetEidList(devIndex uint32) ([]UrmaEidInfo, Return) {
	devId, ret := l.devID()
	if !ret.IsSuccess() {
		return nil, ret
	}

	// UrmaEidInfo 不包含 Go 指针，可以将数组地址传给驱动填充。
	var list [eidNumMax]UrmaEidInfo
	count := uint32(len(list))
	if ret := Return(dcmiv2GetEidListByUrmaDevIndex(devId, devIndex, &list[0], &count)); !ret.IsSuccess() {
		return nil, ret
	}
	if count > uint32(len(list)) {
		return nil, ERROR_INVALID_PARAMETER
	}

	return list[:count:count], SUCCESS
}

func (ipAddr IpAddr) String() string {
	if ipAddr.Ip_type == IPADDR_TYPE_V4 {
		return fmt.Sprintf("%d.%d.%d.%d", ipAddr.U_addr[0], ipAddr.U_addr[1], ipAddr.U_addr[2], ipAddr.U_addr[3])
	}
	if ipAddr.Ip_type == IPADDR_TYPE_V6 {
		var parts []string
		for i := 0; i < 16; i += 2 {
			part := uint16(ipAddr.U_addr[i])<<8 | uint16(ipAddr.U_addr[i+1])
			parts = append(parts, fmt.Sprintf("%x", part))
		}
		return strings.Join(parts, ":")
	}
	return ""
}

// GetIp 查询指定端口类型的 IP 和子网掩码。第二代驱动不提供此查询。
func (l Device) GetIp(portType PortType, portId int32) (IpAddr, IpAddr, Return) {
	var ipAddr IpAddr
	var subnetMask IpAddr
	ret := Return(dcmiGetDeviceIp(l.cardId, l.deviceId, portType, portId, &ipAddr, &subnetMask))
	return ipAddr, subnetMask, ret
}

// GetGateway 查询指定端口类型的网关。第二代驱动不提供此查询。
func (l Device) GetGateway(portType PortType, portId int32) (IpAddr, Return) {
	var gateway IpAddr
	ret := Return(dcmiGetDeviceGateway(l.cardId, l.deviceId, portType, portId, &gateway))
	return gateway, ret
}
