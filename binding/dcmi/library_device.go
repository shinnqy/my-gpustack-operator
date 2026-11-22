package dcmi

import (
	"C"
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

// GetUtilizationRateV 用于获取设备各组件的利用率，并返回一个 UtilizationRateHandler 以供进一步查询。
func (l Device) GetUtilizationRateV() UtilizationRateHandler {
	return UtilizationRateHandler(l)
}

type UtilizationRateHandler Device

// V1 通过四种按类型进行的读取数据，组合出多组件比率。
//
// 两个代次都提供了按类型划分的入口点，因此此调整是合理的：在 V2 版本中，四个读取操作会指向
// dcmiv2_get_device_utilization_rate。首先尝试调用 V2() 的调用者，在任一代次下都会得到多类型划分和按类型
// 划分的结果，这与他们预期的情况一致。
func (l UtilizationRateHandler) V1() (MultiUtilizationInfo, Return) {
	var (
		info  MultiUtilizationInfo
		ret   Return
		devId int32
		onV2  = apiVersion() == APIVersionV2
	)

	if onV2 {
		if devId, ret = Device(l).devID(); !ret.IsSuccess() {
			return info, ret
		}
	}

	rate := func(inputType int32, out *uint32) Return {
		if onV2 {
			return Return(dcmiv2GetDeviceUtilizationRate(devId, inputType, out))
		}
		return Return(dcmiGetDeviceUtilizationRate(l.cardId, l.deviceId, inputType, out))
	}

	// 按与 GetVDeviceInfo 相同的原因，将数据读入本地每个组件：返回的结构体中没有任何地址会传递到 C 语
	// 言，因此安全性并不取决于该结构体当前包含什么内容。MultiUtilizationInfo 采用纯 C 语言布局，没有
	// 指针槽需要查找，因此目前没有任何成本——这样就能让文件保持单一规则而非两个。
	//
	// 每个本地变量在自身读取成功时就会被分配，而不是在最后一批中分配。在返回码处停止的调用者仍然会收
	// 到在失败前已读取的组件，这与在本地变量引入之前的情况一致。
	var aic, aiv, aicore, npu uint32
	if ret = rate(UTILIZATION_RATE_AICPU, &aic); !ret.IsSuccess() {
		return info, ret
	}
	info.Aic_util = aic
	if ret = rate(UTILIZATION_RATE_VECTORCORE, &aiv); !ret.IsSuccess() {
		return info, ret
	}
	info.Aiv_util = aiv
	if ret = rate(UTILIZATION_RATE_AICORE, &aicore); !ret.IsSuccess() {
		return info, ret
	}
	info.Aicore_util = aicore
	ret = rate(UTILIZATION_RATE_NPU, &npu)
	info.Npu_util = npu

	return info, ret
}

func (l UtilizationRateHandler) V2() (MultiUtilizationInfo, Return) {
	var info MultiUtilizationInfo

	if apiVersion() == APIVersionV2 {
		devId, ret := Device(l).devID()
		if !ret.IsSuccess() {
			return info, ret
		}
		ret = Return(dcmiv2GetDeviceMultiUtilizationRate(devId, &info))

		return info, ret
	}

	ret := Return(dcmiGetDeviceUtilizationRateV2(l.cardId, l.deviceId, &info))

	return info, ret
}

// GetTemperature 函数用于获取设备的当前温度。
func (l Device) GetTemperature() (int32, Return) {
	var temp int32

	if apiVersion() == APIVersionV2 {
		devId, ret := l.devID()
		if !ret.IsSuccess() {
			return 0, ret
		}
		ret = Return(dcmiv2GetDeviceTemperature(devId, &temp))

		return temp, ret
	}

	ret := Return(dcmiGetDeviceTemperature(l.cardId, l.deviceId, &temp))

	return temp, ret
}

// GetPowerInfo 用于获取设备的当前电源信息。
func (l Device) GetPowerInfo() (int32, Return) {
	var power int32

	if apiVersion() == APIVersionV2 {
		devId, ret := l.devID()
		if !ret.IsSuccess() {
			return 0, ret
		}
		ret = Return(dcmiv2GetDevicePowerInfo(devId, &power))

		return power, ret
	}

	ret := Return(dcmiGetDevicePowerInfo(l.cardId, l.deviceId, &power))

	return power, ret
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

// GetShareEnabled 用于报告设备是否启用了容器共享模式。在驱动程序 25.5.1 下的 910B2 设备上测试发现：当该模
// 式被禁用时，打开同一设备的第二个容器会失败，而第一个容器及整个卡片的所有用户则不受影响。目前尚未确定是
// 否所有共租配置都依赖于该模式，因此应将其视为驱动程序自身的单容器保护机制，而非完整规则。如果驱动程序未
// 实现该查询功能，将返回 ERROR_FUNCTION_NOT_FOUND。
//
// V2 根本不声明任何容器共享入口点，因此该请求会被传递，而 V2 驱动程序会拒绝它。那个代次没有用于读取的标记，
// 这与存在但被关闭的标记不同，分配器能够区分这两种情况。
func (l Device) GetShareEnabled() (bool, Return) {
	var enableFlag int32
	ret := Return(dcmiGetDeviceShareEnable(l.cardId, l.deviceId, &enableFlag))
	return enableFlag != 0, ret
}

// SetShareEnabled 用于启用或禁用设备的容器共享模式。该标志位于驱动程序中，而非本进程中，因此会持续存在直
// 到调用者结束，除非驱动程序的共享配置恢复模式也处于开启状态，否则重启后该设置将被重置。
//
// 与上面的 GetShareEnabled 函数一样，V2 没有对应的实现：没有用于写入的标记。
func (l Device) SetShareEnabled(enabled bool) Return {
	var enableFlag int32
	if enabled {
		enableFlag = 1
	}
	return Return(dcmiSetDeviceShareEnable(l.cardId, l.deviceId, enableFlag))
}

const (
	// maxProcMemInfoRows 表示读取设备时可接受的进程行数。这一数值远高于任何实际工作负载——只有少数几个
	// 进程使用加速器，而非数百个——这种差异是故意设计的，因为库无法确定缓冲区结束的位置。
	maxProcMemInfoRows = 256
	// procMemInfoGuardRows 是指超过 maxProcMemInfoRows 后剩余待被库层占用内存的量。它的整体目的就是让
	// 这些超出部分被该包所拥有的内存占用。
	//
	// 它远大于该读取器能接受的列表大小，这种不对称性正是关键：尾部无法阻止溢出——因为没有任何办法可以
	// 做到，而计数仅用于输出——它只能足够大，以便一个合理的列表能保留在这一分配范围内。一行仅包含几字
	// 节数据，因此整个缓冲区在每次读取时占用数十千字节，并弥补了读取被拒绝与过程中其他内存损坏之间的
	// 差异。驱动程序写入的计数仍会与接受的列表进行比对，因此扩大这一范围并不会增加调用者所收到的内容。
	procMemInfoGuardRows = 4096
	// procMemInfoGuard 标记了一个未写入的保护行。进程 ID 为正数，因此在任何保护行上写入内容的库都会更
	// 改它。
	procMemInfoGuard int32 = -1
)

// GetProcessMemoryUsage 会返回每个进程在设备上的内存占用情况，库层报告的 Mem_usage 单位为字节。
//
// `count` 是一个仅作为输出使用的参数——其测量基准是驱动程序 25.5.1，即使传入了值为 0 的计数，第一行仍会被
// 写入，且计数会被实际值覆盖——因此无法告诉库程序缓冲区的大小。如果一个设备的进程数超过缓冲区容量，该设备
// 就会将数据写入缓冲区末尾。因此，缓冲区被过度分配了保护尾行，库程序预计无法触及该区域：写入的保护行意味
// 着设备的进程数超过了读取所能接受的数量，此时会返回 ERROR_LIST_TRUNCATED，而不是较短的列表。这种溢出仅局
// 限于该函数所拥有的内存范围内。
//
// 被截断的读取操作是一种不完整的读取，而非小规模的读取。如果返回那些确实符合要求的行，就会让设备的部分处
// 理流程看起来像是全部都如此，因此从这些行中计算出的每个租户数量就会显得合理且错误。
//
// V2 入口点采用相同的计数方式——仅输出数据，因此保护尾部和计数检查均保持不变地适用于它。
func (l Device) GetProcessMemoryUsage() ([]ProcMemInfo, Return) {
	rows := make([]ProcMemInfo, maxProcMemInfoRows+procMemInfoGuardRows)
	for i := maxProcMemInfoRows; i < len(rows); i++ {
		rows[i].Id = procMemInfoGuard
	}

	var (
		procNum int32
		ret     Return
	)

	if apiVersion() == APIVersionV2 {
		devId, devRet := l.devID()
		if !devRet.IsSuccess() {
			return nil, devRet
		}
		ret = Return(dcmiv2GetDeviceProcMemInfo(devId, &rows[0], &procNum))
	} else {
		ret = Return(dcmiGetDeviceResourceInfo(l.cardId, l.deviceId, &rows[0], &procNum))
	}
	if !ret.IsSuccess() {
		return nil, ret
	}

	for i := maxProcMemInfoRows; i < len(rows); i++ {
		if rows[i].Id != procMemInfoGuard {
			return nil, ERROR_LIST_TRUNCATED
		}
	}
	// 同时检查计数和保护机制：如果一个库报告的内容超过其写入的内容，那么超出其填充范围的行将被视为没
	// 有内存的进程。
	if procNum < 0 || procNum > maxProcMemInfoRows {
		return nil, ERROR_LIST_TRUNCATED
	}
	return rows[:procNum], ret
}
