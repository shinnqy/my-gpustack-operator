package nvidia

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"

	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/binding"
	"gpustack.ai/gpustack/binding/nvml"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/utils/loggerx"
)

const Manufacturer = nodefeature.ManufacturerNVIDIA

// hbmMemoryBusWidthBits 用位宽区分 HBM 和 GDDR。数据中心 HBM 至少为 1024 位，例如 A30 为 3072，
// A100/H100 为 5120。GDDR 不超过 512 位。探测器据此选择 ECC 容量换算规则。
const hbmMemoryBusWidthBits = 1024

// gddr7ComputeCapabilityMajor 标记首个采用片内 ECC 的 GDDR7 计算架构。
// 此前部分 GDDR 显卡启用 ECC 会占用约十六分之一的显存。
const gddr7ComputeCapabilityMajor = 12

var _PciVendor string

// 这个方法是 Go 加载包时执行的初始化
// 取编号，NVIDIA 默认是 10de
func init() {
	pciID := nodefeature.GetPciVendorID(Manufacturer)
	p := strings.Split(pciID, "_")
	_PciVendor = p[len(p)-1]
}

type nvidia struct {
	once   sync.Once
	nvml   *nvml.NVML
	logger klog.Logger

	// fabricsMu 保护逐卡互连注册缓存，避免不同调用并发读写 map。
	fabricsMu sync.Mutex
	// fabrics 按设备 UUID 保存最近一次成功读取的互连注册信息，供本轮查询失败时复用。
	fabrics map[string]fabricInfo
}

// IMPORTANT: 从 detector_linux.go 文件的 {nvidia.Manufacturer, nvidia.New} 调用到这里
// New 创建 NVIDIA 探测器并配置 NVML 管理库。
func New(opts device.DetectorOptions) device.Detector {
	logger := opts.Logger.WithName(Manufacturer)
	return &nvidia{
		nvml:    nvml.New(binding.WithLogger(logger)),
		logger:  logger,
		fabrics: make(map[string]fabricInfo),
	}
}

// 以下方法提供厂商名称和硬件枚举结果。

func (in *nvidia) Name() string {
	return Manufacturer
}

func (in *nvidia) DetectAccelerator(noPciCheck bool) (_ device.DevicesGroupList, err error) {
	defer loggerx.RecoverWithStackScanner(func(s loggerx.Scanner, e error) {
		in.logger.Error(e, "failed to detect nvidia devices")
		for s.Scan() {
			in.logger.Error(nil, s.Text())
		}
		err = e
	})

	// 先看 PCI，找卡
	pciDevs := binding.GetPCIDevices([]string{_PciVendor}, nil)
	if !noPciCheck && len(pciDevs) == 0 {
		in.logger.Info("no nvidia pci devices found")
		return nil, nil
	}

	// 这个是探测到需要读卡时才调用的方法，用来初始化 NVML
	in.init()

	// 先知道驱动能枚举几个设备
	cnt, ret := in.nvml.DeviceGetCount()
	if !ret.IsSuccess() || cnt == 0 {
		if ret.IsSuccess() {
			in.logger.Info("no nvidia devices found")
		} else {
			in.logger.Error(ret, "no nvidia devices found")
		}
		return nil, nil
	}

	drVer, _ := in.nvml.SystemGetDriverVersion()
	rtVer, _ := in.nvml.SystemGetCudaDriverVersion()

	var index uint32
	grpList := device.DevicesGroupList{}
	for i := 0; i < cnt; i++ {
		logger := in.logger.WithValues("index", i)

		dev, ret := in.nvml.DeviceGetHandleByIndex(i)
		if !ret.IsSuccess() {
			logger.Error(ret, "failed to get device handle")
			continue
		}

		// 不管几张卡即使外观、型号和显存都一样，也需要两个独立身份
		uuid, ret := dev.GetUUID()
		if !ret.IsSuccess() {
			logger.Error(ret, "failed to get device UUID")
			continue
		}

		var pciBusId string
		{
			// 新旧驱动都要能读
			pciHandler := dev.GetPciInfoV()
			pciInfo, ret := pciHandler.V2()
			if !ret.IsSuccess() {
				pciInfo, ret = pciHandler.V1()
				if !ret.IsSuccess() {
					logger.Error(ret, "failed to get device PCI info")
					continue
				}
			}
			pciBusId = pciInfo.GetBusId()
		}
		pciDev := pciDevs[pciBusId]

		name, ret := dev.GetName()
		if !ret.IsSuccess() {
			logger.Error(ret, "failed to get device name")
			continue
		}

		// 先读计算能力。ECC 容量换算需要判断架构代际，建立设备组时也会使用相同读数。
		ccMajor, ccMinor, ccRet := dev.GetCudaComputeCapability()

		var (
			memory          uint64
			memoryUnhealthy bool
		)
		{
			memHandler := dev.GetMemoryInfoV()
			memInfo, ret := memHandler.V2()
			if !ret.IsSuccess() {
				memInfo, ret = memHandler.V1()
				if !ret.IsSuccess() {
					logger.Error(ret, "failed to get device memory info")
					continue
				}
			}
			// API 统一用 MiB。`>> 20` 等价于对非负字节数做整数除法 ÷ 1,048,576
			// NVML 的显存总量已经反映当前 ECC 模式。
			memory = device.ConvertBytesToMiB(memInfo.Total)

			// 从 Turing 起，部分专业 GDDR 卡启用 ECC 会从显存划出约 1/16 做校验，需乘 16/15 还原名义容量。
			// HBM 的校验空间由硬件保留，无需换算。
			// Blackwell 的 GDDR7 使用片内 ECC。
			// 实测 RTX PRO 6000 Blackwell Server Edition 在驱动 580.173.02 下报告 97887 MiB，
			// 套用旧公式会错误增至 104412 MiB。因此只在已知位宽、ECC 已启用且计算能力主版本低于 12 时换算。
			// 任一读数不确定时保留驱动原值。
			if bw, ret := dev.GetMemoryBusWidth(); ret.IsSuccess() && bw > 0 && bw < hbmMemoryBusWidthBits {
				if cur, _, ret := dev.GetEccMode(); ret.IsSuccess() && cur == nvml.FEATURE_ENABLED {
					if ccRet.IsSuccess() && ccMajor < gddr7ComputeCapabilityMajor {
						memory = memory * 16 / 15
					}
				}
			}
			memoryUnhealthy = isDeviceUnhealthy(dev)
		}

		grpIndex := slices.IndexFunc(grpList, func(grp device.DevicesGroup) bool {
			return grp.Name == name && grp.Memory == memory
		})
		if grpIndex == -1 {
			// 首次见到这一型号和显存容量，建立新组。
			cores, _ := dev.GetNumGpuCores()
			family := getFamilyFromComputeCapability(ccMajor, ccMinor)
			grpList = append(grpList, device.DevicesGroup{
				ID:                device.ConstructGroupID(Manufacturer, name, memory),
				Manufacturer:      Manufacturer,
				Name:              name,
				Memory:            memory,
				Cores:             cores,
				DriverVersion:     drVer,
				RuntimeVersion:    stringifyRuntimeVersion(rtVer),
				Family:            family,
				ComputeCapability: stringifyComputeCapability(ccMajor, ccMinor),
			})
			grpIndex = len(grpList) - 1
		}

		// 驱动能返回次设备号时才记录。它关系到设备节点的实际名称，用枚举索引补空值会令后续消费者打开错误的设备路径。
		var physicalIndexes []uint32
		// PhysicalIndexes 要保存系统实际能用来寻址的次设备号，例如用 4 找 /dev/nvidia4
		if minorNum, ret := dev.GetMinorNumber(); ret.IsSuccess() {
			physicalIndexes = []uint32{minorNum}
		} else {
			logger.V(3).Info("recorded no minor number for a card whose driver could not answer for it; "+
				"a consumer that names a device node after that number cannot address this card",
				"card", uuid, "reason", ret.Error())
		}

		// 单卡各有自己的 PCI 位置、CPU 亲和性和健康状态。NVIDIA 路径会查显存不可纠正 ECC 错误，只有成功读到非零计数才标记 Unhealthy
		topo := device.ConstructTopology(pciBusId, pciDev.Root, pciDev.Class, pciDev.Switches)
		topo.Fabric = in.readFabric(dev, uuid, logger)

		var status device.AcceleratorStatus
		{
			status.Unhealthy = memoryUnhealthy

			// 每张卡按当前 MIG 模式在逻辑切分和硬件分区之间选择。已启用 MIG 的卡只公布实测物理规格。
			// 待生效的模式要在重置并重新探测后才算数。
			// 驱动明确不支持 MIG 是有效答复。模式读取出错时按原有软切路径报告，同时记录错误，以便定位本应启用 MIG 的卡。
			migCurrent, _, migRet := dev.GetMigMode()
			if !migRet.IsSuccess() && !driverReportsAbsent(migRet) {
				logger.Error(migRet, "could not read a card's MIG mode, so it is reported as "+
					"MIG off; if MIG is in fact enabled, the card advertises logical slicing it cannot serve",
					"card", uuid)
			}
			if migCurrent == nvml.DEVICE_MIG_ENABLE {
				profiles := detectMigProfiles(dev)
				status.PhysicalSliced = device.AcceleratorPhysicalSliced{
					Profiles: profiles,
					Count:    maxProfileCount(profiles),
				}
			} else {
				// NVIDIA 逻辑切分依靠 HAMi-core 预加载库。Volta 及之后架构每卡最多支持 128 个 CUDA 用户进程，
				// 因此将单卡切片数上限设为 128。
				status.LogicalSliced = device.AcceleratorLogicalSliced{
					Count:                     128,  // 用来描述后续 HAMi-core 路径可以使用的软切能力，`128` 是切片名额的上限
					CoresPercentageOvercommit: true, // 用来描述后续 HAMi-core 路径可以使用的软切能力，`true` 表示算力可以按时间共享
				}
			}
		}

		grpList[grpIndex].Accelerators = append(
			grpList[grpIndex].Accelerators,
			device.Accelerator{
				ID:              uuid,
				Index:           index,
				PhysicalIndexes: physicalIndexes,
				Topology:        topo,
				Status:          status,
			},
		)
		index++
	}

	device.SetGroupSlicedDetails(grpList)

	return grpList, nil
}

func isDeviceUnhealthy(dev nvml.Device) bool {
	memEccDramUE, ret := dev.GetMemoryErrorCounter(
		nvml.MEMORY_ERROR_TYPE_UNCORRECTED,
		nvml.VOLATILE_ECC,
		nvml.MEMORY_LOCATION_DRAM,
	)
	return evaluateDeviceUnhealthy(memEccDramUE, ret)
}

// evaluateDeviceUnhealthy 根据厂商的不可纠正 ECC 错误计数判断设备状态，读取失败本身不能证明设备故障。
func evaluateDeviceUnhealthy(eccErrors uint64, ret nvml.Return) bool {
	return ret.IsSuccess() && eccErrors > 0
}

func (in *nvidia) init() {
	in.once.Do(func() {
		if ret := in.nvml.Init(); !ret.IsSuccess() {
			in.logger.Error(ret, "failed to initialize NVML library")
		}
	})
}

func stringifyRuntimeVersion(rtVer int32) string {
	if rtVer <= 0 {
		return ""
	}
	major := rtVer / 1000
	minor := (rtVer % 1000) / 10
	return strconv.Itoa(int(major)) + "." + strconv.Itoa(int(minor))
}

func stringifyComputeCapability(ccMajor, ccMinor int32) string {
	if ccMajor == 0 {
		return ""
	}
	return fmt.Sprintf("%d.%d", ccMajor, ccMinor)
}

func getFamilyFromComputeCapability(ccMajor, ccMinor int32) string {
	switch ccMajor {
	case 1:
		return "Tesla"
	case 2:
		return "Fermi"
	case 3:
		return "Kepler"
	case 5:
		return "Maxwell"
	case 6:
		return "Pascal"
	case 7:
		if ccMinor < 5 {
			return "Volta"
		}
		return "Turing"
	case 8:
		if ccMinor < 9 {
			return "Ampere"
		}
		return "Ada-Lovelace"
	case 9:
		return "Hopper"
	case 10, 12:
		return "Blackwell"
	}
	return ""
}
