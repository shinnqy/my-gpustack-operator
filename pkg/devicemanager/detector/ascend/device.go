package ascend

import (
	"bufio"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"

	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/binding"
	"gpustack.ai/gpustack/binding/dcmi"
	"gpustack.ai/gpustack/pkg/device"
	productascend "gpustack.ai/gpustack/pkg/devicemanager/product/ascend"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/utils/loggerx"
	"gpustack.ai/gpustack/pkg/utils/strconvx"
)

const Manufacturer = nodefeature.ManufacturerAscend

var (
	_ToolkitHome string
	_PciVendor   string
)

func init() {
	_ToolkitHome = os.Getenv("ASCEND_TOOLKIT_HOME")
	if _ToolkitHome == "" {
		_ToolkitHome = "/usr/local/Ascend/cann"
		if s, err := os.Stat(_ToolkitHome); err != nil || !s.IsDir() {
			_ToolkitHome = "/usr/local/Ascend/ascend-toolkit/latest/runtime"
		}
	}

	pciID := nodefeature.GetPciVendorID(Manufacturer)
	p := strings.Split(pciID, "_")
	_PciVendor = p[len(p)-1]
}

type ascend struct {
	once sync.Once
	dcmi *dcmi.DCMI
	// product 解析本节点 A5 的产品形态。硬件形态在进程存续期间稳定，
	// 可与分配器使用相同的判定规则并复用首次成功结果。
	product *productascend.Resolver
	logger  klog.Logger

	// superPodsMu 保护逐卡超节点缓存，使访问同一探测器的调用安全读写。
	superPodsMu sync.Mutex
	// superPods 按设备地址记住最近一次读出的超节点坐标，单次读取失败时仍可延用。
	superPods map[superPodKey]dcmi.SpodInfo
}

// New 创建 Ascend 探测器，准备 DCMI 库和 A5 产品形态解析器。
func New(opts device.DetectorOptions) device.Detector {
	logger := opts.Logger.WithName(Manufacturer)
	lib := dcmi.New(binding.WithLogger(logger))
	return &ascend{
		dcmi:      lib,
		product:   productascend.NewResolver(productDriver{lib: lib}),
		logger:    logger,
		superPods: make(map[superPodKey]dcmi.SpodInfo),
	}
}

// 以下方法实现设备探测接口。

func (in *ascend) Name() string {
	return Manufacturer
}

func (in *ascend) DetectAccelerator(noPciCheck bool) (_ device.DevicesGroupList, err error) {
	defer loggerx.RecoverWithStackScanner(func(s loggerx.Scanner, e error) {
		in.logger.Error(e, "failed to detect ascend devices")
		for s.Scan() {
			in.logger.Error(nil, s.Text())
		}
		err = e
	})

	pciDevs := binding.GetPCIDevices([]string{_PciVendor}, nil)
	if !noPciCheck && len(pciDevs) == 0 {
		in.logger.Info("no ascend pci devices found")
		return nil, nil
	}

	in.init()

	_, cardList, ret := in.dcmi.GetCardList()
	if !ret.IsSuccess() || len(cardList) == 0 {
		if ret.IsSuccess() {
			in.logger.Info("no ascend devices found")
		} else {
			in.logger.Error(ret, "no ascend devices found")
		}
		return nil, nil
	}

	drVer, _ := in.dcmi.GetDriverVersion()
	rtVer := getToolkitVersion()

	var index uint32
	grpList := device.DevicesGroupList{}
	for _, card := range cardList {
		logger := in.logger.WithValues("card", card)

		cnt, ret := in.dcmi.GetDeviceNumInCard(card)
		if !ret.IsSuccess() {
			logger.Error(ret, "failed to get device count in card")
			continue
		}

		for i := int32(0); i < cnt; i++ {
			logger := logger.WithValues("index", i)

			dev := in.dcmi.GetDeviceHandleByCardAndIndex(card, i)

			typ, ret := dev.GetType()
			if ret.IsSuccess() && typ != dcmi.NPU_TYPE {
				logger.Info("skipping non-NPU device")
				continue
			}

			uuid, ret := deviceUUID(dev)
			if !ret.IsSuccess() {
				logger.Error(ret, "failed to get device vdie info")
				continue
			}

			var pciBusId string
			{
				pciHandler := dev.GetPcieInfoV()
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

			var chipInfo dcmi.ChipInfoV2
			{
				infoHandler := dev.GetChipInfoV()
				chipInfo, ret = infoHandler.V2()
				if !ret.IsSuccess() {
					chipInfo, ret = infoHandler.V1()
					if !ret.IsSuccess() {
						logger.Error(ret, "failed to get device chip info")
						continue
					}
				}
			}

			name := chipInfo.GetChipName()

			var (
				memory          uint64
				memoryUnhealthy bool
			)
			{
				hbmInfo, ret := dev.GetHbmInfo()
				if !ret.IsSuccess() {
					memHandler := dev.GetMemoryInfoV()
					memInfo, ret := memHandler.V2()
					if !ret.IsSuccess() {
						logger.Error(ret, "failed to get device memory info")
						continue
					}
					memory = memInfo.Memory_size
					memoryUnhealthy = isDeviceUnhealthy(dev, dcmi.DEVICE_TYPE_DDR)
				} else {
					memory = hbmInfo.Memory_size
					memoryUnhealthy = isDeviceUnhealthy(dev, dcmi.DEVICE_TYPE_HBM)
				}
			}

			phyId, ret := dev.GetPhysicalID()
			if !ret.IsSuccess() {
				logger.Error(ret, "failed to get device physical ID")
				continue
			}
			// PhysicalIndexes 按固定顺序保存三种硬件地址。
			// 第 0 项是物理 ID，vcann-rt 以它索引配额，多数代际也用它选择可见设备。
			// 第 1 项是 DCMI 卡号。V2 扁平枚举时它同时是逻辑设备 ID，A5 的 ASCEND_VISIBLE_DEVICES 使用这一项。
			// 第 2 项是卡内索引，在 V2 中总为 0。容器共享接口组合使用第 1、2 项。读错槽位可能选到另一张真实设备。
			physicalIndexes := []uint32{phyId, uint32(card), uint32(i)}

			grpIndex := slices.IndexFunc(grpList, func(grp device.DevicesGroup) bool {
				return grp.Name == name && grp.Memory == memory
			})
			if grpIndex == -1 {
				// 首次见到这一型号和显存容量，建立新组。
				cores := chipInfo.Aicore_cnt
				family := getFamilyFromSocName(guessSocNameFromDeviceName(name))
				grpList = append(grpList, device.DevicesGroup{
					ID:             device.ConstructGroupID(Manufacturer, name, memory),
					Manufacturer:   Manufacturer,
					Name:           name,
					Memory:         memory,
					Cores:          cores,
					DriverVersion:  drVer,
					RuntimeVersion: device.NormalizeVersion(rtVer),
					Family:         family,
				})
				grpIndex = len(grpList) - 1
			}

			topo := device.ConstructTopology(pciBusId, pciDev.Root, pciDev.Class, pciDev.Switches)

			// RoCE 网络配置属于可承载多枚 NPU 的 DCMI 卡，因此需要写入该卡下每枚加速器的拓扑。
			if ip, snm, ret := dev.GetIp(dcmi.ROCE_PORT, 0); ret.IsSuccess() {
				if gw, ret := dev.GetGateway(dcmi.ROCE_PORT, 0); ret.IsSuccess() {
					topo.RoCE = &device.Ethernet{
						IP:         ip.String(),
						SubnetMask: snm.String(),
						Gateway:    gw.String(),
					}
				}
			}

			// UB 互连属于 A5，所需查询仅由 V2 提供。旧型号跳过这次调用。
			if grpList[grpIndex].Family == productascend.Family {
				topo.Fabric = in.readFabric(dev, card, i, logger)
			}

			var status device.AcceleratorStatus
			{
				status.Unhealthy = memoryUnhealthy
				status.LogicalSliced = getLogicalSliced(
					grpList[grpIndex].Family,
					grpList[grpIndex].RuntimeVersion,
				)
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
	}

	device.SetGroupSlicedDetails(grpList)

	return grpList, nil
}

func (in *ascend) init() {
	in.once.Do(func() {
		if ret := in.dcmi.Init(in.logger); !ret.IsSuccess() {
			in.logger.Error(ret, "failed to initialize DCMI library")
		}
	})
}

// SoC 型号映射参考 Ascend PyTorch 的 NpuVariables.cpp 和 NpuVariables.h。
// https://gitcode.com/Ascend/pytorch/blob/master/torch_npu/csrc/core/npu/NpuVariables.cpp#L13-L40
// https://gitcode.com/Ascend/pytorch/blob/master/torch_npu/csrc/core/npu/NpuVariables.h#L5-L34
// 产品形态另见 https://www.hiascend.com/document/detail/zh/AscendFAQ/ProduTech/productform/hardwaredesc_0001.html 和 https://blog.csdn.net/fuhanghang/article/details/146411242 。
var socNameVersionMap = map[string]int{
	"Ascend910PremiumA": 100,
	"Ascend910ProA":     101,
	"Ascend910A":        102,
	"Ascend910ProB":     103,
	"Ascend910B":        104,
	"Ascend310P1":       200,
	"Ascend310P2":       201,
	"Ascend310P3":       202,
	"Ascend310P4":       203,
	"Ascend310P5":       204,
	"Ascend310P7":       205,
	"Ascend910B1":       220,
	"Ascend910B2":       221,
	"Ascend910B2C":      222,
	"Ascend910B3":       223,
	"Ascend910B4":       224,
	"Ascend910B4-1":     225,
	"Ascend310B1":       240,
	"Ascend310B2":       241,
	"Ascend310B3":       242,
	"Ascend310B4":       243,
	"Ascend910_9391":    250,
	"Ascend910":         250,
	"Ascend910_9392":    251,
	"Ascend910_9381":    252,
	"Ascend910_9382":    253,
	"Ascend910_9372":    254,
	"Ascend910_9362":    255,
	"Ascend910_9363":    256,
	"Ascend910_9579":    260,
	"Ascend910_95":      260,
	"Ascend950":         260,
}

// deviceUUID 从芯片裸片 ID 构造 Devices API 的设备标识。先尝试新接口，再回退到旧接口。
// 无法读取身份时保留错误，避免不同节点报告相同的伪造标识。
func deviceUUID(dev dcmi.Device) (string, dcmi.Return) {
	dieHandler := dev.GetVDieV()

	die, ret := dieHandler.V2()
	if ret.IsSuccess() {
		return die.String(), ret
	}

	die, v1Ret := dieHandler.V1()
	if v1Ret.IsSuccess() {
		return die.String(), v1Ret
	}

	// 旧查询若根本不可用，返回新查询提供的具体失败原因。若旧查询确实执行过，就采用它的返回码，
	// 避免把驱动故障掩盖为接口不受支持。
	if v1Ret == dcmi.ERROR_NOT_SUPPORT || v1Ret == dcmi.ERROR_FUNCTION_NOT_FOUND {
		return "", ret
	}

	return "", v1Ret
}

var (
	_910ARegex = regexp.MustCompile(`^910`)
	_910BRegex = regexp.MustCompile(`^(910B\d|A2G\d)`)
	_310PRegex = regexp.MustCompile(`^(310P\d?|I2\d?)`)
)

func guessSocNameFromDeviceName(devName string) string {
	devName = strings.TrimPrefix(strings.TrimSpace(devName), "Ascend")
	socName := "Ascend" + strings.TrimSpace(devName)
	if _, ok := socNameVersionMap[socName]; ok {
		return socName
	}
	// 950 使用前缀匹配。950PR、950DT 等后缀还会增加，ascend-common、容器运行时和 torch_npu 都按同一代 SoC 处理。
	if strings.HasPrefix(devName, "950") {
		return "Ascend950"
	}
	// https://gitcode.com/Ascend/mind-cluster/blob/master/component/ascend-common/devmanager/common/utils.go#L159-L176
	if _310PRegex.MatchString(devName) {
		return "Ascend310P1"
	}
	if strings.Contains(devName, "310B") {
		return "Ascend310B1"
	}
	// if strings.Contains(devName, "310") {
	// 	return "Ascend310"
	// }
	if _910BRegex.MatchString(devName) {
		return "Ascend910B1"
	}
	if _910ARegex.MatchString(devName) {
		return "Ascend910A"
	}
	return ""
}

// slicedRuntimeMajors 列出每个型号族实际构建过 vcann-rt 的 CANN 主版本，
// 与 xbuild-ascend-cann-<major>-<family> 阶段一一对应。只有匹配组合才能发布软切能力，
// 否则容器会指向镜像中不存在的库目录。
var slicedRuntimeMajors = map[string][]int{
	"910B": {8, 9},
	"910C": {8, 9},
	"950":  {9},
	"310P": {9}, // 上游自 CANN 9.1.0 起提供 310P 的 DCMI 适配。
}

// getLogicalSliced 按型号族和 CANN 版本返回 vcann-rt 支持的分时计算与显存切分能力，
// 单卡切片位以设备最大用户进程数 63 为上限。仅发布镜像中有运行库的组合。主版本按整数比较，
// 避免把字符串 10 排在 9 前面。
func getLogicalSliced(family, runtimeVersion string) device.AcceleratorLogicalSliced {
	majors, supported := slicedRuntimeMajors[family]
	if !supported {
		return device.AcceleratorLogicalSliced{}
	}
	major, err := strconvx.Atoi[int](device.RuntimeMajor(runtimeVersion, "8"))
	if err != nil || !slices.Contains(majors, major) {
		return device.AcceleratorLogicalSliced{}
	}

	return device.AcceleratorLogicalSliced{
		Count:                     63,
		CoresPercentageOvercommit: true,
	}
}

// isDeviceUnhealthy 优先读取 DCMI 整体健康状态。0 正常，1 为轻微告警，2 及以上为严重异常。
// 健康查询失败时才以不可纠正的双位 ECC 错误作为后备判断。
func isDeviceUnhealthy(dev dcmi.Device, memType dcmi.DeviceType) bool {
	health, ret := dev.GetHealth()
	return evaluateDeviceUnhealthy(health, ret, func() (dcmi.EccInfo, dcmi.Return) {
		return dev.GetEccInfo(memType)
	})
}

// evaluateDeviceUnhealthy 先看整体健康状态，缺少该数据时再读双位 ECC 错误。两种查询都没有证据时，
// 不据此标记故障。
func evaluateDeviceUnhealthy(health uint32, healthRet dcmi.Return, readECC func() (dcmi.EccInfo, dcmi.Return)) bool {
	if healthRet.IsSuccess() {
		return health >= 2
	}

	eccInfo, eccRet := readECC()
	return eccRet.IsSuccess() && eccInfo.Enable_flag > 0 && eccInfo.Double_bit_error_cnt > 0
}

func getFamilyFromSocName(socName string) string {
	if socName != "" {
		socVersion, ok := socNameVersionMap[socName]
		if ok {
			switch {
			case socVersion < 200:
				return "910"
			case socVersion < 220:
				return "310P"
			case socVersion < 240:
				return "910B" // 910B/A2
			case socVersion < 250:
				return "310B"
			case socVersion < 260:
				return "910C" // 910C/A3
			case socVersion < 270:
				return "950" // 950/A5
			}
		}
	}

	return ""
}

func getToolkitVersion() string {
	const prefix = "Version="

	for _, path := range []string{
		filepath.Join(_ToolkitHome, "version.info"),
		filepath.Join(_ToolkitHome, "share", "info", "runtime", "version.info"),
	} {
		if s, err := os.Stat(path); err == nil && s.Mode().IsRegular() {
			f, err := os.Open(path)
			if err != nil {
				continue
			}

			sc := bufio.NewScanner(f)
			for sc.Scan() {
				line := sc.Text()
				if strings.HasPrefix(line, prefix) {
					_ = f.Close()
					return strings.TrimPrefix(line, prefix)
				}
			}
			_ = f.Close()
		}
	}
	return ""
}
