package thead

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/binding"
	"gpustack.ai/gpustack/binding/hgml"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/utils/loggerx"
)

const Manufacturer = nodefeature.ManufacturerTHead

var _PciVendor string

func init() {
	pciID := nodefeature.GetPciVendorID(Manufacturer)
	p := strings.Split(pciID, "_")
	_PciVendor = p[len(p)-1]
}

type thead struct {
	once   sync.Once
	hgml   *hgml.HGML
	logger klog.Logger
}

// New 创建平头哥 PPU 探测器并配置 HGML 管理库。
func New(opts device.DetectorOptions) device.Detector {
	logger := opts.Logger.WithName(Manufacturer)
	return &thead{
		hgml:   hgml.New(binding.WithLogger(logger)),
		logger: logger,
	}
}

// 以下方法提供厂商名称和硬件枚举结果。

func (in *thead) Name() string {
	return Manufacturer
}

func (in *thead) DetectAccelerator(noPciCheck bool) (_ device.DevicesGroupList, err error) {
	defer loggerx.RecoverWithStackScanner(func(s loggerx.Scanner, e error) {
		in.logger.Error(e, "failed to detect thead devices")
		for s.Scan() {
			in.logger.Error(nil, s.Text())
		}
		err = e
	})

	pciDevs := binding.GetPCIDevices([]string{_PciVendor}, nil)
	if !noPciCheck && len(pciDevs) == 0 {
		in.logger.Info("no thead pci devices found")
		return nil, nil
	}

	in.init()

	cnt, ret := in.hgml.DeviceGetCount()
	if !ret.IsSuccess() || cnt == 0 {
		if ret.IsSuccess() {
			in.logger.Info("no thead devices found")
		} else {
			in.logger.Error(ret, "no thead devices found")
		}
		return nil, nil
	}

	drVer, _ := in.hgml.SystemGetDriverVersion()
	rtVer, _ := in.hgml.SystemGetHggcDriverVersion()

	var index uint32
	grpList := device.DevicesGroupList{}
	for i := 0; i < cnt; i++ {
		logger := in.logger.WithValues("index", i)

		dev, ret := in.hgml.DeviceGetHandleByIndex(i)
		if !ret.IsSuccess() {
			logger.Error(ret, "failed to get device handle")
			continue
		}

		uuid, ret := dev.GetUUID()
		if !ret.IsSuccess() {
			logger.Error(ret, "failed to get device UUID")
			continue
		}

		var pciBusId string
		{
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
			memory = device.ConvertBytesToMiB(memInfo.Total)
			memoryUnhealthy = isDeviceUnhealthy(dev)
		}

		ccMajor, ccMinor, _ := dev.GetHggcComputeCapability()

		grpIndex := slices.IndexFunc(grpList, func(grp device.DevicesGroup) bool {
			return grp.Name == name && grp.Memory == memory
		})
		if grpIndex == -1 {
			// 首次见到这一型号和显存容量，建立新组。
			cores, _ := dev.GetNumGpuCores()
			grpList = append(grpList, device.DevicesGroup{
				ID:                device.ConstructGroupID(Manufacturer, name, memory),
				Manufacturer:      Manufacturer,
				Name:              name,
				Memory:            memory,
				Cores:             cores,
				DriverVersion:     drVer,
				RuntimeVersion:    stringifyRuntimeVersion(rtVer),
				ComputeCapability: stringifyComputeCapability(ccMajor, ccMinor),
			})
			grpIndex = len(grpList) - 1
		}

		// 用驱动报告的次设备号核对按卡序号命名的字符设备节点是否真属于这张卡。查询失败时留空，
		// 避免用序号猜测后把其他设备误认成此卡。
		var physicalIndexes []uint32
		if minorNum, ret := dev.GetMinorNumber(); ret.IsSuccess() {
			physicalIndexes = []uint32{minorNum}
		} else {
			// 单卡读取失败记录在调试级别。驱动不提供该属性通常是持续状态，周期探测时应避免反复输出同一条普通日志。
			logger.V(3).Info("recorded no minor number for a card whose driver could not answer for it; "+
				"every allocation on it will be refused rather than addressed by an ordinal nothing "+
				"can prove",
				"card", uuid, "reason", ret.Error())
		}

		topo := device.ConstructTopology(pciBusId, pciDev.Root, pciDev.Class, pciDev.Switches)

		var status device.AcceleratorStatus
		{
			status.Unhealthy = memoryUnhealthy

			// 逐卡按分区模式选择逻辑或物理切分。已启用分区时只发布实际硬切规格，
			// 使同一张卡的容量不会被两种资源名重复计量。
			migCurrent, _, migRet := dev.GetMigMode()
			if !migRet.IsSuccess() && !driverReportsAbsent(migRet) {
				logger.Error(migRet, "could not read a card's partitioning mode, so it is reported as "+
					"not partitioned; if the mode is in fact enabled, the card advertises logical "+
					"slicing it cannot serve",
					"card", uuid)
			}
			if migCurrent == hgml.DEVICE_MIG_ENABLE {
				status.PhysicalSliced = physicalSliced(detectMigProfiles(dev, logger))
			} else {
				// 平头哥通过镜像中的两套预加载库提供逻辑切分。HGML 头文件未规定单卡用户进程上限，因此令牌池只提供注入入口，
				// 实际预算仍取决于每份资源限制。
				status.LogicalSliced = device.AcceleratorLogicalSliced{
					Count:                     128,
					CoresPercentageOvercommit: true,
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

	// 同一组卡必须对同名规格给出相同的几何形状，再按名称汇总。存在分歧时从整组剔除该名称。
	for i := range grpList {
		for _, reason := range rejectDivergentGroupProfiles(&grpList[i]) {
			in.logger.Info("withheld a partition profile the group's cards disagree on",
				"group", grpList[i].ID, "reason", reason)
		}
	}

	device.SetGroupSlicedDetails(grpList)

	return grpList, nil
}

func (in *thead) MonitorAccelerator(noPciCheck bool) (_ device.MetricsGroupList, err error) {
	defer loggerx.RecoverWithStackScanner(func(s loggerx.Scanner, e error) {
		in.logger.Error(e, "failed to monitor thead devices")
		for s.Scan() {
			in.logger.Error(nil, s.Text())
		}
		err = e
	})

	pciDevs := binding.GetPCIDevices([]string{_PciVendor}, nil)
	if !noPciCheck && len(pciDevs) == 0 {
		in.logger.Info("no thead pci devices found")
		return nil, nil
	}

	in.init()

	cnt, ret := in.hgml.DeviceGetCount()
	if !ret.IsSuccess() || cnt == 0 {
		if ret.IsSuccess() {
			in.logger.Info("no thead devices found")
		} else {
			in.logger.Error(ret, "no thead devices found")
		}
		return nil, nil
	}

	grpList := device.MetricsGroupList{
		{
			Manufacturer: Manufacturer,
			Timestamp:    time.Now(),
			Accelerators: make([]device.AcceleratorMetrics, 0, cnt),
		},
	}
	for i := 0; i < cnt; i++ {
		logger := in.logger.WithValues("index", i)

		dev, ret := in.hgml.DeviceGetHandleByIndex(i)
		if !ret.IsSuccess() {
			logger.Error(ret, "failed to get device handle")
			continue
		}

		uuid, ret := dev.GetUUID()
		if !ret.IsSuccess() {
			logger.Error(ret, "failed to get device UUID")
			continue
		}

		var (
			memory            uint64
			memoryUsage       uint64
			memoryUtilization uint32
			memoryUnhealthy   bool
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

			memory = device.ConvertBytesToMiB(memInfo.Total)
			memoryUsage = device.ConvertBytesToMiB(memInfo.Used)
			memoryUtilization = device.CalculateUtilization(memoryUsage, memory)
			memoryUnhealthy = isDeviceUnhealthy(dev)
		}

		var (
			coresUtilization uint32
			temperature      uint32
			powerUsage       uint32
		)
		{
			gpmSupportHandler := dev.GpmQueryDeviceSupportV()
			gpmSupport, ret := gpmSupportHandler.V1()
			if ret.IsSuccess() && gpmSupport.IsSupportedDevice != 0 {
				gpmMetricsHandler := dev.GpmMetricsGetV()
				gpmMetrics, ret := gpmMetricsHandler.V1(100*time.Millisecond, hgml.GPM_METRIC_SM_UTIL)
				if !ret.IsSuccess() {
					logger.V(4).Error(ret, "failed to get device cores utilization with GPM, fallback")
					coresUtilization = coresUtilizationOf(logger, dev)
				} else {
					coresUtilization = uint32(gpmMetrics[0].Value)
				}
			} else {
				logger.Info("cannot get device cores utilization with GPM, fallback")
				coresUtilization = coresUtilizationOf(logger, dev)
			}

			temperature, ret = dev.GetTemperature()
			if !ret.IsSuccess() {
				logger.V(3).Error(ret, "failed to get device temperature")
			}

			powerUsage, ret = dev.GetPowerUsage()
			if !ret.IsSuccess() {
				logger.V(3).Error(ret, "failed to get device power usage")
			} else {
				powerUsage /= 1_000 // 从毫瓦转换为瓦特。
			}
		}

		grpList[0].Accelerators = append(
			grpList[0].Accelerators,
			device.AcceleratorMetrics{
				ID:                uuid,
				Memory:            memory,
				MemoryUsage:       memoryUsage,
				MemoryUtilization: memoryUtilization,
				CoresUtilization:  coresUtilization,
				Temperature:       temperature,
				PowerUsage:        powerUsage,
				Unhealthy:         memoryUnhealthy,
			},
		)
	}

	return grpList, nil
}

func isDeviceUnhealthy(dev hgml.Device) bool {
	memEccDramUE, ret := dev.GetMemoryErrorCounter(
		hgml.MEMORY_ERROR_TYPE_UNCORRECTED,
		hgml.VOLATILE_ECC,
		hgml.MEMORY_LOCATION_DRAM,
	)
	return evaluateDeviceUnhealthy(memEccDramUE, ret)
}

// evaluateDeviceUnhealthy 根据厂商的不可纠正 ECC 错误计数判断设备状态，读取失败本身不能证明设备故障。
func evaluateDeviceUnhealthy(eccErrors uint64, ret hgml.Return) bool {
	return ret.IsSuccess() && eccErrors > 0
}

// coresUtilizationOf 读取加速器的全设备核心利用率。
//
// NOT_SUPPORTED 是设备自身的响应，而非故障信号。分区加速器正是如此：在 MIG 模式下，计算任务按分区进行分配，
// 不存在整个设备的汇总数据。这是设备的固有特性，因此无论探测器运行多少轮，这一情况都会持续存在。将其报告
// 为错误会导致故障，而一旦发生这种情况，设备就会永远处于故障状态，除非其运行符合设计要求。这一情况会在探
// 测器自身的详细报告中体现。
//
// 无论如何，读数都是零：该数据没有缺失形式，分区加速器实际使用后的消费者会读取每个分区的独立条目，而非整
// 个卡组的条目。
func coresUtilizationOf(logger klog.Logger, dev hgml.Device) uint32 {
	utilInfo, ret := dev.GetUtilizationRates()
	switch {
	case ret.IsSuccess():
		return utilInfo.Gpu
	case ret == hgml.ERROR_NOT_SUPPORTED:
		logger.Info("device-level cores utilization is unsupported")
	default:
		logger.Error(ret, "failed to get device cores utilization")
	}
	return 0
}

func (in *thead) init() {
	in.once.Do(func() {
		if ret := in.hgml.Init(); !ret.IsSuccess() {
			in.logger.Error(ret, "failed to initialize HGML library")
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
