package cambricon

import (
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/binding"
	"gpustack.ai/gpustack/binding/cndev"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/utils/loggerx"
)

const Manufacturer = nodefeature.ManufacturerCambricon

var _PciVendor string

func init() {
	pciID := nodefeature.GetPciVendorID(Manufacturer)
	p := strings.Split(pciID, "_")
	_PciVendor = p[len(p)-1]
}

type cambricon struct {
	once   sync.Once
	cndev  *cndev.CNDev
	logger klog.Logger
}

// New 创建寒武纪探测器并配置 CNDev 管理库。
func New(opts device.DetectorOptions) device.Detector {
	logger := opts.Logger.WithName(Manufacturer)
	return &cambricon{
		cndev:  cndev.New(binding.WithLogger(logger)),
		logger: logger,
	}
}

// Name 和 DetectAccelerator 实现厂商硬件枚举接口。
func (in *cambricon) Name() string {
	return Manufacturer
}

func (in *cambricon) DetectAccelerator(noPciCheck bool) (_ device.DevicesGroupList, err error) {
	defer loggerx.RecoverWithStackScanner(func(s loggerx.Scanner, e error) {
		in.logger.Error(e, "failed to detect cambricon devices")
		for s.Scan() {
			in.logger.Error(nil, s.Text())
		}
		err = e
	})

	pciDevs := binding.GetPCIDevices([]string{_PciVendor}, nil)
	if !noPciCheck && len(pciDevs) == 0 {
		in.logger.Info("no cambricon PCI devices found")
		return nil, nil
	}

	in.init()

	cnt, ret := in.cndev.GetDeviceCount()
	if !ret.IsSuccess() || cnt == 0 {
		if ret.IsSuccess() {
			in.logger.Info("no cambricon devices found")
		} else {
			in.logger.Error(ret, "no cambricon devices found")
		}
		return nil, nil
	}

	rtVer := getRuntimeVersion()

	var index uint32
	grpList := device.DevicesGroupList{}
	for i := 0; i < cnt; i++ {
		logger := in.logger.WithValues("index", i)

		dev, ret := in.cndev.GetDeviceHandleByIndex(i)
		if !ret.IsSuccess() {
			logger.Error(ret, "failed to get device handle by index")
			continue
		}

		uuid, ret := dev.GetUUID()
		if !ret.IsSuccess() {
			logger.Error(ret, "failed to get device UUID")
			continue
		}

		var pciBusId string
		{
			pciHandler := dev.GetPCIeInfoV()
			pciInfo, ret := pciHandler.V2()
			if !ret.IsSuccess() {
				logger.Error(ret, "failed to get device PCI info")
				continue
			}
			pciBusId = pciInfo.GetBusId()
		}
		pciDev := pciDevs[pciBusId]

		name, ret := dev.GetCardName()
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
				logger.Error(ret, "failed to get device memory total")
				continue
			}
			memory = uint64(memInfo.PhysicalMemoryTotal)
			memoryUnhealthy = isDeviceUnhealthy(dev)
		}

		grpIndex := slices.IndexFunc(grpList, func(grp device.DevicesGroup) bool {
			return grp.Name == name && grp.Memory == memory
		})
		if grpIndex == -1 {
			var drVer string
			if verInfo, ret := dev.GetVersionInfo(); ret.IsSuccess() {
				drVer = fmt.Sprintf("%d.%d", verInfo.DriverMajorVersion, verInfo.DriverMinorVersion)
			}

			// 首次见到这一型号和显存容量，建立新组。
			grpList = append(grpList, device.DevicesGroup{
				ID:             device.ConstructGroupID(Manufacturer, name, memory),
				Manufacturer:   Manufacturer,
				Name:           name,
				Memory:         memory,
				DriverVersion:  drVer,
				RuntimeVersion: device.NormalizeVersion(rtVer),
			})
			grpIndex = len(grpList) - 1
		}

		physicalIndexes := []uint32{uint32(i)}

		topo := device.ConstructTopology(pciBusId, pciDev.Root, pciDev.Class, pciDev.Switches)

		var status device.AcceleratorStatus
		{
			status.Unhealthy = memoryUnhealthy
			// MLU 通过 cnDev 的 sMLU 规格进行逻辑切分。每卡上限为实测的 16 份，算力按空间分配。
			status.LogicalSliced = device.AcceleratorLogicalSliced{
				Count: 16,
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

func (in *cambricon) MonitorAccelerator(noPciCheck bool) (_ device.MetricsGroupList, err error) {
	defer loggerx.RecoverWithStackScanner(func(s loggerx.Scanner, e error) {
		in.logger.Error(e, "failed to monitor cambricon devices")
		for s.Scan() {
			in.logger.Error(nil, s.Text())
		}
		err = e
	})

	pciDevs := binding.GetPCIDevices([]string{_PciVendor}, nil)
	if !noPciCheck && len(pciDevs) == 0 {
		in.logger.Info("no cambricon PCI devices found")
		return nil, nil
	}

	in.init()

	cnt, ret := in.cndev.GetDeviceCount()
	if !ret.IsSuccess() || cnt == 0 {
		if ret.IsSuccess() {
			in.logger.Info("no cambricon devices found")
		} else {
			in.logger.Error(ret, "no cambricon devices found")
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

		dev, ret := in.cndev.GetDeviceHandleByIndex(i)
		if !ret.IsSuccess() {
			logger.Error(ret, "failed to get device handle by index")
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
				logger.Error(ret, "failed to get device memory total")
				continue
			}

			memory = uint64(memInfo.PhysicalMemoryTotal)
			memoryUsage = uint64(memInfo.PhysicalMemoryUsed)
			memoryUtilization = device.CalculateUtilization(memoryUsage, memory)
			memoryUnhealthy = isDeviceUnhealthy(dev)
		}

		var (
			coresUtilization uint32
			temperature      uint32
			powerUsage       uint32
		)
		{
			utilInfo, ret := dev.GetUtilizationInfo()
			if !ret.IsSuccess() {
				logger.Error(ret, "failed to get device cores utilization")
			} else {
				coresUtilization = uint32(utilInfo.AverageCoreUtilization)
			}

			tempInfo, ret := dev.GetTemperatureInfo()
			if !ret.IsSuccess() {
				logger.V(3).Error(ret, "failed to get device temperature")
			} else {
				temperature = uint32(tempInfo.Chip)
			}

			powerInfo, ret := dev.GetPowerInfo()
			if !ret.IsSuccess() {
				logger.V(3).Error(ret, "failed to get device power usage")
			} else {
				powerUsage = uint32(powerInfo.Usage)
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

func (in *cambricon) init() {
	in.once.Do(func() {
		if ret := in.cndev.Init(); !ret.IsSuccess() {
			in.logger.Error(ret, "failed to initialize CNDev library")
		}
	})
}

func isDeviceUnhealthy(dev cndev.Device) bool {
	healthHandler := dev.GetCardHealthStateV()
	return evaluateDeviceUnhealthy(healthHandler.V2, healthHandler.V1)
}

// evaluateDeviceUnhealthy 读取厂商健康状态，优先使用 V2 并可回退到 V1。查询失败本身不构成设备故障。
func evaluateDeviceUnhealthy(
	readV2 func() (cndev.CardHealthStateV2, cndev.Return),
	readV1 func() (cndev.CardHealthStateV2, cndev.Return),
) bool {
	if healthInfo, ret := readV2(); ret.IsSuccess() {
		return healthInfo.Health == 0
	}
	if healthInfo, ret := readV1(); ret.IsSuccess() {
		return healthInfo.Health == 0
	}
	return false
}

var versionRegexp = regexp.MustCompile(`\d+\.\d+\.\d+`)

func getRuntimeVersion() string {
	paths := []string{
		"/usr/local/neuware/version.txt",
	}
	for _, path := range paths {
		if _, err := os.Stat(path); err == nil {
			if data, err := os.ReadFile(path); err == nil {
				match := versionRegexp.FindString(strings.TrimSpace(string(data)))
				if match != "" {
					return match
				}
			}
			break
		}
	}
	return ""
}
