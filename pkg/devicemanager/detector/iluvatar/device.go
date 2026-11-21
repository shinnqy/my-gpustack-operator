package iluvatar

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"

	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/binding"
	"gpustack.ai/gpustack/binding/ixml"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/utils/loggerx"
)

const Manufacturer = nodefeature.ManufacturerIluvatar

var _PciVendor string

func init() {
	pciID := nodefeature.GetPciVendorID(Manufacturer)
	p := strings.Split(pciID, "_")
	_PciVendor = p[len(p)-1]
}

type iluvatar struct {
	once   sync.Once
	ixml   *ixml.IXML
	logger klog.Logger
}

// New 创建天数智芯探测器并配置 IXML 管理库。
func New(opts device.DetectorOptions) device.Detector {
	logger := opts.Logger.WithName(Manufacturer)
	return &iluvatar{
		ixml:   ixml.New(binding.WithLogger(logger)),
		logger: logger,
	}
}

// Name 和 DetectAccelerator 实现厂商硬件枚举接口。
func (in *iluvatar) Name() string {
	return Manufacturer
}

func (in *iluvatar) DetectAccelerator(noPciCheck bool) (_ device.DevicesGroupList, err error) {
	defer loggerx.RecoverWithStackScanner(func(s loggerx.Scanner, e error) {
		in.logger.Error(e, "failed to detect iluvatar devices")
		for s.Scan() {
			in.logger.Error(nil, s.Text())
		}
		err = e
	})

	pciDevs := binding.GetPCIDevices([]string{_PciVendor}, nil)
	if !noPciCheck && len(pciDevs) == 0 {
		in.logger.Info("no iluvatar pci devices found")
		return nil, nil
	}

	in.init()

	cnt, ret := in.ixml.DeviceGetCount()
	if !ret.IsSuccess() || cnt == 0 {
		if ret.IsSuccess() {
			in.logger.Info("no iluvatar devices found")
		} else {
			in.logger.Error(ret, "no iluvatar devices found")
		}
		return nil, nil
	}

	drVer, _ := in.ixml.SystemGetDriverVersion()
	rtVer, _ := in.ixml.SystemGetCudaDriverVersion()

	var index uint32
	grpList := device.DevicesGroupList{}
	for i := 0; i < cnt; i++ {
		logger := in.logger.WithValues("index", i)

		dev, ret := in.ixml.DeviceGetHandleByIndex(i)
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
			pciInfo, ret := dev.GetPciInfo()
			if !ret.IsSuccess() {
				logger.Error(ret, "failed to get device PCI Info")
				continue
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

		ccMajor, ccMinor, _ := dev.GetCudaComputeCapability()

		grpIndex := slices.IndexFunc(grpList, func(grp device.DevicesGroup) bool {
			return grp.Name == name && grp.Memory == memory
		})
		if grpIndex == -1 {
			// 首次见到这一型号和显存容量，建立新组。
			grpList = append(grpList, device.DevicesGroup{
				ID:                device.ConstructGroupID(Manufacturer, name, memory),
				Manufacturer:      Manufacturer,
				Name:              name,
				Memory:            memory,
				DriverVersion:     drVer,
				RuntimeVersion:    stringifyRuntimeVersion(rtVer),
				ComputeCapability: stringifyComputeCapability(ccMajor, ccMinor),
			})
			grpIndex = len(grpList) - 1
		}

		// 次设备号用于确认 DRM 设备节点 /dev/dri/card<minor> 的身份。驱动无法报告时省略该值，枚举索引不能代替它。
		var physicalIndexes []uint32
		if minorNum, ret := dev.GetMinorNumber(); ret.IsSuccess() {
			physicalIndexes = []uint32{minorNum}
		} else {
			logger.V(3).Info("recorded no minor number for a card whose driver could not answer for it; "+
				"a consumer that names a device node after that number cannot address this card",
				"card", uuid, "reason", ret.Error())
		}

		topo := device.ConstructTopology(pciBusId, pciDev.Root, pciDev.Class, pciDev.Switches)

		var status device.AcceleratorStatus
		{
			status.Unhealthy = memoryUnhealthy
			// 天数智芯通过 HAMi-core 预加载运行库进行逻辑切分。原生 vcuda-core 按 1% 粒度计量，一整卡等于 100 份。
			status.LogicalSliced = device.AcceleratorLogicalSliced{
				Count:                     100,
				CoresPercentageOvercommit: true,
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

func isDeviceUnhealthy(dev ixml.Device) bool {
	memHealth, ret := dev.GetHealth()
	return evaluateDeviceUnhealthy(memHealth, ret)
}

// evaluateDeviceUnhealthy 根据驱动返回的健康状态判断设备是否异常，读取失败本身不算故障。
func evaluateDeviceUnhealthy(health uint64, ret ixml.Return) bool {
	return ret.IsSuccess() && health != ixml.HealthOK
}

func (in *iluvatar) init() {
	in.once.Do(func() {
		if ret := in.ixml.Init(); !ret.IsSuccess() {
			in.logger.Error(ret, "failed to initialize IXML library")
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
