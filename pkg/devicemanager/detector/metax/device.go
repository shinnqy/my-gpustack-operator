package metax

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"

	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/binding"
	"gpustack.ai/gpustack/binding/mxsml"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/utils/loggerx"
)

const Manufacturer = nodefeature.ManufacturerMetaX

var _PciVendor string

func init() {
	pciID := nodefeature.GetPciVendorID(Manufacturer)
	p := strings.Split(pciID, "_")
	_PciVendor = p[len(p)-1]
}

type metax struct {
	once   sync.Once
	mxsml  *mxsml.MXSML
	logger klog.Logger
}

// New 创建沐曦探测器并配置 MXSML 管理库。
func New(opts device.DetectorOptions) device.Detector {
	logger := opts.Logger.WithName(Manufacturer)
	return &metax{
		mxsml:  mxsml.New(binding.WithLogger(logger)),
		logger: logger,
	}
}

// Name 和 DetectAccelerator 实现厂商硬件枚举接口。
func (in *metax) Name() string {
	return Manufacturer
}

func (in *metax) DetectAccelerator(noPciCheck bool) (_ device.DevicesGroupList, err error) {
	defer loggerx.RecoverWithStackScanner(func(s loggerx.Scanner, e error) {
		in.logger.Error(e, "failed to detect metax devices")
		for s.Scan() {
			in.logger.Error(nil, s.Text())
		}
		err = e
	})

	pciDevs := binding.GetPCIDevices([]string{_PciVendor}, nil)
	if !noPciCheck && len(pciDevs) == 0 {
		in.logger.Info("no metax PCI devices found")
		return nil, nil
	}

	in.init()

	cnt := in.mxsml.GetDeviceCount()
	if cnt == 0 {
		in.logger.Info("no metax devices found")
		return nil, nil
	}

	drVer, _ := in.mxsml.GetDriverVersion()
	rtVer, _ := in.mxsml.GetMacaVersion()

	var index uint32
	grpList := device.DevicesGroupList{}
	for i := 0; i < cnt; i++ {
		logger := in.logger.WithValues("index", i)

		dev := in.mxsml.GetDeviceHandleByIndex(i)

		info, ret := dev.GetInfo()
		if !ret.IsSuccess() {
			logger.Error(ret, "failed to get device info")
			continue
		}
		if mxsml.DeviceVirtualizationMode(info.Mode) == mxsml.Virtualization_Mode_Vf {
			logger.Info("skipping virtual function device")
			continue
		}

		uuid := info.GetUUID()

		pciBusId := info.GetBusId()
		pciDev := pciDevs[pciBusId]

		name := info.GetDeviceName()

		var (
			memory          uint64
			memoryUnhealthy bool
		)
		{
			memInfo, ret := dev.GetMemoryInfo()
			if !ret.IsSuccess() {
				logger.Error(ret, "failed to get device memory info")
				continue
			}
			memory = device.ConvertKiBToMiB(memInfo.VramTotal)
			memoryUnhealthy = isDeviceUnhealthy(dev)
		}

		grpIndex := slices.IndexFunc(grpList, func(grp device.DevicesGroup) bool {
			return grp.Name == name && grp.Memory == memory
		})
		if grpIndex == -1 {
			// 首次见到这一型号和显存容量，建立新组。
			family := guessFamilyFromDeviceName(name)
			grpList = append(grpList, device.DevicesGroup{
				ID:             device.ConstructGroupID(Manufacturer, name, memory),
				Manufacturer:   Manufacturer,
				Name:           name,
				Memory:         memory,
				DriverVersion:  drVer,
				RuntimeVersion: device.NormalizeVersion(rtVer),
				Family:         family,
			})
			grpIndex = len(grpList) - 1
		}

		physicalIndexes := getPhysicalIndexes(pciBusId)

		topo := device.ConstructTopology(pciBusId, pciDev.Root, pciDev.Class, pciDev.Switches)

		var status device.AcceleratorStatus
		{
			status.Unhealthy = memoryUnhealthy
			// 沐曦通过 sysfs 的 sgpu/create 和 METAX_SGPUS 暴露逻辑设备，每卡最多 16 份。计算单元按空间切分，不能超配。
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

func isDeviceUnhealthy(dev mxsml.Device) bool {
	memEcc, ret := dev.GetTotalEccErrors()
	return evaluateDeviceUnhealthy(memEcc, ret)
}

// evaluateDeviceUnhealthy 依据不可纠正的 DRAM ECC 错误计数判断设备健康状态，读取失败本身不算故障。
func evaluateDeviceUnhealthy(eccErrors mxsml.EccErrorCount, ret mxsml.Return) bool {
	return ret.IsSuccess() && eccErrors.DramUE > 0
}

func (in *metax) init() {
	in.once.Do(func() {
		if ret := in.mxsml.Init(); !ret.IsSuccess() {
			in.logger.Error(ret, "failed to initialize MXSML library")
		}
	})
}

func getPhysicalIndexes(bdf string) []uint32 {
	var (
		cardID    *int
		renderDID *int
	)

	paths := []string{
		filepath.Join("/sys/module/metax/drivers/pci:METAX", bdf, "drm"),
		filepath.Join("/sys/module/metax/drivers/pci:metax", bdf, "drm"),
	}
	for _, drmPath := range paths {
		info, err := os.Stat(drmPath)
		if err != nil || !info.IsDir() {
			continue
		}
		entries, err := os.ReadDir(drmPath)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			name := entry.Name()
			if strings.HasPrefix(name, "card") {
				if id, err := strconv.Atoi(name[4:]); err == nil {
					cardID = &id
				}
			} else if strings.HasPrefix(name, "renderD") {
				if id, err := strconv.Atoi(name[7:]); err == nil {
					renderDID = &id
				}
			}
		}
		break
	}

	if cardID != nil {
		if renderDID != nil {
			return []uint32{uint32(*cardID), uint32(*renderDID)}
		}
		return []uint32{uint32(*cardID)}
	}
	return nil
}

var (
	_MXCRegex = regexp.MustCompile(`c\d{2,4}`)
	_MXNRegex = regexp.MustCompile(`n\d{2,4}`)
	_MXGRegex = regexp.MustCompile(`g\d{2,4}`)
)

func guessFamilyFromDeviceName(name string) string {
	if _MXCRegex.MatchString(name) {
		return "MXC"
	}
	if _MXNRegex.MatchString(name) {
		return "MXN"
	}
	if _MXGRegex.MatchString(name) {
		return "MXG"
	}
	return ""
}
