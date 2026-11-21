package thead

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"

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
