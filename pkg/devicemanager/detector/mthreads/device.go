package mthreads

import (
	"slices"
	"strings"
	"sync"

	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/binding"
	"gpustack.ai/gpustack/binding/mtml"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/utils/loggerx"
)

const Manufacturer = nodefeature.ManufacturerMThreads

var _PciVendor string

func init() {
	pciID := nodefeature.GetPciVendorID(Manufacturer)
	p := strings.Split(pciID, "_")
	_PciVendor = p[len(p)-1]
}

type mthreads struct {
	once   sync.Once
	mtml   *mtml.MTML
	logger klog.Logger
}

// New 创建摩尔线程探测器并配置 MTML 管理库。
func New(opts device.DetectorOptions) device.Detector {
	logger := opts.Logger.WithName(Manufacturer)
	return &mthreads{
		mtml:   mtml.New(binding.WithLogger(logger)),
		logger: logger,
	}
}

// Name 和 DetectAccelerator 实现厂商硬件枚举接口。
func (in *mthreads) Name() string {
	return Manufacturer
}

func (in *mthreads) DetectAccelerator(noPciCheck bool) (_ device.DevicesGroupList, err error) {
	defer loggerx.RecoverWithStackScanner(func(s loggerx.Scanner, e error) {
		in.logger.Error(e, "failed to detect mthreads devices")
		for s.Scan() {
			in.logger.Error(nil, s.Text())
		}
		err = e
	})

	pciDevs := binding.GetPCIDevices([]string{_PciVendor}, nil)
	if !noPciCheck && len(pciDevs) == 0 {
		in.logger.Info("no mthreads pci devices found")
		return nil, nil
	}

	in.init()

	cnt, ret := in.mtml.CountDevice()
	if !ret.IsSuccess() || cnt == 0 {
		if ret.IsSuccess() {
			in.logger.Info("no mthreads devices found")
		} else {
			in.logger.Error(ret, "no mthreads devices found")
		}
		return nil, nil
	}

	drVer, _ := in.mtml.SystemGetDriverVersion()

	var index uint32
	grpList := device.DevicesGroupList{}
	for i := 0; i < cnt; i++ {
		logger := in.logger.WithValues("index", i)

		dev, ret := in.mtml.InitDeviceByIndex(i)
		if !ret.IsSuccess() {
			logger.Error(ret, "failed to get device handle")
			continue
		}

		func() {
			defer func() {
				_ = dev.Free()
			}()

			prop, ret := dev.GetProperty()
			if !ret.IsSuccess() {
				logger.Error(ret, "failed to get device property")
				return
			}
			if mtml.VirtRole(prop.VirtCap) == mtml.VIRT_ROLE_GUEST_VIRTDEVICE {
				logger.Info("skipping virtual device")
				return
			}

			uuid, ret := dev.GetUUID()
			if !ret.IsSuccess() {
				logger.Error(ret, "failed to get device UUID")
				return
			}

			var pciBusId string
			{
				pciInfo, ret := dev.GetPciInfo()
				if !ret.IsSuccess() {
					logger.Error(ret, "failed to get device PCI Info")
					return
				}
				pciBusId = pciInfo.GetBusId()
			}
			pciDev := pciDevs[pciBusId]

			name, ret := dev.GetName()
			if !ret.IsSuccess() {
				logger.Error(ret, "failed to get device name")
				return
			}

			var (
				memory          uint64
				memoryUnhealthy bool
			)
			{
				memHandler, ret := dev.InitMemory()
				if !ret.IsSuccess() {
					logger.Error(ret, "failed to initialize memory handler")
					return
				}
				defer func() {
					_ = memHandler.Free()
				}()

				memTotal, ret := memHandler.GetTotal()
				if !ret.IsSuccess() {
					logger.Error(ret, "failed to get device memory total")
					return
				}
				memory = device.ConvertBytesToMiB(memTotal)
				memoryUnhealthy = isDeviceUnhealthy(memHandler)
			}

			grpIndex := slices.IndexFunc(grpList, func(grp device.DevicesGroup) bool {
				return grp.Name == name && grp.Memory == memory
			})
			if grpIndex == -1 {
				// 首次见到这一型号和显存容量，建立新组。
				cores, _ := dev.CountGpuCores()
				grpList = append(grpList, device.DevicesGroup{
					ID:            device.ConstructGroupID(Manufacturer, name, memory),
					Manufacturer:  Manufacturer,
					Name:          name,
					Memory:        memory,
					Cores:         cores,
					DriverVersion: drVer,
				})
				grpIndex = len(grpList) - 1
			}

			physicalIndexes := []uint32{uint32(i)}

			topo := device.ConstructTopology(pciBusId, pciDev.Root, pciDev.Class, pciDev.Switches)

			var status device.AcceleratorStatus
			{
				status.Unhealthy = memoryUnhealthy
				// 摩尔线程通过 sGPU 内核模块及 MTHREADS_QOS_* 环境变量提供每卡最多 16 份软切。计算配额表达相对权重，
				// 按不可超配发布。
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
		}()
	}

	device.SetGroupSlicedDetails(grpList)

	return grpList, nil
}

func isDeviceUnhealthy(memHandler mtml.MemoryHandler) bool {
	memEccDramUE, ret := memHandler.GetEccErrorCounter(
		mtml.MEMORY_ERROR_TYPE_UNCORRECTED,
		mtml.VOLATILE_ECC,
		mtml.MEMORY_LOCATION_DRAM,
	)
	return evaluateDeviceUnhealthy(memEccDramUE, ret)
}

// evaluateDeviceUnhealthy 根据厂商的不可纠正 ECC 错误计数判断设备状态，读取失败本身不能证明设备故障。
func evaluateDeviceUnhealthy(eccErrors uint64, ret mtml.Return) bool {
	return ret.IsSuccess() && eccErrors > 0
}

func (in *mthreads) init() {
	in.once.Do(func() {
		if ret := in.mtml.Init(); !ret.IsSuccess() {
			in.logger.Error(ret, "failed to initialize MTML library")
		}
	})
}
