package amd

import (
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"

	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/binding"
	"gpustack.ai/gpustack/binding/amdgpu"
	"gpustack.ai/gpustack/binding/amdsmi"
	"gpustack.ai/gpustack/binding/hsa"
	"gpustack.ai/gpustack/binding/rsmi"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/utils/loggerx"
)

const Manufacturer = nodefeature.ManufacturerAMD

var _PciVendor string

func init() {
	pciID := nodefeature.GetPciVendorID(Manufacturer)
	p := strings.Split(pciID, "_")
	_PciVendor = p[len(p)-1]
}

type amd struct {
	once   sync.Once
	amdgpu *amdgpu.AMDGPU
	amdsmi *amdsmi.AMDSMI
	// ROCm SMI 用于进程与设备的对应查询。实测 AMD SMI 26.2.1 在 ROCm 7.2.0 上对运行中的进程也会返回 INVAL。
	// ROCm SMI 能按卡索引取得对应的进程数据，rocm-smi 也采用这条路径。
	rsmi   *rsmi.RSMI
	hsa    *hsa.HSA
	logger klog.Logger
}

// New 创建 AMD 探测器并配置它使用的管理库。
func New(opts device.DetectorOptions) device.Detector {
	logger := opts.Logger.WithName(Manufacturer)
	return &amd{
		amdgpu: amdgpu.New(binding.WithLogger(logger)),
		amdsmi: amdsmi.New(binding.WithLogger(logger)),
		rsmi:   rsmi.New(binding.WithLogger(logger)),
		hsa:    hsa.New(binding.WithLogger(logger)),
		logger: logger,
	}
}

// Name 和 DetectAccelerator 实现厂商硬件枚举接口。
func (in *amd) Name() string {
	return Manufacturer
}

func (in *amd) DetectAccelerator(noPciCheck bool) (_ device.DevicesGroupList, err error) {
	defer loggerx.RecoverWithStackScanner(func(s loggerx.Scanner, e error) {
		in.logger.Error(e, "failed to detect amd devices")
		for s.Scan() {
			in.logger.Error(nil, s.Text())
		}
		err = e
	})

	pciDevs := binding.GetPCIDevices([]string{_PciVendor}, nil)
	if !noPciCheck && len(pciDevs) == 0 {
		in.logger.Info("no amd pci devices found")
		return nil, nil
	}

	in.init()

	devs, ret := in.amdsmi.GetProcessorHandles()
	if !ret.IsSuccess() || len(devs) == 0 {
		if ret.IsSuccess() {
			in.logger.Info("no amd devices found")
		} else {
			in.logger.Error(ret, "no amd devices found")
		}
		return nil, nil
	}

	pciDevNames := binding.GetPCIDeviceNames([]string{_PciVendor})
	hsaAgts := in.hsa.GetAgents()

	rtVer, _ := in.amdsmi.GetROCMVersion()

	var index uint32
	grpList := device.DevicesGroupList{}
	for i := 0; i < len(devs); i++ {
		logger := in.logger.WithValues("index", i)

		dev := devs[i]

		asicInfo, ret := dev.GetGpuAsicInfo()
		if !ret.IsSuccess() {
			logger.Error(ret, "failed to get device ASIC info")
			continue
		}

		uuid := asicInfo.GetUniqueId()

		var pciBusId string
		{
			bdf, ret := dev.GetGpuDeviceBdf()
			if !ret.IsSuccess() {
				logger.Error(ret, "failed to get device PCI ID")
				continue
			}
			pciBusId = bdf.String()
		}
		pciDev := pciDevs[pciBusId]
		hsaAgt, ok := hsaAgts[pciBusId]
		if !ok {
			hsaAgt = hsaAgts[uuid]
		}

		var (
			memory          uint64
			memoryUnhealthy bool
		)
		{
			mem, ret := dev.GetGpuVramUsage()
			if !ret.IsSuccess() {
				logger.Error(ret, "failed to get device VRAM usage")
				continue
			}
			memory = uint64(mem.Total)
			memoryUnhealthy = isDeviceUnhealthy(dev)
		}

		physicalIndexes := getPhysicalIndexes(pciBusId)

		name := pciDevNames.GetName(pciDev.Vendor, pciDev.Device, pciDev.SubVendor, pciDev.SubDevice)
		if name == "" {
			name = hsaAgt.ProductName
			if name == "" && len(physicalIndexes) != 0 {
				name = in.amdgpu.GetMarketingName(physicalIndexes[0])
			}
			if name == "" {
				name = asicInfo.GetMarketName()
			}
		}

		tgVer := hsaAgt.Name
		if tgVer == "" {
			tgVer = asicInfo.GetTargetGraphicsVersion()
		}

		grpIndex := slices.IndexFunc(grpList, func(grp device.DevicesGroup) bool {
			return grp.Name == name && grp.Memory == memory
		})
		if grpIndex == -1 {
			// 首次见到这一型号和显存容量，建立新组。
			var drVer string
			if drInfo, ret := dev.GetGpuDriverInfo(); ret.IsSuccess() {
				drVer = drInfo.GetVersion()
			}
			var (
				cores  = hsaAgt.ComputeUnitCount
				family = hsaAgt.Family()
			)
			if cores == 0 {
				if len(physicalIndexes) != 0 {
					gpuInfo, ret2 := in.amdgpu.QueryGPUInfo(physicalIndexes[0])
					if ret2.IsSuccess() {
						cores = gpuInfo.Cu_active_number
						family = gpuInfo.Family()
					}
				}
			}
			grpList = append(grpList, device.DevicesGroup{
				ID:                device.ConstructGroupID(Manufacturer, name, memory),
				Manufacturer:      Manufacturer,
				Name:              name,
				Memory:            memory,
				Cores:             cores,
				DriverVersion:     drVer,
				RuntimeVersion:    device.NormalizeVersion(rtVer),
				Family:            family,
				ComputeCapability: tgVer,
			})
			grpIndex = len(grpList) - 1
		}

		topo := device.ConstructTopology(pciBusId, pciDev.Root, pciDev.Class, pciDev.Switches)

		var status device.AcceleratorStatus
		{
			status.Unhealthy = memoryUnhealthy

			// AMD 通过 ld.so.preload 提供逻辑切分，每卡发布 128 个注入令牌，真正限制请求的是显存预算。
			// CU 空间隔离由硬件与运行时支持的掩码决定，显存按请求单独计量。
			status.LogicalSliced = device.AcceleratorLogicalSliced{
				Count:                     128,
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

func isDeviceUnhealthy(dev amdsmi.Device) bool {
	memEcc, ret := dev.GetGpuEccCount(amdsmi.GPU_BLOCK_UMC)
	return evaluateDeviceUnhealthy(memEcc, ret)
}

// evaluateDeviceUnhealthy 根据厂商的不可纠正 ECC 错误计数判断设备状态，读取失败本身不能证明设备故障。
func evaluateDeviceUnhealthy(eccCount amdsmi.ErrorCount, ret amdsmi.Return) bool {
	return ret.IsSuccess() && eccCount.Uncorrectable_count > 0
}

func (in *amd) init() {
	in.once.Do(func() {
		if ret := in.amdgpu.Init(); !ret.IsSuccess() {
			in.logger.Error(ret, "failed to initialize AMDGPU library")
		}
		// 先加载 ROCm SMI，再加载 AMD SMI。ROCm 7.2.0 上反向加载会在 dlopen 内触发 SIGBUS。
		// 另需让 AMD SMI 以 RTLD_DEEPBIND 优先解析自身符号，防止全局已加载的 ROCm SMI 干扰插槽枚举。
		if ret := in.rsmi.Init(); !ret.IsSuccess() {
			in.logger.Error(ret, "failed to initialize ROCm SMI library")
		}
		if ret := in.amdsmi.Init(); !ret.IsSuccess() {
			in.logger.Error(ret, "failed to initialize AMDSMI library")
		}
		if ret := in.hsa.Init(); !ret.IsSuccess() {
			in.logger.Error(ret, "failed to initialize HSA library")
		}
	})
}

func getPhysicalIndexes(bdf string) []uint32 {
	var (
		cardID    *int
		renderDID *int
	)

	paths := []string{
		filepath.Join("/sys/module/amdgpu/drivers/pci:amdgpu", bdf, "drm"),
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
