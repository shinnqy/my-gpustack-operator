package amd

import (
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

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

// unavailableReading 是 AMD 的库用来表示读取操作不可用时使用的数值，而非用于报告该操作。它属于`uint16`的
// 最大值，而 GpuMetrics 在报告中会利用这个数值来显示利用率和温度。此外，驱动程序也会将数值写入更宽的
// `uint32`电源字段中，而不是使用它们自己的最大值，因此同一个常量同时服务于这两个用途。
//
// 这些电源字段并未扩展至 uint32 的最大值：AMD 自己的绑定逻辑会将整个电源块与这个值进行比较，如果进行扩展
// 则无法正确捕获实际报告的数值。此后端中 ROCm SMI 端在扩展值处使用了一个不同的哨兵值——位于
// binding/rsmi/library_device.go 文件中的 kfdStatsInvalid——这两个值不可互换。
const unavailableReading uint32 = 0xFFFF

// isUnavailableReading 用于指示是否为那个哨兵值，而非实际测量值。
//
// 这是必要的，因为产生该值的调用成功执行了。SR-IOV 虚拟函数不会向其客户机暴露主机端的功率遥测数据，而是会
// 处理这个问题，而不是失败。同样，驱动程序未填充的度量字段也会如此。因此，仅检查返回代码的调用者会将
// 65535 视为利用率、温度或瓦特数。不会发生崩溃，这就是需要检查的原因：该数值会被纳入监控和调度中，仿佛它
// 是经过测量的一样。
func isUnavailableReading(v uint32) bool {
	return v == unavailableReading
}

func (in *amd) MonitorAccelerator(noPciCheck bool) (_ device.MetricsGroupList, err error) {
	defer loggerx.RecoverWithStackScanner(func(s loggerx.Scanner, e error) {
		in.logger.Error(e, "failed to monitor amd devices")
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

	grpList := device.MetricsGroupList{
		{
			Manufacturer: Manufacturer,
			Timestamp:    time.Now(),
			Accelerators: make([]device.AcceleratorMetrics, 0, len(devs)),
		},
	}
	for i := 0; i < len(devs); i++ {
		logger := in.logger.WithValues("index", i)

		dev := devs[i]

		asicInfo, ret := dev.GetGpuAsicInfo()
		if !ret.IsSuccess() {
			logger.Error(ret, "failed to get device ASIC info")
			continue
		}

		uuid := asicInfo.GetUniqueId()

		var (
			memory            uint64
			memoryUsage       uint64
			memoryUtilization uint32
			memoryUnhealthy   bool
		)
		{
			mem, ret := dev.GetGpuVramUsage()
			if !ret.IsSuccess() {
				logger.Error(ret, "failed to get device VRAM usage")
				continue
			}

			memory = uint64(mem.Total)
			memoryUsage = uint64(mem.Used)
			memoryUtilization = device.CalculateUtilization(memoryUsage, memory)
			memoryUnhealthy = isDeviceUnhealthy(dev)
		}

		var (
			coresUtilization uint32
			temperature      uint32
			powerUsage       uint32
		)
		{
			metricsInfo, ret := dev.GetGpuMetricsInfo()
			if !ret.IsSuccess() {
				logger.V(3).Error(ret, "failed to get device cores utilization and temperature")
			} else {
				// 仅在读取值为一时才会赋值：不可用的读取会保持变量原有的零值，而不是进行
				// 赋值后再进行修正。
				if v := uint32(metricsInfo.Average_gfx_activity); !isUnavailableReading(v) {
					coresUtilization = v
				}
				if v := uint32(metricsInfo.Temperature_hotspot); !isUnavailableReading(v) {
					temperature = v
				}
			}

			powerInfo, ret := dev.GetPowerInfo()
			if !ret.IsSuccess() {
				logger.V(3).Error(ret, "failed to get device power usage")
			} else {
				// 当前套接字功率，作为备用值的平均值，以及两者都不存在的位置——这就是虚函
				// 数为它们中的每一个所报告的内容。
				switch {
				case !isUnavailableReading(powerInfo.Current_socket_power):
					powerUsage = powerInfo.Current_socket_power
				case !isUnavailableReading(powerInfo.Average_socket_power):
					powerUsage = powerInfo.Average_socket_power
				}
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
