package hygon

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
	"gpustack.ai/gpustack/binding/dmi"
	"gpustack.ai/gpustack/binding/hsa"
	"gpustack.ai/gpustack/binding/rsmi"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/utils/loggerx"
)

const Manufacturer = nodefeature.ManufacturerHygon

var _PciVendor string

func init() {
	pciID := nodefeature.GetPciVendorID(Manufacturer)
	p := strings.Split(pciID, "_")
	_PciVendor = p[len(p)-1]
}

type hygon struct {
	once   sync.Once
	amdgpu *amdgpu.AMDGPU
	rsmi   *rsmi.RSMI
	hsa    *hsa.HSA
	dmi    *dmi.DMI
	logger klog.Logger
}

// New 创建海光探测器，配置 RSMI 等硬件管理库。
func New(opts device.DetectorOptions) device.Detector {
	logger := opts.Logger.WithName(Manufacturer)
	return &hygon{
		amdgpu: amdgpu.New(binding.WithLogger(logger)),
		rsmi:   rsmi.New(binding.WithLogger(logger)),
		hsa:    hsa.New(binding.WithLogger(logger)),
		dmi:    dmi.New(binding.WithLogger(logger)),
		logger: logger,
	}
}

// Name 和 DetectAccelerator 实现厂商硬件枚举接口。
func (in *hygon) Name() string {
	return Manufacturer
}

func (in *hygon) DetectAccelerator(noPciCheck bool) (_ device.DevicesGroupList, err error) {
	defer loggerx.RecoverWithStackScanner(func(s loggerx.Scanner, e error) {
		in.logger.Error(e, "failed to detect hygon devices")
		for s.Scan() {
			in.logger.Error(nil, s.Text())
		}
		err = e
	})

	pciDevs := binding.GetPCIDevices([]string{_PciVendor}, nil)
	if !noPciCheck && len(pciDevs) == 0 {
		in.logger.Info("no hygon pci devices found")
		return nil, nil
	}

	in.init()

	cnt, ret := in.rsmi.GetDeviceCount()
	if !ret.IsSuccess() || cnt == 0 {
		if ret.IsSuccess() {
			in.logger.Info("no hygon devices found")
		} else {
			in.logger.Error(ret, "no hygon devices found")
		}
		return nil, nil
	}

	pciDevNames := binding.GetPCIDeviceNames([]string{_PciVendor})
	hsaAgts := in.hsa.GetAgents()

	// 每轮只查一次多实例模式。这个开关作用于整台节点，逐卡查询不能提供额外信息。
	migEnabled, migKnown := in.migModeEnabled()

	modelNames := make(modelNameCache)

	drVer := getDriverVersion()
	rtVer, _ := in.rsmi.GetROCMVersion()

	var index uint32
	grpList := device.DevicesGroupList{}
	for i := 0; i < cnt; i++ {
		logger := in.logger.WithValues("index", i)

		dev := in.rsmi.GetDeviceHandleByIndex(i)

		uuid, ret := dev.GetUniqueId()
		if !ret.IsSuccess() {
			logger.Error(ret, "failed to get device UUID")
			continue
		}

		var pciBusId string
		{
			pciBusId, ret = dev.GetPciId()
			if !ret.IsSuccess() {
				logger.Error(ret, "failed to get device PCI ID")
				continue
			}
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
			memTotal, ret := dev.GetMemoryTotal(rsmi.MEM_TYPE_VRAM)
			if !ret.IsSuccess() {
				logger.Error(ret, "failed to get device memory total")
				continue
			}
			memory = device.ConvertBytesToMiB(memTotal)
			memoryUnhealthy = isDeviceUnhealthy(dev)
		}

		physicalIndexes := getPhysicalIndexes(pciBusId)

		// 建组前先解析分区规格。多实例模式下整卡的计算单元数量需要由这些规格还原。
		var migCard migCardCapability
		if migEnabled {
			migCard = in.detectCardMigProfiles(pciBusId, logger)
		}

		name := modelNames.resolve(pciDev, func() string {
			n := pciDevNames.GetName(pciDev.Vendor, pciDev.Device, pciDev.SubVendor, pciDev.SubDevice)
			// 分区节点跳过 HSA 取名。HSA 能否在容器中初始化取决于是否有可见分区，同一物理节点重启后可能读到不同名称，
			// 进而错误发布新的 InstanceType 和队列。
			if n == "" && !migEnabled {
				n = hsaAgt.ProductName
			}
			if n == "" && len(physicalIndexes) != 0 {
				n = in.amdgpu.GetMarketingName(physicalIndexes[0])
			}
			if n == "" {
				// 最后尝试 RSMI。它在分区模式下仍能读取每张卡，最差可返回 PCI 设备编号，避免因空名称而无法发布设备组。
				// 可读的商品名还取决于 pci.ids。
				n, _ = dev.GetName()
			}
			return n
		})

		tgVer := hsaAgt.Name
		if tgVer == "" {
			tgVer, _ = dev.GetTargetGraphicsVersion()
		}

		grpIndex := slices.IndexFunc(grpList, func(grp device.DevicesGroup) bool {
			return grp.Name == name && grp.Memory == memory
		})
		if grpIndex == -1 {
			// 首次见到这一型号和显存容量，建立新组。
			var (
				cores  = hsaAgt.ComputeUnitCount
				family = hsaAgt.Family()
			)
			if migEnabled {
				// 此时 HSA 报告的是一个实例的计算单元数。整卡算力从分区规格还原。
				cores = migCard.cores
			}
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

			// 海光的软切与硬切由节点级模式互斥。启用多实例后，即使设备节点可见，没有计算实例配置的容器仍查不到设备。
			// 模式未能读出时保留未知，不发布不能兑现的能力，等待下一次实测。
			switch {
			case !migKnown:
			case migEnabled:
				status.PhysicalSliced = migCard.sliced
			default:
				// DCU 通过 hy-virtual 的 vdev.conf 和 DTK/hyhal 软切，每卡上限为 4 份。各份使用互不重叠的 CU 掩码，
				// 算力总量不能超配。
				status.LogicalSliced = device.AcceleratorLogicalSliced{
					Count: 4,
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

	// 同一组卡对同名规格给出不一致结果时，在按名称汇总之前剔除此规格。
	for i := range grpList {
		for _, reason := range rejectDivergentGroupProfiles(&grpList[i]) {
			in.logger.Info("withheld a partition profile from a group", "reason", reason)
		}
	}

	device.SetGroupSlicedDetails(grpList)

	return grpList, nil
}

func (in *hygon) MonitorAccelerator(noPciCheck bool) (_ device.MetricsGroupList, err error) {
	defer loggerx.RecoverWithStackScanner(func(s loggerx.Scanner, e error) {
		in.logger.Error(e, "failed to monitor hygon devices")
		for s.Scan() {
			in.logger.Error(nil, s.Text())
		}
		err = e
	})

	pciDevs := binding.GetPCIDevices([]string{_PciVendor}, nil)
	if !noPciCheck && len(pciDevs) == 0 {
		in.logger.Info("no hygon pci devices found")
		return nil, nil
	}

	in.init()

	cnt, ret := in.rsmi.GetDeviceCount()
	if !ret.IsSuccess() || cnt == 0 {
		if ret.IsSuccess() {
			in.logger.Info("no hygon devices found")
		} else {
			in.logger.Error(ret, "no hygon devices found")
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

		dev := in.rsmi.GetDeviceHandleByIndex(i)

		uuid, ret := dev.GetUniqueId()
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
			memTotal, ret := dev.GetMemoryTotal(rsmi.MEM_TYPE_VRAM)
			if !ret.IsSuccess() {
				logger.Error(ret, "failed to get device memory total")
				continue
			}
			memory = device.ConvertBytesToMiB(memTotal)

			memUsage, ret := dev.GetMemoryUsage(rsmi.MEM_TYPE_VRAM)
			if !ret.IsSuccess() {
				logger.V(3).Error(ret, "failed to get device memory usage")
			} else {
				memoryUsage = device.ConvertBytesToMiB(memUsage)
			}

			memoryUtilization = device.CalculateUtilization(memoryUsage, memory)
			memoryUnhealthy = isDeviceUnhealthy(dev)
		}

		var (
			coresUtilization uint32
			temperature      uint32
			powerUsage       uint32
		)
		{
			coresUtilization, ret = dev.GetBusyPercent()
			if !ret.IsSuccess() {
				logger.V(3).Error(ret, "failed to get device cores utilization")
			}

			temp, ret := dev.GetTempMetric(rsmi.TEMP_CURRENT)
			if !ret.IsSuccess() {
				logger.V(3).Error(ret, "failed to get device temperature")
			} else if temp > 0 {
				temperature = uint32(temp) / 1_000 // 将毫摄氏度转换为摄氏度。
			}

			power, ret := dev.GetPower()
			if !ret.IsSuccess() {
				logger.V(3).Error(ret, "failed to get device power usage")
			} else if power > 0 {
				powerUsage = uint32(power) / 1_000_000 // 从微瓦转换为瓦特。
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

func isDeviceUnhealthy(dev rsmi.Device) bool {
	memEcc, ret := dev.GetEccCount(rsmi.GPU_BLOCK_UMC)
	return evaluateDeviceUnhealthy(memEcc, ret)
}

// evaluateDeviceUnhealthy 根据厂商的不可纠正 ECC 错误计数判断设备状态，读取失败本身不能证明设备故障。
func evaluateDeviceUnhealthy(eccCount rsmi.ErrorCount, ret rsmi.Return) bool {
	return ret.IsSuccess() && eccCount.Uncorrectable_err > 0
}

// modelNameCache 按 PCI 型号缓存第一次成功读取的卡名。同一型号的多张卡必须使用同一组标识，
// 避免分成两个规格并拆散池容量。
type modelNameCache map[string]string

// resolve 首次遇到一个型号时通过 resolveOne 取名。空名称不缓存，使后续同型号卡仍有机会读到有效值。
func (c modelNameCache) resolve(pciDev binding.PCIDevice, resolveOne func() string) string {
	key := strings.Join([]string{pciDev.Vendor, pciDev.Device, pciDev.SubVendor, pciDev.SubDevice}, ":")
	if name, ok := c[key]; ok && name != "" {
		return name
	}
	name := resolveOne()
	if name != "" {
		c[key] = name
	}
	return name
}

func (in *hygon) init() {
	in.once.Do(func() {
		if ret := in.amdgpu.Init(); !ret.IsSuccess() {
			in.logger.Error(ret, "failed to initialize AMDGPU library")
		}
		if ret := in.rsmi.Init(); !ret.IsSuccess() {
			in.logger.Error(ret, "failed to initialize RSMI library")
		}
		if ret := in.hsa.Init(); !ret.IsSuccess() {
			in.logger.Error(ret, "failed to initialize HSA library")
		}
		// 多实例 DMI 库只随支持分区的驱动提供。常规节点缺少它时按调试级别记录，Init 自己提供具体加载原因。
		if ret := in.dmi.Init(in.logger.V(3)); !ret.IsSuccess() {
			in.logger.V(3).Info("hygon multi-instance library unavailable; the node cannot be partitioned",
				"return", ret)
		}
	})
}

func getDriverVersion() string {
	paths := []string{
		"/sys/module/hycu/version",
		"/sys/module/hydcu/version",
	}
	for _, path := range paths {
		if _, err := os.Stat(path); err == nil {
			if data, err := os.ReadFile(path); err == nil {
				return strings.TrimSpace(string(data))
			}
			break
		}
	}
	return ""
}

func getPhysicalIndexes(bdf string) []uint32 {
	var (
		cardID    *int
		renderDID *int
	)

	paths := []string{
		filepath.Join("/sys/module/hycu/drivers/pci:hycu", bdf, "drm"),
		filepath.Join("/sys/module/hydcu/drivers/pci:hydcu", bdf, "drm"),
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
