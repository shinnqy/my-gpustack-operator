package detector

import (
	"cmp"
	"context"
	"math"
	"slices"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"
	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/utils/osx"
	"gpustack.ai/gpustack/pkg/utils/stringx"
)

var logger = klog.Background().WithName("detector")

// MonitorSnapshot 保存设备管理器最近一次采集的加速器指标。整个快照有独立的采样时间，
// 方便读取方统一判断新鲜度。
type MonitorSnapshot struct {
	// Timestamp 是保存快照的时间，首次采样前为零值。
	Timestamp time.Time `json:"timestamp"`
	// PeriodSeconds 记录采样时实际采用的周期，供读取方按周期判断数据是否过期。
	PeriodSeconds int64 `json:"periodSeconds"`
	// Groups 保存最近一次设备指标，首次采样前为空。
	Groups device.MetricsGroupList `json:"groups"`
	// Slices 保存各 Instance 在整卡指标中所占的份额。没有测量结果时省略。存在但为空代表本轮确实读到了空集合。
	// SchemaVersion 供读取方判断生产端是否支持该结构。
	Slices *MonitorSliceSection `json:"slices,omitempty"`
}

// Detector 依次采集各厂商的硬件清单，并分别保留最近一次成功的结果。
type Detector struct {
	noPCICheck   bool
	detectors    []device.Detector // 装着选中的厂商探测器
	lastDetected map[string]device.DevicesGroupList
}

// New 根据节点平台和请求的厂商构造探测器。在 WSL 环境自动跳过 PCI 预检查。
func New(c *Config) (*Detector, error) {
	// osx.GetRelease() 通过 uname 取得 Linux 内核版本字符串
	isWsl := stringx.ContainSubstrings(osx.GetRelease(), "microsoft", "wsl")
	noPCICheck := c.NoPCICheck || isWsl

	// 求交集。比如用户请求 {nvidia, ascend}，Linux 这份程序包含这两家，结果就是这两家。
	manufacturers := getAllowedManufacturers(c.Manufacturers)

	// [是 detector_linux.go 里的那个方法] 再从表里取出 nvidia.New 和 ascend.New
	creators := getAllowedDetectorCreators(manufacturers)
	createOpts := device.DetectorOptions{
		Logger: logger.V(3),
	}
	detectors := make([]device.Detector, 0, len(creators))
	for i := range creators {
		detectors = append(detectors, creators[i](createOpts))
	}

	return &Detector{noPCICheck: noPCICheck, detectors: detectors}, nil
}

// DetectAccelerator 逐厂商执行一次探测，返回合并后的设备组和本轮未能测量的厂商。
// 读取失败时深拷贝上一次成功结果，使下游不会因一次驱动异常而撤销已有库存。首次失败的厂商没有历史数据。
// 成功返回空清单才清掉该厂商的缓存，避免已拔出的卡在后续故障时重新出现。
// 本方法持有探测循环的跨轮状态，同一探测器上的调用应串行。
func (d *Detector) DetectAccelerator(
	_ context.Context,
) (grpListMerged device.DevicesGroupList, unmeasured sets.Set[string]) {
	unmeasured = sets.New[string]()

	for i := range d.detectors {
		manufacturer := d.detectors[i].Name()
		logger := logger.V(2).WithValues("manufacturer", manufacturer)

		grpList, err := d.detectors[i].DetectAccelerator(d.noPCICheck)
		if err != nil {
			logger.Error(err, "detect accelerators")
			unmeasured.Insert(manufacturer)
			if lastGrpList := d.lastDetected[manufacturer]; len(lastGrpList) != 0 {
				logger.Info("reporting the accelerators last detected")
				grpListMerged = append(grpListMerged, cloneDeviceGroups(lastGrpList)...)
			}
			continue
		}
		if len(grpList) == 0 {
			delete(d.lastDetected, manufacturer)
			continue
		}

		if d.lastDetected == nil {
			d.lastDetected = make(map[string]device.DevicesGroupList, len(d.detectors))
		}
		d.lastDetected[manufacturer] = grpList

		grpListMerged = append(grpListMerged, grpList...)
		logger.Info("detected accelerators")
	}

	return grpListMerged, unmeasured
}

// cloneDeviceGroups 深拷贝整组设备，避免使用者修改返回值后污染下一轮还要复用的缓存。
func cloneDeviceGroups(groups device.DevicesGroupList) device.DevicesGroupList {
	out := make(device.DevicesGroupList, len(groups))
	for i := range groups {
		groups[i].DeepCopyInto(&out[i])
	}
	return out
}

type (
	// _DeviceGroupKey 用厂商和组 ID 共同标识一个设备组。
	_DeviceGroupKey struct {
		Manufacturer string
		ID           string
	}
	// _DeviceGroupValue 保存组数据和待删除标志，供对齐时记录此次枚举是否仍包含该组。
	_DeviceGroupValue struct {
		Group  device.DevicesGroup
		Remove bool
	}
)

// alignDeviceGroups 将已有设备组与本轮实测组对齐。只移除当前负责厂商已经确认消失的组，
// 其余厂商的记录继续保留。返回值指明内容或存储顺序是否变化。
func alignDeviceGroups(
	aGroups, eGroups device.DevicesGroupList, allowedManufacturers sets.Set[string],
) (groups device.DevicesGroupList, changed bool) {
	// 先规范化两边的顺序，避免把随机枚举顺序误判为硬件变化。另存原始列表用于最后比较，
	// 这样旧列表顺序不规范时仍会得到一次修正。
	stored := aGroups
	aGroups = canonicalDeviceGroups(aGroups)
	eGroups = canonicalDeviceGroups(eGroups)

	// 以厂商和组 ID 两个字段共同索引旧组。
	devGrpIndex := make(map[_DeviceGroupKey]*_DeviceGroupValue)
	for i := range aGroups {
		k := _DeviceGroupKey{aGroups[i].Manufacturer, aGroups[i].ID}
		v := &_DeviceGroupValue{
			Group: aGroups[i],
			// 当前探测器不负责的厂商，默认保留旧记录。
			Remove: allowedManufacturers.Has(aGroups[i].Manufacturer),
		}
		devGrpIndex[k] = v
	}
	// 遍历实测组，匹配到旧组便检查更新，否则加入新组。
	groups = make(device.DevicesGroupList, 0, len(eGroups))
	for i := range eGroups {
		k := _DeviceGroupKey{eGroups[i].Manufacturer, eGroups[i].ID}
		if v, ok := devGrpIndex[k]; ok {
			// 本轮仍能看到这个组。保留它，并在内容变化时换成实测值。
			v.Remove = false
			if !kubemeta.DeepEqual(v.Group, eGroups[i]) {
				v.Group = eGroups[i]
				changed = true
			}
			continue
		}
		// 首次看到此组，把它加入结果。
		groups = append(groups, eGroups[i])
		changed = true
	}
	// 遍历旧组，跳过仍标记为删除的组，把保留项写入结果。
	for i := range aGroups {
		k := _DeviceGroupKey{aGroups[i].Manufacturer, aGroups[i].ID}
		v := devGrpIndex[k]
		if v.Remove {
			changed = true
			continue
		}
		groups = append(groups, v.Group)
	}

	// 新组此前先于旧组加入列表，最后统一排序，避免存储顺序依赖每个型号第一次出现的时间。
	groups = canonicalDeviceGroups(groups)

	return groups, changed || !kubemeta.DeepEqual(stored, groups)
}

// canonicalDeviceGroups 先按设备枚举索引排序组内卡，再按厂商、组内最小索引及组 ID 排序各组。
// 每个厂商独立编号，空组排在本厂商末尾。
// 复制组切片和每个加速器切片后再排序，调用方的旧列表保持原样。API 仍按标识关联对象，
// 稳定顺序仅用于持久化结果和判断变化。
func canonicalDeviceGroups(groups device.DevicesGroupList) device.DevicesGroupList {
	byIndex := func(a, b device.Accelerator) int { return cmp.Compare(a.Index, b.Index) }

	out := slices.Clone(groups)
	for i := range out {
		out[i].Accelerators = slices.Clone(out[i].Accelerators)
		slices.SortStableFunc(out[i].Accelerators, byIndex)
	}
	slices.SortStableFunc(out, func(a, b device.DevicesGroup) int {
		if c := cmp.Compare(a.Manufacturer, b.Manufacturer); c != 0 {
			return c
		}
		if c := cmp.Compare(firstAcceleratorIndex(a), firstAcceleratorIndex(b)); c != 0 {
			return c
		}
		return cmp.Compare(a.ID, b.ID)
	})

	return out
}

// firstAcceleratorIndex 返回组内最小枚举索引，空组返回最大索引，使其排在有卡的组之后。
func firstAcceleratorIndex(grp device.DevicesGroup) uint32 {
	if len(grp.Accelerators) == 0 {
		return math.MaxUint32
	}
	return slices.MinFunc(grp.Accelerators, func(a, b device.Accelerator) int {
		return cmp.Compare(a.Index, b.Index)
	}).Index
}
