package detector

import (
	"time"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/device"
)

// MonitorSnapshotMaxAge 是未上报采样周期的快照可接受的最大存活时长，仅较旧版本的 device manager 会产生此类快照。
const MonitorSnapshotMaxAge = 45 * time.Second

// AllocatedAcceleratorMetric 包含单个已分配设备的指标及调用方用于打标签或建立索引的元数据：
// 所属指标分组的厂商名称，以及分配记录中登记的设备索引。
// 该索引取自分配记录而非仅包含 ID 的快照——它是厂商自身工具用来命名设备的序号，便于读取方将指标数值与宿主机上看到的设备对应起来。
type AllocatedAcceleratorMetric struct {
	Manufacturer string
	Index        uint32
	Metrics      device.AcceleratorMetrics
}

// MonitorSnapshotFresh 报告快照是否足够新鲜以用于上报指标。
//
// 快照若超过三个监控周期仍未更新，说明监控已发生故障：采样循环仅在成功获取非空样本后才会替换快照，
// 因此过期的快照反映的是最后一次成功时的状态，而非当前状态。
// 该时间上限随快照上报的周期按比例缩放，以免将较慢的采集频率误判为故障；当该字段缺失时回退为 MonitorSnapshotMaxAge。
func MonitorSnapshotFresh(snapshot *MonitorSnapshot) bool {
	if snapshot == nil {
		return false
	}

	maxAge := MonitorSnapshotMaxAge
	if snapshot.PeriodSeconds > 0 {
		maxAge = time.Duration(snapshot.PeriodSeconds) * time.Second * 3
	}
	return time.Since(snapshot.Timestamp) <= maxAge
}

// AllocatedAcceleratorMetricsOf 按厂商和设备 ID 从监控快照中筛选出分配记录对应的设备指标；若快照不再新鲜则返回空。
//
// 快照未包含的厂商不会为其设备产出任何指标。这使得对任意单个 device manager 的快照调用此函数都是安全的：
// chart 为每个厂商部署一个独立实例且各自仅采集自家加速卡，因此每个快照只响应其已知的设备，对其余设备保持静默而非报告缺失。
func AllocatedAcceleratorMetricsOf(
	snapshot *MonitorSnapshot,
	allocGroups []workercore.DevicesAllocationGroup,
) []AllocatedAcceleratorMetric {
	if !MonitorSnapshotFresh(snapshot) {
		return nil
	}

	allocatedByManufacturer := make(map[string]map[string]uint32, len(allocGroups))
	for i := range allocGroups {
		indexes := allocatedByManufacturer[allocGroups[i].Manufacturer]
		if indexes == nil {
			indexes = make(map[string]uint32)
			allocatedByManufacturer[allocGroups[i].Manufacturer] = indexes
		}
		for j := range allocGroups[i].Accelerators {
			indexes[allocGroups[i].Accelerators[j].ID] = allocGroups[i].Accelerators[j].Index
		}
	}

	var metrics []AllocatedAcceleratorMetric
	for i := range snapshot.Groups {
		grp := &snapshot.Groups[i]
		allocated, ok := allocatedByManufacturer[grp.Manufacturer]
		if !ok {
			continue
		}
		for j := range grp.Accelerators {
			am := &grp.Accelerators[j]
			index, ok := allocated[am.ID]
			if !ok {
				continue
			}
			metrics = append(metrics, AllocatedAcceleratorMetric{
				Manufacturer: grp.Manufacturer,
				Index:        index,
				Metrics:      *am,
			})
		}
	}
	return metrics
}
