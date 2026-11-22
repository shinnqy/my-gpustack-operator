package device

// IsLogicallySliceable 报告加速器上报的能力是否支持逻辑（软件）切分——即不处于物理分区模式且至少提供一个逻辑切片。
// 两种能力均未上报的加速器返回 false。
//
// 各检测器对每个加速器会互斥地上报这两种能力，因此此处的分区检查在当前不会改变结果。
// 保留该检查是为了让此谓词自身强制执行所声明的契约，而非依赖每个检测器的实现：
// 即使某个加速器同时上报了两种能力，也只会归入分区设备族而不会同时提供两种模式，这正是拆分能力视图所要消除的重复宣称。
func IsLogicallySliceable(status AcceleratorStatus) bool {
	return status.LogicalSliced.Count > 0 && !IsPartitioned(status)
}

// IsPartitioned 报告加速器上报的能力是否处于物理（硬件）分区模式，即至少包含一个硬件分区配置。
// 两种能力均未上报的加速器返回 false。
func IsPartitioned(status AcceleratorStatus) bool {
	return status.PhysicalSliced.Count > 0
}

// IsWholeAcceleratorCapable 报告加速器能否满足独占或共享的整卡请求——即未进行物理分区。
// 两种能力均未上报的加速器返回 true，因为未分区的加速器无论是否支持逻辑切分，始终可以整卡形式使用。
func IsWholeAcceleratorCapable(status AcceleratorStatus) bool {
	return !IsPartitioned(status)
}

// LogicalSlotsFree 根据加速器当前已承载的切片数，报告其还能容纳多少个逻辑切片：即 LogicalSliced.Count 减去 allocatedSlices。
// 分配一个切片既需要空闲槽位也需要空闲资源单元，两者相互独立耗尽——一张装满小切片的加速卡仍会显示大部分显存空闲。
// 所有判断切片是否可容纳的层级都会读取此值，从而确保 DevicePlugin、准入检查、适配标签与 InstanceType 视图保持一致。
// 非正数表示已无剩余槽位，且不支持逻辑切分的加速器恒返回非正数。
func LogicalSlotsFree(status AcceleratorStatus, allocatedSlices int32) int32 {
	// 在第 10 章实现此函数。
	return 0
}
