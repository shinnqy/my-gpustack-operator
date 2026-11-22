package device

import (
	"cmp"
	"slices"
)

// ProfileCountSlice 将各分区规格的实例数按规格名排序，返回 []AcceleratorProfileCount。空表返回 nil，
// 序列化时省略该字段，使无分区设备沿用引入账本前的格式。
func ProfileCountSlice(counts map[string]int32) []AcceleratorProfileCount {
	if len(counts) == 0 {
		return nil
	}
	out := make([]AcceleratorProfileCount, 0, len(counts))
	for name, count := range counts {
		out = append(out, AcceleratorProfileCount{Name: name, Count: count})
	}
	slices.SortFunc(out, func(a, b AcceleratorProfileCount) int { return cmp.Compare(a.Name, b.Name) })
	return out
}

// PartitionLedgerReady 判断单卡的分配记录是否已发布可用的分区账本。记录缺失，
// 或同时缺少已分配和剩余实例时，表示账本尚未发布。设备管理器用缓存的分区布局重建运行时状态，
// 旧版本没有记录这份布局。
//
// 所有读取方在账本尚未就绪时都使用设备的静态容量上限。如果此时发布零，
// 正常节点上的卡也会被误判为已满。
func PartitionLedgerReady(alloc *AcceleratorAllocation) bool {
	return alloc != nil && (len(alloc.AllocatedProfiles) > 0 || len(alloc.RemainingProfiles) > 0)
}
