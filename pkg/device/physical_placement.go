package device

import (
	"cmp"
	"slices"
)

// ComputeRemainingProfiles 根据已占区间，逐个规格统计还能创建多少个不重叠的硬件分区。只返回仍有位置的规格，
// 整个过程只算区间，无需访问设备。
func ComputeRemainingProfiles(
	occupied []AcceleratorPlacement,
	possible map[string][]AcceleratorPlacement,
) map[string]int32 {
	remaining := make(map[string]int32, len(possible))
	for name, slots := range possible {
		var n int32
		for i := range slots {
			if !placementOverlapsAny(slots[i], occupied) {
				n++
			}
		}
		if n > 0 {
			remaining[name] = n
		}
	}
	return remaining
}

// PartitionCandidate 描述一张可能承载该分区的卡，包括规格允许的区间和已经被占用的区间。
// 选址算法只需一个可原样返回的 ID，无需知道调用方的数据结构或访问硬件。
type PartitionCandidate struct {
	// ID 是调用方识别这张卡的标识。
	ID string
	// Possible 按设备报告的顺序列出该规格在卡上的所有合法区间。
	Possible []AcceleratorPlacement
	// Occupied 包含已有分配占用的区间以及其他请求已预留的区间。
	Occupied []AcceleratorPlacement
}

// PartitionSelection 记录一个落定的分区实例将放在哪张卡、占用哪段显存。
type PartitionSelection struct {
	ID        string
	Placement AcceleratorPlacement
}

// SelectPartitionPlacements 从候选卡中为指定规格选择 count 个实例的位置，数量放不下就返回 false。
// 单次请求在每张卡上最多记录一个实例，两个 Pod 仍可以用同一张卡的不同区间。
//
// 放得下时优先填已用最多的卡，给后续大规格保留整卡。MIG 实例的显存带宽由硬件隔离，
// 无需为了带宽把小分区分散到每张卡上。卡内选择起点最低的空区间，尽量让高位空间连续，
// 相同输入也会得到相同结果。
func SelectPartitionPlacements(candidates []PartitionCandidate, count int) ([]PartitionSelection, bool) {
	if count <= 0 {
		return nil, true
	}

	used := make([]bool, len(candidates))
	selections := make([]PartitionSelection, 0, count)
	for range count {
		var (
			best     = -1
			bestSlot AcceleratorPlacement
			bestRoom int32
		)
		for i := range candidates {
			if used[i] {
				continue
			}
			slot, ok := lowestFreePlacement(candidates[i].Possible, candidates[i].Occupied)
			if !ok {
				continue
			}
			if room := occupiedLength(candidates[i].Occupied); best < 0 || room > bestRoom {
				best, bestSlot, bestRoom = i, slot, room
			}
		}
		if best < 0 {
			return nil, false
		}
		used[best] = true
		selections = append(selections, PartitionSelection{ID: candidates[best].ID, Placement: bestSlot})
	}
	return selections, true
}

// PartitionCandidateFull 检查该规格在卡上的所有合法位置是否都与已有占用重叠。和实际选址共用区间判断，
// 确保拒绝原因与选址结果一致。
func PartitionCandidateFull(candidate PartitionCandidate) bool {
	_, ok := lowestFreePlacement(candidate.Possible, candidate.Occupied)
	return !ok
}

// lowestFreePlacement 按探测器上报的起点升序返回第一个不与已占区间重叠的位置。
func lowestFreePlacement(
	possible, occupied []AcceleratorPlacement,
) (AcceleratorPlacement, bool) {
	for i := range possible {
		if !placementOverlapsAny(possible[i], occupied) {
			return possible[i], true
		}
	}
	return AcceleratorPlacement{}, false
}

// occupiedLength 累计所有已占区间的显存切片长度，用于比较卡的占用程度。
func occupiedLength(occupied []AcceleratorPlacement) int32 {
	var n int32
	for i := range occupied {
		n += occupied[i].Length
	}
	return n
}

// placementOverlapsAny 检查候选半开区间 [Start, Start+Length) 是否碰到已有区间。
// [a, a+m) 与 [b, b+n) 重叠当且仅当 a < b+n 且 b < a+m；首尾相接可以并存。
func placementOverlapsAny(slot AcceleratorPlacement, occupied []AcceleratorPlacement) bool {
	slotEnd := slot.Start + slot.Length
	for i := range occupied {
		occ := occupied[i]
		if slot.Start < occ.Start+occ.Length && occ.Start < slotEnd {
			return true
		}
	}
	return false
}

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
