package device

import (
	"reflect"
	"testing"
)

// a100PossiblePlacements 是硬编码的 A100 合法放置集合（以内存切片单位表示的开始位置与长度），用于这
// 六个保留的配置文件——即自由计数算法所依赖的组件。
var a100PossiblePlacements = map[string][]AcceleratorPlacement{
	"1g.5gb":  {{Start: 0, Length: 1}, {Start: 1, Length: 1}, {Start: 2, Length: 1}, {Start: 3, Length: 1}, {Start: 4, Length: 1}, {Start: 5, Length: 1}, {Start: 6, Length: 1}},
	"1g.10gb": {{Start: 0, Length: 2}, {Start: 2, Length: 2}, {Start: 4, Length: 2}, {Start: 6, Length: 2}},
	"2g.10gb": {{Start: 0, Length: 2}, {Start: 2, Length: 2}, {Start: 4, Length: 2}},
	"3g.20gb": {{Start: 0, Length: 4}, {Start: 4, Length: 4}},
	"4g.20gb": {{Start: 0, Length: 4}},
	"7g.40gb": {{Start: 0, Length: 8}},
}

func TestComputeRemainingProfiles(t *testing.T) {
	testCases := []struct {
		name     string
		occupied []AcceleratorPlacement
		possible map[string][]AcceleratorPlacement
		want     map[string]int32
	}{
		{
			// 规范中的实际案例：一个已持有 1x3g.20gb@slot0（占用内存片段 [0,4]）的加速器仍可以构建精简集合。
			name:     "a100 with one 3g.20gb at slot 0",
			occupied: []AcceleratorPlacement{{Start: 0, Length: 4}},
			possible: a100PossiblePlacements,
			want:     map[string]int32{"1g.5gb": 3, "1g.10gb": 2, "2g.10gb": 1, "3g.20gb": 1},
		},
		{
			// 空的加速器可以生成所有配置文件，直至每个文件对应的合法放置数量上限。
			name:     "empty a100 accelerator",
			occupied: nil,
			possible: a100PossiblePlacements,
			want:     map[string]int32{"1g.5gb": 7, "1g.10gb": 4, "2g.10gb": 3, "3g.20gb": 2, "4g.20gb": 1, "7g.40gb": 1},
		},
		{
			// 第 0 个插槽的 1GB 5GB 内存（占用 [0,1] 范围）会减少所有仅存在低插槽放置的配置文件；无法构建的配置
			// 文件将被丢弃。
			name:     "a100 fragmented by one 1g.5gb at slot 0",
			occupied: []AcceleratorPlacement{{Start: 0, Length: 1}},
			possible: a100PossiblePlacements,
			want:     map[string]int32{"1g.5gb": 6, "1g.10gb": 3, "2g.10gb": 2, "3g.20gb": 1},
		},
		{
			// 完全被占用的加速器（whole-accelerator 7g.40gb）无法留下任何可构建的内容。
			name:     "a100 fully occupied by 7g.40gb",
			occupied: []AcceleratorPlacement{{Start: 0, Length: 8}},
			possible: a100PossiblePlacements,
			want:     map[string]int32{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := ComputeRemainingProfiles(tc.occupied, tc.possible)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("ComputeRemainingProfiles() mismatch\n got: %+v\nwant: %+v", got, tc.want)
			}
		})
	}
}

// TestSelectPartitionPlacements 涵盖了设备插件在 kubelet 替代下的决策：一个分区会落在哪个加速器上，
// 以及位于哪个位置。
func TestSelectPartitionPlacements(t *testing.T) {
	// card 在 A100 硬件上构建候选产品配置的文件位置。
	card := func(id, profile string, occupied ...AcceleratorPlacement) PartitionCandidate {
		return PartitionCandidate{ID: id, Possible: a100PossiblePlacements[profile], Occupied: occupied}
	}

	testCases := []struct {
		name       string
		candidates []PartitionCandidate
		count      int
		want       []PartitionSelection
		wantOK     bool
	}{
		{
			// 在空节点上运行一个实例，会采用第一个加速器的最低间隔。
			name:       "empty node places at the lowest interval",
			candidates: []PartitionCandidate{card("a", "3g.20gb"), card("b", "3g.20gb")},
			count:      1,
			want:       []PartitionSelection{{ID: "a", Placement: AcceleratorPlacement{Start: 0, Length: 4}}},
			wantOK:     true,
		},
		{
			// 选择一个选项最多允许每个加速器使用一个实例，因此两个实例的请求会使用两个加速器，无论第一个加速器还
			// 剩多少资源。
			name:       "two instances of one request use two accelerators",
			candidates: []PartitionCandidate{card("a", "3g.20gb"), card("b", "3g.20gb")},
			count:      2,
			want: []PartitionSelection{
				{ID: "a", Placement: AcceleratorPlacement{Start: 0, Length: 4}},
				{ID: "b", Placement: AcceleratorPlacement{Start: 0, Length: 4}},
			},
			wantOK: true,
		},
		{
			// 正在使用的加速器会优先处理，因此空的状态会保持完整，以便满足需要全部资源的配置文件需求。
			name: "an accelerator already in use is filled before an empty sibling",
			candidates: []PartitionCandidate{
				card("a", "3g.20gb"),
				card("b", "3g.20gb", AcceleratorPlacement{Start: 0, Length: 4}),
			},
			count:  1,
			want:   []PartitionSelection{{ID: "b", Placement: AcceleratorPlacement{Start: 4, Length: 4}}},
			wantOK: true,
		},
		{
			// 每个选择最多允许每个加速器运行一个实例，无论剩余资源多少：Pod的注释中记录了每个加速器的放置集，因
			// 此同一加速器上对同一请求的第二个实例不会被计入。
			name:       "a third instance has no accelerator left, even with room on both",
			candidates: []PartitionCandidate{card("a", "3g.20gb"), card("b", "3g.20gb")},
			count:      3,
			wantOK:     false,
		},
		{
			// 同一加速器上另一个 Pod 的实例情况不同：它拥有自己的记录，因此该加速器仍然可能是剩余空间的候选对象。
			name: "an accelerator another Pod partly holds still takes one instance",
			candidates: []PartitionCandidate{
				card("a", "3g.20gb", AcceleratorPlacement{Start: 0, Length: 4}),
			},
			count:  1,
			want:   []PartitionSelection{{ID: "a", Placement: AcceleratorPlacement{Start: 4, Length: 4}}},
			wantOK: true,
		},
		{
			// 对于不再符合要求的配置，会跳过已被活跃实例阻塞的加速器，由下一个空出实例处理该请求——在混合配置情况
			// 下，kubelet自身的选择无法解决问题。
			name: "an accelerator blocked for this profile is skipped",
			candidates: []PartitionCandidate{
				card("a", "7g.40gb", AcceleratorPlacement{Start: 0, Length: 1}),
				card("b", "7g.40gb"),
			},
			count:  1,
			want:   []PartitionSelection{{ID: "b", Placement: AcceleratorPlacement{Start: 0, Length: 8}}},
			wantOK: true,
		},
		{
			// 没有加速器可以承载它：不是某个加速器，而是该节点没有足够的空间。
			name: "a node with no free placement fails",
			candidates: []PartitionCandidate{
				card("a", "7g.40gb", AcceleratorPlacement{Start: 0, Length: 1}),
			},
			count:  1,
			wantOK: false,
		},
		{
			name:       "no candidates at all fails",
			candidates: nil,
			count:      1,
			wantOK:     false,
		},
		{
			name:       "a zero count places nothing",
			candidates: []PartitionCandidate{card("a", "3g.20gb")},
			count:      0,
			wantOK:     true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := SelectPartitionPlacements(tc.candidates, tc.count)
			if ok != tc.wantOK {
				t.Fatalf("SelectPartitionPlacements() ok = %v, want %v", ok, tc.wantOK)
			}
			if !tc.wantOK {
				return
			}
			if len(tc.want) == 0 && len(got) == 0 {
				return
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("SelectPartitionPlacements() mismatch\n got: %+v\nwant: %+v", got, tc.want)
			}
		})
	}
}

// TestSelectPartitionPlacementsPacksToPreserveWholeCards 展示了该策略实际执行时的原理：每个 Allocate
// 操作都是独立调用，之前放置的实例会成为下一个实例占用的集合的一部分。三个较小的分区依次到达时，会填
// 充一个加速器，而另一个分区则保持完整状态，以便后续更大的分区能够使用——如果将这些分区分散到两个加速
// 器上，就会导致无法放置。
func TestSelectPartitionPlacementsPacksToPreserveWholeCards(t *testing.T) {
	occupied := map[string][]AcceleratorPlacement{}
	candidatesFor := func(profile string) []PartitionCandidate {
		return []PartitionCandidate{
			{ID: "a", Possible: a100PossiblePlacements[profile], Occupied: occupied["a"]},
			{ID: "b", Possible: a100PossiblePlacements[profile], Occupied: occupied["b"]},
		}
	}

	for i := range 3 {
		got, ok := SelectPartitionPlacements(candidatesFor("1g.5gb"), 1)
		if !ok {
			t.Fatalf("small request %d could not be placed", i)
		}
		occupied[got[0].ID] = append(occupied[got[0].ID], got[0].Placement)
	}
	if len(occupied["b"]) != 0 {
		t.Errorf("packing must leave the sibling untouched, got %+v", occupied["b"])
	}

	got, ok := SelectPartitionPlacements(candidatesFor("7g.40gb"), 1)
	if !ok {
		t.Fatal("the whole-card profile must still fit on the untouched card")
	}
	if got[0].ID != "b" {
		t.Errorf("the whole-card profile must land on the untouched card, got %q", got[0].ID)
	}
}

// TestSelectPartitionPlacementsDoesNotMutateCandidates 指出，该决策不会修改调用者已占用的集合，因此
// 调用者在多次尝试中持有的账本不会发生偏移。
func TestSelectPartitionPlacementsDoesNotMutateCandidates(t *testing.T) {
	occupied := []AcceleratorPlacement{{Start: 0, Length: 4}}
	candidates := []PartitionCandidate{
		{ID: "a", Possible: a100PossiblePlacements["3g.20gb"], Occupied: occupied},
	}

	if _, ok := SelectPartitionPlacements(candidates, 1); !ok {
		t.Fatal("SelectPartitionPlacements() should place one more 3g.20gb")
	}
	if !reflect.DeepEqual(candidates[0].Occupied, []AcceleratorPlacement{{Start: 0, Length: 4}}) {
		t.Errorf("the candidate's occupied set was mutated: %+v", candidates[0].Occupied)
	}
	if len(occupied) != 1 {
		t.Errorf("the caller's backing slice grew to %d entries", len(occupied))
	}
}

func TestProfileCountSlice(t *testing.T) {
	testCases := []struct {
		name   string
		counts map[string]int32
		want   []AcceleratorProfileCount
	}{
		{
			// 非空映射会按名称排序，与映射的遍历顺序无关。
			name:   "sorted by name",
			counts: map[string]int32{"2g.10gb": 1, "1g.5gb": 3, "1g.10gb": 2},
			want: []AcceleratorProfileCount{
				{Name: "1g.10gb", Count: 2},
				{Name: "1g.5gb", Count: 3},
				{Name: "2g.10gb", Count: 1},
			},
		},
		{
			// 空映射会返回 nil，因此序列化时会省略 ledger 字段。
			name:   "empty map yields nil",
			counts: map[string]int32{},
			want:   nil,
		},
		{
			name:   "nil map yields nil",
			counts: nil,
			want:   nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := ProfileCountSlice(tc.counts)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("ProfileCountSlice() mismatch\n got: %+v\nwant: %+v", got, tc.want)
			}
		})
	}
}
