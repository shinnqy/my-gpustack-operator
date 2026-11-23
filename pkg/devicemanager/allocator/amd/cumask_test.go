package amd

import (
	"testing"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

// 测试拓扑取自 AMD CU 掩码一致性实测，参见
// .claude/skills/gpustack-operator-xbuild-and-verify/references/amd-cumask-conformance.md。
var (
	// gfx1101 属于 RDNA，60 个 CU、30 个 WGP、3 个 SE，每个 SE 含 2 个 SA，全卡 1 个 XCC。
	rdna = Topology{Name: "gfx1101", CU: 60, SE: 3, SAPerSE: 2, XCC: 1}
	// gfx942 属于 CDNA，304 个 CU、32 个 SE，每个 SE 含 1 个 SA，全卡 8 个 XCC。
	cdna = Topology{Name: "gfx942", CU: 304, SE: 32, SAPerSE: 1, XCC: 8}
	// gfx90a 对应 MI210，属于 CDNA，报告单个 XCC。
	cdnaSingleXCC = Topology{Name: "gfx90a", CU: 104, SE: 4, SAPerSE: 1, XCC: 1}
)

// derivedMask 是指空加速器上某一百分比对应的整个生产路径：其长度、位置以及渲染片段。符合性表中的
// mask 列正是如此。
func derivedMask(t *testing.T, topo Topology, pct int) string {
	t.Helper()

	length, err := WindowCUs(topo, pct)
	if err != nil {
		t.Fatalf("WindowCUs(%s, %d%%): unexpected error: %v", topo.Name, pct, err)
	}
	return Mask(0, PackWindow(topo, length, nil))
}

// TestPackWindow拦截那些窗口落在已持有部分资源的加速器上的情况。
func TestPackWindow(t *testing.T) {
	testCases := []struct {
		name      string
		topology  Topology
		length    int
		occupied  []workercore.AcceleratorPlacement
		wantStart int32
	}{
		{
			name:      "an empty accelerator places at zero",
			topology:  rdna,
			length:    12,
			wantStart: 0,
		},
		{
			name:      "an accelerator carrying one window places the next after it",
			topology:  rdna,
			length:    12,
			occupied:  []workercore.AcceleratorPlacement{{Start: 0, Length: 12}},
			wantStart: 12,
		},
		{
			name:     "a hole in the middle is reused ahead of the tail",
			topology: rdna,
			length:   6,
			occupied: []workercore.AcceleratorPlacement{
				{Start: 0, Length: 6},
				{Start: 12, Length: 6},
			},
			wantStart: 6,
		},
		{
			name:      "a full accelerator takes the least-overlapped start",
			topology:  rdna,
			length:    42,
			occupied:  []workercore.AcceleratorPlacement{{Start: 0, Length: 30}},
			wantStart: 18,
		},
		{
			// 占用联合会将预留间隔添加到注释间隔中，因此实际分配通常会出现两次。如果未合并，重复的分配会使得
			// start 0 的拥挤程度是 start 30 的3倍，并改变窗口位置；如果合并，每个 start 都会与相同的30 CUs重叠，
			// 此时由最低值决定。
			name:     "the same allocation twice does not bias the least-overlap choice",
			topology: rdna,
			length:   30,
			occupied: []workercore.AcceleratorPlacement{
				{Start: 0, Length: 60},
				{Start: 0, Length: 30},
				{Start: 0, Length: 30},
			},
			wantStart: 0,
		},
		{
			name:      "a CDNA window starts on an XCC atom boundary",
			topology:  cdna,
			length:    8,
			occupied:  []workercore.AcceleratorPlacement{{Start: 0, Length: 8}},
			wantStart: 8,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := PackWindow(tc.topology, tc.length, tc.occupied)
			if got.Start != tc.wantStart {
				t.Errorf("Start = %d, want %d", got.Start, tc.wantStart)
			}
			if got.Length != int32(tc.length) {
				t.Errorf("Length = %d, want %d", got.Length, tc.length)
			}
		})
	}
}

// TestPackWindow_QuantisedOnBothAxes：每个输出的窗口在两个轴方向上都是其分支配额的倍数，并且会保留
// 在加速器上；未进行量化处理的起始值会在 RDNA 上拆分 WGP 对，破坏 CDNA 上的 XCC 覆盖，甚至可能导致
// 整个遮罩的丢失。
func TestPackWindow_QuantisedOnBothAxes(t *testing.T) {
	testCases := []struct {
		name     string
		topology Topology
		occupied []workercore.AcceleratorPlacement
	}{
		{name: "RDNA, empty card", topology: rdna},
		{
			name:     "RDNA, partly occupied",
			topology: rdna,
			occupied: []workercore.AcceleratorPlacement{{Start: 6, Length: 12}},
		},
		{
			name:     "RDNA, full accelerator",
			topology: rdna,
			occupied: []workercore.AcceleratorPlacement{{Start: 0, Length: 60}},
		},
		{name: "CDNA, empty card", topology: cdna},
		{
			name:     "CDNA, full accelerator",
			topology: cdna,
			occupied: []workercore.AcceleratorPlacement{{Start: 0, Length: 304}},
		},
		{name: "single-XCC CDNA, empty card", topology: cdnaSingleXCC},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			q := int32(tc.topology.Quantum())
			for pct := 1; pct <= 100; pct++ {
				length, err := WindowCUs(tc.topology, pct)
				if err != nil {
					continue // 拒绝分配一个配额；由自身案例覆盖。
				}
				w := PackWindow(tc.topology, length, tc.occupied)
				if w.Start%q != 0 || w.Length%q != 0 {
					t.Errorf("%d%%: window %+v is not a multiple of the %d-CU quantum", pct, w, q)
				}
				if w.Start < 0 || int(w.Start+w.Length) > tc.topology.CU {
					t.Errorf("%d%%: window %+v runs off a %d-CU card", pct, w, tc.topology.CU)
				}
			}
		})
	}
}

// TestWindowCUs_ConformanceTableA 逐行复现了 RDNA 表。其中占比为 25% 和 75% 的行最为重要：简单的推
// 导结果分别得到了 0:0-14 和 0:0-44 的数据，且这两种方法都是基于硬件对整个加速器进行的测量。
func TestWindowCUs_ConformanceTableA(t *testing.T) {
	testCases := []struct {
		name string
		pct  int
		want string
	}{
		{name: "10 percent, 3 WGPs", pct: 10, want: "0:0-5"},
		{name: "20 percent, 6 WGPs", pct: 20, want: "0:0-11"},
		{name: "25 percent, 8 WGPs aligned down to 6", pct: 25, want: "0:0-11"},
		{name: "50 percent, 15 WGPs", pct: 50, want: "0:0-29"},
		{name: "75 percent, 23 WGPs aligned down to 21", pct: 75, want: "0:0-41"},
		{name: "100 percent, 30 WGPs", pct: 100, want: "0:0-59"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := derivedMask(t, rdna, tc.pct); got != tc.want {
				t.Errorf("mask = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestWindowCUs_ConformanceTableB 逐行复现了 CDNA 表格。
//
// 该表格中的百分比就是探针所运行的精确 CU 比例；一个请求会包含一个整数百分比，因此每一行的小数部分
// 都通过最接近相同原子计数的最小整数来确定——2.63 % -> 3 %, 5.26 % -> 6 %, 10.5 % -> 11 %。用于推导
// 的掩码列不受此影响，其内容保持不变。
func TestWindowCUs_ConformanceTableB(t *testing.T) {
	testCases := []struct {
		name string
		pct  int
		want string
	}{
		{name: "2.63 percent row, one atom", pct: 3, want: "0:0-7"},
		{name: "5 percent, 15 CUs aligned down to 8", pct: 5, want: "0:0-7"},
		{name: "5.26 percent row, two atoms", pct: 6, want: "0:0-15"},
		{name: "10.5 percent row, four atoms", pct: 11, want: "0:0-31"},
		{name: "50 percent, 152 CUs", pct: 50, want: "0:0-151"},
		{name: "100 percent, 304 CUs", pct: 100, want: "0:0-303"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := derivedMask(t, cdna, tc.pct); got != tc.want {
				t.Errorf("mask = %q, want %q", got, tc.want)
			}
		})
	}
}
