package amd

import (
	"fmt"
	"sort"
	"strings"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/utils/mathx"
)

// Topology 保存 HSA agent-info 报告的单卡拓扑，供计算 CU 掩码使用。
//
// 五个字段都是值。读取设备由平台实现负责，下面的整数算法可用
// .claude/skills/gpustack-operator-xbuild-and-verify/references/amd-cumask-conformance.md 中的实测
// 数据核对。
type Topology struct {
	// Name 是 HSA 报告的 gfx 架构名，例如 gfx1101、gfx942、gfx90a。架构决定换算规则，无法识别时返回错
	// 误。
	Name string

	// CU 是 COMPUTE_UNIT_COUNT，也就是整卡的计算单元数。掩码的位编号就是 CU 编号。
	CU int

	// SE 是 NUM_SHADER_ENGINES，已包含所有 XCC。若 SE 为 32、NUM_XCC 为 8，则每个 XCC 有 4 个着色器引
	// 擎。
	SE int

	// SAPerSE 是 NUM_SHADER_ARRAYS_PER_SE，随拓扑一并记录，目前各计算分支都不读取它。
	SAPerSE int

	// XCC 是 NUM_XCC。零表示未读到该字段。CDNA/GCN 分支会在 Validate 中拒绝，RDNA 分支用不到它。
	XCC int
}

// Validate 检查能否根据该卡的拓扑生成有效掩码。
//
// binding/hsa 将未读到的字段置零。在必要字段为零的情况下继续计算，可能除以零，或生成 ROCr 静默丢弃
// 的掩码。后者没有报错、日志或返回码变化，容器却会失去算力限制。
func (t Topology) Validate() error {
	if t.CU <= 0 {
		return fmt.Errorf("cannot derive a CU mask for %q: it reports %d compute units", t.Name, t.CU)
	}
	cdna, known := t.family()
	if !known {
		return fmt.Errorf("cannot derive a CU mask for %q: unrecognised GPU architecture family, "+
			"only gfx9* (CDNA/GCN) and gfx10*/gfx11*/gfx12* (RDNA) are supported", t.Name)
	}
	if cdna {
		// CDNA 的最小分配单位取决于 XCC 数量。把未知数当作 1 会让窗口漏掉部分 XCC，遗漏的 XCC 可以不受掩码
		// 限制。实测单比特掩码看起来只有约 3.7% 吞吐，实际仍可访问 304 个 CU 中的 267 个。gfx90a 确实会报
		// 告 1，可以正常处理。
		if t.XCC <= 0 {
			return fmt.Errorf("cannot derive a CU mask for %q: it reports no XCC count, and on this "+
				"architecture an unknown one cannot be assumed to be 1 — a mask sized for one XCC "+
				"leaves every other XCC unmasked", t.Name)
		}
	} else {
		// RDNA 以 WGP 为单位，每个 WGP 是一对 CU。拆散其中一对会让整个掩码失效。因此必须有偶数个 CU，且
		// WGP 数要足够覆盖所有着色器引擎。此分支无需读取 XCC。
		if t.SE <= 0 {
			return fmt.Errorf("cannot derive a CU mask for %q: it reports %d shader engines", t.Name, t.SE)
		}
		if t.CU%2 != 0 {
			return fmt.Errorf("cannot derive a CU mask for %q: %d compute units is odd, and RDNA "+
				"allocates in WGP pairs", t.Name, t.CU)
		}
		if t.SE > t.CU/2 {
			return fmt.Errorf("cannot derive a CU mask for %q: %d shader engines exceed its %d WGPs",
				t.Name, t.SE, t.CU/2)
		}
	}
	// 窗口起点和长度都按同一步长对齐。整卡 CU 数必须能被这个步长整除，否则末尾的 CU 无法被完整覆盖。
	if q := t.Quantum(); t.CU%q != 0 {
		return fmt.Errorf("cannot derive a CU mask for %q: its %d-CU quantum does not divide %d "+
			"compute units", t.Name, q, t.CU)
	}
	return nil
}

// IsCDNAFamily 判断该型号是否采用 CDNA/GCN 的换算规则。
//
// 按 gfx 架构名选择规则。MI210 以及 MI250X 的每个 GCD 都是 gfx90a，属于 CDNA，却只报告一个 XCC。用
// XCC > 1 判断会误入 RDNA 分支，对没有 WGP 的卡套用配对规则。csrc/amd/rocm-slicing-shim/build.sh
// 也为 gfx90a 编译设备代码。
//
// 未知型号在此返回 false，由 Validate 拒绝该拓扑。
func (t Topology) IsCDNAFamily() bool {
	cdna, _ := t.family()
	return cdna
}

// Quantum 返回窗口起点和长度需要对齐的最小 CU 数。
//
// RDNA 轮流向每个着色器引擎分配一对 CU，完整一轮需要 2×SE 个 CU，未对齐的起点可能拆散 WGP，导致
// ROCr 丢弃整个掩码。CDNA 的第 i 位分给第 i mod X 个 XCC，一轮覆盖每个 XCC 恰好需要 NUM_XCC 个 CU。
//
// 仅对通过 Validate 的拓扑使用此结果。
func (t Topology) Quantum() int {
	if t.IsCDNAFamily() {
		return t.xcc()
	}
	return 2 * t.SE
}

// MinPercent 返回该卡能提供的最小整数百分比，即至少能形成一个完整窗口步长的百分比，供拒绝请求时提
// 示。
//
// 仅对通过 Validate 的拓扑使用此结果。
func (t Topology) MinPercent() int {
	units, atom := t.CU, t.xcc()
	if !t.IsCDNAFamily() {
		units, atom = t.CU/2, t.SE
	}
	// RoundDiv 在恰好一半时向上取整。要使取整结果达到一个完整分配单位，所求百分比为 ceil((100*atom -
	// 50) / units)，至少为 1。
	return mathx.CeilDiv(100*atom-50, units)
}

// WindowCUs 将请求的计算百分比换算成以 CU 计的窗口长度。
//
// 两类架构使用不同算法。给 CDNA 套 RDNA 的 CU 配对规则会把切片扩大一倍；给 RDNA 套 CDNA 的对齐规则
// 可能拆散一对 WGP 中的 CU，令整个掩码失效。
func WindowCUs(t Topology, pct int) (int, error) {
	if err := t.Validate(); err != nil {
		return 0, err
	}
	if pct <= 0 || pct > 100 {
		return 0, fmt.Errorf("cannot derive a CU mask for %q: %d%% is not a percentage of a card",
			t.Name, pct)
	}

	if t.IsCDNAFamily() {
		// 掩码各位交错落到 X 个 XCC 上，第 i 位属于第 i mod X 个 XCC。窗口至少应覆盖完整的一轮，否则漏掉的
		// XCC 仍可自由使用。实测掩码 0:0 可占用 304 个 CU 中的 267 个，尽管吞吐仅为整卡的约 3.7%。单 XCC
		// 的 gfx90a 步长为 1，允许任意长度的连续窗口。
		x := t.xcc()
		n := mathx.RoundDiv(t.CU*pct, 100) / x * x
		if n == 0 {
			return 0, fmt.Errorf("cannot derive a CU mask for %q: %d%% of %d CUs is below one "+
				"%d-CU atom on this card; its smallest slice is %d%%",
				t.Name, pct, t.CU, x, t.MinPercent())
		}
		return min(n, t.CU), nil
	}

	// RDNA 以 WGP 对分配 CU，并轮流分到各着色器引擎，未凑满一轮的余量不能增加吞吐。C 参考工具
	// rocm_cumask_check.c 的 derive_and_reexec 会把不足一轮的请求提升到一轮。这里须按申请计费，所以拒
	// 绝过小的请求。例如一张 60 CU、3 SE 的卡，不能把按 1% 计费的请求悄悄放大到 10%。
	wgps := t.CU / 2
	n := mathx.RoundDiv(wgps*pct, 100) / t.SE * t.SE
	if n == 0 {
		return 0, fmt.Errorf("cannot derive a CU mask for %q: %d%% of %d WGPs is below one "+
			"%d-WGP round on this card; its smallest slice is %d%%",
			t.Name, pct, wgps, t.SE, t.MinPercent())
	}
	return 2 * min(n, wgps), nil
}

// PackWindow 在给定卡和已有占用区间中，为长度为 length 个 CU 的窗口选择起点。
//
// 同一掩码位置分给两个容器时，它们会争用同一组 CU。先取起点最小且无重叠的对齐窗口。所有候选都有重
// 叠时，选择重叠 CU 数最少的窗口，同样多时取较小起点。
//
// 先合并占用区间。调用方同时读取进程内预留和 Pod 注解，可能重复看见同一次分配。合并可以避免重复计
// 数，但也无法区分同一窗口已经有一位还是两位持有者，这只影响没有空闲窗口时的回退选择。
//
// 窗口限制在 [0, CU) 内。传入长度大于整卡时按整卡处理，因为该函数没有错误返回值，ROCr 也会静默忽略
// 越界的掩码位。
func PackWindow(
	t Topology,
	length int,
	occupied []workercore.AcceleratorPlacement,
) workercore.AcceleratorPlacement {
	if length <= 0 || t.CU <= 0 {
		return workercore.AcceleratorPlacement{}
	}
	q := t.Quantum()
	if q <= 0 {
		q = 1
	}
	length = min(length, t.CU)

	covered := mergePlacements(occupied, t.CU)

	best, bestOverlap := 0, -1
	for start := 0; start+length <= t.CU; start += q {
		overlap := coveredCUs(covered, start, start+length)
		if overlap == 0 {
			return workercore.AcceleratorPlacement{
				Start:  int32(start),
				Length: int32(length),
			}
		}
		if bestOverlap < 0 || overlap < bestOverlap {
			best, bestOverlap = start, overlap
		}
	}
	return workercore.AcceleratorPlacement{
		Start:  int32(best),
		Length: int32(length),
	}
}

// Mask 生成 HSA_CU_MASK 的一个片段，格式为 <index>:<lo>-<hi>。index 是该卡在 ROCR_VISIBLE_DEVICES
// 中的位置，lo 和 hi 都是包含在窗口内的 CU 位号。
func Mask(index int, w workercore.AcceleratorPlacement) string {
	return fmt.Sprintf("%d:%d-%d", index, w.Start, w.Start+w.Length-1)
}

// family 根据 gfx 型号返回所属算法分支，并说明是否识别了该架构。
func (t Topology) family() (cdna, known bool) {
	switch {
	case strings.HasPrefix(t.Name, "gfx10"),
		strings.HasPrefix(t.Name, "gfx11"),
		strings.HasPrefix(t.Name, "gfx12"):
		return false, true
	case strings.HasPrefix(t.Name, "gfx9"):
		return true, true
	}
	return false, false
}

// xcc 将下限设为 1，使未校验的拓扑也能计算步长。是否允许缺少 XCC 数量由 Validate 决定，CDNA 仍会拒
// 绝缺失的值。
func (t Topology) xcc() int {
	return max(t.XCC, 1)
}

// mergePlacements 将已占用区间裁剪到 [0, cu)，排序并合并成互不重叠的区间。
func mergePlacements(
	in []workercore.AcceleratorPlacement,
	cu int,
) []workercore.AcceleratorPlacement {
	out := make([]workercore.AcceleratorPlacement, 0, len(in))
	for _, p := range in {
		start, end := max(int(p.Start), 0), min(int(p.Start)+int(p.Length), cu)
		if end <= start {
			continue
		}
		out = append(out, workercore.AcceleratorPlacement{
			Start:  int32(start),
			Length: int32(end - start),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Start < out[j].Start })

	merged := out[:0]
	for _, p := range out {
		if n := len(merged); n > 0 {
			last := &merged[n-1]
			if p.Start <= last.Start+last.Length {
				last.Length = max(last.Length, p.Start+p.Length-last.Start)
				continue
			}
		}
		merged = append(merged, p)
	}
	return merged
}

// coveredCUs 统计合并后的占用区间在 [start, end) 中覆盖了多少个 CU。
func coveredCUs(merged []workercore.AcceleratorPlacement, start, end int) int {
	total := 0
	for _, p := range merged {
		lo, hi := max(int(p.Start), start), min(int(p.Start)+int(p.Length), end)
		if hi > lo {
			total += hi - lo
		}
	}
	return total
}
