package hygon

import (
	"bufio"
	"fmt"
	"io/fs"
	"math"
	"math/bits"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"gpustack.ai/gpustack/pkg/utils/osx"
)

// DTK 虚拟方案的限制范围：vdev ID 数量低于 200，且每个加速器最多可分配 20 个管道 ID（每个加速器的≤4 片片
// 限制由资源池在源头设定）。只有管道 ID 属于资源池——vdev ID 则是记录自身文件的序号，详见 allocateVdev。加
// 速器的计算单元以 128 位掩码形式表示（两个 cu_mask 字）。
const (
	maxVdevID  = 200
	maxPipeID  = 20
	cuWordBits = 64
)

// allocMu 实现了整个扫描过程的序列化 -> 验证 -> 分配 -> 写入循环。设备插件响应器在协调器的 allocateMutex
// 之外运行，而 kubelet 无法可靠地序列化分配操作（即并发的 Kueue 批次），因此若没有这一机制，Hygon 的两次
// 分配可能会读取到相同的磁盘槽集，导致 CU 掩码重叠或重复使用 vdev 标识符。
var allocMu sync.Mutex

// cuMask 是加速器 128 位计算单元位掩码，分为两个 64 位字，这些字被 vdev.conf 作为`cu_mask`保存。位 c 表示
// 计算单元 c：字 lo 覆盖 CU 0..63，字 hi 覆盖 CU 64..127。
type cuMask struct {
	lo, hi uint64
}

func (m cuMask) has(cu int) bool {
	if cu < cuWordBits {
		return m.lo&(uint64(1)<<uint(cu)) != 0
	}
	return m.hi&(uint64(1)<<uint(cu-cuWordBits)) != 0
}

func (m *cuMask) set(cu int) {
	if cu < cuWordBits {
		m.lo |= uint64(1) << uint(cu)
		return
	}
	m.hi |= uint64(1) << uint(cu-cuWordBits)
}

func (m cuMask) or(o cuMask) cuMask {
	return cuMask{lo: m.lo | o.lo, hi: m.hi | o.hi}
}

func (m cuMask) count() int {
	return bits.OnesCount64(m.lo) + bits.OnesCount64(m.hi)
}

// fullMask 是指给定 CU 数量所拥有的每个计算单元的掩码。已使用的掩码必须是其子集（超出加速器 CUs 范围的位
// 则属于损坏的记录）。
func fullMask(cores int) cuMask {
	var m cuMask
	for cu := 0; cu < cores; cu++ {
		m.set(cu)
	}
	return m
}

// sliceCUCount 将计算百分比转换为与加速器总 CU 数相对应的 CU 数量：使用 ceil 函数，以确保正百分比不会向下
// 取整为 0，其数值上限为加速器的总 CU 数。
func sliceCUCount(coresPct int, cores uint32) int {
	return min(int(math.Ceil(float64(coresPct)*float64(cores)/100)), int(cores))
}

// packCUMask 会选取 `[0, 核心数) 范围内最少的空闲计算单元数量（即 cuCount），这些单元未被占用，并返回新的
// （不重叠的）掩码。该操作会失败：如果占用的掩码指向的 CU 数量超过加速器所能支持的数量，或者空闲 CUs 的数
// 量少于 cuCount，则会出现错误，而非静默的重叠或短掩码情况。
func packCUMask(used cuMask, cuCount, cores int) (cuMask, error) {
	if cores > 2*cuWordBits {
		return cuMask{}, fmt.Errorf("card compute unit count %d exceeds the 128-bit cu_mask width", cores)
	}
	full := fullMask(cores)
	if used.lo&^full.lo != 0 || used.hi&^full.hi != 0 {
		return cuMask{}, fmt.Errorf("used cu_mask references a compute unit beyond the card's %d", cores)
	}
	var m cuMask
	picked := 0
	for cu := 0; cu < cores && picked < cuCount; cu++ {
		if used.has(cu) {
			continue
		}
		m.set(cu)
		picked++
	}
	if picked < cuCount {
		return cuMask{}, fmt.Errorf("insufficient free compute units: need %d, %d of %d free", cuCount, cores-used.count(), cores)
	}
	return m, nil
}

// vdevConf 是一个解析后的 vdev.conf 记录：这是加速器中的切片预留区域，DTK/hyhal 运行时会读取该区域，而磁
// 盘扫描器则将其视为被占用状态。
type vdevConf struct {
	pciBusID string
	mask     cuMask
	cuCount  int
	memMib   int64
	deviceID int
	vdevID   int
	pipeID   int
}

// render 按照 DTK/hyhal 运行时所需的确切模式对记录进行序列化。
func (c vdevConf) render() string {
	var b strings.Builder
	_, _ = fmt.Fprintf(&b, "PciBusId: %s\n", c.pciBusID)
	_, _ = fmt.Fprintf(&b, "cu_mask: 0x%016x\n", c.mask.lo)
	_, _ = fmt.Fprintf(&b, "cu_mask: 0x%016x\n", c.mask.hi)
	_, _ = fmt.Fprintf(&b, "cu_count: %d\n", c.cuCount)
	_, _ = fmt.Fprintf(&b, "mem: %d MiB\n", c.memMib)
	_, _ = fmt.Fprintf(&b, "device_id: %d\n", c.deviceID)
	_, _ = fmt.Fprintf(&b, "vdev_id: %d\n", c.vdevID)
	_, _ = fmt.Fprintf(&b, "pipe_id: %d\n", c.pipeID)
	b.WriteString("enable: 1\n")
	return b.String()
}

// parseVdevConf 读取 vdev.conf 文件时采用失败关闭模式：缺失、重复、格式错误或超出范围的字段会导致错误，因
// 此扫描器不会通过跳过无法读取的记录来静默释放活跃切片中的 vdev_id / pipe_id / CU 掩码。
func parseVdevConf(path string) (vdevConf, error) {
	f, err := os.Open(path)
	if err != nil {
		return vdevConf{}, err
	}
	defer func() { _ = f.Close() }()

	var (
		c     vdevConf
		masks []uint64
		seen  = map[string]bool{}
	)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}
		key, val, found := strings.Cut(line, ":")
		if !found {
			return vdevConf{}, fmt.Errorf("vdev.conf %q: malformed line %q", path, line)
		}
		key, val = strings.TrimSpace(key), strings.TrimSpace(val)
		if key != "cu_mask" && seen[key] {
			return vdevConf{}, fmt.Errorf("vdev.conf %q: duplicate key %q", path, key)
		}
		seen[key] = true

		switch key {
		case "PciBusId":
			c.pciBusID = val
		case "cu_mask":
			m, err := strconv.ParseUint(strings.TrimPrefix(val, "0x"), 16, 64)
			if err != nil {
				return vdevConf{}, fmt.Errorf("vdev.conf %q: bad cu_mask %q: %w", path, val, err)
			}
			masks = append(masks, m)
		case "cu_count":
			if c.cuCount, err = strconv.Atoi(val); err != nil {
				return vdevConf{}, fmt.Errorf("vdev.conf %q: bad cu_count %q: %w", path, val, err)
			}
		case "mem":
			num, _, _ := strings.Cut(val, " ")
			if c.memMib, err = strconv.ParseInt(num, 10, 64); err != nil {
				return vdevConf{}, fmt.Errorf("vdev.conf %q: bad mem %q: %w", path, val, err)
			}
		case "device_id":
			if c.deviceID, err = strconv.Atoi(val); err != nil {
				return vdevConf{}, fmt.Errorf("vdev.conf %q: bad device_id %q: %w", path, val, err)
			}
		case "vdev_id":
			if c.vdevID, err = strconv.Atoi(val); err != nil {
				return vdevConf{}, fmt.Errorf("vdev.conf %q: bad vdev_id %q: %w", path, val, err)
			}
		case "pipe_id":
			if c.pipeID, err = strconv.Atoi(val); err != nil {
				return vdevConf{}, fmt.Errorf("vdev.conf %q: bad pipe_id %q: %w", path, val, err)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return vdevConf{}, fmt.Errorf("vdev.conf %q: %w", path, err)
	}

	// 只需要以下字段：PciBusId、两个 cu_mask 字段、cu_count、vdev_id、pipe_id。`mem`和 device_id 属于
	// 信息性字段——生成过程始终会输出它们，但用于重建已使用 vdev/pipe/CU 设置的扫描结果不会读取这些字
	// 段——因此在此处不强制要求这些字段存在。
	if c.pciBusID == "" || len(masks) != 2 || !seen["cu_count"] || !seen["vdev_id"] || !seen["pipe_id"] {
		return vdevConf{}, fmt.Errorf("vdev.conf %q: incomplete record", path)
	}
	if c.vdevID < 0 || c.vdevID >= maxVdevID || c.pipeID < 0 || c.pipeID >= maxPipeID {
		return vdevConf{}, fmt.Errorf("vdev.conf %q: vdev_id %d / pipe_id %d out of range", path, c.vdevID, c.pipeID)
	}
	c.mask = cuMask{lo: masks[0], hi: masks[1]}
	return c, nil
}

// scanVdevConfs 会解析 pod 目录下的每个 vdev.conf 文件（位于 <podsDir>/<podUID>/c-<container>
// /etc/vdev/docker/vdev*.conf 路径下），但会跳过 selfPath。该工具采用失败关闭机制：如果遇到无法读取的目录
// 或损坏的记录，则整个扫描过程会失败，不会被跳过——因为这两种情况都会默默释放一个可用的资源，导致后续分配
// 时该资源被占用。特意避免了 filepath.Glob 方法，因为该方法会吞没目录读取错误（仅报告 ErrBadPattern 错误），
// 这会导致一个可用的配置文件被丢弃。
func scanVdevConfs(podsDir, selfPath string) ([]vdevConf, error) {
	confs := make([]vdevConf, 0)
	err := filepath.WalkDir(podsDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			// `pods dir`缺失表示尚未有切片。任何其他读取错误则会导致失败关闭。
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() || path == selfPath {
			return nil
		}
		name := d.Name()
		if !strings.HasPrefix(name, "vdev") || !strings.HasSuffix(name, ".conf") {
			return nil
		}
		c, perr := parseVdevConf(path)
		if perr != nil {
			return perr
		}
		confs = append(confs, c)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return confs, nil
}

// allocateVdev 从磁盘扫描中获取加速器 vdev.conf 的槽位，并在 selfPath 处原子性地将其发布。该操作是幂等的—
// —对于同一加速器重新分配相同的 selfPath，会复用现有记录——且基于层级：死亡容器占用的工作目录会在下一次扫
// 描中释放其槽位，因此无需使用内存中的计数器或释放回调。整个扫描 -> 验证 -> 分配 -> 写入过程由 allocMu 处
// 理，因为并发的分配操作在上游并未进行序列化。
func allocateVdev(podsDir, selfPath, pciBusID string, cores uint32, coresPct int, memMib int64, deviceID int) (vdevConf, error) {
	allocMu.Lock()
	defer allocMu.Unlock()

	// 对同一加速器而言，对现有的自配置项进行幂等式复用。同时会检查 vdev ID，因此那些在 vdev ID 出现之
	// 前写入的记录，会替换而不是复用，因为运行时会拒绝这种记录，详见下文。
	if c, err := parseVdevConf(selfPath); err == nil && c.pciBusID == pciBusID && c.vdevID == deviceID {
		return c, nil
	}

	confs, err := scanVdevConfs(podsDir, selfPath)
	if err != nil {
		return vdevConf{}, err
	}

	// 汇总已使用的槽位：CU 位和管道 ID 是按加速器划分的，因此只有同一卡片的记录才会产生贡献。Vdev ID
	// 则完全不进行汇总——详见下文。
	var usedCU cuMask
	usedPipe := make(map[int]bool)
	for i := range confs {
		if confs[i].pciBusID == pciBusID {
			usedCU = usedCU.or(confs[i].mask)
			usedPipe[confs[i].pipeID] = true
		}
	}

	n := sliceCUCount(coresPct, cores)
	if n <= 0 {
		return vdevConf{}, fmt.Errorf("card %s: sliced request resolves to zero compute units (cores-percent=%d, card cores=%d)", pciBusID, coresPct, cores)
	}
	mask, err := packCUMask(usedCU, n, int(cores))
	if err != nil {
		return vdevConf{}, err
	}
	// vdev ID 是记录自身的序号，并非从资源池中取用的槽位。DTK/hyhal 运行时会根据读取记录时文件名中的
	// 序号进行校验——vdev<N>.conf 必须包含 vdev_id 为 N——如果两者不符，则不会导致切片降级：此时容器根
	// 本没有加速器可用（“没有 HIP GPUs 可供使用”），该情况是在 8-DCU 主机上进行的测量。如果采用节点级
	// 资源池，则节点上的第二个 Pod 会获得与其文件名相矛盾的 ID，因为每个 Pod 都会从零开始为自己的配置
	// 文件编号。
	//
	// 不需要实现 Node 级的唯一性来弥补这一点：在一张卡上，两个分别包含 vdev_id 为 0 的容器并排运行，
	// 运行时系统通过容器 ID 来区分它们，并对其实例进行编号（在 kfd vgpu sysfs 下为 0x<gpu_id>@0 和 @1）。
	// 管道 ID 是必须避免在同一张卡上冲突的，它们仍然来自上述扫描结果。
	vdevID := deviceID
	if vdevID < 0 || vdevID >= maxVdevID {
		return vdevConf{}, fmt.Errorf("card %s: device ordinal %d is not a usable vdev_id", pciBusID, vdevID)
	}
	pipeID, err := lowestFreeSlot(usedPipe, maxPipeID)
	if err != nil {
		return vdevConf{}, fmt.Errorf("pipe_id pool exhausted for card %s: %w", pciBusID, err)
	}

	c := vdevConf{
		pciBusID: pciBusID,
		mask:     mask,
		cuCount:  mask.count(),
		memMib:   memMib,
		deviceID: deviceID,
		vdevID:   vdevID,
		pipeID:   pipeID,
	}
	if err := writeVdevConf(selfPath, c); err != nil {
		return vdevConf{}, err
	}
	return c, nil
}

// lowestFreeSlot 返回 [0, limit) 区间中最低索引值，如果该索引不在已使用的资源中，则返回错误值，或者当资源
// 池耗尽时返回错误值。
func lowestFreeSlot(used map[int]bool, limit int) (int, error) {
	for i := 0; i < limit; i++ {
		if !used[i] {
			return i, nil
		}
	}
	return 0, fmt.Errorf("no free slot below %d", limit)
}

// writeVdevConf 会持久性地发布 vdev.conf：并发扫描器不会读取部分记录，且已写入的记录在异常关闭后仍能保留。
func writeVdevConf(path string, c vdevConf) error {
	dir := filepath.Dir(path)
	if err := osx.MkdirAll(dir, 0o777); err != nil {
		return fmt.Errorf("create vdev.conf dir %q: %w", dir, err)
	}
	data := []byte(c.render())
	if err := osx.DurableWrite(path, data, 0o644); err != nil {
		return fmt.Errorf("write vdev.conf %q: %w", path, err)
	}
	return nil
}
