package nvidia

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	core "k8s.io/api/core/v1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/deviceplugin"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/utils/json"
	"gpustack.ai/gpustack/pkg/utils/osx"
)

// markerName 是 Pod 工作目录下记录 MIG 归属的文件基名。每个容器、每张卡各有一份，重启后可据此识别已
// 经绑定的分区，并回收准确的 GPU Instance 和 Compute Instance。MetaX 和寒武纪也分别保存
// metax-sgpu.json、cambricon-smlu.json。
const markerName = "nvidia-mig.json"

// migPlacement 是加速器上的一段内存切片区间 [开始位置, 开始位置+长度)，这是插槽选取和标记所依赖的与平台无
// 关的放置几何结构，因此纯核心模块可以在 Darwin 上进行测试。NVML 放置类型则保留在 _linux 接口中。
type migPlacement struct {
	Start  int32
	Length int32
}

// migInstance 是加速器上存在一个实时运行或新创建的 MIG 分区：其 GPU 实例和计算实例的 ID、计算切片数量（该
// 数量与放置长度共同决定了规格几何结构）、内存切片位置，以及作为 NVIDIA_VISIBLE_DEVICES 注入的 MIG 设备
// UUID。
type migInstance struct {
	GiID          uint32
	CiID          uint32
	ComputeSlices int32
	Placement     migPlacement
	UUID          string
}

// migCardState 是加速器的实时 MIG 状态，用于某个规格进行读取：该规格中合法的空加速器位置，以及加速器上所
// 有活跃的 GPU 实例（涵盖所有规格）。核心逻辑会从“可能位置”中排除已占用的活跃实例，以选择空闲位置，并遍历
// “活跃实例”以寻找可重新使用的未绑定实例。
type migCardState struct {
	Possible []migPlacement
	Live     []migInstance
}

// errInstanceInUse 表示被 NVML_ERROR_IN_USE 拒绝的驱动程序正在被销毁——某个残留进程仍持有该实例。回收循环
// 将其视为有边界的、可重试的部分失败情况（不会清除去抖机制），并在边界处显示给操作员可见的日志。_linux
// seam 会对其进行包装，因此与平台无关的回收核心可以通过 errors.Is 进行测试。
var errInstanceInUse = errors.New("mig instance in use")

// errNoAddressableDevice 表示某个分区没有 MIG 设备句柄可以寻址：要么是没有计算实例的 GPU 实例，要么是驱动
// 程序的 MIG 设备无法访问的容器。以这种方式读取加速器中的每个分区。这是回收循环允许销毁所涉及的那个进程查
// 询失败——那里没有任何可询问的内容，与那些被询问但未得到响应的驱动程序形成对比。_linux 的 seam 会处理这个
// 问题，因此与平台无关的回收核心可以通过 errors.Is 进行测试。
var errNoAddressableDevice = errors.New("no mig device addresses the instance")

// migLiveInstance 是用于回收的全局枚举的实时 GPU 实例：其加速器 UUID 加上该实例。它使得无标记的 GPU 实例
// 能够在任何加速器上被孤儿 GC 发现（包括完全没有标记的加速器），而无需针对每个加速器提供规格说明。
//
// Card 字段名称已冻结：它仅在 mig_driver_linux.go 中作为带键字面量构建，而非 Linux 构建版本无法编译该文件，
// 因此错过重命名操作不会在本地产生问题。
type migLiveInstance struct {
	Card string
	Inst migInstance
}

// migDriver 是 _linux.go/_other.go 构建标签背后的 NVML 执行器接口：真正的实现负责在 Linux 上进行绑定/nvml
// 处理。由于存在 Darwin stub 错误，因此纯化的标记/槽位选择核心通过假驱动进行表测试。每个操作都通过 GPU
// UUID（即 NVIDIA 的运算符加速器 ID）来识别加速器。CardState/CreateInstance 函数会获取规格文件的几何信息，
// 因为仅通过计算切片数量无法确定相同计算环境的 REV 规格文件 ID（1g.5gb 与 1g.10gb）——该接口根据名称与规格
// 文件匹配。
type migDriver interface {
	// CardState 会读取给定规格下加速器的实时 MIG 状态（包括其合法的空加速器配置以及加速器上的所有活跃
	// GPU 实例）。
	CardState(cardUUID, profile string, computeSlices, memorySlices int32) (migCardState, error)
	// CreateInstance 会根据规格在插槽中实例化 GPU 实例及其完整的 GI 计算实例，并返回所创建的分区信息
	// （ID、放置位置、MIG UUID）。
	CreateInstance(cardUUID, profile string, computeSlices, memorySlices int32, slot migPlacement) (migInstance, error)
	// DestroyInstance 会销毁分区的计算实例以及其 GPU 实例。当仍有进程阻塞销毁过程时，它会返回包含
	// errInstanceInUse 的错误信息。
	DestroyInstance(cardUUID string, inst migInstance) error
	// ListInstances 会枚举所有具备 MIG 能力的加速器中的活跃 GPU 实例，每个实例都带有其加速器 UUID，因
	// 此 reclaim 的孤儿 GC 能够在被释放的加速器上找到没有标记的 GI。MIG GI 没有操作器标签，因此这是发
	// 现未被追踪的 GI 的唯一方法。
	ListInstances() ([]migLiveInstance, error)
	// InstanceProcesses 统计了在某一分区上运行的计算进程，它会读取每个 MIG 设备句柄对应的地址——这些句
	// 柄包含该分区的进程列表，同时包含加速器针对整个卡端的查询响应。当无法查询到分区时，它会报错而非
	// 返回零值，调用者绝不能将此情况理解为“没有人使用它”。当没有设备可供查询时，会抛出
	// errNoAddressableDevice 错误。
	InstanceProcesses(cardUUID string, inst migInstance) (int, error)
	// CardInstances 列出了某个加速器的实时 GPU 实例，供那些已知正在选择哪个加速器的人使用。其存在是为
	// 了避免回收循环中的验证重读过程需要遍历节点：该读取操作是在持有加速器的锁的情况下进行的，而全节
	// 点的遍历则涉及数百次 NVML 调用，而每次调用都会同时阻塞加速器的所有分配操作。其错误提示与
	// ListInstances 相同。
	CardInstances(cardUUID string) ([]migInstance, error)
}

// cardLocks 以 GPU UUID 为键。创建分区、写入所有权标记、回收分区都受同一把卡锁保护，不同卡可以并行。
// 设备插件的节点锁只负责识别请求和预留候选位置。 节点锁即设备插件的 allocateMutex。
var cardLocks sync.Map

// lockCard 获取该卡的互斥锁，并返回解锁函数。
func lockCard(cardUUID string) func() {
	m, _ := cardLocks.LoadOrStore(cardUUID, &sync.Mutex{})
	mu := m.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

// migMarker 记录哪个 Pod 的哪个容器持有这张卡上的哪个分区。分配重试和后台回收都要读取该记录。
//
// Card 和 JSON 字段名 card 属于持久化格式。已有节点上的旧记录仍使用这个键，修改它会影响重试、可见性、
// 接管和回收。
type migMarker struct {
	PodUID        string `json:"podUID"`
	Container     string `json:"container"`
	Card          string `json:"card"`
	Profile       string `json:"profile"`
	GiID          uint32 `json:"giID"`
	CiID          uint32 `json:"ciID"`
	MigUUID       string `json:"migUUID"`
	ComputeSlices int32  `json:"computeSlices"`
	Start         int32  `json:"start"`
	Length        int32  `json:"length"`
}

// instance 从标记中的身份和几何信息还原 migInstance。
func (m migMarker) instance() migInstance {
	return migInstance{
		GiID:          m.GiID,
		CiID:          m.CiID,
		ComputeSlices: m.ComputeSlices,
		Placement:     migPlacement{Start: m.Start, Length: m.Length},
		UUID:          m.MigUUID,
	}
}

// markerEntry 把解析后的标记与文件路径保存在一起，回收时可精确删除该条记录。
type markerEntry struct {
	path   string
	marker migMarker
}

// markerFileName 返回 nvidia-mig-<GPU UUID>.json。GPU UUID 可以直接用作文件名，一个容器使用多张卡时
// 各自独立保存，由各自的卡锁保护。
func markerFileName(cardUUID string) string {
	return strings.TrimSuffix(markerName, ".json") + "-" + cardUUID + ".json"
}

// markerPath 返回指定 Pod、容器和物理卡的 MIG 标记路径。
func markerPath(podUID, container, cardUUID string) string {
	return filepath.Join(deviceplugin.PodWorkDir(podUID, container), markerFileName(cardUUID))
}

// parseMarker 读取并检查标记，内容缺失、格式损坏或身份信息不完整都返回错误，避免重试或回收误认分区。
//
// 记录中的 Card 必须与文件名中的 GPU UUID 一致，否则有两处可能出错。
// - 按记录内容分组时，文件名所指卡上的 GI 可能被误判为无人持有，让另一个 Pod 接管。
// - 按文件名查当前容器的旧分配时，可能会拿着另一张卡的 GI 编号去绑定。
//
// 两种身份不一致时把文件列入损坏路径。文件名仍可确定受影响的卡；待所属 Pod 消失后再清理。
func parseMarker(path string) (migMarker, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return migMarker{}, err
	}
	var m migMarker
	if err := json.Unmarshal(data, &m); err != nil {
		return migMarker{}, fmt.Errorf("marker %q: %w", path, err)
	}
	if m.PodUID == "" || m.Card == "" || m.Profile == "" || m.MigUUID == "" {
		return migMarker{}, fmt.Errorf("marker %q: incomplete record", path)
	}
	if card, ok := cardFromMarkerPath(path); !ok || card != m.Card {
		return migMarker{}, fmt.Errorf(
			"marker %q records card %q, not the card its file name names: fail closed", path, m.Card,
		)
	}
	return m, nil
}

// writeMarker 持久写入标记，避免并发扫描读到半份记录，并使已完成的写入能经受异常关机。
//
// 容器工作目录还要存放供工作负载读取的文件，因此目录权限为 0777。所有权记录只供设备插件自己使用，文
// 件权限为 0600。
func writeMarker(path string, m migMarker) error {
	dir := filepath.Dir(path)
	if err := osx.MkdirAll(dir, 0o777); err != nil {
		return fmt.Errorf("create marker dir %q: %w", dir, err)
	}
	data, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshal marker: %w", err)
	}
	if err := osx.DurableWrite(path, data, 0o600); err != nil {
		return fmt.Errorf("write marker %q: %w", path, err)
	}
	return nil
}

// scanMarkers 遍历 podsDir 下的 MIG 标记。解析失败的文件会加入损坏路径列表，其余可以读取的记录照常返
// 回。
//
// 调用方必须处理损坏路径。即使文件内容无法解析，文件名和所属工作目录仍可能指明卡和 Pod，这足以限制以
// 下两个决定。 设备名来自 markerFileName，Pod 所属目录由 deviceplugin.PodWorkDir 生成。
// - reserveMigInstance 不能将该卡上的遗留实例直接交给另一个 Pod，因为损坏文件可能正是它的所有权记录。
// - reclaimer.reconcile 和 destroyOrphans 不能把这张卡视为空闲，因为其中一个活跃分区的记录可能已经损
// 坏。
//
// reserveMigInstance 单独检查当前容器自己的标记。这能保护当前调用的幂等重试，其他 Pod 的接管和后台回
// 收仍须检查全卡记录。
func scanMarkers(podsDir string) (entries []markerEntry, corrupt []string) {
	_ = filepath.WalkDir(podsDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			corrupt = append(corrupt, path)
			return nil
		}
		if d.IsDir() || !isMarkerFile(d.Name()) {
			return nil
		}
		m, perr := parseMarker(path)
		if perr != nil {
			corrupt = append(corrupt, path)
			return nil //nolint:nilerr
		}
		entries = append(entries, markerEntry{path: path, marker: m})
		return nil
	})
	return entries, corrupt
}

// isMarkerFile 判断文件名是否符合 nvidia-mig-<GPU UUID>.json 的格式。
func isMarkerFile(name string) bool {
	return strings.HasPrefix(name, strings.TrimSuffix(markerName, ".json")+"-") &&
		strings.HasSuffix(name, ".json")
}

// ownedGiIDsOnCard 返回该卡上所有已被标记认领的 GI 编号，供接管时排除。
func ownedGiIDsOnCard(entries []markerEntry, cardUUID string) map[uint32]bool {
	owned := make(map[uint32]bool)
	for i := range entries {
		if entries[i].marker.Card == cardUUID {
			owned[entries[i].marker.GiID] = true
		}
	}
	return owned
}

// cardFromMarkerPath 从标记文件名提取 GPU UUID。内容损坏时仍能把影响限定到这一张卡。路径不是标记文件
// 或没有编码设备名时返回 false。
func cardFromMarkerPath(path string) (string, bool) {
	name := filepath.Base(path)
	if !isMarkerFile(name) {
		return "", false
	}
	card := strings.TrimSuffix(strings.TrimPrefix(name, strings.TrimSuffix(markerName, ".json")+"-"), ".json")
	if card == "" {
		return "", false
	}
	return card, true
}

// podUIDFromMarkerPath 从 <podsDir>/<podUID>/c-<container>/<marker> 中提取所有者 UID。即使内容被截断，
// 回收器也可按这个 UID 判断 Pod 是否已离开。层级不对或文件名无效时返回 false。
func podUIDFromMarkerPath(podsDir, path string) (string, bool) {
	if !isMarkerFile(filepath.Base(path)) {
		return "", false
	}
	rel, err := filepath.Rel(podsDir, path)
	if err != nil {
		return "", false
	}
	parts := strings.Split(rel, string(filepath.Separator))
	if len(parts) != 3 || parts[0] == "" || parts[0] == ".." {
		return "", false
	}
	return parts[0], true
}

// ownershipUnknownOnCard 判断损坏的路径是否使这张卡的归属信息不完整。有明确卡名时仅阻止该卡上需要证
// 明所有权的操作，其他卡可以照常分配。
//
// 如果目录遍历失败且无法确定卡名，就不能证明节点上任何卡的遗留实例无主，此时暂停接管和无标记实例的回
// 收。驱动仍能证明空闲的区间可以用于新建。标记所指 Pod 离开后回收器会清理其损坏文件；如果连 Pod 都无
// 法识别，则保留该路径，并在 reclaimMaxCorruptHoldMisses 次后报告文件系统问题。
func ownershipUnknownOnCard(corrupt []string, cardUUID string) bool {
	for _, p := range corrupt {
		card, ok := cardFromMarkerPath(p)
		if !ok || card == cardUUID {
			return true
		}
	}
	return false
}

// reuseUnboundInstance 查找算力切片数和显存区间长度均符合请求、且没有标记认领的存活实例。MIG UUID 为
// 空可能表示创建 GI 后、创建 CI 前进程中断，此时没有设备可注入，留给回收器处理。 若接管没有 CI 的 GI，
// NVIDIA_VISIBLE_DEVICES 将会为空。
func reuseUnboundInstance(state migCardState, owned map[uint32]bool, computeSlices, memorySlices int32) (migInstance, bool) {
	for i := range state.Live {
		inst := state.Live[i]
		if inst.UUID == "" {
			continue
		}
		if inst.ComputeSlices == computeSlices && inst.Placement.Length == memorySlices && !owned[inst.GiID] {
			return inst, true
		}
	}
	return migInstance{}, false
}

// pickPlacement 在合法且未占用的区间里选择 Start 最小的一个，卡已满时返回 false。同卡创建由卡锁串行
// 执行。
func pickPlacement(possible, occupied []migPlacement) (migPlacement, bool) {
	sorted := make([]migPlacement, len(possible))
	copy(sorted, possible)
	slices.SortFunc(sorted, func(a, b migPlacement) int { return cmp.Compare(a.Start, b.Start) })
	for _, slot := range sorted {
		if !placementOverlapsAny(slot, occupied) {
			return slot, true
		}
	}
	return migPlacement{}, false
}

// placementOverlapsAny 检查半开区间是否相交。[a,a+m) 和 [b,b+n) 相交当且仅当 a < b+n 且 b < a+m。
func placementOverlapsAny(slot migPlacement, occupied []migPlacement) bool {
	slotEnd := slot.Start + slot.Length
	for _, occ := range occupied {
		if slot.Start < occ.Start+occ.Length && occ.Start < slotEnd {
			return true
		}
	}
	return false
}

// migReserveOutcome 区分本次调用做了哪些操作，以便失败时只撤销本次变更。
// - migCreated 表示新建了 GI 和 CI 并写了标记。回滚时销毁新分区并删除标记。
// - migBound 表示接管了一个已存在的无主分区。回滚只删除新写的标记，使分区再次可供接管。
// - migRebound 表示沿用自身标记完成重试。分区和标记属于以前已成功的分配，回滚时均保留。
type migReserveOutcome int

const (
	migCreated migReserveOutcome = iota
	migBound
	migRebound
)

// String 返回分配结果的日志名称，便于区分本次新建、接管遗留实例和重用已有分配。
func (o migReserveOutcome) String() string {
	switch o {
	case migBound:
		return "bound"
	case migRebound:
		return "rebound"
	default:
		return "created"
	}
}

// reserveMigInstance 在卡锁内完成分区选择和标记写入。标记写入失败时撤销本次新建的实例；成功时返回本
// 次操作类型，供外层回滚使用。依次尝试以下路径。
// - 先检查自己的标记是否匹配卡和规格，并对照驱动确认 GI 和 MIG UUID 仍对应同一实例。kubelet 重试可以
// 据此取回已有分区。
// - 没有自身标记时，优先接管几何匹配、归属可确认的无主分区。
// - 没有可接管的分区时，选择起点最低的空闲区间，创建新的 GI 和 CI。
func reserveMigInstance(
	drv migDriver, podsDir, podUID, container, cardUUID, profile string, computeSlices, memorySlices int32,
) (inst migInstance, outcome migReserveOutcome, err error) {
	// IMPORTANT: 路线 1：先翻一翻“张三自己的存包纸条”还在不在？（解决 Kubelet 重试）
	self := markerPath(podUID, container, cardUUID)
	if m, perr := parseMarker(self); perr == nil {
		// 1. 核对规格是不是一样
		if m.Profile != profile {
			return migInstance{}, migRebound, fmt.Errorf(
				"marker %q mismatches request (profile=%s): fail closed", self, profile,
			)
		}
		// 2. 问驱动：卡 A 上现在真正活着哪些分区？
		state, serr := drv.CardState(cardUUID, profile, computeSlices, memorySlices)
		if serr != nil {
			return migInstance{}, migRebound, fmt.Errorf("read card %s state: %w", cardUUID, serr)
		}
		// 3. 核对纸条上的 GiID 还在不在？
		liveInst, ok := findLiveInstance(state, m.GiID)
		if !ok {
			return migInstance{}, migRebound, fmt.Errorf(
				"marker %q references missing gpu instance %d on card %s: fail closed", self, m.GiID, cardUUID,
			)
		}
		// 4. 核对这个 GiID 的 UUID 是不是还等于纸条上的 MigUUID？
		// 驱动可把已销毁 GI 的数字编号分给其他分区。绑定前再比对实时 MIG 设备 UUID 和标记，避免把旧标记里的
		// 标识交给容器。
		if liveInst.UUID != m.MigUUID {
			return migInstance{}, migRebound, fmt.Errorf(
				"marker %q gpu instance %d uuid %q no longer matches live uuid %q (id reused): fail closed",
				self, m.GiID, m.MigUUID, liveInst.UUID,
			)
		}
		return m.instance(), migRebound, nil
	} else if !os.IsNotExist(perr) {
		return migInstance{}, migRebound, fmt.Errorf("read self marker %q: %w", self, perr)
	}

	state, err := drv.CardState(cardUUID, profile, computeSlices, memorySlices)
	if err != nil {
		return migInstance{}, migCreated, fmt.Errorf("read card %s state: %w", cardUUID, err)
	}

	// IMPORTANT: 路线 2：张三没有自己的纸条 → 看看卡 A 上有没有“别人留下的、没人要的现成同规格分区”？（接管）

	// 该卡上的损坏标记可能正是遗留实例的所有权记录，因此不能接管。创建新实例时直接以驱动上所有存活实例计
	// 算占用，若仍有合法的空闲区间，仍可创建。
	entries, corrupt := scanMarkers(podsDir)
	var (
		reused    migInstance
		adoptable bool
	)
	if !ownershipUnknownOnCard(corrupt, cardUUID) {
		owned := ownedGiIDsOnCard(entries, cardUUID)
		reused, adoptable = reuseUnboundInstance(state, owned, computeSlices, memorySlices)
	}

	if adoptable {
		inst = reused
		outcome = migBound
	} else {
		// IMPORTANT: 路线 3：没法接管 → 找块空地，叫驱动真正新建一个！（新建）
		occupied := make([]migPlacement, 0, len(state.Live))
		for i := range state.Live {
			occupied = append(occupied, state.Live[i].Placement)
		}
		slot, ok := pickPlacement(state.Possible, occupied)
		if !ok {
			return migInstance{}, migCreated, fmt.Errorf(
				"card %s has no free placement for profile %s", cardUUID, profile,
			)
		}
		// IMPORTANT: 真正调 NVML 驱动创建 GI + CI！
		inst, err = drv.CreateInstance(cardUUID, profile, computeSlices, memorySlices, slot)
		if err != nil {
			return migInstance{}, migCreated, fmt.Errorf("create %s instance on card %s: %w", profile, cardUUID, err)
		}
		outcome = migCreated
	}

	m := migMarker{
		PodUID: podUID, Container: container, Card: cardUUID, Profile: profile,
		GiID: inst.GiID, CiID: inst.CiID, MigUUID: inst.UUID,
		ComputeSlices: inst.ComputeSlices, Start: inst.Placement.Start, Length: inst.Placement.Length,
	}
	// IMPORTANT: 把张三的新纸条落盘
	if werr := writeMarker(self, m); werr != nil {
		if outcome == migCreated {
			// 写标记失败时撤销本次创建的实例。进程若在创建成功和写标记之间崩溃，遗留物由回收器处理。
			_ = drv.DestroyInstance(cardUUID, inst)
		}
		return migInstance{}, outcome, werr
	}
	return inst, outcome, nil
}

// findLiveInstance 在该卡的实时状态中查找指定 GI 编号。
func findLiveInstance(state migCardState, giID uint32) (migInstance, bool) {
	for i := range state.Live {
		if state.Live[i].GiID == giID {
			return state.Live[i], true
		}
	}
	return migInstance{}, false
}

// profileGeometry 从 devs.Spec 所记录的单卡探测结果读取指定规格的计算切片数和显存切片数。卡或规格不
// 存在时返回 false。
func profileGeometry(devs *workercore.Devices, cardUUID, profile string) (computeSlices, memorySlices int32, ok bool) {
	for i := range devs.Spec.Groups {
		grp := &devs.Spec.Groups[i]
		for j := range grp.Accelerators {
			acc := &grp.Accelerators[j]
			if acc.ID != cardUUID {
				continue
			}
			for k := range acc.Status.PhysicalSliced.Profiles {
				p := &acc.Status.PhysicalSliced.Profiles[k]
				if p.Name == profile {
					return p.ComputeSlices, p.MemorySlices, true
				}
			}
		}
	}
	return 0, 0, false
}

// ActuatePhysicalSliced 在每张获分配的卡上落实一个指定规格的 MIG 分区，回报实际区间和身份，并把 MIG
// UUIDs 写入 NVIDIA_VISIBLE_DEVICES。任一卡失败时，按各卡的 reserve 结果撤销本次操作。 这条路径无需
// libvgpu 和 CUDA_DEVICE_* 软切环境变量。
func (s *server) ActuatePhysicalSliced(
	_ context.Context,
	pod *core.Pod,
	ctr *core.Container,
	devs *workercore.Devices,
	allocated map[deviceplugin.Resource]int32,
	profile string,
) (*deviceplugin.PhysicalSlicedAllocation, error) {
	if s.mig == nil {
		return nil, fmt.Errorf("mig actuator not configured")
	}

	cards := allocatedAccelerators(devs, allocated)
	if len(cards) == 0 {
		return nil, fmt.Errorf("no allocated card for physical-slice container %q", ctr.Name)
	}

	placements := make(map[deviceplugin.Resource][]workercore.AcceleratorPlacement, len(cards))
	ids := make(map[deviceplugin.Resource]string, len(cards))
	uuids := make([]string, 0, len(cards))
	// results 逐卡记录分配结果。回滚同样取得对应的卡锁，且仅撤销本次调用的变更。
	type cardResult struct {
		card    string
		inst    migInstance
		outcome migReserveOutcome
	}
	var results []cardResult
	rollback := func() {
		for i := range results {
			r := results[i]
			unlock := lockCard(r.card)
			switch r.outcome {
			case migCreated:
				// （本次刚新建的）：销毁驱动实例 + 撕掉刚才写的纸条（彻底还原）。
				_ = s.mig.DestroyInstance(r.card, r.inst)
				_ = os.Remove(markerPath(string(pod.UID), ctr.Name, r.card))
			case migBound:
				// （本次接管无主的）：只撕掉刚才写的纸条，不销毁实例（让它恢复无主状态）。
				// 该实例在本次接管前已经存在。删除本次写入的标记即可恢复无主状态，后续由回收器按条件处理。
				_ = os.Remove(markerPath(string(pod.UID), ctr.Name, r.card))
			case migRebound:
				// （以前早就建好、这次只是重试的）：啥也不动！
				// 以前的分配仍拥有这个分区及其标记。
			}
			unlock()
		}
	}

	for _, cardUUID := range cards {
		computeSlices, memorySlices, ok := profileGeometry(devs, cardUUID, profile)
		if !ok {
			rollback()
			return nil, fmt.Errorf("card %s has no physical-slice profile %q", cardUUID, profile)
		}
		unlock := lockCard(cardUUID)
		inst, outcome, err := reserveMigInstance(
			s.mig, deviceplugin.OperatorPodsDir, string(pod.UID), ctr.Name, cardUUID, profile, computeSlices, memorySlices,
		)
		unlock()
		if err != nil {
			rollback()
			return nil, err
		}
		results = append(results, cardResult{card: cardUUID, inst: inst, outcome: outcome})
		// 日志记录物理卡、GI/CI、MIG UUID、区间及本次取得方式。容器运行时随后按环境变量解析设备，如果启动失
		// 败，可据此分辨分区是否曾经存在以及如何取得。
		s.Logger.Info("granted a hardware partition",
			"pod", kubemeta.GetNamespacedNameKey(pod), "container", ctr.Name,
			"card", cardUUID, "profile", profile, "outcome", outcome.String(),
			"giID", inst.GiID, "ciID", inst.CiID, "placement", inst.Placement, "migUUID", inst.UUID)
		res := resourceForAccelerator(devs, cardUUID)
		placements[res] = []workercore.AcceleratorPlacement{{Start: inst.Placement.Start, Length: inst.Placement.Length}}
		ids[res] = inst.UUID
		uuids = append(uuids, inst.UUID)
	}

	return &deviceplugin.PhysicalSlicedAllocation{
		Profile:    profile,
		Placements: placements,
		IDs:        ids,
		Response: &deviceplugin.ContainerAllocateResponse{
			Envs: map[string]string{"NVIDIA_VISIBLE_DEVICES": strings.Join(uuids, ",")},
		},
		Rollback: rollback,
	}, nil
}

// allocatedAccelerators 按 devs 中的设备顺序返回获分配的 GPU UUID。同一顺序用于生成主容器和随附容器
// 的 NVIDIA_VISIBLE_DEVICES。
func allocatedAccelerators(devs *workercore.Devices, allocated map[deviceplugin.Resource]int32) []string {
	cards := make([]string, 0, len(allocated))
	for i := range devs.Spec.Groups {
		grp := &devs.Spec.Groups[i]
		for j := range grp.Accelerators {
			res := deviceplugin.Resource{Group: grp.ID, Device: grp.Accelerators[j].ID}
			if _, ok := allocated[res]; ok {
				cards = append(cards, grp.Accelerators[j].ID)
			}
		}
	}
	return cards
}

// resourceForAccelerator 把设备 UUID 转换成账本以设备组和设备组成的 Resource 键。
func resourceForAccelerator(devs *workercore.Devices, cardUUID string) deviceplugin.Resource {
	for i := range devs.Spec.Groups {
		grp := &devs.Spec.Groups[i]
		for j := range grp.Accelerators {
			if grp.Accelerators[j].ID == cardUUID {
				return deviceplugin.Resource{Group: grp.ID, Device: cardUUID}
			}
		}
	}
	return deviceplugin.Resource{Device: cardUUID}
}
