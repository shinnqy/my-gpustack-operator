package metax

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/util/sets"
	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/pkg/deviceplugin"
	"gpustack.ai/gpustack/pkg/utils/json"
	"gpustack.ai/gpustack/pkg/utils/osx"
)

// maxSGPUPerCard 限定了单个 MetaX 加速器可以承载的 SGPU 子设备数量（DefaultDevCnt=16）。加速器的最低可用
// 索引和上限都是基于实时驱动程序注册表与磁盘上的标记相结合得出的，而非仅基于标记本身。
const maxSGPUPerCard = 16

// wholeCardIndex 用于标记整个加速器切片已被占用：原生整个加速器路径不会创建 sgpu 子设备，但该标记仍会记录
// 加速器已被占用，因此磁盘扫描器不会重复记录该设备。
const wholeCardIndex = -1

// markerName 是每个容器对应的相关性数据 + 槽位账本文件，该文件被写入 pod 的工作目录下。它是 Ascend 的
// npu_info.config 的重启后可用版本。
const markerName = "metax-sgpu.json"

// sgpuAliasPrefix 使用其所属节点 UID 对由操作器创建的 sgpu 子设备进行标记。这是一种与标记无关的关联键，使
// 得“崩溃孤儿”回收规则能够安全运行：没有标记的子设备仅在其嵌入的 UID 失效时才会被销毁。如果驱动程序不提供
// 读取功能，该子设备的别名将保持为空。此时回收机制会对其采取保守处理（不会自动销毁无法识别的子设备）。
const sgpuAliasPrefix = "gpustack-"

// reclaimMaxMisses 与 deviceplugin 的 podDirGC 相匹配：在连续进行若干次重整合后，某个 pod UID 必须不在实
// 时集合中，否则其切片才会被回收。因此，短暂的列表间隔永远不会破坏一个实时切片。
const reclaimMaxMisses = 3

// allocMu 会序列化整个扫描 -> 验证 -> 创建 -> 写入标记流程，以及分配过程中的扫描操作 -> 在回收循环中销毁
// 关键区域。设备插件响应器在协调器的 allocateMutex 之外运行，而 kubelet 不会对分配操作进行序列化（存在并
// 发的 Kueue 批次），因此若没有这一机制，两种分配操作可能会得到相同的免费 sgpu 索引，或者回收操作可能会观
// 察到正在进行的创建操作在标记之前的情况，进而破坏一个正在使用的切片。
var allocMu sync.Mutex

// schedClass 是一个 MetaX sgpu QoS 调度类。运营商始终使用固定共享机制，因此 .sliced.cores 百分比属于固定
// 的计算配额（而非尽力获取）。
type schedClass int

const (
	schedFixedShare schedClass = 1
)

// sgpuSubdevice 是来自驱动程序注册表的某个 sgpu 子设备：它所在的加速器、其针对每个加速器的索引，以及操作
// 者对它的别名标识（当驱动程序不提供该标识时为空）。
type sgpuSubdevice struct {
	bdf   string
	index int
	alias string
}

// sgpuManager 是注入到 MetaX sysfs sgpu 控制接口上的模块。真实实现会写入 /sys/bus/pci/devices/<bdf>/sgpu/
// * 路径。测试实现则使用虚拟根目录，因此编码、标记、槽位推导、回收逻辑可以在无硬件环境下进行测试。该列表
// 是全局的（返回所有加速器的子设备，每个子设备都携带其 bdf），因此回收循环能够捕获到尚未有标记的加速器上
// 的崩溃孤儿设备。
type sgpuManager interface {
	// EnsureModel 将加速器置于 sgpu 模式。仅当加速器没有现有子设备时调用（已托管子设备的加速器已处于
	// sgpu 模式）。
	EnsureModel(bdf string) error
	// SetSchedClass 设置加速器的 QoS 调度类。仅在加速器没有现有运营商子设备时调用，因此真实的加速器不
	// 会发生就地修改。
	SetSchedClass(bdf string, c schedClass) error
	// Create 函数会在操作器生成的索引位置创建一个子设备，并指定 VRAM 配额（MiB）和关联别名。
	Create(bdf string, index int, vramMiB int64, alias string) error
	// 移除操作会删除 bdf 中索引位置的子设备。它具有幂等性：删除已不存在的子设备不会引发错误，因此没有
	// 子设备的标记也能干净地清除。
	Remove(bdf string, index int) error
	// 列表列出了所有加速器上的每个 sgpu 子设备。
	List() ([]sgpuSubdevice, error)
}

// encodeAlias 标签会使用所属 Pod UID 来标记子设备。
func encodeAlias(podUID string) string {
	return sgpuAliasPrefix + podUID
}

// decodeAliasUID 用于提取操作员别名所携带的 pod UID。对于为空或非本操作员的别名，该函数会返回 false。因此，
// reclaim 功能能够区分“我们的孤儿”设备与不可标记的/第三方子设备。
func decodeAliasUID(alias string) (string, bool) {
	uid, ok := strings.CutPrefix(alias, sgpuAliasPrefix)
	if !ok || uid == "" {
		return "", false
	}
	return uid, true
}

// encodeMetaxSGPUs 会渲染 METAX_SGPUS 环境条目，MXMACA 运行时会读取该条目以处理部分切片：sgpu=<BDF>#<idx>
// ;compute=<cores%>;vram=<MiB>;alias=<tag>.
func encodeMetaxSGPUs(bdf string, index, coresPct int, vramMiB int64, alias string) string {
	return fmt.Sprintf("sgpu=%s#%d;compute=%d;vram=%d;alias=%s", bdf, index, coresPct, vramMiB, alias)
}

// sgpuMarker 是一种解析标记：即 pod 与子设备的关联关系，以及扫描器将占用的插槽视为已占用的情况，其存活性
// 判断也依赖于这些循环键。对于全加速器占用标记（无子设备），索引为 wholeCardIndex。
//
// CardBDF 字段及其 "cardBDF" JSON 标签属于磁盘格式，而非词汇表：在此包之前编写的与加速器相关的标记仍存在
// 于真实节点上。若进行重命名，将使其无法被读取，并破坏重试、可见性、采用性和回收功能。两者均已被冻结。
type sgpuMarker struct {
	PodUID    string `json:"podUID"`
	Container string `json:"container"`
	CardBDF   string `json:"cardBDF"`
	Index     int    `json:"index"`
	CoresPct  int    `json:"coresPct"`
	MemMiB    int64  `json:"memMiB"`
}

func (m sgpuMarker) wholeCard() bool { return m.Index == wholeCardIndex }

// markerEntry 将解析后的标记与其磁盘路径配对，因此 reclaim 仅移除特定的标记文件（不会移除同级容器中的文件）。
type markerEntry struct {
	path   string
	marker sgpuMarker
}

// markerPath 返回切片容器对应的标记文件路径。
func markerPath(podUID, container string) string {
	return filepath.Join(deviceplugin.PodWorkDir(podUID, container), markerName)
}

// parseMarker 会读取标记的失败关闭机制：缺失/格式错误/不完整的记录会引发错误，因此调用方（自标记重用、回
// 收）绝不会对处于活跃状态的切片关联数据进行无声的误读。
func parseMarker(path string) (sgpuMarker, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return sgpuMarker{}, err
	}
	var m sgpuMarker
	if err := json.Unmarshal(data, &m); err != nil {
		return sgpuMarker{}, fmt.Errorf("marker %q: %w", path, err)
	}
	if m.PodUID == "" || m.CardBDF == "" || m.Index < wholeCardIndex {
		return sgpuMarker{}, fmt.Errorf("marker %q: incomplete record", path)
	}
	return m, nil
}

// writeMarker 会持续发布标记：同时运行的扫描器不会读取部分记录，已写入的记录在异常关闭后仍能保留。
func writeMarker(path string, m sgpuMarker) error {
	dir := filepath.Dir(path)
	if err := osx.MkdirAll(dir, 0o777); err != nil {
		return fmt.Errorf("create marker dir %q: %w", dir, err)
	}
	data, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshal marker: %w", err)
	}
	if err := osx.DurableWrite(path, data, 0o644); err != nil {
		return fmt.Errorf("write marker %q: %w", path, err)
	}
	return nil
}

// scanMarkers 会解析 podsDir 下的每个容器标记。其设计具有容错性：无法解析的标记会被作为损坏路径收集（供调
// 用者日志使用），而不会导致整个扫描失败，因为索引占用问题由驱动程序注册表联合机制解决——仍然存在子设备的
// 损坏标记仍会被注册表统计，而子设备已消失的标记则会留下真正空闲的索引。错误关闭机制则体现在自标记重用检
// 查中，因此损坏标记仅会导致所属容器分配失败（仅限于该加速器范围内），而不会影响整个制造商的分配情况。
func scanMarkers(podsDir string) (entries []markerEntry, corrupt []string) {
	_ = filepath.WalkDir(podsDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			// 目录读取错误被记录为损坏的作用域，而非致命错误。
			corrupt = append(corrupt, path)
			return nil
		}
		if d.IsDir() || d.Name() != markerName {
			return nil
		}
		m, perr := parseMarker(path)
		if perr != nil {
			// 上方记录了损坏的标记，行走继续进行。它不会向上传播出错，因此扫描会覆盖所有剩余
			// 的标记。
			corrupt = append(corrupt, path)
			return nil //nolint:nilerr
		}
		entries = append(entries, markerEntry{path: path, marker: m})
		return nil
	})
	return entries, corrupt
}

// usedIndexesOnCard 从驱动程序注册表中收集了被 bdf 占用的 sgpu 索引，并合并了部分标记。该注册表起到后备保
// 护作用：即使没有标记，出现故障的子设备仍会占用其索引，因此仅依赖标记进行操作可能会导致资源冲突。
func usedIndexesOnCard(registry []sgpuSubdevice, markers []markerEntry, bdf string) map[int]bool {
	used := make(map[int]bool)
	for i := range registry {
		if registry[i].bdf == bdf {
			used[registry[i].index] = true
		}
	}
	for i := range markers {
		m := markers[i].marker
		if m.CardBDF == bdf && !m.wholeCard() {
			used[m.Index] = true
		}
	}
	return used
}

// lowestFreeIndex 返回 [0, maxSGPUPerCard) 范围内最小的 sgpu 索引，如果该索引未被使用，或者当每个加速器资
// 源池用尽时则返回错误。
func lowestFreeIndex(used map[int]bool) (int, error) {
	for i := 0; i < maxSGPUPerCard; i++ {
		if !used[i] {
			return i, nil
		}
	}
	return 0, fmt.Errorf("card sgpu pool exhausted (%d subdevices)", maxSGPUPerCard)
}

// sliceResult 是 reserveSlice 返回给响应器的内容：完整的加速器切片不会包含 METAX_SGPUS 条目（而是返回原生
// 完整的加速器设备），而部分切片则包含所创建子设备的索引和环境条目。
type sliceResult struct {
	wholeCard bool
	index     int
	envValue  string
}

// reserveSlice 是分配核心。在 allocMu 函数中，它：(1) 在完全匹配（加速器、核心数、内存大小）的情况下复用
// 现有的自身标记，在不匹配时则失败关闭（Pod 资源请求是不可变的，因此不匹配表示错误）。(2) 对于完整加速器
// 切片，仅写入占用标记。(3) 对于部分切片，确保加速器处于 sgpu 模式且属于固定共享类（仅当加速器没有现有子
// 设备时，即活着的加速器不会发生变更），从注册表 UNION 中获取最低空闲索引，创建子设备，并写入标记。
// wholeCard 由调用者决定（核心数≥100 且内存大小≥VRAM）。
func reserveSlice(mgr sgpuManager, podUID, container, bdf string, coresPct int, memMiB int64, wholeCard bool) (sliceResult, error) {
	allocMu.Lock()
	defer allocMu.Unlock()

	registry, err := mgr.List()
	if err != nil {
		return sliceResult{}, fmt.Errorf("list sgpu subdevices: %w", err)
	}

	self := markerPath(podUID, container)
	if m, err := parseMarker(self); err == nil {
		// 幂等复用：请求是不可变的，因此只有完全匹配才有效。
		if m.CardBDF != bdf || m.CoresPct != coresPct || m.MemMiB != memMiB || m.wholeCard() != wholeCard {
			return sliceResult{}, fmt.Errorf(
				"marker %q mismatches request (card=%s cores=%d mem=%d wholeCard=%t): fail closed",
				self, bdf, coresPct, memMiB, wholeCard,
			)
		}
		if m.wholeCard() {
			return sliceResult{wholeCard: true}, nil
		}
		// 如果标记器的子设备从驱动程序注册表中消失（带外垃圾回收/手动清理），则会导致失败：重新使
		// 用该子设备会注入 METAX_SGPUS，导致不存在的子设备出现，进而引发配置错误的容器。真正死亡
		// 的节点标记器会被回收循环移除，因此存活节点出现此情况属于真实故障。
		if !subdeviceOnCard(registry, bdf, m.Index) {
			return sliceResult{}, fmt.Errorf(
				"marker %q references missing sgpu subdevice %s#%d: fail closed", self, bdf, m.Index,
			)
		}
		return sliceResult{index: m.Index, envValue: encodeMetaxSGPUs(bdf, m.Index, coresPct, memMiB, encodeAlias(podUID))}, nil
	} else if !os.IsNotExist(err) {
		// 标记存在，但已损坏：仅针对此 Pod（此加速器）会失败关闭。
		return sliceResult{}, fmt.Errorf("read self marker %q: %w", self, err)
	}

	markers, _ := scanMarkers(deviceplugin.OperatorPodsDir)

	if wholeCard {
		// 当加速器同时托管部分切片或已被其他容器整体占用时，无法将其完全占用（系统应防止此情况发
		// 生。若发生则应关闭失败，而非暴露共享加速器）。
		if len(usedIndexesOnCard(registry, markers, bdf)) > 0 || cardTakenWhole(markers, bdf) {
			return sliceResult{}, fmt.Errorf("card %s is already occupied, cannot take it whole", bdf)
		}
		m := sgpuMarker{PodUID: podUID, Container: container, CardBDF: bdf, Index: wholeCardIndex, CoresPct: coresPct, MemMiB: memMiB}
		if err := writeMarker(self, m); err != nil {
			return sliceResult{}, err
		}
		return sliceResult{wholeCard: true}, nil
	}

	// 部分切片绝不能落在已被另一个 Pod 完全占用的加速器上。
	if cardTakenWhole(markers, bdf) {
		return sliceResult{}, fmt.Errorf("card %s is held whole, cannot slice it", bdf)
	}

	used := usedIndexesOnCard(registry, markers, bdf)
	index, err := lowestFreeIndex(used)
	if err != nil {
		return sliceResult{}, fmt.Errorf("card %s: %w", bdf, err)
	}

	// 没有现有子设备的加速器只需设置一次其模式与固定共享类。已经拥有子设备的加速器则已配置完成，重写
	// 实时加速器的类设置会被驱动程序拒绝，因此切勿进行此类操作。
	if !cardHasSubdevice(registry, bdf) {
		if err := mgr.EnsureModel(bdf); err != nil {
			return sliceResult{}, fmt.Errorf("card %s: ensure sgpu model: %w", bdf, err)
		}
		if err := mgr.SetSchedClass(bdf, schedFixedShare); err != nil {
			return sliceResult{}, fmt.Errorf("card %s: set fixed-share sched class: %w", bdf, err)
		}
	}

	alias := encodeAlias(podUID)
	if err := mgr.Create(bdf, index, memMiB, alias); err != nil {
		return sliceResult{}, fmt.Errorf("card %s: create sgpu subdevice %d: %w", bdf, index, err)
	}

	m := sgpuMarker{PodUID: podUID, Container: container, CardBDF: bdf, Index: index, CoresPct: coresPct, MemMiB: memMiB}
	if err := writeMarker(self, m); err != nil {
		// 回滚子设备，以避免“创建前标记”的崩溃窗口因我们的错误路径而留下隐患（两者之间的真正崩溃
		// 则由回收循环的注册表协调机制处理）。
		_ = mgr.Remove(bdf, index)
		return sliceResult{}, err
	}
	return sliceResult{index: index, envValue: encodeMetaxSGPUs(bdf, index, coresPct, memMiB, alias)}, nil
}

// cardTakenWhole 用于指示 bdf 是否由完整的加速器占用标记所持有，因此部分切片不会默默共享其他容器所拥有的
// 加速器（反之亦然）。
func cardTakenWhole(markers []markerEntry, bdf string) bool {
	for i := range markers {
		m := markers[i].marker
		if m.CardBDF == bdf && m.wholeCard() {
			return true
		}
	}
	return false
}

// subdeviceOnCard 报告驱动程序注册表当前是否包含 sgpu 子设备（bdf，索引）——当重复使用的标记引用到已消失的
// 设备时，该设置用于实现失败关闭。
func subdeviceOnCard(registry []sgpuSubdevice, bdf string, index int) bool {
	for i := range registry {
		if registry[i].bdf == bdf && registry[i].index == index {
			return true
		}
	}
	return false
}

func cardHasSubdevice(registry []sgpuSubdevice, bdf string) bool {
	for i := range registry {
		if registry[i].bdf == bdf {
			return true
		}
	}
	return false
}

// reclaimer 是基于级别的、每个制造商的回收循环状态。它由广播 livePodUIDs 以及周期性同步计时器（任务 2 的
// 电路设计）驱动，因此被丢弃的广播计时不会导致回收功能失效。每次协调过程都会重新扫描注册表和标记，因此可
// 以在重启后自行修复，无需使用内存中的槽位计数器。
type reclaimer struct {
	mgr     sgpuManager
	podsDir string
	logger  klog.Logger
	misses  map[string]int // pod UID -> 连续缺失的协调
}

func newReclaimer(mgr sgpuManager, podsDir string, logger klog.Logger) *reclaimer {
	return &reclaimer{mgr: mgr, podsDir: podsDir, logger: logger, misses: make(map[string]int)}
}

// cardMissPrefix 命名空间用于记录每个加速器的丢失计数（被耗尽加速器的孤儿回收），其计数映射与每个 Pod 的
// 计数相同。Pod 的`UID`为`UUID`，因此不会与这个前缀产生冲突。
const cardMissPrefix = "card:"

// reconcile 功能会针对某个实时 Pod-UID 快照，对驱动注册表与磁盘上的标记进行协调。它会调用 allocMu，因此不
// 会在正在进行的 Allocate 操作中的“创建标记前”窗口出现竞争。每次存活性判断都会受到 reclaimMaxMisses 次连
// 续未完成的协调操作的抑制，因此短暂的列表差异不会恢复其存活状态。该功能可从三个方向进行协调：
//   - 一个其 Pod 已死亡的标记器（按 Pod 决策） -> 销毁其子设备（允许已不存在的情况，这涵盖了无子设备的标
//     记器情况），并仅移除该标记器文件（仅当 Pod 目录为空时才清空 Pod 目录）。
//   - 无标记子设备，其嵌入的 UID 已失效 -> 将其销毁。嵌入的 UID 仍有效的设备（在预留节点上因标记前创建导
//     致的崩溃）则保留原样。
//   - 一种没有标记、没有可解码别名（与当前 MetaX 版本不同，驱动程序未公开该标签）的子设备，只有当其加速器
//     没有任何正在运行的保留 Pod 时才会被回收（这是每个加速器对应的决定）——即用有限期的泄漏进行交换，当加
//     速器耗尽资源时即被回收，而永远破坏一个正在运行的 Pod 则不可挽回。如果加速器仍持有任何正在运行的 Pod，
//     此类子设备将保持完整（可能是那个 Pod 在标记出现之前的崩溃孤儿状态）。
func (r *reclaimer) reconcile(livePodUIDs []string) {
	allocMu.Lock()
	defer allocMu.Unlock()

	live := sets.New[string](livePodUIDs...)

	registry, err := r.mgr.List()
	if err != nil {
		r.logger.Error(err, "reclaim: list sgpu subdevices, skipping this pass")
		return
	}
	markers, corrupt := scanMarkers(r.podsDir)
	for _, p := range corrupt {
		r.logger.Info("reclaim: skipping unparseable marker", "path", p)
	}

	// 对标记器部分子设备进行索引，以便检测注册表中的孤儿项。
	markedSubdev := make(map[subdevKey]bool, len(markers))
	for i := range markers {
		m := markers[i].marker
		if !m.wholeCard() {
			markedSubdev[subdevKey{m.CardBDF, m.Index}] = true
		}
	}

	// 按 Pod UID 收集可回收目标：需要移除的标记以及需要销毁的子设备。带标记的子设备会贡献其（bdf,
	// index）值。无标记的别名子设备则在其解码后的 UID 下贡献其（bdf,index）值。没有可解码所有者的无标
	// 记子设备会由加速器保存，仅在加速器没有活跃 Pod 时才会被回收。liveOnCard 记录了哪些加速器托管了
	// 活跃保留 Pod（通过活跃标记或活跃 UID 的别名子设备），因此这些加速器的不可识别子设备会被保留。
	type target struct {
		markers    []markerEntry
		subdevices []subdevKey
	}
	targets := make(map[string]*target)
	get := func(uid string) *target {
		t := targets[uid]
		if t == nil {
			t = &target{}
			targets[uid] = t
		}
		return t
	}
	unidentified := make(map[string][]subdevKey) // bdf -> 无标记、无别名子设备
	liveOnCard := make(map[string]bool)
	for i := range markers {
		m := markers[i].marker
		t := get(m.PodUID)
		t.markers = append(t.markers, markers[i])
		if !m.wholeCard() {
			t.subdevices = append(t.subdevices, subdevKey{m.CardBDF, m.Index})
		}
		if live.Has(m.PodUID) {
			liveOnCard[m.CardBDF] = true
		}
	}
	for i := range registry {
		sd := registry[i]
		if markedSubdev[subdevKey{sd.bdf, sd.index}] {
			continue // 有标记。在上方处理
		}
		uid, ok := decodeAliasUID(sd.alias)
		if !ok {
			unidentified[sd.bdf] = append(unidentified[sd.bdf], subdevKey{sd.bdf, sd.index})
			continue
		}
		get(uid).subdevices = append(get(uid).subdevices, subdevKey{sd.bdf, sd.index})
		if live.Has(uid) {
			liveOnCard[sd.bdf] = true
		}
	}

	// 那些被触及的标记在本次中仍然具有相关性。其余的则被修剪到下方。
	touched := sets.New[string]()

	// 每个 Pod 的存活性判断 + 延迟机制。
	for uid, t := range targets {
		touched.Insert(uid)
		if live.Has(uid) {
			r.misses[uid] = 0
			continue
		}
		r.misses[uid]++
		if r.misses[uid] < reclaimMaxMisses {
			continue
		}
		r.destroy(uid, t.subdevices, t.markers)
	}

	// 每个加速器对应的耗尽加速器资源，回收无法识别的子设备及去抖处理。
	for bdf, keys := range unidentified {
		key := cardMissPrefix + bdf
		touched.Insert(key)
		if liveOnCard[bdf] {
			r.misses[key] = 0
			continue
		}
		r.misses[key]++
		if r.misses[key] < reclaimMaxMisses {
			continue
		}
		r.destroyCard(key, keys)
	}

	// Drop miss 计数器不再相关（容器已消失/加速器不再保留孤儿对象）。
	for k := range r.misses {
		if !touched.Has(k) {
			delete(r.misses, k)
		}
	}
}

// 销毁一个已死亡的节点中的各个子设备：移除每个子设备（幂等操作），然后当该节点为空时移除其标记文件及节点
// 目录。只有在整个节点被回收时，失败计数才会清零，因此部分失败会在下一次尝试中重新进行。
func (r *reclaimer) destroy(uid string, subdevices []subdevKey, markers []markerEntry) {
	ok := true
	for _, sd := range subdevices {
		if err := r.mgr.Remove(sd.bdf, sd.index); err != nil {
			r.logger.Error(err, "reclaim: remove subdevice", "podUID", uid, "bdf", sd.bdf, "index", sd.index)
			ok = false
		}
	}
	for i := range markers {
		if err := os.Remove(markers[i].path); err != nil && !os.IsNotExist(err) {
			r.logger.Error(err, "reclaim: remove marker", "path", markers[i].path)
			ok = false
			continue
		}
		// 当容器目录和 Pod 目录为空时，请将其移除，这样同级容器的活动标记就不会被删除。
		removeIfEmpty(filepath.Dir(markers[i].path))
		removeIfEmpty(filepath.Dir(filepath.Dir(markers[i].path)))
	}
	if ok {
		delete(r.misses, uid)
		r.logger.Info("reclaim: reclaimed dead pod's slices", "podUID", uid, "subdevices", len(subdevices))
	}
}

// destroyCard 功能会移除完全耗尽能量、无活体容器中的无法识别的无标记子设备。只有当所有移除操作都成功时，
// 失败计数才会清零，因此部分失败将导致下一次尝试。
func (r *reclaimer) destroyCard(missKey string, subdevices []subdevKey) {
	ok := true
	for _, sd := range subdevices {
		if err := r.mgr.Remove(sd.bdf, sd.index); err != nil {
			r.logger.Error(err, "reclaim: remove orphan subdevice on drained card", "bdf", sd.bdf, "index", sd.index)
			ok = false
		}
	}
	if ok {
		delete(r.misses, missKey)
		r.logger.Info("reclaim: reclaimed unidentifiable orphans on drained card", "count", len(subdevices))
	}
}

// removeIfEmpty 仅在目录中没有条目时才会移除它，因此回收一个容器不会使某个兄弟节点的标记变成孤立状态。
func removeIfEmpty(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) > 0 {
		return
	}
	_ = os.Remove(dir)
}

// subdevKey 通过加速器及每个加速器索引来标识子设备。
type subdevKey struct {
	bdf   string
	index int
}

// sysfsSGPUManager 就是真正的 sgpuManager。它会在根目录（默认路径为 /sys/bus/pci/devices）下读取和写入
// MetaX 驱动程序的 sysfs sgpu 控制项。
//
// 以下节点名称（model、sgpu/sched_class、sgpu/create、sgpu/remove）已通过实际硬件确认。目前仍有文档中未解
// 决的硬件问题（参见规范），即索引分配问题——参见 Create 自身的注释——因此每次写入操作仍会经过这个薄层，只
// 有该问题若有所不同时才会产生变化。单元测试直接针对这种实际实现（Test_sysfsSGPUManager），同时也针对假管
// 理器进行了测试（包括分配/回收测试）。
type sysfsSGPUManager struct {
	root string
}

func newSysfsSGPUManager() *sysfsSGPUManager {
	return &sysfsSGPUManager{root: "/sys/bus/pci/devices"}
}

func (m *sysfsSGPUManager) cardDir(bdf string) string { return filepath.Join(m.root, bdf, "sgpu") }

func (m *sysfsSGPUManager) EnsureModel(bdf string) error {
	return os.WriteFile(filepath.Join(m.root, bdf, "model"), []byte("sgpu"), 0o600)
}

func (m *sysfsSGPUManager) SetSchedClass(bdf string, c schedClass) error {
	return os.WriteFile(filepath.Join(m.cardDir(bdf), "sched_class"), []byte(strconv.Itoa(int(c))), 0o600)
}

func (m *sysfsSGPUManager) Create(bdf string, index int, vramMiB int64, alias string) error {
	// 驱动程序的创建节点仅需要 VRAM 配额（MiB）。它会自行分配子设备索引。操作员传递其预测的最低可用
	// `index`（针对标记/METAX_SGPUS 条目），并假设驱动程序会尊重该槽位——但在实际硬件上尚未验证（参见
	// 规范中的开放问题）。如果实际驱动程序出现差异，请切换此选项，以便在写入标记之前从 List()中读取创
	// 建的索引。
	//
	// 该别名存储在 METAX_SGPUS 中（即运行时的关键变量），并未持久化到 sysfs 中——当前的 MetaX 没有可供
	// 读取的子设备标签，因此 reclaim 机制并不依赖它：崩溃后的孤儿设备会被该标记捕获，若失败则遵循
	// reconcile 中的“耗尽加速器”规则。仅当未来的驱动程序通过 List 接口暴露该标签时，基于别名的孤儿设
	// 备规则才会生效。
	_ = index
	_ = alias
	return os.WriteFile(filepath.Join(m.cardDir(bdf), "create"), []byte(strconv.FormatInt(vramMiB, 10)), 0o600)
}

// Remove 通过驱动程序的 "remove" 控制节点移除子设备。通过硬件验证：驱动程序根本没有 "destroy" 节点——在该
// 节点进行写入操作时，总会返回 os.IsNotExist 错误，而该函数自身的 IsNotExist 容错机制会将其识别为已被移除
// 的片段，从而静默地不做任何操作。
func (m *sysfsSGPUManager) Remove(bdf string, index int) error {
	err := os.WriteFile(filepath.Join(m.cardDir(bdf), "remove"), []byte(strconv.Itoa(index)), 0o600)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func (m *sysfsSGPUManager) List() ([]sgpuSubdevice, error) {
	entries, err := os.ReadDir(m.root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var subdevs []sgpuSubdevice
	for _, e := range entries {
		bdf := e.Name()
		subEntries, err := os.ReadDir(m.cardDir(bdf))
		if err != nil {
			continue // 不是支持 SGPU 的卡
		}
		for _, se := range subEntries {
			idx, err := strconv.Atoi(strings.TrimPrefix(se.Name(), "sgpu"))
			if err != nil {
				continue
			}
			alias, _ := os.ReadFile(filepath.Join(m.cardDir(bdf), se.Name(), "alias"))
			subdevs = append(subdevs, sgpuSubdevice{bdf: bdf, index: idx, alias: strings.TrimSpace(string(alias))})
		}
	}
	return subdevs, nil
}
