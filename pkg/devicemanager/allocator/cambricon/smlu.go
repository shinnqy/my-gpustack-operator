package cambricon

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/util/sets"
	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/pkg/deviceplugin"
	"gpustack.ai/gpustack/pkg/utils/json"
	"gpustack.ai/gpustack/pkg/utils/osx"
	"gpustack.ai/gpustack/pkg/utils/stringx"
)

// markerName 是每个容器对应的关联规格及规格账本文件，该文件被写入 Pod 的工作目录下。它是 Ascend 的
// npu_info.config 文件的重启存活版本。
const markerName = "cambricon-smlu.json"

// smluInstancePrefix 使用所属 Pod UID 对由操作员创建的 sMLU 实例名称进行标记。cnDev 实例名称由操作员设定，
// 因此该名称始终是一个可解码的关联键：没有标记的实例仅在其嵌入的 UID 失效时被销毁，而没有该前缀的实例则被
// 视为外部对象，绝不会被操作。
const smluInstancePrefix = "gpustack"

// smluNameSep 用于分隔编码实例名称的各个字段。
const smluNameSep = ":"

// smluHashLen 限定了容器短哈希后缀的长度。其完整名称（前缀 + UID + 短哈希）完全在 cnDev 100 字节的实例名
// 缓冲区范围内：8 + 1 + 36 + 1 + 12 = 58 字节，因此 36 字节的 UID 和 63 字节的容器名称始终可以容纳。
const smluHashLen = 12

// reclaimMaxMisses 与 deviceplugin 的 podDirGC 相匹配：在连续进行这么多次重整合之前，Pod UID 必须不存在于
// 实时集合中，否则其实例才会被回收。因此，短暂的列表间隔永远不会破坏实时数据片段。
const reclaimMaxMisses = 3

// errSMLUUnsupported 表示某种模式调用失败，原因是该 cnDev 库或驱动程序根本没有 sMLU API。这种失败与卡片或
// 特定时刻的问题无关。这是那种无论重试、写入还是执行主机命令都无法解决的失败情况，因此预检查会拒绝处理该
// 问题，既不会操作卡片，也不会提供任何可行的修复方案。
var errSMLUUnsupported = errors.New("this cnDev library or driver has no sMLU API")

// smluModeError 会在 sMLU 模式下处理失败的驱动调用，当 apiUnavailable 表示该调用根本无法执行时，会标记为
// errSMLUUnsupported。
//
// 该判断结果以 bool 类型呈现，因为只有带有 build 标签的驱动程序能够读取 cnDev 返回代码，而此文件必须能够
// 被 Darwin 测试二进制文件导入。以这种方式拆分文件，使得标记本身也受到测试：那条接缝线保持了“单行代码到
// bool 类型的分类”，而悬挂在其上的决策则在此得到验证。
//
// 这张卡片故意没有命名。调用者知道加速器及其对应的序号，因此调用者会组合出面向运营商的语句。
func smluModeError(what string, cause error, apiUnavailable bool) error {
	if apiUnavailable {
		return fmt.Errorf("%s: %w: %w", what, cause, errSMLUUnsupported)
	}
	return fmt.Errorf("%s: %w", what, cause)
}

// allocMu 会序列化整个分配、验证、创建、写入标记流程，以及扫描过程。同时会在回收循环中销毁关键区域。设备
// 插件响应器在协调器的 allocateMutex 之外运行，而 kubelet 不会对分配操作进行串行化处理（存在并发的 Kueue
// 批次），因此若没有这一机制，两次分配可能会重复使用或重复创建规格，或者回收操作可能会遇到“标记前创建”的
// 窗口，导致实例被销毁。
var allocMu sync.Mutex

// smluInstance 是来自驱动程序的一个 sMLU 实例：它所在的加速器、由操作器指定的名称、其所属的规格，以及
// compute/VRAM 配额，还有其设备节点（注入到容器中）。
type smluInstance struct {
	card      string
	name      string
	profileID int32
	coresPct  int
	memMiB    int64
	devNode   string
}

// smluRef 是回收循环用于销毁实例所需的最小身份：父加速器及其实例名称。
type smluRef struct {
	card string
	name string
}

// profileKey 通过父加速器和规格 ID 来识别一个 sMLU 规格。
type profileKey struct {
	card      string
	profileID int32
}

// smluDriver 是可以注入的 Seam 组件，位于 cnDev sMLU 包装层之上。真正的实现代码（位于 Linux 系统的
// deviceplugin.go 中）通过 PCI 总线 ID 所指定的加速器接口驱动 cnDev。测试用实现则是内存中的，因此映射、名
// 称编码、标记和回收逻辑可以通过表格测试而不依赖硬件。规格复用和引用计数机制位于此 Seam 组件之上（在
// reserveInstance / 回收函数中），因此可以进行测试。该 Seam 组件本身仅作为包装层上的创建/销毁/列表操作接
// 口。ListInstances 是全局性的（每个加速器都有实例，每个实例都拥有自己的实例管理逻辑），因此回收机制可以
// 处理尚未设置标记的加速器上的崩溃孤儿实例。
type smluDriver interface {
	// GetSMLUMode 报告加速器的 sMLU 模式是否处于开启状态。
	GetSMLUMode(card string) (enabled bool, err error)
	// SetSMLUMode 用于开启或关闭加速器的 sMLU 模式。它会写入会持续存在于进程之外的主机驱动程序状态，
	// 因此调用者仅在读取到与当前模式不符的结果后才会进行写入操作。
	SetSMLUMode(card string, enabled bool) error
	// CreateProfile 会根据给定的计算资源百分比和 VRAM（MiB）配额创建一份规格文件，并返回其规格文件 ID。
	CreateProfile(card string, coresPct int, memMiB int64) (int32, error)
	// DestroyProfile 用于销毁个人资料。仅在没有实例引用它时调用。
	DestroyProfile(card string, profileID int32) error
	// CreateInstance 根据规格创建名为实例，并返回该实例，包括从驱动程序中读取到的设备节点。
	CreateInstance(card string, profileID int32, name string) (smluInstance, error)
	// DestroyInstance 会销毁加速器上名为该实例的对象。它具有幂等性：销毁已不存在的实例不会引发错误。
	DestroyInstance(card, name string) error
	// ListInstances 会枚举所有加速器上的每一个 sMLU 实例。
	ListInstances() ([]smluInstance, error)
	// ListProfiles 枚举了所有加速器上的每一个 sMLU 规格，因此回收循环可能会破坏一个没有实例引用的规格
	// （包括当创建操作发生在规格与实例之间时产生的崩溃孤儿状态）。
	ListProfiles() ([]profileKey, error)
}

// smluSetFor 将独立的计算资源 (%) 和 VRAM (MiB) 参数映射到 cnDev sMLU 配额字段中：mluQuota 直接表示计算比
// 例（无需进行 CU 转换），memorySize 以字节为单位（MiB << 20）。返回值为普通数值，以便核心部分不受 cnDev
// cgo 绑定影响。驱动程序会组装 cnDev SMluSet。该结果已通过报告验证，但尚未经过硬件验证（参见规范中的开放
// 问题）。
func smluSetFor(coresPct int, memMiB int64) (mluQuota uint32, memorySize uint64) {
	return uint32(coresPct), uint64(memMiB) << 20
}

// encodeInstanceName 会生成一个有边界、可解码的实例名称（<prefix>:<podUID>:<shortHash(container)>）。pod
// UID 是关联键。容器短哈希可以避免不同容器之间的混淆，同时不会造成 cnDev 100 字节缓冲区的占用。完整的 pod
// <->容器<->名称映射关系存储在标记中。
func encodeInstanceName(podUID, container string) string {
	short := stringx.SumBySHA256(container)[:smluHashLen]
	return smluInstancePrefix + smluNameSep + podUID + smluNameSep + short
}

// decodeInstanceUID 会提取实例名称所携带的 pod UID。如果名称不是由 operator 编码的，该方法会返回 false，
// 因此 reclaim 功能会让外部/第三方实例保持原状。
func decodeInstanceUID(name string) (string, bool) {
	parts := strings.Split(name, smluNameSep)
	if len(parts) != 3 || parts[0] != smluInstancePrefix || parts[1] == "" {
		return "", false
	}
	return parts[1], true
}

// smluMarker 是一种解析标记：pod<->实例关联以及规格账本被扫描器视为被占用状态，其存活性判断也依赖于回收循
// 环键。
//
// Card 字段及其 "card" JSON 标签属于磁盘格式，而非词汇表：此包之前所写的与加速器相关的标记仍存在于真实节
// 点上，若进行重命名则会导致这些标记无法被读取，并破坏重试、可见性、采用及回收功能。两者均已被冻结。
type smluMarker struct {
	PodUID    string `json:"podUID"`
	Container string `json:"container"`
	Card      string `json:"card"`
	Instance  string `json:"instance"`
	ProfileID int32  `json:"profileID"`
	CoresPct  int    `json:"coresPct"`
	MemMiB    int64  `json:"memMiB"`
}

// markerEntry 将解析后的标记与其磁盘路径配对，因此 reclaim 仅移除特定的标记文件（不会移除同级容器中的文件）。
type markerEntry struct {
	path   string
	marker smluMarker
}

// markerPath 返回切片容器对应的标记文件路径。
func markerPath(podUID, container string) string {
	return filepath.Join(deviceplugin.PodWorkDir(podUID, container), markerName)
}

// parseMarker 会读取标记的失败关闭机制：缺失/格式错误/不完整的记录会引发错误，因此调用方（自标记重用、回
// 收）绝不会对处于活跃状态的切片关联数据进行无声的误读。
func parseMarker(path string) (smluMarker, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return smluMarker{}, err
	}
	var m smluMarker
	if err := json.Unmarshal(data, &m); err != nil {
		return smluMarker{}, fmt.Errorf("marker %q: %w", path, err)
	}
	if m.PodUID == "" || m.Card == "" || m.Instance == "" {
		return smluMarker{}, fmt.Errorf("marker %q: incomplete record", path)
	}
	return m, nil
}

// writeMarker 会持续发布标记：同时运行的扫描器不会读取部分记录，已写入的记录在异常关闭后仍能保留。
func writeMarker(path string, m smluMarker) error {
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

// scanMarkers 会解析 podsDir 下的每个容器级标记。其设计具有容错性：无法解析的标记会被作为损坏的路径收集（
// 供调用者日志分析），而不会导致整个扫描失败。容器级别的自我标记重用检查则会在每个容器层面直接通过。
func scanMarkers(podsDir string) (entries []markerEntry, corrupt []string) {
	_ = filepath.WalkDir(podsDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
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

// ensureSMLUModeEnabled 会将逻辑切片状态转换为 sMLU 模式，这是该卡在任何规格或实例存在之前必须达到的状态。
// 该函数在关闭时读取数据，仅在开启时写入数据，因此对于已经处于该状态的加速器来说，只需进行一次查询，且它
// 永远不会关闭该模式——因为那样会使得其他节点运行的切片也无法使用。
//
// 它位于此处，而非驾驶员缝线之后，因此“读取后再写入”的决策由一个假设备件执行，该设备会记录其中实际发生的
// 是哪种情况。而缝线本身仅负责驱动硬件。
//
// 读取操作被分类处理，而非被直接处理。当报告称该库或驱动程序缺少 sMLU API 时，或者拒绝在不接触卡片的情况
// 下进行分配时——包括卡片模式已开启的情况，因为该包无法查询的库也是它无法管理的库——其他读取失败情况并不说
// 明 API 是否存在，因此写入操作仍会尝试进行：写入操作决定了模式的状态，而因超时或权限问题而拒绝操作则会导
// 致无法正常运行的节点崩溃。
//
// card 是 PCI 总线 ID cnDev 所指向的加速器，其序号为 cnDev 设备索引，这是操作员在 cnmon 中命名该卡时所需
// 的参数。两者都能在故障中识别该卡，因为序号与 cnmon 自身的-c 索引在硬件上无法得到验证。
//
// 调用方从 reserveInstance 的关键区域中进入此处，其位置的重要性源于两个与模式本身无关的原因：标记重用快速
// 路径会在此之前返回，因此重新启动一个 Pod 以使用其当前的活跃切片时，完全不需要驱动程序调用。此外，该操作
// 仅在请求验证完成后执行，因此在写入数据之前，如果标记损坏，则会被拒绝。将该模式开启两次并无害处——设置器
// 只是声明一个状态，而不是执行一个转换过程——因此这种位置安排并未带来序列化方面的收益。
func ensureSMLUModeEnabled(driver smluDriver, card string, ordinal int, logger klog.Logger) error {
	enabled, readErr := driver.GetSMLUMode(card)
	switch {
	case errors.Is(readErr, errSMLUUnsupported):
		return fmt.Errorf("card %s (device index %d) cannot be sliced: %w", card, ordinal, readErr)
	case readErr == nil && enabled:
		return nil
	}

	err := driver.SetSMLUMode(card, true)
	if err == nil {
		if readErr != nil {
			// 模式已开启，因此分配仍有效——但读取此代码时不能依赖它，因为在下一次分配时它将再
			// 次失败，每次都会重新写入主机状态，因此必须得到确认。
			logger.Info("enabled sMLU mode without a trustworthy read",
				"card", card, "deviceIndex", ordinal, "readError", readErr.Error())
		}
		return nil
	}
	if readErr != nil {
		err = fmt.Errorf("%w (the mode read failed too: %w)", err, readErr)
	}
	// 编写者可以报告阅读者未进行的操作：cnDev 能够独立地处理获取器和设置器。在这种情况下提供命令会导
	// 致在修复后仍然出现缺失符号，因此这两种结果在这里也被区分开来。
	if errors.Is(err, errSMLUUnsupported) {
		return fmt.Errorf("card %s (device index %d) cannot be sliced: %w", card, ordinal, err)
	}
	return fmt.Errorf(
		"card %s (device index %d): enable sMLU mode: %w; to enable it on the host for the card "+
			"at this PCI address run `cnmon set -c %d -smlu on`, confirming the ordinal with `cnmon`",
		card, ordinal, err, ordinal,
	)
}

// reserveInstance 是分配核心。在 allocMu 函数中，它：(1) 在完全匹配条件满足时（加速器、核心数%、
// `内存大小`单位），复用现有的自标记，获取活动实例的设备节点。若条件不匹配（Pod 资源请求不可更改，因此不
// 匹配表示数据损坏），则关闭进程。(2) 否则确保加速器处于 sMLU 模式，在该加速器满足特定条件（核心数%、
// `内存大小`单位）时复用或创建规格，实例化命名实例，并写入标记（若标记写入失败，则回滚实例及新创建的规格）。
func reserveInstance(driver smluDriver, podUID, container, card string, ordinal, coresPct int,
	memMiB int64, logger klog.Logger,
) (smluInstance, error) {
	allocMu.Lock()
	defer allocMu.Unlock()

	self := markerPath(podUID, container)
	if m, err := parseMarker(self); err == nil {
		// 幂等复用：请求是不可变的，因此只有完全匹配才有效。
		if m.Card != card || m.CoresPct != coresPct || m.MemMiB != memMiB {
			return smluInstance{}, fmt.Errorf(
				"marker %q mismatches request (card=%s cores=%d mem=%d): fail closed",
				self, card, coresPct, memMiB,
			)
		}
		list, err := driver.ListInstances()
		if err != nil {
			return smluInstance{}, fmt.Errorf("list smlu instances: %w", err)
		}
		for i := range list {
			if list[i].name == m.Instance {
				return list[i], nil
			}
		}
		// 标记文件仍在，但对应实例已消失，可能是创建时进程崩溃或实例被外部删除。本轮拒绝复用这个
		// 标记，由回收循环清理它。
		return smluInstance{}, fmt.Errorf("marker %q references missing instance %q: fail closed", self, m.Instance)
	} else if !os.IsNotExist(err) {
		return smluInstance{}, fmt.Errorf("read self marker %q: %w", self, err)
	}

	// 预检查步骤已经指定了该卡片，并包含其自身的修复措施，因此其错误会原样返回，而不会在第二次提及同
	// 一卡片时被包裹。
	if err := ensureSMLUModeEnabled(driver, card, ordinal, logger); err != nil {
		return smluInstance{}, err
	}

	list, err := driver.ListInstances()
	if err != nil {
		return smluInstance{}, fmt.Errorf("list smlu instances: %w", err)
	}
	profileID, created, err := findOrCreateProfile(driver, list, card, coresPct, memMiB)
	if err != nil {
		return smluInstance{}, err
	}

	name := encodeInstanceName(podUID, container)
	inst, err := driver.CreateInstance(card, profileID, name)
	if err != nil {
		if created {
			_ = driver.DestroyProfile(card, profileID)
		}
		return smluInstance{}, fmt.Errorf("card %s: create smlu instance: %w", card, err)
	}

	m := smluMarker{PodUID: podUID, Container: container, Card: card, Instance: name, ProfileID: profileID, CoresPct: coresPct, MemMiB: memMiB}
	if err := writeMarker(self, m); err != nil {
		// 回滚操作，以确保“创建前标记”窗口不会因我们的错误路径而丢失（两者之间的真正崩溃由回收循
		// 环处理）。
		_ = driver.DestroyInstance(card, name)
		if created {
			_ = driver.DestroyProfile(card, profileID)
		}
		return smluInstance{}, err
	}
	return inst, nil
}

// findOrCreateProfile 会在加速器上复用某个规格，只要其计算资源与 VRAM 配额与请求要求完全匹配（若配额不符
// 则违反所要求的隔离性），或者创建一个新的规格。它返回规格 ID 以及该规格是否刚刚创建（因此如果实例创建失
// 败，可以回滚新的规格）。
func findOrCreateProfile(driver smluDriver, list []smluInstance, card string, coresPct int, memMiB int64) (int32, bool, error) {
	for i := range list {
		if list[i].card == card && list[i].coresPct == coresPct && list[i].memMiB == memMiB {
			return list[i].profileID, false, nil
		}
	}
	id, err := driver.CreateProfile(card, coresPct, memMiB)
	if err != nil {
		return 0, false, fmt.Errorf("card %s: create smlu profile: %w", card, err)
	}
	return id, true, nil
}

// reclaimer 表示基于级别的每个制造商回收循环的状态。它由广播 livePodUIDs 以及周期性同步计时器（
// deviceplugin.go 代码逻辑）驱动，因此被丢弃的广播计时不会导致回收功能失效。每次协调操作都会重新列出驱动
// 程序，并扫描标记信息，因此即使在重启后也能自动修复，无需使用内存中的计数器。
type reclaimer struct {
	driver  smluDriver
	podsDir string
	logger  klog.Logger
	misses  map[string]int // pod UID -> 连续缺失的协调
}

func newReclaimer(driver smluDriver, podsDir string, logger klog.Logger) *reclaimer {
	return &reclaimer{driver: driver, podsDir: podsDir, logger: logger, misses: make(map[string]int)}
}

// reconcile 用于协调驱动程序实例与磁盘上某个实时 Pod-UID 快照的标记。它调用 allocMu，因此不会在正在进行的
// Allocate 操作创建标记前的窗口期出现竞争。每次存活性判断都会受到连续 reclaimMaxMisses 次未进行的协调操作
// 的抑制，因此短暂的列表差异不会恢复实时状态。该过程支持双向协调。
//   - 一个其 Pod 已死亡（按 Pod 决策）的标记 -> 销毁其实例（允许已不存在的实例，这涵盖了无实例的标记情况），
//     并仅移除该标记文件（只有当 Pod 目录为空时才清空 Pod 目录）。
//   - 没有标记器的实例，其嵌入的 UID 已失效 -> 销毁它。嵌入的 UID 仍然有效的实例（在预留节点上因先创建再
//     设置标记导致的崩溃）则保留原样。未被操作符编码的名称属于外部数据，永远不会被修改。
//
// 每次执行 Pod 检查后，它会扫描一次配置文件（gcOrphanProfiles），删除任何没有实例引用的配置文件。
func (r *reclaimer) reconcile(livePodUIDs []string) {
	allocMu.Lock()
	defer allocMu.Unlock()

	live := sets.New[string](livePodUIDs...)

	instances, err := r.driver.ListInstances()
	if err != nil {
		r.logger.Error(err, "reclaim: list smlu instances, skipping this pass")
		return
	}
	markers, corrupt := scanMarkers(r.podsDir)
	for _, p := range corrupt {
		r.logger.Info("reclaim: skipping unparseable marker", "path", p)
	}

	// 对标记的实例名称进行索引，以便检测注册表中的孤儿项。
	markedName := make(map[string]bool, len(markers))
	for i := range markers {
		markedName[markers[i].marker.Instance] = true
	}

	// 按 Pod UID 收集回收目标：需要删除的标记文件以及需要销毁的实例。带有标记的实例会提供其（加速器、
	// 名称、profileID）。没有标记的实例则根据其解码后的 UID 提供相关信息（无法解码的名称被视为外部内
	// 容，将被跳过）。
	type target struct {
		markers   []markerEntry
		instances []smluRef
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
	for i := range markers {
		m := markers[i].marker
		t := get(m.PodUID)
		t.markers = append(t.markers, markers[i])
		t.instances = append(t.instances, smluRef{card: m.Card, name: m.Instance})
	}
	for i := range instances {
		inst := instances[i]
		if markedName[inst.name] {
			continue // 有标记。在上方处理
		}
		uid, ok := decodeInstanceUID(inst.name)
		if !ok {
			continue // 使用 not 运算符编码。不要干预其他实例
		}
		get(uid).instances = append(get(uid).instances, smluRef{card: inst.card, name: inst.name})
	}

	// 那些被触及的标记在本次中仍然具有相关性。其余的则被修剪到下方。
	touched := sets.New[string]()
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
		r.destroy(uid, t.instances, t.markers)
	}

	for k := range r.misses {
		if !touched.Has(k) {
			delete(r.misses, k)
		}
	}

	// 每次遍历时扫描一次规格：销毁任何没有实例引用的规格——要么是被上述销毁操作释放的，要么是在创建操
	// 作与实例之间产生的崩溃孤儿。在 allocMu 下是安全的：因为此时没有正在进行的 Allocate 操作，且规格
	// 中尚未有实例。
	r.gcOrphanProfiles()
}

// 销毁一个已死亡的 Pod 实例：先销毁每个实例（幂等操作），然后当该目录为空时移除每个标记文件及对应的 Pod
// 目录。只有当整个 Pod 被回收时，失败计数才会清零，因此部分失败会在下一次尝试中重新进行。释放的配置文件会
// 在协调过程结束时由 gcOrphanProfiles 进行回收。
func (r *reclaimer) destroy(uid string, instances []smluRef, markers []markerEntry) {
	ok := true
	for _, inst := range instances {
		if err := r.driver.DestroyInstance(inst.card, inst.name); err != nil {
			r.logger.Error(err, "reclaim: destroy instance", "podUID", uid, "card", inst.card, "instance", inst.name)
			ok = false
			continue
		}
	}
	for i := range markers {
		if err := os.Remove(markers[i].path); err != nil && !os.IsNotExist(err) {
			r.logger.Error(err, "reclaim: remove marker", "path", markers[i].path)
			ok = false
			continue
		}
		// 当目录为空时，会移除容器目录和 Pod 目录，因此同级容器的活动标记永远不会被删除。
		removeIfEmpty(filepath.Dir(markers[i].path))
		removeIfEmpty(filepath.Dir(filepath.Dir(markers[i].path)))
	}
	if ok {
		delete(r.misses, uid)
		r.logger.Info("reclaim: reclaimed dead pod's instances", "podUID", uid, "instances", len(instances))
	}
}

// gcOrphanProfiles 会销毁所有没有存活实例引用的规格，因此一个由活跃实例共享的规格在真正的孤儿规格被释放时
// 不会被移除（其实例会被回收，或者发生“创建前实例崩溃”的情况）。该操作会失败：列表错误会导致扫描操作中断，
// 而不是基于部分信息进行处理。
func (r *reclaimer) gcOrphanProfiles() {
	profiles, err := r.driver.ListProfiles()
	if err != nil {
		r.logger.Error(err, "reclaim: list smlu profiles for gc, skipping")
		return
	}
	if len(profiles) == 0 {
		return
	}
	instances, err := r.driver.ListInstances()
	if err != nil {
		r.logger.Error(err, "reclaim: list smlu instances for profile gc, skipping")
		return
	}
	inUse := make(map[profileKey]bool, len(instances))
	for i := range instances {
		inUse[profileKey{card: instances[i].card, profileID: instances[i].profileID}] = true
	}
	for _, pk := range profiles {
		if inUse[pk] {
			continue
		}
		if err := r.driver.DestroyProfile(pk.card, pk.profileID); err != nil {
			r.logger.Error(err, "reclaim: destroy orphan profile", "card", pk.card, "profileID", pk.profileID)
		}
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
