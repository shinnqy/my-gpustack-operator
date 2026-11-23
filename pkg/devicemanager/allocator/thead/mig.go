package thead

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"
	core "k8s.io/api/core/v1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/deviceplugin"
	"gpustack.ai/gpustack/pkg/utils/json"
	"gpustack.ai/gpustack/pkg/utils/osx"
)

// markerName 是写入 Pod 工作目录下的、每个容器/每个加速器分区所有权标记。它是重启后保留的记录，能够摧毁原
// 有的 GPU/compute 实例，从而允许从绑定实例中创建可复用的未绑定实例。
const markerName = "thead-mig.json"

// migPlacement 是加速器上的一段内存切片区间 [Start, Start+Length)，这是与平台无关的放置几何形状，也是
// slot-pick 和 marker 所使用的形状。因此，无需使用厂商库即可对纯核心进行测试。厂商的放置类型则保留在
// _linux 框架内。
type migPlacement struct {
	Start  int32
	Length int32
}

// migInstance 是加速器上存在一个实时运行或新创建的分区：其 GPU 实例和计算实例的 ID、GPU 实例所基于的原始
// 厂商规格文件 ID、计算切片数量、内存切片位置，以及用于标识该容器的分区身份字符串。
//
// ProfileID 是采用身份，它没有"unknown"值：厂商的编号设定中，0 是一个合法的规格 ID，因此无法读取实时实例
// 规格 ID 的接口必须返回错误，而不是将字段置零——如果置零，则会被视为某个真实的规格，从而可以被采用。
type migInstance struct {
	GiID          uint32
	CiID          uint32
	ProfileID     uint32
	ComputeSlices int32
	Placement     migPlacement
	UUID          string
}

// migCardState 是加速器中的实时分区状态，用于针对某个规格进行读取：所请求的规格名称对应的原始厂商规格 ID、
// 该规格在加速器上的合法空位，以及加速器上所有规格下的每个实时 GPU 实例（涵盖所有规格）。核心逻辑会从“可
// 能位置”中排除已占用的实时位置，以选择空闲的实例，并遍历“实时位置”以寻找同一原始规格的可复用未绑定实例。
//
// 规格 ID 是由 Seam 组件解析的，而非由核心组件解析：厂商保留上游编号，但不赋予其上游片段计数含义，因此 ID
// 只能从驱动程序中读取，无法从片段计数中计算出来。
type migCardState struct {
	ProfileID uint32
	Possible  []migPlacement
	Live      []migInstance
}

// errInstanceInUse 表示驱动程序被标记为忙碌状态——某个残留进程仍持有该实例。回收循环将其视为有边界的、可重
// 试的部分失败情况（不会清除去抖机制），并在边界处显示给操作员可见的日志。_linux seam 会处理它，因此与平
// 台无关的核心部分和回收循环可以通过 errors.Is 进行测试。
var errInstanceInUse = errors.New("mig instance in use")

// errNoAddressableDevice 表示某个分区没有分区设备句柄可寻址：要么是没有计算实例的 GPU 实例，要么是驱动程
// 序无法访问的分区容器，因此以这种方式会读取加速器的每个分区。这是回收循环允许销毁所涉及的那个进程查询失
// 败——因为那里没有任何可询问的内容，与那些被询问但未得到响应的驱动程序形成对比。_linux 框架通过封装处理该
// 问题，使得与平台无关的回收核心可以通过 errors.Is 进行测试。
var errNoAddressableDevice = errors.New("no partition device addresses the instance")

// migLiveInstance 是用于回收的全局枚举的实时 GPU 实例：它包含加速器 ID 和该实例的标识。它使得孤儿收集器能
// 够在任何加速器上找到无标记的 GPU 实例（包括完全没有标记的实例），而无需针对每个加速器提供规格说明。
type migLiveInstance struct {
	Card string
	Inst migInstance
}

// migDriver 是厂商管理库中的执行器接口，位于_linux.go/_other.go 构建标签之后：真正的实现负责在 Linux 上处
// 理厂商绑定。非 Linux 环境下的存根错误则通过虚假驱动程序对纯标记/槽位选择核心进行测试。每个操作都通过厂
// 商 UUID（操作器加速器 ID）来关联加速器。CardState 和 CreateInstance 函数会接收规格文件中的名称及几何信
// 息，因为仅通过计算切片数量无法确定 GPU 实例的规格文件 ID——该接口通过探针对各个 ID 进行名称匹配来对应规
// 格文件。
//
// 每个枚举操作在无法证明其枚举是完整的时，必须返回错误。且绝不能将部分状态作为成功情况报告。被读取为缺失
// 的活分区并非无害的空白：
//
//   - 其所有权标记将可移除，显示为“已消失”。
//   - 其被占的槽位被第二次分配。
//   - 无标记版本会泄漏通过孤儿收集器。
type migDriver interface {
	// CardState 会读取给定规格下加速器实时分区状态：即原始厂商规格 ID 对应的名称、该规格下的合法空加
	// 速器部署位置，以及加速器上所有的实时 GPU 实例。它只会报错，而不会描述无法完整读取的加速器状态。
	CardState(cardUUID, profile string, computeSlices, memorySlices int32) (migCardState, error)
	// CreateInstance 会根据规格在插槽中实例化 GPU 实例及其完整的 GI 计算实例，并返回所创建的分区信息
	// （ID、原始规格 ID、放置位置、身份）。
	CreateInstance(cardUUID, profile string, computeSlices, memorySlices int32, slot migPlacement) (migInstance, error)
	// DestroyInstance 会销毁分区的计算实例以及其 GPU 实例，之后会在加速器锁定机制下再次验证，确保该实
	// 例仍保留其被捕获时的身份。如果还有残留进程阻塞了销毁过程，它将返回包含 errInstanceInUse 的错误
	// 信息。
	DestroyInstance(cardUUID string, inst migInstance) error
	// ListInstances 会枚举所有支持分区功能的加速器中的活跃 GPU 实例，每个实例都带有其加速器 ID，因此
	// orphan 收集器能够在被释放的加速器上找到没有标记的实例。GPU 实例不携带操作器标签，因此这是查看未
	// 被追踪实例的唯一方法。它会报错，而不是返回无法证明完整的列表。
	ListInstances() ([]migLiveInstance, error)
	// InstanceProcesses 统计了在某一分区上运行的计算进程，以及每个分区-设备句柄所指向的进程——这些句柄
	// 包含该分区的进程列表，同时包含加速器针对整个卡域的查询响应。当无法获取分区信息时，会产生错误而
	// 非零响应，调用者绝不能将此情况理解为“没有人使用它”。当没有设备可供查询时，会抛出
	// errNoAddressableDevice 错误。
	InstanceProcesses(cardUUID string, inst migInstance) (int, error)
	// CardInstances 列出了某个加速器的实时 GPU 实例，供那些已知正在选择哪个加速器的人使用。其存在是为
	// 了避免回收循环中的验证重读过程需要遍历节点：该读取操作是在持有加速器的锁的情况下进行的，而全节
	// 点遍历则需要在满负荷的主机上进行数千次驱动调用，而每一次调用都会同时阻塞加速器的所有分配操作。
	// 其错误提示条件与 ListInstances 相同。
	CardInstances(cardUUID string) ([]migInstance, error)
}

// cardLocks 包含一个针对每个加速器的互斥锁，用于保护 create+marker-write（以及 reclaim destroy）关键区域，
// 因此同一加速器上的并发分配操作会串行选择其槽位，而其他加速器则可以并行执行。该互斥锁以加速器 UUID 为键。
var cardLocks sync.Map // 按卡的 UUID 保存互斥锁，键值关系为 cardUUID -> *sync.Mutex。

// lockCard 会锁定加速器的互斥锁，并返回其解锁函数。
func lockCard(cardUUID string) func() {
	m, _ := cardLocks.LoadOrStore(cardUUID, &sync.Mutex{})
	mu := m.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

// migMarker 是每个容器/加速器所有权记录中解析的一个项：pod<->partition 关联回收键决定了其存活性判断，同时
// 也会读取创建操作的幂等重试与重用检查。它会在规格名称旁记录原始规格 ID，因此重启后的判断取决于驱动器的自
// 身身份，而非可能因规范化变更而变动的名称。
//
// Card 字段及其 "card" json 标签采用 ON-DISK 格式，而非词汇表格式：早期版本所编写的标记仍存在于真实节点上，
// 若对其进行重命名将使其无法被读取，并破坏重试、可见性、采用及回收功能。该字段包含加速器 UUID。
type migMarker struct {
	PodUID        string `json:"podUID"`
	Container     string `json:"container"`
	Card          string `json:"card"`
	Profile       string `json:"profile"`
	ProfileID     uint32 `json:"profileID"`
	GiID          uint32 `json:"giID"`
	CiID          uint32 `json:"ciID"`
	MigUUID       string `json:"migUUID"`
	ComputeSlices int32  `json:"computeSlices"`
	Start         int32  `json:"start"`
	Length        int32  `json:"length"`
}

// 实例会重建 migInstance 标记记录。
func (m migMarker) instance() migInstance {
	return migInstance{
		GiID:          m.GiID,
		CiID:          m.CiID,
		ProfileID:     m.ProfileID,
		ComputeSlices: m.ComputeSlices,
		Placement:     migPlacement{Start: m.Start, Length: m.Length},
		UUID:          m.MigUUID,
	}
}

// markerEntry 将解析后的标记与其磁盘路径配对，因此 reclaim 仅移除特定的标记文件（不会移除其兄弟文件）。
type markerEntry struct {
	path   string
	marker migMarker
}

// markerFileName 用物理卡的 UUID 组成 thead-mig-<GPU UUID>.json。容器涉及多张卡时，每张卡有独立的所
// 有权标记，并由自己的锁保护。
func markerFileName(cardUUID string) string {
	return strings.TrimSuffix(markerName, ".json") + "-" + cardUUID + ".json"
}

// markerPath 以传入的 Pod 工作根目录、Pod UID、容器名和设备 UUID 定位标记，分配和回收因此可以在同一
// 目录树中查找记录。
func markerPath(podsDir, podUID, container, cardUUID string) string {
	return filepath.Join(podsDir, podUID, "c-"+container, markerFileName(cardUUID))
}

// parseMarker 会读取标记失败关闭机制：缺失、格式错误或不完整的记录会引发错误，因此自标记的重复使用和回收
// 不会默默地错误读取到正在使用的分区。原始规格中的 ID 不会检查其存在性，因为 0 是合法的厂商 ID。
//
// 记录的加速器必须就是该文件自身 NAME 编码的加速器。与名称不符的记录属于内部不一致，无论是读取还是使用都
// 是不安全的：
//
//   - 所有权集合按照记录的加速器进行分组，因此该记录所拥有的 GPU 实例在文件所属的加速器上看起来将没有归属，
//     而第二个 Pod 可能会占用另一个 Pod 正在使用的分区。
//   - 自标记重绑定功能会根据加速器的路径名称读取记录的 ID，因此它会将某个加速器的实例 ID 绑定到另一个加速
//     器中。
//
// 因此，这里被拒绝处理，它会被作为损坏的路径报告给扫描工具——由于加速器名称的编码，该路径仅由加速器保留，
// 一旦其 Pod 消失，该路径也会像其他不可读记录一样被废弃。
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
// 每个容器的工作目录也用于其他厂商运行时文件，因此允许容器镜像自行选择的用户访问。所有权标记只由设备
// 插件读取，文件仅授予写入者读写权限。
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

// scanMarkers 会解析 podsDir 下的每个分区标记。无法解析的标记会被作为损坏的路径收集，而不会导致整个扫描失
// 败，因此一个损坏的文件不会阻塞节点。调用者通过 ownershipUnknownOnCard 将问题缩小到文件名所指向的加速器，
// 这是唯一一个不可知的拥有权设置可能改变决策的场景——或者当路径名称无法识别时则指向所有加速器。
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

// isMarkerFile 报告名称是否为分区标记文件（thead-mig-<加速器 UUID>.json）。
func isMarkerFile(name string) bool {
	return strings.HasPrefix(name, strings.TrimSuffix(markerName, ".json")+"-") &&
		strings.HasSuffix(name, ".json")
}

// ownedGiIDsOnCard 返回了加速器 cardUUID 上所有标记所拥有的 GPU 实例 ID 集合，因此重复利用检查能够区分受
// 限实例与可重复使用的非受限实例。
func ownedGiIDsOnCard(entries []markerEntry, cardUUID string) map[uint32]bool {
	owned := make(map[uint32]bool)
	for i := range entries {
		if entries[i].marker.Card == cardUUID {
			owned[entries[i].marker.GiID] = true
		}
	}
	return owned
}

// cardFromMarkerPath 返回标记文件名称所编码的加速器 UUID（格式为 thead-mig-<加速器 UUID.json），即使文件
// 内容不符合该编码规则也能正确解析——该属性可将损坏的标记影响范围缩小到单个加速器。当基础名称根本不是标记
// 名称（例如无法读取的目录或路径错误）或编码出空加速器时，该函数会返回 ok=false，因为这样的路径无法归属于
// 任何特定的加速器。
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

// podUIDFromMarkerPath 返回标记路径所编码的 Pod UID——标记文件位于 <podsDir>/<podUID>/c-<container>/<
// marker> 位置，因此即使内部的记录被截断，所有者仍可以通过路径进行解析。正是这一点使得回收循环能够仅基于
// 存活性证据来移除损坏的标记。如果路径不是该深度的标记文件，且其所有者不可知，则会返回 ok=false 的结果。
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

// ownershipUnknownOnCard 报告是否存在任何无法解析的标记使得加速器 cardUUID 的归属状态无法确定，在这种情况
// 下，该加速器上剩余的标记无法被证明是未绑定的，所有依赖该证明的决策都将以失败告终：
//
//   - 采用无标识的剩余物品。
//   - 被释放的加速器判决，以及回收循环中的孤儿收集器所破坏的内容。
//
// 所有标记都能被解析的加速器不会受到影响，而新创建的加速器也不会，其占用情况来自驱动程序自身的完整枚举，
// 而非标记。
//
// 根据加速器对损坏文件名称的编码规则，一个损坏的文件不会影响其他加速器：如果整个节点失败，则单个被截断的
// 文件会限制整个节点的分区容量。而如果仅在该加速器上失败，则无法影响其他相关情况。
//
// 一条不指向任何加速器的损坏路径会使所有加速器变暗，因为未知范围本身也是未知的——它可能代表任何加速器的标
// 记。拒绝采用并不等同于拒绝容量（占用来自驱动程序的实时集合，因此在空闲 slot 中创建一个新的实例仍然可以
// 成功）。一个损坏的 MARKER 可以自行清除：当路径所指向的 Pod 消失后，回收循环会将其移除。然而，一条不指向
// 任何加速器的损坏路径也不指向任何 Pod——在 pod 目录上的遍历错误是可到达的情况——因此没有活体证据可以决定将
// 其移除，循环也故意保留它。因此这种状态是永久的，而非暂时的：只要该路径不可读，节点就不会有任何采用行为，
// 也不会在任何加速器上产生孤儿收集对象。这是文件系统故障，运维人员可以修复，回收循环在重试限制下会明确说
// 明这一点（reclaimMaxCorruptHoldMisses），而不是让节点默默退化。
func ownershipUnknownOnCard(corrupt []string, cardUUID string) bool {
	for _, p := range corrupt {
		card, ok := cardFromMarkerPath(p)
		if !ok || card == cardUUID {
			return true
		}
	}
	return false
}

// adoptUnboundInstance 查找这张卡上没有标记认领且厂商原始 profile ID 与请求一致的存活分区，可用于接
// 管以前遗留的实例。
//
// 用厂商原始 profile ID 区分配置。即使算力切片数与显存区间长度一致，媒体或图形等变体仍可能属于不同规
// 格，不能仅凭大小接管。
//
// 若实例身份为空，说明 GI 尚无可寻址的 CI，例如进程在两次创建之间退出。留给回收器处理，不将它作为可
// 交付分区。
func adoptUnboundInstance(state migCardState, owned map[uint32]bool, computeSlices, memorySlices int32) (migInstance, bool) {
	for i := range state.Live {
		inst := state.Live[i]
		if inst.UUID == "" || owned[inst.GiID] {
			continue
		}
		if inst.ProfileID == state.ProfileID &&
			inst.ComputeSlices == computeSlices && inst.Placement.Length == memorySlices {
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

// placementOverlapsAny 报告该槽位区间是否与任何已占用的区间相交。两个半开区间 [a, a+m) 和 [b, b+n) 若满足
// a < b+n 且 b < a+m，则会发生重叠。
func placementOverlapsAny(slot migPlacement, occupied []migPlacement) bool {
	slotEnd := slot.Start + slot.Length
	for _, occ := range occupied {
		if slot.Start < occ.Start+occ.Length && occ.Start < slotEnd {
			return true
		}
	}
	return false
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

// migReserveOutcome 区分本次调用做了哪些操作，以便失败时只撤销本次变更。
// - migCreated 表示本次创建了 GI、CI 并写入标记，回滚时销毁它们并清除标记。
// - migBound 表示接管了一个已存在的无主分区。回滚只删除新写的标记，使分区再次可供接管。
// - migRebound 表示沿用自身标记完成重试。分区和标记属于以前已成功的分配，回滚时均保留。
type migReserveOutcome int

const (
	migCreated migReserveOutcome = iota
	migBound
	migRebound
)

// reserveMigInstance 在卡锁内为当前容器分配分区，依次检查以下三种来源。
//
// - 先核对当前容器自己的标记、物理卡、规格和驱动中的实例身份，使 kubelet 重试时能返回同一分区。
// - 没有自身标记时，尝试接管厂商原始规格相同的无主分区。
// - 否则在最低空闲位置新建 GPU 实例和计算实例。
//
// 在卡锁内持久化标记。写入失败时撤销刚创建的实例，成功时返回本次实际执行的预留类型供外层回滚。
//
// 驱动无法完整读取当前状态时返回错误。若把读取失败视作空卡，可能将一个仍被占用的区间分配两次。
//
// 仅在 err 为 nil 时解释 outcome。失败路径已经撤销本次部分变更，不能再依据返回的枚举值做二次回滚。
func reserveMigInstance(
	drv migDriver, podsDir, podUID, container, cardUUID, profile string, computeSlices, memorySlices int32,
) (inst migInstance, outcome migReserveOutcome, err error) {
	self := markerPath(podsDir, podUID, container, cardUUID)
	if m, perr := parseMarker(self); perr == nil {
		if m.Profile != profile {
			return migInstance{}, migRebound, fmt.Errorf(
				"marker %q mismatches request (profile=%s): fail closed", self, profile,
			)
		}
		state, serr := drv.CardState(cardUUID, profile, computeSlices, memorySlices)
		if serr != nil {
			return migInstance{}, migRebound, fmt.Errorf("read card %s state: %w", cardUUID, serr)
		}
		liveInst, ok := findLiveInstance(state, m.GiID)
		if !ok {
			return migInstance{}, migRebound, fmt.Errorf(
				"marker %q references missing gpu instance %d on card %s: fail closed", self, m.GiID, cardUUID,
			)
		}
		// GI 的数字 ID 可以被复用，重新绑定前还要确认驱动报告的实例身份与标记一致。
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

	entries, corrupt := scanMarkers(podsDir)
	owned := ownedGiIDsOnCard(entries, cardUUID)

	reused, adopt := migInstance{}, false
	if !ownershipUnknownOnCard(corrupt, cardUUID) {
		reused, adopt = adoptUnboundInstance(state, owned, computeSlices, memorySlices)
	}

	if adopt {
		inst = reused
		outcome = migBound
	} else {
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
		inst, err = drv.CreateInstance(cardUUID, profile, computeSlices, memorySlices, slot)
		if err != nil {
			return migInstance{}, migCreated, fmt.Errorf("create %s instance on card %s: %w", profile, cardUUID, err)
		}
		outcome = migCreated
	}

	m := migMarker{
		PodUID: podUID, Container: container, Card: cardUUID, Profile: profile, ProfileID: inst.ProfileID,
		GiID: inst.GiID, CiID: inst.CiID, MigUUID: inst.UUID,
		ComputeSlices: inst.ComputeSlices, Start: inst.Placement.Start, Length: inst.Placement.Length,
	}
	if werr := writeMarker(self, m); werr != nil {
		if outcome == migCreated {
			// 标记写入失败时撤销本次新建的实例。接管的实例早已存在，应继续保留；两步间崩溃遗留的实例由回收器处理。
			_ = drv.DestroyInstance(cardUUID, inst)
		}
		return migInstance{}, outcome, werr
	}
	return inst, outcome, nil
}

// profileGeometry 从 devs.Spec 中该卡探测到的目标规格读取计算和显存切片数，找不到卡或规格时返回
// false。
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

// allocatedAccelerators 按 devs 的顺序列出本次分配的设备 UUID，使主容器和共用分区的容器按相同顺序组
// 装设备列表。
func allocatedAccelerators(devs *workercore.Devices, allocated map[deviceplugin.Resource]int32) []string {
	cards := make([]string, 0, len(allocated))
	for i := range devs.Spec.Groups {
		grp := &devs.Spec.Groups[i]
		for j := range grp.Accelerators {
			res := deviceplugin.Resource{
				Group:  grp.ID,
				Device: grp.Accelerators[j].ID,
			}
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

// hostDevDir 和 hostProcDriverDir 是厂商设备节点的根目录，也是驱动程序 procfs 能力树的根目录。它们是变量
// 而非常量，因此测试中可以将其指向临时目录，而无需读取主机自身的/dev 和/proc 目录。
var (
	hostDevDir        = "/dev"
	hostProcDriverDir = "/proc/driver"
)

// 以下节点集合来自厂商的容器隔离文档，该文档中的示例正好包含了这里所描述的五个分区的容器要素——两个共享控
// 制节点、父加速器的节点，以及一个 GPU 实例及其计算实例的能力节点——并描述结果为：该容器能够使用其中一个分
// 区，而看不到其他设备。没有对应的环境变量，也没有运行时钩子：这些节点构成了容器访问的全部内容。
//
// 这些节点的命名方式是通过硬件来确定的，因为文档中的命名规则并未对这款驱动程序进行描述。该名称是由 CARD
// ORDINAL 构成的——这是一个寻址索引，而非加速器的身份标识。在拥有 16 个加速器的主机上，每个加速器的节点为
// /dev/alixpu_ppu<ordinal>，其能力子树为 /proc/driver/alixpu/capabilities/ppu<ordinal>，两者共用一个序号，
// 运行范围为 0..15，没有 ppu16。在卡序号为 14 的加速器上创建的实时 GPU 实例则属于 ppu14。
//
// 这些节点所携带的内核次要编号各不相同，这代表了加速器的身份而非其名称：这是探测器记录的内容，也是证明某
// 张卡片的序号已到达该记录所描述的状态的关键。在该主机上，两者是分开运行的，因为共享控制节点 /dev/alixpu
// 具有与每个加速器节点相同的字符设备主编号——但这只是关于一个主机和一个驱动程序的观察结果，并非此文件可能
// 遵循的规则。这里没有任何内容可以从另一个方向计算出任一数字，下面的寻址证明适用于任何偏移量或没有偏移量
// 的情况。
const (
	// devControlName 和 devCtlName 是每个容器所需的共享控制节点，用于管理任何分区。这些节点不是按加速
	// 器划分的，因此仅通过名称即可识别。
	devControlName = "alixpu"
	devCtlName     = "alixpu_ctl"
	// devCardPrefix 用于指定加速器自身的设备节点，该节点以 CARD ORDINAL——即加速器索引——作为后缀，而不
	// 会使用该节点所携带的内核次要编号作为后缀。
	devCardPrefix = "alixpu_ppu"
	// devCapDir 和 devCapPrefix 用于根据驱动在 procfs 中公布的次设备号拼出能力设备路径。
	devCapDir    = "alixpu-caps"
	devCapPrefix = "alixpu-cap"
	// procDriverName 是驱动在 procfs 下保存能力文件的目录名。
	procDriverName = "alixpu"
	// procCapMinorField 是 access 文件中给出能力节点次设备号的字段名。
	procCapMinorField = "DeviceFileMinor:"
)

// sharedControlNodePaths 返回厂商的共享控制节点，每个容器需要一个。
func sharedControlNodePaths() []string {
	return []string{
		filepath.Join(hostDevDir, devControlName),
		filepath.Join(hostDevDir, devCtlName),
	}
}

// cardNodePath 返回由给定卡片序号指定的设备节点。
func cardNodePath(ordinal uint32) string {
	return filepath.Join(hostDevDir, devCardPrefix+strconv.FormatUint(uint64(ordinal), 10))
}

// capNodePath 返回与指定次设备号对应的能力设备路径。
func capNodePath(minor uint32) string {
	return filepath.Join(hostDevDir, devCapDir, devCapPrefix+strconv.FormatUint(uint64(minor), 10))
}

// cardCapDir 返回该卡分区 access 文件所在的 procfs 目录。目录与卡设备节点使用同一数字序号，避免把一
// 张卡的设备与另一张卡的能力文件混用。数字后缀也会排除驱动级的配置和监控目录。
func cardCapDir(ordinal uint32) string {
	return filepath.Join(hostProcDriverDir, procDriverName, "capabilities",
		"ppu"+strconv.FormatUint(uint64(ordinal), 10), "mig")
}

// giAccessPath 指向该卡上指定 GI 的 procfs access 文件。
func giAccessPath(ordinal, giID uint32) string {
	return filepath.Join(cardCapDir(ordinal), "gi"+strconv.FormatUint(uint64(giID), 10), "access")
}

// ciAccessPath 指向指定 GI 内部 CI 的 procfs access 文件。
func ciAccessPath(ordinal, giID, ciID uint32) string {
	return filepath.Join(cardCapDir(ordinal),
		"gi"+strconv.FormatUint(uint64(giID), 10), "ci"+strconv.FormatUint(uint64(ciID), 10), "access")
}

// readCapMinor 从 procfs access 文件读取能力节点的次设备号。文件不可读或没有该字段时返回错误。
//
// 次设备号在分配时读取。它不是按卡连续编号，也不能从 GI/CI 编号推出。厂商示例中一个 GI 的能力编号为
// 256，另一个为 1280，其首个 CI 为 1281。创建或删除分区会重新分配编号，缓存探测时的值可能将容器指向
// 其他人的分区。
func readCapMinor(path string) (uint32, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, fmt.Errorf("read capability file: %w", err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		rest, ok := strings.CutPrefix(strings.TrimSpace(line), procCapMinorField)
		if !ok {
			continue
		}
		minor, perr := strconv.ParseUint(strings.TrimSpace(rest), 10, 32)
		if perr != nil {
			return 0, fmt.Errorf("capability file %q: parse %s %w", path, procCapMinorField, perr)
		}
		return uint32(minor), nil
	}
	return 0, fmt.Errorf("capability file %q carries no %s field", path, procCapMinorField)
}

// deviceNodeMinor 表示路径下字符设备的次要版本号。它是一个包变量，因此测试中可以忽略其在无根目录的临时目
// 录中无法生成的单一事实——因为真正的字符设备——而路径的存在则仍属于真正的文件系统属性。
var deviceNodeMinor = statDeviceNodeMinor

// statDeviceNodeMinor 统计路径并报告设备的 minor 编号，当路径不存在或不是字符设备时会发生错误。
func statDeviceNodeMinor(path string) (uint32, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	if fi.Mode()&os.ModeCharDevice == 0 {
		return 0, fmt.Errorf("%q is not a character device", path)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, fmt.Errorf("%q exposes no device numbers", path)
	}
	// Rdev 字段属于平台类型——在 Linux 上为 uint64，在开发平台上为 int32——因此这种转换是编译必需的，在
	// Linux 上也是多余的，正因如此才带有指令，而非被“清理”：删除它会破坏开发构建，且没有任何未标记的
	// 表达式能同时满足两种平台的 linter 要求。注意，此文件根本没有构建标签。仅修改平台类型的 syscall
	// 字段就足以使普通文件在不同平台上以不同的方式通过 lint 检查，这与 _linux.go 文件跳过本地检查的情
	// 况不同。
	return unix.Minor(uint64(st.Rdev)), nil //nolint:unconvert // platform-typed Rdev, see above
}

// newPartitionDeviceSpec 将一个已验证的节点配置为读写设备规范。
func newPartitionDeviceSpec(path string) *deviceplugin.DeviceSpec {
	return &deviceplugin.DeviceSpec{
		ContainerPath: path,
		HostPath:      path,
		Permissions:   "rw",
	}
}

// requireDeviceNode 会验证分区设备集合中所需的节点，并返回其规格说明。
//
// 它故意不使用共享的设备辅助函数，因为对于不存在的路径，该函数会返回 nil：whole-accelerator 响应器只添加
// 非 null 的值，因此在此处复用该辅助函数会使得缺失的节点被处理为成功的分配，但设备集仍处于不完整状态。一
// 个分区需要其集合中的每个节点，因此缺失的节点——或者不是字符设备的节点——会导致分配失败。
func requireDeviceNode(path string) (*deviceplugin.DeviceSpec, error) {
	if _, err := deviceNodeMinor(path); err != nil {
		return nil, fmt.Errorf("device node %q: %w", path, err)
	}
	return newPartitionDeviceSpec(path), nil
}

// requireNumberedDeviceNode 会验证一个节点，其次要编号即为其身份标识：要么是能力节点，其次要编号是 procfs
// 为其发布的编号。要么是加速器自身的节点，其次要编号是检测器为该加速器记录的编号。编号不同的节点属于与驱
// 动程序不符的 /dev 树结构，因此会失败分配，而不会像正确的节点那样被交付使用。
//
// 只有两个共享控制节点通过 requireDeviceNode 得到验证，没有具体的数字可供比较：它们仅通过名称进行标识，而
// 非按加速器来划分，因此不会与任何东西产生混淆。
func requireNumberedDeviceNode(path string, wantMinor uint32) (*deviceplugin.DeviceSpec, error) {
	minor, err := deviceNodeMinor(path)
	if err != nil {
		return nil, fmt.Errorf("device node %q: %w", path, err)
	}
	if minor != wantMinor {
		return nil, fmt.Errorf("device node %q carries minor %d, want %d: fail closed", path, minor, wantMinor)
	}
	return newPartitionDeviceSpec(path), nil
}

// requireCardNode 返回了用于标识 cardUUID 自身设备节点序号的卡片序号，同时还会关联其 procfs 能力子树——即
// 加速器索引——以及该设备节点的验证规范。这是该厂商节点上处理加速器的方式的单一定义，因此无论是整个加速器
// 还是分区响应器都通过此方式访问加速器，无法以其他方式访问。
//
// 该序号只有在被展示出来以表明探测器所测量的加速器时才能使用。证明方式就是节点自身的核心编号与探测器为该
// 加速器记录的编号对比：若相等，则表明该序号对应该加速器。该说明并未涉及这两个编号之间的关系。驱动程序所
// 设定的两者之间的偏移量属于驱动程序所有，在此不予以重建：从某个主机观察结果所假设的偏移量可能适用于原本
// 已脱离该偏移量的加速器，并且会拒绝与编号不同的主机上的任何加速器。
//
// 该证明使得“加速器”索引能够安全地用于路径命名，因为该索引是一个后过滤计数器——它仅会在探测器认可的加速器
// 出现时推进——因此，在枚举过程中被跳过的加速器会使得后续索引与相邻索引对齐。如此偏移的索引所代表的节点，
// 其次要值并非用于该加速器的记录值，而这正是该索引所拒绝的。
//
// 如果出现以下情况，则视为错误：开发者未提供加速器。没有记录下来的序号。缺失的节点或非字符设备。以及与记
// 录不符的节点。检测器使得“无记录拒绝”具有意义——当驱动程序无法提供加速器的序号时，检测器不会记录任何内容，
// 而是使用枚举计数器作为替代，因为替代的数字与真实数字无法区分，这样就能将无法证明的序号当作可证明的序号
// 来使用。
func requireCardNode(devs *workercore.Devices, cardUUID string) (uint32, *deviceplugin.DeviceSpec, error) {
	for i := range devs.Spec.Groups {
		grp := &devs.Spec.Groups[i]
		for j := range grp.Accelerators {
			acc := &grp.Accelerators[j]
			if acc.ID != cardUUID {
				continue
			}
			if len(acc.PhysicalIndexes) == 0 {
				return 0, nil, fmt.Errorf(
					"card %s: no recorded minor number to prove its device node addresses it: fail closed", cardUUID,
				)
			}
			spec, err := requireNumberedDeviceNode(cardNodePath(acc.Index), acc.PhysicalIndexes[0])
			if err != nil {
				return 0, nil, fmt.Errorf("card %s: %w", cardUUID, err)
			}
			return acc.Index, spec, nil
		}
	}
	return 0, nil, fmt.Errorf("card %s: absent from the device record: fail closed", cardUUID)
}

// partitionDeviceSpecs 生成访问单个分区所需的卡设备节点及其 GI、CI 对应的能力设备节点。节点编号按驱
// 动实际报告的值解析。
//
// 所有这些设备都必需。该厂商没有额外的容器运行时钩子可以补齐设备，少给节点会导致分区不可用，错给节点
// 则会暴露其他分区。
//
// 调用方在预留前已经将卡节点的内核次设备号与探测记录核对。直接沿用验证过的节点及卡序号，保证 procfs
// 能力目录与实际设备一致。
func partitionDeviceSpecs(
	ordinal uint32,
	card *deviceplugin.DeviceSpec,
	inst migInstance,
) ([]*deviceplugin.DeviceSpec, error) {
	specs := []*deviceplugin.DeviceSpec{card}
	for _, access := range []string{
		giAccessPath(ordinal, inst.GiID),
		ciAccessPath(ordinal, inst.GiID, inst.CiID),
	} {
		minor, merr := readCapMinor(access)
		if merr != nil {
			return nil, merr
		}
		spec, derr := requireNumberedDeviceNode(capNodePath(minor), minor)
		if derr != nil {
			return nil, derr
		}
		specs = append(specs, spec)
	}
	return specs, nil
}

// ActuatePhysicalSliced 在每张分到的卡上按规格创建或复用分区，并把实际区间和分区身份返回给节点账本。
// 容器响应携带访问这些分区所需的设备节点。
//
// 容器内全部访问权限由这份设备列表决定。任何必需节点无法构造都应拒绝分配，并按各卡的预留结果只撤销本
// 次变更。
//
// 这是 PhysicalSlicedResponder 的分区执行方法，完整接口的编译期断言位于可见性响应文件中。
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

	// 在预留之前先验证所有卡都会用到的共享控制节点，缺少这些节点时可以在创建任何分区之前返回错误。
	sharedPaths := sharedControlNodePaths()
	devices := make([]*deviceplugin.DeviceSpec, 0, len(sharedPaths)+3*len(cards))
	for _, path := range sharedPaths {
		spec, err := requireDeviceNode(path)
		if err != nil {
			return nil, err
		}
		devices = append(devices, spec)
	}

	placements := make(map[deviceplugin.Resource][]workercore.AcceleratorPlacement, len(cards))
	ids := make(map[deviceplugin.Resource]string, len(cards))
	// results 保存每张卡的实际分配结果。回滚时重新取得对应卡锁，避免与同卡的其他分配或回收竞争。
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
				_ = s.mig.DestroyInstance(r.card, r.inst)
				_ = os.Remove(markerPath(deviceplugin.OperatorPodsDir, string(pod.UID), ctr.Name, r.card))
			case migBound:
				// 该实例在本次接管前已经存在。删除本次写入的标记即可恢复无主状态，后续由回收器按条件处理。
				_ = os.Remove(markerPath(deviceplugin.OperatorPodsDir, string(pod.UID), ctr.Name, r.card))
			case migRebound:
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
		// 先确定卡序号和卡节点确实对应探测到的同一物理卡，再执行预留。若不能验证就返回错误，避免在一张卡上创
		// 建分区、却把另一张卡的设备节点和能力目录交给容器。
		ordinal, cardNode, aerr := requireCardNode(devs, cardUUID)
		if aerr != nil {
			s.Logger.Info("refusing a partition on a card whose device node cannot be shown to address it; "+
				"the node named by the card's accelerator index must carry the minor number the detector "+
				"recorded for that card",
				"card", cardUUID, "profile", profile, "container", ctr.Name, "reason", aerr.Error())
			rollback()
			return nil, aerr
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

		specs, derr := partitionDeviceSpecs(ordinal, cardNode, inst)
		if derr != nil {
			rollback()
			return nil, fmt.Errorf("card %s partition device nodes: %w", cardUUID, derr)
		}
		devices = append(devices, specs...)

		res := resourceForAccelerator(devs, cardUUID)
		placements[res] = []workercore.AcceleratorPlacement{
			{Start: inst.Placement.Start, Length: inst.Placement.Length},
		}
		// 虽然容器最终按设备节点而非标识字符串寻址，仍在分配时记录分区身份，供状态读取和故障定位。
		ids[res] = inst.UUID
	}

	return &deviceplugin.PhysicalSlicedAllocation{
		Profile:    profile,
		Placements: placements,
		IDs:        ids,
		Response:   &deviceplugin.ContainerAllocateResponse{Devices: devices},
		Rollback:   rollback,
	}, nil
}
