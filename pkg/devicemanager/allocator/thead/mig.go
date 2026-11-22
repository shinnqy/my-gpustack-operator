package thead

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/deviceplugin"
	"gpustack.ai/gpustack/pkg/utils/json"
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

// hostDevDir 和 hostProcDriverDir 是厂商设备节点的根目录，也是驱动程序 procfs 能力树的根目录。它们是变量
// 而非常量，因此测试中可以将其指向临时目录，而无需读取主机自身的/dev 和/proc 目录。
var (
	hostDevDir = "/dev"
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
