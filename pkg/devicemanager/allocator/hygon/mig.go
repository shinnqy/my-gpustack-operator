package hygon

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/deviceplugin"
	"gpustack.ai/gpustack/pkg/utils/json"
	"gpustack.ai/gpustack/pkg/utils/osx"
)

// migMarkerName 是每个容器/每个加速器的所有权记录的基础名称。它采用“磁盘格式”：早期版本所创建的记录仍存在
// 于真实节点上，若对其进行重命名，则会导致重试、采用和回收过程无法处理已分配的资源。
const migMarkerName = "hygon-mig.json"

// _MigInstanceIDPrefix 是厂商工具显示实例以及运行时选择实例时使用的前缀。
const _MigInstanceIDPrefix = "MIG-"

// migConfigDir 是厂商自己的实时实例注册表。驱动程序在每个计算实例创建后，会在此处写入一个文件，该文件就是
// 容器用于使用该实例的绑定对象——因此它既是驱动程序识别分区身份的依据，也是分配给工作负载的工件。它是一个
// 变量，因此测试可以指向一个固定值。
//
// 它位于此处而非驾驶员旁边，因为回收扫描功能在所有平台上都能识别它，而驾驶员端本身仅支持 Linux 系统。
var migConfigDir = "/etc/dmi_mig_config"

// errInstanceInUse 报告称，驱动程序拒绝销毁一个分区，因为某些因素仍使其处于有效状态。这并非本端系统统计的
// 结果，而是驱动程序自身的回应：厂商以“设备仍然存在且可能被使用”为由拒绝销毁，而此时工作负载却使实例保持
// 开放状态，这比任何进程查询结果都更具说服力——实际上，该库根本不处理与分区相关的任何操作。
var errInstanceInUse = errors.New("partition is in use")

// migPlacement 是在厂商自己的切片单元中，在单个加速器上进行的 GPU 切片操作。
type migPlacement struct {
	Start  int32
	Length int32
}

// migInstance 是一个已物化分区：包括 GPU 实例、其内部的计算实例，以及容器使用该实例所需的另外两项内容。
//
// 身份标识是计算实例的 UUID，而非任何衍生出的内容。值得注意的是，该厂商的 UUID 并非由放置位置决定：破坏一
// 个分区并在同一位置创建相同的规格，会生成不同的 UUID。因此，下面的身份校验是严格可靠的——被重新使用的 GPU
// 实例 ID 绝无法伪装成标记记录的分区。这也是为何此处没有尝试从放置位置重新计算 UUID。
type migInstance struct {
	GiID      uint32
	CiID      uint32
	ProfileID uint32
	Placement migPlacement
	// UUID 是厂商为计算实例提供的 mig_uuid。
	UUID string
	// ConfPath 是主机上厂商的计算实例文件。它是注入的载体：每个容器都会获得这个文件，位于相同的路径上，
	// 然后厂商运行时会从这个文件启动实例。
	ConfPath string
}

// migLiveInstance 将实时分区与承载它的加速器的 PCI 地址进行关联。
type migLiveInstance struct {
	PciBusID string
	Instance migInstance
}

// migCardState 是某个加速器针对某个规格的分区状态：即被解析出的厂商规格 ID、该规格的合法空加速器配置位置，
// 以及该加速器上所有处于运行状态的 GPU 实例，无论其属于何种规格。
//
// Live 是特意针对每个规格进行的。放置选择必须避免占用其他规格实例的片段，而仅列出相同规格实例的状态列表则
// 会导致运行被占用。
type migCardState struct {
	ProfileID uint32
	Possible  []migPlacement
	Live      []migInstance
}

// migDriver 是分区分配器面向驱动程序的那部分。它是一个接口，因此预留逻辑无需依赖硬件即可实现，也便于测试。
// 同时，非 Linux 构建版本也有内容可以编译使用。
//
// 每个方法都会报错，而不是描述其无法完整读取的状态。如果加速器在不可用时被处理为空，就会导致同一槽位被处
// 理两次。
type migDriver interface {
	// CardState 会根据指定的规格名称，读取某个加速器当前的分区状态。
	CardState(pciBusID, profile string) (migCardState, error)
	// CreateInstance 会将该规格的 GPU 实例在指定槽位上实例化，以及其内部的完整 GI 计算实例也一并实例
	// 化，并返回包含容器所绑定用于使用的文件的分区。
	CreateInstance(pciBusID, profile string, slot migPlacement) (migInstance, error)
	// DestroyInstance 会按先销毁计算实例、再销毁其 GPU 实例的顺序进行，这是厂商所要求的。当驱动程序因
	// 某些资源仍占用分区而拒绝时，它会返回包含 errInstanceInUse 的错误信息。
	DestroyInstance(pciBusID string, inst migInstance) error
	// ListInstances 会枚举节点上所有处于运行状态的分区，每个分区都带有承载该分区的加速器的 PCI 地址，
	// 因此无标记收集器也能找到此类分区。
	ListInstances() ([]migLiveInstance, error)
}

// migCard 同时保存上层账本使用的 UUID 和访问 Multi-Instance 库所需的 PCI 地址。该库不能按设备 UUID
// 查卡，二者都要从探测记录带入。
type migCard struct {
	UUID     string
	PciBusID string
}

// migCardLocks 会对加速器分区中的每一次修改进行序列化处理，因此两个并发分配操作无法同时选择相同的空闲位置，
// 且回收扫描操作也无法在分配过程仍在进行时销毁实例。
//
// 锁是按加速器进行的，而非全局的，因为驱动程序本身是这样的：在一个卡上创建实例，同时另一个卡上有工作负载
// 运行是安全的，且经测量也确实如此，因此全局锁会导致那些根本不会竞争的资源分配出现串行化问题。
//
// 其识别依据为 PCI 地址，而非加速器 UUID，这属于承载性要求而非偏好设置。这两种身份对应同一张卡，因此持有
// 一种身份的用户与持有另一种身份的用户会获得不同的锁，且无需任何序列化操作——这并非理论假设：回收扫描过程
// 通过驱动程序进行，而驱动程序仅知道 PCI 地址。而分配过程则从 Devices 记录开始，该记录以 UUID 作为识别依
// 据。每个用户都能通过身份进行识别，这就是实现排除机制的关键所在。
var migCardLocks sync.Map

// lockMigCard 获取加速器的锁，并返回其释放。
func lockMigCard(pciBusID string) func() {
	v, _ := migCardLocks.LoadOrStore(pciBusID, &sync.Mutex{})
	mu := v.(*sync.Mutex) //nolint:forcetypeassert
	mu.Lock()
	return mu.Unlock
}

// migMarker 是一个解析后的所有权记录：它描述了容器与其所包含分区之间的关联关系。重试路径会读取此关联以重
// 新绑定，而回收路径则依据此关联来决定系统的活性状态。
//
// 字段名称及其 json 标签采用 ON-DISK 格式。参见 migMarkerName。
type migMarker struct {
	PodUID    string `json:"podUID"`
	Container string `json:"container"`
	Card      string `json:"card"`
	// PciBusID 是加速器的 PCI 地址，之所以记录下来，是因为仅通过多实例库就能访问到该地址——它根本不用
	// 于 UUID 查询。如果没有这个地址，回收机制将读取一个指向某张卡记录的地址，而该卡实际上并不存在。
	PciBusID  string `json:"pciBusID"`
	Profile   string `json:"profile"`
	ProfileID uint32 `json:"profileID"`
	GiID      uint32 `json:"giID"`
	CiID      uint32 `json:"ciID"`
	MigUUID   string `json:"migUUID"`
	ConfPath  string `json:"confPath"`
	Start     int32  `json:"start"`
	Length    int32  `json:"length"`
}

// 实例会重建分区及标记记录。
func (m migMarker) instance() migInstance {
	return migInstance{
		GiID:      m.GiID,
		CiID:      m.CiID,
		ProfileID: m.ProfileID,
		Placement: migPlacement{Start: m.Start, Length: m.Length},
		UUID:      m.MigUUID,
		ConfPath:  m.ConfPath,
	}
}

// migMarkerEntry 将解析后的标记与其路径关联起来，因此回收操作仅会移除该文件。
type migMarkerEntry struct {
	path   string
	marker migMarker
}

// migMarkerFileName 用物理卡的 UUID 命名记录；即便一个容器涉及多张卡，也能逐卡跟踪所有权。
func migMarkerFileName(cardUUID string) string {
	return strings.TrimSuffix(migMarkerName, ".json") + "-" + cardUUID + ".json"
}

// migMarkerPath 在给定的 Pod 工作根目录下拼出当前容器与卡的标记路径。
func migMarkerPath(podsDir, podUID, container, cardUUID string) string {
	return filepath.Join(podsDir, podUID, "c-"+container, migMarkerFileName(cardUUID))
}

// parseMigMarker 会读取记录失败关闭状态：缺失、格式错误或不完整均为错误，因此重新绑定和回收路径永远不会误
// 读活跃分区。原始规格 ID 不会检查其存在性，因为 0 是合法的厂商 ID。
//
// 记录的加速器必须就是文件自身 NAME 所编码的那一个。与名称不符的记录在内部存在矛盾且不安全：所有权集合是
// 根据记录的加速器进行分组的，因此当文件属于某个加速器时，其实例看起来就像是未被拥有的。而第二个 Pod 可能
// 会占用已使用的分区。
func parseMigMarker(path string) (migMarker, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return migMarker{}, err
	}
	var m migMarker
	if err := json.Unmarshal(data, &m); err != nil {
		return migMarker{}, fmt.Errorf("marker %q: %w", path, err)
	}
	if m.PodUID == "" || m.Card == "" || m.PciBusID == "" || m.Profile == "" ||
		m.MigUUID == "" || m.ConfPath == "" {
		return migMarker{}, fmt.Errorf("marker %q: incomplete record", path)
	}
	if card, ok := cardFromMigMarkerPath(path); !ok || card != m.Card {
		return migMarker{}, fmt.Errorf(
			"marker %q records card %q, not the card its file name names: fail closed", path, m.Card,
		)
	}
	return m, nil
}

// writeMigMarker 持久化写入记录，避免并发扫描读到半份文件，并使完整记录能在异常关机后恢复。
//
// 标记由设备插件自己读取。只有写入者需要访问这个文件，容器和厂商库都不读取它。
func writeMigMarker(path string, m migMarker) error {
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

// scanMigMarkers 会解析 podsDir 下的每个分区记录。无法解析的记录会被作为损坏路径处理，而不会导致扫描失败，
// 因此一个损坏的文件不会阻塞节点。调用者可以通过 migOwnershipUnknownOnCard 来确定文件名所代表的加速器，这
// 是唯一一个不可知的归属设置可能改变决策范围的领域。
func scanMigMarkers(podsDir string) (entries []migMarkerEntry, corrupt []string) {
	_ = filepath.WalkDir(podsDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			corrupt = append(corrupt, path)
			return nil
		}
		if d.IsDir() || !isMigMarkerFile(d.Name()) {
			return nil
		}
		m, perr := parseMigMarker(path)
		if perr != nil {
			corrupt = append(corrupt, path)
			return nil //nolint:nilerr
		}
		entries = append(entries, migMarkerEntry{path: path, marker: m})
		return nil
	})
	return entries, corrupt
}

// isMigMarkerFile 用于指示一个名称是否为分区记录。
func isMigMarkerFile(name string) bool {
	base := strings.TrimSuffix(migMarkerName, ".json")
	return strings.HasPrefix(name, base+"-") && strings.HasSuffix(name, ".json")
}

// cardFromMigMarkerPath 恢复该加速器记录文件名称所编码的内容。
func cardFromMigMarkerPath(path string) (string, bool) {
	name := filepath.Base(path)
	if !isMigMarkerFile(name) {
		return "", false
	}
	base := strings.TrimSuffix(migMarkerName, ".json")
	card := strings.TrimSuffix(strings.TrimPrefix(name, base+"-"), ".json")
	if card == "" {
		return "", false
	}
	return card, true
}

// migOwnedGiIDsOnCard 收集这张卡上已被容器认领的 GI 编号。
func migOwnedGiIDsOnCard(entries []migMarkerEntry, cardUUID string) map[uint32]bool {
	owned := make(map[uint32]bool)
	for i := range entries {
		if entries[i].marker.Card == cardUUID {
			owned[entries[i].marker.GiID] = true
		}
	}
	return owned
}

// migOwnershipUnknownOnCard 判断损坏文件是否可能属于当前卡。文件名指向当前卡，或文件名无法识别卡时，
// 归属均不能确定。
//
// 归属不明时停止接管已有实例，避免将已有容器的分区交给第二个容器。
func migOwnershipUnknownOnCard(corrupt []string, cardUUID string) bool {
	for _, path := range corrupt {
		card, ok := cardFromMigMarkerPath(path)
		if !ok || card == cardUUID {
			return true
		}
	}
	return false
}

// pickMigPlacement 从驱动报告的合法位置中，选择起点最低且无重叠的位置。
//
// 总从低位放置可以使布局可复现，并尽量为较大的规格留下连续空间。
func pickMigPlacement(possible, occupied []migPlacement) (migPlacement, bool) {
	candidates := slices.Clone(possible)
	slices.SortFunc(candidates, func(a, b migPlacement) int { return int(a.Start - b.Start) })
	for _, slot := range candidates {
		if !migPlacementOverlapsAny(slot, occupied) {
			return slot, true
		}
	}
	return migPlacement{}, false
}

// migPlacementOverlapsAny 判断候选区间是否与已占用区间有交集。
func migPlacementOverlapsAny(slot migPlacement, occupied []migPlacement) bool {
	for _, o := range occupied {
		if slot.Start < o.Start+o.Length && o.Start < slot.Start+slot.Length {
			return true
		}
	}
	return false
}

// findLiveMigInstance 从实时枚举结果查找指定 GI 编号。
func findLiveMigInstance(state migCardState, giID uint32) (migInstance, bool) {
	for i := range state.Live {
		if state.Live[i].GiID == giID {
			return state.Live[i], true
		}
	}
	return migInstance{}, false
}

// adoptUnboundMigInstance 查找符合目标规格且未被容器认领的实时实例。
//
// 创建完成后写标记失败，或进程在两步之间退出，都可能留下无主实例。接管同一厂商 profile ID 的实例可避
// 免重启后不断积累分区。几何不匹配的实例不能交付给该申请。
func adoptUnboundMigInstance(state migCardState, owned map[uint32]bool) (migInstance, bool) {
	candidates := slices.Clone(state.Live)
	slices.SortFunc(candidates, func(a, b migInstance) int { return int(a.Placement.Start - b.Placement.Start) })
	for _, inst := range candidates {
		if inst.ProfileID == state.ProfileID && !owned[inst.GiID] {
			return inst, true
		}
	}
	return migInstance{}, false
}

// migReserveOutcome 记录本次预留做了什么，决定失败时能撤销哪些状态。
type migReserveOutcome int

const (
	// migRebound 表示沿用该容器自己的记录，本次没有创建实例或新增认领。
	migRebound migReserveOutcome = iota
	// migAdopted 表示为既有的无主实例写了标记，回滚时仅撤销这份认领。
	migAdopted
	// migCreated 表示实例由本次调用创建，回滚时负责销毁。
	migCreated
)

// reserveMigInstance 对一张卡完成预留和登记，调用方持有该卡的锁。
//
// 先尝试复用当前容器已持有的分区，然后尝试接管同规格的无主实例。都不适用时，在最低空闲位置新建 GI 并
// 创建覆盖该 GI 的 CI。
//
// 在同一临界区内写所有权标记。若写入失败，撤销本调用刚创建的分区。
//
// 仅在 err 为 nil 时使用返回的 outcome。预留失败已经撤销本次部分变更，调用方不能再按错误路径返回的枚
// 举值做二次回滚。
func reserveMigInstance(
	drv migDriver, podsDir, podUID, container string, card migCard, profile string,
) (inst migInstance, outcome migReserveOutcome, err error) {
	cardUUID := card.UUID
	self := migMarkerPath(podsDir, podUID, container, cardUUID)

	if m, perr := parseMigMarker(self); perr == nil {
		if m.Profile != profile {
			return migInstance{}, migRebound, fmt.Errorf(
				"marker %q mismatches request (profile=%s): fail closed", self, profile,
			)
		}
		state, serr := drv.CardState(card.PciBusID, profile)
		if serr != nil {
			return migInstance{}, migRebound, fmt.Errorf("read card %s state: %w", cardUUID, serr)
		}
		live, ok := findLiveMigInstance(state, m.GiID)
		if !ok {
			return migInstance{}, migRebound, fmt.Errorf(
				"marker %q references missing gpu instance %d on card %s: fail closed", self, m.GiID, cardUUID,
			)
		}
		// GI 编号可以被复用。此厂商每次创建分区都会生成新的 UUID，因此重新绑定时须比较实时 UUID 和记录中的
		// UUID。
		if live.UUID != m.MigUUID {
			return migInstance{}, migRebound, fmt.Errorf(
				"marker %q gpu instance %d uuid %q no longer matches live uuid %q (id reused): fail closed",
				self, m.GiID, m.MigUUID, live.UUID,
			)
		}
		return m.instance(), migRebound, nil
	} else if !os.IsNotExist(perr) {
		return migInstance{}, migRebound, fmt.Errorf("read self marker %q: %w", self, perr)
	}

	state, err := drv.CardState(card.PciBusID, profile)
	if err != nil {
		return migInstance{}, migCreated, fmt.Errorf("read card %s state: %w", cardUUID, err)
	}

	entries, corrupt := scanMigMarkers(podsDir)
	owned := migOwnedGiIDsOnCard(entries, cardUUID)

	reused, adopt := migInstance{}, false
	if !migOwnershipUnknownOnCard(corrupt, cardUUID) {
		reused, adopt = adoptUnboundMigInstance(state, owned)
	}

	if adopt {
		inst, outcome = reused, migAdopted
	} else {
		occupied := make([]migPlacement, 0, len(state.Live))
		for i := range state.Live {
			occupied = append(occupied, state.Live[i].Placement)
		}
		slot, ok := pickMigPlacement(state.Possible, occupied)
		if !ok {
			return migInstance{}, migCreated, fmt.Errorf(
				"card %s has no free placement for profile %s", cardUUID, profile,
			)
		}
		inst, err = drv.CreateInstance(card.PciBusID, profile, slot)
		if err != nil {
			return migInstance{}, migCreated, fmt.Errorf(
				"create %s instance on card %s: %w", profile, cardUUID, err,
			)
		}
		outcome = migCreated
	}

	m := migMarker{
		PodUID: podUID, Container: container, Card: cardUUID, PciBusID: card.PciBusID, Profile: profile,
		ProfileID: inst.ProfileID, GiID: inst.GiID, CiID: inst.CiID, MigUUID: inst.UUID,
		ConfPath: inst.ConfPath, Start: inst.Placement.Start, Length: inst.Placement.Length,
	}
	if werr := writeMigMarker(self, m); werr != nil {
		if outcome == migCreated {
			// 写标记失败时撤销本次创建。进程真正崩溃而遗留的实例则由后续回收处理。
			_ = drv.DestroyInstance(card.PciBusID, inst)
		}
		return migInstance{}, outcome, werr
	}
	return inst, outcome, nil
}

// migAccelerator 同时保存所属设备组和探测记录，前者用于上报资源键，后者用于构造容器设备节点。
type migAccelerator struct {
	groupID string
	accel   *workercore.Accelerator
}

// migAllocatedAccelerator 查找具有给定身份且属于本次分配的加速器。
func migAllocatedAccelerator(
	devs *workercore.Devices, allocated map[deviceplugin.Resource]int32, cardUUID string,
) (migAccelerator, bool) {
	accelerators := deviceplugin.AllocatedAccelerators(devs, allocated)
	for i := range accelerators {
		if accelerators[i].Accel.ID == cardUUID {
			return migAccelerator{
				groupID: accelerators[i].Group.ID,
				accel:   accelerators[i].Accel,
			}, true
		}
	}
	return migAccelerator{}, false
}

// migAllocatedCards 为每张已分配的卡同时返回 UUID 和 PCI 地址。
//
// 卡缺少 PCI 地址时返回错误。厂商库无法定位这张卡，静默跳过会让容器拿到的设备数少于已分配的设备数。
func migAllocatedCards(
	devs *workercore.Devices, allocated map[deviceplugin.Resource]int32,
) ([]migCard, error) {
	accelerators := deviceplugin.AllocatedAccelerators(devs, allocated)
	cards := make([]migCard, 0, len(accelerators))
	for i := range accelerators {
		accel := accelerators[i].Accel
		if accel.Topology.PciBusID == "" {
			return nil, fmt.Errorf(
				"card %s: no recorded pci address to reach it through the multi-instance library: fail closed",
				accel.ID,
			)
		}
		cards = append(cards, migCard{UUID: accel.ID, PciBusID: accel.Topology.PciBusID})
	}
	return cards, nil
}
