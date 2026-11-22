//go:build linux

package thead

import (
	"fmt"
	"strings"
	"sync"

	"gpustack.ai/gpustack/binding/hgml"
	"gpustack.ai/gpustack/pkg/nodefeature"
)

// newMigDriver 返回了由真实厂商管理库支持的分区驱动程序。该驱动程序仅适用于 Linux：设备管理器也仅在 Linux
// 上运行，若将 cgo 厂商绑定层链接到 Darwin 测试二进制文件（该文件会链接 Go 的插件包），在 dyld 加载未解析
// 的厂商符号时将会失败，因此 Darwin 构建版本使用 mig_driver_other.go 中的 stub 实现。
func newMigDriver() migDriver {
	l := hgml.New()
	return &hgmlMigDriver{
		lib:      l,
		initRet:  l.Init(),
		profiles: make(map[string][]hgml.GpuInstanceProfileInfo_v3),
	}
}

// hgmlMigDriver 即为真正的 migDriver，它通过其 UUID 所指向的加速器，在 GPU/compute-instance 生命周期包装
// 层上实现厂商管理绑定。
//
// 这里的每种枚举结果要么表明其能够证明为完整状态，要么表明存在错误。在查询失败时，它绝不会降级为部分状态，
// 这一要求比厂商实现的方案更为严格：一个看似不存在的活跃分区会将其所有权标记移除，标记为“已消失”，其占用
// 状态会被重新分配，而没有标记的分区则会被遗漏在孤儿收集器中。创建/重用序列以及身份解析功能在真实硬件上得
// 到了验证。与平台无关的核心功能则通过虚假驱动程序在表中进行了测试。
type hgmlMigDriver struct {
	lib *hgml.HGML
	// initRet 记录了库的初始化结果，因此当库无法加载或初始化时，所有操作都能指出一个可解决的根本问题，
	// 而不会在无法运行的调用中导致难以定位的失败。
	initRet hgml.Return

	// 这些规格文件将每个加速器分区规格目录缓存在 profilesMu 目录下。该目录是加速器及其分区模式的属性，
	// 且对于本进程而言，这两种属性都是固定的：只有重启时才会重新加载模式变化信息。缓存该目录是有必要
	// 的，因为对它的探测会遍历厂商所有的规格 ID 空间——共 85 个 ID，每个 ID 对应最多三次库调用，即约
	// 255 次调用——而且这种遍历会在每次分配和每次释放资源时发生，后者还会持有加速器的锁。
	//
	// 空的目录被特意没有缓存。这是当分区模式关闭时，加速器读取的内容。记住它则比重启开启该模式要持久。
	// 锁定是真实的，而非理论上的：一个驱动值被分区和可见性服务器共享。
	profilesMu sync.Mutex
	profiles   map[string][]hgml.GpuInstanceProfileInfo_v3
}

// cardProfileCatalogue 返回加速器分区配置目录，在首次请求提供任何加速器的驱动程序时才会进行探测。
func (d *hgmlMigDriver) cardProfileCatalogue(dev hgml.Device, cardUUID string) ([]hgml.GpuInstanceProfileInfo_v3, error) {
	d.profilesMu.Lock()
	cached, ok := d.profiles[cardUUID]
	d.profilesMu.Unlock()
	if ok {
		return cached, nil
	}

	probed, err := cardProfiles(dev, cardUUID)
	if err != nil {
		return nil, err
	}
	if len(probed) == 0 {
		return probed, nil
	}

	d.profilesMu.Lock()
	d.profiles[cardUUID] = probed
	d.profilesMu.Unlock()
	return probed, nil
}

// CardInstances 枚举了某台加速器下的实时 GPU 实例。它是全节点枚举的工作单位，而回收循环中的验证重读会直接
// 调用它，因此读取操作只需消耗一台加速器，而非整个节点，同时该操作会持有该加速器的锁。
func (d *hgmlMigDriver) CardInstances(cardUUID string) ([]migInstance, error) {
	dev, err := d.device(cardUUID)
	if err != nil {
		return nil, err
	}
	return d.cardInstances(dev, cardUUID)
}

// cardInstances 是一个由调用者已解析的句柄管理的 CardInstances，因此整个节点遍历过程不会让每个加速器被重
// 复解析两次。
func (d *hgmlMigDriver) cardInstances(dev hgml.Device, cardUUID string) ([]migInstance, error) {
	partitioning, err := migModeEnabled(dev, cardUUID)
	if err != nil {
		return nil, err
	}
	if !partitioning {
		return nil, nil
	}
	infos, err := d.cardProfileCatalogue(dev, cardUUID)
	if err != nil {
		return nil, err
	}
	identityByGI, err := migIdentities(dev, cardUUID)
	if err != nil {
		return nil, err
	}
	return liveInstances(dev, cardUUID, infos, identityByGI)
}

// ready 报告了捕获到的初始化失败情况，因此无法正常运行的驱动会在每个入口点都显示此状态，而不是仅显示部分
// 正常。
func (d *hgmlMigDriver) ready() error {
	if !d.initRet.IsSuccess() {
		return fmt.Errorf("vendor management library init failed: %w", d.initRet)
	}
	return nil
}

// InstanceProcesses 统计了在某一分区上运行的计算进程。该计数数据来自用于标识该分区的分区-设备句柄，这些句
// 柄是仅能携带分区自身进程列表的句柄——即加速器为卡片上的每个租户提供的查询响应。
//
// 一个分区可以被进一步细分，每个细分部分都拥有独立的句柄。因此，任何指向该分区的句柄都会被询问，第一个响
// 应并启动进程的句柄就会结束流程：决定是否销毁分区的调用者，实际上是在询问是否有任何进程在运行。如果只处
// 理第一个句柄，就会让一个闲置的细分部分代表忙碌的细分部分发声。驱动程序不认可的索引会被跳过，因为其答案
// 就是被跳过。其他所有无法读取句柄的情况都是错误，因为被静默跳过的句柄可能正是承载进程的那个。
//
// 手柄的匹配是通过 GPU 实例 ID 而非身份字符串进行的，因为仅通过枚举发现的孤立实例可能完全没有身份标识，而
// 其 ID 才是最初定义它的依据。
//
// 没有句柄地址的分区是一个错误，绝不应出现零计数：决定是否销毁该分区的调用者，不得将“无法操作”理解为“无人
// 使用”。
func (d *hgmlMigDriver) InstanceProcesses(cardUUID string, inst migInstance) (int, error) {
	dev, err := d.device(cardUUID)
	if err != nil {
		return 0, err
	}

	count, ret := dev.GetMaxMigDeviceCount()
	if !ret.IsSuccess() {
		if driverReportsAbsent(ret) {
			return 0, fmt.Errorf(
				"card %s: accelerator addresses no partition device: %w", cardUUID, errNoAddressableDevice)
		}
		return 0, fmt.Errorf("card %s: get max partition-device count: %w", cardUUID, ret)
	}
	addressed := false
	for i := 0; i < count; i++ {
		mig, ret := dev.GetMigDeviceHandleByIndex(i)
		if !ret.IsSuccess() {
			if driverReportsAbsent(ret) {
				continue
			}
			return 0, fmt.Errorf("card %s: get partition-device handle %d: %w", cardUUID, i, ret)
		}
		if mig == nil {
			return 0, fmt.Errorf(
				"card %s: partition-device handle %d is absent though the driver reported success", cardUUID, i)
		}
		giID, ret := mig.GetGpuInstanceId()
		if !ret.IsSuccess() {
			return 0, fmt.Errorf(
				"card %s: get owning gpu-instance id of partition device %d: %w", cardUUID, i, ret)
		}
		if giID != inst.GiID {
			continue
		}
		addressed = true
		procs, ret := mig.GetComputeRunningProcesses()
		if !ret.IsSuccess() {
			return 0, fmt.Errorf(
				"card %s: list compute processes on gpu instance %d: %w", cardUUID, inst.GiID, ret)
		}
		if len(procs) > 0 {
			return len(procs), nil
		}
	}
	if !addressed {
		return 0, fmt.Errorf(
			"card %s: gpu instance %d: %w", cardUUID, inst.GiID, errNoAddressableDevice)
	}
	return 0, nil
}

// 设备从操作员用于指代加速器的 UUID 中解析出加速器句柄。
func (d *hgmlMigDriver) device(cardUUID string) (hgml.Device, error) {
	if err := d.ready(); err != nil {
		return hgml.Device{}, err
	}
	dev, ret := d.lib.DeviceGetHandleByUUID(cardUUID)
	if !ret.IsSuccess() {
		return hgml.Device{}, fmt.Errorf("get device handle for card %s: %w", cardUUID, ret)
	}
	return dev, nil
}

// driverReportsAbsent 用于报告返回码是否为驱动程序自己的声明，即被探测的 ID 或索引在该加速器上没有任何结
// 果，这通常是大多数探测场景下的正常情况。在探测过程中，只有这三种代码会被跳过。其他所有失败情况都意味着
// 查询本身无法得到回答，而无法证明完整的枚举也必然会失败。缺失的库符号特意未被包含在此集合中：这样做是为
// 了让所有 ID 变得不可读，而不是使其不存在，同时静默报告空加速器结果也是该契约旨在防止的失败情况。
func driverReportsAbsent(ret hgml.Return) bool {
	switch ret {
	case hgml.ERROR_NOT_SUPPORTED, hgml.ERROR_NOT_FOUND, hgml.ERROR_INVALID_ARGUMENT:
		return true
	default:
		return false
	}
}

// driverReportsInUse 报告是否因剩余进程仍持有实例而拒绝销毁——这种是受限制的、可重试的部分失败，被回收循环
// 识别为可恢复的情况，而非永久性的失败。
func driverReportsInUse(ret hgml.Return) bool {
	return ret == hgml.ERROR_IN_USE
}

// migModeEnabled 用于指示加速器当前是否处于分区模式，因此那些确实没有分区的加速器会被全局枚举机制跳过。如
// 果无法读取该模式，则会出现错误：无法读取的加速器分区状态未知，将未知情况视为“没有内容”与另一种情况下的“
// 活跃分区被识别为不存在”的失败情况相同。如果驱动程序表明该模式不受支持，则该加速器不可分区，这也是其自身
// 的答案。
func migModeEnabled(dev hgml.Device, cardUUID string) (bool, error) {
	current, _, ret := dev.GetMigMode()
	if ret.IsSuccess() {
		return current == hgml.DEVICE_MIG_ENABLE, nil
	}
	if ret == hgml.ERROR_NOT_SUPPORTED {
		return false, nil
	}
	return false, fmt.Errorf("card %s: read partitioning mode: %w", cardUUID, ret)
}

// cardProfiles 会查询加速器的整个 GPU-实例规格空间，并返回驱动程序所响应的所有规格。该探针需要规格的 ENUM
// INDEX 值，而后续调用则使用探针在规格自身记录中返回的创建 ID。由于这两种 ID 在硬件层面存在差异，因此这里
// 永远不会计算 ID，仅被保留。
//
// 它返回所有规格，包括媒体和图形变体。GPUStack 并未提供这些功能：占用率视图必须考虑任何规格的划分，因为宽
// 实例会阻碍窄实例的放置。只有名称解析才会过滤掉这些变体。
func cardProfiles(dev hgml.Device, cardUUID string) ([]hgml.GpuInstanceProfileInfo_v3, error) {
	infos := make([]hgml.GpuInstanceProfileInfo_v3, 0, hgml.GPU_INSTANCE_PROFILE_COUNT)
	for id := uint32(0); id < hgml.GPU_INSTANCE_PROFILE_COUNT; id++ {
		info, ret := dev.GetGpuInstanceProfileInfo(id)
		if ret.IsSuccess() {
			infos = append(infos, info)
			continue
		}
		if driverReportsAbsent(ret) {
			continue
		}
		return nil, fmt.Errorf("card %s: probe gpu-instance profile %d: %w", cardUUID, id, ret)
	}
	return infos, nil
}

// isMediaOrGraphicsProfile 用于报告被探测到的规格是媒体引擎还是图形变体，而 GPUStack 既不发布也不分配此类
// 规格：只有能够覆盖整个 GPU 实例的计算实例的普通规格才可被请求。每个变体名称中都会带有“+..."的后缀，而图
// 形能力位则作为不同名称的支撑。该规格的 ID 被刻意不引用：该厂商在其头部保留了上游的编号，但未赋予其 ID
// 上游的含义，因此 ID 范围可能会导致重复。这反映了检测器在发布规格时应用的过滤机制，因此往返过程中的两端
// 都认为同一组规格是可请求的。
func isMediaOrGraphicsProfile(info hgml.GpuInstanceProfileInfo_v3, name string) bool {
	return strings.ContainsRune(name, '+') ||
		info.Capabilities&hgml.GPU_INSTANCE_PROFILE_CAPS_GFX != 0
}

// sameProfileOffer 用于报告两个个人资料是否在所有发布的字段中描述相同的优惠。等价性是解决某个请求名称对应
// 其中任何一个的安全保证。其他情况则属于矛盾，调用者必须拒绝。
func sameProfileOffer(a, b hgml.GpuInstanceProfileInfo_v3) bool {
	return a.SliceCount == b.SliceCount &&
		a.MemorySizeMB == b.MemorySizeMB &&
		a.InstanceCount == b.InstanceCount
}

// matchProfile 通过比对被查询的规格名称与加速器自身的 GPU-实例规格记录，将名称标准化后完成匹配。该名称是
// 唯一的键：计算切片计数无法选择特定规格，因为该厂商的标识没有切片计数的含义，且多个规格可以共享宽度。
//
// 整个往返过程的双方都会通过同一个共享函数进行标准化处理：检测器会从标准化的驱动程序名称中提取规格的资源
// 键，再通过以相同方式对驱动程序名称进行标准化，将该键重新转换为原始的驱动程序 ID。如果复制出那些会变动的
// 规则，那么发布的规格就会变得无法被请求使用。
//
// 两个标准化为同一个请求名称的档案只有在其描述相同的产品时才能被解决——检测器会保留一个与读数不符的名称，
// 因此这里的矛盾即代表已发布内容与当前报告内容之间的差异。无论如何解决，都会产生一个并非所要求的划分结果。
func matchProfile(
	infos []hgml.GpuInstanceProfileInfo_v3, cardUUID, profile string,
) (hgml.GpuInstanceProfileInfo_v3, error) {
	want := nodefeature.NormalizePartitionedProfileName(profile)

	var (
		matched hgml.GpuInstanceProfileInfo_v3
		found   bool
	)
	for i := range infos {
		info := infos[i]
		raw := info.GetName()
		if isMediaOrGraphicsProfile(info, raw) {
			continue
		}
		if nodefeature.NormalizePartitionedProfileName(raw) != want {
			continue
		}
		switch {
		case !found:
			matched, found = info, true
		case info.Id == matched.Id:
		case !sameProfileOffer(matched, info):
			return hgml.GpuInstanceProfileInfo_v3{}, fmt.Errorf(
				"card %s: profile %q is reported by two gpu-instance profiles describing different offers"+
					" (id %d: %d compute slices, %d MiB, count %d; id %d: %d compute slices, %d MiB, count %d): fail closed",
				cardUUID, profile,
				matched.Id, matched.SliceCount, matched.MemorySizeMB, matched.InstanceCount,
				info.Id, info.SliceCount, info.MemorySizeMB, info.InstanceCount)
		case info.Id < matched.Id:
			matched = info
		}
	}
	if !found {
		return hgml.GpuInstanceProfileInfo_v3{}, fmt.Errorf(
			"card %s has no gpu-instance profile named %q", cardUUID, profile)
	}
	return matched, nil
}

// migIdentity 是实时分区地址记录，由某个分区设备报告：即容器所分配的身份字符串，以及该身份所指向的计算实
// 例 ID 及其能力节点。两者都来自同一个分区设备句柄，因此无法描述两个不同的计算实例——将一个分区的身份与另
// 一个分区的能力节点混用，属于隔离故障，而非标签错误。
type migIdentity struct {
	UUID string
	CiID uint32
}

// migIdentities 通过枚举其分区设备句柄并读取每个设备的 GPU 实例 ID，将加速器上每个 GPU 实例的 ID 映射到其
// 分区地址记录中。尚未实现分区的 GPU 实例在映射中根本不存在——即没有其计算实例的 GPU 实例，这种实例无法指
// 向任何内容，且会被回收机制破坏。
//
// 任何超出驱动程序“当前索引无设备”回答范围的失败情况，都属于错误：一个无法被识别的句柄——无论是身份、计算
// 实例还是所有者——使得地图缺少一个活跃的分区，而缺失的身份特性使得该分区可被回收。计算实例的 ID 会被读取，
// 不会默认设置，原因与规格 ID 的设定相同：0 是一个合法的 ID，因此由无法读取的查询留下的零值会指向分区 ci0
// ——要么是一个不存在的路径，要么是另一个计算实例的能力节点。
//
// 两个分区设备报告相同的拥有 GPU 实例是一个错误，而非“最后一个存活”的覆盖行为。GPUStack 每个 GPU 实例都会
// 分配一个计算实例，因此第二个实例是它未创建的，映射表中恰好有一个槽位用于描述它：无论驱动程序最终返回哪
// 个值，都将成为该 GPU 实例的记录地址。随后，Reclaim 会读取其标记未记录的计算实例 ID，并丢弃该标记而不破
// 坏分区，而同一加速器的预留资源则会在不匹配时失败，并持续失败直到额外设备存在。拒绝命名加速器而非默默处
// 理错误部分——这种拒绝会导致加速器的枚举失败，进而导致节点的枚举失败，因此当存在此类设备时，节点上没有任
// 何资源被回收。
func migIdentities(dev hgml.Device, cardUUID string) (map[uint32]migIdentity, error) {
	count, ret := dev.GetMaxMigDeviceCount()
	if !ret.IsSuccess() {
		return nil, fmt.Errorf("card %s: get max partition-device count: %w", cardUUID, ret)
	}

	out := make(map[uint32]migIdentity, count)
	for i := 0; i < count; i++ {
		mig, ret := dev.GetMigDeviceHandleByIndex(i)
		if !ret.IsSuccess() {
			if driverReportsAbsent(ret) {
				continue
			}
			return nil, fmt.Errorf("card %s: get partition-device handle %d: %w", cardUUID, i, ret)
		}
		if mig == nil {
			return nil, fmt.Errorf(
				"card %s: partition-device handle %d is absent though the driver reported success", cardUUID, i)
		}
		// 直接从分区设备句柄中读取所属 GPU 实例 ID。要解析完整的实例信息，需要父加速器句柄，而在
		// 此情况下无法实现。
		giID, ret := mig.GetGpuInstanceId()
		if !ret.IsSuccess() {
			return nil, fmt.Errorf(
				"card %s: get owning gpu-instance id of partition device %d: %w", cardUUID, i, ret)
		}
		ciID, ret := mig.GetComputeInstanceId()
		if !ret.IsSuccess() {
			return nil, fmt.Errorf(
				"card %s: get compute-instance id of partition device %d: %w", cardUUID, i, ret)
		}
		uuid, ret := mig.GetUUID()
		if !ret.IsSuccess() {
			return nil, fmt.Errorf("card %s: get identity of partition device %d: %w", cardUUID, i, ret)
		}
		if prev, dup := out[giID]; dup {
			return nil, fmt.Errorf(
				"card %s: gpu instance %d owns more than one partition device (%s ci%d and %s ci%d): "+
					"refusing to address it by either",
				cardUUID, giID, prev.UUID, prev.CiID, uuid, ciID)
		}
		out[giID] = migIdentity{UUID: uuid, CiID: ciID}
	}
	return out, nil
}

// liveInstances 列出了加速器上所有处于运行状态的 GPU 实例，这些实例属于所有规格。每个实例都携带其原始规格
// ID、计算切片数量、内存切片位置以及分区地址记录——即身份字符串和计算实例 ID。占用率必须覆盖所有规格，因为
// 某个规格下的实例会占据其他规格可能使用的位置。
//
// 地址记录的重要性不仅仅体现在占用情况上：其中一些实例是被采用的实例，它写入的标记则决定了分配机制如何确
// 定能力节点的归属。那些计算实例 ID 被置零的被采用实例会指向 ci0——这种路径要么不存在，导致分配失败，并且
// 在每次重试时都会再次失败，直到被回收。要么属于其他计算实例，从而让容器使用错误的分区节点。
//
// 失败的实例查询属于错误情况，而非被跳过的规格。原始规格 ID 是从实例本身读取的，而非默认值：厂商的编号设
// 定下，0 是一个合法的 ID，因此因无法读取的查询而留下的零号会被视为一个真实的规格，并可能被采用。实例报告
// 的规格与其被枚举的规格不符，属于自相矛盾的情况，以任何方式解决该问题都可能使实例被用于与其不匹配的规格。
func liveInstances(
	dev hgml.Device, cardUUID string, infos []hgml.GpuInstanceProfileInfo_v3,
	identityByGI map[uint32]migIdentity,
) ([]migInstance, error) {
	var live []migInstance
	for i := range infos {
		info := infos[i]
		gis, ret := dev.GetGpuInstances(info.Id)
		if !ret.IsSuccess() {
			return nil, fmt.Errorf(
				"card %s: list live gpu instances of profile %d: %w", cardUUID, info.Id, ret)
		}
		for j := range gis {
			gi := gis[j].GetInfo()
			if gi.ProfileId != info.Id {
				return nil, fmt.Errorf(
					"card %s: gpu instance %d reports profile %d while enumerated under profile %d: fail closed",
					cardUUID, gi.Id, gi.ProfileId, info.Id)
			}
			identity := identityByGI[gi.Id]
			live = append(live, migInstance{
				GiID:          gi.Id,
				CiID:          identity.CiID,
				ProfileID:     gi.ProfileId,
				ComputeSlices: int32(info.SliceCount),
				Placement:     migPlacement{Start: int32(gi.Placement.Start), Length: int32(gi.Placement.Size)},
				UUID:          identity.UUID,
			})
		}
	}
	return live, nil
}

// wholeGiComputeProfile 会在刚刚创建的 GPU 实例上发现整个 GPU 实例的计算实例规格——这是 GPUStack 创建的唯
// 一形态，因为容器会获得整个实例。它是通过探测该 GPU 实例自身的计算实例规格空间来发现的，且规格范围与 GPU
// 实例一致，并非固定值：该厂商对其 ID 没有赋予任何上游含义，因此硬编码的 ID 会导致冲突。
//
// 该探针会遍历规格*索引*空间，而一个索引并不等同于一个身份：此驱动程序使用同一个规格回应多个索引，因此通
// 过 ID 已识别的候选项会被跳过。将别名视为第二个候选项会导致错误，因为某个产品仅提供一种规格——这正是整个
// 加速器分区所遇到的状况，其中两个索引都使用驱动程序的单一默认规格进行回应。
//
// 真正的模糊性仍然无法被解决。与 GPU 实例规格不同，计算实例规格在实例存在之前就已被探测到，既没有名称也没
// 有能力位，因此普通的完整实例规格无法与完整实例媒体或图形变体区分开来。仅凭猜测来创建会赋予容器错误的分
// 区，因此每次报告都会包含多个明确识别的候选方案及其 ID 和宽度，这些正是决定该产品是否需要携带名称的探针
// 的依据。
func wholeGiComputeProfile(
	gi hgml.GpuInstance, cardUUID string, giProfile hgml.GpuInstanceProfileInfo_v3,
) (hgml.ComputeInstanceProfileInfo, error) {
	var candidates []hgml.ComputeInstanceProfileInfo
	seen := make(map[uint32]struct{})
	for ciID := uint32(0); ciID < hgml.COMPUTE_INSTANCE_PROFILE_COUNT; ciID++ {
		info, ret := gi.GetComputeInstanceProfileInfo(ciID, hgml.COMPUTE_INSTANCE_ENGINE_PROFILE_SHARED)
		if !ret.IsSuccess() {
			if driverReportsAbsent(ret) {
				continue
			}
			return hgml.ComputeInstanceProfileInfo{}, fmt.Errorf(
				"card %s: probe compute-instance profile %d of gpu-instance profile %d: %w",
				cardUUID, ciID, giProfile.Id, ret)
		}
		if info.SliceCount != giProfile.SliceCount {
			continue
		}
		if _, alias := seen[info.Id]; alias {
			continue
		}
		seen[info.Id] = struct{}{}
		candidates = append(candidates, info)
	}

	switch len(candidates) {
	case 0:
		return hgml.ComputeInstanceProfileInfo{}, fmt.Errorf(
			"card %s: gpu-instance profile %d (%d compute slices) offers no compute-instance profile"+
				" spanning the whole gpu instance",
			cardUUID, giProfile.Id, giProfile.SliceCount)
	case 1:
		return candidates[0], nil
	default:
		return hgml.ComputeInstanceProfileInfo{}, fmt.Errorf(
			"card %s: gpu-instance profile %d (%d compute slices) offers %d compute-instance profiles spanning"+
				" the whole gpu instance (%s), and a pre-create probe carries no name to tell the plain one from a"+
				" media or graphics variant: fail closed",
			cardUUID, giProfile.Id, giProfile.SliceCount, len(candidates), describeComputeProfiles(candidates))
	}
}

// describeComputeProfiles 将计算实例规格候选项渲染为“id N: M compute slices”条目，因此可以通过一条日志行
// 来识别歧义。
func describeComputeProfiles(infos []hgml.ComputeInstanceProfileInfo) string {
	parts := make([]string, 0, len(infos))
	for i := range infos {
		parts = append(parts, fmt.Sprintf("id %d: %d compute slices", infos[i].Id, infos[i].SliceCount))
	}
	return strings.Join(parts, "; ")
}

func (d *hgmlMigDriver) CardState(cardUUID, profile string, _, _ int32) (migCardState, error) {
	dev, err := d.device(cardUUID)
	if err != nil {
		return migCardState{}, err
	}
	infos, err := d.cardProfileCatalogue(dev, cardUUID)
	if err != nil {
		return migCardState{}, err
	}
	matched, err := matchProfile(infos, cardUUID, profile)
	if err != nil {
		return migCardState{}, err
	}

	slots, ret := dev.GetGpuInstancePossiblePlacements(matched.Id)
	if !ret.IsSuccess() {
		return migCardState{}, fmt.Errorf(
			"card %s: get possible placements of profile %d: %w", cardUUID, matched.Id, ret)
	}
	possible := make([]migPlacement, 0, len(slots))
	for i := range slots {
		possible = append(possible, migPlacement{Start: int32(slots[i].Start), Length: int32(slots[i].Size)})
	}

	identityByGI, err := migIdentities(dev, cardUUID)
	if err != nil {
		return migCardState{}, err
	}
	live, err := liveInstances(dev, cardUUID, infos, identityByGI)
	if err != nil {
		return migCardState{}, err
	}

	return migCardState{ProfileID: matched.Id, Possible: possible, Live: live}, nil
}

// CreateInstance 会在指定位置实例化指定规格的 GPU 实例，并生成覆盖该实例的计算实例，然后报告分区的 ID、原
// 始规格 ID、几何信息及身份信息——这些信息由驱动程序自行报告，而非按照请求时的形式提供。
//
// GPU 实例存在后发生的每一次失败都会将其摧毁（首先是其计算实例，在创建时就已经存在），因此半建成的分区无
// 法存活并被采用：没有计算实例的 GPU 实例毫无意义，而没有身份字符串的分区也无法被交付给容器或记录在失败关
// 闭的标记中。
func (d *hgmlMigDriver) CreateInstance(
	cardUUID, profile string, _, _ int32, slot migPlacement,
) (migInstance, error) {
	dev, err := d.device(cardUUID)
	if err != nil {
		return migInstance{}, err
	}
	infos, err := d.cardProfileCatalogue(dev, cardUUID)
	if err != nil {
		return migInstance{}, err
	}
	matched, err := matchProfile(infos, cardUUID, profile)
	if err != nil {
		return migInstance{}, err
	}

	placement := &hgml.GpuInstancePlacement{Start: uint32(slot.Start), Size: uint32(slot.Length)}
	gi, ret := dev.CreateGpuInstanceWithPlacement(&hgml.GpuInstanceProfileInfo{Id: matched.Id}, placement)
	if !ret.IsSuccess() {
		return migInstance{}, fmt.Errorf(
			"card %s: create gpu instance of profile %d at placement %d:%d: %w",
			cardUUID, matched.Id, slot.Start, slot.Length, ret)
	}
	giInfo := gi.GetInfo()
	if giInfo.ProfileId != matched.Id {
		_ = gi.Destroy()
		return migInstance{}, fmt.Errorf(
			"card %s: created gpu instance %d reports profile %d, not the requested %d: fail closed",
			cardUUID, giInfo.Id, giInfo.ProfileId, matched.Id)
	}

	ciProfile, err := wholeGiComputeProfile(gi, cardUUID, matched)
	if err != nil {
		_ = gi.Destroy()
		return migInstance{}, err
	}
	ci, ret := gi.CreateComputeInstance(&ciProfile)
	if !ret.IsSuccess() {
		_ = gi.Destroy()
		return migInstance{}, fmt.Errorf(
			"card %s: create compute instance of profile %d on gpu instance %d: %w",
			cardUUID, ciProfile.Id, giInfo.Id, ret)
	}

	identityByGI, err := migIdentities(dev, cardUUID)
	if err != nil {
		_ = ci.Destroy()
		_ = gi.Destroy()
		return migInstance{}, err
	}
	uuid := identityByGI[giInfo.Id].UUID
	if uuid == "" {
		_ = ci.Destroy()
		_ = gi.Destroy()
		return migInstance{}, fmt.Errorf(
			"card %s: created gpu instance %d has no partition identity", cardUUID, giInfo.Id)
	}

	return migInstance{
		GiID:          giInfo.Id,
		CiID:          ci.GetInfo().Id,
		ProfileID:     giInfo.ProfileId,
		ComputeSlices: int32(matched.SliceCount),
		Placement: migPlacement{
			Start:  int32(giInfo.Placement.Start),
			Length: int32(giInfo.Placement.Size),
		},
		UUID: uuid,
	}, nil
}

// ListInstances 会枚举节点分区加速器上所有活跃的 GPU 实例，每个实例都带有其加速器 ID，因此无主收集器能够
// 在被释放的加速器上找到没有标记的实例。GPU 实例不携带操作器标签，因此这是唯一能发现未被追踪的实例的方式。
//
// 驾驶员应答的加速器在分区模式下不会保留分区，会被跳过。那些无法读取其模式、配置、身份或实例的加速器则会
// 导致整个枚举过程失败，因为操作列表中的调用者若缺失某个加速器的分区，就会破坏或重复占用它无法看到的资源。
func (d *hgmlMigDriver) ListInstances() ([]migLiveInstance, error) {
	if err := d.ready(); err != nil {
		return nil, err
	}
	count, ret := d.lib.DeviceGetCount()
	if !ret.IsSuccess() {
		return nil, fmt.Errorf("get device count: %w", ret)
	}

	var out []migLiveInstance
	for i := 0; i < count; i++ {
		dev, ret := d.lib.DeviceGetHandleByIndex(i)
		if !ret.IsSuccess() {
			return nil, fmt.Errorf("get device handle at index %d: %w", i, ret)
		}
		cardUUID, ret := dev.GetUUID()
		if !ret.IsSuccess() {
			return nil, fmt.Errorf("get card uuid at device index %d: %w", i, ret)
		}

		live, err := d.cardInstances(dev, cardUUID)
		if err != nil {
			return nil, err
		}
		for j := range live {
			out = append(out, migLiveInstance{Card: cardUUID, Inst: live[j]})
		}
	}
	return out, nil
}

// DestroyInstance 会销毁调用者捕获的分区，该分区受调用者持有的加速器锁保护。它会重新读取关键段内该加速器
// 的实时数据，并验证 GPU 实例 ID 是否仍保留着记录的身份信息，然后再销毁任何内容：被销毁的实例 ID 可以被重
// 新分配，因此经过一次分配后已过期的快照可能会指向另一个——可能仍存活的——分区。如果数据不匹配，则不会销毁
// 任何内容，并将矛盾情况返回。
//
// 如果完整枚举中缺少一个 id，则意味着该分区已消失，这是成功的：回收循环对所有权标记的移除依赖于其幂等性。
func (d *hgmlMigDriver) DestroyInstance(cardUUID string, inst migInstance) error {
	dev, err := d.device(cardUUID)
	if err != nil {
		return err
	}
	infos, err := d.cardProfileCatalogue(dev, cardUUID)
	if err != nil {
		return err
	}
	identityByGI, err := migIdentities(dev, cardUUID)
	if err != nil {
		return err
	}

	for i := range infos {
		gis, ret := dev.GetGpuInstances(infos[i].Id)
		if !ret.IsSuccess() {
			return fmt.Errorf(
				"card %s: list live gpu instances of profile %d: %w", cardUUID, infos[i].Id, ret)
		}
		for j := range gis {
			giInfo := gis[j].GetInfo()
			if giInfo.Id != inst.GiID {
				continue
			}
			if verr := verifyInstanceIdentity(cardUUID, inst, giInfo, identityByGI[giInfo.Id].UUID); verr != nil {
				return verr
			}
			return destroyGpuInstance(cardUUID, gis[j], inst.GiID)
		}
	}
	return nil
}

// verifyInstanceIdentity 会通过原始规格 ID、身份字符串以及位置信息，验证记录下来的实时 GPU 实例是否仍属于
// 记录中的分区。该位置检测与身份检测并列为一种不一致性检测机制：对于自洽的驱动程序而言，该检测是多余的。
// 而当一个实例与其中一个状态不符时，这种不匹配状态正是“销毁”操作必须拒绝而非解决的情况。
func verifyInstanceIdentity(
	cardUUID string, inst migInstance, live hgml.GpuInstanceInfo, liveUUID string,
) error {
	if live.ProfileId != inst.ProfileID {
		return fmt.Errorf(
			"card %s: gpu instance %d now carries profile %d, not the recorded %d: refusing to destroy",
			cardUUID, inst.GiID, live.ProfileId, inst.ProfileID)
	}
	if liveUUID != inst.UUID {
		return fmt.Errorf(
			"card %s: gpu instance %d now carries identity %q, not the recorded %q: refusing to destroy",
			cardUUID, inst.GiID, liveUUID, inst.UUID)
	}
	if int32(live.Placement.Start) != inst.Placement.Start ||
		int32(live.Placement.Size) != inst.Placement.Length {
		return fmt.Errorf(
			"card %s: gpu instance %d now occupies %d:%d, not the recorded %d:%d: refusing to destroy",
			cardUUID, inst.GiID,
			live.Placement.Start, live.Placement.Size, inst.Placement.Start, inst.Placement.Length)
	}
	return nil
}

// destroyGpuInstance 会按以下顺序移除 GPU 实例的计算实例，然后再移除该实例本身：因为驱动程序会拒绝仍保留
// 计算实例的 GPU 实例。无论是哪种情况导致的繁忙拒绝，都会映射到共享的正在使用的错误中，因此回收循环将其视
// 为有边界的、可重试的部分失败，而非永久性的失败。
//
// 它会移除 GPU 实例在任何规格配置下所报告的每一个计算实例，而不仅仅是由 GPUStack 创建的整个实例索引所涉及
// 的实例。这一范围比厂商实现中的范围更广，后者会采用不同的方式处理：其他索引下的非带内或部分创建的计算实
// 例会导致 GPU 实例的清理操作被永久拒绝，从而将有约束的重试机制变成无法清除的永久阻塞状态，节点上的任何操
// 作都无法解决这一问题。
//
// “任何规格”都只是那些索引中的任意一个，而非任何推理引擎的规格——这一点值得指出，因为下面的列举仅通过
// SHARED 推理引擎规格来解析每个索引，因此读者可能会误以为这种遍历范围比实际更窄。实际上并非如此：SHARED
// 是厂商头文件所定义的唯一推理引擎规格（COMPUTE_INSTANCE_ENGINE_PROFILE_COUNT 为 1，如上游所示），因此不
// 存在一个计算实例可以存在却未被发现的情况。如果后续头文件增加了新的规格，这种遍历将不再全面，从而又会出
// 现一直忙碌的情况。
func destroyGpuInstance(cardUUID string, gi hgml.GpuInstance, giID uint32) error {
	for ciID := uint32(0); ciID < hgml.COMPUTE_INSTANCE_PROFILE_COUNT; ciID++ {
		cis, ret := gi.GetComputeInstances(ciID)
		if !ret.IsSuccess() {
			if driverReportsAbsent(ret) {
				continue
			}
			return fmt.Errorf(
				"card %s: list compute instances of profile %d on gpu instance %d: %w",
				cardUUID, ciID, giID, ret)
		}
		for k := range cis {
			ciInfo := cis[k].GetInfo()
			if r := cis[k].Destroy(); !r.IsSuccess() {
				if driverReportsInUse(r) {
					return fmt.Errorf(
						"card %s: destroy compute instance %d on gpu instance %d: %w",
						cardUUID, ciInfo.Id, giID, errInstanceInUse)
				}
				return fmt.Errorf(
					"card %s: destroy compute instance %d on gpu instance %d: %w",
					cardUUID, ciInfo.Id, giID, r)
			}
		}
	}

	if r := gi.Destroy(); !r.IsSuccess() {
		if driverReportsInUse(r) {
			return fmt.Errorf("card %s: destroy gpu instance %d: %w", cardUUID, giID, errInstanceInUse)
		}
		return fmt.Errorf("card %s: destroy gpu instance %d: %w", cardUUID, giID, r)
	}
	return nil
}
