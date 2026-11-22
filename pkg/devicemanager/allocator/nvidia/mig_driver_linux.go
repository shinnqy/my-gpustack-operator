package nvidia

import (
	"fmt"
	"strings"
	"sync"

	"gpustack.ai/gpustack/binding/nvml"
)

// newMigDriver 返回了真正的基于 NVML 的 MIG 驱动程序。该驱动程序仅适用于 Linux：设备管理器也仅在 Linux 上
// 运行，若将 cgo 绑定/NVML 链接到 Darwin 测试二进制文件中（该文件会链接 Go 的插件包），在加载未解析的
// NVML 符号时会导致 dyld 崩溃，因此 Darwin 构建版本使用 mig_driver_other.go 中的 stub 实现。
func newMigDriver() migDriver {
	l := nvml.New()
	return &nvmlMigDriver{
		lib:      l,
		initRet:  l.Init(),
		profiles: make(map[string][]nvml.GpuInstanceProfileInfo_v3),
	}
}

// nvmlMigDriver 就是真正的 migDriver，它通过 GPU UUID 所指向的加速器，在 MIG GPU/compute-instance 生命周
// 期包装器之上实现绑定/nvml 功能。具体的创建/重用流程以及 MIG-设备 UUID 的解析方式已在真实硬件上得到验证
// （F8 e2e）。所有单元测试均使用虚拟驱动程序。
type nvmlMigDriver struct {
	lib *nvml.NVML
	// initRet 捕获了 nvmlInit 的结果，因此当库无法加载/初始化时，第一次调用 device() 就能指出一个可解
	// 决的根本问题。
	initRet nvml.Return

	// 这些规格文件将每个加速器의 MIG 规格目录缓存在 profilesMu 目录下。该目录是加速器及其 MIG 模式的
	// 属性，且这两个属性在本流程中都是固定的：模式变更仅能通过重置和重启来触发。缓存该目录是有必要的，
	// 因为对它的探测会遍历整个规格文件 ID 空间，而这种遍历会在每次分配和每次释放资源时发生，后者还涉
	// 及使用加速器的锁。
	//
	// 空的目录被特意没有缓存。这就是在 MIG 关闭时加速器所读取的内容，记住它将会持续存在，超越重启后
	// MIG 重新启动的状态。锁定是真实的，而非理论上的：一个驱动值由分区和可见性服务器共同使用。
	profilesMu sync.Mutex
	profiles   map[string][]nvml.GpuInstanceProfileInfo_v3
}

// cardProfileCatalogue 返回了加速器的 MIG 规格目录，在首次请求提供任何类型加速器的场景下，仅会探测 NVML。
func (d *nvmlMigDriver) cardProfileCatalogue(dev nvml.Device, cardUUID string) ([]nvml.GpuInstanceProfileInfo_v3, error) {
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
func (d *nvmlMigDriver) CardInstances(cardUUID string) ([]migInstance, error) {
	dev, err := d.device(cardUUID)
	if err != nil {
		return nil, err
	}
	return d.cardInstances(dev, cardUUID)
}

// cardInstances 是一个由调用者已解析的句柄管理的 CardInstances 对象，因此整个节点遍历不会让每个加速器被重
// 复解析。那些其驱动程序声明放弃 MIG 的加速器则不会产生任何贡献。
func (d *nvmlMigDriver) cardInstances(dev nvml.Device, cardUUID string) ([]migInstance, error) {
	uuidByGI, err := d.migUUIDs(dev, cardUUID)
	if err != nil {
		return nil, err
	}
	infos, err := d.cardProfileCatalogue(dev, cardUUID)
	if err != nil {
		return nil, err
	}
	return liveInstances(dev, cardUUID, infos, uuidByGI)
}

// InstanceProcesses 统计了在某一分区上运行的计算进程。该计数数据来自用于标识该分区的 MIG 设备句柄，这些句
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
// 没有句柄地址的分区是一个错误，绝不应出现零计数的情况：决定是否销毁该分区的调用者，不得将“无法操作”理解
// 为“没有人使用它”。除非容器被允许访问驱动程序的 MIG 能力，否则根本不会出现句柄。
func (d *nvmlMigDriver) InstanceProcesses(cardUUID string, inst migInstance) (int, error) {
	dev, err := d.device(cardUUID)
	if err != nil {
		return 0, err
	}

	count, ret := dev.GetMaxMigDeviceCount()
	if !ret.IsSuccess() {
		if driverReportsAbsent(ret) {
			return 0, fmt.Errorf("card %s: accelerator addresses no mig device: %w", cardUUID, errNoAddressableDevice)
		}
		return 0, fmt.Errorf("card %s: get max mig device count: %w", cardUUID, ret)
	}
	addressed := false
	for i := 0; i < count; i++ {
		mig, ret := dev.GetMigDeviceHandleByIndex(i)
		if !ret.IsSuccess() {
			if driverReportsAbsent(ret) {
				continue
			}
			return 0, fmt.Errorf("card %s: get mig device handle %d: %w", cardUUID, i, ret)
		}
		if mig == nil {
			return 0, fmt.Errorf(
				"card %s: mig device handle %d is absent though the driver reported success", cardUUID, i)
		}
		giID, ret := mig.GetGpuInstanceId()
		if !ret.IsSuccess() {
			return 0, fmt.Errorf("card %s: get owning gpu-instance id of mig device %d: %w", cardUUID, i, ret)
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

func (d *nvmlMigDriver) device(cardUUID string) (nvml.Device, error) {
	if !d.initRet.IsSuccess() {
		return nvml.Device{}, fmt.Errorf("nvml init failed: %w", d.initRet)
	}
	dev, ret := d.lib.DeviceGetHandleByUUID(cardUUID)
	if !ret.IsSuccess() {
		return nvml.Device{}, fmt.Errorf("get nvml handle for %s: %w", cardUUID, ret)
	}
	return dev, nil
}

// profileID 根据名称与加速器所探测的 GPU 实例规格进行匹配，并返回其 NVML GPU 实例规格 ID。跳过 +me/+gfx
// 变体，因为 GPUStack 并未公开这些规格（即探测器使用的同一过滤规则）。它还能区分相同的计算 REV 规格（
// 1g.5gb 与 1g.10gb），而仅通过计算切片数量无法区分。
func (d *nvmlMigDriver) profileID(dev nvml.Device, profile string) (uint32, error) {
	for id := uint32(0); id < nvml.GPU_INSTANCE_PROFILE_COUNT; id++ {
		info, ret := dev.GetGpuInstanceProfileInfo(id)
		if !ret.IsSuccess() {
			continue
		}
		name := info.GetName()
		if strings.ContainsRune(name, '+') || info.Capabilities&nvml.GPU_INSTANCE_PROFILE_CAPS_GFX != 0 {
			continue
		}
		if name == profile {
			return info.Id, nil
		}
	}
	return 0, fmt.Errorf("card has no gpu-instance profile named %q", profile)
}

// driverReportsAbsent 用于报告：当没有成功返回结果时，是否表示 NVML 回答说所询问的实体不存在，而不是完全
// 未给出回应。以下所有内容都基于这一区别：被排除的实体属于状态信息，而不可读取的实体则是视图中的空白区域，
// 其完整性取决于每个调用者。
func driverReportsAbsent(ret nvml.Return) bool {
	switch ret {
	case nvml.ERROR_NOT_SUPPORTED, nvml.ERROR_NOT_FOUND, nvml.ERROR_INVALID_ARGUMENT:
		return true
	default:
		return false
	}
}

// migUUIDs 通过枚举加速器的 MIG 设备句柄并读取每个设备的对应 GPU 实例 ID，将设备上的每个活跃 GPU 实例 ID
// 映射到其 MIG 设备 UUID（即 NVIDIA_VISIBLE_DEVICES 值）。尚未有实体化的 MIG 设备的 GI 则完全不存在——即没
// 有计算实例的 GPU 实例，这无法解决问题，且会被回收机制破坏。
//
// 如果加速器驱动程序完全不承认 MIG 设备，则会产生一个空地图且没有错误：那就是普通的 GPU，其上方的枚举会遍
// 历节点上的所有加速器。其他所有故障都属于错误，因为无法读取所有者或身份的句柄会导致地图中缺少一个活跃分
// 区——而缺失的身份正是使活跃分区看起来可重新获取的原因，也使得销毁操作无法得到验证。
//
// 一个拥有多个 MIG 设备的 GPU 实例被拒绝使用，而不是被指定为其中某一个，这也就体现了“分区即整个 GPU 实例”
// 的约定：该运营商为每个分区创建单个计算实例来覆盖整个分区，因此可能是有人手动将其拆分，要么是容器部分负
// 责计算任务，而整个实例则被计入总计数中。这种拒绝行为会阻碍加速器的枚举操作，进而也会影响节点的状态。
func (d *nvmlMigDriver) migUUIDs(dev nvml.Device, cardUUID string) (map[uint32]string, error) {
	count, ret := dev.GetMaxMigDeviceCount()
	if !ret.IsSuccess() {
		if driverReportsAbsent(ret) {
			return map[uint32]string{}, nil
		}
		return nil, fmt.Errorf("card %s: get max mig device count: %w", cardUUID, ret)
	}
	out := make(map[uint32]string, count)
	for i := 0; i < count; i++ {
		mig, ret := dev.GetMigDeviceHandleByIndex(i)
		if !ret.IsSuccess() {
			if driverReportsAbsent(ret) {
				continue
			}
			return nil, fmt.Errorf("card %s: get mig device handle %d: %w", cardUUID, i, ret)
		}
		if mig == nil {
			return nil, fmt.Errorf(
				"card %s: mig device handle %d is absent though the driver reported success", cardUUID, i)
		}
		// 直接从 MIG 设备句柄中读取所属 GPU 实例 ID。要解析完整的实例信息，需要父设备，而在此处会
		// 失败（INVALID_ARGUMENT）。
		giID, ret := mig.GetGpuInstanceId()
		if !ret.IsSuccess() {
			return nil, fmt.Errorf("card %s: get owning gpu-instance id of mig device %d: %w", cardUUID, i, ret)
		}
		uuid, ret := mig.GetUUID()
		if !ret.IsSuccess() {
			return nil, fmt.Errorf("card %s: get identity of mig device %d: %w", cardUUID, i, ret)
		}
		if prev, dup := out[giID]; dup {
			return nil, fmt.Errorf(
				"card %s: gpu instance %d owns more than one mig device (%s and %s): refusing to address it by either",
				cardUUID, giID, prev, uuid)
		}
		out[giID] = uuid
	}
	return out, nil
}

// cardProfiles 会在加速器上遍历所有 GPU 实例的规格 ID，并返回驱动程序所响应的那些 ID。驱动程序不承认的 ID
// 会被跳过，因为其答案无效。无法读取的 ID 则会导致整个探测失败，因为该集合中缺少的规格会带走其实例，而这
// 里的每个调用者都会认为结果完整。
func cardProfiles(dev nvml.Device, cardUUID string) ([]nvml.GpuInstanceProfileInfo_v3, error) {
	var infos []nvml.GpuInstanceProfileInfo_v3
	for id := uint32(0); id < nvml.GPU_INSTANCE_PROFILE_COUNT; id++ {
		info, ret := dev.GetGpuInstanceProfileInfo(id)
		if !ret.IsSuccess() {
			if driverReportsAbsent(ret) {
				continue
			}
			return nil, fmt.Errorf("card %s: get gpu-instance profile info %d: %w", cardUUID, id, ret)
		}
		infos = append(infos, info)
	}
	return infos, nil
}

// liveInstances 列出了加速器上所有处于运行状态的 GPU 实例，这些实例属于不同的规格，每个实例都包含其计算份
// 额、部署位置以及 MIG 设备身份。占用率必须覆盖所有规格，因为某个规格下的实例会占用另一个规格下可能可用的
// 部署位置。
//
// 失败的实例查询属于错误情况，而非被跳过的规格。有两个机制负责处理此问题：分配路径会减去这些分配记录，以
// 选择空余的槽位。因此，一个未列出的规格会默默分配一个已被占用的槽位。同时，回收读取操作会将“已消失”的状
// 态从集合中移除，并删除所有权标记，因此一个处于活跃状态的分区读取操作会无所有者地泄漏，其分配记录会被第
// 二次分配。
func liveInstances(
	dev nvml.Device, cardUUID string, infos []nvml.GpuInstanceProfileInfo_v3, uuidByGI map[uint32]string,
) ([]migInstance, error) {
	var live []migInstance
	for i := range infos {
		info := infos[i]
		gis, ret := dev.GetGpuInstances(info.Id)
		if !ret.IsSuccess() {
			return nil, fmt.Errorf("card %s: list live gpu instances of profile %d: %w", cardUUID, info.Id, ret)
		}
		for j := range gis {
			gi := gis[j].GetInfo()
			if gi.ProfileId != info.Id {
				return nil, fmt.Errorf(
					"card %s: gpu instance %d reports profile %d while enumerated under profile %d: fail closed",
					cardUUID, gi.Id, gi.ProfileId, info.Id)
			}
			live = append(live, migInstance{
				GiID:          gi.Id,
				ComputeSlices: int32(info.SliceCount),
				Placement:     migPlacement{Start: int32(gi.Placement.Start), Length: int32(gi.Placement.Size)},
				UUID:          uuidByGI[gi.Id],
			})
		}
	}
	return live, nil
}

func (d *nvmlMigDriver) CardState(cardUUID, profile string, _, _ int32) (migCardState, error) {
	dev, err := d.device(cardUUID)
	if err != nil {
		return migCardState{}, err
	}

	targetID, err := d.profileID(dev, profile)
	if err != nil {
		return migCardState{}, fmt.Errorf("card %s: %w", cardUUID, err)
	}
	possibleSlots, ret := dev.GetGpuInstancePossiblePlacements(targetID)
	if !ret.IsSuccess() {
		return migCardState{}, fmt.Errorf("card %s: get possible placements: %w", cardUUID, ret)
	}
	possible := make([]migPlacement, 0, len(possibleSlots))
	for i := range possibleSlots {
		possible = append(possible, migPlacement{Start: int32(possibleSlots[i].Start), Length: int32(possibleSlots[i].Size)})
	}

	uuidByGI, err := d.migUUIDs(dev, cardUUID)
	if err != nil {
		return migCardState{}, err
	}

	// 收集所有规格下的每个活跃 GPU 实例，因此占用率会考虑任何规格的分区情况（例如，3g.20gb 会占用
	// 1g.10gb 的插槽）。
	infos, err := d.cardProfileCatalogue(dev, cardUUID)
	if err != nil {
		return migCardState{}, err
	}
	live, err := liveInstances(dev, cardUUID, infos, uuidByGI)
	if err != nil {
		return migCardState{}, err
	}

	return migCardState{Possible: possible, Live: live}, nil
}

func (d *nvmlMigDriver) CreateInstance(
	cardUUID, profile string, computeSlices, _ int32, slot migPlacement,
) (migInstance, error) {
	dev, err := d.device(cardUUID)
	if err != nil {
		return migInstance{}, err
	}

	targetID, err := d.profileID(dev, profile)
	if err != nil {
		return migInstance{}, fmt.Errorf("card %s: %w", cardUUID, err)
	}
	ciIDs, ok := nvml.MigProfileIDsForComputeSlices(computeSlices)
	if !ok {
		return migInstance{}, fmt.Errorf("card %s: no compute-instance profile for %d compute slices", cardUUID, computeSlices)
	}

	giInfo := &nvml.GpuInstanceProfileInfo{Id: targetID}
	placement := &nvml.GpuInstancePlacement{Start: uint32(slot.Start), Size: uint32(slot.Length)}
	gi, ret := dev.CreateGpuInstanceWithPlacement(giInfo, placement)
	if !ret.IsSuccess() {
		return migInstance{}, fmt.Errorf("card %s: create gpu instance: %w", cardUUID, ret)
	}
	giID := gi.GetInfo().Id

	ciInfo, ret := gi.GetComputeInstanceProfileInfo(ciIDs.ComputeInstanceProfileID, ciIDs.ComputeInstanceEngineProfileID)
	if !ret.IsSuccess() {
		_ = gi.Destroy()
		return migInstance{}, fmt.Errorf("card %s: get compute-instance profile info: %w", cardUUID, ret)
	}
	ci, ret := gi.CreateComputeInstance(&ciInfo)
	if !ret.IsSuccess() {
		// 镜像 mig-parted 的清理操作：没有 CI 的 GI 无法使用，因此需将其销毁。
		_ = gi.Destroy()
		return migInstance{}, fmt.Errorf("card %s: create compute instance: %w", cardUUID, ret)
	}

	// 解决刚刚创建的 GI 的 MIG 设备 UUID（NVIDIA_VISIBLE_DEVICES）。如果读取失败，将像其他故障一样被
	// 回退：没有容器所指向的标识，该对设备将无法使用，将其废弃会导致部署中断。
	uuidByGI, err := d.migUUIDs(dev, cardUUID)
	if err != nil {
		_ = ci.Destroy()
		_ = gi.Destroy()
		return migInstance{}, err
	}
	uuid := uuidByGI[giID]
	if uuid == "" {
		_ = ci.Destroy()
		_ = gi.Destroy()
		return migInstance{}, fmt.Errorf("card %s: created gpu instance %d has no mig-device uuid", cardUUID, giID)
	}

	return migInstance{
		GiID:          giID,
		CiID:          ci.GetInfo().Id,
		ComputeSlices: computeSlices,
		Placement:     slot,
		UUID:          uuid,
	}, nil
}

// ListInstances 会枚举所有具备 MIG 能力的加速器上运行的 GPU 实例，获取每个实例的 MIG 设备 UUID，从而让回
// 收的孤儿 GC 能够在被释放的加速器上找到无标记的 GI。它复制了 CardState 的 Live 循环逻辑，但适用于所有加
// 速器，且无需按配置文件进行可能的放置（孤儿对象的销毁只需使用 ID 和计算切片，无需填充 slot）。
//
// 驱动程序响应的加速器没有 MIG 设备，也不持有分区，无法提供任何贡献。而无法读取其句柄、身份、配置、MIG 设
// 备或实例的加速器则会导致整个枚举失败。这种差异很重要，因为调用者面对缺失的情况会采取不同的处理措施：对
// 于缺失的 GPU 实例，回收操作会将其视为已失效实例并移除其所有权标记。而对于无法提供任何贡献的加速器，则会
// 被视为被释放的加速器，其孤儿状态可能会被收集起来。因此，缺少某个加速器分区的列表会破坏或重复记录那些本
// 应被看到的资源。
func (d *nvmlMigDriver) ListInstances() ([]migLiveInstance, error) {
	if !d.initRet.IsSuccess() {
		return nil, fmt.Errorf("nvml init failed: %w", d.initRet)
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

// DestroyInstance 会销毁调用者捕获的 MIG 实例，该实例受调用者持有的加速器锁保护。它会重新读取该关键段中加
// 速器的实时集合，确认 GPU 实例 ID 仍保留已记录的身份，然后再销毁任何内容：被销毁的实例 ID 可能被 NVML 重
// 新分配，因此经过一次分配后已过期的快照可能指向另一个——可能仍存活的——实例。如果信息不匹配，则不会销毁任
// 何内容，并将矛盾情况返回。
//
// 在完整的枚举中缺失的 id 表示该实例已消失，这属于正常情况：回收循环对所有权标记的移除依赖于其幂等性。正
// 因如此，不完整的枚举可能无法返回该结果——曾经出现过的失败实例查询会陷入这种状态，报告一个从未发生的销毁
// 事件，之后该标记被移除作为回收操作，而实际实例则无所有者地泄露。
func (d *nvmlMigDriver) DestroyInstance(cardUUID string, inst migInstance) error {
	dev, err := d.device(cardUUID)
	if err != nil {
		return err
	}
	ciIDs, ok := nvml.MigProfileIDsForComputeSlices(inst.ComputeSlices)
	if !ok {
		return fmt.Errorf("card %s: no compute-instance profile for %d compute slices", cardUUID, inst.ComputeSlices)
	}
	infos, err := d.cardProfileCatalogue(dev, cardUUID)
	if err != nil {
		return err
	}
	uuidByGI, err := d.migUUIDs(dev, cardUUID)
	if err != nil {
		return err
	}

	// 通过 ID 在多个配置文件中找到实时 GPU 实例，然后在该实例被销毁之前销毁其计算实例（即 F1 反向操作
	// 序列）。
	for i := range infos {
		gis, ret := dev.GetGpuInstances(infos[i].Id)
		if !ret.IsSuccess() {
			return fmt.Errorf("card %s: list live gpu instances of profile %d: %w", cardUUID, infos[i].Id, ret)
		}
		for j := range gis {
			giInfo := gis[j].GetInfo()
			if giInfo.Id != inst.GiID {
				continue
			}
			if verr := verifyInstanceIdentity(cardUUID, inst, giInfo, uuidByGI[giInfo.Id]); verr != nil {
				return verr
			}
			cis, ret := gis[j].GetComputeInstances(ciIDs.ComputeInstanceProfileID)
			if !ret.IsSuccess() && !driverReportsAbsent(ret) {
				return fmt.Errorf(
					"card %s: list compute instances on gpu instance %d: %w", cardUUID, inst.GiID, ret)
			}
			for k := range cis {
				if r := cis[k].Destroy(); !r.IsSuccess() {
					if r == nvml.ERROR_IN_USE {
						return fmt.Errorf("card %s: destroy compute instance: %w", cardUUID, errInstanceInUse)
					}
					return fmt.Errorf("card %s: destroy compute instance: %w", cardUUID, r)
				}
			}
			if r := gis[j].Destroy(); !r.IsSuccess() {
				if r == nvml.ERROR_IN_USE {
					return fmt.Errorf("card %s: destroy gpu instance %d: %w", cardUUID, inst.GiID, errInstanceInUse)
				}
				return fmt.Errorf("card %s: destroy gpu instance %d: %w", cardUUID, inst.GiID, r)
			}
			return nil
		}
	}
	return nil
}

// verifyInstanceIdentity 会通过身份字符串和位置信息，确认记录下来的实际 ID 对应的实时 GPU 实例是否仍为记
// 录时的那个实例。该位置检测作为一致性校验的辅助手段存在：对于自洽的驱动程序来说，这一检测是多余的。而当
// 一个实例与其中一个标准不符时，这种矛盾状态正是“销毁”操作必须拒绝而非解决的情况。
func verifyInstanceIdentity(
	cardUUID string, inst migInstance, live nvml.GpuInstanceInfo, liveUUID string,
) error {
	if liveUUID != inst.UUID {
		return fmt.Errorf(
			"card %s: gpu instance %d now carries mig device %q, not the recorded %q: refusing to destroy",
			cardUUID, inst.GiID, liveUUID, inst.UUID)
	}
	if int32(live.Placement.Start) != inst.Placement.Start || int32(live.Placement.Size) != inst.Placement.Length {
		return fmt.Errorf(
			"card %s: gpu instance %d now occupies slices [%d,%d), not the recorded [%d,%d): refusing to destroy",
			cardUUID, inst.GiID,
			live.Placement.Start, live.Placement.Start+live.Placement.Size,
			inst.Placement.Start, inst.Placement.Start+inst.Placement.Length)
	}
	return nil
}
