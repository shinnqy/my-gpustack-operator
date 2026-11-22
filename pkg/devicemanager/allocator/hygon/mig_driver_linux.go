//go:build linux

package hygon

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/binding/dmi"
	"gpustack.ai/gpustack/pkg/nodefeature"
)

// migSliceWidths 是规格查询所覆盖的实例宽度范围。该库中的规格查询采用宽度参数而非规格 ID，如果某卡片没有
// 规格信息且宽度符合要求，则会返回缺失的代码——所有被测量的卡片都存在这样的孔洞，位于三个切片位置。
var migSliceWidths = []uint32{1, 2, 3, 4}

// newMigDriver 构建了真正的驱动程序。它仅适用于 Linux 环境，因为 Darwin 测试二进制文件无法支持该分配器的
// 厂商绑定功能。在 Darwin 构建过程中，会使用 mig_driver_other.go 中的 stub 进行编译。
func newMigDriver() migDriver {
	l := dmi.New()
	return &dmiMigDriver{lib: l, initRet: l.Init(klog.Background())}
}

// dmiMigDriver 负责在分区生命周期内实现厂商的多实例绑定功能。
//
// 每个枚举值要么报告其能够证明为完整的状态，要么报告错误。在查询失败时，它绝不会降级为部分状态：一个被标
// 记为“不存在”的活跃分区会将其所有权记录标记为“已消失”，其位置会被重新分配第二次，而没有记录的分区则会被
// 漏过孤儿收集器。
type dmiMigDriver struct {
	lib *dmi.DMI
	// initRet 记录了库的初始化结果，因此当无法加载库时，每个操作都能指出一个可解决的根本问题，而不是
	// 在某个无法运行的调用中导致晦涩的失败。
	initRet dmi.Return
}

// 设备从其 PCI 地址解析出加速器的一个 `Multi-Instance 句柄。
//
// 该地址是唯一的桥梁：这个库不进行 UUID 查询，也会用空字符串回应自己的 PCI 查询，因此持有其他来源身份的调
// 用者可能在这里能访问该卡片，也可能根本无法访问。
func (d *dmiMigDriver) device(pciBusID string) (dmi.Device, error) {
	if !d.initRet.IsSuccess() {
		return dmi.Device{}, fmt.Errorf(
			"hygon multi-instance library unavailable (%v); the node cannot serve partitions", d.initRet)
	}
	dev, ret := d.lib.GetDeviceHandleByPciBusId(pciBusID)
	if !ret.IsSuccess() {
		return dmi.Device{}, fmt.Errorf("resolve card %s: %w", pciBusID, ret)
	}
	return dev, nil
}

// migProfile 是卡片的 GPU 实例规格之一，它对应着生命周期所需的两项要素：用于标识的厂商 ID，以及选择其中完
// 整 GI 计算规格的宽度。
type migProfile struct {
	id     uint32
	slices uint32
}

// resolveMigProfile 会找到所请求名称对应的规格。
//
// 这种扫描是基于宽度进行的，因为该库查询需要一个宽度参数，每个答案都拥有自己的 ID——两者无关，因此只能通过
// 读取卡片提供的所有宽度来匹配名称。卡片声明不提供的宽度将被跳过，作为答案处理。无法回答的宽度则属于错误，
// 因为缺少该扫描中规格的 profile 会导致请求被拒绝。
func resolveMigProfile(dev dmi.Device, profile string) (migProfile, error) {
	want := nodefeature.NormalizePartitionedProfileName(profile)
	if want == "" {
		return migProfile{}, fmt.Errorf("empty partition profile requested")
	}
	for _, width := range migSliceWidths {
		info, ret := dev.GetGpuInstanceProfileInfoBySliceCount(width)
		if !ret.IsSuccess() {
			if ret.ReportsAbsent() {
				continue
			}
			return migProfile{}, fmt.Errorf("read %d-slice profile: %w", width, ret)
		}
		if nodefeature.NormalizePartitionedProfileName(migProfileName(info)) != want {
			continue
		}
		return migProfile{id: info.Id, slices: info.Gpu_slice_count}, nil
	}
	return migProfile{}, fmt.Errorf("card offers no partition profile named %q", profile)
}

// cardProfiles 返回了该卡所提供的所有 GPU 实例规格，因此实时实例扫描可以针对每一种规格进行查询。部分目录
// 存在错误：此处遗漏的规格实例，其实属于会被重新分配的实例。
func cardProfiles(dev dmi.Device) ([]migProfile, error) {
	var profiles []migProfile
	for _, width := range migSliceWidths {
		info, ret := dev.GetGpuInstanceProfileInfoBySliceCount(width)
		if !ret.IsSuccess() {
			if ret.ReportsAbsent() {
				continue
			}
			return nil, fmt.Errorf("read %d-slice profile: %w", width, ret)
		}
		profiles = append(profiles, migProfile{id: info.Id, slices: info.Gpu_slice_count})
	}
	if len(profiles) == 0 {
		return nil, fmt.Errorf("card offers no partition profile at all")
	}
	return profiles, nil
}

// CardState 实现了 migDriver。
func (d *dmiMigDriver) CardState(pciBusID, profile string) (migCardState, error) {
	dev, err := d.device(pciBusID)
	if err != nil {
		return migCardState{}, err
	}

	want, err := resolveMigProfile(dev, profile)
	if err != nil {
		return migCardState{}, fmt.Errorf("card %s: %w", pciBusID, err)
	}

	slots, ret := dev.GetGpuInstancePossiblePlacements(want.id)
	if !ret.IsSuccess() {
		return migCardState{}, fmt.Errorf("card %s: read placements of profile %d: %w", pciBusID, want.id, ret)
	}
	possible := make([]migPlacement, 0, len(slots))
	for _, s := range slots {
		possible = append(possible, migPlacement{Start: int32(s.Start), Length: int32(s.Size)})
	}

	live, err := d.liveInstances(dev, pciBusID)
	if err != nil {
		return migCardState{}, err
	}

	return migCardState{ProfileID: want.id, Possible: possible, Live: live}, nil
}

// liveInstances 列出了该卡上所有 GPU 实例及其所有配置。
//
// 每个规格都会被询问，因为查询是通过规格 ID 进行筛选的：仅询问一个规格并将答案理解为“卡片的实例”会遗漏其
// 他宽度的所有实例，而占用此空间的布局选择则会选择一个已被占用的运行实例。
func (d *dmiMigDriver) liveInstances(dev dmi.Device, pciBusID string) ([]migInstance, error) {
	index, ret := dev.GetIndex()
	if !ret.IsSuccess() {
		return nil, fmt.Errorf("card %s: read device index: %w", pciBusID, ret)
	}

	profiles, err := cardProfiles(dev)
	if err != nil {
		return nil, fmt.Errorf("card %s: %w", pciBusID, err)
	}

	var live []migInstance
	for _, p := range profiles {
		gis, ret := dev.GetGpuInstances(p.id)
		if !ret.IsSuccess() {
			return nil, fmt.Errorf("card %s: list instances of profile %d: %w", pciBusID, p.id, ret)
		}
		for _, gi := range gis {
			info, ret := gi.GetInfo()
			if !ret.IsSuccess() {
				return nil, fmt.Errorf("card %s: read a gpu instance of profile %d: %w", pciBusID, p.id, ret)
			}

			// 尚未有计算实例的 GPU 实例属于半完成的分区：分配器总是会同时创建两者，因此当前这
			// 个实例在创建过程中被中断。它仍然占据其位置，因此被报告——没有身份标识，这使其无
			// 法被采用，也允许孤儿收集器对其进行清理。
			ciID, uuid, confPath, found, err := d.computeInstanceOf(gi, p, index, info.Id)
			if err != nil {
				return nil, fmt.Errorf("card %s: gpu instance %d: %w", pciBusID, info.Id, err)
			}
			inst := migInstance{
				GiID:      info.Id,
				ProfileID: info.Profile_id,
				Placement: migPlacement{Start: int32(info.Placement.Start), Length: int32(info.Placement.Size)},
			}
			if found {
				inst.CiID, inst.UUID, inst.ConfPath = ciID, uuid, confPath
			}
			live = append(live, inst)
		}
	}
	return live, nil
}

// computeInstanceOf 会在 GPU 实例中返回整个 GI 计算实例，其身份与其注册文件所携带的信息一致。
//
// 只要求完整的 GI 规格：这是该分配器所创建的唯一形状，因为这里的容器只能使用一个实例，而更窄的计算实例所
// 提供的 GPU 实例数量会低于其规格所承诺的数量。
func (d *dmiMigDriver) computeInstanceOf(
	gi dmi.GpuInstance, p migProfile, deviceIndex, giID uint32,
) (ciID uint32, uuid, confPath string, found bool, err error) {
	info, ret := gi.GetComputeInstanceProfileInfoBySliceCount(p.slices, 0)
	if !ret.IsSuccess() {
		if ret.ReportsAbsent() {
			return 0, "", "", false, nil
		}
		return 0, "", "", false, fmt.Errorf("read the whole-instance compute profile: %w", ret)
	}

	cis, ret := gi.GetComputeInstances(info.Id)
	if !ret.IsSuccess() {
		return 0, "", "", false, fmt.Errorf("list compute instances of profile %d: %w", info.Id, ret)
	}
	if len(cis) == 0 {
		return 0, "", "", false, nil
	}

	ciInfo, ret := cis[0].GetInfo()
	if !ret.IsSuccess() {
		return 0, "", "", false, fmt.Errorf("read a compute instance: %w", ret)
	}

	path := migConfPath(deviceIndex, giID, ciInfo.Id)
	id, err := readMigConfUUID(path)
	if err != nil {
		return 0, "", "", false, fmt.Errorf("read the instance registry file %q: %w", path, err)
	}
	if id == "" {
		return 0, "", "", false, fmt.Errorf("the instance registry file %q carries no identity", path)
	}
	return ciInfo.Id, id, path, true, nil
}

// CreateInstance 实现了 migDriver。
//
// 这两部分是共同创建的，因为没有计算实例的 GPU 实例无法作为可用的分区——厂商运行时会从计算实例的文件中激活
// 一个容器，而如果没有这些文件的话，就根本不会有任何容器。因此，在 GPU 实例出现故障后，会直接将其销毁，而
// 不会留下一个半完成的分区占据某个位置。
func (d *dmiMigDriver) CreateInstance(pciBusID, profile string, slot migPlacement) (migInstance, error) {
	dev, err := d.device(pciBusID)
	if err != nil {
		return migInstance{}, err
	}

	want, err := resolveMigProfile(dev, profile)
	if err != nil {
		return migInstance{}, fmt.Errorf("card %s: %w", pciBusID, err)
	}

	index, ret := dev.GetIndex()
	if !ret.IsSuccess() {
		return migInstance{}, fmt.Errorf("card %s: read device index: %w", pciBusID, ret)
	}

	gi, ret := dev.CreateGpuInstanceWithPlacement(want.id, dmi.GpuInstancePlacement{
		Start: uint32(slot.Start),
		Size:  uint32(slot.Length),
	})
	if !ret.IsSuccess() {
		return migInstance{}, fmt.Errorf("card %s: create gpu instance of profile %d at %d:%d: %w",
			pciBusID, want.id, slot.Start, slot.Length, ret)
	}

	inst, err := d.completeInstance(gi, want, index)
	if err != nil {
		if r := gi.Destroy(); !r.IsSuccess() {
			return migInstance{}, fmt.Errorf(
				"card %s: %w (and the half-built gpu instance could not be removed: %v)", pciBusID, err, r)
		}
		return migInstance{}, fmt.Errorf("card %s: %w", pciBusID, err)
	}
	return inst, nil
}

// completeInstance 会创建整个-GI 计算实例，并读取厂商为其分配的身份信息。
//
// IT 在退出时不会销毁自己的计算实例。销毁顺序是强制的——仍保留的 GPU 实例无法被移除——因此这里留下的计算实
// 例不仅会泄漏自身资源，还会导致调用者的回滚操作失败，进而使 GPU 实例陷入困境。此时没有任何东西可以分配，
// 因为预订机制所记录的归属记录从未被更新，且该资源会一直占用，直到节点被重建为止。
func (d *dmiMigDriver) completeInstance(
	gi dmi.GpuInstance, p migProfile, deviceIndex uint32,
) (inst migInstance, err error) {
	giInfo, ret := gi.GetInfo()
	if !ret.IsSuccess() {
		return migInstance{}, fmt.Errorf("read the created gpu instance: %w", ret)
	}

	ciProfile, ret := gi.GetComputeInstanceProfileInfoBySliceCount(p.slices, 0)
	if !ret.IsSuccess() {
		return migInstance{}, fmt.Errorf("read the whole-instance compute profile: %w", ret)
	}

	ci, ret := gi.CreateComputeInstance(ciProfile.Id)
	if !ret.IsSuccess() {
		return migInstance{}, fmt.Errorf("create the compute instance of profile %d: %w", ciProfile.Id, ret)
	}
	defer func() {
		if err == nil {
			return
		}
		if r := ci.Destroy(); !r.IsSuccess() {
			err = fmt.Errorf(
				"%w (and the compute instance could not be removed, so the gpu instance holding it"+
					" cannot be either: %v)", err, r)
		}
	}()

	ciInfo, ret := ci.GetInfo()
	if !ret.IsSuccess() {
		return migInstance{}, fmt.Errorf("read the created compute instance: %w", ret)
	}

	path := migConfPath(deviceIndex, giInfo.Id, ciInfo.Id)
	uuid, cerr := readMigConfUUID(path)
	if cerr != nil {
		return migInstance{}, fmt.Errorf("read the instance registry file %q: %w", path, cerr)
	}
	if uuid == "" {
		return migInstance{}, fmt.Errorf("the instance registry file %q carries no identity", path)
	}

	return migInstance{
		GiID:      giInfo.Id,
		CiID:      ciInfo.Id,
		ProfileID: giInfo.Profile_id,
		Placement: migPlacement{Start: int32(giInfo.Placement.Start), Length: int32(giInfo.Placement.Size)},
		UUID:      uuid,
		ConfPath:  path,
	}, nil
}

// DestroyInstance 实现了 migDriver 接口。
//
// 这一要求由厂商设定：仍保留计算实例的 GPU 实例无法被移除。首先会在调用者的加速器锁定下重新验证身份，因此
// 从快照开始，以相同的 ID 重新创建的分区永远不会被删除——由于该厂商每次创建时都会生成新的身份，因此该检查
// 已完成。
func (d *dmiMigDriver) DestroyInstance(pciBusID string, inst migInstance) error {
	dev, err := d.device(pciBusID)
	if err != nil {
		return err
	}

	live, err := d.liveInstances(dev, pciBusID)
	if err != nil {
		return err
	}

	var found *migInstance
	for i := range live {
		if live[i].GiID == inst.GiID {
			found = &live[i]
			break
		}
	}
	if found == nil {
		// 已经消失了。报告成功状态是使重复销毁操作成为幂等的关键，而回收循环正是依赖于这一点。
		return nil
	}
	if inst.UUID != "" && found.UUID != "" && found.UUID != inst.UUID {
		return fmt.Errorf(
			"card %s: gpu instance %d now carries identity %q, not the recorded %q (id reused): refusing to destroy it",
			pciBusID, inst.GiID, found.UUID, inst.UUID)
	}

	profiles, err := cardProfiles(dev)
	if err != nil {
		return fmt.Errorf("card %s: %w", pciBusID, err)
	}

	for _, p := range profiles {
		gis, ret := dev.GetGpuInstances(p.id)
		if !ret.IsSuccess() {
			return fmt.Errorf("card %s: list instances of profile %d: %w", pciBusID, p.id, ret)
		}
		for _, gi := range gis {
			info, ret := gi.GetInfo()
			if !ret.IsSuccess() || info.Id != inst.GiID {
				continue
			}
			return d.destroyGpuInstance(pciBusID, gi, p)
		}
	}
	return nil
}

// destroyGpuInstance 会移除 GPU 实例的计算实例，随后也会删除该实例本身。
func (d *dmiMigDriver) destroyGpuInstance(pciBusID string, gi dmi.GpuInstance, p migProfile) error {
	ciProfile, ret := gi.GetComputeInstanceProfileInfoBySliceCount(p.slices, 0)
	if ret.IsSuccess() {
		cis, ret := gi.GetComputeInstances(ciProfile.Id)
		if !ret.IsSuccess() {
			return fmt.Errorf("card %s: list compute instances: %w", pciBusID, ret)
		}
		for _, ci := range cis {
			if ret := ci.Destroy(); !ret.IsSuccess() {
				return migDestroyError(pciBusID, "compute instance", ret)
			}
		}
	} else if !ret.ReportsAbsent() {
		return fmt.Errorf("card %s: read the whole-instance compute profile: %w", pciBusID, ret)
	}

	if ret := gi.Destroy(); !ret.IsSuccess() {
		return migDestroyError(pciBusID, "gpu instance", ret)
	}
	return nil
}

// migDestroyError 包装了被拒绝的销毁操作，这是调用者不得重试的情况。
//
// 因为某些原因拒绝操作的驱动程序，是回答“是否有人在使用它”问题的权威答案——这个库根本不处理按分区进行的查
// 询，因此没有其他需要询问的内容。
func migDestroyError(pciBusID, what string, ret dmi.Return) error {
	if ret == dmi.ERROR_IN_USE {
		return fmt.Errorf("card %s: destroy %s: %w: %v", pciBusID, what, errInstanceInUse, ret)
	}
	return fmt.Errorf("card %s: destroy %s: %w", pciBusID, what, ret)
}

// ListInstances 实现了 migDriver 接口。
func (d *dmiMigDriver) ListInstances() ([]migLiveInstance, error) {
	if !d.initRet.IsSuccess() {
		return nil, fmt.Errorf(
			"hygon multi-instance library unavailable (%v); the node cannot serve partitions", d.initRet)
	}

	count, ret := d.lib.GetDeviceCount()
	if !ret.IsSuccess() {
		return nil, fmt.Errorf("count cards: %w", ret)
	}

	var out []migLiveInstance
	for i := uint32(0); i < count; i++ {
		pciBusID, err := migCardPciBusID(i)
		if err != nil {
			return nil, err
		}
		dev, ret := d.lib.GetDeviceHandleByIndex(i)
		if !ret.IsSuccess() {
			return nil, fmt.Errorf("card %d: resolve handle: %w", i, ret)
		}
		live, err := d.liveInstances(dev, pciBusID)
		if err != nil {
			return nil, err
		}
		for _, inst := range live {
			out = append(out, migLiveInstance{PciBusID: pciBusID, Instance: inst})
		}
	}
	return out, nil
}

// migCardPciBusID 读取厂商记录中对应设备索引的 PCI 地址。
//
// 库自身的 PCI 查询返回成功，并写入空字符串，因此这一行文件就是映射内容。它仅在节点处于多实例模式时存在，
// 这也是该驱动程序能够运行的唯一情况。
func migCardPciBusID(index uint32) (string, error) {
	path := filepath.Join(migConfigDir, fmt.Sprintf("dev%d", index))
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read the pci address of card %d from %q: %w", index, path, err)
	}
	addr := strings.TrimSpace(string(data))
	if addr == "" {
		return "", fmt.Errorf("the pci address file %q of card %d is empty", path, index)
	}
	return addr, nil
}

// migConfPath 指的是计算实例的注册文件，该文件既是该实例的标识所在，也是容器用于使用它的工件。
func migConfPath(deviceIndex, giID, ciID uint32) string {
	return filepath.Join(migConfigDir, "ci", fmt.Sprintf("dev%dgi%dci%d.conf", deviceIndex, giID, ciID))
}

// readMigConfUUID 返回计算实例注册文件所携带的标识。
//
// 该文件是其 GPU 实例字段与自身字段的拼接结果，因此某些键会重复出现。该身份仅出现一次，且仅出现在计算实例
// 部分，因此通过键进行读取。
func readMigConfUUID(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()

	s := bufio.NewScanner(f)
	for s.Scan() {
		key, value, ok := strings.Cut(s.Text(), ":")
		if !ok || strings.TrimSpace(key) != "mig_uuid" {
			continue
		}
		return strings.TrimSpace(value), nil
	}
	return "", s.Err()
}

// migProfileName 用于显示规格的固定宽度名称字段。生成的结构体类型将其定义为 int8 而非 byte，而厂商则通过
// NULs 对其进行填充。
func migProfileName(info dmi.GpuInstanceProfileInfo) string {
	out := make([]byte, 0, len(info.Name))
	for _, c := range info.Name {
		if c == 0 {
			break
		}
		out = append(out, byte(c))
	}
	return string(out)
}
