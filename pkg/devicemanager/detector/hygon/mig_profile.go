package hygon

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/binding/dmi"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/nodefeature"
)

// migSliceWidths 列出厂商允许的实例切片宽度。DMI 以宽度枚举索引查询，真正的 profile ID 由驱动返回。
var migSliceWidths = []uint32{1, 2, 3, 4}

// migModeEnabled 同时返回节点是否处于多实例模式，以及本轮能否确定该模式。
// 查询失败要与驱动明确报告关闭分开处理。
func (in *hygon) migModeEnabled() (enabled, known bool) {
	current, _, ret := in.dmi.GetSystemMigMode()
	switch {
	case ret.IsSuccess():
		return current == dmi.DEVICE_MIG_ENABLE, true
	case ret.IsAPIUnavailable():
		in.logger.V(3).Info("this node offers no multi-instance library, so it serves no partitions",
			"return", ret, "library", in.dmi.Path())
		return false, true
	default:
		in.logger.Error(ret, "could not read the node's multi-instance mode, so neither capability is published",
			"library", in.dmi.Path())
		return false, false
	}
}

// migCardCapability 汇总一张物理卡的硬件分区能力和整卡计算单元数，这两项都来自卡的分区规格。
type migCardCapability struct {
	sliced device.AcceleratorPhysicalSliced
	cores  uint32
}

// detectCardMigProfiles 按完整 PCI 地址将 RSMI 枚举的卡映射到 DMI 物理设备句柄，
// 读取该卡支持的分区及整卡算力。
func (in *hygon) detectCardMigProfiles(pciBusID string, logger klog.Logger) migCardCapability {
	dev, ret := in.dmi.GetDeviceHandleByPciBusId(pciBusID)
	if !ret.IsSuccess() {
		logger.Error(ret, "failed to reach a card through the multi-instance library, so it publishes no"+
			" partition profiles even though the node is in multi-instance mode", "pci", pciBusID)
		return migCardCapability{}
	}

	infos, err := probeMigProfiles(dev.GetGpuInstanceProfileInfoBySliceCount)
	if err != nil {
		logger.Error(err,
			"left partition profiles out of a card's inventory because the driver could not answer for "+
				"them; a profile absent here is absent from the node's capacity, its flavor and its "+
				"InstanceType, so a partition of it can stop being nameable and a new request for it is refused")
	}

	return migCardCapability{
		sliced: physicalSliced(migProfilesOf(infos, dev, logger)),
		cores:  migCardCores(infos),
	}
}

// migCardCores 通过分区规格还原整卡计算单元。多实例模式下 HSA 只看见单个实例，用其计数会低估整卡。
func migCardCores(infos []dmi.GpuInstanceProfileInfo) uint32 {
	var cores uint32
	for _, info := range infos {
		if total := info.Cu_count * info.Gi_count_max; total > cores {
			cores = total
		}
	}
	return cores
}

// migProfilesOf 查询各规格允许的落位，将能核实的规格转换成设备清单。
func migProfilesOf(
	infos []dmi.GpuInstanceProfileInfo, dev dmi.Device, logger klog.Logger,
) []device.AcceleratorPhysicalSlicedProfile {
	// 在探测阶段缓存空卡上的合法落位。后续可从这些区间中减去 Pod 注解里已占用的位置，得到剩余能力。
	answered, placementsByID, err := migPlacementsByProfile(infos, dev.GetGpuInstancePossiblePlacements)
	if err != nil {
		logger.Error(err, "dropped partition profiles whose possible placements are unreadable")
	}

	profiles, rejected := deriveSlicedProfiles(answered,
		func(profileID uint32) []device.AcceleratorPlacement {
			return placementsByID[profileID]
		})
	for _, reason := range rejected {
		logger.Info("dropped unpublishable partition profile", "reason", reason)
	}
	return profiles
}

// probeMigProfiles 遍历各切片宽度，分别处理已读到规格、驱动明确不支持和查询失败，跳过合法空位并保留故障原因。
func probeMigProfiles(
	probe func(sliceCount uint32) (dmi.GpuInstanceProfileInfo, dmi.Return),
) ([]dmi.GpuInstanceProfileInfo, error) {
	var (
		infos      []dmi.GpuInstanceProfileInfo
		unreadable []error
	)
	for _, width := range migSliceWidths {
		info, ret := probe(width)
		if !ret.IsSuccess() {
			if !ret.ReportsAbsent() {
				unreadable = append(unreadable, fmt.Errorf("%d-slice profile: %w", width, ret))
			}
			continue
		}
		infos = append(infos, info)
	}
	return infos, errors.Join(unreadable...)
}

// migPlacementsByProfile 按 profile ID 收集各规格在空卡上的合法落位，只接纳驱动实际回答的查询。
func migPlacementsByProfile(
	infos []dmi.GpuInstanceProfileInfo,
	query func(profileID uint32) ([]dmi.GpuInstancePlacement, dmi.Return),
) ([]dmi.GpuInstanceProfileInfo, map[uint32][]device.AcceleratorPlacement, error) {
	answered := make([]dmi.GpuInstanceProfileInfo, 0, len(infos))
	byID := make(map[uint32][]device.AcceleratorPlacement, len(infos))
	var errs []error
	for _, info := range infos {
		slots, ret := query(info.Id)
		if !ret.IsSuccess() {
			errs = append(errs, fmt.Errorf("profile %d: %w", info.Id, ret))
			continue
		}
		answered = append(answered, info)
		byID[info.Id] = migPlacementsFromDMI(slots)
	}
	return answered, byID, errors.Join(errs...)
}

// migPlacementsFromDMI 将 DMI 落位转换为 Operator 的起点与长度。空输入返回 nil，省略相应字段。
func migPlacementsFromDMI(slots []dmi.GpuInstancePlacement) []device.AcceleratorPlacement {
	if len(slots) == 0 {
		return nil
	}
	out := make([]device.AcceleratorPlacement, len(slots))
	for i := range slots {
		out[i] = device.AcceleratorPlacement{
			Start:  int32(slots[i].Start),
			Length: int32(slots[i].Size),
		}
	}
	return out
}

// deriveSlicedProfiles 按厂商规格名提取可用的几何描述，核对每个规格的切片范围和合法落位，再生成物理切分清单。
func deriveSlicedProfiles(
	infos []dmi.GpuInstanceProfileInfo,
	placementsFor func(profileID uint32) []device.AcceleratorPlacement,
) (profiles []device.AcceleratorPhysicalSlicedProfile, rejected []string) {
	// 先按名称收集，全部检查完再发布，使后出现的冲突也能撤下该名称的先前记录。
	type candidate struct {
		raw     string
		id      uint32
		profile device.AcceleratorPhysicalSlicedProfile
	}
	var (
		order  []string
		byName = make(map[string][]candidate, len(infos))
	)

	for _, info := range infos {
		raw := migProfileName(info)

		name := nodefeature.NormalizePartitionedProfileName(raw)
		if nodefeature.GetAcceleratablePartitionedProfileResourceName(Manufacturer, name) == "" {
			rejected = append(rejected, fmt.Sprintf(
				"profile %d named %q yields no valid resource name, so it cannot be requested", info.Id, raw,
			))
			continue
		}

		placements := placementsFor(info.Id)
		if len(placements) == 0 || placements[0].Length <= 0 {
			rejected = append(rejected, fmt.Sprintf(
				"profile %d named %q has no legal placement, so its memory-slice span is unknown", info.Id, raw,
			))
			continue
		}

		if _, seen := byName[name]; !seen {
			order = append(order, name)
		}
		byName[name] = append(byName[name], candidate{
			raw: raw,
			id:  info.Id,
			profile: device.AcceleratorPhysicalSlicedProfile{
				Name: name,
				// 厂商字段标为 MB，实测单位为 MiB。四片规格的 65520 与这张卡按 MiB 读取的容量一致。
				MemoryMib:     int64(info.Memory_size_MB),
				ComputeSlices: int32(info.Gpu_slice_count),
				MemorySlices:  placements[0].Length,
				Count:         int32(info.Gi_count_max),
				Placements:    placements,
			},
		})
	}

	for _, name := range order {
		group := byName[name]
		divergent := slices.ContainsFunc(group[1:], func(c candidate) bool {
			return !profileEqual(group[0].profile, c.profile)
		})
		if divergent {
			raws := make([]string, len(group))
			for i, c := range group {
				raws[i] = fmt.Sprintf("%d (%q)", c.id, c.raw)
			}
			rejected = append(rejected, fmt.Sprintf(
				"profiles %s normalize to %q with a different geometry, memory, count or placement set;"+
					" all of them are withheld", strings.Join(raws, ", "), name,
			))
			continue
		}
		profiles = append(profiles, group[0].profile)
	}
	return profiles, rejected
}

// profileEqual 比较对外发布的全部字段。只有规格完全一致，才能合并同名的两条读数。
func profileEqual(a, b device.AcceleratorPhysicalSlicedProfile) bool {
	return a.Name == b.Name &&
		a.MemoryMib == b.MemoryMib &&
		a.ComputeSlices == b.ComputeSlices &&
		a.MemorySlices == b.MemorySlices &&
		a.Count == b.Count &&
		slices.Equal(a.Placements, b.Placements)
}

// migProfileName 从以 NUL 填充的定长 int8 数组中读取厂商规格名。
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

// physicalSliced 将已确认的规格转换为硬切能力。驱动没有给出可用规格时，保留空能力。
func physicalSliced(profiles []device.AcceleratorPhysicalSlicedProfile) device.AcceleratorPhysicalSliced {
	if len(profiles) == 0 {
		return device.AcceleratorPhysicalSliced{}
	}
	return device.AcceleratorPhysicalSliced{
		Profiles: profiles,
		Count:    maxProfileCount(profiles),
	}
}

// maxProfileCount 取所有规格 Count 的最大值作为单卡硬切上限，规格列表为空时返回 0。
func maxProfileCount(profiles []device.AcceleratorPhysicalSlicedProfile) int32 {
	var ceiling int32
	for _, p := range profiles {
		if p.Count > ceiling {
			ceiling = p.Count
		}
	}
	return ceiling
}

// rejectDivergentGroupProfiles 在组汇总前排除各卡读数不一致的同名规格，并给调用方返回每个被排除名称的原因。
func rejectDivergentGroupProfiles(group *device.DevicesGroup) (rejected []string) {
	first := make(map[string]device.AcceleratorPhysicalSlicedProfile)
	divergent := make(map[string]struct{})
	for i := range group.Accelerators {
		for _, p := range group.Accelerators[i].Status.PhysicalSliced.Profiles {
			seen, ok := first[p.Name]
			if !ok {
				first[p.Name] = p
				continue
			}
			if _, known := divergent[p.Name]; !known && !profileEqual(seen, p) {
				divergent[p.Name] = struct{}{}
				rejected = append(rejected, fmt.Sprintf(
					"profile %q is exposed with a different geometry, memory, count or placement set by"+
						" card %s than by another card of the same group; it is withheld from the group",
					p.Name, group.Accelerators[i].ID,
				))
			}
		}
	}
	if len(divergent) == 0 {
		return nil
	}

	for i := range group.Accelerators {
		sliced := &group.Accelerators[i].Status.PhysicalSliced
		kept := slices.DeleteFunc(sliced.Profiles, func(p device.AcceleratorPhysicalSlicedProfile) bool {
			_, drop := divergent[p.Name]
			return drop
		})
		if len(kept) == 0 {
			sliced.Profiles = nil
			sliced.Count = 0
			continue
		}
		sliced.Profiles = kept
		sliced.Count = maxProfileCount(kept)
	}
	return rejected
}
