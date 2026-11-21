package thead

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/binding/hgml"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/nodefeature"
)

// deriveSlicedProfiles 过滤媒体与图形变体，规范化厂商名称，通过实测落位计算几何跨度。
// 同名规格若给出矛盾的形状则不发布。
func deriveSlicedProfiles(
	infos []hgml.GpuInstanceProfileInfo_v3,
	placementsFor func(giProfileID uint32) []device.AcceleratorPlacement,
) (profiles []device.AcceleratorPhysicalSlicedProfile, rejected []string) {
	seen := make(map[string]int, len(infos))
	withheld := make(map[string]struct{}, len(infos))
	for _, info := range infos {
		raw := info.GetName()
		if isMediaOrGraphicsVariant(info, raw) {
			continue
		}

		name := nodefeature.NormalizePartitionedProfileName(raw)
		if nodefeature.GetAcceleratablePartitionedProfileResourceName(Manufacturer, name) == "" {
			rejected = append(rejected, fmt.Sprintf(
				"profile %d named %q yields no valid resource name, so it cannot be requested", info.Id, raw,
			))
			continue
		}

		// 在处理名称前先剔除跨度为空的读数，避免它占用或阻止同名的有效规格。
		var placements []device.AcceleratorPlacement
		if placementsFor != nil {
			placements = placementsFor(info.Id)
		}
		if len(placements) == 0 || placements[0].Length <= 0 {
			rejected = append(rejected, fmt.Sprintf(
				"profile %d named %q has no legal placement, so its memory-slice span is unknown", info.Id, raw,
			))
			continue
		}

		p := device.AcceleratorPhysicalSlicedProfile{
			Name:          name,
			MemoryMib:     int64(info.MemorySizeMB),
			ComputeSlices: int32(info.SliceCount),
			MemorySlices:  placements[0].Length,
			Count:         int32(info.InstanceCount),
			Placements:    placements,
		}

		if _, dropped := withheld[name]; dropped {
			continue
		}
		if idx, dup := seen[name]; dup {
			if profileEqual(profiles[idx], p) {
				continue
			}
			rejected = append(rejected, fmt.Sprintf(
				"profile %d named %q normalizes to %q, which another profile already exposes with a"+
					" different geometry, memory, count or placement set; both are withheld",
				info.Id, raw, name,
			))
			withheld[name] = struct{}{}
			profiles = slices.Delete(profiles, idx, idx+1)
			for n, i := range seen {
				if i > idx {
					seen[n] = i - 1
				}
			}
			delete(seen, name)
			continue
		}
		seen[name] = len(profiles)
		profiles = append(profiles, p)
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

// rejectDivergentGroupProfiles 在逐卡规格汇总前排除同组卡对同名规格的矛盾读数，并返回原因供日志记录。
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

// migPlacementsFromHGML 将 HGML 的分区起点、长度转换为 Operator 落位，无数据时返回 nil。
func migPlacementsFromHGML(slots []hgml.GpuInstancePlacement) []device.AcceleratorPlacement {
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

// migPlacementsByProfile 查询各规格在空卡上的合法区间，以 profile ID 保存成功结果，并记录失败。
func migPlacementsByProfile(
	infos []hgml.GpuInstanceProfileInfo_v3,
	query func(giProfileID uint32) ([]hgml.GpuInstancePlacement, hgml.Return),
) ([]hgml.GpuInstanceProfileInfo_v3, map[uint32][]device.AcceleratorPlacement, error) {
	answered := make([]hgml.GpuInstanceProfileInfo_v3, 0, len(infos))
	byID := make(map[uint32][]device.AcceleratorPlacement, len(infos))
	var errs []error
	for _, info := range infos {
		slots, ret := query(info.Id)
		if !ret.IsSuccess() {
			errs = append(errs, fmt.Errorf("profile %d: %w", info.Id, ret))
			continue
		}
		answered = append(answered, info)
		byID[info.Id] = migPlacementsFromHGML(slots)
	}
	return answered, byID, errors.Join(errs...)
}

// isMediaOrGraphicsVariant 判断规格是否带媒体引擎或图形能力，当前仅公布可由单一计算实例完整使用的普通规格。
func isMediaOrGraphicsVariant(info hgml.GpuInstanceProfileInfo_v3, name string) bool {
	return strings.ContainsRune(name, '+') ||
		info.Capabilities&hgml.GPU_INSTANCE_PROFILE_CAPS_GFX != 0
}

// driverReportsAbsent 判断驱动是否明确表示编号不存在。对不能回答的编号保留故障信息，
// 避免把读取失败默认为卡的固有容量。
func driverReportsAbsent(ret hgml.Return) bool {
	switch ret {
	case hgml.ERROR_NOT_SUPPORTED, hgml.ERROR_NOT_FOUND, hgml.ERROR_INVALID_ARGUMENT:
		return true
	default:
		return false
	}
}

// probeMigProfiles 遍历全部 GPU 实例规格 ID，区分成功、明确不支持与读取失败。
func probeMigProfiles(
	probe func(giProfileID uint32) (hgml.GpuInstanceProfileInfo_v3, hgml.Return),
) ([]hgml.GpuInstanceProfileInfo_v3, error) {
	var infos []hgml.GpuInstanceProfileInfo_v3
	var unreadable []error
	for id := uint32(0); id < hgml.GPU_INSTANCE_PROFILE_COUNT; id++ {
		info, ret := probe(id)
		if !ret.IsSuccess() {
			if !driverReportsAbsent(ret) {
				unreadable = append(unreadable, fmt.Errorf("profile %d: %w", id, ret))
			}
			continue
		}
		infos = append(infos, info)
	}
	return infos, errors.Join(unreadable...)
}

// detectMigProfiles 为设备汇总可核实的 GPU 实例规格，跳过驱动确认的空位，记录未能读取的 ID。
func detectMigProfiles(dev hgml.Device, logger klog.Logger) []device.AcceleratorPhysicalSlicedProfile {
	infos, err := probeMigProfiles(dev.GetGpuInstanceProfileInfo)
	if err != nil {
		logger.Error(err,
			"left partition profiles out of a card's inventory because the driver could not answer for "+
				"them; a profile absent here is absent from the node's capacity, its flavor and its "+
				"InstanceType, so a partition of it can stop being nameable and a new request for it is refused")
	}

	// 探测时按规格缓存空卡落位，后续由账本中的占用区间推导剩余容量，无需在每次协调时调用 HGML。
	answered, placementsByID, err := migPlacementsByProfile(infos, dev.GetGpuInstancePossiblePlacements)
	if err != nil {
		logger.Error(err, "dropped partition profiles whose possible placements are unreadable")
	}

	profiles, rejected := deriveSlicedProfiles(answered,
		func(giProfileID uint32) []device.AcceleratorPlacement {
			return placementsByID[giProfileID]
		})
	for _, reason := range rejected {
		logger.Info("dropped unpublishable partition profile", "reason", reason)
	}
	return profiles
}

// physicalSliced 对外发布单卡实际探测到的硬切能力。没有规格时保留空值。
func physicalSliced(profiles []device.AcceleratorPhysicalSlicedProfile) device.AcceleratorPhysicalSliced {
	if len(profiles) == 0 {
		return device.AcceleratorPhysicalSliced{}
	}
	return device.AcceleratorPhysicalSliced{
		Profiles: profiles,
		Count:    maxProfileCount(profiles),
	}
}

// maxProfileCount 返回各规格可创建数量的最大值，无规格时返回 0。
func maxProfileCount(profiles []device.AcceleratorPhysicalSlicedProfile) int32 {
	var ceiling int32
	for _, p := range profiles {
		if p.Count > ceiling {
			ceiling = p.Count
		}
	}
	return ceiling
}
