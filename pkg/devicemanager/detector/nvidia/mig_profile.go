package nvidia

import (
	"errors"
	"fmt"
	"strings"

	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/binding/nvml"
	"gpustack.ai/gpustack/pkg/device"
)

// deriveSlicedProfiles 将 NVML 实测规格转为设备的硬切清单。跳过 +me 和 +gfx 等特殊变体，按规范名称去重，
// 并依据合法区间确定切片跨度。没有真实名称的规格不能交给分配器，汇总这些 ID 的错误供排查。
func deriveSlicedProfiles(
	infos []nvml.GpuInstanceProfileInfo_v3,
	placementsFor func(giProfileID uint32) []device.AcceleratorPlacement,
) ([]device.AcceleratorPhysicalSlicedProfile, error) {
	seen := make(map[string]struct{}, len(infos))
	var profiles []device.AcceleratorPhysicalSlicedProfile
	var errs []error
	for _, info := range infos {
		name := info.GetName()
		if name == "" {
			errs = append(errs, fmt.Errorf("profile %d: driver reported no name", info.Id))
			continue
		}
		if isMediaOrGraphicsVariant(info, name) {
			continue
		}

		// 先剔除没有合法跨度的规格，再按名称去重，使其他真正可用的同名 ID 仍可发布。
		var placements []device.AcceleratorPlacement
		if placementsFor != nil {
			placements = placementsFor(info.Id)
		}
		if len(placements) == 0 || placements[0].Length <= 0 {
			errs = append(errs, fmt.Errorf(
				"profile %d named %q: driver enumerated no legal placement, so its memory-slice span is unknown",
				info.Id, name,
			))
			continue
		}

		if _, dup := seen[name]; dup {
			continue
		}
		seen[name] = struct{}{}

		profiles = append(profiles, device.AcceleratorPhysicalSlicedProfile{
			Name:          name,
			MemoryMib:     int64(info.MemorySizeMB),
			ComputeSlices: int32(info.SliceCount),
			MemorySlices:  placements[0].Length,
			Count:         int32(info.InstanceCount),
			Placements:    placements,
		})
	}
	return profiles, errors.Join(errs...)
}

// migPlacementsFromNVML 将 NVML 的起点与长度转换为 Operator 的落位类型，无数据时返回 nil。
func migPlacementsFromNVML(slots []nvml.GpuInstancePlacement) []device.AcceleratorPlacement {
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

// migPlacementsByProfile 按实际探测出的 profile ID 查询空卡合法落位。只保留驱动成功回答的规格及其区间，
// 并报告不可读的 ID。
func migPlacementsByProfile(
	infos []nvml.GpuInstanceProfileInfo_v3,
	query func(giProfileID uint32) ([]nvml.GpuInstancePlacement, nvml.Return),
) ([]nvml.GpuInstanceProfileInfo_v3, map[uint32][]device.AcceleratorPlacement, error) {
	answered := make([]nvml.GpuInstanceProfileInfo_v3, 0, len(infos))
	byID := make(map[uint32][]device.AcceleratorPlacement, len(infos))
	var errs []error
	for _, info := range infos {
		slots, ret := query(info.Id)
		if !ret.IsSuccess() {
			errs = append(errs, fmt.Errorf("profile %d: %w", info.Id, ret))
			continue
		}
		answered = append(answered, info)
		byID[info.Id] = migPlacementsFromNVML(slots)
	}
	return answered, byID, errors.Join(errs...)
}

// isMediaOrGraphicsVariant 识别带专用媒体引擎或图形能力的 +me、+me.all、+gfx 变体。
// 当前仅发布计算实例可覆盖整个 GPU 实例的普通几何规格。
func isMediaOrGraphicsVariant(info nvml.GpuInstanceProfileInfo_v3, name string) bool {
	return strings.ContainsRune(name, '+') ||
		info.Capabilities&nvml.GPU_INSTANCE_PROFILE_CAPS_GFX != 0
}

// driverReportsAbsent 判断驱动是否明确表示编号不存在。对不能回答的编号保留故障信息，
// 避免把读取失败默认为卡的固有容量。
func driverReportsAbsent(ret nvml.Return) bool {
	switch ret {
	case nvml.ERROR_NOT_SUPPORTED, nvml.ERROR_NOT_FOUND, nvml.ERROR_INVALID_ARGUMENT:
		return true
	default:
		return false
	}
}

// probeMigProfiles 遍历完整的规格 ID 范围，分别记录有效规格、明确不存在的编号，以及未能测量的编号。
func probeMigProfiles(
	probe func(giProfileID uint32) (nvml.GpuInstanceProfileInfo_v3, nvml.Return),
) ([]nvml.GpuInstanceProfileInfo_v3, error) {
	var infos []nvml.GpuInstanceProfileInfo_v3
	var unreadable []error
	for id := uint32(0); id < nvml.GPU_INSTANCE_PROFILE_COUNT; id++ {
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

// detectMigProfiles 探测单卡的硬切规格。跳过驱动确认的空位，保留可读规格，并记录其他读取错误。
func detectMigProfiles(dev nvml.Device) []device.AcceleratorPhysicalSlicedProfile {
	infos, probeErr := probeMigProfiles(dev.GetGpuInstanceProfileInfo)
	if probeErr != nil {
		uuid, _ := dev.GetUUID()
		klog.Background().Error(probeErr,
			"Left MIG profiles out of a card's inventory because the driver could not answer for them; "+
				"a profile absent here is absent from the node's capacity, its flavor and its InstanceType, "+
				"so a MIG instance of it can stop being nameable and a new request for it is refused",
			"device", uuid)
	}

	// 缓存各规格在空卡上的合法位置，后续可从中减去分配注解记录的占用区间。
	// 按真实 profile ID 查询可区分计算切片数相同的不同规格，查询失败时记录对应 ID。
	// 按驱动返回的真实 profile ID 查询合法落位
	answered, placementsByID, err := migPlacementsByProfile(infos, dev.GetGpuInstancePossiblePlacements)
	if err != nil {
		uuid, _ := dev.GetUUID()
		klog.Background().Error(err, "Dropped MIG profiles whose possible placements are unreadable",
			"device", uuid)
	}

	placementsFor := func(giProfileID uint32) []device.AcceleratorPlacement {
		return placementsByID[giProfileID]
	}

	// 没有真实名称的规格无法按名分配。没有合法落位的规格也无法确定占用跨度。两种情况都跳过并保留诊断信息。
	profiles, err := deriveSlicedProfiles(answered, placementsFor)
	if err != nil {
		uuid, _ := dev.GetUUID()
		klog.Background().Error(err, "Dropped unpublishable MIG profiles", "device", uuid)
	}
	return profiles
}

// maxProfileCount 取可用规格中最大的 Count 作为硬切上限。例如 A100 可容纳 7 个 1g.5gb，无规格时返回 0。
func maxProfileCount(profiles []device.AcceleratorPhysicalSlicedProfile) int32 {
	var ceiling int32
	for _, p := range profiles {
		if p.Count > ceiling {
			ceiling = p.Count
		}
	}
	return ceiling
}
