package nvml

// sliceCountToGPUInstanceProfileID 和 sliceCountToComputeInstanceProfileID 将规格的计算切片数量映射到
// NVML GPU 实例和计算实例的 profile ID。这种关系属于固定的 NVML 查找，而非算术运算：7 片、8 片及 6 片切片
// 的规格分别对应 ID 4、5 和 6（ID 的排序为 1、2、3、4、7、8、6 片），因此这种映射是明确指定的，而非通过计
// 算得出的。
var (
	sliceCountToGPUInstanceProfileID = map[int32]uint32{
		1: GPU_INSTANCE_PROFILE_1_SLICE,
		2: GPU_INSTANCE_PROFILE_2_SLICE,
		3: GPU_INSTANCE_PROFILE_3_SLICE,
		4: GPU_INSTANCE_PROFILE_4_SLICE,
		6: GPU_INSTANCE_PROFILE_6_SLICE,
		7: GPU_INSTANCE_PROFILE_7_SLICE,
		8: GPU_INSTANCE_PROFILE_8_SLICE,
	}
	sliceCountToComputeInstanceProfileID = map[int32]uint32{
		1: COMPUTE_INSTANCE_PROFILE_1_SLICE,
		2: COMPUTE_INSTANCE_PROFILE_2_SLICE,
		3: COMPUTE_INSTANCE_PROFILE_3_SLICE,
		4: COMPUTE_INSTANCE_PROFILE_4_SLICE,
		6: COMPUTE_INSTANCE_PROFILE_6_SLICE,
		7: COMPUTE_INSTANCE_PROFILE_7_SLICE,
		8: COMPUTE_INSTANCE_PROFILE_8_SLICE,
	}
)

// MigInstanceProfileIDs 包含了创建保留的 "<C>g.<M>gb" 规格实例的一个 MIG 实例所需的 NVML 规格 ID。对于这
// 些规格，计算实例会覆盖整个 GPU 实例，因此 ComputeInstanceProfileID 是与切片数量对应的
// COMPUTE_INSTANCE_PROFILE_*，而推理引擎规格始终为共享状态。
type MigInstanceProfileIDs struct {
	GpuInstanceProfileID           uint32
	ComputeInstanceProfileID       uint32
	ComputeInstanceEngineProfileID uint32
}

// MigProfileIDsForComputeSlices 返回用于创建 MIG 实例的 GPU 实例和计算实例规格 ID。该实例的 GPU 实例会覆
// 盖 computeSlices 个计算片。如果计算片数量符合 NVML 未定义规格的要求（例如 5），则返回 ok=false。计算实
// 例采用 SHARED 推理引擎规格，覆盖整个 GPU 实例。
func MigProfileIDsForComputeSlices(computeSlices int32) (MigInstanceProfileIDs, bool) {
	gi, ok := sliceCountToGPUInstanceProfileID[computeSlices]
	if !ok {
		return MigInstanceProfileIDs{}, false
	}
	return MigInstanceProfileIDs{
		GpuInstanceProfileID:           gi,
		ComputeInstanceProfileID:       sliceCountToComputeInstanceProfileID[computeSlices],
		ComputeInstanceEngineProfileID: COMPUTE_INSTANCE_ENGINE_PROFILE_SHARED,
	}, true
}
