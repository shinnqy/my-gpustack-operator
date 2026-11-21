// SPDX-FileCopyrightText: 2026 GPUStack, Inc.
// SPDX-License-Identifier: Apache-2.0

package dmi

// Device 可表示物理 DCU 或划出的 MIG 实例，二者共享内存和利用率等查询接口。
type Device struct {
	handle dmiDevice
	lib    *DMI
}

// maxInstancesPerQuery 限制两种数组查询的缓冲区大小。当前卡有四个 GPU 切片，
// 这里保留额外空间以兼容更细的切分。
const maxInstancesPerQuery = 32

// GetGpuInstanceProfileInfoBySliceCount 按切片数量查询规格。厂商参数是切片数枚举的索引，0 表示一片，
// 3 表示四片。真正的 profile ID 在返回结构中。
func (d Device) GetGpuInstanceProfileInfoBySliceCount(sliceCount uint32) (GpuInstanceProfileInfo, Return) {
	var info GpuInstanceProfileInfo
	if sliceCount == 0 || sliceCount > GPU_INSTANCE_PROFILE_COUNT {
		return info, ERROR_INVALID_ARGUMENT
	}
	ret := nvmlDeviceGetGpuInstanceProfileInfo(d.handle, sliceCount-1, &info)
	return info, ret
}

// GetGpuInstancePossiblePlacements 枚举空卡上该规格允许的起点和长度。分配时还要另外排除已经占用的区间。
func (d Device) GetGpuInstancePossiblePlacements(profileID uint32) ([]GpuInstancePlacement, Return) {
	placements := make([]GpuInstancePlacement, maxInstancesPerQuery)
	count := uint32(len(placements))
	ret := nvmlDeviceGetGpuInstancePossiblePlacements(d.handle, profileID, &placements[0], &count)
	if !ret.IsSuccess() {
		return nil, ret
	}
	if count > uint32(len(placements)) {
		return nil, ERROR_INSUFFICIENT_SIZE
	}
	return placements[:count], ret
}
