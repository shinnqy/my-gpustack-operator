package deviceplugin

import (
	"fmt"

	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/nodefeature"
)

// PreflightPodName、PreflightPodNamespace 和 PreflightPodUID 为预检分配请求提供固定身份。
// 相同请求会生成相同的宿主机工作目录。
const (
	PreflightPodName      = "preflight"
	PreflightPodNamespace = "default"
	PreflightPodUID       = types.UID("preflight")
	// PreflightContainerName 是分配请求中初始容器的名称。
	PreflightContainerName = "preflight"
)

// NewPreflightAllocationRequest 将实际探测到的设备组、分配模式和单卡额度转换为响应器所需的 Pod、容器、库存和分配表，
// 资源名与生产 webhook 一致。缺少设备、额度无效或模式无法表达时返回错误。
func NewPreflightAllocationRequest(
	groups []workercore.DevicesGroup,
	manufacturer string,
	mode workercore.DeviceAllocationMode,
	quota int32,
) (*core.Pod, *core.Container, *workercore.Devices, map[Resource]int32, error) {
	if len(groups) == 0 {
		return nil, nil, nil, nil, fmt.Errorf("no device groups given")
	}
	if quota <= 0 {
		return nil, nil, nil, nil, fmt.Errorf("quota must be positive, got %d", quota)
	}
	// 超过整卡的请求需要拒绝，以免账本用整卡封顶，而容器资源和注入文件却记录超过整卡的额度。
	if quota > nodefeature.ResourceMaxUnits {
		return nil, nil, nil, nil, fmt.Errorf(
			"quota must not exceed one whole accelerator (%d units), got %d",
			nodefeature.ResourceMaxUnits, quota,
		)
	}

	// 先验证厂商，避免把未知厂商的空资源名前缀和模式后缀拼成无效的扩展资源名。
	if !nodefeature.IsKnownAcceleratableManufacturer(manufacturer) {
		return nil, nil, nil, nil, fmt.Errorf(
			"manufacturer %q is not an acceleratable manufacturer this operator knows", manufacturer,
		)
	}

	// 先验证分配模式。资源名和计量函数对未知模式都有默认分支，盲用默认值会把错误的请求当作整卡请求。
	switch mode {
	case workercore.DeviceAllocationModeExclusive, workercore.DeviceAllocationModeShared,
		workercore.DeviceAllocationModeSliced, workercore.DeviceAllocationModeVisibility:

	case workercore.DeviceAllocationModePartitioned:
		// 硬切分需要具体的分区类型和 profile。预检通过读取驱动检查分区能力，因此这里拒绝构造缺少 profile 的分区请求。
		return nil, nil, nil, nil, fmt.Errorf(
			"allocation mode %s cannot be asked about through a preflight request: a partition is "+
				"named by a <kind>-<profile> limit this builder has no profile to write, and a "+
				"request carrying only the bare resource is refused at allocation for naming no "+
				"partition profile", mode,
		)

	default:
		return nil, nil, nil, nil, fmt.Errorf(
			"allocation mode %s (%d) is not a mode a preflight can ask about", mode, uint32(mode),
		)
	}

	// 此处已确认厂商有资源名，并排除了没有 profile 的分区模式。
	resName := nodefeature.GetAcceleratableResourceName(manufacturer, mode)

	var found bool
	allocation := make(map[Resource]int32)
	for i := range groups {
		grp := &groups[i]
		if grp.Manufacturer != manufacturer {
			continue
		}
		found = true
		for j := range grp.Accelerators {
			res := Resource{Group: grp.ID, Device: grp.Accelerators[j].ID}
			allocation[res] = preflightAllocatedUnits(mode, quota)
		}
	}
	if !found {
		return nil, nil, nil, nil, fmt.Errorf("no device group for manufacturer %q", manufacturer)
	}
	if len(allocation) == 0 {
		return nil, nil, nil, nil, fmt.Errorf("manufacturer %q has no accelerator in the given groups", manufacturer)
	}

	limits := core.ResourceList{
		resName: *resource.NewQuantity(int64(len(allocation)), resource.DecimalSI),
	}
	// 直接向分配器查询计量资源名，确保预检请求与实际请求读取同一项 units。
	srv := &ResourceServer{Manufacturer: manufacturer, AllocationMode: mode}
	if unitsName := srv.unitsResourceName(); unitsName != "" {
		limits[unitsName] = *resource.NewQuantity(int64(quota), resource.DecimalSI)
	}

	// 逻辑切片除了 units，还要给出显存和算力两个维度，响应器据此生成卡内配额。
	if mode == workercore.DeviceAllocationModeSliced {
		// 百分比字段只接受整数。无法精确换算的 units 要直接报错，避免容器约束和账本记录出现偏差。
		if int64(quota)*100%nodefeature.ResourceMaxUnits != 0 {
			return nil, nil, nil, nil, fmt.Errorf(
				"a sliced quota must be a whole percent of an accelerator (a multiple of %d units), got %d",
				nodefeature.ResourceMaxUnits/100, quota,
			)
		}
		pct := int64(quota) * 100 / nodefeature.ResourceMaxUnits
		limits[nodefeature.GetAcceleratableSlicedMemoryPercentageResourceName(manufacturer)] = *resource.NewQuantity(pct, resource.DecimalSI)
		limits[nodefeature.GetAcceleratableSlicedCoresPercentageResourceName(manufacturer)] = *resource.NewQuantity(pct, resource.DecimalSI)
	}

	pod := &core.Pod{
		ObjectMeta: meta.ObjectMeta{
			Name:      PreflightPodName,
			Namespace: PreflightPodNamespace,
			UID:       PreflightPodUID,
		},
		Spec: core.PodSpec{
			Containers: []core.Container{{
				Name:      PreflightContainerName,
				Resources: core.ResourceRequirements{Limits: limits},
			}},
		},
	}

	devs := &workercore.Devices{
		Spec: workercore.DevicesSpec{Groups: groups},
	}

	return pod, &pod.Spec.Containers[0], devs, allocation, nil
}

// preflightAllocatedUnits 按生产分配器的计费规则换算单个 token。整卡计满额，共享计一份共享额度，
// 切片按请求的 units 计。
func preflightAllocatedUnits(mode workercore.DeviceAllocationMode, quota int32) int32 {
	var allocated int32
	switch mode {
	default:
		allocated = nodefeature.ResourceMaxUnits
	case workercore.DeviceAllocationModeShared:
		allocated = nodefeature.ResourceMaxUnits / nodefeature.SharedResourceMaxSize
	case workercore.DeviceAllocationModeSliced, workercore.DeviceAllocationModePartitioned:
		allocated = quota
	}
	return allocated
}
