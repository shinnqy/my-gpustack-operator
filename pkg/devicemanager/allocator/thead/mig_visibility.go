package thead

import (
	"context"
	"fmt"

	core "k8s.io/api/core/v1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/deviceplugin"
)

// 设备插件通过类型断言识别物理分区接口。编译期验证确保创建和可见性两个方法都已实现。
var _ deviceplugin.PhysicalSlicedResponder = (*server)(nil)

// GetPhysicalSlicedVisibilityResponse 为随附容器构造主容器已持有分区的设备列表。从重启后仍存在的所有
// 权标记读取身份，再对照驱动确认实例。
//
// 该厂商通过设备节点控制访问，没有负责选分区的容器运行时钩子。和主容器一样，每个容器只取一次共享控制
// 节点，再按卡依次添加卡本身、GI 和 CI 的能力节点。
//
// 任何一项身份尚未证实时返回错误，不能以父卡代替已分配的分区，否则其他租户的分区也可能暴露。
func (s *server) GetPhysicalSlicedVisibilityResponse(
	_ context.Context,
	pod *core.Pod,
	ctr *core.Container,
	devs *workercore.Devices,
	allocated map[deviceplugin.Resource]int32,
	owner string,
) (*deviceplugin.ContainerAllocateResponse, error) {
	if s.mig == nil {
		return nil, fmt.Errorf("mig driver not configured; cannot address the partition container %q holds", owner)
	}

	// 沿用主容器枚举 devs 的顺序组装随附容器的设备列表。
	cards := allocatedAccelerators(devs, allocated)
	if len(cards) == 0 {
		return nil, fmt.Errorf("no allocated card for visibility container %q", ctr.Name)
	}

	// 共享控制节点供所有卡使用，每个容器只需预先验证一次。
	sharedPaths := sharedControlNodePaths()
	devices := make([]*deviceplugin.DeviceSpec, 0, len(sharedPaths)+3*len(cards))
	for _, path := range sharedPaths {
		spec, err := requireDeviceNode(path)
		if err != nil {
			return nil, err
		}
		devices = append(devices, spec)
	}

	for _, cardUUID := range cards {
		// 卡序号用于定位设备节点和 procfs 能力目录。先用探测到的设备身份验证序号确实指向该卡，再构造路径，避
		// 免误用相邻卡的分区。
		ordinal, cardNode, err := requireCardNode(devs, cardUUID)
		if err != nil {
			return nil, err
		}

		inst, err := s.liveOwnedInstance(devs, string(pod.UID), owner, cardUUID)
		if err != nil {
			return nil, err
		}

		specs, derr := partitionDeviceSpecs(ordinal, cardNode, inst)
		if derr != nil {
			return nil, fmt.Errorf("card %s partition device nodes: %w", cardUUID, derr)
		}
		devices = append(devices, specs...)
	}

	return &deviceplugin.ContainerAllocateResponse{Devices: devices}, nil
}

// liveOwnedInstance 读取所有者在 cardUUID 上的标记，并核对驱动实时状态。记录缺失、损坏、指向其他卡或
// 规格、实例已消失或编号复用均返回错误。
//
// 驱动枚举的是 GI，不能提供内部 CI 的编号。用驱动中的实例身份验证标记，再从标记取回 GI 和 CI 的完整
// 编号以构造设备节点。
func (s *server) liveOwnedInstance(devs *workercore.Devices, podUID, owner, cardUUID string) (migInstance, error) {
	path := markerPath(deviceplugin.OperatorPodsDir, podUID, owner, cardUUID)
	m, err := parseMarker(path)
	if err != nil {
		return migInstance{}, fmt.Errorf("read the partition ownership marker of container %q on card %s: %w",
			owner, cardUUID, err)
	}
	if m.Card != cardUUID {
		return migInstance{}, fmt.Errorf("marker %q records card %s, not %s: fail closed", path, m.Card, cardUUID)
	}
	// 读取驱动状态所需的几何参数取自该卡探测时公布的规格。若已找不到该规格，无法继续验证。
	computeSlices, memorySlices, ok := profileGeometry(devs, cardUUID, m.Profile)
	if !ok {
		return migInstance{}, fmt.Errorf("card %s no longer offers the profile %q recorded by marker %q: fail closed",
			cardUUID, m.Profile, path)
	}

	// 在 reserveMigInstance 使用的卡锁内读取一致的驱动快照，再在锁外检查快照。
	unlock := lockCard(cardUUID)
	state, err := s.mig.CardState(cardUUID, m.Profile, computeSlices, memorySlices)
	unlock()
	if err != nil {
		return migInstance{}, fmt.Errorf("read card %s state: %w", cardUUID, err)
	}

	liveInst, ok := findLiveInstance(state, m.GiID)
	if !ok {
		return migInstance{}, fmt.Errorf("marker %q references missing gpu instance %d on card %s: fail closed",
			path, m.GiID, cardUUID)
	}
	// GI 的数字编号可被重新分配，因此还要比较实时实例标识与标记中的标识。
	if liveInst.UUID != m.MigUUID {
		return migInstance{}, fmt.Errorf(
			"marker %q gpu instance %d uuid %q no longer matches live uuid %q (id reused): fail closed",
			path, m.GiID, m.MigUUID, liveInst.UUID,
		)
	}
	return m.instance(), nil
}
