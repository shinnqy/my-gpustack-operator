package nvidia

import (
	"context"
	"fmt"
	"strings"

	core "k8s.io/api/core/v1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/deviceplugin"
)

// 设备插件通过类型断言识别物理分区接口。编译期验证确保创建和可见性两个方法都已实现。
var _ deviceplugin.PhysicalSlicedResponder = (*server)(nil)

// GetPhysicalSlicedVisibilityResponse 为共用分区的容器返回主容器持有的 MIG 设备。按主容器的持久标记
// 定位，再查询驱动确认分区仍存活。
//
// 无法证明分区身份时返回错误。若退回到父 GPU 的可见性，可能让该容器看到其他租户在同一张卡上的分区。
func (s *server) GetPhysicalSlicedVisibilityResponse(
	_ context.Context,
	pod *core.Pod,
	ctr *core.Container,
	devs *workercore.Devices,
	allocated map[deviceplugin.Resource]int32,
	owner string,
) (*deviceplugin.ContainerAllocateResponse, error) {
	if s.mig == nil {
		return nil, fmt.Errorf("mig driver not configured; cannot name the partition container %q holds", owner)
	}

	// 沿用主容器的 devs 顺序，逐卡生成相同顺序的 NVIDIA_VISIBLE_DEVICES。
	cards := allocatedAccelerators(devs, allocated)
	if len(cards) == 0 {
		return nil, fmt.Errorf("no allocated card for visibility container %q", ctr.Name)
	}

	uuids := make([]string, 0, len(cards))
	for _, cardUUID := range cards {
		uuid, err := s.livePartitionUUID(devs, string(pod.UID), owner, cardUUID)
		if err != nil {
			return nil, err
		}
		uuids = append(uuids, uuid)
	}

	return &deviceplugin.ContainerAllocateResponse{
		Envs: map[string]string{"NVIDIA_VISIBLE_DEVICES": strings.Join(uuids, ",")},
	}, nil
}

// livePartitionUUID 读取主容器在指定物理卡上的标记，并验证实时 MIG UUID。标记缺失、损坏、指向别的卡
// 或分区已经消失时，返回错误。
func (s *server) livePartitionUUID(devs *workercore.Devices, podUID, owner, cardUUID string) (string, error) {
	path := markerPath(podUID, owner, cardUUID)
	m, err := parseMarker(path)
	if err != nil {
		return "", fmt.Errorf("read the partition ownership marker of container %q on card %s: %w",
			owner, cardUUID, err)
	}
	if m.Card != cardUUID {
		return "", fmt.Errorf("marker %q records card %s, not %s: fail closed", path, m.Card, cardUUID)
	}
	// 读取驱动状态所需的几何参数取自该卡探测时公布的规格。若已找不到该规格，无法继续验证。
	computeSlices, memorySlices, ok := profileGeometry(devs, cardUUID, m.Profile)
	if !ok {
		return "", fmt.Errorf("card %s no longer offers the profile %q recorded by marker %q: fail closed",
			cardUUID, m.Profile, path)
	}

	// 与 reserveMigInstance 共用卡锁，在锁内获取一致的驱动快照，再在锁外检查。解锁后仍可能发生真正的回收，
	// 快照只代表读取时的状态。
	unlock := lockCard(cardUUID)
	state, err := s.mig.CardState(cardUUID, m.Profile, computeSlices, memorySlices)
	unlock()
	if err != nil {
		return "", fmt.Errorf("read card %s state: %w", cardUUID, err)
	}

	liveInst, ok := findLiveInstance(state, m.GiID)
	if !ok {
		return "", fmt.Errorf("marker %q references missing gpu instance %d on card %s: fail closed",
			path, m.GiID, cardUUID)
	}
	// GI 编号可以回收再用，因此还要核对实时 MIG UUID 与标记中的值。
	if liveInst.UUID != m.MigUUID {
		return "", fmt.Errorf("marker %q gpu instance %d uuid %q no longer matches live uuid %q (id reused): fail closed",
			path, m.GiID, m.MigUUID, liveInst.UUID)
	}
	return m.MigUUID, nil
}
