package hygon

import (
	"context"
	"fmt"

	core "k8s.io/api/core/v1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/deviceplugin"
	"gpustack.ai/gpustack/pkg/utils/osx"
)

// 设备插件通过类型断言识别物理分区接口。编译期断言可防止方法签名变化后，插件悄悄失去分区能力。
var _ deviceplugin.PhysicalSlicedResponder = (*server)(nil)

// migVisibleDeviceEnv 是厂商 HSA 运行时读取的实例选择变量。必须保留 MIG- 前缀；实测裸 UUID 无法被解
// 析，运行时会退回到首个可见实例。
const migVisibleDeviceEnv = "DMI_MIG_VISIBLE_DEVICE"

// ActuatePhysicalSliced 为容器创建或复用一个分区，并构造绑定它所需的响应。
//
// # 单个容器为何只允许一张卡
//
// 在两代驱动上，即使绑定两个实例文件、传入逗号分隔的 ID 列表或 all，容器也只能看到一份分区。因此不能
// 向一个容器承诺来自多张加速器的分区。
//
// 分配阶段直接拒绝多卡请求，避免在工作负载无法使用的卡上创建分区并占用配额。
func (s *server) ActuatePhysicalSliced(
	_ context.Context,
	pod *core.Pod,
	ctr *core.Container,
	devs *workercore.Devices,
	allocated map[deviceplugin.Resource]int32,
	profile string,
) (*deviceplugin.PhysicalSlicedAllocation, error) {
	if s.mig == nil {
		return nil, fmt.Errorf("partition driver not configured")
	}

	cards, err := migAllocatedCards(devs, allocated)
	if err != nil {
		return nil, err
	}
	switch len(cards) {
	case 0:
		return nil, fmt.Errorf("no allocated card for partition container %q", ctr.Name)
	case 1:
	default:
		return nil, fmt.Errorf(
			"container %q was granted %d accelerators, but a hygon partition request can only be served by"+
				" one: the vendor runtime makes exactly one partition visible to a container, so the rest"+
				" would be carved and never reachable", ctr.Name, len(cards),
		)
	}
	card := cards[0]

	accel, ok := migAllocatedAccelerator(devs, allocated, card.UUID)
	if !ok {
		return nil, fmt.Errorf("card %s: absent from the device record: fail closed", card.UUID)
	}

	release := lockMigCard(card.PciBusID)
	inst, outcome, err := reserveMigInstance(
		s.mig, deviceplugin.OperatorPodsDir, string(pod.UID), ctr.Name, card, profile,
	)
	release()
	if err != nil {
		return nil, err
	}

	resp, err := s.migContainerResponse(accel, inst)
	if err != nil {
		s.rollbackMigInstance(card, inst, outcome, string(pod.UID), ctr.Name)
		return nil, err
	}

	res := deviceplugin.Resource{Group: accel.groupID, Device: card.UUID}
	return &deviceplugin.PhysicalSlicedAllocation{
		Profile: profile,
		Placements: deviceplugin.Placements{
			res: {{Start: inst.Placement.Start, Length: inst.Placement.Length}},
		},
		IDs:      map[deviceplugin.Resource]string{res: _MigInstanceIDPrefix + inst.UUID},
		Response: resp,
		Rollback: func() {
			s.rollbackMigInstance(card, inst, outcome, string(pod.UID), ctr.Name)
		},
	}, nil
}

// rollbackMigInstance 依照预留结果，只撤销当前调用所做的操作。
//
// 接管或重新绑定的分区在本次调用之前就已存在。对接管只删本次新增的标记；对已有绑定连标记也保留。
func (s *server) rollbackMigInstance(
	card migCard, inst migInstance, outcome migReserveOutcome, podUID, container string,
) {
	if outcome == migRebound {
		return
	}

	release := lockMigCard(card.PciBusID)
	defer release()

	if err := osx.Remove(migMarkerPath(deviceplugin.OperatorPodsDir, podUID, container, card.UUID)); err != nil {
		s.Logger.Error(err, "failed to drop the ownership record of a rolled-back partition",
			"card", card.UUID, "gpuInstance", inst.GiID)
	}
	if outcome != migCreated {
		return
	}
	if err := s.mig.DestroyInstance(card.PciBusID, inst); err != nil {
		s.Logger.Error(err, "failed to destroy a partition this allocation created; the orphan collector"+
			" will reclaim it", "card", card.UUID, "gpuInstance", inst.GiID)
	}
}

// GetPhysicalSlicedVisibilityResponse 为共用设备的容器指向所有者已经持有的分区。
//
// 先从持久记录定位，再对照驱动确认实例身份，最后返回与主容器相同的设备与配置。无法确认身份时拒绝提供
// 父卡访问，避免意外暴露同卡其他租户的分区。
func (s *server) GetPhysicalSlicedVisibilityResponse(
	_ context.Context,
	pod *core.Pod,
	ctr *core.Container,
	devs *workercore.Devices,
	allocated map[deviceplugin.Resource]int32,
	owner string,
) (*deviceplugin.ContainerAllocateResponse, error) {
	if s.mig == nil {
		return nil, fmt.Errorf(
			"partition driver not configured; cannot address the partition container %q holds", owner,
		)
	}

	cards, err := migAllocatedCards(devs, allocated)
	if err != nil {
		return nil, err
	}
	if len(cards) != 1 {
		return nil, fmt.Errorf(
			"container %q co-allocates %d accelerators, but a hygon partition is only ever held on one",
			ctr.Name, len(cards),
		)
	}
	card := cards[0]

	accel, ok := migAllocatedAccelerator(devs, allocated, card.UUID)
	if !ok {
		return nil, fmt.Errorf("card %s: absent from the device record: fail closed", card.UUID)
	}

	inst, err := s.liveOwnedMigInstance(card, string(pod.UID), owner)
	if err != nil {
		return nil, err
	}
	return s.migContainerResponse(accel, inst)
}

// liveOwnedMigInstance 读取某张卡上所有者的分区记录，并用驱动的实时状态核验。
//
// 记录缺失、损坏、卡名不符、实例已消失或 UUID 变化都会报错。此厂商每次创建都生成新 UUID，所以复用同
// 一数字编号的新实例可以据此区分。
func (s *server) liveOwnedMigInstance(card migCard, podUID, owner string) (migInstance, error) {
	path := migMarkerPath(deviceplugin.OperatorPodsDir, podUID, owner, card.UUID)
	m, err := parseMigMarker(path)
	if err != nil {
		return migInstance{}, fmt.Errorf(
			"read the partition record of container %q on card %s: %w", owner, card.UUID, err,
		)
	}

	release := lockMigCard(card.PciBusID)
	defer release()

	state, err := s.mig.CardState(card.PciBusID, m.Profile)
	if err != nil {
		return migInstance{}, fmt.Errorf("read card %s state: %w", card.UUID, err)
	}
	live, ok := findLiveMigInstance(state, m.GiID)
	if !ok {
		return migInstance{}, fmt.Errorf(
			"the partition container %q holds on card %s (gpu instance %d) is gone: fail closed",
			owner, card.UUID, m.GiID,
		)
	}
	if live.UUID != m.MigUUID {
		return migInstance{}, fmt.Errorf(
			"gpu instance %d on card %s now carries identity %q, not the %q container %q holds (id reused):"+
				" fail closed", m.GiID, card.UUID, live.UUID, m.MigUUID, owner,
		)
	}
	return m.instance(), nil
}

// migContainerResponse 构造使容器能访问特定分区的分配响应。
//
// 厂商运行时需要四项内容：节点级控制设备、对应加速器的 DRM 节点、用户态运行库和该分区的注册文件。注
// 册文件必须在原绝对路径只读挂载，因为运行时会扫描该目录。另用 DMI_MIG_VISIBLE_DEVICE 显式选择实例。
//
// 注册文件是必需项。文件缺少时，即便容器拿到了设备节点，运行时仍会报 no devices available；在分配阶
// 段返回错误能直接指出缺少的绑定。
func (s *server) migContainerResponse(
	accel migAccelerator, inst migInstance,
) (*deviceplugin.ContainerAllocateResponse, error) {
	confMount := deviceplugin.NewROMount(inst.ConfPath)
	if confMount == nil {
		return nil, fmt.Errorf(
			"the registry file %q of partition %s is absent, so the container could not activate it",
			inst.ConfPath, inst.UUID,
		)
	}

	resp := &deviceplugin.ContainerAllocateResponse{}

	// 复用整卡和软切分使用的设备辅助函数，保持几个模式传入的控制节点一致。
	appendNodeDevices(resp, s.Logger)
	appendCardDevices(resp, accel.accel, s.Logger)

	if osx.Exists(hostHygonPath) {
		resp.Mounts = append(resp.Mounts,
			&deviceplugin.Mount{ContainerPath: ctrHygonDrvr, HostPath: hostHygonPath, ReadOnly: true})
	}
	if pMount := deviceplugin.NewROMount(hostHyhalDir); pMount != nil {
		resp.Mounts = append(resp.Mounts, pMount)
	}
	resp.Mounts = append(resp.Mounts, confMount)

	resp.Envs = map[string]string{migVisibleDeviceEnv: _MigInstanceIDPrefix + inst.UUID}

	return resp, nil
}
