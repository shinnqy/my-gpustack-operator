package amd

import (
	"fmt"
	"time"

	klog "k8s.io/klog/v2"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/deviceplugin"
)

// cuMaskCapability 对应切片分配器使用的 HSA 拓扑。只有能由拓扑导出 CU 掩码的卡才能按窗口切分。
const cuMaskCapability = "cu-mask-topology"

// NewPreflighter 用 PlaceLogicalSliced 同一个拓扑读取函数取得 HSA 数据，再验证能否生成 CU 掩码。
// 这个路径只读，不需要切换驱动状态。
func NewPreflighter(opts device.PreflighterOptions) device.AcceleratorPreflighter {
	return &preflighter{
		logger:       opts.Logger.WithName(Manufacturer),
		readTopology: readTopologyFn,
	}
}

type preflighter struct {
	logger klog.Logger
	// 构造时保存 readTopologyFn，单个预检器可以独立选择拓扑读取函数。
	readTopology func(pciBusID, cardUUID string) (Topology, error)
}

func (p *preflighter) PreflightAccelerator(groups device.DevicesGroupList) device.PreflightGroup {
	grp := device.PreflightGroup{
		Manufacturer: Manufacturer,
		Timestamp:    time.Now(),
	}
	for i := range groups {
		accels := groups[i].Accelerators
		for j := range accels {
			grp.Checks = append(grp.Checks, p.check(&accels[j]))
		}
	}
	return grp
}

// PreflightResponder 为指定模式创建生产分配使用的 AMD 响应器。复用相同的环境变量和计算窗口逻辑来渲染注入结果。
func (p *preflighter) PreflightResponder(
	mode workercore.DeviceAllocationMode,
) (deviceplugin.ContainerAllocateResponder, func(), error) {
	_, restore, err := deviceplugin.NewPreflightRedirect()
	if err != nil {
		return nil, nil, err
	}

	srv := newServer(p.logger, mode)

	responder, ok := srv.(deviceplugin.ContainerAllocateResponder)
	if !ok {
		restore()
		return nil, nil, fmt.Errorf("amd %s server serves no container allocate response", mode)
	}
	return responder, restore, nil
}

// check 和 PlaceLogicalSliced 一样，先定位加速器、读取并校验其拓扑，再判断 WindowCUs 能否建立窗口。
// 空设备 ID 会提前报错。
func (p *preflighter) check(accel *workercore.Accelerator) device.PreflightCheck {
	c := device.PreflightCheck{Accelerator: accel.ID, Capability: cuMaskCapability, Mode: device.PreflightModeOf(workercore.DeviceAllocationModeSliced)}

	if accel.ID == "" {
		c.State = device.PreflightStateUnavailable
		c.Reason = "the accelerator reports no unique id, so no hsa agent can be matched to it"
		return c
	}

	topo, err := p.readTopology(accel.Topology.PciBusID, accel.ID)
	c.State, c.Detail, c.Reason = classifyTopologyCall(topo, err)
	return c
}

// classifyTopologyCall 沿用分配器的判断，拓扑读取或校验失败时返回 unavailable，
// 成功时附上可用的 CU 拓扑。
func classifyTopologyCall(topo Topology, err error) (state device.PreflightState, detail, reason string) {
	if err != nil {
		return device.PreflightStateUnavailable, "", err.Error()
	}
	if err := topo.Validate(); err != nil {
		return device.PreflightStateUnavailable, "", err.Error()
	}
	return device.PreflightStateOK,
		fmt.Sprintf("%s reports %d compute units in %d-cu allocation atoms", topo.Name, topo.CU, topo.Quantum()),
		""
}
