package metax

import (
	"fmt"
	"time"

	klog "k8s.io/klog/v2"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/deviceplugin"
)

// sgpuCapability 是报告中用于标识 sysfs sGPU 能力的名称。
const sgpuCapability = "sgpu-mode"

const (
	sgpuDetailEnabled  = "the accelerator already hosts an sgpu subdevice and is in sgpu mode"
	sgpuDetailDisabled = "the accelerator is not yet in sgpu mode; the allocator puts it there when " +
		"the first slice lands"
)

// NewPreflighter 复用逻辑切分分配器的 sGPU 管理器，在创建子设备之前读取注册表。
func NewPreflighter(opts device.PreflighterOptions) device.AcceleratorPreflighter {
	logger := opts.Logger.WithName(Manufacturer)
	return &preflighter{
		logger: logger,
		sgpu:   newSysfsSGPUManager(),
	}
}

type preflighter struct {
	logger klog.Logger
	sgpu   sgpuManager
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

// PreflightResponder 复用生产响应器读取实际 sGPU 状态，拦截会修改宿主机的操作，并将容器工作目录重定向到临时位置。
func (p *preflighter) PreflightResponder(
	mode workercore.DeviceAllocationMode,
) (deviceplugin.ContainerAllocateResponder, func(), error) {
	_, restore, err := deviceplugin.NewPreflightRedirect()
	if err != nil {
		return nil, nil, err
	}

	srv := newServer(p.logger, mode)
	if s, ok := srv.(*server); ok && s.sgpu != nil {
		s.sgpu = &recordingSGPUManager{read: p.sgpu}
	}

	responder, ok := srv.(deviceplugin.ContainerAllocateResponder)
	if !ok {
		restore()
		return nil, nil, fmt.Errorf("metax %s server serves no container allocate response", mode)
	}
	return responder, restore, nil
}

// recordingSGPUManager 向实际管理器查询已有子设备，生成响应时屏蔽切换模式、修改 QoS、创建和回收子设备的操作。
type recordingSGPUManager struct {
	read sgpuManager
}

func (d *recordingSGPUManager) EnsureModel(string) error {

	return nil
}

func (d *recordingSGPUManager) SetSchedClass(string, schedClass) error {

	return nil
}

func (d *recordingSGPUManager) Create(string, int, int64, string) error {

	return nil
}

func (d *recordingSGPUManager) Remove(string, int) error {

	return nil
}

func (d *recordingSGPUManager) List() ([]sgpuSubdevice, error) {

	return d.read.List()
}

// check 只读一张卡的 sGPU 注册表。创建及回收子设备会改变节点，因此在这个检查项中只报告已读到的信息。
func (p *preflighter) check(accel *workercore.Accelerator) device.PreflightCheck {
	c := device.PreflightCheck{Accelerator: accel.ID, Capability: sgpuCapability, Mode: device.PreflightModeOf(workercore.DeviceAllocationModeSliced)}

	// 使用 reserveSlice 也使用的 PCI 总线地址定位卡。
	bdf := accel.Topology.PciBusID
	if bdf == "" {
		c.State = device.PreflightStateUnavailable
		c.Reason = "the accelerator reports no pci bus id, so sysfs cannot address it"
		return c
	}

	registry, err := p.sgpu.List()
	c.State, c.Detail, c.Reason = classifySGPUListCall(registry, bdf, err)
	return c
}

// classifySGPUListCall 复用 reserveSlice 对注册表读取结果的判断，成功时报告已登记的子设备数，
// 失败时附上驱动错误。
func classifySGPUListCall(registry []sgpuSubdevice, bdf string, err error) (state device.PreflightState, detail, reason string) {
	if err != nil {
		return device.PreflightStateUnavailable, "", err.Error()
	}
	if cardHasSubdevice(registry, bdf) {
		return device.PreflightStateOK, sgpuDetailEnabled, ""
	}
	return device.PreflightStateOK, sgpuDetailDisabled, ""
}
