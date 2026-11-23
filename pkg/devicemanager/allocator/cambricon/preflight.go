package cambricon

import (
	"errors"
	"fmt"
	"time"

	klog "k8s.io/klog/v2"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/deviceplugin"
)

// smluCapability 使用 cnDev 的名称标识 sMLU 模式检查。
const smluCapability = "smlu-mode"

const (
	smluDetailEnabled  = "sMLU mode is on"
	smluDetailDisabled = "sMLU mode is off, and this driver accepted being asked to turn it on, so " +
		"the allocator will manage to when a slice lands on this accelerator; it was put back the " +
		"way it was found"
	smluDetailNotAsked = "sMLU mode is off and was left that way: asking it on to see whether this " +
		"driver accepts is a write, and this is a dry run"

	smluDetailNotRestored = "sMLU mode is off, and this driver accepted being asked to turn it on " +
		"-- but putting it back failed, so the card is in sMLU mode now and stays there until it " +
		"is turned off"
)

// NewPreflighter 创建寒武纪预检器。基础检查读取逻辑切分依赖的 sMLU 模式，必要时短暂开启并恢复。
func NewPreflighter(opts device.PreflighterOptions) device.AcceleratorPreflighter {
	logger := opts.Logger.WithName(Manufacturer)
	return &preflighter{
		logger: logger,
		smlu:   newSMLUDriver(),
		dryRun: opts.DryRun,
	}
}

type preflighter struct {
	logger klog.Logger
	smlu   smluDriver
	dryRun bool
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

// PreflightResponder 用分配器原有的响应器生成注入文件，将目录重定向到临时位置，
// 避免渲染过程中创建真实 profile 或实例。
func (p *preflighter) PreflightResponder(
	mode workercore.DeviceAllocationMode,
) (deviceplugin.ContainerAllocateResponder, func(), error) {
	_, restore, err := deviceplugin.NewPreflightRedirect()
	if err != nil {
		return nil, nil, err
	}

	srv := newServer(p.logger, mode)
	if s, ok := srv.(*server); ok && s.smlu != nil {
		s.smlu = &recordingSMLUDriver{read: p.smlu}
	}

	responder, ok := srv.(deviceplugin.ContainerAllocateResponder)
	if !ok {
		restore()
		return nil, nil, fmt.Errorf("cambricon %s server serves no container allocate response", mode)
	}
	return responder, restore, nil
}

// recordingSMLUDriver 读取真实模式和现有实例，生成分配响应时屏蔽模式切换及 profile、实例的写入。
type recordingSMLUDriver struct {
	read smluDriver

	// nextProfileID 为同一轮响应生成的每个 profile 提供不同编号，避免多个实例引用同一个占位编号。
	nextProfileID int32
}

func (d *recordingSMLUDriver) GetSMLUMode(card string) (bool, error) {
	return d.read.GetSMLUMode(card)
}

func (d *recordingSMLUDriver) SetSMLUMode(string, bool) error {

	return nil
}

func (d *recordingSMLUDriver) CreateProfile(string, int, int64) (int32, error) {

	d.nextProfileID++
	return d.nextProfileID, nil
}

func (d *recordingSMLUDriver) DestroyProfile(string, int32) error {
	return nil
}

func (d *recordingSMLUDriver) CreateInstance(card string, profileID int32, name string) (smluInstance, error) {

	// 设备节点由驱动在真正创建实例时分配。此处只渲染响应，保留未知节点，避免返回不存在的设备路径。
	return smluInstance{card: card, profileID: profileID, name: name}, nil
}

func (d *recordingSMLUDriver) DestroyInstance(string, string) error {
	return nil
}

func (d *recordingSMLUDriver) ListInstances() ([]smluInstance, error) {
	return d.read.ListInstances()
}

func (d *recordingSMLUDriver) ListProfiles() ([]profileKey, error) {
	return d.read.ListProfiles()
}

// check 读取 sMLU 模式，确认关闭后才测试能否打开，再恢复为关闭。恢复失败时报告卡的实际状态并让预检失败。
func (p *preflighter) check(accel *workercore.Accelerator) device.PreflightCheck {
	c := device.PreflightCheck{Accelerator: accel.ID, Capability: smluCapability, Mode: device.PreflightModeOf(workercore.DeviceAllocationModeSliced)}

	// LogicalSliced.Count 是卡上报的切分容量，零值与驱动声明一起用于区分设备不支持和驱动读取失败。
	if accel.Status.LogicalSliced.Count == 0 {
		c.State = device.PreflightStateNotDeclared
		c.Reason = "the accelerator reports no logical-slicing capability, so no sMLU instance is carved on it"
		return c
	}

	// 用 PCI 总线地址调用 cnDev，与实际分配器使用同一种寻址方式。
	card := accel.Topology.PciBusID
	if card == "" {
		c.State = device.PreflightStateUnavailable
		c.Reason = "the accelerator reports no pci bus id, so cnDev cannot address it"
		return c
	}

	enabled, readErr := p.smlu.GetSMLUMode(card)
	c.State, c.Detail, c.Reason = classifySMLUCall(enabled, readErr)

	switch {
	case errors.Is(readErr, errSMLUUnsupported):
		// 驱动库无法查询时直接返回，跳过会改变卡状态的调用。
		return c
	case readErr != nil:
		// 读取失败后不能确定原状态，继续切换可能关闭原本已启用的模式。
		return c
	case enabled:
		return c
	case p.dryRun:
		// dry-run 在读取后返回，避免留下尚未恢复的 sMLU 模式。
		c.State, c.Detail, c.Reason = device.PreflightStateOK, smluDetailNotAsked, ""
		return c
	}

	if err := p.smlu.SetSMLUMode(card, true); err != nil {
		c.State, c.Detail, c.Reason = classifySMLUCall(false, err)
		return c
	}
	// 正常返回路径各自报告恢复结果。驱动意外触发 panic 时，也要尽力把已确认原为关闭的模式恢复。
	done := false
	defer func() {
		if done {
			return
		}
		if err := p.smlu.SetSMLUMode(card, false); err != nil {
			p.logger.Error(err, "could not put sMLU mode back after a panic",
				"accelerator", accel.ID, "card", card)
		}
	}()

	if err := p.smlu.SetSMLUMode(card, false); err != nil {
		done = true
		p.logger.Error(err, "could not put sMLU mode back", "accelerator", accel.ID, "card", card)
		c.State, c.Detail, c.Reason = device.PreflightStateUnavailable, smluDetailNotRestored, err.Error()
		return c
	}
	done = true
	p.logger.Info("sMLU mode accepted being turned on, and was put back",
		"accelerator", accel.ID, "card", card)
	c.State, c.Detail, c.Reason = device.PreflightStateOK, smluDetailDisabled, ""
	return c
}

// classifySMLUCall 把模式查询映射为分配器使用的结论。卡自身是否声明逻辑切分能力已由 check 先行判断。
func classifySMLUCall(enabled bool, err error) (state device.PreflightState, detail, reason string) {
	switch {
	case err != nil:
		return device.PreflightStateUnavailable, "", err.Error()
	case enabled:
		return device.PreflightStateOK, smluDetailEnabled, ""
	default:
		return device.PreflightStateOK, smluDetailDisabled, ""
	}
}
