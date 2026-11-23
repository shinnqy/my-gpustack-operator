package thead

import (
	"fmt"
	"time"

	klog "k8s.io/klog/v2"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/deviceplugin"
)

// migCapability 是平头哥驱动对硬件分区使用的检查名称。
const migCapability = "mig-partitioning"

// migNotDeclaredReason 用于设备没有声明物理分区 profile 且驱动查询也没有找到分区能力的情况。
const migNotDeclaredReason = "the accelerator declares no physical-slice profile and its driver " +
	"reports no live gpu instance on it, so partitioning is unavailable or off on it"

// migInventoryShortFormat 指出设备未声明分区但驱动仍有活动实例。
const migInventoryShortFormat = "the accelerator declares no physical-slice profile, yet the vendor " +
	"library reports %d live gpu instance(s) on it: the profile inventory taken at detection is " +
	"short, so every partitioned allocation is refused on an accelerator that is in fact partitioned"

// NewPreflighter 创建硬件分区预检器，枚举分配器也会读取的活动实例，不创建测试分区。
func NewPreflighter(opts device.PreflighterOptions) device.AcceleratorPreflighter {
	logger := opts.Logger.WithName(Manufacturer)
	return &preflighter{logger: logger, mig: newMigDriver()}
}

type preflighter struct {
	logger klog.Logger
	mig    migDriver
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

// PreflightResponder 复用分配响应器，查询真实分区状态，在预检过程中屏蔽分区写入并将文件写入临时目录。
func (p *preflighter) PreflightResponder(
	mode workercore.DeviceAllocationMode,
) (deviceplugin.ContainerAllocateResponder, func(), error) {
	_, restore, err := deviceplugin.NewPreflightRedirect()
	if err != nil {
		return nil, nil, err
	}

	srv := newServer(p.logger, mode, &recordingMigDriver{read: p.mig})

	responder, ok := srv.(deviceplugin.ContainerAllocateResponder)
	if !ok {
		restore()
		return nil, nil, fmt.Errorf("thead %s server serves no container allocate response", mode)
	}
	return responder, restore, nil
}

// recordingMigDriver 把查询交给真实驱动，拒绝创建硬件分区。探针没有实际创建实例，销毁请求可按已释放处理。
type recordingMigDriver struct {
	read migDriver
}

var _ migDriver = (*recordingMigDriver)(nil)

func (d *recordingMigDriver) CardState(cardUUID, profile string, computeSlices, memorySlices int32) (migCardState, error) {
	return d.read.CardState(cardUUID, profile, computeSlices, memorySlices)
}

func (d *recordingMigDriver) CreateInstance(
	cardUUID, profile string, computeSlices, memorySlices int32, slot migPlacement,
) (migInstance, error) {

	return migInstance{}, fmt.Errorf("thead preflight: refusing to create a partition through the simulated responder")
}

func (d *recordingMigDriver) DestroyInstance(string, migInstance) error {

	return nil
}

func (d *recordingMigDriver) ListInstances() ([]migLiveInstance, error) {
	return d.read.ListInstances()
}

func (d *recordingMigDriver) InstanceProcesses(cardUUID string, inst migInstance) (int, error) {
	return d.read.InstanceProcesses(cardUUID, inst)
}

func (d *recordingMigDriver) CardInstances(cardUUID string) ([]migInstance, error) {
	return d.read.CardInstances(cardUUID)
}

// check 查询当前卡的活动分区。驱动返回错误时保留错误原因，避免将无法探测的卡误判为空闲。
func (p *preflighter) check(accel *workercore.Accelerator) device.PreflightCheck {
	c := device.PreflightCheck{
		Accelerator: accel.ID,
		Capability:  migCapability,
		Mode:        device.PreflightModeOf(workercore.DeviceAllocationModePartitioned),
	}

	if accel.ID == "" {
		c.State = device.PreflightStateUnavailable
		c.Reason = "the accelerator reports no unique id, so no vendor handle addresses it"
		return c
	}

	declared := len(accel.Status.PhysicalSliced.Profiles) > 0

	instances, err := p.mig.CardInstances(accel.ID)
	c.State, c.Detail, c.Reason = classifyCardInstancesCall(declared, instances, err)
	return c
}

// classifyCardInstancesCall 将驱动返回的实例与设备声明的 profile 对照，
// 对不能解释的库存差异报告 unavailable。
func classifyCardInstancesCall(
	declared bool, instances []migInstance, err error,
) (state device.PreflightState, detail, reason string) {
	switch {
	case err != nil:
		return device.PreflightStateUnavailable, "", err.Error()
	case !declared && len(instances) > 0:
		return device.PreflightStateUnavailable, "", fmt.Sprintf(migInventoryShortFormat, len(instances))
	case !declared:
		return device.PreflightStateNotDeclared, "", migNotDeclaredReason
	}
	return device.PreflightStateOK,
		fmt.Sprintf("the partition subtree is readable and carries %d live gpu instance(s)", len(instances)), ""
}
