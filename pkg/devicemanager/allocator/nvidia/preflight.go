package nvidia

import (
	"fmt"
	"time"

	klog "k8s.io/klog/v2"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/deviceplugin"
)

// migCapability 是 NVML 硬件分区检查的名称。
const migCapability = "mig-partitioning"

// migNotDeclaredReason 仅在卡未上报分区 profile 且 NVML 的读取也支持这一判断时使用。
const migNotDeclaredReason = "the accelerator declares no physical-slice profile and its driver " +
	"reports no live gpu instance on it, so mig is unavailable or off on it"

// migInventoryShortFormat 报告库存未声明 profile 而驱动仍有活动分区的矛盾，提示重新探测。
const migInventoryShortFormat = "the accelerator declares no physical-slice profile, yet nvml " +
	"reports %d live gpu instance(s) on it: the profile inventory taken at detection is short, so " +
	"every partitioned allocation is refused on an accelerator that is in fact partitioned"

// NewPreflighter 复用分配器使用的 NVML 路径枚举 GPU 实例，预检不会创建分区。
func NewPreflighter(opts device.PreflighterOptions) device.AcceleratorPreflighter {
	logger := opts.Logger.WithName(Manufacturer)

	// 使用与 New 相同的可见性解析器，整卡等模式的环境变量或 CDI 选择与该节点生产分配一致。
	injection, err := deviceplugin.NewInjectionResolver(injectionConfig)
	if err != nil {
		logger.Error(err, "keeping the default device-injection strategy")
		injection = deviceplugin.DefaultInjectionResolver(injectionConfig)
	}

	return &preflighter{
		logger:    logger,
		mig:       newMigDriver(),
		injection: injection,
	}
}

type preflighter struct {
	logger    klog.Logger
	mig       migDriver
	injection *deviceplugin.InjectionResolver
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

// PreflightResponder 复用 NVIDIA 的生产响应器和可见性解析器，并将切片的私有锁目录等写入重定向到临时目录。
func (p *preflighter) PreflightResponder(
	mode workercore.DeviceAllocationMode,
) (deviceplugin.ContainerAllocateResponder, func(), error) {
	_, restore, err := deviceplugin.NewPreflightRedirect(&hostVgpuLockPath)
	if err != nil {
		return nil, nil, err
	}

	srv := newServer(p.logger, mode, nil, p.injection)

	responder, ok := srv.(deviceplugin.ContainerAllocateResponder)
	if !ok {
		restore()
		return nil, nil, fmt.Errorf("nvidia %s server serves no container allocate response", mode)
	}
	return responder, restore, nil
}

// check 只读卡的 MIG 实例。通过核对设备声明和 NVML 实测，既能识别真正不支持 MIG 的卡，也能发现库存遗漏了活动分区。
func (p *preflighter) check(accel *workercore.Accelerator) device.PreflightCheck {
	c := device.PreflightCheck{
		Accelerator: accel.ID,
		Capability:  migCapability,
		Mode:        device.PreflightModeOf(workercore.DeviceAllocationModePartitioned),
	}

	if accel.ID == "" {
		c.State = device.PreflightStateUnavailable
		c.Reason = "the accelerator reports no unique id, so no nvml handle addresses it"
		return c
	}

	// 分配器也用 profile 列表判断能否创建分区。单看列表为空或当前没有活动实例都不足以判断能力，所以两者要一起检查。
	declared := len(accel.Status.PhysicalSliced.Profiles) > 0

	instances, err := p.mig.CardInstances(accel.ID)
	c.State, c.Detail, c.Reason = classifyMigOutcome(declared, len(instances), err)
	return c
}

// classifyMigOutcome 对照声明的 profile、活动实例数和 NVML 错误。
// 库存与驱动矛盾时返回 unavailable，使调用方知道需要重新探测。
func classifyMigOutcome(declared bool, liveInstances int, err error) (state device.PreflightState, detail, reason string) {
	switch {
	case err != nil:
		return device.PreflightStateUnavailable, "", err.Error()
	case !declared && liveInstances > 0:
		return device.PreflightStateUnavailable, "", fmt.Sprintf(migInventoryShortFormat, liveInstances)
	case !declared:
		return device.PreflightStateNotDeclared, "", migNotDeclaredReason
	default:
		return device.PreflightStateOK,
			fmt.Sprintf("the mig subtree is readable and carries %d live gpu instance(s)", liveInstances), ""
	}
}
