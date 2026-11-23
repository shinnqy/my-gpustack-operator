package ascend

import (
	"errors"
	"fmt"
	"path/filepath"
	"time"

	klog "k8s.io/klog/v2"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/device"
	productascend "gpustack.ai/gpustack/pkg/devicemanager/product/ascend"
	"gpustack.ai/gpustack/pkg/deviceplugin"
)

// shareCapability 沿用 DCMI 对容器共享开关的名称。
const shareCapability = "container-share"

const (
	shareDetailEnabled  = "container-share is enabled"
	shareDetailDisabled = "container-share is disabled, and this driver accepted being asked to " +
		"turn it on, so the allocator will manage to when a second container lands on this " +
		"accelerator; it was put back the way it was found"
	shareDetailNotAsked = "container-share is disabled and was left that way: asking it on to see whether " +
		"this driver accepts is a write, and this is a dry run"

	shareDetailNotRestored = "container-share is disabled, and this driver accepted being asked to " +
		"turn it on -- but putting it back failed, so it is ON now and this accelerator will admit " +
		"a second container until it is turned off"
)

// a5PreconditionModes 列出依赖 A5 节点前置条件的各分配模式。
// 同一条运行时或 rank table 检查需要出现在每个受影响的模式下。
var a5PreconditionModes = []workercore.DeviceAllocationMode{
	workercore.DeviceAllocationModeExclusive,
	workercore.DeviceAllocationModeShared,
	workercore.DeviceAllocationModeSliced,
	workercore.DeviceAllocationModeVisibility,
}

// NewPreflighter 复用 Ascend 分配器使用的共享开关和产品型号查询，
// 并为 A5 准备运行时和 rank table 的检查路径。
func NewPreflighter(opts device.PreflighterOptions) device.AcceleratorPreflighter {
	logger := opts.Logger.WithName(Manufacturer)
	return &preflighter{
		logger:      logger,
		share:       newShareDriver(logger),
		product:     productascend.NewResolver(newProductDriver(logger)),
		installInfo: dockerRuntimeInstallInfo,
		// 安装信息挂载在容器同名路径。rank table 位于宿主机的 /etc，必须从宿主机根目录读取。
		rootInfo: hostRankTablePath(opts.HostRoot),
		dryRun:   opts.DryRun,
	}
}

// hostRankTablePath 只在宿主机根目录可用时拼接 rank table 的路径。根目录为空时保留未知状态，
// 避免落到容器自己的 /etc。
func hostRankTablePath(hostRoot string) string {
	if hostRoot == "" {
		return ""
	}
	return filepath.Join(hostRoot, hcclRootInfoPath)
}

type preflighter struct {
	logger klog.Logger
	share  shareDriver
	// product 复用生产产品型号查询器。定位拓扑文件是只读操作。
	product *productascend.Resolver
	// installInfo 指向厂商安装程序写入版本的文件，供 A5 预检读取。
	installInfo string
	// rootInfo 指向 A5 容器运行时使用的宿主机 rank table。
	rootInfo string
	dryRun   bool
}

func (p *preflighter) PreflightAccelerator(groups device.DevicesGroupList) device.PreflightGroup {
	grp := device.PreflightGroup{
		Manufacturer: Manufacturer,
		Timestamp:    time.Now(),
	}

	for i := range groups {
		// 这两项是 A5 的节点属性。检测到 A5 后只读一次，再把结果分给该节点的各张卡。
		var runtime, rankTable device.PreflightCheck
		serves950 := groups[i].Family == family950
		if serves950 {
			runtime = checkDockerRuntime(p.installInfo)
			rankTable = checkRankTable(p.rootInfo)
		}

		accels := groups[i].Accelerators
		for j := range accels {
			grp.Checks = append(grp.Checks, p.check(&accels[j]))

			// 把节点检查写入每张卡受影响的各个模式，按卡号或分配模式过滤报告时也能看到原因。
			if serves950 {
				for _, mode := range a5PreconditionModes {
					runtime.Accelerator, runtime.Mode = accels[j].ID, device.PreflightModeOf(mode)
					rankTable.Accelerator, rankTable.Mode = accels[j].ID, device.PreflightModeOf(mode)
					grp.Checks = append(grp.Checks, runtime, rankTable)
				}
			}
		}
	}
	return grp
}

// PreflightResponder 使用分配器原有的响应器生成注入文件。设备状态读取真实驱动，生成响应时屏蔽共享开关写入，
// 并把文件重定向到临时目录。
func (p *preflighter) PreflightResponder(
	mode workercore.DeviceAllocationMode,
) (deviceplugin.ContainerAllocateResponder, func(), error) {
	_, restore, err := deviceplugin.NewPreflightRedirect()
	if err != nil {
		return nil, nil, err
	}

	srv := newServer(p.logger, mode, &recordingShareDriver{read: p.share}, p.product)

	responder, ok := srv.(deviceplugin.ContainerAllocateResponder)
	if !ok {
		restore()
		return nil, nil, fmt.Errorf("ascend %s server serves no container allocate response", mode)
	}
	return responder, restore, nil
}

// recordingShareDriver 用真实驱动读取共享开关，只在生成预检响应时屏蔽写入。真实开关的启用和恢复由单独的能力检查负责。
type recordingShareDriver struct {
	read shareDriver
}

func (d *recordingShareDriver) GetShareEnabled(cardID, deviceID int32) (bool, error) {
	return d.read.GetShareEnabled(cardID, deviceID)
}

func (d *recordingShareDriver) SetShareEnabled(int32, int32, bool) error {

	return nil
}

// check 先读取共享开关。已知关闭时才能尝试打开后恢复，未声明、读取失败和恢复失败分别记录。恢复失败会使节点预检失败。
func (p *preflighter) check(accel *workercore.Accelerator) device.PreflightCheck {
	c := device.PreflightCheck{Accelerator: accel.ID, Capability: shareCapability, Mode: device.PreflightModeOf(workercore.DeviceAllocationModeSliced)}

	// PhysicalIndexes 依次包含 DCMI 的物理 ID、卡 ID 和卡内设备 ID。
	if len(accel.PhysicalIndexes) < 3 {
		c.State = device.PreflightStateUnavailable
		c.Reason = "the accelerator carries no dcmi card/device index"
		return c
	}
	cardID, deviceID := int32(accel.PhysicalIndexes[1]), int32(accel.PhysicalIndexes[2])

	enabled, readErr := p.share.GetShareEnabled(cardID, deviceID)
	c.State, c.Detail, c.Reason = classifyShareCall(enabled, readErr)

	switch {
	case c.State == device.PreflightStateNotDeclared:
		// 这一代设备没有共享开关，跳过写入。
		return c
	case errors.Is(readErr, errShareUnsupported):
		// 共享接口无法查询时，分配器也会拒绝请求，此处直接返回。
		return c
	case readErr != nil:
		// 读取失败时开关原始值未知，跳过写入才能避免错误地关闭原本已开启的共享。
		return c
	case enabled:
		return c
	case p.dryRun:
		// dry-run 只报告已读到的状态。切换后恢复仍有失败的可能，因此这里不切换。
		c.State, c.Detail, c.Reason = device.PreflightStateOK, shareDetailNotAsked, ""
		return c
	}

	if err := p.share.SetShareEnabled(cardID, deviceID, true); err != nil {
		c.State, c.Detail, c.Reason = classifyShareCall(false, err)
		return c
	}

	// 驱动在切换后触发 panic 时也要尽力恢复原值。只有先前确认为关闭，才在这里恢复为关闭。
	done := false
	defer func() {
		if done {
			return
		}
		if err := p.share.SetShareEnabled(cardID, deviceID, false); err != nil {
			p.logger.Error(err, "could not put container-share back after a panic",
				"accelerator", accel.ID, "card", cardID, "device", deviceID)
		}
	}()

	if err := p.share.SetShareEnabled(cardID, deviceID, false); err != nil {
		done = true
		p.logger.Error(err, "could not put container-share back",
			"accelerator", accel.ID, "card", cardID, "device", deviceID)
		c.State, c.Detail, c.Reason = device.PreflightStateUnavailable, shareDetailNotRestored, err.Error()
		return c
	}
	done = true
	p.logger.Info("container-share accepted being turned on, and was put back",
		"accelerator", accel.ID, "card", cardID, "device", deviceID)
	c.State, c.Detail, c.Reason = device.PreflightStateOK, shareDetailDisabled, ""
	return c
}

// classifyShareCall 与分配器使用同样的三种结论。先判断这代设备未声明能力，再处理其他驱动错误，以免把未声明错报成无法查询。
func classifyShareCall(enabled bool, err error) (state device.PreflightState, detail, reason string) {
	switch {
	case errors.Is(err, errShareNotDeclared):
		return device.PreflightStateNotDeclared, "", err.Error()
	case err != nil:
		return device.PreflightStateUnavailable, "", err.Error()
	case enabled:
		return device.PreflightStateOK, shareDetailEnabled, ""
	default:
		return device.PreflightStateOK, shareDetailDisabled, ""
	}
}
