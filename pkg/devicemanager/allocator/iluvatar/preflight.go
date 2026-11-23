package iluvatar

import (
	"fmt"
	"time"

	klog "k8s.io/klog/v2"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/deviceplugin"
)

// noDriverNote 说明天数智芯的分配响应从设备路径和配置生成，无需在分配时调用 CoreX 驱动。
const noDriverNote = "the iluvatar allocator reads no driver at allocation time: exclusive/" +
	"shared/visibility hand device visibility to corex's ix-container-runtime, and the sliced " +
	"path's sm/vram limits are hami-core preload envs derived purely from the container's own " +
	"resource request -- no corex driver call is made to serve an allocation"

// NewPreflighter 创建预检器，驱动读取的说明和后续容器注入检查分别报告。
func NewPreflighter(opts device.PreflighterOptions) device.AcceleratorPreflighter {
	return &preflighter{logger: opts.Logger.WithName(Manufacturer)}
}

type preflighter struct {
	logger klog.Logger
}

// PreflightAccelerator 用组说明解释该厂商没有驱动检查项。
func (p *preflighter) PreflightAccelerator(_ device.DevicesGroupList) device.PreflightGroup {
	return device.PreflightGroup{
		Manufacturer: Manufacturer,
		Timestamp:    time.Now(),
		Note:         noDriverNote,
	}
}

// PreflightResponder 复用实际响应器，并重定向共享目录及厂商私有锁目录，使输出的命令能还原宿主机路径。
func (p *preflighter) PreflightResponder(
	mode workercore.DeviceAllocationMode,
) (deviceplugin.ContainerAllocateResponder, func(), error) {
	_, restore, err := deviceplugin.NewPreflightRedirect(&hostVgpuLockPath)
	if err != nil {
		return nil, nil, err
	}

	srv := newServer(p.logger, mode)

	responder, ok := srv.(deviceplugin.ContainerAllocateResponder)
	if !ok {
		restore()
		return nil, nil, fmt.Errorf("iluvatar %s server serves no container allocate response", mode)
	}
	return responder, restore, nil
}
