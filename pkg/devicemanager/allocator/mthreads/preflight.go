package mthreads

import (
	"fmt"
	"time"

	klog "k8s.io/klog/v2"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/deviceplugin"
)

// noDriverNote 说明摩尔线程的分配过程从路径和环境变量生成注入配置，不依赖实时查询驱动。
const noDriverNote = "the mthreads allocator reads no driver at allocation time: every mode's " +
	"response is env vars only -- mthreads_visible_devices plus, for a sliced container, a qos " +
	"memory cap and compute weight derived purely from the container's own resource request -- " +
	"the host sgpu kmod and mthreads container runtime enforce them, and no driver call is made " +
	"to serve an allocation"

// NewPreflighter 创建摩尔线程预检器。后续注入检查使用真实分配器生成的响应。
func NewPreflighter(opts device.PreflighterOptions) device.AcceleratorPreflighter {
	return &preflighter{logger: opts.Logger.WithName(Manufacturer)}
}

type preflighter struct {
	logger klog.Logger
}

// PreflightAccelerator 在结果中解释为何没有驱动检查项。
func (p *preflighter) PreflightAccelerator(_ device.DevicesGroupList) device.PreflightGroup {
	return device.PreflightGroup{
		Manufacturer: Manufacturer,
		Timestamp:    time.Now(),
		Note:         noDriverNote,
	}
}

// PreflightResponder 使用生产响应器，并为这次单独运行重定向文件写入路径。
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
		return nil, nil, fmt.Errorf("mthreads %s server serves no container allocate response", mode)
	}
	return responder, restore, nil
}
