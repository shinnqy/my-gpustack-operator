package preflight

import (
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/devicemanager/allocator/amd"
	"gpustack.ai/gpustack/pkg/devicemanager/allocator/ascend"
	"gpustack.ai/gpustack/pkg/devicemanager/allocator/cambricon"
	"gpustack.ai/gpustack/pkg/devicemanager/allocator/hygon"
	"gpustack.ai/gpustack/pkg/devicemanager/allocator/iluvatar"
	"gpustack.ai/gpustack/pkg/devicemanager/allocator/metax"
	"gpustack.ai/gpustack/pkg/devicemanager/allocator/mthreads"
	"gpustack.ai/gpustack/pkg/devicemanager/allocator/nvidia"
	"gpustack.ai/gpustack/pkg/devicemanager/allocator/thead"
)

// supportedPreflighterCreators 按厂商登记 Linux 预检器。需要读取驱动的厂商检查分配前置条件，
// 其余厂商仍可提供自己的说明与容器分配响应。
var supportedPreflighterCreators = map[string]func(device.PreflighterOptions) device.AcceleratorPreflighter{
	amd.Manufacturer:       amd.NewPreflighter,
	ascend.Manufacturer:    ascend.NewPreflighter,
	cambricon.Manufacturer: cambricon.NewPreflighter,
	hygon.Manufacturer:     hygon.NewPreflighter,
	iluvatar.Manufacturer:  iluvatar.NewPreflighter,
	metax.Manufacturer:     metax.NewPreflighter,
	mthreads.Manufacturer:  mthreads.NewPreflighter,
	nvidia.Manufacturer:    nvidia.NewPreflighter,
	thead.Manufacturer:     thead.NewPreflighter,
}
