//go:build !linux

package preflight

import (
	"gpustack.ai/gpustack/pkg/device"
)

// supportedPreflighterCreators 在非 Linux 系统上为空，和分配器的注册条件一致。
// 真实厂商驱动路径由 Linux 节点执行。
var supportedPreflighterCreators = map[string]func(device.PreflighterOptions) device.AcceleratorPreflighter{}
