//go:build !linux

package allocator

import (
	"gpustack.ai/gpustack/pkg/device"
)

var supportedAllocatorCreators = map[string]func(device.AllocatorOptions) device.Allocator{}

// rdmaAllocatorCreator 未被设置：RDMA 服务器绑定在 kubelet 提供的 device-plugin 目录上，该目录属于 Linux
// 系统，因此非 Linux 设备管理器不会失败，而是不会尝试访问不存在的目录。
var rdmaAllocatorCreator func(device.AllocatorOptions) device.Allocator
