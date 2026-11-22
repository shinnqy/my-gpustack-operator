package deviceplugin

import (
	"path/filepath"

	"gpustack.ai/gpustack/pkg/utils/osx"
)

// IMPORTANT: 封装软切分预加载库路径（/etc/gpustack/lib）、容器工作目录和设备挂载构造工具。

// 逻辑切片主机路径。设备管理器初始化容器将镜像 /etc/gpustack/lib 树存储在主机上的 OperatorLibDir 中。每个
// 容器的工作目录位于 OperatorPodsDir 下。它们是包变量（而非常量），因此测试可以将其重定向到临时目录。
var (
	OperatorLibDir  = "/var/lib/gpustack/operator/lib"
	OperatorPodsDir = "/var/lib/gpustack/operator/pods"
)

// PodWorkDir 返回切片容器的主机工作目录，<OperatorPodsDir>/<podUID>/c-<container>`。
func PodWorkDir(podUID, containerName string) string {
	return filepath.Join(OperatorPodsDir, podUID, "c-"+containerName)
}

// NewDevice 会创建一个具有给定路径和权限的 DeviceSpec。
func NewDevice(path, permissions string) *DeviceSpec {
	if !osx.Exists(path) {
		return nil
	}
	return &DeviceSpec{
		ContainerPath: path,
		HostPath:      path,
		Permissions:   permissions,
	}
}

// NewRWDevice 会创建一个具有读写权限的 DeviceSpec。
func NewRWDevice(path string) *DeviceSpec {
	return NewDevice(path, "rw")
}

// NewMount 会根据给定的路径和权限创建一个挂载点。
func NewMount(path string, readOnly bool) *Mount {
	if !osx.Exists(path) {
		return nil
	}
	return &Mount{
		ContainerPath: path,
		HostPath:      path,
		ReadOnly:      readOnly,
	}
}

// NewROMount 会创建一个具有只读权限的挂载点。
func NewROMount(path string) *Mount {
	return NewMount(path, true)
}
