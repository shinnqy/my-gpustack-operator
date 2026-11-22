package system

import (
	"path/filepath"
	"time"

	"gpustack.ai/gpustack/pkg/utils/osx"
)

// DataDir 是暴露数据的路径，
// 从宿主机挂载，用于存储 gpustack 的数据。
var _DataDir = osx.Getenv("GPUSTACK_DATA_DIR", "/var/lib/gpustack")

// SubDataDir 返回 DataDir 子目录的路径，
// 从宿主机挂载，用于存储 gpustack 的数据。
func SubDataDir(sub string, others ...string) string {
	if IsRunningInContainer() {
		return filepath.Join(_DataDir, "operator", sub, filepath.Join(others...))
	}
	// NB(thxCode): 便于开发调试。
	return osx.SubTempDir(filepath.Join(time.Now().Format(time.DateOnly), _DataDir, "operator", sub, filepath.Join(others...)))
}

// ConfDir 是用于访问 gpustack 的元数据、
// 内部配置及其他配置文件的路径。
var _ConfDir = osx.Getenv("GPUSTACK_CONF_DIR", "/etc/gpustack")

// SubConfDir 返回 ConfDir 子目录的路径，
// 用于访问 gpustack 的内部配置及其他配置文件。
func SubConfDir(sub string, others ...string) string {
	if IsRunningInContainer() {
		return filepath.Join(_ConfDir, sub, filepath.Join(others...))
	}
	// NB(thxCode): 便于开发调试。
	return osx.SubTempDir(filepath.Join(time.Now().Format(time.DateOnly), _ConfDir, sub, filepath.Join(others...)))
}

// IsRunningInContainer 在进程运行于容器内部时返回 true。
func IsRunningInContainer() bool {
	return osx.Getenv("_RUNNING_INSIDE_CONTAINER_", "false") == "true"
}

// IsRunningAsPrivileged 在进程以特权模式运行时返回 true。
func IsRunningAsPrivileged() bool {
	return osx.ExistsDevice("/dev/kmsg")
}
