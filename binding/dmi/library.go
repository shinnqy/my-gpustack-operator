// SPDX-FileCopyrightText: 2026 GPUStack, Inc.
// SPDX-License-Identifier: Apache-2.0

package dmi

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"

	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/binding"
)

// DMI 管理海光 DCU 的 GPU 实例和计算实例。厂商沿用了 NVML 的符号名，但 C 结构体布局不同。
// C 包装层用 RTLD_LOCAL 打开指定共享库，再通过专用句柄解析各函数，避免错误调用同一进程中的 NVIDIA 库。
// binding.Library 在这里仅用于选择路径。
type DMI struct {
	so binding.Library
}

// New 配置 DMI 库的搜索位置。hyhal 常不在系统动态链接器路径内，因此列出绝对路径，并优先尝试带版本号的库名。
func New(opts ...binding.LibraryOption) *DMI {
	soPaths := []string{
		"libhydmi_mig.so.1",
		"libhydmi_mig.so",
	}
	for _, dir := range []string{
		"/opt/hyhal/lib",
		"/opt/dtk/lib",
	} {
		if s, err := os.Stat(dir); err == nil && s.IsDir() {
			soPaths = append(soPaths,
				filepath.Join(dir, "libhydmi_mig.so.1"),
				filepath.Join(dir, "libhydmi_mig.so"),
			)
		}
	}

	so := binding.NewLibrary(soPaths, opts...)

	return &DMI{so: so}
}

// Init 加载厂商库并解析函数。对同一已打开的库可重复调用，探测和分配组件可以分别初始化。
func (l *DMI) Init(logger klog.Logger) Return {
	ret, errStr := l.initLocked()
	if !ret.IsSuccess() {
		logger.Error(ret, "failed to initialize the hygon mig library",
			"path", l.so.Path(), "reason", errStr)
	}
	return ret
}

// initLocked 在同一操作系统线程内完成加载和错误读取。C 包装层使用线程局部缓冲区，
// goroutine 在两次 cgo 调用之间换线程会丢失诊断消息。
func (l *DMI) initLocked() (Return, string) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	ret := dmi_mig_init(l.so.Path())
	if ret.IsSuccess() {
		return ret, ""
	}
	// 在解锁前复制错误字符串。生成的访问器借用 C 缓冲区，释放线程后下一次调用可能立即重写原数据。
	return ret, strings.Clone(dmi_mig_last_error())
}

// Path 返回当前实例选中的共享库路径，供排查驱动问题。
func (l *DMI) Path() string {
	return l.so.Path()
}

// GetSystemMigMode 读取整个节点当前和待生效的多实例模式。海光驱动按节点而非按卡切换，节点上的卡共享此项设置。
func (l *DMI) GetSystemMigMode() (current, pending uint32, ret Return) {
	ret = nvmlGetSystemMigMode(&current, &pending)
	return current, pending, ret
}

// GetDeviceCount 报告了库中看到的物理 `DCU` 数量。
func (l *DMI) GetDeviceCount() (uint32, Return) {
	var count uint32
	ret := nvmlDeviceGetCount(&count)
	return count, ret
}

// GetDeviceHandleByIndex 返回索引对应物理 DCU 的句柄。
func (l *DMI) GetDeviceHandleByIndex(index uint32) (Device, Return) {
	var handle dmiDevice
	ret := nvmlDeviceGetHandleByIndex(index, &handle)
	return Device{handle: handle, lib: l}, ret
}

// GetDeviceHandleByPciBusId 用完整 PCI 地址把 RSMI 枚举的物理 DCU 映射到 DMI 句柄。地址必须含域，
// 如 0000:09:00.0。厂商的 PCI 读取和 UUID 查询无法替代这次映射。找不到该地址时返回 NOT_FOUND。
func (l *DMI) GetDeviceHandleByPciBusId(pciBusID string) (Device, Return) {
	var handle dmiDevice
	ret := nvmlDeviceGetHandleByPciBusId(pciBusID, &handle)
	return Device{handle: handle, lib: l}, ret
}

// IsSuccess 判断本次调用是否成功。
func (r Return) IsSuccess() bool {
	return r == SUCCESS
}

// IsAPIUnavailable 表示当前库没有对应入口。驱动对某张卡返回“不支持”或“找不到”仍是有效答复。
func (r Return) IsAPIUnavailable() bool {
	switch r {
	case ERROR_LIBRARY_NOT_FOUND, ERROR_FUNCTION_NOT_FOUND,
		ERROR_DRIVER_NOT_LOADED, ERROR_LIB_RM_VERSION_MISMATCH:
		return true
	}
	return false
}

// ReportsAbsent 判断驱动是否明确报告某个 ID 或索引为空。多实例规格编号和全节点共享的实例索引允许有空位，
// 枚举时要把这些空位和查询故障分别处理。
func (r Return) ReportsAbsent() bool {
	switch r {
	case ERROR_NOT_SUPPORTED, ERROR_NOT_FOUND, ERROR_INVALID_ARGUMENT:
		return true
	}
	return false
}

// String 返回厂商返回码的文字说明。
func (r Return) String() string {
	return r.Error()
}

// Error 将厂商返回码作为 Go 错误呈现。
func (r Return) Error() string {
	switch r {
	case SUCCESS:
		return "SUCCESS"
	case ERROR_UNINITIALIZED:
		return "UNINITIALIZED"
	case ERROR_INVALID_ARGUMENT:
		return "INVALID_ARGUMENT"
	case ERROR_NOT_SUPPORTED:
		return "NOT_SUPPORTED"
	case ERROR_NO_PERMISSION:
		return "NO_PERMISSION"
	case ERROR_ALREADY_INITIALIZED:
		return "ALREADY_INITIALIZED"
	case ERROR_NOT_FOUND:
		return "NOT_FOUND"
	case ERROR_INSUFFICIENT_SIZE:
		return "INSUFFICIENT_SIZE"
	case ERROR_INSUFFICIENT_POWER:
		return "INSUFFICIENT_POWER"
	case ERROR_DRIVER_NOT_LOADED:
		return "DRIVER_NOT_LOADED"
	case ERROR_TIMEOUT:
		return "TIMEOUT"
	case ERROR_IRQ_ISSUE:
		return "IRQ_ISSUE"
	case ERROR_LIBRARY_NOT_FOUND:
		return "LIBRARY_NOT_FOUND"
	case ERROR_FUNCTION_NOT_FOUND:
		return "FUNCTION_NOT_FOUND"
	case ERROR_CORRUPTED_INFOROM:
		return "CORRUPTED_INFOROM"
	case ERROR_GPU_IS_LOST:
		return "GPU_IS_LOST"
	case ERROR_RESET_REQUIRED:
		return "RESET_REQUIRED"
	case ERROR_OPERATING_SYSTEM:
		return "OPERATING_SYSTEM"
	case ERROR_LIB_RM_VERSION_MISMATCH:
		return "LIB_RM_VERSION_MISMATCH"
	case ERROR_IN_USE:
		return "IN_USE"
	case ERROR_MEMORY:
		return "MEMORY"
	case ERROR_NO_DATA:
		return "NO_DATA"
	case ERROR_VGPU_ECC_NOT_SUPPORTED:
		return "VGPU_ECC_NOT_SUPPORTED"
	case ERROR_INSUFFICIENT_RESOURCES:
		return "INSUFFICIENT_RESOURCES"
	case ERROR_FREQ_NOT_SUPPORTED:
		return "FREQ_NOT_SUPPORTED"
	case ERROR_ARGUMENT_VERSION_MISMATCH:
		return "ARGUMENT_VERSION_MISMATCH"
	case ERROR_DEPRECATED:
		return "DEPRECATED"
	case ERROR_NOT_READY:
		return "NOT_READY"
	case ERROR_UNKNOWN:
		return "UNKNOWN"
	}
	return "UNKNOWN_RETURN_CODE"
}
