package ixml

import (
	"fmt"
	"os"
	"path/filepath"

	"gpustack.ai/gpustack/binding"
)

type IXML struct {
	so binding.Library
}

// New 创建 IXML 库对象，准备加载厂商 API。
func New(opts ...binding.LibraryOption) *IXML {
	soPaths := []string{
		"libixml.so",
	}
	{
		home := os.Getenv("COREX_HOME")
		if home == "" {
			home = "/usr/local/corex"
		}
		if s, err := os.Stat(home); err == nil && s.IsDir() {
			soPaths = append(soPaths,
				filepath.Join(home, "lib64", "libixml.so"),
				filepath.Join(home, "lib", "libixml.so"),
			)
		}
	}

	so := binding.NewLibrary(soPaths, opts...)

	return &IXML{so: so}
}

// Init 初始化 IXML 管理库。
func (l *IXML) Init() Return {
	if err := l.so.Load(); err != nil {
		return ERROR_LIBRARY_NOT_FOUND
	}
	if l.so.Lookup("nvmlInit_v2") == nil {
		return ERROR_FUNCTION_NOT_FOUND
	}

	return nvmlInit_v2()
}

// SystemGetDriverVersion 读取设备驱动版本。
func (l *IXML) SystemGetDriverVersion() (string, Return) {
	if l.so.Lookup("nvmlSystemGetDriverVersion") != nil {
		return "", ERROR_FUNCTION_NOT_FOUND
	}

	version := make([]byte, SYSTEM_DRIVER_VERSION_BUFFER_SIZE)
	ret := nvmlSystemGetDriverVersion(&version[0], SYSTEM_DRIVER_VERSION_BUFFER_SIZE)
	return string(version[:clen(version)]), ret
}

// SystemGetCudaDriverVersion 读取 CoreX 驱动版本。
func (l *IXML) SystemGetCudaDriverVersion() (int32, Return) {
	var version int32
	if l.so.Lookup("nvmlSystemGetCudaDriverVersion") != nil {
		ret := nvmlSystemGetCudaDriverVersion(&version)
		return version, ret
	}
	if l.so.Lookup("nvmlSystemGetCudaDriverVersion_v2") != nil {
		ret := nvmlSystemGetCudaDriverVersion_v2(&version)
		return version, ret
	}
	return 0, ERROR_FUNCTION_NOT_FOUND
}

// IsSuccess 判断调用是否成功。
func (r Return) IsSuccess() bool {
	return r == SUCCESS
}

// IsAPIUnavailable 用于指示是否因为加载的库或安装的驱动程序无法提供该功能，导致调用无法完成：共享对象不存
// 在，符号缺失，或者库与驱动程序版本不兼容。
//
// 对于调用者可以不同地操作的每个代码——另一种结构版本、另一种设备、不同的权限——以及作为驱动程序对设备的自
// 定义响应的每个代码，尤其是 ERROR_NOT_SUPPORTED，都是错误的。
func (r Return) IsAPIUnavailable() bool {
	switch r {
	case ERROR_LIBRARY_NOT_FOUND, ERROR_FUNCTION_NOT_FOUND,
		ERROR_DRIVER_NOT_LOADED, ERROR_LIB_RM_VERSION_MISMATCH:
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
	return defaultErrorStringFunc(r)
}

func defaultErrorStringFunc(r Return) string {
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
	case ERROR_UNKNOWN:
		return "UNKNOWN"
	default:
		return fmt.Sprintf("unknown return value: %d", r)
	}
}
