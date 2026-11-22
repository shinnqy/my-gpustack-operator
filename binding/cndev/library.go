package cndev

import (
	"fmt"
	"os"
	"path/filepath"

	"gpustack.ai/gpustack/binding"
)

const (
	ERROR_FUNCTION_NOT_FOUND = -99998
	ERROR_LIBRARY_NOT_FOUND  = -99999
)

type CNDev struct {
	so binding.Library
}

// New 创建 CNDev 库对象，准备从系统加载管理库和解析其 API。
func New(opts ...binding.LibraryOption) *CNDev {
	soPaths := []string{
		"libcndev.so",
	}
	{
		home := os.Getenv("NEUWARE_HOME")
		if home == "" {
			home = "/usr/local/neuware"
		}
		if s, err := os.Stat(home); err == nil && s.IsDir() {
			soPaths = append(soPaths,
				filepath.Join(home, "lib64", "libcndev.so"),
				filepath.Join(home, "lib", "libcndev.so"),
			)
		}
	}

	so := binding.NewLibrary(soPaths, opts...)
	return &CNDev{so: so}
}

func (l *CNDev) Init() Return {
	if err := l.so.Load(); err != nil {
		return ERROR_LIBRARY_NOT_FOUND
	}
	if l.so.Lookup("cndevInit") != nil {
		return ERROR_FUNCTION_NOT_FOUND
	}
	return cndevInit(0)
}

// IsSuccess 判断调用是否成功。
func (r Return) IsSuccess() bool {
	return r == SUCCESS
}

// IsAPIUnavailable 用于指示是否因为加载的库或安装的驱动程序无法提供该功能，导致调用无法完成：共享对象不存
// 在，符号缺失，或者驱动程序过于老旧，无法支持该 API。
//
// 对于每一个本身就是关于该设备或请求的答案的代码来说，这都是错误的。值得注意的是：
//   - ERROR_NOT_SUPPORTED 表示驱动程序指出该设备缺少所查询的功能——这涉及硬件情况，而非 API 的存在问题。
//   - ERROR_UNSUPPORTED_API_VERSION 是一个结构体版本协商失败的错误。头文件中的描述为：“该 API 的版本不受
//     支持或无效。请使用正确的 API 版本号。”——入口点已提供，调用者可以重试该驱动程序已知的版本。
func (r Return) IsAPIUnavailable() bool {
	switch r {
	case ERROR_LIBRARY_NOT_FOUND, ERROR_FUNCTION_NOT_FOUND,
		ERROR_NO_DRIVER, ERROR_LOW_DRIVER_VERSION:
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
	case ERROR_NO_DRIVER:
		return "NO_DRIVER"
	case ERROR_LOW_DRIVER_VERSION:
		return "LOW_DRIVER_VERSION"
	case ERROR_UNSUPPORTED_API_VERSION:
		return "UNSUPPORTED_API_VERSION"
	case ERROR_UNINITIALIZED:
		return "UNINITIALIZED"
	case ERROR_INVALID_ARGUMENT:
		return "INVALID_ARGUMENT"
	case ERROR_INVALID_DEVICE_ID:
		return "INVALID_DEVICE_ID"
	case ERROR_UNKNOWN:
		return "UNKNOWN"
	case ERROR_MALLOC:
		return "MALLOC"
	case ERROR_INSUFFICIENT_SPACE:
		return "INSUFFICIENT_SPACE"
	case ERROR_NOT_SUPPORTED:
		return "NOT_SUPPORTED"
	case ERROR_INVALID_LINK:
		return "INVALID_LINK"
	case ERROR_NO_DEVICES:
		return "NO_DEVICES"
	case ERROR_NO_PERMISSION:
		return "NO_PERMISSION"
	case ERROR_NOT_FOUND:
		return "NOT_FOUND"
	case ERROR_IN_USE:
		return "IN_USE"
	case ERROR_DUPLICATE:
		return "DUPLICATE"
	case ERROR_TIMEOUT:
		return "TIMEOUT"
	case ERROR_IN_PROBLEM:
		return "IN_PROBLEM"
	case ERROR_FUNCTION_NOT_FOUND:
		return "FUNCTION_NOT_FOUND"
	case ERROR_LIBRARY_NOT_FOUND:
		return "LIBRARY_NOT_FOUND"
	default:
		return fmt.Sprintf("unknown return value: %d", r)
	}
}
