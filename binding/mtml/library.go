package mtml

import (
	"fmt"

	"gpustack.ai/gpustack/binding"
)

type MTML struct {
	so  binding.Library
	lib *mtmlLibrary
}

// New 创建 MTML 库对象，准备加载厂商 API。
func New(opts ...binding.LibraryOption) *MTML {
	soPaths := []string{
		"libmtml.so",
	}

	so := binding.NewLibrary(soPaths, opts...)

	return &MTML{so: so}
}

const (
	ERROR_FUNCTION_NOT_FOUND Return = -999
	ERROR_LIBRARY_NOT_FOUND  Return = -1000
)

// Init 初始化 MTML。只在初始化成功时保存库句柄。失败时保留空句柄，后续访问会返回无效句柄错误。
func (l *MTML) Init() Return {
	if err := l.so.Load(); err != nil {
		return ERROR_LIBRARY_NOT_FOUND
	}
	if l.so.Lookup("mtmlLibraryInit") != nil {
		return ERROR_FUNCTION_NOT_FOUND
	}

	// 先用局部变量接收 C 句柄，再写回对象。直接把字段地址交给 C，会连带传入同一 Go 对象中的接口字段，
	// 触发 cgo 指针检查。
	var lib *mtmlLibrary
	ret := mtmlLibraryInit(&lib)
	if ret.IsSuccess() {
		l.lib = lib
	}
	return ret
}

const (
	MTML_SYSTEM_DRIVER_VERSION_BUFFER_SIZE = 80
)

// SystemGetDriverVersion 读取设备驱动版本。
func (l *MTML) SystemGetDriverVersion() (string, Return) {
	if l.so.Lookup("mtmlSystemGetDriverVersion") != nil {
		return "", ERROR_FUNCTION_NOT_FOUND
	}

	var sys *mtmlSystem
	if ret := mtmlLibraryInitSystem(l.lib, &sys); !ret.IsSuccess() {
		return "", ret
	}
	defer func() {
		_ = mtmlLibraryFreeSystem(sys)
	}()

	version := make([]byte, MTML_SYSTEM_DRIVER_VERSION_BUFFER_SIZE)
	ret := mtmlSystemGetDriverVersion(sys, &version[0], MTML_SYSTEM_DRIVER_VERSION_BUFFER_SIZE)
	return string(version[:clen(version)]), ret
}

// IsSuccess 判断调用是否成功。
func (r Return) IsSuccess() bool {
	return r == SUCCESS
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
	case ERROR_DRIVER_NOT_LOADED:
		return "DRIVER_NOT_LOADED"
	case ERROR_DRIVER_FAILURE:
		return "DRIVER_FAILURE"
	case ERROR_INVALID_ARGUMENT:
		return "INVALID_ARGUMENT"
	case ERROR_NOT_SUPPORTED:
		return "NOT_SUPPORTED"
	case ERROR_NO_PERMISSION:
		return "NO_PERMISSION"
	case ERROR_INSUFFICIENT_SIZE:
		return "INSUFFICIENT_SIZE"
	case ERROR_NOT_FOUND:
		return "NOT_FOUND"
	case ERROR_INSUFFICIENT_MEMORY:
		return "INSUFFICIENT_MEMORY"
	case ERROR_DRIVER_TOO_OLD:
		return "DRIVER_TOO_OLD"
	case ERROR_DRIVER_TOO_NEW:
		return "DRIVER_TOO_NEW"
	case ERROR_TIMEOUT:
		return "TIMEOUT"
	case ERROR_RESOURCE_IS_BUSY:
		return "RESOURCE_IS_BUSY"
	case ERROR_UNKNOWN:
		return "UNKNOWN"
	case ERROR_FUNCTION_NOT_FOUND:
		return "FUNCTION_NOT_FOUND"
	case ERROR_LIBRARY_NOT_FOUND:
		return "LIBRARY_NOT_FOUND"
	default:
		return fmt.Sprintf("unknown return value: %d", r)
	}
}
