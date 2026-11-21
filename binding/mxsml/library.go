package mxsml

import (
	"fmt"
	"os"
	"path/filepath"

	"gpustack.ai/gpustack/binding"
)

type MXSML struct {
	so binding.Library
}

// New 创建 MXSML 库对象，准备加载厂商 API。
func New(opts ...binding.LibraryOption) *MXSML {
	soPaths := []string{
		"libmxsml.so",
		"/opt/mxdriver/lib/libmxsml.so",
		"/opt/mxn100/lib/libmxsml.so",
	}
	{
		home := os.Getenv("MACA_HOME")
		if home == "" {
			home = "/opt/maca"
		}
		if s, err := os.Stat(home); err == nil && s.IsDir() {
			soPaths = append(soPaths,
				filepath.Join(home, "lib64", "libmxsml.so"),
				filepath.Join(home, "lib", "libmxsml.so"),
			)
		}
	}

	so := binding.NewLibrary(soPaths, opts...)

	return &MXSML{so: so}
}

const (
	FunctionNotFound Return = -999
	LibraryNotFound  Return = -1000
)

// Init 初始化 MXSML 管理库。
func (l *MXSML) Init() Return {
	if err := l.so.Load(); err != nil {
		return LibraryNotFound
	}
	if l.so.Lookup("mxSmlInit") != nil {
		return FunctionNotFound
	}
	return mxSmlInit()
}

// GetDriverVersion 获取 MXSML 驱动版本。
func (l *MXSML) GetDriverVersion() (string, Return) {
	if l.so.Lookup("mxSmlGetDeviceVersion") != nil {
		return "", FunctionNotFound
	}

	version := make([]byte, VERSION_INFO_SIZE)
	versionLen := uint32(len(version))
	ret := mxSmlGetDeviceVersion(0, Version_Driver, &version[0], &versionLen)
	return string(version[:clen(version)]), ret
}

// GetMacaVersion 获取 MACA 运行时版本。
func (l *MXSML) GetMacaVersion() (string, Return) {
	if l.so.Lookup("mxSmlGetMacaVersion") != nil {
		return "", FunctionNotFound
	}

	version := make([]byte, VERSION_INFO_SIZE)
	versionLen := uint32(len(version))
	ret := mxSmlGetMacaVersion(&version[0], &versionLen)
	return string(version[:clen(version)]), ret
}

// IsSuccess 判断调用是否成功。
func (r Return) IsSuccess() bool {
	return r == Success
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
	case Success:
		return "Success"
	case Failure:
		return "Failure"
	case NoDevice:
		return "NoDevice"
	case OperationNotSupport:
		return "OperationNotSupport"
	case SysfsError:
		return "SysfsError"
	case SysfsWriteError:
		return "SysfsWriteError"
	case InvalidDeviceId:
		return "InvalidDeviceId"
	case InvalidDieId:
		return "InvalidDieId"
	case PermissionDenied:
		return "PermissionDenied"
	case InvalidInput:
		return "InvalidInput"
	case InsufficientSize:
		return "InsufficientSize"
	case Reserved3:
		return "Reserved3"
	case IOControlFailure:
		return "IOControlFailure"
	case MmapFailure:
		return "MmapFailure"
	case UnMmapFailure:
		return "UnMmapFailure"
	case InvalidInputForMmap:
		return "InvalidInputForMmap"
	case Reserved1:
		return "Reserved1"
	case Reserved2:
		return "Reserved2"
	case TargetVfNotFound:
		return "TargetVfNotFound"
	case InvalidFrequency:
		return "InvalidFrequency"
	case FlrNotReady:
		return "FlrNotReady"
	case OpenDeviceFileFailure:
		return "OpenDeviceFileFailure"
	case CloseDeviceFileFailure:
		return "CloseDeviceFileFailure"
	case BusyDevice:
		return "BusyDevice"
	case MmioNotEnough:
		return "MmioNotEnough"
	case GetPciBridgeFailure:
		return "GetPciBridgeFailure"
	case LoadDllFailure:
		return "LoadDllFailure"
	case FunctionNotFound:
		return "FunctionNotFound"
	case LibraryNotFound:
		return "LibraryNotFound"
	default:
		return fmt.Sprintf("unknown return value: %d", r)
	}
}
