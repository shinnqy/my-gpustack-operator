package dcmi

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/binding"
)

type DCMI struct {
	so binding.Library
}

// New 创建 DCMI 库对象，并配置系统中厂商库的候选路径。
func New(opts ...binding.LibraryOption) *DCMI {
	soPaths := []string{
		"libdcmi.so",
	}
	{
		home := os.Getenv("CANN_HOME")
		if home == "" {
			home = "/usr/local/Ascend"
		}
		if s, err := os.Stat(home); err == nil && s.IsDir() {
			soPaths = append(soPaths,
				filepath.Join(home, "driver", "lib64", "driver", "libdcmi.so"),
			)
		}
	}

	so := binding.NewLibrary(soPaths, opts...)

	return &DCMI{so: so}
}

// Init 优先尝试 DCMI V1，确认驱动不提供该接口时再尝试 V2。两代都失败时返回 V1 错误码，
// 并在日志中保留 V2 的失败原因。
// C 包装层把诊断消息存在操作系统线程的局部缓冲区，初始化与读取原因必须固定在同一线程，
// 避免 goroutine 在两次 cgo 调用间切换线程。
func (l *DCMI) Init(logger klog.Logger) Return {
	ret, errStr := l.initLocked()
	if !ret.IsSuccess() {
		logger.Errorf(nil, "dcmiInit(%s): %s", l.so.Path(), errStr)
		return ret
	}

	// 只记录选择 V2 的情况，便于在新驱动上定位问题，避免每次常规 V1 初始化都产生一条日志。
	if version := l.APIVersion(); version == APIVersionV2 {
		logger.Infof("dcmiInit(%s): the driver serves the %s API", l.so.Path(), version)
	}

	return ret
}

// initLocked 在同一操作系统线程上初始化驱动并读取失败原因。日志在释放线程后写入，
// 重试时也不会让日志 I/O 长时间占用线程。
func (l *DCMI) initLocked() (Return, string) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	ret := Return(dcmiInit(l.so.Path()))
	if ret.IsSuccess() {
		return ret, ""
	}

	return ret, dcmiLastError()
}

// APIVersion 标识当前已加载的 DCMI 使用哪一代接口。
type APIVersion int32

const (
	// APIVersionUnknown 是零值，表示尚未成功初始化。此时还不能选择 V1 或 V2 的查询路径。
	APIVersionUnknown APIVersion = API_VERSION_UNKNOWN
	// APIVersionV1 表示包括 910B、310P 在内的较早驱动所用的接口。
	APIVersionV1 APIVersion = API_VERSION_V1
	// APIVersionV2 对应 A5/950。它直接枚举设备，省略卡层级，也移除了若干 V1 查询。
	APIVersionV2 APIVersion = API_VERSION_V2
)

// String 返回 API 代际的文本表示。
func (v APIVersion) String() string {
	switch v {
	case APIVersionV1:
		return "V1"
	case APIVersionV2:
		return "V2"
	case APIVersionUnknown:
		return "unknown"
	default:
		return fmt.Sprintf("unknown API version: %d", int32(v))
	}
}

// APIVersion 每次从进程共享的驱动读取当前代际。不同使用者可能分别初始化或重试，在某个对象中缓存失败时的结果，
// 会让稍后已成功加载的 V2 驱动一直走错分支。
func (l *DCMI) APIVersion() APIVersion {
	return apiVersion()
}

// apiVersion 供设备方法选择正确的调用路径。驱动是进程级共享对象，设备句柄无需保存初始化当时的 API 代际。
func apiVersion() APIVersion {
	return APIVersion(dcmiApiVersion())
}

// DCMI 库由进程中的多个使用者共享。卸载会让其他句柄持有的函数指针失效。待 C 包装层实现持有者计数后，
// 再提供对称的 Shutdown。

// GetDriverVersion 读取驱动版本。V2 缺少这个接口，应该保留查询失败。
// dcmiv2_get_dcmi_version 返回的是管理库自身的版本。
func (l *DCMI) GetDriverVersion() (string, Return) {
	version := make([]byte, MAX_VER_LEN)
	ret := Return(dcmiGetDriverVersion(&version[0], uint32(len(version))))
	return string(version[:clen(version)]), ret
}

type Return int

// IsSuccess 判断调用是否成功。
func (r Return) IsSuccess() bool {
	return r == SUCCESS
}

// IsAPIUnavailable 用于指示是否因为加载的库或安装的驱动程序无法提供该功能，导致调用无法完成：共享对象不存
// 在，符号缺失，或者库与驱动程序版本不兼容。
//
// 对于调用者可以不同地操作的每个代码——另一种结构版本、另一种设备、不同的权限——以及作为驱动程序对设备的个
// 人回应，尤其是 ERROR_NOT_SUPPORT，都是错误的。
func (r Return) IsAPIUnavailable() bool {
	switch r {
	case ERROR_LIBRARY_NOT_FOUND, ERROR_FUNCTION_NOT_FOUND:
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
	return defaultErrorStringFunc(int(r))
}

func defaultErrorStringFunc(r int) string {
	switch r {
	case SUCCESS:
		return "SUCCESS"
	case ERROR_INVALID_PARAMETER:
		return "INVALID_PARAMETER"
	case ERROR_MEM_OPERATE_FAIL:
		return "MEM_OPERATE_FAIL"
	case ERROR_INVALID_DEVICE_ID:
		return "INVALID_DEVICE_ID"
	case ERROR_DEVICE_NOT_EXIST:
		return "DEVICE_NOT_EXIST"
	case ERROR_CONFIG_INFO_NOT_EXIST:
		return "CONFIG_INFO_NOT_EXIST"
	case ERROR_OPER_NOT_PERMITTED:
		return "OPER_NOT_PERMITTED"
	case ERROR_NOT_SUPPORT_IN_CONTAINER:
		return "NOT_SUPPORT_IN_CONTAINER"
	case ERROR_NOT_SUPPORT:
		return "NOT_SUPPORT"
	case ERROR_TIME_OUT:
		return "TIME_OUT"
	case ERROR_NOT_REDAY:
		return "NOT_REDAY"
	case ERROR_IS_UPGRADING:
		return "IS_UPGRADING"
	case ERROR_RESOURCE_OCCUPIED:
		return "RESOURCE_OCCUPIED"
	case ERROR_SECURE_FUN_FAIL:
		return "SECURE_FUN_FAIL"
	case ERROR_INNER_ERR:
		return "INNER_ERR"
	case ERROR_IOCTL_FAIL:
		return "IOCTL_FAIL"
	case ERROR_SEND_MSG_FAIL:
		return "SEND_MSG_FAIL"
	case ERROR_RECV_MSG_FAIL:
		return "RECV_MSG_FAIL"
	case ERROR_RESET_FAIL:
		return "RESET_FAIL"
	case ERROR_ABORT_OPERATE:
		return "ABORT_OPERATE"
	case ERROR_FUNCTION_NOT_FOUND:
		return "FUNCTION_NOT_FOUND"
	case ERROR_LIBRARY_NOT_FOUND:
		return "LIBRARY_NOT_FOUND"
	case ERROR_UNKNOWN:
		return "UNKNOWN"
	default:
		return fmt.Sprintf("unknown return value: %d", r)
	}
}
