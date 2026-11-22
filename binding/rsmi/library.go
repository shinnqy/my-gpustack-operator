package rsmi

import (
	"fmt"
	"os"
	"path/filepath"

	"gpustack.ai/gpustack/binding"
)

var home string

func init() {
	for _, env := range []string{"ROCM_HOME", "ROCM_PATH"} {
		home = os.Getenv(env)
		if home != "" {
			break
		}
	}
	if home == "" {
		home = "/opt/rocm"
	}
}

const (
	STATUS_FUNCTION_NOT_FOUND = -99998
	STATUS_LIBRARY_NOT_FOUND  = -99999
)

type RSMI struct {
	so binding.Library
}

// New 创建 RSMI 库对象并配置候选共享库。
func New(opts ...binding.LibraryOption) *RSMI {
	soPaths := []string{
		"librocm_smi64.so",
	}
	{
		path := os.Getenv("ROCM_SMI_LIB_PATH")
		if path != "" {
			soPaths = append(soPaths,
				filepath.Join(path, "librocm_smi64.so"),
			)
		}

		for _, soPathDir := range []string{
			"/opt/hyhal/lib",
			"/opt/dtk/rocm_smi/lib",
			"/opt/dtk/.hyhal/rocm_smi/lib",
		} {
			if s, err := os.Stat(soPathDir); err == nil && s.IsDir() {
				soPaths = append(soPaths,
					filepath.Join(soPathDir, "librocm_smi64.so"),
				)
			}
		}

		if s, err := os.Stat(home); err == nil && s.IsDir() {
			soPaths = append(soPaths,
				filepath.Join(home, "lib", "librocm_smi64.so"),
				filepath.Join(home, "rocm_smi", "lib", "librocm_smi64.so"),
			)
		}
	}

	so := binding.NewLibrary(soPaths, opts...)

	return &RSMI{so: so}
}

// Init 初始化 RSMI 管理库。
func (l *RSMI) Init() Return {
	if err := l.so.Load(); err != nil {
		return STATUS_LIBRARY_NOT_FOUND
	}
	if l.so.Lookup("rsmi_init") != nil {
		return STATUS_FUNCTION_NOT_FOUND
	}
	return rsmiInit(uint64(INIT_FLAG_ALL_GPUS))
}

// GetROCMVersion 从已知的安装路径读取 ROCm 版本。
func (l *RSMI) GetROCMVersion() (string, Return) {
	for _, path := range []string{
		filepath.Join(home, ".info", "version"),
		filepath.Join(home, ".info", "version-rocm"),
		filepath.Join(home, ".info", "version-dev"),
		filepath.Join(home, ".info", "version-libs"),
		filepath.Join(filepath.Dir(home), ".info", "version"),
		filepath.Join(filepath.Dir(home), ".info", "version-rocm"),
		filepath.Join(filepath.Dir(home), ".info", "version-dev"),
		filepath.Join(filepath.Dir(home), ".info", "version-libs"),
	} {
		if s, err := os.Stat(path); err == nil && s.Mode().IsRegular() {
			c, err := os.ReadFile(path)
			if err == nil {
				return string(c), STATUS_SUCCESS
			}
		}
	}
	return "", STATUS_NOT_FOUND
}

// IsSuccess 判断调用是否成功。
func (r Return) IsSuccess() bool {
	return r == STATUS_SUCCESS
}

// IsAPIUnavailable 用于指示是否因为加载的库或安装的驱动程序无法提供该功能，导致调用无法完成：共享对象不存
// 在，符号缺失，或者库与驱动程序版本不兼容。
//
// 对于调用者可以不同地操作的每一个代码——另一种结构版本、另一种设备、不同的权限——以及作为驱动程序对设备的
// 自定义响应的每一个代码，尤其是 STATUS_NOT_SUPPORTED，都是错误的。
func (r Return) IsAPIUnavailable() bool {
	switch r {
	case STATUS_LIBRARY_NOT_FOUND, STATUS_FUNCTION_NOT_FOUND,
		STATUS_DRIVER_NOT_LOADED, STATUS_NOT_YET_IMPLEMENTED:
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
	case STATUS_SUCCESS:
		return "SUCCESS"
	case STATUS_INVALID_ARGS:
		return "INVALID_ARGS"
	case STATUS_NOT_SUPPORTED:
		return "NOT_SUPPORTED"
	case STATUS_FILE_ERROR:
		return "FILE_ERROR"
	case STATUS_PERMISSION:
		return "PERMISSION"
	case STATUS_OUT_OF_RESOURCES:
		return "OUT_OF_RESOURCES"
	case STATUS_INTERNAL_EXCEPTION:
		return "INTERNAL_EXCEPTION"
	case STATUS_INPUT_OUT_OF_BOUNDS:
		return "INPUT_OUT_OF_BOUNDS"
	case STATUS_INIT_ERROR:
		return "INIT_ERROR"
	case STATUS_NOT_YET_IMPLEMENTED:
		return "NOT_YET_IMPLEMENTED"
	case STATUS_NOT_FOUND:
		return "NOT_FOUND"
	case STATUS_INSUFFICIENT_SIZE:
		return "INSUFFICIENT_SIZE"
	case STATUS_INTERRUPT:
		return "INTERRUPT"
	case STATUS_UNEXPECTED_SIZE:
		return "UNEXPECTED_SIZE"
	case STATUS_NO_DATA:
		return "NO_DATA"
	case STATUS_UNEXPECTED_DATA:
		return "UNEXPECTED_DATA"
	case STATUS_BUSY:
		return "BUSY"
	case STATUS_REFCOUNT_OVERFLOW:
		return "REFCOUNT_OVERFLOW"
	case STATUS_DIRECTORY_NOT_FOUND:
		return "DIRECTORY_NOT_FOUND"
	case STATUS_SETTING_UNAVAILABLE:
		return "SETTING_UNAVAILABLE"
	case STATUS_AMDGPU_RESTART_ERR:
		return "AMDGPU_RESTART_ERR"
	case STATUS_DRIVER_NOT_LOADED:
		return "DRIVER_NOT_LOADED"
	case STATUS_IPC_ERROR:
		return "IPC_ERROR"
	case STATUS_FUNCTION_NOT_FOUND:
		return "FUNCTION_NOT_FOUND"
	case STATUS_LIBRARY_NOT_FOUND:
		return "LIBRARY_NOT_FOUND"
	default:
		return fmt.Sprintf("unknown return value: %d", r)
	}
}
