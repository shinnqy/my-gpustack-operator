package hsa

import "C"
import (
	"fmt"
	"os"
	"path/filepath"
	"unsafe"

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
	STATUS_ERROR_FUNCTION_NOT_FOUND = -99998
	STATUS_ERROR_LIBRARY_NOT_FOUND  = -99999
)

type HSA struct {
	so binding.Library
}

// New 创建 HSA 库对象，配置系统中厂商库的候选路径。
func New(opts ...binding.LibraryOption) *HSA {
	soPaths := []string{
		"libhsa-runtime64.so.1",
		"libhsa-runtime64.so",
	}
	{
		// 海光把 HSA 运行库放在 hyhal 下，这个目录不一定在动态链接器或 ROCm 的搜索路径中，因此显式加入。
		for _, soPathDir := range []string{
			"/opt/hyhal/lib",
			"/opt/dtk/hsa/lib",
		} {
			if s, err := os.Stat(soPathDir); err == nil && s.IsDir() {
				soPaths = append(soPaths,
					filepath.Join(soPathDir, "libhsa-runtime64.so.1"),
					filepath.Join(soPathDir, "libhsa-runtime64.so"),
				)
			}
		}

		if s, err := os.Stat(home); err == nil && s.IsDir() {
			soPaths = append(soPaths,
				filepath.Join(home, "lib", "libhsa-runtime64.so.1"),
				filepath.Join(home, "lib", "libhsa-runtime64.so"),
			)
		}
	}

	so := binding.NewLibrary(soPaths, opts...)

	return &HSA{so: so}
}

func (l *HSA) Init() Return {
	if err := l.so.Load(); err != nil {
		return STATUS_ERROR_LIBRARY_NOT_FOUND
	}
	if l.so.Lookup("hsa_init") != nil {
		return STATUS_ERROR_FUNCTION_NOT_FOUND
	}
	return hsaInit()
}

type (
	// AgentProperty 保存 HSA GPU 代理的属性。
	AgentProperty struct {
		BDF                  string
		UUID                 string
		ProductName          string
		Name                 string
		ComputeUnitCount     uint32
		NumShaderEngines     uint32
		NumShaderArraysPerSE uint32
		NumXcc               uint32
		AsicFamilyId         uint32
	}
	// Agents 按 AgentUUID 索引本轮枚举到的 HSA GPU 属性。
	Agents map[string]AgentProperty
)

func (l *HSA) GetAgents() Agents {
	agents := Agents{}

	if l.so.Lookup("hsa_agent_get_info") != nil {
		return agents
	}
	if l.so.Lookup("hsa_iterate_agents") != nil {
		return agents
	}

	_ = hsaIterateAgents(func(agent Agent, data unsafe.Pointer) Return {
		device, _ := hsaAgentGetInfoDevice(agent)
		if DeviceType(device) != DEVICE_TYPE_GPU {
			return STATUS_SUCCESS // 跳过非 GPU 的 HSA 代理。
		}

		bdf, _ := hsaAgentGetBDF(agent)
		uuid, _ := hsaAgentGetInfoUUID(agent)
		productName, _ := hsaAgentGetInfoProduceName(agent)
		name, _ := hsaAgentGetInfoName(agent)
		computeUnitCount, _ := hsaAgentGetInfoComputeUnitCount(agent)
		numShaderEngines, _ := hsaAgentGetInfoNumShaderEngines(agent)
		numShaderArraysPerSE, _ := hsaAgentGetInfoNumShaderArraysPerSE(agent)
		numXcc, _ := hsaAgentGetInfoNumXcc(agent)
		asicFamilyId, _ := hsaAgentGetInfoAsicFamilyId(agent)

		key := bdf
		if key == "" {
			key = uuid
		}
		agents[key] = AgentProperty{
			BDF:                  bdf,
			UUID:                 uuid,
			ProductName:          productName,
			Name:                 name,
			ComputeUnitCount:     computeUnitCount,
			NumShaderEngines:     numShaderEngines,
			NumShaderArraysPerSE: numShaderArraysPerSE,
			NumXcc:               numXcc,
			AsicFamilyId:         asicFamilyId,
		}
		return STATUS_SUCCESS
	})

	return agents
}

// IsSuccess 判断调用是否成功。
func (r Return) IsSuccess() bool {
	return r == STATUS_SUCCESS
}

// String 返回厂商返回码的文字说明。
func (r Return) String() string {
	return r.Error()
}

// Error 将厂商返回码作为 Go 错误呈现。
func (r Return) Error() string {
	return defaultErrorStringFunc(r)
}

var defaultErrorStringFunc = func(r Return) string {
	switch r {
	case STATUS_SUCCESS:
		return "SUCCESS"
	case STATUS_INFO_BREAK:
		return "INFO_BREAK"
	case STATUS_ERROR:
		return "ERROR"
	case STATUS_ERROR_INVALID_ARGUMENT:
		return "INVALID_ARGUMENT"
	case STATUS_ERROR_INVALID_QUEUE_CREATION:
		return "INVALID_QUEUE_CREATION"
	case STATUS_ERROR_INVALID_ALLOCATION:
		return "INVALID_ALLOCATION"
	case STATUS_ERROR_INVALID_AGENT:
		return "INVALID_AGENT"
	case STATUS_ERROR_INVALID_REGION:
		return "INVALID_REGION"
	case STATUS_ERROR_INVALID_SIGNAL:
		return "INVALID_SIGNAL"
	case STATUS_ERROR_INVALID_QUEUE:
		return "INVALID_QUEUE"
	case STATUS_ERROR_OUT_OF_RESOURCES:
		return "OUT_OF_RESOURCES"
	case STATUS_ERROR_INVALID_PACKET_FORMAT:
		return "INVALID_PACKET_FORMAT"
	case STATUS_ERROR_RESOURCE_FREE:
		return "RESOURCE_FREE"
	case STATUS_ERROR_NOT_INITIALIZED:
		return "NOT_INITIALIZED"
	case STATUS_ERROR_REFCOUNT_OVERFLOW:
		return "REFCOUNT_OVERFLOW"
	case STATUS_ERROR_INCOMPATIBLE_ARGUMENTS:
		return "INCOMPATIBLE_ARGUMENTS"
	case STATUS_ERROR_INVALID_INDEX:
		return "INVALID_INDEX"
	case STATUS_ERROR_INVALID_ISA:
		return "INVALID_ISA"
	case STATUS_ERROR_INVALID_ISA_NAME:
		return "INVALID_ISA_NAME"
	case STATUS_ERROR_INVALID_CODE_OBJECT:
		return "INVALID_CODE_OBJECT"
	case STATUS_ERROR_INVALID_EXECUTABLE:
		return "INVALID_EXECUTABLE"
	case STATUS_ERROR_FROZEN_EXECUTABLE:
		return "FROZEN_EXECUTABLE"
	case STATUS_ERROR_INVALID_SYMBOL_NAME:
		return "INVALID_SYMBOL_NAME"
	case STATUS_ERROR_VARIABLE_ALREADY_DEFINED:
		return "VARIABLE_ALREADY_DEFINED"
	case STATUS_ERROR_VARIABLE_UNDEFINED:
		return "VARIABLE_UNDEFINED"
	case STATUS_ERROR_EXCEPTION:
		return "EXCEPTION"
	case STATUS_ERROR_INVALID_CODE_SYMBOL:
		return "INVALID_CODE_SYMBOL"
	case STATUS_ERROR_INVALID_EXECUTABLE_SYMBOL:
		return "INVALID_EXECUTABLE_SYMBOL"
	case STATUS_ERROR_INVALID_FILE:
		return "INVALID_FILE"
	case STATUS_ERROR_INVALID_CODE_OBJECT_READER:
		return "INVALID_CODE_OBJECT_READER"
	case STATUS_ERROR_INVALID_CACHE:
		return "INVALID_CACHE"
	case STATUS_ERROR_INVALID_WAVEFRONT:
		return "INVALID_WAVEFRONT"
	case STATUS_ERROR_INVALID_SIGNAL_GROUP:
		return "INVALID_SIGNAL_GROUP"
	case STATUS_ERROR_INVALID_RUNTIME_STATE:
		return "INVALID_RUNTIME_STATE"
	case STATUS_ERROR_FATAL:
		return "FATAL"
	default:
		return fmt.Sprintf("unknown return value: %d", r)

	}
}

// AMDGPU 的家族编号及对应芯片代号如下。
// AMDGPU_FAMILY_SI = 110  # Hainan, Oland, Verde, Pitcairn, Tahiti
// AMDGPU_FAMILY_CI = 120  # Bonaire, Hawaii
// AMDGPU_FAMILY_KV = 125  # Kaveri, Kabini, Mullins
// AMDGPU_FAMILY_VI = 130  # Iceland, Tonga
// AMDGPU_FAMILY_CZ = 135  # Carrizo, Stoney
// AMDGPU_FAMILY_AI = 141  # Vega10
// AMDGPU_FAMILY_RV = 142  # Raven
// AMDGPU_FAMILY_NV = 143  # Navi10
// AMDGPU_FAMILY_VGH = 144  # Van Gogh
// AMDGPU_FAMILY_GC_11_0_0 = 145  # GC 11.0.0
// AMDGPU_FAMILY_YC = 146  # Yellow Carp
// AMDGPU_FAMILY_GC_11_0_1 = 148  # GC 11.0.1
// AMDGPU_FAMILY_GC_10_3_6 = 149  # GC 10.3.6
// AMDGPU_FAMILY_GC_10_3_7 = 151  # GC 10.3.7
// AMDGPU_FAMILY_GC_11_5_0 = 150  # GC 11.5.0
// AMDGPU_FAMILY_GC_12_0_0 = 152  # GC 12.0.0
var familyIdMap = map[uint32]string{
	110: "Southern Islands",
	120: "Sea Islands",
	125: "Kaveri",
	130: "Volcanic Islands",
	135: "Carrizo",
	141: "Arctic Islands",
	142: "Raven",
	143: "Navi",
	144: "Van Gogh",
	145: "GC 11.0.0",
	146: "Yellow Carp",
	148: "GC 11.0.1",
	149: "GC 10.3.6",
	151: "GC 10.3.7",
	150: "GC 11.5.0",
	152: "GC 12.0.0",
}

func (in AgentProperty) Family() string {
	if v, ok := familyIdMap[in.AsicFamilyId]; ok {
		return v
	}
	return ""
}
