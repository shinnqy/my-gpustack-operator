// Package priors 从实测服务画像的离线目录中解析容量先验。
//
// 先验指针对特定部署形态测得的容量：包含精确的模型、推理引擎、硬件与拓扑、
// 声明的工作负载包络，以及在其上实测得到的负载点。
//
// 该目录绝不做猜测。无法精确匹配的画像，或缺少解析器所需依据的画像，
// 会以带类型的原因拒绝，而不会自动填充默认值。
package priors

import (
	"gpustack.ai/gpustack/pkg/worker/scaling/algorithm"
)

// CatalogAPIVersion 是容量先验目录文档的 API 组与版本。
const CatalogAPIVersion = "worker.gpustack.ai/v1alpha1"

// CatalogKind 是容量先验目录文档的资源类型（kind）。
const CatalogKind = "ModelDeploymentScalerCapacityPriorCatalog"

// SchemaVersion 是本包读取的目录 schema 版本。
//
// 由其他 schema 版本编写的目录会被直接拒绝而非猜测解析，
// 因为其携带的字段决定了哪些测量数据可用。
const SchemaVersion = CatalogAPIVersion

// 解码后的目录不得超过的上限。
//
// 设置这些上限是为了防止从 ConfigMap 读取的目录导致控制器持有无界数据。
const (
	// MaxCatalogBytes 是读取器接受的解码后目录最大字节数。
	MaxCatalogBytes = 512 * 1024

	// MaxProfiles 是单个目录最多可包含的画像数量。
	MaxProfiles = 256

	// MaxPointsPerProfile 是单个画像最多可包含的实测负载点数量。
	MaxPointsPerProfile = 128
)

// RoleKind 是画像所描述的服务角色。
//
// 它复用算法包的 RoleKind，因为目录描述的角色与算法扩缩容的角色属于同一集合，
// 另起一套名称会导致两者逐渐偏离。
type RoleKind = algorithm.RoleKind

// 目录画像可描述的服务角色。
const (
	RoleKindServer  = algorithm.RoleKindServer
	RoleKindPrefill = algorithm.RoleKindPrefill
	RoleKindDecode  = algorithm.RoleKindDecode
	RoleKindEncode  = algorithm.RoleKindEncode
)

// ThroughputMetric 指定实测吞吐量的分子指标名称。
//
// 该分子必须与采样器一致，因为采样器读取的正是用来与容量相除的需求量；
// 测量不同物理量的数据源不能填入同一字段。
type ThroughputMetric string

const (
	// ThroughputMetricCacheMissedInputTokensPerSecond 统计单副本每秒在缓存未命中后
	// 预取的 prompt token 数。
	//
	// 它是 prefill 阶段的分子，因为采样器读取的正是缓存未命中的 prompt token 数。
	ThroughputMetricCacheMissedInputTokensPerSecond ThroughputMetric = "CacheMissedInputTokensPerSecond"

	// ThroughputMetricOutputTokensPerSecond 统计单副本每秒生成的 token 数。
	//
	// 它是 decode 阶段的分子。
	ThroughputMetricOutputTokensPerSecond ThroughputMetric = "OutputTokensPerSecond"

	// ThroughputMetricCacheMissedTotalTokensPerSecond 统计每秒未被缓存命中的
	// prompt token 数与生成的 token 数之和。
	//
	// 该指标作为服务端分子，因为采样器针对服务端角色会将两者相加；且它显式
	// 标注了缓存未命中，因为包含已缓存 prompt token 的总计属于不同的度量口径。
	ThroughputMetricCacheMissedTotalTokensPerSecond ThroughputMetric = "CacheMissedTotalTokensPerSecond"

	// ThroughputMetricInputTokensPerSecondIncludingCacheHits 统计每个请求的
	// prompt token 数，无论缓存是否命中。
	//
	// 该指标不匹配任何采样器分子，因此仅包含此指标的 profile 不适用于任何角色。
	ThroughputMetricInputTokensPerSecondIncludingCacheHits ThroughputMetric = "InputTokensPerSecondIncludingCacheHits"

	// ThroughputMetricTotalTokensPerSecond 统计每个请求的 prompt token 数与
	// 生成的 token 数之和。
	//
	// 这是数据源可发布的最弱总计口径，因为它未说明 prompt 部分是否包含
	// 缓存命中的 token；其自身不匹配任何采样器分子。
	ThroughputMetricTotalTokensPerSecond ThroughputMetric = "TotalTokensPerSecond"
)

// TokenCountConvention 指定实测速率所统计的 token 口径。
//
// 它与指标定义相互独立：指标说明该速率对解析器的语义含义，而口径说明
// 数据源为生成该速率具体统计了哪些 token。
type TokenCountConvention string

const (
	// TokenCountConventionCacheMissedInput 统计未被缓存命中的 prompt token。
	TokenCountConventionCacheMissedInput TokenCountConvention = "CacheMissedInput"

	// TokenCountConventionOutput 统计生成的 token。
	TokenCountConventionOutput TokenCountConvention = "Output"

	// TokenCountConventionCacheMissedInputPlusOutput 统计未被缓存命中的
	// prompt token 与生成的 token 之和。
	TokenCountConventionCacheMissedInputPlusOutput TokenCountConvention = "CacheMissedInputPlusOutput"

	// TokenCountConventionInputIncludingCacheHits 统计每个请求的 prompt token。
	TokenCountConventionInputIncludingCacheHits TokenCountConvention = "InputIncludingCacheHits"

	// TokenCountConventionInputPlusOutput 统计每个请求的 prompt token 与
	// 生成的 token 之和。
	TokenCountConventionInputPlusOutput TokenCountConvention = "InputPlusOutput"
)

// Denominator 指定实测吞吐量的统计基准。
//
// 它决定了如何针对单个扩缩容副本重构测量结果。
type Denominator string

const (
	// DenominatorPerGPU 按实测系统的单块 GPU 统计每秒 token 数。
	DenominatorPerGPU Denominator = "PerGPU"

	// DenominatorPerReplica 按单个完整的扩缩容副本统计每秒 token 数。
	DenominatorPerReplica Denominator = "PerReplica"

	// DenominatorWholeSystem 按整个实测系统统计每秒 token 数。
	DenominatorWholeSystem Denominator = "WholeSystem"
)

// LatencyMetric 指定一种实测延迟指标。
type LatencyMetric string

const (
	// LatencyMetricTTFT 表示首 token 延迟（TTFT）。
	LatencyMetricTTFT LatencyMetric = "TTFT"

	// LatencyMetricITL 表示 token 间延迟（ITL）。
	//
	// 它不同于单输出 token 平均耗时（TPOT），后者是单个请求所有输出 token 的
	// 均值，而非 token 间隔的分布。
	LatencyMetricITL LatencyMetric = "ITL"

	// LatencyMetricTPOT 表示单输出 token 平均耗时（TPOT）。
	//
	// 记录该值是为了在数据源仅发布 TPOT 时能够给出明确的拒绝原因，因为它
	// 无法确定任何 ITL 分位数。
	LatencyMetricTPOT LatencyMetric = "TPOT"
)

// LoadKind 指定实测负载点所变化的维度。
type LoadKind string

const (
	// LoadKindConcurrency 表示固定数量的并发请求。
	LoadKindConcurrency LoadKind = "Concurrency"

	// LoadKindRequestRate 表示固定的请求到达速率。
	LoadKindRequestRate LoadKind = "RequestRate"
)

// ClientModel 指定负载的发送模式。
type ClientModel string

const (
	// ClientModelFixedConcurrency 表示闭环客户端，仅在上一个请求完成后才发送下一个请求。
	ClientModelFixedConcurrency ClientModel = "FixedConcurrency"

	// ClientModelOpenLoop 表示开环客户端，无论服务端响应快慢均以固定速率发送请求。
	ClientModelOpenLoop ClientModel = "OpenLoop"
)

// CacheBehavior 指定实测工作负载对前缀缓存的处理方式。
type CacheBehavior string

const (
	// CacheBehaviorAllMisses 表示运行期间统计的每个 prompt token 均为缓存未命中。
	CacheBehaviorAllMisses CacheBehavior = "AllMisses"

	// CacheBehaviorIncludesHits 表示统计的 prompt token 包含由缓存提供的 token。
	CacheBehaviorIncludesHits CacheBehavior = "IncludesCacheHits"

	// CacheBehaviorUnknown 表示数据源未说明缓存行为。
	//
	// 它不是通配符；缓存行为未知的 profile 不能用于统计缓存未命中 prompt token 的分子。
	CacheBehaviorUnknown CacheBehavior = "Unknown"
)

// catalog 中存储测量数据时使用的单位。
//
// 数据源在导入时即完成单位换算，因此存储值不会附带需要读取方自行解释的单位。
const (
	// UnitTokensPerSecond 表示 token 速率单位。
	UnitTokensPerSecond = "tokens/second"

	// UnitMilliseconds 表示时长单位。
	UnitMilliseconds = "milliseconds"

	// UnitQueriesPerSecond 表示到达速率单位。
	UnitQueriesPerSecond = "queries/second"
)

// DefaultLatencyQuantile 是请求未指定分位数时解析器默认要求的延迟分位数。
//
// 由于同一指标的更高分位数可从上方约束目标分位数，因此允许用更高分位数替代。
//
// 该值仅作为默认值而非强制要求，因为请求可以显式指定目标分位数，解析器会
// 按该单一分位数约束所有必需的延迟指标。
const DefaultLatencyQuantile = 0.95

// Catalog 是带版本控制的、有界的实测 profile 集合。
type Catalog struct {
	// SchemaVersion 是编写该 catalog 所使用的 schema 版本。
	SchemaVersion string `json:"schemaVersion"`

	// Digest 覆盖整个 catalog 的内容。
	//
	// 它本身不属于被覆盖的内容，因此可用来校验与其描述的内容是否一致。
	Digest string `json:"digest"`

	// Sources 记录 profile 的来源信息。
	Sources []Source `json:"sources"`

	// Profiles 保存实测的 profile 列表。
	Profiles []Profile `json:"profiles"`
}

// Source 记录单个导入测量数据集的归属信息。
//
// 即使导入未生成 profile 也会记录该信息，以便被拒绝的数据源能够说明原因。
type Source struct {
	// ID 在 catalog 内唯一标识该数据源。
	ID string `json:"id"`

	// Name 是数据源的可读名称。
	Name string `json:"name"`

	// URL 是读取该数据源的地址。
	URL string `json:"url"`

	// Revision 固定产物所来自的确切源码修订版本。
	Revision string `json:"revision"`

	// CaptureDate 是进行该测量的日期。
	CaptureDate string `json:"captureDate"`

	// License 是产物所遵循许可证的 SPDX 标识符。
	License string `json:"license"`

	// AdapterVersion 是读取该数据源的适配器版本。
	AdapterVersion string `json:"adapterVersion"`

	// Artifacts 记录导入过程读取的固定文件列表。
	Artifacts []Artifact `json:"artifacts"`

	// Outcome 在导入成功生成 profile 时为 Accepted，否则为 Refused。
	Outcome string `json:"outcome"`

	// RefusalError 说明结果为 Refused 的原因。
	RefusalError *RefusalError `json:"refusal,omitempty"`
}

// 数据源导入可能产生的结果状态。
const (
	SourceOutcomeAccepted = "Accepted"
	SourceOutcomeRefused  = "Refused"
)

// Artifact 表示单个固定的源文件。
type Artifact struct {
	// Path 是该文件在上游仓库中的路径。
	Path string `json:"path"`

	// SHA256 是导入时所读取上游字节内容的摘要。
	SHA256 string `json:"sha256"`
}

// Profile 是单个不可变的实测 profile。
//
// 每个 profile 仅对应一种服务配置和一种工作负载包络，因为混合多种服务配置的
// 前沿曲线无法代表单一部署的容量曲线。
type Profile struct {
	// Digest 覆盖该 profile 的内容。
	Digest string `json:"digest"`

	// SourceID 指定导入该 profile 的数据源 ID。
	SourceID string `json:"sourceID"`

	// Role 是测量结果所描述的服务角色。
	Role RoleKind `json:"role"`

	// Identity 是部署必须匹配的标识信息。
	Identity Identity `json:"identity"`

	// ServingConfig 是进行测量时所采用的服务配置。
	ServingConfig ServingConfig `json:"servingConfig"`

	// Topology 是单个扩缩容副本的拓扑形态。
	Topology Topology `json:"topology"`

	// MeasuredReplicas 是被测系统运行的扩缩容副本数。
	//
	// 该字段不参与匹配，因为在相同拓扑形态下，不同副本数测得的结果仍属于同一性能画像。
	MeasuredReplicas int `json:"measuredReplicas"`

	// Workload 是进行测量时所处的工作负载范围。
	Workload Workload `json:"workload"`

	// Points 是测得的负载数据点。
	Points []MeasuredPoint `json:"points"`
}

// Identity 是部署使用某性能画像时必须匹配的标识信息。
//
// 每个字段均为必填项，因为未知值并非通配符，不得将其视为宽松的默认值。
type Identity struct {
	// Model 是所服务的模型。
	Model ModelIdentity `json:"model"`

	// Engine 是服务引擎。
	Engine EngineIdentity `json:"engine"`

	// Hardware 是副本运行所用的加速器。
	Hardware HardwareIdentity `json:"hardware"`
}

// ModelIdentity 标识所服务的模型。
type ModelIdentity struct {
	// Name 是模型名称，并保留其变体后缀。
	Name string `json:"name"`

	// Revision 是权重版本或摘要。
	Revision string `json:"revision"`

	// Tokenizer 是分词器标识。
	Tokenizer string `json:"tokenizer"`

	// Quantization 是量化方案。
	Quantization string `json:"quantization"`
}

// EngineIdentity 标识服务引擎。
type EngineIdentity struct {
	// Image 是引擎镜像。
	Image string `json:"image"`

	// Build 是引擎构建号或版本。
	Build string `json:"build"`
}

// HardwareIdentity 标识加速器。
type HardwareIdentity struct {
	// Accelerator 是加速器型号，并保留其外形规格后缀。
	Accelerator string `json:"accelerator"`

	// MemoryGB 是加速器显存大小（单位为 GiB）。
	MemoryGB int `json:"memoryGB"`

	// FormFactor 是加速器的外形规格。
	FormFactor string `json:"formFactor"`
}

// ServingConfig 是进行测量时所采用的服务配置。
//
// 每个字段均为必填项，因为被省略的性能关键设置并非通配符，宽松的默认值会导致不兼容的部署复用该测量结果。
type ServingConfig struct {
	// Router 是位于引擎前端的路由器或调度器。
	Router string `json:"router"`

	// KVCacheDType 是 KV 缓存的数据类型。
	KVCacheDType string `json:"kvCacheDType"`

	// Offload 说明 KV 缓存的卸载方式。
	Offload string `json:"offload"`

	// PrefixCaching 说明是否开启前缀缓存。
	PrefixCaching string `json:"prefixCaching"`

	// SpeculativeDecoding 说明投机解码的配置方式。
	SpeculativeDecoding string `json:"speculativeDecoding"`

	// LaunchParameters 保留会影响性能的有序启动参数。
	// 调整顺序可能会改变重复标志和选项值的生效结果。
	LaunchParameters []string `json:"launchParameters"`

	// EffectiveLaunchParameters 在引擎实际运行参数与启动参数不同时，记录引擎实际使用的参数。
	//
	// 它仅作为证据而非匹配条件，因为它反映的是引擎对启动参数的实际解析结果，而非部署所请求的配置。
	// 使用所记录启动参数拉起的部署，在所记录的引擎构建版本上就会以所记录的实际生效参数运行。
	//
	// 当两者一致时该字段省略，因为与启动参数完全相同的副本不会提供额外信息。
	EffectiveLaunchParameters []string `json:"effectiveLaunchParameters,omitempty"`

	// EngineEnv 记录测量运行时的引擎环境变量。
	//
	// 它是数据源能够证实的完整集合，因为不完整的集合会让带有未知引擎变量的部署复用该测量结果。
	// 部署必须匹配其中的每一项，且不得携带测量时未包含的变量。
	//
	// 它仅作为证据而非匹配条件，因为测量环境描述的是测量运行时的条件，需要由持有部署信息的调用方据此进行校验。
	EngineEnv []string `json:"engineEnv,omitempty"`

	// EngineEnvComplete 表示 EngineEnv 包含了测量运行时的全部环境变量。
	//
	// 仅当数据源观测到了完整环境时才设置该字段，因为数据源未观测到的环境并不等同于空环境。
	// 无法监控环境的数据源会在此标明，此时该性能画像不能与部署进行匹配，因为这会导致部署根据一个无法反映测量实际运行环境的集合进行校验。
	EngineEnvComplete bool `json:"engineEnvComplete,omitempty"`

	// EffectiveDefaults 记录数据源未显式传入的引擎参数的实际生效默认值。
	//
	// 它仅作为证据而非匹配条件，因为引擎自行选定的默认值并不代表该引擎构建版本的固有默认值。
	// 只有当部署的镜像、启动参数和环境与测量时完全一致时，才能依赖这些默认值。
	EffectiveDefaults map[string]string `json:"effectiveDefaults,omitempty"`
}

// Topology 是单个扩缩容副本的拓扑形态。
//
// 每个字段均为必填项，因为数据源未指明的并行度宽度并非通配符。
type Topology struct {
	// GPUsPerReplica 是单个扩缩容副本使用的 GPU 数量。
	GPUsPerReplica int `json:"gpusPerReplica"`

	// TensorParallel 是张量并行度。
	TensorParallel int `json:"tensorParallel"`

	// PipelineParallel 是流水线并行度。
	PipelineParallel int `json:"pipelineParallel"`

	// DataParallel 是数据并行度。
	DataParallel int `json:"dataParallel"`

	// LocalDataParallel 是本地数据并行度。
	LocalDataParallel int `json:"localDataParallel"`

	// ContextParallel 是上下文并行度。
	ContextParallel int `json:"contextParallel"`

	// ExpertParallel 是专家并行度。
	ExpertParallel int `json:"expertParallel"`
}

// Normalized 返回规范化形式的拓扑结构。
//
// 它会将未指明的并行度宽度替换为 1，因为数据源未声明的宽度即为 1，而宽度为 0 不对应任何有效的部署。
func (t Topology) Normalized() Topology {
	if t.TensorParallel <= 0 {
		t.TensorParallel = 1
	}
	if t.PipelineParallel <= 0 {
		t.PipelineParallel = 1
	}
	if t.DataParallel <= 0 {
		t.DataParallel = 1
	}
	if t.LocalDataParallel <= 0 {
		t.LocalDataParallel = 1
	}
	if t.ContextParallel <= 0 {
		t.ContextParallel = 1
	}
	if t.ExpertParallel <= 0 {
		t.ExpertParallel = 1
	}
	return t
}

// Workload 是进行测量时所处的工作负载范围。
type Workload struct {
	// ID 标识该工作负载。
	ID string `json:"id"`

	// Revision 锁定工作负载的版本定义。
	Revision string `json:"revision"`

	// Modality 是请求的模态类型。
	Modality string `json:"modality"`

	// InputLengths 描述输入 token 长度分布。
	InputLengths LengthDistribution `json:"inputLengths"`

	// OutputLengths 描述输出 token 长度分布。
	OutputLengths LengthDistribution `json:"outputLengths"`

	// CacheBehavior 说明前缀缓存处理请求的方式。
	CacheBehavior CacheBehavior `json:"cacheBehavior"`

	// ArrivalModel 说明负载的施加方式。
	ArrivalModel ClientModel `json:"arrivalModel"`
}

// LengthDistribution 描述 token 长度分布。
//
// 数据源未公布的长度字段将保持缺省，因为长度为 0 属于实际测量值，而缺失值并非如此。
type LengthDistribution struct {
	// Min 是测得的最短长度。
	Min *int `json:"min,omitempty"`

	// Max 是测得的最长长度。
	Max *int `json:"max,omitempty"`

	// Mean 是测得的平均长度。
	Mean *float64 `json:"mean,omitempty"`

	// Count 是测量该分布时所统计的请求数量。
	Count int `json:"count"`
}

// MeasuredPoint 表示一个实测负载点。
type MeasuredPoint struct {
	// Load 是施加的负载。
	Load LoadLevel `json:"load"`

	// Throughput 是系统在该负载下持续达到的吞吐量。
	Throughput Throughput `json:"throughput"`

	// Latencies 是实测延迟统计信息。
	Latencies []Latency `json:"latencies"`

	// Requests 统计该负载点运行的请求数。
	Requests RequestCounts `json:"requests"`

	// Duration 是该负载点的运行时长。
	Duration MeasurementDuration `json:"duration"`

	// SLO 记录此次运行是否满足延迟约束。
	SLO *SLOOutcome `json:"slo,omitempty"`

	// Saturation 记录显式的饱和度测量结果。
	Saturation *Saturation `json:"saturation,omitempty"`
}

// LoadLevel 表示实测点所施加的负载。
type LoadLevel struct {
	// Kind 表示负载的变化维度。
	Kind LoadKind `json:"kind"`

	// Value 是施加的负载值。
	Value float64 `json:"value"`

	// ClientModel 说明负载是如何施加的。
	ClientModel ClientModel `json:"clientModel"`

	// Unit 是 Value 的单位。
	Unit string `json:"unit"`
}

// Throughput 表示一项实测吞吐量。
type Throughput struct {
	// Metric 是分子指标。
	Metric ThroughputMetric `json:"metric"`

	// Value 是实测速率。
	Value float64 `json:"value"`

	// Unit 是 Value 的单位。
	Unit string `json:"unit"`

	// Denominator 说明该速率统计的分母维度。
	Denominator Denominator `json:"denominator"`

	// TokenCountConvention 说明该速率统计了哪些 token。
	TokenCountConvention TokenCountConvention `json:"tokenCountConvention"`

	// TokenTotals 保存用于计算该速率的 token 总数。
	//
	// 当数据源未公布这些数据时该字段缺省，因为缺失的测量值应保持缺失。
	TokenTotals *TokenTotals `json:"tokenTotals,omitempty"`
}

// TokenTotals 保存实测速率背后的 token 总数。
type TokenTotals struct {
	// InputTokens 是统计的 prompt token 总数。
	InputTokens *float64 `json:"inputTokens,omitempty"`

	// OutputTokens 是统计的生成 token 总数。
	OutputTokens *float64 `json:"outputTokens,omitempty"`
}

// RequestCounts 统计负载点运行的请求数。
type RequestCounts struct {
	// Successful 是已完成的请求数。
	Successful int `json:"successful"`

	// Failed 是失败的请求数。
	Failed int `json:"failed"`
}

// MeasurementDuration 表示负载点的运行时长。
type MeasurementDuration struct {
	// WarmUpSeconds 是被排除的预热时间（秒）。
	WarmUpSeconds float64 `json:"warmUpSeconds"`

	// MeasuredSeconds 是实测持续时间（秒）。
	MeasuredSeconds float64 `json:"measuredSeconds"`
}

// Latency 表示一项实测延迟统计信息。
type Latency struct {
	// Metric 是所测量的指标。
	Metric LatencyMetric `json:"metric"`

	// Quantile 是该数值对应的分位数。
	Quantile float64 `json:"quantile"`

	// Value 是实测值。
	Value float64 `json:"value"`

	// Unit 是 Value 的单位。
	Unit string `json:"unit"`

	// SampleCount 是计算该统计量所用的样本数量。
	SampleCount int `json:"sampleCount"`

	// Censoring 说明样本的截尾方式。
	Censoring string `json:"censoring"`
}

// SLOOutcome 记录某次运行是否满足延迟约束。
type SLOOutcome struct {
	// ConstraintsSatisfied 表示该次运行是否满足约束。
	ConstraintsSatisfied bool `json:"constraintsSatisfied"`

	// TTFTConstraintMilliseconds 是首 token 延迟（TTFT）约束（毫秒）。
	TTFTConstraintMilliseconds float64 `json:"ttftConstraintMilliseconds"`

	// TPOTConstraintMilliseconds 是单输出 token 耗时（TPOT）约束（毫秒）。
	TPOTConstraintMilliseconds float64 `json:"tpotConstraintMilliseconds"`
}

// Saturation 记录显式的饱和度测量结果。
type Saturation struct {
	// Measured 表示是否测量了饱和度。
	Measured bool `json:"measured"`

	// Note 说明饱和度是如何确定的。
	Note string `json:"note,omitempty"`
}
