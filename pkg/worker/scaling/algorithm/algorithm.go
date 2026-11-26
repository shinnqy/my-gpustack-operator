// Package algorithm 包含纯扩缩容建议算法。
package algorithm

import (
	"time"

	"gpustack.ai/gpustack/pkg/utils/datax"
)

// RoleKind 标识观测数据所代表的服务角色。
type RoleKind string

const (
	RoleKindServer  RoleKind = "Server"
	RoleKindPrefill RoleKind = "Prefill"
	RoleKindDecode  RoleKind = "Decode"
	RoleKindEncode  RoleKind = "Encode"
	// RoleKindUnknown 表示该角色属于算法未知的类型。
	//
	// 它不会默认回退为 Server，因为未指定类型的角色并非 Server：
	// 若将其当作 Server 处理，就会按它从未声明过的目标进行扩缩容。
	RoleKindUnknown RoleKind = "Unknown"

	ServerRole  = RoleKindServer
	PrefillRole = RoleKindPrefill
	DecodeRole  = RoleKindDecode
	EncodeRole  = RoleKindEncode
)

// SignalState 描述单个采样信号的质量状态。
type SignalState string

const (
	SignalFresh   SignalState = "Fresh"
	SignalIdle    SignalState = "Idle"
	SignalMissing SignalState = "Missing"
	SignalStale   SignalState = "Stale"
)

// 算法给出副本数建议时一并返回的原因。
const (
	ReasonWaitingForMetrics           = "WaitingForMetrics"
	ReasonRatioCoupled                = "RatioCoupled"
	ReasonPredictorWarmingUp          = "PredictorWarmingUp"
	ReasonPredictiveConsolidationHold = "PredictiveConsolidationHold"
	ReasonLatencyGuardScaleUp         = "LatencyGuardScaleUp"
	ReasonLatencyGuardHold            = "LatencyGuardHold"
	ReasonTokenThroughput             = "TokenThroughput"
	ReasonNoReadyReplicas             = "NoReadyReplicas"
	ReasonWithinTolerance             = "WithinTolerance"
	ReasonScaleUp                     = "ScaleUp"
	ReasonScaleDown                   = "ScaleDown"
	ReasonPanic                       = "Panic"
)

// RoleObservation 包含某个角色在某一时刻的采样值。
type RoleObservation struct {
	Role                      string
	Kind                      RoleKind
	CurrentReplicas           int32
	ReadyReplicas             int32
	PromptTokensPerSecond     float64
	GenerationTokensPerSecond float64
	RunningRequests           float64
	WaitingRequests           float64
	KVCacheUsage              float64
	TTFTMilliseconds          float64
	ITLMilliseconds           float64
	// TTFTUnmeasured 表示未测量到首 Token 延迟。
	//
	// 当采样器无法读取该延迟时会设置此标记，因为未测量的延迟并不等于零延迟。
	// 旁边的数值不具实际意义，依赖该延迟对角色进行保持决策的算法会跳过它。
	TTFTUnmeasured bool
	// ITLUnmeasured 表示未测量到 Token 间延迟。
	//
	// 当采样器无法读取该延迟时会设置此标记，原因与首 Token 延迟携带相同标记一致。
	ITLUnmeasured    bool
	ThroughputState  SignalState
	LatencyState     SignalState
	GaugeState       SignalState
	CompleteCoverage bool
	ObservedAt       time.Time
}

// RoleRecommendation 是一个角色在应用约束前的副本建议。
type RoleRecommendation struct {
	DesiredReplicas int32
	Reason          string
	HoldScaleDown   bool
	InPanic         bool
}

// DemandSample 是带时间戳的角色需求采样点。
type DemandSample struct {
	Timestamp       time.Time
	TokensPerSecond float64
}

// PredictorState 是单个角色在多次扩缩容建议之间保留的状态。
type PredictorState struct {
	Level       float64
	Trend       float64
	Covariance  [4]float64
	SampleCount int32
	// History 是该角色的需求历史记录。
	//
	// 它采用环形缓冲区实现，因为历史记录会在每个周期新增一个样本并按窗口读取：
	// 若使用切片，则在每个周期都需要重新分配内存来维持同一窗口。
	History     *datax.RingBuffer[DemandSample]
	PanicExitAt time.Time
}

// NewDemandHistory 创建空的需求历史记录。
//
// 预测器用它保存需求样本，容量覆盖算法可能请求的最长窗口。
func NewDemandHistory() *datax.RingBuffer[DemandSample] {
	return datax.NewRingBuffer[DemandSample](maxDemandHistoryCap)
}

// Algorithm 根据一个角色的观测数据建议其副本数。
type Algorithm interface {
	Name() string
	Recommend(obs RoleObservation, prior *PredictorState) (RoleRecommendation, *PredictorState)
}

// copyDemandHistory 将需求样本复制到新的环形缓冲区。
func copyDemandHistory(source *datax.RingBuffer[DemandSample]) *datax.RingBuffer[DemandSample] {
	if source == nil {
		return NewDemandHistory()
	}
	return source.Clone()
}

// copyState 返回算法计算本轮建议时使用的起始状态。
//
// 算法会把本轮样本追加到历史记录中，因此先复制上一轮历史。如果调谐在保存前失败，
// 采样器保留的旧状态仍然有效。
func copyState(prior *PredictorState) PredictorState {
	if prior == nil {
		return PredictorState{
			History: NewDemandHistory(),
		}
	}
	state := *prior
	state.History = copyDemandHistory(prior.History)
	return state
}

// signalUsable 仅在信号新鲜或已确认空闲时返回 true。缺失、过期、空状态及未知状态均视为不可用。
func signalUsable(state SignalState) bool {
	return state == SignalFresh || state == SignalIdle
}

func roleKind(obs RoleObservation) RoleKind {
	if obs.Kind != "" {
		return obs.Kind
	}
	switch obs.Role {
	case "server", "Server":
		return RoleKindServer
	case "prefill", "Prefill":
		return RoleKindPrefill
	case "decode", "Decode":
		return RoleKindDecode
	case "encode", "Encode":
		return RoleKindEncode
	default:
		return RoleKindUnknown
	}
}

func clampPartialCoverage(rec *RoleRecommendation, obs RoleObservation) {
	if obs.CompleteCoverage || rec.DesiredReplicas >= obs.CurrentReplicas {
		return
	}
	rec.DesiredReplicas = obs.CurrentReplicas
	rec.HoldScaleDown = true
}

func maxInt32(a, b int32) int32 {
	if a > b {
		return a
	}
	return b
}
