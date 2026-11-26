package algorithm

import (
	"math"
)

// ProportionalMetric 指定比例算法读取哪一项指标。
type ProportionalMetric string

const (
	MetricRunningRequests           ProportionalMetric = "RunningRequests"
	MetricWaitingRequests           ProportionalMetric = "WaitingRequests"
	MetricKVCacheUsage              ProportionalMetric = "KVCacheUsage"
	MetricPromptTokensPerSecond     ProportionalMetric = "PromptTokensPerSecond"
	MetricGenerationTokensPerSecond ProportionalMetric = "GenerationTokensPerSecond"
	MetricQueueDepth                ProportionalMetric = "QueueDepth"
	MetricTotalTokensPerSecond      ProportionalMetric = "TotalTokensPerSecond"
)

// Proportional 根据观测值与目标值的比例计算副本数，并在容忍区间内保持不变。
type Proportional struct {
	Metric      ProportionalMetric
	TargetValue float64
	Tolerance   float64
}

// Name 返回算法名称。
func (p Proportional) Name() string { return "Proportional" }

// Recommend 根据选定指标返回副本数建议。
func (p Proportional) Recommend(obs RoleObservation, _ *PredictorState) (RoleRecommendation, *PredictorState) {
	rec := RoleRecommendation{DesiredReplicas: obs.CurrentReplicas}
	if obs.ReadyReplicas <= 0 {
		rec.HoldScaleDown = true
		rec.Reason = ReasonNoReadyReplicas
		return rec, nil
	}
	target := p.TargetValue
	if target <= 0 || math.IsNaN(target) || math.IsInf(target, 0) {
		rec.HoldScaleDown = true
		rec.Reason = ReasonWaitingForMetrics
		return rec, nil
	}
	value, state, perReplica := p.value(obs)
	if !signalUsable(state) {
		rec.HoldScaleDown = true
		rec.Reason = ReasonWaitingForMetrics
		return rec, nil
	}
	if !perReplica {
		value /= float64(obs.ReadyReplicas)
	}
	ratio := value / target
	tolerance := p.Tolerance
	if tolerance <= 0 {
		tolerance = 0.10
	}
	if math.Abs(ratio-1) <= tolerance {
		rec.Reason = ReasonWithinTolerance
		return rec, nil
	}
	if ratio <= 0 {
		if obs.GaugeState == SignalIdle {
			rec.DesiredReplicas = 1
		} else {
			// 只有已确认空闲的零读数才能用于空闲判断。
			// 其他零读数让算法保持当前副本数并等待指标。
			rec.DesiredReplicas = obs.CurrentReplicas
			rec.HoldScaleDown = true
			rec.Reason = ReasonWaitingForMetrics
			clampPartialCoverage(&rec, obs)
			return rec, nil
		}
	} else {
		rec.DesiredReplicas = int32(math.Ceil(float64(obs.ReadyReplicas) * ratio))
		if ratio > 1 && rec.DesiredReplicas < obs.CurrentReplicas {
			rec.DesiredReplicas = obs.CurrentReplicas
		}
		if ratio < 1 && rec.DesiredReplicas < obs.CurrentReplicas && obs.ReadyReplicas < obs.CurrentReplicas {
			rec.DesiredReplicas = obs.CurrentReplicas
			rec.HoldScaleDown = true
		}
		if rec.DesiredReplicas < 1 {
			rec.DesiredReplicas = 1
		}
	}
	switch {
	case rec.DesiredReplicas > obs.CurrentReplicas:
		rec.Reason = ReasonScaleUp
	case rec.DesiredReplicas < obs.CurrentReplicas:
		rec.Reason = ReasonScaleDown
	default:
		rec.Reason = ReasonWithinTolerance
	}
	clampPartialCoverage(&rec, obs)
	return rec, nil
}

func (p Proportional) value(obs RoleObservation) (float64, SignalState, bool) {
	state := obs.GaugeState
	switch p.Metric {
	case MetricWaitingRequests:
		return obs.WaitingRequests, state, false
	case MetricQueueDepth:
		return obs.RunningRequests + obs.WaitingRequests, state, false
	case MetricKVCacheUsage:
		return obs.KVCacheUsage, state, true
	case MetricPromptTokensPerSecond:
		return obs.PromptTokensPerSecond, obs.ThroughputState, false
	case MetricGenerationTokensPerSecond:
		return obs.GenerationTokensPerSecond, obs.ThroughputState, false
	case MetricTotalTokensPerSecond:
		return obs.PromptTokensPerSecond + obs.GenerationTokensPerSecond, obs.ThroughputState, false
	case MetricRunningRequests, "":
		return obs.RunningRequests, state, false
	default:
		return 0, SignalMissing, false
	}
}
