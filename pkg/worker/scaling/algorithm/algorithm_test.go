package algorithm

import (
	"math"
	"testing"
	"time"
)

func freshObservation(at time.Time) RoleObservation {
	return RoleObservation{
		Role:             "role",
		Kind:             RoleKindServer,
		CurrentReplicas:  2,
		ReadyReplicas:    2,
		ThroughputState:  SignalFresh,
		LatencyState:     SignalFresh,
		GaugeState:       SignalFresh,
		CompleteCoverage: true,
		ObservedAt:       at,
	}
}

func TestProportionalUsesReadyReplicasAndDeadband(t *testing.T) {
	at := time.Unix(0, 0)
	alg := Proportional{Metric: MetricRunningRequests, TargetValue: 4, Tolerance: 0.1}
	obs := freshObservation(at)
	obs.RunningRequests = 12
	got, _ := alg.Recommend(obs, nil)
	if got.DesiredReplicas != 3 || got.Reason != "ScaleUp" {
		t.Fatalf("scale up = %#v", got)
	}
	obs.CurrentReplicas = 3
	obs.ReadyReplicas = 3
	obs.RunningRequests = 12.2
	got, _ = alg.Recommend(obs, nil)
	if got.DesiredReplicas != 3 || got.Reason != "WithinTolerance" {
		t.Fatalf("deadband = %#v", got)
	}
	obs.RunningRequests = 0
	obs.GaugeState = SignalIdle
	got, _ = alg.Recommend(obs, nil)
	if got.DesiredReplicas != 1 {
		t.Fatalf("idle = %#v", got)
	}
	obs.ReadyReplicas = 0
	got, _ = alg.Recommend(obs, nil)
	if got.DesiredReplicas != obs.CurrentReplicas || !got.HoldScaleDown || got.Reason != "NoReadyReplicas" {
		t.Fatalf("no ready = %#v", got)
	}

	// 正在发布新副本时，当前副本数为 4、就绪数为 1。即使负载较轻，也要等发布完成后再考虑缩容。
	obs.CurrentReplicas = 4
	obs.ReadyReplicas = 1
	obs.GaugeState = SignalFresh
	obs.RunningRequests = 3
	got, _ = alg.Recommend(obs, nil)
	if got.DesiredReplicas != 4 || !got.HoldScaleDown {
		t.Fatalf("partial readiness under-target = %#v, want 4 with HoldScaleDown", got)
	}
}

func TestProportionalMissingAndPartialCoverage(t *testing.T) {
	alg := Proportional{Metric: MetricRunningRequests, TargetValue: 1}
	obs := freshObservation(time.Unix(0, 0))
	obs.RunningRequests = 0
	obs.GaugeState = SignalMissing
	got, _ := alg.Recommend(obs, nil)
	if got.Reason != "WaitingForMetrics" || got.DesiredReplicas != obs.CurrentReplicas || !got.HoldScaleDown {
		t.Fatalf("missing = %#v", got)
	}
	obs.GaugeState = SignalFresh
	obs.RunningRequests = 10
	obs.CompleteCoverage = false
	got, _ = alg.Recommend(obs, nil)
	if got.DesiredReplicas <= obs.CurrentReplicas || got.HoldScaleDown {
		t.Fatalf("partial scale-up = %#v", got)
	}
	obs.RunningRequests = 0
	got, _ = alg.Recommend(obs, nil)
	if got.DesiredReplicas != obs.CurrentReplicas || !got.HoldScaleDown {
		t.Fatalf("partial scale-down = %#v", got)
	}

	obs.GaugeState = SignalIdle
	obs.CompleteCoverage = true
	got, _ = alg.Recommend(obs, nil)
	if got.DesiredReplicas != 1 {
		t.Fatalf("idle scale-down = %#v, want 1", got)
	}
}

// TestProportionalZeroReadingIsNotAToleranceBand 验证未经空闲状态确认的零读数会让算法等待指标、保持副本数。
// 只有可用的读数才能判断偏差是否落入容忍区间。
func TestProportionalZeroReadingIsNotAToleranceBand(t *testing.T) {
	alg := Proportional{Metric: MetricRunningRequests, TargetValue: 4, Tolerance: 0.1}
	cases := []struct {
		name              string
		gaugeState        SignalState
		wantDesired       int32
		wantReason        string
		wantHoldScaleDown bool
	}{
		{
			name:              "a zero reading that is not idle waits for metrics",
			gaugeState:        SignalFresh,
			wantDesired:       2,
			wantReason:        ReasonWaitingForMetrics,
			wantHoldScaleDown: true,
		},
		{
			name:        "an idle zero reading scales down to the floor",
			gaugeState:  SignalIdle,
			wantDesired: 1,
			wantReason:  ReasonScaleDown,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obs := freshObservation(time.Unix(0, 0))
			obs.GaugeState = tc.gaugeState
			obs.RunningRequests = 0
			got, _ := alg.Recommend(obs, nil)
			if got.DesiredReplicas != tc.wantDesired || got.Reason != tc.wantReason ||
				got.HoldScaleDown != tc.wantHoldScaleDown {
				t.Fatalf("recommendation = %#v, want desired %d, reason %s and hold %v",
					got, tc.wantDesired, tc.wantReason, tc.wantHoldScaleDown)
			}
		})
	}
}

func TestTokenThroughputRolesPanicAndLatency(t *testing.T) {
	base := time.Unix(100, 0)
	alg := TokenThroughput{
		PrefillTargetTokensPerSecond: 10,
		DecodeTargetTokensPerSecond:  10,
		ServerTargetTokensPerSecond:  10,
		StableWindowSeconds:          180,
		PanicWindowSeconds:           60,
		PanicThreshold:               2,
		TTFTTargetMilliseconds:       100,
		ITLTargetMilliseconds:        10,
	}
	obs := freshObservation(base)
	obs.Kind = RoleKindPrefill
	obs.PromptTokensPerSecond = 15
	got, state := alg.Recommend(obs, nil)
	if got.DesiredReplicas != 2 || state == nil || state.History.Len() != 1 {
		t.Fatalf("prefill = %#v state=%#v", got, state)
	}
	obs.Kind = RoleKindDecode
	obs.GenerationTokensPerSecond = 10
	got, _ = alg.Recommend(obs, nil)
	if got.DesiredReplicas != 1 {
		t.Fatalf("decode = %#v", got)
	}
	state = nil
	obs.Kind = RoleKindServer
	obs.PromptTokensPerSecond = 20
	obs.GenerationTokensPerSecond = 20
	obs.ObservedAt = base.Add(10 * time.Second)
	got, state = alg.Recommend(obs, state)
	if got.DesiredReplicas < 2 || !got.InPanic || !got.HoldScaleDown {
		t.Fatalf("panic = %#v state=%#v", got, state)
	}
	obs.PromptTokensPerSecond = 0
	obs.GenerationTokensPerSecond = 0
	obs.ThroughputState = SignalIdle
	obs.ObservedAt = base.Add(30 * time.Second)
	got, state = alg.Recommend(obs, state)
	if !got.InPanic || !got.HoldScaleDown {
		t.Fatalf("sticky panic = %#v state=%#v", got, state)
	}
	obs.LatencyState = SignalFresh
	obs.ThroughputState = SignalFresh
	obs.PromptTokensPerSecond = 10
	obs.GenerationTokensPerSecond = 10
	obs.TTFTMilliseconds = 110
	obs.ObservedAt = base.Add(4 * time.Minute)
	got, _ = alg.Recommend(obs, state)
	if got.Reason != ReasonLatencyGuardHold || got.DesiredReplicas != obs.CurrentReplicas || !got.HoldScaleDown {
		t.Fatalf("latency hold = %#v, want ReasonLatencyGuardHold", got)
	}

	obs.TTFTMilliseconds = 121
	obs.ObservedAt = base.Add(5 * time.Minute)
	got, _ = alg.Recommend(obs, state)
	if got.Reason != "LatencyGuardScaleUp" || got.DesiredReplicas < obs.CurrentReplicas+1 {
		t.Fatalf("latency up = %#v", got)
	}
}

func TestTokenThroughputMissingLatencyAndPartialCoverage(t *testing.T) {
	alg := TokenThroughput{ServerTargetTokensPerSecond: 10, TTFTTargetMilliseconds: 100}
	obs := freshObservation(time.Unix(0, 0))
	obs.Kind = RoleKindServer
	obs.PromptTokensPerSecond = 0
	obs.GenerationTokensPerSecond = 0
	obs.ThroughputState = SignalIdle
	obs.LatencyState = SignalMissing
	got, _ := alg.Recommend(obs, nil)
	if got.Reason != "WaitingForMetrics" || got.DesiredReplicas != obs.CurrentReplicas {
		t.Fatalf("missing latency = %#v", got)
	}
	obs.ThroughputState = SignalFresh
	obs.PromptTokensPerSecond = 50
	got, _ = alg.Recommend(obs, nil)
	if got.DesiredReplicas <= obs.CurrentReplicas || !got.HoldScaleDown {
		t.Fatalf("missing latency must not suppress throughput scale-up = %#v", got)
	}
	obs.PromptTokensPerSecond = 0
	obs.ThroughputState = SignalIdle
	obs.LatencyState = SignalIdle
	got, _ = alg.Recommend(obs, nil)
	if got.DesiredReplicas != 1 {
		t.Fatalf("idle latency = %#v", got)
	}

	obsUnknown := freshObservation(time.Unix(0, 0))
	obsUnknown.Kind = RoleKindUnknown
	gotUnknown, _ := alg.Recommend(obsUnknown, nil)
	if gotUnknown.Reason != ReasonWaitingForMetrics || gotUnknown.DesiredReplicas != obsUnknown.CurrentReplicas || !gotUnknown.HoldScaleDown {
		t.Fatalf("unknown role kind = %#v, want ReasonWaitingForMetrics and HoldScaleDown", gotUnknown)
	}
	obs.LatencyState = SignalFresh
	obs.PromptTokensPerSecond = 100
	obs.CompleteCoverage = false
	got, _ = alg.Recommend(obs, nil)
	if got.DesiredReplicas <= obs.CurrentReplicas {
		t.Fatalf("partial up = %#v", got)
	}

	// 即使状态为 Fresh，NaN 延迟仍应让算法返回 WaitingForMetrics 并保持副本数。
	obsNaN := freshObservation(time.Unix(0, 0))
	obsNaN.Kind = RoleKindServer
	obsNaN.TTFTMilliseconds = math.NaN()
	obsNaN.PromptTokensPerSecond = 50
	obsNaN.LatencyState = SignalFresh
	got, _ = alg.Recommend(obsNaN, nil)
	if !got.HoldScaleDown {
		t.Fatalf("NaN latency must hold scale-down = %#v", got)
	}

	// 一旦历史覆盖整个稳定窗口，缺少延迟读数本身不会继续阻止此处的缩容。
	var state *PredictorState
	historyObs := freshObservation(time.Unix(0, 0))
	historyObs.Kind = RoleKindServer
	historyObs.CurrentReplicas = 4
	historyObs.PromptTokensPerSecond = 10
	historyObs.GenerationTokensPerSecond = 0
	historyObs.LatencyState = SignalMissing
	historyObs.ThroughputState = SignalFresh
	got, state = alg.Recommend(historyObs, state)
	if got.Reason != "WaitingForMetrics" || got.DesiredReplicas != 4 {
		t.Fatalf("initial observation with missing latency should hold: %#v", got)
	}

	// 以固定间隔持续提供较低的 token 需求，让采样历史覆盖整个稳定窗口。
	for ts := int64(15); ts <= 180; ts += 15 {
		historyObs.ObservedAt = time.Unix(ts, 0)
		got, state = alg.Recommend(historyObs, state)
	}
	if got.Reason != "ScaleDown" || got.DesiredReplicas != 1 || got.HoldScaleDown {
		t.Fatalf("matured history should allow scale-down despite missing latency: %#v", got)
	}
}

func TestPredictiveWarmupForecastAndHistory(t *testing.T) {
	alg := Predictive{
		TokenThroughput:               TokenThroughput{ServerTargetTokensPerSecond: 100},
		ServerTokenCapacityPerReplica: 100,
		HorizonSeconds:                300,
	}
	var state *PredictorState
	var rec RoleRecommendation
	for i := 0; i < 6; i++ {
		obs := freshObservation(time.Unix(int64(i*15), 0))
		obs.Kind = RoleKindServer
		obs.PromptTokensPerSecond = float64(100 + i*40)
		rec, state = alg.Recommend(obs, state)
		if i < 5 && rec.Reason != "PredictorWarmingUp" {
			t.Fatalf("warmup %d = %#v", i, rec)
		}
	}
	if state.SampleCount != 6 || state.History.Len() != 6 {
		t.Fatalf("state = %#v", state)
	}
	if state.Level <= 0 || math.IsNaN(state.Level) || math.IsNaN(state.Trend) {
		t.Fatalf("filter state = %#v", state)
	}
	obs := freshObservation(time.Unix(90, 0))
	obs.Kind = RoleKindServer
	obs.PromptTokensPerSecond = 340
	obs.CurrentReplicas = 1
	obs.ReadyReplicas = 1
	rec, state = alg.Recommend(obs, state)
	if rec.DesiredReplicas < 2 || state.SampleCount != 7 {
		t.Fatalf("forecast = %#v state=%#v", rec, state)
	}
	for i := 0; i < 40; i++ {
		obs.ObservedAt = time.Unix(int64(105+i*15), 0)
		_, state = alg.Recommend(obs, state)
	}
	if state.History.Len() > 32 {
		t.Fatalf("history len = %d", state.History.Len())
	}
}

func TestPredictiveConsolidationAndLatencyCorrection(t *testing.T) {
	alg := Predictive{
		TokenThroughput:               TokenThroughput{ServerTargetTokensPerSecond: 200, TTFTTargetMilliseconds: 100},
		ServerTokenCapacityPerReplica: 100,
		HorizonSeconds:                1,
	}
	state := &PredictorState{SampleCount: 6, Level: 100, Trend: 0, Covariance: [4]float64{1, 0, 0, 1}}
	obs := freshObservation(time.Unix(0, 0))
	obs.Kind = RoleKindServer
	obs.CurrentReplicas = 3
	obs.ReadyReplicas = 3
	obs.PromptTokensPerSecond = 170
	obs.LatencyState = SignalFresh
	obs.TTFTMilliseconds = 100
	rec, _ := alg.Recommend(obs, state)
	if rec.DesiredReplicas != obs.CurrentReplicas {
		t.Fatalf("consolidation hold = %#v", rec)
	}
	obs.TTFTMilliseconds = 121
	rec, _ = alg.Recommend(obs, state)
	if rec.DesiredReplicas < obs.CurrentReplicas+1 || rec.Reason != "LatencyGuardScaleUp" {
		t.Fatalf("latency correction = %#v", rec)
	}
}
