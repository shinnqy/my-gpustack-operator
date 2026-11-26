package worker

import (
	"context"
	"math"
	"sort"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	gpustack "gpustack.ai/gpustack/api/v1"
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/worker/scaling/algorithm"
	"gpustack.ai/gpustack/pkg/worker/scaling/priors"
)

const modelDeploymentScalerPredictorCheckpointStep = int32(16)

// modelDeploymentScalerStatusChanged 报告 scaler 的控制面状态在两次观测之间是否发生了变化。
//
// 该函数会忽略滑动窗口和原始指标，因为它们在每次采样周期都会变化，而控制面状态不会：
// 处于稳定状态的 scaler 不应被判定为发生变化，且每次采样周期都写入会让集群中每个
// scaler 在每个周期内都走一次 apiserver 写入路径。
//
// 这不会导致滑动窗口丢失，因为采样器会在内存中保留这些窗口并在每个周期持续累积，无论
// status 是否写入。此外，在稳定状态下每经过 modelDeploymentScalerPredictorCheckpointStep
// 次采样就会对预测器状态执行一次 checkpoint，从而在无需每次采样都写入的情况下，将
// operator 重启带来的 Kalman 滤波器状态损失限制在可控范围内。
func modelDeploymentScalerStatusChanged(before, after *workercore.ModelDeploymentScalerStatus) bool {
	if before == nil || after == nil {
		return true
	}
	if before.ObservedGeneration != after.ObservedGeneration {
		return true
	}
	// 需要比较分位数，因为配置变更时它会随之改变，且 status 必须明确反映 scaler 当前
	// 所依据的分位数。
	if before.LatencyQuantile != after.LatencyQuantile {
		return true
	}
	if !kubemeta.DeepEqual(before.ResolvedCapacityPriors, after.ResolvedCapacityPriors) {
		return true
	}
	if !kubemeta.DeepEqual(before.Binding, after.Binding) {
		return true
	}
	if !kubemeta.DeepEqual(before.LastCommit, after.LastCommit) {
		return true
	}
	if !kubemeta.DeepEqual(before.PendingCohort, after.PendingCohort) {
		return true
	}
	if !kubemeta.DeepEqual(before.Rebalance, after.Rebalance) {
		return true
	}
	if !kubemeta.DeepEqual(before.LastScaleDown, after.LastScaleDown) {
		return true
	}
	if !equalScalerConditions(before.Conditions, after.Conditions) {
		return true
	}
	if !equalScalerPredictorCheckpoints(before.Predictors, after.Predictors) {
		return true
	}
	return !equalScalerRoleStatuses(before.Roles, after.Roles)
}

func equalScalerPredictorCheckpoints(
	before, after []workercore.ModelDeploymentScalerPredictorStatus,
) bool {
	if len(before) != len(after) {
		if len(before) == 0 {
			for i := range after {
				if after[i].SampleCount >= modelDeploymentScalerPredictorCheckpointStep {
					return false
				}
			}
			return true
		}
		return false
	}
	for i := range before {
		if before[i].Name != after[i].Name {
			return false
		}
		if after[i].SampleCount < before[i].SampleCount {
			return false
		}
		if after[i].SampleCount-before[i].SampleCount >= modelDeploymentScalerPredictorCheckpointStep {
			return false
		}
	}
	return true
}

// equalScalerConditions 报告两个 Condition 列表是否描述了相同的状态。
//
// 该函数会忽略 Condition 的转换时间，因为状态未发生变化的 Condition 并没有发生转换。
func equalScalerConditions(before, after []gpustack.Condition) bool {
	if len(before) != len(after) {
		return false
	}
	for i := range before {
		if before[i].Type != after[i].Type ||
			before[i].Status != after[i].Status ||
			before[i].Reason != after[i].Reason ||
			before[i].Message != after[i].Message ||
			before[i].ObservedGeneration != after[i].ObservedGeneration {
			return false
		}
	}
	return true
}

// equalScalerRoleStatuses 报告两个角色状态列表是否描述了相同的控制面状态。
//
// 该函数会忽略角色的指标与稳定化历史，因为它们在每个周期都会被读取并发生变化。
func equalScalerRoleStatuses(before, after []workercore.ModelDeploymentScalerRoleStatus) bool {
	if len(before) != len(after) {
		return false
	}
	for i := range before {
		if before[i].Name != after[i].Name ||
			before[i].CurrentReplicas != after[i].CurrentReplicas ||
			before[i].ReadyReplicas != after[i].ReadyReplicas ||
			before[i].SignalState != after[i].SignalState ||
			before[i].RawRecommendedReplicas != after[i].RawRecommendedReplicas ||
			before[i].ConstrainedReplicas != after[i].ConstrainedReplicas ||
			before[i].Reason != after[i].Reason {
			return false
		}
		if !equalScalerTime(before[i].LastScaleUpTime, after[i].LastScaleUpTime) {
			return false
		}
	}
	return true
}

// equalScalerTime 报告两个时间戳是否表示同一时刻。
func equalScalerTime(before, after *meta.Time) bool {
	if before == nil || after == nil {
		return before == after
	}
	return before.Time.Equal(after.Time)
}

func modelDeploymentScalerConditionReason(scaler *workercore.ModelDeploymentScaler, conditionType string) string {
	for _, condition := range scaler.Status.Conditions {
		if condition.Type == conditionType {
			return condition.Reason
		}
	}
	return ""
}

func (r *ModelDeploymentScalerReconciler) resetTargetState(scaler *workercore.ModelDeploymentScaler) {
	scaler.Status.ResolvedCapacityPriors = nil
	scaler.Status.Roles = nil
	scaler.Status.LastCommit = nil
	scaler.Status.PendingCohort = nil
	scaler.Status.Rebalance = nil
	scaler.Status.LastScaleDown = nil
	scaler.Status.Predictors = nil
}

func (r *ModelDeploymentScalerReconciler) setCondition(
	scaler *workercore.ModelDeploymentScaler, conditionType string, status meta.ConditionStatus,
	reason, message string, now time.Time,
) {
	for i := range scaler.Status.Conditions {
		condition := &scaler.Status.Conditions[i]
		if condition.Type != conditionType {
			continue
		}
		if condition.Status != status {
			condition.LastTransitionTime = meta.NewTime(now)
		}
		condition.Status = status
		condition.Reason = reason
		condition.Message = message
		condition.ObservedGeneration = scaler.Generation
		return
	}
	scaler.Status.Conditions = append(scaler.Status.Conditions, gpustack.Condition{
		Type: conditionType, Status: status, LastTransitionTime: meta.NewTime(now),
		Reason: reason, Message: message, ObservedGeneration: scaler.Generation,
	})
}

// updateStatus 在 scaler 的控制面状态发生变化时写入其 status。
//
// 当 scaler 处于稳定状态时不执行写入，因为滑动窗口和原始指标在每次采样周期都会变化，
// 写入它们会让集群中每个 scaler 在每个周期内都走一次 apiserver 写入路径。
//
// status 会说明 scaler 所依据的分位数，并且记录 spec 中指定的分位数；若 status
// 仍保留旧配置的分位数，就会描述一个 scaler 已不再遵循的分位数。分位数在此处（即所有
// status 写入的统一位置）读取，使得即使规划逻辑未运行的 scaler，也能反映其 spec
// 所指定的分位数。
//
// 当无法读取分位数时，保留 generation 为未观测状态，因为无法说明所依据分位数的
// scaler 尚未观测到其所给定的 spec。
func (r *ModelDeploymentScalerReconciler) updateStatus(
	ctx context.Context, before, scaler *workercore.ModelDeploymentScaler,
) error {
	quantile, err := ModelDeploymentScalerLatencyQuantile(&scaler.Spec.Algorithm)
	if err != nil {
		scaler.Status.ObservedGeneration = before.Status.ObservedGeneration
	} else {
		scaler.Status.LatencyQuantile = priors.FormatLatencyQuantile(quantile)
	}
	if !modelDeploymentScalerStatusChanged(&before.Status, &scaler.Status) {
		return nil
	}
	return r.Client.Status().Update(ctx, scaler)
}

func modelDeploymentScalerQuantityValue(quantity *resource.Quantity) float64 {
	if quantity == nil {
		return 0
	}
	return quantity.AsApproximateFloat64()
}

func quantityPointerFloat(value float64) *resource.Quantity {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return nil
	}
	quantity := *resource.NewMilliQuantity(int64(math.Round(value*1000)), resource.DecimalSI)
	return &quantity
}

// modelDeploymentScalerLatencyQuantity 将已测出的角色延迟转为 Quantity。缺少实测值时返回 nil。
//
// 状态只写入实际测得的延迟。填入零会让读者以为引擎瞬间响应。
func modelDeploymentScalerLatencyQuantity(milliseconds float64, unmeasured bool) *resource.Quantity {
	if unmeasured {
		return nil
	}
	return quantityPointerFloat(milliseconds)
}

func scalerObservationState(observation algorithm.RoleObservation) string {
	states := []algorithm.SignalState{observation.GaugeState, observation.ThroughputState, observation.LatencyState}
	missing := false
	fresh := false
	for _, state := range states {
		switch state {
		case algorithm.SignalMissing:
			missing = true
		case algorithm.SignalFresh:
			fresh = true
		case algorithm.SignalStale:
			return string(algorithm.SignalStale)
		}
	}
	if missing || !observation.CompleteCoverage {
		return string(algorithm.SignalMissing)
	}
	if fresh {
		return string(algorithm.SignalFresh)
	}
	return string(algorithm.SignalIdle)
}

func scalerPredictor(
	statuses []workercore.ModelDeploymentScalerPredictorStatus,
	role string,
) *algorithm.PredictorState {
	for i := range statuses {
		status := &statuses[i]
		if status.Name != role {
			continue
		}
		state := &algorithm.PredictorState{SampleCount: status.SampleCount, History: algorithm.NewDemandHistory()}
		if status.Level != nil {
			state.Level = status.Level.AsApproximateFloat64()
		}
		if status.Trend != nil {
			state.Trend = status.Trend.AsApproximateFloat64()
		}
		if status.Covariance != nil {
			state.Covariance = [4]float64{
				modelDeploymentScalerQuantityValue(status.Covariance.P00), modelDeploymentScalerQuantityValue(status.Covariance.P01),
				modelDeploymentScalerQuantityValue(status.Covariance.P10), modelDeploymentScalerQuantityValue(status.Covariance.P11),
			}
		}
		for _, sample := range status.History {
			state.History.Push(algorithm.DemandSample{
				Timestamp: sample.Timestamp.Time, TokensPerSecond: sample.TokensPerSecond.AsApproximateFloat64(),
			})
		}
		return state
	}
	return nil
}

func scalerPredictorStatuses(states map[string]*algorithm.PredictorState) []workercore.ModelDeploymentScalerPredictorStatus {
	roles := make([]string, 0, len(states))
	for role := range states {
		roles = append(roles, role)
	}
	sort.Strings(roles)
	result := make([]workercore.ModelDeploymentScalerPredictorStatus, 0, len(roles))
	for _, role := range roles {
		state := states[role]
		if state == nil {
			continue
		}
		status := workercore.ModelDeploymentScalerPredictorStatus{
			Name: role, Level: quantityPointerFloat(state.Level), Trend: quantityPointerFloat(state.Trend), SampleCount: state.SampleCount,
			Covariance: &workercore.ModelDeploymentScalerKalmanCovariance{
				P00: quantityPointerFloat(state.Covariance[0]), P01: quantityPointerFloat(state.Covariance[1]),
				P10: quantityPointerFloat(state.Covariance[2]), P11: quantityPointerFloat(state.Covariance[3]),
			},
		}
		if state.History != nil {
			for _, sample := range state.History.Data() {
				tps := quantityPointerFloat(sample.TokensPerSecond)
				if tps == nil {
					continue
				}
				status.History = append(status.History, workercore.ModelDeploymentScalerDemandSample{
					Timestamp: meta.NewTime(sample.Timestamp), TokensPerSecond: *tps,
				})
			}
		}
		result = append(result, status)
	}
	return result
}

func scalerStabilizationHistory(
	roles []workercore.ModelDeploymentScalerRoleStatus,
	name string,
) []workercore.ModelDeploymentScalerStabilizationSample {
	for i := range roles {
		if roles[i].Name == name {
			return append([]workercore.ModelDeploymentScalerStabilizationSample(nil), roles[i].StabilizationHistory...)
		}
	}
	return nil
}

func scalerRoleLastScaleUp(roles []workercore.ModelDeploymentScalerRoleStatus, name string) *meta.Time {
	for i := range roles {
		if roles[i].Name == name && roles[i].LastScaleUpTime != nil {
			cloned := *roles[i].LastScaleUpTime
			return &cloned
		}
	}
	return nil
}

func scalerRoleLastScaleUpTime(roles []workercore.ModelDeploymentScalerRoleStatus, name string) time.Time {
	if value := scalerRoleLastScaleUp(roles, name); value != nil {
		return value.Time
	}
	return time.Time{}
}
