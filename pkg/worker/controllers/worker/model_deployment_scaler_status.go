package worker

import (
	"context"
	"time"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	gpustack "gpustack.ai/gpustack/api/v1"
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubemeta"
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
