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

// modelDeploymentScalerStatusChanged reports whether the control-plane state of a scaler
// changed between two observations.
//
// IT IGNORES THE SLIDING WINDOWS AND THE RAW METRICS, because those change on every
// sampling tick while the control-plane state does not: a scaler that is steady is not
// observed to be different, and a write per sampling tick puts every scaler of the cluster
// on the write path of the apiserver once per interval.
//
// THE WINDOWS ARE NOT LOST BY THIS, because the sampler keeps them in memory and grows them
// on every tick whether the status is written or not. Predictor state is additionally
// checkpointed every modelDeploymentScalerPredictorCheckpointStep samples during steady
// state so an operator restart bounds Kalman filter state loss without writing per tick.
func modelDeploymentScalerStatusChanged(before, after *workercore.ModelDeploymentScalerStatus) bool {
	if before == nil || after == nil {
		return true
	}
	if before.ObservedGeneration != after.ObservedGeneration {
		return true
	}
	// THE QUANTILE IS COMPARED, because it changes when the configuration
	// changes and the status has to say which quantile the scaler is held to.
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

// equalScalerConditions reports whether two lists of conditions describe the same state.
//
// IT IGNORES THE TRANSITION TIME of a condition, because a condition that did not change
// its status did not transition.
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

// equalScalerRoleStatuses reports whether two lists of role statuses describe the same
// control-plane state.
//
// IT IGNORES THE METRICS AND THE STABILIZATION HISTORY of a role, because they are read on
// every tick and change on every tick.
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

// equalScalerTime reports whether two timestamps are the same moment.
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

// updateStatus writes the status of a scaler when its control-plane state changed.
//
// IT WRITES NOTHING WHEN THE SCALER IS STEADY, because the sliding windows and the raw
// metrics of a scaler change on every sampling tick and writing them would put every
// scaler of the cluster on the write path of the apiserver once per interval.
//
// IT SAYS WHICH QUANTILE THE SCALER IS HELD TO, and it says the one the specification names,
// because a status that keeps the quantile of another configuration describes a quantile the
// scaler is no longer held to. The quantile is read here, where every status is written, so
// that a scaler whose planning did not run still says which quantile its specification names.
//
// IT LEAVES THE GENERATION UNOBSERVED WHEN THE QUANTILE CANNOT BE READ, because a scaler that
// cannot say which quantile it is held to has not observed the specification it was given.
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
