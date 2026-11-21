package worker

import (
	"context"
	"time"

	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

// plan computes the replica vector of a scaler and writes the state it observed.
//
// THE BEFORE SNAPSHOT IS THE ONE THE RECONCILE TOOK, because the status of the scaler is
// written once at the end of the plan and the write has to carry every change the reconcile
// made, not only the ones the plan made.
func (r *ModelDeploymentScalerReconciler) plan(
	ctx context.Context,
	before *workercore.ModelDeploymentScaler,
	scaler *workercore.ModelDeploymentScaler,
	md *workercore.ModelDeployment,
	now time.Time,
	active bool,
) (ctrlreconcile.Result, error) {
	return r.planWithPriors(ctx, before, scaler, md, nil, now, active)
}

func (r *ModelDeploymentScalerReconciler) planWithPriors(
	ctx context.Context,
	before *workercore.ModelDeploymentScaler,
	scaler *workercore.ModelDeploymentScaler,
	md *workercore.ModelDeployment,
	preResolved *ModelDeploymentScalerCapacityPriors,
	now time.Time,
	active bool,
) (ctrlreconcile.Result, error) {
	// Implement this function in chapter 24.
	var result0 ctrlreconcile.Result
	var result1 error
	return result0, result1
}

func deploymentReplicaVector(md *workercore.ModelDeployment) map[string]int32 {
	vector := make(map[string]int32, len(md.Spec.Roles))
	for _, role := range md.Spec.Roles {
		vector[role.Name] = role.Replicas
	}
	return vector
}
