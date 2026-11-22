package worker

import (
	"context"
	"time"

	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

// plan 计算扩缩器的副本向量并写入其观测到的状态。
//
// 采用的是 Reconcile 开始时获取的前置快照，因为扩缩器的 status 仅在 plan 结束时
// 统一写入一次，而此次写入必须包含整个 Reconcile 过程所做的全部变更，而不仅仅是
// plan 内部产生的变更。
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
	// 在第 24 章中实现此函数。
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
