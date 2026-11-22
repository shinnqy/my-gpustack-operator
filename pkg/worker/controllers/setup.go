package controllers

import (
	"context"

	"gpustack.ai/gpustack/pkg/controller"
	"gpustack.ai/gpustack/pkg/manager"
	"gpustack.ai/gpustack/pkg/worker/controllers/worker"
)

// setups 是控制器初始化函数列表。
var setups = []controller.Setup{
	new(worker.InstanceReconciler),
	new(worker.InstanceIsolationReconciler),
	new(worker.InstanceTypeReconciler),
	new(worker.KVCacheBackendReconciler),
	new(worker.KVCachePoolReconciler),
	new(worker.ModelArtifactReconciler),
	new(worker.ModelDeploymentReconciler),
	new(worker.ModelDeploymentScalerReconciler),
	new(worker.ModelDeploymentJointAdmissionReconciler),
	new(worker.ModelDeploymentJointAdmissionCheckReconciler),
	new(worker.ModelStoreReconciler),
	new(worker.ModelStoreBindingReconciler),
	new(worker.ModelPrefetchReconciler),
	new(worker.NodeQueueReconciler),
	new(worker.NodeQueueEntranceReconciler),
	new(worker.TopologySourceReconciler),
	new(worker.NodeTopologyReconciler),
	new(worker.NodeCapacityReconciler),
	new(worker.NodeFitLabelReconciler),
	new(worker.NodeFeatureReconciler),
	new(worker.NodeModelStoreReconciler),
	new(worker.NodeFlavorReconciler),
	new(worker.NodeDevicesReconciler),
	new(worker.NodeDevicesAdmissionReconciler),
	new(worker.NodeDevicesAdmissionCheckReconciler),
}

// IMPORTANT: 控制面 Deployment 入口
// Setup 初始化所有控制器。
func Setup(ctx context.Context, mgr manager.CtrlManager) error {
	return controller.ExecuteSetup(ctx, mgr, setups)
}
