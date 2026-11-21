package worker

import (
	"context"
	"maps"

	core "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlbuilder "sigs.k8s.io/controller-runtime/pkg/builder"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlevent "sigs.k8s.io/controller-runtime/pkg/event"
	ctrlpredicate "sigs.k8s.io/controller-runtime/pkg/predicate"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"

	"gpustack.ai/gpustack/pkg/controller"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/systemname"
	"gpustack.ai/gpustack/pkg/utils/mapx"
)

// NodeFeatureReconciler reconciles nfd.NodeFeature objects driven by Kubernetes Node changes to finish the following tasks:
//   - When a Node's labels or any input of its derived labels are updated,
//     create/update the corresponding nfd.NodeFeature.
type NodeFeatureReconciler struct {
	Client ctrlcli.Client
}

var _ ctrlreconcile.Reconciler = (*NodeFeatureReconciler)(nil)

func (r *NodeFeatureReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// Implement this function in chapter 05.
	return ctrl.Result{}, nil
}

func (r *NodeFeatureReconciler) SetupController(_ context.Context, opts controller.SetupOptions) error {
	r.Client = opts.Manager.GetClient()

	return ctrl.NewControllerManagedBy(opts.Manager).
		Named("nodefeature").
		For(
			&core.Node{},
			ctrlbuilder.WithPredicates(
				// Trigger reconciliation when a Node is:
				// - created.
				// - updated if labels or any input of the derived labels have changed.
				ctrlpredicate.Funcs{
					DeleteFunc: func(e ctrlevent.DeleteEvent) bool {
						return false
					},
					UpdateFunc: func(e ctrlevent.UpdateEvent) bool {
						return nodeFeatureNodeUpdated(e.ObjectOld.(*core.Node), e.ObjectNew.(*core.Node))
					},
				},
			),
		).
		Complete(r)
}

// nodeFeatureNodeUpdated reports whether a Node update must re-derive the node's NodeFeature.
func nodeFeatureNodeUpdated(oldNd, newNd *core.Node) bool {
	if newNd.DeletionTimestamp != nil {
		return false
	}
	// Fire when labels have changed.
	if !mapx.EqualWithStringPrefix(oldNd.Labels, newNd.Labels,
		systemname.ManagedLabelKey,
		nodefeature.FeatureLabelPrefix,
		nodefeature.GeneralFeatureLabelPrefix,
		nodefeature.AcceleratableFeatureLabelPrefix) {
		return true
	}
	// Fire when any input of the derived labels has changed, not only the labels above: NFD
	// publishes the cpu-model labels and the cpu-name annotation after the node registers, and a
	// derivation that ran first falls back to the generic group. Comparing the derivation itself
	// covers every input it reads. Manual management is assumed on both sides so the managed
	// label is compared as the node carries it, which detects every change either mode derives.
	return !maps.Equal(
		nodefeature.ConstructNodeCapacityLabels(oldNd, nodefeature.WithManualNodeManagement(true)),
		nodefeature.ConstructNodeCapacityLabels(newNd, nodefeature.WithManualNodeManagement(true)),
	)
}
