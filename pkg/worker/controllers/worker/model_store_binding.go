package worker

import (
	"context"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	ctrlbuilder "sigs.k8s.io/controller-runtime/pkg/builder"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlhandler "sigs.k8s.io/controller-runtime/pkg/handler"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	ctrlpredicate "sigs.k8s.io/controller-runtime/pkg/predicate"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/controller"
)

// ModelStoreBindingReconciler keeps a grant's status equal to what its namespace actually warms:
// usedBytes counts each distinct digest any of the namespace's prefetches targets, at its full
// size, for every TARGET node holding it — a digest a workload mounted on a node the prefetch
// never asked for belongs to the node's watermarks, not to this budget. A shared tree charges
// every namespace whose prefetches target it, and a digest nobody holds charges nothing.
//
// An unmeasurable contribution — an artifact that has not resolved, mostly — leaves the whole
// figure absent rather than half a number: a usedBytes that silently excluded a prefetch would
// read as headroom the namespace does not have.
type ModelStoreBindingReconciler struct {
	Client ctrlcli.Client
	Now    func() time.Time
}

var _ ctrlreconcile.Reconciler = (*ModelStoreBindingReconciler)(nil)

func (r *ModelStoreBindingReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// Implement this function in chapter 16.
	var result0 ctrl.Result
	var result1 error
	return result0, result1
}

func (r *ModelStoreBindingReconciler) SetupController(_ context.Context, opts controller.SetupOptions) error {
	r.Client = opts.Manager.GetClient()

	return ctrl.NewControllerManagedBy(opts.Manager).
		Named("modelstorebinding").
		For(&workercore.ModelStoreBinding{}, ctrlbuilder.WithPredicates(ctrlpredicate.GenerationChangedPredicate{})).
		Watches(
			&workercore.ModelPrefetch{},
			ctrlhandler.EnqueueRequestsFromMapFunc(r.enqueueAllBindings),
			ctrlbuilder.WithPredicates(ctrlpredicate.GenerationChangedPredicate{}),
		).
		Watches(
			&workercore.ModelArtifact{},
			ctrlhandler.EnqueueRequestsFromMapFunc(r.enqueueAllBindings),
			ctrlbuilder.WithPredicates(artifactResolutionChanged),
		).
		Watches(
			&workercore.NodeModelStore{},
			ctrlhandler.EnqueueRequestsFromMapFunc(r.enqueueAllBindings),
			ctrlbuilder.WithPredicates(nodeReportChanged),
		).
		Complete(r)
}

// enqueueAllBindings re-enqueues every grant: the accounting reads across namespaces by design, and
// grants are few.
func (r *ModelStoreBindingReconciler) enqueueAllBindings(ctx context.Context, _ ctrlcli.Object) []ctrlreconcile.Request {
	bindings := new(workercore.ModelStoreBindingList)
	if err := r.Client.List(ctx, bindings); err != nil {
		ctrllog.FromContext(ctx).Error(err, "list model store bindings")

		return nil
	}

	reqs := make([]ctrlreconcile.Request, 0, len(bindings.Items))
	for i := range bindings.Items {
		reqs = append(reqs, ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKey{
			Namespace: bindings.Items[i].Namespace, Name: bindings.Items[i].Name,
		}})
	}

	return reqs
}
