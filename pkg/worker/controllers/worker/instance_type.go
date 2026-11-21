package worker

import (
	"context"
	"strings"
	"time"

	core "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlbuilder "sigs.k8s.io/controller-runtime/pkg/builder"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlevent "sigs.k8s.io/controller-runtime/pkg/event"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	ctrlpredicate "sigs.k8s.io/controller-runtime/pkg/predicate"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/controller"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/systemmeta"
	"gpustack.ai/gpustack/pkg/systemname"
	"gpustack.ai/gpustack/pkg/utils/ctrlclix"
	"gpustack.ai/gpustack/pkg/utils/ctrlhandlerx"
)

// InstanceTypeReconciler owns the lifecycle of worker.gpustack.ai InstanceType CRs and the
// existence + metadata of their backing Kueue ClusterQueue — creation, teardown, and the schedule
// labels. The NodeQueueReconciler owns the queue's quota (resource groups + StopPolicy) and its
// node-devices AdmissionCheck reference; the InstanceType webhook stamps the entrance label and
// the Pod webhook reads the per-card VRAM off the InstanceType spec:
//   - From an InstanceType it ensures the name-identical CQ exists, carrying the schedule labels
//     (derived from the spec identity); the queue is created with the fixed no-borrow isolation
//     policy (empty cohort, no-borrow preemption) stamped into its spec.
//   - It watches the CQ to refresh its status (phase + CPU view) from the queue's quota and
//     conditions, and to recreate the queue on an admin's accidental delete while its
//     InstanceType still lives. The NodeQueueReconciler owns the quota and the HoldAndDrain
//     StopPolicy (teardown / no-flavors drain); this reconciler owns only the admin Inactive
//     Hold<->None pair on the StopPolicy (see syncInactive) and otherwise reads the quota for status.
//   - It does not author InstanceTypes: the NodeFlavorReconciler creates a derived InstanceType
//     (create-only) after it syncs a pool's flavors. This reconciler manages only types that
//     already exist, and never deletes one for lack of flavors — the NodeQueueReconciler
//     drains/empties the backing queue instead.
//   - It materializes the four-view (or CPU) status from the Devices ledger + the
//     ClusterQueue; the hardware-descriptor spec is a one-time snapshot the defaulting webhook
//     fills at admission, no longer refreshed here.
//   - On delete a finalizer holds the InstanceType until it has deleted the backing CQ and the
//     CQ has actually disappeared; the NodeQueueReconciler drains the deleting queue.
type InstanceTypeReconciler struct {
	Client    ctrlcli.Client
	APIReader ctrlcli.Reader
}

var _ ctrlreconcile.Reconciler = (*InstanceTypeReconciler)(nil)

func (r *InstanceTypeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// Implement this function in chapter 06.
	var result0 ctrl.Result
	var result1 error
	return result0, result1
}

func (r *InstanceTypeReconciler) SetupController(_ context.Context, opts controller.SetupOptions) error {
	r.Client = opts.Manager.GetClient()
	r.APIReader = opts.Manager.GetAPIReader()

	dedupWindow := ctrlhandlerx.NewDedupWindow[ctrlreconcile.Request]()

	return ctrl.NewControllerManagedBy(opts.Manager).
		Named("instancetype").
		For(
			// Reconcile each InstanceType by its own name.
			&workercore.InstanceType{},
			// Trigger reconciliation when an InstanceType is:
			// - created, or updated on a generation (spec) change — (re)align its ClusterQueue.
			// - marked for deletion (its DeletionTimestamp is set) — run the teardown.
			// Never react to the final removal (nothing to do once gone) or to the operator's own
			// status/finalizer writes (status churn), to avoid self-triggering.
			ctrlbuilder.WithPredicates(ctrlpredicate.Funcs{
				DeleteFunc: func(ctrlevent.DeleteEvent) bool { return false },
				UpdateFunc: func(e ctrlevent.UpdateEvent) bool {
					oldIt, newIt := e.ObjectOld.(*workercore.InstanceType), e.ObjectNew.(*workercore.InstanceType)
					if !oldIt.DeletionTimestamp.Equal(newIt.DeletionTimestamp) {
						return true
					}
					return oldIt.Generation != newIt.Generation
				},
			}),
		).
		Watches(
			// The InstanceType tracks its backing ClusterQueue: an update refreshes its status
			// (phase + CPU view) from the queue's quota and conditions (which the
			// NodeQueueReconciler and Kueue write), and a deletion is recreated if the
			// InstanceType still lives (guards an admin's accidental delete).
			&kueue.ClusterQueue{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFuncWithWindow(
				3*time.Second,
				dedupWindow,
				r.enqueueInstanceTypeWhenClusterQueueChanged,
			),
			ctrlbuilder.WithPredicates(
				// Trigger reconciliation when a relevant ClusterQueue is created, updated, or
				// deleted — an update keeps the status fresh, a deletion drives the recreate.
				ctrlpredicate.NewPredicateFuncs(func(obj ctrlcli.Object) bool {
					return systemmeta.MatchResource(obj, _ClusterQueueResType)
				}),
			),
		).
		Watches(
			// A Devices ledger change moves the accelerated four-view; this is the
			// watch a ClusterQueue projection could not serve.
			&workercore.Devices{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFuncWithWindow(
				3*time.Second,
				dedupWindow,
				r.enqueueInstanceTypeWhenDevicesChanged,
			),
			ctrlbuilder.WithPredicates(
				// Trigger reconciliation when a managed node's Devices ledger is created, updated,
				// or deleted (it drives the accelerated four-view status).
				ctrlpredicate.NewPredicateFuncs(func(obj ctrlcli.Object) bool {
					return obj.GetLabels()[systemname.ManagedLabelKey] == "true"
				}),
			),
		).
		Complete(r)
}

// enqueueInstanceTypeWhenClusterQueueChanged maps a changed backing ClusterQueue back to its
// name-identical InstanceType, so the reconcile refreshes status from the queue (or recreates it
// when the queue was deleted while the InstanceType still lives).
func (r *InstanceTypeReconciler) enqueueInstanceTypeWhenClusterQueueChanged(
	ctx context.Context, obj ctrlcli.Object,
) []ctrlreconcile.Request {
	logger := ctrllog.FromContext(ctx).
		WithValues("cluster queue", ctrlcli.ObjectKeyFromObject(obj))

	reqs := []ctrlreconcile.Request{
		{
			NamespacedName: ctrlcli.ObjectKeyFromObject(obj),
		},
	}

	logger.V(2).Info("enqueued instance type from cluster queue", "requests", reqs)
	return reqs
}

// enqueueInstanceTypeWhenDevicesChanged enqueues every InstanceType whose pool the changed
// Devices contributes to. For each feature key the Devices carries it lists the InstanceTypes
// sharing that key plus kubernetes.io/os|arch — a node with accelerators serves both its CPU pool
// and its device pool, so a single Devices can enqueue several types — resolving them by label so
// an admin-named type is found without guessing its name.
func (r *InstanceTypeReconciler) enqueueInstanceTypeWhenDevicesChanged(
	ctx context.Context, obj ctrlcli.Object,
) []ctrlreconcile.Request {
	logger := ctrllog.FromContext(ctx).
		WithValues("devices", ctrlcli.ObjectKeyFromObject(obj))

	labels := obj.GetLabels()
	os, arch := labels[core.LabelOSStable], labels[core.LabelArchStable]

	var reqs []ctrlreconcile.Request
	seen := make(map[string]bool)
	for k, v := range labels {
		if v != "true" ||
			(!strings.HasPrefix(k, nodefeature.GeneralFeatureLabelPrefix) &&
				!strings.HasPrefix(k, nodefeature.AcceleratableFeatureLabelPrefix)) {
			continue
		}
		match := ctrlcli.MatchingLabels{k: "true"}
		if os != "" {
			match[core.LabelOSStable] = os
		}
		if arch != "" {
			match[core.LabelArchStable] = arch
		}
		itList := new(workercore.InstanceTypeList)
		if err := r.Client.List(ctx, itList, match,
			ctrlclix.WithoutQuorum, ctrlcli.UnsafeDisableDeepCopy); err != nil {
			logger.Error(err, "list instance types by devices labels")
			continue
		}
		for i := range itList.Items {
			name := itList.Items[i].Name
			if seen[name] {
				continue
			}
			seen[name] = true
			reqs = append(reqs, ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKey{Name: name}})
		}
	}
	if len(reqs) == 0 {
		logger.V(3).Info("devices has no schedule labels or no matching instance type, skip")
		return nil
	}

	logger.V(2).Info("enqueued instance types from devices", "requests", reqs)
	return reqs
}
