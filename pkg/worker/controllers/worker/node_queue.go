package worker

import (
	"context"
	"fmt"
	"time"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlbuilder "sigs.k8s.io/controller-runtime/pkg/builder"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlevent "sigs.k8s.io/controller-runtime/pkg/event"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	ctrlpredicate "sigs.k8s.io/controller-runtime/pkg/predicate"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"gpustack.ai/gpustack/pkg/controller"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/systemmeta"
	"gpustack.ai/gpustack/pkg/utils/ctrlclix"
	"gpustack.ai/gpustack/pkg/utils/ctrlhandlerx"
	"gpustack.ai/gpustack/pkg/utils/mapx"
	"gpustack.ai/gpustack/pkg/utils/slicex"
	"gpustack.ai/gpustack/pkg/worker/apistatus"
	"gpustack.ai/gpustack/pkg/worker/settings"
)

// NodeQueueReconciler owns the quota and admission gating of an operator-owned Kueue
// ClusterQueue — its resource groups, the StopPolicy, and the node-devices AdmissionCheck
// reference — driven by ClusterQueue, ResourceFlavor, and AdmissionCheck changes. The
// InstanceTypeReconciler owns the queue's lifecycle (creation, schedule labels,
// cohort/preemption isolation, and deletion); this reconciler
// validates the flavor selectors against Nodes, then converges credit/CPU quota without looking
// at the owning InstanceType:
//   - Being deleted: drive HoldAndDrain unconditionally so Kueue evicts the admitted workloads
//     and can then drop its own finalizer and remove the queue — Kueue never evicts on delete by
//     itself. (This covers both an admin's direct delete and the InstanceType teardown's delete.)
//   - Flavors present: fill the resource groups from the flavors, smallest per-node count
//     first so Kueue packs small nodes before large ones, reference the node-devices
//     AdmissionCheck on an accelerated derived queue and the joint-admission AdmissionCheck on
//     every queue once each is Active, reactivate a queue
//     that had been drained to empty (StopPolicy None), and drop the marker from the Hold this
//     reconciler placed on a queue that had no resource groups yet, leaving its release to the
//     InstanceTypeReconciler.
//   - No flavors, quota still defined: gated by instance-type-drain-when-no-flavors, drive the
//     queue to HoldAndDrain and requeue until every reservation clears, then empty the resource
//     groups — so Kueue's reservation counters never go negative — and keep the emptied queue
//     held until a flavor returns.
//   - No resource groups yet, and not stopped: Hold the queue and mark the Hold as this
//     reconciler's, whether the pool has no flavors or its flavors fail validation. A queue that
//     declares no resource would admit every Workload.
//
// Reactivation fires only on a queue whose resource groups are already empty, so it never
// contends with a drain still in progress.
type NodeQueueReconciler struct {
	Client ctrlcli.Client
}

var _ ctrlreconcile.Reconciler = (*NodeQueueReconciler)(nil)

// _ClusterQueueResType is the systemmeta resource type carried by the backing
// ClusterQueue this reconciler owns.
const _ClusterQueueResType = "instancetypes"

// IsInstanceTypeClusterQueue reports whether a ClusterQueue is one this operator backs an
// InstanceType with, by the resource-type mark it stamps on each one it creates. The InstanceType
// shares the ClusterQueue's name.
func IsInstanceTypeClusterQueue(cq *kueue.ClusterQueue) bool {
	return systemmeta.MatchResource(cq, _ClusterQueueResType)
}

const (
	_TASQueueAnnotation                    = "topology.gpustack.ai/tas-queue"
	_TASQueueMigrationPhaseAnnotation      = "topology.gpustack.ai/migration-phase"
	_TASQueueMigrationStopPolicyAnnotation = "topology.gpustack.ai/migration-stop-policy"
	_TASQueueMigrationPhaseDraining        = "draining"
	_TASQueueMigrationPhaseSwitched        = "switched"
	_TASQueueMigrationStopPolicyUnset      = "unset"
	_TASQueueEmptyPlanHoldAnnotation       = "topology.gpustack.ai/empty-plan-hold"
	_TASQueueMigrationRequeueAfter         = 5 * time.Second
	_maxQueueFlavors                       = 64

	nodeQueueConditionTopologyReady = apistatus.ClusterQueueConditionTopologyReady
)

type nodeQueueValidationError struct {
	reason  string
	message string
}

func (e *nodeQueueValidationError) Error() string { return e.message }

func (r *NodeQueueReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := ctrllog.FromContext(ctx)

	// Fetch.
	cq := new(kueue.ClusterQueue)
	err := r.Client.Get(ctx, req.NamespacedName, cq,
		ctrlclix.WithoutQuorum)
	if err != nil {
		if !kerrors.IsNotFound(err) {
			logger.Error(err, "fetch cluster queue")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// The queue is being deleted (an admin's delete, or the InstanceType teardown). Kueue holds
	// its own ResourceInUse finalizer until the queue is empty but never evicts admitted
	// workloads on its own, so drive HoldAndDrain to evict them; once every reservation clears
	// Kueue drops its finalizer and removes the queue. This drain is unconditional —
	// instance-type-drain-when-no-flavors gates only the no-flavors auto-drain, not an explicit
	// delete. We keep no finalizer of our own: Kueue's finalizer holds the queue alive while
	// workloads remain, which is exactly the window this write needs.
	if cq.DeletionTimestamp != nil {
		if ptr.Deref(cq.Spec.StopPolicy, kueue.None) == kueue.HoldAndDrain {
			return ctrl.Result{}, nil
		}
		cq.Spec.StopPolicy = ptr.To(kueue.HoldAndDrain)
		if err = r.Client.Update(ctx, cq); err != nil {
			// Every write of the queue in this reconciler requeues on a conflict. The usual cause is
			// Kueue's status churn, which the predicate drops, so no event may follow the conflict.
			return objectWriteResult(logger, err, "hold and drain deleting cluster queue", _requeueAfterConflict)
		}
		logger.V(2).Info("holding and draining deleting cluster queue")
		return ctrl.Result{}, nil
	}

	// The pool's ResourceFlavors, matched by the queue's schedule labels (feature key + os +
	// arch). Admin queue names are arbitrary, so the pool is resolved by labels, not by name.
	lbs := nodefeature.PoolFlavorSelector(cq.Labels)
	if len(lbs) == 0 {
		return ctrl.Result{}, nil
	}
	// Scope to operator-owned flavors: a foreign ResourceFlavor that happens to share the pool's
	// schedule labels must not pollute the queue's quota (mirrors the InstanceType webhook selector).
	lbs[systemmeta.ResourceTypeLabel] = _ResourceFlavorResType
	rfList := new(kueue.ResourceFlavorList)
	err = r.Client.List(ctx, rfList,
		ctrlcli.MatchingLabels(lbs),
		ctrlcli.UnsafeDisableDeepCopy)
	if err != nil {
		logger.Error(err, "list resource flavors by cluster queue")
		return ctrl.Result{}, err
	}

	// Drop ResourceFlavors Kueue is still finalizing (DeletionTimestamp set): the NodeFlavor
	// reconciler deleted them because their nodes left the pool, but Kueue holds each one's
	// resource-in-use finalizer until no ClusterQueue references it, and removes it only on a
	// ClusterQueue update that drops the reference. Keeping a mid-deletion flavor in the resource
	// groups re-holds that finalizer and deadlocks its removal, so it is treated as absent from the
	// desired plan. NodeQueue drains the ClusterQueue before dropping a flavor Kueue may still hold
	// quota on because removing a flavor does not itself evict or re-admit Workloads. An
	// all-terminating pool falls through to the no-flavors drain/empty path.
	live := rfList.Items[:0]
	for i := range rfList.Items {
		if rfList.Items[i].DeletionTimestamp == nil {
			live = append(live, rfList.Items[i])
		}
	}
	rfList.Items = live

	if len(rfList.Items) > 0 {
		return r.fillClusterQueue(ctx, cq, rfList)
	}
	return r.drainOrEmptyClusterQueue(ctx, cq)
}

// fillClusterQueue converges the resource groups from the pool's flavors (smallest per-node
// count first) and reactivates a queue that had been drained to empty.
func (r *NodeQueueReconciler) fillClusterQueue(
	ctx context.Context, cq *kueue.ClusterQueue, rfList *kueue.ResourceFlavorList,
) (ctrl.Result, error) {
	// Implement this function in chapter 06.
	var result0 ctrl.Result
	var result1 error
	return result0, result1
}

func (r *NodeQueueReconciler) migrateClusterQueueResourceGroups(
	ctx context.Context,
	cq *kueue.ClusterQueue,
	desired []kueue.ResourceGroup,
	changed bool,
) (ctrl.Result, error) {
	logger := ctrllog.FromContext(ctx)
	phase := cq.Annotations[_TASQueueMigrationPhaseAnnotation]

	if phase == "" {
		cq.Annotations[_TASQueueMigrationPhaseAnnotation] = _TASQueueMigrationPhaseDraining
		cq.Annotations[_TASQueueMigrationStopPolicyAnnotation] = encodeStopPolicy(cq.Spec.StopPolicy)
		cq.Spec.StopPolicy = ptr.To(kueue.HoldAndDrain)
		if err := r.Client.Update(ctx, cq); err != nil {
			return objectWriteResult(logger, err, "start cluster queue topology migration", _requeueAfterConflict)
		}
		if err := r.setTopologyReadyCondition(ctx, cq, false, "Migrating", "holding and draining before switching topology flavors"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: _TASQueueMigrationRequeueAfter}, nil
	}

	if phase != _TASQueueMigrationPhaseDraining && phase != _TASQueueMigrationPhaseSwitched {
		return r.rejectClusterQueue(ctx, cq, &nodeQueueValidationError{
			reason:  "InvalidMigrationState",
			message: fmt.Sprintf("ClusterQueue has unsupported topology migration phase %q", phase),
		})
	}
	if _, failure := decodeStopPolicy(cq.Annotations[_TASQueueMigrationStopPolicyAnnotation]); failure != nil {
		return r.rejectClusterQueue(ctx, cq, failure)
	}
	if ptr.Deref(cq.Spec.StopPolicy, kueue.None) != kueue.HoldAndDrain {
		cq.Spec.StopPolicy = ptr.To(kueue.HoldAndDrain)
		changed = true
	}
	if changed {
		if err := r.Client.Update(ctx, cq); err != nil {
			return objectWriteResult(logger, err, "maintain held cluster queue topology migration", _requeueAfterConflict)
		}
		return ctrl.Result{RequeueAfter: _TASQueueMigrationRequeueAfter}, nil
	}
	if !clusterQueueStoppedAtCurrentGeneration(cq) || hasReserved(cq) {
		return ctrl.Result{RequeueAfter: _TASQueueMigrationRequeueAfter}, nil
	}

	if phase == _TASQueueMigrationPhaseDraining || !kubemeta.DeepEqual(cq.Spec.ResourceGroups, desired) {
		cq.Spec.ResourceGroups = desired
		cq.Annotations[_TASQueueMigrationPhaseAnnotation] = _TASQueueMigrationPhaseSwitched
		if err := r.Client.Update(ctx, cq); err != nil {
			return objectWriteResult(logger, err, "switch held cluster queue topology flavors", _requeueAfterConflict)
		}
		return ctrl.Result{RequeueAfter: _TASQueueMigrationRequeueAfter}, nil
	}

	// A queue without resource groups declares no resource, and Kueue ignores undeclared
	// resources (quotaCheckStrategy IgnoreUndeclared), so restoring an admitting stop policy here
	// would admit every Workload with no flavor and therefore no AdmissionCheck. The emptied queue
	// stays held and keeps its migration annotations until a returning flavor switches it to a
	// non-empty plan.
	if len(desired) == 0 {
		return ctrl.Result{}, r.setTopologyReadyCondition(ctx, cq, true, "Ready", "all queue flavors are topology-aware and quota is conserved")
	}

	restored, _ := decodeStopPolicy(cq.Annotations[_TASQueueMigrationStopPolicyAnnotation])
	cq.Spec.StopPolicy = restored
	delete(cq.Annotations, _TASQueueMigrationPhaseAnnotation)
	delete(cq.Annotations, _TASQueueMigrationStopPolicyAnnotation)
	if err := r.Client.Update(ctx, cq); err != nil {
		return objectWriteResult(logger, err, "restore cluster queue after topology migration", _requeueAfterConflict)
	}
	return ctrl.Result{}, r.setTopologyReadyCondition(ctx, cq, true, "Ready", "all queue flavors are topology-aware and quota is conserved")
}

func encodeStopPolicy(policy *kueue.StopPolicy) string {
	if policy == nil {
		return _TASQueueMigrationStopPolicyUnset
	}
	return string(*policy)
}

func decodeStopPolicy(value string) (*kueue.StopPolicy, *nodeQueueValidationError) {
	if value == _TASQueueMigrationStopPolicyUnset {
		return nil, nil
	}
	policy := kueue.StopPolicy(value)
	switch policy {
	case kueue.None, kueue.Hold, kueue.HoldAndDrain:
		return ptr.To(policy), nil
	default:
		return nil, &nodeQueueValidationError{
			reason:  "InvalidMigrationState",
			message: fmt.Sprintf("ClusterQueue has unsupported saved stop policy %q", value),
		}
	}
}

func clusterQueueStoppedAtCurrentGeneration(cq *kueue.ClusterQueue) bool {
	condition := apimeta.FindStatusCondition(cq.Status.Conditions, kueue.ClusterQueueActive)
	return clusterQueueObservedAtCurrentGeneration(cq) &&
		condition.Status == meta.ConditionFalse &&
		condition.Reason == kueue.ClusterQueueActiveReasonStopped
}

// clusterQueueObservedAtCurrentGeneration reports whether Kueue has written the queue status for
// the current spec. Kueue sets the Active condition's observedGeneration in the same status write
// as the reservation and usage counters, so the counters are current only when this holds.
func clusterQueueObservedAtCurrentGeneration(cq *kueue.ClusterQueue) bool {
	condition := apimeta.FindStatusCondition(cq.Status.Conditions, kueue.ClusterQueueActive)
	return condition != nil && condition.ObservedGeneration >= cq.Generation
}

func (r *NodeQueueReconciler) rejectClusterQueue(
	ctx context.Context, cq *kueue.ClusterQueue, failure *nodeQueueValidationError,
) (ctrl.Result, error) {
	ctrllog.FromContext(ctx).Error(failure, "reject topology-aware cluster queue")
	// A refused plan leaves the last complete one serving; a queue that has none must not admit
	// meanwhile.
	if err := r.holdEmptyClusterQueue(ctx, cq); err != nil {
		return ctrl.Result{}, err
	}
	result := ctrl.Result{}
	// NodeQueue does not watch Nodes: a missing Topology or a flavor that does not yet count every
	// pool Node heals only through a later ResourceFlavor event, so both are retried.
	if failure.reason == "MissingTopology" || failure.reason == "NonConservedQuota" ||
		cq.Annotations[_TASQueueMigrationPhaseAnnotation] != "" {
		result.RequeueAfter = 30 * time.Second
	}
	return result, r.setTopologyReadyCondition(ctx, cq, false, failure.reason, failure.message)
}

func (r *NodeQueueReconciler) setTopologyReadyCondition(
	ctx context.Context, cq *kueue.ClusterQueue, ready bool, reason, message string,
) error {
	status := meta.ConditionFalse
	if ready {
		status = meta.ConditionTrue
	}
	return r.setTopologyReadyConditionStatus(ctx, cq, status, reason, message)
}

func (r *NodeQueueReconciler) setTopologyReadyConditionStatus(
	ctx context.Context, cq *kueue.ClusterQueue, status meta.ConditionStatus, reason, message string,
) error {
	before := cq.DeepCopy()
	nodeQueueConditionTopologyReady.Status(cq, string(status), reason, message)
	if kubemeta.DeepEqual(before.Status, cq.Status) {
		return nil
	}
	return r.Client.Status().Patch(ctx, cq, ctrlcli.MergeFrom(before))
}

// drainOrEmptyClusterQueue handles a queue whose pool has lost all its flavors: it empties the
// quota through the held migration, switching only once every reservation has cleared so Kueue
// never counts negative, and leaves the emptied queue held. The drain setting decides only whether
// remaining reservations are drained (HoldAndDrain) or waited out without holding; a queue with
// nothing reserved is always held before it is emptied, and an already-empty queue is a no-op.
func (r *NodeQueueReconciler) drainOrEmptyClusterQueue(
	ctx context.Context, cq *kueue.ClusterQueue,
) (ctrl.Result, error) {
	if cq.Annotations[_TASQueueMigrationPhaseAnnotation] != "" {
		return r.migrateClusterQueueResourceGroups(ctx, cq, nil, false)
	}
	if len(cq.Spec.ResourceGroups) == 0 {
		return ctrl.Result{}, r.holdEmptyClusterQueue(ctx, cq)
	}

	drain := settings.InstanceTypeDrainWhenNoFlavors.ShouldValueBool(ctx)
	if !drain && hasReserved(cq) {
		// Without automatic drain, wait for reservations to clear on their own.
		return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
	}

	if cq.Annotations == nil {
		cq.Annotations = make(map[string]string)
	}
	// Emptying the last flavor reference is the same identity-stable plan migration as a
	// profile replacement, also when nothing is reserved: holding first closes the race in which a
	// reservation lands between the zero check and the switch. The migration marker preserves the
	// prior stop policy and lets a returning flavor join the in-progress plan before admission is
	// restored.
	return r.migrateClusterQueueResourceGroups(ctx, cq, nil, false)
}

// holdEmptyClusterQueue puts a queue that has no resource groups and is not stopped on Hold, and
// marks the Hold so that neither reconciler releases it until fillClusterQueue drops the marker
// with the queue's first resource groups. Such a queue
// declares no resource, and Kueue ignores undeclared resources (quotaCheckStrategy
// IgnoreUndeclared), so it would admit every Workload with no flavor and therefore no
// AdmissionCheck. It is Hold, not HoldAndDrain: the queue reserves no quota to drain, and
// HoldAndDrain would also stop the Instances of its InstanceType. A queue in the flavor migration
// is held by the migration instead, and an admin Hold is left alone.
func (r *NodeQueueReconciler) holdEmptyClusterQueue(ctx context.Context, cq *kueue.ClusterQueue) error {
	if len(cq.Spec.ResourceGroups) != 0 || cq.Annotations[_TASQueueMigrationPhaseAnnotation] != "" ||
		ptr.Deref(cq.Spec.StopPolicy, kueue.None) != kueue.None {
		return nil
	}
	if cq.Annotations == nil {
		cq.Annotations = make(map[string]string)
	}
	cq.Annotations[_TASQueueEmptyPlanHoldAnnotation] = "true"
	cq.Spec.StopPolicy = ptr.To(kueue.Hold)
	if err := r.Client.Update(ctx, cq); err != nil {
		ctrllog.FromContext(ctx).Error(err, "hold cluster queue without resource groups")
		return err
	}
	ctrllog.FromContext(ctx).V(2).Info("held cluster queue without resource groups")
	return nil
}

// hasReserved reports whether the ClusterQueue still holds reserved quota or
// admitted/reserving workloads. The queue must not be emptied or deleted until Kueue has
// finished draining; the workload counters guard against the flavor reservation snapshot
// momentarily reading zero while eviction is still in flight. Pending workloads are
// intentionally not counted — they hold no reservation, and gating on them would block
// deletion forever.
func hasReserved(cq *kueue.ClusterQueue) bool {
	if cq.Status.ReservingWorkloads != 0 || cq.Status.AdmittedWorkloads != 0 {
		return true
	}
	return slicex.Any(cq.Status.FlavorsReservation, func(i int) bool {
		return slicex.Any(cq.Status.FlavorsReservation[i].Resources, func(j int) bool {
			return !cq.Status.FlavorsReservation[i].Resources[j].Total.IsZero() ||
				!cq.Status.FlavorsReservation[i].Resources[j].Borrowed.IsZero()
		})
	})
}

// nodeQueueAdmissionCheck reports whether an AdmissionCheck is one a queue can reference: the
// node-devices check or the joint-admission check. Either one turning Active, going inactive, or
// being deleted changes what the queue's reference should be.
func nodeQueueAdmissionCheck(ac *kueue.AdmissionCheck) bool {
	return ac.Spec.ControllerName == _NodeDevicesControllerName ||
		ac.Spec.ControllerName == _JointAdmissionControllerName
}

func (r *NodeQueueReconciler) SetupController(_ context.Context, opts controller.SetupOptions) error {
	r.Client = opts.Manager.GetClient()

	dedupWindow := ctrlhandlerx.NewDedupWindow[ctrlreconcile.Request]()

	return ctrl.NewControllerManagedBy(opts.Manager).
		Named("nodequeue").
		For(
			// Reconcile each operator-owned ClusterQueue by its own name. The reconcile is
			// idempotent, so the operator's own writes settle without looping.
			&kueue.ClusterQueue{},
			ctrlbuilder.WithPredicates(
				// Interested in relevant ClusterQueue objects (WithPredicates ANDs, so this
				// gates every event below).
				ctrlpredicate.NewPredicateFuncs(func(obj ctrlcli.Object) bool {
					return systemmeta.MatchResource(obj, _ClusterQueueResType)
				}),
				// Trigger reconciliation when a ClusterQueue is:
				// - created (incl. the start-up resync).
				// - updated if its generation (spec) changed, or its DeletionTimestamp was set
				//   (so an explicit delete is drained).
				// Never react to the final deletion (a gone queue has nothing to reconcile) or to
				// status churn (reservation counters, conditions), which Kueue writes constantly.
				ctrlpredicate.Funcs{
					DeleteFunc: func(ctrlevent.DeleteEvent) bool { return false },
					UpdateFunc: func(e ctrlevent.UpdateEvent) bool {
						oldCq, newCq := e.ObjectOld.(*kueue.ClusterQueue), e.ObjectNew.(*kueue.ClusterQueue)
						if !oldCq.DeletionTimestamp.Equal(newCq.DeletionTimestamp) {
							return true
						}
						return oldCq.Generation != newCq.Generation
					},
				},
			),
		).
		Watches(
			// Re-converge the quota of every ClusterQueue a flavor feeds when the pool gains or
			// loses flavors.
			&kueue.ResourceFlavor{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFuncWithWindow(
				3*time.Second,
				dedupWindow,
				r.enqueueNodeQueueWhenResourceFlavorChanged,
			),
			ctrlbuilder.WithPredicates(
				// Trigger reconciliation when a relevant ResourceFlavor is created, updated, or
				// deleted — any of them changes a pool's flavor set.
				ctrlpredicate.NewPredicateFuncs(func(obj ctrlcli.Object) bool {
					return systemmeta.MatchResource(obj, _ResourceFlavorResType)
				}),
			),
		).
		Watches(
			// Re-enqueue the operator-owned queues when either AdmissionCheck a queue can reference
			// changes, so each queue acquires the reference once the check turns Active (or drops
			// it should the check go inactive/away).
			&kueue.AdmissionCheck{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFuncWithWindow(
				3*time.Second,
				dedupWindow,
				r.enqueueNodeQueuesWhenAdmissionCheckChanged,
			),
			ctrlbuilder.WithPredicates(
				// Trigger reconciliation when the node-devices or joint-admission AdmissionCheck is
				// created, updated, or deleted (its Active state gates the reference).
				ctrlpredicate.NewPredicateFuncs(func(obj ctrlcli.Object) bool {
					return nodeQueueAdmissionCheck(obj.(*kueue.AdmissionCheck))
				}),
			),
		).
		Complete(r)
}

// enqueueNodeQueueWhenResourceFlavorChanged enqueues every operator-owned ClusterQueue whose
// flavor selector this changed flavor feeds. The flavor is the finest grain (it always carries
// the CPU key), whereas a queue's pool may be collapsed and carry fewer discriminators, so a
// MatchingLabels query keyed on the flavor's labels would miss a collapsed queue. Instead it
// lists the operator queues and keeps those whose own poolFlavorSelector is a subset of the
// flavor's discriminators.
func (r *NodeQueueReconciler) enqueueNodeQueueWhenResourceFlavorChanged(
	ctx context.Context, obj ctrlcli.Object,
) []ctrlreconcile.Request {
	logger := ctrllog.FromContext(ctx).
		WithValues("resource flavor", ctrlcli.ObjectKeyFromObject(obj))

	rfSel := nodefeature.PoolFlavorSelector(obj.GetLabels())
	if len(rfSel) == 0 {
		return nil
	}

	// Narrow to the operator-owned queues server-side via the resource-type label instead of
	// listing every ClusterQueue. The subset match below still runs in-memory: a collapsed queue
	// carries fewer discriminators than the flavor, so a labels-keyed query on the flavor's own
	// labels would miss it.
	cqList := new(kueue.ClusterQueueList)
	err := r.Client.List(ctx, cqList,
		systemmeta.GetResourcesLabelSetOfType[ctrlcli.MatchingLabels](_ClusterQueueResType),
		ctrlclix.WithoutQuorum,
		ctrlcli.UnsafeDisableDeepCopy)
	if err != nil {
		logger.Error(err, "list cluster queues for resource flavor")
		return nil
	}

	var reqs []ctrlreconcile.Request
	for i := range cqList.Items {
		cq := &cqList.Items[i]
		cqSel := nodefeature.PoolFlavorSelector(cq.Labels)
		if len(cqSel) == 0 || !mapx.Contain(rfSel, cqSel) {
			continue
		}
		reqs = append(reqs, ctrlreconcile.Request{
			NamespacedName: ctrlcli.ObjectKey{Name: cq.Name},
		})
	}
	if len(reqs) == 0 {
		return nil
	}

	logger.V(2).Info("enqueued node queues from resource flavor", "requests", reqs)
	return reqs
}

// enqueueNodeQueuesWhenAdmissionCheckChanged enqueues every operator-owned ClusterQueue when
// the node-devices or joint-admission AdmissionCheck changes, so each queue (re)acquires the
// reference once the check turns Active (or drops it should the check go inactive/away).
func (r *NodeQueueReconciler) enqueueNodeQueuesWhenAdmissionCheckChanged(
	ctx context.Context, obj ctrlcli.Object,
) []ctrlreconcile.Request {
	logger := ctrllog.FromContext(ctx).
		WithValues("admission check", ctrlcli.ObjectKeyFromObject(obj))

	if !nodeQueueAdmissionCheck(obj.(*kueue.AdmissionCheck)) {
		return nil
	}

	cqList := new(kueue.ClusterQueueList)
	if err := r.Client.List(ctx, cqList, ctrlclix.WithoutQuorum, ctrlcli.UnsafeDisableDeepCopy); err != nil {
		logger.Error(err, "list cluster queues for admission check")
		return nil
	}

	var reqs []ctrlreconcile.Request
	for i := range cqList.Items {
		cq := &cqList.Items[i]
		if !systemmeta.MatchResource(cq, _ClusterQueueResType) || cq.DeletionTimestamp != nil {
			continue
		}
		reqs = append(reqs, ctrlreconcile.Request{
			NamespacedName: ctrlcli.ObjectKey{Name: cq.Name},
		})
	}
	if len(reqs) == 0 {
		return nil
	}

	logger.V(2).Info("enqueued node queues from admission check", "requests", reqs)
	return reqs
}
