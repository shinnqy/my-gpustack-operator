package worker

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	ctrl "sigs.k8s.io/controller-runtime"
	ctrlbuilder "sigs.k8s.io/controller-runtime/pkg/builder"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlevent "sigs.k8s.io/controller-runtime/pkg/event"
	ctrlhandler "sigs.k8s.io/controller-runtime/pkg/handler"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	ctrlpredicate "sigs.k8s.io/controller-runtime/pkg/predicate"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/controller"
	"gpustack.ai/gpustack/pkg/kubeapistatus"
)

const (
	// IndexingKVCachePoolBindingByPool indexes each KVCachePoolBinding by the pool it names. It is
	// what makes "the Bindings of this pool" one scoped query rather than a list of every Binding in
	// the cluster filtered in memory — a pass runs it on every reconcile, so the difference is per
	// pass rather than per startup.
	IndexingKVCachePoolBindingByPool = "kvcachepoolbindings.worker.gpustack.ai/pool"

	// IndexingKVCachePoolByBackend indexes each KVCachePool by the backends it draws from.
	//
	// It exists because the tenant ledger and the policy file converge per MASTER and not per pool:
	// several pools may sit on one backend, and a pass that wrote only its own pool's tenants would
	// erase the others' on every reconcile. Finding those sibling pools has to be one query for the
	// same reason above — it happens on every pass, on both of them.
	IndexingKVCachePoolByBackend = "kvcachepools.worker.gpustack.ai/backend"
)

// indexKVCachePoolBindingByPool is the field-index extractor for IndexingKVCachePoolBindingByPool.
//
// A Binding whose pool does not exist is still indexed. That is deliberate: the pool may be created
// afterwards, and the index is what carries the Binding into the pass that follows — dropping it
// here would leave a Binding that named a pool early permanently invisible to it.
func indexKVCachePoolBindingByPool(obj ctrlcli.Object) []string {
	kvcpb, ok := obj.(*workercore.KVCachePoolBinding)
	if !ok || kvcpb == nil || kvcpb.Spec.PoolRef.Name == "" {
		return nil
	}
	return []string{kvcpb.Spec.PoolRef.Name}
}

// indexKVCachePoolByBackend is the field-index extractor for IndexingKVCachePoolByBackend.
//
// It indexes every entry of spec.backends rather than the first, although admission admits exactly
// one: the index has to describe what is STORED, and an object stored before that rule tightened, or
// written by a client that bypassed the webhook, would otherwise be missing from the very query its
// siblings use to find each other.
func indexKVCachePoolByBackend(obj ctrlcli.Object) []string {
	kvcp, ok := obj.(*workercore.KVCachePool)
	if !ok || kvcp == nil {
		return nil
	}

	names := make([]string, 0, len(kvcp.Spec.Backends))
	for _, name := range kvcp.Spec.Backends {
		if name != "" {
			names = append(names, name)
		}
	}
	return names
}

// The phases a KVCachePool reports, derived from what was observed rather than from what was asked
// for: a pool whose backend has published no address is Provisioning however complete its spec is.
const (
	KVCachePoolPhaseProvisioning = "Provisioning"
	KVCachePoolPhaseReady        = "Ready"
	KVCachePoolPhaseError        = "Error"
	KVCachePoolPhaseDeleting     = "Deleting"
)

// The condition types a KVCachePool reports, one per axis.
//
// They are spelled POSITIVELY, as every condition in this repository is and as the Kubernetes API
// conventions ask: the type names the healthy state and False carries the fault. The spec names two
// faults — MultiTenancyDisabled and QuotaPolicyNotWritable — and those are REASONS on the two
// conditions below rather than condition types of their own, because a type that is True when
// something is wrong reads backwards from every other condition an operator sees on this cluster.
const (
	// KVCachePoolConditionBackendResolved is False while the backend cannot be read or has published
	// no address to reach it on.
	KVCachePoolConditionBackendResolved kubeapistatus.ConditionType = "BackendResolved"
	// KVCachePoolConditionQuotaLedgerAvailable is False when the master holds no tenant ledger to
	// write into — reason MultiTenancyDisabled, which is F5's precondition observed at runtime
	// rather than assumed from admission.
	KVCachePoolConditionQuotaLedgerAvailable kubeapistatus.ConditionType = "QuotaLedgerAvailable"
	// KVCachePoolConditionQuotaPolicyWritable is False when the master accepted no quota write —
	// reason QuotaPolicyNotWritable when its own policy source is what refused it.
	KVCachePoolConditionQuotaPolicyWritable kubeapistatus.ConditionType = "QuotaPolicyWritable"
	// KVCachePoolConditionQuotaWithinTotal is False when the ceilings this pool's Bindings ask for
	// together exceed the total the pool itself declared — reason Oversubscribed, naming the sum and
	// the total both. False is NOT a fault and nothing is broken: the store grants every tenant a
	// share of what it can serve in proportion to what each asked, and each Binding's
	// status.effectiveQuota reports the figure actually granted. It is a Condition and not a refusal
	// because refusing would break the ordinary sequence of creating Bindings and then growing the
	// backend; admission refuses only the one ceiling that could never be granted alone.
	//
	// It is deliberately absent from the axes summarizeKVCachePool reads, so an oversubscribed pool
	// does not read as Error: what False names is a proportional division the store performs by
	// design, and a pool telling an operator to fix what is working is the reading this spelling
	// exists to avoid.
	KVCachePoolConditionQuotaWithinTotal kubeapistatus.ConditionType = "QuotaWithinTotal"
	// KVCachePoolConditionCapacityAllocatable is False when the master reports nothing to allocate.
	// That is the startup-ordering trap: no member has mounted, so every effective quota is zero and
	// no write can succeed while every object still looks correctly configured.
	KVCachePoolConditionCapacityAllocatable kubeapistatus.ConditionType = "CapacityAllocatable"
	// KVCachePoolConditionReleasable is False only while a deletion is being HELD, and it is
	// deliberately absent from the axes summarizeKVCachePool reads: a live pool never carries it, and
	// an axis nobody has observed reads as Provisioning to that function.
	KVCachePoolConditionReleasable kubeapistatus.ConditionType = "Releasable"
)

// The reasons the spec names by name. They are constants because criterion 11 asserts them and an
// e2e case greps for them.
const (
	KVCachePoolReasonMultiTenancyDisabled   = "MultiTenancyDisabled"
	KVCachePoolReasonQuotaPolicyNotWritable = "QuotaPolicyNotWritable"
	KVCachePoolReasonOversubscribed         = "Oversubscribed"
)

// KVCacheMasterSeparatesTenants reports whether the master serving a pool can hold two reuse
// domains apart. It is the renderer's question: a master that cannot separates no tenant names, so
// forwarding one would claim an isolation the store does not deliver.
//
// The OBSERVATION answers first — the pool's QuotaLedgerAvailable condition, which the reconciler
// writes from the master's own responses and which therefore covers an external master this
// operator never started. True separates; False with reason MultiTenancyDisabled does not. Every
// other reading (absent, Unknown, False for another cause) is no answer, and then the DECLARATION
// answers for a managed backend — exact, because this operator renders that flag onto the leader's
// own command line — while an unreadable external one keeps the tenant, matching the gates that
// hold a Pod off such a pool until it reports.
func KVCacheMasterSeparatesTenants(kvcb *workercore.KVCacheBackend, kvcp *workercore.KVCachePool) bool {
	condition := KVCachePoolConditionQuotaLedgerAvailable
	switch {
	case condition.IsTrue(kvcp):
		return true
	case condition.IsFalse(kvcp) && condition.GetReason(kvcp) == KVCachePoolReasonMultiTenancyDisabled:
		return false
	}
	if managed := kvcb.Spec.Connection.Managed; managed != nil {
		return managed.Leader.MultiTenancyEnabled()
	}
	return true
}

// KVCachePoolReconciler reconciles a KVCachePool.
//
// The pool is the ONLY reconciled object of this pair: a KVCachePoolBinding has no reconciler of its
// own, and every Binding event is mapped onto the pool it names. That is not a simplification, it is
// what makes the pass correct — the tenant ledger and the policy file are per master, so writing
// them from a per-Binding loop would have each Binding's pass erase what the others just wrote.
type KVCachePoolReconciler struct {
	Client ctrlcli.Client

	// AdminHTTP reads and writes the leader's admin surface. It carries its own timeout for the
	// reason the backend reconciler's does: a master that accepts a connection and then stalls would
	// otherwise hold this reconcile open for as long as the transport allows.
	AdminHTTP *http.Client
}

func (r *KVCachePoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// Implement this function in chapter 22.
	var result0 ctrl.Result
	var result1 error
	return result0, result1
}

// kvCachePoolPredicate decides which events about the pool itself are worth a pass.
//
// It triggers on a create, on a generation (spec) change, and on the object being marked for
// deletion. It refuses the final removal — there is nothing left to converge — and, load-bearingly,
// it refuses this operator's OWN writes: a status update and a finalizer edit change neither the
// generation nor the deletion timestamp, so without this every pass would schedule the next one and
// the loop would never settle.
func kvCachePoolPredicate() ctrlpredicate.Predicate {
	return ctrlpredicate.Funcs{
		DeleteFunc: func(ctrlevent.DeleteEvent) bool { return false },
		UpdateFunc: func(e ctrlevent.UpdateEvent) bool {
			oldKvcp, ok := e.ObjectOld.(*workercore.KVCachePool)
			if !ok {
				return true
			}
			newKvcp, ok := e.ObjectNew.(*workercore.KVCachePool)
			if !ok {
				return true
			}
			if !oldKvcp.DeletionTimestamp.Equal(newKvcp.DeletionTimestamp) {
				return true
			}
			return oldKvcp.Generation != newKvcp.Generation
		},
	}
}

func (r *KVCachePoolReconciler) SetupController(ctx context.Context, opts controller.SetupOptions) error {
	// Configure field indexers. Both are registered here rather than where they are read, because
	// an index has to exist before the cache starts and a List against a missing one fails at
	// runtime rather than at compile time.
	fi := opts.Manager.GetFieldIndexer()
	err := fi.IndexField(ctx, &workercore.KVCachePoolBinding{},
		IndexingKVCachePoolBindingByPool, indexKVCachePoolBindingByPool)
	if err != nil {
		return fmt.Errorf("index kv cache pool binding '%s': %w", IndexingKVCachePoolBindingByPool, err)
	}
	err = fi.IndexField(ctx, &workercore.KVCachePool{},
		IndexingKVCachePoolByBackend, indexKVCachePoolByBackend)
	if err != nil {
		return fmt.Errorf("index kv cache pool '%s': %w", IndexingKVCachePoolByBackend, err)
	}

	r.Client = opts.Manager.GetClient()
	if r.AdminHTTP == nil {
		r.AdminHTTP = newAdminHTTPClient()
	}

	return ctrl.NewControllerManagedBy(opts.Manager).
		Named("kvcachepool").
		For(
			// Reconcile each KVCachePool by its own name.
			//
			// Trigger on create, on a generation (spec) change, and on the object being marked for
			// deletion. Never on the final removal — there is nothing left to converge — and never
			// on this operator's own status or finalizer writes, which change neither the generation
			// nor the deletion timestamp and would otherwise make every pass schedule the next one.
			&workercore.KVCachePool{},
			ctrlbuilder.WithPredicates(kvCachePoolPredicate()),
		).
		Watches(
			// Every Binding event reaches the pool it names. A Binding is where a reuse domain and a
			// ceiling are declared, so its create, its edit and its deletion are all changes to what
			// the pool has to write into the master's ledger.
			//
			// Deletion is NOT filtered out here, unlike on the pool itself: a Binding that goes away
			// is a tenant whose entry has to be removed, and no other event says so.
			&workercore.KVCachePoolBinding{},
			ctrlhandler.EnqueueRequestsFromMapFunc(r.enqueueKVCachePoolWhenBindingChanged),
		).
		Complete(r)
}

// compareKVCacheObjectReferences orders a usedBy list. Kind first, because one list may hold several,
// then the namespace and the name that identify an object within it.
func compareKVCacheObjectReferences(a, b workercore.KVCacheObjectReference) int {
	if c := strings.Compare(a.Kind, b.Kind); c != 0 {
		return c
	}
	if c := strings.Compare(a.Namespace, b.Namespace); c != 0 {
		return c
	}
	return strings.Compare(a.Name, b.Name)
}

// The condition types a KVCachePoolBinding reports. Spelled positively, like the pool's.
const (
	// KVCachePoolBindingConditionDomainExclusive is False when another Binding claims the same reuse
	// domain — reason DomainClaimedByMultipleBindings, naming the other claimant.
	KVCachePoolBindingConditionDomainExclusive kubeapistatus.ConditionType = "DomainExclusive"
	// KVCachePoolBindingConditionQuotaObserved is False when the master's figures for this domain
	// could not be read, and True even when the master has never heard of the domain — that is an
	// observation, not a failure to observe.
	KVCachePoolBindingConditionQuotaObserved kubeapistatus.ConditionType = "QuotaObserved"
	// KVCachePoolBindingConditionQuotaGranted is False when the observed grant cannot serve a write:
	// no ledger entry, no exported figure, or a grant of zero. It is separate from QuotaObserved
	// because a grant of zero is a successful observation — the pair distinguishes "we could not
	// look" from "we looked, and there is nothing to write into".
	//
	// Without it a Binding reached Ready on observation alone, so a domain whose grant was zero — the
	// ordinary state for tens of seconds after its master restarts — reported Ready while no write
	// could succeed. That is precisely the reading a Binding exists to prevent.
	KVCachePoolBindingConditionQuotaGranted kubeapistatus.ConditionType = "QuotaGranted"
	// KVCachePoolBindingConditionReleasable is False while a workload in this namespace still holds
	// the pool through this Binding. Absent from the axes summarizeKVCachePoolBinding reads, for the
	// reason the pool's own is.
	KVCachePoolBindingConditionReleasable kubeapistatus.ConditionType = "Releasable"
)

// enqueueKVCachePoolWhenBindingChanged maps a Binding onto the pool it names.
//
// The pool is cluster-scoped, so the request carries a bare name and no namespace — a Binding from
// any namespace enqueues the same pool, which is the whole point of the pool being the single
// reconciled object.
//
// A Binding naming a pool that does not exist still enqueues it. The pass then finds nothing and
// returns, which costs one no-op reconcile and keeps the Binding from being invisible if the pool is
// created a moment later.
func (r *KVCachePoolReconciler) enqueueKVCachePoolWhenBindingChanged(
	ctx context.Context, obj ctrlcli.Object,
) []ctrlreconcile.Request {
	logger := ctrllog.FromContext(ctx).
		WithValues("kv cache pool binding", ctrlcli.ObjectKeyFromObject(obj))

	kvcpb, ok := obj.(*workercore.KVCachePoolBinding)
	if !ok || kvcpb.Spec.PoolRef.Name == "" {
		return nil
	}

	reqs := []ctrlreconcile.Request{
		{NamespacedName: ctrlcli.ObjectKey{Name: kvcpb.Spec.PoolRef.Name}},
	}

	logger.V(2).Info("enqueued kv cache pool from binding", "requests", reqs)
	return reqs
}
