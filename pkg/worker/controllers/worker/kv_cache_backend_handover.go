package worker

import (
	"context"
	"strings"
	"sync"

	coordination "k8s.io/api/coordination/v1"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/worker/kuberess"
	"gpustack.ai/gpustack/pkg/worker/kvcache/mooncake"
)

// leaseHolders remembers which replica each backend's lease named the last time this operator
// looked, so a change can be told from a renewal.
//
// It is IN MEMORY and deliberately not a field. A handover is a property of the transition, and a
// field recording it would be a copy of a value the Lease already publishes, kept only to be looked
// at -- which is the test three candidate fields have already failed in this design. The cost is
// stated rather than hidden: a handover that happens while this operator is restarting produces no
// Event, because nothing here observed the before. The Lease's own leaseTransitions is what survives
// that, and it is a count rather than an event.
//
// The held value NEVER leaves this map. Which replica holds the lease is a question this API does
// not answer, so the identity is read to compare and the Event says only that it moved.
//
// Its ZERO VALUE is usable, so a reconciler built without SetupController -- which is every test
// helper in this package -- observes handovers the same way the real one does rather than taking a
// different path through the code under test.
type leaseHolders struct {
	mu sync.Mutex
	by map[string]string
}

// enqueueKVCacheBackendWhenLeaseChanged maps a Lease back to the backend that elects through it.
//
// The Lease carries no resource note and no owner reference, because THE STORE creates it rather
// than this operator -- so the only link is the name, which is the backend's own with the leader
// suffix. That is a guess until the backend is read back, which is what this does: a lease belonging
// to something else that happens to fit the pattern names no backend and enqueues nothing.
func (r *KVCacheBackendReconciler) enqueueKVCacheBackendWhenLeaseChanged(
	ctx context.Context, obj ctrlcli.Object,
) []ctrlreconcile.Request {
	name, ok := strings.CutSuffix(obj.GetName(), mooncake.LeaderObjectNameSuffix)
	if !ok || name == "" {
		return nil
	}

	kvcb := new(workercore.KVCacheBackend)
	if err := r.Client.Get(ctx, ctrlcli.ObjectKey{Name: name}, kvcb); err != nil {
		return nil
	}
	if kvcb.Spec.Connection.Managed == nil ||
		!kvcb.Spec.Connection.Managed.Leader.KubernetesElectionEnabled() {
		return nil
	}

	return []ctrlreconcile.Request{{NamespacedName: ctrlcli.ObjectKey{Name: name}}}
}

// kvCacheBackendLeaseHolderChanged passes only the events that could BE a handover.
//
// This is a load-bearing filter rather than noise reduction. A lease is renewed every few seconds
// for the life of every leader, and each update reaching the reconciler would cost that backend
// three sequential HTTP reads against its admin surface -- so an unfiltered watch here would turn a
// steady cluster into a permanent load on every store in it.
//
// The holder is compared rather than any other field because it is the only one a handover moves:
// renewTime moves on every renewal and acquireTime moves with the holder.
func kvCacheBackendLeaseHolderChanged(old, updated ctrlcli.Object) bool {
	if updated.GetNamespace() != kuberess.SystemNamespaceName {
		return false
	}

	oldLease, ok := old.(*coordination.Lease)
	if !ok {
		return false
	}
	newLease, ok := updated.(*coordination.Lease)
	if !ok {
		return false
	}

	return leaseHolderIdentity(oldLease) != leaseHolderIdentity(newLease)
}

func leaseHolderIdentity(lease *coordination.Lease) string {
	if lease.Spec.HolderIdentity == nil {
		return ""
	}
	return *lease.Spec.HolderIdentity
}
