package worker

import (
	"context"
	"net/http"
	"time"

	apps "k8s.io/api/apps/v1"
	coordination "k8s.io/api/coordination/v1"
	core "k8s.io/api/core/v1"
	ctrlrecord "k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlbuilder "sigs.k8s.io/controller-runtime/pkg/builder"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlcontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	ctrlevent "sigs.k8s.io/controller-runtime/pkg/event"
	ctrlhandler "sigs.k8s.io/controller-runtime/pkg/handler"
	ctrlpredicate "sigs.k8s.io/controller-runtime/pkg/predicate"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/controller"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/systemmeta"
	"gpustack.ai/gpustack/pkg/utils/ctrlhandlerx"
	"gpustack.ai/gpustack/pkg/worker/kuberess"
	"gpustack.ai/gpustack/pkg/worker/kvcache"
	"gpustack.ai/gpustack/pkg/worker/kvcache/mooncake"
)

// KVCacheBackendReconciler reconciles a KVCacheBackend.
//
// It owns the object's lifecycle and, from later tasks on, the workloads that serve it:
//
//   - A live object is locked with the repository's own finalizer, so nothing it renders is
//     orphaned by a delete that races the reconcile.
//   - On delete the finalizer is held while status.usedBy names a consumer. The refusal is not a
//     requeue: usedBy is written by the consumer's own controller onto this object's status, and
//     that write is the event that brings the teardown back. A timer would only re-ask a question
//     whose answer has not changed.
//   - Status is written wholesale under a DeepEqual guard, so a settled backend produces no write
//     and therefore no event of its own.
type KVCacheBackendReconciler struct {
	Client ctrlcli.Client

	// AdminHTTP reads the leader's admin surface. It carries its own timeout, because a leader that
	// accepts a connection and then stalls would otherwise hold this reconcile open for as long as
	// the transport allows.
	//
	// The timeout bounds one stall; it does not isolate the others from it. That is what
	// kvCacheBackendConcurrency is for, and the two are only useful together.
	AdminHTTP *http.Client

	// Recorder publishes what a status field cannot hold. The disk tier's cleanup records against
	// the NODE rather than against this backend, because the backend is being deleted at the moment
	// there is something to say and would take the record with it.
	Recorder ctrlrecord.EventRecorder

	// leaseHolders is the only state this reconciler keeps between passes, and it keeps it because
	// the question it answers is about a TRANSITION: a lease that changed hands and one that was
	// merely renewed are the same object read twice. Its zero value is usable, so no construction
	// path has to remember it.
	leaseHolders leaseHolders
}

// adminReadTimeout bounds one read of the leader's admin surface. It is short: everything read there
// is served out of memory by a process in the same cluster, so a read that takes longer is a leader
// in trouble, and reporting that quickly is more useful than waiting for it.
const adminReadTimeout = 5 * time.Second

// newAdminHTTPClient builds the client every admin read goes through. One client for the
// controller's whole life, so its connection pool is reused across reconciles: building one per pass
// would reconnect to the same leader every time.
//
// Redirects are refused rather than followed. For an external backend the address is the spec
// author's, and the default client would follow a 302 from it to any host this operator can reach —
// reading it with the operator's network identity, and then copying an excerpt of the answer into a
// status that everyone who can read the object can read. The three reads are fixed paths on an
// address admission has already validated, so a redirect away from it is never something to honor;
// the response that carried it is what gets reported.
func newAdminHTTPClient() *http.Client {
	return &http.Client{
		Timeout: adminReadTimeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

var _ ctrlreconcile.Reconciler = (*KVCacheBackendReconciler)(nil)

func (r *KVCacheBackendReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// Implement this function in chapter 22.
	var result0 ctrl.Result
	var result1 error
	return result0, result1
}

// isTerminalWaitReason reports whether a waiting container is stuck rather than starting.
//
// The two excluded reasons are the kubelet's own progress states: a container being created, and a
// Pod whose init containers have not finished. Everything else it reports while waiting — every
// image pull failure, every backoff — is a state that persists until something outside this reconcile
// changes.
func isTerminalWaitReason(reason string) bool {
	switch reason {
	case "", "ContainerCreating", "PodInitializing":
		return false
	}
	return true
}

// podIsReady reports the Pod's own readiness, which for a member is the store client having mounted
// its segment and not merely started: the readiness probe connects to a port the entrypoint serves
// only after the mount. Without that probe this would be true the moment the container ran, and
// every member would spend its startup counted as a shortfall.
func podIsReady(pod *core.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == core.PodReady {
			return cond.Status == core.ConditionTrue
		}
	}
	return false
}

// alignRenderedContainer copies the rendered fields onto a running container and reports whether
// anything moved. The fields are named one by one because the ones NOT named here are the server's
// to default; overwriting them is what would make every pass a write.
//
// One function serves the leader and the members. A field only one of them uses compares equal on
// the other, and keeping the list in one place is what stops a field added to a renderer from being
// rendered but never converged.
func alignRenderedContainer(actual *core.Container, expected core.Container) (changed bool) {
	if actual.Image != expected.Image {
		actual.Image = expected.Image
		changed = true
	}
	// Unconditional, because the renderer resolves the policy rather than leaving it empty. An
	// empty expectation was the problem: the API server fills the field in on write, so comparing
	// against its default rolls the workload forever, and skipping the comparison to avoid that
	// left a stale policy standing — a value that was removed from the spec, or one derived from an
	// image tag the spec has since moved off. kvcache.EffectivePullPolicy owns that rule now.
	if actual.ImagePullPolicy != expected.ImagePullPolicy {
		actual.ImagePullPolicy = expected.ImagePullPolicy
		changed = true
	}
	// Safe to compare, unlike the pull policy: the renderer names a non-empty value, and the API
	// server only defaults an empty one.
	if actual.TerminationMessagePolicy != expected.TerminationMessagePolicy {
		actual.TerminationMessagePolicy = expected.TerminationMessagePolicy
		changed = true
	}
	if !kubemeta.DeepEqual(actual.Command, expected.Command) {
		actual.Command = expected.Command
		changed = true
	}
	if !kubemeta.DeepEqual(actual.Args, expected.Args) {
		actual.Args = expected.Args
		changed = true
	}
	if !kubemeta.DeepEqual(actual.Env, expected.Env) {
		actual.Env = expected.Env
		changed = true
	}
	if !kubemeta.DeepEqual(actual.Ports, expected.Ports) {
		actual.Ports = expected.Ports
		changed = true
	}
	if !kubemeta.DeepEqual(actual.ReadinessProbe, expected.ReadinessProbe) {
		actual.ReadinessProbe = expected.ReadinessProbe
		changed = true
	}
	if !kubemeta.DeepEqual(actual.LivenessProbe, expected.LivenessProbe) {
		actual.LivenessProbe = expected.LivenessProbe
		changed = true
	}
	// The whole value, not only Requests. The renderers set no limits at all, so a limit is a field
	// they leave at its zero value — and comparing only what a renderer fills in is exactly how an
	// injected one survives: a low memory limit OOM-kills the container on a loop, and nothing in
	// this object says why. The backend's own claim is a REQUEST by design, since a member sizes its
	// segment from the same figure and a limit under it would kill the member that fits.
	if !kubemeta.DeepEqual(actual.Resources, expected.Resources) {
		actual.Resources = expected.Resources
		changed = true
	}
	if !kubemeta.DeepEqual(actual.VolumeMounts, expected.VolumeMounts) {
		actual.VolumeMounts = expected.VolumeMounts
		changed = true
	}
	// The security context is compared as a whole rather than capability by capability: it is
	// rendered as a whole, and a running container that acquired one out of band must lose it.
	if !kubemeta.DeepEqual(actual.SecurityContext, expected.SecurityContext) {
		actual.SecurityContext = expected.SecurityContext
		changed = true
	}
	// The shutdown hook carries the scale-in grace INSIDE its argv, and that grace is editable. Left
	// out of this comparison the hook is written once and never again: the pod fingerprint moves
	// with it, so the members are deleted and recreated — from a template whose hook still holds the
	// old grace. The result is worse than not converging, because everything else says the change
	// took effect.
	if !kubemeta.DeepEqual(actual.Lifecycle, expected.Lifecycle) {
		actual.Lifecycle = expected.Lifecycle
		changed = true
	}
	return changed
}

// enqueueKVCacheBackendWhenPoolChanged maps a pool back to every backend it draws from.
//
// It reads the pool's spec rather than the backends' usedBy, because the event that matters most is
// the pool's DELETE — and a delete carries the object's last known state, which still names them.
func (r *KVCacheBackendReconciler) enqueueKVCacheBackendWhenPoolChanged(
	_ context.Context, obj ctrlcli.Object,
) []ctrlreconcile.Request {
	kvcp, ok := obj.(*workercore.KVCachePool)
	if !ok {
		return nil
	}

	reqs := make([]ctrlreconcile.Request, 0, len(kvcp.Spec.Backends))
	for _, name := range kvcp.Spec.Backends {
		reqs = append(reqs, ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKey{Name: name}})
	}
	return reqs
}

// enqueueKVCacheBackendWhenWorkloadChanged maps a rendered object back to the backend that owns it.
//
// It reads the backend's name from the object's own resource note rather than walking its owner
// references: the note is what the predicate has already filtered on, so the two agree by
// construction, and a delete event carries the note as readily as an update does.
func (r *KVCacheBackendReconciler) enqueueKVCacheBackendWhenWorkloadChanged(
	_ context.Context, obj ctrlcli.Object,
) []ctrlreconcile.Request {
	name := systemmeta.DescribeResourceNote(obj, kvcache.ResourceNoteBackend)
	if name == "" {
		return nil
	}
	return []ctrlreconcile.Request{{NamespacedName: ctrlcli.ObjectKey{Name: name}}}
}

// kvCacheBackendWorkloadPredicate keeps the watches to this operator's own objects. Without it every
// Deployment and Service in the cluster would wake this reconciler.
//
// The namespace is half of the test, and it is the half that is not about noise. A resource note is
// an annotation, so anyone who can create an object in ANY namespace can write one that matches —
// and each match costs the named backend a reconcile, which is three HTTP reads against its admin
// surface. Rendering only ever happens in the system namespace, so nothing outside it can be one of
// this operator's objects, whatever it says about itself.
func kvCacheBackendWorkloadPredicate() ctrlpredicate.Predicate {
	return ctrlpredicate.NewPredicateFuncs(func(obj ctrlcli.Object) bool {
		return obj.GetNamespace() == kuberess.SystemNamespaceName &&
			systemmeta.MatchResource(obj, kvcache.ResourceType)
	})
}

func (r *KVCacheBackendReconciler) SetupController(_ context.Context, opts controller.SetupOptions) error {
	r.Client = opts.Manager.GetClient()
	r.Recorder = opts.Manager.GetEventRecorderFor("kvcachebackend")
	if r.AdminHTTP == nil {
		r.AdminHTTP = newAdminHTTPClient()
	}

	dedupWindow := ctrlhandlerx.NewDedupWindow[ctrlreconcile.Request]()

	return ctrl.NewControllerManagedBy(opts.Manager).
		Named("kvcachebackend").
		For(&workercore.KVCacheBackend{}).
		Watches(
			// A pool claims this backend by writing itself into status.usedBy, and its own reconciler
			// clears that entry on the way out — so ordinarily this backend is woken by its own status
			// changing and this watch adds nothing. What it covers is the pool going away WITHOUT
			// clearing the entry, which nothing else can wake: the claim would name an object that no
			// longer exists and hold the teardown with no event ever arriving to re-examine it.
			//
			// Not deduped, unlike the workload watches. Those fire per rendered object and collapse
			// onto one backend; this one fires per pool, and a pool names each of its backends once.
			&workercore.KVCachePool{},
			ctrlhandler.EnqueueRequestsFromMapFunc(r.enqueueKVCacheBackendWhenPoolChanged),
		).
		Watches(
			// Watch the leader's Deployment and enqueue the backend that owns it. Without this the
			// reconciler converges the rendered objects only when the BACKEND changes, so a hand
			// edit to the Deployment would stand until something else happened to the object —
			// which is not continuous reconciliation, it is reconciliation on a coincidence.
			&apps.Deployment{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFuncWithWindow(
				3*time.Second,
				dedupWindow,
				r.enqueueKVCacheBackendWhenWorkloadChanged,
			),
			ctrlbuilder.WithPredicates(kvCacheBackendWorkloadPredicate()),
		).
		Watches(
			// Same for the Service in front of it, which carries the published endpoints.
			&core.Service{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFuncWithWindow(
				3*time.Second,
				dedupWindow,
				r.enqueueKVCacheBackendWhenWorkloadChanged,
			),
			ctrlbuilder.WithPredicates(kvCacheBackendWorkloadPredicate()),
		).
		Watches(
			// And the member groups' DaemonSets, which is where a node joining or leaving a group
			// first becomes visible.
			&apps.DaemonSet{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFuncWithWindow(
				3*time.Second,
				dedupWindow,
				r.enqueueKVCacheBackendWhenWorkloadChanged,
			),
			ctrlbuilder.WithPredicates(kvCacheBackendWorkloadPredicate()),
		).
		Watches(
			// And the leader's own Pods, for the one fault that is only ever written there. The
			// scheduler records "no node will take this" on the POD, as PodScheduled=False, and that
			// write moves nothing on the Deployment — whose own account of it arrives ten minutes
			// later as ProgressDeadlineExceeded. Without this watch the pass that reports an
			// unschedulable leader would have to be the one some other event happened to trigger.
			&core.Pod{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFuncWithWindow(
				3*time.Second,
				dedupWindow,
				r.enqueueKVCacheBackendWhenLeaderPodChanged,
			),
			ctrlbuilder.WithPredicates(kvCacheBackendLeaderPodPredicate()),
		).
		Watches(
			// And the Lease the election runs through, which is the only object that says a handover
			// happened. It is created by the STORE rather than by this operator, so it carries no
			// resource note and the map function has to earn the link back from the name.
			//
			// The predicate is not tidiness. A lease is renewed every few seconds for the life of
			// every leader, and an unfiltered watch would turn each renewal into a reconcile costing
			// three sequential reads of that backend's admin surface -- a permanent load on every
			// store in the cluster, produced by a feature that records one event per failover.
			&coordination.Lease{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFuncWithWindow(
				3*time.Second,
				dedupWindow,
				r.enqueueKVCacheBackendWhenLeaseChanged,
			),
			ctrlbuilder.WithPredicates(ctrlpredicate.Funcs{
				// A lease appearing for the first time establishes the baseline rather than
				// reporting anything, and the pass it wakes is what stores it.
				CreateFunc: func(e ctrlevent.CreateEvent) bool {
					return e.Object.GetNamespace() == kuberess.SystemNamespaceName
				},
				UpdateFunc: func(e ctrlevent.UpdateEvent) bool {
					return kvCacheBackendLeaseHolderChanged(e.ObjectOld, e.ObjectNew)
				},
				// A deleted lease says the election stopped, not that it moved. The pass that would
				// forget the holder is the one the backend's own change wakes.
				DeleteFunc:  func(ctrlevent.DeleteEvent) bool { return false },
				GenericFunc: func(ctrlevent.GenericEvent) bool { return false },
			}),
		).
		WithOptions(ctrlcontroller.Options{
			MaxConcurrentReconciles: kvCacheBackendConcurrency,
		}).
		Complete(r)
}

// kvCacheBackendConcurrency is how many backends this controller reconciles at once.
//
// It is set because this reconciler is the only one here that BLOCKS on something outside the
// cluster: every backend re-enqueues itself on a timer and each pass makes three sequential reads
// against an address the object names — for an external backend, an address this operator has no
// control over at all. On the default single worker, one endpoint that accepts a connection and then
// stalls holds the queue for its timeout on every pass, and every other backend waits behind it —
// including one being deleted.
//
// A per-request timeout does not substitute for this. It bounds how long one stall lasts and says
// nothing about who else is blocked by it.
//
// Four rather than more: the work is one HTTP read at a time against distinct hosts, so this is
// about not letting a few bad addresses monopolize the queue, not about throughput.
const kvCacheBackendConcurrency = 4

// kvCacheBackendLeaderPodPredicate keeps the Pod watch to this operator's leaders.
//
// It cannot use the resource note the other three watches filter on: a Pod is made by the
// Deployment's controller from a template, and the note is on the Deployment rather than inside that
// template. The identity labels are what a leader's Pod does carry — and a label is cheaper to
// forge than a note, so the namespace matters here for the same reason it does there.
func kvCacheBackendLeaderPodPredicate() ctrlpredicate.Predicate {
	return ctrlpredicate.NewPredicateFuncs(func(obj ctrlcli.Object) bool {
		return obj.GetNamespace() == kuberess.SystemNamespaceName &&
			mooncake.LeaderPodBackendName(obj.GetLabels()) != ""
	})
}

// enqueueKVCacheBackendWhenLeaderPodChanged maps a leader's Pod back to the backend that owns it.
func (r *KVCacheBackendReconciler) enqueueKVCacheBackendWhenLeaderPodChanged(
	_ context.Context, obj ctrlcli.Object,
) []ctrlreconcile.Request {
	name := mooncake.LeaderPodBackendName(obj.GetLabels())
	if name == "" {
		return nil
	}
	return []ctrlreconcile.Request{{NamespacedName: ctrlcli.ObjectKey{Name: name}}}
}
