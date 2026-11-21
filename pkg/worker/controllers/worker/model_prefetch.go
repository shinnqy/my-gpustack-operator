package worker

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"time"

	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlbuilder "sigs.k8s.io/controller-runtime/pkg/builder"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlevent "sigs.k8s.io/controller-runtime/pkg/event"
	ctrlhandler "sigs.k8s.io/controller-runtime/pkg/handler"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	ctrlpredicate "sigs.k8s.io/controller-runtime/pkg/predicate"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	gpustack "gpustack.ai/gpustack/api/v1"
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/controller"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/setting"
	"gpustack.ai/gpustack/pkg/systemmeta"
	"gpustack.ai/gpustack/pkg/utils/mapx"
	"gpustack.ai/gpustack/pkg/worker/settings"
)

// Conditions of a ModelPrefetch's status.
const (
	// ModelPrefetchConditionProgressing says delivery is under way: the artifact is resolved and
	// some target nodes do not hold the content yet.
	ModelPrefetchConditionProgressing = "Progressing"
	// ModelPrefetchConditionAvailable says enough target nodes hold the content: ReadyNodes
	// reached the MinReady bar, where MinReady 0 means all of them.
	ModelPrefetchConditionAvailable = "Available"
	// ModelPrefetchConditionDegraded says a target node gave up; the message names the reasons.
	ModelPrefetchConditionDegraded = "Degraded"
	// ModelPrefetchConditionLapsed says the MinReady shortfall is the retention TTL's doing: the
	// copies that would close the gap lapsed past TTLAfterLastUse, stopped counting as ready and
	// were unpinned, and nothing re-warms them. It stays False while the shortfall has any other
	// cause, so a lapse never masks a warm-up still running or a delivery that failed.
	ModelPrefetchConditionLapsed = "Lapsed"
)

// ModelPrefetchReconciler turns a prefetch's residency intent into one warm-up Pod per target
// node, reads the delivery's progress from the target nodes' own reports, and pins what its
// retention asks to keep.
//
// The warm-up Pod IS the delivery operation: it mounts the artifact's volume the consumer path
// mounts, so the node's cache does the download, the verification and the publishing exactly once
// per node; the Pod reads the tree back and exits, dropping its reference. It never renders a
// queue-name label, so neither Kueue nor the operator's Pod webhook has anything to say about it.
type ModelPrefetchReconciler struct {
	Client ctrlcli.Client
	Now    func() time.Time
}

var _ ctrlreconcile.Reconciler = (*ModelPrefetchReconciler)(nil)

func (r *ModelPrefetchReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}

	return time.Now()
}

func (r *ModelPrefetchReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// Implement this function in chapter 18.
	var result0 ctrl.Result
	var result1 error
	return result0, result1
}

// prefetchProgress is one pass's reading of the delivery, the raw material of the status.
type prefetchProgress struct {
	desired     int32
	ready       []string
	downloading int32
	failed      int32
	// lapsed counts the target nodes whose copy is Ready but past the retention TTL: they stopped
	// counting as ready and their pins are released, and no warm-up Pod re-warms them.
	lapsed int32
}

// prefetchPlacementTypeNames returns the InstanceType names a prefetch's placement selects, or
// the ones its namespace's deployments serve the artifact on. An empty result means the
// placement's nodeSelector owns the target set, or that there is nothing to derive it from.
func prefetchPlacementTypeNames(
	pf *workercore.ModelPrefetch, deps []workercore.ModelDeployment,
) []string {
	placement := pf.Spec.Placement
	switch {
	case placement != nil && len(placement.InstanceTypes) > 0 && placement.NodeSelector != nil:
		return nil
	case placement != nil && len(placement.InstanceTypes) > 0:
		return placement.InstanceTypes
	case placement != nil && placement.NodeSelector != nil:
		return nil
	default:
		// The derived set: every InstanceType the namespace's own deployments serve these weights
		// on, so warming follows serving without a second place to say where.
		return deploymentInstanceTypes(deps, pf.Spec.ArtifactRef.Name)
	}
}

// deploymentInstanceTypes collects the InstanceType names the namespace's deployments serve the
// artifact's weights on.
func deploymentInstanceTypes(deps []workercore.ModelDeployment, artifactName string) []string {
	var out []string
	for i := range deps {
		ref := deps[i].Spec.Model.ArtifactRef
		if ref == nil || ref.Name != artifactName {
			continue
		}
		for _, role := range deps[i].Spec.Roles {
			if role.InstanceType != "" {
				out = append(out, role.InstanceType)
			}
		}
	}

	return out
}

// writeStatus fills the status from the progress. Available wants readyNodes to reach MinReady,
// where 0 means all of them; Degraded waits for a target node to give up — or for the artifact the
// prefetch names to disappear, which stops the delivery rather than leaving it fetching forever.
// Lapsed says when a shortfall below MinReady is the retention TTL's doing: it turns True only
// when the lapsed copies alone account for the gap, so a warm-up or a failure keeps its own voice.
func (r *ModelPrefetchReconciler) writeStatus(
	pf *workercore.ModelPrefetch, progress prefetchProgress, broken error, artifactMissing bool,
) {
	pf.Status.DesiredNodes = progress.desired
	pf.Status.ReadyNodes = int32(len(progress.ready))
	pf.Status.DownloadingNodes = progress.downloading

	required := requiredNodes(pf, progress.desired)
	delivered := int32(len(progress.ready)) >= required
	progressing := gpustack.Condition{
		Type:               ModelPrefetchConditionProgressing,
		ObservedGeneration: pf.Generation,
	}
	available := gpustack.Condition{
		Type:               ModelPrefetchConditionAvailable,
		Status:             meta.ConditionFalse,
		Reason:             "NotEnoughNodes",
		Message:            "fewer target nodes hold the content than the prefetch requires",
		ObservedGeneration: pf.Generation,
	}
	degraded := gpustack.Condition{
		Type:               ModelPrefetchConditionDegraded,
		Status:             meta.ConditionFalse,
		Reason:             "NoFailures",
		Message:            "no target node has given up",
		ObservedGeneration: pf.Generation,
	}
	lapsed := gpustack.Condition{
		Type:               ModelPrefetchConditionLapsed,
		Status:             meta.ConditionFalse,
		Reason:             "NoLapsedCopies",
		Message:            "no target node's copy has lapsed past retention.ttlAfterLastUse",
		ObservedGeneration: pf.Generation,
	}

	// The lapse explains the shortfall only when the lapsed copies alone account for the gap below
	// the requirement: a gap they cannot close has other causes — a warm-up still running or a
	// failure — that Lapsed must never paper over.
	lead := fmt.Sprintf("%d of %d target node copies lapsed past retention.ttlAfterLastUse",
		progress.lapsed, progress.desired)
	switch gap := required - int32(len(progress.ready)); {
	case progress.lapsed == 0:
		// Nothing lapsed; the default stands.
	case gap <= 0:
		lapsed.Reason = "NoShortfall"
		lapsed.Message = lead + ", but readyNodes still meets the requirement"
	case progress.lapsed >= gap:
		lapsed.Status, lapsed.Reason = meta.ConditionTrue, "RetentionTTLExpired"
		lapsed.Message = lead + " and no longer count toward readyNodes"
		if pf.Spec.Retention.Pinned {
			lapsed.Message = lead + " and their pins were released; they no longer count toward readyNodes"
		}
	default:
		lapsed.Reason = "ShortfallBeyondLapse"
		lapsed.Message = lead + ", but the shortfall below minReady is wider than the lapse; the rest is still warming or has failed"
	}

	switch {
	case broken != nil:
		progressing.Status, progressing.Reason = meta.ConditionFalse, "PlacementInvalid"
		progressing.Message = "the placement does not parse, so no target set exists"
	case artifactMissing:
		progressing.Status, progressing.Reason = meta.ConditionFalse, "ArtifactMissing"
		progressing.Message = "the artifact this prefetch names no longer exists"
		available.Status, available.Reason = meta.ConditionFalse, "ArtifactMissing"
		available.Message = "the artifact is gone, so the delivery cannot proceed"
		degraded.Status, degraded.Reason = meta.ConditionTrue, "ArtifactMissing"
		degraded.Message = "the named artifact was deleted after admission; create it again or delete the prefetch"
	case progress.desired == 0:
		progressing.Status, progressing.Reason = meta.ConditionFalse, "NoTargets"
		progressing.Message = "the placement matches no nodes"
	case delivered:
		progressing.Status, progressing.Reason = meta.ConditionFalse, "Delivered"
		progressing.Message = "every node the prefetch requires holds the content"
		available.Status, available.Reason = meta.ConditionTrue, "MinReadyReached"
		available.Message = "enough target nodes hold the content"
	default:
		progressing.Status, progressing.Reason = meta.ConditionTrue, "Delivering"
		progressing.Message = "some target nodes are still fetching or waiting on the artifact"
	}

	if progress.failed > 0 {
		degraded.Status, degraded.Reason = meta.ConditionTrue, "NodeFailed"
		degraded.Message = "one or more target nodes reported the delivery failed; their reasons are on the nodes' reports"
	}

	pf.Status.Conditions = carryConditionTransitions(pf.Status.Conditions,
		[]gpustack.Condition{progressing, available, degraded, lapsed}, r.now())
}

// requiredNodes is how many ready nodes satisfy the prefetch: MinReady, or every target when it
// is left at its default of zero.
func requiredNodes(pf *workercore.ModelPrefetch, desired int32) int32 {
	if pf.Spec.MinReady == 0 || pf.Spec.MinReady > desired {
		return desired
	}

	return pf.Spec.MinReady
}

// InstanceTypeFlavorNames carries, per InstanceType name, the ResourceFlavor names the type's
// pool selects. A type resolving to no flavors — unknown, or no synced flavor yet — maps to an
// empty entry, which leaves its target set empty rather than erroring: the placement contract
// is "every node carrying the type's flavors", and a type with none carries none.
type InstanceTypeFlavorNames map[string][]string

// ResolveInstanceTypeFlavorNames resolves the InstanceType names a prefetch's placement — or, in
// its default form, the type names its namespace's deployments serve the artifact on — into the
// ResourceFlavor names each type's pool selects: the flavors carrying the schedule labels the
// type's spec derives, the same reverse-lookup the type's backing ClusterQueue and its status
// Detail run. Flavor names are topology-qualified and count-suffixed, so an InstanceType's
// object name is never itself a flavor name and cannot be compared against one.
func ResolveInstanceTypeFlavorNames(
	ctx context.Context, r ctrlcli.Reader, pf *workercore.ModelPrefetch, deps []workercore.ModelDeployment,
) (InstanceTypeFlavorNames, error) {
	typeNames := prefetchPlacementTypeNames(pf, deps)
	if len(typeNames) == 0 {
		return nil, nil
	}
	// One list serves every named type: a cluster carries a handful of operator-owned flavors,
	// and a type's pool is a label containment over the shared set.
	flavors := new(kueue.ResourceFlavorList)
	if err := r.List(ctx, flavors,
		systemmeta.GetResourcesLabelSetOfType[ctrlcli.MatchingLabels](_ResourceFlavorResType),
		ctrlcli.UnsafeDisableDeepCopy); err != nil {
		return nil, err
	}
	cpuAware := settings.InstanceTypeAwareCPUManufacturer.ShouldValueBool(ctx)
	pools := InstanceTypeFlavorNames{}
	for _, name := range typeNames {
		if _, resolved := pools[name]; resolved {
			continue
		}
		it := new(workercore.InstanceType)
		err := r.Get(ctx, ctrlcli.ObjectKey{Name: name}, it)
		if kerrors.IsNotFound(err) {
			pools[name] = nil
			continue
		}
		if err != nil {
			return nil, err
		}
		sched := nodefeature.PoolScheduleLabels(
			it.Spec.Acceleratable, cpuAware,
			it.Spec.GeneralGroup, it.Spec.AcceleratorGroup,
			it.Spec.OS, it.Spec.Arch,
		)
		var flavorNames []string
		for i := range flavors.Items {
			if mapx.Contain(flavors.Items[i].Labels, sched) {
				flavorNames = append(flavorNames, flavors.Items[i].Name)
			}
		}
		sort.Strings(flavorNames)
		pools[name] = flavorNames
	}

	return pools, nil
}

// PrefetchTargetNodes resolves a prefetch's placement into node names. It is the controller's own
// expansion, shared with the admission webhook so admission and delivery can never disagree about
// what a prefetch would warm. The pools carry the named InstanceTypes' flavor resolution from
// ResolveInstanceTypeFlavorNames.
func PrefetchTargetNodes(
	pf *workercore.ModelPrefetch, nodes []core.Node, deps []workercore.ModelDeployment,
	pools InstanceTypeFlavorNames,
) ([]string, error) {
	// Implement this function in chapter 18.
	var result0 []string
	var result1 error
	return result0, result1
}

// The placement refusal the webhook shares: both kinds set is ambiguous, and the schema cannot
// see it because neither field is required.
var ErrPrefetchPlacementAmbiguous = fmt.Errorf("placement sets both instanceTypes and nodeSelector")

// artifactResolutionChanged enqueues only when an artifact's resolution moved — the fact both the
// accounting and the prefetch delivery read. Description and label churn is nobody's feed here.
var artifactResolutionChanged = ctrlpredicate.Funcs{
	CreateFunc: func(ctrlevent.CreateEvent) bool { return true },
	DeleteFunc: func(ctrlevent.DeleteEvent) bool { return true },
	UpdateFunc: func(e ctrlevent.UpdateEvent) bool {
		o, ok := e.ObjectOld.(*workercore.ModelArtifact)
		if !ok {
			return false
		}
		n, ok := e.ObjectNew.(*workercore.ModelArtifact)
		if !ok {
			return false
		}

		return !kubemeta.DeepEqual(o.Status.Resolved, n.Status.Resolved)
	},
	GenericFunc: func(ctrlevent.GenericEvent) bool { return false },
}

// nodeReportChanged enqueues only when a node's report changed in a way a prefetch reads: the
// per-digest entries and the pin list. Watermark bookkeeping the plugin writes alongside is
// another object's feed.
var nodeReportChanged = ctrlpredicate.Funcs{
	CreateFunc: func(ctrlevent.CreateEvent) bool { return true },
	DeleteFunc: func(ctrlevent.DeleteEvent) bool { return true },
	UpdateFunc: func(e ctrlevent.UpdateEvent) bool {
		o, ok := e.ObjectOld.(*workercore.NodeModelStore)
		if !ok {
			return false
		}
		n, ok := e.ObjectNew.(*workercore.NodeModelStore)
		if !ok {
			return false
		}

		return !kubemeta.DeepEqual(o.Status.Models, n.Status.Models) ||
			!kubemeta.DeepEqual(o.Spec.Pinned, n.Spec.Pinned)
	},
	GenericFunc: func(ctrlevent.GenericEvent) bool { return false },
}

// nodeLabelsChanged enqueues only when a node's labels changed: the heartbeat's status writes
// decide nothing about which stores match a node or what a prefetch targets. The store
// reconciler keeps its own inline copy of this shape; both read only labels.
var nodeLabelsChanged = ctrlpredicate.Funcs{
	CreateFunc: func(ctrlevent.CreateEvent) bool { return true },
	DeleteFunc: func(ctrlevent.DeleteEvent) bool { return true },
	UpdateFunc: func(e ctrlevent.UpdateEvent) bool {
		o, ok := e.ObjectOld.(*core.Node)
		if !ok {
			return false
		}
		n, ok := e.ObjectNew.(*core.Node)
		if !ok {
			return false
		}

		return !maps.Equal(o.Labels, n.Labels)
	},
	GenericFunc: func(ctrlevent.GenericEvent) bool { return false },
}

// SetupController watches everything a prefetch's target set or delivery depends on: the
// artifact's resolution is a status change, the deployments name the derived set, the nodes are
// the targets, and the nodes' reports are the readiness.
func (r *ModelPrefetchReconciler) SetupController(_ context.Context, opts controller.SetupOptions) error {
	r.Client = opts.Manager.GetClient()

	return ctrl.NewControllerManagedBy(opts.Manager).
		Named("modelprefetch").
		For(&workercore.ModelPrefetch{}, ctrlbuilder.WithPredicates(ctrlpredicate.GenerationChangedPredicate{})).
		Watches(
			&workercore.ModelArtifact{},
			ctrlhandler.EnqueueRequestsFromMapFunc(r.enqueuePrefetchesInNamespace),
			ctrlbuilder.WithPredicates(artifactResolutionChanged),
		).
		Watches(
			&workercore.ModelDeployment{},
			ctrlhandler.EnqueueRequestsFromMapFunc(r.enqueuePrefetchesInNamespace),
			ctrlbuilder.WithPredicates(ctrlpredicate.GenerationChangedPredicate{}),
		).
		Watches(
			&core.Node{},
			ctrlhandler.EnqueueRequestsFromMapFunc(r.enqueueAllPrefetches),
			ctrlbuilder.WithPredicates(nodeLabelsChanged),
		).
		Watches(
			&workercore.NodeModelStore{},
			ctrlhandler.EnqueueRequestsFromMapFunc(r.enqueueAllPrefetches),
			ctrlbuilder.WithPredicates(nodeReportChanged),
		).
		Watches(
			&core.Secret{},
			ctrlhandler.EnqueueRequestsFromMapFunc(r.enqueueAllPrefetches),
			ctrlbuilder.WithPredicates(ctrlpredicate.NewPredicateFuncs(func(o ctrlcli.Object) bool {
				return o.GetNamespace() == setting.DelegatedSecretNamespace && o.GetName() == setting.DelegatedSecretName
			})),
		).
		Complete(r)
}

// enqueueAllPrefetches enqueues every prefetch: nodes and their reports are cluster-wide facts any
// of them may read.
func (r *ModelPrefetchReconciler) enqueueAllPrefetches(ctx context.Context, _ ctrlcli.Object) []ctrlreconcile.Request {
	pfs := new(workercore.ModelPrefetchList)
	if err := r.Client.List(ctx, pfs); err != nil {
		ctrllog.FromContext(ctx).Error(err, "list model prefetches")

		return nil
	}

	reqs := make([]ctrlreconcile.Request, 0, len(pfs.Items))
	for i := range pfs.Items {
		reqs = append(reqs, ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKey{
			Namespace: pfs.Items[i].Namespace, Name: pfs.Items[i].Name,
		}})
	}

	return reqs
}

// enqueuePrefetchesInNamespace enqueues the prefetches of the object's namespace: an artifact's
// resolution and a deployment's placement both decide what their namespace's prefetches do.
func (r *ModelPrefetchReconciler) enqueuePrefetchesInNamespace(ctx context.Context, obj ctrlcli.Object) []ctrlreconcile.Request {
	pfs := new(workercore.ModelPrefetchList)
	if err := r.Client.List(ctx, pfs, ctrlcli.InNamespace(obj.GetNamespace())); err != nil {
		ctrllog.FromContext(ctx).Error(err, "list model prefetches")

		return nil
	}

	reqs := make([]ctrlreconcile.Request, 0, len(pfs.Items))
	for i := range pfs.Items {
		reqs = append(reqs, ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKey{
			Namespace: pfs.Items[i].Namespace, Name: pfs.Items[i].Name,
		}})
	}

	return reqs
}
