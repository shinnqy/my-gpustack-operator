package worker

import (
	"context"
	"fmt"
	"slices"
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
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/systemmeta"
	"gpustack.ai/gpustack/pkg/systemname"
	"gpustack.ai/gpustack/pkg/utils/ctrlhandlerx"
	"gpustack.ai/gpustack/pkg/utils/mapx"
	"gpustack.ai/gpustack/pkg/utils/slicex"
)

// NodeFlavorReconciler reconciles kueue.ResourceFlavor objects keyed by their own
// name, driven by both ResourceFlavor and Kubernetes Node changes:
//   - When one or more Nodes contribute to the flavor's name, (re)build the
//     ResourceFlavor — capacity = pooled nodes × per-node count — and then, when
//     instance-type-derived-from-node is enabled, author the pool's InstanceType.
//     The authoring is create-only: it never updates or deletes an existing type, so
//     an admin's edits are preserved and the InstanceTypeReconciler stays the sole
//     owner of an InstanceType's lifecycle.
//   - When no Node contributes, delete the flavor; an unused flavor advertises
//     stale capacity and a tombstone buys nothing once the ClusterQueue is rebuilt
//     from ResourceFlavor labels.
//
// The flavor is identified by (key, os, arch, count) and its notes carry only device
// information — never a unit spec. The unit spec is no longer node-derived; it is a
// fixed default stamped on the InstanceType.
//
// Watching ResourceFlavor with For(...) means a full resync on start-up
// re-evaluates every flavor, so orphans left behind by a key/count switch are
// deleted even though no Node event would ever enqueue them.
type NodeFlavorReconciler struct {
	Client    ctrlcli.Client
	APIReader ctrlcli.Reader
}

var _ ctrlreconcile.Reconciler = (*NodeFlavorReconciler)(nil)

const (
	// The schedule labels are stamped on both the ResourceFlavor and its backing
	// ClusterQueue in the node-feature vocabulary, so each is reverse-looked-up from
	// the other and from the matching Nodes/Devices by a label selector: the flavor's
	// feature key ("general."/"acceleratable." prefixed, value "true"), the well-known
	// kubernetes.io/os and kubernetes.io/arch, and — on the ResourceFlavor only — the
	// per-key ".count"/".capacity" siblings the InstanceTypeReconciler reads to build the
	// queue without listing or watching Nodes (capacity = pooled nodes × count).
	_ResourceFlavorCountLabelSuffix    = ".count"
	_ResourceFlavorCapacityLabelSuffix = ".capacity"
)

// _ResourceFlavorResType is the systemmeta resource type carried by the
// ResourceFlavors this reconciler owns.
const _ResourceFlavorResType = "nodes"

func (r *NodeFlavorReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// Implement this function in chapter 06.
	var result0 ctrl.Result
	var result1 error
	return result0, result1
}

func topologyQualifiedFlavorName(flavorName, profile string) string {
	return flavorName + "-" + profile
}

const (
	// _InstanceTypeDerivedFromNodeLabel marks an InstanceType the operator authored by deriving it
	// from the node-fed ResourceFlavors (instance-type-derived-from-node); it is a provenance marker
	// only — a derived type is never auto-removed.
	_InstanceTypeDerivedFromNodeLabel = "schedule.gpustack.ai/derived-from-node"

	// cpuOnlyDisplayName is the DisplayName a derived CPU-manufacturer-agnostic collapsed pool
	// carries, since no single pooled node's product represents it.
	cpuOnlyDisplayName = "CPU-only"

	// derivedLocalStorage is the per-unit local storage every derived InstanceType is stamped
	// with. Unlike the unit CPU/RAM it is not preset per product, because the storage a workload
	// wants does not track the accelerator it runs on. Admins override it per InstanceType.
	derivedLocalStorage = "100Gi"
)

const (
	// IndexingNodeByScheduleFlavor indexes managed Nodes by the ResourceFlavor names
	// they contribute to (one CPU flavor plus one per device key). A node that is not
	// managed drops out of the index, which is how the reconciler detects a flavor has
	// no Node left (→ delete).
	IndexingNodeByScheduleFlavor = "nodes.schedule.gpustack.ai/flavor"
)

// indexNodeByScheduleFlavor is the field-index extractor for
// IndexingNodeByScheduleFlavor. A deleting node still counts as present (a brief
// deletion must not churn the pool), but an unmanaged node is excluded so its
// flavors are deleted.
func indexNodeByScheduleFlavor(obj ctrlcli.Object) []string {
	nd, ok := obj.(*core.Node)
	if !ok || nd == nil {
		return nil
	}
	if !kubemeta.IsLabeled(nd, systemname.ManagedLabelKey, "true") {
		return nil
	}
	profile := nd.Labels[TopologyProfileLabel]
	if profile == "" {
		return nil
	}
	return slicex.Transform(nodefeature.ExtractNodeFlavors(nd),
		func(f nodefeature.NodeFlavor) string {
			return topologyQualifiedFlavorName(f.Name, profile)
		})
}

// nodeFlavorNodeUpdated reports whether a Node update must reconcile the flavors the node
// contributes to, before and after the update.
func nodeFlavorNodeUpdated(oldNd, newNd *core.Node) bool {
	if newNd.DeletionTimestamp != nil {
		return false
	}
	// Fire when the managed mark or feature labels have changed.
	if !mapx.EqualWithStringPrefix(oldNd.Labels, newNd.Labels,
		systemname.ManagedLabelKey,
		TopologyProfileLabel,
		nodefeature.FeatureLabelPrefix,
		nodefeature.GeneralFeatureLabelPrefix,
		nodefeature.AcceleratableFeatureLabelPrefix) {
		return true
	}
	// Fire when the set of flavors the node contributes to has changed. A flavor name also blends
	// the NFD cpu-model labels and the cpu-name annotation, which the labels above do not cover:
	// when NFD publishes them after the node registers, the node leaves its generic flavor before
	// the NodeFeature re-derives its labels, and only this update still maps to that flavor.
	oldFlavors, newFlavors := indexNodeByScheduleFlavor(oldNd), indexNodeByScheduleFlavor(newNd)
	slices.Sort(oldFlavors)
	slices.Sort(newFlavors)
	return !slices.Equal(oldFlavors, newFlavors)
}

func (r *NodeFlavorReconciler) SetupController(ctx context.Context, opts controller.SetupOptions) error {
	// Configure field indexer.
	fi := opts.Manager.GetFieldIndexer()
	err := fi.IndexField(ctx, &core.Node{}, IndexingNodeByScheduleFlavor, indexNodeByScheduleFlavor)
	if err != nil {
		return fmt.Errorf("index node '%s': %w", IndexingNodeByScheduleFlavor, err)
	}

	r.Client = opts.Manager.GetClient()
	r.APIReader = opts.Manager.GetAPIReader()

	return ctrl.NewControllerManagedBy(opts.Manager).
		Named("nodeflavor").
		For(
			// Reconcile relevant ResourceFlavor objects keyed by their own name.
			// A full resync on start-up re-evaluates every flavor, so orphans left
			// behind by a key/count switch get deleted even though no Node event
			// would ever enqueue them.
			&kueue.ResourceFlavor{},
			ctrlbuilder.WithPredicates(
				// Interested in relevant ResourceFlavor objects.
				ctrlpredicate.NewPredicateFuncs(func(obj ctrlcli.Object) bool {
					return systemmeta.MatchResource(obj, _ResourceFlavorResType)
				}),
				// Trigger reconciliation when a ResourceFlavor is:
				// - created (incl. the start-up resync).
				// - updated if its spec, schedule labels (incl. capacity) or notes
				//   have changed.
				// Reconcile deletion to retire a generated Topology after its last flavor
				// reference disappears. A Node event re-creates a still-needed flavor.
				ctrlpredicate.Funcs{
					DeleteFunc: func(e ctrlevent.DeleteEvent) bool {
						return true
					},
					UpdateFunc: func(e ctrlevent.UpdateEvent) bool {
						oldRf, newRf := e.ObjectOld.(*kueue.ResourceFlavor), e.ObjectNew.(*kueue.ResourceFlavor)
						if newRf.DeletionTimestamp == nil {
							// Fire when spec has changed.
							if !kubemeta.DeepEqual(oldRf.Spec, newRf.Spec) {
								return true
							}
							// Fire when schedule labels have changed.
							if !mapx.EqualWithStringPrefix(oldRf.Labels, newRf.Labels,
								nodefeature.GeneralFeatureLabelPrefix,
								nodefeature.AcceleratableFeatureLabelPrefix,
								core.LabelOSStable,
								core.LabelArchStable) {
								return true
							}
							// Fire when notes have changed.
							if !systemmeta.EqualResourceTypeAndNotes(oldRf, newRf) {
								return true
							}
						}
						return false
					},
				},
			),
		).
		Watches(
			// Watch Nodes and enqueue the corresponding ResourceFlavor by name.
			&core.Node{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFunc(
				5*time.Second,
				r.enqueueResourceFlavorWhenNodeChanged,
			),
			ctrlbuilder.WithPredicates(
				// Trigger reconciliation when a Node is:
				// - created.
				// - deleted (so a flavor losing its last Node gets deleted).
				// - updated if its managed mark, feature labels or contributed flavors
				//   have changed (a node leaving management or a flavor deletes the
				//   orphaned flavors).
				ctrlpredicate.Funcs{
					UpdateFunc: func(e ctrlevent.UpdateEvent) bool {
						return nodeFlavorNodeUpdated(e.ObjectOld.(*core.Node), e.ObjectNew.(*core.Node))
					},
				},
			),
		).
		Watches(
			// Watch the InstanceTypes this reconciler authors, and enqueue the flavors when one
			// is deleted. Without it the reconciler hears about its inputs only, so an
			// InstanceType destroyed on its own is invisible: a definition lost at runtime takes
			// every one of them at once, and the flavors that derived them never change, so
			// nothing would ever author them again. The periodic informer resync is no fallback
			// either — it re-delivers the flavor unchanged, which the update filter above drops.
			&workercore.InstanceType{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFunc(
				5*time.Second,
				r.enqueueResourceFlavorsWhenDerivedInstanceTypeDeleted,
			),
			ctrlbuilder.WithPredicates(
				// Interested in the derived InstanceTypes only; an admin-owned one is nothing
				// this reconciler authors.
				ctrlpredicate.NewPredicateFuncs(func(obj ctrlcli.Object) bool {
					return obj.GetLabels()[_InstanceTypeDerivedFromNodeLabel] == "true"
				}),
				// Trigger reconciliation when a derived InstanceType is:
				// - deleted.
				// Nothing else says anything the inputs do not: authoring is create-only, so a
				// creation is this reconciler's own write, and the status churn an InstanceType
				// carries never changes whether it exists.
				ctrlpredicate.Not(ctrlpredicate.Funcs{
					DeleteFunc: func(e ctrlevent.DeleteEvent) bool {
						return false
					},
				}),
			),
		).
		Complete(r)
}

// enqueueResourceFlavorsWhenDerivedInstanceTypeDeleted enqueues every managed ResourceFlavor when
// a derived InstanceType is deleted, so the one that derived it authors it again.
//
// Which flavor derived which type cannot be read back from the type, and a cluster carries a
// handful of flavors, so all of them are enqueued; a reconcile with nothing to author is a no-op
// and authoring is create-only, so the ones that still have their type are untouched.
func (r *NodeFlavorReconciler) enqueueResourceFlavorsWhenDerivedInstanceTypeDeleted(
	ctx context.Context,
	obj ctrlcli.Object,
) []ctrlreconcile.Request {
	logger := ctrllog.FromContext(ctx).
		WithValues("instance type", ctrlcli.ObjectKeyFromObject(obj))

	rfList := new(kueue.ResourceFlavorList)
	err := r.Client.List(ctx, rfList,
		systemmeta.GetResourcesLabelSetOfType[ctrlcli.MatchingLabels](_ResourceFlavorResType),
		ctrlcli.UnsafeDisableDeepCopy)
	if err != nil {
		logger.Error(err, "list resource flavors for deleted derived instance type")
		return nil
	}
	if len(rfList.Items) == 0 {
		return nil
	}

	reqs := make([]ctrlreconcile.Request, 0, len(rfList.Items))
	for i := range rfList.Items {
		reqs = append(reqs, ctrlreconcile.Request{
			NamespacedName: ctrlcli.ObjectKey{
				Name: rfList.Items[i].Name,
			},
		})
	}

	logger.V(2).Info("enqueue resource flavors from deleted derived instance type",
		"requests", reqs)
	return reqs
}

func (r *NodeFlavorReconciler) enqueueResourceFlavorWhenNodeChanged(
	ctx context.Context,
	obj ctrlcli.Object,
) []ctrlreconcile.Request {
	logger := ctrllog.FromContext(ctx).
		WithValues("node", ctrlcli.ObjectKeyFromObject(obj))

	nd := obj.(*core.Node)
	profile := nd.Labels[TopologyProfileLabel]
	if profile == "" {
		return nil
	}

	flavors := nodefeature.ExtractNodeFlavors(nd)
	if len(flavors) == 0 {
		logger.V(2).Info("node has no flavor")
		return nil
	}

	reqs := make([]ctrlreconcile.Request, 0, len(flavors))
	for i := range flavors {
		if flavors[i].Name == "" {
			continue
		}
		reqs = append(reqs, ctrlreconcile.Request{
			NamespacedName: ctrlcli.ObjectKey{
				Name: topologyQualifiedFlavorName(flavors[i].Name, profile),
			},
		})
	}
	if len(reqs) == 0 {
		return nil
	}

	logger.V(2).Info("enqueue resource flavor from node", "requests", reqs)
	return reqs
}
