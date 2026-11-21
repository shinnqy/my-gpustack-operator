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
	"gpustack.ai/gpustack/pkg/utils/ctrlhandlerx"
)

const (
	// TopologyProfileLabel partitions Nodes by their ordered topology label-key set.
	TopologyProfileLabel = "topology.gpustack.ai/profile"

	topologyNamePrefix    = "gpustack-"
	topologyProfilePrefix = "fnv64-"
	topologyResourceType  = "topology"
	topographFabricPrefix = "fabric.topograph.run/tier-"
)

// NodeTopologyReconciler assigns topology profiles and materializes their Kueue Topologies.
type NodeTopologyReconciler struct {
	Client ctrlcli.Client
}

var _ ctrlreconcile.Reconciler = (*NodeTopologyReconciler)(nil)

func (r *NodeTopologyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// Implement this function in chapter 06.
	return ctrl.Result{}, nil
}

func (r *NodeTopologyReconciler) enqueueAllNodes(ctx context.Context, _ ctrlcli.Object) []ctrlreconcile.Request {
	nodes := new(core.NodeList)
	if err := r.Client.List(ctx, nodes); err != nil {
		ctrllog.FromContext(ctx).Error(err, "list Nodes for topology change")
		return nil
	}
	requests := make([]ctrlreconcile.Request, len(nodes.Items))
	for i := range nodes.Items {
		requests[i] = ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKeyFromObject(&nodes.Items[i])}
	}
	return requests
}

func (r *NodeTopologyReconciler) enqueueNodesForTopology(ctx context.Context, topology ctrlcli.Object) []ctrlreconcile.Request {
	profile := strings.TrimPrefix(topology.GetName(), topologyNamePrefix)
	if profile == topology.GetName() {
		return nil
	}
	nodes := new(core.NodeList)
	if err := r.Client.List(ctx, nodes, ctrlcli.MatchingLabels{TopologyProfileLabel: profile}); err != nil {
		ctrllog.FromContext(ctx).Error(err, "list Nodes for managed Topology change")
		return nil
	}
	requests := make([]ctrlreconcile.Request, len(nodes.Items))
	for i := range nodes.Items {
		requests[i] = ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKeyFromObject(&nodes.Items[i])}
	}
	return requests
}

func (r *NodeTopologyReconciler) SetupController(_ context.Context, opts controller.SetupOptions) error {
	r.Client = opts.Manager.GetClient()
	return ctrl.NewControllerManagedBy(opts.Manager).
		Named("nodetopology").
		For(
			&core.Node{},
			ctrlbuilder.WithPredicates(ctrlpredicate.Funcs{
				DeleteFunc: func(ctrlevent.DeleteEvent) bool { return false },
				UpdateFunc: func(event ctrlevent.UpdateEvent) bool {
					return nodeLabelsChangedIgnoringFit(event.ObjectOld.(*core.Node), event.ObjectNew.(*core.Node))
				},
			}),
		).
		Watches(
			&workercore.TopologySource{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFunc(time.Second, r.enqueueAllNodes),
		).
		Watches(
			&kueue.Topology{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFunc(time.Second, r.enqueueNodesForTopology),
			ctrlbuilder.WithPredicates(ctrlpredicate.NewPredicateFuncs(func(obj ctrlcli.Object) bool {
				return systemmeta.MatchResource(obj, topologyResourceType)
			})),
		).
		Complete(r)
}

// nodeLabelsChangedIgnoringFit reports whether a Node update changed any label other than a fit
// label. The fit labels move on almost every accelerator allocation and name no topology level, so
// a watcher deriving topology from labels ignores an update that moved only them.
func nodeLabelsChangedIgnoringFit(oldNd, newNd *core.Node) bool {
	return !nodefeature.EqualIgnoringFitLabels(oldNd.Labels, newNd.Labels)
}
