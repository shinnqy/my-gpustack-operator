package worker

import (
	"context"
	"slices"

	core "k8s.io/api/core/v1"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

const (
	// modelPlacementPreferenceWeight is the weight of the preferred term naming a digest's nodes. It
	// is the weight the preference was measured with; TAS only compares scores with each other, so a
	// Pod carrying one term is placed the same under any weight, and several terms weigh equally.
	modelPlacementPreferenceWeight = 100

	// modelPlacementMaxNodes bounds the hostnames one term names, so the size a preference adds to a
	// Pod and to its Workload does not grow with the cluster.
	modelPlacementMaxNodes = 16

	// nodeModelStoreConditionReady is the condition the plugin keeps True while it serves its node.
	nodeModelStoreConditionReady = "Ready"
)

// placementPreference returns the preference for the Pods these weights are mounted by: the nodes
// holding a node-delivered digest ready to mount, the nodes kubelet reports holding an image
// artifact's image, or nil for any other delivery, a blocked resolution, or a reference no node
// holds.
//
// It walks every NodeModelStore, so it is computed only by a pass that creates Pods, once for all of
// them: that keeps a reconcile that creates nothing from paying for it, and gives every member of a
// replica the same term, which Kueue needs because it builds the replica's PodSet from one member.
func (w *modelArtifactWeights) placementPreference(ctx context.Context, cli ctrlcli.Reader) *core.PreferredSchedulingTerm {
	if w == nil || w.Blocked || w.Render == nil {
		return nil
	}

	switch w.Render.Delivery {
	case workercore.ModelDeploymentModelDeliveryNode:
		return modelPlacementPreference(ctx, cli, w.Render.ManifestDigest)
	case workercore.ModelDeploymentModelDeliveryImage:
		return modelPlacementImagePreference(ctx, cli, w.Render.ImageReference)
	}
	return nil
}

// modelPlacementImagePreference returns the preferred node-affinity term naming the nodes kubelet
// reports holding an image artifact's reference, or nil when none does.
//
// A PREFERENCE, NEVER A FILTER, for the same reasons the digest preference is one: Kueue's
// topology-aware scheduling reads the term as a score among the nodes that already fit the Pod, so
// a wrong or stale entry costs at most one pull on another node. And a stale entry is likely:
// the report is kubelet's Node.status.images, bounded by --node-status-max-images (default 50), so
// a node running many images may never name this one, and a collected one lingers until the next
// report. The softness is what makes that affordable: absence only forfeits this round's locality
// gain. No size is read — the CRI size is the compressed image, measured well under the real
// occupancy, and capacity is nobody's question here.
//
// The order is hostname-sorted and then capped, because Node.status.images carries no
// referenced-now or last-used signal for an image: sorting on the names is the only ordering that
// depends on nothing but the candidates, so the members of one replica built from one snapshot
// carry the same list.
func modelPlacementImagePreference(ctx context.Context, cli ctrlcli.Reader, reference string) *core.PreferredSchedulingTerm {
	logger := ctrllog.FromContext(ctx)

	nodes := new(core.NodeList)
	if err := cli.List(ctx, nodes); err != nil {
		logger.Error(err, "list nodes for an image placement preference; none is added")
		return nil
	}

	var hostnames []string
	for i := range nodes.Items {
		node := &nodes.Items[i]
		holds := slices.ContainsFunc(node.Status.Images, func(img core.ContainerImage) bool {
			return slices.Contains(img.Names, reference)
		})
		if !holds {
			continue
		}
		hostname := node.Labels[core.LabelHostname]
		if hostname == "" {
			continue
		}
		hostnames = append(hostnames, hostname)
	}
	if len(hostnames) == 0 {
		return nil
	}
	slices.Sort(hostnames)
	if len(hostnames) > modelPlacementMaxNodes {
		hostnames = hostnames[:modelPlacementMaxNodes]
	}

	return &core.PreferredSchedulingTerm{
		Weight: modelPlacementPreferenceWeight,
		Preference: core.NodeSelectorTerm{MatchExpressions: []core.NodeSelectorRequirement{
			{Key: core.LabelHostname, Operator: core.NodeSelectorOpIn, Values: hostnames},
		}},
	}
}

// modelPlacementPreference returns the preferred node-affinity term naming the nodes that hold digest
// ready to mount, or nil when none does.
//
// A PREFERENCE, NEVER A FILTER. Kueue's topology-aware scheduling reads the term as a node score among
// the nodes that already fit the Pod, so a wrong or stale entry costs at most one download on another
// node. For the same reason a read that fails yields no term rather than an error: holding a Pod back
// because its preference could not be computed would put the file location ahead of compute.
//
// A node counts only while three things hold: its NodeModelStore lists the digest Ready, that object's
// Ready condition is True, and its CSINode lists the plugin's driver now. The third is there because
// the object does not say when it went stale: a node the plugin left keeps its object and a Ready
// condition that stops changing, while kubelet drops the driver from CSINode.
//
// The values are the nodes' kubernetes.io/hostname labels, not their names: TAS keys its nodes by that
// label, and some providers set it to something other than the Node's name.
func modelPlacementPreference(ctx context.Context, cli ctrlcli.Reader, digest string) *core.PreferredSchedulingTerm {
	// Implement this function in chapter 18.
	return nil
}

// injectModelPlacementPreference appends term to a Pod's preferred node affinity, keeping every term
// already there.
//
// A Pod asking Kueue for a preferred topology level gets nothing: with TASBalancedPlacement on, as the
// chart has it, such a PodSet was measured to lose its node-affinity score entirely, so the term would
// claim a preference that nothing honors.
func injectModelPlacementPreference(pod *core.Pod, term *core.PreferredSchedulingTerm) {
	if term == nil {
		return
	}
	if _, ok := pod.Annotations[kueue.PodSetPreferredTopologyAnnotation]; ok {
		return
	}
	if pod.Spec.Affinity == nil {
		pod.Spec.Affinity = &core.Affinity{}
	}
	if pod.Spec.Affinity.NodeAffinity == nil {
		pod.Spec.Affinity.NodeAffinity = &core.NodeAffinity{}
	}
	na := pod.Spec.Affinity.NodeAffinity
	na.PreferredDuringSchedulingIgnoredDuringExecution = append(na.PreferredDuringSchedulingIgnoredDuringExecution, *term.DeepCopy())
}
