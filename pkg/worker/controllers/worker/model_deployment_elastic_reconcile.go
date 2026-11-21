package worker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubeapistatus"
)

const (
	modelDeploymentElasticWithdrawnAnnotation = "modeldeployment.gpustack.ai/elastic-withdrawn"
	elasticObservationLabel                   = "gpustack.ai/elastic-observation"

	// ModelDeploymentConditionElasticResize reports whether an elastic scale stands blocked or
	// terminally refused. It is declared beside the convergence that decides it, like every other
	// condition whose vocabulary sits with the code that can observe it. It is False with
	// ScaleRefused only while the durable record says the retry bound is spent, Unknown with
	// WorldUnprovable while no record exists and the live width disagrees with the spec without
	// proving itself, and True with NoRefusal the rest of the time: a refusal still inside its
	// bound is not terminal and must not read as one, and an unprovable world is not a refusal.
	ModelDeploymentConditionElasticResize kubeapistatus.ConditionType = "ElasticResize"

	// modelDeploymentReasonScaleRefused is ElasticResize's False reason: the record is abandoned
	// and its message carries the engine's own last refusal.
	modelDeploymentReasonScaleRefused = "ScaleRefused"

	// modelDeploymentReasonWorldUnprovable is ElasticResize's Unknown reason: no record exists,
	// the live width disagrees with the spec or cannot be named, and the corrective scale is
	// blocked until a pass proves the world it would have to act on. Its message carries the
	// desired width, the observed width and the proof that failed.
	modelDeploymentReasonWorldUnprovable = "WorldUnprovable"

	// modelDeploymentReasonNoScaleRefusal is ElasticResize's True reason: no record stands
	// refused, whether none exists, one is in flight, or one has just completed.
	modelDeploymentReasonNoScaleRefusal = "NoRefusal"
)

type modelDeploymentElasticRayReader interface {
	Observe(context.Context, *workercore.ModelDeployment, []modelDeploymentElasticCapturedMember,
		int32, string) (modelDeploymentElasticObservation, error)
}

func elasticOwnedBy(md *workercore.ModelDeployment, obj ctrlcli.Object) bool {
	owner := meta.GetControllerOf(obj)
	return owner != nil && owner.Kind == "ModelDeployment" &&
		owner.APIVersion == workercore.GroupVersion.String() && owner.Name == md.Name && owner.UID == md.UID && obj.GetUID() != ""
}

// errElasticDeploymentChanged reports that the live deployment no longer matches the identity or
// generation this pass captured. It is not a read failure: the spec watch requeues the deployment and
// the next pass re-reads it, so callers that would otherwise persist a reason skip it.
var errElasticDeploymentChanged = errors.New("deployment identity or generation changed")

func (r *ModelDeploymentReconciler) elasticLiveMembers(ctx context.Context, md *workercore.ModelDeployment) ([]core.Pod, error) {
	if r.APIReader == nil {
		return nil, fmt.Errorf("elastic observations require an uncached API reader")
	}
	live := new(workercore.ModelDeployment)
	if err := r.APIReader.Get(ctx, ctrlcli.ObjectKeyFromObject(md), live); err != nil {
		return nil, err
	}
	if live.UID != md.UID || live.Generation != md.Generation || live.DeletionTimestamp != nil {
		return nil, errElasticDeploymentChanged
	}
	// The list is narrowed to this deployment's identity labels so the pass does not walk the whole
	// namespace. The labels are only a server-side prefilter: elasticOwnedBy still carries the owner
	// UID, kind, API version and name, so a Pod a same-name recreated deployment left behind is
	// still excluded.
	list := new(core.PodList)
	if err := r.APIReader.List(ctx, list,
		ctrlcli.InNamespace(md.Namespace),
		ctrlcli.MatchingLabels{
			modelDeploymentLabelKeyName:     modelDeploymentLabelValueName,
			modelDeploymentLabelKeyInstance: md.Name,
		},
	); err != nil {
		return nil, err
	}
	role := ModelDeploymentElasticRole(md)
	var pods []core.Pod
	for i := range list.Items {
		pod := &list.Items[i]
		if !elasticOwnedBy(md, pod) {
			continue
		}
		if modelDeploymentPodRole(pod) == role.Name || modelDeploymentPodRole(pod) == modelDeploymentElasticHeadName(md) {
			pods = append(pods, *pod)
		}
	}
	slices.SortFunc(pods, func(a, b core.Pod) int { return strings.Compare(a.Name, b.Name) })
	return pods, nil
}

// reconcileModelDeploymentElasticResize is the only native mutation caller.
// The durable record precedes the request. A resumed sent record only observes.
func (r *ModelDeploymentReconciler) reconcileModelDeploymentElasticResize(ctx context.Context, md *workercore.ModelDeployment) error {
	// Implement this function in chapter 25.
	var result0 error
	return result0
}
