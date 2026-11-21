package worker

import (
	"context"
	"maps"
	"strings"

	app "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	rbac "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/systemmeta"
	"gpustack.ai/gpustack/pkg/worker/kvcache/router"
)

const (
	modelDeploymentRouterConfigKey            = "epp.yaml"
	modelDeploymentRouterEnvoyConfigKey       = "envoy.yaml"
	modelDeploymentRouterSelectorKey          = "endpoint-selector"
	modelDeploymentRouterTargetPortsKey       = "endpoint-target-ports"
	modelDeploymentRouterExtraArgsKey         = "extra-args.json"
	modelDeploymentRouterConfigHash           = "modeldeployment.gpustack.ai/router-config-hash"
	modelDeploymentRouterLabelKey             = "modeldeployment.gpustack.ai/router"
	modelDeploymentRouterHTTPPort       int32 = 8000
	modelDeploymentRouterEPPPort        int32 = 9002
	// modelDeploymentRouterMetricsPort is where every router serves Prometheus metrics, so that one
	// scrape configuration reaches all of them. The llm-d picker listens here by default; the
	// routers configured by argv are told to, because their own default is elsewhere.
	modelDeploymentRouterMetricsPort  int32 = 9090
	modelDeploymentResourceNoteRouter       = "router"
)

// ModelDeploymentRouterObjects carries the complete namespaced object set one managed router needs
// and the typed contract rendered alongside it.
type ModelDeploymentRouterObjects struct {
	Deployment     *app.Deployment
	ConfigMap      *core.ConfigMap
	Service        *core.Service
	ServiceAccount *core.ServiceAccount
	Role           *rbac.Role
	RoleBinding    *rbac.RoleBinding

	// Contract is the typed projection of the same values used to render the objects above.
	Contract workercore.ModelDeploymentRouterStatus
}

func renderModelDeploymentRouterObjects(
	ctx context.Context, md *workercore.ModelDeployment, manufacturers map[string]string,
	retainedEligibility bool,
) (ModelDeploymentRouterObjects, error) {
	// Implement this function in chapter 20.
	var result0 ModelDeploymentRouterObjects
	var result1 error
	return result0, result1
}

func (r *ModelDeploymentReconciler) syncModelDeploymentRouter(
	ctx context.Context, md *workercore.ModelDeployment, eligibilityDecided bool,
) error {
	var rendered ModelDeploymentRouterObjects
	if md.Spec.Router != nil {
		// RETENTION: the discovery equality, once published, survives a quiet pass on EVERY
		// shipped profile. The llm-d profile publishes the ConfigMap; the vllm-router and
		// sglang-gateway profiles configure by argv and render none, so absence of a ConfigMap is
		// not proof a surface was never narrowed -- the live router Deployment's args and env are
		// read too. The static activatability half still governs: a deployment whose shapes
		// stopped being activatable loses the term on this pass.
		//
		// IT IS READ FROM THE DOCUMENT AS WELL AS THE SELECTOR KEY, because the two are two places
		// one requirement lives. Where a deployment's roles disagree about activatability the
		// requirement sits in a stage filter rather than the selector, and a pass that read only
		// the selector would find nothing, decide the gate had never been active, and put back the
		// endpoints the fault withdrew. The document is PARSED rather than searched: a model name
		// the user chose can be the very string of this label, and reading that as a demand would
		// activate a gate on a deployment that never decided one.
		needle := modelDeploymentLabelKeyEndpointEligible + "=" + modelDeploymentEndpointEligibleValue
		// THE MARKER IS THE OTHER NAME THE REQUIREMENT TRAVELS UNDER, and a deployment whose roles
		// disagree carries it instead of the eligibility term. A quiet pass must find the
		// requirement wherever it was published, or it would conclude the gate had never fired and
		// put back the endpoints the fault withdrew.
		candidateNeedle := modelDeploymentRouterCandidateLabel + "=" + modelDeploymentEndpointEligibleValue
		retained := false
		liveConfig := new(core.ConfigMap)
		err := r.Client.Get(ctx, ctrlcli.ObjectKey{Namespace: md.Namespace, Name: md.Name + "-router"}, liveConfig)
		switch {
		case err == nil:
			retained = strings.Contains(liveConfig.Data[modelDeploymentRouterSelectorKey], needle) ||
				strings.Contains(liveConfig.Data[modelDeploymentRouterSelectorKey], candidateNeedle) ||
				router.DocumentStageFilterDemands(
					liveConfig.Data[modelDeploymentRouterConfigKey],
					modelDeploymentLabelKeyEndpointEligible, modelDeploymentEndpointEligibleValue,
				) ||
				router.DocumentStageFilterDemands(
					liveConfig.Data[modelDeploymentRouterConfigKey],
					modelDeploymentRouterCandidateLabel, modelDeploymentEndpointEligibleValue,
				)
		case apierrors.IsNotFound(err):
		default:
			return err
		}
		if !retained {
			liveDeployment := new(app.Deployment)
			err = r.Client.Get(ctx, ctrlcli.ObjectKey{Namespace: md.Namespace, Name: md.Name + "-router"}, liveDeployment)
			switch {
			case err == nil:
				for _, container := range liveDeployment.Spec.Template.Spec.Containers {
					for _, arg := range container.Args {
						if strings.Contains(arg, needle) || strings.Contains(arg, candidateNeedle) {
							retained = true
						}
					}
					for _, env := range container.Env {
						if strings.Contains(env.Value, needle) || strings.Contains(env.Value, candidateNeedle) {
							retained = true
						}
					}
				}
			case apierrors.IsNotFound(err):
			default:
				return err
			}
		}

		rendered, err = renderModelDeploymentRouterObjects(
			ctx, md, r.modelDeploymentRoleManufacturers(ctx, md), retained || eligibilityDecided,
		)
		if err != nil {
			return err
		}
	}

	wanted := md.Spec.Router != nil
	if err := r.syncModelDeploymentOwnedChildren(ctx, md, objectIfPresent(rendered.Deployment, wanted),
		func() ctrlcli.Object { return new(app.Deployment) }, new(app.DeploymentList),
		func(actual, expected ctrlcli.Object) bool {
			return alignModelDeploymentRouterDeployment(actual.(*app.Deployment), expected.(*app.Deployment))
		}, modelDeploymentResourceNoteRouter, "deployment"); err != nil {
		return err
	}
	// A router configured entirely by argv renders no ConfigMap, so the desired list is empty for it
	// and the prune path removes one a previous shape left behind. The typed nil is tested for
	// rather than the interface, which would be non-nil while holding it.
	if err := r.syncModelDeploymentOwnedChildren(ctx, md,
		objectIfPresent(rendered.ConfigMap, wanted && rendered.ConfigMap != nil),
		func() ctrlcli.Object { return new(core.ConfigMap) }, new(core.ConfigMapList),
		func(actual, expected ctrlcli.Object) bool {
			return alignModelDeploymentRouterConfigMap(actual.(*core.ConfigMap), expected.(*core.ConfigMap))
		}, modelDeploymentResourceNoteRouter, "configmap"); err != nil {
		return err
	}
	if err := r.syncModelDeploymentOwnedChildren(ctx, md, objectIfPresent(rendered.Service, wanted),
		func() ctrlcli.Object { return new(core.Service) }, new(core.ServiceList),
		func(actual, expected ctrlcli.Object) bool {
			changed := alignModelDeploymentService(actual.(*core.Service), expected.(*core.Service))
			return alignModelDeploymentChildMetadata(actual, expected) || changed
		}, modelDeploymentResourceNoteRouter, "service"); err != nil {
		return err
	}
	if err := r.syncModelDeploymentOwnedChildren(ctx, md, objectIfPresent(rendered.ServiceAccount, wanted),
		func() ctrlcli.Object { return new(core.ServiceAccount) }, new(core.ServiceAccountList),
		alignModelDeploymentChildMetadata, modelDeploymentResourceNoteRouter, "serviceaccount"); err != nil {
		return err
	}
	if err := r.syncModelDeploymentOwnedChildren(ctx, md, objectIfPresent(rendered.Role, wanted),
		func() ctrlcli.Object { return new(rbac.Role) }, new(rbac.RoleList),
		func(actual, expected ctrlcli.Object) bool {
			role, want := actual.(*rbac.Role), expected.(*rbac.Role)
			changed := false
			if !kubemeta.DeepEqual(role.Rules, want.Rules) {
				role.Rules = want.Rules
				changed = true
			}
			return alignModelDeploymentChildMetadata(actual, expected) || changed
		}, modelDeploymentResourceNoteRouter, "role"); err != nil {
		return err
	}
	return r.syncModelDeploymentOwnedChildren(ctx, md, objectIfPresent(rendered.RoleBinding, wanted),
		func() ctrlcli.Object { return new(rbac.RoleBinding) }, new(rbac.RoleBindingList),
		func(actual, expected ctrlcli.Object) bool {
			binding, want := actual.(*rbac.RoleBinding), expected.(*rbac.RoleBinding)
			changed := false
			if !kubemeta.DeepEqual(binding.Subjects, want.Subjects) {
				binding.Subjects = want.Subjects
				changed = true
			}
			return alignModelDeploymentChildMetadata(actual, expected) || changed
		}, modelDeploymentResourceNoteRouter, "rolebinding")
}

func objectIfPresent(object ctrlcli.Object, present bool) []ctrlcli.Object {
	if !present {
		return nil
	}
	return []ctrlcli.Object{object}
}

// alignModelDeploymentRouterDeployment brings a live router Deployment up to what was rendered.
//
// IT DELIBERATELY DOES NOT COMPARE THE SELECTOR, which is immutable in apps/v1 and would make every
// later Update fail if it ever differed. The rendered selector embeds `spec.router.name`, so the
// only way to change it is to change that field -- and admission freezes it after creation. The
// value can differ between two deployments and never within one, which is what makes leaving the
// selector alone safe rather than merely untested.
//
// If that freeze is ever lifted, this function is where the breakage lands, and comparing the
// selector is NOT the fix: the object has to be deleted and recreated, because the API server will
// not accept the new selector on the existing one.
func alignModelDeploymentRouterDeployment(actual, expected *app.Deployment) (changed bool) {
	if !kubemeta.DeepEqual(actual.Spec.Replicas, expected.Spec.Replicas) {
		actual.Spec.Replicas = expected.Spec.Replicas
		changed = true
	}
	actualPod, expectedPod := &actual.Spec.Template, &expected.Spec.Template
	if !kubemeta.DeepEqual(actualPod.Labels, expectedPod.Labels) {
		actualPod.Labels = expectedPod.Labels
		changed = true
	}
	if !kubemeta.DeepEqual(actualPod.Annotations, expectedPod.Annotations) {
		actualPod.Annotations = expectedPod.Annotations
		changed = true
	}
	if actualPod.Spec.ServiceAccountName != expectedPod.Spec.ServiceAccountName {
		actualPod.Spec.ServiceAccountName = expectedPod.Spec.ServiceAccountName
		changed = true
	}
	// Compared although the pull policy below rides inside the containers: a secret added or
	// removed here changes what a pull can authenticate, and a field rendered but never aligned
	// is one an edit moves only by recreating the Deployment by hand.
	if !kubemeta.DeepEqual(actualPod.Spec.ImagePullSecrets, expectedPod.Spec.ImagePullSecrets) {
		actualPod.Spec.ImagePullSecrets = expectedPod.Spec.ImagePullSecrets
		changed = true
	}
	if !kubemeta.DeepEqual(actualPod.Spec.Volumes, expectedPod.Spec.Volumes) {
		actualPod.Spec.Volumes = expectedPod.Spec.Volumes
		changed = true
	}
	if len(actualPod.Spec.Containers) != len(expectedPod.Spec.Containers) {
		actualPod.Spec.Containers = expectedPod.Spec.Containers
		changed = true
	} else {
		for i := range expectedPod.Spec.Containers {
			if alignRenderedContainer(&actualPod.Spec.Containers[i], expectedPod.Spec.Containers[i]) {
				changed = true
			}
		}
	}

	return alignModelDeploymentChildMetadata(actual, expected) || changed
}

func alignModelDeploymentRouterConfigMap(actual, expected *core.ConfigMap) (changed bool) {
	if !maps.Equal(actual.Data, expected.Data) {
		actual.Data = expected.Data
		changed = true
	}
	return alignModelDeploymentChildMetadata(actual, expected) || changed
}

// alignModelDeploymentChildMetadata converges a child's labels and resource note onto the rendered
// ones. The note half is LOAD-BEARING rather than cosmetic: it is written at Create only, while the
// prune gate skips any child whose note is empty -- so a stripped note would make the object
// permanently exempt from pruning, and this is what keeps the gate honest.
func alignModelDeploymentChildMetadata(actual, expected ctrlcli.Object) (changed bool) {
	if !maps.Equal(actual.GetLabels(), expected.GetLabels()) {
		actual.SetLabels(maps.Clone(expected.GetLabels()))
		changed = true
	}
	if !systemmeta.EqualResourceTypeAndNotes(expected, actual) {
		systemmeta.SyncResourceTypeAndNotes(expected, actual)
		changed = true
	}
	return changed
}
