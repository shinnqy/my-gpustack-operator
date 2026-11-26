package worker

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"

	v1alpha1 "gpustack.ai/gpustack/api/worker/v1alpha1"
)

func scalerRatio(num, den, minRatio, target, maxRatio string) v1alpha1.ModelDeploymentScalerRoleRatio {
	return v1alpha1.ModelDeploymentScalerRoleRatio{
		NumeratorRole: num, DenominatorRole: den,
		Min: resourcePtr(minRatio), Target: resourcePtr(target), Max: resourcePtr(maxRatio),
	}
}

func resourcePtr(value string) *resource.Quantity {
	quantity := resource.MustParse(value)
	return &quantity
}

func constraintSpec(names ...string) v1alpha1.ModelDeploymentScalerSpec {
	roles := make([]v1alpha1.ModelDeploymentScalerRoleBounds, 0, len(names))
	for _, name := range names {
		roles = append(roles, v1alpha1.ModelDeploymentScalerRoleBounds{Name: name, MinReplicas: 1, MaxReplicas: 8})
	}
	return v1alpha1.ModelDeploymentScalerSpec{Roles: roles}
}

func roleState(name string, current, raw int32) ModelDeploymentScalerRoleState {
	return ModelDeploymentScalerRoleState{
		Name: name, CurrentReplicas: current, RawRecommendedReplicas: raw,
		CompleteCoverage: true,
		StabilizationHistory: []v1alpha1.ModelDeploymentScalerStabilizationSample{{
			Timestamp: metav1Time(time.Unix(0, 0)), Replicas: raw,
		}},
	}
}

func TestScalerConstraintsJointPacingAndRatioProjection(t *testing.T) {
	spec := constraintSpec("prefill", "decode")
	spec.Ratios = []v1alpha1.ModelDeploymentScalerRoleRatio{scalerRatio("prefill", "decode", "2", "2", "2")}
	spec.Roles[1].MaxReplicas = 3
	input := ModelDeploymentScalerConstraintInput{
		Spec: spec, Now: time.Unix(20, 0),
		Roles: []ModelDeploymentScalerRoleState{roleState("prefill", 4, 8), roleState("decode", 2, 2)},
	}
	input.Roles[1].StabilizationHistory = []v1alpha1.ModelDeploymentScalerStabilizationSample{{Timestamp: metav1Time(time.Unix(0, 0)), Replicas: 2}}
	result := SolveModelDeploymentScalerConstraints(input)
	if result.Replicas["prefill"] != 6 || result.Replicas["decode"] != 3 {
		t.Fatalf("joint projection = %#v, intervals=%#v, reason=%q", result.Replicas, result.Intervals, result.BlockingReason)
	}
}

func TestScalerConstraintsRatioConflict(t *testing.T) {
	spec := constraintSpec("prefill", "decode")
	spec.Ratios = []v1alpha1.ModelDeploymentScalerRoleRatio{scalerRatio("prefill", "decode", "1.5", "1.5", "1.5")}
	spec.Roles[0].MaxReplicas = 1
	spec.Roles[1].MaxReplicas = 1
	result := SolveModelDeploymentScalerConstraints(ModelDeploymentScalerConstraintInput{
		Spec: spec, Now: time.Unix(20, 0), Roles: []ModelDeploymentScalerRoleState{
			roleState("prefill", 1, 1), roleState("decode", 1, 1),
		},
	})
	if result.Replicas["prefill"] != 1 || result.Replicas["decode"] != 1 || result.BlockingReason != v1alpha1.ModelDeploymentScalerReasonRatioConflict {
		t.Fatalf("ratio conflict = %#v reason=%q", result.Replicas, result.BlockingReason)
	}
}
