package worker

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/resource"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/controller"
	"gpustack.ai/gpustack/pkg/utils/quantityx"
	"gpustack.ai/gpustack/pkg/utils/strconvx"
)

// InstanceReconciler reconciles v1alpha1.Instance objects to finish the following tasks:
//   - Create/Update/Delete a Kubernetes Pod as the backing resource of the Instance based on the Instance spec.
//   - Create a Kubernetes Service to expose the ports of the backing Pod if specified in the Instance spec. The Service
//     belongs to the Instance rather than to the Pod, so it survives a stop and keeps its allocated node ports.
//   - Update the Instance status based on the backing Pod status for better visibility and stability.
type InstanceReconciler struct {
	Client    ctrlcli.Client
	APIReader ctrlcli.Reader
}

var _ ctrlreconcile.Reconciler = (*InstanceReconciler)(nil)

const (
	// InstancePhaseStarting is the Instance status phase reported when the
	// backing Pod is being created or updated to match the Instance spec.
	InstancePhaseStarting = "Starting"
	// InstancePhaseStopping is the Instance status phase reported when the
	// backing Pod is being deleted to stop the Instance.
	InstancePhaseStopping = "Stopping"
	// InstancePhaseStopped is the Instance status phase reported when the
	// backing Pod is deleted and the Instance is stopped.
	InstancePhaseStopped = "Stopped"
	// InstancePhaseReady is the Instance status phase reported when the
	// backing Pod is running and ready.
	InstancePhaseReady = "Ready"
	// InstancePhaseDeleting is the Instance status phase reported when the
	// Instance is marked as deleted and the backing Pod is being deleted.
	InstancePhaseDeleting = "Deleting"
)

func (r *InstanceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// Implement this function in chapter 07.
	var result0 ctrl.Result
	var result1 error
	return result0, result1
}

// additionalVolumeName is the Pod volume name of the additional volume at the given index.
func additionalVolumeName(i int) string {
	return "additional-" + strconvx.Itoa(i)
}

func (r *InstanceReconciler) SetupController(_ context.Context, opts controller.SetupOptions) error {
	// Implement this function in chapter 07.
	var result0 error
	return result0
}

// PartitionProfileMemoryPercent reports the share of one card's VRAM the requested hardware
// partition profile occupies, as a percentage in [1,100].
//
// It reports sizeable=false when the pool offers the profile but its observed Detail cannot size
// it yet — the profile's per-instance memory has not been populated, or the per-card VRAM has not
// — which is a transient state during detection or a device-manager rollout skew. The caller must
// reject such a request as retryable rather than fall back to whole-card sizing.
//
// It reports (0, true) when the request is not a partition request at all, or when the named
// profile is not offered. That second case is permanent, not transient, and each caller's own
// validation rejects it with a message naming the offered profiles.
//
// It sits beside getResourceRequirements rather than in the webhook package it was written for,
// because sizing a request against an InstanceType is now done by two callers — the Instance
// webhook, which defaults the values onto the object, and the ModelDeployment renderer, which
// derives them at render time because its own mutating webhook defaults the card count alone. A second copy of a VRAM-anchored percentage
// would be free to drift from this one, and the symptom of that drift is a Pod whose host CPU and
// memory do not match the fraction of the card it holds.
func PartitionProfileMemoryPercent(
	instType *workercore.InstanceType, profile string,
) (pct int64, sizeable bool) {
	if profile == "" {
		return 0, true
	}
	prof, _, found := PartitionProfile(instType, profile)
	if !found {
		return 0, true
	}
	if prof.MemoryMib <= 0 {
		return 0, false
	}
	cardVRAMMib, err := InstanceTypeCardVRAMMib(instType)
	if err != nil || cardVRAMMib <= 0 {
		return 0, false
	}
	return min(max(prof.MemoryMib*100/cardVRAMMib, 1), 100), true
}

// PartitionProfile finds a partition profile in an InstanceType's observed physical-slice
// inventory (Status.Detail), returning the aggregate (its per-instance MemoryMib and pool-wide
// instance ceiling Count), whether the accelerator Detail has been computed at all, and whether
// the profile was found.
func PartitionProfile(
	instType *workercore.InstanceType, profile string,
) (prof workercore.AcceleratorSlicedPhysicalDetailProfile, ready, found bool) {
	ready = instType.Status.Detail.AcceleratorReady()
	for _, p := range instType.Status.Detail.SlicedDetail.Physical.Profiles {
		if p.Name == profile {
			return p, ready, true
		}
	}
	return workercore.AcceleratorSlicedPhysicalDetailProfile{}, ready, false
}

// InstanceTypeCardVRAMMib parses the per-card VRAM (MiB) from an InstanceType's observed
// Status.Detail.Memory. An empty value is the not-yet-ready state (reject as retryable rather than
// mis-size); a non-positive or unparseable value is a hard error.
func InstanceTypeCardVRAMMib(instType *workercore.InstanceType) (int64, error) {
	memStr := instType.Status.Detail.Memory
	if memStr == "" {
		return 0, fmt.Errorf("instance type %s has no per-card memory yet (detail not ready)", instType.Name)
	}
	q, err := resource.ParseQuantity(memStr)
	if err != nil {
		return 0, fmt.Errorf("parse memory %q of instance type %s: %w", memStr, instType.Name, err)
	}
	mib := q.Value() / quantityx.Mi
	if mib <= 0 {
		return 0, fmt.Errorf("instance type %s has non-positive memory %q", instType.Name, memStr)
	}
	return mib, nil
}
