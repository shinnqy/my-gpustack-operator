package deviceplugin

import (
	"sort"

	"github.com/go-logr/logr"
	core "k8s.io/api/core/v1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/utils/json"
	"gpustack.ai/gpustack/pkg/utils/stringx"
)

// BuildDesiredStatus rebuilds a node's Devices Status wholesale from Spec + Pod annotations: it
// seeds every accelerator from Spec, then merges in what each Pod's annotation says it holds. The
// two steps run as one pass — mergePodAllocations both folds the physical-slice profile ledger and
// collects the live pod-UID set in the same walk it merges allocations in — so there is never a
// second, separately-applied write that a later pass could stomp.
//
// It is exported for the worker's node-devices AdmissionCheck, which rebuilds the ledger from the
// same Pod list it reads charged Pods from. The published Status is a later copy of this result.
// Judging against that copy would let a Pod count as charged before the ledger shows its
// allocation.
func BuildDesiredStatus(
	logger logr.Logger, devs *workercore.Devices, podList *core.PodList,
) (workercore.DevicesStatus, []string) {
	// Implement this function in chapter 08.
	var result0 workercore.DevicesStatus
	var result1 []string
	return result0, result1
}

const (
	// AllocatedAcceleratorAnnoKey names the annotation carrying a Pod's per-container accelerator
	// allocation, as the JSON of PodAllocations. It is the only durable record of what a container
	// holds — the in-process reservations do not survive a device-manager restart — so the ledger
	// rebuild, the reclaim loops and the Pod-delete watch are all gated on it.
	AllocatedAcceleratorAnnoKey       = "device.gpustack.ai/accelerator.allocated"
	_PreferredAcceleratorIDAnnoKey    = "device.gpustack.ai/accelerator.preferred-id"
	_PreferredAcceleratorIndexAnnoKey = "device.gpustack.ai/accelerator.preferred-index"
)

type (
	// ContainerAllocation is what the device plugin allocated for one container of a pod.
	//
	// DeviceIDs are the device IDs kubelet offered for that container's Allocate. They are recorded
	// rather than re-derived because a family whose Allocate picks the accelerator itself hands
	// back devices those IDs do not name, and because the IDs must keep being advertised Healthy
	// for as long as the allocation lives: kubelet refuses to re-admit a container whose
	// checkpointed IDs have left the healthy set.
	ContainerAllocation struct {
		Devices   workercore.DevicesStatus `json:"devices"`
		DeviceIDs []string                 `json:"deviceIDs,omitempty"`
	}

	// PodAllocations is the value of the AllocatedAcceleratorAnnoKey annotation, keyed by container
	// name. The container dimension is what stops a second Allocate from erasing the first
	// container's claim, and what makes a repeated Allocate for one container overwrite its own
	// entry instead of double-counting the accelerator.
	PodAllocations map[string]ContainerAllocation
)

// Aggregate folds every container's record into the pod-wide allocation the ledger consumes. Two
// containers holding the same accelerator keep separate entries, so the accelerator is charged for
// both. Containers are visited in name order, so the result does not depend on map iteration.
func (in PodAllocations) Aggregate() workercore.DevicesStatus {
	names := make([]string, 0, len(in))
	for name := range in {
		names = append(names, name)
	}
	sort.Strings(names)

	var (
		aggregated workercore.DevicesStatus
		groupIndex = make(map[string]int)
	)
	for _, name := range names {
		groups := in[name].Devices.Groups
		for i := range groups {
			src := &groups[i]
			idx, ok := groupIndex[src.ID]
			if !ok {
				idx = len(aggregated.Groups)
				groupIndex[src.ID] = idx
				aggregated.Groups = append(aggregated.Groups, workercore.DevicesAllocationGroup{
					ID:           src.ID,
					Manufacturer: src.Manufacturer,
				})
			}
			dst := &aggregated.Groups[idx]
			dst.Accelerators = append(dst.Accelerators, src.Accelerators...)
		}
	}
	return aggregated
}

// AllocatedAcceleratorsOf reads a pod's per-container allocation records. An unannotated pod
// yields an empty map and no error.
func AllocatedAcceleratorsOf(pod *core.Pod) (PodAllocations, error) {
	str := pod.Annotations[AllocatedAcceleratorAnnoKey]
	if str == "" {
		return nil, nil
	}
	var allocations PodAllocations
	if err := json.Unmarshal(stringx.ToBytes(&str), &allocations); err != nil {
		return nil, err
	}
	return allocations, nil
}
