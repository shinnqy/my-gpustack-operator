package nodefeature

import (
	core "k8s.io/api/core/v1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

// rdmaResourceNameBase is the base of the RDMA resource keys: the operator's own device
// prefix, under which every node-level resource this operator advertises itself lives, plus
// the family name. It carries no manufacturer, because a network interface belongs to the
// node rather than to a vendor.
const rdmaResourceNameBase = VisibilityResourceNamePrefix + "rdma"

// GetRDMAResourceName returns the node-level RDMA resource key the given allocation mode is
// served under. There are three: "device.gpustack.ai/rdma", "device.gpustack.ai/rdma.shared"
// and "device.gpustack.ai/rdma.partitioned".
//
// Sliced names no key, and that absence is the deliberate part. It once returned a ".sliced"
// key whose contract was word for word the shared one, over the same HCAs, with the two not
// decrementing each other. Two keys over one pool is not what two extended resources mean to a
// scheduler: one endpoint then admitted a full complement of holders under each name, so the
// ceiling either key appeared to set was not a ceiling at all. The symmetry it bought -- letting
// a workload spell its RDMA request with the same suffix as its accelerator request -- did not
// pay for a number that does not hold.
//
// Visibility names none either, for a different reason: a visibility allocation names an
// endpoint another container of the same Pod holds, and no RDMA allocation record is written to
// answer that from, so the mode is unserved rather than half-served. None and any unrecognized
// mode likewise return "", because an unknown mode must not silently name the whole-function key.
func GetRDMAResourceName(mode workercore.DeviceAllocationMode) core.ResourceName {
	switch mode {
	case workercore.DeviceAllocationModeExclusive:
		return rdmaResourceNameBase
	case workercore.DeviceAllocationModeShared:
		return rdmaResourceNameBase + SharedResourceNameSuffix
	case workercore.DeviceAllocationModePartitioned:
		return rdmaResourceNameBase + PartitionedResourceNameSuffix
	default:
		return ""
	}
}
