package apistatus

import (
	"gpustack.ai/gpustack/pkg/kubeapistatus"
)

const (
	// ClusterQueueConditionActive indicates the cluster queue is active,
	// which means the cluster queue is ready to admit workloads.
	ClusterQueueConditionActive kubeapistatus.ConditionType = "Active"
	// ClusterQueueConditionTopologyReady indicates that every managed flavor is TAS-enabled,
	// selects disjoint Nodes, references an existing Topology, and conserves physical quota.
	ClusterQueueConditionTopologyReady kubeapistatus.ConditionType = "TopologyReady"
)
