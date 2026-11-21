package worker

import (
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

const (
	// modelDeploymentDefaultTerminationGracePeriodSeconds is the whole time a departing replica gets,
	// from the delete to the kill, when its role sets no terminationGracePeriodSeconds. It is the
	// Kubernetes default, rendered explicitly because the drain below is budgeted against it: the
	// kubelet starts this countdown BEFORE the preStop hook and sends SIGTERM only once the hook
	// returns, so the hook and the engine's own exit share it.
	//
	// A role may raise it, and the default stays at the floor on purpose: a longer grace holds the
	// replica's accelerators and quota for longer on every delete, and a rollout replaces one replica
	// per role at a time and waits each departing one out, so the grace multiplies into every
	// rollout. The role that needs more is the one whose engine takes longer to exit, not every role.
	modelDeploymentDefaultTerminationGracePeriodSeconds int64 = 30

	// modelDeploymentDrainSettleSeconds is how long the engine keeps serving normally after the
	// delete before the hook starts waiting for it to go idle.
	//
	// EVERY SUPPORTED ROUTER DROPS A REPLICA ON ITS DELETION TIMESTAMP, not on its readiness -- the
	// llm-d endpoint picker, vllm-router and the SGLang gateway all key on the timestamp -- but a
	// router can pick this replica in the instant before it learns of the delete. The settle is the
	// window that request arrives in. Reading the in-flight count at once would find it zero, end the
	// hook, and send that request to an engine that is already shutting down.
	modelDeploymentDrainSettleSeconds int64 = 5

	// modelDeploymentDrainExitSeconds is the part of the grace kept for the engine to exit once the
	// hook returns and SIGTERM arrives. It holds no request: by then the replica has been idle, or
	// the hook gave up on the requests still running.
	modelDeploymentDrainExitSeconds int64 = 5
)

// modelDeploymentTerminationGracePeriodSeconds is the grace a replica of the role is rendered with:
// the role's own value, or the default when it sets none.
func modelDeploymentTerminationGracePeriodSeconds(role *workercore.ModelDeploymentRole) int64 {
	if role.TerminationGracePeriodSeconds != nil {
		return *role.TerminationGracePeriodSeconds
	}

	return modelDeploymentDefaultTerminationGracePeriodSeconds
}

// modelDeploymentInFlightMetrics names, per engine, the gauges whose sum is the work a replica
// still has in flight. They are read off the engine's own /metrics, which both engines serve on
// their API port: vLLM by default, SGLang because the rendered command carries --enable-metrics.
//
// A request a decoder is still waiting to receive KV for is IN these sums, and it is the one a
// naive "running requests" read would miss. vLLM parks it as WAITING_FOR_REMOTE_KVS and counts it
// under num_requests_waiting; SGLang keeps it in its prealloc and transfer queues, which it reports
// as gauges of their own, and a prefiller's bootstrap and in-flight transfer queues are the same on
// the other side of the pair.
var modelDeploymentInFlightMetrics = map[string][]string{
	workercore.ModelDeploymentEngineVLLM: {
		"vllm:num_requests_running",
		"vllm:num_requests_waiting",
	},
	workercore.ModelDeploymentEngineSGLang: {
		"sglang:num_running_reqs",
		"sglang:num_queue_reqs",
		"sglang:num_prefill_bootstrap_queue_reqs",
		"sglang:num_prefill_inflight_queue_reqs",
		"sglang:num_decode_prealloc_queue_reqs",
		"sglang:num_decode_transfer_queue_reqs",
	},
}
