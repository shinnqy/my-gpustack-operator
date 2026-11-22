package worker

import (
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

const (
	// modelDeploymentDefaultTerminationGracePeriodSeconds 是当角色未设置 terminationGracePeriodSeconds 时，
	// 正在退出的副本从收到删除请求到被强制终止所能获得的全部时间。这是 Kubernetes 的默认值，此处显式列出
	// 是因为下方的排空（drain）预算基于它计算：kubelet 会在执行 preStop hook 之前启动该倒计时，
	// 并且仅在 hook 返回后才发送 SIGTERM，因此 hook 与引擎自身的退出过程共享这段时间。
	//
	// 角色可以调高该值，但默认值有意维持在下限：更长的宽限期会在每次删除时更久地占用副本的加速器和配额；
	// 而在滚动更新期间，每个角色每次只替换一个副本并等待旧副本完全退出，因此宽限期会成倍叠加到每次滚动
	// 更新中。只有退出耗时更长的引擎所在角色才需要调高该值，而非所有角色。
	modelDeploymentDefaultTerminationGracePeriodSeconds int64 = 30

	// modelDeploymentDrainSettleSeconds 是引擎在收到删除请求后、preStop hook 开始等待其变为空闲之前，
	// 继续正常提供服务的时长。
	//
	// 所有受支持的路由器都会依据副本的 DeletionTimestamp（而非就绪状态）将其摘除——llm-d endpoint picker、
	// vllm-router 和 SGLang gateway 均以该时间戳为准——但路由器仍可能在感知到删除前的瞬间将请求路由到
	// 该副本。这段静置窗口正是用来容纳此类在途请求的。若立即读取在途计数，可能会误判为零并提前结束
	// hook，从而导致该请求被发往已开始关闭的引擎。
	modelDeploymentDrainSettleSeconds int64 = 5

	// modelDeploymentDrainExitSeconds 是宽限期中为引擎预留的退出时间，用于 preStop hook 返回且
	// SIGTERM 送达之后。该阶段不再承载任何请求：此时副本要么已经空闲，要么 hook 已放弃等待仍在运行的请求。
	modelDeploymentDrainExitSeconds int64 = 5
)

// modelDeploymentTerminationGracePeriodSeconds 返回为该角色副本渲染的终止宽限期：
// 若角色自身设置了该值则使用角色的值，否则使用默认值。
func modelDeploymentTerminationGracePeriodSeconds(role *workercore.ModelDeploymentRole) int64 {
	if role.TerminationGracePeriodSeconds != nil {
		return *role.TerminationGracePeriodSeconds
	}

	return modelDeploymentDefaultTerminationGracePeriodSeconds
}

// modelDeploymentInFlightMetrics 按引擎列出所有用于统计副本当前在途工作量的 gauge 指标名（求和后即总在途量）。
// 这些指标读取自引擎自身的 /metrics 接口，两个引擎均在其 API 端口提供该接口：vLLM 默认开启，
// SGLang 则因渲染的启动命令包含 --enable-metrics 而开启。
//
// decoder 仍在等待接收 KV 的请求也包含在这些总和之中，而朴素的“正在运行请求数”读取方式会漏掉这类请求。
// vLLM 将其状态标记为 WAITING_FOR_REMOTE_KVS 并计入 num_requests_waiting；SGLang 将其保留在
// prealloc 和 transfer 队列中并作为独立 gauge 上报；而在配对的另一侧，prefiller 的 bootstrap 和
// 在途传输队列也是同样的原理。
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
