package worker

import (
	"fmt"

	core "k8s.io/api/core/v1"
)

const (
	// modelDeploymentKVLeaseWindow 是 kv_lease_duration 的默认值。
	//
	// 它不会因请求排队时间过长而过期，但当引擎 Pod 的心跳中断（如抢占、驱逐、重启）时确实会过期，
	// 且在默认失败策略下请求会直接失败。因此，运维人员需要借助该时间窗口
	// 将突发失败请求与下线副本关联起来。
	//
	// 此处将其声明为默认值而非从池中读取：该数值实际位于后端，由解析 Binding 的规则读取。
	// 在此之前，明确指明默认值并如实说明是严谨的做法，而直接硬编码一个本 operator 未曾读取的数值则不然。
	modelDeploymentKVLeaseWindow = "30s"

	// _PodReasonEvicted 是 kubelet 在驱逐 Pod（无论是因节点资源压力还是 Pod 超出本地存储限制）时
	// 设置的 Pod 状态原因。Kueue 抢占则表现为普通的删除操作，因此通过观测 Pod 自身状态
	// 可以覆盖这两种退出路径。
	_PodReasonEvicted = "Evicted"
)

// ModelDeployment 记录的副本相关事件原因。它们拆分为独立原因而非合并为一个，
// 因为运维人员过滤事件时需要明确知道究竟发生了哪一类事件。
const (
	modelDeploymentEventReplicaEvicted   = "ReplicaEvicted"
	modelDeploymentEventReplicaLeaving   = "ReplicaLeaving"
	modelDeploymentEventReplicaRestarted = "ReplicaRestarted"

	// modelDeploymentEventRenderFailed 报告本轮调谐完全无法构建副本的情况。
	//
	// 之所以设置该事件，是因为此类失败没有其他对读取方可见的承载位置。渲染失败会在写入任何 status 之前
	// 中止本轮调谐，导致对象一直停留在上一次的状态（Phase=Starting，附带“尚未创建任何副本”），
	// 且没有任何 condition 指明原因。若无此 Event，部署将长期处于看似启动缓慢的状态。
	//
	// 准入阶段无法代为拦截此类问题，原因并非缺少客户端（webhook 现已持有客户端），
	// 而是校验依赖 InstanceType 的已观测详情；新创建对象的该详情尚未收敛，
	// 若在准入阶段校验，会因与 InstanceType 调谐器的竞态而拒绝合法的部署。
	//
	// 重复上报会聚合成带计数的单条 Event 而非连续事件流，从而确保每轮触发的事件仍具有可读性。
	modelDeploymentEventRenderFailed = "RenderFailed"
)

// modelDeploymentReplicaDeparture 表示一个已停止从其共享缓存提供服务的副本及其原因。
type modelDeploymentReplicaDeparture struct {
	pod     string
	reason  string
	message string
}

// modelDeploymentReplicaDepartures 仅根据观测到的 Pod 列表识别哪些副本已经退出。
//
// 该设计完全基于观测状态而非删除钩子推导。钩子仅在可能丢失的事件（如管理器重启、watch 中断）上
// 触发一次，若关联信息仅存在于无人收到的事件中，就会彻底丢失记录。而直接读取 Pod 状态
// 能在每轮调谐中回答同一问题，即使错过某次事件也只会带来延迟，不会丢失记录。
//
// 因此，副本在退出过程中必须保持可观测性。这也是调谐器使用默认宽限期而非 ctrlclix.Terminated
// 删除副本的原因：完全没有宽限期的 Pod 可能在任何一轮调谐观测到之前就已消失。
//
// 重复上报由 API server 而非本函数处理：事件记录器会将相同事件聚合成带计数的一条记录，
// 因此重复上报仍在终止中的副本只会累加计数，而不会新增事件条目。
func modelDeploymentReplicaDepartures(pods []core.Pod) []modelDeploymentReplicaDeparture {
	var departures []modelDeploymentReplicaDeparture

	for i := range pods {
		pod := &pods[i]

		switch {
		case pod.Status.Reason == _PodReasonEvicted:
			// kubelet 的 message 是记录驱逐原因的唯一依据：被驱逐容器的日志会随驱逐一同丢失。
			why := ""
			if pod.Status.Message != "" {
				why = " (" + pod.Status.Message + ")"
			}
			departures = append(departures, modelDeploymentReplicaDeparture{
				pod:    pod.Name,
				reason: modelDeploymentEventReplicaEvicted,
				message: fmt.Sprintf(
					"%s was evicted%s; its cached blocks are lost to its siblings, and any KV "+
						"lease it holds lapses after the %s kv_lease_duration window, failing the "+
						"requests still waiting on them",
					modelDeploymentPodDescription(pod), why, modelDeploymentKVLeaseWindow,
				),
			})
		case pod.DeletionTimestamp != nil:
			departures = append(departures, modelDeploymentReplicaDeparture{
				pod:    pod.Name,
				reason: modelDeploymentEventReplicaLeaving,
				message: fmt.Sprintf(
					"%s is going away; its cached blocks are lost to its siblings, and any "+
						"KV lease it holds lapses after the %s kv_lease_duration window, failing "+
						"the requests still waiting on them",
					modelDeploymentPodDescription(pod), modelDeploymentKVLeaseWindow,
				),
			})
		default:
			restarts := modelDeploymentReplicaRestarts(pod)
			if restarts == 0 {
				continue
			}
			departures = append(departures, modelDeploymentReplicaDeparture{
				pod:    pod.Name,
				reason: modelDeploymentEventReplicaRestarted,
				message: fmt.Sprintf(
					"%s has restarted %d time(s); each restart interrupts the engine's "+
						"heartbeat, so the blocks it held were lost and any KV lease lapsed after "+
						"the %s kv_lease_duration window",
					modelDeploymentPodDescription(pod), restarts, modelDeploymentKVLeaseWindow,
				),
			})
		}
	}

	return departures
}

// modelDeploymentReplicaRestarts 表示一个副本的容器已重启的次数。
//
// 在此场景的所有关键方面，重启都等同于节点离开：引擎进程已终止，心跳随之停止，
// 其持有的 blocks 也一同丢失——即便 Pod 及其名称仍然保留。
func modelDeploymentReplicaRestarts(pod *core.Pod) int32 {
	var restarts int32
	for i := range pod.Status.ContainerStatuses {
		restarts += pod.Status.ContainerStatuses[i].RestartCount
	}

	return restarts
}
