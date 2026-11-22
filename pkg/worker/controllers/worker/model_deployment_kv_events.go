package worker

import (
	"fmt"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubeapistatus"
)

// ModelDeploymentConditionKVEventsPublishing 报告所有产生缓存块的角色是否均已配置为发布事件。
//
// 它反映的是配置状态，而非实际流量。已配置但随后崩溃的发布者在此处仍保持 True，
// 因为观测事件流需要活跃的消费者，属于另一项独立检查。
const ModelDeploymentConditionKVEventsPublishing kubeapistatus.ConditionType = "KVEventsPublishing"

const (
	modelDeploymentReasonKVEventsPublishing        = "Publishing"
	modelDeploymentReasonKVEventsNotApplicable     = "NotApplicable"
	modelDeploymentReasonKVEventsPublisherDisabled = "PublisherDisabled"
	modelDeploymentReasonKVEventsNoRouter          = "NoRouter"
	modelDeploymentReasonKVEventsRoleUnmanaged     = "RoleUnmanaged"
)

func observeModelDeploymentKVEvents(
	md, holder *workercore.ModelDeployment, manufacturers map[string]string,
) {
	if md.Spec.Router == nil {
		for i := range md.Spec.Roles {
			// 编码器（Encode）并非该消息所指的角色之一。它读取媒体数据并将 embedding 传递给 prefiller，
			// 因此既不产生也不消费缓存块；若将其计入，就会错误地报告部署需要为任何 router 都无法服务的角色配置 router。
			switch ModelDeploymentEffectiveRoleKind(&md.Spec.Roles[i]) {
			case workercore.ModelDeploymentRoleKindPrefill, workercore.ModelDeploymentRoleKindDecode:
				ModelDeploymentConditionKVEventsPublishing.False(holder,
					modelDeploymentReasonKVEventsNoRouter,
					"the deployment declares prefill and decode roles but no router consumes their cache events")
				return
			}
		}

		// 因此该消息可以直接指明角色类型，因为此处不可能出现编码器：上方的循环会在遇到 prefiller 或 decoder 时返回，
		// 且准入控制会拒绝没有 router 的 Encode 角色。该角色类型足够新，不存在早于该规则的历史持久化对象。
		ModelDeploymentConditionKVEventsPublishing.True(holder,
			modelDeploymentReasonKVEventsNotApplicable,
			"the unrouted deployment has only server roles, so nothing consumes cache events")
		return
	}

	configured := 0
	for i := range md.Spec.Roles {
		role := &md.Spec.Roles[i]
		// 消费者不会发布事件，编码器也是如此。编码器接收媒体数据并为 prefiller 生成 embedding，
		// 因此它并非本 operator 未能找到配置的生产者——若将其计入，会将健康的编码器部署误判为未配置，
		// 并指向一个根本不发布任何事件的角色。
		if kind := ModelDeploymentEffectiveRoleKind(role); kind == workercore.ModelDeploymentRoleKindDecode ||
			kind == workercore.ModelDeploymentRoleKindEncode {
			continue
		}
		if len(role.Command) > 0 {
			ModelDeploymentConditionKVEventsPublishing.Unknown(holder,
				modelDeploymentReasonKVEventsRoleUnmanaged,
				fmt.Sprintf("role %q replaced the whole command line, so the operator cannot say whether it publishes cache events", role.Name))
			return
		}
		if !modelDeploymentPublishesKVEvents(md, role, manufacturers[role.Name]) {
			ModelDeploymentConditionKVEventsPublishing.False(holder,
				modelDeploymentReasonKVEventsPublisherDisabled,
				fmt.Sprintf("role %q produces cache blocks but its rendered configuration does not enable event publishing", role.Name))
			return
		}
		configured++
	}

	ModelDeploymentConditionKVEventsPublishing.True(holder,
		modelDeploymentReasonKVEventsPublishing,
		fmt.Sprintf("all %d producing roles are configured to publish cache events", configured))
}
