package worker

import (
	"context"
	"fmt"
	"slices"

	apps "k8s.io/api/apps/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/utils/ptr"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"

	gpustack "gpustack.ai/gpustack/api/v1"
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/worker/kuberess"
	"gpustack.ai/gpustack/pkg/worker/kvcache/mooncake"
)

// reportRolloutComplete 用于回答主节点的最后更新是否完成，对于这种工作负载而言，这是 Kubernetes 无法解答的问题。
//
// Deployment 对其的描述是“进展状态”，由截止时间驱动，一旦有待处理任务，此操作器就会禁用该状态。必须满足以下
// 条件：控制器完整性测试要求 availableReplicas 等于 spec.replicas，而此处只有一份副本可用，因为待处理任务故
// 意未准备好。如果保持启用状态，进展状态会在滚动更新完成后停止推进——就像无法拉取镜像时一样——因此，超时的两
// 个答案是相同的，而答案相同的检查则不是有效的检查。
//
// 这是那个删除了结构性错误条款的测试。updatedReplicas 和副本在健康的滚动更新环境中都达到了 spec.replicas 的
// 数量，因此这两个参数可以区分出 triple 无法达到的位置。
//
// 禁止：无法将其与健康规则结合。就绪状态询问是否存在主节点，这是关于健康的正确问题，对于滚动更新则不是；带
// 有此条款的健康谓词会认为在每次普通更新期间，正常运行的后端系统处于不健康状态。这两个问题都涉及两个条件。
func (r *KVCacheBackendReconciler) reportRolloutComplete(
	ctx context.Context, kvcb, holder *workercore.KVCacheBackend,
) {
	dropped := func() {
		holder.Status.Conditions = slices.DeleteFunc(holder.Status.Conditions,
			func(c gpustack.Condition) bool {
				return c.Type == string(KVCacheBackendConditionRolloutComplete)
			})
	}

	if kvcb.Spec.Connection.Managed == nil {
		// 外部后端没有我们的工作负载，因此没有需要完成的滚动更新。
		dropped()
		return
	}

	deployName := mooncake.LeaderObjectName(kvcb)
	deploy := new(apps.Deployment)
	err := r.Client.Get(ctx, ctrlcli.ObjectKey{
		Name:      deployName,
		Namespace: kuberess.SystemNamespaceName,
	}, deploy)

	switch {
	case kerrors.IsNotFound(err):
		// 目前尚未完成任何渲染工作，或者一次传递无法完成渲染。将滚动更新报告为未完成，实际上是指该
		// 滚动更新从未开始。
		dropped()

	case err != nil:
		KVCacheBackendConditionRolloutComplete.Unknown(holder, "WorkloadUnreadable", fmt.Sprintf(
			"the leader's deployment %q could not be read: %v",
			deployName, clipFaultDetail(err.Error()),
		))

	case deploy.Status.ObservedGeneration < deploy.Generation:
		// 工作负载控制器并未查看这位操作员刚刚制定的规范，因此下面的所有描述都指的是之前的版本。从
		// 他们的角度来回答，才使得一个完成后的旧滚动更新看起来就像完成后的新版本一样。
		KVCacheBackendConditionRolloutComplete.Unknown(holder, "UpdateNotObserved", fmt.Sprintf(
			"the deployment controller has not observed generation %d of %q yet, so its replica "+
				"counts still describe the update before it", deploy.Generation, deployName,
		))

	case ptr.Deref(deploy.Spec.Replicas, 1) != mooncake.LeaderReplicas(kvcb.Spec.Connection.Managed.Leader):
		// 将未当选的主节点提升至一个级别需要两次写入操作，在这两次写入之间，Deployment 会运行所选模
		// 板的其中一个副本，而后端则会请求更多。如果下面的计数与 Deployment 的自身规范一致，那么没
		// 有这些操作的话，两次写入之间的暂停时间将被视为一次完成的交付。
		KVCacheBackendConditionRolloutComplete.False(holder, "ReplicasPending", fmt.Sprintf(
			"the deployment %q runs %d leader replicas and this backend asks for %d; the count is "+
				"raised once no replica of the previous template is left",
			deployName, ptr.Deref(deploy.Spec.Replicas, 1),
			mooncake.LeaderReplicas(kvcb.Spec.Connection.Managed.Leader),
		))

	default:
		desired := int32(1)
		if deploy.Spec.Replicas != nil {
			desired = *deploy.Spec.Replicas
		}
		if deploy.Status.UpdatedReplicas == desired && deploy.Status.Replicas == desired {
			KVCacheBackendConditionRolloutComplete.True(holder, "Complete", fmt.Sprintf(
				"all %d leader replicas run the current template, and none of the previous one is "+
					"left", desired,
			))
			return
		}
		KVCacheBackendConditionRolloutComplete.False(holder, "Progressing", fmt.Sprintf(
			"%d of %d leader replicas run the current template and %d replicas exist in total; a "+
				"rollout that stays here is one that will not finish on its own, because this "+
				"workload disables the deadline that would otherwise say so",
			deploy.Status.UpdatedReplicas, desired, deploy.Status.Replicas,
		))
	}
}
