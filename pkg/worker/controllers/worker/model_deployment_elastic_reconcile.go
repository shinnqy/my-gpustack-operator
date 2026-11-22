package worker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubeapistatus"
)

const (
	modelDeploymentElasticWithdrawnAnnotation = "modeldeployment.gpustack.ai/elastic-withdrawn"
	elasticObservationLabel                   = "gpustack.ai/elastic-observation"

	// ModelDeploymentConditionElasticResize 报告弹性扩缩容是否处于阻塞或最终被拒状态。
	// 它声明在决定该状态的收敛逻辑旁，与其他状态条件一样，其语义词表与可观测该状态的代码放在一起。
	// 仅当持久化记录表明重试上限已耗尽时，其状态为 False 且原因为 ScaleRefused；
	// 当无记录存在且实时宽度与 spec 不一致又无法自证时，其状态为 Unknown 且原因为 WorldUnprovable；
	// 其余时间均为 True 且原因为 NoRefusal：仍在重试上限内的拒绝并非终态，不应被解读为终态拒绝，
	// 而无法证明的外部世界状态也不属于拒绝。
	ModelDeploymentConditionElasticResize kubeapistatus.ConditionType = "ElasticResize"

	// modelDeploymentReasonScaleRefused 是 ElasticResize 为 False 时的原因：操作记录已放弃，
	// 其消息携带引擎自身最后一次拒绝的详情。
	modelDeploymentReasonScaleRefused = "ScaleRefused"

	// modelDeploymentReasonWorldUnprovable 是 ElasticResize 为 Unknown 时的原因：无操作记录存在，
	// 实时宽度与 spec 不一致或无法确定，纠正性扩缩容被阻塞，直到某一轮调谐能证明其将要作用的实际状态。
	// 其消息包含期望宽度、观测宽度以及失败的证明项。
	modelDeploymentReasonWorldUnprovable = "WorldUnprovable"

	// modelDeploymentReasonNoScaleRefusal 是 ElasticResize 为 True 时的原因：当前没有处于拒绝状态的记录，
	// 无论是不存在记录、有操作正在进行中，还是操作刚刚完成。
	modelDeploymentReasonNoScaleRefusal = "NoRefusal"
)

type modelDeploymentElasticRayReader interface {
	Observe(context.Context, *workercore.ModelDeployment, []modelDeploymentElasticCapturedMember,
		int32, string) (modelDeploymentElasticObservation, error)
}

func elasticOwnedBy(md *workercore.ModelDeployment, obj ctrlcli.Object) bool {
	owner := meta.GetControllerOf(obj)
	return owner != nil && owner.Kind == "ModelDeployment" &&
		owner.APIVersion == workercore.GroupVersion.String() && owner.Name == md.Name && owner.UID == md.UID && obj.GetUID() != ""
}

// errElasticDeploymentChanged 表示实时部署已不再匹配本轮调谐捕获的身份或 generation。
// 它并非读取失败：spec watch 会将部署重新入队并在下一轮重新读取，
// 因此原本会持久化错误原因的调用方应跳过持久化。
var errElasticDeploymentChanged = errors.New("deployment identity or generation changed")

func (r *ModelDeploymentReconciler) elasticLiveMembers(ctx context.Context, md *workercore.ModelDeployment) ([]core.Pod, error) {
	if r.APIReader == nil {
		return nil, fmt.Errorf("elastic observations require an uncached API reader")
	}
	live := new(workercore.ModelDeployment)
	if err := r.APIReader.Get(ctx, ctrlcli.ObjectKeyFromObject(md), live); err != nil {
		return nil, err
	}
	if live.UID != md.UID || live.Generation != md.Generation || live.DeletionTimestamp != nil {
		return nil, errElasticDeploymentChanged
	}
	// 列表查询通过当前部署的身份标签进行缩窄，避免遍历整个 Namespace。
	// 这些标签仅作服务端预过滤：elasticOwnedBy 仍会校验属主的 UID、kind、API 版本和名称，
	// 因此同名重建的部署所遗留的 Pod 依然会被排除。
	list := new(core.PodList)
	if err := r.APIReader.List(ctx, list,
		ctrlcli.InNamespace(md.Namespace),
		ctrlcli.MatchingLabels{
			modelDeploymentLabelKeyName:     modelDeploymentLabelValueName,
			modelDeploymentLabelKeyInstance: md.Name,
		},
	); err != nil {
		return nil, err
	}
	role := ModelDeploymentElasticRole(md)
	var pods []core.Pod
	for i := range list.Items {
		pod := &list.Items[i]
		if !elasticOwnedBy(md, pod) {
			continue
		}
		if modelDeploymentPodRole(pod) == role.Name || modelDeploymentPodRole(pod) == modelDeploymentElasticHeadName(md) {
			pods = append(pods, *pod)
		}
	}
	slices.SortFunc(pods, func(a, b core.Pod) int { return strings.Compare(a.Name, b.Name) })
	return pods, nil
}

// reconcileModelDeploymentElasticResize 是唯一的原生变更调用方。
// 持久化记录先于请求写入；恢复的已发送记录仅执行观测。
func (r *ModelDeploymentReconciler) reconcileModelDeploymentElasticResize(ctx context.Context, md *workercore.ModelDeployment) error {
	// 在第 25 章中实现此函数。
	var result0 error
	return result0
}
