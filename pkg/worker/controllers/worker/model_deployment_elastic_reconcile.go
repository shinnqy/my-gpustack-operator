package worker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/deviceplugin"
	"gpustack.ai/gpustack/pkg/kubeapistatus"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/utils/json"
	"gpustack.ai/gpustack/pkg/worker/elasticengine"
	"gpustack.ai/gpustack/pkg/worker/elasticprofile"
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

// 内部观测文档分别保存各层读数，以及最近一次证明过的稳定宽度。上次的数值只用于选择本轮从哪个宽度开始探测，
// 执行引擎命令仍需本轮证据。
type modelDeploymentElasticStatus struct {
	DeploymentUID      types.UID             `json:"deploymentUID"`
	ObservedGeneration int64                 `json:"observedGeneration"`
	Desired            elasticLayer          `json:"desired"`
	Admitted           elasticLayer          `json:"admitted"`
	Allocated          elasticLayer          `json:"allocated"`
	Ray                elasticLayer          `json:"ray"`
	Effective          elasticLayer          `json:"effective"`
	State              elasticOperationState `json:"state"`
	Reason             string                `json:"reason"`
	StableWidth        int                   `json:"stableWidth"`
	MasterUID          types.UID             `json:"masterUID"`
}

type modelDeploymentElasticRayReader interface {
	Observe(context.Context, *workercore.ModelDeployment, []modelDeploymentElasticCapturedMember,
		int32, string) (modelDeploymentElasticObservation, error)
}

func modelDeploymentElasticObservationName(md *workercore.ModelDeployment) string {
	return "elastic-observation-" + string(md.UID)
}

func (r *ModelDeploymentReconciler) elasticObservationDocument(ctx context.Context, md *workercore.ModelDeployment) (*core.ConfigMap, error) {
	cm := new(core.ConfigMap)
	err := r.APIReader.Get(ctx, ctrlcli.ObjectKey{Namespace: md.Namespace, Name: modelDeploymentElasticObservationName(md)}, cm)
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err := elasticControllingOwnerIs(cm, md.Name, md.UID); err != nil {
		return nil, err
	}
	return cm, nil
}

func (r *ModelDeploymentReconciler) persistElasticObservation(
	ctx context.Context, md *workercore.ModelDeployment, status modelDeploymentElasticStatus,
) error {
	live := new(workercore.ModelDeployment)
	if err := r.APIReader.Get(ctx, ctrlcli.ObjectKeyFromObject(md), live); err != nil {
		return err
	}
	if live.UID != md.UID || live.Generation != md.Generation || live.DeletionTimestamp != nil {
		return fmt.Errorf("deployment changed before observation persistence")
	}
	cm, err := r.elasticObservationDocument(ctx, md)
	if err != nil {
		return err
	}
	if cm != nil {
		var prior modelDeploymentElasticStatus
		if err := json.Unmarshal([]byte(cm.Data["observation.json"]), &prior); err != nil {
			return err
		}
		if status.StableWidth == 0 && prior.MasterUID == status.MasterUID {
			status.StableWidth = prior.StableWidth
		}
	}
	body, err := json.Marshal(status)
	if err != nil {
		return err
	}
	if cm == nil {
		cm = &core.ConfigMap{
			ObjectMeta: meta.ObjectMeta{
				Name: modelDeploymentElasticObservationName(md), Namespace: md.Namespace,
				Labels: map[string]string{elasticObservationLabel: "true"},
				OwnerReferences: []meta.OwnerReference{{
					APIVersion: workercore.GroupVersion.String(), Kind: "ModelDeployment",
					Name: md.Name, UID: md.UID, Controller: elasticControllerRef(),
				}},
			},
			Data: map[string]string{"observation.json": string(body)},
		}
		return r.Client.Create(ctx, cm)
	}
	if cm.Data["observation.json"] == string(body) {
		return nil
	}
	cm.Data = map[string]string{"observation.json": string(body)}
	return r.Client.Update(ctx, cm)
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

// [IMPORTANT: elastic EP] 识别出谁是 CPU Head、谁是 ordinal=0 的 Master、谁是 Worker，确保主容器正在运行且 Master 有 IP，然后构造连向 Master 的 elasticengine.Client。
func elasticRuntimeMembers(md *workercore.ModelDeployment, pods []core.Pod) ([]modelDeploymentElasticCapturedMember, *core.Pod, error) {
	role := ModelDeploymentElasticRole(md)
	var captured []modelDeploymentElasticCapturedMember
	var master *core.Pod
	heads := 0
	for i := range pods {
		pod := &pods[i]
		memberRole := modelDeploymentElasticRoleWorker
		if modelDeploymentPodRole(pod) == modelDeploymentElasticHeadName(md) {
			memberRole = modelDeploymentElasticRoleHead
			heads++
		} else {
			ordinal, valid := modelDeploymentPodOrdinal(pod)
			if !valid {
				return nil, nil, fmt.Errorf("GPU member has no ordinal")
			}
			if ordinal == 0 {
				if master != nil {
					return nil, nil, fmt.Errorf("multiple reserved masters")
				}
				master = pod
				memberRole = modelDeploymentElasticRoleMaster
			}
			if modelDeploymentPodRole(pod) != role.Name {
				return nil, nil, fmt.Errorf("foreign role")
			}
		}
		running := false
		for _, c := range pod.Status.ContainerStatuses {
			if c.Name == modelDeploymentMainContainerName && c.ContainerID != "" && c.State.Running != nil {
				running = true
			}
		}
		if !running {
			return nil, master, fmt.Errorf("member %s has no running engine identity", pod.Name)
		}
		captured = append(captured, modelDeploymentElasticCapturedMember{
			PodUID: pod.UID, PodName: pod.Name, Container: modelDeploymentMainContainerName, Role: memberRole,
		})
	}
	if master == nil || heads != 1 || master.Status.PodIP == "" {
		return nil, master, fmt.Errorf("reserved master or CPU head is not ready for observation")
	}
	return captured, master, nil
}

func elasticSameRuntime(before, after []core.Pod) bool {
	if len(before) != len(after) {
		return false
	}
	for i := range before {
		a, b := &before[i], &after[i]
		if a.Name != b.Name || a.UID != b.UID || a.Status.PodIP != b.Status.PodIP || a.DeletionTimestamp != nil || b.DeletionTimestamp != nil ||
			!reflect.DeepEqual(a.OwnerReferences, b.OwnerReferences) || !reflect.DeepEqual(a.Status.ContainerStatuses, b.Status.ContainerStatuses) ||
			!reflect.DeepEqual(a.Spec, b.Spec) ||
			a.Annotations[deviceplugin.AllocatedAcceleratorAnnoKey] != b.Annotations[deviceplugin.AllocatedAcceleratorAnnoKey] {
			return false
		}
	}
	return true
}

func elasticRankMapComplete(obs modelDeploymentElasticObservation, width int, master types.UID) bool {
	if !obs.WidthKnown || obs.RayWidth != width || len(obs.RankToPodUID) != width || obs.RankToPodUID[0] != master {
		return false
	}
	seen := sets.New[types.UID]()
	for rank := 0; rank < width; rank++ {
		uid := obs.RankToPodUID[rank]
		if uid == "" || seen.Has(uid) {
			return false
		}
		seen.Insert(uid)
	}
	return true
}

func (r *ModelDeploymentReconciler) elasticNativeClient(master *core.Pod) (*elasticengine.Client, error) {
	if r.elasticClient != nil {
		return r.elasticClient(master)
	}
	port := master.Annotations["prometheus.io/port"]
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return nil, fmt.Errorf("master listener port is unknown")
	}
	scheme := master.Annotations["prometheus.io/scheme"]
	if scheme != "http" {
		return nil, fmt.Errorf("unsupported native listener scheme")
	}
	return elasticengine.New("http://"+net.JoinHostPort(master.Status.PodIP, port), 150*time.Second)
}

/*
* // [IMPORTANT: elastic EP] 证明引擎“真的”在宽度 W 上服务

	[上层] 1. 读 Ray GCS (first)  ──▶ 验证 rank 0..W-1 完整映射到唯一 Pod UID，且 rank 0 == master
	                │
	[中间] 2. 调 vLLM 原生探测     ──▶ 发 rank=-1 查边界 == W
	             ObserveNativeWorld       逐个 rank 0..W-1 跑真实 1-token 推理
	                                      再发 rank=-1 查边界 == W
	                │
	[下层] 3. 再读 Ray GCS (second) + 再读 K8s Pod (after)
	                                  ──▶ 验证前后 Pod 身份未变 (elasticSameRuntime)
	                                      且前后 RankToPodUID 映射完全一致!
*/
func (r *ModelDeploymentReconciler) elasticEffective(ctx context.Context, md *workercore.ModelDeployment, pods []core.Pod,
	captured []modelDeploymentElasticCapturedMember, master *core.Pod, client *elasticengine.Client, width int,
) (elasticLayer, modelDeploymentElasticObservation, bool) {
	if r.elasticObserver == nil {
		return unknownLayer("Ray reader is unavailable"), modelDeploymentElasticObservation{}, false
	}
	// 读 Ray GCS， modelDeploymentElasticDefaultGCSAddress 是地址
	first, err := r.elasticObserver.Observe(ctx, md, captured, int32(width), modelDeploymentElasticDefaultGCSAddress)
	if err != nil || !elasticRankMapComplete(first, width, master.UID) {
		return unknownLayer("native rank identities are unknown"), first, false
	}
	names, present := ModelDeploymentServedModelNames(md.Spec.Engine.Name, ModelDeploymentElasticRole(md).ExtraArgs)
	model := md.Spec.Model.Name
	if present && len(names) > 0 {
		model = names[0]
	}
	// 调 vLLM 原生探测     ──▶ 发 rank=-1 查边界 == W
	native, failure := client.ObserveNativeWorld(ctx, model, "Hello", width)
	if failure != nil {
		return unknownLayer(failure.Error()), first, false
	}
	// 读 Ray GCS， modelDeploymentElasticDefaultGCSAddress 是地址
	second, err := r.elasticObserver.Observe(ctx, md, captured, int32(width), modelDeploymentElasticDefaultGCSAddress)
	after, identityErr := r.elasticLiveMembers(ctx, md)
	if err != nil || identityErr != nil || !elasticSameRuntime(pods, after) || !elasticRankMapComplete(second, width, master.UID) ||
		!reflect.DeepEqual(first.RankToPodUID, second.RankToPodUID) {
		return unknownLayer("runtime identities moved across native forwards"), second, false
	}
	if native.ObservedWidth != width || len(native.Forwards) != width {
		return unknownLayer("native forward proof is incomplete"), second, false
	}
	for rank, forward := range native.Forwards {
		if forward.Rank != rank || !forward.Served {
			return unknownLayer("native forward proof is incomplete"), second, false
		}
	}
	return knownLayer(width), second, true
}

func (r *ModelDeploymentReconciler) elasticSetWithdrawn(ctx context.Context, md *workercore.ModelDeployment, master *core.Pod, withdraw bool) error {
	fresh := new(core.Pod)
	if err := r.APIReader.Get(ctx, ctrlcli.ObjectKeyFromObject(master), fresh); err != nil {
		return err
	}
	if fresh.UID != master.UID || !elasticOwnedBy(md, fresh) {
		return fmt.Errorf("master changed before endpoint update")
	}
	if fresh.Annotations == nil {
		fresh.Annotations = map[string]string{}
	}
	if withdraw {
		fresh.Annotations[modelDeploymentElasticWithdrawnAnnotation] = "true"
		delete(fresh.Labels, modelDeploymentLabelKeyEndpointEligible)
	} else {
		delete(fresh.Annotations, modelDeploymentElasticWithdrawnAnnotation)
	}
	if !reflect.DeepEqual(fresh.Annotations, master.Annotations) || !reflect.DeepEqual(fresh.Labels, master.Labels) {
		if err := r.Client.Update(ctx, fresh); err != nil {
			return err
		}
	}
	if withdraw {
		pods, err := r.elasticLiveMembers(ctx, md)
		if err != nil {
			return err
		}
		if err = r.syncModelDeploymentService(ctx, md, true, pods); err != nil {
			return err
		}
		return r.syncModelDeploymentRouter(ctx, md, true)
	}
	return nil
}

func (r *ModelDeploymentReconciler) elasticWithdrawal(ctx context.Context, md *workercore.ModelDeployment, master *core.Pod) (bool, string) {
	if err := r.elasticSetWithdrawn(ctx, md, master, true); err != nil {
		return false, err.Error()
	}
	if reason, held := r.observeModelDeploymentRetirementResidual(ctx, md, sets.New(master.UID)); held {
		return false, reason
	}
	if disaggregated, reason := modelDeploymentRetirementDisaggregated(md, []*core.Pod{master}); reason != "" {
		return false, reason
	} else if disaggregated {
		return false, "the member runs a disaggregated shape, which has no verifiable decoder-side release in elastic mode"
	}
	drain, err := r.modelDeploymentDrainReaderOf().Drain(ctx, modelDeploymentDrainTarget{
		PodUID:        master.UID,
		DeploymentUID: md.UID, Container: modelDeploymentMainContainerName, Role: ModelDeploymentElasticRole(md).Name, Engine: md.Spec.Engine.Name,
	})
	if err != nil {
		return false, err.Error()
	}
	return drain.Complete && drain.State == modelDeploymentDrainIdle, drain.Reason
}

// elasticOperationMemberSurvives 判断记录里是否还有原来那个 UID 的 Pod 存活。节点丢失后，
// 同名替代 Pod 会获得新 UID。如果原成员已全部消失，继续等它们匹配旧记录将永远无法推进。
func elasticOperationMemberSurvives(op *elasticOperation, pods []core.Pod) bool {
	byName := map[string]types.UID{}
	for i := range pods {
		byName[pods[i].Name] = pods[i].UID
	}
	for _, member := range append(slices.Clone(op.Members), op.Master, op.Head) {
		if member.UID != "" && byName[member.Name] == member.UID {
			return true
		}
	}
	return false
}

func elasticOperationMembersMatch(op *elasticOperation, pods []core.Pod) bool {
	byName := map[string]types.UID{}
	for i := range pods {
		byName[pods[i].Name] = pods[i].UID
	}
	for _, member := range append(slices.Clone(op.Members), op.Master, op.Head) {
		if member.UID == "" {
			return false
		}
		if byName[member.Name] != member.UID {
			retired := false
			if op.State == elasticStateReleased {
				for _, worker := range op.Workers {
					if worker == member && byName[member.Name] == "" {
						retired = true
					}
				}
			}
			if !retired {
				return false
			}
		}
	}
	return true
}

// recoverElasticUnsentMemberDrift 处理尚处于 Recorded、命令未发送且捕获成员已全部被替换的内部缩容。
// 即使名称相同，也要先重新证明现存成员的宽度，再清理旧记录。下一轮按新 UID 判断是否已经就绪，
// 或需要创建新操作。
func (r *ModelDeploymentReconciler) recoverElasticUnsentMemberDrift(
	ctx context.Context, md *workercore.ModelDeployment, status *modelDeploymentElasticStatus,
	pods []core.Pod, captured []modelDeploymentElasticCapturedMember,
	master *core.Pod, client *elasticengine.Client, store *elasticOperationStore, op *elasticOperation,
) (bool, error) {
	if op.State != elasticStateRecorded || op.CommandSent || op.Width.Upward() ||
		!status.Desired.Known || status.Desired.Value != op.Width.Target ||
		elasticOperationMemberSurvives(op, pods) {
		return false, nil
	}
	currentWidth := len(captured) - 1
	if currentWidth < elasticprofile.WidthMin || currentWidth > elasticprofile.WidthMax {
		return false, nil
	}
	if !status.Admitted.Known || status.Admitted.Value != currentWidth ||
		!status.Allocated.Known || status.Allocated.Value != currentWidth {
		return false, nil
	}
	effective, ray, proven := r.elasticEffective(ctx, md, pods, captured, master, client, currentWidth)
	status.Effective = effective
	if ray.RegisteredGPUKnown {
		status.Ray = knownLayer(ray.RegisteredGPU)
	} else {
		status.Ray = unknownLayer(ray.RegisteredGPUUnknownReason)
	}
	if !proven {
		return false, nil
	}

	status.StableWidth = currentWidth
	status.State = elasticStateCompleted
	status.Reason = fmt.Sprintf(
		"the unsent operation held replaced member identities; the current native world proves width %d and the stale record is retired",
		currentWidth,
	)
	if err := r.persistElasticObservation(ctx, md, *status); err != nil {
		return false, err
	}
	if currentWidth == status.Desired.Value && master.Annotations[modelDeploymentElasticWithdrawnAnnotation] == "true" {
		if err := r.elasticSetWithdrawn(ctx, md, master, false); err != nil {
			return false, err
		}
	}
	if err := store.Delete(ctx, op); err != nil {
		return false, err
	}
	return true, r.recordElasticResizeCondition(ctx, md, nil)
}

// recordElasticResizeCondition 根据本轮写入后的持久记录同步 ElasticResize 条件。
// 记录已放弃时设为 False 并保留拒绝原因，其余情况设为 True、NoRefusal。记录已删除的调用方传 nil，
// 避免过期的拒绝一直留在部署状态上。
func (r *ModelDeploymentReconciler) recordElasticResizeCondition(
	ctx context.Context, md *workercore.ModelDeployment, op *elasticOperation,
) error {
	target, reason, message := string(meta.ConditionTrue),
		modelDeploymentReasonNoScaleRefusal, "no elastic scale command stands refused"
	if op != nil && op.State == elasticStateAbandoned {
		target, reason, message = string(meta.ConditionFalse),
			modelDeploymentReasonScaleRefused, elasticScaleRefusalMessage(op)
	}
	return r.writeElasticResizeCondition(ctx, md, target, reason, message)
}

// writeElasticResizeCondition 单独更新部署的 ElasticResize 条件。
//
// 这里单独做乐观并发补丁。如果先改进程内的状态，后续比较可能把同一个值当作修改前后两份，导致漏写。
// 补丁只包含这一项条件，并用服务端应答更新本地对象，供本轮之后的状态计算使用。条件没有变化时跳过写入。
func (r *ModelDeploymentReconciler) writeElasticResizeCondition(
	ctx context.Context, md *workercore.ModelDeployment, status, reason, message string,
) error {
	base := md.DeepCopy()
	candidate := base.DeepCopy()
	ModelDeploymentConditionElasticResize.Status(candidate, status, reason, message)
	if kubemeta.DeepEqual(base.Status, candidate.Status) {
		return nil
	}
	optimistic := ctrlcli.MergeFromWithOptions(base, ctrlcli.MergeFromWithOptimisticLock{})
	if err := r.Client.Status().Patch(ctx, candidate, optimistic); err != nil {
		return err
	}
	md.ResourceVersion = candidate.ResourceVersion
	md.Status = candidate.Status
	return nil
}

// finishElasticWorldUnprovable 处理没有操作记录、又无法证明当前并行组宽度的情形。
// 如果实际宽度未知或与 spec 不同，暂停纠正操作，将 ElasticResize 置为 Unknown，记录期望宽度、
// 观测宽度和缺失的证据。如果宽度与 spec 相同，无需发起纠正。后续观测重新取得完整证明后即可继续。
func (r *ModelDeploymentReconciler) finishElasticWorldUnprovable(
	ctx context.Context, md *workercore.ModelDeployment, status *modelDeploymentElasticStatus,
	finish func(string) error, desired int,
) error {
	if status.Ray.Known && status.Ray.Value == desired {
		return finish(status.Effective.Reason)
	}
	status.Reason = status.Effective.Reason
	if err := r.persistElasticObservation(ctx, md, *status); err != nil {
		return err
	}
	ray := strconv.Itoa(status.Ray.Value)
	if !status.Ray.Known {
		ray = "unknown"
	}
	return r.writeElasticResizeCondition(ctx, md, string(meta.ConditionUnknown),
		modelDeploymentReasonWorldUnprovable,
		fmt.Sprintf("the corrective scale from the live width %s to the desired width %d is blocked: the live world cannot be proven (%s)",
			ray, desired, status.Effective.Reason))
}

// elasticScaleRetryDue 计算下次重试还要等待多久。第 n 次失败后等待 min(15s × 2^(n−1), 4m)。
// 明确拒绝和重发后没有收到答复的次数都延长退避间隔，只有前者消耗明确拒绝额度。
func elasticScaleRetryDue(op *elasticOperation, now time.Time) time.Duration {
	if op.ScaleLastFailedAt == nil {
		return 0
	}
	wait := elasticScaleRetryBase
	for i := max(op.ScaleAttempts+op.ScaleAmbiguousAttempts, 1); i > 1 && wait < elasticScaleRetryCap; i-- {
		wait *= 2
	}
	wait = min(wait, elasticScaleRetryCap)
	if due := op.ScaleLastFailedAt.Add(wait); now.Before(due) {
		return due.Sub(now)
	}
	return 0
}

// elasticScaleAnswerIsRefusal 区分引擎的明确错误和结果未知的发送。引擎返回 408、
// 5xx（包括忙碌时的 503）或拒绝格式错误的请求，可记录为明确拒绝。断连、调用方超时或回执无法解析时，
// 保留不确定状态并观察实际宽度。
func elasticScaleAnswerIsRefusal(err error) bool {
	var engineErr *elasticengine.Error
	return errors.As(err, &engineErr) &&
		engineErr.Kind != elasticengine.KindAmbiguous &&
		engineErr.Kind != elasticengine.KindMalformed
}

// [IMPORTANT: elastic EP] 总编排入口
// reconcileModelDeploymentElasticResize 是唯一的原生变更调用方。
// 持久化记录先于请求写入；恢复的已发送记录仅执行观测。
func (r *ModelDeploymentReconciler) reconcileModelDeploymentElasticResize(ctx context.Context, md *workercore.ModelDeployment) error {
	if r.APIReader == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	role := ModelDeploymentElasticRole(md)
	status := modelDeploymentElasticStatus{
		DeploymentUID: md.UID, ObservedGeneration: md.Generation,
		Desired: knownLayer(int(role.ElasticEP.Width)), Admitted: unknownLayer("not collected"),
		Allocated: unknownLayer("not collected"), Ray: unknownLayer("not collected"), Effective: unknownLayer("not collected"),
	}
	finish := func(reason string) error { status.Reason = reason; return r.persistElasticObservation(ctx, md, status) }
	// [IMPORTANT: elastic EP] 拉取当前活着的 Pod
	pods, err := r.elasticLiveMembers(ctx, md)
	if err != nil {
		if errors.Is(err, errElasticDeploymentChanged) {
			// 这次读取期间部署已变化。丢弃属于旧部署或旧 generation 的观测，
			// spec 的监听事件会触发下一轮调谐。
			return nil
		}
		// 读取失败时将错误写成等待原因，让使用者能看到本轮被阻塞的位置。
		return finish(err.Error())
	}
	// [IMPORTANT: elastic EP] 统计 Admitted（Kueue 准入） 和 Allocated（GPU 账本分配）
	status.Admitted, status.Allocated = r.observeModelDeploymentElasticAllocation(ctx, md, pods)
	// [IMPORTANT: elastic EP] 识别出谁是 CPU Head、谁是 ordinal=0 的 Master、谁是 Worker，确保主容器正在运行且 Master 有 IP，然后构造连向 Master 的 elasticengine.Client。
	captured, master, err := elasticRuntimeMembers(md, pods)
	if master != nil {
		status.MasterUID = master.UID
	}
	if err != nil {
		return finish(err.Error())
	}
	// [IMPORTANT: elastic EP] 持久化操作记录与防篡改
	client, err := r.elasticNativeClient(master)
	if err != nil {
		return finish(err.Error())
	}
	store := newElasticOperationStore(r.Client)
	var op *elasticOperation
	op, err = store.Read(ctx, md.Namespace, md.Name, md.UID)
	if err != nil && !apierrors.IsNotFound(err) {
		return finish(err.Error())
	}
	if apierrors.IsNotFound(err) {
		op = nil
	}
	// 至此已读过操作记录，本轮退出前可以按记录同步部署的拒绝状态。此前的提前返回缺少这项依据，
	// 会保留上一次有证据的条件。
	finish = func(reason string) error {
		status.Reason = reason
		if err := r.persistElasticObservation(ctx, md, status); err != nil {
			return err
		}
		return r.recordElasticResizeCondition(ctx, md, op)
	}
	if op != nil && !elasticOperationMembersMatch(op, pods) {
		if recovered, err := r.recoverElasticUnsentMemberDrift(
			ctx, md, &status, pods, captured, master, client, store, op,
		); err != nil {
			return err
		} else if recovered {
			return nil
		}
		// 成员不匹配时先判断旧记录是否仍可能有效。若还有捕获的成员存活，退场可能尚有工作要做，
		// 应继续等待。若成员全已替换且 spec 已换代，
		// 就根据现存成员重新验证 rank 和真实推理后再恢复记录。同代记录继续等待，
		// 上方已单独处理同代、未发送的缩容且成员全被替换的情况。
		if op.Generation == md.Generation || elasticOperationMemberSurvives(op, pods) {
			status.State = op.State
			return finish("a captured member identity no longer matches; the operation holds")
		}
		return r.reconstructElasticGeneration(ctx, md, &status, finish, pods, captured, master, client, store, op)
	}
	if op != nil && op.Generation != md.Generation {
		return r.reconstructElasticGeneration(ctx, md, &status, finish, pods, captured, master, client, store, op)
	}
	// [IMPORTANT: elastic EP] 确定引擎“真的”在宽度 W 上服务
	var width int
	if op != nil {
		if op.SentState() {
			width = op.Width.Target
		} else {
			width = op.Width.Old
		}
	} else {
		boot, valid := ModelDeploymentElasticBootstrapFromMaster(
			string(master.UID), master.Annotations[ModelDeploymentElasticMasterBootWidthAnnotation], md,
		)
		if !valid {
			return finish("master bootstrap width is unknown")
		}
		width = int(boot)
		cm, readErr := r.elasticObservationDocument(ctx, md)
		if readErr != nil {
			return finish(readErr.Error())
		}
		if cm != nil {
			var old modelDeploymentElasticStatus
			if err := json.Unmarshal([]byte(cm.Data["observation.json"]), &old); err != nil {
				return finish("prior observation is malformed")
			}
			if old.MasterUID == master.UID && old.StableWidth >= 2 && old.StableWidth <= 64 {
				width = old.StableWidth
			}
		}
	}
	// [IMPORTANT: elastic EP] 确定引擎“真的”在宽度 W 上服务
	effective, ray, proven := r.elasticEffective(ctx, md, pods, captured, master, client, width)
	status.Effective = effective
	if ray.RegisteredGPUKnown {
		status.Ray = knownLayer(ray.RegisteredGPU)
	} else {
		status.Ray = unknownLayer(ray.RegisteredGPUUnknownReason)
	}
	if op == nil {
		// 历史宽度是本轮探测的起点。如果扩容已生效却丢了记录，只探测旧宽度会一直发现 rank 数不符。
		// 获准成员数比历史宽度大时，按这个新数值再次检查全部 rank 和前向推理。若证明成功，
		// 将实际宽度作为下一次纠正的起点。遇到更窄或未知的并行组，保留等待状态。
		if !proven && status.Admitted.Known && status.Admitted.Value > width {
			live := status.Admitted.Value
			effective, ray, proven = r.elasticEffective(ctx, md, pods, captured, master, client, live)
			status.Effective = effective
			if ray.RegisteredGPUKnown {
				status.Ray = knownLayer(ray.RegisteredGPU)
			} else {
				status.Ray = unknownLayer(ray.RegisteredGPUUnknownReason)
			}
			if !proven {
				return r.finishElasticWorldUnprovable(ctx, md, &status, finish, int(role.ElasticEP.Width))
			}
			width = live
		}
		if !proven {
			return r.finishElasticWorldUnprovable(ctx, md, &status, finish, int(role.ElasticEP.Width))
		}
		status.StableWidth = width
		if width == int(role.ElasticEP.Width) {
			return finish("native ranks and forwards agree")
		}
		if int(role.ElasticEP.Width) > width &&
			(!status.Admitted.Known || status.Admitted.Value < int(role.ElasticEP.Width) ||
				!status.Allocated.Known || status.Allocated.Value < int(role.ElasticEP.Width) ||
				!status.Ray.Known || status.Ray.Value < int(role.ElasticEP.Width)) {
			return finish("new members are not admitted, allocated and registered")
		}
		op = &elasticOperation{
			Name: md.Name, Namespace: md.Namespace, DeploymentUID: md.UID, Generation: md.Generation,
			Width:  elasticWidth{Old: width, Target: int(role.ElasticEP.Width)},
			Master: elasticCapturedIdentity{Name: master.Name, UID: master.UID}, State: elasticStateRecorded,
		}
		var retiring []core.Pod
		for i := range pods {
			pod := &pods[i]
			identity := elasticCapturedIdentity{Name: pod.Name, UID: pod.UID}
			if modelDeploymentPodRole(pod) == modelDeploymentElasticHeadName(md) {
				op.Head = identity
			} else {
				op.Members = append(op.Members, identity)
			}
			if op.Width.Upward() {
				continue
			}
			for rank, uid := range ray.RankToPodUID {
				if rank >= op.Width.Target && uid == pod.UID {
					op.Workers = append(op.Workers, identity)
					retiring = append(retiring, *pod)
				}
			}
		}
		if !op.Width.Upward() {
			if len(op.Workers) != op.Width.Old-op.Width.Target {
				return finish("retirement rank identities are incomplete")
			}
			op.Release, err = r.captureModelDeploymentElasticRelease(ctx, md, retiring)
			if err != nil {
				return finish(err.Error())
			}
		}
		if _, err = store.Create(ctx, op); err != nil {
			return err
		}
	}
	status.State = op.State
	// [IMPORTANT: elastic EP] 把所有证据装盘，交给“纯函数大脑”做裁决
	facts := elasticFacts{
		DeploymentUID:      md.UID,
		Generation:         md.Generation,
		Desired:            status.Desired,
		Admitted:           status.Admitted,
		AdmittedAllocation: status.Allocated,
		Ray:                status.Ray,
		Effective:          status.Effective,
		ForwardProven:      proven,
		ActorMappingKnown:  ray.ActorsComplete,
		ActorFree:          map[types.UID]bool{},
	}

	// 原生查询失败时保留错误并等待。若直接把失败视为 NativeComplete 为假，
	// 就会把缺少应答误报成引擎仍在变更。服务摘除与资源归还仍按原有次序采集证据。
	var nativeReadFailure error
	if op.SentState() && proven {
		scaling, readErr := client.IsScaling(ctx)
		if readErr != nil {
			nativeReadFailure = readErr
		}
		facts.NativeComplete = readErr == nil && !scaling
		// 将同一次查询的结果同时写入 ScalingKnown 和 Scaling。这样即使记录里还有拒绝，
		// 也能按引擎当前的应答判断已证明的目标宽度。
		facts.ScalingKnown = readErr == nil
		facts.Scaling = scaling
	} else if op.SentState() && op.ScaleLastError != "" {
		// 即使目标宽度尚未证明，已记录拒绝时也要查询引擎是否仍在变更。只有明确空闲才可能重试。
		scaling, readErr := client.IsScaling(ctx)
		if readErr != nil {
			nativeReadFailure = readErr
		}
		facts.ScalingKnown = readErr == nil
		facts.Scaling = scaling
	}
	for uid, fact := range ray.ActorFree {
		facts.ActorFree[uid] = fact.ActorFree
	}
	if !op.Width.Upward() {
		var reason string
		facts.WithdrawalConfirmed, reason = r.elasticWithdrawal(ctx, md, master)
		if !facts.WithdrawalConfirmed {
			return finish(reason)
		}
	}
	if op.State == elasticStateReleased {
		if !proven || !facts.NativeComplete || !ray.ActorsComplete {
			return finish("native release proof is not current")
		}
		if err := r.elasticDeleteCaptured(ctx, md, op, ray); err != nil {
			return finish(err.Error())
		}
		reason, gone := r.observeReleaseTargetsAbsent(ctx, op.Release)
		if !gone {
			return finish(reason)
		}
		reason, gone = r.observeReleaseWorkloadsGone(ctx, md, op.Release)
		if !gone {
			return finish(reason)
		}
		reason, gone = r.observeReleaseAcceleratorsReturned(ctx, op.Release)
		if !gone {
			return finish(reason)
		}
		facts.ReleaseObserved = true
	}
	if !op.SentState() && !proven {
		return finish("current native ranks and forwards do not prove the old world")
	}
	if nativeReadFailure != nil {
		return finish(nativeReadFailure.Error())
	}
	// [IMPORTANT: elastic EP] 交给“纯函数大脑”做裁决
	decision := decideElasticOperation(op, facts)
	switch decision.Action {
	// [IMPORTANT: elastic EP]（首次发令）—— 教科书级的 WAL（预写日志）思想
	case elasticActionRequestScale:
		// 形成发送意图所需的观测完成后，再从 API 核对部署和全部成员的身份。
		live, err := r.elasticLiveMembers(ctx, md)
		if err != nil || !elasticSameRuntime(pods, live) {
			return finish("identity changed before native intent")
		}
		op.State = elasticStateCommandSent
		op.CommandSent = true
		op.CommandIntent = fmt.Sprintf("scale to %d", op.Width.Target)
		if err := store.Update(ctx, op); err != nil {
			return err
		}
		if err := client.Scale(ctx, op.Width.Target, 120*time.Second); err != nil {
			// 引擎给出明确应答后才写入拒绝原因和次数，并在同一次更新中保存。调用过程中若崩溃，
			// 记录不会凭空增加拒绝，恢复后再根据现场观测判断结果。
			if elasticScaleAnswerIsRefusal(err) {
				failedAt := r.modelDeploymentNow()
				op.ScaleAttempts++
				op.ScaleLastError = err.Error()
				op.ScaleLastFailedAt = &failedAt
				if err := store.Update(ctx, op); err != nil {
					return err
				}
			}
			status.State = op.State
			return finish(err.Error())
		}
		status.State = op.State
		return finish("native command recorded and sent; completion unobserved")
	// [IMPORTANT: elastic EP]（有界指数退避重试）
	case elasticActionRetryScale:
		// 重试前也要像首次发送时一样重新核实身份。成员在观测后可能已被替换，
		// 旧名单不足以授权发送命令。
		live, err := r.elasticLiveMembers(ctx, md)
		if err != nil || !elasticSameRuntime(pods, live) {
			return finish("identity changed before the refused scale command is re-issued")
		}
		// 状态机决定能否重试，当前时钟决定本轮是否已到重试时间。
		if wait := elasticScaleRetryDue(op, r.modelDeploymentNow()); wait > 0 {
			return finish(fmt.Sprintf(
				"the engine refused the scale command to width %d; the next attempt backs off %s; the recorded refusal: %s",
				op.Width.Target, wait.Truncate(time.Second), op.ScaleLastError,
			))
		}
		if err := client.Scale(ctx, op.Width.Target, 120*time.Second); err != nil {
			// 与首次发送一样，在收到结果后一次写回。若重发结果不明，延长退避时间，
			// 同时保留上次明确拒绝的原因和剩余额度。
			failedAt := r.modelDeploymentNow()
			if elasticScaleAnswerIsRefusal(err) {
				op.ScaleAttempts++
				op.ScaleLastError = err.Error()
				op.ScaleLastFailedAt = &failedAt
			} else {
				op.ScaleAmbiguousAttempts++
				op.ScaleLastFailedAt = &failedAt
			}
			if err := store.Update(ctx, op); err != nil {
				return err
			}
			status.State = op.State
			return finish(err.Error())
		}
		// 引擎已接受重发命令，清除先前的拒绝，再由常规观测判断新宽度是否生效。
		op.ScaleLastError = ""
		op.ScaleLastFailedAt = nil
		op.ScaleAmbiguousAttempts = 0
		if err := store.Update(ctx, op); err != nil {
			return err
		}
		status.State = op.State
		return finish("native command recorded and sent; completion unobserved")
	// [IMPORTANT: elastic EP]（缩容回收成员）
	case elasticActionRelease:
		op.State = elasticStateReleased
		if err := store.Update(ctx, op); err != nil {
			return err
		}
		if err := r.elasticDeleteCaptured(ctx, md, op, ray); err != nil {
			return finish(err.Error())
		}
	// [IMPORTANT: elastic EP]（优雅收尾）
	case elasticActionComplete:
		status.StableWidth = op.Width.Target
		status.State = elasticStateCompleted
		if err := r.persistElasticObservation(ctx, md, status); err != nil {
			return err
		}
		if err := r.elasticSetWithdrawn(ctx, md, master, false); err != nil {
			return err
		}
		if err := store.Delete(ctx, op); err != nil {
			return err
		}
		// 操作记录已删除，按当前无记录的事实更新条件，避免继续报告内存副本里的拒绝。
		return r.recordElasticResizeCondition(ctx, md, nil)
	default:
		if op.State != decision.State {
			op.State = decision.State
			if err := store.Update(ctx, op); err != nil {
				return err
			}
		}
	}
	status.State = op.State
	return finish(decision.Reason)
}

// reconstructElasticGeneration 为绑定到旧 generation 的操作重新收集证据。旧记录保存原宽度、
// 退场成员和发送意图，而本轮读取当前部署。此处不会向引擎发送新目标，也不会删除成员。
// 只有本轮证明满足恢复条件才清理或替换旧记录，否则保留原记录和全部资源。
func (r *ModelDeploymentReconciler) reconstructElasticGeneration(
	ctx context.Context, md *workercore.ModelDeployment, status *modelDeploymentElasticStatus,
	finish func(string) error, pods []core.Pod, captured []modelDeploymentElasticCapturedMember,
	master *core.Pod, client *elasticengine.Client, store *elasticOperationStore, op *elasticOperation,
) error {
	// 用原记录的目标宽度进行验证。spec 后来更新的目标不能覆盖要核实的那次操作。
	observed := op.Width.Old
	if op.SentState() {
		observed = op.Width.Target
	}
	effective, ray, proven := r.elasticEffective(ctx, md, pods, captured, master, client, observed)
	status.Effective = effective
	if ray.RegisteredGPUKnown {
		status.Ray = knownLayer(ray.RegisteredGPU)
	} else {
		status.Ray = unknownLayer(ray.RegisteredGPUUnknownReason)
	}
	status.State = op.State
	status.StableWidth = 0
	if !proven {
		if reason, recovered, err := r.recoverElasticUpscaleAfterGenerationChange(
			ctx, md, status, pods, captured, master, client, store, op,
		); err != nil {
			return err
		} else if recovered {
			// 恢复已清理旧记录或将它替换为仅负责释放的记录。两种情况都应清除旧拒绝，
			// 按恢复完成后的状态更新条件。
			op.State = elasticStateCompleted
			return finish(reason)
		}
	}

	reason, safe := r.elasticGenerationExit(ctx, md, status, client, ray, proven, op)
	if !safe {
		return finish(reason)
	}

	// 先持久化已经证明的稳定宽度，并恢复实例的服务资格，再删除旧记录。下一轮看到记录为空时，
	// 仍能读到已核实的宽度。
	status.StableWidth = observed
	status.Reason = reason
	status.State = ""
	if op.SentState() {
		status.State = elasticStateCompleted
	}
	if err := r.persistElasticObservation(ctx, md, *status); err != nil {
		return err
	}
	if err := r.elasticSetWithdrawn(ctx, md, master, false); err != nil {
		return err
	}
	if err := store.Delete(ctx, op); err != nil {
		return err
	}
	// 本轮持有的记录已经删除。根据删除后的状态清除拒绝，不再使用内存中遗留的旧阶段。
	return r.recordElasticResizeCondition(ctx, md, nil)
}

// recoverElasticUpscaleAfterGenerationChange 处理一种被打断的扩容。spec 已改回旧宽度，
// 引擎也证明仍在旧宽度运行，但集群可能还有为扩容而获准的多余成员。为它们建立仅负责释放的记录。
// 记录以 Released 为起点，仍要按退场路径核实摘除、actor 退出和资源归还。
func (r *ModelDeploymentReconciler) recoverElasticUpscaleAfterGenerationChange(
	ctx context.Context, md *workercore.ModelDeployment, status *modelDeploymentElasticStatus,
	pods []core.Pod, captured []modelDeploymentElasticCapturedMember, master *core.Pod,
	client *elasticengine.Client, store *elasticOperationStore, op *elasticOperation,
) (string, bool, error) {
	if !op.Width.Upward() || !status.Desired.Known || status.Desired.Value != op.Width.Old {
		return "", false, nil
	}

	oldEffective, oldRay, oldProven := r.elasticEffective(
		ctx, md, pods, captured, master, client, op.Width.Old,
	)
	if !oldProven {
		return "", false, nil
	}
	status.Effective = oldEffective
	if oldRay.RegisteredGPUKnown {
		status.Ray = knownLayer(oldRay.RegisteredGPU)
	} else {
		status.Ray = unknownLayer(oldRay.RegisteredGPUUnknownReason)
	}
	if !oldRay.ActorsComplete {
		return "", false, nil
	}
	scaling, err := client.IsScaling(ctx)
	if err != nil {
		return "", false, err
	}
	if scaling {
		return "", false, nil
	}

	ranked := sets.New[types.UID]()
	for _, uid := range oldRay.RankToPodUID {
		ranked.Insert(uid)
	}
	role := ModelDeploymentElasticRole(md)
	var retiring []core.Pod
	var gpuMembers []core.Pod
	var headIdentity, masterIdentity elasticCapturedIdentity
	for i := range pods {
		pod := &pods[i]
		if modelDeploymentPodRole(pod) == modelDeploymentElasticHeadName(md) {
			headIdentity = elasticCapturedIdentity{Name: pod.Name, UID: pod.UID}
			continue
		}
		if modelDeploymentPodRole(pod) != role.Name {
			continue
		}
		gpuMembers = append(gpuMembers, *pod)
		identity := elasticCapturedIdentity{Name: pod.Name, UID: pod.UID}
		ordinal, valid := modelDeploymentPodOrdinal(pod)
		if valid && ordinal == 0 {
			masterIdentity = identity
		}
		if !ranked.Has(pod.UID) {
			retiring = append(retiring, *pod)
		}
	}
	if len(retiring) == 0 {
		// 先持久化已经证实的旧宽度，再清理过时记录。写失败或此时崩溃都会保留原记录，
		// 下一轮可以继续恢复。
		status.StableWidth = op.Width.Old
		status.State = elasticStateCompleted
		status.Reason = "the interrupted upscale never left the proven old native width; the stale record was retired"
		if err := r.persistElasticObservation(ctx, md, *status); err != nil {
			return "", false, err
		}
		if err := store.Delete(ctx, op); err != nil {
			return "", false, err
		}
		return status.Reason, true, nil
	}
	if len(gpuMembers) <= op.Width.Old || masterIdentity.UID == "" || headIdentity.UID == "" {
		return "", false, nil
	}
	release, err := r.captureModelDeploymentElasticRelease(ctx, md, retiring)
	if err != nil {
		return "", false, err
	}
	recovered := &elasticOperation{
		Name: md.Name, Namespace: md.Namespace, DeploymentUID: md.UID, Generation: md.Generation,
		Width:  elasticWidth{Old: len(gpuMembers), Target: op.Width.Old},
		Master: masterIdentity, Head: headIdentity, Release: release,
		State: elasticStateReleased, CommandSent: true,
		CommandIntent: fmt.Sprintf("recovered at already observed native width %d", op.Width.Old),
	}
	for _, member := range captured {
		identity := elasticCapturedIdentity{Name: member.PodName, UID: member.PodUID}
		if member.Role != modelDeploymentElasticRoleHead {
			recovered.Members = append(recovered.Members, identity)
		}
	}
	for _, pod := range retiring {
		recovered.Workers = append(recovered.Workers,
			elasticCapturedIdentity{Name: pod.Name, UID: pod.UID})
	}
	// 先写观测，再用单次更新替换操作记录。若分成删除和创建两步，
	// 二者之间的崩溃可能同时丢失旧操作及尚待执行的退场。
	status.State = recovered.State
	status.StableWidth = op.Width.Old
	status.Reason = "the interrupted upscale was recovered at the proven old native width; excess members await the normal release proofs"
	if err := r.persistElasticObservation(ctx, md, *status); err != nil {
		return "", false, err
	}
	if err := store.RecoverInterrupted(ctx, op, recovered); err != nil {
		return "", false, err
	}
	return status.Reason, true, nil
}

// elasticGenerationExit 根据当前并行组判断能否结束重建，为每种等待情况给出原因。它不删除成员，
// 只有现场资源已经归还才允许结束旧缩容记录。
func (r *ModelDeploymentReconciler) elasticGenerationExit(
	ctx context.Context, md *workercore.ModelDeployment, status *modelDeploymentElasticStatus,
	client *elasticengine.Client, ray modelDeploymentElasticObservation,
	proven bool, op *elasticOperation,
) (string, bool) {
	if !proven {
		return "the current native ranks and forwards do not prove this record's own width; " +
			"the operation holds", false
	}
	scaling, err := client.IsScaling(ctx)
	if err != nil {
		return err.Error(), false
	}
	if scaling {
		return "the engine is still scaling, so this record's outcome is not observable yet; " +
			"the operation holds", false
	}
	if !op.SentState() {
		if !status.Desired.Known || status.Desired.Value != op.Width.Target {
			return "the desired target changed before send; the operation and admitted capacity stay held", false
		}
		return "the current native world proves this unsent record's old width and no scale is in " +
			"flight, so it never left that width", true
	}
	if !ray.ActorsComplete {
		return "the actor mapping is not complete, so the runtime identities this record captured " +
			"cannot be re-proved; the operation holds", false
	}
	if op.Width.Upward() {
		if !status.Admitted.Known || status.Admitted.Value < op.Width.Target ||
			!status.Allocated.Known || status.Allocated.Value < op.Width.Target ||
			!status.Ray.Known || status.Ray.Value < op.Width.Target {
			return "the target width is reached natively, but the current admitted, allocated and " +
				"registered members do not yet hold it; the operation holds", false
		}
		return "the current native width, every rank forward, the actor mapping and the admitted, " +
			"allocated and registered members all prove the upward target", true
	}
	// 本轮不会主动归还旧缩容记录中的资源。只有逐项观测到它们已释放，才可结束恢复。
	if op.Release == nil || op.Release.ModelDeploymentUID != op.DeploymentUID ||
		op.Release.ObservedGeneration != op.Generation || len(op.Workers) == 0 {
		return "this narrowing record carries no complete captured release, so its return cannot be " +
			"proved; the operation holds", false
	}
	if reason, absent := r.observeReleaseTargetsAbsent(ctx, op.Release); !absent {
		return reason, false
	}
	if reason, gone := r.observeReleaseWorkloadsGone(ctx, md, op.Release); !gone {
		return reason, false
	}
	if reason, returned := r.observeReleaseAcceleratorsReturned(ctx, op.Release); !returned {
		return reason, false
	}
	return "the target width, every rank forward, the actor mapping and the already-returned " +
		"captured release all prove the narrowing outcome", true
}

// elasticDeleteCaptured 只回收该操作捕获并已证明可以退场的成员，删除时用 UID 和资源版本作前置条件，
// 保护同名替代对象。
func (r *ModelDeploymentReconciler) elasticDeleteCaptured(
	ctx context.Context, md *workercore.ModelDeployment, op *elasticOperation, ray modelDeploymentElasticObservation,
) error {
	if op.Release == nil || op.Release.ModelDeploymentUID != op.DeploymentUID || op.Release.ObservedGeneration != op.Generation || len(op.Workers) == 0 {
		return fmt.Errorf("captured allocation lineage is missing")
	}
	freshMD := new(workercore.ModelDeployment)
	if err := r.APIReader.Get(ctx, ctrlcli.ObjectKeyFromObject(md), freshMD); err != nil {
		return err
	}
	if freshMD.UID != op.DeploymentUID || freshMD.Generation != op.Generation {
		return fmt.Errorf("deployment changed before retirement")
	}
	for _, worker := range op.Workers {
		pod := new(core.Pod)
		err := r.APIReader.Get(ctx, ctrlcli.ObjectKey{Namespace: md.Namespace, Name: worker.Name}, pod)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return err
		}
		fact, known := ray.ActorFree[worker.UID]
		if !known || !fact.ActorFree {
			return fmt.Errorf("captured worker has no current actor-free proof")
		}
		if pod.UID != worker.UID || !elasticOwnedBy(md, pod) || pod.UID == op.Master.UID {
			return fmt.Errorf("captured worker identity changed")
		}
		matched := false
		for _, member := range op.Release.Members {
			if member.PodUID == worker.UID && member.Name == worker.Name && member.Claim == pod.Annotations[deviceplugin.AllocatedAcceleratorAnnoKey] {
				matched = true
			}
		}
		if !matched {
			return fmt.Errorf("captured allocation changed before retirement")
		}
		if pod.DeletionTimestamp != nil {
			continue
		}
		if err := r.Client.Delete(ctx, pod, ctrlcli.Preconditions{UID: &worker.UID, ResourceVersion: &pod.ResourceVersion}); err != nil {
			return err
		}
	}
	workloads := new(kueue.WorkloadList)
	if err := r.APIReader.List(ctx, workloads, ctrlcli.InNamespace(md.Namespace)); err != nil {
		return err
	}
	for i := range workloads.Items {
		wl := &workloads.Items[i]
		if !slices.Contains(op.Release.WorkloadUIDs, string(wl.UID)) {
			continue
		}
		matched := false
		for _, owner := range wl.OwnerReferences {
			for _, worker := range op.Workers {
				if owner.UID == worker.UID && owner.Name == worker.Name && owner.Kind == "Pod" && owner.APIVersion == "v1" {
					matched = true
				}
			}
		}
		if !matched {
			return fmt.Errorf("captured Workload owner changed")
		}
		if wl.DeletionTimestamp == nil {
			if err := r.Client.Delete(ctx, wl, ctrlcli.Preconditions{UID: &wl.UID, ResourceVersion: &wl.ResourceVersion}); err != nil {
				return err
			}
		}
	}
	return nil
}
