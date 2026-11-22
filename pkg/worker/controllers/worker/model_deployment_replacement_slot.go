package worker

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	resourcehelper "k8s.io/component-helpers/resource"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	kueuectrlconst "sigs.k8s.io/kueue/pkg/controller/constants"
	kueuepodconst "sigs.k8s.io/kueue/pkg/controller/jobs/pod/constants"
	utilpod "sigs.k8s.io/kueue/pkg/util/pod"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubeapistatus"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/utils/json"
	"gpustack.ai/gpustack/pkg/utils/strconvx"
)

// modelDeploymentReplacementSlotsAnnotation 保存每个 role 当前活跃的替换槽位（replacement slot）。
//
// 使用注解而非 status 字段：它属于记账信息而非观测状态，用于覆盖从删除旧副本成员到创建替代副本之间的空窗期——在此期间该 ordinal 为空，没有任何观测状态能够指出正在被替换的对象。
const modelDeploymentReplacementSlotsAnnotation = "modeldeployment.gpustack.ai/replacement-slots"

// modelDeploymentReplacementPhase 表示单个槽位目前已推进到的阶段。Cleanup 记录该槽位捕获的旧成员标识；WaitingForAdmission 记录用于构建其替代副本的渲染信息。
type modelDeploymentReplacementPhase string

const (
	// modelDeploymentReplacementCleanup 表示旧成员仍在退出中的槽位。
	modelDeploymentReplacementCleanup modelDeploymentReplacementPhase = "Cleanup"

	// modelDeploymentReplacementWaitingForAdmission 表示在替代副本获得准入前持续锁定该 role。
	modelDeploymentReplacementWaitingForAdmission modelDeploymentReplacementPhase = "WaitingForAdmission"
)

// modelDeploymentReplacementSlot 表示单个 role 正在进行的替换操作。每个 role 最多存在一个槽位：若本轮未清理某 role 的槽位，就不会再轮换第二个健康的旧副本。
type modelDeploymentReplacementSlot struct {
	// Role 是正在替换其副本的 role 名称。
	Role string `json:"role"`

	// Ordinal 是该 role 中正在被替换的槽位序号。
	Ordinal int `json:"ordinal"`

	// Phase 是该槽位目前已观测到的推进阶段。
	Phase modelDeploymentReplacementPhase `json:"phase"`

	// MemberUIDs 是该槽位允许删除的成员 UID 列表。推导出的组名用于定位候选对象，而捕获到的 UID 才是授权删除的依据。
	MemberUIDs []types.UID `json:"memberUIDs,omitempty"`

	// WorkloadUID 是即将退出的成员组对应的 Workload UID。尚未观测到时为空。
	WorkloadUID types.UID `json:"workloadUID,omitempty"`

	// MemberHashes 是构建替代副本所依据的指纹列表，在旧成员组退出后记录。若后续修改与这些指纹不同，将在同一槽位内取代该替换计划。
	MemberHashes []string `json:"memberHashes,omitempty"`

	// CurrentMemberUIDs 和 CurrentWorkloadUID 用于在成员丢失期间保留新建成员的观测记录。
	CurrentMemberUIDs  []types.UID `json:"currentMemberUIDs,omitempty"`
	CurrentWorkloadUID types.UID   `json:"currentWorkloadUID,omitempty"`
}

// modelDeploymentReplacementSlots 是所有 role 的替换槽位映射，以 role 名称为键。
type modelDeploymentReplacementSlots map[string]modelDeploymentReplacementSlot

// slotFor 返回指定 role 的槽位以及该槽位是否存在。
func (s modelDeploymentReplacementSlots) slotFor(role string) (modelDeploymentReplacementSlot, bool) {
	slot, held := s[role]

	return slot, held
}

// held 判断指定 role 是否存在未完成的替换操作。
func (s modelDeploymentReplacementSlots) held(role string) bool {
	_, present := s.slotFor(role)

	return present
}

// modelDeploymentReplacementSlotsOf 返回已记录的槽位集合。若记录缺失或无法解析，则返回空，其行为与从未持有过槽位的 deployment 一致。
func modelDeploymentReplacementSlotsOf(md *workercore.ModelDeployment) modelDeploymentReplacementSlots {
	raw, present := md.Annotations[modelDeploymentReplacementSlotsAnnotation]
	if !present {
		return nil
	}

	slots := modelDeploymentReplacementSlots{}
	if err := json.Unmarshal([]byte(raw), &slots); err != nil {
		return nil
	}

	return slots
}

// setSlot 返回写入单个 role 槽位后的槽位集合，其余 role 的槽位保持不变。
func (s modelDeploymentReplacementSlots) setSlot(
	slot modelDeploymentReplacementSlot,
) modelDeploymentReplacementSlots {
	next := make(modelDeploymentReplacementSlots, len(s)+1)
	for role, held := range s {
		next[role] = held
	}
	next[slot.Role] = slot

	return next
}

// clearRole 返回移除指定 role 槽位后的槽位集合；若不再剩余任何槽位，则返回 nil。
func (s modelDeploymentReplacementSlots) clearRole(role string) modelDeploymentReplacementSlots {
	next := make(modelDeploymentReplacementSlots, len(s))
	for name, held := range s {
		if name != role {
			next[name] = held
		}
	}
	if len(next) == 0 {
		return nil
	}

	return next
}

// replacementDescription 用文字描述单个替换槽位。
func replacementDescription(slot modelDeploymentReplacementSlot) string {
	return fmt.Sprintf("role %s replica %d", slot.Role, slot.Ordinal)
}

// replacementCapturesMember 判断 Pod 是否属于该槽位已捕获的成员。
func replacementCapturesMember(slot modelDeploymentReplacementSlot, uid types.UID) bool {
	return slices.Contains(slot.MemberUIDs, uid)
}

// replacementCapturesWorkload 判断 Workload 是否为该槽位捕获的 Workload。槽位从未见过的 Workload 不具有操作权限，无论其名称如何。
func replacementCapturesWorkload(slot modelDeploymentReplacementSlot, wl *kueue.Workload) bool {
	return slot.WorkloadUID != "" && slot.WorkloadUID == wl.UID
}

// loadReplacementSlots 直接从 API server 而非缓存中读取当前记录，从而确保并发轮次为其他 role 写入的槽位不会在本轮写入时丢失。
func (r *ModelDeploymentReconciler) loadReplacementSlots(
	ctx context.Context, md *workercore.ModelDeployment,
) (*workercore.ModelDeployment, modelDeploymentReplacementSlots, error) {
	current := new(workercore.ModelDeployment)
	if err := r.APIReader.Get(ctx, ctrlcli.ObjectKeyFromObject(md), current); err != nil {
		return nil, nil, fmt.Errorf("read the deployment's replacement slots: %w", err)
	}

	// 该记录引用的是当前 deployment 的 role，若在读取与写入之间发生删除并重建，就会导致它描述一个已不存在对象的副本。
	if current.UID != md.UID {
		return nil, nil, fmt.Errorf(
			"the deployment was replaced while this pass ran: read UID %s, expected %s",
			current.UID, md.UID,
		)
	}

	return current, modelDeploymentReplacementSlotsOf(current), nil
}

// writeReplacementSlots 持久化槽位记录；如果现有记录已与本轮结果一致，则不执行写入。
//
// 补丁基于服务端自身的记录构建，并携带读取时的 resourceVersion，这样写入不同 role 的两个轮次不会互相覆盖，若底层记录已被修改则会返回 Conflict 而非静默丢失。调用方持有的对象会向前传递新记录和版本号，以便同一轮次中的第二次写入同样接受服务端校验。
func (r *ModelDeploymentReconciler) writeReplacementSlots(
	ctx context.Context, current *workercore.ModelDeployment,
	slots modelDeploymentReplacementSlots, carried *workercore.ModelDeployment,
) error {
	previous := modelDeploymentReplacementSlotsOf(current)
	if kubemeta.DeepEqual(previous, slots) {
		return nil
	}

	base := current.DeepCopy()
	candidate := base.DeepCopy()
	if candidate.Annotations == nil {
		candidate.Annotations = map[string]string{}
	}
	if len(slots) == 0 {
		// 此处移除该键而非将其置空：仅省略该键的 merge patch 会保留原有内容，而已经完成的记录绝不能继续保留从而错误标识某次替换。
		delete(candidate.Annotations, modelDeploymentReplacementSlotsAnnotation)
	} else {
		raw, err := json.Marshal(slots)
		if err != nil {
			return fmt.Errorf("encode the replacement slots: %w", err)
		}
		candidate.Annotations[modelDeploymentReplacementSlotsAnnotation] = string(raw)
	}
	encoded := candidate.Annotations[modelDeploymentReplacementSlotsAnnotation]

	optimistic := ctrlcli.MergeFromWithOptions(base, ctrlcli.MergeFromWithOptimisticLock{})
	if err := r.Client.Patch(ctx, candidate, optimistic); err != nil {
		if kerrors.IsConflict(err) {
			return fmt.Errorf("the replacement slots were written against a stale object: %w", err)
		}

		return fmt.Errorf("write the replacement slots: %w", err)
	}

	if len(slots) == 0 {
		delete(carried.Annotations, modelDeploymentReplacementSlotsAnnotation)
	} else {
		if carried.Annotations == nil {
			carried.Annotations = map[string]string{}
		}
		carried.Annotations[modelDeploymentReplacementSlotsAnnotation] = encoded
	}
	carried.ResourceVersion = candidate.ResourceVersion

	return nil
}

// beginReplacementSlot 在删除任何对象之前先记录该 role 的替换槽位。捕获到的标识是记录即将退出副本的唯一依据；一旦其成员消失，空的 ordinal 便无法指明需要恢复的旧副本。
func (r *ModelDeploymentReconciler) beginReplacementSlot(
	ctx context.Context, md *workercore.ModelDeployment, slot modelDeploymentReplacementSlot,
) error {
	current, observed, err := r.loadReplacementSlots(ctx, md)
	if err != nil {
		return err
	}
	if held, present := observed.slotFor(slot.Role); present && held.Ordinal == slot.Ordinal {
		return nil
	}

	return r.writeReplacementSlots(ctx, current, observed.setSlot(slot), md)
}

// advanceReplacementSlot 持久化对非本轮创建的槽位所做的状态推进修改。
func (r *ModelDeploymentReconciler) advanceReplacementSlot(
	ctx context.Context, md *workercore.ModelDeployment, slot modelDeploymentReplacementSlot,
) error {
	current, observed, err := r.loadReplacementSlots(ctx, md)
	if err != nil {
		return err
	}

	return r.writeReplacementSlots(ctx, current, observed.setSlot(slot), md)
}

// endReplacementSlot 移除已完成的替换槽位。
func (r *ModelDeploymentReconciler) endReplacementSlot(
	ctx context.Context, md *workercore.ModelDeployment, role string,
) error {
	current, observed, err := r.loadReplacementSlots(ctx, md)
	if err != nil {
		return err
	}
	if !observed.held(role) {
		return nil
	}

	return r.writeReplacementSlots(ctx, current, observed.clearRole(role), md)
}

// replacementCapturedIdentities 构建选择轮次所要写入的槽位：按当前对象状态捕获即将删除的成员及其所属的 Workload。
func replacementCapturedIdentities(
	view modelDeploymentReplicaView, workloads []kueue.Workload,
) modelDeploymentReplacementSlot {
	memberUIDs := make([]types.UID, 0, len(view.Members))
	for _, member := range view.Members {
		memberUIDs = append(memberUIDs, member.UID)
	}
	slices.Sort(memberUIDs)

	slot := modelDeploymentReplacementSlot{
		Role:       view.Role,
		Ordinal:    view.Ordinal,
		Phase:      modelDeploymentReplacementCleanup,
		MemberUIDs: memberUIDs,
	}
	if wl := replicaOwningWorkload(view.Members, workloadPointers(workloads)); wl != nil {
		slot.WorkloadUID = wl.UID
	}

	return slot
}

// workloadPointers 将列出的 Workload 切片转换为所有权辅助函数所需的指针切片。
func workloadPointers(workloads []kueue.Workload) []*kueue.Workload {
	pointers := make([]*kueue.Workload, 0, len(workloads))
	for i := range workloads {
		pointers = append(pointers, &workloads[i])
	}

	return pointers
}

// replicaOwningWorkload 返回声明拥有某副本任意成员的 Workload。仅名称与组名相同的 Workload 并不属于该副本。
func replicaOwningWorkload(members []*core.Pod, workloads []*kueue.Workload) *kueue.Workload {
	pods := make([]core.Pod, 0, len(members))
	for _, member := range members {
		pods = append(pods, *member)
	}

	byMember := modelDeploymentReplicaWorkloads(pods, workloads)
	for _, member := range members {
		if wl := byMember[member.UID]; wl != nil {
			return wl
		}
	}

	return nil
}

// replacementMemberDeletes 返回槽位中捕获的、当前仍然存在的成员。调用方按捕获到的 UID 逐一删除，因此捕获后才占用该名称的 Pod 不会被误删。
func replacementMemberDeletes(
	slot modelDeploymentReplacementSlot, members []*core.Pod,
) []*core.Pod {
	deletes := make([]*core.Pod, 0, len(slot.MemberUIDs))
	for _, member := range members {
		if replacementCapturesMember(slot, member.UID) {
			deletes = append(deletes, member)
		}
	}
	slices.SortFunc(deletes, func(a, b *core.Pod) int { return strings.Compare(a.Name, b.Name) })

	return deletes
}

// replacementWorkloadDeletes 返回槽位允许删除的 Workload：包括槽位直接捕获的 Workload，以及将槽位捕获的某个成员列为 owner 的 Workload。后一种情况用于处理 informer 尚未同步退出组 Workload 时就已写入的槽位，这类槽位虽然未记录 WorkloadUID，但仍可确证其属于该成员组。仅凭名称匹配或无 owner 的形态均不构成删除授权。
func replacementWorkloadDeletes(
	slot modelDeploymentReplacementSlot, workloads []*kueue.Workload,
) []*kueue.Workload {
	members := sets.New[types.UID](slot.MemberUIDs...)

	var deletes []*kueue.Workload
	for _, wl := range workloads {
		if replacementCapturesWorkload(slot, wl) || modelDeploymentWorkloadOwnsAny(wl, members) {
			deletes = append(deletes, wl)
		}
	}

	return deletes
}

// workloadOwnsGroupName 判断 Workload 是否使用了某个成员组的派生名称。该函数仅用于定位对象，绝不用于授权删除或接管对象。
func workloadOwnsGroupName(wl *kueue.Workload, group string) bool {
	if wl.Name == group {
		return true
	}
	for _, ps := range wl.Spec.PodSets {
		if ps.Name == kueue.PodSetReference(group) {
			return true
		}
	}

	return false
}

// replacementOccupancyOnServer 列出 API server 上某一角色在指定 ordinal 处的成员，
// 包括正在终止的成员：仍在服务端存在的离场成员仍占用该 slot。
func (r *ModelDeploymentReconciler) replacementOccupancyOnServer(
	ctx context.Context, md *workercore.ModelDeployment, role string, ordinal int,
) ([]core.Pod, error) {
	podList := new(core.PodList)
	if err := r.APIReader.List(ctx, podList,
		ctrlcli.InNamespace(md.Namespace),
		ctrlcli.MatchingLabels{
			modelDeploymentLabelKeyName:      modelDeploymentLabelValueName,
			modelDeploymentLabelKeyInstance:  md.Name,
			modelDeploymentLabelKeyComponent: role,
		}); err != nil {
		return nil, fmt.Errorf("list the members of role %q on the api server: %w", role, err)
	}

	held := make([]core.Pod, 0, len(podList.Items))
	for i := range podList.Items {
		pod := &podList.Items[i]
		if !modelDeploymentOwns(pod, md) || modelDeploymentPodRole(pod) != role {
			continue
		}
		if pod.Labels[modelDeploymentReplicaOrdinalLabel] == strconvx.Itoa(ordinal) {
			held = append(held, *pod)
		}
	}

	return held, nil
}

// modelDeploymentPartialGroupMembers 报告替换 slot 的成员组还缺少哪些成员，
// 以及是否应由本 operator 补全它们。
//
// 部分创建会使副本组少于其应有总数，而 Kueue 不会为不完整的组组合 Workload，
// 因此等待它的 slot 会无限期等待。仅在满足以下三个条件时补全才是安全的，且每个条件
// 都必须经过显式检查而非假设成立：
//
//   - 该组声明的规模与本轮渲染的规模一致，因此待添加的成员与组内已有成员
//     对该组的定义保持一致；
//   - 当前存在的每个成员都带有本轮自身的渲染结果，因此该组体现的是当前 spec
//     所声明的配置，而非早期配置的残留；
//   - 该组未持有任何 Workload，因此尚未针对它组合任何对象，也不会延长
//     任何已准入的作业。
//
// 不满足上述任一条件的组不会在此处补全。在早期渲染下丢失成员的副本会在 slot 后
// 等待并被整体替换，因为将当前 spec 的成员与上一版 spec 的成员放在同一组中，
// 会导致组内成员对自身身份产生分歧。
//
// 成员占用情况和 Workload 均直接从 API server 读取，因为该判断所处的正是
// 缓存滞后的时间窗口：成员刚刚在不久前被创建。
func (r *ModelDeploymentReconciler) modelDeploymentPartialGroupMembers(
	ctx context.Context, md *workercore.ModelDeployment,
	slot modelDeploymentReplacementSlot, want []*core.Pod,
) ([]*core.Pod, bool, error) {
	members, err := r.replacementOccupancyOnServer(ctx, md, slot.Role, slot.Ordinal)
	if err != nil {
		return nil, false, err
	}

	standing := replacementPodPointers(members)
	for _, member := range standing {
		if member.DeletionTimestamp != nil {
			return nil, false, nil
		}
	}

	// 没有任何成员存在不属于部分创建：空 ordinal 属于创建门控自身处理的情况，
	// 它会先等待空位得到证实，再在该位置创建任何对象。
	if len(standing) == 0 || len(standing) >= len(want) {
		return nil, false, nil
	}

	// 每个已存在的成员都必须落在当前渲染所包含的位置上，并且必须带有当前渲染
	// 自身的指纹与渲染版本。此处允许组内成员尚不完整，但不允许成员来自其他渲染版本。
	seated := make(map[int]*core.Pod, len(standing))
	for _, member := range standing {
		seat := modelDeploymentPodMemberIndex(member)
		if seat >= len(want) {
			return nil, false, nil
		}
		if _, duplicated := seated[seat]; duplicated {
			return nil, false, nil
		}
		seated[seat] = member
	}
	for index, rendered := range want {
		if member, present := seated[index]; present &&
			(member.Annotations[modelDeploymentPodSpecHashAnnotation] !=
				rendered.Annotations[modelDeploymentPodSpecHashAnnotation] ||
				modelDeploymentPodRenderVersion(member) != modelDeploymentPodRenderVersion(rendered)) {
			return nil, false, nil
		}
	}

	total, totalReason := modelDeploymentReplicaDeclaredTotal(standing)
	if totalReason != "" || total != len(want) {
		return nil, false, nil
	}

	workloads, err := r.findModelDeploymentGroupWorkloads(ctx, md, members)
	if err != nil {
		return nil, false, err
	}
	if len(workloads) > 0 {
		return nil, false, nil
	}

	missing := make([]*core.Pod, 0, len(want)-len(standing))
	for index, rendered := range want {
		if _, present := seated[index]; !present {
			missing = append(missing, rendered)
		}
	}

	return missing, len(missing) > 0, nil
}

// modelDeploymentLiveRoleSelectors 读取每个角色的 Service 当前发布时使用的选择器，
// 以便已启用准入资格判断的部署在未重新决策的轮次中仍按该选择器路由。
//
// 此读取绕过缓存。上一轮发布的选择器正是当前要查询的事实，而在发布它的那一轮中，
// informer 正好滞后于写入方。
func (r *ModelDeploymentReconciler) modelDeploymentLiveRoleSelectors(
	ctx context.Context, md *workercore.ModelDeployment,
) (map[string]map[string]string, error) {
	selectors := make(map[string]map[string]string, len(md.Spec.Roles))
	for i := range md.Spec.Roles {
		role := &md.Spec.Roles[i]
		svc := new(core.Service)
		err := r.APIReader.Get(ctx, ctrlcli.ObjectKey{
			Namespace: md.Namespace, Name: md.Name + "-" + role.Name,
		}, svc)
		if err != nil {
			if kerrors.IsNotFound(err) {
				continue
			}

			return nil, fmt.Errorf("read the service of role %q: %w", role.Name, err)
		}
		selectors[role.Name] = maps.Clone(svc.Spec.Selector)
		if svc.Annotations[modelDeploymentEligibilityActiveAnnotation] == modelDeploymentEndpointEligibleValue {
			selectors[role.Name][modelDeploymentLabelKeyEndpointEligible] = modelDeploymentEndpointEligibleValue
		}
	}

	return selectors, nil
}

// replacementVacant 报告该 slot 的 ordinal 在服务端是否已可证实为空闲。
//
// 此读取绕过缓存，因为 informer 恰好在此时间窗口内滞后于写入方：缓存中仍列出的
// 离场成员、响应丢失的创建请求、以及实际上已消失的旧 Workload。
// 丢失响应并不代表对象不存在。
func (r *ModelDeploymentReconciler) replacementVacant(
	ctx context.Context, md *workercore.ModelDeployment, slot modelDeploymentReplacementSlot,
) (vacant bool, why string, err error) {
	occupied, err := r.replacementOccupancyOnServer(ctx, md, slot.Role, slot.Ordinal)
	if err != nil {
		return false, "", err
	}
	if len(occupied) > 0 {
		names := make([]string, 0, len(occupied))
		for i := range occupied {
			names = append(names, occupied[i].Name)
		}
		slices.Sort(names)

		return false, fmt.Sprintf("the server still holds %s", strings.Join(names, ", ")), nil
	}

	// 即使是一个本 operator 无法证明由自己组合的 Workload，只要其名称处于占用状态，
	// 也会占用该 slot。这类 Workload 绝不会被删除或收编，但在其旁侧创建替换组
	// 会发生名称冲突，因此必须持续等待直至其离开。
	blocking, err := r.replacementGroupWorkload(ctx, md, slot)
	if err != nil {
		return false, "", err
	}
	if blocking != nil {
		if !blocking.DeletionTimestamp.IsZero() {
			return false, fmt.Sprintf("workload %s is still terminating", blocking.Name), nil
		}

		return false, fmt.Sprintf("workload %s still occupies the group", blocking.Name), nil
	}

	return true, "", nil
}

// replacementGroupWorkload 返回仍停留在 slot 派生组名上的 Workload，
// 若该组未持有任何 Workload 则返回 nil。
func (r *ModelDeploymentReconciler) replacementGroupWorkload(
	ctx context.Context, md *workercore.ModelDeployment, slot modelDeploymentReplacementSlot,
) (*kueue.Workload, error) {
	group := modelDeploymentReplicaGroupName(md, slot.Role, slot.Ordinal)
	wlList := new(kueue.WorkloadList)
	if err := r.APIReader.List(ctx, wlList, ctrlcli.InNamespace(md.Namespace)); err != nil {
		return nil, fmt.Errorf("list workloads: %w", err)
	}
	for i := range wlList.Items {
		if workloadOwnsGroupName(&wlList.Items[i], group) {
			return &wlList.Items[i], nil
		}
	}

	return nil, nil
}

// replacementAdmitted 报告该 slot 的 ordinal 当前是否已持有一个完整且对应的当前组，
// 且其 Workload 已被准入。配额预留不等于准入：被预留的组仅表示 Kueue 同意考虑该请求，
// 若基于预留就释放角色，会在替换组尚未持有任何资源时就将其释放。
func (r *ModelDeploymentReconciler) replacementAdmitted(
	ctx context.Context, md *workercore.ModelDeployment, slot modelDeploymentReplacementSlot,
	want []*core.Pod,
) (admitted bool, why string, err error) {
	members, err := r.replacementOccupancyOnServer(ctx, md, slot.Role, slot.Ordinal)
	if err != nil {
		return false, "", err
	}

	standing := make([]*core.Pod, 0, len(members))
	for i := range members {
		if members[i].DeletionTimestamp != nil {
			return false, fmt.Sprintf("member %s is on its way out", members[i].Name), nil
		}
		standing = append(standing, &members[i])
	}

	if reason := replacementMembersMatch(want, standing, modelDeploymentEffectiveMinRenderVersion(md)); reason != "" {
		return false, reason, nil
	}

	wl, err := r.replacementGroupWorkload(ctx, md, slot)
	if err != nil {
		return false, "", err
	}
	if wl == nil {
		return false, "the group holds no workload yet", nil
	}

	if reason := replacementAdmissionHolds(wl, standing, want); reason != "" {
		return false, reason, nil
	}

	return true, "", nil
}

// replacementAdmissionHolds 报告一个已准入的 Workload 是否是对当前成员所组成之组的
// 全新准入，若不是则说明原因。
//
// 仅靠属主关系是不够的：重建的 Pod 会使用相同的名称和相同的组，因此为离场副本
// 组合的 Workload 也可能拥有替换它的成员。仅当调度输入描述的就是当前存在的
// 组时（相同的成员、数量、模板、资源预留与队列），该准入才算全新准入。
func replacementAdmissionHolds(wl *kueue.Workload, standing, want []*core.Pod) string {
	if !kubeapistatus.ConditionType(kueue.WorkloadAdmitted).IsTrue(wl) {
		return fmt.Sprintf("workload %s is not admitted yet", wl.Name)
	}

	ours := sets.New[types.UID]()
	for _, member := range standing {
		ours.Insert(member.UID)
	}
	if !modelDeploymentWorkloadOwnsAny(wl, ours) {
		return fmt.Sprintf("workload %s owns none of the replica's current members", wl.Name)
	}
	for _, member := range standing {
		if !modelDeploymentWorkloadOwnsAny(wl, sets.New(member.UID)) {
			return fmt.Sprintf("workload %s does not own member %s", wl.Name, member.Name)
		}
	}

	if reason := replacementCompositionHolds(wl, standing); reason != "" {
		return reason
	}

	// 无条件比较队列名称：未指定队列的 Workload 无法证明其提交目标与成员所发往的队列一致。
	if queue := replacementQueueName(want); queue != "" && string(wl.Spec.QueueName) != queue {
		return fmt.Sprintf("workload %s is queued on %q rather than %q", wl.Name, wl.Spec.QueueName, queue)
	}

	return ""
}

// replacementCompositionHolds 将 Workload 的调度输入与其当前自身成员产生的组合
// 进行比较，并指出第一处差异。
//
// 该组合基于当前存在的成员重新构建，而非直接从 Workload 复制：按照 Kueue
// 的分组方式按 role hash 分组，每组的数量均从携带该 hash 的成员中读取。
func replacementCompositionHolds(wl *kueue.Workload, members []*core.Pod) string {
	pinned := replacementPinnedPodSets(members)
	// 共享同一 role hash 的成员会组合成一个携带该数量的 PodSet，因此 PodSet 的数量
	// 等于其中不同 hash 的数量，而非成员的数量。
	if len(wl.Spec.PodSets) != len(pinned) {
		return fmt.Sprintf("workload %s describes %d podsets for members composing %d",
			wl.Name, len(wl.Spec.PodSets), len(pinned))
	}
	live := make(map[kueue.PodSetReference]kueue.PodSet, len(wl.Spec.PodSets))
	for _, ps := range wl.Spec.PodSets {
		live[ps.Name] = ps
	}
	grouped := map[kueue.PodSetReference][]*core.Pod{}
	for _, member := range members {
		hash, err := replacementRoleHash(member)
		if err != nil {
			continue
		}
		grouped[hash] = append(grouped[hash], member)
	}

	for _, want := range pinned {
		got, present := live[want.Name]
		if !present {
			return fmt.Sprintf("workload %s carries no podset %s", wl.Name, want.Name)
		}
		if got.Count != want.Count {
			return fmt.Sprintf("workload %s asks for %d pods in podset %s, not the %d its members compose to",
				wl.Name, got.Count, want.Name, want.Count)
		}
		if reason := replacementTemplateMatches(wl, got, grouped[want.Name]); reason != "" {
			return reason
		}
	}

	return ""
}

// replacementTemplateMatches 报告 Workload 的 PodSet 是否描述了组内的某一个成员。
//
// 只要匹配任意一个成员即可，而不限于 Kueue 排在首位的成员：其 List 本身无序，
// 且带 rank 的成员共享同一个 role hash，因此即使读取方先看到 m0，基于 m1
// 组合的模板也是正确的。
func replacementTemplateMatches(
	wl *kueue.Workload, got kueue.PodSet, members []*core.Pod,
) string {
	for _, member := range members {
		template := core.PodTemplateSpec{Spec: *member.Spec.DeepCopy()}
		if replacementPodShapesMatch(got.Template.Spec, template.Spec) {
			// Kueue 不会在 PodSet 上单独声明请求量：它用于预留配额的请求量是从模板的
			// 容器中推导出来的。
			if resourceQuantityListEqual(
				replacementPodRequests(&core.Pod{Spec: got.Template.Spec}),
				replacementPodRequests(&core.Pod{Spec: template.Spec}),
			) {
				return ""
			}

			return fmt.Sprintf("workload %s reserves resources in podset %s that its members do not declare",
				wl.Name, got.Name)
		}
	}

	return fmt.Sprintf("workload %s describes a pod in podset %s that its members do not match",
		wl.Name, got.Name)
}

// replacementPinnedPodSets 遵循 Kueue 自身的规则，根据一组给定的成员重建它们
// 应组合出的 PodSet：按原样读取的 role hash 分组，每组数量取自携带该 hash 的成员数。
func replacementPinnedPodSets(members []*core.Pod) []kueue.PodSet {
	order := make([]kueue.PodSetReference, 0, len(members))
	grouped := make(map[kueue.PodSetReference][]*core.Pod, len(members))
	for _, member := range members {
		hash, err := replacementRoleHash(member)
		if err != nil {
			continue
		}
		if _, present := grouped[hash]; !present {
			order = append(order, hash)
		}
		grouped[hash] = append(grouped[hash], member)
	}

	pinned := make([]kueue.PodSet, 0, len(order))
	for _, hash := range order {
		group := grouped[hash]
		pinned = append(pinned, replacementPodSetFor(hash, group))
	}

	return pinned
}

// replacementPodSetFor 表示单个 role hash 组合出的 PodSet：数量为携带该 hash 的
// 成员数，模板取自 Kueue 排在首位的成员。
//
// 具体是哪一个成员并不重要，且固定集合会保留所有成员，因为 Kueue 的 List
// 并不保证顺序：按设计带 rank 的成员共享同一 role hash，若只取恰好排在首位
// 的成员模板，可能会将有效的调度放置误判为过期配置。
func replacementPodSetFor(hash kueue.PodSetReference, members []*core.Pod) kueue.PodSet {
	return kueue.PodSet{
		Name:     hash,
		Count:    int32(len(members)),
		Template: core.PodTemplateSpec{Spec: *members[0].Spec.DeepCopy()},
	}
}

// replacementRoleHash 是 Kueue 赋予 PodSet 的组名：当本 operator 写入的注解存在时
// 原样读取，否则使用 Kueue 根据 Pod 结构推导出的摘要值。
func replacementRoleHash(pod *core.Pod) (kueue.PodSetReference, error) {
	if hash, present := pod.Annotations[kueuepodconst.RoleHashAnnotation]; present && hash != "" {
		return kueue.NewPodSetReference(hash), nil
	}

	hash, err := utilpod.GenerateRoleHash(&pod.Spec)
	if err != nil {
		return "", err
	}

	return kueue.NewPodSetReference(hash), nil
}

// replacementPodRequests 计算单个成员对其所属 PodSet 资源预留的贡献。
//
// 此处直接调用 Kubernetes 自身的辅助函数，而非在此处自定规则，因为该规则不应由
// 本 operator 定义：requests 优先于 limits、多个普通容器累加、init 容器取最大值。
// 手写聚合逻辑即便在某一结构上与其一致，也不代表在所有场景下等价，而此处需要
// 比对的正是 Kueue 实际扣减的预留量。
func replacementPodRequests(pod *core.Pod) core.ResourceList {
	return resourcehelper.PodRequests(pod, resourcehelper.PodResourcesOptions{})
}

// replacementPodShapesMatch 比较 Kueue 据以预留配额的 Pod spec，
// 并忽略预期在准入过程中会发生变化的部分。
func replacementPodShapesMatch(got, want core.PodSpec) bool {
	actual, member := replacementComparablePod(got), replacementComparablePod(want)
	actual.Affinity = replacementWorkloadAffinity(actual.Affinity, member.Affinity)

	return kubemeta.DeepEqual(actual, member)
}

// replacementWorkloadAffinity 仅移除成员中不存在的 Workload webhook 固定约束。
// 适配与 model-manager 的固定约束用于限制 Kueue 放置，但不会被复制到 Pod 上。
// 其余必要条件和所有其他亲和性仍须与成员的放置约束一致。
func replacementWorkloadAffinity(got, want *core.Affinity) *core.Affinity {
	if got == nil || got.NodeAffinity == nil || got.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return got
	}
	var wanted []core.NodeSelectorTerm
	if want != nil && want.NodeAffinity != nil && want.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		wanted = want.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	}
	required := got.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if len(required.NodeSelectorTerms) != max(1, len(wanted)) {
		return got
	}
	for i := range required.NodeSelectorTerms {
		term := &required.NodeSelectorTerms[i]
		var original []core.NodeSelectorRequirement
		if len(wanted) > 0 {
			original = wanted[i].MatchExpressions
		}
		before := len(term.MatchExpressions)
		term.MatchExpressions = slices.DeleteFunc(term.MatchExpressions, func(e core.NodeSelectorRequirement) bool {
			if !nodefeature.IsFitLabelKey(e.Key) && e.Key != ModelManagerRegisteredLabel {
				return false
			}
			return !slices.ContainsFunc(original, func(existing core.NodeSelectorRequirement) bool {
				return kubemeta.DeepEqual(existing, e)
			})
		})
		if len(wanted) == 0 && before > 0 && len(term.MatchExpressions) == 0 && len(term.MatchFields) == 0 {
			got.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = nil
		}
	}
	if kubemeta.DeepEqual(got.NodeAffinity, &core.NodeAffinity{}) {
		got.NodeAffinity = nil
	}
	if kubemeta.DeepEqual(got, &core.Affinity{}) {
		return nil
	}

	return got
}

// replacementComparablePod 将 Pod spec 精简为 operator 所渲染的形态，这正是
// Kueue 据以预留配额的基准。
//
// PodSet 保存的是准入前的模板，而运行中的 Pod 则带有集群在 Kueue 读取之后
// 施加的放置结果：准入链添加的 node selector 与 toleration、被消费的
// scheduling gate，以及调度器绑定的节点。这些只是同一配置在准入前后的不同体现，
// 因此在比较中予以剔除。其余由 operator 渲染的内容仍会参与比较，以确保描述
// 不同配置的模板仍会被拒绝。
func replacementComparablePod(spec core.PodSpec) core.PodSpec {
	reduced := *spec.DeepCopy()
	reduced.Volumes = nil
	reduced.InitContainers = nil
	reduced.EphemeralContainers = nil
	reduced.RestartPolicy = ""
	reduced.TerminationGracePeriodSeconds = nil
	reduced.ActiveDeadlineSeconds = nil
	reduced.DNSPolicy = ""
	reduced.NodeSelector = nil
	reduced.Tolerations = nil
	reduced.SchedulingGates = nil
	reduced.NodeName = ""
	for i := range reduced.Containers {
		reduced.Containers[i].TerminationMessagePath = ""
		reduced.Containers[i].TerminationMessagePolicy = ""
		reduced.Containers[i].ImagePullPolicy = ""
		// 资源单独进行比较，因为这正是 Kueue 据以预留配额的依据。
		reduced.Containers[i].Resources = core.ResourceRequirements{}
	}

	return reduced
}

// resourceQuantityListEqual 按数值而非表示形式比较两个资源列表是否相等。
func resourceQuantityListEqual(got, want core.ResourceList) bool {
	if len(got) != len(want) {
		return false
	}
	for name, quantity := range want {
		other, present := got[name]
		if !present {
			return false
		}
		if quantity.Cmp(other) != 0 {
			return false
		}
	}

	return true
}

// replacementMembersMatch 报告当前存在的组是否符合期望的 spec 与渲染版本要求，
// 并指出第一处差异。
func replacementMembersMatch(want, standing []*core.Pod, minRenderVersion int) string {
	if len(standing) != len(want) {
		return fmt.Sprintf("the replica holds %d of the %d members it is rendered at",
			len(standing), len(want))
	}

	seats := make(map[int]*core.Pod, len(standing))
	for _, member := range standing {
		seat := modelDeploymentPodMemberIndex(member)
		if _, duplicated := seats[seat]; duplicated {
			return fmt.Sprintf("two members sit at index %d", seat)
		}
		seats[seat] = member
	}

	for index, rendered := range want {
		member, seated := seats[index]
		if !seated {
			return fmt.Sprintf("no member sits at index %d", index)
		}
		if !modelDeploymentPodMatchesCurrency(member, rendered, minRenderVersion) {
			return fmt.Sprintf("member %s does not carry the render this pass produced", member.Name)
		}
	}

	if !modelDeploymentReplicaRenderVersionUniform(standing) {
		return "the replica's members carry different render versions"
	}

	return ""
}

// replacementQueueName 是渲染后的副本所提交到的 LocalQueue，直接从渲染结果中读取，
// 从而确保此检查与 Kueue 准入该组所依据的标签不会不一致。
func replacementQueueName(want []*core.Pod) string {
	for _, rendered := range want {
		if entrance := rendered.Labels[kueuectrlconst.QueueLabel]; entrance != "" {
			return entrance
		}
	}

	return ""
}

// replacementRenderHashes 是某次渲染为单个副本生成的一组指纹，按成员顺序排列。
func replacementRenderHashes(want []*core.Pod) []string {
	hashes := make([]string, 0, len(want))
	for _, rendered := range want {
		hashes = append(hashes, rendered.Annotations[modelDeploymentPodSpecHashAnnotation])
	}

	return hashes
}

// replacementSuperseded 报告该 slot 中的替换副本是否基于随后已被 spec 替换掉的
// 配置渲染而成。
//
// 此处比较的是渲染指纹而非 generation：仅修改副本数会推进 generation，但不会改变
// 任何成员的渲染结果；若将其视为配置变更，会导致健康的替换副本被拆除并按相同配置重建。
func replacementSuperseded(slot modelDeploymentReplacementSlot, want []*core.Pod) bool {
	if len(slot.MemberHashes) == 0 {
		// 该 slot 尚未记录目标渲染结果，因此尚不能判定为已过时。
		return false
	}
	if len(slot.MemberHashes) != len(want) {
		return true
	}

	return !slices.Equal(slot.MemberHashes, replacementRenderHashes(want))
}

// modelDeploymentReplacementProgress 表示单次调谐对某个角色当前活跃 slot 所做的推进。
type modelDeploymentReplacementProgress struct {
	// slot 是需要延续到本轮调谐其余阶段的记录。
	slot modelDeploymentReplacementSlot

	// cleared 表示替换副本已获得准入，且该角色不再持有该 slot。
	cleared bool

	// requeue 表示在该 slot 能够继续推进之前，没有其他事件会唤醒此部署。
	requeue bool
}

// resolveReplacementSlot 将某个角色的活跃 slot 向前推进一个步骤。
//
// 检查问题的顺序即为其生命周期：spec 中不再声明的 ordinal 仅通过清理即可结束；
// 接着移除已捕获的成员；随后在服务端证实空位，因为丢失删除响应不等于对象已消失；
// 再接着在同一个 slot 内重建已过时的替换副本；最后在替换副本获准入后释放该角色。
func (r *ModelDeploymentReconciler) resolveReplacementSlot(
	ctx context.Context, md *workercore.ModelDeployment, slot modelDeploymentReplacementSlot,
	role *workercore.ModelDeploymentRole, want []*core.Pod,
) (modelDeploymentReplacementProgress, error) {
	logger := ctrllog.FromContext(ctx)
	progress := modelDeploymentReplacementProgress{slot: slot}
	minRenderVersion := modelDeploymentEffectiveMinRenderVersion(md)

	// 当副本数不再声明此 ordinal 时会取消该 slot，且取消流程同样要走完清理步骤：
	// 该 slot 捕获的成员与 Workload 仍须先离开，才能清除该记录，否则 finalizer
	// 与资源预留会在记录消失后继续残留。
	if slot.Ordinal >= int(role.Replicas) {
		members, err := r.replacementOccupancyOnServer(ctx, md, slot.Role, slot.Ordinal)
		if err != nil {
			return progress, err
		}
		if err = r.cleanReplacementSlot(ctx, md, slot, replacementPodPointers(members)); err != nil {
			return progress, err
		}
		vacant, why, err := r.replacementVacant(ctx, md, slot)
		if err != nil {
			return progress, err
		}
		if !vacant {
			progress.requeue = true
			logger.V(3).Info("waiting for a cancelled replacement to leave",
				"slot", replacementDescription(slot), "because", why)

			return progress, nil
		}
		progress.cleared = true

		return progress, nil
	}

	// 首先依据自身权限移除 slot 所捕获的对象。此后才新到达的成员属于当前 slot
	// 正在等待的替换副本，应保留不动。
	captured, err := r.replacementOccupancyOnServer(ctx, md, slot.Role, slot.Ordinal)
	if err != nil {
		return progress, err
	}
	// 已经在退出过程中的被捕获成员交由其 finalizer 处理。重新发起删除没有任何意义，
	// 还会导致本轮调谐持续报告自己发起了实际未执行的清理，而下文关于空位的判断
	// 正是基于该状态进行的。
	mine := replacementStandingMemberDeletes(slot, replacementPodPointers(captured))
	if slot.Phase == modelDeploymentReplacementCleanup {
		if err = r.cleanReplacementSlot(ctx, md, slot, mine); err != nil {
			return progress, err
		}
		if len(mine) > 0 {
			progress.requeue = true
			return progress, nil
		}
	}

	standing := replacementUncapturedMembers(slot, captured)
	wl, wlErr := r.replacementGroupWorkload(ctx, md, slot)
	if wlErr != nil {
		return progress, wlErr
	}
	if slot.Phase == modelDeploymentReplacementWaitingForAdmission {
		present := sets.New[types.UID]()
		broken := false
		for _, member := range standing {
			present.Insert(member.UID)
			broken = broken || member.DeletionTimestamp != nil
			seat := modelDeploymentPodMemberIndex(member)
			if seat >= len(want) || !modelDeploymentPodMatchesCurrency(member, want[seat], minRenderVersion) {
				broken = true
			} else if len(standing) < len(want) &&
				modelDeploymentPodRenderVersion(member) != modelDeploymentPodRenderVersion(want[seat]) {
				broken = true
			}
		}
		if !modelDeploymentReplicaRenderVersionUniform(standing) {
			broken = true
		}
		for _, uid := range slot.CurrentMemberUIDs {
			broken = broken || !present.Has(uid)
		}
		// 不能在已组合的组现有的资源预留旁再填充成员。
		ownsCurrent := wl != nil && (wl.UID == slot.CurrentWorkloadUID ||
			modelDeploymentWorkloadOwnsAny(wl, present.Union(sets.New(slot.CurrentMemberUIDs...))))
		if ownsCurrent && len(standing) < len(want) {
			broken = true
		}
		if broken || replacementSuperseded(slot, want) {
			next := modelDeploymentReplacementSlot{
				Role: slot.Role, Ordinal: slot.Ordinal, Phase: modelDeploymentReplacementCleanup,
				MemberUIDs:  sets.List(present.Union(sets.New(slot.CurrentMemberUIDs...))),
				WorkloadUID: slot.CurrentWorkloadUID,
			}
			if ownsCurrent {
				next.WorkloadUID = wl.UID
			}
			progress.slot = next
			if err = r.advanceReplacementSlot(ctx, md, next); err != nil {
				return progress, err
			}
			if err = r.cleanReplacementSlot(ctx, md, next, standing); err != nil {
				return progress, err
			}
			progress.requeue = true
			return progress, nil
		}
		// 在准入或清理抹除可观测证据之前，先持久化成员与 Workload 的身份标识。
		slot.CurrentMemberUIDs = sets.List(present)
		if ownsCurrent {
			slot.CurrentWorkloadUID = wl.UID
		}
		if err = r.advanceReplacementSlot(ctx, md, slot); err != nil {
			return progress, err
		}
		progress.slot = slot
	}

	// 一旦存在未被 slot 捕获的成员，即视为替换副本已存在。此后 slot 将转为等待
	// 准入而非等待空位，这是两个不同的问题：空位关注的是旧组是否离开，
	// 而准入关注的是新组是否已获得配额。
	if len(standing) == 0 {
		vacant, why, err := r.replacementVacant(ctx, md, slot)
		if err != nil {
			return progress, err
		}
		if !vacant {
			// 仍有对象占用着该 ordinal。这是正常的等待状态（例如成员正在其 finalizer 下排空，
			// 或 Workload 尚未消失），它应通过 Pod 和 Workload 事件来结束，而非依赖定时器。
			logger.V(3).Info("waiting for the replaced group to leave",
				"slot", replacementDescription(slot), "because", why)

			return progress, nil
		}

		// 一旦旧组离开，便记录目标渲染结果，后续配置修改将以此作为比对基准。
		if slot.Phase == modelDeploymentReplacementCleanup || len(slot.MemberHashes) == 0 {
			progress.slot = modelDeploymentReplacementSlot{
				Role:         slot.Role,
				Ordinal:      slot.Ordinal,
				Phase:        modelDeploymentReplacementWaitingForAdmission,
				MemberHashes: replacementRenderHashes(want),
			}
			if err = r.advanceReplacementSlot(ctx, md, progress.slot); err != nil {
				return progress, err
			}
		}

		return progress, nil
	}

	admitted, reason, err := r.replacementAdmitted(ctx, md, slot, want)
	if err != nil {
		return progress, err
	}
	if admitted {
		progress.cleared = true

		return progress, nil
	}
	logger.V(3).Info("waiting for the replacement to be admitted",
		"slot", replacementDescription(slot), "because", reason)

	return progress, nil
}

// replacementStandingMemberDeletes 返回仍在运行的被捕获成员，跳过已在终止中的成员。
// 已执行过删除的成员无论本轮是否操作都已处于退出流程中。
func replacementStandingMemberDeletes(
	slot modelDeploymentReplacementSlot, members []*core.Pod,
) []*core.Pod {
	deletes := make([]*core.Pod, 0, len(members))
	for _, member := range replacementMemberDeletes(slot, members) {
		if member.DeletionTimestamp == nil {
			deletes = append(deletes, member)
		}
	}

	return deletes
}

// replacementUncapturedMembers 返回位于 slot 所在 ordinal 上、但未被该 slot 捕获的成员：
// 即此前某轮调谐成功创建出来的替换副本成员。
func replacementUncapturedMembers(
	slot modelDeploymentReplacementSlot, live []core.Pod,
) []*core.Pod {
	pointers := replacementPodPointers(live)
	uncaptured := make([]*core.Pod, 0, len(pointers))
	for _, member := range pointers {
		if !replacementCapturesMember(slot, member.UID) {
			uncaptured = append(uncaptured, member)
		}
	}
	slices.SortFunc(uncaptured, func(a, b *core.Pod) int { return strings.Compare(a.Name, b.Name) })

	return uncaptured
}

// replacementPodPointers 将 Pod 值切片转换为清理辅助函数所需的指针切片。
func replacementPodPointers(pods []core.Pod) []*core.Pod {
	pointers := make([]*core.Pod, 0, len(pods))
	for i := range pods {
		pointers = append(pointers, &pods[i])
	}

	return pointers
}

// cleanReplacementSlot 移除 slot 所捕获的对象：先按各自的 UID 移除其成员，
// 再移除持有这些成员的 Workload。
//
// 成员及其 Workload 仅依据已捕获的 UID 删除。组名仅用于定位阻塞空位的对象，
// 绝不能作为授权清理的依据，因此无法证明由本 operator 组合的 Workload 会被保留，
// 进而由空位检查继续等待它离开。
func (r *ModelDeploymentReconciler) cleanReplacementSlot(
	ctx context.Context, md *workercore.ModelDeployment, slot modelDeploymentReplacementSlot,
	members []*core.Pod,
) error {
	for _, member := range replacementMemberDeletes(slot, members) {
		uid := member.UID
		if err := r.Client.Delete(ctx, member, ctrlcli.Preconditions{UID: &uid}); err != nil &&
			!kerrors.IsNotFound(err) {
			return fmt.Errorf("delete replaced member %s: %w", member.Name, err)
		}
	}

	// 即使所有成员都已消失，slot 仍须删除其对应的 Workload。正是该 Workload 负责
	// 释放 Pod 上 Kueue 的 finalizer 并归还配额预留；若因成员已离开而误以为一切完成，
	// 会导致该组永久占用配额。
	listed := new(kueue.WorkloadList)
	if err := r.APIReader.List(ctx, listed, ctrlcli.InNamespace(md.Namespace)); err != nil {
		return fmt.Errorf("list replaced workloads: %w", err)
	}
	workloads := make([]*kueue.Workload, 0, len(listed.Items))
	for i := range listed.Items {
		workloads = append(workloads, &listed.Items[i])
	}
	for _, wl := range replacementWorkloadDeletes(slot, workloads) {
		uid := wl.UID
		if err := r.Client.Delete(ctx, wl, ctrlcli.Preconditions{UID: &uid}); err != nil &&
			!kerrors.IsNotFound(err) {
			return fmt.Errorf("delete replaced replica's workload %s: %w", wl.Name, err)
		}
	}

	return nil
}
