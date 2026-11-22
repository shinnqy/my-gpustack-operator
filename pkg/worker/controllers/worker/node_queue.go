package worker

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlbuilder "sigs.k8s.io/controller-runtime/pkg/builder"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlevent "sigs.k8s.io/controller-runtime/pkg/event"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	ctrlpredicate "sigs.k8s.io/controller-runtime/pkg/predicate"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"gpustack.ai/gpustack/pkg/controller"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/systemmeta"
	"gpustack.ai/gpustack/pkg/systemname"
	"gpustack.ai/gpustack/pkg/utils/ctrlclix"
	"gpustack.ai/gpustack/pkg/utils/ctrlhandlerx"
	"gpustack.ai/gpustack/pkg/utils/mapx"
	"gpustack.ai/gpustack/pkg/utils/slicex"
	"gpustack.ai/gpustack/pkg/utils/strconvx"
	"gpustack.ai/gpustack/pkg/worker/apistatus"
	"gpustack.ai/gpustack/pkg/worker/settings"
)

// NodeQueueReconciler 监听 ClusterQueue、ResourceFlavor 和 AdmissionCheck，
// 为受管队列维护配额和准入条件。InstanceTypeReconciler 负责创建和删除队列，
// 并设置调度标签及隔离策略。此处按标签直接查询池，核对 flavor 能选中的节点，
// 再按 CPU 核或加速器 credits 填配额。
//   - 删除队列时设置 HoldAndDrain，使 Kueue 驱逐已准入负载并在排空后解除 finalizer。
//   - 池内有 flavor 时按单节点数量升序填入资源组。设备检查满足条件后挂到加速器队列，联合准入检查就绪后挂到所有队列。撤去空资源组的暂停标记，再由 InstanceTypeReconciler 按管理员意图恢复准入。
//   - 池失去全部 flavor 时，按设置主动排空或等待预留自然释放，之后才清空配额，防止 Kueue 的预留计数变为负数。
//   - 初次创建或校验失败的队列如果没有资源组，就先 Hold 并标记归属，避免它接纳任意资源请求。
//
// 只有资源组已经清空时才恢复队列，不会打断正在进行的排空。
type NodeQueueReconciler struct {
	Client ctrlcli.Client
}

var _ ctrlreconcile.Reconciler = (*NodeQueueReconciler)(nil)

// _ClusterQueueResType 是 systemmeta 为规格对应的 ClusterQueue 写入的资源类别。
const _ClusterQueueResType = "instancetypes"

// IsInstanceTypeClusterQueue 根据创建队列时写入的资源类别，判断它是否为 Operator 管理的规格队列。
// 该队列与 InstanceType 同名。
func IsInstanceTypeClusterQueue(cq *kueue.ClusterQueue) bool {
	return systemmeta.MatchResource(cq, _ClusterQueueResType)
}

const (
	_TASQueueAnnotation                    = "topology.gpustack.ai/tas-queue"
	_TASQueueMigrationPhaseAnnotation      = "topology.gpustack.ai/migration-phase"
	_TASQueueMigrationStopPolicyAnnotation = "topology.gpustack.ai/migration-stop-policy"
	_TASQueueMigrationPhaseDraining        = "draining"
	_TASQueueMigrationPhaseSwitched        = "switched"
	_TASQueueMigrationStopPolicyUnset      = "unset"
	_TASQueueEmptyPlanHoldAnnotation       = "topology.gpustack.ai/empty-plan-hold"
	_TASQueueMigrationRequeueAfter         = 5 * time.Second
	_maxQueueFlavors                       = 64

	nodeQueueConditionTopologyReady = apistatus.ClusterQueueConditionTopologyReady
)

type nodeQueueValidationError struct {
	reason  string
	message string
}

func (e *nodeQueueValidationError) Error() string { return e.message }

// IMPORTANT: 查池内 ResourceFlavor → 校验拓扑与配额守恒 → 填 Quota(credits/cpu)
func (r *NodeQueueReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := ctrllog.FromContext(ctx)

	// 按名字读取队列。
	cq := new(kueue.ClusterQueue)
	err := r.Client.Get(ctx, req.NamespacedName, cq,
		ctrlclix.WithoutQuorum)
	if err != nil {
		if !kerrors.IsNotFound(err) {
			logger.Error(err, "fetch cluster queue")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// 管理员或规格清理流程请求删除队列后，Kueue 会保留 ResourceInUse finalizer 直到排空。
	// 此处无条件设置 HoldAndDrain，促使 Kueue 驱逐已准入负载、释放预留并删除队列。
	// instance-type-drain-when-no-flavors 只控制丢失 flavor 时的自动排空。Kueue 已有 finalizer，
	// 无需再加一层。
	if cq.DeletionTimestamp != nil {
		if ptr.Deref(cq.Spec.StopPolicy, kueue.None) == kueue.HoldAndDrain {
			return ctrl.Result{}, nil
		}
		cq.Spec.StopPolicy = ptr.To(kueue.HoldAndDrain)
		if err = r.Client.Update(ctx, cq); err != nil {
			// 所有队列写入遇到冲突时都主动重试。常见原因是 Kueue 同时更新了 status，
			// 而此类更新会被监听过滤器忽略。
			return objectWriteResult(logger, err, "hold and drain deleting cluster queue", _requeueAfterConflict)
		}
		logger.V(2).Info("holding and draining deleting cluster queue")
		return ctrl.Result{}, nil
	}

	// 按队列的特征键、系统和架构等标签查找池内的 ResourceFlavor，这也适用于管理员自定义名称的队列。
	lbs := nodefeature.PoolFlavorSelector(cq.Labels)
	if len(lbs) == 0 {
		return ctrl.Result{}, nil
	}
	// 加上受管资源标记，避免相同调度标签的外部 ResourceFlavor 混入配额。
	// InstanceType webhook 也使用相应的筛选条件。
	lbs[systemmeta.ResourceTypeLabel] = _ResourceFlavorResType
	rfList := new(kueue.ResourceFlavorList)
	err = r.Client.List(ctx, rfList,
		ctrlcli.MatchingLabels(lbs),
		ctrlcli.UnsafeDisableDeepCopy)
	if err != nil {
		logger.Error(err, "list resource flavors by cluster queue")
		return ctrl.Result{}, err
	}

	// 正在删除的 flavor 从期望资源组中排除。Kueue 会保留它的 finalizer，
	// 直到所有 ClusterQueue 都移除引用。若仍可能有使用该 flavor 的配额，先排空队列，
	// 因为单独撤掉引用不会驱逐或重新准入现有负载。池内全部 flavor 都在删除时，
	// 按无 flavor 的流程处理。
	live := rfList.Items[:0]
	for i := range rfList.Items {
		if rfList.Items[i].DeletionTimestamp == nil {
			live = append(live, rfList.Items[i])
		}
	}
	rfList.Items = live

	if len(rfList.Items) > 0 {
		return r.fillClusterQueue(ctx, cq, rfList)
	}
	return r.drainOrEmptyClusterQueue(ctx, cq)
}

// fillClusterQueue 用池内 flavor 填充资源组，单节点数量小的在前；若队列先前已排空，按其策略恢复。
func (r *NodeQueueReconciler) fillClusterQueue(
	ctx context.Context, cq *kueue.ClusterQueue, rfList *kueue.ResourceFlavorList,
) (ctrl.Result, error) {
	logger := ctrllog.FromContext(ctx)
	if failure := duplicateCoveredResource(cq.Spec.ResourceGroups); failure != nil {
		return r.rejectClusterQueue(ctx, cq, failure)
	}
	if len(cq.Spec.ResourceGroups) > 0 && cq.Annotations[_TASQueueAnnotation] != "true" {
		return r.rejectClusterQueue(ctx, cq, &nodeQueueValidationError{
			reason:  "UnsupportedExistingObject",
			message: "existing ClusterQueue has quota that was not created by the topology-aware queue controller; create fresh managed objects",
		})
	}
	if failure, err := r.validateTASFlavors(ctx, cq, rfList); err != nil {
		return ctrl.Result{}, err
	} else if failure != nil {
		return r.rejectClusterQueue(ctx, cq, failure)
	}

	// 先列单节点数量较小的 flavor，使 Kueue 在有多种选择时优先用小节点。
	slices.SortStableFunc(rfList.Items, func(a, b kueue.ResourceFlavor) int {
		if byCount := cmp.Compare(parseResourceFlavorCount(&a), parseResourceFlavorCount(&b)); byCount != 0 {
			return byCount
		}
		return cmp.Compare(a.Name, b.Name)
	})

	_, firstNotes := systemmeta.DescribeResource(&rfList.Items[0])
	acceleratable := firstNotes["acceleratable"] == "true"
	eGroups := buildResourceGroups(rfList, acceleratable, firstNotes["manufacturer"])

	changed := false
	if cq.Annotations == nil {
		cq.Annotations = make(map[string]string)
	}
	if cq.Annotations[_TASQueueAnnotation] != "true" {
		cq.Annotations[_TASQueueAnnotation] = "true"
		changed = true
	}
	// 仅在设备 AdmissionCheck 已存在且为 Active 时才为加速器队列添加引用，
	// 否则 Kueue 会把队列标记为不可用。监听该检查可以在激活时补上引用，失效或删除时撤下引用。
	//
	// 节点派生设置对整个集群生效。开启时，包括管理员自建规格在内的加速器队列均可启用设备检查；
	// 关闭时继续填配额，但所有队列都跳过该设备检查。
	//
	// 联合准入检查一旦 Active，就应挂到所有队列，不受设备类型或派生设置限制。
	// 多角色部署的不同 Workload 可能进入 CPU 池，也可能使用管理员自建的规格；若漏掉一个入口，
	// 可能有一部分角色先行对外服务。不属于多角色部署的工作负载会直接通过这项检查。
	derived := settings.InstanceTypeDerivedFromNode.ShouldValueBool(ctx)

	var rules []kueue.AdmissionCheckStrategyRule
	if acceleratable && derived && r.admissionCheckActive(ctx, _NodeDevicesAdmissionCheckName) {
		rules = append(rules,
			kueue.AdmissionCheckStrategyRule{Name: kueue.AdmissionCheckReference(_NodeDevicesAdmissionCheckName)})
	}
	if r.admissionCheckActive(ctx, _JointAdmissionCheckName) {
		rules = append(rules,
			kueue.AdmissionCheckStrategyRule{Name: kueue.AdmissionCheckReference(_JointAdmissionCheckName)})
	}

	var admissionChecks *kueue.AdmissionChecksStrategy
	if len(rules) > 0 {
		admissionChecks = &kueue.AdmissionChecksStrategy{AdmissionChecks: rules}
	}
	if !kubemeta.DeepEqual(cq.Spec.AdmissionChecksStrategy, admissionChecks) {
		cq.Spec.AdmissionChecksStrategy = admissionChecks
		changed = true
	}

	// 仅当要移除的 flavor 可能仍持有配额时才暂停并排空，驱逐旧负载后再按新方案准入。
	// 节点加入或离开同一 profile 只改变标称配额，新增 profile 只是多一个引用，
	// 移除已确认空闲的 flavor 也无负载要迁移；这些变更都可原地完成。
	planChanged := !kubemeta.DeepEqual(cq.Spec.ResourceGroups, eGroups)
	migrate := cq.Annotations[_TASQueueMigrationPhaseAnnotation] != ""
	if !migrate && planChanged {
		if dropped := droppedFlavorReferences(cq.Spec.ResourceGroups, eGroups); len(dropped) > 0 {
			// Kueue 在同一次状态更新中写入 flavor 用量和 Active 条件的 observedGeneration。
			// 若尚未观察到当前 spec，使用量可能遗漏新 flavor。等待状态追上后再判断，
			// 避免基于旧快照误驱逐。
			if !clusterQueueObservedAtCurrentGeneration(cq) {
				return ctrl.Result{RequeueAfter: _TASQueueMigrationRequeueAfter}, r.setTopologyReadyConditionStatus(
					ctx, cq, meta.ConditionUnknown, "AwaitingQueueStatus",
					fmt.Sprintf("waiting for Kueue to report flavor usage at generation %d before dropping flavors %v",
						cq.Generation, dropped),
				)
			}
			migrate = flavorsMayBeInUse(cq, dropped)
		}
	}
	if migrate {
		return r.migrateClusterQueueResourceGroups(ctx, cq, eGroups, changed)
	}

	// 只恢复由本控制器排空的队列，即 HoldAndDrain 且配额为空。
	// 管理员通过 Inactive 设置的 Hold 由 InstanceTypeReconciler 维护，flavor 返回时也应保留，
	// 避免停用的规格短暂接纳工作负载。
	if ptr.Deref(cq.Spec.StopPolicy, kueue.None) == kueue.HoldAndDrain && len(cq.Spec.ResourceGroups) == 0 {
		cq.Spec.StopPolicy = ptr.To(kueue.None)
		changed = true
	}
	// 与首批资源组和 AdmissionCheck 引用一同写入时，只移除“空队列暂停”标记。
	// InstanceTypeReconciler 能读取 Inactive，由它决定是否解除 Hold，
	// 避免尚无配额、尚无检查或管理员已停用的队列短暂开放。
	if cq.Annotations[_TASQueueEmptyPlanHoldAnnotation] != "" && len(eGroups) > 0 {
		delete(cq.Annotations, _TASQueueEmptyPlanHoldAnnotation)
		changed = true
	}
	if planChanged {
		cq.Spec.ResourceGroups = eGroups
		changed = true
	}
	if changed {
		if err := r.Client.Update(ctx, cq); err != nil {
			return objectWriteResult(logger, err, "fill cluster queue resource groups", _requeueAfterConflict)
		}
		logger.V(2).Info("filled cluster queue resource groups")
	}
	return ctrl.Result{}, r.setTopologyReadyCondition(ctx, cq, true, "Ready", "all queue flavors are topology-aware and quota is conserved")
}

func (r *NodeQueueReconciler) migrateClusterQueueResourceGroups(
	ctx context.Context,
	cq *kueue.ClusterQueue,
	desired []kueue.ResourceGroup,
	changed bool,
) (ctrl.Result, error) {
	logger := ctrllog.FromContext(ctx)
	phase := cq.Annotations[_TASQueueMigrationPhaseAnnotation]

	if phase == "" {
		cq.Annotations[_TASQueueMigrationPhaseAnnotation] = _TASQueueMigrationPhaseDraining
		cq.Annotations[_TASQueueMigrationStopPolicyAnnotation] = encodeStopPolicy(cq.Spec.StopPolicy)
		cq.Spec.StopPolicy = ptr.To(kueue.HoldAndDrain)
		if err := r.Client.Update(ctx, cq); err != nil {
			return objectWriteResult(logger, err, "start cluster queue topology migration", _requeueAfterConflict)
		}
		if err := r.setTopologyReadyCondition(ctx, cq, false, "Migrating", "holding and draining before switching topology flavors"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: _TASQueueMigrationRequeueAfter}, nil
	}

	if phase != _TASQueueMigrationPhaseDraining && phase != _TASQueueMigrationPhaseSwitched {
		return r.rejectClusterQueue(ctx, cq, &nodeQueueValidationError{
			reason:  "InvalidMigrationState",
			message: fmt.Sprintf("ClusterQueue has unsupported topology migration phase %q", phase),
		})
	}
	if _, failure := decodeStopPolicy(cq.Annotations[_TASQueueMigrationStopPolicyAnnotation]); failure != nil {
		return r.rejectClusterQueue(ctx, cq, failure)
	}
	if ptr.Deref(cq.Spec.StopPolicy, kueue.None) != kueue.HoldAndDrain {
		cq.Spec.StopPolicy = ptr.To(kueue.HoldAndDrain)
		changed = true
	}
	if changed {
		if err := r.Client.Update(ctx, cq); err != nil {
			return objectWriteResult(logger, err, "maintain held cluster queue topology migration", _requeueAfterConflict)
		}
		return ctrl.Result{RequeueAfter: _TASQueueMigrationRequeueAfter}, nil
	}
	if !clusterQueueStoppedAtCurrentGeneration(cq) || hasReserved(cq) {
		return ctrl.Result{RequeueAfter: _TASQueueMigrationRequeueAfter}, nil
	}

	if phase == _TASQueueMigrationPhaseDraining || !kubemeta.DeepEqual(cq.Spec.ResourceGroups, desired) {
		cq.Spec.ResourceGroups = desired
		cq.Annotations[_TASQueueMigrationPhaseAnnotation] = _TASQueueMigrationPhaseSwitched
		if err := r.Client.Update(ctx, cq); err != nil {
			return objectWriteResult(logger, err, "switch held cluster queue topology flavors", _requeueAfterConflict)
		}
		return ctrl.Result{RequeueAfter: _TASQueueMigrationRequeueAfter}, nil
	}

	// 空资源组没有声明资源，Kueue 的 IgnoreUndeclared 策略会忽略未声明的请求；
	// 此时解除暂停可能使任意 Workload 绕过 flavor 和准入检查。因此保持暂停及迁移注解，
	// 待 flavor 返回并给出非空方案后恢复。
	if len(desired) == 0 {
		return ctrl.Result{}, r.setTopologyReadyCondition(ctx, cq, true, "Ready", "all queue flavors are topology-aware and quota is conserved")
	}

	restored, _ := decodeStopPolicy(cq.Annotations[_TASQueueMigrationStopPolicyAnnotation])
	cq.Spec.StopPolicy = restored
	delete(cq.Annotations, _TASQueueMigrationPhaseAnnotation)
	delete(cq.Annotations, _TASQueueMigrationStopPolicyAnnotation)
	if err := r.Client.Update(ctx, cq); err != nil {
		return objectWriteResult(logger, err, "restore cluster queue after topology migration", _requeueAfterConflict)
	}
	return ctrl.Result{}, r.setTopologyReadyCondition(ctx, cq, true, "Ready", "all queue flavors are topology-aware and quota is conserved")
}

func encodeStopPolicy(policy *kueue.StopPolicy) string {
	if policy == nil {
		return _TASQueueMigrationStopPolicyUnset
	}
	return string(*policy)
}

func decodeStopPolicy(value string) (*kueue.StopPolicy, *nodeQueueValidationError) {
	if value == _TASQueueMigrationStopPolicyUnset {
		return nil, nil
	}
	policy := kueue.StopPolicy(value)
	switch policy {
	case kueue.None, kueue.Hold, kueue.HoldAndDrain:
		return ptr.To(policy), nil
	default:
		return nil, &nodeQueueValidationError{
			reason:  "InvalidMigrationState",
			message: fmt.Sprintf("ClusterQueue has unsupported saved stop policy %q", value),
		}
	}
}

func clusterQueueStoppedAtCurrentGeneration(cq *kueue.ClusterQueue) bool {
	condition := apimeta.FindStatusCondition(cq.Status.Conditions, kueue.ClusterQueueActive)
	return clusterQueueObservedAtCurrentGeneration(cq) &&
		condition.Status == meta.ConditionFalse &&
		condition.Reason == kueue.ClusterQueueActiveReasonStopped
}

// clusterQueueObservedAtCurrentGeneration 判断 Kueue 是否已为当前 spec 写过状态。
// Active 条件的 observedGeneration 与预留及用量计数在同次更新中写入，
// 只有版本一致时这些计数才对应当前方案。
func clusterQueueObservedAtCurrentGeneration(cq *kueue.ClusterQueue) bool {
	condition := apimeta.FindStatusCondition(cq.Status.Conditions, kueue.ClusterQueueActive)
	return condition != nil && condition.ObservedGeneration >= cq.Generation
}

func (r *NodeQueueReconciler) rejectClusterQueue(
	ctx context.Context, cq *kueue.ClusterQueue, failure *nodeQueueValidationError,
) (ctrl.Result, error) {
	ctrllog.FromContext(ctx).Error(failure, "reject topology-aware cluster queue")
	// 方案校验失败时保留上一次完整的资源组；还没有资源组的队列须保持暂停。
	if err := r.holdEmptyClusterQueue(ctx, cq); err != nil {
		return ctrl.Result{}, err
	}
	result := ctrl.Result{}
	// NodeQueue 没有直接监听 Node。缺少 Topology 或 flavor 尚未覆盖全部节点时，
	// 后续资源事件可能修复数据，所以安排重试。
	if failure.reason == "MissingTopology" || failure.reason == "NonConservedQuota" ||
		cq.Annotations[_TASQueueMigrationPhaseAnnotation] != "" {
		result.RequeueAfter = 30 * time.Second
	}
	return result, r.setTopologyReadyCondition(ctx, cq, false, failure.reason, failure.message)
}

func (r *NodeQueueReconciler) setTopologyReadyCondition(
	ctx context.Context, cq *kueue.ClusterQueue, ready bool, reason, message string,
) error {
	status := meta.ConditionFalse
	if ready {
		status = meta.ConditionTrue
	}
	return r.setTopologyReadyConditionStatus(ctx, cq, status, reason, message)
}

func (r *NodeQueueReconciler) setTopologyReadyConditionStatus(
	ctx context.Context, cq *kueue.ClusterQueue, status meta.ConditionStatus, reason, message string,
) error {
	before := cq.DeepCopy()
	nodeQueueConditionTopologyReady.Status(cq, string(status), reason, message)
	if kubemeta.DeepEqual(before.Status, cq.Status) {
		return nil
	}
	return r.Client.Status().Patch(ctx, cq, ctrlcli.MergeFrom(before))
}

func duplicateCoveredResource(groups []kueue.ResourceGroup) *nodeQueueValidationError {
	seen := make(map[core.ResourceName]struct{})
	for _, group := range groups {
		for _, name := range group.CoveredResources {
			if _, exists := seen[name]; exists {
				return &nodeQueueValidationError{
					reason:  "DuplicateCoveredResource",
					message: fmt.Sprintf("resource %q is covered by more than one ClusterQueue resource group", name),
				}
			}
			seen[name] = struct{}{}
		}
	}
	return nil
}

func (r *NodeQueueReconciler) validateTASFlavors(
	ctx context.Context, cq *kueue.ClusterQueue, rfList *kueue.ResourceFlavorList,
) (*nodeQueueValidationError, error) {
	if len(rfList.Items) > _maxQueueFlavors {
		return &nodeQueueValidationError{
			reason:  "TooManyFlavors",
			message: fmt.Sprintf("covered resource requires %d flavors; Kueue permits at most %d", len(rfList.Items), _maxQueueFlavors),
		}, nil
	}
	selected := make(map[string]string)
	selectedCounts := make([]int, len(rfList.Items))
	for i := range rfList.Items {
		rf := &rfList.Items[i]
		profile := rf.Spec.NodeLabels[TopologyProfileLabel]
		if profile == "" || rf.Labels[TopologyProfileLabel] != profile || rf.Spec.TopologyName == nil {
			return &nodeQueueValidationError{
				reason:  "MissingTopology",
				message: fmt.Sprintf("ResourceFlavor %q is not bound to one topology profile", rf.Name),
			}, nil
		}
		if expected := topologyName(profile); string(*rf.Spec.TopologyName) != expected {
			return &nodeQueueValidationError{
				reason: "MissingTopology",
				message: fmt.Sprintf(
					"ResourceFlavor %q references Topology %q instead of profile Topology %q",
					rf.Name, *rf.Spec.TopologyName, expected,
				),
			}, nil
		}
		topology := new(kueue.Topology)
		if err := r.Client.Get(ctx, ctrlcli.ObjectKey{Name: string(*rf.Spec.TopologyName)}, topology); err != nil {
			if kerrors.IsNotFound(err) {
				return &nodeQueueValidationError{
					reason:  "MissingTopology",
					message: fmt.Sprintf("ResourceFlavor %q references missing Topology %q", rf.Name, *rf.Spec.TopologyName),
				}, nil
			}
			return nil, err
		}
		nodes := new(core.NodeList)
		if err := r.Client.List(ctx, nodes, ctrlcli.MatchingLabels(rf.Spec.NodeLabels)); err != nil {
			return nil, err
		}
		selectedCounts[i] = len(nodes.Items)
		for _, node := range nodes.Items {
			if prior, exists := selected[node.Name]; exists {
				return &nodeQueueValidationError{
					reason:  "OverlappingSelectors",
					message: fmt.Sprintf("Node %q is selected by ResourceFlavors %q and %q", node.Name, prior, rf.Name),
				}, nil
			}
			selected[node.Name] = rf.Name
		}
	}
	for i := range rfList.Items {
		rf := &rfList.Items[i]
		count := parseResourceFlavorCount(rf)
		capacity := parseResourceFlavorCapacity(rf)
		expected := int64(selectedCounts[i]) * count
		if count <= 0 || capacity != expected {
			return &nodeQueueValidationError{
				reason: "NonConservedQuota",
				message: fmt.Sprintf(
					"ResourceFlavor %q advertises capacity %d but %d selected Nodes contribute %d each",
					rf.Name, capacity, selectedCounts[i], count,
				),
			}, nil
		}
	}

	// NodeFlavor 会计入该池的所有受管且已带 profile 的节点，都必须被有效 flavor 的选择器覆盖。
	// 关闭混合模式时，加速器节点不计入 CPU 池。
	poolSelector := nodefeature.PoolFlavorSelector(cq.Labels)
	if poolSelector == nil {
		return nil, nil
	}
	cpuPool := poolSelector[nodefeature.NodeAcceleratableLabelKey] != "true"
	if cpuPool {
		// 纯 CPU 节点通常没有 acceleratable 标签，查询时不能要求它带有值为 false 的标签。
		delete(poolSelector, nodefeature.NodeAcceleratableLabelKey)
	}
	poolSelector[systemname.ManagedLabelKey] = "true"
	mixingAllowed := settings.InstanceTypeMixedOnNode.ShouldValueBool(ctx)
	poolNodes := new(core.NodeList)
	if err := r.Client.List(ctx, poolNodes, ctrlcli.MatchingLabels(poolSelector)); err != nil {
		return nil, err
	}
	for i := range poolNodes.Items {
		node := &poolNodes.Items[i]
		if node.Labels[TopologyProfileLabel] == "" || (cpuPool && !mixingAllowed && nodeIsAccelerated(node)) {
			continue
		}
		if _, exists := selected[node.Name]; !exists {
			return &nodeQueueValidationError{
				reason:  "NonConservedQuota",
				message: fmt.Sprintf("Node %q belongs to the queue pool but contributes to no ResourceFlavor", node.Name),
			}, nil
		}
	}
	return nil, nil
}

// droppedFlavorReferences 找出当前资源组中存在、期望资源组中将移除的 flavor 引用。
func droppedFlavorReferences(current, desired []kueue.ResourceGroup) []kueue.ResourceFlavorReference {
	wanted := make(map[kueue.ResourceFlavorReference]struct{})
	for _, group := range desired {
		for _, flavor := range group.Flavors {
			wanted[flavor.Name] = struct{}{}
		}
	}
	var dropped []kueue.ResourceFlavorReference
	for _, group := range current {
		for _, flavor := range group.Flavors {
			if _, exists := wanted[flavor.Name]; !exists {
				dropped = append(dropped, flavor.Name)
			}
		}
	}
	return dropped
}

// flavorsMayBeInUse 检查要移除的 flavor 是否仍有预留或使用量。需要先确认状态对应当前 generation。
// 计数中缺少某个 flavor 时，其用量未知，也按可能仍在使用处理。
//
// 计数可能晚于实际准入。Kueue 写完状态后仍可能给 flavor 新增预留，
// 直接删去资源组中的引用也不会驱逐已经运行的 Workload。
func flavorsMayBeInUse(cq *kueue.ClusterQueue, flavors []kueue.ResourceFlavorReference) bool {
	idle := func(usage []kueue.FlavorUsage, name kueue.ResourceFlavorReference) bool {
		i := slices.IndexFunc(usage, func(u kueue.FlavorUsage) bool { return u.Name == name })
		return i >= 0 && !slices.ContainsFunc(usage[i].Resources, func(u kueue.ResourceUsage) bool {
			return !u.Total.IsZero() || !u.Borrowed.IsZero()
		})
	}
	return slices.ContainsFunc(flavors, func(name kueue.ResourceFlavorReference) bool {
		return !idle(cq.Status.FlavorsReservation, name) || !idle(cq.Status.FlavorsUsage, name)
	})
}

// drainOrEmptyClusterQueue 处理池内所有 flavor 消失的情况。
// 已预留资源时按设置选择主动 HoldAndDrain 或等待自然释放；即使预留已归零，仍先暂停再清空配额，
// 保证计数正确。已清空的队列维持暂停，等待容量恢复。
func (r *NodeQueueReconciler) drainOrEmptyClusterQueue(
	ctx context.Context, cq *kueue.ClusterQueue,
) (ctrl.Result, error) {
	if cq.Annotations[_TASQueueMigrationPhaseAnnotation] != "" {
		return r.migrateClusterQueueResourceGroups(ctx, cq, nil, false)
	}
	if len(cq.Spec.ResourceGroups) == 0 {
		return ctrl.Result{}, r.holdEmptyClusterQueue(ctx, cq)
	}

	drain := settings.InstanceTypeDrainWhenNoFlavors.ShouldValueBool(ctx)
	if !drain && hasReserved(cq) {
		// 未启用自动排空时，等现有预留自然释放。
		return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
	}

	if cq.Annotations == nil {
		cq.Annotations = make(map[string]string)
	}
	// 移除最后一个 flavor 和替换 profile 一样需要原地迁移。先暂停，
	// 防止看到零预留之后、切换方案之前又出现新预留。标记中保存原有策略，若迁移期间有 flavor 返回，
	// 也可在恢复准入前纳入新方案。
	return r.migrateClusterQueueResourceGroups(ctx, cq, nil, false)
}

// holdEmptyClusterQueue 将没有资源组且仍开放的队列设为 Hold，并标记由本控制器管理，
// 直到 fillClusterQueue 写入首批资源组。Kueue 会忽略未声明资源，过早开放可能绕过 flavor 和准入检查。
// 此时没有配额需要排空，所以使用 Hold，避免 HoldAndDrain 同时停掉规格的 Instance。
// 已经在迁移或由管理员暂停的队列沿用其现有策略。
func (r *NodeQueueReconciler) holdEmptyClusterQueue(ctx context.Context, cq *kueue.ClusterQueue) error {
	if len(cq.Spec.ResourceGroups) != 0 || cq.Annotations[_TASQueueMigrationPhaseAnnotation] != "" ||
		ptr.Deref(cq.Spec.StopPolicy, kueue.None) != kueue.None {
		return nil
	}
	if cq.Annotations == nil {
		cq.Annotations = make(map[string]string)
	}
	cq.Annotations[_TASQueueEmptyPlanHoldAnnotation] = "true"
	cq.Spec.StopPolicy = ptr.To(kueue.Hold)
	if err := r.Client.Update(ctx, cq); err != nil {
		ctrllog.FromContext(ctx).Error(err, "hold cluster queue without resource groups")
		return err
	}
	ctrllog.FromContext(ctx).V(2).Info("held cluster queue without resource groups")
	return nil
}

// hasReserved 检查队列是否仍有配额预留，或仍有正在预留、已准入的负载。驱逐尚未完成时，
// 某个 flavor 的计数可能已为零，因此还要看工作负载计数。Pending 负载尚无预留，不计入排空条件，
// 否则可能永远无法删除队列。
func hasReserved(cq *kueue.ClusterQueue) bool {
	if cq.Status.ReservingWorkloads != 0 || cq.Status.AdmittedWorkloads != 0 {
		return true
	}
	return slicex.Any(cq.Status.FlavorsReservation, func(i int) bool {
		return slicex.Any(cq.Status.FlavorsReservation[i].Resources, func(j int) bool {
			return !cq.Status.FlavorsReservation[i].Resources[j].Total.IsZero() ||
				!cq.Status.FlavorsReservation[i].Resources[j].Borrowed.IsZero()
		})
	})
}

// buildResourceGroups 汇总各 flavor 的池容量。加速器队列覆盖厂商的 credits 资源，
// 单个 flavor 的标称配额为卡数乘每卡额度。纯 CPU 队列只覆盖 cpu，标称配额为池内总核数。
func buildResourceGroups(rfList *kueue.ResourceFlavorList, acceleratable bool, manufacturer string) []kueue.ResourceGroup {
	covered := []core.ResourceName{core.ResourceCPU}
	if acceleratable {
		covered = []core.ResourceName{nodefeature.GetAcceleratableCreditsResourceName(manufacturer)}
	}

	group := kueue.ResourceGroup{CoveredResources: covered}
	for i := range rfList.Items {
		rf := &rfList.Items[i]
		capacity := parseResourceFlavorCapacity(rf)
		if capacity <= 0 {
			continue
		}

		nominal := *resource.NewQuantity(capacity, resource.DecimalSI)
		if acceleratable {
			nominal = nodefeature.AcceleratorsToCredits(nominal)
		}

		group.Flavors = append(group.Flavors, kueue.FlavorQuotas{
			Name: kueue.ResourceFlavorReference(rf.Name),
			Resources: []kueue.ResourceQuota{
				{
					Name:         covered[0],
					NominalQuota: nominal,
					// 空 cohort 使该队列与其他队列隔离。
					// Kueue 不允许未加入 cohort 的队列设置借入或借出上限，因此两个字段都留空。
				},
			},
		})
	}
	if len(group.Flavors) == 0 {
		return nil
	}
	return []kueue.ResourceGroup{group}
}

// parseResourceFlavorCount 从 flavor 选择标签中读取单节点数量。带拓扑信息的名称以 profile 哈希结尾。
func parseResourceFlavorCount(rf *kueue.ResourceFlavor) int64 {
	prefix := nodefeature.GeneralFeatureLabelPrefix
	if rf.Labels[nodefeature.NodeAcceleratableLabelKey] == "true" {
		prefix = nodefeature.AcceleratableFeatureLabelPrefix
	}
	for key, value := range rf.Spec.NodeLabels {
		if value != "true" || !strings.HasPrefix(key, prefix) {
			continue
		}
		if count, err := strconvx.Atoi[int64](rf.Spec.NodeLabels[key+_ResourceFlavorCountLabelSuffix]); err == nil && count > 0 {
			return count
		}
	}
	return 0
}

// parseNodeFlavorCount 从 flavor 名称解析单节点数量。
// 带拓扑信息的名称可能在数量后追加 -fnv64-<hash>，因此从后往前查找数量段。
// 已有 ResourceFlavor 对象时，队列排序优先使用 parseResourceFlavorCount 读取标签。
func parseNodeFlavorCount(name string) int64 {
	if profileIndex := strings.LastIndex(name, "-"+topologyProfilePrefix); profileIndex >= 0 {
		name = name[:profileIndex]
	}
	segments := strings.Split(name, "-")
	for i := len(segments) - 1; i >= 0; i-- {
		segment := segments[i]
		if len(segment) < 2 {
			continue
		}
		switch segment[len(segment)-1] {
		case 'c', 'd':
			if value, err := strconvx.Atoi[int64](segment[:len(segment)-1]); err == nil {
				return value
			}
		}
	}
	return 0
}

// parseResourceFlavorCapacity 从 flavor 自身特征键下的 .capacity 读取全池容量（节点数 × 单节点数量）。
// 加速器 flavor 取 acceleratable. 键，CPU flavor 取 general. 键。
// 设备 flavor 也可能带一个没有容量的 CPU 键，所以先按加速器标记选定前缀，再忽略缺失或非正的容量。
// 找不到有效容量时返回零。
func parseResourceFlavorCapacity(rf *kueue.ResourceFlavor) int64 {
	prefix := nodefeature.GeneralFeatureLabelPrefix
	if rf.Labels[nodefeature.NodeAcceleratableLabelKey] == "true" {
		prefix = nodefeature.AcceleratableFeatureLabelPrefix
	}
	for k, v := range rf.Labels {
		if v != "true" || !strings.HasPrefix(k, prefix) {
			continue
		}
		if capacity, err := strconvx.Atoi[int64](rf.Labels[k+_ResourceFlavorCapacityLabelSuffix]); err == nil && capacity > 0 {
			return capacity
		}
	}
	return 0
}

// nodeQueueAdmissionCheck 识别队列可能引用的设备检查和联合准入检查。任一检查被激活、停用或删除时，
// 队列都需要重新核对引用。
func nodeQueueAdmissionCheck(ac *kueue.AdmissionCheck) bool {
	return ac.Spec.ControllerName == _NodeDevicesControllerName ||
		ac.Spec.ControllerName == _JointAdmissionControllerName
}

// admissionCheckActive 确认指定 AdmissionCheck 存在、未进入删除流程并且为 Active。
// 引用无效检查会令整个 ClusterQueue 停止准入。
//
// 正在删除的检查即便仍报告 Active，也应移除引用。只有所有 ClusterQueue 都解除引用，
// Kueue 才能移除 resource-in-use finalizer。
func (r *NodeQueueReconciler) admissionCheckActive(ctx context.Context, name string) bool {
	ac := new(kueue.AdmissionCheck)
	err := r.Client.Get(ctx, ctrlcli.ObjectKey{Name: name}, ac, ctrlclix.WithoutQuorum)
	if err != nil || ac.DeletionTimestamp != nil {
		return false
	}

	return kubemeta.IsConditionTrue(ac.Status.Conditions, kueue.AdmissionCheckActive)
}

func (r *NodeQueueReconciler) SetupController(_ context.Context, opts controller.SetupOptions) error {
	r.Client = opts.Manager.GetClient()

	dedupWindow := ctrlhandlerx.NewDedupWindow[ctrlreconcile.Request]()

	return ctrl.NewControllerManagedBy(opts.Manager).
		Named("nodequeue").
		For(
			// 按受管 ClusterQueue 自身的名称协调。写入幂等，重复处理同一对象可以收敛。
			&kueue.ClusterQueue{},
			ctrlbuilder.WithPredicates(
				// 只处理相应的受管 ClusterQueue，后续事件过滤条件同时生效。
				ctrlpredicate.NewPredicateFuncs(func(obj ctrlcli.Object) bool {
					return systemmeta.MatchResource(obj, _ClusterQueueResType)
				}),
				// 创建（含启动首次同步）、spec 的 generation 变化或设置 DeletionTimestamp 时触发协调。
				// 最终删除后已无对象可处理；Kueue 持续写入的计数和条件等纯状态变化也不触发本控制器。
				ctrlpredicate.Funcs{
					DeleteFunc: func(ctrlevent.DeleteEvent) bool { return false },
					UpdateFunc: func(e ctrlevent.UpdateEvent) bool {
						oldCq, newCq := e.ObjectOld.(*kueue.ClusterQueue), e.ObjectNew.(*kueue.ClusterQueue)
						if !oldCq.DeletionTimestamp.Equal(newCq.DeletionTimestamp) {
							return true
						}
						return oldCq.Generation != newCq.Generation
					},
				},
			),
		).
		Watches(
			// flavor 加入或离开池时，为它所供给的每个队列重新计算配额。
			&kueue.ResourceFlavor{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFuncWithWindow(
				3*time.Second,
				dedupWindow,
				r.enqueueNodeQueueWhenResourceFlavorChanged,
			),
			ctrlbuilder.WithPredicates(
				// 相关 ResourceFlavor 的创建、修改和删除都可能改变池内可用的 flavor。
				ctrlpredicate.NewPredicateFuncs(func(obj ctrlcli.Object) bool {
					return systemmeta.MatchResource(obj, _ResourceFlavorResType)
				}),
			),
		).
		Watches(
			// 设备检查或联合准入检查的状态变化后，重新协调受管队列，激活时补上引用，
			// 失效或删除时撤下。
			&kueue.AdmissionCheck{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFuncWithWindow(
				3*time.Second,
				dedupWindow,
				r.enqueueNodeQueuesWhenAdmissionCheckChanged,
			),
			ctrlbuilder.WithPredicates(
				// 监听两种 AdmissionCheck 的创建、更新和删除事件，以 Active 状态决定是否引用。
				ctrlpredicate.NewPredicateFuncs(func(obj ctrlcli.Object) bool {
					return nodeQueueAdmissionCheck(obj.(*kueue.AdmissionCheck))
				}),
			),
		).
		Complete(r)
}

// enqueueNodeQueueWhenResourceFlavorChanged 找出使用该 flavor 的受管队列。flavor 身份最细，
// 始终带有 CPU 键；合并池的区分标签可能更少。先列出受管队列，
// 再检查每个队列要求的选择标签是否是该 flavor 标签的子集，以免漏掉合并池。
func (r *NodeQueueReconciler) enqueueNodeQueueWhenResourceFlavorChanged(
	ctx context.Context, obj ctrlcli.Object,
) []ctrlreconcile.Request {
	logger := ctrllog.FromContext(ctx).
		WithValues("resource flavor", ctrlcli.ObjectKeyFromObject(obj))

	rfSel := nodefeature.PoolFlavorSelector(obj.GetLabels())
	if len(rfSel) == 0 {
		return nil
	}

	// 先在服务端用资源类别过滤出 Operator 管理的队列，再在内存中比较选择标签。
	// 直接带着 flavor 的全部标签查询会漏掉区分条件较少的合并池。
	cqList := new(kueue.ClusterQueueList)
	err := r.Client.List(ctx, cqList,
		systemmeta.GetResourcesLabelSetOfType[ctrlcli.MatchingLabels](_ClusterQueueResType),
		ctrlclix.WithoutQuorum,
		ctrlcli.UnsafeDisableDeepCopy)
	if err != nil {
		logger.Error(err, "list cluster queues for resource flavor")
		return nil
	}

	var reqs []ctrlreconcile.Request
	for i := range cqList.Items {
		cq := &cqList.Items[i]
		cqSel := nodefeature.PoolFlavorSelector(cq.Labels)
		if len(cqSel) == 0 || !mapx.Contain(rfSel, cqSel) {
			continue
		}
		reqs = append(reqs, ctrlreconcile.Request{
			NamespacedName: ctrlcli.ObjectKey{Name: cq.Name},
		})
	}
	if len(reqs) == 0 {
		return nil
	}

	logger.V(2).Info("enqueued node queues from resource flavor", "requests", reqs)
	return reqs
}

// enqueueNodeQueuesWhenAdmissionCheckChanged 在设备或联合准入检查变化时，
// 将受管 ClusterQueue 重新入队，以便按最新可用状态增删引用。
func (r *NodeQueueReconciler) enqueueNodeQueuesWhenAdmissionCheckChanged(
	ctx context.Context, obj ctrlcli.Object,
) []ctrlreconcile.Request {
	logger := ctrllog.FromContext(ctx).
		WithValues("admission check", ctrlcli.ObjectKeyFromObject(obj))

	if !nodeQueueAdmissionCheck(obj.(*kueue.AdmissionCheck)) {
		return nil
	}

	cqList := new(kueue.ClusterQueueList)
	if err := r.Client.List(ctx, cqList, ctrlclix.WithoutQuorum, ctrlcli.UnsafeDisableDeepCopy); err != nil {
		logger.Error(err, "list cluster queues for admission check")
		return nil
	}

	var reqs []ctrlreconcile.Request
	for i := range cqList.Items {
		cq := &cqList.Items[i]
		if !systemmeta.MatchResource(cq, _ClusterQueueResType) || cq.DeletionTimestamp != nil {
			continue
		}
		reqs = append(reqs, ctrlreconcile.Request{
			NamespacedName: ctrlcli.ObjectKey{Name: cq.Name},
		})
	}
	if len(reqs) == 0 {
		return nil
	}

	logger.V(2).Info("enqueued node queues from admission check", "requests", reqs)
	return reqs
}
