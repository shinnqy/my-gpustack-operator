package worker

import (
	"cmp"
	"context"
	"slices"
	"strings"
	"time"

	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
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

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/controller"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/systemmeta"
	"gpustack.ai/gpustack/pkg/systemname"
	"gpustack.ai/gpustack/pkg/utils/ctrlclix"
	"gpustack.ai/gpustack/pkg/utils/ctrlhandlerx"
	"gpustack.ai/gpustack/pkg/utils/json"
	"gpustack.ai/gpustack/pkg/utils/mapx"
	"gpustack.ai/gpustack/pkg/utils/strconvx"
	"gpustack.ai/gpustack/pkg/worker/apistatus"
	"gpustack.ai/gpustack/pkg/worker/kuberequest"
	"gpustack.ai/gpustack/pkg/worker/settings"
)

// InstanceTypeReconciler 维护 worker.gpustack.ai 的 InstanceType 和与它同名的 Kueue ClusterQueue。
//   - 根据规格创建队列、同步调度标签，在创建时设置队列隔离策略。
//   - 监听队列配额和 Conditions，刷新规格的阶段及 CPU 容量视图。若规格仍在而队列被误删，重新创建队列。NodeQueueReconciler 维护资源组、设备准入检查和排空策略；管理员设置的 Inactive 由 syncInactive 映射为 Hold 或 None。
//   - 派生规格由 NodeFlavorReconciler 在同步 flavor 后创建。池暂时没有 flavor 时保留规格，由 NodeQueueReconciler 将队列排空或暂停。
//   - 用 Devices 分配账本和 ClusterQueue 生成加速器的四种资源视图或 CPU 视图。硬件规格由默认值 webhook 在准入时写入快照。InstanceType webhook 写入队列入口标签，Pod webhook 通过入口找到规格，从 Status.Detail.Memory 读取观测到的单卡显存。
//   - 删除规格时先请求删除队列，保留 finalizer，直到 NodeQueueReconciler 排空队列并且队列对象消失。
type InstanceTypeReconciler struct {
	Client    ctrlcli.Client
	APIReader ctrlcli.Reader
}

var _ ctrlreconcile.Reconciler = (*InstanceTypeReconciler)(nil)

// IMPORTANT: 为每个 InstanceType 建同名 ClusterQueue(先 Hold 锁住)
func (r *InstanceTypeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := ctrllog.FromContext(ctx)

	// 读取需要协调的 InstanceType。
	it := new(workercore.InstanceType)
	err := r.Client.Get(ctx, req.NamespacedName, it, ctrlclix.WithoutQuorum)
	if err != nil {
		if !kerrors.IsNotFound(err) {
			logger.Error(err, "fetch instance type")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// -- 处理删除与加 finalizer 锁
	// 已标记删除时，执行 finalizer 对应的清理流程。
	if it.DeletionTimestamp != nil {
		return r.teardownInstanceType(ctx, it)
	}

	// 添加 finalizer，使删除规格前有机会清理队列。
	if !systemmeta.Lock(it) {
		err = r.Client.Update(ctx, it)
		if err != nil {
			logger.Error(err, "lock instance type")
			return ctrl.Result{}, err
		}
	}

	// 确保同名 ClusterQueue 和调度标签已就绪，然后用 Devices 账本和队列刷新状态。
	// 队列配额由 NodeQueueReconciler 填写，硬件规格已由 webhook 在准入时写入快照。
	// 只有状态发生变化才提交更新。
	cq, err := r.ensureClusterQueue(ctx, it)
	if err != nil {
		logger.Error(err, "ensure cluster queue")
		return ctrl.Result{}, err
	}
	if cq.DeletionTimestamp != nil {
		// 规格仍存在，但队列正在删除。等待 Kueue 完成删除后用同名重建，
		// 跳过本轮基于旧队列的状态更新。
		logger.V(2).Info("cluster queue terminating; requeue in 15s")
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	// 同步管理员的 Inactive 设置和队列的 StopPolicy。若本轮写入了队列或规格，
	// 就等下次协调读取稳定结果后再刷新状态。
	changed, err := r.syncInactive(ctx, it, cq)
	if err != nil {
		// 写入冲突可能来自 InstanceType 的状态更新，而监听器会过滤这类事件。主动重新入队，
		// 避免一直等不到下一次同步。
		return objectWriteResult(logger, err, "sync instance type inactive with cluster queue stop policy",
			_requeueAfterConflict)
	}
	if changed {
		return ctrl.Result{}, nil
	}

	overcommit := settings.InstanceGeneralResourcesOvercommit.ShouldValueBool(ctx)
	desiredStatus := r.computeStatus(ctx, it, cq, overcommit)
	if !kubemeta.DeepEqual(desiredStatus, it.Status) {
		it.Status = desiredStatus
		if err = r.Client.Status().Update(ctx, it); err != nil {
			// 只改状态或元数据时，过滤器可能不触发新的协调。冲突后主动重新入队。
			return objectWriteResult(logger, err, "update instance type status", _requeueAfterConflict)
		}
		logger.V(2).Info("refreshed instance type status")
	}
	return ctrl.Result{}, nil
}

// syncInactive 按下表同步 Spec.Inactive 和全局队列的 StopPolicy。
//
//	队列策略（管理方）                  Spec.Inactive   动作
//	None                                false           已同步
//	None                                true            改为 Hold
//	Hold（管理员）                      true            已同步
//	Hold（管理员，已有资源组）          false           改为 None
//	Hold（管理员，尚无资源组）          false           标记由 NodeQueue 接管
//	HoldAndDrain（NodeQueue）           true            已同步
//	HoldAndDrain（NodeQueue）           false           回填 Spec.Inactive=true
//	带接管标记的 Hold（NodeQueue）      任意            保持当前策略
//	迁移中的任意策略（NodeQueue）       任意            改写迁移结束时要恢复的策略
//
// 优先把 Inactive 设置映射到 Hold 或 None，HoldAndDrain 由 NodeQueueReconciler 维护。
// 反向同步只会将已停止的队列回填为 Inactive=true。排空后即使节点恢复，
// 管理员仍须清除此标记才会重新开放。每轮最多写入一次。迁移期间只修改要恢复的策略，暂停反向同步。
// 空队列若解除 Hold 会在没有资源组时接纳任意 Workload，所以先标记由 NodeQueue 接管，
// 等首批资源组写入并清除标记后，再按管理员意图决定是否开放。返回值表示此次是否写入。
func (r *InstanceTypeReconciler) syncInactive(
	ctx context.Context, it *workercore.InstanceType, cq *kueue.ClusterQueue,
) (bool, error) {
	// 拓扑迁移期间由 NodeQueue 管理实时 StopPolicy。临时 HoldAndDrain 如果写回 Inactive，
	// 迁移结束后队列会被再次暂停。因此此时将管理员意图写入迁移结束时要恢复的策略，
	// 使等待新 flavor 的队列恢复时也能遵从最新设置。
	if cq.Annotations[_TASQueueMigrationPhaseAnnotation] != "" {
		saved := cq.Annotations[_TASQueueMigrationStopPolicyAnnotation]
		want := saved
		switch {
		case it.Spec.Inactive && (saved == string(kueue.None) || saved == _TASQueueMigrationStopPolicyUnset):
			want = string(kueue.Hold)
		case !it.Spec.Inactive && saved == string(kueue.Hold):
			want = string(kueue.None)
		}
		if want == saved {
			return false, nil
		}
		cq.Annotations[_TASQueueMigrationStopPolicyAnnotation] = want
		return true, r.Client.Update(ctx, cq)
	}
	if cq.Annotations[_TASQueueEmptyPlanHoldAnnotation] != "" {
		return false, nil
	}

	switch ptr.Deref(cq.Spec.StopPolicy, kueue.None) {
	case kueue.None:
		if it.Spec.Inactive {
			cq.Spec.StopPolicy = ptr.To(kueue.Hold)
			return true, r.Client.Update(ctx, cq)
		}
	case kueue.Hold:
		if !it.Spec.Inactive {
			if len(cq.Spec.ResourceGroups) == 0 {
				if cq.Annotations == nil {
					cq.Annotations = make(map[string]string)
				}
				cq.Annotations[_TASQueueEmptyPlanHoldAnnotation] = "true"
			} else {
				cq.Spec.StopPolicy = ptr.To(kueue.None)
			}
			return true, r.Client.Update(ctx, cq)
		}
	}

	if ptr.Deref(cq.Spec.StopPolicy, kueue.None) != kueue.None && !it.Spec.Inactive {
		it.Spec.Inactive = true
		return true, r.Client.Update(ctx, it)
	}

	return false, nil
}

// ensureClusterQueue 保证规格有对应的 ClusterQueue。首次创建或误删后重建时写入队列隔离策略，
// 之后只按规格标识同步调度标签。
//
//   - Pod webhook 按入口名从 InstanceType 的 Status.Detail.Memory 读取单卡显存，因此队列无需显存记录。
//   - 配额、设备准入检查及排空时使用的 StopPolicy 由 NodeQueueReconciler 维护。
//   - 规格仍在而队列正在删除时，先等队列消失，再由下一轮协调重建。
func (r *InstanceTypeReconciler) ensureClusterQueue(
	ctx context.Context, it *workercore.InstanceType,
) (*kueue.ClusterQueue, error) {
	logger := ctrllog.FromContext(ctx)

	cq := new(kueue.ClusterQueue)
	err := r.Client.Get(ctx, ctrlcli.ObjectKey{Name: it.Name}, cq, ctrlclix.WithoutQuorum)
	if err != nil {
		if !kerrors.IsNotFound(err) {
			return nil, err
		}
		return r.createClusterQueue(ctx, it)
	}

	// 同名队列正在删除，暂时无法重建。把对象交给调用方安排重试，等旧队列消失后再创建。
	if cq.DeletionTimestamp != nil {
		return cq, nil
	}

	// 现有队列只同步元数据。创建时已写入隔离策略，队列配额由 NodeQueueReconciler 维护。
	eLabels := instanceTypeScheduleLabels(ctx, it)
	changed := !systemmeta.NoteResource(cq, _ClusterQueueResType, nil)
	// 分组或加速器标记变化时，特征键的名称也会变化；单纯合并标签会留下旧键。
	// flavor 和设备选择器要求所有特征键同时匹配，因此须移除旧键，以免队列找不到节点。
	for k := range cq.Labels {
		if _, want := eLabels[k]; want {
			continue
		}
		if strings.HasPrefix(k, nodefeature.GeneralFeatureLabelPrefix) ||
			strings.HasPrefix(k, nodefeature.AcceleratableFeatureLabelPrefix) {
			delete(cq.Labels, k)
			changed = true
		}
	}
	if !mapx.Contain(cq.Labels, eLabels) {
		if cq.Labels == nil {
			cq.Labels = make(map[string]string)
		}
		for k, v := range eLabels {
			cq.Labels[k] = v
		}
		changed = true
	}
	if changed {
		if err = r.Client.Update(ctx, cq); err != nil {
			logger.Error(err, "align cluster queue")
			return nil, err
		}
		logger.V(2).Info("aligned cluster queue")
	}
	return cq, nil
}

// createClusterQueue 按规格生成调度标签，设置由 NodeQueue 管理的初始 Hold。队列对所有命名空间开放，
// 使用空 cohort，禁用跨队列借用与回收，只允许抢占本队列内低优先级负载。新队列还没有资源组，
// 若立即开放会接纳任意 Workload。NodeQueue 填入资源组后移除暂停标记，
// syncInactive 再决定是否解除 Hold。
//
// NodeQueueReconciler 随后填充资源组。
// 只有集群开启节点派生且设备 AdmissionCheck 已为 Active 时才引用该检查。
// 默认模式下管理员自建规格的队列也会挂上检查，完全由管理员定义规格时则跳过。
func (r *InstanceTypeReconciler) createClusterQueue(
	ctx context.Context, it *workercore.InstanceType,
) (*kueue.ClusterQueue, error) {
	logger := ctrllog.FromContext(ctx)

	cq := &kueue.ClusterQueue{
		ObjectMeta: meta.ObjectMeta{
			Name:        it.Name,
			Labels:      instanceTypeScheduleLabels(ctx, it),
			Annotations: map[string]string{_TASQueueEmptyPlanHoldAnnotation: "true"},
		},
		Spec: kueue.ClusterQueueSpec{
			NamespaceSelector: &meta.LabelSelector{},
			StopPolicy:        ptr.To(kueue.Hold),
			FlavorFungibility: &kueue.FlavorFungibility{
				WhenCanBorrow:  kueue.TryNextFlavor,
				WhenCanPreempt: kueue.MayStopSearch,
			},
			Preemption: &kueue.ClusterQueuePreemption{
				ReclaimWithinCohort: kueue.PreemptionPolicyNever,
				BorrowWithinCohort:  &kueue.BorrowWithinCohort{Policy: kueue.BorrowWithinCohortPolicyNever},
				WithinClusterQueue:  kueue.PreemptionPolicyLowerPriority,
			},
		},
	}
	systemmeta.NoteResource(cq, _ClusterQueueResType, nil)
	if err := r.Client.Create(ctx, cq); err != nil {
		if kerrors.IsAlreadyExists(err) {
			return cq, r.Client.Get(ctx, ctrlcli.ObjectKey{Name: it.Name}, cq, ctrlclix.WithoutQuorum)
		}
		return nil, err
	}
	logger.V(2).Info("created cluster queue")
	return cq, nil
}

// teardownInstanceType 请求删除底层 ClusterQueue，并保留 InstanceType 的 finalizer，
// 直到队列实际消失。监听到删除的 NodeQueueReconciler 设置 HoldAndDrain，
// 由 Kueue 驱逐工作负载并移除队列。
func (r *InstanceTypeReconciler) teardownInstanceType(
	ctx context.Context, it *workercore.InstanceType,
) (ctrl.Result, error) {
	logger := ctrllog.FromContext(ctx)

	if !systemmeta.IsLocked(it) {
		return ctrl.Result{}, nil
	}

	cq := new(kueue.ClusterQueue)
	err := r.Client.Get(ctx, ctrlcli.ObjectKey{Name: it.Name}, cq, ctrlclix.WithoutQuorum)
	switch {
	case err == nil:
		// 先把清理状态反映到 InstanceType。这样在队列消失前，
		// 使用方和判断是否停机的 Instance 控制器就能看到 Inactive。
		if it.Status.Phase != InstanceTypePhaseInactive {
			it.Status = workercore.InstanceTypeStatus{
				Phase:        InstanceTypePhaseInactive,
				PhaseMessage: "instance type is terminating",
				Entrance:     nodefeature.FormatLocalQueueName(cq.Name),
			}
			if err = r.Client.Status().Update(ctx, it); err != nil {
				return objectWriteResult(logger, err, "mark instance type inactive while terminating", _requeueAfterConflict)
			}
		}
		// 仅发起一次删除请求。NodeQueueReconciler 负责排空，等 Kueue 移除队列后再解除 finalizer。
		if cq.DeletionTimestamp == nil {
			if err = r.Client.Delete(ctx, cq); err != nil && !kerrors.IsNotFound(err) {
				logger.Error(err, "delete cluster queue")
				return ctrl.Result{}, err
			}
			logger.V(2).Info("requested cluster queue deletion; the node queue drains it")
		}
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	case !kerrors.IsNotFound(err):
		logger.Error(err, "fetch cluster queue")
		return ctrl.Result{}, err
	}

	// 队列已消失，可以解除规格的 finalizer。
	systemmeta.Unlock(it)
	if err = r.Client.Update(ctx, it); err != nil {
		return ctrl.Result{}, ctrlcli.IgnoreNotFound(err)
	}
	logger.V(2).Info("released drained instance type")
	return ctrl.Result{}, nil
}

// computeStatus 计算 InstanceType 的状态。加速器的四种资源视图从池内 Devices 的单卡账本汇总，
// CPU 视图来自 ClusterQueue；排空中各资源视图保持零值。阶段反映队列条件。协调器每次替换整份状态，
// 因此在此一并计算硬件 Detail，避免另一次 status 写入被覆盖。
func (r *InstanceTypeReconciler) computeStatus(
	ctx context.Context, it *workercore.InstanceType, cq *kueue.ClusterQueue, withOvercommit bool,
) workercore.InstanceTypeStatus {
	acceleratable := it.Spec.Acceleratable
	draining := ptr.Deref(cq.Spec.StopPolicy, kueue.None) == kueue.HoldAndDrain

	var st workercore.InstanceTypeStatus
	var devices []workercore.Devices
	if acceleratable {
		devices = r.listFlavorPoolDevices(ctx, cq)
	}
	if !draining {
		if acceleratable {
			st.Accelerator, st.AcceleratorShared, st.AcceleratorSliced, st.AcceleratorPartitioned = getAcceleratorResources(devices, it.Spec.AcceleratorGroup)
		} else {
			st.CPU = getCPUResource(cq, withOvercommit)
		}
	}
	st.Detail = r.computeDetail(ctx, it, devices)
	publishPartitionProfileNames(&st)
	st.Entrance = nodefeature.FormatLocalQueueName(cq.Name)
	st.Phase, st.PhaseMessage = apistatus.GetSummaryOfClusterQueue(cq)
	return st
}

// publishPartitionProfileNames 统一状态中分区规格的对外名称。账本保存厂商的原始拼写，
// 用户选择规格时看到的名称必须与最终申请的资源键一致，所以在状态发布前统一转换。
//
// 先计算 Detail 才能确定厂商。尚无匹配 flavor 时先保留原名，等待后续协调。
// 生成资源键时能识别两种拼写。转换后将两份按规格汇总的账本重新按名字排序，以便展示及比较状态。
func publishPartitionProfileNames(st *workercore.InstanceTypeStatus) {
	manufacturer := st.Detail.Manufacturer
	if manufacturer == "" {
		return
	}

	profiles := st.Detail.SlicedDetail.Physical.Profiles
	for i := range profiles {
		profiles[i].Name = nodefeature.PublishPartitionedProfileName(manufacturer, profiles[i].Name)
	}
	for _, counts := range [][]workercore.AcceleratorProfileCount{
		st.AcceleratorPartitioned.AllocatedProfiles,
		st.AcceleratorPartitioned.RemainingProfiles,
	} {
		for i := range counts {
			counts[i].Name = nodefeature.PublishPartitionedProfileName(manufacturer, counts[i].Name)
		}
		slices.SortFunc(counts, func(a, b workercore.AcceleratorProfileCount) int {
			return cmp.Compare(a.Name, b.Name)
		})
	}
}

// isGenericCollapsedPool 检查纯 CPU 池是否已关闭 CPU 厂商感知。这种池汇合多种 CPU，
// 单个 ResourceFlavor 无法代表全池，因此只发布最少的硬件信息。
// NodeFlavorReconciler 派生时将显示名设为 CPU-only。
func isGenericCollapsedPool(ctx context.Context, it *workercore.InstanceType) bool {
	return !it.Spec.Acceleratable &&
		!settings.InstanceTypeAwareCPUManufacturer.ShouldValueBool(ctx)
}

// computeDetail 汇总观测到的硬件信息。厂商、产品、系列，
// 以及加速器的单卡显存、核心数和 CPU 信息来自匹配的 ResourceFlavor；
// 切分能力来自池内 Devices 的 AcceleratorSlicedDetail。flavor 由规格推导的标签查找，
// 队列尚未建立时也可计算。合并的通用 CPU 池没有代表性 flavor，或 flavor 尚未同步时，Detail 为空，
// 后续协调时再刷新。
func (r *InstanceTypeReconciler) computeDetail(
	ctx context.Context, it *workercore.InstanceType, devices []workercore.Devices,
) workercore.InstanceTypeDetail {
	if isGenericCollapsedPool(ctx, it) {
		return workercore.InstanceTypeDetail{}
	}

	lbs := systemmeta.GetResourcesLabelSetOfType[ctrlcli.MatchingLabels](_ResourceFlavorResType)
	for k, v := range instanceTypeScheduleLabels(ctx, it) {
		lbs[k] = v
	}
	rfList := new(kueue.ResourceFlavorList)
	err := r.Client.List(ctx, rfList, lbs,
		ctrlclix.WithoutQuorum, ctrlcli.UnsafeDisableDeepCopy, ctrlcli.Limit(1))
	if err != nil {
		ctrllog.FromContext(ctx).Error(err, "list resource flavor for instance type detail")
		return workercore.InstanceTypeDetail{}
	}
	if len(rfList.Items) == 0 {
		ctrllog.FromContext(ctx).V(3).
			Info("no matching resource flavor for instance type detail; refresh on later reconcile")
		return workercore.InstanceTypeDetail{}
	}
	_, notes := systemmeta.DescribeResource(&rfList.Items[0])

	detail := workercore.InstanceTypeDetail{
		Manufacturer: notes["manufacturer"],
		Product:      notes["product"],
		Family:       notes["family"],
	}
	if it.Spec.Acceleratable {
		detail.Memory = notes["memory"]
		detail.Cores = notes["cores"]
		detail.SlicedDetail = poolAcceleratorSlicedDetail(devices, it.Spec.AcceleratorGroup)
		// 两个运行时版本字段共用同一份排序结果，单值字段直接取第一项，避免两次计算得出不同结果。
		detail.RuntimeVersions = poolAcceleratorRuntimeVersions(devices, it.Spec.AcceleratorGroup)
		if len(detail.RuntimeVersions) > 0 {
			detail.RuntimeVersion = detail.RuntimeVersions[0]
		}
		// CPU flavor 始终携带 cpuDetail。加速器 flavor 只有启用 CPU 厂商感知时才记录该字段，
		// 两种情况都与写入方保持一致。
		if settings.InstanceTypeAwareCPUManufacturer.ShouldValueBool(ctx) {
			foldDetailCPU(&detail, notes["cpuDetail"], true)
		}
	} else {
		foldDetailCPU(&detail, notes["cpuDetail"], false)
	}
	return detail
}

// foldDetailCPU 按 NodeFlavorReconciler 写入时的结构解析 cpuDetail。
// 加速器 flavor 使用 InstanceTypeAcceleratorCPU，
// 将 CPU 厂商、产品、系列及其详情写入加速器的 CPU 字段；纯 CPU flavor 使用 InstanceTypeCPU，
// 写入顶层 CPU。记录为空或解析失败时保留原值。
func foldDetailCPU(detail *workercore.InstanceTypeDetail, raw string, acceleratable bool) {
	if raw == "" {
		return
	}
	if acceleratable {
		var d workercore.InstanceTypeAcceleratorCPU
		if err := json.Unmarshal([]byte(raw), &d); err == nil {
			detail.CPU = d
		}
	} else {
		var d workercore.InstanceTypeCPU
		if err := json.Unmarshal([]byte(raw), &d); err == nil {
			detail.InstanceTypeCPU = d
		}
	}
}

// poolAcceleratorSlicedDetail 从各节点的 Devices 中收集本池加速器组的卡，
// 再按探测器“单卡汇总到设备组”的规则汇总全池切分能力，使结果与资源视图的池容量一致。
// 按“厂商-组 ID”完整匹配，因为 ConstructGroupID 会去掉厂商前缀，不同厂商可能有相同的组 ID。
// 没有匹配组时返回零值详情。
func poolAcceleratorSlicedDetail(
	devices []workercore.Devices, acceleratorKey string,
) workercore.AcceleratorSlicedDetail {
	var cards []workercore.Accelerator
	for i := range devices {
		for j := range devices[i].Spec.Groups {
			g := &devices[i].Spec.Groups[j]
			if acceleratorGroupMatches(g.Manufacturer, g.ID, acceleratorKey) {
				cards = append(cards, g.Accelerators...)
			}
		}
	}
	return device.AggregateAcceleratorSlicedDetail(cards)
}

// poolAcceleratorRuntimeVersions 收集本池各节点上报的不同加速器运行时版本，按数值升序返回。
// 没有节点上报时返回 nil。
//
// 与汇总切分能力时一样，遍历每个节点的 Devices 并按“厂商-组 ID”匹配，此处读取组内的 RuntimeVersion。
//
// 调用方直接取有序列表第一项发布为 runtimeVersion，两处显示始终引用同一份数据。
//
// 升序排列使第一项为最低版本。旧运行时镜像可在较新的驱动上运行，
// 而镜像在 Pod 准入选定节点之前就已确定，故选择最低版本才能覆盖整个池。
//
// nil 表示没有观测值；如果所有节点一致上报同一版本，列表应含一项。
// 匹配到设备组但版本为空时跳过该条目。
//
// 以版本的数值去重，例如 9 和 9.0 视为同一版本。调用方用列表长度判断节点间是否存在版本差异。
func poolAcceleratorRuntimeVersions(devices []workercore.Devices, acceleratorKey string) []string {
	var versions []string
	for i := range devices {
		for j := range devices[i].Spec.Groups {
			g := &devices[i].Spec.Groups[j]
			if !acceleratorGroupMatches(g.Manufacturer, g.ID, acceleratorKey) || g.RuntimeVersion == "" {
				continue
			}
			// 跳过无法解析的版本字符串。device.NormalizeVersion 可能保留无法解析的片段；
			// 若把 N/A 当成 0.0，它会误成全池最低版本，影响每个副本选用的镜像。
			if _, _, ok := splitRuntimeVersion(g.RuntimeVersion); !ok {
				continue
			}
			versions = append(versions, g.RuntimeVersion)
		}
	}
	slices.SortFunc(versions, compareRuntimeVersions)

	return slices.CompactFunc(versions, func(a, b string) bool {
		return compareRuntimeVersions(a, b) == 0
	})
}

// compareRuntimeVersions 先比较主版本，再比较次版本，均按数值排序。
//
// 字典序能排对 12.8 和 12.9，却会把 12.9 排在 9.0 之前，导致跨主版本的池选错最低版本。
//
// 探测器通过 device.NormalizeVersion 将版本规范为“主版本.次版本”，只有主版本时可保留原值。
// 比如 9 的次版本视为 0。无法解析的主版本在收集时已经剔除。
func compareRuntimeVersions(a, b string) int {
	aMajor, aMinor, _ := splitRuntimeVersion(a)
	bMajor, bMinor, _ := splitRuntimeVersion(b)
	if c := cmp.Compare(aMajor, bMajor); c != 0 {
		return c
	}

	return cmp.Compare(aMinor, bMinor)
}

// splitRuntimeVersion 将版本拆成主、次两个数值。省略次版本的 9 等同于 9.0，
// 已提供却无法解析的片段会令 ok 为 false。排除 N/A 或 12.x 可防止伪造的数值参与最低版本选择及去重。
func splitRuntimeVersion(v string) (major, minor int, ok bool) {
	majorStr, minorStr, hasMinor := strings.Cut(v, ".")
	major, err := strconvx.Atoi[int](majorStr)
	if err != nil {
		return 0, 0, false
	}
	if !hasMinor {
		return major, 0, true
	}
	minor, err = strconvx.Atoi[int](minorStr)
	if err != nil {
		return 0, 0, false
	}

	return major, minor, true
}

// instanceTypeScheduleLabels 按 InstanceType 的 spec 和本轮读取的 CPU 厂商感知设置生成队列标签，
// 包括加速器标记、适用的加速器键或 CPU 键，以及 kubernetes.io/os 和 kubernetes.io/arch。
// 它们用于反查 ResourceFlavor 和 Devices，管理员只填写 spec 也能得到正确队列。
func instanceTypeScheduleLabels(ctx context.Context, it *workercore.InstanceType) map[string]string {
	cpuAware := settings.InstanceTypeAwareCPUManufacturer.ShouldValueBool(ctx)
	return nodefeature.PoolScheduleLabels(
		it.Spec.Acceleratable, cpuAware,
		it.Spec.GeneralGroup, it.Spec.AcceleratorGroup,
		it.Spec.OS, it.Spec.Arch,
	)
}

// listFlavorPoolDevices 根据队列的调度标签和 gpustack.ai/managed=true，
// 从缓存读取池内节点的 Devices 账本。缺少有效选择器时返回空结果，避免误选所有设备。
func (r *InstanceTypeReconciler) listFlavorPoolDevices(ctx context.Context, cq *kueue.ClusterQueue) []workercore.Devices {
	sel := poolDevicesSelector(cq.Labels)
	if len(sel) == 0 {
		return nil
	}
	list := new(workercore.DevicesList)
	// 后续资源和切分能力的汇总仅读取 Devices，因此可直接使用 informer 缓存中的对象，无需深拷贝。
	if err := r.Client.List(ctx, list, ctrlcli.MatchingLabels(sel),
		ctrlclix.WithoutQuorum, ctrlcli.UnsafeDisableDeepCopy); err != nil {
		return nil
	}
	return list.Items
}

// poolDevicesSelector 从队列标签构造反查设备的选择器，包含特征键、系统、架构和管理标记。
// 缺少能标识池的标签时返回 nil，避免匹配全部对象。
func poolDevicesSelector(cqLabels map[string]string) map[string]string {
	sel := make(map[string]string, 4)
	hasFeatureKey := false
	for k, v := range cqLabels {
		switch {
		case k == core.LabelOSStable, k == core.LabelArchStable:
			sel[k] = v
		case v == "true" &&
			(strings.HasPrefix(k, nodefeature.GeneralFeatureLabelPrefix) ||
				strings.HasPrefix(k, nodefeature.AcceleratableFeatureLabelPrefix)):
			sel[k] = v
			hasFeatureKey = true
		}
	}
	if !hasFeatureKey {
		return nil
	}
	sel[systemname.ManagedLabelKey] = "true"
	return sel
}

// getAcceleratorResources 将同一池的单卡账本汇总为 InstanceType 的四种资源视图。
// 每个 Devices 代表一个节点，每张卡从 ResourceMaxUnits（D）个单位开始扣减。Mode 表示当前分配方式。
//
// 卡按探测到的硬件分区能力归入互斥的两类。空闲的分区卡在标量账本中看起来也有整卡余额，
// 必须根据能力判断。各节点按以下规则计算。
//   - 独占：每张完全空闲的未分区卡（Remaining >= D）计一张。
//   - 共享：空闲或正在共享的未分区卡按 Remaining/(D/SharedResourceMaxSize) 计算份数。
//   - 软切分：具备逻辑切分能力且空闲或正在软切分的卡，按 Remaining/(D/100) 计算显存百分比。
//   - 硬切分：按已分区的卡还能接纳的分区实例数计量。
//
// 只汇总与本池匹配的设备组。同一节点可同时有 H100 和 A100，如果混算两个组，
// 一个池会公布另一个池的卡或分区规格名，随后 Instance webhook 会因该池的目录里找不到规格而拒绝请求。
//
// 三种非硬切分视图从分配账本读取卡的当前占用。硬分区视图先从能力清单枚举卡，再关联账本，
// 方式与节点的 partitionInstancesByProfile 相同。探测到卡但分配行还未重建，或行内尚无分区数据时，
// 使用静态能力上限。
//
// Capacity 表示按该模式换算的池总量，分别为卡数、卡数×10、卡数×100 或硬分区实例上限。
// Remaining 汇总各节点余量。
// OnceMaxRequest 表示一次最多能申请的数量：独占和共享取单节点可用卡数的最大值，
// 因为共享的一次请求在每张不同的卡上各取一份；软切分取单卡最大空闲显存百分比；
// 硬切分请求限单卡单实例，只要还有可放置的实例即为 1。
func getAcceleratorResources(devices []workercore.Devices, acceleratorKey string) (
	exclusive, shared, sliced workercore.InstanceTypeResource,
	partitioned workercore.InstanceTypePartitionedResource,
) {
	const (
		d         = int64(nodefeature.ResourceMaxUnits)
		sharedMax = int64(nodefeature.SharedResourceMaxSize) // 每张卡可分出的共享份数。
		slicedMax = int64(100)                               // 每张卡可计量的显存百分比单位数。
	)
	sharedUnit, slicedUnit := d/sharedMax, d/slicedMax

	var (
		capExcl, remExcl, ormExcl       int64
		capShared, remShared, ormShared int64
		capSliced, remSliced, ormSliced int64
	)
	part := newPoolPartitionView()
	for i := range devices {
		dev := &devices[i]
		caps := acceleratorCapabilities(dev)
		part.addNode(dev, acceleratorKey)

		var nodeCards, nodeLogicalCards, nodeExcl, nodeShared, nodeSharedCards, nodeSliced int64
		for j := range dev.Status.Groups {
			g := &dev.Status.Groups[j]
			if !acceleratorGroupMatches(g.Manufacturer, g.ID, acceleratorKey) {
				continue
			}
			for k := range g.Accelerators {
				a := &g.Accelerators[k]
				st := caps[acceleratorCapabilityKey(g.Manufacturer, g.ID, a.ID)]

				// 已在上方按硬分区能力汇总该卡。
				if device.IsPartitioned(st) {
					continue
				}

				nodeCards++
				rem := int64(a.Remaining)
				free := rem >= d
				if free {
					nodeExcl++
				}
				if free || a.Mode == workercore.DeviceAllocationModeShared {
					nodeShared += rem / sharedUnit
					if rem >= sharedUnit {
						nodeSharedCards++
					}
				}
				// 逻辑切分视图只计真正支持逻辑切分的卡。两种切分能力都没有的卡只能整卡使用。
				// 设备插件、节点容量和 AdmissionCheck 使用同一判断，确保规格公布的切片实际可分配。
				if !device.IsLogicallySliceable(st) {
					continue
				}
				nodeLogicalCards++
				// 单卡可建的切片数已用尽时，即使还有空闲显存，也不能再计入剩余量或单次最大请求量。
				if (free || a.Mode == workercore.DeviceAllocationModeSliced) &&
					device.LogicalSlotsFree(st, a.AllocatedSlices) > 0 {
					cardSliced := rem / slicedUnit
					nodeSliced += cardSliced
					// 一次软切分请求使用单卡，显存不能跨卡相加。取最空闲卡的剩余百分比，最大为 100；
					// 独占和共享请求则可使用同一节点上的多张卡。
					ormSliced = max(ormSliced, cardSliced)
				}
			}
		}
		capExcl += nodeCards
		capShared += nodeCards * sharedMax
		capSliced += nodeLogicalCards * slicedMax
		remExcl += nodeExcl
		remShared += nodeShared
		remSliced += nodeSliced
		ormExcl = max(ormExcl, nodeExcl)
		ormShared = max(ormShared, nodeSharedCards)
		// 软切分的 ormSliced 已在上方逐卡统计。硬切分的单次最大请求量由 poolPartitionView 计算。
	}

	mk := func(orm, rem, total int64) workercore.InstanceTypeResource {
		return workercore.InstanceTypeResource{
			OnceMaxRequest: *resource.NewQuantity(orm, resource.DecimalSI),
			Remaining:      *resource.NewQuantity(rem, resource.DecimalSI),
			Capacity:       *resource.NewQuantity(total, resource.DecimalSI),
		}
	}
	return mk(ormExcl, remExcl, capExcl),
		mk(ormShared, remShared, capShared),
		mk(ormSliced, remSliced, capSliced),
		workercore.InstanceTypePartitionedResource{
			InstanceTypeResource: mk(part.onceMaxRequest, part.remaining, part.capacity),
			AllocatedProfiles:    device.ProfileCountSlice(part.allocatedProfiles),
			RemainingProfiles:    device.ProfileCountSlice(part.remainingProfiles),
		}
}

// poolPartitionView 逐卡累计硬分区的总量、余量、单次最大请求量和按规格细分的账本，
// 在池级沿用节点 partitionInstancesByProfile 的统计方式。
type poolPartitionView struct {
	capacity       int64
	remaining      int64
	onceMaxRequest int64

	allocatedProfiles map[string]int32
	remainingProfiles map[string]int32
}

func newPoolPartitionView() *poolPartitionView {
	return &poolPartitionView{
		allocatedProfiles: make(map[string]int32),
		remainingProfiles: make(map[string]int32),
	}
}

// addNode 从单节点的能力清单枚举本池的分区卡，再关联各卡账本。找到第一张分区卡后才创建账本索引，
// 全部使用逻辑切分的池可以省去这次分配。
func (in *poolPartitionView) addNode(dev *workercore.Devices, acceleratorKey string) {
	var ledger map[string]*workercore.AcceleratorAllocation
	for i := range dev.Spec.Groups {
		g := &dev.Spec.Groups[i]
		if !acceleratorGroupMatches(g.Manufacturer, g.ID, acceleratorKey) {
			continue
		}
		for j := range g.Accelerators {
			st := g.Accelerators[j].Status
			if !device.IsPartitioned(st) {
				continue
			}
			if ledger == nil {
				ledger = acceleratorLedger(dev, acceleratorKey)
			}
			in.addCard(st, ledger[g.Accelerators[j].ID])
		}
	}
}

// addCard 汇总单张分区卡，将能力记录 st 和账本行 a 关联起来。节点状态还未发布该行时 a 为 nil。
func (in *poolPartitionView) addCard(st workercore.AcceleratorStatus, a *workercore.AcceleratorAllocation) {
	in.capacity += int64(st.PhysicalSliced.Count)
	cardRemaining := remainingPartitionInstances(st, a)
	in.remaining += cardRemaining
	// 两个准入入口都要求一次只在一张卡上申请一个分区实例。只要有卡能接纳实例，就公布单次上限 1；
	// 全部已满则为 0。直接公布最空闲卡的实例总数会允许用户填写准入必定拒绝的数量。
	if cardRemaining > 0 {
		in.onceMaxRequest = 1
	}
	in.addProfiles(st, a)
}

// addProfiles 将单卡各分区规格的已分配和剩余实例数累加到池中，
// 与节点的 partitionInstancesByProfile 计量方式一致。
//
// 所有支持的规格都保留剩余数，即使为零。某种规格的实例占用了其他规格的布局后，
// 读者仍能区分“该规格已满”和“设备不提供该规格”，分配和释放也不必反复增删规格键。
// 已分配表中缺项即可表示未占用。
//
// 单卡账本尚未就绪时，暂用每种规格的静态上限。已占用的卡可能暂时被高估，账本发布后会纠正；
// 直接公布零则会让正常节点上的卡看起来已满。
func (in *poolPartitionView) addProfiles(st workercore.AcceleratorStatus, a *workercore.AcceleratorAllocation) {
	ready := device.PartitionLedgerReady(a)
	for i := range st.PhysicalSliced.Profiles {
		p := &st.PhysicalSliced.Profiles[i]
		if _, seen := in.remainingProfiles[p.Name]; !seen {
			in.remainingProfiles[p.Name] = 0
		}
		if !ready {
			in.remainingProfiles[p.Name] += p.Count
		}
	}
	if !ready {
		return
	}
	for i := range a.AllocatedProfiles {
		in.allocatedProfiles[a.AllocatedProfiles[i].Name] += a.AllocatedProfiles[i].Count
	}
	for i := range a.RemainingProfiles {
		in.remainingProfiles[a.RemainingProfiles[i].Name] += a.RemainingProfiles[i].Count
	}
}

// acceleratorCapabilities 按设备组及卡 ID 索引节点上报的单卡能力。
// 能力位于 spec.groups[].accelerators[].status，分配账本位于 status；要知道某张卡能提供什么资源，
// 须关联两处数据。
func acceleratorCapabilities(dev *workercore.Devices) map[string]workercore.AcceleratorStatus {
	caps := make(map[string]workercore.AcceleratorStatus)
	for i := range dev.Spec.Groups {
		g := &dev.Spec.Groups[i]
		for j := range g.Accelerators {
			a := &g.Accelerators[j]
			caps[acceleratorCapabilityKey(g.Manufacturer, g.ID, a.ID)] = a.Status
		}
	}
	return caps
}

// acceleratorLedger 将单一设备组的分配记录按卡 ID 建索引。卡 ID 在组内唯一，
// 调用方可以从能力侧枚举卡，并识别尚无分配记录的卡。
func acceleratorLedger(dev *workercore.Devices, acceleratorKey string) map[string]*workercore.AcceleratorAllocation {
	ledger := make(map[string]*workercore.AcceleratorAllocation)
	for i := range dev.Status.Groups {
		g := &dev.Status.Groups[i]
		if !acceleratorGroupMatches(g.Manufacturer, g.ID, acceleratorKey) {
			continue
		}
		for j := range g.Accelerators {
			a := &g.Accelerators[j]
			ledger[a.ID] = a
		}
	}
	return ledger
}

// acceleratorGroupMatches 按“厂商-组 ID”完整匹配池。ConstructGroupID 会移除厂商前缀，
// 因此同一节点上不同厂商的组 ID 可能相同。
func acceleratorGroupMatches(manufacturer, groupID, acceleratorKey string) bool {
	return manufacturer+"-"+groupID == acceleratorKey
}

// acceleratorCapabilityKey 将厂商、组 ID 和卡 ID 组合成键，因为组 ID 仅在厂商内唯一，
// 卡 ID 仅在组内唯一。
func acceleratorCapabilityKey(manufacturer, groupID, cardID string) string {
	return manufacturer + "/" + groupID + "/" + cardID
}

// remainingPartitionInstances 取单卡各分区规格剩余实例数的最大值。不同规格争用同一组物理切片，
// 相加会重复计数；取最大值与静态容量按单一规格最大实例数计量的口径一致。
//
// 分配行缺失，或已分配和剩余实例两项都未发布时，暂用静态能力上限。
// 按规格的账本和节点容量键也采用同样规则，避免把未上报误读为已满。
func remainingPartitionInstances(
	st workercore.AcceleratorStatus, a *workercore.AcceleratorAllocation,
) int64 {
	if !device.PartitionLedgerReady(a) {
		return int64(st.PhysicalSliced.Count)
	}
	var most int64
	for i := range a.RemainingProfiles {
		most = max(most, int64(a.RemainingProfiles[i].Count))
	}
	return most
}

// getCPUResource 从纯 CPU 规格的 ClusterQueue 计算容量。总量为各 flavor 标称 CPU 配额之和，
// 余量扣除按超配比例还原后的预留核数，单次最大请求取 flavor 名中的最大单节点核数，并以剩余量为上限。
func getCPUResource(cq *kueue.ClusterQueue, withOvercommit bool) workercore.InstanceTypeResource {
	var capCPU, ormCPU resource.Quantity
	for i := range cq.Spec.ResourceGroups {
		rg := &cq.Spec.ResourceGroups[i]
		for j := range rg.Flavors {
			flv := &rg.Flavors[j]
			for k := range flv.Resources {
				if flv.Resources[k].Name == core.ResourceCPU {
					capCPU.Add(flv.Resources[k].NominalQuota)
				}
			}
			if c := parseNodeFlavorCount(string(flv.Name)); c > ormCPU.Value() {
				ormCPU = *resource.NewQuantity(c, resource.DecimalSI)
			}
		}
	}

	remCPU := capCPU.DeepCopy()
	for i := range cq.Status.FlavorsReservation {
		flv := &cq.Status.FlavorsReservation[i]
		for j := range flv.Resources {
			res := &flv.Resources[j]
			if res.Name != core.ResourceCPU {
				continue
			}
			total := res.Total
			if withOvercommit {
				total = kuberequest.ScaleBackOvercommit(res.Name, total, false)
			}
			remCPU.Sub(total)
		}
	}
	if remCPU.Sign() < 0 {
		remCPU = *resource.NewQuantity(0, resource.DecimalSI)
	}
	if ormCPU.Cmp(remCPU) > 0 {
		ormCPU = remCPU.DeepCopy()
	}

	return workercore.InstanceTypeResource{
		OnceMaxRequest: ormCPU,
		Remaining:      remCPU,
		Capacity:       capCPU,
	}
}

func (r *InstanceTypeReconciler) SetupController(_ context.Context, opts controller.SetupOptions) error {
	r.Client = opts.Manager.GetClient()
	r.APIReader = opts.Manager.GetAPIReader()

	dedupWindow := ctrlhandlerx.NewDedupWindow[ctrlreconcile.Request]()

	return ctrl.NewControllerManagedBy(opts.Manager).
		Named("instancetype").
		For(
			// 使用 InstanceType 自身的名字作为协调键。
			&workercore.InstanceType{},
			// 创建或 spec 的 generation 变化时同步全局队列，设置 DeletionTimestamp 时开始清理。
			// 过滤最终删除事件以及仅改 status 或 finalizer 的更新，避免无事可做时重复触发。
			ctrlbuilder.WithPredicates(ctrlpredicate.Funcs{
				DeleteFunc: func(ctrlevent.DeleteEvent) bool { return false },
				UpdateFunc: func(e ctrlevent.UpdateEvent) bool {
					oldIt, newIt := e.ObjectOld.(*workercore.InstanceType), e.ObjectNew.(*workercore.InstanceType)
					if !oldIt.DeletionTimestamp.Equal(newIt.DeletionTimestamp) {
						return true
					}
					return oldIt.Generation != newIt.Generation
				},
			}),
		).
		Watches(
			// 监听规格对应的 ClusterQueue，
			// 利用 NodeQueueReconciler 和 Kueue 写入的配额及条件刷新阶段和 CPU 视图。
			// 若规格仍存在而队列被误删，则重建队列。
			&kueue.ClusterQueue{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFuncWithWindow(
				3*time.Second,
				dedupWindow,
				r.enqueueInstanceTypeWhenClusterQueueChanged,
			),
			ctrlbuilder.WithPredicates(
				// 相关队列的创建、更新和删除均触发协调。更新时刷新规格状态，删除时检查是否需要重建。
				ctrlpredicate.NewPredicateFuncs(func(obj ctrlcli.Object) bool {
					return systemmeta.MatchResource(obj, _ClusterQueueResType)
				}),
			),
		).
		Watches(
			// Devices 账本变化会改变加速器的四种视图，必须直接监听账本才能及时刷新。
			&workercore.Devices{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFuncWithWindow(
				3*time.Second,
				dedupWindow,
				r.enqueueInstanceTypeWhenDevicesChanged,
			),
			ctrlbuilder.WithPredicates(
				// 受管节点的 Devices 创建、变更或删除时重新计算加速器视图。
				ctrlpredicate.NewPredicateFuncs(func(obj ctrlcli.Object) bool {
					return obj.GetLabels()[systemname.ManagedLabelKey] == "true"
				}),
			),
		).
		Complete(r)
}

// enqueueInstanceTypeWhenClusterQueueChanged 将底层队列的事件映射到同名规格，以便刷新状态，
// 或在规格仍存在时重建误删的队列。
func (r *InstanceTypeReconciler) enqueueInstanceTypeWhenClusterQueueChanged(
	ctx context.Context, obj ctrlcli.Object,
) []ctrlreconcile.Request {
	logger := ctrllog.FromContext(ctx).
		WithValues("cluster queue", ctrlcli.ObjectKeyFromObject(obj))

	reqs := []ctrlreconcile.Request{
		{
			NamespacedName: ctrlcli.ObjectKeyFromObject(obj),
		},
	}

	logger.V(2).Info("enqueued instance type from cluster queue", "requests", reqs)
	return reqs
}

// enqueueInstanceTypeWhenDevicesChanged 按 Devices 的特征键、系统和架构查找它能贡献容量的所有 InstanceType。
// 同一个节点可能同时贡献 CPU 池和加速器池。按标签反查也能找到管理员自定义名称的规格。
func (r *InstanceTypeReconciler) enqueueInstanceTypeWhenDevicesChanged(
	ctx context.Context, obj ctrlcli.Object,
) []ctrlreconcile.Request {
	logger := ctrllog.FromContext(ctx).
		WithValues("devices", ctrlcli.ObjectKeyFromObject(obj))

	labels := obj.GetLabels()
	os, arch := labels[core.LabelOSStable], labels[core.LabelArchStable]

	var reqs []ctrlreconcile.Request
	seen := make(map[string]bool)
	for k, v := range labels {
		if v != "true" ||
			(!strings.HasPrefix(k, nodefeature.GeneralFeatureLabelPrefix) &&
				!strings.HasPrefix(k, nodefeature.AcceleratableFeatureLabelPrefix)) {
			continue
		}
		match := ctrlcli.MatchingLabels{k: "true"}
		if os != "" {
			match[core.LabelOSStable] = os
		}
		if arch != "" {
			match[core.LabelArchStable] = arch
		}
		itList := new(workercore.InstanceTypeList)
		if err := r.Client.List(ctx, itList, match,
			ctrlclix.WithoutQuorum, ctrlcli.UnsafeDisableDeepCopy); err != nil {
			logger.Error(err, "list instance types by devices labels")
			continue
		}
		for i := range itList.Items {
			name := itList.Items[i].Name
			if seen[name] {
				continue
			}
			seen[name] = true
			reqs = append(reqs, ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKey{Name: name}})
		}
	}
	if len(reqs) == 0 {
		logger.V(3).Info("devices has no schedule labels or no matching instance type, skip")
		return nil
	}

	logger.V(2).Info("enqueued instance types from devices", "requests", reqs)
	return reqs
}
