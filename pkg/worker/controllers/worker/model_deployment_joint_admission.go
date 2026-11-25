package worker

import (
	"context"
	"fmt"
	"strings"
	"time"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlbuilder "sigs.k8s.io/controller-runtime/pkg/builder"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlhandler "sigs.k8s.io/controller-runtime/pkg/handler"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	ctrlpredicate "sigs.k8s.io/controller-runtime/pkg/predicate"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	kueuepodconst "sigs.k8s.io/kueue/pkg/controller/jobs/pod/constants"
	kueueadmissioncheck "sigs.k8s.io/kueue/pkg/util/admissioncheck"
	kueueworkload "sigs.k8s.io/kueue/pkg/workload"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/controller"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/utils/ctrlclix"
	"gpustack.ai/gpustack/pkg/worker/kuberess"
)

const (
	// _JointAdmissionCheckName 是 Operator 管理的 ClusterQueue 在联合准入检查激活后引用的名称，
	// _JointAdmissionControllerName 则告诉 Kueue 由哪个控制器作答。两者与安装 AdmissionCheck 的代码共用常量，
	// 否则安装端和响应端的名称可能不一致，队列也就收不到预期的联合准入结果。
	_JointAdmissionCheckName = kuberess.ModelDeploymentJointAdmissionCheckName

	_JointAdmissionControllerName = kuberess.ModelDeploymentJointAdmissionControllerName

	// _JointAdmissionFieldOwner 标识本控制器在 Workload.status.admissionChecks 中管理的条目。
	_JointAdmissionFieldOwner = "worker.gpustack.ai/model-deployment-joint"

	// _JointAdmissionParkedMarker 写进暂停原因，状态读取方通过同一句话识别因超时而暂停的负载。
	// Kueue 和管理员也能设置 spec.active=false，仅凭该字段无法知道是谁暂停了负载。
	_JointAdmissionParkedMarker = "parked rather than held"

	// _JointAdmissionUndecidedMessage 说明暂时无法确定 Workload 的归属。这个状态与等待同组配额时都是 Pending，
	// 但原因和处理方式不同。副本可能还在重建，也可能已经消失。消息要说明控制器会重试，方便管理员判断是否需介入。
	_JointAdmissionUndecidedMessage = "cannot decide yet: some of this workload's owner replicas " +
		"could not be read, so whether it is one group of a multi-group model deployment is unknown. " +
		"Holding rather than admitting on a guess, and retrying"

	// _JointAdmissionInfeasibleAfter 限制一组角色可以占着预留配额等多久。30 分钟给节点回归、其他任务退出
	// 或管理员提高配额留出时间，也避免无解的部署一直占着配额。若提前暂停，可按状态消息恢复。
	_JointAdmissionInfeasibleAfter = 30 * time.Minute
)

// ModelDeploymentJointAdmissionCheckReconciler 在控制器运行后将它负责的 AdmissionCheck 标为 Active。
// Kueue 会停用引用未激活检查的 ClusterQueue，因此应由能处理检查的控制器报告就绪。
type ModelDeploymentJointAdmissionCheckReconciler struct {
	Client ctrlcli.Client
}

var _ ctrlreconcile.Reconciler = (*ModelDeploymentJointAdmissionCheckReconciler)(nil)

func (r *ModelDeploymentJointAdmissionCheckReconciler) Reconcile(
	ctx context.Context, req ctrl.Request,
) (ctrl.Result, error) {
	logger := ctrllog.FromContext(ctx)

	ac := new(kueue.AdmissionCheck)
	if err := r.Client.Get(ctx, req.NamespacedName, ac); err != nil {
		// AdmissionCheck 已被删除时，本轮无需再激活它。
		if err = ctrlcli.IgnoreNotFound(err); err != nil {
			logger.Error(err, "fetch admission check")
		}
		return ctrl.Result{}, err
	}

	if ac.Spec.ControllerName != _JointAdmissionControllerName {
		logger.V(3).Info("skip admission check not owned by this controller")
		return ctrl.Result{}, nil
	}

	if kubemeta.IsConditionTrue(ac.Status.Conditions, kueue.AdmissionCheckActive) {
		return ctrl.Result{}, nil
	}

	kubemeta.SetCondition(&ac.Status.Conditions, meta.Condition{
		Type:    kueue.AdmissionCheckActive,
		Status:  meta.ConditionTrue,
		Reason:  "Ready",
		Message: "the model deployment joint admission check controller is running",
	})
	if err := r.Client.Status().Update(ctx, ac); err != nil {
		// 过滤器会放行本控制器负责的检查的每次更新，发生版本冲突后，新版本事件会触发下一轮。
		return objectWriteResult(logger, err, "mark admission check active", ctrl.Result{})
	}

	logger.V(2).Info("activated model deployment joint admission check", "name", ac.Name)

	return ctrl.Result{}, nil
}

func (r *ModelDeploymentJointAdmissionCheckReconciler) SetupController(
	_ context.Context, opts controller.SetupOptions,
) error {
	r.Client = opts.Manager.GetClient()

	return ctrl.NewControllerManagedBy(opts.Manager).
		Named("modeldeploymentjointadmissioncheck").
		For(
			&kueue.AdmissionCheck{},
			ctrlbuilder.WithPredicates(ctrlpredicate.NewPredicateFuncs(func(obj ctrlcli.Object) bool {
				ac := obj.(*kueue.AdmissionCheck)

				return ac.Spec.ControllerName == _JointAdmissionControllerName
			})),
		).
		Complete(r)
}

// ModelDeploymentJointAdmissionReconciler 协调同一部署各角色的准入。Kueue 以单个 Workload 为单位预留配额，
// 跨组部署可能出现 prefill 已经获准、decode 还在等待的情况。联合检查使这些组在都具备配额后再获准。
//
// 等待中的组报告 Pending，保留已预留的配额。Retry 会让 Kueue 驱逐负载并撤销预留，
// 各组可能轮流失去对方正在等待的配额。控制器不会抢占其他组或拒绝负载。稳定且长期无解的部署由超时分支处理。
type ModelDeploymentJointAdmissionReconciler struct {
	Client ctrlcli.Client

	// Clock 用于测量各组等待的时间，可以在测试中推进时钟以检验超时边界。
	Clock clock.PassiveClock
}

// now 返回当前时间。未注入时钟时使用系统时间。
func (r *ModelDeploymentJointAdmissionReconciler) now() time.Time {
	if r.Clock == nil {
		return time.Now()
	}

	return r.Clock.Now()
}

var _ ctrlreconcile.Reconciler = (*ModelDeploymentJointAdmissionReconciler)(nil)

func (r *ModelDeploymentJointAdmissionReconciler) Reconcile(
	ctx context.Context, req ctrl.Request,
) (ctrl.Result, error) {
	logger := ctrllog.FromContext(ctx)

	wl := new(kueue.Workload)
	if err := r.Client.Get(ctx, req.NamespacedName, wl); err != nil {
		// Workload 已删除时，无需继续判定。
		if err = ctrlcli.IgnoreNotFound(err); err != nil {
			logger.Error(err, "fetch workload")
		}
		return ctrl.Result{}, err
	}

	// 只处理仍活跃且已预留配额的负载。驱逐和释放配额由 Kueue 分两次写入，
	// 两次写入之间仍可能看到旧预留，此时再写结论会覆盖 Kueue 的重置。
	if !kueueworkload.HasQuotaReservation(wl) || kueueworkload.IsEvicted(wl) ||
		kueueworkload.IsFinished(wl) || !kueueworkload.IsActive(wl) {
		logger.V(3).Info("skip workload not holding quota reservation, evicted, finished or deactivated")

		return ctrl.Result{}, nil
	}
	if kueueworkload.IsAdmitted(wl) {
		logger.V(3).Info("skip already-admitted workload; the barrier has already opened")

		return ctrl.Result{}, nil
	}

	checks, err := kueueadmissioncheck.FilterForController(
		ctx, r.Client, wl.Status.AdmissionChecks, _JointAdmissionControllerName,
	)
	if err != nil {
		logger.Error(err, "filter admission checks for controller")
		return ctrl.Result{}, err
	}
	if len(checks) == 0 {
		return ctrl.Result{}, nil
	}

	md, decided, err := r.workloadModelDeployment(ctx, wl)
	if err != nil {
		logger.Error(err, "resolve the workload's model deployment")
		return ctrl.Result{}, err
	}
	// 如果部分所属副本暂时读不到，就无法确认这个 Workload 是否依赖其他组。Kueue 一旦根据 Ready 准入，
	// 本控制器无法撤回，所以先写 Pending 和具体原因，再安排重试。这样管理员能看到负载为何在等。
	if !decided {
		logger.V(3).Info("requeue workload whose owning replicas could not be resolved", "workload", wl.Name)

		if err := r.applyVerdict(
			ctx, wl, checks, kueue.CheckStatePending, _JointAdmissionUndecidedMessage,
		); err != nil {
			// 本控制器监听所有 Workload 更新。写入版本冲突后，新版本事件会触发下次协调。
			return objectWriteResult(logger, err, "write the undecided verdict", ctrl.Result{})
		}

		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	// 普通 Instance 的 Pod 也使用引用了联合检查的队列。读清归属后，普通负载和单角色部署都应立即得到 Ready，
	// 否则它们会永远等待不存在的第二个角色。这里按角色数决定是否需要联合准入，进入联合判定后才按副本检查预留。
	// 同一角色即使有多个副本，也无需承担跨角色等待和超时暂停的代价。
	if md == nil || len(modelDeploymentPodGroups(md)) < 2 {
		if err = r.applyVerdict(ctx, wl, checks, kueue.CheckStateReady,
			"nothing to wait for: this workload is not one replica of a multi-role model deployment"); err != nil {
			return objectWriteResult(logger, err, "write the verdict", ctrl.Result{})
		}
		return ctrl.Result{}, nil
	}

	held, err := r.jointVerdict(ctx, md)
	if err != nil {
		logger.Error(err, "judge the deployment's groups")
		return ctrl.Result{}, err
	}

	// 仅在副本已齐备、仍因配额不足持续等待超过时限时才暂停。
	// 等待副本自身重建的部署仍应保留运行机会，区别见 jointHeld.Settled。
	if held.State == kueue.CheckStatePending && held.Settled &&
		r.heldPast(wl, _JointAdmissionInfeasibleAfter) {
		logger.Info("parking a deployment whose groups have not assembled", "workload", wl.Name)

		if err = r.park(ctx, wl, checks, held.Message); err != nil {
			return objectWriteResult(logger, err, "park the deployment", ctrl.Result{})
		}
		return ctrl.Result{}, nil
	}

	if err = r.applyVerdict(ctx, wl, checks, held.State, held.Message); err != nil {
		return objectWriteResult(logger, err, "write the verdict", ctrl.Result{})
	}
	return ctrl.Result{}, nil
}

// heldPast 根据本检查最近一次状态转换的时间判断 Pending 是否已经超时。
// 该时间由 Kueue 保存在 Workload 上，控制器重启后仍然有效。若检查曾离开 Pending，再次进入时重新计时。
func (r *ModelDeploymentJointAdmissionReconciler) heldPast(wl *kueue.Workload, bound time.Duration) bool {
	acs := jointCheckEntry(wl)
	if acs == nil || acs.State != kueue.CheckStatePending {
		return false
	}
	if acs.LastTransitionTime.IsZero() {
		// 尚无状态转换时间时，不能判断超时。首次写入 Pending 的那一轮会记录时间。
		return false
	}

	return r.now().Sub(acs.LastTransitionTime.Time) > bound
}

// park 暂停 Workload 并记录恢复方法。写入 spec.active=false 后，Kueue 停止为它预留配额。
// 直接删除 Workload 会让 Kueue 根据仍在运行的 Pod 重新创建它。重复提交相同清单也不会自动恢复。
// 可补足容量后重新激活工作负载，或删除并重新创建部署。
func (r *ModelDeploymentJointAdmissionReconciler) park(
	ctx context.Context,
	wl *kueue.Workload,
	checks []kueue.AdmissionCheckReference,
	waiting string,
) error {
	// 用同一个标记拼接消息，使状态读取方能够识别这里写下的暂停原因。
	parked := fmt.Sprintf(
		"%s. This has not changed for %s, so the deployment is %s: its workloads "+
			"are deactivated and no longer ask for quota. An identical re-apply does not clear this — "+
			"free the capacity the waiting groups need and reactivate the workloads, or delete the "+
			"deployment and create it again",
		waiting, _JointAdmissionInfeasibleAfter, _JointAdmissionParkedMarker,
	)

	if err := r.applyVerdict(ctx, wl, checks, kueue.CheckStatePending, parked); err != nil {
		return err
	}

	if !kueueworkload.IsActive(wl) {
		return nil
	}

	patched := wl.DeepCopy()
	patched.Spec.Active = ptr.To(false)
	if err := r.Client.Patch(ctx, patched, ctrlcli.MergeFrom(wl)); err != nil {
		return fmt.Errorf("deactivate workload %s: %w", wl.Name, err)
	}

	return nil
}

// jointHeld 记录对当前部署各组的联合准入结论及依据。
type jointHeld struct {
	// State 和 Message 写入本控制器负责的每个准入检查条目。
	State   kueue.CheckState
	Message string

	// Settled 表示各角色的副本已齐备，剩下的阻塞来自容量。冷节点拉取大镜像等正常过程也可能耗时很久，
	// 若把重建中的副本算作无解，就会暂停本可启动的部署。部署刚刚齐备但容量仍不足时，
	// 如果检查原有的 Pending 时间已超过上限，也可能在本轮暂停。状态消息会给出恢复方法。
	Settled bool
}

// IMPORTANT: 这是 PD 分离场景下的 联合准入函数
//
// jointVerdict 逐个检查各角色声明的副本。Kueue 先为 Workload 预留配额，等联合检查通过后才准入。
// 因此这里检查配额预留。若等待其他 Workload 先获准，各组就会相互等待。
func (r *ModelDeploymentJointAdmissionReconciler) jointVerdict(
	ctx context.Context, md *workercore.ModelDeployment,
) (jointHeld, error) {
	// 1. 拉取“现状快照”
	byGroup, liveByGroup, shapes, err := modelDeploymentReplicaGroups(ctx, r.Client, md)
	if err != nil {
		return jointHeld{}, err
	}

	wlList := new(kueue.WorkloadList)
	if err = r.Client.List(ctx, wlList, ctrlcli.InNamespace(md.Namespace), ctrlclix.WithoutQuorum); err != nil {
		return jointHeld{}, fmt.Errorf("list workloads: %w", err)
	}

	// 2. 按“图纸（Spec）”而不是按“现场（Pod）”逐个点名
	//
	// 一个角色可以有多个独立预留配额的副本。按声明的角色和序号推导组名，才能发现尚未创建的组。
	// 两个角色可以使用同一规格，因此等待消息记录角色名，便于定位缺少配额的副本。
	var waiting, assembling []string
	reserved, replicas := 0, 0
	for i := range md.Spec.Roles {
		role := &md.Spec.Roles[i]

		var roleWaiting, roleAssembling bool
		for ordinal := range int(role.Replicas) {
			replicas++
			group := modelDeploymentReplicaGroupName(md, role.Name, ordinal)
			members := byGroup[group]
			// 3. 判断该副本是不是“还在盖房子（集结中）”
			//
			// 组的大小取自已部署的成员。修改规格时，新规格可能与眼前这组 Pod 的大小不同。
			// 正在终止的成员仍可能持有配额，但只有留下的成员才算组齐。组尚未齐备时不能因超时暂停。
			if shape := shapes[group]; !shape.modelDeploymentReplicaIsWhole() ||
				liveByGroup[group].Len() < shape.Total {
				roleAssembling = true
			}
			// 4. 判断“配额到位了，还是正在受控替换中”
			if anyWorkloadHoldsQuotaFor(wlList.Items, members) ||
				jointReplicaDepartingRollout(md, wlList.Items, members, liveByGroup[group]) {
				reserved++
				continue
			}
			roleWaiting = true
		}

		if roleWaiting {
			waiting = append(waiting, role.Name)
		}
		if roleAssembling {
			assembling = append(assembling, role.Name)
		}
	}
	// 正常滚动替换时旧成员正在退出，这组也会暂时缺人。先判断所有组是否都已预留或正在替换。
	if len(waiting) == 0 {
		return jointHeld{
			State: kueue.CheckStateReady,
			Message: "every replica of this deployment has reserved quota or is being replaced by " +
				"this operator's own rollout",
		}, nil
	}
	if len(assembling) > 0 {
		return jointHeld{
			State: kueue.CheckStatePending,
			Message: fmt.Sprintf(
				"holding this replica while the deployment is still assembling: the roles "+
					"%s do not yet have every replica they declare, so Kueue has composed no "+
					"workload for them yet. Nothing is wrong with the cluster; this resolves itself as "+
					"the replicas appear",
				strings.Join(assembling, ", "),
			),
		}, nil
	}

	return jointHeld{
		State: kueue.CheckStatePending,
		Message: fmt.Sprintf(
			"holding this replica until the whole deployment can run: %d of %d replicas have reserved quota, "+
				"and the ones still waiting belong to the roles %s. Every replica keeps the quota it has "+
				"reserved while it waits",
			reserved, replicas, strings.Join(waiting, ", "),
		),
		Settled: true,
	}, nil
}

// jointReplicaDepartingRollout 利用替换时留下的状态识别正在交接的副本。释放副本位置时，
// Operator 删除 Workload 并让旧 Pod 退出。如果是 Kueue 抢占，失去配额的 Workload 仍会引用旧 Pod。
// 手工同时删除两者也会留下相同状态，协调循环会重建该副本。部署自身已开始删除时则无需等待重建。
func jointReplicaDepartingRollout(
	md *workercore.ModelDeployment,
	wls []kueue.Workload,
	members, live sets.Set[types.UID],
) bool {
	if md.DeletionTimestamp != nil || members.Len() == 0 || members.Len() == live.Len() {
		return false
	}

	return !anyWorkloadOwnsAny(wls, members)
}

// anyWorkloadOwnsAny 检查是否还有 Workload 引用组内任一 Pod，包括已经失去配额的 Workload。
func anyWorkloadOwnsAny(wls []kueue.Workload, members sets.Set[types.UID]) bool {
	for i := range wls {
		if modelDeploymentWorkloadOwnsAny(&wls[i], members) {
			return true
		}
	}

	return false
}

// modelDeploymentReplicaGroups 只列带当前部署标签的 Pod，再根据所有者引用核实归属，避免在每个事件上扫描整个命名空间。
// all 包含正在终止的副本，供事件映射找到所有需重新入队的兄弟 Workload。live 只计入仍然留在组内的副本，用来判断组是否齐备。
// 各组的实际形态也从同一批 Pod 读取，检查每个成员声明的副本位置，避免把重复占位误判为组正在集结。
func modelDeploymentReplicaGroups(
	ctx context.Context, cli ctrlcli.Client, md *workercore.ModelDeployment,
) (all, live map[string]sets.Set[types.UID], shapes map[string]modelDeploymentReplicaShape, err error) {
	podList := new(core.PodList)
	if err = cli.List(ctx, podList,
		ctrlcli.InNamespace(md.Namespace),
		ctrlcli.MatchingLabels{modelDeploymentLabelKeyInstance: md.Name},
		ctrlclix.WithoutQuorum); err != nil {
		return nil, nil, nil, fmt.Errorf("list replicas: %w", err)
	}

	all, live = make(map[string]sets.Set[types.UID]), make(map[string]sets.Set[types.UID])
	shapes = make(map[string]modelDeploymentReplicaShape)
	members := make(map[string][]*core.Pod)
	for i := range podList.Items {
		pod := &podList.Items[i]
		if !modelDeploymentOwns(pod, md) {
			continue
		}
		group := pod.Labels[kueuepodconst.GroupNameLabel]
		if all[group] == nil {
			all[group] = sets.New[types.UID]()
		}
		members[group] = append(members[group], pod)
		all[group].Insert(pod.UID)
		if pod.DeletionTimestamp != nil {
			continue
		}
		if live[group] == nil {
			live[group] = sets.New[types.UID]()
		}
		live[group].Insert(pod.UID)
	}

	// 与其他观察路径共用同一套成员位置和组形态检查。
	for group, groupMembers := range members {
		shapes[group] = modelDeploymentDeployedReplicaShape(
			modelDeploymentReplicaView{Members: groupMembers},
		)
	}

	return all, live, shapes, nil
}

// anyWorkloadHoldsQuotaFor 检查是否有已预留配额的 Workload 引用组内任一 Pod 的 UID。
func anyWorkloadHoldsQuotaFor(wls []kueue.Workload, members sets.Set[types.UID]) bool {
	for i := range wls {
		wl := &wls[i]
		if modelDeploymentWorkloadOwnsAny(wl, members) && kueueworkload.HasQuotaReservation(wl) {
			return true
		}
	}

	return false
}

// workloadModelDeployment 沿 Workload 的 Pod 所有者引用，再沿 Pod 的控制器引用查找 ModelDeployment。
// Kueue 为组内每个 Pod 建立普通所有者引用。同组成员属于同一部署，所以找到其中一个即可。
func (r *ModelDeploymentJointAdmissionReconciler) workloadModelDeployment(
	ctx context.Context, wl *kueue.Workload,
) (md *workercore.ModelDeployment, decided bool, err error) {
	// 已引用的 Pod 或部署暂时读不到时，可能正在重建，也可能缓存尚未追上，必须保留“归属待定”的状态。
	// 如果根本没有 Pod 所有者，或所有可读的 Pod 都属于其他工作负载，就可以放行。所有 Operator 队列都可能收到这种普通请求。
	missing := false

	for _, ref := range wl.OwnerReferences {
		if !modelDeploymentOwnerRefNamesAPod(ref) {
			continue
		}

		pod := new(core.Pod)
		err := r.Client.Get(ctx,
			ctrlcli.ObjectKey{Namespace: wl.Namespace, Name: ref.Name}, pod, ctrlclix.WithoutQuorum)
		if err != nil {
			if ctrlcli.IgnoreNotFound(err) != nil {
				return nil, false, err
			}
			missing = true

			continue
		}

		owner := meta.GetControllerOf(pod)
		if owner == nil || owner.Kind != "ModelDeployment" {
			continue
		}

		md := new(workercore.ModelDeployment)
		err = r.Client.Get(ctx,
			ctrlcli.ObjectKey{Namespace: wl.Namespace, Name: owner.Name}, md, ctrlclix.WithoutQuorum)
		if err != nil {
			if ctrlcli.IgnoreNotFound(err) != nil {
				return nil, false, err
			}
			missing = true

			continue
		}

		return md, true, nil
	}

	return nil, !missing, nil
}

// applyVerdict 写回本控制器负责的所有检查。admissionChecks 按 name 合并。服务端应用时，
// 若字段所有者在新提交中漏掉以前管理的条目，这个条目可能被移除。
func (r *ModelDeploymentJointAdmissionReconciler) applyVerdict(
	ctx context.Context,
	wl *kueue.Workload,
	checks []kueue.AdmissionCheckReference,
	state kueue.CheckState,
	message string,
) error {
	desired, changed := desiredCheckStates(wl, checks, state, message)
	if !changed {
		return nil
	}

	return kueueworkload.PatchStatus(ctx, r.Client, wl, ctrlcli.FieldOwner(_JointAdmissionFieldOwner),
		func(w *kueue.Workload) (bool, error) {
			for i := range desired {
				kueueworkload.SetAdmissionCheckState(&w.Status.AdmissionChecks, desired[i], clock.RealClock{})
			}

			return true, nil
		})
}

// jointCheckEntry 在已有的 Workload 对象中按名称取本控制器的准入条目，缺少时返回 nil。
// 等待时间、状态和说明都从同一条目读取，无需再次请求 API。
func jointCheckEntry(wl *kueue.Workload) *kueue.AdmissionCheckState {
	return kueueadmissioncheck.FindAdmissionCheck(
		wl.Status.AdmissionChecks, kueue.AdmissionCheckReference(_JointAdmissionCheckName),
	)
}

// carriesJointCheck 判断 Workload 是否携带本控制器的检查。这个条件可以直接过滤其他队列，
// 还要继续检查归属才能确定它是否属于多角色部署。
func carriesJointCheck(wl *kueue.Workload) bool {
	return jointCheckEntry(wl) != nil
}

// jointSiblings 将一个 Workload 的变化映射给同一部署的其他 Workload。
// 某组等待的配额可能刚刚写在另一个组上，重新检查前者才能在条件满足时放行。
// 即使各组的预留事件相隔较长，这条监听也能唤醒先前被阻塞的组。
func (r *ModelDeploymentJointAdmissionReconciler) jointSiblings(
	ctx context.Context, obj ctrlcli.Object,
) []ctrlreconcile.Request {
	logger := ctrllog.FromContext(ctx)

	wl, ok := obj.(*kueue.Workload)
	if !ok {
		return nil
	}

	// 先从事件中的 Workload 判断是否带本检查。这样绝大多数无关事件无需读取 Pod 或列出 Workload。
	if !carriesJointCheck(wl) {
		return nil
	}

	// 映射函数无法确认归属时不生成兄弟请求。事件自身的 Workload 仍会进入 Reconcile，由它写入等待原因并重试。
	md, _, err := r.workloadModelDeployment(ctx, wl)
	if err != nil {
		logger.Error(err, "resolve the workload's model deployment for sibling mapping")
		return nil
	}
	if md == nil || len(modelDeploymentPodGroups(md)) < 2 {
		return nil
	}

	// 包括正在终止的副本，仍持有这些 Pod 的兄弟 Workload 也要收到事件。
	byGroup, _, _, err := modelDeploymentReplicaGroups(ctx, r.Client, md)
	if err != nil {
		logger.Error(err, "index the deployment's replicas for sibling mapping")
		return nil
	}
	members := sets.New[types.UID]()
	for _, group := range byGroup {
		members = members.Union(group)
	}

	wlList := new(kueue.WorkloadList)
	if err = r.Client.List(ctx, wlList, ctrlcli.InNamespace(md.Namespace), ctrlclix.WithoutQuorum); err != nil {
		logger.Error(err, "list workloads for sibling mapping")
		return nil
	}

	var reqs []ctrlreconcile.Request
	for i := range wlList.Items {
		sibling := &wlList.Items[i]
		// 主监听已经将发生变化的 Workload 入队，这里只补它的兄弟。
		if sibling.Name == wl.Name || !modelDeploymentWorkloadOwnsAny(sibling, members) {
			continue
		}
		reqs = append(reqs, ctrlreconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: sibling.Namespace, Name: sibling.Name},
		})
	}

	return reqs
}

func (r *ModelDeploymentJointAdmissionReconciler) SetupController(
	_ context.Context, opts controller.SetupOptions,
) error {
	r.Client = opts.Manager.GetClient()

	return ctrl.NewControllerManagedBy(opts.Manager).
		Named("modeldeploymentjointadmission").
		For(&kueue.Workload{}).
		Watches(&kueue.Workload{}, ctrlhandler.EnqueueRequestsFromMapFunc(r.jointSiblings)).
		Complete(r)
}
