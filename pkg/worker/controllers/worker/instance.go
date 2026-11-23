package worker

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	core "k8s.io/api/core/v1"
	node "k8s.io/api/node/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilversion "k8s.io/apimachinery/pkg/util/version"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlbuilder "sigs.k8s.io/controller-runtime/pkg/builder"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlevent "sigs.k8s.io/controller-runtime/pkg/event"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	ctrlpredicate "sigs.k8s.io/controller-runtime/pkg/predicate"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	kueuectrlconst "sigs.k8s.io/kueue/pkg/controller/constants"

	worker "gpustack.ai/gpustack/api/worker/v1"
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/controller"
	"gpustack.ai/gpustack/pkg/deviceplugin"
	"gpustack.ai/gpustack/pkg/kubediscovery"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/systemmeta"
	"gpustack.ai/gpustack/pkg/utils/ctrlclix"
	"gpustack.ai/gpustack/pkg/utils/ctrlhandlerx"
	"gpustack.ai/gpustack/pkg/utils/quantityx"
	"gpustack.ai/gpustack/pkg/utils/slicex"
	"gpustack.ai/gpustack/pkg/utils/strconvx"
	"gpustack.ai/gpustack/pkg/worker/apistatus"
	"gpustack.ai/gpustack/pkg/worker/kuberequest"
	"gpustack.ai/gpustack/pkg/worker/settings"
)

// InstanceReconciler 协调 v1alpha1.Instance 的资源和状态。
//   - 根据 Instance spec 创建、更新或删除承载实例的 Pod。
//   - 如规格声明了端口，则为 Instance 创建 Service。Instance 拥有 Service，停止 Pod 时仍可保留已分配的节点端口。
//   - 根据 Pod 状态更新 Instance 状态。
type InstanceReconciler struct {
	Client    ctrlcli.Client
	APIReader ctrlcli.Reader
}

var _ ctrlreconcile.Reconciler = (*InstanceReconciler)(nil)

const (
	// InstancePhaseStarting 表示正在创建或更新 Pod，使其符合 Instance 规格。
	InstancePhaseStarting = "Starting"
	// InstancePhaseStopping 表示实例正在停止，正在删除其 Pod。
	InstancePhaseStopping = "Stopping"
	// InstancePhaseStopped 表示 Pod 已删除，实例已停止。
	InstancePhaseStopped = "Stopped"
	// InstancePhaseReady 表示 Pod 已运行并就绪。
	InstancePhaseReady = "Ready"
	// InstancePhaseDeleting 表示 Instance 已进入删除流程，正在删除其 Pod。
	InstancePhaseDeleting = "Deleting"
)

const (
	// InstanceTypePhaseInactive 表示规格的 ClusterQueue 已因管理员停用而进入 Hold，
	// 或已在 HoldAndDrain 下排空所有预留。
	// 该值与 apistatus.GetSummaryOfClusterQueue 返回的 Inactive 一致。
	InstanceTypePhaseInactive = "Inactive"
)

const (
	// _PodReasonUnexpectedAdmissionError 是 kubelet 在准入阶段拒绝 Pod 时填写的原因。
	// 加速器 Pod 遇到设备插件 Allocate 报错时也会得到它，例如两种分配模式同时争用一张卡。
	_PodReasonUnexpectedAdmissionError = "UnexpectedAdmissionError"
	// _InstanceAdmissionFailureRetryWindow 限定准入失败后可重建 Pod 的时间。实例与当前 Pod 的创建时间差
	// 会随重建增大，可同时用来计算退避时间和判断截止时间，无需在 Instance 上保存重试次数。
	_InstanceAdmissionFailureRetryWindow = 5 * time.Minute
)

// IMPORTANT: InstanceReconciler 的大脑中枢
// 控制器被叫醒后，就会进入这里的 Reconcile
// 进入 Reconcile(namespace/name)
//
//	│
//	├─ 1. Get Instance ── 查不到？说明已删完，直接 return nil
//	│
//	├─ 2. DeletionTimestamp != nil？（正在删除）
//	│     └─ 删自己拥有的 Pod & Service → 等两者都消失 → 摘 Finalizer 放行删除 → return
//	│
//	├─ 3. 还没加 Finalizer？ ── 加上 Finalizer（防止对象被直接清掉留下孤儿资源）
//	│
//	├─ 4. Get 同名 Pod ── 不是我拥有的？当不存在！老 Pod 缺隔离标签？原地补上！
//	│
//	├─ 5. Spec.Stop == true？（用户要求停止）
//	│     └─ 删 Pod（但故意保留 Service 保住 NodePort！）→ Pod 消失后写 Status=Stopped → return
//	│
//	├─ 6. 查 InstanceType + 查 ClusterQueue.StopPolicy
//	│     └─ 规格没了 / 规格在删 / 队列在 HoldAndDrain？ → 写 Spec.Stop = true 持久化停机意图 → return
//	│
//	├─ 7. Pod 被 kubelet 准入拒绝（UnexpectedAdmissionError）？
//	│     └─ 在 5 分钟重试窗口内 → 删 Pod 按时间差退避重建；超窗则保留作错误现场
//	│
//	├─ 8. Pod == nil？（需要创建 Pod）
//	│     ├─ 闸门1：加速规格的 Detail 就绪了吗？未就绪 → 2s 后重试
//	│     ├─ 闸门2：规格发布队列入口 Entrance 了吗？未发布 → 2s 后重试
//	│     ├─ 闸门3：把旧的 Stopped/Ready 状态改回 Starting
//	│     ├─ 闸门4：解析模型卷 & 持久卷（未就绪写入 PhaseMessage，10s 后重试）
//	│     └─ 调用 convertPodFromInstance 渲染 Pod + 注入 PV 亲和性 → Client.Create(pod)
//	│     （若 Pod 正在删除中 `pod.DeletionTimestamp != nil` → 2s 后重试，不抢跑）
//	│
//	├─ 9. 调谐 Service
//	│     ├─ 没配端口 → 如果有自己拥有的旧 Service，删掉
//	│     └─ 配了端口 → 没有就建；撞名别人的就报错；正在删就等；已存在就对齐且保留旧 NodePort
//	│
//	└─ 10. 汇总状态回写 Instance.Status
//	      └─ 翻译 Pod 状态 + 若 Running 则同步 NodePort / NodeName / HostIPs / PodIPs / Allocations
//	      └─ DeepEqual 对比有变化才写 Status().Update
func (r *InstanceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := ctrllog.FromContext(ctx)

	// 按请求中的命名空间和名称读取 Instance。排队期间对象可能已被删除。
	inst := new(workercore.Instance)
	// -- 第一、Get Instance ── 查不到？说明已删完，直接 return nil
	err := r.Client.Get(ctx, req.NamespacedName, inst)
	if err != nil {
		if !kerrors.IsNotFound(err) {
			logger.Error(err, "fetch instance")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// -- 第二、正在删除
	// Instance 有删除时间戳时，先清理它拥有的资源。
	if inst.DeletionTimestamp != nil {
		if systemmeta.Unlock(inst) {
			logger.V(3).Info("skip deleted instance")
			return ctrl.Result{}, nil
		}

		// 等 Pod 和 Service 都消失后才移除终结器，让 API 服务器继续删除 Instance。
		pod := new(core.Pod)
		err = r.Client.Get(ctx, ctrlcli.ObjectKeyFromObject(inst), pod,
			ctrlclix.WithoutQuorum)
		if err != nil {
			if !kerrors.IsNotFound(err) {
				logger.Error(err, "fetch pod")
				return ctrl.Result{}, err
			}
			pod = nil
		}

		// 停止实例时保留的 Service 在这里结束生命周期，否则一个已经删除的实例还会占用 NodePort。
		svc := new(core.Service)
		err = r.Client.Get(ctx, ctrlcli.ObjectKeyFromObject(inst), svc,
			ctrlclix.WithoutQuorum)
		if err != nil {
			if !kerrors.IsNotFound(err) {
				logger.Error(err, "fetch service")
				return ctrl.Result{}, err
			}
			svc = nil
		}

		// 只同名但归属不同的 Service 由其所有者管理，不能阻碍本实例释放终结器。
		if svc != nil && !kubemeta.IsControlledBy(svc, inst) {
			logger.V(2).Info("service of the same name belongs to something else; leaving it alone",
				"service", ctrlcli.ObjectKeyFromObject(svc))
			svc = nil
		}

		// 同样检查 Pod 的所有者，以免终止其他工作负载。
		if pod != nil && !kubemeta.IsControlledBy(pod, inst) {
			logger.V(2).Info("pod of the same name belongs to something else; leaving it alone",
				"pod", ctrlcli.ObjectKeyFromObject(pod))
			pod = nil
		}

		if pod == nil && svc == nil {
			err = r.Client.Update(ctx, inst)
			if err != nil {
				logger.Error(err, "unlock instance")
			}
			return ctrl.Result{}, err
		}

		if inst.Status.Phase != InstancePhaseDeleting {
			logger.Info("instance deleting")

			inst.Status.Phase = InstancePhaseDeleting
			err = r.Client.Status().Update(ctx, inst)
			if err != nil {
				// 监听过滤器会忽略仅 status 或元数据变化的事件。写入冲突后主动重新排队，以便用新版本再试。
				// 下面写 Instance 的操作也遵循这个规则。
				return objectWriteResult(logger, err, "update instance status to deleting", _requeueAfterConflict)
			}
		}

		if pod != nil && pod.DeletionTimestamp == nil {
			err = r.Client.Delete(ctx, pod,
				ctrlclix.Terminated)
			if err != nil {
				if !kerrors.IsNotFound(err) {
					logger.Error(err, "delete pod")
					return ctrl.Result{}, err
				}
			}
		}

		if svc != nil && svc.DeletionTimestamp == nil {
			err = r.Client.Delete(ctx, svc,
				ctrlclix.Terminated)
			if err != nil {
				if !kerrors.IsNotFound(err) {
					logger.Error(err, "delete service")
					return ctrl.Result{}, err
				}
			}
		}

		logger.V(3).Info("pod and service deletion in progress; requeue in 2s")
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}

	// 添加终结器，让本控制器有机会在 Instance 删除前清理子对象。
	if !systemmeta.Lock(inst) {
		err = r.Client.Update(ctx, inst)
		if err != nil {
			logger.Error(err, "lock instance")
			return ctrl.Result{}, err
		}
	}

	// 读取与 Instance 同命名空间、同名的 Pod。
	pod := new(core.Pod)
	err = r.Client.Get(ctx, req.NamespacedName, pod,
		ctrlclix.WithoutQuorum)
	if err != nil {
		if !kerrors.IsNotFound(err) {
			logger.Error(err, "fetch pod")
			return ctrl.Result{}, err
		}
		pod = nil
	}

	// 只有控制器所有者指向本 Instance 的 Pod 才能作为实例的运行 Pod。
	// 对其他同名 Pod 跳过标签修复、停止、删除和状态同步。
	if pod != nil && !kubemeta.IsControlledBy(pod, inst) {
		logger.V(2).Info("pod of the same name belongs to something else; leaving it alone",
			"pod", ctrlcli.ObjectKeyFromObject(pod))
		pod = nil
	}

	// 旧版本创建的 Pod 可能缺少命名空间隔离策略用来选中实例的资源类型标签。
	// 标签可以原地更新，补齐它无需重启 Pod。
	if pod != nil && pod.Labels[systemmeta.ResourceTypeLabel] != "instances" {
		if pod.Labels == nil {
			pod.Labels = map[string]string{}
		}
		pod.Labels[systemmeta.ResourceTypeLabel] = "instances"
		if err = r.Client.Update(ctx, pod); err != nil {
			logger.Error(err, "patch instance resource-type label")
			return ctrl.Result{}, err
		}
	}

	// 用户要求停止时，先删除运行 Pod，再把状态写为 Stopped。Service 保管着用户连接的 NodePort，
	// 停止期间保留它才能在下次启动时复用原端口。删除实例时清理 Service。若停止期间移除了所有端口，则在下次启动时清理。
	if inst.Spec.Stop {
		// Pod 已经消失，可以把实例标为 Stopped。
		if pod == nil {
			if inst.Status.Phase == InstancePhaseStopped {
				logger.V(3).Info("instance already stopped")
				return ctrl.Result{}, nil
			}

			inst.Status = workercore.InstanceStatus{
				Phase: InstancePhaseStopped,
			}
			err = r.Client.Status().Update(ctx, inst)
			if err != nil {
				return objectWriteResult(logger, err, "update instance status to stopped", _requeueAfterConflict)
			}
			logger.Info("instance stopped")
			return ctrl.Result{}, nil
		}

		// 先发布 Stopping，让其他观察者能在 Pod 删除期间看到停止意图。
		if inst.Status.Phase != InstancePhaseStopping {
			inst.Status.Phase = InstancePhaseStopping
			err = r.Client.Status().Update(ctx, inst)
			if err != nil {
				return objectWriteResult(logger, err, "update instance status to stopping", _requeueAfterConflict)
			}
		}

		// 请求删除仍在运行且尚未进入删除流程的 Pod。
		if pod.DeletionTimestamp == nil {
			err = r.Client.Delete(ctx, pod,
				ctrlclix.Terminated)
			if err != nil {
				if !kerrors.IsNotFound(err) {
					logger.Error(err, "delete pod")
					return ctrl.Result{}, err
				}
			}
		}

		// Pod 仍在删除，稍后重新检查。
		logger.V(2).Info("pod deletion in progress; requeue in 2s")
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}

	// 读取规格。它提供新 Pod 的资源预算，并反映该规格的可用状态。
	instType := new(worker.InstanceType)
	err = r.Client.Get(ctx, ctrlcli.ObjectKey{Name: inst.Spec.Type}, instType,
		ctrlclix.WithoutQuorum)
	if err != nil {
		if !kerrors.IsNotFound(err) {
			logger.Error(err, "fetch instance type")
			return ctrl.Result{}, err
		}
		// 本地缓存可能还未看到这个规格，再直接向 API 服务器读一次。
		err = r.APIReader.Get(ctx, ctrlcli.ObjectKey{Name: inst.Spec.Type}, instType,
			ctrlclix.WithoutQuorum)
		if err != nil && !kerrors.IsNotFound(err) {
			logger.Error(err, "fetch instance type")
			return ctrl.Result{}, err
		}
	}
	instTypeGone := kerrors.IsNotFound(err)

	// 读取与规格同名的 ClusterQueue 的 StopPolicy。Hold 暂停新的准入，HoldAndDrain 还会驱逐已准入的负载。
	// 预留清空后两者都可能显示 Inactive，快速排空也可能跳过可观察的 Draining 阶段，因此要以队列策略判断。
	var draining bool
	if !instTypeGone {
		cq := new(kueue.ClusterQueue)
		cqErr := r.Client.Get(ctx, ctrlcli.ObjectKey{Name: inst.Spec.Type}, cq)
		if cqErr != nil && !kerrors.IsNotFound(cqErr) {
			logger.Error(cqErr, "fetch backing cluster queue")
			return ctrl.Result{}, cqErr
		}
		draining = cqErr == nil && ptr.Deref(cq.Spec.StopPolicy, kueue.None) == kueue.HoldAndDrain
	}

	// 规格消失、开始删除或队列进入 HoldAndDrain 时，即使 Pod 尚未创建也要停止实例。
	// Hold 保留运行中的 Pod，新请求可继续等待。监听规格删除和队列策略变化，即使 Pod 没有事件也能处理这一轮。
	if instTypeGone || instType.DeletionTimestamp != nil || draining {
		// 把停止意图写入 spec。即使排空信号随后消失，下一轮仍会经过停止分支删除 Pod，并更新状态。
		inst.Spec.Stop = true
		err = r.Client.Update(ctx, inst)
		if err != nil {
			return objectWriteResult(logger, err, "stop instance with unschedulable instance type",
				_requeueAfterConflict)
		}

		logger.Info("stop instance as its instance type is gone, deleting, or draining", "type", inst.Spec.Type)
		return ctrl.Result{}, nil
	}

	// kubelet 在准入阶段拒绝 Pod 时，容器尚未启动，也未留下设备分配，可以删除 Pod 后重新创建。
	// 例如设备插件看到的设备快照落后于一次刚发生的预留，Allocate 可能暂时失败。
	// 用本次 Pod 与 Instance 的创建时间差作为退避依据。超过重试窗口后保留失败的 Pod，让用户能看到原因。
	if pod != nil && pod.DeletionTimestamp == nil &&
		pod.Status.Phase == core.PodFailed && pod.Status.Reason == _PodReasonUnexpectedAdmissionError {
		gap := max(pod.CreationTimestamp.Sub(inst.CreationTimestamp.Time), 0)
		if gap < _InstanceAdmissionFailureRetryWindow {
			err = r.Client.Delete(ctx, pod,
				ctrlclix.Terminated)
			if err != nil && !kerrors.IsNotFound(err) {
				logger.Error(err, "delete admission-rejected pod")
				return ctrl.Result{}, err
			}
			backoff := max(gap, 2*time.Second)
			logger.Info("rebuild admission-rejected pod",
				"gap", gap, "backoff", backoff, "message", pod.Status.Message)
			return ctrl.Result{RequeueAfter: backoff}, nil
		}
		logger.Info("admission-rejected pod past the retry window; leaving it as the visible error",
			"gap", gap, "message", pod.Status.Message)
	}

	// 实例尚无运行 Pod 时，根据规格创建它。
	if pod == nil {
		// 加速器资源名、RuntimeClass 和切分方式都依赖规格的 Status.Detail。
		// 等详情就绪后再创建 Pod，避免把错误的资源名或空的运行时写进 Pod。
		if instType.Spec.Acceleratable && !instType.Status.Detail.AcceleratorReady() {
			logger.V(2).Info("instance type accelerator detail not ready; requeue in 2s")
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		}

		// Pod 的队列标签取自规格发布的 Entrance。没有标签会绕过 Kueue 准入和配额，猜测的名称也可能找不到 LocalQueue。
		// 这里和上面的详情检查一样，稍后重新排队等规格就绪。控制器只监听队列策略变化，发布入口未必会触发新事件。
		if instType.Status.Entrance == "" {
			logger.V(2).Info("instance type publishes no queue entrance yet; requeue in 2s")
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		}

		// 重建 Pod 前，将 Stopping、Stopped 或 Ready 改回 Starting，先让观察者知道本轮正在启动。
		if inst.Status.Phase == InstancePhaseStopping ||
			inst.Status.Phase == InstancePhaseStopped ||
			inst.Status.Phase == InstancePhaseReady {
			inst.Status = workercore.InstanceStatus{
				Phase: InstancePhaseStarting,
			}
			err = r.Client.Status().Update(ctx, inst)
			if err != nil {
				return objectWriteResult(logger, err, "update instance status to starting", _requeueAfterConflict)
			}

			logger.Info("instance starting")
		}

		// 模型卷决定权重是否已就绪，持久卷的绑定还可能限制 Pod 所在节点。
		// Pod 只在创建时渲染，必须先解析卷。条件尚未满足时把具体原因写入实例状态。
		models, wait, err := r.resolveInstanceModelVolumes(ctx, inst)
		if err != nil {
			logger.Error(err, "resolve model volumes")
			return ctrl.Result{}, err
		}
		var claimAffinities []*core.NodeSelector
		if wait == "" {
			claimAffinities, wait, err = r.resolveInstancePersistentVolumes(ctx, inst)
			if err != nil {
				logger.Error(err, "resolve persistent volumes")
				return ctrl.Result{}, err
			}
		}
		if wait != "" {
			if inst.Status.Phase != InstancePhaseStarting || inst.Status.PhaseMessage != wait {
				inst.Status = workercore.InstanceStatus{Phase: InstancePhaseStarting, PhaseMessage: wait}
				if err = r.Client.Status().Update(ctx, inst); err != nil {
					return ctrl.Result{}, ctrlcli.IgnoreNotFound(err)
				}
			}
			logger.V(2).Info("volume not available; requeue in 10s", "reason", wait)
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}

		pod = r.convertPodFromInstance(ctx, inst, instType, models)
		for _, required := range claimAffinities {
			injectModelArtifactAffinity(pod, required)
		}
		err = r.Client.Create(ctx, pod)
		if err != nil {
			logger.Error(err, "create pod")
			return ctrl.Result{}, err
		}
	} else if pod.DeletionTimestamp != nil {
		logger.V(3).Info("previous pod deletion in progress; requeue in 2s")
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}

	// 查找实例用来暴露端口的同名 Service。
	svc := new(core.Service)
	err = r.Client.Get(ctx, req.NamespacedName, svc,
		ctrlclix.WithoutQuorum)
	if err != nil {
		if !kerrors.IsNotFound(err) {
			logger.Error(err, "fetch service")
			return ctrl.Result{}, err
		}
		svc = nil
	}

	// Service 需要跨越停止和启动，更新已有对象才能保住 NodePort。
	// NodePort 类型的 Service 必须至少有一个端口。实例清空端口时清理原来的 Service。
	if len(inst.Spec.Ports) == 0 {
		// 只删除本实例拥有的 Service，其他应用仍可能依赖同名对象。
		if svc != nil && !kubemeta.IsControlledBy(svc, inst) {
			logger.V(2).Info("service of the same name belongs to something else; leaving it alone",
				"service", ctrlcli.ObjectKeyFromObject(svc))
			svc = nil
		}

		if svc != nil {
			if svc.DeletionTimestamp == nil {
				err = r.Client.Delete(ctx, svc,
					ctrlclix.Terminated)
				if err != nil && !kerrors.IsNotFound(err) {
					logger.Error(err, "delete service")
					return ctrl.Result{}, err
				}
			}

			logger.V(3).Info("service deletion in progress; requeue in 2s")
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		}
	} else {
		desired := r.convertServiceFromInstance(inst)
		switch {
		case svc == nil:
			err = r.Client.Create(ctx, desired)
			if err != nil {
				logger.Error(err, "create service")
				return ctrl.Result{}, err
			}
			svc = desired
		case !kubemeta.IsControlledBy(svc, inst):
			// 先检查归属，再检查删除时间戳。即使外部的同名 Service 正在终止，也要报告名称冲突，
			// 以免反复等它消失，或修改其他应用的流量入口。
			return ctrl.Result{}, fmt.Errorf("service %s/%s is not owned by this instance",
				svc.Namespace, svc.Name)
		case svc.DeletionTimestamp != nil:
			logger.V(3).Info("previous service deletion in progress; requeue in 2s")
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		default:
			if alignServiceToInstance(svc, desired) {
				err = r.Client.Update(ctx, svc)
				if err != nil {
					logger.Error(err, "update service")
					return ctrl.Result{}, err
				}
			}
		}
	}

	instStatus := inst.Status.DeepCopy()

	// 根据 Pod 的实际状态更新 Instance 的阶段及说明。
	instStatus.Phase, instStatus.PhaseMessage = apistatus.GetSummaryOfPod(&pod.Status)

	if pod.Status.Phase == core.PodRunning {
		// Pod 运行后发布已分配的节点端口。实例没有 Service 时，清除旧端口。
		if svc == nil {
			instStatus.Ports = nil
		} else {
			allReady := true
			portsMap := make(map[string]int32)
			for i := range svc.Spec.Ports {
				if svc.Spec.Ports[i].NodePort == 0 {
					allReady = false
					break
				}
				portsMap[svc.Spec.Ports[i].Name] = svc.Spec.Ports[i].NodePort
			}
			if allReady {
				instStatus.Ports = make([]workercore.InstanceServicePort, 0, len(inst.Spec.Ports))
				for i := range inst.Spec.Ports {
					portKey := getPortName(inst.Spec.Ports[i])
					instStatus.Ports = append(instStatus.Ports, workercore.InstanceServicePort{
						InstancePort: inst.Spec.Ports[i],
						NodePort:     portsMap[portKey],
					})
				}
			} else {
				logger.V(2).Info("instance ports not ready; requeue in 2s")
				return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
			}
		}

		// 将实际执行节点的名称和地址写入实例状态。
		if instStatus.NodeName != pod.Spec.NodeName {
			// 读取运行 Pod 的 Node，取得节点地址。
			nd := new(core.Node)
			err = r.Client.Get(ctx, ctrlcli.ObjectKey{Name: pod.Spec.NodeName}, nd,
				ctrlclix.WithoutQuorum)
			if err != nil {
				logger.Error(err, "fetch node")
				return ctrl.Result{}, err
			}
			instStatus.NodeName = nd.Name
			for i := range nd.Status.Addresses {
				if nd.Status.Addresses[i].Type == core.NodeExternalIP {
					instStatus.HostIPs = append(instStatus.HostIPs, core.HostIP{
						IP: nd.Status.Addresses[i].Address,
					})
				}
			}
			for i := range nd.Status.Addresses {
				if nd.Status.Addresses[i].Type == core.NodeInternalIP {
					instStatus.HostIPs = append(instStatus.HostIPs, core.HostIP{
						IP: nd.Status.Addresses[i].Address,
					})
				}
			}
		}

		// 第一次获得 Pod 地址时写入实例状态。
		if len(instStatus.PodIPs) == 0 {
			instStatus.PodIPs = pod.Status.PodIPs
			if len(instStatus.PodIPs) == 0 && pod.Status.PodIP != "" {
				instStatus.PodIPs = append(instStatus.PodIPs, core.PodIP{
					IP: pod.Status.PodIP,
				})
			}
		}

		// 实例状态中还没有设备分配记录时，从 Pod 注解中读取。
		if len(instStatus.Allocations) == 0 {
			// 合并 Pod 各容器的设备分配记录，供 Instance 状态报告实际获得的设备组。
			instStatus.Allocations = deviceplugin.AllocatedAcceleratorGroupsOf(pod)
		}
	}

	if kubemeta.DeepEqual(&inst.Status, instStatus) {
		logger.V(3).Info("instance status up to date")
		return ctrl.Result{}, nil
	}

	currentPhase, lastPhase := inst.Status.Phase, instStatus.Phase

	inst.Status = *instStatus
	err = r.Client.Status().Update(ctx, inst)
	if err != nil {
		return objectWriteResult(logger, err, "update instance status to ready", _requeueAfterConflict)
	}

	if currentPhase != lastPhase && lastPhase == InstancePhaseReady {
		logger.Info("instance started")
	} else {
		logger.V(3).Info("updated instance status")
	}
	return ctrl.Result{}, nil
}

// 把 Instance 翻译成 Pod
//   - 这是一个尽量保持纯粹的“渲染函数”，逐块把用户 YAML 映射到 K8s Pod
func (r *InstanceReconciler) convertPodFromInstance(
	ctx context.Context,
	inst *workercore.Instance,
	instType *worker.InstanceType,
	models map[int]*modelArtifactWeights,
) *core.Pod {
	needSSHD := inst.Spec.SSHPublicKey != nil && inst.Spec.SSHPublicKey.Name != ""

	overcommit := settings.InstanceGeneralResourcesOvercommit.ShouldValueBool(ctx)

	additionalVols, additionalMounts := convertAdditionalVolumes(inst.Spec.AdditionalVolumes, models)

	// 先构造运行用户镜像和命令的主容器。
	mainC := core.Container{
		Name:            "main",
		Image:           inst.Spec.Image,
		ImagePullPolicy: inst.Spec.ImagePullPolicy,
		Command:         inst.Spec.Command,
		SecurityContext: func() *core.SecurityContext {
			sc := &core.SecurityContext{
				RunAsUser: ptr.To[int64](0),
			}
			if inst.Spec.Privileged {
				sc.Privileged = ptr.To(true)
			}
			return sc
		}(),
		Resources: getResourceRequirements(inst.Spec.Resources, instType, true, overcommit, true, false),
		Ports: slicex.Transform(inst.Spec.Ports, func(p workercore.InstancePort) core.ContainerPort {
			return core.ContainerPort{
				Name:          getPortName(p),
				Protocol:      p.Protocol,
				ContainerPort: p.Port,
			}
		}),
		Env: slicex.Transform(inst.Spec.Env, func(e workercore.InstanceEnvVar) core.EnvVar {
			return core.EnvVar{
				Name:  e.Name,
				Value: e.Value,
			}
		}),
		// 附加卷只挂到主容器。sshd 通过 nsenter 进入它的挂载命名空间，SSH 会话也能访问这些卷。
		VolumeMounts: append([]core.VolumeMount{{
			Name:      "workspace",
			MountPath: inst.Spec.VolumeMount,
		}}, additionalMounts...),
	}
	containers := []core.Container{mainC}

	if needSSHD {
		// 用户提供了 SSH 公钥时才创建 sshd 边车。
		sshdC := core.Container{
			Name:            "sshd",
			Image:           redirectedImage(ctx, settings.InstanceSSHServerImage.ShouldValue(ctx)),
			ImagePullPolicy: inst.Spec.ImagePullPolicy,
			Stdin:           true,
			TTY:             true,
			SecurityContext: &core.SecurityContext{
				Capabilities: &core.Capabilities{
					Add: []core.Capability{
						"SYS_ADMIN",
						"SYS_PTRACE",
					},
				},
			},
			Resources: getResourceRequirements(inst.Spec.Resources, instType, false, false, false, true),
			Env: []core.EnvVar{
				{
					Name:  "VOLUME_MOUNT_PATH",
					Value: inst.Spec.VolumeMount,
				},
			},
			VolumeMounts: func() []core.VolumeMount {
				if inst.Spec.SSHPublicKey == nil {
					return nil
				}
				return []core.VolumeMount{{
					Name:      "sshd-authorized-keys",
					MountPath: "/var/run/sshd-authorized-keys",
					ReadOnly:  true,
				}}
			}(),
		}

		containers = append(containers, sshdC)
	}

	// 将容器、卷和调度元数据组装为 Pod。
	pod := &core.Pod{
		ObjectMeta: meta.ObjectMeta{
			Name:      inst.Name,
			Namespace: inst.Namespace,
			Labels: map[string]string{
				// 使用 InstanceType 发布的 LocalQueue 入口。队列的实际名称由发布规格的控制器决定，
				// 调用前 Reconcile 已确认 Entrance 非空。
				kueuectrlconst.QueueLabel:           instType.Status.Entrance, // 供 Kueue 选择队列。
				deviceplugin.InstancePartOfLabelKey: string(inst.UID),         // 供 Service 选择本实例的 Pod。
				// 使用 Operator 自有的资源类型标签，让隔离策略能统一选中 Instance 创建的 Pod。
				systemmeta.ResourceTypeLabel: "instances", // 供隔离策略选择实例。
			},
		},
		Spec: core.PodSpec{
			HostIPC:                      true,
			ShareProcessNamespace:        ptr.To(true),
			AutomountServiceAccountToken: ptr.To(false),
			EnableServiceLinks:           ptr.To(false),
			ImagePullSecrets: func() []core.LocalObjectReference {
				if inst.Spec.ImagePullSecret == nil {
					return nil
				}
				return []core.LocalObjectReference{
					*inst.Spec.ImagePullSecret,
				}
			}(),
			// 将指定节点转换成 hostname 选择器，保留调度器和 Kueue 的准入流程。
			// 直接设置 Pod 的 nodeName 会跳过调度器。
			NodeSelector: func() map[string]string {
				if inst.Spec.NodeName == "" {
					return nil
				}
				return map[string]string{
					core.LabelHostname: r.getNodeHostname(ctx, inst.Spec.NodeName),
				}
			}(),
			Volumes: func() (vols []core.Volume) {
				if inst.Spec.SSHPublicKey != nil {
					vols = append(vols, core.Volume{
						Name: "sshd-authorized-keys",
						VolumeSource: core.VolumeSource{
							Secret: &core.SecretVolumeSource{
								SecretName:  inst.Spec.SSHPublicKey.Name,
								DefaultMode: ptr.To[int32](0o600),
							},
						},
					})
				}
				if inst.Spec.Volume.Ephemeral != nil {
					vols = append(vols, core.Volume{
						Name: "workspace",
						VolumeSource: core.VolumeSource{
							EmptyDir: &core.EmptyDirVolumeSource{
								SizeLimit: &inst.Spec.Volume.Ephemeral.Capacity,
							},
						},
					})
					return append(vols, additionalVols...)
				}
				vols = append(vols, core.Volume{
					Name: "workspace",
					VolumeSource: core.VolumeSource{
						PersistentVolumeClaim: &core.PersistentVolumeClaimVolumeSource{
							ClaimName: inst.Spec.Volume.Persistent.Name,
						},
					},
				})
				return append(vols, additionalVols...)
			}(),
			Containers: containers,
		},
	}

	// 规格使用加速器时，查找厂商的 RuntimeClass。
	if instType.Spec.Acceleratable {
		rn := nodefeature.GetAcceleratableRuntimeName(instType.Status.Detail.Manufacturer)
		if rn != "" {
			rc := new(node.RuntimeClass)
			err := r.Client.Get(ctx, ctrlcli.ObjectKey{Name: rn}, rc,
				ctrlclix.WithoutQuorum)
			if err == nil {
				pod.Spec.RuntimeClassName = ptr.To(rn)
			}
		}
	}

	// PVC 绑定的 PV 可能要求特定节点，将其节点亲和性传给 Pod，Kueue 的拓扑调度才能看到。
	// 节点交付的模型还会优先选择已有权重的节点。按卷索引遍历，每个摘要只加一次偏好，保证结果稳定。
	preferred := map[string]bool{}
	for _, i := range slices.Sorted(maps.Keys(models)) {
		w := models[i]
		injectModelArtifactAffinity(pod, w.Affinity)
		if w.Render == nil || preferred[w.Render.ManifestDigest] {
			continue
		}
		if term := w.placementPreference(ctx, r.Client); term != nil {
			preferred[w.Render.ManifestDigest] = true
			injectModelPlacementPreference(pod, term)
		}
	}

	systemmeta.NoteResource(pod, "instances", nil)
	kubemeta.ControlOnWithoutBlock(pod, inst, workercore.SchemeGroupVersionKind("Instance"))

	return pod
}

// convertAdditionalVolumes 把每个附加卷转换为 Pod 的卷及对应的主容器挂载。
// 卷名由索引生成，可避开 workspace 和 sshd-authorized-keys，也保证卷与挂载的名称相同。
// 缺少来源的条目由准入拒绝，这里跳过它，避免生成整个 API 服务器都会拒绝的 Pod。
func convertAdditionalVolumes(
	avs []workercore.InstanceAdditionalVolume, models map[int]*modelArtifactWeights,
) (vols []core.Volume, mounts []core.VolumeMount) {
	if len(avs) == 0 {
		return nil, nil
	}

	vols = make([]core.Volume, 0, len(avs))
	mounts = make([]core.VolumeMount, 0, len(avs))
	for i := range avs {
		av := &avs[i]

		var vs core.VolumeSource
		readOnly, subPath := av.ReadOnly, av.SubPath
		switch {
		case av.Model != nil:
			// 模型权重一律只读。PVC 来源挂载模型目录，节点来源交给节点插件，镜像来源使用镜像卷。
			// 未成功解析的来源跳过。协调循环会先等待解析完成再创建 Pod。
			w := models[i]
			switch {
			case w == nil || w.Render == nil:
				continue
			case w.Render.Delivery == workercore.ModelDeploymentModelDeliveryPVC:
				vs.PersistentVolumeClaim = &core.PersistentVolumeClaimVolumeSource{
					ClaimName: w.Render.ClaimName, ReadOnly: true,
				}
				readOnly, subPath = true, w.Render.Path
			case w.Render.Delivery == workercore.ModelDeploymentModelDeliveryNode:
				vs = w.Render.nodeVolumeSource()
				readOnly, subPath = true, ""
			case w.Render.Delivery == workercore.ModelDeploymentModelDeliveryImage:
				vs.Image = &core.ImageVolumeSource{Reference: w.Render.ImageReference}
				readOnly, subPath = true, ""
			default:
				continue
			}
		case av.Persistent != nil:
			vs.PersistentVolumeClaim = &core.PersistentVolumeClaimVolumeSource{
				ClaimName: av.Persistent.Name,
			}
		case av.ConfigMap != nil:
			vs.ConfigMap = &core.ConfigMapVolumeSource{
				LocalObjectReference: *av.ConfigMap,
			}
		case av.Secret != nil:
			vs.Secret = &core.SecretVolumeSource{
				SecretName: av.Secret.Name,
			}
		case av.HostPath != nil:
			vs.HostPath = av.HostPath.DeepCopy()
		default:
			continue
		}

		name := additionalVolumeName(i)
		vols = append(vols, core.Volume{
			Name:         name,
			VolumeSource: vs,
		})
		mounts = append(mounts, core.VolumeMount{
			Name:      name,
			MountPath: av.MountPath,
			ReadOnly:  readOnly,
			SubPath:   subPath,
		})
	}

	return vols, mounts
}

// resolveInstanceModelVolumes 按附加卷索引解析模型。需要等待时返回原因，所有模型就绪时原因是空字符串。
func (r *InstanceReconciler) resolveInstanceModelVolumes(
	ctx context.Context, inst *workercore.Instance,
) (map[int]*modelArtifactWeights, string, error) {
	models := map[int]*modelArtifactWeights{}
	for i := range inst.Spec.AdditionalVolumes {
		av := &inst.Spec.AdditionalVolumes[i]
		if av.Model == nil {
			continue
		}
		// 一个 Instance 只有一个 Pod 挂载卷，单 Pod 使用的 PVC 也可用。
		// Instance 没有推理引擎替它下载模型仓库的权重，由节点交付。
		w, err := resolveModelArtifactWeights(ctx, r.Client, inst.Namespace, av.Model.ArtifactRef.Name, 1, true)
		if err != nil {
			return nil, "", err
		}
		if w.Blocked {
			return nil, w.Message, nil
		}
		// 镜像卷需要节点的 kubelet 和容器运行时配合。实例指定了节点，可在创建 Pod 前给出明确的不兼容原因。
		// 尚未确定节点时，只能等待 Pod 实际运行的结果。
		if w.Render.Delivery == workercore.ModelDeploymentModelDeliveryImage && inst.Spec.NodeName != "" {
			nd := new(core.Node)
			if err := r.Client.Get(ctx, ctrlcli.ObjectKey{Name: inst.Spec.NodeName}, nd, ctrlclix.WithoutQuorum); err != nil {
				if kerrors.IsNotFound(err) {
					return nil, fmt.Sprintf("the node %q this Instance pins does not exist", inst.Spec.NodeName), nil
				}
				return nil, "", fmt.Errorf("read node %q: %w", inst.Spec.NodeName, err)
			}
			if unsupported := modelImageRuntimeUnsupported(
				nd.Status.NodeInfo.KubeletVersion, nd.Status.NodeInfo.ContainerRuntimeVersion,
			); unsupported != "" {
				return nil, fmt.Sprintf("ModelArtifact %q is delivered as an image volume, and node %q cannot run one: "+
					"%s; pin another node or upgrade it", av.Model.ArtifactRef.Name, inst.Spec.NodeName, unsupported), nil
			}
		}
		models[i] = w
	}

	return models, "", nil
}

// instanceModelImageContainerdFloor 是支持挂载镜像卷的最低 containerd 版本。
var instanceModelImageContainerdFloor = utilversion.MustParseGeneric("2.1")

// modelImageRuntimeUnsupported 返回节点未达到的第一项镜像卷要求。kubelet 从 1.35 起默认启用 ImageVolume，
// containerd 从 2.1 起支持挂载。版本无法识别时返回原因，两项均满足时返回空字符串。
func modelImageRuntimeUnsupported(kubeletVersion, containerRuntimeVersion string) string {
	if !kubediscovery.SupportsFeature(&kubediscovery.Version{GitVersion: kubeletVersion}, kubediscovery.FeatureImageVolume) {
		return fmt.Sprintf("kubelet %q is below the 1.35 the ImageVolume feature turns on at", kubeletVersion)
	}
	runtime, runtimeVersion, found := strings.Cut(containerRuntimeVersion, "://")
	if !found || runtime != "containerd" {
		return fmt.Sprintf("the container runtime %q is not containerd, which is what mounts image volumes",
			containerRuntimeVersion)
	}
	if v, err := utilversion.ParseGeneric(runtimeVersion); err != nil || v.LessThan(instanceModelImageContainerdFloor) {
		return fmt.Sprintf("containerd %q is below the 2.1 that mounts image volumes from", runtimeVersion)
	}

	return ""
}

// additionalVolumeName 根据附加卷的索引生成 Pod 中的卷名。
func additionalVolumeName(i int) string {
	return "additional-" + strconvx.Itoa(i)
}

// getNodeHostname 从节点读取 kubernetes.io/hostname。某些集群的标签值和 Node 名称不同，
// 而 Pod 的选择器在创建后不会重新计算，因此缓存读失败后再直接向 API 服务器读取。
// 仍无法读取或缺少标签时回退为节点名。若选择器匹配不到节点，调度原因会反映在实例状态中。
func (r *InstanceReconciler) getNodeHostname(ctx context.Context, nodeName string) string {
	nd := new(core.Node)
	err := r.Client.Get(ctx, ctrlcli.ObjectKey{Name: nodeName}, nd,
		ctrlclix.WithoutQuorum)
	if err != nil {
		err = r.APIReader.Get(ctx, ctrlcli.ObjectKey{Name: nodeName}, nd,
			ctrlclix.WithoutQuorum)
		if err != nil {
			// 记录最后一次读取错误。Pod 将一直使用回退得到的选择器，而调度事件只能解释选择器为何匹配不到节点。
			ctrllog.FromContext(ctx).Error(err, "fetch node for pin", "node", nodeName)
			return nodeName
		}
	}
	// 两次读取都允许读取稍早的版本。kubelet 在注册节点时设置 hostname，正常情况下该标签随节点保持稳定。
	if hostname := nd.Labels[core.LabelHostname]; hostname != "" {
		return hostname
	}
	return nodeName
}

// convertServiceFromInstance 为实例端口构造 NodePort Service。所有者设为 Instance，停止时删除 Pod 仍能保留端口。
// Service 端口名由协议和端口号组成，以便编辑端口列表时保留已有分配。
// 只用实例的 part-of 标签选择 Pod，实例换队列或重建后仍能选中新 Pod。
func (r *InstanceReconciler) convertServiceFromInstance(
	inst *workercore.Instance,
) *core.Service {
	svc := &core.Service{
		// 深拷贝元数据，生成可用于创建新对象的副本
		ObjectMeta: kubemeta.SanitizeObjectMeta(inst.ObjectMeta),
		Spec: core.ServiceSpec{
			Selector: map[string]string{
				deviceplugin.InstancePartOfLabelKey: string(inst.UID),
			},
			Type: core.ServiceTypeNodePort,
		},
	}

	for i := range inst.Spec.Ports {
		p := inst.Spec.Ports[i]
		svc.Spec.Ports = append(svc.Spec.Ports, core.ServicePort{
			Name:       getPortName(p),
			Port:       p.Port,
			Protocol:   p.Protocol,
			TargetPort: intstr.FromInt32(p.Port),
		})
	}

	systemmeta.NoteResource(svc, "instances", nil)
	kubemeta.ControlOnWithoutBlock(svc, inst, workercore.SchemeGroupVersionKind("Instance"))

	return svc
}

// alignServiceToInstance 根据期望规格更新已有 Service，返回是否发生变化。
// 相同名称的端口保留 API 服务器分配的 NodePort，只有新端口留零等待分配，避免用户已有的地址失效。
func alignServiceToInstance(svc, desired *core.Service) bool {
	allocated := make(map[string]int32, len(svc.Spec.Ports))
	for i := range svc.Spec.Ports {
		allocated[svc.Spec.Ports[i].Name] = svc.Spec.Ports[i].NodePort
	}

	ports := make([]core.ServicePort, len(desired.Spec.Ports))
	for i := range desired.Spec.Ports {
		ports[i] = desired.Spec.Ports[i]
		ports[i].NodePort = allocated[ports[i].Name]
	}

	if svc.Spec.Type == desired.Spec.Type &&
		kubemeta.DeepEqual(svc.Spec.Selector, desired.Spec.Selector) &&
		kubemeta.DeepEqual(svc.Spec.Ports, ports) {
		return false
	}

	svc.Spec.Type = desired.Spec.Type
	svc.Spec.Selector = desired.Spec.Selector
	svc.Spec.Ports = ports

	return true
}

// 知道“该干活了”
func (r *InstanceReconciler) SetupController(_ context.Context, opts controller.SetupOptions) error {
	r.Client = opts.Manager.GetClient()
	r.APIReader = opts.Manager.GetAPIReader()

	// 建一个 3 秒去重窗口
	dedupWindow := ctrlhandlerx.NewDedupWindow[ctrlreconcile.Request]()

	// IMPORTANT: 注册 5 路监听
	return ctrl.NewControllerManagedBy(opts.Manager).
		Named("instance").
		// 监听 Instance
		For(
			&workercore.Instance{},
			ctrlbuilder.WithPredicates(
				ctrlpredicate.GenerationChangedPredicate{},
			),
		).
		// 监听 Pod
		Watches(
			// Pod 变化时，将对应的 Instance 加入协调队列。
			&core.Pod{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFuncWithWindow(
				3*time.Second,
				dedupWindow,
				r.enqueueInstanceWhenPodChanged,
			),
			ctrlbuilder.WithPredicates(
				// 只处理标记为实例资源的 Pod。
				ctrlpredicate.NewPredicateFuncs(func(obj ctrlcli.Object) bool {
					return systemmeta.MatchResource(obj, "instances")
				}),
			),
		).
		// 监听 Service
		Watches(
			// Service 变化时，按命名空间和名称重新检查对应的 Instance。
			&core.Service{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFuncWithWindow(
				3*time.Second,
				dedupWindow,
				r.enqueuePodWhenServiceChanged,
			),
			ctrlbuilder.WithPredicates(
				// 只处理标记为实例资源的 Service。
				ctrlpredicate.NewPredicateFuncs(func(obj ctrlcli.Object) bool {
					return systemmeta.MatchResource(obj, "instances")
				}),
			),
		).
		// 监听 InstanceType
		Watches(
			// InstanceType 开始删除时，重新检查使用它的所有实例，以便在队列排空前及时停止。
			// 排空策略本身由下面的 ClusterQueue 监听处理。
			&workercore.InstanceType{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFuncWithWindow(
				3*time.Second,
				dedupWindow,
				r.enqueueInstancesWhenInstanceTypeChanged,
			),
			ctrlbuilder.WithPredicates(
				// 只有新出现或变化的删除时间戳影响这里的停止判断。
				// 忽略规格创建以及 Devices 更新引起的常规规格状态变化。
				ctrlpredicate.Funcs{
					CreateFunc: func(e ctrlevent.CreateEvent) bool {
						return false
					},
					DeleteFunc: func(e ctrlevent.DeleteEvent) bool {
						return false
					},
					UpdateFunc: func(e ctrlevent.UpdateEvent) bool {
						oldInstType, newInstType := e.ObjectOld.(*workercore.InstanceType), e.ObjectNew.(*workercore.InstanceType)
						if newInstType.DeletionTimestamp == nil {
							return false
						}
						return oldInstType.DeletionTimestamp == nil ||
							!oldInstType.DeletionTimestamp.Equal(newInstType.DeletionTimestamp)
					},
				},
			),
		).
		// 监听 ClusterQueue
		Watches(
			// ClusterQueue 与规格同名。它进入排空或 StopPolicy 改变时，重新检查使用该规格的实例。
			// 普通队列状态更新不会改变停止意图。
			&kueue.ClusterQueue{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFuncWithWindow(
				3*time.Second,
				dedupWindow,
				r.enqueueInstancesWhenInstanceTypeChanged,
			),
			ctrlbuilder.WithPredicates(
				ctrlpredicate.Funcs{
					CreateFunc: func(e ctrlevent.CreateEvent) bool {
						cq, ok := e.Object.(*kueue.ClusterQueue)
						return ok && ptr.Deref(cq.Spec.StopPolicy, kueue.None) == kueue.HoldAndDrain
					},
					UpdateFunc: func(e ctrlevent.UpdateEvent) bool {
						oldCQ, ok1 := e.ObjectOld.(*kueue.ClusterQueue)
						newCQ, ok2 := e.ObjectNew.(*kueue.ClusterQueue)
						if !ok1 || !ok2 {
							return false
						}
						return ptr.Deref(oldCQ.Spec.StopPolicy, kueue.None) != ptr.Deref(newCQ.Spec.StopPolicy, kueue.None)
					},
					DeleteFunc: func(e ctrlevent.DeleteEvent) bool {
						return false
					},
				},
			),
		).
		Complete(r)
}

func (r *InstanceReconciler) enqueueInstanceWhenPodChanged(
	ctx context.Context,
	obj ctrlcli.Object,
) []ctrlreconcile.Request {
	logger := ctrllog.FromContext(ctx).
		WithValues("pod", ctrlcli.ObjectKeyFromObject(obj))

	if !systemmeta.MatchResource(obj, "instances") {
		logger.Error(nil, "mismatched resource type")
		return nil
	}

	reqs := []ctrlreconcile.Request{
		{
			NamespacedName: ctrlcli.ObjectKeyFromObject(obj),
		},
	}
	logger.V(2).Info("enqueue instance from pod", "requests", reqs)
	return reqs
}

func (r *InstanceReconciler) enqueuePodWhenServiceChanged(
	ctx context.Context,
	obj ctrlcli.Object,
) []ctrlreconcile.Request {
	logger := ctrllog.FromContext(ctx).
		WithValues("service", ctrlcli.ObjectKeyFromObject(obj))

	if !systemmeta.MatchResource(obj, "instances") {
		logger.Error(nil, "mismatched resource type")
		return nil
	}

	reqs := []ctrlreconcile.Request{
		{
			NamespacedName: ctrlcli.ObjectKeyFromObject(obj),
		},
	}
	logger.V(2).Info("enqueue instance from service", "requests", reqs)
	return reqs
}

func (r *InstanceReconciler) enqueueInstancesWhenInstanceTypeChanged(
	ctx context.Context,
	obj ctrlcli.Object,
) []ctrlreconcile.Request {
	logger := ctrllog.FromContext(ctx).
		WithValues("instancetype", obj.GetName())

	instances := new(workercore.InstanceList)
	if err := r.Client.List(ctx, instances); err != nil {
		logger.Error(err, "list instances of instance type")
		return nil
	}

	var reqs []ctrlreconcile.Request
	for i := range instances.Items {
		if instances.Items[i].Spec.Type != obj.GetName() {
			continue
		}
		reqs = append(reqs, ctrlreconcile.Request{
			NamespacedName: ctrlcli.ObjectKeyFromObject(&instances.Items[i]),
		})
	}
	if len(reqs) == 0 {
		return nil
	}

	logger.V(2).Info("enqueue instances from instance type", "requests", reqs)
	return reqs
}

// getPortName 将协议与端口号拼成小写名称，让容器端口和 Service 端口共享一个符合 Kubernetes 要求的标识。
func getPortName(port workercore.InstancePort) string {
	return strings.ToLower(fmt.Sprintf("%s-%d", port.Protocol, port.Port))
}

// IMPORTANT: 第一关，把算力需求翻译的地方，做 GPU 默认不切分、软切分、硬切分时，第一道要处理的地方
// getResourceRequirements 把同一规格下的容器资源需求转换为 requests 和 limits。
// withGeneral 控制 CPU、内存和临时存储。limit 始终保留用户预算。开启 withGeneralOvercommit 时，
// request 交给 ScaleToOvercommit 换算。加速器规格使用较小的 CPU 基数，避免 CPU 先于设备耗尽。
//
// 主容器使用通用资源和 withAccelerator，sshd 使用与它互斥的 withVisibility，按主容器的卡数申请同卡可见性。
// 两个设备分支都要求规格支持加速器且申请卡数为正。Instance 和 ModelDeployment 共用资源参数与规格，
// 两者都用这个函数设置主机资源、独占整卡请求和边车的设备可见性。
func getResourceRequirements(
	instRess *workercore.InstanceResources,
	instType *worker.InstanceType,
	withGeneral, withGeneralOvercommit bool,
	withAccelerator bool,
	withVisibility bool,
) core.ResourceRequirements {
	rr := core.ResourceRequirements{
		Limits:   core.ResourceList{},
		Requests: core.ResourceList{},
	}

	if withGeneral {
		for n, q := range map[core.ResourceName]resource.Quantity{
			core.ResourceCPU:              instRess.CPU,
			core.ResourceMemory:           instRess.RAM,
			core.ResourceEphemeralStorage: instRess.LocalStorage,
		} {
			rr.Limits[n] = q
			if withGeneralOvercommit {
				rr.Requests[n] = kuberequest.ScaleToOvercommit(n, q, instType.Spec.Acceleratable)
				continue
			}
			rr.Requests[n] = q
		}
	}

	requestAccelerator := instType.Spec.Acceleratable &&
		instRess.Accelerator != nil &&
		instRess.Accelerator.Sign() > 0
	if requestAccelerator {
		cardQ := *instRess.Accelerator
		manufacturer := instType.Status.Detail.Manufacturer
		partProfile := instRess.AcceleratorPartitionedProfile
		partCardResName := nodefeature.GetAcceleratableResourceName(manufacturer, workercore.DeviceAllocationModePartitioned)
		partProfileResName := nodefeature.GetAcceleratablePartitionedProfileResourceName(manufacturer, partProfile)
		switch {
		case withAccelerator:
			switch {
			case partProfile != "" && partCardResName != "" && partProfileResName != "":
				// 第 11 章在这里填写硬件分区及其 profile 对应的资源。
			case instType.Status.Detail.IsLogicallySliceable() && instRess.AcceleratorSlicedMemoryPercentage > 0:
				// 写入 .sliced 卡数和每卡的显存、算力百分比。Pod webhook 在持久化前把显存百分比折成 .sliced.units，
				// Kueue 再用卡数和每卡 units 计算 credits。
				slicedResName := nodefeature.GetAcceleratableResourceName(manufacturer, workercore.DeviceAllocationModeSliced)
				memResName := nodefeature.GetAcceleratableSlicedMemoryPercentageResourceName(manufacturer)
				coresResName := nodefeature.GetAcceleratableSlicedCoresPercentageResourceName(manufacturer)
				memQ := *resource.NewQuantity(int64(instRess.AcceleratorSlicedMemoryPercentage), resource.DecimalSI)
				coresQ := *resource.NewQuantity(int64(instRess.AcceleratorSlicedCoresPercentage), resource.DecimalSI)
				rr.Limits[slicedResName] = cardQ
				rr.Requests[slicedResName] = cardQ
				rr.Limits[memResName] = memQ
				rr.Requests[memResName] = memQ
				rr.Limits[coresResName] = coresQ
				rr.Requests[coresResName] = coresQ
			default:
				// IMPORTANT: 第一次处理 GPU 资源，让 K8S 认识普通的 GPU 资源 （就是没有逻辑切分和硬切分的情况）
				// 不支持逻辑切片的规格，或者请求的切片比例为零，都按独占整卡处理。若厂商不支持请求的硬件分区，
				// 此处也没有对应的分区资源键，webhook 会拒绝这类请求。
				resName := nodefeature.GetAcceleratableResourceName(manufacturer, workercore.DeviceAllocationModeExclusive)
				rr.Limits[resName] = cardQ
				rr.Requests[resName] = cardQ
			}
		case withVisibility:
			// SSH 边车按主容器的卡数请求可见性。设备插件据此授予边车访问同一批设备的权限。切片的资源份额仍由主容器申请。
			visResName := nodefeature.GetAcceleratableResourceName(manufacturer, workercore.DeviceAllocationModeVisibility)
			rr.Limits[visResName] = cardQ
			rr.Requests[visResName] = cardQ
		}
	}

	return rr
}

// PartitionProfileMemoryPercent 返回指定硬件分区规格占单卡显存的比例，以 1 到 100 的整数百分比表示。
//
// 若池提供该规格，但探测或设备管理器滚动升级期间尚未上报每实例显存或单卡总显存，
// 则返回 sizeable=false。调用方应提示稍后重试，避免按整卡计算。
//
// 请求不涉及分区，或池未提供指定的分区规格时，返回 (0, true)。后者属于规格不匹配，
// 调用方会在校验时列出实际提供的规格。
//
// Instance webhook 和 ModelDeployment 渲染器共用此函数，因此把它放在 getResourceRequirements 旁。
// 前者把默认值写入 Instance，后者的 webhook 只补卡数，渲染时才换算规格。
// 共用算法可以让 Pod 的主机 CPU 和内存与实际占用的卡份额对应。
func PartitionProfileMemoryPercent(
	instType *workercore.InstanceType, profile string,
) (pct int64, sizeable bool) {
	if profile == "" {
		return 0, true
	}
	prof, _, found := PartitionProfile(instType, profile)
	if !found {
		return 0, true
	}
	if prof.MemoryMib <= 0 {
		return 0, false
	}
	cardVRAMMib, err := InstanceTypeCardVRAMMib(instType)
	if err != nil || cardVRAMMib <= 0 {
		return 0, false
	}
	return min(max(prof.MemoryMib*100/cardVRAMMib, 1), 100), true
}

// PartitionProfile 在 Status.Detail 的物理分区清单中查找指定规格。
// 结果包含每个实例的 MemoryMib、池内实例总上限 Count，以及是否已生成加速器详情、是否找到该规格。
func PartitionProfile(
	instType *workercore.InstanceType, profile string,
) (prof workercore.AcceleratorSlicedPhysicalDetailProfile, ready, found bool) {
	ready = instType.Status.Detail.AcceleratorReady()
	for _, p := range instType.Status.Detail.SlicedDetail.Physical.Profiles {
		if p.Name == profile {
			return p, ready, true
		}
	}
	return workercore.AcceleratorSlicedPhysicalDetailProfile{}, ready, false
}

// InstanceTypeCardVRAMMib 从 Status.Detail.Memory 解析单卡显存，单位为 MiB。空值表示尚未就绪，
// 可以重试。非正数或无法解析的值会返回错误。
func InstanceTypeCardVRAMMib(instType *workercore.InstanceType) (int64, error) {
	memStr := instType.Status.Detail.Memory
	if memStr == "" {
		return 0, fmt.Errorf("instance type %s has no per-card memory yet (detail not ready)", instType.Name)
	}
	q, err := resource.ParseQuantity(memStr)
	if err != nil {
		return 0, fmt.Errorf("parse memory %q of instance type %s: %w", memStr, instType.Name, err)
	}
	mib := q.Value() / quantityx.Mi
	if mib <= 0 {
		return 0, fmt.Errorf("instance type %s has non-positive memory %q", instType.Name, memStr)
	}
	return mib, nil
}
