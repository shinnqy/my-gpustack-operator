package worker

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlrecord "k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlcontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	ctrlcontrollerutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/controller"
	"gpustack.ai/gpustack/pkg/kubeapistatus"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/modelartifact"
	"gpustack.ai/gpustack/pkg/utils/ctrlhandlerx"
	"gpustack.ai/gpustack/pkg/worker/kuberess"
	"gpustack.ai/gpustack/pkg/worker/settings"
)

const (
	// ModelArtifactConditionResolved 表示来源已绑定到其不可变标识，且最近一次访问检查通过。
	// 在其不为 True 期间，消费方不会创建新的 Pod。
	ModelArtifactConditionResolved kubeapistatus.ConditionType = "Resolved"
	// ModelArtifactConditionDegraded 表示解析或访问检查正在失败，包括尚未吊销访问权限的失败情形。
	ModelArtifactConditionDegraded kubeapistatus.ConditionType = "Degraded"

	// ModelArtifactProtectionFinalizer 使仍被引用的 ModelArtifact 保持在 Terminating 状态，
	// 直到最后一个引用消失，其模式与 PVC 保护一致。
	ModelArtifactProtectionFinalizer = "worker.gpustack.ai/model-artifact-protection"

	modelArtifactReasonResolved      = "Resolved"
	modelArtifactReasonResolving     = "Resolving"
	modelArtifactReasonSecretMissing = "SecretNotFound"
	modelArtifactReasonClaimMissing  = "ClaimNotFound"
	modelArtifactReasonHealthy       = "Healthy"

	// modelArtifactTokenKey 是读取令牌的 Secret 键名，沿用了 TopologySource 读取 Bearer Token
	// 的约定。
	modelArtifactTokenKey = "token"

	// modelArtifactConfirmDelay 是首次访问检查失败后，在触发用于吊销访问权限的第二次检查前
	// 的等待时长。单次失败绝不会直接吊销权限：一次请求被拒并不等同于授权已被撤销。
	modelArtifactConfirmDelay = time.Minute
	// modelArtifactUnavailableRetry 控制在完全无法连接到源站时的重试节奏。
	modelArtifactUnavailableRetry = time.Minute
	// modelArtifactRefusedRetry 控制在源站拒绝解析时的重试节奏。通常可通过修改 Secret 来修复
	// （系统已 watch Secret）；而在 Hub 侧授予权限不会被 watch，因此同样需要定时重试。
	modelArtifactRefusedRetry = 10 * time.Minute
	// modelArtifactMaxConcurrentReconciles 限制同时与 Hub 通信的 ModelArtifact 数量上限。
	modelArtifactMaxConcurrentReconciles = 4
	// modelArtifactResolveTimeout 限制单次完整解析（包括所有目录树分页）的总超时时间。
	modelArtifactResolveTimeout = 5 * time.Minute
)

// ModelArtifactReconciler 负责解析 ModelArtifact、重新验证其访问权限，并在仍被引用时
// 保护其不被删除。
type ModelArtifactReconciler struct {
	Client   ctrlcli.Client
	Recorder ctrlrecord.EventRecorder
	Now      func() time.Time
	// NewHuggingFace 根据当前 Settings 构建 Hub 客户端。测试中会替换该函数。
	NewHuggingFace func(ctx context.Context) (*modelartifact.HuggingFace, error)
	// NewModelScope 根据当前 Settings 构建 ModelScope 客户端。测试中会替换该函数。
	NewModelScope func(ctx context.Context) (modelArtifactHub, error)

	// checks 按 artifact UID 记录上次向源站请求时所用的 Secret resourceVersion 以及下次允许
	// 请求的时间。它用于控制向 Hub 发起调用的频率：该控制器也会因自身写入 status 以及引用它的
	// deployment 发生任何变更而触发，但这些都不是再次请求 Hub 的理由，只有 Secret 变更才是。
	// 重启丢失该记录最多导致每个 artifact 提前多检查一次，别无其他影响：是否吊销访问权限由
	// condition 决定。
	checks sync.Map
	// nodesWritten 按 artifact UID 记录上次写入 status.nodes 的时间，用于控制两次写入之间的
	// 时间窗口。重启丢失该记录最多导致每个 artifact 提前写入一次。
	nodesWritten sync.Map
}

var _ ctrlreconcile.Reconciler = (*ModelArtifactReconciler)(nil)

// modelArtifactHub 是 Hub 源站进行解析与重新验证的抽象接口。两个 Hub 共用同一套阶梯式
// 逻辑；仅接口端点和原因分类器有所不同，这些差异封装在各自的客户端实现中。
type modelArtifactHub interface {
	Resolve(ctx context.Context, repository, revision, token string, filter modelartifact.Filter) (modelartifact.Resolution, error)
	ListManifest(ctx context.Context, repository, commit, token string, filter modelartifact.Filter) (modelartifact.Manifest, error)
	Revalidate(ctx context.Context, repository, commit, token string) error
	ValidToken(ctx context.Context, token string) (bool, error)
}

func (r *ModelArtifactReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := ctrllog.FromContext(ctx)

	ma := new(workercore.ModelArtifact)
	if err := r.Client.Get(ctx, req.NamespacedName, ma); err != nil {
		return ctrl.Result{}, ctrlcli.IgnoreNotFound(err)
	}

	if ma.DeletionTimestamp != nil {
		return ctrl.Result{}, r.releaseModelArtifact(ctx, ma)
	}
	if !ctrlcontrollerutil.ContainsFinalizer(ma, ModelArtifactProtectionFinalizer) {
		ctrlcontrollerutil.AddFinalizer(ma, ModelArtifactProtectionFinalizer)
		if err := r.Client.Update(ctx, ma); err != nil {
			return ctrl.Result{}, err
		}
	}

	// 在向源站发起请求前会先单独写入 Resolving 状态：一次解析可能耗时数分钟，如果在此期间
	// artifact 没有任何 condition，看起来就像无人处理一样。
	if !ModelArtifactConditionResolved.Exists(ma) {
		ma.Status.ObservedGeneration = ma.Generation
		ModelArtifactConditionResolved.Unknown(ma, modelArtifactReasonResolving, "the source has not answered yet")
		// 此处的写入与下文写入响应结果时在遇到冲突时均返回空结果：由于 watch 未设置 predicate，
		// 导致冲突的那次变更本身就会作为事件再次触发调谐。
		if err := r.Client.Status().Update(ctx, ma); err != nil {
			return objectWriteResult(logger, err, "update model artifact status to resolving", ctrl.Result{})
		}
		return ctrl.Result{Requeue: true}, nil
	}

	before := ma.Status.DeepCopy()
	ma.Status.ObservedGeneration = ma.Generation

	var (
		result ctrl.Result
		check  *modelArtifactCheck
	)
	switch {
	case ma.Spec.Source.HuggingFace != nil:
		hub, err := r.NewHuggingFace(ctx)
		if err != nil {
			ModelArtifactConditionDegraded.True(ma, modelartifact.ReasonSourceUnavailable, err.Error())
			return ctrl.Result{RequeueAfter: modelArtifactUnavailableRetry}, nil
		}
		result, check = r.reconcileHubSource(ctx, ma, hub, ma.Spec.Source.HuggingFace)
	case ma.Spec.Source.ModelScope != nil:
		hub, err := r.NewModelScope(ctx)
		if err != nil {
			ModelArtifactConditionDegraded.True(ma, modelartifact.ReasonSourceUnavailable, err.Error())
			return ctrl.Result{RequeueAfter: modelArtifactUnavailableRetry}, nil
		}
		result, check = r.reconcileHubSource(ctx, ma, hub, ma.Spec.Source.ModelScope)
	case ma.Spec.Source.PersistentVolumeClaim != nil:
		result = r.reconcileClaim(ctx, ma)
	case ma.Spec.Source.Image != nil:
		r.reconcileImage(ma)
	default:
		// 准入阶段会拒绝其他所有形态；在规则生效前已存储的无效对象将保持未解析状态并说明原因。
		ModelArtifactConditionResolved.False(ma, "UnsupportedSource", "the source is not accepted in this version")
	}
	result = earliestResult(result, r.reconcileNodes(ctx, ma, before.Nodes))

	if !kubemeta.DeepEqual(before, &ma.Status) {
		if err := r.Client.Status().Update(ctx, ma); err != nil {
			// 写入失败意味着响应结果已丢失，因此必须立即重新请求源站，而不能等待限频记录指定的时间：
			// 否则状态未能落盘的解析任务将白白等待一整个重新验证周期。
			r.checks.Delete(ma.UID)
			return objectWriteResult(logger, err, "update model artifact status", ctrl.Result{})
		}
	}
	// 仅在结果成功落盘后才记录，原因同上。
	if check != nil {
		r.checks.Store(ma.UID, *check)
	}
	if !kubemeta.DeepEqual(before.Nodes, ma.Status.Nodes) {
		r.nodesWritten.Store(ma.UID, r.now())
	}

	return result, nil
}

// earliestResult 返回两次 pass 所请求结果中最早需要重新入队的那一个。直接重新入队
// 是最急迫的请求，因此只要任意一方请求了它，结果就会保留它。
func earliestResult(a, b ctrl.Result) ctrl.Result {
	switch {
	case a.Requeue || b.Requeue:
		return ctrl.Result{Requeue: true}
	case a.RequeueAfter == 0:
		return b
	case b.RequeueAfter == 0:
		return a
	case b.RequeueAfter < a.RequeueAfter:
		return b
	}
	return a
}

// releaseModelArtifact 在不再有任何对象引用该 artifact 时移除保护性 finalizer。
// 当某个引用消失时，ModelDeployment 和 Instance 的 watch 会将该 artifact 重新入队。
func (r *ModelArtifactReconciler) releaseModelArtifact(ctx context.Context, ma *workercore.ModelArtifact) error {
	if !ctrlcontrollerutil.ContainsFinalizer(ma, ModelArtifactProtectionFinalizer) {
		return nil
	}
	referrers, err := r.modelArtifactReferrers(ctx, ma)
	if err != nil {
		return err
	}
	if len(referrers) > 0 {
		ctrllog.FromContext(ctx).V(4).Info("model artifact is still referenced", "referrers", referrers)
		return nil
	}
	ctrlcontrollerutil.RemoveFinalizer(ma, ModelArtifactProtectionFinalizer)
	if err := r.Client.Update(ctx, ma); err != nil {
		return err
	}
	// UID 不会被复用，因此若不清理，已释放 artifact 的节流记录会一直保留到进程结束。
	r.checks.Delete(ma.UID)
	r.nodesWritten.Delete(ma.UID)

	return nil
}

// modelArtifactReferrers 列出同一 namespace 中引用该 artifact 的资源，格式为 "kind/name"。
func (r *ModelArtifactReconciler) modelArtifactReferrers(ctx context.Context, ma *workercore.ModelArtifact) ([]string, error) {
	var referrers []string

	mds := new(workercore.ModelDeploymentList)
	if err := r.Client.List(ctx, mds, ctrlcli.InNamespace(ma.Namespace)); err != nil {
		return nil, err
	}
	for i := range mds.Items {
		if ModelDeploymentArtifactName(&mds.Items[i]) == ma.Name {
			referrers = append(referrers, "ModelDeployment/"+mds.Items[i].Name)
		}
	}

	insts := new(workercore.InstanceList)
	if err := r.Client.List(ctx, insts, ctrlcli.InNamespace(ma.Namespace)); err != nil {
		return nil, err
	}
	for i := range insts.Items {
		for _, name := range InstanceArtifactNames(&insts.Items[i]) {
			if name == ma.Name {
				referrers = append(referrers, "Instance/"+insts.Items[i].Name)
				break
			}
		}
	}

	return referrers, nil
}

// ModelDeploymentArtifactName 返回 ModelDeployment 引用的 ModelArtifact 名称，若未引用则返回 ""。
func ModelDeploymentArtifactName(md *workercore.ModelDeployment) string {
	if md.Spec.Model.ArtifactRef == nil {
		return ""
	}

	return md.Spec.Model.ArtifactRef.Name
}

// InstanceArtifactNames 返回 Instance 的卷所引用的 ModelArtifact 名称列表。
func InstanceArtifactNames(inst *workercore.Instance) []string {
	var names []string
	for _, av := range inst.Spec.AdditionalVolumes {
		if av.Model != nil {
			names = append(names, av.Model.ArtifactRef.Name)
		}
	}

	return names
}

// reconcileClaim 解析 claim 来源：operator 从不读取 claim 中的实际内容，因此只需确认
// claim 是否存在即可。
func (r *ModelArtifactReconciler) reconcileClaim(ctx context.Context, ma *workercore.ModelArtifact) ctrl.Result {
	source := ma.Spec.Source.PersistentVolumeClaim
	pvc := new(core.PersistentVolumeClaim)
	err := r.Client.Get(ctx, ctrlcli.ObjectKey{Namespace: ma.Namespace, Name: source.ClaimName}, pvc)
	switch {
	case kerrors.IsNotFound(err):
		ModelArtifactConditionResolved.False(ma, modelArtifactReasonClaimMissing,
			fmt.Sprintf("PersistentVolumeClaim %q does not exist in this namespace", source.ClaimName))
		ModelArtifactConditionDegraded.True(ma, modelArtifactReasonClaimMissing,
			fmt.Sprintf("PersistentVolumeClaim %q does not exist in this namespace", source.ClaimName))
		return ctrl.Result{}
	case err != nil:
		ModelArtifactConditionDegraded.True(ma, modelartifact.ReasonSourceUnavailable,
			fmt.Sprintf("read PersistentVolumeClaim %q: %v", source.ClaimName, err))
		return ctrl.Result{RequeueAfter: modelArtifactUnavailableRetry}
	}

	if ma.Status.Resolved == nil {
		ma.Status.Resolved = &workercore.ModelArtifactResolved{ResolvedTime: meta.NewTime(r.now())}
	}
	ModelArtifactConditionResolved.True(ma, modelArtifactReasonResolved,
		fmt.Sprintf("PersistentVolumeClaim %q exists; its content is the user's", source.ClaimName))
	ModelArtifactConditionDegraded.False(ma, modelArtifactReasonHealthy, "")

	return ctrl.Result{}
}

// reconcileImage 解析镜像来源：准入阶段已固定镜像引用，因此解析结果即直接保存 spec 中的引用。
// 既不会读取镜像仓库，也不会调度重新校验，更不会由节点聚合状态——镜像数据由用户提供，Pod
// 调度到任意节点时均由 kubelet 拉取交付。
func (r *ModelArtifactReconciler) reconcileImage(ma *workercore.ModelArtifact) {
	if ma.Status.Resolved == nil {
		ma.Status.Resolved = &workercore.ModelArtifactResolved{ResolvedTime: meta.NewTime(r.now())}
	}
	ModelArtifactConditionResolved.True(ma, modelArtifactReasonResolved,
		"the image is the user's; the operator does not read it")
	ModelArtifactConditionDegraded.False(ma, modelArtifactReasonHealthy, "")
}

// modelArtifactCheck 表示单个 artifact 的检查节流记录。
type modelArtifactCheck struct {
	secretVersion string
	next          time.Time
}

// reconcileHubSource 先对 hub 来源执行一次解析，之后定期重新校验其访问权限。它返回在状态保存后
// 需要记录的节流条目；若本次未向来源发起请求，则返回 nil。
func (r *ModelArtifactReconciler) reconcileHubSource(
	ctx context.Context, ma *workercore.ModelArtifact, hub modelArtifactHub, source *workercore.ModelArtifactHubSource,
) (ctrl.Result, *modelArtifactCheck) {
	token, secretVersion, err := r.modelArtifactToken(ctx, ma.Namespace, source.SecretRef)
	switch {
	case errors.Is(err, errModelArtifactSecretMissing):
		// Secret 消失意味着凭据失效，因此已解析的 artifact 会立即变为不可用；否则将要读取它的推理引擎
		// 也同样无法完成下载。
		ModelArtifactConditionResolved.False(ma, modelArtifactReasonSecretMissing, err.Error())
		ModelArtifactConditionDegraded.True(ma, modelArtifactReasonSecretMissing, err.Error())
		return ctrl.Result{RequeueAfter: modelArtifactRefusedRetry}, nil
	case err != nil:
		// 无法读取 Secret 并不能说明授权状态，就像来源暂时不可达一样，因此绝不会吊销已解析状态。
		ModelArtifactConditionDegraded.True(ma, modelartifact.ReasonSourceUnavailable, err.Error())
		return ctrl.Result{RequeueAfter: modelArtifactUnavailableRetry}, nil
	}

	now := r.now()
	secretChanged := true
	if last, ok := r.checks.Load(ma.UID); ok {
		check := last.(modelArtifactCheck)
		secretChanged = check.secretVersion != secretVersion
		if !secretChanged && now.Before(check.next) {
			return ctrl.Result{RequeueAfter: check.next.Sub(now)}, nil
		}
	}

	ctx, cancel := context.WithTimeout(ctx, modelArtifactResolveTimeout)
	defer cancel()

	var after time.Duration
	if ma.Status.Resolved == nil {
		after = r.resolveHubSource(ctx, ma, hub, source, token, now)
	} else {
		after = r.revalidateHubSource(ctx, ma, hub, source, token, now, secretChanged)
	}

	return ctrl.Result{RequeueAfter: after}, &modelArtifactCheck{secretVersion: secretVersion, next: now.Add(after)}
}

// resolveHubSource 对来源执行一次解析，并报告下一次需要向来源发起请求的时间间隔。
func (r *ModelArtifactReconciler) resolveHubSource(
	ctx context.Context, ma *workercore.ModelArtifact, hub modelArtifactHub, source *workercore.ModelArtifactHubSource,
	token string, now time.Time,
) time.Duration {
	// 在第 16 章中实现此函数。
	return modelArtifactUnavailableRetry
}

// revalidateHubSource 检查对已解析 commit 的访问权限，并报告下一次请求来源的时间间隔。
// 已解析的 commit 和摘要绝不会重新解析。
//
// 只有在确认后，拒绝访问才会吊销已解析状态。首次被拒时会设置 Degraded 并记录发生时间，随后在
// modelArtifactConfirmDelay 后重试检查；若再次收到相同的拒绝响应，则将 Resolved 设为 False。
// 无法连通来源绝不会触发吊销，因为这不能说明授权是否失效。
func (r *ModelArtifactReconciler) revalidateHubSource(
	ctx context.Context, ma *workercore.ModelArtifact, hub modelArtifactHub, source *workercore.ModelArtifactHubSource,
	token string, now time.Time, secretChanged bool,
) time.Duration {
	interval := r.revalidateInterval(ctx)
	if ma.Status.Resolved.DigestSource == workercore.ModelArtifactDigestSourceExpected {
		// 带锚点的标识没有可供探测的 commit，也不假定存在对应的 hub；仅靠节流记录即可避免
		// reconciler 空转。
		return interval
	}
	if last := ma.Status.Resolved.LastValidatedTime; last != nil && ModelArtifactConditionResolved.IsTrue(ma) &&
		!ModelArtifactConditionDegraded.IsTrue(ma) && now.Before(last.Add(interval)) {
		// 尚无需立即检查：可能是节流记录已丢失，或者 Secret 已恢复为原先版本。
		if _, ok := r.checks.Load(ma.UID); !ok {
			return last.Add(interval).Sub(now)
		}
	}

	// 锚点使定期检查成为一次断言：在已解析的 commit 处重新列出文件树并重新规范化，因此若 hub 或
	// 镜像站不再提供所固定的内容，检查会以与解析阶段相同的方式失败。无锚点的 artifact 则继续使用
	// 两步请求探测。
	var err error
	if anchor := ma.Spec.ExpectedDigest; anchor != "" {
		manifest, listErr := hub.ListManifest(ctx, source.Repository, ma.Status.Resolved.Revision, token,
			modelartifact.Filter{Allow: ma.Spec.AllowPatterns, Ignore: ma.Spec.IgnorePatterns})
		switch {
		case listErr != nil:
			err = listErr
		case manifest.Digest != anchor:
			err = &modelartifact.SourceError{
				Reason: modelartifact.ReasonDigestMismatch,
				Message: fmt.Sprintf("the artifact expects %s, the hub resolves %s@%s to %s",
					anchor, source.Repository, ma.Status.Resolved.Revision, manifest.Digest),
			}
		}
	} else {
		err = hub.Revalidate(ctx, source.Repository, ma.Status.Resolved.Revision, token)
	}
	if err == nil {
		validated := meta.NewTime(now)
		ma.Status.Resolved.LastValidatedTime = &validated
		ModelArtifactConditionResolved.True(ma, modelArtifactReasonResolved, "access confirmed at the resolved commit")
		ModelArtifactConditionDegraded.False(ma, modelArtifactReasonHealthy, "")
		// 仅在 token 更新时对其进行检查，而非每次周期检查都判断：未发生变化的被拒 token 此前已报告过，
		// 每个周期再发一次事件只会造成重复告警。
		if secretChanged {
			r.warnOnRejectedToken(ctx, ma, source, hub, token)
		}
		return interval
	}

	reason := modelartifact.ReasonOf(err)
	if !modelArtifactRevokingReason(reason) {
		ModelArtifactConditionDegraded.True(ma, reason, err.Error())
		return modelArtifactUnavailableRetry
	}

	first := !ModelArtifactConditionDegraded.IsTrue(ma) || ModelArtifactConditionDegraded.GetReason(ma) != reason
	if first {
		ModelArtifactConditionDegraded.True(ma, reason,
			fmt.Sprintf("%v; access is checked again in %s and revoked if refused again", err, modelArtifactConfirmDelay))
		// 显式记录时间戳：Degraded 可能已因其他原因处于 True，而确认延迟必须从此次拒绝开始计算，
		// 而非沿用之前的时间。
		ModelArtifactConditionDegraded.LastTransitionTime(ma, now.UTC().Format(time.RFC3339))
		return modelArtifactConfirmDelay
	}
	if wait := modelArtifactConditionSince(ma, ModelArtifactConditionDegraded).Add(modelArtifactConfirmDelay).Sub(now); wait > 0 {
		return wait
	}
	ModelArtifactConditionResolved.False(ma, reason, err.Error())
	ModelArtifactConditionDegraded.True(ma, reason, err.Error())

	return modelArtifactRefusedRetry
}

// modelArtifactRevokingReason 报告失败的访问检查是否可吊销已解析状态：明确拒绝可以，无法连通
// 来源则不能。摘要不匹配也可以吊销，因为 hub 已经作出响应——只是其返回并非 artifact 所断言的内容。
func modelArtifactRevokingReason(reason string) bool {
	return reason == modelartifact.ReasonAccessDenied || reason == modelartifact.ReasonRevisionNotFound ||
		reason == modelartifact.ReasonDigestMismatch
}

func modelArtifactConditionSince(ma *workercore.ModelArtifact, c kubeapistatus.ConditionType) time.Time {
	for _, cond := range ma.Status.Conditions {
		if cond.Type == string(c) {
			return cond.LastTransitionTime.Time
		}
	}

	return time.Time{}
}

// warnOnRejectedToken 在 hub 直接拒绝 token 时发出 Warning 事件。对于公开仓库，hub 会忽略
// 无效 token 并按未携带 token 处理，若不在此检查，拼写错误的 token 将永远不会暴露。该检查
// 不会修改 condition。
func (r *ModelArtifactReconciler) warnOnRejectedToken(
	ctx context.Context, ma *workercore.ModelArtifact, source *workercore.ModelArtifactHubSource,
	hub modelArtifactHub, token string,
) {
	if token == "" || r.Recorder == nil || source.SecretRef == nil {
		return
	}
	valid, err := hub.ValidToken(ctx, token)
	if err != nil || valid {
		return
	}
	r.Recorder.Eventf(ma, core.EventTypeWarning, "InvalidToken",
		"the hub rejects the token in Secret %q; a public repository still resolves, as if no token had been sent",
		source.SecretRef.Name)
}

var errModelArtifactSecretMissing = errors.New("secret missing")

// modelArtifactToken 读取 hub 来源所指定的 token 及其所在 Secret 的 resourceVersion。若未设置
// 引用，则返回空 token 且不报错。
func (r *ModelArtifactReconciler) modelArtifactToken(
	ctx context.Context, namespace string, ref *core.LocalObjectReference,
) (token, version string, err error) {
	if ref == nil {
		return "", "", nil
	}
	secret := new(core.Secret)
	if err := r.Client.Get(ctx, ctrlcli.ObjectKey{Namespace: namespace, Name: ref.Name}, secret); err != nil {
		if kerrors.IsNotFound(err) {
			return "", "", fmt.Errorf("%w: Secret %q does not exist in this namespace", errModelArtifactSecretMissing, ref.Name)
		}
		return "", "", fmt.Errorf("read Secret %q: %w", ref.Name, err)
	}
	token = strings.TrimSpace(string(secret.Data[modelArtifactTokenKey]))
	if token == "" {
		return "", "", fmt.Errorf("%w: Secret %q has no %q key", errModelArtifactSecretMissing, ref.Name, modelArtifactTokenKey)
	}

	return token, secret.ResourceVersion, nil
}

func (r *ModelArtifactReconciler) revalidateInterval(ctx context.Context) time.Duration {
	d, err := time.ParseDuration(settings.ModelArtifactRevalidateInterval.ShouldValue(ctx))
	if err != nil || d < time.Minute {
		return 24 * time.Hour
	}

	return d
}

func (r *ModelArtifactReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}

	return time.Now()
}

// newHuggingFaceFromSettings 根据管理员的 Settings 构建 Hub 客户端。
func (r *ModelArtifactReconciler) newHuggingFaceFromSettings(ctx context.Context) (*modelartifact.HuggingFace, error) {
	client, err := r.newHubHTTPClient(ctx)
	if err != nil {
		return nil, err
	}

	return &modelartifact.HuggingFace{
		Endpoint: settings.ModelArtifactHuggingFaceEndpoint.ShouldValue(ctx),
		Client:   client,
	}, nil
}

// newModelScopeFromSettings 根据管理员的 Settings 构建 ModelScope 客户端，使用与 Hugging Face
// 客户端相同的代理和 CA 证书包。
func (r *ModelArtifactReconciler) newModelScopeFromSettings(ctx context.Context) (modelArtifactHub, error) {
	client, err := r.newHubHTTPClient(ctx)
	if err != nil {
		return nil, err
	}

	return &modelartifact.ModelScope{
		Endpoint: settings.ModelArtifactModelScopeEndpoint.ShouldValue(ctx),
		Client:   client,
	}, nil
}

// newHubHTTPClient 构建供两个 hub 控制器发送请求使用的 HTTP 客户端。
func (r *ModelArtifactReconciler) newHubHTTPClient(ctx context.Context) (*http.Client, error) {
	opts := modelartifact.HTTPClientOptions{
		HTTPSProxy: settings.ModelArtifactHTTPSProxy.ShouldValue(ctx),
		NoProxy:    settings.ModelArtifactNoProxy.ShouldValue(ctx),
	}
	if name := settings.ModelArtifactCABundle.ShouldValue(ctx); name != "" {
		cm := new(core.ConfigMap)
		if err := r.Client.Get(ctx, ctrlcli.ObjectKey{Namespace: kuberess.SystemNamespaceName, Name: name}, cm); err != nil {
			return nil, fmt.Errorf("read the CA bundle ConfigMap %q: %w", name, err)
		}
		opts.CABundle = []byte(cm.Data["ca.crt"])
	}

	return modelartifact.NewHTTPClient(opts)
}

func (r *ModelArtifactReconciler) SetupController(ctx context.Context, opts controller.SetupOptions) error {
	// 该代理 Setting 的默认值来自环境变量，准入阶段不会读取它，且该值会被渲染进租户 Pod 中：
	// 因此应拒绝启动，避免将凭据渲染到租户 Pod 内。
	if v := settings.ModelArtifactHTTPSProxy.DefaultValue(); v != "" {
		if err := modelartifact.ValidateProxy(v); err != nil {
			return fmt.Errorf("setting %s: %w", settings.ModelArtifactHTTPSProxy.Name(), err)
		}
	}

	if err := opts.Manager.GetFieldIndexer().IndexField(
		ctx, &workercore.ModelArtifact{}, IndexingModelArtifactByManifestDigest, indexModelArtifactByManifestDigest,
	); err != nil {
		return fmt.Errorf("index model artifact '%s': %w", IndexingModelArtifactByManifestDigest, err)
	}
	if err := opts.Manager.GetFieldIndexer().IndexField(
		ctx, &workercore.NodeModelStore{}, IndexingNodeModelStoreByModelDigest, indexNodeModelStoreByModelDigest,
	); err != nil {
		return fmt.Errorf("index node model store '%s': %w", IndexingNodeModelStoreByModelDigest, err)
	}

	r.Client = opts.Manager.GetClient()
	r.Recorder = opts.Manager.GetEventRecorderFor("modelartifact")
	if r.NewHuggingFace == nil {
		r.NewHuggingFace = r.newHuggingFaceFromSettings
	}
	if r.NewModelScope == nil {
		r.NewModelScope = r.newModelScopeFromSettings
	}

	return ctrl.NewControllerManagedBy(opts.Manager).
		Named("modelartifact").
		For(&workercore.ModelArtifact{}).
		WithOptions(ctrlcontroller.Options{MaxConcurrentReconciles: modelArtifactMaxConcurrentReconciles}).
		Watches(&core.Secret{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFunc(time.Second, r.enqueueModelArtifactsForSecret)).
		Watches(&core.PersistentVolumeClaim{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFunc(time.Second, r.enqueueModelArtifactsForClaim)).
		Watches(&workercore.ModelDeployment{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFunc(time.Second, r.enqueueTerminatingModelArtifactOfDeployment)).
		Watches(&workercore.Instance{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFunc(time.Second, r.enqueueTerminatingModelArtifactsOfInstance)).
		Watches(&workercore.NodeModelStore{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFunc(time.Second, r.mapModelArtifactNodeModelStore)).
		Complete(r)
}

func (r *ModelArtifactReconciler) enqueueModelArtifactsForSecret(ctx context.Context, obj ctrlcli.Object) []ctrlreconcile.Request {
	return r.enqueueModelArtifacts(ctx, obj.GetNamespace(), func(ma *workercore.ModelArtifact) bool {
		for _, hub := range []*workercore.ModelArtifactHubSource{ma.Spec.Source.HuggingFace, ma.Spec.Source.ModelScope} {
			if hub != nil && hub.SecretRef != nil && hub.SecretRef.Name == obj.GetName() {
				return true
			}
		}
		return false
	})
}

func (r *ModelArtifactReconciler) enqueueModelArtifactsForClaim(ctx context.Context, obj ctrlcli.Object) []ctrlreconcile.Request {
	return r.enqueueModelArtifacts(ctx, obj.GetNamespace(), func(ma *workercore.ModelArtifact) bool {
		claim := ma.Spec.Source.PersistentVolumeClaim
		return claim != nil && claim.ClaimName == obj.GetName()
	})
}

func (r *ModelArtifactReconciler) enqueueModelArtifacts(
	ctx context.Context, namespace string, match func(*workercore.ModelArtifact) bool,
) []ctrlreconcile.Request {
	list := new(workercore.ModelArtifactList)
	if err := r.Client.List(ctx, list, ctrlcli.InNamespace(namespace)); err != nil {
		ctrllog.FromContext(ctx).Error(err, "list model artifacts")
		return nil
	}
	var reqs []ctrlreconcile.Request
	for i := range list.Items {
		if match(&list.Items[i]) {
			reqs = append(reqs, ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKeyFromObject(&list.Items[i])})
		}
	}

	return reqs
}

// enqueueTerminatingModelArtifactOfDeployment 与 enqueueTerminatingModelArtifactsOfInstance 唤醒
// 正在等待引用解除的 artifact。仅将处于 Terminating 状态的 artifact 入队：deployment 的状态会在
// 其自身控制器的每次调谐中变化，而这些变化与未被删除的 artifact 无关。
func (r *ModelArtifactReconciler) enqueueTerminatingModelArtifactOfDeployment(ctx context.Context, obj ctrlcli.Object) []ctrlreconcile.Request {
	md, ok := obj.(*workercore.ModelDeployment)
	if !ok {
		return nil
	}

	return r.enqueueTerminatingModelArtifacts(ctx, md.Namespace, ModelDeploymentArtifactName(md))
}

func (r *ModelArtifactReconciler) enqueueTerminatingModelArtifactsOfInstance(ctx context.Context, obj ctrlcli.Object) []ctrlreconcile.Request {
	inst, ok := obj.(*workercore.Instance)
	if !ok {
		return nil
	}

	return r.enqueueTerminatingModelArtifacts(ctx, inst.Namespace, InstanceArtifactNames(inst)...)
}

func (r *ModelArtifactReconciler) enqueueTerminatingModelArtifacts(
	ctx context.Context, namespace string, names ...string,
) []ctrlreconcile.Request {
	var reqs []ctrlreconcile.Request
	for _, name := range names {
		if name == "" {
			continue
		}
		key := ctrlcli.ObjectKey{Namespace: namespace, Name: name}
		ma := new(workercore.ModelArtifact)
		if err := r.Client.Get(ctx, key, ma); err != nil || ma.DeletionTimestamp == nil {
			continue
		}
		reqs = append(reqs, ctrlreconcile.Request{NamespacedName: key})
	}

	return reqs
}
