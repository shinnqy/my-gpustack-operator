package worker

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlbuilder "sigs.k8s.io/controller-runtime/pkg/builder"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlevent "sigs.k8s.io/controller-runtime/pkg/event"
	ctrlhandler "sigs.k8s.io/controller-runtime/pkg/handler"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	ctrlpredicate "sigs.k8s.io/controller-runtime/pkg/predicate"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	gpustack "gpustack.ai/gpustack/api/v1"
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/controller"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/modelstore"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/setting"
	"gpustack.ai/gpustack/pkg/systemmeta"
	"gpustack.ai/gpustack/pkg/utils/mapx"
	"gpustack.ai/gpustack/pkg/utils/stringx"
	"gpustack.ai/gpustack/pkg/worker/settings"
)

// modelPrefetchPodsPerPass 限制单轮对账最多创建的预热 Pod 数量，避免针对大节点池预取时向 API Server 突发大量写入。
const modelPrefetchPodsPerPass = 8

// ModelPrefetch 状态的各 Condition。
const (
	// ModelPrefetchConditionProgressing 表示分发正在进行中：制品已解析，且部分目标节点尚未持有该内容。
	ModelPrefetchConditionProgressing = "Progressing"
	// ModelPrefetchConditionAvailable 表示已有足够多的目标节点持有该内容：ReadyNodes 已达到 MinReady 门槛
	// （MinReady 为 0 表示所有目标节点）。
	ModelPrefetchConditionAvailable = "Available"
	// ModelPrefetchConditionDegraded 表示有目标节点已放弃分发；message 中会列出具体原因。
	ModelPrefetchConditionDegraded = "Degraded"
	// ModelPrefetchConditionLapsed 表示 MinReady 缺口是由保留 TTL 导致的：本可补足缺口的副本已超过
	// TTLAfterLastUse 而过期，不再计为就绪且已被取消固定（unpin），也不会再被重新预热。只要缺口存在任何其他
	// 原因，该条件就会保持 False，从而避免过期状态掩盖仍在进行的预热或已失败的分发。
	ModelPrefetchConditionLapsed = "Lapsed"
)

// ModelPrefetchReconciler 将预取（prefetch）的驻留意图转化为每个目标节点上的一个预热 Pod，从目标节点
// 各自的上报中读取分发进度，并固定（pin）其保留策略要求保留的内容。
//
// 预热 Pod 本身即完成分发操作：它挂载与使用方路径相同的制品卷，因此节点缓存会在每个节点上恰好执行一次
// 下载、校验与发布；Pod 读回目录树后即退出并释放引用。它从不渲染 queue-name 标签，因此 Kueue 和
// operator 的 Pod webhook 都不会干预它。
type ModelPrefetchReconciler struct {
	Client ctrlcli.Client
	Now    func() time.Time
}

var _ ctrlreconcile.Reconciler = (*ModelPrefetchReconciler)(nil)

func (r *ModelPrefetchReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}

	return time.Now()
}

func (r *ModelPrefetchReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := ctrllog.FromContext(ctx)

	pf := new(workercore.ModelPrefetch)
	switch err := r.Client.Get(ctx, req.NamespacedName, pf); {
	case kerrors.IsNotFound(err):
		// 已删除的预取对象不在列表中，因此需通过并集重算释放它的 pin；此处执行可覆盖删除的恰好是最后一个预取对象的场景。
		return ctrl.Result{}, r.recomputePinned(ctx)
	case err != nil:
		return ctrl.Result{}, err
	}

	if pf.DeletionTimestamp != nil {
		// 预热 Pod 会随 OwnerReference 自动级联清理；pin 则通过并集重算移除，重算时不会再计入当前预取对象。
		return ctrl.Result{}, r.recomputePinned(ctx)
	}

	// 先计算目标节点集合：它在制品解析完成之前即可得出，且 status 中的 desiredNodes 取自该集合大小。
	nodes := new(core.NodeList)
	if err := r.Client.List(ctx, nodes); err != nil {
		return ctrl.Result{}, err
	}
	deps := new(workercore.ModelDeploymentList)
	if err := r.Client.List(ctx, deps, ctrlcli.InNamespace(pf.Namespace)); err != nil {
		return ctrl.Result{}, err
	}
	pools, err := ResolveInstanceTypeFlavorNames(ctx, r.Client, pf, deps.Items)
	if err != nil {
		return ctrl.Result{}, err
	}
	targets, err := prefetchTargetNodes(pf, nodes.Items, deps.Items, pools)
	if err != nil {
		// 无法解析的选择器本应被 API Server 的校验规则拒绝；此处将其记录为异常状态并等待下一次变更。
		logger.Error(err, "the prefetch's placement does not parse")
		before := pf.Status.DeepCopy()
		r.writeStatus(pf, prefetchProgress{}, err, false)
		return ctrl.Result{}, r.commitStatus(ctx, pf, before)
	}

	artifact := new(workercore.ModelArtifact)
	artifactErr := r.Client.Get(ctx,
		ctrlcli.ObjectKey{Namespace: pf.Namespace, Name: pf.Spec.ArtifactRef.Name}, artifact)
	artifactMissing := kerrors.IsNotFound(artifactErr)
	if artifactErr != nil && !artifactMissing {
		return ctrl.Result{}, artifactErr
	}
	digest := ""
	if artifactErr == nil && artifact.Status.Resolved != nil {
		digest = artifact.Status.Resolved.ManifestDigest
	}
	artifactReady := digest != ""

	// 分发事实来自各目标节点针对该摘要的上报条目，通过与制品节点视图共享的存储聚合函数读取。
	stores := new(workercore.NodeModelStoreList)
	if err := r.Client.List(ctx, stores); err != nil {
		return ctrl.Result{}, err
	}
	entries := modelstore.NodeEntries(stores.Items, digest)

	progress := prefetchProgress{desired: int32(len(targets))}
	expired := map[string]bool{}
	for _, node := range targets {
		switch entry, ok := entries[node]; {
		case !ok || entry.State == workercore.NodeModelStoreModelStateDownloading:
			progress.downloading++
		case entry.State == workercore.NodeModelStoreModelStateFailed:
			progress.failed++
		default: // 状态为 Ready，除非该节点副本已超过保留 TTL。
			if r.ttlExpired(pf, entry) {
				expired[node] = true
				progress.lapsed++
				continue
			}
			progress.ready = append(progress.ready, node)
		}
	}

	// 仅当制品已完成解析后才创建预热 Pod：在此之前所有目标节点都在等待解析结果，无需创建 Pod。
	var requeue ctrl.Result
	if artifactReady {
		image, err := r.warmupImage(ctx)
		if err != nil {
			return ctrl.Result{}, err
		}
		requeue, err = r.deliver(ctx, pf, artifact, digest, image, targets, progress.ready, entries, expired)
		if err != nil {
			return ctrl.Result{}, err
		}
	}

	before := pf.Status.DeepCopy()
	r.writeStatus(pf, progress, err, artifactMissing)
	if err := r.commitStatus(ctx, pf, before); err != nil {
		return ctrl.Result{}, err
	}

	// 每轮对账结束后整体重算 pin，确保已过期、重新定向或已删除的预取对象不会遗留 pin，
	// 同时也不会误删其他活跃预取对象仍需保留的 pin。
	if err := r.recomputePinned(ctx); err != nil {
		return ctrl.Result{}, err
	}

	if pf.Spec.Retention.TTLAfterLastUse != nil {
		// 节点上报的 TTL 过期粒度为小时级；按小时重新入队即可准确执行过期策略。
		return requeueOr(requeue, ctrl.Result{RequeueAfter: time.Hour}), nil
	}

	return requeueOr(requeue, ctrl.Result{}), nil
}

// prefetchProgress 是单轮对账中读取到的分发进度，是生成状态（status）的原始数据。
type prefetchProgress struct {
	desired     int32
	ready       []string
	downloading int32
	failed      int32
	// lapsed 统计副本处于 Ready 但已超过保留 TTL 的目标节点数量：这些副本不再计为就绪，其固定标记（pin）
	// 已被释放，且不会有预热 Pod 重新预热它们。
	lapsed int32
}

// prefetchTargetNodes 返回排好序的目标节点名。可以显式指定节点标签或 InstanceType。未指定放置条件时，
// 从当前命名空间引用此制品的部署推导 InstanceType。
func prefetchTargetNodes(
	pf *workercore.ModelPrefetch, nodes []core.Node, deps []workercore.ModelDeployment,
	pools InstanceTypeFlavorNames,
) ([]string, error) {
	placement := pf.Spec.Placement
	if placement != nil && len(placement.InstanceTypes) > 0 && placement.NodeSelector != nil {
		return nil, ErrPrefetchPlacementAmbiguous
	}
	typeNames := prefetchPlacementTypeNames(pf, deps)
	if placement != nil && placement.NodeSelector != nil {
		sel, err := meta.LabelSelectorAsSelector(placement.NodeSelector)
		if err != nil {
			return nil, err
		}
		var out []string
		for i := range nodes {
			if sel.Matches(labels.Set(nodes[i].Labels)) {
				out = append(out, nodes[i].Name)
			}
		}
		sort.Strings(out)

		return out, nil
	}

	// 先通过 InstanceType 对应的调度标签解析出资源池的 ResourceFlavor，再检查节点是否属于这些池。
	// Flavor 名含拓扑和数量信息，不能用 InstanceType 对象名代替。
	wanted := map[string]bool{}
	for _, name := range typeNames {
		for _, flavorName := range pools[name] {
			wanted[flavorName] = true
		}
	}
	var out []string
	for i := range nodes {
		for flavorName := range wanted {
			if matchNodeFlavor(&nodes[i], flavorName) != nil {
				out = append(out, nodes[i].Name)
				break
			}
		}
	}
	sort.Strings(out)

	return out, nil
}

// prefetchPlacementTypeNames 返回预取的 placement 所选择的 InstanceType 名称，或该 Namespace 下的
// 部署用于提供该制品服务的 InstanceType 名称。返回空表示由 placement 的 nodeSelector 决定目标节点集，
// 或没有任何可用于推导的来源。
func prefetchPlacementTypeNames(
	pf *workercore.ModelPrefetch, deps []workercore.ModelDeployment,
) []string {
	placement := pf.Spec.Placement
	switch {
	case placement != nil && len(placement.InstanceTypes) > 0 && placement.NodeSelector != nil:
		return nil
	case placement != nil && len(placement.InstanceTypes) > 0:
		return placement.InstanceTypes
	case placement != nil && placement.NodeSelector != nil:
		return nil
	default:
		// 推导出的集合：该 Namespace 自身部署中用于提供这些权重服务的全部 InstanceType，使预热自动跟随服务部署，
		// 无需在第二处重复指定位置。
		return deploymentInstanceTypes(deps, pf.Spec.ArtifactRef.Name)
	}
}

// deploymentInstanceTypes 收集该 Namespace 的部署中用于提供该制品权重的 InstanceType 名称。
func deploymentInstanceTypes(deps []workercore.ModelDeployment, artifactName string) []string {
	var out []string
	for i := range deps {
		ref := deps[i].Spec.Model.ArtifactRef
		if ref == nil || ref.Name != artifactName {
			continue
		}
		for _, role := range deps[i].Spec.Roles {
			if role.InstanceType != "" {
				out = append(out, role.InstanceType)
			}
		}
	}

	return out
}

// deliver 按受控批次为每个目标节点维持一个运行中的预热 Pod，并清理离开目标集合或已完成工作的 Pod。
// 替代 Pod 会使用新的生成名创建，因此即便被删除的旧 Pod 仍在终止宽限期内，新旧尝试也可短时共存。
//
// 节点自身的条目状态决定了已结束 Pod 删除后的后续动作。状态仍为 Downloading 或尚无条目的节点需要重新创建
// 预热 Pod；状态已为 Ready 或 Failed 的节点表示已经收敛——清理已结束的 Pod 且不再重建，避免确定性失败的
// 物化以事件速率不断重试，也避免 Pod 成功后状态上报延迟引发无意义的重复读回。
func (r *ModelPrefetchReconciler) deliver(ctx context.Context, pf *workercore.ModelPrefetch,
	artifact *workercore.ModelArtifact, digest, image string, targets, ready []string,
	entries map[string]workercore.NodeModelStoreModel, expired map[string]bool,
) (ctrl.Result, error) {
	want := map[string]bool{}
	for _, node := range targets {
		want[node] = true
	}

	readySet := map[string]bool{}
	for _, node := range ready {
		readySet[node] = true
	}
	failed := map[string]bool{}
	waiting := map[string]bool{}
	for _, node := range targets {
		if expired[node] {
			continue
		}
		entry, ok := entries[node]
		switch {
		case !ok || entry.State == workercore.NodeModelStoreModelStateDownloading:
			waiting[node] = true
		case entry.State == workercore.NodeModelStoreModelStateFailed:
			failed[node] = true
		}
	}

	pods := new(core.PodList)
	if err := r.Client.List(ctx, pods, ctrlcli.InNamespace(pf.Namespace),
		ctrlcli.MatchingLabels{"worker.gpustack.ai/model-prefetch": pf.Name}); err != nil {
		return ctrl.Result{}, err
	}

	created := 0
	requeue := ctrl.Result{}
	removed := map[string]bool{}
	for _, pod := range pods.Items {
		_, onTarget := want[pod.Spec.NodeName]
		// 若 Pod 挂载的摘要与制品当前解析结果不一致，立即视为陈旧并清理，避免节点一直占用旧权重。
		stale := podWarmDigest(&pod) != digest
		done := pod.Status.Phase == core.PodSucceeded || pod.Status.Phase == core.PodFailed
		settled := readySet[pod.Spec.NodeName] || failed[pod.Spec.NodeName] || expired[pod.Spec.NodeName]
		// 已收敛节点上的已结束 Pod 已完成使命；仍处于等待节点上的已结束 Pod 属于失败尝试，需清理后在旁重建替代尝试。
		if !onTarget || stale || (done && (settled || waiting[pod.Spec.NodeName])) {
			if err := r.Client.Delete(ctx, &pod); ctrlcli.IgnoreNotFound(err) != nil {
				return ctrl.Result{}, err
			}
			removed[pod.Name] = true
		}
	}

	// 被删除的 Pod 会在终止宽限期内保持 Terminating，因此以本轮列表扣除刚删除对象后的结果判断节点是否已有存活尝试。
	// 基于列表而非 Pod 名称判断，可避免正在终止的失效尝试把替代 Pod 阻塞到下一轮对账。
	live := map[string]bool{}
	for i := range pods.Items {
		if !removed[pods.Items[i].Name] {
			live[pods.Items[i].Spec.NodeName] = true
		}
	}

	for _, node := range targets {
		if !waiting[node] || live[node] {
			continue
		}
		if created >= modelPrefetchPodsPerPass {
			// 超出单轮上限的其余节点留待下一轮继续创建。
			requeue = ctrl.Result{RequeueAfter: 5 * time.Second}

			return requeue, nil
		}
		pod := warmupPod(pf, artifact, image, node, digest)
		kubemeta.ControlOn(pod, pf, workercore.SchemeGroupVersion.WithKind("ModelPrefetch"))
		if err := r.Client.Create(ctx, pod); err != nil {
			return ctrl.Result{}, err
		}
		created++
	}

	return requeue, nil
}

// bindingAllowsPinned 返回绑定对象生效的 allowPinned 值，nil 视为默认的 false。
func bindingAllowsPinned(binding *workercore.ModelStoreBinding) bool {
	return binding.Spec.AllowPinned != nil && *binding.Spec.AllowPinned
}

// recomputePinned 将每个节点的 spec.pinned 重写为全部存活预取对象所要求固定摘要的并集：
// 只要分发已就绪且未超过保留 TTL，摘要就会固定在该节点上，多个 Namespace 同时固定同一摘要时共同保留。
// 本控制器是该字段的唯一写入方，因此整表替换而非局部合并，确保从期望集合消失的 pin 在本轮结束后立即清除。
func (r *ModelPrefetchReconciler) recomputePinned(ctx context.Context) error {
	pfs := new(workercore.ModelPrefetchList)
	if err := r.Client.List(ctx, pfs); err != nil {
		return err
	}
	nodes := new(core.NodeList)
	if err := r.Client.List(ctx, nodes); err != nil {
		return err
	}
	stores := new(workercore.NodeModelStoreList)
	if err := r.Client.List(ctx, stores); err != nil {
		return err
	}
	// depsByNS 将推导型 placement 的展开范围限制在预取对象所属 Namespace 内，避免其他 Namespace 同名制品扩大目标集。
	deps := new(workercore.ModelDeploymentList)
	if err := r.Client.List(ctx, deps); err != nil {
		return err
	}
	depsByNS := map[string][]workercore.ModelDeployment{}
	for i := range deps.Items {
		ns := deps.Items[i].Namespace
		depsByNS[ns] = append(depsByNS[ns], deps.Items[i])
	}

	// 一次性列出所有绑定与制品并建索引，避免按预取对象逐条 Get 产生 2P 次请求往返。
	bindings := new(workercore.ModelStoreBindingList)
	if err := r.Client.List(ctx, bindings); err != nil {
		return err
	}
	bindingByKey := map[string]*workercore.ModelStoreBinding{}
	for i := range bindings.Items {
		b := &bindings.Items[i]
		bindingByKey[b.Namespace+"/"+b.Name] = b
	}
	artifacts := new(workercore.ModelArtifactList)
	if err := r.Client.List(ctx, artifacts); err != nil {
		return err
	}
	artifactByKey := map[string]*workercore.ModelArtifact{}
	for i := range artifacts.Items {
		a := &artifacts.Items[i]
		artifactByKey[a.Namespace+"/"+a.Name] = a
	}

	// desired[node] 记录所有保留策略仍生效的预取对象要求在该节点上固定的摘要集合。
	desired := map[string]map[string]bool{}
	for i := range pfs.Items {
		pf := &pfs.Items[i]
		if pf.DeletionTimestamp != nil || !pf.Spec.Retention.Pinned {
			continue
		}
		binding := bindingByKey[pf.Namespace+"/"+pf.Spec.BindingRef.Name]
		if binding == nil {
			continue // 授权已删除，不能再依据它维持固定状态。
		}
		if !bindingAllowsPinned(binding) {
			continue
		}

		artifact := artifactByKey[pf.Namespace+"/"+pf.Spec.ArtifactRef.Name]
		if artifact == nil || artifact.Status.Resolved == nil {
			continue
		}
		digest := artifact.Status.Resolved.ManifestDigest
		if digest == "" {
			continue
		}
		pools, err := ResolveInstanceTypeFlavorNames(ctx, r.Client, pf, depsByNS[pf.Namespace])
		if err != nil {
			return err
		}
		targets, err := prefetchTargetNodes(pf, nodes.Items, depsByNS[pf.Namespace], pools)
		if err != nil {
			continue // placement 配置无效，错误由该预取对象自身的状态体现。
		}
		entries := modelstore.NodeEntries(stores.Items, digest)
		for _, node := range targets {
			entry, ok := entries[node]
			entryReady := ok && entry.State == workercore.NodeModelStoreModelStateReady && !r.ttlExpired(pf, entry)
			if !entryReady {
				continue
			}
			if desired[node] == nil {
				desired[node] = map[string]bool{}
			}
			desired[node][digest] = true
		}
	}

	for i := range stores.Items {
		nms := &stores.Items[i]
		want := desired[nms.Name]
		if want == nil && len(nms.Spec.Pinned) == 0 {
			continue
		}
		fresh := make([]string, 0, len(want))
		for digest := range want {
			fresh = append(fresh, digest)
		}
		sort.Strings(fresh)
		if slices.Equal(nms.Spec.Pinned, fresh) {
			continue
		}
		updated := nms.DeepCopy()
		updated.Spec.Pinned = fresh
		if err := r.Client.Update(ctx, updated); err != nil {
			return err
		}
	}

	return nil
}

// ttlExpired 判断节点对该内容的最近使用时间是否已超过保留 TTL。从未上报过使用时间的节点不视为过期。
func (r *ModelPrefetchReconciler) ttlExpired(pf *workercore.ModelPrefetch, entry workercore.NodeModelStoreModel) bool {
	ttl := pf.Spec.Retention.TTLAfterLastUse
	if ttl == nil || entry.LastUsedTime == nil {
		return false
	}

	return r.now().Sub(entry.LastUsedTime.Time) > ttl.Duration
}

// writeStatus 根据进度填充状态。Available 要求 readyNodes 达到 MinReady（0 表示全部目标节点）；Degraded
// 在有目标节点放弃分发时置位——或当预取指定的制品消失时置位，从而终止分发而非一直拉取。Lapsed 表示
// 低于 MinReady 的缺口是由保留 TTL 导致的：仅当过期副本本身足以解释该缺口时才变为 True，以确保预热中
// 或失败的状态仍能独立体现。
func (r *ModelPrefetchReconciler) writeStatus(
	pf *workercore.ModelPrefetch, progress prefetchProgress, broken error, artifactMissing bool,
) {
	pf.Status.DesiredNodes = progress.desired
	pf.Status.ReadyNodes = int32(len(progress.ready))
	pf.Status.DownloadingNodes = progress.downloading

	required := requiredNodes(pf, progress.desired)
	delivered := int32(len(progress.ready)) >= required
	progressing := gpustack.Condition{
		Type:               ModelPrefetchConditionProgressing,
		ObservedGeneration: pf.Generation,
	}
	available := gpustack.Condition{
		Type:               ModelPrefetchConditionAvailable,
		Status:             meta.ConditionFalse,
		Reason:             "NotEnoughNodes",
		Message:            "fewer target nodes hold the content than the prefetch requires",
		ObservedGeneration: pf.Generation,
	}
	degraded := gpustack.Condition{
		Type:               ModelPrefetchConditionDegraded,
		Status:             meta.ConditionFalse,
		Reason:             "NoFailures",
		Message:            "no target node has given up",
		ObservedGeneration: pf.Generation,
	}
	lapsed := gpustack.Condition{
		Type:               ModelPrefetchConditionLapsed,
		Status:             meta.ConditionFalse,
		Reason:             "NoLapsedCopies",
		Message:            "no target node's copy has lapsed past retention.ttlAfterLastUse",
		ObservedGeneration: pf.Generation,
	}

	// 仅当过期副本单独即可填补低于要求的缺口时，才将缺口归因于过期：若过期副本无法弥补该缺口，则说明
	// 存在其他原因（仍在运行的预热或分发失败），Lapsed 绝不能掩盖这些原因。
	lead := fmt.Sprintf("%d of %d target node copies lapsed past retention.ttlAfterLastUse",
		progress.lapsed, progress.desired)
	switch gap := required - int32(len(progress.ready)); {
	case progress.lapsed == 0:
		// 无副本过期；保持默认值。
	case gap <= 0:
		lapsed.Reason = "NoShortfall"
		lapsed.Message = lead + ", but readyNodes still meets the requirement"
	case progress.lapsed >= gap:
		lapsed.Status, lapsed.Reason = meta.ConditionTrue, "RetentionTTLExpired"
		lapsed.Message = lead + " and no longer count toward readyNodes"
		if pf.Spec.Retention.Pinned {
			lapsed.Message = lead + " and their pins were released; they no longer count toward readyNodes"
		}
	default:
		lapsed.Reason = "ShortfallBeyondLapse"
		lapsed.Message = lead + ", but the shortfall below minReady is wider than the lapse; the rest is still warming or has failed"
	}

	switch {
	case broken != nil:
		progressing.Status, progressing.Reason = meta.ConditionFalse, "PlacementInvalid"
		progressing.Message = "the placement does not parse, so no target set exists"
	case artifactMissing:
		progressing.Status, progressing.Reason = meta.ConditionFalse, "ArtifactMissing"
		progressing.Message = "the artifact this prefetch names no longer exists"
		available.Status, available.Reason = meta.ConditionFalse, "ArtifactMissing"
		available.Message = "the artifact is gone, so the delivery cannot proceed"
		degraded.Status, degraded.Reason = meta.ConditionTrue, "ArtifactMissing"
		degraded.Message = "the named artifact was deleted after admission; create it again or delete the prefetch"
	case progress.desired == 0:
		progressing.Status, progressing.Reason = meta.ConditionFalse, "NoTargets"
		progressing.Message = "the placement matches no nodes"
	case delivered:
		progressing.Status, progressing.Reason = meta.ConditionFalse, "Delivered"
		progressing.Message = "every node the prefetch requires holds the content"
		available.Status, available.Reason = meta.ConditionTrue, "MinReadyReached"
		available.Message = "enough target nodes hold the content"
	default:
		progressing.Status, progressing.Reason = meta.ConditionTrue, "Delivering"
		progressing.Message = "some target nodes are still fetching or waiting on the artifact"
	}

	if progress.failed > 0 {
		degraded.Status, degraded.Reason = meta.ConditionTrue, "NodeFailed"
		degraded.Message = "one or more target nodes reported the delivery failed; their reasons are on the nodes' reports"
	}

	pf.Status.Conditions = carryConditionTransitions(pf.Status.Conditions,
		[]gpustack.Condition{progressing, available, degraded, lapsed}, r.now())
}

func (r *ModelPrefetchReconciler) commitStatus(
	ctx context.Context, pf *workercore.ModelPrefetch, before *workercore.ModelPrefetchStatus,
) error {
	if kubemeta.DeepEqual(before, &pf.Status) {
		return nil
	}

	return r.Client.Status().Update(ctx, pf)
}

// requiredNodes 返回满足预取所需的就绪节点数：即 MinReady，当其保持默认值 0 时为全部目标节点数。
func requiredNodes(pf *workercore.ModelPrefetch, desired int32) int32 {
	if pf.Spec.MinReady == 0 || pf.Spec.MinReady > desired {
		return desired
	}

	return pf.Spec.MinReady
}

// warmupImage 按照配置控制器读取层级的方式读取预热镜像配置：优先从委托 Secret 读取，未设置时回退到内置默认值。
func (r *ModelPrefetchReconciler) warmupImage(ctx context.Context) (string, error) {
	sec := new(core.Secret)
	key := ctrlcli.ObjectKey{Namespace: setting.DelegatedSecretNamespace, Name: setting.DelegatedSecretName}
	if err := r.Client.Get(ctx, key, sec); ctrlcli.IgnoreNotFound(err) != nil {
		return "", err
	}
	if v, ok := sec.Data[settings.ModelPrefetchWarmupImage.Name()]; ok {
		return string(v), nil
	}

	return settings.ModelPrefetchWarmupImage.DefaultValue(), nil
}

// warmupPod 将分发操作渲染为预热 Pod：挂载制品卷、绑定目标节点、无特权配置、读回目录树后退出。
// 它不带 queue-name 标签，因此 Kueue 与本 operator 的 Pod webhook 均不干预它。
// 每次尝试使用新的生成名，确保替代 Pod 与仍在终止中的旧 Pod 不会重名冲突。
func warmupPod(pf *workercore.ModelPrefetch, artifact *workercore.ModelArtifact, image, nodeName, digest string) *core.Pod {
	return &core.Pod{
		ObjectMeta: meta.ObjectMeta{
			GenerateName: prefetchPodName(pf.Name, nodeName) + "-",
			Namespace:    pf.Namespace,
			Labels: map[string]string{
				"worker.gpustack.ai/model-prefetch":        pf.Name,
				"worker.gpustack.ai/model-prefetch-digest": warmDigestLabel(digest),
			},
		},
		Spec: core.PodSpec{
			NodeName:                      nodeName,
			RestartPolicy:                 core.RestartPolicyNever,
			TerminationGracePeriodSeconds: ptr.To[int64](10),
			// 卡住的挂载或挂起的读回必须有终点：超时会使 Pod 失败，下一轮对账再根据条目状态决定是否重试。
			ActiveDeadlineSeconds: ptr.To[int64](2 * 60 * 60),
			SecurityContext: &core.PodSecurityContext{
				RunAsNonRoot:   ptr.To(true),
				RunAsUser:      ptr.To[int64](65534),
				SeccompProfile: &core.SeccompProfile{Type: core.SeccompProfileTypeRuntimeDefault},
			},
			Containers: []core.Container{{
				Name:    "warmup",
				Image:   image,
				Command: []string{"sh", "-c", "cd /model && find . -type f | sort | xargs -r sha256sum"},
				SecurityContext: &core.SecurityContext{
					AllowPrivilegeEscalation: ptr.To(false),
					Capabilities:             &core.Capabilities{Drop: []core.Capability{"ALL"}},
				},
				VolumeMounts: []core.VolumeMount{{Name: "model", MountPath: "/model", ReadOnly: true}},
			}},
			Volumes: []core.Volume{{
				Name: "model",
				VolumeSource: core.VolumeSource{
					CSI: &core.CSIVolumeSource{
						Driver:   modelstore.DriverName,
						ReadOnly: ptr.To(true),
						VolumeAttributes: map[string]string{
							modelstore.VolumeAttrArtifact:       artifact.Name,
							modelstore.VolumeAttrArtifactUID:    string(artifact.UID),
							modelstore.VolumeAttrManifestDigest: digest,
						},
					},
				},
			}},
		},
	}
}

// podWarmDigest 读取 Pod 预热卷中声明的摘要；若 Pod 缺少该属性或未声明对应卷则返回空字符串。
func podWarmDigest(pod *core.Pod) string {
	for _, v := range pod.Spec.Volumes {
		if v.CSI != nil && v.CSI.Driver == modelstore.DriverName {
			return v.CSI.VolumeAttributes[modelstore.VolumeAttrManifestDigest]
		}
	}

	return ""
}

// warmDigestLabel 将摘要折叠为 API Server 可接受的标签值：标签值上限为 63 个字符而完整摘要含 64 位十六进制，
// 因此此处取前 32 位作为可读标识；权威的摘要匹配仍以保存完整值的 CSI 卷属性为准。
func warmDigestLabel(digest string) string {
	hexPart := strings.TrimPrefix(digest, "sha256:")
	if len(hexPart) > 32 {
		hexPart = hexPart[:32]
	}

	return hexPart
}

// prefetchPodName 构造预热 Pod 的 generateName 前缀，由预取名与节点名拼接而成。
// 长度合法时保留可读拼接；超长时以两者哈希值收尾，既避免超长前缀引发校验失败，又防止两台节点名截断后发生碰撞。
// 247 的上限为 API Server 追加的连字符和 5 位随机后缀预留空间（253 - 1 - 5）。
func prefetchPodName(pf, node string) string {
	name := pf + "-warmup-" + node
	if len(name) <= 247 {
		return name
	}
	tag := stringx.SumBySHA256(pf + "/" + node)[:8]
	room := 247 - len(tag) - 1
	base := (pf + "-warmup-" + node)[:room]

	return base + "-" + tag
}

func requeueOr(chosen, fallback ctrl.Result) ctrl.Result {
	if chosen.RequeueAfter > 0 || chosen.Requeue {
		return chosen
	}

	return fallback
}

// InstanceTypeFlavorNames 按 InstanceType 名称保存该类型资源池所选择的 ResourceFlavor 名称。若某个类型
// 未解析出任何 flavor（未知类型或尚未同步出 flavor），则映射为空条目，使其目标节点集为空而非报错：placement
// 的契约是“所有带有该类型 flavor 的节点”，没有 flavor 的类型自然不对应任何节点。
type InstanceTypeFlavorNames map[string][]string

// ResolveInstanceTypeFlavorNames 将预取 placement 指定的 InstanceType 名称（或在默认形式下，该 Namespace
// 的部署用于提供该制品服务的类型名称）解析为各类型资源池所选择的 ResourceFlavor 名称：即携带该类型 spec
// 派生出的调度标签的 flavor，这与该类型背后的 ClusterQueue 及其状态 Detail 所执行的反向查找一致。
// flavor 名称包含拓扑限定和数量后缀，因此 InstanceType 的对象名称本身绝不是 flavor 名称，也不能直接与之比较。
func ResolveInstanceTypeFlavorNames(
	ctx context.Context, r ctrlcli.Reader, pf *workercore.ModelPrefetch, deps []workercore.ModelDeployment,
) (InstanceTypeFlavorNames, error) {
	typeNames := prefetchPlacementTypeNames(pf, deps)
	if len(typeNames) == 0 {
		return nil, nil
	}
	// 一次 List 查询即可服务所有指定类型：集群中只有少数由 operator 管理的 flavor，而一个类型的资源池
	// 只是该共享集合上的标签包含筛选。
	flavors := new(kueue.ResourceFlavorList)
	if err := r.List(ctx, flavors,
		systemmeta.GetResourcesLabelSetOfType[ctrlcli.MatchingLabels](_ResourceFlavorResType),
		ctrlcli.UnsafeDisableDeepCopy); err != nil {
		return nil, err
	}
	cpuAware := settings.InstanceTypeAwareCPUManufacturer.ShouldValueBool(ctx)
	pools := InstanceTypeFlavorNames{}
	for _, name := range typeNames {
		if _, resolved := pools[name]; resolved {
			continue
		}
		it := new(workercore.InstanceType)
		err := r.Get(ctx, ctrlcli.ObjectKey{Name: name}, it)
		if kerrors.IsNotFound(err) {
			pools[name] = nil
			continue
		}
		if err != nil {
			return nil, err
		}
		sched := nodefeature.PoolScheduleLabels(
			it.Spec.Acceleratable, cpuAware,
			it.Spec.GeneralGroup, it.Spec.AcceleratorGroup,
			it.Spec.OS, it.Spec.Arch,
		)
		var flavorNames []string
		for i := range flavors.Items {
			if mapx.Contain(flavors.Items[i].Labels, sched) {
				flavorNames = append(flavorNames, flavors.Items[i].Name)
			}
		}
		sort.Strings(flavorNames)
		pools[name] = flavorNames
	}

	return pools, nil
}

// PrefetchTargetNodes 将预取的 placement 解析为节点名称列表。它是控制器自身的展开逻辑，并与准入 Webhook
// 共享，确保准入校验与实际分发对预取所要预热的节点绝无分歧。pools 承载了由 ResolveInstanceTypeFlavorNames
// 解析出的各指定 InstanceType 的 flavor 映射。
func PrefetchTargetNodes(
	pf *workercore.ModelPrefetch, nodes []core.Node, deps []workercore.ModelDeployment,
	pools InstanceTypeFlavorNames,
) ([]string, error) {
	return prefetchTargetNodes(pf, nodes, deps, pools)
}

// Webhook 共用的 placement 拒绝错误：同时设置两种选择方式会产生歧义，而 schema 因两者均非必填而无法校验。
var ErrPrefetchPlacementAmbiguous = fmt.Errorf("placement sets both instanceTypes and nodeSelector")

// artifactResolutionChanged 仅在制品的解析结果发生变化时触发入队——这是记账和预取分发共同读取的事实。
// 描述和标签的频繁变动在此处无任何监听者关注。
var artifactResolutionChanged = ctrlpredicate.Funcs{
	CreateFunc: func(ctrlevent.CreateEvent) bool { return true },
	DeleteFunc: func(ctrlevent.DeleteEvent) bool { return true },
	UpdateFunc: func(e ctrlevent.UpdateEvent) bool {
		o, ok := e.ObjectOld.(*workercore.ModelArtifact)
		if !ok {
			return false
		}
		n, ok := e.ObjectNew.(*workercore.ModelArtifact)
		if !ok {
			return false
		}

		return !kubemeta.DeepEqual(o.Status.Resolved, n.Status.Resolved)
	},
	GenericFunc: func(ctrlevent.GenericEvent) bool { return false },
}

// nodeReportChanged 仅当节点的上报以预取关注的方式发生变化时才触发入队：即按 digest 组织的条目和 pin 列表。
// 插件顺带写入的水位线（watermark）记录由其他对象消费。
var nodeReportChanged = ctrlpredicate.Funcs{
	CreateFunc: func(ctrlevent.CreateEvent) bool { return true },
	DeleteFunc: func(ctrlevent.DeleteEvent) bool { return true },
	UpdateFunc: func(e ctrlevent.UpdateEvent) bool {
		o, ok := e.ObjectOld.(*workercore.NodeModelStore)
		if !ok {
			return false
		}
		n, ok := e.ObjectNew.(*workercore.NodeModelStore)
		if !ok {
			return false
		}

		return !kubemeta.DeepEqual(o.Status.Models, n.Status.Models) ||
			!kubemeta.DeepEqual(o.Spec.Pinned, n.Spec.Pinned)
	},
	GenericFunc: func(ctrlevent.GenericEvent) bool { return false },
}

// nodeLabelsChanged 仅在节点标签发生变化时触发入队：心跳带来的 status 写入既不决定哪些存储匹配节点，
// 也不决定预取的目标节点。ModelStore 协调器保留了同样形态的内联副本；两者都只读取标签。
var nodeLabelsChanged = ctrlpredicate.Funcs{
	CreateFunc: func(ctrlevent.CreateEvent) bool { return true },
	DeleteFunc: func(ctrlevent.DeleteEvent) bool { return true },
	UpdateFunc: func(e ctrlevent.UpdateEvent) bool {
		o, ok := e.ObjectOld.(*core.Node)
		if !ok {
			return false
		}
		n, ok := e.ObjectNew.(*core.Node)
		if !ok {
			return false
		}

		return !maps.Equal(o.Labels, n.Labels)
	},
	GenericFunc: func(ctrlevent.GenericEvent) bool { return false },
}

// SetupController 监听预取的目标集合或分发过程所依赖的一切资源：制品解析结果属于状态变更，部署决定了
// 推导出的目标集合，Node 本身即目标，而节点的上报则反映了就绪状态。
func (r *ModelPrefetchReconciler) SetupController(_ context.Context, opts controller.SetupOptions) error {
	r.Client = opts.Manager.GetClient()

	return ctrl.NewControllerManagedBy(opts.Manager).
		Named("modelprefetch").
		For(&workercore.ModelPrefetch{}, ctrlbuilder.WithPredicates(ctrlpredicate.GenerationChangedPredicate{})).
		Watches(
			&workercore.ModelArtifact{},
			ctrlhandler.EnqueueRequestsFromMapFunc(r.enqueuePrefetchesInNamespace),
			ctrlbuilder.WithPredicates(artifactResolutionChanged),
		).
		Watches(
			&workercore.ModelDeployment{},
			ctrlhandler.EnqueueRequestsFromMapFunc(r.enqueuePrefetchesInNamespace),
			ctrlbuilder.WithPredicates(ctrlpredicate.GenerationChangedPredicate{}),
		).
		Watches(
			&core.Node{},
			ctrlhandler.EnqueueRequestsFromMapFunc(r.enqueueAllPrefetches),
			ctrlbuilder.WithPredicates(nodeLabelsChanged),
		).
		Watches(
			&workercore.NodeModelStore{},
			ctrlhandler.EnqueueRequestsFromMapFunc(r.enqueueAllPrefetches),
			ctrlbuilder.WithPredicates(nodeReportChanged),
		).
		Watches(
			&core.Secret{},
			ctrlhandler.EnqueueRequestsFromMapFunc(r.enqueueAllPrefetches),
			ctrlbuilder.WithPredicates(ctrlpredicate.NewPredicateFuncs(func(o ctrlcli.Object) bool {
				return o.GetNamespace() == setting.DelegatedSecretNamespace && o.GetName() == setting.DelegatedSecretName
			})),
		).
		Complete(r)
}

// enqueueAllPrefetches 将所有预取对象入队：Node 及其上报信息属于集群级事实，任何预取都可能读取。
func (r *ModelPrefetchReconciler) enqueueAllPrefetches(ctx context.Context, _ ctrlcli.Object) []ctrlreconcile.Request {
	pfs := new(workercore.ModelPrefetchList)
	if err := r.Client.List(ctx, pfs); err != nil {
		ctrllog.FromContext(ctx).Error(err, "list model prefetches")

		return nil
	}

	reqs := make([]ctrlreconcile.Request, 0, len(pfs.Items))
	for i := range pfs.Items {
		reqs = append(reqs, ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKey{
			Namespace: pfs.Items[i].Namespace, Name: pfs.Items[i].Name,
		}})
	}

	return reqs
}

// enqueuePrefetchesInNamespace 将对象所在 Namespace 内的预取对象入队：制品的解析结果与部署的 placement
// 都会决定其所属 Namespace 中预取的行为。
func (r *ModelPrefetchReconciler) enqueuePrefetchesInNamespace(ctx context.Context, obj ctrlcli.Object) []ctrlreconcile.Request {
	pfs := new(workercore.ModelPrefetchList)
	if err := r.Client.List(ctx, pfs, ctrlcli.InNamespace(obj.GetNamespace())); err != nil {
		ctrllog.FromContext(ctx).Error(err, "list model prefetches")

		return nil
	}

	reqs := make([]ctrlreconcile.Request, 0, len(pfs.Items))
	for i := range pfs.Items {
		reqs = append(reqs, ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKey{
			Namespace: pfs.Items[i].Namespace, Name: pfs.Items[i].Name,
		}})
	}

	return reqs
}
