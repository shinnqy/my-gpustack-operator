package worker

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"time"

	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/setting"
	"gpustack.ai/gpustack/pkg/systemmeta"
	"gpustack.ai/gpustack/pkg/utils/mapx"
	"gpustack.ai/gpustack/pkg/worker/settings"
)

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
	// 在第 18 章中实现此函数。
	var result0 ctrl.Result
	var result1 error
	return result0, result1
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

// requiredNodes 返回满足预取所需的就绪节点数：即 MinReady，当其保持默认值 0 时为全部目标节点数。
func requiredNodes(pf *workercore.ModelPrefetch, desired int32) int32 {
	if pf.Spec.MinReady == 0 || pf.Spec.MinReady > desired {
		return desired
	}

	return pf.Spec.MinReady
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
	// 在第 18 章中实现此函数。
	var result0 []string
	var result1 error
	return result0, result1
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
