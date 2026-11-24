package worker

import (
	"context"
	"time"

	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlbuilder "sigs.k8s.io/controller-runtime/pkg/builder"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlhandler "sigs.k8s.io/controller-runtime/pkg/handler"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	ctrlpredicate "sigs.k8s.io/controller-runtime/pkg/predicate"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"

	gpustack "gpustack.ai/gpustack/api/v1"
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/controller"
	"gpustack.ai/gpustack/pkg/kubemeta"
)

// ModelStoreBinding 上报用量与预算状态时使用的条件。
const (
	// ModelStoreBindingConditionReady 表示本轮能完整核算预热用量，并且引用的存储都存在。
	ModelStoreBindingConditionReady = "Ready"
	// ModelStoreBindingConditionOverQuota 表示命名空间的预热占用超过授权预算。
	ModelStoreBindingConditionOverQuota = "OverQuota"
)

// ModelStoreBindingReconciler 使授权对象的状态与所属 Namespace 实际预热的数据保持一致：
// usedBytes 按完整大小统计该 Namespace 下任意预取任务所指定的每个去重 digest，计及持有它的每个目标 Node——
// 若某个工作负载将 digest 挂载到了预取从未请求的 Node 上，则该副本归入 Node 水位线，不计入此配额预算。
// 共享目录树会对每个预取指向它的 Namespace 分别计费，而没有任何 Node 持有的 digest 不计入。
//
// 若存在无法度量的贡献（主要指尚未解析的 artifact），则整体不输出该数值，而非给出残缺值：
// 静默排除某个预取的 usedBytes 会使 Namespace 看起来拥有并不存在的剩余空间。
type ModelStoreBindingReconciler struct {
	Client ctrlcli.Client
	Now    func() time.Time
}

var _ ctrlreconcile.Reconciler = (*ModelStoreBindingReconciler)(nil)

func (r *ModelStoreBindingReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := ctrllog.FromContext(ctx)

	binding := new(workercore.ModelStoreBinding)
	switch err := r.Client.Get(ctx, req.NamespacedName, binding); {
	case kerrors.IsNotFound(err):
		return ctrl.Result{}, nil
	case err != nil:
		return ctrl.Result{}, err
	}
	if binding.DeletionTimestamp != nil {
		return ctrl.Result{}, nil
	}

	pfs := new(workercore.ModelPrefetchList)
	if err := r.Client.List(ctx, pfs, ctrlcli.InNamespace(binding.Namespace)); err != nil {
		return ctrl.Result{}, err
	}
	stores := new(workercore.NodeModelStoreList)
	if err := r.Client.List(ctx, stores); err != nil {
		return ctrl.Result{}, err
	}
	nodes := new(core.NodeList)
	if err := r.Client.List(ctx, nodes); err != nil {
		return ctrl.Result{}, err
	}
	deps := new(workercore.ModelDeploymentList)
	if err := r.Client.List(ctx, deps, ctrlcli.InNamespace(binding.Namespace)); err != nil {
		return ctrl.Result{}, err
	}

	// holding[digest] 记录哪些节点报告了该摘要。下载中的文件也已经占用本地磁盘，因此不按模型状态过滤。
	holding := map[string]map[string]bool{}
	for i := range stores.Items {
		for _, m := range stores.Items[i].Status.Models {
			if holding[m.Digest] == nil {
				holding[m.Digest] = map[string]bool{}
			}
			holding[m.Digest][stores.Items[i].Name] = true
		}
	}

	// 一次读取命名空间中的全部制品。这样每次节点上报时，无需再为每个预热请求分别访问 API Server。
	artifacts := new(workercore.ModelArtifactList)
	if err := r.Client.List(ctx, artifacts, ctrlcli.InNamespace(binding.Namespace)); err != nil {
		return ctrl.Result{}, err
	}
	artifactByName := map[string]*workercore.ModelArtifact{}
	for i := range artifacts.Items {
		artifactByName[artifacts.Items[i].Name] = &artifacts.Items[i]
	}

	// charged[digest] 只收集预热请求选中且实际持有该摘要的节点。同一节点上的重复请求只计一次，
	// 其他节点因普通工作负载挂载而产生的副本由节点缓存水位管理。
	charged := map[string]map[string]bool{}
	sizes := map[string]int64{}
	var used int64
	measured := true
	for i := range pfs.Items {
		pf := &pfs.Items[i]
		artifact, ok := artifactByName[pf.Spec.ArtifactRef.Name]
		if !ok {
			// 预热请求引用的制品不存在时，无法确认完整用量。将这次结果标为未测量。
			measured = false
			continue
		}
		resolved := artifact.Status.Resolved
		if resolved == nil || resolved.ManifestDigest == "" || resolved.SizeBytes == 0 {
			measured = false
			continue
		}
		sizes[resolved.ManifestDigest] = resolved.SizeBytes

		pools, err := ResolveInstanceTypeFlavorNames(ctx, r.Client, pf, deps.Items)
		if err != nil {
			measured = false
			continue
		}
		targets, err := PrefetchTargetNodes(pf, nodes.Items, deps.Items, pools)
		if err != nil {
			// 放置条件无法解析时也不能确定应计哪些副本，因此本轮不发布部分用量。
			measured = false
			continue
		}
		if charged[resolved.ManifestDigest] == nil {
			charged[resolved.ManifestDigest] = map[string]bool{}
		}
		for _, node := range targets {
			if holding[resolved.ManifestDigest][node] {
				charged[resolved.ManifestDigest][node] = true
			}
		}
	}
	for digest, nodesCharged := range charged {
		used += sizes[digest] * int64(len(nodesCharged))
	}

	before := binding.Status.DeepCopy()
	if measured {
		u := resource.NewQuantity(used, resource.BinarySI)
		binding.Status.UsedBytes = u
	} else {
		binding.Status.UsedBytes = nil
	}

	quota := binding.Spec.Quota.Bytes.Value()
	over := gpustack.Condition{
		Type:               ModelStoreBindingConditionOverQuota,
		ObservedGeneration: binding.Generation,
	}
	ready := gpustack.Condition{
		Type:               ModelStoreBindingConditionReady,
		ObservedGeneration: binding.Generation,
	}
	storeMissing := false
	for _, ref := range binding.Spec.StoreRefs {
		if err := r.Client.Get(ctx, ctrlcli.ObjectKey{Name: ref.Name}, new(workercore.ModelStore)); err != nil {
			if kerrors.IsNotFound(err) {
				storeMissing = true
				break
			}

			return ctrl.Result{}, err
		}
	}
	switch {
	case !measured:
		ready.Status, ready.Reason = meta.ConditionFalse, "Unmeasured"
		ready.Message = "an artifact of this namespace's prefetches has not resolved, so the footprint cannot be counted"
		over.Status, over.Reason = meta.ConditionUnknown, "Unmeasured"
		over.Message = "the footprint is not fully counted, so over-quota cannot be judged yet"
	default:
		ready.Status, ready.Reason = meta.ConditionTrue, "Accounted"
		ready.Message = "the namespace's warm footprint is counted"
		if used > quota {
			over.Status, over.Reason = meta.ConditionTrue, "PastBudget"
			over.Message = "the namespace's warm footprint passed the grant; existing prefetches keep running, new ones are refused"
		} else {
			over.Status, over.Reason = meta.ConditionFalse, "WithinBudget"
			over.Message = "the namespace's warm footprint is within the grant"
		}
	}
	if storeMissing {
		ready.Status, ready.Reason = meta.ConditionFalse, "StoreMissing"
		ready.Message = "a store this grant names no longer exists; new prefetches are refused until the grant is fixed"
	}
	binding.Status.Phase = bindingStatusPhase(ready.Status, over.Status)
	binding.Status.PhaseMessage = ready.Message
	binding.Status.Conditions = carryConditionTransitions(binding.Status.Conditions,
		[]gpustack.Condition{ready, over}, r.nowOrNow())
	if kubemeta.DeepEqual(before, &binding.Status) {
		return ctrl.Result{}, nil
	}

	logger.V(2).Info("accounted the namespace's warm footprint",
		"binding", binding.Name, "usedBytes", used, "measured", measured, "storeMissing", storeMissing)

	return ctrl.Result{}, r.Client.Status().Update(ctx, binding)
}

// bindingStatusPhase 将 Ready 和 OverQuota 汇成供状态列展示的阶段。
// 已超过预算时优先显示 OverQuota。
func bindingStatusPhase(ready, over meta.ConditionStatus) string {
	if over == meta.ConditionTrue {
		return "OverQuota"
	}
	if ready == meta.ConditionTrue {
		return "Ready"
	}

	return "Error"
}

func (r *ModelStoreBindingReconciler) nowOrNow() time.Time {
	if r.Now != nil {
		return r.Now()
	}

	return time.Now()
}

func (r *ModelStoreBindingReconciler) SetupController(_ context.Context, opts controller.SetupOptions) error {
	r.Client = opts.Manager.GetClient()

	return ctrl.NewControllerManagedBy(opts.Manager).
		Named("modelstorebinding").
		For(&workercore.ModelStoreBinding{}, ctrlbuilder.WithPredicates(ctrlpredicate.GenerationChangedPredicate{})).
		Watches(
			&workercore.ModelPrefetch{},
			ctrlhandler.EnqueueRequestsFromMapFunc(r.enqueueAllBindings),
			ctrlbuilder.WithPredicates(ctrlpredicate.GenerationChangedPredicate{}),
		).
		Watches(
			&workercore.ModelArtifact{},
			ctrlhandler.EnqueueRequestsFromMapFunc(r.enqueueAllBindings),
			ctrlbuilder.WithPredicates(artifactResolutionChanged),
		).
		Watches(
			&workercore.NodeModelStore{},
			ctrlhandler.EnqueueRequestsFromMapFunc(r.enqueueAllBindings),
			ctrlbuilder.WithPredicates(nodeReportChanged),
		).
		Complete(r)
}

// enqueueAllBindings 将所有授权对象重新加入队列：配额核算按设计跨 Namespace 读取数据，且授权对象数量很少。
func (r *ModelStoreBindingReconciler) enqueueAllBindings(ctx context.Context, _ ctrlcli.Object) []ctrlreconcile.Request {
	bindings := new(workercore.ModelStoreBindingList)
	if err := r.Client.List(ctx, bindings); err != nil {
		ctrllog.FromContext(ctx).Error(err, "list model store bindings")

		return nil
	}

	reqs := make([]ctrlreconcile.Request, 0, len(bindings.Items))
	for i := range bindings.Items {
		reqs = append(reqs, ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKey{
			Namespace: bindings.Items[i].Namespace, Name: bindings.Items[i].Name,
		}})
	}

	return reqs
}
