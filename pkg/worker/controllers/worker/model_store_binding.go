package worker

import (
	"context"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	ctrlbuilder "sigs.k8s.io/controller-runtime/pkg/builder"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlhandler "sigs.k8s.io/controller-runtime/pkg/handler"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	ctrlpredicate "sigs.k8s.io/controller-runtime/pkg/predicate"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/controller"
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
	// 在第 16 章中实现此函数。
	var result0 ctrl.Result
	var result1 error
	return result0, result1
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
