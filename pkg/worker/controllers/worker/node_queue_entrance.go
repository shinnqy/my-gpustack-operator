package worker

import (
	"context"
	"time"

	"go.uber.org/multierr"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlbuilder "sigs.k8s.io/controller-runtime/pkg/builder"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlevent "sigs.k8s.io/controller-runtime/pkg/event"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	ctrlpredicate "sigs.k8s.io/controller-runtime/pkg/predicate"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"gpustack.ai/gpustack/pkg/controller"
	"gpustack.ai/gpustack/pkg/kubeclientset"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/systemmeta"
	"gpustack.ai/gpustack/pkg/systemname"
	"gpustack.ai/gpustack/pkg/utils/ctrlclix"
	"gpustack.ai/gpustack/pkg/utils/ctrlhandlerx"
	"gpustack.ai/gpustack/pkg/worker/kuberess"
)

// NodeQueueEntranceReconciler 监听 ClusterQueue 和 Namespace 的创建。
// 它在每个可用的非系统命名空间创建 LocalQueue，把入口指向相应的全局队列。
//
// LocalQueue 记录队列入口，用户可在命名空间内修改它。后续 Pod webhook 用入口名反查 InstanceType，
// 单卡显存取自该规格的 Status.Detail.Memory。
//
// LocalQueue 使用 nodefeature.FormatLocalQueueName 计算全局队列名的 FNV-64a 哈希。
// ClusterQueue 的原名可能超过 kueue.x-k8s.io/queue-name 标签值的 63 字符限制。
type NodeQueueEntranceReconciler struct {
	Client ctrlcli.Client
}

var _ ctrlreconcile.Reconciler = (*NodeQueueEntranceReconciler)(nil)

const (
	// _LocalQueueClusterQueueNameAnnoKey 在 LocalQueue 注解中保存完整 ClusterQueue 名，
	// 方便从短入口名追溯全局队列。
	_LocalQueueClusterQueueNameAnnoKey = "schedule." + systemname.LabelPrefix + "queue"
)

// IMPORTANT: 把 ClusterQueue 铺到每个普通 Namespace，生成短名 LocalQueue
func (r *NodeQueueEntranceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := ctrllog.FromContext(ctx)

	// 读取此次需要同步的全局队列。
	cq := new(kueue.ClusterQueue)
	err := r.Client.Get(ctx, req.NamespacedName, cq)
	if err != nil {
		// ClusterQueue 已不存在时，本次同步结束。
		if err = ctrlcli.IgnoreNotFound(err); err != nil {
			logger.Error(err, "fetch cluster queue")
		}
		return ctrl.Result{}, err
	}

	// 跳过正在删除的全局队列。
	if cq.DeletionTimestamp != nil {
		logger.V(3).Info("skip deleted cluster queue")
		return ctrl.Result{}, nil
	}

	// 列出命名空间。
	nsList := new(core.NamespaceList)
	err = r.Client.List(ctx, nsList,
		ctrlclix.WithoutQuorum,
		ctrlcli.UnsafeDisableDeepCopy)
	if err != nil {
		logger.Error(err, "list namespaces")
		return ctrl.Result{}, err
	}

	var errs []error

	// 逐个检查命名空间，创建缺失的 LocalQueue 并同步已有入口。
	for i := range nsList.Items {
		ns := &nsList.Items[i]

		// 跳过正在删除的命名空间。
		if ns.DeletionTimestamp != nil {
			logger.V(3).Info("skip deleted namespace")
			continue
		}

		// 保留命名空间不创建工作负载入口。
		if kuberess.IsReservedNamespace(ns.Name) {
			logger.V(3).Info("skip reserved namespace")
			continue
		}

		lq := &kueue.LocalQueue{
			ObjectMeta: meta.ObjectMeta{
				Name:      nodefeature.FormatLocalQueueName(cq.Name),
				Namespace: ns.Name,
				Annotations: map[string]string{
					_LocalQueueClusterQueueNameAnnoKey: cq.Name,
				},
			},
			Spec: kueue.LocalQueueSpec{
				ClusterQueue: kueue.ClusterQueueReference(cq.Name),
			},
		}
		kubemeta.ControlOnWithoutBlock(lq, cq, kueue.SchemeGroupVersion.WithKind("ClusterQueue"))
		_, err = kubeclientset.UpdateWithCtrlClient(ctx, r.Client, lq,
			kubeclientset.WithCreateIfNotExisted[*kueue.LocalQueue]())
		if err != nil {
			logger.Error(err, "sync local queue", "namespace", ns.Name, "cluster queue", lq.Name)
			errs = append(errs, err)
			continue
		}
	}

	return ctrl.Result{}, multierr.Combine(errs...)
}

func (r *NodeQueueEntranceReconciler) SetupController(ctx context.Context, opts controller.SetupOptions) error {
	r.Client = opts.Manager.GetClient()

	return ctrl.NewControllerManagedBy(opts.Manager).
		Named("nodequeueentrance").
		For(
			&kueue.ClusterQueue{},
			ctrlbuilder.WithPredicates(
				// 只处理匹配的 ClusterQueue。
				ctrlpredicate.NewPredicateFuncs(func(obj ctrlcli.Object) bool {
					return systemmeta.MatchResource(obj, "instancetypes")
				}),
				// 创建 ClusterQueue 时开始同步入口。
				ctrlpredicate.Not(ctrlpredicate.Funcs{
					CreateFunc: func(e ctrlevent.CreateEvent) bool {
						return false
					},
				}),
			),
		).
		Watches(
			// 监听 Namespace 的创建，为对应的全局队列安排同步。
			&core.Namespace{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFunc(
				5*time.Second,
				r.enqueueClusterQueueWhenNamespaceCreated,
			),
			ctrlbuilder.WithPredicates(
				// 只响应命名空间的创建事件。
				ctrlpredicate.Not(ctrlpredicate.Funcs{
					CreateFunc: func(e ctrlevent.CreateEvent) bool {
						return false
					},
				}),
			),
		).
		Complete(r)
}

func (r *NodeQueueEntranceReconciler) enqueueClusterQueueWhenNamespaceCreated(
	ctx context.Context,
	obj ctrlcli.Object,
) []ctrlreconcile.Request {
	logger := ctrllog.FromContext(ctx)

	ns := obj.(*core.Namespace)

	// 跳过正在删除的命名空间。
	if ns.DeletionTimestamp != nil {
		logger.V(3).Info("skip deleted namespace")
		return nil
	}

	// 跳过保留命名空间。
	if kuberess.IsReservedNamespace(ns.Name) {
		logger.V(3).Info("skip reserved namespace")
		return nil
	}

	cqList := new(kueue.ClusterQueueList)
	err := r.Client.List(ctx, cqList,
		ctrlclix.WithoutQuorum,
		ctrlcli.UnsafeDisableDeepCopy)
	if err != nil {
		logger.Error(err, "list cluster queues")
		return nil
	}

	reqs := make([]ctrlreconcile.Request, 0, len(cqList.Items))
	for i := range cqList.Items {
		cq := &cqList.Items[i]

		// 跳过正在终止的 ClusterQueue。
		if cq.DeletionTimestamp != nil {
			continue
		}

		reqs = append(reqs, ctrlreconcile.Request{
			NamespacedName: ctrlcli.ObjectKey{
				Name: cq.Name,
			},
		})
	}
	if len(reqs) == 0 {
		return nil
	}

	logger.V(2).Info("enqueue queue from namespace", "requests", reqs)
	return reqs
}
