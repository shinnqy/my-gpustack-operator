package worker

import (
	"context"
	"maps"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlbuilder "sigs.k8s.io/controller-runtime/pkg/builder"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlevent "sigs.k8s.io/controller-runtime/pkg/event"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	ctrlpredicate "sigs.k8s.io/controller-runtime/pkg/predicate"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"
	nfd "sigs.k8s.io/node-feature-discovery/api/nfd/v1alpha1"

	"gpustack.ai/gpustack/pkg/controller"
	"gpustack.ai/gpustack/pkg/kubeclientset"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/systemname"
	"gpustack.ai/gpustack/pkg/utils/mapx"
	"gpustack.ai/gpustack/pkg/worker/kuberess"
	"gpustack.ai/gpustack/pkg/worker/settings"
)

// NodeFeatureReconciler 监听 Kubernetes Node。节点的标签或计算标签所需的输入发生变化时，它会创建或更新该
// 节点的 NFD NodeFeature。
type NodeFeatureReconciler struct {
	Client ctrlcli.Client
}

var _ ctrlreconcile.Reconciler = (*NodeFeatureReconciler)(nil)

func (r *NodeFeatureReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := ctrllog.FromContext(ctx)

	// 读取触发本轮协调的对象。
	nd := new(core.Node)
	// -- 从本地 informer 缓存读 Node
	err := r.Client.Get(ctx, req.NamespacedName, nd)
	if err != nil {
		// Node 已经删除时，本轮协调结束。
		if err = ctrlcli.IgnoreNotFound(err); err != nil {
			logger.Error(err, "fetch node")
		}
		return ctrl.Result{}, err
	}

	// 对象正在删除时，跳过本轮。
	if nd.DeletionTimestamp != nil {
		logger.V(3).Info("skip deleted node")
		return ctrl.Result{}, nil
	}

	// IMPORTANT: 构造期望的 NodeFeature
	eNf := &nfd.NodeFeature{
		ObjectMeta: meta.ObjectMeta{
			Name:      nd.Name + "-gpustack-worker",
			Namespace: kuberess.SystemNamespaceName,
			Labels: map[string]string{
				nfd.NodeFeatureObjNodeNameLabel: nd.Name,
				"app.kubernetes.io/part-of":     "gpustack-operator-worker",
			},
		},
		Spec: func() nfd.NodeFeatureSpec {
			nfs := nfd.NewNodeFeatureSpec()
			nfs.Labels = nodefeature.ConstructNodeCapacityLabels(nd,
				// 每次协调都读取管理模式，管理员修改配置后，下次协调即可生效。
				nodefeature.WithManualNodeManagement(settings.NodeManagementManual.ShouldValueBool(ctx)))
			return *nfs
		}(),
	}
	kubemeta.ControlOnWithoutBlock(eNf, nd, core.SchemeGroupVersion.WithKind("Node"))
	nfAlignFn := func(aNf *nfd.NodeFeature) (_ *nfd.NodeFeature, skip bool, err error) {
		skip = true
		if aNf.Labels == nil {
			aNf.Labels = make(map[string]string)
		}
		if aNf.Spec.Labels == nil {
			aNf.Spec.Labels = make(map[string]string)
		}
		// 更新对象元数据中由本控制器维护的标签。
		for k, v := range eNf.Labels {
			if aNf.Labels[k] != v {
				aNf.Labels[k] = v
				skip = false
			}
		}
		// 仅在内容变化时替换 spec.labels。
		if !kubemeta.DeepEqual(aNf.Spec.Labels, eNf.Spec.Labels) {
			logger.V(2).Info("node feature spec changed, need update",
				"old", aNf.Spec.Labels,
				"new", eNf.Spec.Labels)
			aNf.Spec.Labels = eNf.Spec.Labels
			skip = false
		}
		// 校正指向 Node 的所有者引用。
		if !kubemeta.IsControlledBy(aNf, nd) {
			kubemeta.ControlOnWithoutBlock(aNf, nd, core.SchemeGroupVersion.WithKind("Node"))
			skip = false
		}
		return aNf, skip, nil
	}
	_, err = kubeclientset.CreateWithCtrlClient(ctx, r.Client, eNf,
		kubeclientset.WithUpdateIfExisted(nfAlignFn))
	if err != nil {
		logger.Error(err, "sync node feature")
		return ctrl.Result{}, err
	}
	logger.V(2).Info("synced node feature")
	return ctrl.Result{}, nil
}

func (r *NodeFeatureReconciler) SetupController(_ context.Context, opts controller.SetupOptions) error {
	r.Client = opts.Manager.GetClient()

	return ctrl.NewControllerManagedBy(opts.Manager).
		Named("nodefeature").
		For(
			&core.Node{},
			ctrlbuilder.WithPredicates(
				// Node 创建时触发协调。更新时，只处理相关标签或生成这些标签的输入发生变化
				// 的事件。
				ctrlpredicate.Funcs{
					DeleteFunc: func(e ctrlevent.DeleteEvent) bool {
						return false
					},
					UpdateFunc: func(e ctrlevent.UpdateEvent) bool {
						return nodeFeatureNodeUpdated(e.ObjectOld.(*core.Node), e.ObjectNew.(*core.Node))
					},
				},
			),
		).
		Complete(r)
}

// nodeFeatureNodeUpdated 判断这次 Node 更新是否需要重新生成 NodeFeature。
func nodeFeatureNodeUpdated(oldNd, newNd *core.Node) bool {
	if newNd.DeletionTimestamp != nil {
		return false
	}
	// 相关标签发生变化时触发协调。
	if !mapx.EqualWithStringPrefix(oldNd.Labels, newNd.Labels,
		systemname.ManagedLabelKey,
		nodefeature.FeatureLabelPrefix,
		nodefeature.GeneralFeatureLabelPrefix,
		nodefeature.AcceleratableFeatureLabelPrefix) {
		return true
	}
	// NFD 可能在 Node 注册后才补上 CPU 型号标签和名称注解，首次计算只能写入通用组。比较两次推导的结果
	// 就能捕获这些输入的变化。两边都启用手动管理来保留节点实际携带的 managed 值。
	return !maps.Equal(
		nodefeature.ConstructNodeCapacityLabels(oldNd, nodefeature.WithManualNodeManagement(true)),
		nodefeature.ConstructNodeCapacityLabels(newNd, nodefeature.WithManualNodeManagement(true)),
	)
}
