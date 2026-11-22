package worker

import (
	"context"
	"slices"
	"sync"
	"time"

	core "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
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

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/controller"
	"gpustack.ai/gpustack/pkg/kubeclientset"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/modelstore"
	"gpustack.ai/gpustack/pkg/setting"
	"gpustack.ai/gpustack/pkg/worker/settings"
)

// nodeModelStoreInvalidRetry 是当节点计算出的配置校验失败时再次检查该节点的间隔时间。准入控制会拒绝此类非法值，因此只有两次并发写入竞态时才会走到这里。
const nodeModelStoreInvalidRetry = time.Minute

// ModelManagerRegisteredLabel 在 kubelet 列出 model-manager CSI 驱动期间存在于 Node 上。
const ModelManagerRegisteredLabel = "model.csi.gpustack.ai/registered"

// NodeModelStoreReconciler 为每个注册了 model-manager 插件的节点维护一个 NodeModelStore，使其 spec 等于该节点的生效配置，并为已注册节点打上标签以便调度。
//
// 该对象的生命周期绑定于 Node 而非插件注册状态。一旦节点的 CSINode 列出了该插件的驱动（即 kubelet 自身记录该插件正在此节点运行），便会创建该对象，并且当驱动从 CSINode 移除时不会删除它：插件重启或滚动升级会短暂注销驱动，若据此删除会导致每次升级都在每个节点上擦除并重建该对象。通过指向 Node 的 owner reference，它会随 Node 一同被回收。组件的卸载则改为在集群级别判断：当 CSIDriver 对象不存在时，删除所有 NodeModelStore。
//
// 它仅写入 spec：包括 Settings 的合并结果，以及从节点 configz 读取的 kubelet 阈值——插件自身不能直接读取 configz，因为 nodes/proxy 权限可访问集群中每一个 kubelet 端点。Status 字段则归节点上的插件所有。
type NodeModelStoreReconciler struct {
	Client ctrlcli.Client
	// ReadKubelet 读取节点的 kubelet 阈值；为 nil 时通过 API server 读取节点的 configz。
	ReadKubelet func(ctx context.Context, node string) (*workercore.NodeModelStoreKubelet, error)
	Now         func() time.Time

	kubeletMu sync.Mutex
	kubelet   map[string]kubeletReading
}

var _ ctrlreconcile.Reconciler = (*NodeModelStoreReconciler)(nil)

func (r *NodeModelStoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := ctrllog.FromContext(ctx)

	installed, err := r.driverInstalled(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !installed {
		nd := new(core.Node)
		if err := r.Client.Get(ctx, req.NamespacedName, nd); ctrlcli.IgnoreNotFound(err) != nil {
			return ctrl.Result{}, err
		} else if err == nil {
			if err := r.syncRegistrationLabel(ctx, nd, false); err != nil {
				return ctrl.Result{}, err
			}
		}
		nms := &workercore.NodeModelStore{ObjectMeta: meta.ObjectMeta{Name: req.Name}}
		if err := r.Client.Delete(ctx, nms); ctrlcli.IgnoreNotFound(err) != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	nd := new(core.Node)
	if err := r.Client.Get(ctx, req.NamespacedName, nd); err != nil {
		// 被删除的 Node 会通过 owner reference 一并清理其 NodeModelStore。
		if kerrors.IsNotFound(err) {
			r.forgetKubelet(req.Name)
		}
		return ctrl.Result{}, ctrlcli.IgnoreNotFound(err)
	}
	if nd.DeletionTimestamp != nil {
		return ctrl.Result{}, nil
	}

	registered, err := r.driverRegistered(ctx, nd.Name)
	if err != nil {
		return ctrl.Result{}, err
	}
	if err := r.syncRegistrationLabel(ctx, nd, registered); err != nil {
		return ctrl.Result{}, err
	}
	existing := new(workercore.NodeModelStore)
	switch err := r.Client.Get(ctx, req.NamespacedName, existing); {
	case kerrors.IsNotFound(err):
		if !registered {
			return ctrl.Result{}, nil
		}
	case err != nil:
		return ctrl.Result{}, err
	}

	spec, err := r.effectiveSpec(ctx, nd)
	if err != nil {
		logger.Error(err, "the node model cache's configuration fails its check; the node's spec is left as it is")
		return ctrl.Result{RequeueAfter: nodeModelStoreInvalidRetry}, nil
	}
	kubelet, read := r.kubeletThresholds(ctx, nd.Name, existing.Spec.Kubelet)
	spec.Kubelet = kubelet
	if err := modelstore.Validate(spec); err != nil {
		// 校验未通过的 kubelet 响应不会被写入；节点保留 kubelet 的默认阈值。
		logger.Error(err, "the node's kubelet thresholds fail their check and are not written")
		spec.Kubelet = nil
	}

	eNms := &workercore.NodeModelStore{
		ObjectMeta: meta.ObjectMeta{Name: nd.Name},
		Spec:       spec,
	}
	kubemeta.ControlOnWithoutBlock(eNms, nd, core.SchemeGroupVersion.WithKind("Node"))
	alignFn := func(aNms *workercore.NodeModelStore) (_ *workercore.NodeModelStore, skip bool, err error) {
		skip = true
		// spec.pinned 归预取控制器所有：将已存储的值保留到期望 spec 中，这样配置写入绝不会覆盖丢弃它，且仅 pinned 变更也不会出现在此差异比对中。若与本写入竞态的 pinned 写入落在了过时的 resourceVersion 上，则会失败并重试。
		eNms.Spec.Pinned = aNms.Spec.Pinned
		if !kubemeta.DeepEqual(aNms.Spec, eNms.Spec) {
			aNms.Spec = eNms.Spec
			skip = false
		}
		if !kubemeta.IsControlledBy(aNms, nd) {
			kubemeta.ControlOnWithoutBlock(aNms, nd, core.SchemeGroupVersion.WithKind("Node"))
			skip = false
		}
		return aNms, skip, nil
	}
	if _, err := kubeclientset.CreateWithCtrlClient(ctx, r.Client, eNms,
		kubeclientset.WithUpdateIfExisted(alignFn)); err != nil {
		// 该对象的 watch 仅传递 spec 变更，因此与插件 status 写入产生的冲突不会以事件形式回传。
		return objectWriteResult(logger, err, "sync node model store", _requeueAfterConflict)
	}

	if !read {
		// kubelet 的响应缺失或已过期；在下次常规刷新之前尽早重试。
		return ctrl.Result{RequeueAfter: nodeModelStoreKubeletRetry}, nil
	}

	return ctrl.Result{RequeueAfter: nodeModelStoreKubeletRefresh}, nil
}

// driverInstalled 报告插件的 CSIDriver 对象是否存在。
func (r *NodeModelStoreReconciler) driverInstalled(ctx context.Context) (bool, error) {
	err := r.Client.Get(ctx, ctrlcli.ObjectKey{Name: modelstore.DriverName}, new(storage.CSIDriver))
	switch {
	case err == nil:
		return true, nil
	case kerrors.IsNotFound(err):
		return false, nil
	default:
		return false, err
	}
}

// driverRegistered 报告节点的 CSINode 是否列出了该插件的驱动。
func (r *NodeModelStoreReconciler) driverRegistered(ctx context.Context, node string) (bool, error) {
	csiNode := new(storage.CSINode)
	if err := r.Client.Get(ctx, ctrlcli.ObjectKey{Name: node}, csiNode); err != nil {
		return false, ctrlcli.IgnoreNotFound(err)
	}

	return csiNodeListsModelDriver(csiNode), nil
}

func csiNodeListsModelDriver(csiNode *storage.CSINode) bool {
	return slices.ContainsFunc(csiNode.Spec.Drivers, func(d storage.CSINodeDriver) bool {
		return d.Name == modelstore.DriverName
	})
}

// syncRegistrationLabel 使调度标签与 kubelet 的插件注册状态保持同步。
func (r *NodeModelStoreReconciler) syncRegistrationLabel(ctx context.Context, nd *core.Node, registered bool) error {
	// 在第 16 章中实现此函数。
	return nil
}

// effectiveSpec 将配置层级合并为节点的 spec 并执行校验。基础层是集群的 Settings，从本控制器 watch 的 Settings 存储中读取，而非通过 Settings 包的读缓存读取，以便变更能随自身事件立即触达每个节点；资源池层是选择器匹配该节点的按字母序排在第一位的 ModelStore，仅覆盖其显式声明的字段。胜出者的名称会一并写入 spec，而选择器重叠则属于 ModelStore 自身 status 处理的范畴，不作为 spec 错误。
func (r *NodeModelStoreReconciler) effectiveSpec(ctx context.Context, nd *core.Node) (workercore.NodeModelStoreSpec, error) {
	sec := new(core.Secret)
	key := ctrlcli.ObjectKey{Namespace: setting.DelegatedSecretNamespace, Name: setting.DelegatedSecretName}
	if err := r.Client.Get(ctx, key, sec); ctrlcli.IgnoreNotFound(err) != nil {
		return workercore.NodeModelStoreSpec{}, err
	}
	value := func(s setting.Setting) string {
		if v, ok := sec.Data[s.Name()]; ok {
			return string(v)
		}
		return s.DefaultValue()
	}

	cluster, err := settings.ModelStoreLayer(value)
	if err != nil {
		return workercore.NodeModelStoreSpec{}, err
	}

	stores := new(workercore.ModelStoreList)
	if err := r.Client.List(ctx, stores); err != nil {
		return workercore.NodeModelStoreSpec{}, err
	}
	matched, err := selectModelStore(stores.Items, nd)
	if err != nil {
		return workercore.NodeModelStoreSpec{}, err
	}

	layers := []modelstore.Layer{cluster}
	if len(matched) > 0 {
		layers = append(layers, modelStoreLayer(matched[0]))
	}
	spec := modelstore.Merge(layers...)
	if len(matched) > 0 {
		spec.Store = matched[0].Name
	}

	return spec, modelstore.Validate(spec)
}

func (r *NodeModelStoreReconciler) SetupController(_ context.Context, opts controller.SetupOptions) error {
	r.Client = opts.Manager.GetClient()

	return ctrl.NewControllerManagedBy(opts.Manager).
		Named("nodemodelstore").
		For(
			&storage.CSINode{},
			ctrlbuilder.WithPredicates(ctrlpredicate.Funcs{
				UpdateFunc: func(e ctrlevent.UpdateEvent) bool {
					return csiNodeListsModelDriver(e.ObjectOld.(*storage.CSINode)) !=
						csiNodeListsModelDriver(e.ObjectNew.(*storage.CSINode))
				},
			}),
		).
		Watches(
			&core.Node{},
			&ctrlhandler.EnqueueRequestForObject{},
			ctrlbuilder.WithPredicates(ctrlpredicate.Funcs{
				CreateFunc: func(e ctrlevent.CreateEvent) bool {
					return e.Object.GetLabels()[ModelManagerRegisteredLabel] != ""
				},
				UpdateFunc: func(e ctrlevent.UpdateEvent) bool {
					return e.ObjectOld.GetLabels()[ModelManagerRegisteredLabel] !=
						e.ObjectNew.GetLabels()[ModelManagerRegisteredLabel]
				},
			}),
		).
		Watches(
			&workercore.NodeModelStore{},
			&ctrlhandler.EnqueueRequestForObject{},
			ctrlbuilder.WithPredicates(ctrlpredicate.GenerationChangedPredicate{}),
		).
		Watches(
			&storage.CSIDriver{},
			ctrlhandler.EnqueueRequestsFromMapFunc(r.enqueueEveryNode),
			ctrlbuilder.WithPredicates(namePredicate(modelstore.DriverName)),
		).
		Watches(
			&core.Secret{},
			ctrlhandler.EnqueueRequestsFromMapFunc(r.enqueueEveryNode),
			ctrlbuilder.WithPredicates(ctrlpredicate.NewPredicateFuncs(func(o ctrlcli.Object) bool {
				return o.GetNamespace() == setting.DelegatedSecretNamespace && o.GetName() == setting.DelegatedSecretName
			})),
		).
		Watches(
			&workercore.ModelStore{},
			ctrlhandler.EnqueueRequestsFromMapFunc(r.enqueueEveryNode),
			ctrlbuilder.WithPredicates(ctrlpredicate.GenerationChangedPredicate{}),
		).
		Watches(
			&core.Node{},
			&ctrlhandler.EnqueueRequestForObject{},
			ctrlbuilder.WithPredicates(nodeLabelsChanged),
		).
		Complete(r)
}

// enqueueEveryNode 将所有拥有 NodeModelStore 或其 CSINode 列出该驱动的节点加入队列：Settings 的变更会影响每个节点的 spec，而 CSIDriver 的移除会影响每个节点的对象。
func (r *NodeModelStoreReconciler) enqueueEveryNode(ctx context.Context, _ ctrlcli.Object) []ctrlreconcile.Request {
	names := map[string]struct{}{}

	stores := new(workercore.NodeModelStoreList)
	if err := r.Client.List(ctx, stores); err != nil {
		ctrllog.FromContext(ctx).Error(err, "list node model stores")
	}
	for i := range stores.Items {
		names[stores.Items[i].Name] = struct{}{}
	}
	csiNodes := new(storage.CSINodeList)
	if err := r.Client.List(ctx, csiNodes); err != nil {
		ctrllog.FromContext(ctx).Error(err, "list csi nodes")
	}
	for i := range csiNodes.Items {
		if csiNodeListsModelDriver(&csiNodes.Items[i]) {
			names[csiNodes.Items[i].Name] = struct{}{}
		}
	}

	reqs := make([]ctrlreconcile.Request, 0, len(names))
	for name := range names {
		reqs = append(reqs, ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKey{Name: name}})
	}

	return reqs
}

func namePredicate(name string) ctrlpredicate.Predicate {
	return ctrlpredicate.NewPredicateFuncs(func(o ctrlcli.Object) bool { return o.GetName() == name })
}
