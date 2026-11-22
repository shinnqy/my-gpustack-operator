package worker

import (
	"context"
	"maps"
	"time"

	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlbuilder "sigs.k8s.io/controller-runtime/pkg/builder"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlevent "sigs.k8s.io/controller-runtime/pkg/event"
	ctrlhandler "sigs.k8s.io/controller-runtime/pkg/handler"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	ctrlpredicate "sigs.k8s.io/controller-runtime/pkg/predicate"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"

	gpustack "gpustack.ai/gpustack/api/v1"
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/controller"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/modelstore"
)

// ModelStore 状态的各 Condition。
const (
	// ModelStoreConditionReady 表示该存储的策略已应用到所有匹配的节点上：每个匹配节点的 NodeModelStore
	// 都将此存储指定为生效的配置来源。
	ModelStoreConditionReady = "Ready"
	// ModelStoreConditionSelectorOverlap 表示另一个存储的 selector 也匹配了当前存储的至少一个节点。
	// 节点仍会得到确定的结果——按字母顺序靠前的存储胜出——且双方存储都会携带此 condition，使配置冲突
	// 能在配置处直接显现。
	ModelStoreConditionSelectorOverlap = "SelectorOverlap"
)

// selectModelStore 将存储列表筛选为 selector 匹配该节点的存储，并按名称排序：第一项即为该节点的生效
// 存储，切片长度即为重叠数。
func selectModelStore(stores []workercore.ModelStore, node *core.Node) ([]*workercore.ModelStore, error) {
	// 在第 16 章中实现此函数。
	var result0 []*workercore.ModelStore
	var result1 error
	return result0, result1
}

// modelStoreLayer 将存储的策略读取为一个合并层。存储未填写的字段保持为 nil，从而让下层配置继续生效——
// 存储不会无意中重申默认值，而显式设置的值（包括 bytesPerSecond 为 0）则作为真正的覆盖。
func modelStoreLayer(store *workercore.ModelStore) modelstore.Layer {
	var layer modelstore.Layer
	if w := store.Spec.Watermarks; w != nil {
		layer.HighWatermarkPercent = &w.HighPercent
		layer.LowWatermarkPercent = &w.LowPercent
	}
	if d := store.Spec.Download; d != nil {
		layer.DownloadConcurrency = d.Concurrency
		layer.DownloadBytesPerSecond = d.BytesPerSecond
	}

	return layer
}

// ModelStoreReconciler 使存储的 status 与资源池的实际情况保持一致：selector 匹配的节点、这些节点持有的
// 缓存，以及是否有其他存储命中了相同节点。策略在单节点上的生效由 NodeModelStoreReconciler 负责，
// 它会将配置写入各个节点的 spec。
type ModelStoreReconciler struct {
	Client ctrlcli.Client
	Now    func() time.Time
}

var _ ctrlreconcile.Reconciler = (*ModelStoreReconciler)(nil)

// modelStoreObservation 是单轮对账中读取到的资源池状态，是生成 status 的原始数据。
type modelStoreObservation struct {
	nodes   int32 // selector 匹配的节点数
	applied int32 // spec 中指定了当前存储的匹配节点数
	overlap bool  // 是否有其他存储也匹配了至少一个当前匹配节点

	capacity       bool  // 是否至少有一个匹配节点上报了容量读数
	capacityTotal  int64 // 匹配节点缓存文件系统的总容量之和
	capacityStored int64 // 已发布目录树与未完成下载的总大小之和
}

func (r *ModelStoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := ctrllog.FromContext(ctx)

	store := new(workercore.ModelStore)
	switch err := r.Client.Get(ctx, req.NamespacedName, store); {
	case kerrors.IsNotFound(err):
		return ctrl.Result{}, nil
	case err != nil:
		return ctrl.Result{}, err
	}
	if store.DeletionTimestamp != nil {
		return ctrl.Result{}, nil
	}

	selector, err := meta.LabelSelectorAsSelector(&store.Spec.NodeSelector)
	if err != nil {
		// API Server 自身的 schema 在写入时会拒绝无法解析的 selector，因此这种情况只可能来自手动修改；
		// 直接将存储报告为异常并等待下一个 generation。
		logger.Error(err, "the store's nodeSelector does not parse; its status reports the store broken")
		before := store.Status.DeepCopy()
		store.Status.Conditions = carryConditionTransitions(store.Status.Conditions,
			modelStoreConditions(store, &modelStoreObservation{}, err), r.now())

		return ctrl.Result{}, modelStoreCommitStatus(ctx, r.Client, store, before)
	}

	nodes := new(core.NodeList)
	listOpts := []ctrlcli.ListOption{}
	if m := store.Spec.NodeSelector.MatchLabels; len(m) > 0 && len(store.Spec.NodeSelector.MatchExpressions) == 0 {
		// 纯 matchLabels selector 直接在本地缓存中过滤：对于仅指定三个标签的资源池，无需将集群全部节点
		// 拉取过来。
		listOpts = append(listOpts, ctrlcli.MatchingLabels(m))
	}
	if err := r.Client.List(ctx, nodes, listOpts...); err != nil {
		return ctrl.Result{}, err
	}
	stores := new(workercore.ModelStoreList)
	if err := r.Client.List(ctx, stores); err != nil {
		return ctrl.Result{}, err
	}
	// 除当前存储外的其余所有存储，用于检查选择器重叠。
	others := make([]workercore.ModelStore, 0, len(stores.Items))
	for i := range stores.Items {
		if stores.Items[i].Name != store.Name {
			others = append(others, stores.Items[i])
		}
	}

	reports := new(workercore.NodeModelStoreList)
	if err := r.Client.List(ctx, reports); err != nil {
		return ctrl.Result{}, err
	}
	reportByName := map[string]*workercore.NodeModelStore{}
	for i := range reports.Items {
		reportByName[reports.Items[i].Name] = &reports.Items[i]
	}

	obs := modelStoreObservation{}
	for i := range nodes.Items {
		nd := &nodes.Items[i]
		if nd.DeletionTimestamp != nil || !selector.Matches(labels.Set(nd.Labels)) {
			continue
		}
		obs.nodes++

		for j := range others {
			other, err := meta.LabelSelectorAsSelector(&others[j].Spec.NodeSelector)
			if err != nil {
				continue
			}
			if other.Matches(labels.Set(nd.Labels)) {
				obs.overlap = true
				break
			}
		}

		nms, ok := reportByName[nd.Name]
		if !ok {
			// 该节点尚未创建插件对象，因此当前存储的任何配置都尚未应用到该节点上。
			continue
		}
		if nms.Spec.Store == store.Name {
			obs.applied++
		}
		if c := nms.Status.Capacity; c != nil {
			obs.capacity = true
			obs.capacityTotal += c.TotalBytes
			obs.capacityStored += c.StoredBytes
		}
	}

	before := store.Status.DeepCopy()
	store.Status.Conditions = carryConditionTransitions(store.Status.Conditions,
		modelStoreConditions(store, &obs, nil), r.now())
	store.Status.Nodes = obs.nodes
	if obs.capacity {
		store.Status.Capacity = &workercore.ModelStoreCapacity{
			TotalBytes: obs.capacityTotal, StoredBytes: obs.capacityStored,
		}
	} else {
		store.Status.Capacity = nil
	}
	if kubemeta.DeepEqual(before, &store.Status) {
		return ctrl.Result{}, nil
	}

	return ctrl.Result{}, r.Client.Status().Update(ctx, store)
}

// modelStoreConditions 将观测结果转换为 status 中的各 condition。
func modelStoreConditions(store *workercore.ModelStore, obs *modelStoreObservation, broken error) []gpustack.Condition {
	ready := gpustack.Condition{Type: ModelStoreConditionReady, ObservedGeneration: store.Generation}
	switch {
	case broken != nil:
		ready.Status, ready.Reason = meta.ConditionFalse, "SelectorInvalid"
		ready.Message = "the nodeSelector does not parse, so no node can be matched"
	case obs.nodes == 0:
		ready.Status, ready.Reason = meta.ConditionTrue, "NoNodes"
		ready.Message = "the selector matches no nodes"
	case obs.applied == obs.nodes:
		ready.Status, ready.Reason = meta.ConditionTrue, "Applied"
		ready.Message = "the policy is applied on every matched node"
	default:
		ready.Status, ready.Reason = meta.ConditionFalse, "Applying"
		ready.Message = "the policy is applied on some matched nodes and pending on the rest"
	}
	if obs.overlap && ready.Status == meta.ConditionFalse {
		// 仅覆写竞争失败的存储的 message：处于 Applied 或 NoNodes 状态的存储保留各自的 reason/message 配对，
		// 而重叠事实已记录在 SelectorOverlap 条件中。
		ready.Message = "the selector overlaps another store's, so the alphabetically first store wins the shared nodes"
		ready.Reason = "Overridden"
	}

	overlap := gpustack.Condition{
		Type:               ModelStoreConditionSelectorOverlap,
		Status:             meta.ConditionFalse,
		Reason:             "Exclusive",
		Message:            "no other store matches this store's nodes",
		ObservedGeneration: store.Generation,
	}
	if obs.overlap {
		overlap.Status, overlap.Reason = meta.ConditionTrue, "SharedNodes"
		overlap.Message = "another store's selector matches at least one of this store's nodes; the alphabetically first name wins them"
	}

	return []gpustack.Condition{ready, overlap}
}

// carryConditionTransitions 在 condition 的状态未变时保留其上次转换时间，从而使未发生变化的资源池
// 计算出的结果与已存储的 status 相等，避免无意义写回。
func carryConditionTransitions(old, next []gpustack.Condition, now time.Time) []gpustack.Condition {
	for i := range next {
		next[i].LastTransitionTime = meta.NewTime(now.UTC().Truncate(time.Second))
		for _, o := range old {
			if o.Type == next[i].Type && o.Status == next[i].Status {
				next[i].LastTransitionTime = o.LastTransitionTime
			}
		}
	}

	return next
}

func (r *ModelStoreReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}

	return time.Now()
}

// modelStoreCommitStatus 在本轮对账修改了 status 时将其写回。
func modelStoreCommitStatus(ctx context.Context, cli ctrlcli.Client, store *workercore.ModelStore, before *workercore.ModelStoreStatus) error {
	if kubemeta.DeepEqual(before, &store.Status) {
		return nil
	}

	return cli.Status().Update(ctx, store)
}

func (r *ModelStoreReconciler) SetupController(_ context.Context, opts controller.SetupOptions) error {
	r.Client = opts.Manager.GetClient()

	return ctrl.NewControllerManagedBy(opts.Manager).
		Named("modelstore").
		For(&workercore.ModelStore{}, ctrlbuilder.WithPredicates(ctrlpredicate.GenerationChangedPredicate{})).
		Watches(
			&core.Node{},
			ctrlhandler.EnqueueRequestsFromMapFunc(r.enqueueAllStores),
			ctrlbuilder.WithPredicates(ctrlpredicate.Funcs{
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
			}),
		).
		Watches(
			&workercore.NodeModelStore{},
			ctrlhandler.EnqueueRequestsFromMapFunc(r.enqueueAllStores),
			ctrlbuilder.WithPredicates(ctrlpredicate.Funcs{
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

					// 只有存储 status 读取的两项事实会触发它更新：节点所指定的存储名称，以及节点缓存的容量信息。
					// 节点上报的其余内容（插件在达到阈值时写入的按模型维度变动）由其他对象消费。
					return o.Spec.Store != n.Spec.Store ||
						!kubemeta.DeepEqual(o.Status.Capacity, n.Status.Capacity)
				},
				GenericFunc: func(ctrlevent.GenericEvent) bool { return false },
			}),
		).
		Watches(
			&workercore.ModelStore{},
			ctrlhandler.EnqueueRequestsFromMapFunc(r.enqueueSiblingStores),
			ctrlbuilder.WithPredicates(modelStoreSelectorChanged),
		).
		Complete(r)
}

// modelStoreSelectorChanged 仅在存储与重叠相关的结构发生变化时唤醒同级存储：创建或删除始终可能
// 改变其他存储的重叠视图，而更新仅在 nodeSelector 变更时才可能影响。status 写入和策略变动均不决定
// 存储之间共享哪些节点，因此绝不唤醒任何对象——这正是防止 watch 循环触发自身对账的关键。
var modelStoreSelectorChanged = ctrlpredicate.Funcs{
	CreateFunc: func(ctrlevent.CreateEvent) bool { return true },
	DeleteFunc: func(ctrlevent.DeleteEvent) bool { return true },
	UpdateFunc: func(e ctrlevent.UpdateEvent) bool {
		o, ok := e.ObjectOld.(*workercore.ModelStore)
		if !ok {
			return false
		}
		n, ok := e.ObjectNew.(*workercore.ModelStore)
		if !ok {
			return false
		}

		return !kubemeta.DeepEqual(o.Spec.NodeSelector, n.Spec.NodeSelector)
	},
	GenericFunc: func(ctrlevent.GenericEvent) bool { return false },
}

// enqueueSiblingStores 将除已变更对象之外的所有 ModelStore 加入队列，已变更对象会由其自身的 For 事件
// 触发协调：选择器移入或移出其他 ModelStore 的 Node 时，会改变该 ModelStore 的重叠视图。
// ModelStore 数量很少，因此此映射刻意保持粗粒度，由 predicate 负责精确过滤。
func (r *ModelStoreReconciler) enqueueSiblingStores(ctx context.Context, obj ctrlcli.Object) []ctrlreconcile.Request {
	stores := new(workercore.ModelStoreList)
	if err := r.Client.List(ctx, stores); err != nil {
		ctrllog.FromContext(ctx).Error(err, "list model stores")
		return nil
	}

	reqs := make([]ctrlreconcile.Request, 0, len(stores.Items))
	for i := range stores.Items {
		if stores.Items[i].Name == obj.GetName() {
			continue
		}
		reqs = append(reqs, ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKey{Name: stores.Items[i].Name}})
	}

	return reqs
}

// enqueueAllStores 将所有 ModelStore 重新加入队列：Node 的标签决定其匹配哪些 ModelStore，
// Node 的上报决定其池容量的计算结果。ModelStore 数量很少，此映射刻意保持粗粒度。
func (r *ModelStoreReconciler) enqueueAllStores(ctx context.Context, _ ctrlcli.Object) []ctrlreconcile.Request {
	stores := new(workercore.ModelStoreList)
	if err := r.Client.List(ctx, stores); err != nil {
		ctrllog.FromContext(ctx).Error(err, "list model stores")
		return nil
	}

	reqs := make([]ctrlreconcile.Request, 0, len(stores.Items))
	for i := range stores.Items {
		reqs = append(reqs, ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKey{Name: stores.Items[i].Name}})
	}

	return reqs
}
