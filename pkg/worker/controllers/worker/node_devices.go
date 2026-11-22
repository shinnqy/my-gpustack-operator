package worker

import (
	"context"
	"strings"
	"time"

	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlbuilder "sigs.k8s.io/controller-runtime/pkg/builder"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlevent "sigs.k8s.io/controller-runtime/pkg/event"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	ctrlpredicate "sigs.k8s.io/controller-runtime/pkg/predicate"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/controller"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/systemname"
	"gpustack.ai/gpustack/pkg/utils/ctrlhandlerx"
	"gpustack.ai/gpustack/pkg/utils/mapx"
)

// NodeDevicesReconciler 将 worker 归属的控制面标签——gpustack.ai/managed、Node 真实的
// general(CPU) 特征键及其拓扑 profile——同步到每个 Devices 对象上。Devices 对象为集群作用域
// 且以 Node 命名，因此两者共享同一键。DeviceManager 会写入 Devices 对象的 os/arch/accelerator-key
// 选择器标签，但刻意将这些控制面标签留给 worker 处理：Node 纳管属于控制面决策，每节点运行的
// DeviceManager 不应越权断言；而 CPU 键（ExtractGeneralNodeKey）依赖 Node 上的 CPU 标签，
// DeviceManager 所属的 NodeFeature 并未携带这些标签（因此它只能猜测 "generic" 哨兵值）。
// 同步这两类标签使队列的 Devices 能够使用与 ResourceFlavor 相同的标签进行筛选。CPU 键用于区分硬件池；
// 拓扑 profile 则将设备准入约束在 Kueue 分配的精确 TAS 容量分区内。
// 此外，该控制器还负责删除已不存在的 Node 所对应的 Devices（见 deleteDevicesOfAbsentNode）。
type NodeDevicesReconciler struct {
	Client    ctrlcli.Client
	APIReader ctrlcli.Reader
}

var _ ctrlreconcile.Reconciler = (*NodeDevicesReconciler)(nil)

func (r *NodeDevicesReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := ctrllog.FromContext(ctx)

	// 获取 Devices（以 Node 命名）。若已不存在则无需同步。
	devs := new(workercore.Devices)
	err := r.Client.Get(ctx, req.NamespacedName, devs)
	if err != nil {
		if !kerrors.IsNotFound(err) {
			logger.Error(err, "fetch devices")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	if devs.DeletionTimestamp != nil {
		logger.V(3).Info("skip deleted devices")
		return ctrl.Result{}, nil
	}

	// 读取 Node。若 Node 缺失，说明该 Devices 描述的加速器已不存在，因此在此直接删除而非交由垃圾回收器处理。
	nd := new(core.Node)
	err = r.Client.Get(ctx, ctrlcli.ObjectKey{Name: req.Name}, nd)
	if err != nil {
		if kerrors.IsNotFound(err) {
			return ctrl.Result{}, r.deleteDevicesOfAbsentNode(ctx, devs)
		}
		logger.Error(err, "fetch node")
		return ctrl.Result{}, err
	}

	// 若已同步——managed 值与 general(CPU) 键均一致，且两者皆无陈旧标签——则保持 Devices 不变
	// （其余标签归 DeviceManager 所有）。
	want := nodeDevicesControlLabels(nd)
	if nodeDevicesControlInSync(devs.Labels, want) {
		logger.V(3).Info("control labels already synced, skip")
		return ctrl.Result{}, nil
	}

	// 收敛状态：保留 DeviceManager 的标签，用 Node 决定的标签替换 worker 归属的标签
	// （Node 变更可能已淘汰了陈旧的 managed 标记或陈旧的 CPU 键）。
	devs.Labels = mapx.Merge(mapx.Filter(devs.Labels, func(k, _ string) bool {
		return !nodeDevicesControlLabelKey(k)
	}), want)
	if err = r.Client.Update(ctx, devs); err != nil {
		logger.Error(err, "sync control labels onto devices")
		return ctrl.Result{}, err
	}

	logger.V(2).Info("synced control labels onto devices", "labels", want)
	return ctrl.Result{}, nil
}

// deleteDevicesOfAbsentNode 删除其所属 Node 已不存在的 Devices。
//
// Node 拥有 Devices，但垃圾回收器需要通过 worker 的聚合 API 访问 Devices（该 API 是此资源组的首选版本）。
// 当 worker 处于停机状态时（例如 worker 本身就运行在已离线的 Node 上），垃圾回收器无法删除该对象；
// 而当 worker 恢复后，垃圾回收器仅在退避时间到期时才会重试，该退避时间可能增长至数十分钟。
// 在此期间，所有账本消费者都会继续统计已离开 Node 的加速器。
//
// 删除前会先通过非缓存读取确认 Node 确实不存在：刚加入集群的 Node 可能尚未进入缓存，
// 若误删其新建的 Devices，会导致其加速器在 DeviceManager 下一次上报前不可见。
// 此类 Node 自身的添加事件会再次将 Devices 入队。UID 前置条件则可防止为同名新 Node 写入的 Devices 被误删。
func (r *NodeDevicesReconciler) deleteDevicesOfAbsentNode(ctx context.Context, devs *workercore.Devices) error {
	logger := ctrllog.FromContext(ctx)

	err := r.APIReader.Get(ctx, ctrlcli.ObjectKey{Name: devs.Name}, new(core.Node))
	if err == nil {
		logger.V(3).Info("node is absent from the cache only, keep devices")
		return nil
	}
	if !kerrors.IsNotFound(err) {
		logger.Error(err, "confirm node absence")
		return err
	}

	err = r.Client.Delete(ctx, devs, ctrlcli.Preconditions{UID: &devs.UID})
	if err != nil && !kerrors.IsNotFound(err) {
		logger.Error(err, "delete devices of absent node")
		return err
	}

	logger.V(2).Info("deleted devices of absent node")
	return nil
}

// nodeDevicesControlLabelKey 判断某个标签键是否属于 worker 在 Devices 对象上管理的控制标签：
// 包括 managed 标记、拓扑 profile 以及 general(CPU) 特征键（裸键或其 .count/.capacity 伴随键）。
func nodeDevicesControlLabelKey(k string) bool {
	return k == systemname.ManagedLabelKey || k == TopologyProfileLabel ||
		strings.HasPrefix(k, nodefeature.GeneralFeatureLabelPrefix)
}

// nodeDevicesControlLabels 生成 worker 从 Node 镜像到其 Devices 的标签：
// managed 标记（仅当 Node 携带时）、拓扑 profile 以及 Node 真实的 general(CPU) 特征键。
func nodeDevicesControlLabels(nd *core.Node) map[string]string {
	out := make(map[string]string, 3)
	if v := nd.Labels[systemname.ManagedLabelKey]; v != "" {
		out[systemname.ManagedLabelKey] = v
	}
	if v := nd.Labels[TopologyProfileLabel]; v != "" {
		out[TopologyProfileLabel] = v
	}
	if gKey := nodefeature.ExtractGeneralNodeKey(nd); gKey != "" {
		out[nodefeature.GeneralFeatureLabelPrefix+gKey] = "true"
	}
	return out
}

// nodeDevicesControlInSync 判断两组标签在 worker 归属的控制标签上是否一致：
// 包括 managed 标记、拓扑 profile 以及所有 general(CPU) 键。它同时驱动跳过检查（Devices 与期望值对比）
// 和 watch predicate（旧对象与新对象对比）。由 DeviceManager 管理的标签——尤其是探测器写入的加速器
// （acceleratable.）选择器键——会被刻意忽略，因此仅涉及加速器的变更绝不会触发控制标签重同步。
// 该函数与探测器的 acceleratableDevicesSelectorLabels 互为镜像，后者保留加速器键而丢弃通用键。
func nodeDevicesControlInSync(a, b map[string]string) bool {
	return mapx.EqualWithKey(a, b, systemname.ManagedLabelKey) &&
		mapx.EqualWithKey(a, b, TopologyProfileLabel) &&
		mapx.EqualWithStringPrefix(a, b, nodefeature.GeneralFeatureLabelPrefix)
}

func (r *NodeDevicesReconciler) SetupController(_ context.Context, opts controller.SetupOptions) error {
	r.Client = opts.Manager.GetClient()
	r.APIReader = opts.Manager.GetAPIReader()

	return ctrl.NewControllerManagedBy(opts.Manager).
		Named("nodedevices").
		For(
			// 按自身名称（即 Node 名称）协调每个 Devices 对象。启动时的全量重同步会为每个 Devices
			// 写入标签，而在运行期间仅当 worker 归属的控制标签发生漂移时才响应更新事件
			// （DeviceManager 会频繁重写其他标签）。
			&workercore.Devices{},
			ctrlbuilder.WithPredicates(
				ctrlpredicate.Funcs{
					DeleteFunc: func(e ctrlevent.DeleteEvent) bool {
						return false
					},
					UpdateFunc: func(e ctrlevent.UpdateEvent) bool {
						oldDevs, newDevs := e.ObjectOld.(*workercore.Devices), e.ObjectNew.(*workercore.Devices)
						if newDevs.DeletionTimestamp != nil {
							return false
						}
						return !nodeDevicesControlInSync(oldDevs.Labels, newDevs.Labels)
					},
				},
			),
		).
		Watches(
			// 监听 Node，并在控制标签发生变化或 Node 被删除时将同名的 Devices 加入队列。
			&core.Node{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFunc(
				5*time.Second,
				r.enqueueDevicesWhenNodeChanged,
			),
			ctrlbuilder.WithPredicates(
				ctrlpredicate.Funcs{
					UpdateFunc: func(e ctrlevent.UpdateEvent) bool {
						oldNd, newNd := e.ObjectOld.(*core.Node), e.ObjectNew.(*core.Node)
						if newNd.DeletionTimestamp != nil {
							return false
						}
						return !nodeDevicesControlInSync(oldNd.Labels, newNd.Labels)
					},
				},
			),
		).
		Complete(r)
}

func (r *NodeDevicesReconciler) enqueueDevicesWhenNodeChanged(
	ctx context.Context,
	obj ctrlcli.Object,
) []ctrlreconcile.Request {
	logger := ctrllog.FromContext(ctx).
		WithValues("node", ctrlcli.ObjectKeyFromObject(obj))

	reqs := []ctrlreconcile.Request{
		{NamespacedName: ctrlcli.ObjectKey{Name: obj.GetName()}},
	}

	logger.V(2).Info("enqueue devices from node", "requests", reqs)
	return reqs
}
