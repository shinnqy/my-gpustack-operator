package worker

import (
	"context"
	"maps"
	"strconv"
	"time"

	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlbuilder "sigs.k8s.io/controller-runtime/pkg/builder"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlevent "sigs.k8s.io/controller-runtime/pkg/event"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	ctrlpredicate "sigs.k8s.io/controller-runtime/pkg/predicate"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/controller"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/systemname"
	"gpustack.ai/gpustack/pkg/utils/ctrlhandlerx"
	"gpustack.ai/gpustack/pkg/utils/json"
	"gpustack.ai/gpustack/pkg/utils/mapx"
)

// NodeFitLabelReconciler 维护受管 Node 上的适配标签（fit label），其值派生自同名的 Devices 账本：按加速器型号统计单张卡上可提供给逻辑切片的最大剩余容量，以及仍可授予独占共享份额（ownership share）的卡数量。
//
// Kueue 的拓扑感知调度（TAS）根据节点的汇总容量放置 Workload，因此对于剩余空间分散在多张卡上但单张卡均不足以容纳的节点，TAS 仍会将 Workload 放到该节点，随后 node-devices 检查返回 Retry，接着 TAS 又会再次放到该节点。Workload webhook 通过在 Workload 的 PodSet 模板中添加硬性节点亲和性（required node affinity），将切片或共享请求绑定到这些适配标签上，而 TAS 会遵循该亲和性，从而跳过碎片化的节点。
//
// 两个标签值均复用 node-devices 检查自身的单卡判定条件，因此对于请求单张卡的单个 Pod，标签允许调度的节点与检查放行的节点完全一致。它们仅用于过滤，不参与任何资源扣减。仅当数值发生变化时才会写入，并结合账本事件的去重时间窗口，因此每次批量分配或释放最多只产生一次 Node 元数据 patch。
type NodeFitLabelReconciler struct {
	Client ctrlcli.Client
}

var _ ctrlreconcile.Reconciler = (*NodeFitLabelReconciler)(nil)

func (r *NodeFitLabelReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := ctrllog.FromContext(ctx)

	// 获取 Node。后续只读不改（下方的标签 patch 针对新建的 Node 对象），因此跳过深拷贝。
	nd := new(core.Node)
	err := r.Client.Get(ctx, req.NamespacedName, nd, ctrlcli.UnsafeDisableDeepCopy)
	if err != nil {
		// 已删除的 Node 无需再进行调谐。
		if err = ctrlcli.IgnoreNotFound(err); err != nil {
			logger.Error(err, "fetch node")
		}
		return ctrl.Result{}, err
	}

	// 若已删除则跳过。
	if nd.DeletionTimestamp != nil {
		logger.V(3).Info("skip deleted node")
		return ctrl.Result{}, nil
	}

	// 获取同名的 Devices 账本。缺失账本时不生成任何适配标签，从而清除所有陈旧标签，使所有带有标签亲和性约束的请求都会跳过无账本的节点，与 node-devices 检查的拦截行为一致。
	devs := new(workercore.Devices)
	err = r.Client.Get(ctx, ctrlcli.ObjectKey{Name: nd.Name}, devs, ctrlcli.UnsafeDisableDeepCopy)
	if err != nil {
		if !kerrors.IsNotFound(err) {
			logger.Error(err, "fetch devices")
			return ctrl.Result{}, err
		}
		devs = nil
	}

	labelPatch := buildFitLabelPatch(desiredFitLabels(nd, devs), nd.Labels)
	if labelPatch == nil {
		return ctrl.Result{}, nil
	}
	data := json.ShouldMarshal(map[string]any{
		"metadata": map[string]any{"labels": labelPatch},
	})
	// 对新建的 Node 对象而非 nd 执行 patch：merge-patch 的响应会被解码回目标对象，而 nd 读取时未做深拷贝。
	patchNode := &core.Node{ObjectMeta: meta.ObjectMeta{Name: nd.Name}}
	err = r.Client.Patch(ctx, patchNode, ctrlcli.RawPatch(types.MergePatchType, data))
	if err != nil {
		logger.Error(err, "patch node fit labels")
		return ctrl.Result{}, err
	}
	logger.V(2).Info("patched node fit labels")
	return ctrl.Result{}, nil
}

// desiredFitLabels 返回节点应携带的适配标签：对于未受管或没有账本的节点返回空，否则返回由其账本推导出的标签。
func desiredFitLabels(nd *core.Node, devs *workercore.Devices) map[string]string {
	if devs == nil || !kubemeta.IsLabeled(nd, systemname.ManagedLabelKey, "true") {
		return nil
	}
	return fitLabelsOf(devs)
}

// fitLabelsOf 从 Devices 账本推导出适配标签，每个 "<manufacturer>-<id>" 分组生成一对标签，并像 node-devices 检查一样通过加速器 ID 将 status 侧的分配记录与 spec 侧的能力规格关联起来。某类型群体若没有对应加速卡，则不为该类型输出键；若所有卡均已满，则输出 "0"。即使某张卡还有剩余显存，只要其持有的逻辑切片数已达上限，对于切片键而言也视作已满，因为它无法再承载更多切片。
func fitLabelsOf(devs *workercore.Devices) map[string]string {
	capByID := make(map[string]workercore.AcceleratorStatus)
	for gi := range devs.Spec.Groups {
		accs := devs.Spec.Groups[gi].Accelerators
		for ai := range accs {
			capByID[accs[ai].ID] = accs[ai].Status
		}
	}

	out := make(map[string]string)
	for gi := range devs.Status.Groups {
		g := &devs.Status.Groups[gi]
		var (
			sliceable, shareable bool
			maxFreeUnits         int32
			freeShareCards       int
		)
		for ai := range g.Accelerators {
			acc := &g.Accelerators[ai]
			card := cardLedger{
				capability: capByID[acc.ID], mode: acc.Mode, remaining: acc.Remaining,
				allocatedSlices: acc.AllocatedSlices,
			}
			if card.servesFamily(nodefeature.ResourceFamilySliced) {
				sliceable = true
				maxFreeUnits = max(maxFreeUnits, card.freeSliceUnits())
			}
			if card.servesFamily(nodefeature.ResourceFamilyShared) {
				shareable = true
				if card.freeShares() > 0 {
					freeShareCards++
				}
			}
		}
		// 若基于分组 ID 构造的键不符合标签语法规范则跳过：否则会导致整个 Node patch 非法，且任何亲和性约束也无法引用它。
		aKey := acceleratorKeyOf(g.Manufacturer, g.ID)
		if key := nodefeature.FitSlicedMaxFreeUnitsLabelKey(aKey); sliceable && nodefeature.IsFitLabelKey(key) {
			out[key] = strconv.FormatInt(int64(maxFreeUnits), 10)
		}
		if key := nodefeature.FitSharedFreeCardsLabelKey(aKey); shareable && nodefeature.IsFitLabelKey(key) {
			out[key] = strconv.Itoa(freeShareCards)
		}
	}
	return out
}

// buildFitLabelPatch 返回用于将节点的适配标签收敛到 desired 状态的 metadata.labels merge patch：新增或变更的值会被设置，不在 desired 中的适配标签会被移除。该 patch 不会涉及任何非适配标签，且在无任何差异时返回 nil。
func buildFitLabelPatch(desired, current map[string]string) map[string]any {
	patch := make(map[string]any)
	for k, v := range desired {
		if cur, ok := current[k]; !ok || cur != v {
			patch[k] = v
		}
	}
	for k := range current {
		if !nodefeature.IsFitLabelKey(k) {
			continue
		}
		if _, ok := desired[k]; !ok {
			patch[k] = nil
		}
	}
	if len(patch) == 0 {
		return nil
	}
	return patch
}

// nodeFitLabelNodeUpdated 报告 Node 的更新是否可能改变其期望适配标签或破坏已有标签：例如受管标记发生变化，或他人改写了适配标签。
func nodeFitLabelNodeUpdated(oldNd, newNd *core.Node) bool {
	if newNd.DeletionTimestamp != nil {
		return false
	}
	return !mapx.EqualWithKey(oldNd.Labels, newNd.Labels, systemname.ManagedLabelKey) ||
		!maps.Equal(nodefeature.FilterFitLabels(oldNd.Labels), nodefeature.FilterFitLabels(newNd.Labels))
}

// fitSignatureChanged 报告 Devices 的变更是否改变了任何适配标签的值。大多数分配操作只会减少某张卡的剩余容量而不会改变最大值或跨越共享份额边界，这类变更不会触发节点调谐。
func fitSignatureChanged(oldDevs, newDevs *workercore.Devices) bool {
	return !maps.Equal(fitLabelsOf(oldDevs), fitLabelsOf(newDevs))
}

func (r *NodeFitLabelReconciler) SetupController(_ context.Context, opts controller.SetupOptions) error {
	r.Client = opts.Manager.GetClient()

	dedupWindow := ctrlhandlerx.NewDedupWindow[ctrlreconcile.Request]()

	return ctrl.NewControllerManagedBy(opts.Manager).
		Named("nodefitlabel").
		For(
			&core.Node{},
			ctrlbuilder.WithPredicates(
				// 当 Node 满足以下条件时触发调谐：
				// - 被创建。
				// - 被更新且其受管标记或任一适配标签发生变化。
				ctrlpredicate.Funcs{
					DeleteFunc: func(ctrlevent.DeleteEvent) bool {
						return false
					},
					UpdateFunc: func(e ctrlevent.UpdateEvent) bool {
						return nodeFitLabelNodeUpdated(e.ObjectOld.(*core.Node), e.ObjectNew.(*core.Node))
					},
				},
			),
		).
		Watches(
			// 账本变更会改变适配标签而不会修改 Node 本身，因此需要入队同名节点。3 秒的去重窗口可将一次突发分配合并为单次 patch。
			&workercore.Devices{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFuncWithWindow(
				3*time.Second,
				dedupWindow,
				r.enqueueNodeWhenDevicesChanged,
			),
			// 无论账本自身的标记如何，创建或删除事件始终触发调谐：节点自身的标记决定了它应携带哪些标签，而被删除的账本也必须一并带走其对应的标签。
			ctrlbuilder.WithPredicates(ctrlpredicate.Funcs{
				UpdateFunc: func(e ctrlevent.UpdateEvent) bool {
					return fitDevicesUpdated(e.ObjectOld.(*workercore.Devices), e.ObjectNew.(*workercore.Devices))
				},
			}),
		).
		Complete(r)
}

// fitDevicesUpdated 报告 Devices 的更新是否需要调谐其对应节点的适配标签：即适配值发生变化，或受管标记本身发生翻转。节点是否携带标签由节点自身的标记决定，因此无论账本标记如何，适配值变化都会触发；此外，Device Manager 会在受管标记同步到账本之前就先创建账本，而同步标记本身不会改变适配值，因此如果不监听标记翻转，节点就可能一直等到发生无关分配时才拿到初始标签。
func fitDevicesUpdated(oldDevs, newDevs *workercore.Devices) bool {
	return isManagedDevices(oldDevs) != isManagedDevices(newDevs) || fitSignatureChanged(oldDevs, newDevs)
}

// enqueueNodeWhenDevicesChanged 将发生变更的 Devices 账本映射为同名的 Node 入队请求。
func (r *NodeFitLabelReconciler) enqueueNodeWhenDevicesChanged(
	ctx context.Context, obj ctrlcli.Object,
) []ctrlreconcile.Request {
	req := ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKey{Name: obj.GetName()}}
	ctrllog.FromContext(ctx).V(2).Info("enqueued node from devices", "request", req)
	return []ctrlreconcile.Request{req}
}
