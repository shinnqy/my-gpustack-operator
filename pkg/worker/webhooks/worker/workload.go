package worker

import (
	"context"
	"slices"
	"strconv"
	"strings"

	admissionv1 "k8s.io/api/admission/v1"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	ctrladmission "sigs.k8s.io/controller-runtime/pkg/webhook/admission"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	kueueworkload "sigs.k8s.io/kueue/pkg/workload"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/modelstore"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/utils/json"
	"gpustack.ai/gpustack/pkg/webhook"
	workerctrl "gpustack.ai/gpustack/pkg/worker/controllers/worker"
	"gpustack.ai/gpustack/pkg/worker/settings"
)

// WorkloadWebhook 将需要节点分发权重的 PodSet 固定到已注册 model-manager 插件的节点上。同时，它还将加速器请求
// 固定到那些 fit 标签显示单卡仍可容纳该请求的节点上，使 Kueue 的拓扑感知调度跳过那些虽然总空闲空间足够、但
// 分散在多张卡上且没有任何单卡能满足要求的节点。
//
// 对每个 PodSet 而言，每卡 U 个单位的逻辑切片要求 sliced-max-free-units 标签大于 U-1，而 N >= 2 张卡的
// 共享请求要求 shared-free-cards 标签大于 N-1。该表达式会以 AND 关系加入到 PodSet 模板的每一个必需
// node-selector term 中。TAS 会读取该模板的亲和性；Pod 及其属主的模板本身绝不携带该亲和性，因为 Kueue
// 只会向它们复制标签、注解、nodeSelector、tolerations 和调度门控。这一点至关重要：kubelet 在运行中的 Pod
// 重启时会根据当前节点标签重新准入，而一旦 Pod 占用了加速卡，fit 标签的值必定会跌破阈值。
//
// 对带有 model-manager CSI 卷的 PodSet 应用插件固定，与 workload-fit-affinity 设置无关。若插件在调度后
// 注销，仍可能导致 Pod 等待挂载；Deployment 的 WeightsReady 条件会报告该等待状态。
//
// Kueue 可能同时服务于其他租户，因此仅当 Workload 的 LocalQueue 指向带有本 operator 的 InstanceType 标记
// 的 ClusterQueue，且同名 InstanceType 指定了加速器组时，才会应用 fit 固定。链路缺失或读取失败只会跳过 fit
// 固定，不影响插件固定。该 Webhook 永远不会拒绝请求；即使没有 fit 固定，node-devices 检查仍会拦截被放置到
// 无法容纳它的节点上的请求。
//
// 仅当旧 Workload 未持有配额预留时，才会在 UPDATE 时执行操作。Kueue 会就地重建挂起任务的 Workload spec（这
// 会清除上述固定条件），而它也仅在相同条件下才允许变更 PodSets。
//
// nolint: lll
// +k8s:webhook-gen:mutating:group="kueue.x-k8s.io",version="v1beta2",resource="workloads",scope="Namespaced"
// +k8s:webhook-gen:mutating:operations=["CREATE","UPDATE"],failurePolicy="Ignore",sideEffects="None",matchPolicy="Equivalent",timeoutSeconds=10
// +k8s:webhook-gen:mutating:matchConditions=[{"name":"gpustack-local-queue","expression":"has(object.spec.queueName) && object.spec.queueName.startsWith('gpustack-fnv64-')"}]
// +k8s:webhook-gen:mutating:namePrefix="gpustack-worker"
type WorkloadWebhook struct {
	Client ctrlcli.Client
}

func (r *WorkloadWebhook) SetupWebhook(_ context.Context, opts webhook.SetupOptions) (runtime.Object, error) {
	r.Client = opts.Manager.GetClient()

	return &kueue.Workload{}, nil
}

var _ ctrladmission.Defaulter[runtime.Object] = (*WorkloadWebhook)(nil)

// fitPin 表示单个 PodSet 的单 Pod 需求，Default 会将其转换为节点亲和性表达式。
type fitPin struct {
	podSet      int
	slicedUnits int32
	sharedCards int32
}

func (r *WorkloadWebhook) Default(ctx context.Context, obj runtime.Object) error {
	wl := obj.(*kueue.Workload)
	logger := ctrllog.FromContext(ctx).WithValues("workload", ctrlcli.ObjectKeyFromObject(wl))

	if !strings.HasPrefix(string(wl.Spec.QueueName), nodefeature.LocalQueueNamePrefix) {
		return nil
	}
	pins := fitPinsOf(wl)
	nodeDelivery := nodeDeliveryPodSets(wl)
	if len(pins) == 0 && len(nodeDelivery) == 0 {
		return nil
	}
	if reserved, err := oldWorkloadHoldsQuota(ctx); err != nil || reserved {
		if err != nil {
			logger.Error(err, "decode old workload, leaving it unpinned")
		}
		return nil
	}
	for _, i := range nodeDelivery {
		requireNodeValue(&wl.Spec.PodSets[i].Template.Spec, workerctrl.ModelManagerRegisteredLabel, "true")
	}
	if len(pins) == 0 || !settings.WorkloadFitAffinity.ShouldValueBool(ctx) {
		return nil
	}

	group, err := r.acceleratorGroup(ctx, wl)
	if err != nil {
		logger.Error(err, "resolve the workload's instance type, leaving it unpinned")
		return nil
	}
	if group == "" {
		return nil
	}

	// 管理员创建的 InstanceType 可能指定了不符合标签语法的加速器组名称；由此构造的键会使亲和性无效，因此
	// 这类 Workload 保持不固定。
	slicedKey, sharedKey := nodefeature.FitSlicedMaxFreeUnitsLabelKey(group), nodefeature.FitSharedFreeCardsLabelKey(group)
	if !nodefeature.IsFitLabelKey(slicedKey) || !nodefeature.IsFitLabelKey(sharedKey) {
		logger.Info("accelerator group is not a valid label key part, leaving the workload unpinned", "group", group)
		return nil
	}
	for _, p := range pins {
		spec := &wl.Spec.PodSets[p.podSet].Template.Spec
		if p.slicedUnits > 0 {
			requireNodeExpression(spec, slicedKey, p.slicedUnits)
		}
		if p.sharedCards >= 2 {
			requireNodeExpression(spec, sharedKey, p.sharedCards)
		}
	}
	return nil
}

// nodeDeliveryPodSets 返回其内联 CSI 卷需要 model-manager 插件的 PodSet 索引列表。
func nodeDeliveryPodSets(wl *kueue.Workload) []int {
	var indexes []int
	for i := range wl.Spec.PodSets {
		for _, volume := range wl.Spec.PodSets[i].Template.Spec.Volumes {
			if volume.CSI != nil && volume.CSI.Driver == modelstore.DriverName {
				indexes = append(indexes, i)
				break
			}
		}
	}
	return indexes
}

// fitPinsOf 返回 Workload 中需要 fit 固定的 PodSet 及其单 Pod 需求。
// 单卡的共享请求不需要此固定：节点汇总的共享容量本身就只统计仍能分配出共享份额的加速卡。
func fitPinsOf(wl *kueue.Workload) []fitPin {
	var pins []fitPin
	for i := range wl.Spec.PodSets {
		units, cards := workerctrl.PodSetFitDemand(&wl.Spec.PodSets[i])
		if units > 0 || cards >= 2 {
			pins = append(pins, fitPin{podSet: i, slicedUnits: units, sharedCards: cards})
		}
	}
	return pins
}

// oldWorkloadHoldsQuota 报告当前请求是否为对已持有配额预留的 Workload 的 UPDATE（Kueue 不再允许变更此类
// Workload 的 PodSets）。不携带准入请求的上下文会被视为 CREATE。
func oldWorkloadHoldsQuota(ctx context.Context) (bool, error) {
	req, reqErr := ctrladmission.RequestFromContext(ctx)
	if reqErr == nil && req.Operation == admissionv1.Update {
		old := new(kueue.Workload)
		if err := json.Unmarshal(req.OldObject.Raw, old); err != nil {
			return false, err
		}
		return kueueworkload.HasQuotaReservation(old), nil
	}
	return false, nil
}

// acceleratorGroup 沿着本 operator 构建的队列链，从 Workload 的 LocalQueue 查找到带有 InstanceType 标记的
// ClusterQueue，再找到同名的 InstanceType，并返回该 InstanceType 的加速器组。当任意一环缺失或不属于本
// operator 时返回空字符串。
func (r *WorkloadWebhook) acceleratorGroup(ctx context.Context, wl *kueue.Workload) (string, error) {
	namespace := wl.Namespace
	if req, err := ctrladmission.RequestFromContext(ctx); err == nil && req.Namespace != "" {
		namespace = req.Namespace
	}
	lq := new(kueue.LocalQueue)
	err := r.Client.Get(ctx, ctrlcli.ObjectKey{Namespace: namespace, Name: string(wl.Spec.QueueName)}, lq)
	if err != nil {
		return "", ctrlcli.IgnoreNotFound(err)
	}
	cq := new(kueue.ClusterQueue)
	err = r.Client.Get(ctx, ctrlcli.ObjectKey{Name: string(lq.Spec.ClusterQueue)}, cq)
	if err != nil {
		return "", ctrlcli.IgnoreNotFound(err)
	}
	if !workerctrl.IsInstanceTypeClusterQueue(cq) {
		return "", nil
	}
	it := new(workercore.InstanceType)
	err = r.Client.Get(ctx, ctrlcli.ObjectKey{Name: cq.Name}, it)
	if err != nil {
		return "", ctrlcli.IgnoreNotFound(err)
	}
	return it.Spec.AcceleratorGroup, nil
}

// requireNodeExpression 要求节点标签 key 的值大于 need-1，并将其以 AND 关系加入到 spec 的每一个必需
// node-selector term 中；若不存在任何 term，则将其作为唯一的 term。
//
// 空的 term 以及已存在但没有任何 term 的必需选择器都会保持原样：它们各自都不匹配任何节点，添加表达式反而会
// 使之开始匹配节点。若已有键、操作符和值均相同的表达式，则不重复添加，同时保留其他已有表达式，以确保针对
// 同一键设置的更严格要求依然有效。
func requireNodeExpression(spec *core.PodSpec, key string, need int32) {
	requireNodeRequirement(spec, core.NodeSelectorRequirement{
		Key:      key,
		Operator: core.NodeSelectorOpGt,
		Values:   []string{strconv.FormatInt(int64(need)-1, 10)},
	})
}

func requireNodeValue(spec *core.PodSpec, key, value string) {
	requireNodeRequirement(spec, core.NodeSelectorRequirement{
		Key: key, Operator: core.NodeSelectorOpIn, Values: []string{value},
	})
}

func requireNodeRequirement(spec *core.PodSpec, pin core.NodeSelectorRequirement) {
	if spec.Affinity == nil {
		spec.Affinity = &core.Affinity{}
	}
	if spec.Affinity.NodeAffinity == nil {
		spec.Affinity.NodeAffinity = &core.NodeAffinity{}
	}
	na := spec.Affinity.NodeAffinity
	// 各个 term 之间是 OR 关系，因此该固定条件需要加入每一个 term；未设置任何要求的模板会获得一个新的 term。
	if na.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		na.RequiredDuringSchedulingIgnoredDuringExecution = &core.NodeSelector{
			NodeSelectorTerms: []core.NodeSelectorTerm{{MatchExpressions: []core.NodeSelectorRequirement{pin}}},
		}
		return
	}
	required := na.RequiredDuringSchedulingIgnoredDuringExecution
	for i := range required.NodeSelectorTerms {
		term := &required.NodeSelectorTerms[i]
		if len(term.MatchExpressions) == 0 && len(term.MatchFields) == 0 {
			continue
		}
		if !slices.ContainsFunc(term.MatchExpressions, func(e core.NodeSelectorRequirement) bool {
			return e.Key == pin.Key && e.Operator == pin.Operator && slices.Equal(e.Values, pin.Values)
		}) {
			term.MatchExpressions = append(term.MatchExpressions, pin)
		}
	}
}
