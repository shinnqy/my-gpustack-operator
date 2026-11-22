package worker

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlbuilder "sigs.k8s.io/controller-runtime/pkg/builder"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlevent "sigs.k8s.io/controller-runtime/pkg/event"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	ctrlpredicate "sigs.k8s.io/controller-runtime/pkg/predicate"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/controller"
	"gpustack.ai/gpustack/pkg/kubeclientset"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/systemmeta"
	"gpustack.ai/gpustack/pkg/systemname"
	"gpustack.ai/gpustack/pkg/utils/ctrlhandlerx"
	"gpustack.ai/gpustack/pkg/utils/json"
	"gpustack.ai/gpustack/pkg/utils/mapx"
	"gpustack.ai/gpustack/pkg/utils/slicex"
	"gpustack.ai/gpustack/pkg/utils/strconvx"
	"gpustack.ai/gpustack/pkg/worker/settings"
)

// NodeFlavorReconciler 以 ResourceFlavor 的名字为键，监听 ResourceFlavor 和节点的变化。
//   - 有节点贡献容量时，按“节点数 × 每节点数量”创建或更新 flavor。若开启 instance-type-derived-from-node，再为该池创建缺失的 InstanceType；已存在的规格交由 InstanceTypeReconciler 管理，管理员的修改得以保留。
//   - 最后一个贡献节点离开时删除 flavor，使队列下次按标签计算配额时撤回旧容量。
//
// flavor 按特征键、系统、架构和单节点数量区分，其附加信息描述硬件。
// 工作负载每单位使用的 CPU 和内存由派生规格在创建时设置。
//
// 把 ResourceFlavor 设为主监听对象，可以在启动时逐个复核现有 flavor。
// 即使后续节点事件只会带上新名称，也能清理因特征键或数量变化而遗留的旧对象。
type NodeFlavorReconciler struct {
	Client    ctrlcli.Client
	APIReader ctrlcli.Reader
}

var _ ctrlreconcile.Reconciler = (*NodeFlavorReconciler)(nil)

const (
	// ResourceFlavor 和 ClusterQueue 使用相同的特征标签、kubernetes.io/os 和 kubernetes.io/arch，
	// 可据此互相查找并匹配 Node 或 Devices。特征键带 general. 或 acceleratable. 前缀，值为 true。
	// ResourceFlavor 还记录每节点 .count 和全池 .capacity，队列据此取得配额；
	// 池容量等于节点数乘以单节点数量。
	_ResourceFlavorCountLabelSuffix    = ".count"
	_ResourceFlavorCapacityLabelSuffix = ".capacity"
)

// _ResourceFlavorResType 是 systemmeta 用来识别本控制器所管 ResourceFlavor 的资源类别。
const _ResourceFlavorResType = "nodes"

// IMPORTANT: 按“相同硬件+相同单机卡数+相同拓扑”打包成 ResourceFlavor(算池容量)
// -- 查现有 flavor + 查所有贡献节点
func (r *NodeFlavorReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := ctrllog.FromContext(ctx)

	// 按名字读取 ResourceFlavor。
	rf := new(kueue.ResourceFlavor)
	err := r.Client.Get(ctx, req.NamespacedName, rf)
	if err != nil {
		if !kerrors.IsNotFound(err) {
			logger.Error(err, "fetch resource flavor")
			return ctrl.Result{}, err
		}
		// 对象尚未创建，后续若找到贡献节点再创建。
		rf = nil
	}

	// 跳过正在删除的 ResourceFlavor。
	if rf != nil && rf.DeletionTimestamp != nil {
		logger.V(3).Info("skip deleted resource flavor")
		return ctrl.Result{}, nil
	}

	// 按字段索引查找参与该 flavor 的所有节点。
	ndList := new(core.NodeList)
	err = r.Client.List(ctx, ndList,
		ctrlcli.MatchingFields{IndexingNodeByScheduleFlavor: req.Name},
		ctrlcli.UnsafeDisableDeepCopy)
	if err != nil {
		logger.Error(err, "list nodes by schedule flavor")
		return ctrl.Result{}, err
	}

	// 每轮读取 instance-type-mixed-on-node，使设置变更无需重启即可生效。关闭混合模式时，
	// 加速器节点不贡献 CPU 池容量。
	mixingAllowed := settings.InstanceTypeMixedOnNode.ShouldValueBool(ctx)

	// 逐节点检查当前名称是否匹配，只保留实际贡献者。稍后从首个贡献节点取出 flavor 的身份信息。
	var contributors []*core.Node
	for i := range ndList.Items {
		nd := &ndList.Items[i]
		matched := matchNodeFlavor(nd, req.Name)
		if matched == nil {
			continue
		}
		if !mixingAllowed && !matched.Acceleratable && nodeIsAccelerated(nd) {
			continue
		}
		contributors = append(contributors, nd)
	}

	if len(contributors) == 0 {
		if rf == nil {
			if err := r.retireUnusedTopologyForFlavor(ctx, req.Name); err != nil {
				return ctrl.Result{}, err
			}
			logger.V(3).Info("resource flavor not found and unused, skip")
			return ctrl.Result{}, nil
		}
		unprofiled, err := r.hasUnprofiledContributor(ctx, rf)
		if err != nil {
			return ctrl.Result{}, err
		}
		if unprofiled {
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		// Kueue 会用 resource-in-use finalizer 保留仍被队列引用的 flavor。先标记删除，
		// NodeQueue 才会撤下旧引用，使 Kueue 能解除 finalizer，防止新队列继续使用过时容量。
		if err = r.Client.Delete(ctx, rf); err != nil {
			if kerrors.IsNotFound(err) {
				return ctrl.Result{}, nil
			}
			logger.Error(err, "delete unused resource flavor")
			return ctrl.Result{}, err
		}
		logger.V(2).Info("deleted unused resource flavor")
		return ctrl.Result{}, nil
	}

	// 所有贡献节点具有相同的特征键、系统、架构和单节点数量。取一个节点读取规格，
	// 乘以贡献节点数即得容量；每单位 CPU 和内存另由预设决定。
	node := contributors[0]
	flavor := matchNodeFlavor(node, req.Name)
	if flavor == nil {
		logger.V(3).Info("matched node no longer carries the flavor, skip")
		return ctrl.Result{}, nil
	}
	capacity := int64(len(contributors)) * flavor.Count

	keyLabel := featureKeyLabel(flavor.Acceleratable, flavor.OwnKey())
	labels := map[string]string{
		keyLabel:             "true",
		core.LabelOSStable:   flavor.OS,
		core.LabelArchStable: flavor.Arch,
		TopologyProfileLabel: node.Labels[TopologyProfileLabel],
		// 用加速器标记区分纯 CPU 和加速器 flavor。这样无论 CPU 池是否按型号拆分，
		// 都能排除同 CPU 型号的加速器 flavor。
		nodefeature.NodeAcceleratableLabelKey:         strconv.FormatBool(flavor.Acceleratable),
		keyLabel + _ResourceFlavorCountLabelSuffix:    strconvx.Itoa(flavor.Count),
		keyLabel + _ResourceFlavorCapacityLabelSuffix: strconvx.Itoa(capacity),
	}
	// 加速器 flavor 同时带有配对 CPU 的特征键。按 CPU 拆分的池可以匹配该键，合并池则忽略它。
	if flavor.Acceleratable {
		labels[nodefeature.GeneralFeatureLabelPrefix+flavor.GeneralKey] = "true"
	}
	profile := node.Labels[TopologyProfileLabel]
	// 关闭混合模式时，CPU flavor 的选择器还须匹配 Worker 写入的纯 CPU 节点标签。
	nodeLabels := maps.Clone(flavor.NodeLabels)
	nodeLabels[TopologyProfileLabel] = profile
	if !flavor.Acceleratable && !mixingAllowed {
		nodeLabels[nodefeature.NodeCPUOnlyLabelKey] = "true"
	}
	eRf := &kueue.ResourceFlavor{
		ObjectMeta: meta.ObjectMeta{
			Name:   req.Name,
			Labels: labels,
		},
		Spec: kueue.ResourceFlavorSpec{
			// 允许所有污点，以免已按配额分流的工作负载又因节点污点受阻。
			// 能否使用该节点由管理标记、特征键、系统及架构等 nodeLabels 决定。
			Tolerations: []core.Toleration{{Operator: core.TolerationOpExists}},
			NodeLabels:  nodeLabels,
			TopologyName: func() *kueue.TopologyReference {
				name := kueue.TopologyReference(topologyName(profile))
				return &name
			}(),
		},
	}

	// 每轮读取 instance-type-aware-cpu-manufacturer。
	// 它在这里仅决定加速器 flavor 是否携带 cpuDetail，改变设置不会改写该 flavor 的名称或标签。
	cpuAware := settings.InstanceTypeAwareCPUManufacturer.ShouldValueBool(ctx)

	// generalGroup 始终保存 CPU 键；acceleratorGroup 在加速器 flavor 上保存设备键，其余为空。
	// 上层结合这两个值和 CPU 厂商感知设置决定如何分池。
	eNotes := map[string]string{
		"acceleratable":    strconv.FormatBool(flavor.Acceleratable),
		"generalGroup":     flavor.GeneralKey,
		"acceleratorGroup": flavor.AcceleratorKey,
		"manufacturer":     flavor.Manufacturer,
		"product":          flavor.Product,
		"family":           flavor.Family,
		"memory":           flavor.Memory,
		"cores":            flavor.Cores,
		"topologyProfile":  profile,
	}
	// 纯 CPU flavor 始终记录原始 CPU 详情；加速器 flavor 只在启用 CPU 厂商感知时记录。
	if !flavor.Acceleratable || cpuAware {
		eNotes["cpuDetail"] = cpuDetailNote(nodefeature.ExtractGeneralDetail(node), flavor.Acceleratable)
	}
	// 混合设置可能改变不可变的节点选择器，却不改变 flavor 名称。
	// 先删除旧对象并等待 NodeQueue 移除引用，之后才能创建同名的新对象。
	if rf != nil && resourceFlavorMixingSelectorDrift(rf, eRf, flavor.Acceleratable) {
		return ctrl.Result{}, ctrlcli.IgnoreNotFound(r.Client.Delete(ctx, rf))
	}

	systemmeta.NoteResource(eRf, _ResourceFlavorResType, eNotes)
	rfAlignFn := func(aRf *kueue.ResourceFlavor) (_ *kueue.ResourceFlavor, skip bool, err error) {
		skip = true
		if !kubemeta.DeepEqual(aRf.Spec, eRf.Spec) {
			return aRf, true, fmt.Errorf("managed ResourceFlavor %q has immutable topology drift; create fresh scheduling objects", aRf.Name)
		}
		// 节点加入或离开后，更新包括容量在内的调度标签。
		if !mapx.Contain(aRf.Labels, eRf.Labels) {
			if aRf.Labels == nil {
				aRf.Labels = make(map[string]string)
			}
			for k, v := range eRf.Labels {
				aRf.Labels[k] = v
			}
			skip = false
		}
		// 将 Operator 的附加信息整体同步到期望值。systemmeta.NoteResource 只能添加或覆盖，
		// 因此须显式清理已失效的键，同时补上新出现的键。例如切换 CPU 厂商感知后，
		// cpuDetail 可能被移除或重新加入。
		if syncNodeFlavorNotes(aRf, eNotes) {
			skip = false
		}
		return aRf, skip, nil
	}
	_, err = kubeclientset.CreateWithCtrlClient(ctx, r.Client, eRf,
		kubeclientset.WithUpdateIfExisted(rfAlignFn))
	if err != nil {
		logger.Error(err, "sync resource flavor")
		return ctrl.Result{}, err
	}
	logger.V(2).Info("synced resource flavor")

	// 更换拓扑 profile 会产生一个新的不可变 flavor 名称。先创建新对象，
	// 再清理同一硬件身份下已无节点贡献的旧对象。NodeQueue 原地更新 ClusterQueue；在引用完成移交前，
	// Kueue 的 finalizer 会保留旧 flavor。
	if err = r.retireUnusedTopologyProfileSiblings(ctx, eRf, mixingAllowed); err != nil {
		logger.Error(err, "retire unused topology profile siblings")
		return ctrl.Result{}, err
	}

	// 已同步 flavor 后，若开启节点派生，则创建该池缺少的 InstanceType。已有规格保留管理员修改，
	// 后续生命周期由 InstanceTypeReconciler 负责。
	if settings.InstanceTypeDerivedFromNode.ShouldValueBool(ctx) {
		err = r.authorDerivedInstanceType(ctx, flavor)
		if err != nil {
			logger.Error(err, "author derived instance type")
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

func (r *NodeFlavorReconciler) hasUnprofiledContributor(
	ctx context.Context, flavor *kueue.ResourceFlavor,
) (bool, error) {
	selector := maps.Clone(flavor.Spec.NodeLabels)
	delete(selector, TopologyProfileLabel)
	if len(selector) == 0 {
		return false, nil
	}
	nodes := new(core.NodeList)
	if err := r.Client.List(ctx, nodes, ctrlcli.MatchingLabels(selector)); err != nil {
		return false, err
	}
	for i := range nodes.Items {
		if nodes.Items[i].Labels[TopologyProfileLabel] == "" {
			return true, nil
		}
	}
	return false, nil
}

func resourceFlavorMixingSelectorDrift(actual, wanted *kueue.ResourceFlavor, acceleratable bool) bool {
	actualLabels, wantedLabels := actual.Spec.NodeLabels, wanted.Spec.NodeLabels
	actualAcceleratable := actualLabels[nodefeature.NodeAcceleratableLabelKey]
	wantedAcceleratable := wantedLabels[nodefeature.NodeAcceleratableLabelKey]
	sameAcceleratable := actualAcceleratable == wantedAcceleratable
	sameCPUOnly := actualLabels[nodefeature.NodeCPUOnlyLabelKey] == wantedLabels[nodefeature.NodeCPUOnlyLabelKey]
	if acceleratable || (sameAcceleratable && sameCPUOnly) {
		return false
	}
	for _, labels := range []map[string]string{actual.Spec.NodeLabels, wanted.Spec.NodeLabels} {
		if value := labels[nodefeature.NodeAcceleratableLabelKey]; value != "" && value != "false" {
			return false
		}
		if value := labels[nodefeature.NodeCPUOnlyLabelKey]; value != "" && value != "true" {
			return false
		}
	}
	actualSpec, wantedSpec := actual.Spec.DeepCopy(), wanted.Spec.DeepCopy()
	delete(actualSpec.NodeLabels, nodefeature.NodeAcceleratableLabelKey)
	delete(wantedSpec.NodeLabels, nodefeature.NodeAcceleratableLabelKey)
	delete(actualSpec.NodeLabels, nodefeature.NodeCPUOnlyLabelKey)
	delete(wantedSpec.NodeLabels, nodefeature.NodeCPUOnlyLabelKey)
	return kubemeta.DeepEqual(actualSpec, wantedSpec)
}

func (r *NodeFlavorReconciler) retireUnusedTopologyProfileSiblings(
	ctx context.Context,
	wanted *kueue.ResourceFlavor,
	mixingAllowed bool,
) error {
	flavors := new(kueue.ResourceFlavorList)
	if err := r.Client.List(ctx, flavors,
		systemmeta.GetResourcesLabelSetOfType[ctrlcli.MatchingLabels](_ResourceFlavorResType)); err != nil {
		return err
	}
	wantedBase := topologyUnqualifiedFlavorName(wanted)
	for i := range flavors.Items {
		flavor := &flavors.Items[i]
		if flavor.Name == wanted.Name || flavor.DeletionTimestamp != nil ||
			topologyUnqualifiedFlavorName(flavor) != wantedBase {
			continue
		}
		nodes := new(core.NodeList)
		if err := r.Client.List(ctx, nodes,
			ctrlcli.MatchingFields{IndexingNodeByScheduleFlavor: flavor.Name},
			ctrlcli.UnsafeDisableDeepCopy); err != nil {
			return err
		}
		contributed := false
		for j := range nodes.Items {
			node := &nodes.Items[j]
			matched := matchNodeFlavor(node, flavor.Name)
			if matched != nil && (mixingAllowed || matched.Acceleratable || !nodeIsAccelerated(node)) {
				contributed = true
				break
			}
		}
		if contributed {
			continue
		}
		if err := r.Client.Delete(ctx, flavor); err != nil && !kerrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

// retireUnusedTopologyForFlavor 在最后一个引用某 profile 的 flavor 消失后清理自动生成的 Topology。
// 只要还有节点或其他 flavor 使用它，就保留该对象。
func (r *NodeFlavorReconciler) retireUnusedTopologyForFlavor(ctx context.Context, flavorName string) error {
	marker := "-" + topologyProfilePrefix
	index := strings.LastIndex(flavorName, marker)
	if index < 0 {
		return nil
	}
	profile := flavorName[index+1:]
	if len(profile) != len(topologyProfilePrefix)+16 {
		return nil
	}
	if strings.Trim(profile[len(topologyProfilePrefix):], "0123456789abcdef") != "" {
		return nil
	}

	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	nodes := new(core.NodeList)
	if err := reader.List(ctx, nodes, ctrlcli.MatchingLabels{TopologyProfileLabel: profile}); err != nil {
		return err
	}
	if len(nodes.Items) > 0 {
		return nil
	}
	topologyName := topologyName(profile)
	flavors := new(kueue.ResourceFlavorList)
	if err := reader.List(ctx, flavors); err != nil {
		return err
	}
	for i := range flavors.Items {
		ref := flavors.Items[i].Spec.TopologyName
		if ref != nil && string(*ref) == topologyName {
			return nil
		}
	}
	topology := new(kueue.Topology)
	if err := reader.Get(ctx, ctrlcli.ObjectKey{Name: topologyName}, topology); err != nil {
		return ctrlcli.IgnoreNotFound(err)
	}
	if topology.DeletionTimestamp != nil || !systemmeta.MatchResource(topology, topologyResourceType) {
		return nil
	}
	return ctrlcli.IgnoreNotFound(r.Client.Delete(ctx, topology))
}

// topologyUnqualifiedFlavorName 去掉拓扑 profile，得到多个 flavor 共用的硬件身份。
func topologyUnqualifiedFlavorName(flavor *kueue.ResourceFlavor) string {
	profile := flavor.Labels[TopologyProfileLabel]
	if profile == "" {
		return flavor.Name
	}
	return strings.TrimSuffix(flavor.Name, "-"+profile)
}

// syncNodeFlavorNotes 使 Operator 管理的附加信息与 want 一致，返回是否有变化。
// 同步时移除过期的键并加入缺少的键，例如按当前设置增删 cpuDetail；
// 没有 Operator 记录前缀的注解由原所有者保留。
func syncNodeFlavorNotes(rf *kueue.ResourceFlavor, want map[string]string) bool {
	rt, have := systemmeta.DescribeResource(rf)
	if rt == _ResourceFlavorResType && maps.Equal(have, want) {
		return false
	}
	for k := range rf.Annotations {
		if strings.HasPrefix(k, systemmeta.ResourceNoteAnnoPrefix) {
			delete(rf.Annotations, k)
		}
	}
	systemmeta.NoteResource(rf, _ResourceFlavorResType, want)
	return true
}

// matchNodeFlavor 查找该节点上与 flavorName 匹配的 flavor。
// 节点没有相应的拓扑 profile 或硬件规格时返回 nil。
func matchNodeFlavor(nd *core.Node, flavorName string) *nodefeature.NodeFlavor {
	profile := nd.Labels[TopologyProfileLabel]
	if profile == "" {
		return nil
	}
	for _, f := range nodefeature.ExtractNodeFlavors(nd) {
		if topologyQualifiedFlavorName(f.Name, profile) == flavorName {
			matched := f
			return &matched
		}
	}
	return nil
}

func topologyQualifiedFlavorName(flavorName, profile string) string {
	return flavorName + "-" + profile
}

// featureKeyLabel 返回标识池的标签键，标签值为 true。CPU 使用 general.feature.gpustack.ai/<key>，
// 设备使用 acceleratable.feature.gpustack.ai/<key>。
func featureKeyLabel(acceleratable bool, key string) string {
	if acceleratable {
		return nodefeature.AcceleratableFeatureLabelPrefix + key
	}
	return nodefeature.GeneralFeatureLabelPrefix + key
}

// nodeIsAccelerated 根据 feature.gpustack.ai/acceleratable=true 判断节点是否带有加速器。
func nodeIsAccelerated(nd *core.Node) bool {
	return kubemeta.IsLabeled(nd, nodefeature.NodeAcceleratableLabelKey, "true")
}

const (
	// _InstanceTypeDerivedFromNodeLabel 记录该规格由节点 ResourceFlavor 自动派生。标签仅标识来源，
	// 节点消失后仍会保留已派生的规格。
	_InstanceTypeDerivedFromNodeLabel = "schedule.gpustack.ai/derived-from-node"

	// cpuOnlyDisplayName 是合并 CPU 池的默认显示名，池内可能包含多种 CPU，无法用单一产品名代表。
	cpuOnlyDisplayName = "CPU-only"

	// derivedLocalStorage 是派生规格每单位本地存储的默认值。
	// 工作负载所需存储与加速器型号没有固定关系，因此 CPU 和 RAM 按产品取预设，而本地存储用统一值。
	// 管理员可按规格另行设置。
	derivedLocalStorage = "100Gi"
)

// authorDerivedInstanceType 以已同步的 flavor 创建派生规格，
// 写入 CPU 组、加速器组、设备标记、系统、架构、每单位资源、派生来源和默认显示名。
// 显示名通常是产品名，合并 CPU 池为 CPU-only。只执行创建，遇到 AlreadyExists 即结束，
// 因此不会改回管理员编辑过的名字，也不会替管理员自建的规格命名。
func (r *NodeFlavorReconciler) authorDerivedInstanceType(ctx context.Context, flavor *nodefeature.NodeFlavor) error {
	logger := ctrllog.FromContext(ctx)

	cpuAware := settings.InstanceTypeAwareCPUManufacturer.ShouldValueBool(ctx)
	name, generalGroup, acceleratorGroup := flavor.DerivedInstanceTypeIdentity(cpuAware)
	unitCpu, unitRam := derivedUnitResources(flavor)

	// 合并池可以容纳多种 CPU，默认显示 CPU-only。
	displayName := flavor.Product
	if !flavor.Acceleratable && !cpuAware {
		displayName = cpuOnlyDisplayName
	}
	if runes := []rune(displayName); len(runes) > 64 {
		displayName = string(runes[:64])
	}

	it := &workercore.InstanceType{
		ObjectMeta: meta.ObjectMeta{
			Name:   name,
			Labels: map[string]string{_InstanceTypeDerivedFromNodeLabel: "true"},
		},
		Spec: workercore.InstanceTypeSpec{
			GeneralGroup:     generalGroup,
			AcceleratorGroup: acceleratorGroup,
			Acceleratable:    flavor.Acceleratable,
			OS:               flavor.OS,
			Arch:             flavor.Arch,
			DisplayName:      displayName,
			UnitResources:    workercore.InstanceTypeUnitResources{CPU: unitCpu, RAM: unitRam},
			LocalStorage:     derivedLocalStorage,
		},
	}
	err := r.Client.Create(ctx, it)
	if err != nil {
		if !kerrors.IsAlreadyExists(err) {
			return err
		}
		return nil
	}

	logger.V(2).Info("authored derived instance type",
		"instance type", it.Name)
	return nil
}

// cpuDetailNote 将 CPU 详情编码为 InstanceType webhook 可直接解析的 JSON。
// 纯 CPU flavor 保存 InstanceTypeCPU，厂商、产品、系列由顶层字段描述。
// 加速器 flavor 保存 InstanceTypeAcceleratorCPU，其中还带有 CPU 自身的厂商和型号，以便与设备区分。
// 两端使用相同的 workercore 类型，此可选记录的编码错误由 ShouldMarshal 处理。
func cpuDetailNote(d nodefeature.CPUDetail, acceleratable bool) string {
	cpu := workercore.InstanceTypeCPU{
		PhysicalCores:          d.PhysicalCores,
		ThreadsPerPhysicalCore: d.ThreadsPerPhysicalCore,
		LogicalCores:           d.LogicalCores,
		Stepping:               d.Stepping,
		ClockSpeed:             d.ClockSpeed,
		MaxClockSpeed:          d.MaxClockSpeed,
		CacheLine:              d.CacheLine,
		Cache: workercore.InstanceTypeCPUCache{
			L1I: d.CacheL1I,
			L1D: d.CacheL1D,
			L2:  d.CacheL2,
			L3:  d.CacheL3,
		},
	}
	if acceleratable {
		return string(json.ShouldMarshal(workercore.InstanceTypeAcceleratorCPU{
			Manufacturer:    d.Manufacturer,
			Product:         d.Product,
			Family:          d.Family,
			InstanceTypeCPU: cpu,
		}))
	}
	return string(json.ShouldMarshal(cpu))
}

// derivedUnitResources 计算派生规格的每单位 CPU 和 RAM。纯 CPU 固定为 1 核、2Gi，
// webhook 同样要求每单位恰为 1 核。加速器以整卡为单位，按产品查预设，无法识别时沿用 4 核、16Gi。
// 这里只决定首次派生的值，后续同步保留已有规格。
func derivedUnitResources(flavor *nodefeature.NodeFlavor) (unitCpu, unitRam string) {
	if !flavor.Acceleratable {
		return "1", "2Gi"
	}
	return nodefeature.PresetUnitResources(flavor.Manufacturer, flavor.Product)
}

const (
	// IndexingNodeByScheduleFlavor 用节点所贡献的 ResourceFlavor 名称为受管节点建立索引，
	// 包括 CPU flavor 和每个设备键的 flavor。节点退出管理后不再进入索引，
	// 因此控制器能发现已无贡献者的 flavor。
	IndexingNodeByScheduleFlavor = "nodes.schedule.gpustack.ai/flavor"
)

// indexNodeByScheduleFlavor 提取字段索引键。正在删除但仍存在的节点暂计在内，避免资源池反复变化。
// 已退出管理的节点则直接排除，便于清理旧 flavor。
func indexNodeByScheduleFlavor(obj ctrlcli.Object) []string {
	nd, ok := obj.(*core.Node)
	if !ok || nd == nil {
		return nil
	}
	if !kubemeta.IsLabeled(nd, systemname.ManagedLabelKey, "true") {
		return nil
	}
	profile := nd.Labels[TopologyProfileLabel]
	if profile == "" {
		return nil
	}
	return slicex.Transform(nodefeature.ExtractNodeFlavors(nd),
		func(f nodefeature.NodeFlavor) string {
			return topologyQualifiedFlavorName(f.Name, profile)
		})
}

// nodeFlavorNodeUpdated 判断更新前后的 flavor 是否需要重新协调。
func nodeFlavorNodeUpdated(oldNd, newNd *core.Node) bool {
	if newNd.DeletionTimestamp != nil {
		return false
	}
	// 管理标记或特征标签变化时触发。
	if !mapx.EqualWithStringPrefix(oldNd.Labels, newNd.Labels,
		systemname.ManagedLabelKey,
		TopologyProfileLabel,
		nodefeature.FeatureLabelPrefix,
		nodefeature.GeneralFeatureLabelPrefix,
		nodefeature.AcceleratableFeatureLabelPrefix) {
		return true
	}
	// 节点贡献的 flavor 集合变化时也须触发。
	// NFD 可能在节点注册之后才公布 cpu-model 标签和 cpu-name 注解；
	// 旧通用 flavor 在 NodeFeature 重新派生标签前就可能失去节点，此次更新仍能把它加入队列。
	oldFlavors, newFlavors := indexNodeByScheduleFlavor(oldNd), indexNodeByScheduleFlavor(newNd)
	slices.Sort(oldFlavors)
	slices.Sort(newFlavors)
	return !slices.Equal(oldFlavors, newFlavors)
}

func (r *NodeFlavorReconciler) SetupController(ctx context.Context, opts controller.SetupOptions) error {
	// 注册节点到 flavor 的字段索引。
	fi := opts.Manager.GetFieldIndexer()
	err := fi.IndexField(ctx, &core.Node{}, IndexingNodeByScheduleFlavor, indexNodeByScheduleFlavor)
	if err != nil {
		return fmt.Errorf("index node '%s': %w", IndexingNodeByScheduleFlavor, err)
	}

	r.Client = opts.Manager.GetClient()
	r.APIReader = opts.Manager.GetAPIReader()

	return ctrl.NewControllerManagedBy(opts.Manager).
		Named("nodeflavor").
		For(
			// 按 ResourceFlavor 名称协调。启动时重新检查所有受管 flavor，
			// 便于清理因特征键或数量变化而失去节点的旧对象。
			&kueue.ResourceFlavor{},
			ctrlbuilder.WithPredicates(
				// 只处理本控制器管理的 ResourceFlavor。
				ctrlpredicate.NewPredicateFuncs(func(obj ctrlcli.Object) bool {
					return systemmeta.MatchResource(obj, _ResourceFlavorResType)
				}),
				// 创建（包括启动时首次同步）以及 spec、调度标签或附加信息变化时重新协调。
				// 最后一个 flavor 删除后要清理自动生成的 Topology，
				// 仍有需求时节点事件会重新创建 flavor。
				ctrlpredicate.Funcs{
					DeleteFunc: func(e ctrlevent.DeleteEvent) bool {
						return true
					},
					UpdateFunc: func(e ctrlevent.UpdateEvent) bool {
						oldRf, newRf := e.ObjectOld.(*kueue.ResourceFlavor), e.ObjectNew.(*kueue.ResourceFlavor)
						if newRf.DeletionTimestamp == nil {
							// spec 变化时触发。
							if !kubemeta.DeepEqual(oldRf.Spec, newRf.Spec) {
								return true
							}
							// 调度标签变化时触发。
							if !mapx.EqualWithStringPrefix(oldRf.Labels, newRf.Labels,
								nodefeature.GeneralFeatureLabelPrefix,
								nodefeature.AcceleratableFeatureLabelPrefix,
								core.LabelOSStable,
								core.LabelArchStable) {
								return true
							}
							// 附加信息变化时触发。
							if !systemmeta.EqualResourceTypeAndNotes(oldRf, newRf) {
								return true
							}
						}
						return false
					},
				},
			),
		).
		Watches(
			// 监听 Node，将对应的 ResourceFlavor 名称放入工作队列。
			&core.Node{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFunc(
				5*time.Second,
				r.enqueueResourceFlavorWhenNodeChanged,
			),
			ctrlbuilder.WithPredicates(
				// 节点创建、删除，或管理标记、特征标签、所贡献的 flavor 变化时触发。
				// 这样节点退出管理或离开某个组后能撤下旧容量。
				ctrlpredicate.Funcs{
					UpdateFunc: func(e ctrlevent.UpdateEvent) bool {
						return nodeFlavorNodeUpdated(e.ObjectOld.(*core.Node), e.ObjectNew.(*core.Node))
					},
				},
			),
		).
		Watches(
			// 监听派生 InstanceType 的删除事件，使规格意外丢失后能触发重建。
			// flavor 本身可能没有任何变化，连 informer 的定期同步也会被更新过滤器忽略，
			// 因此需要单独的监听。
			&workercore.InstanceType{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFunc(
				5*time.Second,
				r.enqueueResourceFlavorsWhenDerivedInstanceTypeDeleted,
			),
			ctrlbuilder.WithPredicates(
				// 只监听带有派生标记的 InstanceType，管理员自建的规格不由此处创建。
				ctrlpredicate.NewPredicateFuncs(func(obj ctrlcli.Object) bool {
					return obj.GetLabels()[_InstanceTypeDerivedFromNodeLabel] == "true"
				}),
				// 只响应派生规格的删除。创建已由本控制器发起，后续状态变化也不影响规格是否存在。
				ctrlpredicate.Not(ctrlpredicate.Funcs{
					DeleteFunc: func(e ctrlevent.DeleteEvent) bool {
						return false
					},
				}),
			),
		).
		Complete(r)
}

// enqueueResourceFlavorsWhenDerivedInstanceTypeDeleted 在派生规格消失时将所有受管 ResourceFlavor 排入队列，
// 由匹配的 flavor 重新创建规格。
//
// InstanceType 没有记录具体的源 flavor，且集群内 flavor 数量通常较少，因此逐个复核。
// 创建逻辑可重复执行，已有关联规格的 flavor 无需写入。
func (r *NodeFlavorReconciler) enqueueResourceFlavorsWhenDerivedInstanceTypeDeleted(
	ctx context.Context,
	obj ctrlcli.Object,
) []ctrlreconcile.Request {
	logger := ctrllog.FromContext(ctx).
		WithValues("instance type", ctrlcli.ObjectKeyFromObject(obj))

	rfList := new(kueue.ResourceFlavorList)
	err := r.Client.List(ctx, rfList,
		systemmeta.GetResourcesLabelSetOfType[ctrlcli.MatchingLabels](_ResourceFlavorResType),
		ctrlcli.UnsafeDisableDeepCopy)
	if err != nil {
		logger.Error(err, "list resource flavors for deleted derived instance type")
		return nil
	}
	if len(rfList.Items) == 0 {
		return nil
	}

	reqs := make([]ctrlreconcile.Request, 0, len(rfList.Items))
	for i := range rfList.Items {
		reqs = append(reqs, ctrlreconcile.Request{
			NamespacedName: ctrlcli.ObjectKey{
				Name: rfList.Items[i].Name,
			},
		})
	}

	logger.V(2).Info("enqueue resource flavors from deleted derived instance type",
		"requests", reqs)
	return reqs
}

func (r *NodeFlavorReconciler) enqueueResourceFlavorWhenNodeChanged(
	ctx context.Context,
	obj ctrlcli.Object,
) []ctrlreconcile.Request {
	logger := ctrllog.FromContext(ctx).
		WithValues("node", ctrlcli.ObjectKeyFromObject(obj))

	nd := obj.(*core.Node)
	profile := nd.Labels[TopologyProfileLabel]
	if profile == "" {
		return nil
	}

	flavors := nodefeature.ExtractNodeFlavors(nd)
	if len(flavors) == 0 {
		logger.V(2).Info("node has no flavor")
		return nil
	}

	reqs := make([]ctrlreconcile.Request, 0, len(flavors))
	for i := range flavors {
		if flavors[i].Name == "" {
			continue
		}
		reqs = append(reqs, ctrlreconcile.Request{
			NamespacedName: ctrlcli.ObjectKey{
				Name: topologyQualifiedFlavorName(flavors[i].Name, profile),
			},
		})
	}
	if len(reqs) == 0 {
		return nil
	}

	logger.V(2).Info("enqueue resource flavor from node", "requests", reqs)
	return reqs
}
