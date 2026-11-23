package worker

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrladmission "sigs.k8s.io/controller-runtime/pkg/webhook/admission"
	kueuectrlconst "sigs.k8s.io/kueue/pkg/controller/constants"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/utils/ctrlclix"
	"gpustack.ai/gpustack/pkg/webhook"
	workerctrl "gpustack.ai/gpustack/pkg/worker/controllers/worker"
)

// PodWebhook 拦截路由到 GPUStack 队列（通过 "kueue.x-k8s.io/queue-name" 标签选择）的核心 Pod，
// 并规范化每个容器的加速器请求。
//
//   - Mutating：逻辑切片的单卡算力预算（.sliced.cores-percentage）默认为整卡（100），
//     其单卡显存（VRAM）预算（.sliced.memory-percentage 或 .sliced.memory-mib）会折算进
//     用于计费配额的 .sliced.units，而分区请求则将 profile 的 VRAM 折算进 .partitioned.units。
//     客户端提供的任何 units 值都会被忽略并重新计算，因为该值仅由 webhook 推导。
//     两张或更多卡的共享请求会被固定到至少拥有相应卡数的节点上（参见 pinSharedCardCount）。
//   - Validating：强制执行规范化的加速器请求规则（参见 validatePodAcceleratorRequest）
//     以及各资源族的形态检查。
//
// nolint: lll
// +k8s:webhook-gen:validating:group="",version="v1",resource="pods",scope="Namespaced"
// +k8s:webhook-gen:validating:operations=["CREATE"],failurePolicy="Fail",sideEffects="None",matchPolicy="Equivalent",timeoutSeconds=10
// +k8s:webhook-gen:validating:objectSelector={"matchExpressions":[{"key":"kueue.x-k8s.io/queue-name","operator":"Exists"}]}
// +k8s:webhook-gen:validating:namePrefix="gpustack-worker"
// +k8s:webhook-gen:mutating:group="",version="v1",resource="pods",scope="Namespaced"
// +k8s:webhook-gen:mutating:operations=["CREATE"],failurePolicy="Fail",sideEffects="None",matchPolicy="Equivalent",timeoutSeconds=10
// +k8s:webhook-gen:mutating:objectSelector={"matchExpressions":[{"key":"kueue.x-k8s.io/queue-name","operator":"Exists"}]}
// +k8s:webhook-gen:mutating:namePrefix="gpustack-worker"
type PodWebhook struct {
	Client    ctrlcli.Client
	APIReader ctrlcli.Reader
}

func (r *PodWebhook) SetupWebhook(_ context.Context, opts webhook.SetupOptions) (runtime.Object, error) {
	r.Client = opts.Manager.GetClient()
	r.APIReader = opts.Manager.GetAPIReader()

	return &core.Pod{}, nil
}

var (
	_ ctrladmission.Validator[runtime.Object] = (*PodWebhook)(nil)
	_ ctrladmission.Defaulter[runtime.Object] = (*PodWebhook)(nil)
)

// containerGroup 是容器所属的生命周期组。请求规则按生命周期而非容器所在字段划定范围，
// 因为不同组中的两个加速器声明并非互不干扰：kubelet 会在 Pod 的整个生命周期内将已完成的
// init 容器的设备保留在 Pod 记录中，而调度器对 Pod 的每个资源键仅按 max(Σ init, Σ app)
// 计费——因此两份声明只消耗一份配额却实际占用两份设备。
type containerGroup string

const (
	// containerGroupInit 表示未设置 restartPolicy: Always 的 spec.initContainers。
	containerGroupInit containerGroup = "initContainers"
	// containerGroupRunning 表示 spec.containers。
	containerGroupRunning containerGroup = "containers"
	// containerGroupSidecar 是可重启的 init 容器（即原生 sidecar）。它不属于上述任何一组——
	// 它在 init 阶段启动并持续运行，因此与后续所有 init 容器以及所有 app 容器的生命周期重叠——
	// 并且完全不允许声明任何加速器。
	containerGroupSidecar containerGroup = "sidecar"
)

// logicalResourceNames 是容器可请求的按基础资源划分的逻辑（软件）切片资源名族。
type logicalResourceNames struct {
	card     core.ResourceName // <base>.sliced
	units    core.ResourceName // <base>.sliced.units
	coresPct core.ResourceName // <base>.sliced.cores-percentage
	memPct   core.ResourceName // <base>.sliced.memory-percentage
	memMib   core.ResourceName // <base>.sliced.memory-mib
}

// logicalResourceNamesForBase 为加速器基础资源（例如 "nvidia.com/gpu"）构建逻辑切片资源族。
func logicalResourceNamesForBase(base core.ResourceName) logicalResourceNames {
	return logicalResourceNames{
		card:     base + core.ResourceName(nodefeature.SlicedResourceNameSuffix),
		units:    base + core.ResourceName(nodefeature.SlicedUnitsResourceNameSuffix),
		coresPct: base + core.ResourceName(nodefeature.SlicedCoresPercentageResourceNameSuffix),
		memPct:   base + core.ResourceName(nodefeature.SlicedMemoryPercentageResourceNameSuffix),
		memMib:   base + core.ResourceName(nodefeature.SlicedMemoryMibResourceNameSuffix),
	}
}

// partitionResourceNames 是容器可请求的按基础资源划分的物理（硬件）分区资源名族。
// 按 profile 区分的键并非每个基础资源固定——它携带厂商的分区类型和 profile 名称——
// 因此是从容器请求中解析得到的，而不在此处构建。
type partitionResourceNames struct {
	card  core.ResourceName // <base>.partitioned
	units core.ResourceName // <base>.partitioned.units
}

// partitionResourceNamesForBase 为加速器基础资源（例如 "nvidia.com/gpu"）构建物理分区资源族。
func partitionResourceNamesForBase(base core.ResourceName) partitionResourceNames {
	return partitionResourceNames{
		card:  base + core.ResourceName(nodefeature.PartitionedResourceNameSuffix),
		units: base + core.ResourceName(nodefeature.PartitionedUnitsResourceNameSuffix),
	}
}

// acceleratorBaseOf 返回资源名所属的加速器基础资源及其所属资源族。对于四大加速器族之外的
// 任何资源（包括可见性资源），它都返回 false；可见性资源被刻意排除在这些族之外，以便
// 单一资源族规则会忽略它。
func acceleratorBaseOf(name core.ResourceName) (core.ResourceName, nodefeature.ResourceFamily, bool) {
	family := nodefeature.ResourceFamilyOf(name)
	s := string(name)
	switch family {
	case nodefeature.ResourceFamilyExclusive:
		return name, family, true
	case nodefeature.ResourceFamilyShared:
		base, _ := strings.CutSuffix(s, nodefeature.SharedResourceNameSuffix)
		return core.ResourceName(base), family, true
	case nodefeature.ResourceFamilySliced:
		base, _, _ := strings.Cut(s, nodefeature.SlicedResourceNameSuffix)
		return core.ResourceName(base), family, true
	case nodefeature.ResourceFamilyPartitioned:
		base, _, _ := strings.Cut(s, nodefeature.PartitionedResourceNameSuffix)
		return core.ResourceName(base), family, true
	}
	return "", nodefeature.ResourceFamilyNone, false
}

// containerClaims 扫描一次容器的 Requests 和 Limits，并按资源族分组返回已排序且去重的
// 所声明加速器基础资源。资源族中的每个键都会计入——包括卡键本身以及 ".sliced.units" 或
// 按 profile 区分的分区键等所有子键——因此仅指定子键的请求同样会被校验和折算，
// 不会未经检查而漏过。
func containerClaims(ctr *core.Container) map[nodefeature.ResourceFamily][]core.ResourceName {
	bases := make(map[nodefeature.ResourceFamily]sets.Set[core.ResourceName])
	for _, rl := range []core.ResourceList{ctr.Resources.Requests, ctr.Resources.Limits} {
		for name := range rl {
			base, family, ok := acceleratorBaseOf(name)
			if !ok {
				continue
			}
			if bases[family] == nil {
				bases[family] = sets.New[core.ResourceName]()
			}
			bases[family].Insert(base)
		}
	}
	claims := make(map[nodefeature.ResourceFamily][]core.ResourceName, len(bases))
	for family, set := range bases {
		claims[family] = sets.List(set)
	}
	return claims
}

// partitionRequest 表示容器设置的一个按 profile 区分的分区键，包含其指定的 profile 名称
// 和请求的数量。
type partitionRequest struct {
	name     core.ResourceName
	profile  string
	quantity resource.Quantity
}

// containerPartitionRequests 返回容器为某个加速器基础资源设置的按 profile 区分的分区键，
// 按 profile 名称排序。该键内嵌了厂商的分区类型，因此需要直接从容器中读取，而不是根据
// webhook 无法映射到具体厂商的基础资源重新构造。与本 webhook 的其他位置一致，Requests 优先于 Limits。
func containerPartitionRequests(ctr *core.Container, base core.ResourceName) []partitionRequest {
	byProfile := make(map[string]partitionRequest)
	for _, rl := range []core.ResourceList{ctr.Resources.Limits, ctr.Resources.Requests} {
		for name, q := range rl {
			profile, ok := nodefeature.PartitionedProfileOf(name)
			if !ok {
				continue
			}
			if b, _, _ := acceleratorBaseOf(name); b == base {
				byProfile[profile] = partitionRequest{name: name, profile: profile, quantity: q}
			}
		}
	}
	out := make([]partitionRequest, 0, len(byProfile))
	for _, profile := range sets.List(sets.KeySet(byProfile)) {
		out = append(out, byProfile[profile])
	}
	return out
}

// containerPartitionProfiles 返回容器为某个加速器基础资源请求的已排序且去重的分区 profile 名称。
func containerPartitionProfiles(ctr *core.Container, base core.ResourceName) []string {
	reqs := containerPartitionRequests(ctr, base)
	profiles := make([]string, 0, len(reqs))
	for i := range reqs {
		profiles = append(profiles, reqs[i].profile)
	}
	return profiles
}

// podContainer 将容器与其生命周期组及字段路径配对。
type podContainer struct {
	ctr   *core.Container
	group containerGroup
	path  *field.Path
}

// podContainers 按清单顺序返回 Pod 的所有 init 容器和 app 容器，及其对应的
// 生命周期组和字段路径。
func podContainers(pod *core.Pod) []podContainer {
	out := make([]podContainer, 0, len(pod.Spec.InitContainers)+len(pod.Spec.Containers))
	for i := range pod.Spec.InitContainers {
		ctr := &pod.Spec.InitContainers[i]
		group := containerGroupInit
		if ctr.RestartPolicy != nil && *ctr.RestartPolicy == core.ContainerRestartPolicyAlways {
			group = containerGroupSidecar
		}
		out = append(out, podContainer{
			ctr: ctr, group: group,
			path: field.NewPath("spec", "initContainers").Index(i),
		})
	}
	for i := range pod.Spec.Containers {
		out = append(out, podContainer{
			ctr: &pod.Spec.Containers[i], group: containerGroupRunning,
			path: field.NewPath("spec", "containers").Index(i),
		})
	}
	return out
}

// containerResource 返回 name 对应的请求数量（优先取 Requests，其次回退到 Limits），
// 以及该资源键是否存在。
func containerResource(ctr *core.Container, name core.ResourceName) (resource.Quantity, bool) {
	if q, ok := ctr.Resources.Requests[name]; ok {
		return q, true
	}
	if q, ok := ctr.Resources.Limits[name]; ok {
		return q, true
	}
	return resource.Quantity{}, false
}

// setContainerResource 同时写入 Requests 和 Limits，保持扩展资源两处的数量一致。
func setContainerResource(ctr *core.Container, name core.ResourceName, q resource.Quantity) {
	if ctr.Resources.Requests == nil {
		ctr.Resources.Requests = core.ResourceList{}
	}
	if ctr.Resources.Limits == nil {
		ctr.Resources.Limits = core.ResourceList{}
	}
	ctr.Resources.Requests[name] = q
	ctr.Resources.Limits[name] = q
}

func (r *PodWebhook) Default(ctx context.Context, obj runtime.Object) error {
	pod := obj.(*core.Pod)

	// 单卡 VRAM 采用惰性查询，仅在容器需要折算绝对值 .sliced.memory-mib 请求时才查找；
	// -1 表示"尚未查询"。
	cardVRAMMib := int64(-1)

	// 折算会覆盖持有声明的任一容器组：请求规则将其限制在单个组内，因此折算所有发出
	// 声明的容器就恰好只折算该组。
	for _, pc := range podContainers(pod) {
		ctr := pc.ctr
		claims := containerClaims(ctr)

		// 根据分区规格的显存折算每卡配额，与绝对 MiB 形式的软切请求使用同一计量尺度。硬切分只填写自己的资源
		// 键。
		for _, base := range claims[nodefeature.ResourceFamilyPartitioned] {
			// 找出容器点名的 profile
			profiles := containerPartitionProfiles(ctr, base)
			if len(profiles) != 1 {
				// 合法请求必须恰好指定一种规格。没有指定或指定多种时交给校验阶段处理。
				continue
			}
			units, err := r.partitionContainerUnits(ctx, pod, ctr, base, profiles[0])
			if err != nil {
				return err
			}
			names := partitionResourceNamesForBase(base)
			setContainerResource(ctr, names.units, *resource.NewQuantity(units, resource.DecimalSI))
		}

		for _, base := range claims[nodefeature.ResourceFamilySliced] {
			names := logicalResourceNamesForBase(base)

			// 未声明算力百分比时，一张卡的计算预算默认为 100%。
			if _, ok := containerResource(ctr, names.coresPct); !ok {
				setContainerResource(ctr, names.coresPct, *resource.NewQuantity(100, resource.DecimalSI))
			}

			// 每次都从显存预算重新计算 .sliced.units，并覆盖客户端填写的值。先检查输入上限，防止整数运算溢出。
			memPctQ, hasMemPct := containerResource(ctr, names.memPct)
			memMibQ, hasMemMib := containerResource(ctr, names.memMib)
			var units int64
			switch {
			case hasMemPct && memPctQ.Value() > 0:
				// 同时给出百分比和 MiB 时优先取百分比。每 1% 显存对应 D/100 个单位。
				if memPctQ.Value() > 100 {
					return fmt.Errorf("container %q: %s must be within (0, 100]", ctr.Name, names.memPct)
				}
				units = memPctQ.Value() * nodefeature.ResourceMaxUnits / 100
			case hasMemMib && memMibQ.Value() > 0:
				if cardVRAMMib < 0 {
					var err error
					cardVRAMMib, err = r.cardVRAMMib(ctx, pod)
					if err != nil {
						return err
					}
				}
				if memMibQ.Value() > cardVRAMMib {
					return fmt.Errorf("container %q: %s must not exceed the per-card VRAM (%d MiB)",
						ctr.Name, names.memMib, cardVRAMMib)
				}
				units = nodefeature.MemoryMibToUnits(memMibQ.Value(), cardVRAMMib)
			default:
				continue // 没有可换算的显存请求，交给校验阶段处理。
			}
			if units <= 0 {
				return fmt.Errorf("container %q: cannot derive a positive %s from the requested memory",
					ctr.Name, names.units)
			}
			setContainerResource(ctr, names.units, *resource.NewQuantity(units, resource.DecimalSI))
		}
	}

	return r.pinSharedCardCount(ctx, pod)
}

// pinSharedCardCount 将 N >= 2 的共享请求限定到至少拥有 N 张资源池加速卡的节点上，
// 具体做法是要求每个加速 flavor 和节点都携带的单节点卡数标签大于 N-1。N 取自最大容器
// 的请求量而非 Pod 总和：DevicePlugin 会独立为每个容器分配 token，因此容器之间可以共享卡。
//
// 共享请求指定的是 N 张不同的卡，但节点每张卡会通告十个共享 token，因此无论 Kueue 的
// flavor 分配还是 TAS，仅凭资源本身都无法区分单卡节点与四卡节点。资源池的 flavor 会按
// 节点从小到大的顺序尝试，且 AdmissionCheck 驱逐会重新从头扫描；若没有该标签，TAS 放到
// 卡数不足节点上的请求在被驱逐后仍会反复被放回原处，即便更大节点还有空间。Kueue 会将
// flavor 自身的 nodeLabels 与 Pod 的 required 节点亲和性进行匹配，因此通过该亲和性来表达此约束。
//
// 该标签表示节点的总卡数（包含已分区的卡），且只有 Pod 会经过此 webhook，因此 Kueue 在任何
// Pod 存在之前基于 Job 模板构建的 Workload 不会被固定。
func (r *PodWebhook) pinSharedCardCount(ctx context.Context, pod *core.Pod) error {
	// IMPORTANT: 第一步：算出这个 Pod 的节点至少要有几张物理卡
	var cards int64
	for _, pc := range podContainers(pod) {
		for _, base := range containerClaims(pc.ctr)[nodefeature.ResourceFamilyShared] {
			if q, ok := containerResource(pc.ctr, base+nodefeature.SharedResourceNameSuffix); ok {
				cards = max(cards, q.Value())
			}
		}
	}
	if cards < 2 {
		return nil
	}

	// IMPORTANT: 第二步：查出这个 Pod 对应的“加速器组名”是什么
	it, err := r.frontingInstanceType(ctx, pod)
	if err != nil {
		return err
	}
	if it.Spec.AcceleratorGroup == "" {
		return fmt.Errorf("instance type %s names no accelerator group, so a shared request of %d cannot be pinned to nodes with that many cards",
			it.Name, cards)
	}
	pin := core.NodeSelectorRequirement{
		Key:      nodefeature.AcceleratableFeatureLabelPrefix + it.Spec.AcceleratorGroup + ".count",
		Operator: core.NodeSelectorOpGt,
		Values:   []string{strconv.FormatInt(cards-1, 10)},
	}

	// IMPORTANT: 第三步：把条件安全、幂等地写进 NodeAffinity
	if pod.Spec.Affinity == nil {
		pod.Spec.Affinity = &core.Affinity{}
	}
	if pod.Spec.Affinity.NodeAffinity == nil {
		pod.Spec.Affinity.NodeAffinity = &core.NodeAffinity{}
	}
	na := pod.Spec.Affinity.NodeAffinity
	if na.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		na.RequiredDuringSchedulingIgnoredDuringExecution = &core.NodeSelector{}
	}
	required := na.RequiredDuringSchedulingIgnoredDuringExecution
	// 各选择项之间是“或”的关系，每个选择项都要加上卡数条件。原先没有选择项时先建一个。
	if len(required.NodeSelectorTerms) == 0 {
		required.NodeSelectorTerms = []core.NodeSelectorTerm{{}}
	}
	for i := range required.NodeSelectorTerms {
		term := &required.NodeSelectorTerms[i]
		if !slices.ContainsFunc(term.MatchExpressions, func(e core.NodeSelectorRequirement) bool {
			return e.Key == pin.Key && e.Operator == pin.Operator && slices.Equal(e.Values, pin.Values)
		}) {
			term.MatchExpressions = append(term.MatchExpressions, pin)
		}
	}
	return nil
}

// frontingInstanceType 根据 Pod 的 LocalQueue 名称，查找带有对应队列入口标签的 InstanceType。
// InstanceType 的默认化 webhook 写入此标签，因此加速器组取自 Operator 管理的规格。
// 找不到匹配的规格或匹配到多个时，无法确定该使用哪一组设备。
func (r *PodWebhook) frontingInstanceType(ctx context.Context, pod *core.Pod) (*workercore.InstanceType, error) {
	lqName := pod.Labels[kueuectrlconst.QueueLabel]
	if lqName == "" {
		return nil, fmt.Errorf("pod has no %q label", kueuectrlconst.QueueLabel)
	}

	itList := new(workercore.InstanceTypeList)
	sel := ctrlcli.MatchingLabels{QueueEntranceLabelKey: lqName}
	err := r.Client.List(ctx, itList, sel)
	if err != nil || len(itList.Items) == 0 {
		// 控制器缓存可能尚未同步，直接向 API 服务器再查一次。
		if err = r.APIReader.List(ctx, itList, sel, ctrlclix.WithoutQuorum); err != nil {
			return nil, fmt.Errorf("list instance types fronting local queue %s: %w", lqName, err)
		}
	}
	switch len(itList.Items) {
	case 0:
		return nil, fmt.Errorf("no instance type fronts local queue %s", lqName)
	case 1:
	default:
		return nil, fmt.Errorf("more than one instance type fronts local queue %s", lqName)
	}
	return &itList.Items[0], nil
}

// cardVRAMMib 从 Pod 队列对应的 InstanceType.Status.Detail 读取每卡显存，单位为 MiB。详情尚未就绪
// 或显存值无效时返回错误，避免按错误容量换算绝对显存请求。
func (r *PodWebhook) cardVRAMMib(ctx context.Context, pod *core.Pod) (int64, error) {
	it, err := r.frontingInstanceType(ctx, pod)
	if err != nil {
		return 0, err
	}
	return workerctrl.InstanceTypeCardVRAMMib(it)
}

// partitionContainerUnits 从队列对应的 InstanceType 读取已观测到的规格和单卡显存，再计算每个分区的
// .partitioned.units。详情尚未就绪时提示重试；资源池未提供所选规格时指出具体的容器和规格。
//
// 按每个分区的显存占整卡显存的比例计费。96 GiB 卡上的 48 GiB 分区占 800,000 units，96 GiB 分区占
// 1,600,000 units。相同显存的 .sliced.memory-mib 请求也得到相同的配额。
func (r *PodWebhook) partitionContainerUnits(
	ctx context.Context, pod *core.Pod, ctr *core.Container, base core.ResourceName, profile string,
) (int64, error) {
	it, err := r.frontingInstanceType(ctx, pod)
	if err != nil {
		return 0, err
	}
	prof, ready, found := workerctrl.PartitionProfile(it, profile)
	if !found {
		if !ready {
			return 0, fmt.Errorf("instance type %s is not ready yet (accelerator detail not computed); retry", it.Name)
		}
		return 0, fmt.Errorf("container %q: partition profile %q is not offered by the target pool (instance type %s)",
			ctr.Name, profile, it.Name)
	}
	// 探测或滚动更新期间，规格名称可能先于 MemoryMib 出现。将缺失的显存视为暂未就绪，请求可以重试。
	if prof.MemoryMib <= 0 {
		return 0, fmt.Errorf("instance type %s is not ready yet (partition profile %q has no memory detail); retry",
			it.Name, profile)
	}
	cardVRAMMib, err := workerctrl.InstanceTypeCardVRAMMib(it)
	if err != nil {
		return 0, err
	}
	units := nodefeature.MemoryMibToUnits(prof.MemoryMib, cardVRAMMib)
	if units <= 0 {
		return 0, fmt.Errorf("container %q: cannot derive a positive %s from partition profile %q",
			ctr.Name, partitionResourceNamesForBase(base).units, profile)
	}
	return units, nil
}

func (r *PodWebhook) ValidateCreate(_ context.Context, obj runtime.Object) (ctrladmission.Warnings, error) {
	pod := obj.(*core.Pod)

	if errs := validatePodAcceleratorRequest(pod); len(errs) > 0 {
		return nil, kerrors.NewInvalid(core.SchemeGroupVersion.WithKind("Pod").GroupKind(), pod.Name, errs)
	}

	return nil, nil
}

func (r *PodWebhook) ValidateUpdate(_ context.Context, _, _ runtime.Object) (ctrladmission.Warnings, error) {
	return nil, nil
}

func (r *PodWebhook) ValidateDelete(_ context.Context, _ runtime.Object) (ctrladmission.Warnings, error) {
	return nil, nil
}

// validatePodAcceleratorRequest 对 Pod 强制执行规范化的加速器请求规则：
//
//  1. 所有容器请求同一资源族，且声明仅位于一个容器组中；
//  2. <base>.sliced 必须恰好为 1（多卡逻辑切片暂缓支持）；
//  3. <base>.partitioned 必须恰好为 1；
//  4. 分区请求必须仅指定一种 profile 形态；
//  5. 每个按 profile 区分的键的值必须恰好为 1；
//  6. 至多一个容器可以请求切片族（逻辑切片或物理分区）；
//  7. 可重启的 init 容器完全不能请求任何加速器族。
//
// 规则 4 和规则 6 的作用域为持有声明的容器组（规则 1 保证了其唯一性）；它们会对所有非 sidecar
// 容器进行求值，以便违反规则 1 的 Pod 仍能报告这些错误。
func validatePodAcceleratorRequest(pod *core.Pod) field.ErrorList {
	var errs field.ErrorList

	containers := podContainers(pod)
	claims := make([]map[nodefeature.ResourceFamily][]core.ResourceName, len(containers))
	for i := range containers {
		claims[i] = containerClaims(containers[i].ctr)
	}

	var (
		families = sets.New[nodefeature.ResourceFamily]()
		groups   = sets.New[containerGroup]()
		profiles = sets.New[string]()
		slicing  int
	)
	for i := range containers {
		pc, claim := &containers[i], claims[i]
		if len(claim) == 0 {
			continue
		}

		// 规则 7。原生 sidecar 与两个组都存在重叠，因此不能属于任何一组，也不得声明加速器。
		// SSH sidecar 不受影响：可见性资源被刻意排除在加速器族之外。
		if pc.group == containerGroupSidecar {
			errs = append(errs, field.Forbidden(pc.path.Child("resources", "limits"),
				"a restartable init container (a native sidecar) may not request an accelerator; "+
					"move the request to an app container"))
			continue
		}

		for family, bases := range claim {
			families.Insert(family)
			if family == nodefeature.ResourceFamilyPartitioned {
				for _, base := range bases {
					profiles.Insert(containerPartitionProfiles(pc.ctr, base)...)
				}
			}
		}
		groups.Insert(pc.group)
		if len(claim[nodefeature.ResourceFamilySliced]) > 0 ||
			len(claim[nodefeature.ResourceFamilyPartitioned]) > 0 {
			slicing++
		}
	}

	// 规则 1，资源族部分。
	if families.Len() > 1 {
		names := make([]string, 0, families.Len())
		for _, f := range sets.List(families) {
			names = append(names, string(f))
		}
		errs = append(errs, field.Forbidden(field.NewPath("spec"),
			fmt.Sprintf("a Pod may request only one accelerator family, found %v", names)))
	}

	// 规则 1，容器组部分。此处要求 init 组放弃其声明：init 容器的设备会在 Pod 的整个
	// 生命周期内保留在 kubelet 的 Pod 记录中，因此两份声明会同时存在，而调度器仅对 Pod 计费一份。
	if groups.Len() > 1 {
		errs = append(errs, field.Forbidden(field.NewPath("spec", string(containerGroupInit)),
			fmt.Sprintf("a Pod's accelerator requests must all sit in one container group; "+
				"spec.%s must give up its request, because its devices are held for the Pod's whole life "+
				"while the scheduler charges the Pod only once", containerGroupInit)))
	}

	// 规则 6。
	if slicing > 1 {
		errs = append(errs, field.Forbidden(field.NewPath("spec"),
			fmt.Sprintf("at most one container may request a slicing family, found %d", slicing)))
	}

	// 规则 4。
	if profiles.Len() > 1 {
		errs = append(errs, field.Forbidden(field.NewPath("spec"),
			fmt.Sprintf("a Pod may request only one partition profile, found %v", sets.List(profiles))))
	}

	// 规则 2、3 和 5，以及各资源族的形态检查。此处跳过 sidecar：规则 7 已要求其完全移除
	// 该请求，因此再评估请求形态只会增加冗余噪音。
	for i := range containers {
		pc, claim := &containers[i], claims[i]
		if pc.group == containerGroupSidecar {
			continue
		}
		// 将字段错误锚定在 resources.limits 下：扩展资源（加速器键）通常在此处指定，
		// 因此即使仅设置了 limits（requests 会被默认设为相等），也能匹配用户的清单。
		limPath := pc.path.Child("resources", "limits")
		for _, base := range claim[nodefeature.ResourceFamilySliced] {
			errs = append(errs, validateLogicalSliceRequest(pc.ctr, base, limPath)...)
		}
		for _, base := range claim[nodefeature.ResourceFamilyPartitioned] {
			errs = append(errs, validatePartitionRequest(pc.ctr, base, limPath)...)
		}
	}

	return errs
}

// isExactlyOne 返回 q 是否恰好等于 1。它使用 Cmp 而非 Value() 进行比较，
// 因为 Value() 会将 "1m" 这类小数量值向上取整为 1 而误放行。
func isExactlyOne(q resource.Quantity) bool {
	return q.Cmp(*resource.NewQuantity(1, resource.DecimalSI)) == 0
}

// validatePartitionRequest 检查单个容器针对某加速器基础资源的物理分区请求：它必须将
// 卡键设为恰好 1（规则 3）、指定恰好一个 profile（容器作用域的规则 4——单独的卡键没有
// 可执行的硬件形态），且设置的每个按 profile 区分的键都必须恰好为 1（规则 5）。它与逻辑
// 切片键之间的互斥属于 Pod 范围的单一资源族规则，而非单容器分支。
func validatePartitionRequest(ctr *core.Container, base core.ResourceName, path *field.Path) field.ErrorList {
	var errs field.ErrorList
	names := partitionResourceNamesForBase(base)

	cardQ, hasCard := containerResource(ctr, names.card)
	switch {
	case !hasCard:
		errs = append(errs, field.Required(path.Key(string(names.card)),
			fmt.Sprintf("a partition request must set %s", names.card)))
	case !isExactlyOne(cardQ):
		errs = append(errs, field.Invalid(path.Key(string(names.card)), cardQ.String(),
			"a partition request is always a single card; request one Pod per instance"))
	}

	reqs := containerPartitionRequests(ctr, base)
	switch len(reqs) {
	case 1:
	case 0:
		errs = append(errs, field.Required(path.Key(string(names.card)),
			fmt.Sprintf("a %s request must name a profile, e.g. %s.<kind>-<profile>", names.card, names.card)))
	default:
		errs = append(errs, field.Forbidden(path.Key(string(names.card)),
			fmt.Sprintf("a container may request only one partition profile, found %v",
				containerPartitionProfiles(ctr, base))))
	}

	for i := range reqs {
		if !isExactlyOne(reqs[i].quantity) {
			errs = append(errs, field.Invalid(path.Key(string(reqs[i].name)), reqs[i].quantity.String(),
				"a partition profile request must be exactly 1 instance"))
		}
	}

	return errs
}

// validateLogicalSliceRequest 检查单个容器针对某加速器基础资源的逻辑（软件）切片请求：
// 它必须将卡键设为恰好 1（规则 2）、指定恰好一种显存预算（百分比或 mib），且设置的每个
// 预算都必须为正数，百分比预算上限为 100。算力预算和显存预算是相互独立的维度。
func validateLogicalSliceRequest(ctr *core.Container, base core.ResourceName, path *field.Path) field.ErrorList {
	var errs field.ErrorList
	names := logicalResourceNamesForBase(base)

	memPctQ, hasMemPct := containerResource(ctr, names.memPct)
	memMibQ, hasMemMib := containerResource(ctr, names.memMib)
	coresPctQ, hasCoresPct := containerResource(ctr, names.coresPct)

	// 若缺少要切分的卡数，切片子资源便毫无意义；否则单独的子键在背后没有卡请求的情况下
	// 也会被折算成配额。
	cardQ, hasCard := containerResource(ctr, names.card)
	switch {
	case !hasCard:
		errs = append(errs, field.Required(path.Key(string(names.card)),
			fmt.Sprintf("a sliced request must set %s", names.card)))
	case !isExactlyOne(cardQ):
		errs = append(errs, field.Invalid(path.Key(string(names.card)), cardQ.String(),
			"a logical slice request is always a single card; multi-card logical slicing is not supported yet"))
	}

	switch {
	case !hasMemPct && !hasMemMib:
		errs = append(errs, field.Required(path.Key(string(names.memPct)),
			fmt.Sprintf("a %s request must set %s or %s", names.card, names.memPct, names.memMib)))
	case hasMemPct && hasMemMib:
		errs = append(errs, field.Forbidden(path.Key(string(names.memMib)),
			fmt.Sprintf("cannot set both %s and %s", names.memPct, names.memMib)))
	}

	if hasMemPct && (memPctQ.Value() <= 0 || memPctQ.Value() > 100) {
		errs = append(errs, field.Invalid(path.Key(string(names.memPct)), memPctQ.String(),
			"must be within (0, 100]"))
	}
	if hasMemMib && memMibQ.Value() <= 0 {
		errs = append(errs, field.Invalid(path.Key(string(names.memMib)), memMibQ.String(),
			"must be greater than zero"))
	}
	if hasCoresPct && (coresPctQ.Value() <= 0 || coresPctQ.Value() > 100) {
		errs = append(errs, field.Invalid(path.Key(string(names.coresPct)), coresPctQ.String(),
			"must be within (0, 100]"))
	}

	return errs
}
