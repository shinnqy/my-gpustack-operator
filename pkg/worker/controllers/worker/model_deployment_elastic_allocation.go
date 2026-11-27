package worker

import (
	"context"
	"fmt"
	"slices"
	"strings"

	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	kueueworkload "sigs.k8s.io/kueue/pkg/workload"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/deviceplugin"
	"gpustack.ai/gpustack/pkg/nodefeature"
)

// 调度器要分别知道已有多少成员获准运行，以及其中有多少实际持有整卡。
// 准入读取 Workload，分配读取成员和节点账本。某一层的证据读取失败时，保留 Unknown 及原因。
// 缩容还要在成员退出前记录它所占的卡和配额，供退场逻辑核实资源是否归还。

// modelDeploymentElasticWorkerOf 判断 Pod 是否占用弹性角色的一个 GPU 成员位置。角色名和成员序号都要有效，
// 控制属主的 UID、资源类型、API 版本和名称也必须与当前部署一致。CPU 上的 Ray head 有单独的角色名，
// 其他部署的 Pod 和缺少序号的 Pod 也会被排除。
func modelDeploymentElasticWorkerOf(
	pod *core.Pod, md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole,
) bool {
	if modelDeploymentPodRole(pod) != role.Name {
		return false
	}
	if _, seated := modelDeploymentPodOrdinal(pod); !seated {
		return false
	}
	owner := meta.GetControllerOf(pod)

	return owner != nil && owner.UID == md.UID && owner.Kind == "ModelDeployment" &&
		owner.APIVersion == workercore.GroupVersion.String() && owner.Name == md.Name
}

// elasticWholeCardsOf 从主容器读取一个成员完整的张量并行（TP）整卡分配。尚无记录表示还在等分配。
// 记录不全或卡的身份有歧义时，返回原因。
func elasticWholeCardsOf(pod *core.Pod, expected int) ([]modelDeploymentRetirementReleaseCard, string, bool) {
	allocations, err := deviceplugin.AllocatedAcceleratorsOf(pod)
	if err != nil {
		return nil, fmt.Sprintf("member %q's allocation record is not readable: %v", pod.Name, err), false
	}
	if len(allocations) == 0 {
		return nil, "", false
	}
	main, recorded := allocations[modelDeploymentMainContainerName]
	if !recorded {
		return nil, fmt.Sprintf("member %q records its allocation on containers other than %q",
			pod.Name, modelDeploymentMainContainerName), false
	}
	for name, allocation := range allocations {
		if name == modelDeploymentMainContainerName {
			continue
		}
		for _, group := range allocation.Devices.Groups {
			if len(group.Accelerators) > 0 {
				return nil, fmt.Sprintf("member %q records accelerators in sibling container %q", pod.Name, name), false
			}
		}
	}
	cards := []modelDeploymentRetirementReleaseCard{}
	seen := map[string]bool{}
	indices := map[string]bool{}
	for i := range main.Devices.Groups {
		group := &main.Devices.Groups[i]
		for j := range group.Accelerators {
			accelerator := &group.Accelerators[j]
			identity := group.ID + "/" + accelerator.ID
			index := fmt.Sprintf("%s/%d", group.ID, accelerator.Index)
			if accelerator.ID == "" || seen[identity] || indices[index] {
				return nil, fmt.Sprintf("member %q records a missing or duplicate accelerator identity", pod.Name), false
			}
			seen[identity], indices[index] = true, true
			switch {
			case accelerator.Mode != workercore.DeviceAllocationModeExclusive:
				return nil, fmt.Sprintf("member %q records a non-exclusive allocation on %q", pod.Name, accelerator.ID), false
			case accelerator.Allocated != int32(nodefeature.ResourceMaxUnits):
				return nil, fmt.Sprintf("member %q records %d of %d units on %q, not a whole card",
					pod.Name, accelerator.Allocated, nodefeature.ResourceMaxUnits, accelerator.ID), false
			case accelerator.AllocatedSlices != 0 || len(accelerator.AllocatedProfiles) != 0 || len(accelerator.RemainingProfiles) != 0:
				return nil, fmt.Sprintf("member %q records a sliced allocation on %q", pod.Name, accelerator.ID), false
			}
			cards = append(cards, modelDeploymentRetirementReleaseCard{
				GroupID: group.ID, Manufacturer: group.Manufacturer,
				DeviceID: accelerator.ID, Index: accelerator.Index,
			})
		}
	}
	if len(cards) != expected {
		return nil, fmt.Sprintf("member %q records %d accelerators where its TP group requires exactly %d",
			pod.Name, len(cards), expected), false
	}
	return cards, "", true
}

// elasticMemberWorkload 用属主引用中的 Pod 类型、名称和 UID 查找成员所属的 Workload。
// 查不到表示成员还在等组建 Workload。若两个 Workload 都认领该 Pod，就无法确定使用了哪份配额。
// 找到后还要检查是否为单成员组，因为每个弹性成员独立申请准入。
func elasticMemberWorkload(
	workloads []kueue.Workload, pod *core.Pod,
) (workload *kueue.Workload, groupOfOne bool, reason string) {
	for i := range workloads {
		candidate := &workloads[i]
		claims := 0
		for _, ref := range candidate.OwnerReferences {
			if modelDeploymentOwnerRefNamesAPod(ref) && ref.UID == pod.UID && ref.Name == pod.Name {
				claims++
			}
		}
		if claims == 0 {
			continue
		}
		if workload != nil {
			return nil, false,
				fmt.Sprintf("workloads %q and %q both claim member %q", workload.Name, candidate.Name, pod.Name)
		}
		workload = candidate
	}
	if workload == nil {
		return nil, false, ""
	}
	owners := 0
	for _, ref := range workload.OwnerReferences {
		if modelDeploymentOwnerRefNamesAPod(ref) {
			owners++
		}
	}

	return workload, owners == 1, ""
}

// elasticMemberAdmitted 核实该成员已获得 Kueue 准入。Workload 必须有 UID，配额已预留且状态为 Admitted，
// 具名 ClusterQueue 对唯一单成员 PodSet 的唯一分配数量必须为 1。分配的 Count 为空时取 spec 的数量。
// 尚无 Workload 表示仍待准入，遇到无法接受的形状则说明原因。
func elasticMemberAdmitted(wl *kueue.Workload, groupOfOne bool) (bool, string) {
	switch {
	case wl == nil:
		return false, ""
	case !groupOfOne:
		return false, "it is not a group of one, so it is not this member's admission"
	case wl.UID == "":
		return false, "it carries no identity, so its admission cannot be released later"
	case !kueueworkload.HasQuotaReservation(wl) || !kueueworkload.IsAdmitted(wl):
		return false, "it holds no reserved admission"
	case wl.Status.Admission == nil || len(wl.Status.Admission.PodSetAssignments) == 0:
		return false, "it records no admission assignment"
	}
	switch {
	case wl.Status.Admission.ClusterQueue == "":
		return false, "it names no ClusterQueue, so no queue holds its quota"
	case len(wl.Spec.PodSets) != 1:
		return false, "its spec does not declare exactly one podset of one member"
	case wl.Spec.PodSets[0].Count != 1:
		return false, fmt.Sprintf("its spec declares a podset of %d members, not one",
			wl.Spec.PodSets[0].Count)
	case len(wl.Status.Admission.PodSetAssignments) != 1:
		return false, fmt.Sprintf("it records %d admission assignments for one podset",
			len(wl.Status.Admission.PodSetAssignments))
	case wl.Status.Admission.PodSetAssignments[0].Name != wl.Spec.PodSets[0].Name:
		return false, fmt.Sprintf("it admits podset %q, which the spec does not declare",
			wl.Status.Admission.PodSetAssignments[0].Name)
	case ptr.Deref(wl.Status.Admission.PodSetAssignments[0].Count, wl.Spec.PodSets[0].Count) != 1:
		return false, fmt.Sprintf("it admits %d members where the member is one",
			ptr.Deref(wl.Status.Admission.PodSetAssignments[0].Count, wl.Spec.PodSets[0].Count))
	}

	return true, ""
}

// elasticNodeLedgerVerify 在前后两次读取之间核验各卡的声明。
// 它接收同一节点上按 Pod 重建的账本和已发布的账本。调用方可以逐个核对成员持有的卡，
// 使有差异的卡只影响持有它的成员。
//
// 回调返回失败会使整个节点的读取作废。如果差异只涉及个别成员，调用方应把原因记在该成员上，
// 让同一节点的其他成员继续核验。
type elasticNodeLedgerVerify func(
	rebuilt, published workercore.DevicesStatus,
) (string, bool)

// elasticNodeLedgerAgrees 确认所声明的卡仍在节点清单内，以严格模式重建分配，
// 再让 verify 逐卡与已发布的账本对照。核验前后重读 Pod 和账本，有变化就等待。返回这次读取的 Devices 对象，
// 供退场记录保存节点设备的身份与清单。这一步核对 Kubernetes 记账，物理设备的观测由其他组件负责。
//
// 一次调用核验一个节点的快照。调用方先归集落在该节点上的成员，再用同一次读取核对它们各自的卡。
func (r *ModelDeploymentReconciler) elasticNodeLedgerAgrees(
	ctx context.Context, nodeName string,
	cards []modelDeploymentRetirementReleaseCard,
	verify elasticNodeLedgerVerify,
) (devices *workercore.Devices, reason string, ok bool) {
	hold := func(format string, args ...any) (*workercore.Devices, string, bool) {
		return nil, fmt.Sprintf(format, args...), false
	}
	devices = new(workercore.Devices)
	if err := r.APIReader.Get(ctx, ctrlcli.ObjectKey{Name: nodeName}, devices); err != nil {
		return hold("read the node's ledger: %v", err)
	}
	if devices.UID == "" {
		return hold("the node's ledger carries no identity")
	}
	for _, card := range cards {
		if !cardInInventory(devices, card) {
			return hold("accelerator %q of group %q is not in the node's current inventory",
				card.DeviceID, card.GroupID)
		}
	}
	onNode := new(core.PodList)
	if err := r.APIReader.List(ctx, onNode, ctrlcli.MatchingFields{"spec.nodeName": nodeName}); err != nil {
		return hold("list the node's pods: %v", err)
	}
	rebuilt, _, err := deviceplugin.BuildDesiredStatusStrict(ctrllog.FromContext(ctx), devices, onNode)
	if err != nil {
		return hold("the node's allocation cannot be read strictly: %v", err)
	}
	if verify != nil {
		if reason, ok := verify(rebuilt, devices.Status); !ok {
			return nil, reason, false
		}
	}
	after := new(core.PodList)
	if err := r.APIReader.List(ctx, after, ctrlcli.MatchingFields{"spec.nodeName": nodeName}); err != nil {
		return hold("re-reading the node's pods: %v", err)
	}
	if !samePodProjection(onNode, after) {
		return hold("the pods on node %q changed while their ledger was being read", nodeName)
	}
	again := new(workercore.Devices)
	if err := r.APIReader.Get(ctx, ctrlcli.ObjectKey{Name: nodeName}, again); err != nil {
		return hold("re-read the node's ledger: %v", err)
	}
	if again.UID != devices.UID {
		return hold("the node's ledger was replaced (was %q, now %q) while it was being read",
			devices.UID, again.UID)
	}
	if got := devicesInventoryProjection(again); got != devicesInventoryProjection(devices) {
		return hold("the inventory on node %q changed while it was being read", nodeName)
	}

	return devices, "", true
}

// elasticMemberClaim 保存单个成员声明的整卡。等待核验节点账本期间 refusal 为空，
// 核验后在这里记录属于该成员的失败原因。
type elasticMemberClaim struct {
	node    string
	cards   []modelDeploymentRetirementReleaseCard
	refusal string
}

// elasticMemberClaimsByNode 按所在节点对本轮的整卡声明分组，并按节点名返回读取顺序。
// 每个成员仍保留自己的声明和核验结果。
func elasticMemberClaimsByNode(claims []*elasticMemberClaim) ([]string, map[string][]*elasticMemberClaim) {
	byNode := map[string][]*elasticMemberClaim{}
	for _, claim := range claims {
		byNode[claim.node] = append(byNode[claim.node], claim)
	}
	nodes := make([]string, 0, len(byNode))
	for node := range byNode {
		nodes = append(nodes, node)
	}
	slices.Sort(nodes)

	return nodes, byNode
}

// [IMPORTANT: elastic EP] 统计 Admitted（Kueue 准入） 和 Allocated（GPU 账本分配）
//
// observeModelDeploymentElasticAllocation 分别读取已准入和已分配两个数。Workload 有疑问时暂停准入计数，
// 节点账本有疑问时暂停分配计数，成员本身读不到才会同时影响两层。只有 Workload 能证明准入，
// 整卡还要由节点账本证明。前后两次读取之间消失或换了 UID 的 Pod，本轮两层都不计入。
//
// 同一轮只需为每个不同节点核对一次账本。即使宽度为 64，每个节点也只需四次读取，同机多个成员共用这组读取。
// 某张卡的数据不一致时，保留该成员报告的具体原因。
func (r *ModelDeploymentReconciler) observeModelDeploymentElasticAllocation(
	ctx context.Context, md *workercore.ModelDeployment, pods []core.Pod,
) (elasticLayer, elasticLayer) {
	role := ModelDeploymentElasticRole(md)
	if role == nil {
		return unknownLayer("the deployment declares no elastic role"),
			unknownLayer("the deployment declares no elastic role")
	}
	tp, err := ModelDeploymentElasticTensorParallelSize(role)
	if err != nil {
		return unknownLayer(err.Error()), unknownLayer(err.Error())
	}
	if r.APIReader == nil {
		reason := "no API reader is configured, so nothing can be observed"
		return unknownLayer(reason), unknownLayer(reason)
	}
	if err := ctx.Err(); err != nil {
		reason := fmt.Sprintf("the observation was cancelled: %v", err)
		return unknownLayer(reason), unknownLayer(reason)
	}

	var (
		workloads        kueue.WorkloadList
		admittedUnknown  string
		allocatedUnknown string
	)
	if err := r.APIReader.List(ctx, &workloads, ctrlcli.InNamespace(md.Namespace)); err != nil {
		admittedUnknown = fmt.Sprintf("listing workloads: %v", err)
	}
	admittedCount, allocatedCount := 0, 0
	// claims 按传入的成员顺序保存具有完整整卡声明的成员，随后按节点读取账本并逐个给出结果。
	claims := make([]*elasticMemberClaim, 0, len(pods))
	seen := map[types.UID]struct{}{}
	for i := range pods {
		pod := &pods[i]
		if !modelDeploymentElasticWorkerOf(pod, md, role) {
			continue
		}
		if _, duplicate := seen[pod.UID]; duplicate {
			return unknownLayer(fmt.Sprintf("member %q is supplied twice", pod.Name)),
				unknownLayer(fmt.Sprintf("member %q is supplied twice", pod.Name))
		}
		seen[pod.UID] = struct{}{}

		actual := new(core.Pod)
		err := r.APIReader.Get(ctx, ctrlcli.ObjectKey{Namespace: pod.Namespace, Name: pod.Name}, actual)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			reason := fmt.Sprintf("member %q: %v", pod.Name, err)
			if admittedUnknown == "" {
				admittedUnknown = reason
			}
			if allocatedUnknown == "" {
				allocatedUnknown = reason
			}
			continue
		}
		if actual.UID != pod.UID {
			continue
		}

		// 成员的准入证据来自关联到该 Pod 的单成员 Workload，它必须已预留配额并获准运行。
		// 未准入的成员计零，证据矛盾则暂停本轮准入计数。
		if admittedUnknown == "" {
			wl, groupOfOne, reason := elasticMemberWorkload(workloads.Items, actual)
			if reason != "" {
				admittedUnknown = reason
			} else if ok, _ := elasticMemberAdmitted(wl, groupOfOne); ok {
				admittedCount++
			}
		}

		// 设备分配单独核算。尚无分配记录的成员贡献零，声明了完整整卡的成员加入 claims，
		// 由后续的节点账本核验共同读取该节点。
		if allocatedUnknown == "" {
			cards, reason, held := elasticWholeCardsOf(actual, tp)
			switch {
			case reason != "":
				allocatedUnknown = reason
			case !held:
			default:
				claims = append(claims, &elasticMemberClaim{node: actual.Spec.NodeName, cards: cards})
			}
		}
	}

	// 按节点合并读取。先对成员的整卡声明分组，再逐个读取节点清单、Pod 和已发布的账本。
	// 前后两次读取一致时，节点上的所有成员可以共用这份快照。
	//
	// 每个成员都要核验自己的卡。某张卡有差异时，仅该成员的结果为 Unknown。整个节点无法读取时，
	// 才会影响其上所有成员。
	nodes, byNode := elasticMemberClaimsByNode(claims)
	for _, node := range nodes {
		seated := byNode[node]
		cards := make([]modelDeploymentRetirementReleaseCard, 0, len(seated))
		for _, claim := range seated {
			cards = append(cards, claim.cards...)
		}
		_, reason, ok := r.elasticNodeLedgerAgrees(ctx, node, cards,
			func(rebuilt, published workercore.DevicesStatus) (string, bool) {
				for _, claim := range seated {
					cardReason, agreed := releaseCardsAgree(
						claim.cards, rebuilt, published,
					)
					if !agreed {
						claim.refusal = cardReason
					}
				}

				return "", true
			})
		if !ok {
			for _, claim := range seated {
				if claim.refusal == "" {
					claim.refusal = reason
				}
			}
		}
	}

	// 按传入的 Pod 顺序汇总结果。即使节点的读取次序不同，暂停时仍会报告第一个有问题的成员。
	for _, claim := range claims {
		switch {
		case claim.refusal != "":
			if allocatedUnknown == "" {
				allocatedUnknown = claim.refusal
			}
		default:
			allocatedCount++
		}
	}

	admitted, allocated := knownLayer(admittedCount), knownLayer(allocatedCount)
	if admittedUnknown != "" {
		admitted = unknownLayer(admittedUnknown)
	}
	if allocatedUnknown != "" {
		allocated = unknownLayer(allocatedUnknown)
	}

	return admitted, allocated
}

// captureModelDeploymentElasticRelease 在成员仍在线时，记录指定退场成员占用的资源。集合须非空，
// 每个成员须由该部署控制、具有明确身份和序号、仍是同一个 Pod，
// 并持有完整的 TP 整卡组及单成员 Workload 的准入配额。master 不能退场。记录绑定当前部署 UID 和 generation，
// 普通副本退场专用的预留字段留空。
func (r *ModelDeploymentReconciler) captureModelDeploymentElasticRelease(
	ctx context.Context, md *workercore.ModelDeployment, pods []core.Pod,
) (*modelDeploymentRetirementRelease, error) {
	if r.APIReader == nil {
		return nil, fmt.Errorf("no API reader is configured, so nothing can be observed")
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("the capture was cancelled: %w", err)
	}
	role := ModelDeploymentElasticRole(md)
	if role == nil {
		return nil, fmt.Errorf("the deployment declares no elastic role, so no member is retirable")
	}
	tp, err := ModelDeploymentElasticTensorParallelSize(role)
	if err != nil {
		return nil, err
	}
	if md.UID == "" {
		return nil, fmt.Errorf("the deployment carries no identity, so the record cannot be bound to it")
	}
	if len(pods) == 0 {
		return nil, fmt.Errorf("no retirable member was supplied, so there is nothing to capture")
	}

	record := &modelDeploymentRetirementRelease{
		ModelDeploymentUID: md.UID,
		ObservedGeneration: md.Generation,
		WorkloadUIDs:       []string{},
	}
	claimKey := deviceplugin.AllocatedAcceleratorAnnoKey
	fresh := map[types.UID]*core.Pod{}
	seen := map[types.UID]struct{}{}
	for i := range pods {
		pod := &pods[i]
		if pod.UID == "" {
			return nil, fmt.Errorf("member %q carries no identity, so it cannot be captured", pod.Name)
		}
		if _, duplicate := seen[pod.UID]; duplicate {
			return nil, fmt.Errorf("member %q is supplied twice, so it cannot be captured", pod.Name)
		}
		seen[pod.UID] = struct{}{}
		if !modelDeploymentElasticWorkerOf(pod, md, role) {
			return nil, fmt.Errorf("member %q is not a fully owned seated member of role %q, so it cannot be captured",
				pod.Name, role.Name)
		}
		if ordinal, _ := modelDeploymentPodOrdinal(pod); ordinal == 0 {
			return nil, fmt.Errorf("member %q is the master, and the master is never retirable", pod.Name)
		}

		actual := new(core.Pod)
		err := r.APIReader.Get(ctx, ctrlcli.ObjectKey{Namespace: pod.Namespace, Name: pod.Name}, actual)
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("member %q is already absent, so its allocation cannot be captured", pod.Name)
		}
		if err != nil {
			return nil, fmt.Errorf("read member %q: %w", pod.Name, err)
		}
		if actual.UID != pod.UID {
			return nil, fmt.Errorf("pod %q carries uid %s, the member was supplied as %s (same-name replacement)",
				pod.Name, actual.UID, pod.UID)
		}
		if actual.Annotations[claimKey] != pod.Annotations[claimKey] {
			return nil, fmt.Errorf("member %q now records a different allocation than the supplied member did",
				pod.Name)
		}
		heldCards, reason, held := elasticWholeCardsOf(actual, tp)
		if !held {
			if reason == "" {
				reason = "it carries no allocation record"
			}
			return nil, fmt.Errorf("member %q holds no whole accelerator to capture: %s", pod.Name, reason)
		}
		// 记录卡集合时，调用随后核对资源归还时使用的同一读取函数。若主容器之外也持有加速卡，
		// 记录无法完整描述该成员的整卡分配，应拒绝捕获。
		cards, err := releaseCardsOf(actual)
		if err != nil {
			return nil, fmt.Errorf("read member %q's allocation: %w", pod.Name, err)
		}
		if !sameCardSet(cards, heldCards) {
			return nil, fmt.Errorf("member %q records accelerators outside its main container", pod.Name)
		}
		if actual.Spec.NodeName == "" {
			return nil, fmt.Errorf("member %q holds accelerators but is not on a node", pod.Name)
		}

		fresh[actual.UID] = actual
		record.TargetMemberUIDs = append(record.TargetMemberUIDs, string(actual.UID))
		record.Members = append(record.Members, modelDeploymentRetirementReleaseMember{
			PodUID:           actual.UID,
			Namespace:        actual.Namespace,
			Name:             actual.Name,
			NodeName:         actual.Spec.NodeName,
			AcceleratorClaim: modelDeploymentRetirementClaimCard,
			Cards:            cards,
			Claim:            actual.Annotations[claimKey],
		})
	}
	slices.Sort(record.TargetMemberUIDs)
	slices.SortFunc(record.Members, func(a, b modelDeploymentRetirementReleaseMember) int {
		return strings.Compare(string(a.PodUID), string(b.PodUID))
	})

	workloads := new(kueue.WorkloadList)
	if err := r.APIReader.List(ctx, workloads, ctrlcli.InNamespace(md.Namespace)); err != nil {
		return nil, fmt.Errorf("list workloads: %w", err)
	}
	for i := range record.Members {
		member := &record.Members[i]
		wl, groupOfOne, reason := elasticMemberWorkload(workloads.Items, fresh[member.PodUID])
		if reason != "" {
			return nil, fmt.Errorf("member %q's workload cannot be decided: %s", member.Name, reason)
		}
		if wl == nil {
			return nil, fmt.Errorf("member %q composes no workload, so its quota cannot be captured", member.Name)
		}
		if admitted, refusal := elasticMemberAdmitted(wl, groupOfOne); !admitted {
			return nil, fmt.Errorf("member %q's workload is not capturable: %s", member.Name, refusal)
		}
		record.WorkloadUIDs = append(record.WorkloadUIDs, string(wl.UID))
	}
	slices.Sort(record.WorkloadUIDs)

	// 在资源仍归属于这些成员时严格读取账本，再将成员与正在建立的记录核对。
	// 每个节点的输入在核验前后都要重读，沿用副本退场采集证据的规则。
	recheck := func() (string, bool) { return r.releaseMembersUnchanged(ctx, md, record) }
	for _, node := range recordNodesOf(record) {
		devices, reason, ok := r.elasticNodeLedgerAgrees(ctx, node.NodeName, node.Cards,
			func(rebuilt, published workercore.DevicesStatus) (string, bool) {
				if reason, ok := releaseCardsAgree(node.Cards, rebuilt, published); !ok {
					return reason, false
				}

				return recheck()
			})
		if !ok {
			return nil, fmt.Errorf("the members' release cannot be captured: %s", reason)
		}
		record.Nodes = append(record.Nodes, modelDeploymentRetirementReleaseNode{
			NodeName: node.NodeName, DevicesName: node.NodeName, DevicesUID: devices.UID,
			Cards: node.Cards, Inventory: devicesInventoryProjection(devices),
		})
	}
	slices.SortFunc(record.Nodes, func(a, b modelDeploymentRetirementReleaseNode) int {
		return strings.Compare(a.NodeName, b.NodeName)
	})

	return record, nil
}
