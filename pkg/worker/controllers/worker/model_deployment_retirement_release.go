package worker

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/deviceplugin"
	"gpustack.ai/gpustack/pkg/utils/json"
)

// IMPORTANT: 本文件负责：退场释放快照与核对

// 释放观测谓词独立放在本文件中，用于回答与退场状态机不同的问题：状态机决定操作处于哪个阶段，
// 本文件则直接读取服务端状态，核实目标承诺归还的 Pod、Workload 配额与节点加速卡是否确实已经释放。

// modelDeploymentRetirementReleaseAnnotation 承载当前唯一在途退场操作所捕获的释放证据。
// 存放在 ModelDeployment 注解而非 status 字段中，既保持已冻结的退场状态结构不变，又复用本控制器
// 既有的写入与监听通道。
const modelDeploymentRetirementReleaseAnnotation = "modeldeployment.gpustack.ai/retirement-release"

// modelDeploymentRetirementRelease 记录单次退场操作捕获的目标占有事实及其身份绑定。
//
// 仅靠 StartedAt 不足以区分同一秒内的两次重试（时间序列化精度为秒），因此同时绑定 LastConsumedRetryToken，
// 确保新操作不会误用旧操作遗留的释放记录。
type modelDeploymentRetirementRelease struct {
	ModelDeploymentUID     types.UID `json:"modelDeploymentUID"`
	ObservedGeneration     int64     `json:"observedGeneration"`
	RoleName               string    `json:"roleName"`
	ReplicaOrdinal         int32     `json:"replicaOrdinal"`
	TargetMemberUIDs       []string  `json:"targetMemberUIDs"`
	TargetWorkloadUID      string    `json:"targetWorkloadUID"`
	StartedAt              string    `json:"startedAt"`
	LastConsumedRetryToken string    `json:"lastConsumedRetryToken"`

	Members      []modelDeploymentRetirementReleaseMember `json:"members"`
	WorkloadUIDs []string                                 `json:"workloadUIDs"`
	// Nodes 记录用于核对释放的节点账本身份、被占用的加速卡以及捕获时的库存投影。
	Nodes []modelDeploymentRetirementReleaseNode `json:"nodes"`
}

type modelDeploymentRetirementReleaseNode struct {
	NodeName    string                                 `json:"nodeName"`
	DevicesName string                                 `json:"devicesName"`
	DevicesUID  types.UID                              `json:"devicesUID"`
	Cards       []modelDeploymentRetirementReleaseCard `json:"cards"`
	// Inventory 是捕获时刻节点设备库存与切分能力的投影；若能力发生变化，释放核对将拒绝放行。
	Inventory string `json:"inventory"`
}

// 捕获成员对加速器适用性的显式声明：只有这两个取值代表已读取结果，空字段表示未记录而非无加速卡。
const (
	modelDeploymentRetirementClaimNone = "none"
	modelDeploymentRetirementClaimCard = "card"
)

type modelDeploymentRetirementReleaseMember struct {
	PodUID           types.UID                              `json:"podUID"`
	Namespace        string                                 `json:"namespace"`
	Name             string                                 `json:"name"`
	NodeName         string                                 `json:"nodeName"`
	AcceleratorClaim string                                 `json:"acceleratorClaim"`
	Cards            []modelDeploymentRetirementReleaseCard `json:"cards"`
	// Claim 原样保存成员当时持有的分配注解内容，防止加速卡身份不变但模式或单位数改变时漏检。
	Claim string `json:"claim"`
}

type modelDeploymentRetirementReleaseCard struct {
	GroupID      string `json:"groupID"`
	Manufacturer string `json:"manufacturer"`
	DeviceID     string `json:"deviceID"`
	Index        uint32 `json:"index"`
}

// bindModelDeploymentRetirementRelease 核对释放记录是否严格绑定到当前活跃的退场预留。
// 任何身份字段不匹配均视为无有效证据，令操作保持阻塞而非错误放行。
func bindModelDeploymentRetirementRelease(
	record *modelDeploymentRetirementRelease, md *workercore.ModelDeployment,
	plan *modelDeploymentRetirementPlan,
) error {
	reservation := plan.Reservation
	switch {
	case record.ModelDeploymentUID != md.UID:
		return fmt.Errorf("it belongs to another deployment")
	case record.ObservedGeneration != reservation.ObservedGeneration:
		return fmt.Errorf("it was captured at generation %d, the reservation is at %d",
			record.ObservedGeneration, reservation.ObservedGeneration)
	case record.RoleName != reservation.RoleName || record.ReplicaOrdinal != reservation.ReplicaOrdinal:
		return fmt.Errorf("it names %s ordinal %d, the reservation names %s ordinal %d",
			record.RoleName, record.ReplicaOrdinal, reservation.RoleName, reservation.ReplicaOrdinal)
	}

	want := slices.Clone(reservation.TargetMemberUIDs)
	got := slices.Clone(record.TargetMemberUIDs)
	slices.Sort(want)
	slices.Sort(got)
	if !slices.Equal(want, got) {
		return fmt.Errorf("it was captured for %v, the reservation targets %v", got, want)
	}

	if record.TargetWorkloadUID != reservation.TargetWorkloadUID {
		return fmt.Errorf("it froze workload %q, the reservation freezes %q",
			record.TargetWorkloadUID, reservation.TargetWorkloadUID)
	}
	if record.StartedAt != reservation.StartedAt.UTC().Format(time.RFC3339) {
		return fmt.Errorf("it was captured for an operation started at %s, this one started at %s",
			record.StartedAt, reservation.StartedAt.UTC().Format(time.RFC3339))
	}
	if record.LastConsumedRetryToken != reservation.LastConsumedRetryToken {
		return fmt.Errorf("it belongs to a different retry")
	}

	return nil
}

// captureModelDeploymentRetirementRelease 在删除授权前通过非缓存 APIReader 读取并冻结目标实际持有的
// 成员、Workload 与节点加速卡账本信息。
func (r *ModelDeploymentReconciler) captureModelDeploymentRetirementRelease(
	ctx context.Context, md *workercore.ModelDeployment, plan *modelDeploymentRetirementPlan,
) (*modelDeploymentRetirementRelease, error) {
	if r.APIReader == nil {
		return nil, fmt.Errorf("no API reader is configured, so nothing can be observed")
	}
	if plan.held.Len() == 0 {
		return nil, fmt.Errorf("the target's members are not all present, so there is nothing to capture")
	}

	// Pod 无法按 UID 直接 Get，因此不过滤地列出整个命名空间并在内存中按 UID 匹配。
	pods := new(core.PodList)
	if err := r.APIReader.List(ctx, pods, ctrlcli.InNamespace(md.Namespace)); err != nil {
		return nil, fmt.Errorf("list the target's pods: %w", err)
	}
	byUID := make(map[types.UID]*core.Pod, len(pods.Items))
	for i := range pods.Items {
		byUID[pods.Items[i].UID] = &pods.Items[i]
	}

	reservation := plan.Reservation
	record := &modelDeploymentRetirementRelease{
		ModelDeploymentUID:     md.UID,
		ObservedGeneration:     reservation.ObservedGeneration,
		RoleName:               reservation.RoleName,
		ReplicaOrdinal:         reservation.ReplicaOrdinal,
		TargetMemberUIDs:       slices.Clone(reservation.TargetMemberUIDs),
		TargetWorkloadUID:      reservation.TargetWorkloadUID,
		StartedAt:              reservation.StartedAt.UTC().Format(time.RFC3339),
		LastConsumedRetryToken: reservation.LastConsumedRetryToken,
		WorkloadUIDs:           []string{},
	}

	// 按预留冻结的 TargetMemberUIDs 捕获，不依赖本轮可变的 held 子集；空 UID 或重复 UID 均拒绝。
	memberUIDs := make([]types.UID, 0, len(reservation.TargetMemberUIDs))
	seen := make(map[types.UID]struct{}, len(reservation.TargetMemberUIDs))
	for _, name := range reservation.TargetMemberUIDs {
		uid := types.UID(name)
		if uid == "" {
			return nil, fmt.Errorf("the frozen target set names an empty member UID, so it cannot be captured")
		}
		if _, duplicate := seen[uid]; duplicate {
			return nil, fmt.Errorf("the frozen target set names member %q twice, so it cannot be captured", name)
		}
		seen[uid] = struct{}{}
		memberUIDs = append(memberUIDs, uid)
	}
	slices.Sort(memberUIDs)
	for _, memberUID := range memberUIDs {
		uid := memberUID
		pod, present := byUID[uid]
		if !present {
			return nil, fmt.Errorf("target member %q is already absent, so its allocation cannot be captured", memberUID)
		}
		requests := podRequestsAccelerator(pod)
		cards, err := releaseCardsOf(pod)
		if err != nil {
			return nil, fmt.Errorf("read member %q's allocation: %w", memberUID, err)
		}
		if requests && len(cards) == 0 {
			return nil, fmt.Errorf(
				"target member %q requests an accelerator but carries no readable allocation record", memberUID,
			)
		}
		if pod.Spec.NodeName == "" && len(cards) > 0 {
			return nil, fmt.Errorf("target member %q holds accelerators but is not on a node", memberUID)
		}
		claim := modelDeploymentRetirementClaimNone
		if requests {
			claim = modelDeploymentRetirementClaimCard
		}
		record.Members = append(record.Members, modelDeploymentRetirementReleaseMember{
			PodUID:           pod.UID,
			Claim:            pod.Annotations[deviceplugin.AllocatedAcceleratorAnnoKey],
			Namespace:        pod.Namespace,
			Name:             pod.Name,
			NodeName:         pod.Spec.NodeName,
			AcceleratorClaim: claim,
			Cards:            cards,
		})
	}
	slices.SortFunc(record.Members, func(a, b modelDeploymentRetirementReleaseMember) int {
		return strings.Compare(string(a.PodUID), string(b.PodUID))
	})

	workloads, err := r.releaseWorkloadUIDs(ctx, md, plan)
	if err != nil {
		return nil, err
	}
	record.WorkloadUIDs = workloads

	// 在加速卡仍归属目标时捕获各节点的 Devices 账本，执行严格校验，并对成员与账本做首尾双读边界校验。
	for _, node := range recordNodesOf(record) {
		devices := new(workercore.Devices)
		if err := r.APIReader.Get(ctx, ctrlcli.ObjectKey{Name: node.NodeName}, devices); err != nil {
			return nil, fmt.Errorf("read the node's ledger: %w", err)
		}
		if devices.UID == "" {
			return nil, fmt.Errorf("the node's ledger carries no identity")
		}
		for _, card := range node.Cards {
			if !cardInInventory(devices, card) {
				return nil, fmt.Errorf("accelerator %q of group %q is not in the node's current inventory",
					card.DeviceID, card.GroupID)
			}
		}
		onNode := new(core.PodList)
		if err := r.APIReader.List(ctx, onNode,
			ctrlcli.MatchingFields{"spec.nodeName": node.NodeName}); err != nil {
			return nil, fmt.Errorf("list the node's pods: %w", err)
		}
		if _, _, err := deviceplugin.BuildDesiredStatusStrict(
			ctrllog.FromContext(ctx), devices, onNode,
		); err != nil {
			return nil, fmt.Errorf("the target's allocation cannot be read strictly: %w", err)
		}

		if reason, ok := r.releaseMembersUnchanged(ctx, md, record); !ok {
			return nil, fmt.Errorf("the target changed while its release was being captured: %s", reason)
		}

		projection := devicesInventoryProjection(devices)
		after := new(workercore.Devices)
		if err := r.APIReader.Get(ctx, ctrlcli.ObjectKey{Name: node.NodeName}, after); err != nil {
			return nil, fmt.Errorf("re-read the node's ledger: %w", err)
		}
		if after.UID != devices.UID {
			return nil, fmt.Errorf("the node's ledger was replaced (was %q, now %q) while its release "+
				"was being captured", devices.UID, after.UID)
		}
		if got := devicesInventoryProjection(after); got != projection {
			return nil, fmt.Errorf("the inventory on node %q changed while its release was being captured",
				node.NodeName)
		}

		record.Nodes = append(record.Nodes, modelDeploymentRetirementReleaseNode{
			NodeName:    node.NodeName,
			DevicesName: node.NodeName,
			DevicesUID:  devices.UID,
			Cards:       node.Cards,
			Inventory:   projection,
		})
	}
	slices.SortFunc(record.Nodes, func(a, b modelDeploymentRetirementReleaseNode) int {
		return strings.Compare(a.NodeName, b.NodeName)
	})

	return record, nil
}

// podRequestsAccelerator 复用生产分类器 requestsCard，报告 Pod 是否有任意容器申请了加速卡。
func podRequestsAccelerator(pod *core.Pod) bool {
	for i := range pod.Spec.Containers {
		if requestsCard(&pod.Spec.Containers[i]) {
			return true
		}
	}

	return false
}

// validateReleaseFacts 校验已绑定的释放记录自身结构完整且自洽，防止缺失成员、节点或库存信息的残缺记录被放行。
func validateReleaseFacts(record *modelDeploymentRetirementRelease) error {
	if len(record.Members) == 0 {
		return fmt.Errorf("it records no member facts, so it cannot say what the target held")
	}

	want := make(map[string]struct{}, len(record.TargetMemberUIDs))
	for _, name := range record.TargetMemberUIDs {
		if name == "" {
			return fmt.Errorf("its target set names an empty member UID")
		}
		if _, repeated := want[name]; repeated {
			return fmt.Errorf("its target set names member %q twice", name)
		}
		want[name] = struct{}{}
	}
	got := make(map[string]struct{}, len(record.Members))
	for i := range record.Members {
		member := &record.Members[i]
		if member.PodUID == "" {
			return fmt.Errorf("member %q records no identity", member.Name)
		}
		if _, duplicate := got[string(member.PodUID)]; duplicate {
			return fmt.Errorf("member %q is recorded twice", member.Name)
		}
		got[string(member.PodUID)] = struct{}{}
		switch member.AcceleratorClaim {
		case modelDeploymentRetirementClaimNone:
			if len(member.Cards) != 0 {
				return fmt.Errorf("member %q claims no accelerator but records %d cards",
					member.Name, len(member.Cards))
			}
		case modelDeploymentRetirementClaimCard:
			if len(member.Cards) == 0 {
				return fmt.Errorf("member %q claims an accelerator but records no card", member.Name)
			}
			if member.Claim == "" {
				return fmt.Errorf("member %q claims an accelerator but records no allocation", member.Name)
			}
		default:
			return fmt.Errorf("member %q states accelerator applicability %q, which is not a recorded answer",
				member.Name, member.AcceleratorClaim)
		}
		cardIdentities := make(map[string]struct{}, len(member.Cards))
		for _, card := range member.Cards {
			if _, repeated := cardIdentities[releaseCardKey(card)]; repeated {
				return fmt.Errorf("member %q records card %q twice", member.Name, card.DeviceID)
			}
			cardIdentities[releaseCardKey(card)] = struct{}{}
		}
	}
	if len(got) != len(want) {
		return fmt.Errorf("it holds facts for %d of the %d target members", len(got), len(want))
	}
	for name := range want {
		if _, present := got[name]; !present {
			return fmt.Errorf("it holds no facts for target member %q", name)
		}
	}

	byNode := make(map[string]map[string]struct{}, len(record.Nodes))
	for i := range record.Nodes {
		node := &record.Nodes[i]
		if node.NodeName == "" || node.DevicesUID == "" {
			return fmt.Errorf("a recorded node carries no name or identity")
		}
		if _, duplicate := byNode[node.NodeName]; duplicate {
			return fmt.Errorf("node %q is recorded twice", node.NodeName)
		}
		byNode[node.NodeName] = map[string]struct{}{}
		for _, card := range node.Cards {
			byNode[node.NodeName][releaseCardKey(card)] = struct{}{}
		}
	}
	for i := range record.Members {
		member := &record.Members[i]
		if len(member.Cards) == 0 {
			continue
		}
		if member.NodeName == "" {
			return fmt.Errorf("member %q holds cards but records no node", member.Name)
		}
		node, present := byNode[member.NodeName]
		if !present {
			return fmt.Errorf("member %q holds cards on node %q, which has no ledger facts",
				member.Name, member.NodeName)
		}
		for _, card := range member.Cards {
			if _, present := node[releaseCardKey(card)]; !present {
				return fmt.Errorf("card %q held by member %q is missing from node %q's facts",
					card.DeviceID, member.Name, member.NodeName)
			}
		}
	}

	workloads := make(map[string]struct{}, len(record.WorkloadUIDs))
	for _, uid := range record.WorkloadUIDs {
		if uid == "" {
			return fmt.Errorf("it records a workload with no identity")
		}
		if _, repeated := workloads[uid]; repeated {
			return fmt.Errorf("it records workload %q twice", uid)
		}
		workloads[uid] = struct{}{}
	}
	if record.TargetWorkloadUID != "" {
		if _, present := workloads[record.TargetWorkloadUID]; !present {
			return fmt.Errorf("it does not record the frozen workload %q it was captured for",
				record.TargetWorkloadUID)
		}
	}

	for i := range record.Nodes {
		if record.Nodes[i].Inventory == "" {
			return fmt.Errorf("node %q records no inventory to judge its release against",
				record.Nodes[i].NodeName)
		}
	}

	return nil
}

// releaseCardKey 生成加速卡在释放记录中的规范身份键。
func releaseCardKey(card modelDeploymentRetirementReleaseCard) string {
	return strings.Join([]string{
		card.GroupID, card.Manufacturer, card.DeviceID,
		strconv.FormatUint(uint64(card.Index), 10),
	}, "/")
}

// releaseMembersUnchanged 在捕获结束前重读各成员，确认捕获期间成员及其分配记录未发生变动。
func (r *ModelDeploymentReconciler) releaseMembersUnchanged(
	ctx context.Context, md *workercore.ModelDeployment,
	record *modelDeploymentRetirementRelease,
) (string, bool) {
	pods := new(core.PodList)
	if err := r.APIReader.List(ctx, pods, ctrlcli.InNamespace(md.Namespace)); err != nil {
		return fmt.Sprintf("re-reading the target's pods: %v", err), false
	}
	byUID := make(map[types.UID]*core.Pod, len(pods.Items))
	for i := range pods.Items {
		byUID[pods.Items[i].UID] = &pods.Items[i]
	}

	for i := range record.Members {
		captured := &record.Members[i]
		pod, present := byUID[captured.PodUID]
		if !present {
			return fmt.Sprintf("member %q left while its release was being captured", captured.Name), false
		}
		cards, err := releaseCardsOf(pod)
		if err != nil {
			return fmt.Sprintf("re-read member %q's allocation: %v", captured.Name, err), false
		}
		if pod.Annotations[deviceplugin.AllocatedAcceleratorAnnoKey] != captured.Claim {
			return fmt.Sprintf("member %q now records a different allocation", captured.Name), false
		}
		if podRequestsAccelerator(pod) != (captured.AcceleratorClaim == modelDeploymentRetirementClaimCard) {
			return fmt.Sprintf("member %q now requests a different set of accelerators", captured.Name), false
		}
		if !sameCardSet(captured.Cards, cards) {
			return fmt.Sprintf("member %q now records different accelerators", captured.Name), false
		}
	}

	return "", true
}

// sameCardSet 无序比较两组加速卡身份集合是否一致。
func sameCardSet(a, b []modelDeploymentRetirementReleaseCard) bool {
	if len(a) != len(b) {
		return false
	}
	keys := make(map[string]struct{}, len(a))
	for _, card := range a {
		keys[releaseCardKey(card)] = struct{}{}
	}
	for _, card := range b {
		if _, present := keys[releaseCardKey(card)]; !present {
			return false
		}
	}

	return true
}

// recordNodesOf 将成员占用的加速卡按节点聚合，供逐节点账本比对使用。
func recordNodesOf(record *modelDeploymentRetirementRelease) []modelDeploymentRetirementReleaseNode {
	byNode := map[string][]modelDeploymentRetirementReleaseCard{}
	for i := range record.Members {
		member := &record.Members[i]
		if len(member.Cards) == 0 || member.NodeName == "" {
			continue
		}
		byNode[member.NodeName] = append(byNode[member.NodeName], member.Cards...)
	}
	nodes := make([]modelDeploymentRetirementReleaseNode, 0, len(byNode))
	for name, cards := range byNode {
		slices.SortFunc(cards, func(a, b modelDeploymentRetirementReleaseCard) int {
			if a.GroupID != b.GroupID {
				return strings.Compare(a.GroupID, b.GroupID)
			}

			return strings.Compare(a.DeviceID, b.DeviceID)
		})
		nodes = append(nodes, modelDeploymentRetirementReleaseNode{NodeName: name, Cards: cards})
	}
	slices.SortFunc(nodes, func(a, b modelDeploymentRetirementReleaseNode) int {
		return strings.Compare(a.NodeName, b.NodeName)
	})

	return nodes
}

// cardInInventory 报告捕获的加速卡是否仍在节点当前设备库存中（按组 ID、厂商、设备 ID 与序号匹配）。
func cardInInventory(devices *workercore.Devices, card modelDeploymentRetirementReleaseCard) bool {
	for i := range devices.Spec.Groups {
		group := &devices.Spec.Groups[i]
		if group.ID != card.GroupID || group.Manufacturer != card.Manufacturer {
			continue
		}
		for j := range group.Accelerators {
			accelerator := &group.Accelerators[j]
			if accelerator.ID == card.DeviceID && accelerator.Index == card.Index {
				return true
			}
		}
	}

	return false
}

// releaseWorkloadUIDs 收集目标关联的全部 Workload UID，并无条件包含预留冻结的 TargetWorkloadUID。
func (r *ModelDeploymentReconciler) releaseWorkloadUIDs(
	ctx context.Context, md *workercore.ModelDeployment, plan *modelDeploymentRetirementPlan,
) ([]string, error) {
	workloads := new(kueue.WorkloadList)
	if err := r.APIReader.List(ctx, workloads, ctrlcli.InNamespace(md.Namespace)); err != nil {
		return nil, fmt.Errorf("list workloads: %w", err)
	}

	uids := make([]string, 0, len(workloads.Items)+1)
	for i := range workloads.Items {
		workload := &workloads.Items[i]
		if workload.UID == "" {
			continue
		}
		for _, owner := range workload.OwnerReferences {
			if plan.holds(owner.UID) {
				uids = append(uids, string(workload.UID))

				break
			}
		}
	}
	if frozen := plan.Reservation.TargetWorkloadUID; frozen != "" {
		if !slices.Contains(uids, frozen) {
			uids = append(uids, frozen)
		}
	}
	slices.Sort(uids)

	return uids, nil
}

// releaseCardsOf 从 Pod 自身的分配记录读取其实际占用的加速卡集合，并按确定顺序排序。
func releaseCardsOf(pod *core.Pod) ([]modelDeploymentRetirementReleaseCard, error) {
	allocations, err := deviceplugin.AllocatedAcceleratorsOf(pod)
	if err != nil {
		return nil, err
	}
	if allocations == nil {
		return nil, nil
	}

	cards := make([]modelDeploymentRetirementReleaseCard, 0)
	for _, container := range allocations {
		for i := range container.Devices.Groups {
			group := &container.Devices.Groups[i]
			for j := range group.Accelerators {
				card := &group.Accelerators[j]
				cards = append(cards, modelDeploymentRetirementReleaseCard{
					GroupID:      group.ID,
					Manufacturer: group.Manufacturer,
					DeviceID:     card.ID,
					Index:        card.Index,
				})
			}
		}
	}
	slices.SortFunc(cards, func(a, b modelDeploymentRetirementReleaseCard) int {
		if a.GroupID != b.GroupID {
			return strings.Compare(a.GroupID, b.GroupID)
		}

		return strings.Compare(a.DeviceID, b.DeviceID)
	})

	return cards, nil
}

// loadModelDeploymentRetirementRelease 从对象注解读取释放记录并完成身份绑定与事实校验。
func (r *ModelDeploymentReconciler) loadModelDeploymentRetirementRelease(
	md *workercore.ModelDeployment, plan *modelDeploymentRetirementPlan,
) (*modelDeploymentRetirementRelease, error) {
	raw, present := md.Annotations[modelDeploymentRetirementReleaseAnnotation]
	if !present {
		return nil, fmt.Errorf("the operation carries no captured release record")
	}
	record := new(modelDeploymentRetirementRelease)
	if err := json.Unmarshal([]byte(raw), record); err != nil {
		return nil, fmt.Errorf("the captured release record is not readable: %w", err)
	}
	if err := bindModelDeploymentRetirementRelease(record, md, plan); err != nil {
		return nil, fmt.Errorf("the captured release record does not belong to this operation: %w", err)
	}
	if err := validateReleaseFacts(record); err != nil {
		return nil, fmt.Errorf("the captured release record cannot say what the target held: %w", err)
	}

	return record, nil
}

// persistModelDeploymentRetirementRelease 使用带乐观锁的 patch 在进入 Deleting 前将释放记录写入对象注解，
// 并同步更新内存对象上的注解与 ResourceVersion 以供同轮后续 status 写入使用。
func (r *ModelDeploymentReconciler) persistModelDeploymentRetirementRelease(
	ctx context.Context, md *workercore.ModelDeployment, record *modelDeploymentRetirementRelease,
) error {
	raw, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode the release record: %w", err)
	}
	patched := md.DeepCopy()
	if patched.Annotations == nil {
		patched.Annotations = map[string]string{}
	}
	patched.Annotations[modelDeploymentRetirementReleaseAnnotation] = string(raw)
	if err := r.Client.Patch(ctx, patched, ctrlcli.MergeFromWithOptions(
		md, ctrlcli.MergeFromWithOptimisticLock{},
	)); err != nil {
		if apierrors.IsConflict(err) {
			return fmt.Errorf("the release record was written against a stale object: %w", err)
		}

		return fmt.Errorf("write the release record: %w", err)
	}
	if md.Annotations == nil {
		md.Annotations = map[string]string{}
	}
	md.Annotations[modelDeploymentRetirementReleaseAnnotation] = string(raw)
	md.ResourceVersion = patched.ResourceVersion

	return nil
}

// observeModelDeploymentRetirementReleased 直接查询服务端，在操作身份首尾双读的保护下依次确认
// 目标 Pod 消失、Workload 配额释放与节点加速卡账本收敛。
func (r *ModelDeploymentReconciler) observeModelDeploymentRetirementReleased(
	ctx context.Context, md *workercore.ModelDeployment, plan *modelDeploymentRetirementPlan,
) (string, bool) {
	if r.APIReader == nil {
		return "no API reader is configured, so the release cannot be observed", false
	}
	if err := ctx.Err(); err != nil {
		return fmt.Sprintf("the observation was cancelled: %v", err), false
	}

	record, err := r.loadModelDeploymentRetirementRelease(md, plan)
	if err != nil {
		return fmt.Sprintf("the release has not been observed: %v", err), false
	}

	if reason, ok := r.observeReleaseOperationUnchanged(ctx, md, plan, record); !ok {
		return reason, false
	}

	if reason, ok := r.observeReleaseTargetsAbsent(ctx, record); !ok {
		return reason, false
	}
	if reason, ok := r.observeReleaseWorkloadsGone(ctx, md, record); !ok {
		return reason, false
	}
	if reason, ok := r.observeReleaseAcceleratorsReturned(ctx, record); !ok {
		return reason, false
	}
	if reason, ok := r.observeReleaseOperationUnchanged(ctx, md, plan, record); !ok {
		return reason, false
	}

	if err := ctx.Err(); err != nil {
		return fmt.Sprintf("the observation was cancelled: %v", err), false
	}

	return "", true
}

// observeReleaseOperationUnchanged 从服务端重读部署对象，校验当前操作、阶段及证据注解与捕获时完全一致。
func (r *ModelDeploymentReconciler) observeReleaseOperationUnchanged(
	ctx context.Context, md *workercore.ModelDeployment, plan *modelDeploymentRetirementPlan,
	record *modelDeploymentRetirementRelease,
) (string, bool) {
	current := new(workercore.ModelDeployment)
	if err := r.APIReader.Get(ctx, ctrlcli.ObjectKeyFromObject(md), current); err != nil {
		return fmt.Sprintf("reading the operation: %v", err), false
	}
	if current.UID != record.ModelDeploymentUID {
		return "the deployment the release was captured for is no longer the one on the server", false
	}
	reservation := current.Status.Retirement
	switch {
	case reservation == nil:
		return "the operation's reservation is no longer on the server", false
	case reservation.ObservedGeneration != record.ObservedGeneration:
		return fmt.Sprintf("the operation is at generation %d, the record was captured at %d",
			reservation.ObservedGeneration, record.ObservedGeneration), false
	case reservation.RoleName != record.RoleName:
		return fmt.Sprintf("the operation now names role %q, the record named %q",
			reservation.RoleName, record.RoleName), false
	case reservation.ReplicaOrdinal != record.ReplicaOrdinal:
		return fmt.Sprintf("the operation now names ordinal %d, the record named %d",
			reservation.ReplicaOrdinal, record.ReplicaOrdinal), false
	case reservation.TargetWorkloadUID != record.TargetWorkloadUID:
		return fmt.Sprintf("the operation now freezes workload %q, the record froze %q",
			reservation.TargetWorkloadUID, record.TargetWorkloadUID), false
	case reservation.LastConsumedRetryToken != record.LastConsumedRetryToken:
		return fmt.Sprintf("the operation moved to retry %q while this release was being read",
			reservation.LastConsumedRetryToken), false
	}

	if got := reservation.StartedAt.UTC().Format(time.RFC3339); got != record.StartedAt {
		return fmt.Sprintf("the operation now started at %s, the record was captured for one started at %s",
			got, record.StartedAt), false
	}

	frozen := sets.New[string]()
	for _, name := range reservation.TargetMemberUIDs {
		if frozen.Has(name) {
			return fmt.Sprintf("the operation's target set names %q twice", name), false
		}
		frozen.Insert(name)
	}
	if !frozen.Equal(sets.New(record.TargetMemberUIDs...)) {
		return fmt.Sprintf("the operation now targets %v, the record captured %v",
			reservation.TargetMemberUIDs, record.TargetMemberUIDs), false
	}

	if reservation.State != plan.Reservation.State {
		return fmt.Sprintf("the operation is now %q, this collection is authorized under %q",
			reservation.State, plan.Reservation.State), false
	}

	onServer := new(modelDeploymentRetirementRelease)
	raw, present := current.Annotations[modelDeploymentRetirementReleaseAnnotation]
	if !present {
		return "the operation carries no release record on the server", false
	}
	if err := json.Unmarshal([]byte(raw), onServer); err != nil {
		return fmt.Sprintf("the release record on the server is not readable: %v", err), false
	}
	if onServer.LastConsumedRetryToken != record.LastConsumedRetryToken ||
		onServer.StartedAt != record.StartedAt {
		return "the release record on the server belongs to a different operation", false
	}
	if err := validateReleaseFacts(onServer); err != nil {
		return fmt.Sprintf("the release record on the server cannot say what the target held: %v", err), false
	}
	if !sameReleaseFacts(onServer, record) {
		return "the release record on the server is not the one this operation loaded", false
	}

	return "", true
}

// observeReleaseTargetsAbsent 检查捕获的所有成员 UID 均已从服务端彻底消失（包括删除中与终态对象）。
func (r *ModelDeploymentReconciler) observeReleaseTargetsAbsent(
	ctx context.Context,
	record *modelDeploymentRetirementRelease,
) (string, bool) {
	for i := range record.Members {
		member := &record.Members[i]
		pod := new(core.Pod)
		err := r.APIReader.Get(ctx,
			ctrlcli.ObjectKey{Namespace: member.Namespace, Name: member.Name}, pod)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return fmt.Sprintf("reading member %q: %v", member.Name, err), false
		}
		if pod.UID == member.PodUID {
			return fmt.Sprintf("member %q is still present (phase %s, deleting %t)",
				member.Name, pod.Status.Phase, pod.DeletionTimestamp != nil), false
		}
	}

	return "", true
}

// observeReleaseWorkloadsGone 检查所有已捕获的 Workload UID 均已消失，且当前没有任何 Workload 仍指向目标成员。
func (r *ModelDeploymentReconciler) observeReleaseWorkloadsGone(
	ctx context.Context, md *workercore.ModelDeployment,
	record *modelDeploymentRetirementRelease,
) (string, bool) {
	workloads := new(kueue.WorkloadList)
	if err := r.APIReader.List(ctx, workloads, ctrlcli.InNamespace(md.Namespace)); err != nil {
		return fmt.Sprintf("listing workloads: %v", err), false
	}

	targets := make(map[types.UID]struct{}, len(record.TargetMemberUIDs))
	for _, name := range record.TargetMemberUIDs {
		targets[types.UID(name)] = struct{}{}
	}

	live := make(map[string]struct{}, len(workloads.Items))
	for i := range workloads.Items {
		workload := &workloads.Items[i]
		if workload.UID == "" {
			for _, owner := range workload.OwnerReferences {
				if _, owned := targets[owner.UID]; owned {
					return fmt.Sprintf(
						"workload %q owns the target's member %q but carries no identity, so its "+
							"release cannot be shown", workload.Name, owner.Name,
					), false
				}
			}

			continue
		}
		live[string(workload.UID)] = struct{}{}
		for _, owner := range workload.OwnerReferences {
			if _, owned := targets[owner.UID]; owned {
				return fmt.Sprintf(
					"workload %q still owns the target's member %q, so the target's quota is not released",
					workload.Name, owner.Name,
				), false
			}
		}
	}
	for _, uid := range record.WorkloadUIDs {
		if uid == "" {
			continue
		}
		if _, present := live[uid]; present {
			return fmt.Sprintf("captured workload %q is still present", uid), false
		}
	}

	return "", true
}

// observeModelDeploymentRetirementTargetsGone 是 Deleting 阶段的消失谓词：确认所有捕获成员 UID 均已不在服务端。
func (r *ModelDeploymentReconciler) observeModelDeploymentRetirementTargetsGone(
	ctx context.Context, md *workercore.ModelDeployment, plan *modelDeploymentRetirementPlan,
) (string, bool) {
	if r.APIReader == nil {
		return "no API reader is configured, so the target's absence cannot be observed", false
	}
	if err := ctx.Err(); err != nil {
		return fmt.Sprintf("the observation was cancelled: %v", err), false
	}

	record, err := r.loadModelDeploymentRetirementRelease(md, plan)
	if err != nil {
		return fmt.Sprintf("the target's absence cannot be observed: %v", err), false
	}

	pods := new(core.PodList)
	if err := r.APIReader.List(ctx, pods, ctrlcli.InNamespace(md.Namespace)); err != nil {
		return fmt.Sprintf("listing the target's pods: %v", err), false
	}
	live := make(map[types.UID]*core.Pod, len(pods.Items))
	for i := range pods.Items {
		live[pods.Items[i].UID] = &pods.Items[i]
	}

	for i := range record.Members {
		member := &record.Members[i]
		pod, present := live[member.PodUID]
		if !present {
			continue
		}
		return fmt.Sprintf("member %q is still present (phase %s, deleting %t)",
			member.Name, pod.Status.Phase, pod.DeletionTimestamp != nil), false
	}

	return "", true
}

// observeReleaseAcceleratorsReturned 逐卡核对节点已发布的 Devices.Status 与基于节点全量 Pod 严格重建的账本是否一致。
// 该断言验证的是账本一致性（而非物理驱动探测），并对节点 Pod 与库存执行首尾双读边界校验。
func (r *ModelDeploymentReconciler) observeReleaseAcceleratorsReturned(
	ctx context.Context,
	record *modelDeploymentRetirementRelease,
) (string, bool) {
	if len(record.Nodes) == 0 {
		return "", true
	}

	for i := range record.Nodes {
		node := &record.Nodes[i]

		devices := new(workercore.Devices)
		if err := r.APIReader.Get(ctx, ctrlcli.ObjectKey{Name: node.DevicesName}, devices); err != nil {
			return fmt.Sprintf("reading the node's ledger: %v", err), false
		}
		if devices.UID != node.DevicesUID {
			return fmt.Sprintf(
				"the node's ledger is no longer the one that was captured (was %q, now %q)",
				node.DevicesUID, devices.UID,
			), false
		}
		for _, card := range node.Cards {
			if !cardInInventory(devices, card) {
				return fmt.Sprintf(
					"accelerator %q of group %q has left the node's inventory", card.DeviceID, card.GroupID,
				), false
			}
		}

		onNode := new(core.PodList)
		if err := r.APIReader.List(ctx, onNode, ctrlcli.MatchingFields{"spec.nodeName": node.NodeName}); err != nil {
			return fmt.Sprintf("listing the node's pods: %v", err), false
		}

		rebuilt, _, err := deviceplugin.BuildDesiredStatusStrict(
			ctrllog.FromContext(ctx), devices, onNode,
		)
		if err != nil {
			return fmt.Sprintf("the node's allocation cannot be read strictly: %v", err), false
		}
		if reason, ok := releaseCardsAgree(node.Cards, rebuilt, devices.Status); !ok {
			return reason, false
		}

		after := new(core.PodList)
		if err := r.APIReader.List(ctx, after, ctrlcli.MatchingFields{"spec.nodeName": node.NodeName}); err != nil {
			return fmt.Sprintf("re-reading the node's pods: %v", err), false
		}
		if !samePodProjection(onNode, after) {
			return fmt.Sprintf("the pods on node %q changed while its release was being read", node.NodeName), false
		}

		again := new(workercore.Devices)
		if err := r.APIReader.Get(ctx, ctrlcli.ObjectKey{Name: node.DevicesName}, again); err != nil {
			return fmt.Sprintf("re-reading the node's ledger: %v", err), false
		}
		if again.UID != node.DevicesUID {
			return fmt.Sprintf("the node's ledger was replaced (was %q, now %q) while its release was being read",
				node.DevicesUID, again.UID), false
		}
		if before, after := devicesInventoryProjection(devices), devicesInventoryProjection(again); before != after {
			return fmt.Sprintf("the inventory on node %q changed while its release was being read",
				node.NodeName), false
		}
		if got := devicesInventoryProjection(devices); got != node.Inventory {
			return fmt.Sprintf("the inventory on node %q is not the one the target was carved from",
				node.NodeName), false
		}
	}

	return "", true
}

// devicesInventoryProjection 将节点完整设备库存与切分能力编码为确定字符串，供前后对比。
func devicesInventoryProjection(devices *workercore.Devices) string {
	lines := make([]string, 0, len(devices.Spec.Groups))
	for i := range devices.Spec.Groups {
		group := &devices.Spec.Groups[i]
		profiles := make([]string, 0, len(group.AcceleratorSlicedDetail.Physical.Profiles))
		for _, profile := range group.AcceleratorSlicedDetail.Physical.Profiles {
			profiles = append(profiles, strings.Join([]string{
				profile.Name,
				strconv.FormatInt(int64(profile.Count), 10),
				strconv.FormatInt(profile.MemoryMib, 10),
			}, "-"))
		}
		slices.Sort(profiles)
		groupLine := strings.Join([]string{
			group.ID, group.Manufacturer, group.Name,
			strconv.FormatUint(group.Memory, 10),
			strconv.FormatUint(uint64(group.Cores), 10),
			group.ComputeCapability, group.Family, group.DriverVersion, group.RuntimeVersion,
			strconv.FormatBool(group.AcceleratorSlicedDetail.Logical.CoresPercentageOvercommit),
			strconv.FormatInt(int64(group.AcceleratorSlicedDetail.Logical.Count), 10),
			strconv.FormatInt(int64(group.AcceleratorSlicedDetail.Physical.Count), 10),
			strings.Join(profiles, ","),
		}, "/")
		for j := range group.Accelerators {
			accelerator := &group.Accelerators[j]
			physical := make([]string, 0, len(accelerator.Status.PhysicalSliced.Profiles))
			for _, profile := range accelerator.Status.PhysicalSliced.Profiles {
				physical = append(physical, strings.Join([]string{
					profile.Name,
					strconv.FormatInt(profile.MemoryMib, 10),
					strconv.FormatInt(int64(profile.ComputeSlices), 10),
					strconv.FormatInt(int64(profile.MemorySlices), 10),
					strconv.FormatInt(int64(profile.Count), 10),
					renderPlacements(profile.Placements),
				}, "-"))
			}
			slices.Sort(physical)
			physicalIndexes := make([]string, 0, len(accelerator.PhysicalIndexes))
			for _, index := range accelerator.PhysicalIndexes {
				physicalIndexes = append(physicalIndexes, strconv.FormatUint(uint64(index), 10))
			}
			lines = append(lines, strings.Join([]string{
				groupLine, accelerator.ID,
				strconv.FormatUint(uint64(accelerator.Index), 10),
				strings.Join(physicalIndexes, ","),
				strconv.FormatBool(accelerator.Status.Unhealthy),
				strconv.FormatBool(accelerator.Status.LogicalSliced.CoresPercentageOvercommit),
				strconv.FormatInt(int64(accelerator.Status.LogicalSliced.Count), 10),
				strconv.FormatInt(int64(accelerator.Status.PhysicalSliced.Count), 10),
				strings.Join(physical, ","),
			}, "/"))
		}
	}
	slices.Sort(lines)

	return strings.Join(lines, "\n")
}

// renderPlacements 将缓存的合法分区位置区间集合编码为排序字符串。
func renderPlacements(placements []workercore.AcceleratorPlacement) string {
	rendered := make([]string, 0, len(placements))
	for _, placement := range placements {
		rendered = append(rendered, strconv.FormatInt(int64(placement.Start), 10)+
			":"+strconv.FormatInt(int64(placement.Length), 10))
	}
	slices.Sort(rendered)

	return strings.Join(rendered, ",")
}

// sameReleaseFacts 判断两条释放记录的内容是否完全一致。
func sameReleaseFacts(a, b *modelDeploymentRetirementRelease) bool {
	left, err := json.Marshal(a)
	if err != nil {
		return false
	}
	right, err := json.Marshal(b)
	if err != nil {
		return false
	}

	return string(left) == string(right)
}

// samePodProjection 按 UID 与 ResourceVersion 判断节点 Pod 列表在两次读取间是否未发生变动。
func samePodProjection(before, after *core.PodList) bool {
	if len(before.Items) != len(after.Items) {
		return false
	}
	byUID := make(map[types.UID]string, len(before.Items))
	for i := range before.Items {
		byUID[before.Items[i].UID] = before.Items[i].ResourceVersion
	}
	for i := range after.Items {
		version, present := byUID[after.Items[i].UID]
		if !present || version != after.Items[i].ResourceVersion {
			return false
		}
	}

	return true
}

// releaseCardsAgree 按身份逐张比对已发布账本与严格重建结果是否一致；允许卡在释放后已被其他 Pod 合法接手。
func releaseCardsAgree(
	cards []modelDeploymentRetirementReleaseCard, rebuilt workercore.DevicesStatus,
	published workercore.DevicesStatus,
) (string, bool) {
	for i := range cards {
		card := &cards[i]
		rebuiltCard := findPublishedCard(rebuilt, card)
		if rebuiltCard == nil {
			return fmt.Sprintf(
				"accelerator %q of group %q has no rebuilt accounting, so its release cannot be read",
				card.DeviceID, card.GroupID,
			), false
		}
		publishedCard := findPublishedCard(published, card)
		if publishedCard == nil {
			return fmt.Sprintf(
				"accelerator %q of group %q has no published row", card.DeviceID, card.GroupID,
			), false
		}
		if !releaseCardsMatch(rebuiltCard, publishedCard) {
			return fmt.Sprintf(
				"accelerator %q of group %q has not settled: published %s, the node's records say %s",
				card.DeviceID, card.GroupID, releaseCardString(publishedCard), releaseCardString(rebuiltCard),
			), false
		}
	}

	return "", true
}

// releaseCardString 格式化单卡的已发布账本摘要，供可读原因文案使用。
func releaseCardString(card *workercore.AcceleratorAllocation) string {
	return fmt.Sprintf("mode %s with %d of %d units and %d slices",
		card.Mode, card.Allocated, card.Allocated+card.Remaining, card.AllocatedSlices)
}

// releaseCardsMatch 逐字段比较单卡的重建账本与已发布账本是否一致。
func releaseCardsMatch(rebuilt, published *workercore.AcceleratorAllocation) bool {
	if rebuilt.ID != published.ID || rebuilt.Index != published.Index ||
		rebuilt.Mode != published.Mode ||
		rebuilt.Allocated != published.Allocated || rebuilt.Remaining != published.Remaining ||
		rebuilt.AllocatedSlices != published.AllocatedSlices ||
		!releaseProfileMapsEqual(rebuilt.AllocatedProfiles, published.AllocatedProfiles) ||
		!releaseProfileMapsEqual(rebuilt.RemainingProfiles, published.RemainingProfiles) {
		return false
	}

	return true
}

// releaseProfileMapsEqual 按名称与计数无序比较两组物理分区规格计数列表，重复名称直接判为不等。
func releaseProfileMapsEqual(a, b []workercore.AcceleratorProfileCount) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	if len(a) != len(b) {
		return false
	}
	counts := make(map[string]int32, len(a))
	for i := range a {
		if _, duplicate := counts[a[i].Name]; duplicate {
			return false
		}
		counts[a[i].Name] = a[i].Count
	}
	seen := make(map[string]struct{}, len(b))
	for i := range b {
		if _, duplicate := seen[b[i].Name]; duplicate {
			return false
		}
		seen[b[i].Name] = struct{}{}
		count, present := counts[b[i].Name]
		if !present || count != b[i].Count {
			return false
		}
	}

	return true
}

// findPublishedCard 按加速卡身份在账本状态中查找唯一匹配项；存在歧义匹配时返回 nil。
func findPublishedCard(
	status workercore.DevicesStatus, card *modelDeploymentRetirementReleaseCard,
) *workercore.AcceleratorAllocation {
	var found *workercore.AcceleratorAllocation
	for i := range status.Groups {
		group := &status.Groups[i]
		if group.ID != card.GroupID || group.Manufacturer != card.Manufacturer {
			continue
		}
		for j := range group.Accelerators {
			accelerator := &group.Accelerators[j]
			if accelerator.ID != card.DeviceID || accelerator.Index != card.Index {
				continue
			}
			if found != nil {
				// 两条记录声称同一身份时无法确定目标，直接拒绝。
				return nil
			}
			found = accelerator
		}
	}

	return found
}
