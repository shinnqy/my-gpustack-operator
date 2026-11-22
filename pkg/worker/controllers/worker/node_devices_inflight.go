package worker

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/clock"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	kueueconstants "sigs.k8s.io/kueue/pkg/constants"
	kueueadmissioncheck "sigs.k8s.io/kueue/pkg/util/admissioncheck"
	kueueworkload "sigs.k8s.io/kueue/pkg/workload"

	"gpustack.ai/gpustack/pkg/deviceplugin"
	"gpustack.ai/gpustack/pkg/nodefeature"
)

// 回复了 Ready 的 Workload 在设备插件的 Allocate 记录其 Pod 之前，在账本（ledger）中并不占用任何加速卡，而 Allocate 只有在 Kueue 准入该 Workload、调度器绑定其 Pod 且 kubelet 启动它们之后才会发生。在此时间窗口内判定的第二个 Workload 会误以为第一个 Workload 的卡仍是空闲的。因此，每次判定都会按节点统计那些已经回复 Ready、但账本尚未记录的 Workload 所对应的 Pod，即在途 Pod。

// _podNodeNameField 是 API server 用于按已绑定节点过滤 Pod 列表的字段。
const _podNodeNameField = "spec.nodeName"

// chargedKey 标识绑定到同一节点上的单个 Workload podset 的 Pod，其中节点按 topology assignment 中指定的 hostname 命名。
type chargedKey struct {
	namespace string
	workload  string
	podSet    kueue.PodSetReference
	hostname  string
}

// ledgerCharges 记录了 chargeLedgers 读取到的内容：重建了账本的节点 hostname 集合，以及每个 Workload podset 中其分配已被该重建账本收录的 Pod 数量。
type ledgerCharges struct {
	hostnames sets.Set[string]
	pods      map[chargedKey]int32
}

// chargeLedgers 将需求可用的每个资源池节点的 Status 替换为基于绑定到该节点的 Pod 通过设备管理器自身的聚合逻辑重建的结果，并按 Workload podset 统计被该重建 Status 收录的 Pod 数量。
//
// 两者均来自同一份非缓存的列表读取。已发布的 Status 是在 Pod 的分配结果落盘到 Pod 之后由设备管理器异步重建的，因此列表中读到已分配的 Pod 可能尚未反映在 Status 中；若直接将其算作已入账，就会导致它既不在账本中也不在在途集合里。
//
// 指定了节点的 demand 仅需要重建该节点；未指定节点的 demand 则可以使用资源池中的任意节点。
func (r *NodeDevicesAdmissionReconciler) chargeLedgers(
	ctx context.Context, pool []scopedDevices, demands []familyDemand,
) (ledgerCharges, error) {
	logger := ctrllog.FromContext(ctx)

	needed := sets.New[string]()
	poolWide := false
	for _, d := range demands {
		if d.node == "" {
			poolWide = true
		}
		needed.Insert(d.node)
	}

	charges := ledgerCharges{hostnames: sets.New[string](), pods: make(map[chargedKey]int32)}
	for i := range pool {
		sd := &pool[i]
		if !poolWide && (sd.hostname == "" || !needed.Has(sd.hostname)) {
			continue
		}
		pods := new(core.PodList)
		if err := r.APIReader.List(ctx, pods, ctrlcli.MatchingFields{_podNodeNameField: sd.devices.Name}); err != nil {
			return ledgerCharges{}, err
		}
		sd.devices.Status, _ = deviceplugin.BuildDesiredStatus(logger, &sd.devices, pods)
		if sd.hostname == "" {
			continue
		}
		charges.hostnames.Insert(sd.hostname)
		for j := range pods.Items {
			pod := &pods.Items[j]
			name := pod.Annotations[kueue.WorkloadAnnotation]
			if name == "" || !podCharged(pod) {
				continue
			}
			key := chargedKey{
				namespace: pod.Namespace,
				workload:  name,
				podSet:    kueue.PodSetReference(pod.Labels[kueueconstants.PodSetLabel]),
				hostname:  sd.hostname,
			}
			charges.pods[key]++
		}
	}
	return charges, nil
}

// podCharged 报告 Pod 的分配记录是否覆盖了所有请求加速卡的容器，从而确保由其重建的账本包含该 Pod 的完整需求。仅部分分配的 Pod 不视作已入账：此时其需求会被完整计为在途需求，叠加在账本已记录的部分之上，这会高估而非低估节点的资源占用。无法读取分配记录的 Pod 同样不视作已入账，因为重建逻辑也会跳过它。
func podCharged(pod *core.Pod) bool {
	allocations, err := deviceplugin.AllocatedAcceleratorsOf(pod)
	if err != nil || len(allocations) == 0 {
		return false
	}
	containers := slices.Concat(pod.Spec.InitContainers, pod.Spec.Containers)
	for i := range containers {
		if !requestsCard(&containers[i]) {
			continue
		}
		if _, ok := allocations[containers[i].Name]; !ok {
			return false
		}
	}
	return true
}

// requestsCard 报告容器是否请求了任意加速器系列的加速卡。
func requestsCard(ctr *core.Container) bool {
	for name, qty := range mergedContainerResources(ctr) {
		family := nodefeature.ResourceFamilyOf(name)
		switch family {
		case nodefeature.ResourceFamilyExclusive, nodefeature.ResourceFamilyShared,
			nodefeature.ResourceFamilySliced, nodefeature.ResourceFamilyPartitioned:
			if isCardKey(name, family) && qty.Value() > 0 {
				return true
			}
		}
	}
	return false
}

// inflightDemands 返回账本已被重建的节点上在途 Pod 的资源需求：针对其他每个仍持有卡占用的 Workload，计算其 topology assignment 放置到该节点上的 Pod 数减去账本在该节点已收录的 Pod 数。每条需求都保留其所属 podset 被分配的 flavor，以便按原判定所用的同一组卡进行匹配，并将该 flavor 的声明添加到对应节点的作用域（scope）中。
//
// 只有 topology assignment 会指明在途 Pod 将要落到的节点。没有 topology assignment 的 Workload 不会被计入；由本 operator 派生的所有队列都会分配 hostname。
//
// 如果某个节点上某条在途需求的 flavor 解析不到任何卡群体，则返回该节点的 hostname；若不存在此类情况则返回空字符串。此类需求无法放置到任何卡上，若将其忽略就会错误地将该资源空间报告为空闲。
func (r *NodeDevicesAdmissionReconciler) inflightDemands(
	ctx context.Context, self *kueue.Workload, pool []scopedDevices, charges ledgerCharges,
) ([]familyDemand, string, error) {
	if charges.hostnames.Len() == 0 {
		return nil, "", nil
	}

	list := new(kueue.WorkloadList)
	if err := r.Client.List(ctx, list); err != nil {
		return nil, "", err
	}

	var inflight []familyDemand
	for _, wl := range r.withUnobservedReady(list.Items) {
		if wl.Namespace == self.Namespace && wl.Name == self.Name {
			continue
		}
		var demands []familyDemand
		for i := range wl.Spec.PodSets {
			ps := &wl.Spec.PodSets[i]
			flavor := assignedFlavor(wl, ps.Name)
			for _, n := range assignedNodes(wl, ps.Name) {
				if !charges.hostnames.Has(n.hostname) {
					continue
				}
				key := chargedKey{namespace: wl.Namespace, workload: wl.Name, podSet: ps.Name, hostname: n.hostname}
				left := n.count - charges.pods[key]
				if left <= 0 {
					continue
				}
				for _, d := range podSetFamilyDemands(ps, left) {
					d.flavor, d.node = flavor, n.hostname
					demands = append(demands, d)
				}
			}
		}
		if len(demands) == 0 {
			continue
		}
		holds, err := r.holdsCards(ctx, wl)
		if err != nil {
			return nil, "", err
		}
		if holds {
			inflight = append(inflight, demands...)
		}
	}
	sortDemands(inflight)

	unresolved, err := r.scopeInflightFlavors(ctx, pool, inflight)
	return inflight, unresolved, err
}

// holdsCards 报告 Workload 的加速卡是否已许诺给它：即该 Workload 持有配额预留，未被驱逐、未结束且未停用，并且已被准入或已由本控制器拥有的某个检查项回复 Ready。一旦被准入，无论此前经过了哪些检查，其 Pod 都已在创建和调度途中。
func (r *NodeDevicesAdmissionReconciler) holdsCards(ctx context.Context, wl *kueue.Workload) (bool, error) {
	if !kueueworkload.HasQuotaReservation(wl) || kueueworkload.IsEvicted(wl) ||
		kueueworkload.IsFinished(wl) || !kueueworkload.IsActive(wl) {
		return false, nil
	}
	if kueueworkload.IsAdmitted(wl) {
		return true, nil
	}
	checks, err := kueueadmissioncheck.FilterForController(ctx, r.Client, wl.Status.AdmissionChecks, _NodeDevicesControllerName)
	if err != nil {
		return false, err
	}
	for _, name := range checks {
		if acs := kueueadmissioncheck.FindAdmissionCheck(wl.Status.AdmissionChecks, name); acs != nil &&
			acs.State == kueue.CheckStateReady {
			return true, nil
		}
	}
	return false, nil
}

// scopeInflightFlavors 将每条在途需求的 flavor 添加到其所指定节点的作用域中，读取方式与 candidateDevices 读取 Workload 自身 flavor 相同，以便 collectCards 标记该 flavor 在该节点覆盖的加速卡。若遇到首个 flavor 已不存在或无法唯一确定单一加速器键的节点，则返回该节点的 hostname。
func (r *NodeDevicesAdmissionReconciler) scopeInflightFlavors(
	ctx context.Context, pool []scopedDevices, inflight []familyDemand,
) (string, error) {
	scopes := make(map[kueue.ResourceFlavorReference]flavorScope)
	for _, d := range inflight {
		if d.flavor == "" {
			return d.node, nil
		}
		scope, seen := scopes[d.flavor]
		if !seen {
			scope = flavorScope{flavor: d.flavor}
			rf := new(kueue.ResourceFlavor)
			err := r.Client.Get(ctx, ctrlcli.ObjectKey{Name: string(d.flavor)}, rf)
			switch {
			case err == nil:
				scope.acceleratorKey = flavorAcceleratorKey(rf)
				scope.acceleratorCount = flavorAcceleratorCount(rf, scope.acceleratorKey)
			case !apierrors.IsNotFound(err):
				return "", err
			}
			scopes[d.flavor] = scope
		}
		if scope.acceleratorKey == "" {
			return d.node, nil
		}
		for i := range pool {
			if pool[i].hostname == d.node && !slices.Contains(pool[i].scopes, scope) {
				pool[i].scopes = append(pool[i].scopes, scope)
			}
		}
	}
	return "", nil
}

// unresolvedInflightMessage 解释因先于当前 Workload 准入到该节点的另一个 Workload 其 flavor 无法解析出对应加速卡而导致的挂起，待该先序 Workload 的 Pod 完成分配后挂起即会解除。
func unresolvedInflightMessage(node string) string {
	return fmt.Sprintf("a Workload admitted to the node %q before this one has a flavor that resolves to no"+
		" card population there, so the cards it will take cannot be counted; will retry once its Pods"+
		" have been allocated", node)
}

// withUnobservedReady 返回缓存中的 Workload 列表，对于缓存中尚未体现 Ready 状态的条目，将其替换为 readyUnobserved 保存的副本，并按 namespace 和 name 排序，使每次判定得到的在途需求顺序一致。
//
// 一旦缓存显示该 Workload 已变为 Ready、不再持有配额预留、同名下已换成了另一个 Workload 或已完全从缓存消失，对应条目就会被移除：此后便直接以缓存为准。
func (r *NodeDevicesAdmissionReconciler) withUnobservedReady(cached []kueue.Workload) []*kueue.Workload {
	out := make([]*kueue.Workload, 0, len(cached))
	seen := sets.New[types.NamespacedName]()
	for i := range cached {
		wl := &cached[i]
		key := types.NamespacedName{Namespace: wl.Namespace, Name: wl.Name}
		seen.Insert(key)
		written, pending := r.readyUnobserved[key]
		if !pending {
			out = append(out, wl)
			continue
		}
		if written.UID != wl.UID || showsChecks(wl, written) || !kueueworkload.HasQuotaReservation(wl) ||
			kueueworkload.IsEvicted(wl) || kueueworkload.IsFinished(wl) {
			delete(r.readyUnobserved, key)
			out = append(out, wl)
			continue
		}
		out = append(out, written)
	}
	for key := range r.readyUnobserved {
		if !seen.Has(key) {
			delete(r.readyUnobserved, key)
		}
	}
	slices.SortFunc(out, func(a, b *kueue.Workload) int {
		return cmp.Or(cmp.Compare(a.Namespace, b.Namespace), cmp.Compare(a.Name, b.Name))
	})
	return out
}

// showsChecks 报告 wl 是否已携带 written 中包含的所有 Ready 检查项。
func showsChecks(wl, written *kueue.Workload) bool {
	for i := range written.Status.AdmissionChecks {
		w := &written.Status.AdmissionChecks[i]
		if w.State != kueue.CheckStateReady {
			continue
		}
		if acs := kueueadmissioncheck.FindAdmissionCheck(wl.Status.AdmissionChecks, w.Name); acs == nil ||
			acs.State != kueue.CheckStateReady {
			return false
		}
	}
	return true
}

// recordVerdict 保留刚刚回复 Ready 的 Workload 的写入后副本，直到缓存中体现出该状态为止；当判定结果为其他状态时，清除此前保存的副本。
func (r *NodeDevicesAdmissionReconciler) recordVerdict(
	wl *kueue.Workload, written []kueue.AdmissionCheckState, state kueue.CheckState,
) {
	key := types.NamespacedName{Namespace: wl.Namespace, Name: wl.Name}
	if state != kueue.CheckStateReady {
		delete(r.readyUnobserved, key)
		return
	}
	cp := wl.DeepCopy()
	for i := range written {
		kueueworkload.SetAdmissionCheckState(&cp.Status.AdmissionChecks, written[i], clock.RealClock{})
	}
	if r.readyUnobserved == nil {
		r.readyUnobserved = make(map[types.NamespacedName]*kueue.Workload)
	}
	r.readyUnobserved[key] = cp
}
