package deviceplugin

import ()

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/nodefeature"
)

// SlicedAllocateGateEnv 在设置为 "false"时会关闭逻辑切片分配拒绝机制：此时逻辑切片 kubelet 会分配没有剩余
// 空间的卡片，而账本会将该卡片的剩余量设为零，这与拒绝机制存在之前的情况一致。
const SlicedAllocateGateEnv = "GPUSTACK_DEVICE_PLUGIN_SLICED_ALLOCATE_GATE"

// _SlicedOccupancy 按卡、按持有容器记录逻辑切片占用。保留持有者身份，可以在重试时排除该容器原有的分配，
// 避免重复计费。
type _SlicedOccupancy map[Resource]map[_ReservationKey]int32

// slicedOccupancy 合并 Pod 注解与内存预留，每个容器只计算一次。预留即时可见，注解跨重启保留。
// 两边都有记录时，依次采用预留、待归还项所指的有效旧记录、注解。
//
// 终止 Pod 的占用与账本一样由 heldAllocation 判定。Pod 已经消失时不再计入残留预留，下一轮协调会清理。
// 读取只用缓存和内存，因此可在节点锁内执行。
func (s *ResourceServer) slicedOccupancy(ctx context.Context) (_SlicedOccupancy, error) {
	podList := new(core.PodList)
	err := s.Reconciler.Client.List(ctx, podList,
		ctrlcli.MatchingFields{IndexingPodsByNodeName: s.Reconciler.NodeName},
		ctrlcli.UnsafeDisableDeepCopy)
	if err != nil {
		return nil, fmt.Errorf("list pods with node name: %w", err)
	}

	occupied := make(_SlicedOccupancy)
	for i := range podList.Items {
		pod := &podList.Items[i]
		// 注解无法解析时不能从中取出容器名，priorClaimOf 也无法由它还原分配。账本重建会报告同一问题。
		allocations, _ := AllocatedAcceleratorsOf(pod)
		holders := sets.New[string]()
		for name := range allocations {
			holders.Insert(name)
		}
		for name := range s.Reconciler.reservationsFor(pod.UID) {
			holders.Insert(name)
		}
		for _, name := range sets.List(holders) {
			if held := s.priorClaimOf(pod, name); held != nil {
				occupied.add(_ReservationKey{PodUID: pod.UID, Container: name}, heldAllocation(pod, held.Devices))
			}
		}
	}
	return occupied, nil
}

// add 登记一个容器持有的逻辑切片。
func (o _SlicedOccupancy) add(holder _ReservationKey, held workercore.DevicesStatus) {
	for i := range held.Groups {
		grp := &held.Groups[i]
		for j := range grp.Accelerators {
			acc := &grp.Accelerators[j]
			if acc.Mode != workercore.DeviceAllocationModeSliced {
				continue
			}
			res := Resource{Group: grp.ID, Device: acc.ID}
			if o[res] == nil {
				o[res] = make(map[_ReservationKey]int32)
			}
			o[res][holder] += acc.Allocated
		}
	}
}

// heldExcept 返回同一卡上除本容器外的已用单位和切片数。切片数按持有容器计算，
// 与账本的 AllocatedSlices 一致。正常经 webhook 的请求只取一个令牌；直接提交的容器可能在同卡拿到多个，
// 这时容器数可能低估已用槽位。kubelet 仍受每卡最多 LogicalSliced.Count 个令牌的约束。
func (o _SlicedOccupancy) heldExcept(res Resource, self _ReservationKey) (units, slices int32) {
	for holder, u := range o[res] {
		if holder == self {
			continue
		}
		units += u
		slices++
	}
	return units, slices
}

// _SlicedRoom 统一回答某容器在各卡上还能用多少切片空间，选卡建议、容器识别和最终检查都用它。
// 没有实时占用时读取 Devices 的 Remaining，关闭超配拦截时也使用这一回退路径。
type _SlicedRoom struct {
	devs     *workercore.Devices
	occupied _SlicedOccupancy
	self     _ReservationKey
}

// remaining 返回这张卡还能给本容器切片提供多少个单位。
func (r _SlicedRoom) remaining(res Resource) int32 {
	if r.occupied == nil {
		return statusRemainingOf(r.devs, res)
	}
	units, _ := r.occupied.heldExcept(res, r.self)
	return max(nodefeature.ResourceMaxUnits-units, 0)
}

// shortage 说明一张卡为什么放不下 need 个单位的切片，能放下时返回空字符串。既要有额度，也要有空槽位，
// 最多可放 LogicalSliced.Count 个切片；恰好放满也允许。
func (r _SlicedRoom) shortage(res Resource, need int32) string {
	if r.occupied != nil {
		_, slices := r.occupied.heldExcept(res, r.self)
		if capability, ok := capabilityOf(r.devs, res); ok && device.IsLogicallySliceable(capability) &&
			device.LogicalSlotsFree(capability, slices) <= 0 {
			return fmt.Sprintf("it already hosts %d of its %d slices", slices, capability.LogicalSliced.Count)
		}
	}
	if free := r.remaining(res); free < need {
		return fmt.Sprintf("it has %d of %d units free and the slice needs %d",
			free, nodefeature.ResourceMaxUnits, need)
	}
	return ""
}

// capabilityOf 返回 Devices.Spec 为这张卡上报的能力。
func capabilityOf(devs *workercore.Devices, res Resource) (workercore.AcceleratorStatus, bool) {
	for i := range devs.Spec.Groups {
		grp := &devs.Spec.Groups[i]
		if grp.ID != res.Group {
			continue
		}
		for j := range grp.Accelerators {
			if grp.Accelerators[j].ID == res.Device {
				return grp.Accelerators[j].Status, true
			}
		}
	}
	return workercore.AcceleratorStatus{}, false
}

// slicedRoomFor 为指定容器构造可用空间视图。启用超配拦截时纳入实时占用。
func (s *ResourceServer) slicedRoomFor(
	devs *workercore.Devices, occupied _SlicedOccupancy, pod *core.Pod, ctr *core.Container,
) _SlicedRoom {
	room := _SlicedRoom{devs: devs, occupied: occupied}
	if pod != nil && ctr != nil {
		room.self = _ReservationKey{PodUID: pod.UID, Container: ctr.Name}
	}
	return room
}

// unitsCharged 将令牌数乘以每个令牌占用的单卡单位数，最高计一整卡。拦截和记账使用同一规则，
// 避免两处对同一笔用量得出不同结果。
func unitsCharged(unitsPerToken int64, tokens int) int32 {
	perToken := min(unitsPerToken, int64(nodefeature.ResourceMaxUnits))
	return int32(min(perToken*int64(tokens), int64(nodefeature.ResourceMaxUnits)))
}

// rejectSlicedOvercommit 在 kubelet 最终交付的卡容纳不了切片时返回 FailedPrecondition。
// kubelet 会给 Pod 记录 UnexpectedAdmissionError，且不启动容器。即使绕过调度链或忽略选卡建议也会经过这里。
//
// 仅将负余额截成零会掩盖实际超配，因此先使用与候选检查相同的余量判断，排除本容器自己的原分配，
// 使重试不会重复计费。
func (s *ResourceServer) rejectSlicedOvercommit(
	d *_AllocationDecision, occupied _SlicedOccupancy,
	tokensByCard map[Resource][]ResourceToken, unitsPerToken int64,
) error {
	if occupied == nil {
		return nil
	}
	room := s.slicedRoomFor(d.Devices, occupied, d.Pod, d.Container)
	cards := make([]Resource, 0, len(tokensByCard))
	for res := range tokensByCard {
		cards = append(cards, res)
	}
	slices.SortFunc(cards, func(a, b Resource) int { return cmp.Compare(a.String(), b.String()) })
	for _, res := range cards {
		need := unitsCharged(unitsPerToken, len(tokensByCard[res]))
		why := room.shortage(res, need)
		if why == "" {
			continue
		}
		s.Logger.Error(nil, "sliced allocation the card cannot hold rejected",
			"pod", kubemeta.GetNamespacedNameKey(d.Pod), "container", d.Container.Name,
			"card", res.String(), "reason", why)
		return grpcstatus.Errorf(grpccodes.FailedPrecondition,
			"container %q of pod %s cannot take a logical slice of card %s: %s",
			d.Container.Name, kubemeta.GetNamespacedNameKey(d.Pod), res, why)
	}
	return nil
}
