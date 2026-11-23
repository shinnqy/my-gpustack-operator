package deviceplugin

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"google.golang.org/grpc"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/sets"
	klog "k8s.io/klog/v2"
	deviceplugin "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/binding"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/utils/slicex"
	"gpustack.ai/gpustack/pkg/utils/stringx"
	"gpustack.ai/gpustack/pkg/utils/waitx"
)

// IMPORTANT:
// - 通用加速器插件服务，把跨模式共享的 DevicesReconciler 和分配锁串起来；具体怎么给容器塞设备则回调各厂商实现的 Responder。
// - 现在是完整的、跨模式通用的 kubelet Device Plugin 服务端

// ResourceServer 通过设备插件 gRPC API 将每一种（制造商、分配模式）组合对传递给 kubelet：它会声明该系列的
// 令牌，回应分配提示，并执行分配操作。每种模式对应一个实例，且每个实例共享一个 DevicesReconciler——正是这
// 个组件使得节点级分配互斥锁以及各加速器跨模式的一致性得以实现，而非仅局限于单一模式内。Responder 负责处
// 理与供应商相关的响应内容。当 `Allocate` 参数未设置时，它会直接拒绝该请求。
type ResourceServer struct {
	deviceplugin.UnimplementedDevicePluginServer

	Logger         klog.Logger
	Manufacturer   string
	AllocationMode workercore.DeviceAllocationMode
	Reconciler     *DevicesReconciler
	Responder      ContainerAllocateResponder

	serving
}

// ResourceName 按厂商和分配模式返回插件向 kubelet 注册的资源名。
func (s *ResourceServer) ResourceName() core.ResourceName {
	// 所有模式共用这个名字映射。可见性资源名为 device.gpustack.ai/<manufacturer>.visibility，只授予设备访问权。
	// 逻辑切片向 kubelet 注册 .sliced 令牌，另由节点补丁发布用于计量的 .sliced.units。
	return nodefeature.GetAcceleratableResourceName(s.Manufacturer, s.AllocationMode)
}

// GetDevicePluginOptions 告诉 kubelet 此插件支持哪些可选调用。
func (s *ResourceServer) GetDevicePluginOptions(context.Context, *Empty) (*Options, error) {
	opts := &Options{
		GetPreferredAllocationAvailable: true,
	}
	return opts, nil
}

// ListAndWatch 持续向 kubelet 发送设备清单。设备状态变化或设备消失时，发送更新后的整份清单。
func (s *ResourceServer) ListAndWatch(_ *Empty, srv grpc.ServerStreamingServer[ListAndWatchResponse]) error {
	// 先订阅再读取首份清单，避免两者之间漏掉更新。kubelet 每次注册都会新建一条流，因此退出时必须解除本次订阅，
	// 免得每重启一次就多留下一个监听者。
	notifier, release := s.Reconciler.getReconcileNotifier(s.Manufacturer, s.AllocationMode)
	defer release()

	ctx := srv.Context()

	// 先把当前设备清单发给 kubelet。
	s.Logger.Info("sending initial list and watch response")
	err := waitx.RetryUntilSuccess(ctx, 2*time.Second, true, func(ctx context.Context) error {
		resp, err := s.getListAndWatchResponse(ctx)
		if err != nil {
			// 首次读取失败就返回错误，让轮询重试。返回 nil 会提前结束轮询，此时 kubelet 没收到清单，
			// 只能一直把资源数看成零，直到下次账本更新。
			s.Logger.Error(err, "get initial list and watch response, retry later")
			return err
		}
		if err = srv.Send(resp); err != nil {
			// 首次发送失败也需要重试。如果 kubelet 已经关闭这条流，流的上下文会结束轮询。
			s.Logger.Error(err, "send initial list and watch response, retry later")
			return err
		}
		return nil
	})
	if err != nil {
		s.Logger.Error(err, "initial list and watch")
		return err
	}

	// 逻辑切片会在宿主机为每个 Pod 建立工作目录。Pod 的 UID 从存活集合消失后，回收对应的目录。
	var gc *podDirGC
	if s.AllocationMode == workercore.DeviceAllocationModeSliced {
		gc = newPodDirGC(OperatorPodsDir)
	}

	// 订阅有变化时，把最新的完整清单发给 kubelet。
	s.Logger.Info("watching for device updates")
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case livePodUIDs := <-notifier:
			resp, err := s.getListAndWatchResponse(ctx)
			if err != nil {
				s.Logger.Error(err, "get list and watch response on update")
				return err
			}
			if err = srv.Send(resp); err != nil {
				s.Logger.Error(err, "send list and watch response")
				return err
			}
			s.Logger.Info("sent list and watch response")
			if gc != nil {
				gc.reconcile(livePodUIDs)
			}
		}
	}
}

func (s *ResourceServer) getListAndWatchResponse(ctx context.Context) (*ListAndWatchResponse, error) {
	devs, err := s.Reconciler.getDevices(ctx)
	if err != nil {
		return nil, err
	}
	// 分区池使用固定的令牌 ID，健康数量按整台节点计算，因此单独发布这类资源。
	if s.AllocationMode == workercore.DeviceAllocationModePartitioned {
		return s.getPartitionListAndWatchResponse(ctx, devs)
	}

	resp := &deviceplugin.ListAndWatchResponse{}
	for i := range devs.Spec.Groups {
		grp := &devs.Spec.Groups[i]
		if grp.Manufacturer != s.Manufacturer {
			continue
		}
		for j := range grp.Accelerators {
			acc := &grp.Accelerators[j]
			res := Resource{
				Group:  grp.ID,
				Device: acc.ID,
			}
			resp.Devices = append(resp.Devices, s.advertiseCard(devs, acc, res)...)
		}
	}

	return resp, nil
}

// IMPORTANT: 把卡变成清单
// advertiseCard 返回一张物理卡能为当前资源类型提供的令牌。卡的能力决定它能服务的模式和令牌数量。
// 已经启用硬件分区的卡无法再整卡分配，此时整卡清单中就没有它。可见性模式涵盖所有卡，
// 边车需要访问主容器实际占用的那张卡。
func (s *ResourceServer) advertiseCard(
	devs *workercore.Devices, acc *workercore.Accelerator, res Resource,
) []*deviceplugin.Device {
	var poolSize int32
	switch s.AllocationMode {
	case workercore.DeviceAllocationModeExclusive, workercore.DeviceAllocationModeShared:
		if !device.IsWholeAcceleratorCapable(acc.Status) {
			return nil
		}
	case workercore.DeviceAllocationModeSliced:
		if !device.IsLogicallySliceable(acc.Status) {
			return nil
		}
		poolSize = acc.Status.LogicalSliced.Count
	}

	// kubelet 可以忽略选卡建议，所以只要一张已被其他模式占用的卡仍被标为 Healthy，就可能分给新的 Pod，
	// 最终因 Allocate 被拒绝而触发 UnexpectedAdmissionError。
	//
	// 保留这些 ID 供 kubelet 恢复已有分配，同时将它们标为 Unhealthy 以阻止新分配。
	// 检查时合并账本和进程内的预留，刚领走的卡也能立即被识别。可见性模式例外，
	// 边车恰好需要访问已被主容器占用的卡。
	health := deviceplugin.Healthy
	if acc.Status.Unhealthy {
		health = deviceplugin.Unhealthy
	} else if s.AllocationMode != workercore.DeviceAllocationModeVisibility {
		if held, _ := s.cardHeldInOtherMode(devs, res); held {
			health = deviceplugin.Unhealthy
		}
	}

	// 内核没有报告 NUMA 亲和性时就省略拓扑信息。
	// 虚拟机里所有设备的 /sys/bus/pci/devices/<bdf>/numa_node 都可能是 -1。若擅自写节点 0，
	// single-numa-node 策略就可能依据未经测量的距离放行，实际性能却达不到预期。
	var topology *deviceplugin.TopologyInfo
	if numa := binding.StrRangeToList(acc.Topology.NumaAffinity); len(numa) > 0 {
		topology = &deviceplugin.TopologyInfo{
			Nodes: slicex.Transform(numa, func(n int) *deviceplugin.NUMANode {
				return &deviceplugin.NUMANode{
					ID: int64(n),
				}
			}),
		}
	}

	ids := res.DeviceIDs(s.AllocationMode, poolSize)
	devices := make([]*deviceplugin.Device, 0, len(ids))
	for k := range ids {
		devices = append(devices,
			&deviceplugin.Device{
				ID:       ids[k],
				Health:   health,
				Topology: topology,
			},
		)
	}

	return devices
}

// getPartitionListAndWatchResponse 发布节点的分区池。令牌可以互换，Allocate 才决定使用哪张卡，
// 因此令牌的健康总数表示节点能承载多少实例，单个令牌也无法承诺 NUMA 位置。
//
// 每张分区卡始终公布固定上限的 ID，以便 kubelet 在重新注册时恢复容器的分配。
// 健康数计入已占实例以及各卡在可用规格中还能放下的最大实例数。调度器会从这个数扣除节点上的 Pod 请求，
// 若只公布余量就会重复扣除。
//
// kubelet 会检查容器原来领到的每个 ID 是否仍然健康，之后才会判断是否需要新设备。
// 活跃分配持有的 ID 必须持续标为 Healthy，其余名额按固定顺序补齐。同一份账本应得到相同的健康 ID 集合。
func (s *ResourceServer) getPartitionListAndWatchResponse(
	ctx context.Context, devs *workercore.Devices,
) (*ListAndWatchResponse, error) {
	held, err := s.Reconciler.liveDeviceIDs(ctx)
	if err != nil {
		return nil, err
	}

	var (
		ids  []string
		room int
	)
	for i := range devs.Spec.Groups {
		grp := &devs.Spec.Groups[i]
		if grp.Manufacturer != s.Manufacturer {
			continue
		}
		for j := range grp.Accelerators {
			acc := &grp.Accelerators[j]
			if !device.IsPartitioned(acc.Status) {
				continue
			}
			res := Resource{Group: grp.ID, Device: acc.ID}
			ceiling := acc.Status.PhysicalSliced.Count
			ids = append(ids, res.DeviceIDs(s.AllocationMode, ceiling)...)
			if acc.Status.Unhealthy {
				// 故障卡仍保留所有 ID，已有容器可能持有其中某个 ID。这张卡不再提供新分区名额。
				continue
			}
			room += partitionRoomOf(devs, res, ceiling)
		}
	}

	// 先将已被持有的 ID 标为 Healthy，再把空余名额分给其余 ID，避免重复计算已占名额。
	granted := 0
	for i := range ids {
		if held.Has(ids[i]) {
			granted++
		}
	}
	resp := &deviceplugin.ListAndWatchResponse{Devices: make([]*deviceplugin.Device, 0, len(ids))}
	for i := range ids {
		health := deviceplugin.Unhealthy
		switch {
		case held.Has(ids[i]):
			health = deviceplugin.Healthy
		case granted < room:
			health = deviceplugin.Healthy
			granted++
		}
		resp.Devices = append(resp.Devices, &deviceplugin.Device{ID: ids[i], Health: health})
	}
	return resp, nil
}

// partitionRoomOf 返回卡上已占的实例数，加上所有可建规格中还能放下的最大实例数。
// 新节点或正在升级的设备管理器可能尚未发布账本，这时先按静态能力上限报告。
func partitionRoomOf(devs *workercore.Devices, res Resource, ceiling int32) int {
	for i := range devs.Status.Groups {
		grp := &devs.Status.Groups[i]
		if grp.ID != res.Group {
			continue
		}
		for j := range grp.Accelerators {
			acc := &grp.Accelerators[j]
			if acc.ID != res.Device {
				continue
			}
			if !device.PartitionLedgerReady(acc) {
				return int(ceiling)
			}
			var allocated, remaining int32
			for k := range acc.AllocatedProfiles {
				allocated += acc.AllocatedProfiles[k].Count
			}
			for k := range acc.RemainingProfiles {
				remaining = max(remaining, acc.RemainingProfiles[k].Count)
			}
			return int(allocated + remaining)
		}
	}
	return int(ceiling)
}

// IMPORTANT: 先礼貌问插件：“你推荐我挑哪几张卡？”
// GetPreferredAllocation 从 kubelet 给定的可用设备里推荐一组 ID。kubelet 最终可以选用其他可用设备。
func (s *ResourceServer) GetPreferredAllocation(ctx context.Context, req *PreferredAllocationRequest) (*PreferredAllocationResponse, error) {
	// 可见性令牌指向主容器已经预留的设备，分区令牌则在 Allocate 时才确定对应的卡。这两种令牌可以互换，
	// 因此这里返回空建议，由 kubelet 自行挑选。
	if s.AllocationMode == workercore.DeviceAllocationModeVisibility ||
		s.AllocationMode == workercore.DeviceAllocationModePartitioned {
		return &PreferredAllocationResponse{
			ContainerResponses: []*ContainerPreferredAllocationResponse{{}},
		}, nil
	}

	ctrReq := req.GetContainerRequests()[0]

	resName := s.ResourceName()
	resQuantity := *resource.NewQuantity(int64(ctrReq.GetAllocationSize()), resource.DecimalSI)
	// 这里只提供选卡建议，已经被预留的容器也可以匹配。真正分配时，Allocate 会再次识别容器并跳过已分配项。
	pod, ctr, err := s.Reconciler.getAllocatingPodWithRetry(ctx, _AllocationMatch{
		ResourceName: resName,
		Quantity:     resQuantity,
		Kubelet:      s.Reconciler.kubeletPending(ctx, resName),
	})
	if err != nil {
		s.Logger.Error(err, "get allocating pod for preferred allocation")
		return nil, grpcstatus.Errorf(grpccodes.Internal, "get allocating pod for preferred allocation: %v", err)
	}

	devs, err := s.Reconciler.getDevices(ctx)
	if err != nil {
		s.Logger.Error(err, "get devices for preferred allocation")
		return nil, grpcstatus.Errorf(grpccodes.Internal, "get devices for preferred allocation: %v", err)
	}

	// 推荐卡时也要看刚登记但尚未折入账本的切片，以免推荐一张 Allocate 随后会拒绝的卡。
	// 如果无法读取这批占用记录，先按账本提供建议，避免 kubelet 因提示失败而拒绝 Pod。
	// 最终的检查仍由 Allocate 执行。
	sliced, err := s.slicedOccupancyForAllocate(ctx)
	if err != nil {
		sliced = nil
	}

	ctrResp, err := s.getContainerPreferredAllocationResponse(ctrReq, pod, ctr, s.slicedRoomFor(devs, sliced, pod, ctr))
	if err != nil {
		s.Logger.Error(err, "get container preferred allocation response")
		return nil, grpcstatus.Errorf(grpccodes.Internal, "get container preferred allocation response: %v", err)
	}

	resp := &PreferredAllocationResponse{
		ContainerResponses: []*ContainerPreferredAllocationResponse{ctrResp},
	}
	s.Logger.Info("get preferred allocation response", "pod", kubemeta.GetNamespacedNameKey(pod), "response", resp)
	return resp, nil
}

func (s *ResourceServer) getContainerPreferredAllocationResponse(
	ctrReq *ContainerPreferredAllocationRequest,
	pod *core.Pod,
	ctr *core.Container,
	room _SlicedRoom,
) (*ContainerPreferredAllocationResponse, error) {
	devs := room.devs
	availableResTokensMap, err := parseResourceTokensByAccelerator(ctrReq.GetAvailableDeviceIDs())
	if err != nil {
		return nil, fmt.Errorf("convert available device id %w", err)
	}
	mustIncludedResTokensMap, err := parseResourceTokensByAccelerator(ctrReq.GetMustIncludeDeviceIDs())
	if err != nil {
		return nil, fmt.Errorf("convert must include device id %w", err)
	}

	allocationSize := ctrReq.GetAllocationSize()
	preferredDeviceIDs := preferredAcceleratorIDsOf(pod, devs)
	preferredDeviceIDsSet := sets.New[string](preferredDeviceIDs...)

	// 逻辑切片候选卡必须同时有空槽位和足够的 .sliced.units。这个预算来自 Pod webhook 折算的显存请求，
	// 使用与 Allocate 相同的余量计算。已使用的单位也决定了下面的排序。若 webhook 未写入预算，
	// 零值表示跳过这项检查。即使 kubelet 忽略建议，Allocate 仍会检查最终结果。
	slicedUnits := int32(0)
	if s.AllocationMode == workercore.DeviceAllocationModeSliced && ctr != nil {
		if q, ok := ctr.Resources.Limits[nodefeature.GetAcceleratableSlicedUnitsResourceName(s.Manufacturer)]; ok {
			slicedUnits = int32(min(q.Value(), int64(nodefeature.ResourceMaxUnits)))
		}
	}

	sel := s.selectPreferredUnits(room, availableResTokensMap, mustIncludedResTokensMap,
		preferredDeviceIDs, preferredDeviceIDsSet, allocationSize, slicedUnits)
	selectedResTokens, remainingSize := sel.Selected, sel.RemainingSize

	if preferredDeviceIDsSet.Len() > 0 {
		s.Logger.Error(nil, "not enough preferred devices",
			"unmet", sets.List(preferredDeviceIDsSet))
	}
	if len(preferredDeviceIDs) > 0 && remainingSize > 0 {
		if len(sel.Unselected) == 0 {
			return &ContainerPreferredAllocationResponse{}, nil
		}
		if remainingSize <= int32(len(sel.Unselected)) {
			s.Logger.Info("try to allocate from unselected devices since preferred devices are not enough")
			selectedResTokens = append(selectedResTokens, sel.Unselected[:remainingSize]...)
			remainingSize = 0
		}
	}

	if remainingSize > 0 {
		s.Logger.Error(nil, "not enough devices",
			"required", allocationSize, "available", allocationSize-remainingSize)
		return &ContainerPreferredAllocationResponse{}, nil
	}

	if len(preferredDeviceIDs) > 0 {
		sortTokensByHardwareOrder(devs, selectedResTokens)
	}

	deviceIDs := make([]string, 0, len(selectedResTokens))
	for i := range selectedResTokens {
		deviceIDs = append(deviceIDs, selectedResTokens[i].String())
	}

	resp := &ContainerPreferredAllocationResponse{
		DeviceIDs: deviceIDs,
	}
	return resp, nil
}

// parseResourceTokensByAccelerator 将设备 ID 解析为令牌，再按对应的物理卡分组。先排序，
// 可以让调用方每次从同一组拿到相同的第一个令牌。
func parseResourceTokensByAccelerator(deviceIDs []string) (map[Resource][]ResourceToken, error) {
	sort.Strings(deviceIDs)

	byAccelerator := make(map[Resource][]ResourceToken)
	for i := range deviceIDs {
		resToken, err := ParseResourceToken(deviceIDs[i])
		if err != nil {
			return nil, fmt.Errorf("%q: %w", deviceIDs[i], err)
		}
		byAccelerator[resToken.Resource] = append(byAccelerator[resToken.Resource], resToken)
	}

	return byAccelerator, nil
}

// _PreferredSelection 保存遍历本节点设备后得到的一次选卡建议。
type _PreferredSelection struct {
	// Selected 保存建议交给 kubelet 的令牌，每张物理卡最多一个。
	Selected []ResourceToken
	// Unselected 保存本轮跳过的卡的令牌，例如余额不足或不在偏好集合中的卡。
	// 只有无法满足指定设备偏好时才会考虑它们。
	Unselected []ResourceToken
	// RemainingSize 是本次扫描还没有满足的请求数量。
	RemainingSize int32
}

// selectPreferredUnits 按放置顺序扫描该厂商的卡，每张卡最多取一个令牌。数量够了且所有指定的卡都已处理，
// 就直接返回并结束两层循环。
//
// 每满足一个偏好就从 preferredDeviceIDsSet 中删除对应 ID，调用方据此判断是否需要回退。
// RemainingSize 作为结果返回，仅属于当前请求。
func (s *ResourceServer) selectPreferredUnits(
	room _SlicedRoom,
	availableResTokensMap, mustIncludedResTokensMap map[Resource][]ResourceToken,
	preferredDeviceIDs []string,
	preferredDeviceIDsSet sets.Set[string],
	allocationSize, slicedUnits int32,
) _PreferredSelection {
	devs := room.devs
	hasPreferred := len(preferredDeviceIDs) > 0
	sel := _PreferredSelection{
		Selected:      make([]ResourceToken, 0, allocationSize),
		RemainingSize: allocationSize,
	}
	for i := range devs.Spec.Groups {
		grp := &devs.Spec.Groups[i]
		if grp.Manufacturer != s.Manufacturer {
			continue
		}
		for _, j := range slicedPackingOrder(room, grp, mustIncludedResTokensMap, slicedUnits) {
			acc := &grp.Accelerators[j]
			res := Resource{
				Group:  grp.ID,
				Device: acc.ID,
			}

			// 跳过 kubelet 未列为可用的资源。
			resTokens, existed := availableResTokensMap[res]
			if !existed {
				continue
			}
			// 跳过已经被其他分配模式占用的资源。
			mode := workercore.DeviceAllocationModeNone
			if len(devs.Status.Groups) > i && len(devs.Status.Groups[i].Accelerators) > j {
				mode = devs.Status.Groups[i].Accelerators[j].Mode
			}
			if mode != workercore.DeviceAllocationModeNone && mode != s.AllocationMode {
				continue
			}

			// 整卡、共享和逻辑切片在这里都是每卡选一个令牌。
			// 并发额度和单卡用量另由 Kueue 的 credits 及 .sliced.units 计量。
			if mustUnits, existed := mustIncludedResTokensMap[res]; existed {
				// 每张卡只回传第一个必须包含的令牌。这些令牌是 kubelet 已分配给该容器的，仍应计入本次请求数量，
				// 否则 kubelet 会从过多的候选中任意补选。slicedPackingOrder 因此优先遍历它们。
				//
				// 已有令牌的用量已经从 Remaining 中扣除，不能再要求同一容器为它提供一次余额，所以这里跳过下方的新分配检查。
				preferredDeviceIDsSet.Delete(res.Device)
				sel.Selected = append(sel.Selected, mustUnits[0])
			} else {
				// 一张卡缺少空槽位，或余额不够放下这份切片时，先放入备选。只有注解指定的卡无法全部选中时才会用这个列表。
				// 没有指定偏好且所有卡都放不下时，返回空建议。
				if slicedUnits > 0 && room.shortage(res, slicedUnits) != "" {
					sel.Unselected = append(sel.Unselected, resTokens[0])
					continue
				}
				if hasPreferred && !preferredDeviceIDsSet.Has(res.Device) {
					sel.Unselected = append(sel.Unselected, resTokens[0])
					continue
				}
				preferredDeviceIDsSet.Delete(res.Device)
				sel.Selected = append(sel.Selected, resTokens[0])
			}
			sel.RemainingSize--
			if !hasPreferred && sel.RemainingSize <= 0 {
				return sel
			}
		}
	}

	if hasPreferred {
		sel.Selected, sel.RemainingSize = capAndOrderPreferredTokens(
			mustIncludedResTokensMap, preferredDeviceIDs, sel.Selected, allocationSize,
		)
	}
	return sel
}

// capAndOrderPreferredTokens 先放必须包含的设备，再按声明的偏好顺序排序，
// 最后截取请求的 allocationSize 个令牌。
func capAndOrderPreferredTokens(
	mustInclude map[Resource][]ResourceToken,
	preferredOrder []string,
	selected []ResourceToken,
	allocationSize int32,
) ([]ResourceToken, int32) {
	preferredRank := make(map[string]int, len(preferredOrder))
	for i, id := range preferredOrder {
		preferredRank[id] = i
	}
	slices.SortStableFunc(selected, func(a, b ResourceToken) int {
		_, mustA := mustInclude[a.Resource]
		_, mustB := mustInclude[b.Resource]
		if c := slicex.CompareTrueFirst(mustA, mustB); c != 0 {
			return c
		}
		return cmp.Compare(preferredRank[a.Device], preferredRank[b.Device])
	})
	if int32(len(selected)) > allocationSize {
		selected = selected[:allocationSize]
	}
	return selected, allocationSize - int32(len(selected))
}

// sortTokensByHardwareOrder 按卡在 devs.Spec.Groups 中的位置排列令牌。
func sortTokensByHardwareOrder(devs *workercore.Devices, tokens []ResourceToken) {
	pos := make(map[Resource]int)
	idx := 0
	for i := range devs.Spec.Groups {
		grp := &devs.Spec.Groups[i]
		for j := range grp.Accelerators {
			pos[Resource{Group: grp.ID, Device: grp.Accelerators[j].ID}] = idx
			idx++
		}
	}
	slices.SortStableFunc(tokens, func(a, b ResourceToken) int {
		return cmp.Compare(pos[a.Resource], pos[b.Resource])
	})
}

// slicedPackingOrder 决定尝试各张卡的顺序。slicedUnits 非正时沿用设备顺序，已分给同一容器的卡应优先回传。
//
// 对仍能满足请求的卡，先用最满的那张，保留整张空卡供后续大请求使用；硬件分区也采用这个原则。
// 放不下的卡排在最后，其中最空的卡最接近满足请求，可在偏好回退时提供建议，Allocate 仍会校验。
// 条件相同就按原下标排序。
func slicedPackingOrder(
	room _SlicedRoom,
	grp *workercore.DevicesGroup,
	mustInclude map[Resource][]ResourceToken,
	slicedUnits int32,
) []int {
	order := make([]int, len(grp.Accelerators))
	for j := range order {
		order[j] = j
	}
	if slicedUnits <= 0 {
		return order
	}

	resourceAt := func(j int) Resource {
		return Resource{Group: grp.ID, Device: grp.Accelerators[j].ID}
	}
	slices.SortStableFunc(order, func(a, b int) int {
		resA, resB := resourceAt(a), resourceAt(b)
		_, mustA := mustInclude[resA]
		_, mustB := mustInclude[resB]
		if c := slicex.CompareTrueFirst(mustA, mustB); c != 0 {
			return c
		}
		remainingA, remainingB := room.remaining(resA), room.remaining(resB)
		fitsA, fitsB := room.shortage(resA, slicedUnits) == "", room.shortage(resB, slicedUnits) == ""
		if c := slicex.CompareTrueFirst(fitsA, fitsB); c != 0 {
			return c
		}
		if fitsA {
			return cmp.Compare(remainingA, remainingB)
		}
		return cmp.Compare(remainingB, remainingA)
	})
	return order
}

// IMPORTANT: 正式下令：“我选好了这几个令牌，请配好设备交给我！”
// kubelet 在创建容器时调用 Allocate。设备插件在此执行厂商要求的操作，并返回让容器能够使用设备所需的信息。
// ==========================================================================================
// ① 验账 + 拆令牌（verifiedKubeletPending + ParseResourceToken）
//
//	↓
//
// ② 【进节点大锁 allocateMutex】认领容器 → 防重放换卡 → 跨模式校验 → 算账 → 写内存预留并广播（decideAllocation）
//
//	↓ 【立刻释放锁！下面的网络和硬件操作绝不能占着锁】
//
// ③ （硬件分区才走）切 MIG / 预渲染响应（actuatePartition / renderPrePatchResponse，整卡直接跳过）
//
//	↓
//
// ④ 把分配结果持久化写进 Pod Annotation（persistAllocation）
//
//	↓
//
// ⑤ 调用厂商 Responder 生成容器注入参数（NVIDIA_VISIBLE_DEVICES）（GetContainerAllocateResponse）
//
//	↓
//
// ⑥ 如果第 ⑤ 步失败 → 触发补偿回滚，撤销内存预留和 Pod Annotation（releaseAllocation）
// ==========================================================================================
func (s *ResourceServer) Allocate(ctx context.Context, req *AllocateRequest) (*AllocateResponse, error) {
	if s.Responder == nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "unconfigured responder")
	}

	kubelet, err := s.Reconciler.verifiedKubeletPending(ctx, s.ResourceName())
	if err != nil {
		s.Logger.Error(err, "verify device bookkeeping before allocation")
		return nil, grpcstatus.Errorf(grpccodes.FailedPrecondition, "verify kubelet device assignments: %v", err)
	}
	if s.AllocationMode == workercore.DeviceAllocationModeVisibility {
		return s.allocateVisibility(ctx, req, kubelet)
	}

	ctrReq := req.GetContainerRequests()[0]

	allocatedDeviceIDs := ctrReq.GetDevicesIds()
	sort.Strings(allocatedDeviceIDs)
	allocatedResTokensMap := make(map[Resource][]ResourceToken)
	for i := range allocatedDeviceIDs {
		resToken, err := ParseResourceToken(allocatedDeviceIDs[i])
		if err != nil {
			s.Logger.Error(err, "convert device id", "device id", allocatedDeviceIDs[i])
			return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "invalid device id %q: %v", allocatedDeviceIDs[i], err)
		}
		allocatedResTokensMap[resToken.Resource] = append(allocatedResTokensMap[resToken.Resource], resToken)
	}
	if err := s.rejectRepeatedSharedCards(len(allocatedDeviceIDs), allocatedResTokensMap); err != nil {
		return nil, err
	}

	// 支持逻辑切片布局的 responder 会被调用两次，锁内选位置，锁外按已选位置生成容器响应。先统一判断一次，
	// 保证两步使用同一套条件。
	logicalPlacer, placesLogical := s.Responder.(LogicalSlicedResponder)
	placesLogical = placesLogical && s.AllocationMode == workercore.DeviceAllocationModeSliced

	// 在同一把节点锁内识别容器、检查模式冲突并预留设备。两个相同请求并发到来时，
	// 后一个会跳过前一个刚认领的容器，其他模式也无法在检查与预留之间抢到同一张卡。
	//
	// 锁包住整个 decideAllocation 调用，defer 保证各条返回路径和 panic 都会解锁，
	// 否则这台节点的后续分配都会卡住。
	decision, err := func() (_AllocationDecision, error) {
		s.Reconciler.allocateMutex.Lock()
		defer s.Reconciler.allocateMutex.Unlock()

		return s.decideAllocation(ctx, allocatedDeviceIDs, allocatedResTokensMap, logicalPlacer, placesLogical, kubelet)
	}()
	if err != nil {
		return nil, err
	}

	physical, err := s.actuatePartition(ctx, &decision, allocatedDeviceIDs)
	if err != nil {
		return nil, err
	}

	ctrResp, err := s.renderPrePatchResponse(ctx, &decision, physical, logicalPlacer, placesLogical)
	if err != nil {
		return nil, err
	}

	if err = s.persistAllocation(ctx, &decision, allocatedDeviceIDs, physical); err != nil {
		return nil, err
	}

	// 其他 responder 在注解写入后执行，有些厂商会在此创建实际的硬件资源。若提前执行，写注解失败时资源可能遗留。
	// 此处返回错误时分配已持久化，需要显式撤销。
	if ctrResp == nil {
		if ctrResp, err = s.Responder.GetContainerAllocateResponse(
			ctx, decision.Pod, decision.Container, decision.Devices, decision.Allocation,
		); err != nil {
			s.releaseAllocation(ctx, &decision, physical)
			s.Logger.Error(err, "get container allocate response")
			return nil, err
		}
	}

	resp := &AllocateResponse{ContainerResponses: []*ContainerAllocateResponse{ctrResp}}
	s.Logger.Info("allocate response", "pod", kubemeta.GetNamespacedNameKey(decision.Pod), "response", resp)

	return resp, nil
}

// _AllocationDecision 保存一次分配在节点锁内确定的结果，包括匹配的 Pod 和容器、
// 判断时使用的账本快照以及预留的设备。
type _AllocationDecision struct {
	Pod       *core.Pod
	Container *core.Container
	// Devices 保存本次分配使用的账本快照，后续步骤继续用它，以免读到其他请求修改后的状态。
	Devices *workercore.Devices
	// Profile 是本次硬件分区请求的规格，其他分配模式下为空。
	Profile string
	// Status 先记录预留结果。硬件操作完成后，再用实际分到的区间覆盖预期位置并重新发布。
	Status workercore.DevicesStatus
	// Allocation 记录每张卡扣除的资源单位，厂商 responder 会据此构造响应。
	Allocation map[Resource]int32
	// LogicalPlacements 是锁内选定的切片位置，GetLogicalSlicedResponse 必须沿用它，才能与已公布的预留一致。
	LogicalPlacements Placements
	// PriorClaim 是该容器原先的分配，首次分配时为 nil。新预留会覆盖旧记录，所以要在锁内先保存；
	// 即使新注解已写入，后续失败仍能恢复原分配。
	PriorClaim *ContainerAllocation
}

// decideAllocation 在调用方持有节点锁时，完成容器识别、跨模式检查和进程内预留。读取走 informer 缓存，
// 唯一的写入是解锁前必须可见的内存预留。写 Pod 注解、操作硬件和构造响应留到锁外。
func (s *ResourceServer) decideAllocation(
	ctx context.Context,
	deviceIDs []string,
	tokensByCard map[Resource][]ResourceToken,
	logicalPlacer LogicalSlicedResponder,
	placesLogical bool,
	kubelet *_KubeletPending,
) (_AllocationDecision, error) {
	var d _AllocationDecision

	// 先读设备账本，后面的可行性检查要据此区分看起来相同的候选容器。
	devs, occupied, occupiedLogical, err := s.snapshotForAllocate(ctx, placesLogical)
	if err != nil {
		return d, err
	}
	d.Devices = devs

	sliced, err := s.slicedOccupancyForAllocate(ctx)
	if err != nil {
		return d, err
	}

	d.Pod, d.Container, err = s.Reconciler.getAllocatingPod(ctx, _AllocationMatch{
		ResourceName: s.ResourceName(),
		Quantity:     *resource.NewQuantity(int64(len(deviceIDs)), resource.DecimalSI),
		SkipReserved: true,
		Feasible:     s.candidateFeasible(devs, tokensByCard, occupied, sliced),
		Kubelet:      kubelet,
	})
	if err != nil {
		s.Logger.Error(err, "get allocating pod for allocation")
		return d, grpcstatus.Errorf(grpccodes.Internal, "get allocating pod for allocation: %v", err)
	}
	d.PriorClaim = s.priorClaimOf(d.Pod, d.Container.Name)
	if d.PriorClaim != nil && s.AllocationMode != workercore.DeviceAllocationModePartitioned {
		priorCards := sets.New[Resource]()
		for _, group := range d.PriorClaim.Devices.Groups {
			for _, acc := range group.Accelerators {
				priorCards.Insert(Resource{Group: group.ID, Device: acc.ID})
			}
		}
		if !priorCards.Equal(sets.KeySet(tokensByCard)) {
			err := grpcstatus.Errorf(grpccodes.FailedPrecondition,
				"refusing conflicting replay for pod %s container %q: prior allocation differs from kubelet device IDs %v",
				kubemeta.GetNamespacedNameKey(d.Pod), d.Container.Name, deviceIDs)
			s.Logger.Error(err, "allocation identity conflict")
			return d, err
		}
	}

	unitsPerToken, unitsRequested := s.requestedUnits(d.Container)

	var placements Placements
	if s.AllocationMode == workercore.DeviceAllocationModePartitioned {
		tokensByCard, placements, unitsPerToken, err = s.choosePartitionCards(&d, occupied, deviceIDs, unitsPerToken, unitsRequested)
		if err != nil {
			return d, err
		}
	}

	if err = s.rejectCrossModeCards(devs, tokensByCard, d.Pod); err != nil {
		return d, err
	}

	if err = s.rejectSlicedOvercommit(&d, sliced, tokensByCard, unitsPerToken); err != nil {
		return d, err
	}

	d.Status, d.Allocation = s.accumulateAllocation(devs, tokensByCard, unitsPerToken)

	// 在登记预留前记录逻辑切片的区间。后续分配在同一把锁下就能避开它，若等到锁外的 responder 才选位置，
	// 请求之间就会冲突。
	if placesLogical {
		if d.LogicalPlacements, err = s.placeLogicalSlice(ctx, &d, logicalPlacer, occupiedLogical); err != nil {
			return d, err
		}
		applyLogicalPlacements(&d.Status, d.LogicalPlacements)
	}

	// 硬件分区也先登记选定的卡、规格和计划占用的区间。下一个请求就能避开还未实际切出的分区。
	// 释放锁并操作硬件之后，再写回实际结果。
	if len(placements) > 0 {
		// 此时分区可能尚未创建，暂时没有硬件 ID，执行硬件操作后再记录。
		applyPhysicalPlacements(&d.Status, d.Profile, placements, nil)
	}

	// 解锁前连同 kubelet 提供的设备 ID 一起登记预留。下一个请求能立即看到占用，边车也能复用主容器的卡，
	// 无需等待注解同步进缓存。只要分配还有效，ListAndWatch 就能持续公布原来那些 ID 为健康。
	s.Reconciler.reserveDevices(d.Pod.UID, d.Container.Name, d.Status, deviceIDs)

	return d, nil
}

// snapshotForAllocate 为本次决定读取设备账本；需要自行选位置的模式还会读取每张卡的实际占用。
// 候选检查和最终选址复用这次读取。数据来自 informer 缓存，因此可以在分配锁内读取。
func (s *ResourceServer) snapshotForAllocate(ctx context.Context, placesLogical bool) (
	*workercore.Devices, Placements, Placements, error,
) {
	devs, err := s.Reconciler.getDevices(ctx)
	if err != nil {
		s.Logger.Error(err, "get devices for allocation")
		return nil, nil, nil, grpcstatus.Errorf(grpccodes.Internal, "get devices for allocation: %v", err)
	}

	// 硬件分区由插件选位置，要把活跃 Pod 注解中的占用与尚未写入注解的预留合并。
	var occupied Placements
	if s.AllocationMode == workercore.DeviceAllocationModePartitioned {
		if occupied, err = s.occupiedPhysicalPlacements(ctx); err != nil {
			s.Logger.Error(err, "get occupied placements for allocation")
			return nil, nil, nil, grpcstatus.Errorf(grpccodes.Internal,
				"get occupied placements for allocation: %v", err)
		}
	}

	// 逻辑切片也需要读取自己的占用区间，避免新切片覆盖同一张卡上已有的位置。
	var occupiedLogical Placements
	if placesLogical {
		if occupiedLogical, err = s.occupiedLogicalPlacements(ctx); err != nil {
			s.Logger.Error(err, "get occupied logical placements for allocation")
			return nil, nil, nil, grpcstatus.Errorf(grpccodes.Internal,
				"get occupied logical placements for allocation: %v", err)
		}
	}

	return devs, occupied, occupiedLogical, nil
}

// slicedOccupancyForAllocate 读取逻辑切片实际占用。其他模式，或已关闭切片超配拦截时，返回 nil，
// 沿用 Devices 账本。
func (s *ResourceServer) slicedOccupancyForAllocate(ctx context.Context) (_SlicedOccupancy, error) {
	if s.AllocationMode != workercore.DeviceAllocationModeSliced || s.Reconciler.slicedAllocateGateOff {
		return nil, nil
	}
	sliced, err := s.slicedOccupancy(ctx)
	if err != nil {
		s.Logger.Error(err, "get logical-slice occupancy for allocation")
		return nil, grpcstatus.Errorf(grpccodes.Internal, "get logical-slice occupancy for allocation: %v", err)
	}
	return sliced, nil
}

// occupiedPhysicalPlacements 合并 Pod 注解中已落盘的物理区间和进程内刚预留的区间。
// 只读注解会漏掉缓存延迟期间的占用，只读内存会在进程重启后丢失已有分配。
func (s *ResourceServer) occupiedPhysicalPlacements(
	ctx context.Context,
) (Placements, error) {
	occupied, err := s.Reconciler.LivePhysicalOccupied(ctx)
	if err != nil {
		return nil, err
	}
	for res, placements := range s.Reconciler.reservedPhysicalOccupied() {
		occupied[res] = append(occupied[res], placements...)
	}
	return occupied, nil
}

// occupiedLogicalPlacements 在逻辑切片的位置坐标内，合并 Pod 注解和内存预留。
// 注解抵达缓存后同一笔分配可能出现在两处。判断有无重叠不受影响；若按重叠多少排序，必须先合并重复区间。
func (s *ResourceServer) occupiedLogicalPlacements(ctx context.Context) (Placements, error) {
	occupied, err := s.Reconciler.LiveLogicalOccupied(ctx)
	if err != nil {
		return nil, err
	}
	for id, placements := range s.Reconciler.reservedLogicalOccupied() {
		occupied[id] = append(occupied[id], placements...)
	}
	return occupied, nil
}

// candidateFeasible 为相同资源名、相同令牌数的候选容器补充可行性检查。RPC 未携带 Pod 身份，若认错容器，
// 就可能执行错误的硬件请求。
//
// 逻辑切片检查 kubelet 提供的每张卡能否容纳 webhook 写入的 .sliced.units。硬件分区令牌不指定具体卡，
// 因此按整台节点是否有能装下目标规格的区间检查。整卡和共享没有这两项额外维度。
func (s *ResourceServer) candidateFeasible(
	devs *workercore.Devices,
	accelerators map[Resource][]ResourceToken,
	occupied Placements,
	sliced _SlicedOccupancy,
) func(*core.Pod, *core.Container) bool {
	switch s.AllocationMode {
	case workercore.DeviceAllocationModeSliced:
		unitsResName := nodefeature.GetAcceleratableSlicedUnitsResourceName(s.Manufacturer)
		return func(pod *core.Pod, ctr *core.Container) bool {
			q, ok := ctr.Resources.Limits[unitsResName]
			if !ok || q.Value() <= 0 {
				// webhook 未折算逻辑切片时，请求没有单卡预算，此处没有可核对的用量。
				return true
			}
			room := s.slicedRoomFor(devs, sliced, pod, ctr)
			for res, tokens := range accelerators {
				if room.shortage(res, unitsCharged(q.Value(), len(tokens))) != "" {
					return false
				}
			}
			return true
		}
	case workercore.DeviceAllocationModePartitioned:
		return func(_ *core.Pod, ctr *core.Container) bool {
			profile, ok := partitionProfileOf(ctr)
			if !ok {
				// webhook 未写入分区规格时，无法据此检查硬件区间。
				return true
			}
			candidates, _, _ := s.partitionCandidates(devs, occupied, profile)
			_, placeable := device.SelectPartitionPlacements(candidates, 1)
			return placeable
		}
	}
	return nil
}

// partitionCandidates 收集该厂商支持目标分区规格的卡，附上探测时报告的合法位置和当前已占区间。
// 第二个返回值把选址算法使用的 ID 映射回 Resource；没有合法位置的卡不会进入候选集。
func (s *ResourceServer) partitionCandidates(
	devs *workercore.Devices,
	occupied Placements,
	profile string,
) ([]device.PartitionCandidate, map[string]Resource, []partitionExclusion) {
	var candidates []device.PartitionCandidate
	var excluded []partitionExclusion
	byID := make(map[string]Resource)
	for i := range devs.Spec.Groups {
		grp := &devs.Spec.Groups[i]
		if grp.Manufacturer != s.Manufacturer {
			continue
		}
		for j := range grp.Accelerators {
			acc := &grp.Accelerators[j]
			// 未启用分区的卡不属于这个资源类型，和 ListAndWatch 一样直接跳过，无需将其登记为被拒的候选。
			if !device.IsPartitioned(acc.Status) {
				continue
			}
			if acc.Status.Unhealthy {
				excluded = append(excluded, partitionExclusion{Device: acc.ID, Reason: "is unhealthy"})
				continue
			}
			possible := profilePlacements(acc.Status.PhysicalSliced.Profiles, profile)
			if len(possible) == 0 {
				excluded = append(excluded, partitionExclusion{
					Device: acc.ID,
					Reason: fmt.Sprintf("offers no placement for profile %q", profile),
				})
				continue
			}
			res := Resource{Group: grp.ID, Device: acc.ID}
			// 其他模式占用的卡不能参与选址，否则后面的跨模式检查一定会拒绝。
			if held, mode := s.cardHeldInOtherMode(devs, res); held {
				excluded = append(excluded, partitionExclusion{
					Device: acc.ID,
					Reason: fmt.Sprintf("is held in %s mode", mode),
				})
				continue
			}
			byID[res.String()] = res
			candidates = append(candidates, device.PartitionCandidate{
				ID:       res.String(),
				Possible: possible,
				Occupied: occupied[res],
			})
		}
	}
	return candidates, byID, excluded
}

// partitionExclusion 记录未能用于本次分区的卡，以及导致排除的原因。
type partitionExclusion struct {
	// Device 是物理卡本身的 ID，方便在节点上查找对应设备。
	Device string
	// Reason 描述这张卡不能参与分配的具体原因，可以直接接在卡名后面阅读。
	Reason string
}

// partitionRefusalDetail 逐卡说明拒绝原因，包括预先排除的卡，以及进入候选集合后仍无法放置的卡。
// 原因可能是卡被占用、请求与探测到的规格不符，或没有可用分区。只在失败时遍历这些状态，
// 成功分配不承担这项开销。
func partitionRefusalDetail(
	candidates []device.PartitionCandidate, byID map[string]Resource, excluded []partitionExclusion,
) string {
	if len(candidates) == 0 && len(excluded) == 0 {
		return "no accelerator on this node is in a partitioning mode"
	}

	parts := make([]string, 0, len(candidates)+len(excluded))
	for i := range excluded {
		parts = append(parts, excluded[i].Device+" "+excluded[i].Reason)
	}
	for i := range candidates {
		id := byID[candidates[i].ID].Device
		if device.PartitionCandidateFull(candidates[i]) {
			parts = append(parts, fmt.Sprintf("%s every legal placement overlaps an occupied interval (occupied %v)",
				id, candidates[i].Occupied))
			continue
		}
		// 一次请求在每张卡上最多选择一个分区实例。即使卡上还有空位，也不能在同一张卡上放入这次请求的第二个实例。
		parts = append(parts, id+" has a free placement but holds at most one instance of one request")
	}

	return strings.Join(parts, "; ")
}

// profilePlacements 返回某张卡支持该规格的所有合法位置。不支持此规格或位置尚未上报时返回 nil。
func profilePlacements(
	profiles []workercore.AcceleratorPhysicalSlicedProfile, profile string,
) []workercore.AcceleratorPlacement {
	for i := range profiles {
		if profiles[i].Name == profile {
			return profiles[i].Placements
		}
	}
	return nil
}

// requestedUnits 返回每个令牌在单卡上实际占用的单位数，并标明容器是否显式申请了它。
// Pod webhook 将显存预算折算到 .sliced.units 或 .partitioned.units；用这个数记账，
// 准入控制和 InstanceType 才能看到真实余量。未折算的 Pod 暂按每令牌一个单位，调用方还要判断回退值是否适用，
// 尤其是硬件分区。
func (s *ResourceServer) requestedUnits(ctr *core.Container) (int64, bool) {
	if unitsResName := s.unitsResourceName(); unitsResName != "" {
		if q, ok := ctr.Resources.Limits[unitsResName]; ok && q.Value() > 0 {
			return q.Value(), true
		}
	}

	return 1, false
}

// unitsResourceName 返回本模式记录精细用量的资源键，Pod webhook 会把折算后的显存预算写到这个键。
// 没有这种键时返回空字符串。
func (s *ResourceServer) unitsResourceName() core.ResourceName {
	switch s.AllocationMode {
	case workercore.DeviceAllocationModeSliced:
		return nodefeature.GetAcceleratableSlicedUnitsResourceName(s.Manufacturer)
	case workercore.DeviceAllocationModePartitioned:
		return nodefeature.GetAcceleratablePartitionedUnitsResourceName(s.Manufacturer)
	}
	return ""
}

// choosePartitionCards 根据节点上实际空闲的位置，选择硬件分区所在的卡和区间。kubelet 令牌只表示数量，
// 具体卡由插件选择，节点都放不下时才拒绝。函数在 d 上记录规格并返回所选卡、预占区间和单卡费用，
// 必要时从规格折算费用。
func (s *ResourceServer) choosePartitionCards(
	d *_AllocationDecision,
	occupied Placements,
	deviceIDs []string,
	unitsPerToken int64,
	unitsRequested bool,
) (map[Resource][]ResourceToken, Placements, int64, error) {
	profile, ok := partitionProfileOf(d.Container)
	if !ok {
		s.Logger.Error(nil, "partition allocation without a profile",
			"pod", kubemeta.GetNamespacedNameKey(d.Pod), "container", d.Container.Name)
		return nil, nil, unitsPerToken, grpcstatus.Errorf(grpccodes.FailedPrecondition,
			"container %q of pod %s requests %s but names no partition profile",
			d.Container.Name, kubemeta.GetNamespacedNameKey(d.Pod), s.ResourceName())
	}
	d.Profile = profile

	// 未经队列提交的 Pod 可能没有经过 webhook，因此缺少分区用量键。一个令牌代表一次分配，
	// 无法表达分区实际占了半张卡还是整张卡。这里按同一规则从规格折算，以免已经切满的卡仍显示有余额。
	if !unitsRequested {
		if units := s.partitionProfileUnits(d.Devices, profile); units > 0 {
			unitsPerToken = units
		}
	}

	// kubelet 丢失检查点后可能为同一容器重试 Allocate。这时旧分区已经计入占用，必须复用原来的卡和区间。
	// 重新选址可能误报整卡已满，也可能选中另一张卡、绕过厂商按 Pod、容器和卡保存的复用标记，重复创建实例。
	var (
		cardTokens map[Resource][]ResourceToken
		placements Placements
	)
	if prior, held := s.priorAllocationOf(d.Pod, d.Container.Name); held {
		cardTokens, placements = priorPartitionTokens(prior)
	}
	if cardTokens == nil {
		candidates, byID, excluded := s.partitionCandidates(d.Devices, occupied, profile)
		selections, placed := device.SelectPartitionPlacements(candidates, len(deviceIDs))
		if !placed {
			detail := partitionRefusalDetail(candidates, byID, excluded)
			s.Logger.Error(nil, "no card can host the requested partition profile",
				"pod", kubemeta.GetNamespacedNameKey(d.Pod), "profile", profile,
				"instances", len(deviceIDs), "candidates", len(candidates), "cards", detail)
			return nil, nil, unitsPerToken, grpcstatus.Errorf(grpccodes.ResourceExhausted,
				"no card on this node can host %d instance(s) of partition profile %q: %s",
				len(deviceIDs), profile, detail)
		}
		cardTokens, placements = partitionTokens(selections, byID)
	}

	return cardTokens, placements, unitsPerToken, nil
}

// partitionProfileUnits 用探测器报告的分区显存占整卡显存的比例计算单位数，与 Pod webhook 使用同一算法。
// 同型号组内找到第一个提供此规格的卡即可。未知规格或旧探测结果缺少显存信息时返回零，由调用方保留原有预算。
func (s *ResourceServer) partitionProfileUnits(devs *workercore.Devices, profile string) int64 {
	for i := range devs.Spec.Groups {
		grp := &devs.Spec.Groups[i]
		if grp.Manufacturer != s.Manufacturer || grp.Memory == 0 {
			continue
		}
		for j := range grp.Accelerators {
			acc := &grp.Accelerators[j]
			if !device.IsPartitioned(acc.Status) {
				continue
			}
			for k := range acc.Status.PhysicalSliced.Profiles {
				prof := &acc.Status.PhysicalSliced.Profiles[k]
				if prof.Name == profile && prof.MemoryMib > 0 {
					return nodefeature.MemoryMibToUnits(prof.MemoryMib, int64(grp.Memory))
				}
			}
		}
	}
	return 0
}

// priorAllocationOf 先查进程内预留，再查 Pod 注解。后者能跨设备管理器重启保留。kubelet 重试时，
// 硬件分区和逻辑切片都可由此找回原卡和原来的位置。
func (s *ResourceServer) priorAllocationOf(
	pod *core.Pod, container string,
) (workercore.DevicesStatus, bool) {
	if reserved, ok := s.Reconciler.reservedDevices(pod.UID, container); ok && len(reserved.Groups) > 0 {
		return reserved, true
	}
	allocations, err := AllocatedAcceleratorsOf(pod)
	if err != nil {
		// 注解读不出来时只能重新决定。必须记录错误，提醒操作人员同一张卡可能被重复切分。
		s.Logger.Error(err, "read the allocation annotation of an allocating pod; deciding a fresh placement",
			"pod", kubemeta.GetNamespacedNameKey(pod), "container", container)
		return workercore.DevicesStatus{}, false
	}
	if held, ok := allocations[container]; ok && len(held.Devices.Groups) > 0 {
		return held.Devices, true
	}
	return workercore.DevicesStatus{}, false
}

// priorClaimOf 返回容器的完整原分配，包括卡上用量和 kubelet 发来的设备 ID。优先读即时的内存预留，
// 再读可能滞后的 Pod 注解。补偿需要把这两部分一并还原，有些模式无法从实际分到的卡反推出令牌。
//
// 注解损坏时按没有可恢复记录处理。patchAllocatingPod 也必须解析这份注解，
// 因此这种情况下要撤销的补丁本身就无法写入。
func (s *ResourceServer) priorClaimOf(pod *core.Pod, container string) *ContainerAllocation {
	if held, ok := s.Reconciler.reservationsFor(pod.UID)[container]; ok && len(held.Allocated.Groups) > 0 {
		return &ContainerAllocation{Devices: held.Allocated, DeviceIDs: held.DeviceIDs}
	}
	// 尚有待重试的归还操作时，注解里的新条目已经作废。容器实际持有的是归还操作要恢复的旧分配；
	// 首次申请失败则应为空。
	if prior, pending := s.Reconciler.peekPendingRelease(pod.UID, container); pending {
		return prior
	}
	allocations, err := AllocatedAcceleratorsOf(pod)
	if err != nil {
		return nil
	}
	if held, ok := allocations[container]; ok && len(held.Devices.Groups) > 0 {
		return &held
	}
	return nil
}

// priorPartitionTokens 从容器已有分配还原 Allocate 需要的两张映射表，以便重试沿用原卡和区间。
func priorPartitionTokens(
	prior workercore.DevicesStatus,
) (map[Resource][]ResourceToken, Placements) {
	tokens := make(map[Resource][]ResourceToken)
	placements := make(Placements)
	for i := range prior.Groups {
		grp := &prior.Groups[i]
		for j := range grp.Accelerators {
			acc := &grp.Accelerators[j]
			res := Resource{Group: grp.ID, Device: acc.ID}
			tokens[res] = append(tokens[res], ResourceToken{Resource: res, Index: uint64(len(tokens[res]))})
			// 有效的硬件分区记录需要同时具备规格名和占用区间，缺少其中任意一项就跳过。
			if acc.AllocatedPhysicalProfile != "" && len(acc.AllocatedPhysicalPlacements) > 0 {
				placements[res] = acc.AllocatedPhysicalPlacements
			}
		}
	}
	return tokens, placements
}

// partitionTokens 把位置选择转换为 Allocate 后面需要的两张表。一张记录每张所选卡的令牌，
// 另一张记录相应的分区区间。一次分配在每张卡上最多建一个实例。
func partitionTokens(
	selections []device.PartitionSelection, byID map[string]Resource,
) (map[Resource][]ResourceToken, Placements) {
	tokens := make(map[Resource][]ResourceToken, len(selections))
	placements := make(Placements, len(selections))
	for i := range selections {
		res := byID[selections[i].ID]
		tokens[res] = append(tokens[res], ResourceToken{Resource: res, Index: uint64(len(tokens[res]))})
		placements[res] = append(placements[res], selections[i].Placement)
	}
	return tokens, placements
}

// rejectCrossModeCards 在节点上核对设备账本和即时预留。若卡已被另一种模式占用，就拒绝分配，
// 即使 Pod 绕过 Kueue 直接提交也一样。空闲卡和同模式卡通过这道检查；
// 硬件分区模式在这里复查插件自己的选卡结果。
func (s *ResourceServer) rejectCrossModeCards(
	devs *workercore.Devices, cardTokens map[Resource][]ResourceToken, pod *core.Pod,
) error {
	for res := range cardTokens {
		if held, mode := s.cardHeldInOtherMode(devs, res); held {
			s.Logger.Error(nil, "cross-mode allocation rejected",
				"pod", kubemeta.GetNamespacedNameKey(pod), "card", res.String(),
				"heldMode", mode.String(), "requestedMode", s.AllocationMode.String())
			return grpcstatus.Errorf(grpccodes.FailedPrecondition,
				"card %s is held in %s mode, cannot allocate it in %s mode", res, mode, s.AllocationMode)
		}
	}

	return nil
}

func (s *ResourceServer) rejectRepeatedSharedCards(requested int, cardTokens map[Resource][]ResourceToken) error {
	// 第 9 章再检查同一张卡是否被重复选作共享卡。
	return nil
}

// accumulateAllocation 按令牌落在哪张卡以及容器实际申请的单位数记账，
// 产生供内存预留和 Pod 注解共用的分配记录。
func (s *ResourceServer) accumulateAllocation(
	devs *workercore.Devices, cardTokens map[Resource][]ResourceToken, unitsPerToken int64,
) (workercore.DevicesStatus, map[Resource]int32) {
	var (
		allocatedStatus     workercore.DevicesStatus
		allocatedAllocation = make(map[Resource]int32)
	)
	for i := range devs.Spec.Groups {
		specGrp := &devs.Spec.Groups[i]
		if specGrp.Manufacturer != s.Manufacturer {
			continue
		}
		for j := range specGrp.Accelerators {
			acc := &specGrp.Accelerators[j]
			res := Resource{
				Group:  specGrp.ID,
				Device: acc.ID,
			}
			resTokens, existed := cardTokens[res]
			if !existed {
				continue
			}
			var allocated int32
			switch s.AllocationMode {
			default:
				allocated = nodefeature.ResourceMaxUnits // 一整张卡的单位数。
			case workercore.DeviceAllocationModeShared:
				allocated = nodefeature.ResourceMaxUnits / nodefeature.SharedResourceMaxSize // 一个共享持有者应扣除的单位数。
			case workercore.DeviceAllocationModeSliced, workercore.DeviceAllocationModePartitioned:
				// 使用切片或分区实际占用的单卡单位数。若对小分区也沿用默认整卡费用，一小块分区就会把整卡余额清零，
				// 其他消费者看不到仍可用的空间。
				allocated = unitsCharged(unitsPerToken, len(resTokens))
			}
			if allocated > nodefeature.ResourceMaxUnits {
				allocated = nodefeature.ResourceMaxUnits
			}
			if len(allocatedStatus.Groups) == 0 || allocatedStatus.Groups[len(allocatedStatus.Groups)-1].ID != specGrp.ID {
				allocatedStatus.Groups = append(allocatedStatus.Groups, workercore.DevicesAllocationGroup{
					ID:           specGrp.ID,
					Manufacturer: specGrp.Manufacturer,
				})
			}
			statusGrp := &allocatedStatus.Groups[len(allocatedStatus.Groups)-1]
			statusGrp.Accelerators = append(statusGrp.Accelerators, workercore.AcceleratorAllocation{
				ID:        acc.ID,
				Index:     acc.Index,
				Mode:      s.AllocationMode,
				Allocated: allocated,
			})
			allocatedAllocation[res] = allocated
		}
	}

	return allocatedStatus, allocatedAllocation
}

// placeLogicalSlice 为容器在每张卡上选逻辑区间。kubelet 丢失检查点后重试时，应沿用原来的区间。
// 若把原区间误当成别人的占用再另选一个，旧区间将一直遗留到 Pod 删除。
func (s *ResourceServer) placeLogicalSlice(
	ctx context.Context,
	d *_AllocationDecision,
	logicalPlacer LogicalSlicedResponder,
	occupiedLogical Placements,
) (Placements, error) {
	if prior, held := s.priorLogicalPlacements(d.Pod, d.Container.Name); held {
		return prior, nil
	}

	placements, err := logicalPlacer.PlaceLogicalSliced(
		ctx, d.Pod, d.Container, d.Devices, d.Allocation, occupiedLogical,
	)
	if err != nil {
		s.Logger.Error(err, "place logical slice for allocation",
			"pod", kubemeta.GetNamespacedNameKey(d.Pod), "container", d.Container.Name)
		return nil, grpcstatus.Errorf(grpccodes.Internal, "place logical slice: %v", err)
	}

	return placements, nil
}

// priorLogicalPlacements 取回该容器已有的逻辑区间，重试时可直接复用。
func (s *ResourceServer) priorLogicalPlacements(pod *core.Pod, container string) (Placements, bool) {
	prior, held := s.priorAllocationOf(pod, container)
	if !held {
		return nil, false
	}
	placements := make(Placements)
	accumulateLogicalOccupied(prior, placements)
	if len(placements) == 0 {
		return nil, false
	}
	return placements, true
}

// applyLogicalPlacements 将 responder 选定的区间写入分配状态，随后内存预留可以立即公布它，
// Pod 注解负责持久保存。
func applyLogicalPlacements(status *workercore.DevicesStatus, placements Placements) {
	for i := range status.Groups {
		grp := &status.Groups[i]
		for j := range grp.Accelerators {
			acc := &grp.Accelerators[j]
			if p, ok := placements[Resource{Group: grp.ID, Device: acc.ID}]; ok && len(p) > 0 {
				acc.AllocatedLogicalPlacements = p
			}
		}
	}
}

// applyPhysicalPlacements 将每张卡的实际分区规格、区间和分区 ID 写入分配状态。控制器从注解聚合占用，
// 后续调用也能按分区 ID 定位硬件实例。
func applyPhysicalPlacements(
	status *workercore.DevicesStatus,
	profile string,
	placements Placements,
	ids map[Resource]string,
) {
	for i := range status.Groups {
		grp := &status.Groups[i]
		for j := range grp.Accelerators {
			acc := &grp.Accelerators[j]
			res := Resource{Group: grp.ID, Device: acc.ID}
			if p, ok := placements[res]; ok {
				acc.AllocatedPhysicalProfile = profile
				acc.AllocatedPhysicalPlacements = p
				acc.AllocatedPhysicalID = ids[res]
			}
		}
	}
}

// actuatePartition 使用厂商的分区执行器把预期分配落实到硬件，并在写注解之前把实际位置存入 d.Status。
// 驱动可能复用已有实例，使实际区间与原计划不同，此时需更新内存预留。
//
// 硬件操作在节点锁外进行，厂商只需锁住同一张卡，其他卡可以并行处理。请求指定规格但执行器不支持时拒绝，
// 未指定分区规格的分配可直接跳过此步。
func (s *ResourceServer) actuatePartition(
	ctx context.Context, d *_AllocationDecision, deviceIDs []string,
) (*PhysicalSlicedAllocation, error) {
	if d.Profile == "" {
		return nil, nil
	}

	actuator, canActuate := s.Responder.(PhysicalSlicedResponder)
	if !canActuate {
		s.Reconciler.releaseReservation(d.Pod.UID, d.Container.Name)

		return nil, grpcstatus.Errorf(grpccodes.Internal,
			"responder cannot actuate partition profile %q", d.Profile)
	}

	physical, err := actuator.ActuatePhysicalSliced(
		ctx, d.Pod, d.Container, d.Devices, d.Allocation, d.Profile,
	)
	if err != nil {
		s.Reconciler.releaseReservation(d.Pod.UID, d.Container.Name)
		s.Logger.Error(err, "actuate partition for allocation", "pod", kubemeta.GetNamespacedNameKey(d.Pod))

		return nil, grpcstatus.Errorf(grpccodes.Internal, "actuate partition: %v", err)
	}
	applyPhysicalPlacements(&d.Status, physical.Profile, physical.Placements, physical.IDs)
	s.Reconciler.reserveDevices(d.Pod.UID, d.Container.Name, d.Status, deviceIDs)

	return physical, nil
}

// renderPrePatchResponse 提前构造两类响应。逻辑切片先按所选区间构造响应，
// 可以避免失败后仍为未启动的容器记着计算区间；它至多创建一个目录。
// 硬件分区直接用执行器准备好的可见设备信息。
//
// 其他模式返回 nil，写完注解后再调 responder。寒武纪和沐曦的 responder 会创建子设备及归属标记，
// 若过早创建而注解写入失败，仍存活的 Pod 会让回收器保留账本看不见的硬件资源。
func (s *ResourceServer) renderPrePatchResponse(
	ctx context.Context,
	d *_AllocationDecision,
	physical *PhysicalSlicedAllocation,
	logicalPlacer LogicalSlicedResponder,
	placesLogical bool,
) (*ContainerAllocateResponse, error) {
	switch {
	case physical != nil:
		return physical.Response, nil

	case placesLogical:
		// 使用已经登记的区间构造响应。若重新计算，容器看到的区间可能与账本不同。
		ctrResp, err := logicalPlacer.GetLogicalSlicedResponse(
			ctx, d.Pod, d.Container, d.Devices, d.Allocation, d.LogicalPlacements,
		)
		if err != nil {
			s.Reconciler.releaseReservation(d.Pod.UID, d.Container.Name)
			s.Logger.Error(err, "get logical sliced response")

			return nil, err
		}

		return ctrResp, nil
	}

	return nil, nil
}

// persistAllocation 在节点分配锁外写 Pod 注解。写入失败时，kubelet 不会启动容器，
// 必须立即撤销内存预留并回滚本次新建的分区。否则，因为没有注解，删除 Pod 的监听也可能无法触发清理。
func (s *ResourceServer) persistAllocation(
	ctx context.Context,
	d *_AllocationDecision,
	deviceIDs []string,
	physical *PhysicalSlicedAllocation,
) error {
	err := s.Reconciler.patchAllocatingPod(ctx, d.Pod, d.Container.Name, d.Status, deviceIDs)
	if err == nil {
		return nil
	}

	if physical != nil && physical.Rollback != nil {
		physical.Rollback()
	}
	s.Reconciler.releaseReservation(d.Pod.UID, d.Container.Name)
	s.Logger.Error(err, "patch allocating pod for allocation")

	return grpcstatus.Errorf(grpccodes.Internal, "patch allocating pod for allocation: %v", err)
}

// releaseAllocationTimeout 限制补偿写入的最长时间。补偿独立于 kubelet 的请求上下文，
// 因此要自行设置截止时间。
const releaseAllocationTimeout = 10 * time.Second

// releaseAllocation 处理注解已经写入、厂商 responder 随后拒绝的情况。kubelet 不会启动容器，
// 本次申请的资源须归还。若这是重试，恢复原分配的内存预留和注解，只撤销本次新增的部分。
//
// 先恢复内存，再写注解，以免同 Pod 的其他容器从预留中重新抄回已拒的分配。
// 另一个容器仍可能从滞后的缓存注解抄回该条目。注解的读改写在节点锁外完成，要完全消除这种竞态，
// 须由所有写入方串行更新。
//
// 补偿使用独立的限时上下文，原请求取消也能归还资源。执行器新建的硬件分区会被回滚，复用的实例会保留。
// 厂商 responder 的约定是创建私有硬件后不能再以错误退出，框架无法替它回收未知资源。各步失败会记日志，
// 向 kubelet 返回的仍是原本的分配错误。
func (s *ResourceServer) releaseAllocation(
	ctx context.Context, d *_AllocationDecision, physical *PhysicalSlicedAllocation,
) {
	if physical != nil && physical.Rollback != nil {
		physical.Rollback()
	}
	if d.PriorClaim != nil {
		s.Reconciler.reserveDevices(d.Pod.UID, d.Container.Name, d.PriorClaim.Devices, d.PriorClaim.DeviceIDs)
	} else {
		s.Reconciler.releaseReservation(d.Pod.UID, d.Container.Name)
	}

	unpatchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseAllocationTimeout)
	defer cancel()

	if err := s.Reconciler.unpatchAllocatingPod(unpatchCtx, d.Pod, d.Container.Name, d.PriorClaim); err != nil {
		s.Logger.Error(err, "unpatch allocating pod for refused allocation",
			"pod", kubemeta.GetNamespacedNameKey(d.Pod), "container", d.Container.Name)
	}
}

// allocateVisibility 让边车访问同一 Pod 中主容器已经取得的物理设备。优先复用内存预留，
// 经厂商 responder 返回可见设备信息，不另扣额度或写分配状态。找不到归属时返回错误，
// 以免运行时将空的设备列表理解为允许访问所有设备。
func (s *ResourceServer) allocateVisibility(ctx context.Context, req *AllocateRequest, kubelet *_KubeletPending) (*AllocateResponse, error) {
	ctrReq := req.GetContainerRequests()[0]

	resName := s.ResourceName()
	resQuantity := *resource.NewQuantity(int64(len(ctrReq.GetDevicesIds())), resource.DecimalSI)
	pod, ctr, err := s.claimVisibilityContainer(ctx, resName, resQuantity, kubelet)
	if err != nil {
		s.Logger.Error(err, "get allocating pod for visibility allocation")
		return nil, grpcstatus.Errorf(grpccodes.Internal, "get allocating pod for visibility allocation: %v", err)
	}
	// 后续步骤仍可能失败。只有真正发出授权才保留记录，否则 kubelet 重试同一容器时可能匹配到另一个 Pod。
	granted := false
	defer func() {
		if !granted {
			s.Reconciler.revokeVisibility(pod.UID, ctr.Name)
		}
	}()

	// 读取同一 Pod 中其他容器持有的设备。加速器请求集中在一个容器组，边车只共享主容器已领到的设备，不独立预留。
	reserved, owner, err := s.coAllocatedAccelerators(pod, ctr.Name)
	if err != nil {
		s.Logger.Error(err, "resolve the visibility co-allocation")
		return nil, grpcstatus.Errorf(grpccodes.FailedPrecondition, "%v", err)
	}
	if len(reserved.Groups) == 0 {
		// 内存和注解都没有主容器的记录时，拒绝此次分配，防止空的可见设备变量暴露所有卡。
		// kubelet 按 init 容器再普通容器的声明顺序准入，当前生成的 Pod 都先分配主容器。若改变顺序，
		// 就得有一个不依赖先后次序的归属来源，否则只能等待控制器重建 Pod。
		err = fmt.Errorf("no accelerator devices allocated for pod %s by a container other than %q "+
			"(owner=%q); refusing to grant visibility",
			kubemeta.GetNamespacedNameKey(pod), ctr.Name, owner)
		s.Logger.Error(err, "visibility allocation without a co-allocation")
		return nil, grpcstatus.Errorf(grpccodes.FailedPrecondition, "%v", err)
	}

	devs, err := s.Reconciler.getDevices(ctx)
	if err != nil {
		s.Logger.Error(err, "get devices for visibility allocation")
		return nil, grpcstatus.Errorf(grpccodes.Internal, "get devices for visibility allocation: %v", err)
	}

	allocated, err := s.validateVisibilityReservation(devs, reserved, pod, owner, len(ctrReq.GetDevicesIds()))
	if err != nil {
		return nil, err
	}

	ctrResp, err := s.visibilityResponse(ctx, pod, ctr, devs, allocated, owner)
	if err != nil {
		s.Logger.Error(err, "get container visibility allocate response")
		return nil, err
	}

	resp := &AllocateResponse{
		ContainerResponses: []*ContainerAllocateResponse{ctrResp},
	}
	granted = true
	s.Logger.Info("visibility allocate response",
		"pod", kubemeta.GetNamespacedNameKey(pod),
		"response", resp)
	return resp, nil
}

// claimVisibilityContainer 在同一把节点锁内识别边车并登记授权。主容器已有预留的 Pod 仍要参与匹配，
// 已经授予可见性的容器要跳过。否则多个同形 Pod 并发请求时可能全部匹配到第一个 Pod，
// 连其他租户的分区也会被错误暴露。
func (s *ResourceServer) claimVisibilityContainer(
	ctx context.Context, resName core.ResourceName, resQuantity resource.Quantity, kubelet *_KubeletPending,
) (*core.Pod, *core.Container, error) {
	s.Reconciler.allocateMutex.Lock()
	defer s.Reconciler.allocateMutex.Unlock()

	pod, ctr, err := s.Reconciler.getAllocatingPod(ctx, _AllocationMatch{
		ResourceName: resName,
		Quantity:     resQuantity,
		SkipGranted:  true,
		Kubelet:      kubelet,
	})
	if err != nil {
		return nil, nil, err
	}
	s.Reconciler.grantVisibility(pod.UID, ctr.Name)
	return pod, ctr, nil
}

// coAllocatedAccelerators 查找同一 Pod 中除自身外持有加速器的容器，返回该容器的名称及设备。先查内存，
// 再查跨重启保留的注解，两个来源都用同一规则选归属。注解损坏时报告错误；两边都没有记录时返回空结果，
// 交给上层拒绝分配。
func (s *ResourceServer) coAllocatedAccelerators(
	pod *core.Pod, self string,
) (workercore.DevicesStatus, string, error) {
	if reserved, owner, ok := s.Reconciler.reservedAcceleratorDevices(pod.UID, self); ok && len(reserved.Groups) > 0 {
		return reserved, owner, nil
	}
	allocations, err := AllocatedAcceleratorsOf(pod)
	if err != nil {
		return workercore.DevicesStatus{}, "", fmt.Errorf(
			"read the allocation annotation of pod %s: %w", kubemeta.GetNamespacedNameKey(pod), err,
		)
	}
	names := make([]string, 0, len(allocations))
	for name := range allocations {
		names = append(names, name)
	}
	owner, ok := pickAcceleratorOwner(names, self)
	if !ok {
		return workercore.DevicesStatus{}, "", nil
	}
	return allocations[owner].Devices, owner, nil
}

// validateVisibilityReservation 要求预留中的每张设备仍在节点库存里，而且设备数量与边车请求完全相符。
// 只给边车开放主容器实际持有的原设备，以免过时或不完整的预留意外开放其他卡。此路径不扣新单位，不写注解，
// 也不添加切片挂载或限额。
func (s *ResourceServer) validateVisibilityReservation(
	devs *workercore.Devices,
	reserved workercore.DevicesStatus,
	pod *core.Pod,
	owner string,
	requestCount int,
) (map[Resource]int32, error) {
	present := make(map[Resource]struct{})
	for i := range devs.Spec.Groups {
		grp := &devs.Spec.Groups[i]
		for j := range grp.Accelerators {
			present[Resource{Group: grp.ID, Device: grp.Accelerators[j].ID}] = struct{}{}
		}
	}

	reservedCount := 0
	var reservedCards []string
	allocated := make(map[Resource]int32)
	for i := range reserved.Groups {
		grp := &reserved.Groups[i]
		for j := range grp.Accelerators {
			reservedCount++
			res := Resource{Group: grp.ID, Device: grp.Accelerators[j].ID}
			reservedCards = append(reservedCards, res.String())
			if _, ok := present[res]; !ok {
				continue
			}
			allocated[res] = grp.Accelerators[j].Allocated
		}
	}

	if len(allocated) != reservedCount || reservedCount != requestCount {
		err := fmt.Errorf("visibility reservation for pod %s held by container %q does not match the "+
			"request (cards=%v, reserved=%d, present=%d, requested=%d); refusing to grant visibility",
			kubemeta.GetNamespacedNameKey(pod), owner, reservedCards, reservedCount, len(allocated), requestCount)
		s.Logger.Error(err, "visibility allocation with mismatched or stale reservation")

		return nil, grpcstatus.Errorf(grpccodes.FailedPrecondition, "%v", err)
	}

	return allocated, nil
}

// visibilityResponse 根据主容器的请求构造边车响应。主容器使用硬件分区时，只能暴露该分区，
// 开放父卡会同时暴露其他租户的分区。主容器的资源请求从 Pod 创建时就存在，与预留发布的先后顺序无关。
// 找不到主容器、缺少厂商能力或驱动返回错误时都拒绝授权；其他模式按物理卡构造响应。
func (s *ResourceServer) visibilityResponse(
	ctx context.Context,
	pod *core.Pod,
	ctr *core.Container,
	devs *workercore.Devices,
	allocated map[Resource]int32,
	owner string,
) (*ContainerAllocateResponse, error) {
	ownerCtr := containerByName(pod, owner)
	if ownerCtr == nil {
		return nil, grpcstatus.Errorf(grpccodes.FailedPrecondition,
			"pod %s has no container %q to co-allocate from; refusing to grant visibility",
			kubemeta.GetNamespacedNameKey(pod), owner)
	}
	profile, partitioned := partitionProfileOf(ownerCtr)
	if !partitioned {
		return s.Responder.GetContainerAllocateResponse(ctx, pod, ctr, devs, allocated)
	}

	responder, canRespond := s.Responder.(PhysicalSlicedResponder)
	if !canRespond {
		return nil, grpcstatus.Errorf(grpccodes.FailedPrecondition,
			"responder cannot name the partition profile %q container %q holds in pod %s; "+
				"refusing to grant visibility",
			profile, owner, kubemeta.GetNamespacedNameKey(pod))
	}
	ctrResp, err := responder.GetPhysicalSlicedVisibilityResponse(ctx, pod, ctr, devs, allocated, owner)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.FailedPrecondition,
			"name the partition profile %q container %q holds in pod %s: %v; refusing to grant visibility",
			profile, owner, kubemeta.GetNamespacedNameKey(pod), err)
	}
	return ctrResp, nil
}

// containerByName 按 init 容器、普通容器的顺序查找名称，和识别待分配容器的遍历顺序一致。找不到时返回 nil。
func containerByName(pod *core.Pod, name string) *core.Container {
	for i := range pod.Spec.InitContainers {
		if pod.Spec.InitContainers[i].Name == name {
			return &pod.Spec.InitContainers[i]
		}
	}
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == name {
			return &pod.Spec.Containers[i]
		}
	}
	return nil
}

// 只要上下文允许，Start 会将此服务器的资源提供给 kubelet，每当 kubelet 取走这些资源时，服务会重新建立。
//
// 它会阻塞，直到 ctx 被取消或调用 Stop 指令，只有在无法建立任何代次时才会报告错误。在已运行的服务器上调用
// 它则毫无效果，只会阻塞直到 ctx 完成，因此重试的主管无法让两个服务器共享一个套接字。
func (s *ResourceServer) Start(ctx context.Context, kubeSocket string) error {
	// serving.Start 会启动 gRPC 服务，并连上 kubelet.sock 报到
	return s.serving.Start(ctx, kubeSocket, s.servingOptions(), s.Logger)
}

// Stop 会停止 gRPC 服务器并结束推理服务循环，在 Start 执行完毕后才会返回。此时没有该服务器的代次需要处理
// 套接字，这允许分配器在该路径上替换服务器。这是一个无操作函数，会被记录，适用于从未启动的服务器。正在进
// 行的分配会被放弃，而非被清空：kubelet 会重试那些未能获得响应的分配，而该服务器所做的预留也会在其 Pods
// 消失后由协调器进行清理。
//
// 稍后的启动操作在之前的启动操作返回后可以再次执行——单个启动保护机制在启动操作离开时会被解除，因此，一个
// 启动操作同时执行自己的停止操作会被拒绝，无法允许在同一个套接字上放置第二个服务器。
func (s *ResourceServer) Stop() {
	s.serving.Stop(s.Logger)
}

// servingOptions 提供注册所需的三项信息，即处理 gRPC 的插件、本实例注册的资源名，
// 以及 kubelet 插件目录下的套接字文件名。
func (s *ResourceServer) servingOptions() servingOptions {
	return servingOptions{
		Plugin:       s,
		ResourceName: s.ResourceName(),
		SocketName:   stringx.Join(".", s.Manufacturer, strings.ToLower(s.AllocationMode.String()), "sock"),
	}
}

// cardHeldInOtherMode 检查账本状态和同步写入的进程内预留。卡被其他模式持有时返回该模式供日志使用；
// 空闲或同模式不算冲突。
func (s *ResourceServer) cardHeldInOtherMode(devs *workercore.Devices, res Resource) (bool, workercore.DeviceAllocationMode) {
	if m := statusModeOf(devs, res); m != workercore.DeviceAllocationModeNone && m != s.AllocationMode {
		return true, m
	}
	if m, _ := s.Reconciler.reservedModeForResource(res.Group, res.Device); m != workercore.DeviceAllocationModeNone && m != s.AllocationMode {
		return true, m
	}
	return false, workercore.DeviceAllocationModeNone
}

// statusModeOf 查询账本所记的物理卡分配模式。卡空闲或还没有状态记录时，返回 DeviceAllocationModeNone。
func statusModeOf(devs *workercore.Devices, res Resource) workercore.DeviceAllocationMode {
	for i := range devs.Status.Groups {
		grp := &devs.Status.Groups[i]
		if grp.ID != res.Group {
			continue
		}
		for j := range grp.Accelerators {
			if grp.Accelerators[j].ID == res.Device {
				return grp.Accelerators[j].Mode
			}
		}
	}
	return workercore.DeviceAllocationModeNone
}

// statusRemainingOf 读取卡在账本上的剩余单位。缺少该卡状态时按尚未占用、整卡满额处理。
func statusRemainingOf(devs *workercore.Devices, res Resource) int32 {
	for i := range devs.Status.Groups {
		grp := &devs.Status.Groups[i]
		if grp.ID != res.Group {
			continue
		}
		for j := range grp.Accelerators {
			if grp.Accelerators[j].ID == res.Device {
				return grp.Accelerators[j].Remaining
			}
		}
	}
	return nodefeature.ResourceMaxUnits
}

// partitionProfileOf 从容器请求中解析 <base>.partitioned.<kind>-<profile>。Pod webhook 保证最多一种规格。
// 返回值使用厂商自己的规格拼法，让库存匹配、分配记录、归属标记和驱动调用都能识别。
func partitionProfileOf(ctr *core.Container) (string, bool) {
	for name := range ctr.Resources.Limits {
		if profile, ok := nodefeature.VendorPartitionedProfileOf(name); ok {
			return profile, true
		}
	}
	return "", false
}
