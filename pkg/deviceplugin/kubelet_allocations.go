package deviceplugin

import (
	"context"
	"fmt"
	"slices"

	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	podresources "k8s.io/kubelet/pkg/apis/podresources/v1"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/nodefeature"
)

// verifiedKubeletPending 使用同一份 kubelet 快照核对账本并识别正在等待设备的候选容器。
func (r *DevicesReconciler) verifiedKubeletPending(ctx context.Context, resource core.ResourceName) (*_KubeletPending, error) {
	if r.kubeletPods == nil {
		return nil, nil
	}
	devs, err := r.getDevices(ctx)
	if err != nil {
		return nil, err
	}
	pods := new(core.PodList)
	if err := r.Client.List(ctx, pods, ctrlcli.MatchingFields{IndexingPodsByNodeName: r.NodeName}); err != nil {
		return nil, err
	}
	listed, err := r.verifyKubeletAllocations(ctx, devs, pods)
	if err != nil {
		return nil, err
	}
	return kubeletPendingFrom(listed, resource), nil
}

// manufacturableAccelerator 保存从资源名解析出的厂商和分配模式。
type manufacturableAccelerator struct {
	manufacturer string
	mode         workercore.DeviceAllocationMode
}

// manufacturableAccelerators 为需要核对的资源名建立索引。厂商、模式和名称在 nodefeature 初始化后就已确定，
// 只需建表一次。各基础名互异，共享后缀与基础名也不会冲突，每个名字可唯一确定厂商和模式。
var manufacturableAccelerators = func() map[string]manufacturableAccelerator {
	table := make(map[string]manufacturableAccelerator)
	for _, manufacturer := range nodefeature.GetKnownAcceleratableManufacturers() {
		for _, mode := range []workercore.DeviceAllocationMode{
			workercore.DeviceAllocationModeExclusive, workercore.DeviceAllocationModeShared,
		} {
			name := string(nodefeature.GetAcceleratableResourceName(manufacturer, mode))
			table[name] = manufacturableAccelerator{manufacturer: manufacturer, mode: mode}
		}
	}

	return table
}()

// verifyKubeletAllocations 发现 kubelet 与本地记录矛盾时，阻止发布账本或继续分配。
// 丢失检查点不能证明卡已释放，因此保留缺少运行时对应项的记录。PodResources 没有 Pod UID，
// 遇到身份冲突需要运维核实。
func (r *DevicesReconciler) verifyKubeletAllocations(
	ctx context.Context, devs *workercore.Devices, pods *core.PodList,
) ([]*podresources.PodResources, error) {
	if r.kubeletPods == nil {
		return nil, nil
	}
	listed, err := r.kubeletPods(ctx)
	if err != nil {
		return nil, fmt.Errorf("verify kubelet device assignments: %w", err)
	}
	for _, reportedPod := range listed {
		for _, container := range reportedPod.GetContainers() {
			// kubelet 可能将同一资源的设备 ID 分在多条记录中，需要先按资源名归并。
			resources := make(map[string][]string)
			for _, devices := range container.GetDevices() {
				name := devices.GetResourceName()
				resources[name] = append(resources[name], devices.GetDeviceIds()...)
			}
			for name, deviceIDs := range resources {
				if len(deviceIDs) == 0 {
					continue
				}
				accelerator, ok := manufacturableAccelerators[name]
				if !ok {
					continue
				}
				if err := verifyKubeletContainer(devs, pods, reportedPod, container.GetName(),
					deviceIDs, accelerator.manufacturer, accelerator.mode); err != nil {
					return nil, err
				}
			}
		}
	}
	return listed, nil
}

// 整卡令牌的 ID 足以还原卡的归属，切片的具体区间还需要持久记录。
func verifyKubeletContainer(devs *workercore.Devices, pods *core.PodList,
	reported *podresources.PodResources, container string, deviceIDs []string, manufacturer string, mode workercore.DeviceAllocationMode,
) error {
	index := slices.IndexFunc(pods.Items, func(p core.Pod) bool { return p.Name == reported.GetName() && p.Namespace == reported.GetNamespace() })
	if index < 0 {
		return fmt.Errorf("kubelet holds accelerators for uncached pod %s/%s; refusing a ledger rebuild", reported.GetNamespace(), reported.GetName())
	}
	pod := &pods.Items[index]
	if pod.Status.Phase == core.PodSucceeded || pod.Status.Phase == core.PodFailed {
		return nil
	}
	tokens := make(map[Resource][]ResourceToken)
	for _, id := range deviceIDs {
		token, err := ParseResourceToken(id)
		if err != nil {
			return fmt.Errorf("parse kubelet device ID for pod %s: %w", ctrlcli.ObjectKeyFromObject(pod), err)
		}
		tokens[token.Resource] = append(tokens[token.Resource], token)
	}
	server := &ResourceServer{Manufacturer: manufacturer, AllocationMode: mode}
	expected, allocation := server.accumulateAllocation(devs, tokens, 0)
	if len(allocation) != len(tokens) {
		return fmt.Errorf("kubelet assigns an accelerator absent from inventory to pod %s", ctrlcli.ObjectKeyFromObject(pod))
	}
	allocations, err := AllocatedAcceleratorsOf(pod)
	if err != nil {
		return fmt.Errorf("read allocation of pod %s: %w", ctrlcli.ObjectKeyFromObject(pod), err)
	}
	claim := allocations[container]
	if !sets.New(claim.DeviceIDs...).Equal(sets.New(deviceIDs...)) || !kubemeta.DeepEqual(claim.Devices, expected) {
		return fmt.Errorf(
			"allocation identity conflict for pod %s container %q: annotation differs from kubelet device IDs %v; refusing device bookkeeping until recovery",
			ctrlcli.ObjectKeyFromObject(pod), container, deviceIDs,
		)
	}
	return nil
}
