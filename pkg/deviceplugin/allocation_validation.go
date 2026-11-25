package deviceplugin

import (
	"fmt"
	"sort"

	"github.com/go-logr/logr"
	core "k8s.io/api/core/v1"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/utils/json"
)

// IMPORTANT: 本文件负责：Fail-Closed 严格分配校验

// 严格重建之所以存在，是因为普通重建只提供尽力而为（best-effort）的结果，而退场协议需要
// 不同的语义。mergePodAllocations 不会失败：不可读的注解、库存中未知的加速卡以及模式冲突
// 都会被记录日志并跳过，导致从合并中脱落的占用者在容器仍持有硬件时被视为空闲。这种权衡适合
// 为 kubelet 提供账本的普通重建，但不适合判断容量是否已释放的谓词。本路径在相同输入上执行
// 相同的算术逻辑，但直接返回错误而非吞掉错误，使不完整的输入保持阻塞而非放行。
//
// 此处不会重新推导账本算术。严格路径直接调用普通路径自身的折叠函数（foldPodAllocations），
// 仅传入遇到失败即停止的回调，因此 heldAllocation、applyAllocatedStatus、
// accumulatePhysicalOccupied 和 foldPhysicalLedger 均以相同方式复用，严格结果与已发布账本
// 不会因存在两套算术实现而产生分歧。

// BuildDesiredStatusStrict 是面向不能接受半成品结果的调用方的 BuildDesiredStatus。
//
// 它在执行任何合并前先校验每个 Pod 的记录，因此单条异常记录会使整次重建失败，而不会静默地
// 将某个占用者从账本中移除。
func BuildDesiredStatusStrict(
	logger logr.Logger, devs *workercore.Devices, podList *core.PodList,
) (workercore.DevicesStatus, []string, error) {
	// 设备库存是输入的另一半，在最前面统一校验一次：重复或自相矛盾的加速卡身份会让后续每次
	// 比对都产生歧义，而在歧义库存上执行算术折叠会给出看似确定实则错误的结果，而不是显式报错。
	if err := validateDeviceInventory(devs); err != nil {
		return workercore.DevicesStatus{}, nil, err
	}

	for i := range podList.Items {
		if err := validatePodAllocationRecord(&podList.Items[i], devs); err != nil {
			return workercore.DevicesStatus{}, nil, err
		}
	}

	// 归约器自身的错误来自与普通重建相同的算术逻辑，此处直接返回而非仅写日志。
	desired, livePodUIDs, err := mergePodAllocationsStrict(devs, podList)
	if err != nil {
		return workercore.DevicesStatus{}, nil, err
	}

	return desired, livePodUIDs, nil
}

// validateDeviceInventory 拒绝身份信息自相矛盾的设备库存。
//
// 一张加速卡由（group ID、device ID）唯一标识，并同时携带 index 和 manufacturer。身份重复
// 说明两条 spec 行声明了同一张卡；index 与其位置不一致则会导致按 index 查找与按 ID 查找解析
// 到不同的加速卡。
func validateDeviceInventory(devs *workercore.Devices) error {
	byID := make(map[string]struct{}, len(devs.Spec.Groups))
	for i := range devs.Spec.Groups {
		group := &devs.Spec.Groups[i]
		if group.ID == "" {
			return fmt.Errorf("accelerator group %d carries no ID", i)
		}
		if _, seen := byID[group.ID]; seen {
			return fmt.Errorf("accelerator group %q is declared twice", group.ID)
		}
		byID[group.ID] = struct{}{}

		ids := make(map[string]struct{}, len(group.Accelerators))
		// index 仅在厂商组内局部唯一。不同厂商都会从 0 开始为首张卡编号，探测器也按此写入，
		// 因此唯一性校验限定在组内，不在跨卡全局范围强制要求，否则会拒绝同时装有 NVIDIA 与 AMD
		// 设备的节点。
		indices := make(map[uint32]struct{}, len(group.Accelerators))
		for j := range group.Accelerators {
			card := &group.Accelerators[j]
			if card.ID == "" {
				return fmt.Errorf("accelerator %d of group %q carries no ID", j, group.ID)
			}
			if _, seen := ids[card.ID]; seen {
				return fmt.Errorf("accelerator %q is declared twice in group %q", card.ID, group.ID)
			}
			ids[card.ID] = struct{}{}
			if _, seen := indices[card.Index]; seen {
				return fmt.Errorf(
					"accelerator index %d is declared twice in group %q", card.Index, group.ID,
				)
			}
			indices[card.Index] = struct{}{}
		}
	}

	return nil
}

// validatePodAllocationRecord 在合并前先校验单个 Pod 的分配记录，不合法则拒绝。
//
// 各项检查按开销从低到高排序，最先校验最基本的身份事实；每个错误都会指明出错的 Pod 与容器，
// 以便运维人员在操作被阻塞时能够定位具体问题所在。
func validatePodAllocationRecord(pod *core.Pod, devs *workercore.Devices) error {
	// 整条记录值为 JSON 字面量 null 并不等同于缺少记录：json.Unmarshal 会无错地将其解码为
	// nil map，若将其当作“完全没有记录”，就会把存在但为空的声明绕过下方的显式 null 拦截。
	// 本 operator 绝不会写入裸 null，因此 Pod 上出现裸 null 即属非法记录。
	if pod.Annotations[AllocatedAcceleratorAnnoKey] == "null" {
		return fmt.Errorf("pod %s: allocation record is null", ctrlcli.ObjectKeyFromObject(pod))
	}
	allocations, err := AllocatedAcceleratorsOf(pod)
	if err != nil {
		return fmt.Errorf("pod %s: read its allocation record: %w", ctrlcli.ObjectKeyFromObject(pod), err)
	}
	if allocations == nil {
		// 完全没有分配记录的 Pod 表示尚未完成设备分配。这本身是正常状态，由调用方决定其含义；
		// 唯一绝不能发生的是调用方将申请了加速卡的 Pod 的空记录误读为“未持有任何设备”。
		return nil
	}
	if pod.Spec.NodeName == "" {
		return fmt.Errorf("pod %s: holds an allocation record but is not on a node",
			ctrlcli.ObjectKeyFromObject(pod))
	}

	// 直接读取按容器划分的原始记录，而不是通过 Aggregate 读取：Aggregate 折叠会丢弃容器自身的
	// 分组身份，对其校验相当于校验待测逻辑的输出本身。
	// 在其他检查之前先区分显式 null 与缺省条目。解码后的 map 存放值而非指针，到达此处时 null
	// 与空记录都是相同的零值，因此需要回读原始注解以保留这一区分。
	if null := nullContainersIn(pod); null != "" {
		return fmt.Errorf("pod %s container %q: allocation record is null",
			ctrlcli.ObjectKeyFromObject(pod), null)
	}

	for container, record := range allocations {
		if !podDeclaresContainer(pod, container) {
			return fmt.Errorf("pod %s container %q: holds an allocation record but declares no such container",
				ctrlcli.ObjectKeyFromObject(pod), container)
		}
		if err := validateRecordedClaims(pod, container, devs, record.Devices); err != nil {
			return err
		}
	}

	return nil
}

// nullContainersIn 返回记录值为显式 JSON null 的首个容器名称。
func nullContainersIn(pod *core.Pod) string {
	raw, present := pod.Annotations[AllocatedAcceleratorAnnoKey]
	if !present || raw == "" {
		return ""
	}
	entries := map[string]json.RawMessage{}
	if err := json.Unmarshal([]byte(raw), &entries); err != nil {
		// 非 JSON 对象形态会由常规解码路径报告，沿用本包既有的错误文案。
		return ""
	}
	names := make([]string, 0, len(entries))
	for name := range entries {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if string(entries[name]) == "null" {
			return name
		}
	}

	return ""
}

// podDeclaresContainer 报告 Pod 是否确实声明了分配记录所指名的容器。指向不存在容器的记录无法
// 与任何对象完成调谐校验。
//
// 此处刻意不检查临时容器（ephemeral containers）：Kubernetes 禁止为其设置资源 requests 或
// limits，因此调试容器无法申请加速器，设备插件也不会收到针对它的 Allocate 请求。仅指向临时
// 容器的记录保持报错。
func podDeclaresContainer(pod *core.Pod, container string) bool {
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == container {
			return true
		}
	}
	for i := range pod.Spec.InitContainers {
		if pod.Spec.InitContainers[i].Name == container {
			return true
		}
	}

	return false
}

// validateRecordedClaims 依据节点设备库存核对单个容器记录的加速器声明。
//
// 不依赖 Aggregate() 保留厂商信息，而是直接读取 Pod 自身记录中的设备组，并在执行任何合并前
// 在库存中解析每一张加速卡。
func validateRecordedClaims(
	pod *core.Pod, container string, devs *workercore.Devices, status workercore.DevicesStatus,
) error {
	for gi := range status.Groups {
		group := &status.Groups[gi]
		spec := findDeviceGroup(devs, group.ID)
		if spec == nil {
			return fmt.Errorf("pod %s container %q: names accelerator group %q, which the inventory does not have",
				ctrlcli.ObjectKeyFromObject(pod), container, group.ID)
		}
		if group.Manufacturer != spec.Manufacturer {
			return fmt.Errorf(
				"pod %s container %q: group %q records manufacturer %q, the inventory says %q",
				ctrlcli.ObjectKeyFromObject(pod), container, group.ID, group.Manufacturer, spec.Manufacturer,
			)
		}
		seen := make(map[string]struct{}, len(group.Accelerators))
		for ci := range group.Accelerators {
			card := &group.Accelerators[ci]
			cardSpec := findDeviceCard(spec, card.ID)
			if cardSpec == nil {
				return fmt.Errorf(
					"pod %s container %q: names accelerator %q in group %q, which the inventory does not have",
					ctrlcli.ObjectKeyFromObject(pod), container, card.ID, group.ID,
				)
			}
			if _, duplicate := seen[card.ID]; duplicate {
				return fmt.Errorf("pod %s container %q: names accelerator %q in group %q twice",
					ctrlcli.ObjectKeyFromObject(pod), container, card.ID, group.ID)
			}
			seen[card.ID] = struct{}{}
			if card.Index != cardSpec.Index {
				return fmt.Errorf(
					"pod %s container %q: accelerator %q in group %q records index %d, the inventory says %d",
					ctrlcli.ObjectKeyFromObject(pod), container, card.ID, group.ID, card.Index, cardSpec.Index,
				)
			}
			if err := validateCardModeAndUnits(pod, container, card); err != nil {
				return err
			}
			if err := validateCardPlacement(pod, container, card); err != nil {
				return err
			}
		}
	}

	return nil
}

// validateCardModeAndUnits 拒绝本 operator 不会写入的分配模式，以及超出合法记录范围的单位数。
//
// 只有分配器写入的四种模式可以出现在注解中。None 和 Visibility 均会被拒绝：None 表示“未分配”，
// 属于已发布状态而非占用声明；Visibility 是不产生账本计费的内部授权。单位数需要设上限而非仅
// 校验非负，因为容器可以合法申请共享卡的一部分份额，上限校验能防止过大的数值被误读为整卡容量。
func validateCardModeAndUnits(
	pod *core.Pod, container string, card *workercore.AcceleratorAllocation,
) error {
	switch card.Mode {
	case workercore.DeviceAllocationModeExclusive,
		workercore.DeviceAllocationModeShared,
		workercore.DeviceAllocationModeSliced,
		workercore.DeviceAllocationModePartitioned:
	default:
		return fmt.Errorf("pod %s container %q: accelerator %q records mode %q, which is not one this operator writes",
			ctrlcli.ObjectKeyFromObject(pod), container, card.ID, card.Mode.String())
	}

	if card.Allocated < 0 || card.Remaining < 0 {
		return fmt.Errorf("pod %s container %q: accelerator %q records negative units (allocated %d, remaining %d)",
			ctrlcli.ObjectKeyFromObject(pod), container, card.ID, card.Allocated, card.Remaining)
	}
	if limit := int32(nodefeature.ResourceMaxUnits); card.Allocated > limit {
		return fmt.Errorf(
			"pod %s container %q: accelerator %q records %d allocated units, more than the %d a card holds",
			ctrlcli.ObjectKeyFromObject(pod), container, card.ID, card.Allocated, limit,
		)
	}
	// 共享切片的单位上限等于单卡总单位数除以最大共享持有者数量。SharedResourceMaxSize 表示一张
	// 共享卡允许的持有者上限，而非单个持有者可占用的单位数；若直接拿它作为单位上限，会错误拒绝
	// 真实加速卡上所有合法的共享声明。
	if card.Mode == workercore.DeviceAllocationModeShared {
		if limit := int32(nodefeature.ResourceMaxUnits) / int32(nodefeature.SharedResourceMaxSize); card.Allocated > limit {
			return fmt.Errorf(
				"pod %s container %q: shared accelerator %q records %d units, more than the %d a shared slice holds",
				ctrlcli.ObjectKeyFromObject(pod), container, card.ID, card.Allocated, limit,
			)
		}
	}

	return nil
}

// validateCardPlacement 拒绝只写入了一半的物理分区记录。
//
// 物理分区记录由分区规格（profile）与实例占用的位置区间（placements）共同构成，缺一不可。
// profile 指明是哪种切片，placements 指明其所处位置，因此只带其中一项的记录无法参与折叠，
// 绝不能假装合法地折叠进账本。placement 仅作传输数据而非发布输出：此处按输入校验，但不会与
// 已发布的账本行直接比对。
func validateCardPlacement(
	pod *core.Pod, container string, card *workercore.AcceleratorAllocation,
) error {
	key := ctrlcli.ObjectKeyFromObject(pod)
	switch {
	case card.AllocatedPhysicalProfile == "" && len(card.AllocatedPhysicalPlacements) == 0:
		return nil
	case card.AllocatedPhysicalProfile == "":
		return fmt.Errorf("pod %s container %q: accelerator %q records placement intervals but no profile",
			key, container, card.ID)
	case len(card.AllocatedPhysicalPlacements) == 0:
		return fmt.Errorf("pod %s container %q: accelerator %q records profile %q but no placement intervals",
			key, container, card.ID, card.AllocatedPhysicalProfile)
	default:
		return nil
	}
}

func findDeviceGroup(devs *workercore.Devices, id string) *workercore.DevicesGroup {
	for i := range devs.Spec.Groups {
		if devs.Spec.Groups[i].ID == id {
			return &devs.Spec.Groups[i]
		}
	}

	return nil
}

func findDeviceCard(group *workercore.DevicesGroup, id string) *workercore.Accelerator {
	for i := range group.Accelerators {
		if group.Accelerators[i].ID == id {
			return &group.Accelerators[i]
		}
	}

	return nil
}
