package nodefeature

import (
	"fmt"
	"strconv"
	"strings"

	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/systemname"
	"gpustack.ai/gpustack/pkg/utils/mapx"
	"gpustack.ai/gpustack/pkg/utils/quantityx"
	"gpustack.ai/gpustack/pkg/utils/strconvx"
	"gpustack.ai/gpustack/pkg/utils/stringx"
)

const (
	// FeatureLabelPrefix 是节点特征标签和注解使用的键前缀。
	FeatureLabelPrefix = "feature." + systemname.LabelPrefix

	// CreditsLabelPrefix 是记录调度配额单位的标签和注解使用的键前缀。
	CreditsLabelPrefix = "credits." + systemname.LabelPrefix
)

const (
	// AcceleratableFeatureLabelPrefix 是加速器特征标签的前缀，例如
	// acceleratable.feature.gpustack.ai/nvidia-tesla-t4.product。
	AcceleratableFeatureLabelPrefix = "acceleratable." + FeatureLabelPrefix

	// NodeAcceleratableLabelKey 概括节点上是否有加速器。有卡时设置
	// feature.gpustack.ai/acceleratable=true，并另外发布各设备组的标签。
	NodeAcceleratableLabelKey = FeatureLabelPrefix + "acceleratable"
	// NodeCPUOnlyLabelKey 让纯 CPU 规格选中尚未探测到加速器的节点。
	NodeCPUOnlyLabelKey = FeatureLabelPrefix + "cpu-only"
)

// ConstructAcceleratableNodeLabels 将设备组清单转换为节点加速器标签。
func ConstructAcceleratableNodeLabels(groups device.DevicesGroupList) map[string]string {
	labels := map[string]string{}
	for i := range groups {
		applyAcceleratorLabels(labels, groups[i])
	}
	return labels
}

// applyAcceleratorLabels 把一组同型号加速器的特征写入标签集合。
func applyAcceleratorLabels(labels map[string]string, group device.DevicesGroup) {
	if len(group.Accelerators) == 0 {
		return
	}

	// 节点具备加速能力，设置 "${prefix}acceleratable=true"。
	labels[NodeAcceleratableLabelKey] = "true"

	manuKey := AcceleratableFeatureLabelPrefix + group.Manufacturer

	// 记录厂商是否存在，设置 "acceleratable.${prefix}${manufacturer}=true"。
	labels[manuKey] = "true"
	// 记录驱动版本 "acceleratable.${prefix}${manufacturer}.driver-version=${driverVersion}"。
	if v := group.DriverVersion; v != "" {
		labels[manuKey+".driver-version"] = v
	}
	// 记录运行时版本 "acceleratable.${prefix}${manufacturer}.runtime-version=${runtimeVersion}"。
	if v := group.RuntimeVersion; v != "" {
		labels[manuKey+".runtime-version"] = v
	}

	nodeKey := manuKey + "-" + group.ID

	// 标记本节点具有这一设备组，设置 "acceleratable.${prefix}${manufacturer}-${id}=true"。
	labels[nodeKey] = "true"
	// 记录产品名称 "acceleratable.${prefix}${manufacturer}-${id}.product=${name}"。
	labels[nodeKey+".product"] = group.Name
	// 记录单卡显存 "acceleratable.${prefix}${manufacturer}-${id}.memory=${memory}"。
	labels[nodeKey+".memory"] = quantityx.Format(resource.MustParse(strconvx.Itoa(group.Memory) + "Mi"))
	// 记录单卡核心数 "acceleratable.${prefix}${manufacturer}-${id}.cores=${cores}"。
	labels[nodeKey+".cores"] = strconvx.Itoa(group.Cores)
	// 记录本组卡数 "acceleratable.${prefix}${manufacturer}-${id}.count=${accelerator}"。
	labels[nodeKey+".count"] = strconvx.Itoa(len(group.Accelerators))
	// 记录设备系列 "acceleratable.${prefix}${manufacturer}-${id}.family=${family}"。
	if v := group.Family; v != "" {
		labels[nodeKey+".family"] = v
	}
	// 记录计算能力 "acceleratable.${prefix}${manufacturer}-${id}.comcap=${computeCapability}"。
	if v := group.ComputeCapability; v != "" {
		labels[nodeKey+".comcap"] = v
	}

	// 将各属性值转换成 Kubernetes 标签允许的形式。
	for k := range labels {
		labels[k] = kubemeta.SanitizeLabelValue(labels[k])
	}
}

// ExtractAcceleratableNodeKeys 返回节点上的加速器特征键，每项的格式为“厂商-设备标识”。
func ExtractAcceleratableNodeKeys(node *core.Node) []string {
	return ExtractAcceleratableKeys(node.Labels)
}

// ExtractAcceleratableKeys 从标签中提取形如 ${manufacturer}-${id} 的加速器组标识。输入既可以是 Node 的标
// 签，也可以是 ResourceFlavor.spec.nodeLabels。
//
// 设备组标识的标签值为 true，并且键中包含已知的厂商。设备组 ID 自身允许包含小数点，所以不能用“键里有点”来
// 判断它是否只是 .count、.product 或 .memory 等属性。
func ExtractAcceleratableKeys(labels map[string]string) []string {
	return mapx.FilterSlice(labels, func(k, v string) (string, bool) {
		if strings.HasPrefix(k, AcceleratableFeatureLabelPrefix) {
			if v == "true" {
				v = strings.TrimPrefix(k, AcceleratableFeatureLabelPrefix)
				manufacturer, _, found := strings.Cut(v, "-")
				if found && IsKnownAcceleratableManufacturer(manufacturer) {
					return v, true
				}
			}
		}
		return "", false
	})
}

const (
	// GeneralFeatureLabelPrefix 是 CPU 特征标签的键前缀，例如
	// general.feature.gpustack.ai/amd-25-1.cpu。
	GeneralFeatureLabelPrefix = "general." + FeatureLabelPrefix

	// GeneralGroupGeneric 表示不指定 CPU 厂商的资源池。选择器省略 general.* 条件后，可以合并操作系统
	// 和架构相同的不同 CPU。
	GeneralGroupGeneric = GeneralManufacturerGeneric
)

// PoolScheduleLabels 生成资源池的选择标签，同时写到 InstanceType 和 ClusterQueue。它总会记录是否带加速器，
// 按需添加加速器组。启用了 CPU 厂商区分并且分组明确时，也写入 CPU 组。
//
// 已知操作系统或架构时，也加入相应标签。
func PoolScheduleLabels(acceleratable, cpuAware bool, generalGroup, acceleratorGroup, os, arch string) map[string]string {
	lbs := map[string]string{
		NodeAcceleratableLabelKey: strconv.FormatBool(acceleratable),
	}
	if os != "" {
		lbs[core.LabelOSStable] = os
	}
	if arch != "" {
		lbs[core.LabelArchStable] = arch
	}
	if acceleratable && acceleratorGroup != "" {
		lbs[AcceleratableFeatureLabelPrefix+acceleratorGroup] = "true"
	}
	if cpuAware && generalGroup != "" && generalGroup != GeneralGroupGeneric {
		lbs[GeneralFeatureLabelPrefix+generalGroup] = "true"
	}
	return lbs
}

// PoolFlavorSelector 从池的标签中取出用于反查规格的条件，包括加速器标志、设备或 CPU 分组、操作系统和架构。
// 没有足够条件时返回 nil，避免选中所有对象。通用 CPU 池也保留加速器标志，以区分具有相同 CPU 的带卡节点。
func PoolFlavorSelector(labels map[string]string) map[string]string {
	lbs := make(map[string]string, 4)
	hasDiscriminator := false
	for k, v := range labels {
		switch {
		case k == core.LabelOSStable, k == core.LabelArchStable:
			lbs[k] = v
		case k == NodeAcceleratableLabelKey:
			lbs[k] = v
			hasDiscriminator = true
		case v == "true" &&
			(strings.HasPrefix(k, GeneralFeatureLabelPrefix) ||
				strings.HasPrefix(k, AcceleratableFeatureLabelPrefix)):
			lbs[k] = v
			hasDiscriminator = true
		}
	}
	if !hasDiscriminator {
		return nil
	}
	return lbs
}

const (
	_NFDCPUModelVendorIDLabelKey = "feature.node.kubernetes.io/cpu-model.vendor_id"
	_NFDCPUModelFamilyLabelKey   = "feature.node.kubernetes.io/cpu-model.family"
	_NFDCPUModelIDLabelKey       = "feature.node.kubernetes.io/cpu-model.id"
)

// ExtractGeneralNodeKey 从 Node 的信息计算 CPU 分组键，即使硬件信息不全也会返回非空的键。
//
// 操作系统和架构已写入 ResourceFlavor、ClusterQueue 的名称和节点选择标签。即使 x86 与 arm64 的 CPU 型号恰
// 好使用相同的编号，它们也会进入不同的池。
//
// CPU 键以小写厂商名开头，厂商未知时使用 generic。优先拼接清洗后的 cpu-name，没有名称时用 NFD 的 family
// 和 id，都缺失时只保留厂商名。
func ExtractGeneralNodeKey(node *core.Node) string {
	// 从 NFD 的 cpu-model.vendor_id 标签读取 CPU 厂商。
	manu := extractGeneralNodeKeyManufacturer(node)

	// 优先采用 NFD NodeFeatureRule 在 feature.gpustack.ai/cpu-name 中报告的 CPU 名称。清洗之后再拼入
	// 键中，便于区分 CPU 型号。
	var idPrefix string
	if name := generalFeatureAnnotation(node, "name"); name != "" {
		// 为 .count 和 .capacity 等后缀预留长度，使组合后的标签名称仍满足 Kubernetes 的 63 字符限
		// 制。
		budget := 53 - len(manu)
		idPrefix = device.NormalizeName(name, manu, budget, true)
	} else {
		family := node.Labels[_NFDCPUModelFamilyLabelKey]
		modelID := node.Labels[_NFDCPUModelIDLabelKey]
		if family != "" && modelID != "" {
			idPrefix = family + "-" + modelID
		}
	}

	// 只有厂商信息时，键为 "${manufacturer}"。
	if idPrefix == "" {
		return manu
	}

	// 已知型号时，键为 "${manufacturer}-${idPrefix}"。
	return manu + "-" + idPrefix
}

// extractGeneralNodeKeyManufacturer 读取 NFD 的 CPU 厂商标识并转换成小写。缺少厂商信息时返回 generic。
func extractGeneralNodeKeyManufacturer(node *core.Node) string {
	// feature.node.kubernetes.io/cpu-model.vendor_id 由 NFD 上报。
	// https://github.com/klauspost/cpuid,
	// 成功采集 CPU 信息时，这个值应包含厂商名。
	manu := strings.ToLower(node.Labels[_NFDCPUModelVendorIDLabelKey])
	if manu == "vendorunknown" || manu == "" {
		manu = GeneralManufacturerGeneric
	}
	return manu
}

// CPUDetail 保存 NFD NodeFeatureRule 写入节点 feature.gpustack.ai/cpu-* 注解的原始 CPU 信息。
// ResourceFlavor 的 cpuDetail 记录保留这些值。启用 CPU 厂商感知后，
// InstanceType webhook 据此填写规格。
type CPUDetail struct {
	Manufacturer           string
	Product                string
	Family                 string
	PhysicalCores          string
	ThreadsPerPhysicalCore string
	LogicalCores           string
	Stepping               string
	ClockSpeed             string
	MaxClockSpeed          string
	CacheLine              string
	CacheL1I               string
	CacheL1D               string
	CacheL2                string
	CacheL3                string
}

// ExtractGeneralDetail 从节点的 feature.gpustack.ai/cpu-* 注解读取 CPU 信息。
// generalFeatureAnnotation 将缺失项和未展开的 @cpu.model.* 模板都处理为空字符串。
func ExtractGeneralDetail(node *core.Node) CPUDetail {
	return CPUDetail{
		Manufacturer:           extractGeneralNodeKeyManufacturer(node),
		Product:                generalFeatureAnnotation(node, "name"),
		Family:                 generalFeatureAnnotation(node, "family"),
		PhysicalCores:          generalFeatureAnnotation(node, "physical-cores"),
		ThreadsPerPhysicalCore: generalFeatureAnnotation(node, "threads-per-core"),
		LogicalCores:           generalFeatureAnnotation(node, "logical-cores"),
		Stepping:               generalFeatureAnnotation(node, "stepping"),
		ClockSpeed:             generalFeatureAnnotation(node, "hz"),
		MaxClockSpeed:          generalFeatureAnnotation(node, "boost-freq"),
		CacheLine:              generalFeatureAnnotation(node, "cache-line"),
		CacheL1I:               generalFeatureAnnotation(node, "cache-l1i"),
		CacheL1D:               generalFeatureAnnotation(node, "cache-l1d"),
		CacheL2:                generalFeatureAnnotation(node, "cache-l2"),
		CacheL3:                generalFeatureAnnotation(node, "cache-l3"),
	}
}

type (
	ConstructNodeCapacityLabelsOptions struct {
		// ManualNodeManagement 为 true 时，管理员通过节点标签决定是否纳管。
		ManualNodeManagement bool
	}

	ConstructNodeCapacityLabelsOption func(*ConstructNodeCapacityLabelsOptions)
)

// WithManualNodeManagement 把当前的 node-management-manual 配置传给标签构造函数。控制器在每次协调时重新读
// 取这个配置。
func WithManualNodeManagement(manual bool) ConstructNodeCapacityLabelsOption {
	return func(opts *ConstructNodeCapacityLabelsOptions) {
		opts.ManualNodeManagement = manual
	}
}

// ConstructNodeCapacityLabels 根据 Node 容量和已有标签生成管理状态及 CPU 分组标签。
func ConstructNodeCapacityLabels(node *core.Node, opt ...ConstructNodeCapacityLabelsOption) map[string]string {
	var opts ConstructNodeCapacityLabelsOptions
	for i := range opt {
		opt[i](&opts)
	}

	labels := map[string]string{}
	// 自动模式默认加上 managed=true。管理员明确设置的值总是优先。手动模式下，管理员可以用这个标签决定
	// 是否纳管节点。
	if !opts.ManualNodeManagement {
		labels[systemname.ManagedLabelKey] = "true"
	}
	if node.Labels != nil && node.Labels[systemname.ManagedLabelKey] != "" {
		labels[systemname.ManagedLabelKey] = node.Labels[systemname.ManagedLabelKey]
	}
	if node.Labels[NodeAcceleratableLabelKey] != "true" {
		labels[NodeCPUOnlyLabelKey] = "true"
	}

	// 写出 CPU 厂商键和型号键，供 NodeFlavorReconciler 把同类 CPU 节点归入同一组。每单位的 CPU、内存
	// 和存储规格由控制器读取 Node 的 status.capacity 后写入 ResourceFlavor 的说明。
	gKey := ExtractGeneralNodeKey(node)
	gManu, _, _ := strings.Cut(gKey, "-")
	// 标记 CPU 厂商，设置 "general.${prefix}${manufacturer}=true"。
	labels[GeneralFeatureLabelPrefix+gManu] = "true"
	// 标记 CPU 型号组，设置 "general.${prefix}${manufacturer}-${id}=true"。
	labels[GeneralFeatureLabelPrefix+gKey] = "true"
	// "general.${prefix}${manufacturer}-${id}.count=${count}" 中的 count 是向上取整后的 CPU 核数。
	// ExtractNodeFlavors 用它确定规格大小，节点选择器也用它保证选中的节点核数一致。
	cpuQ := node.Status.Capacity[core.ResourceCPU]
	if count := cpuQ.Value(); count > 0 {
		labels[GeneralFeatureLabelPrefix+gKey+".count"] = strconvx.Itoa(count)
	}

	return labels
}

// NodeFlavor 表示一个节点可以贡献给 Kueue 的一种规格。每个受管节点有按 CPU 键和核数区分的 CPU 规格，每个
// 加速器组另有按型号和数量区分的设备规格。
//
// NodeFlavorReconciler 把相同名称的规格聚到一起，总容量为节点数乘以 Count。每单位资源由控制器根据
// Manufacturer、Product 和 PresetUnitResources 确定，创建 InstanceType 时使用。
type NodeFlavor struct {
	// Name 是 ResourceFlavor 的名称。CPU 规格采用 gpustack--${gKey}-${os}-${arch}-${count}c，设备规格
	// 采用 gpustack--${gKey}--${aKey}-${os}-${arch}-${count}d。
	Name string
	// GeneralKey 是 CPU 分组键，始终有值，对应 InstanceType 的 GeneralGroup。
	GeneralKey string
	// AcceleratorKey 是设备规格的加速器组，对应 InstanceType 的 AcceleratorGroup。纯 CPU 规格的这个字
	// 段为空。
	AcceleratorKey string
	// OS 和 Arch 分别来自节点的 kubernetes.io/os 和 kubernetes.io/arch，完整值如 linux 和 amd64 也会
	// 进入规格名和调度标签。
	OS   string
	Arch string
	// Count 对 CPU 规格表示该节点的 CPU 核数，对设备规格表示该组的卡数。
	Count int64
	// Acceleratable 指明这个规格是否包含加速器。
	Acceleratable bool
	// Manufacturer 对设备规格表示设备厂商，对 CPU 规格表示 CPU 厂商。
	Manufacturer string
	// Product 和 Family 描述设备产品和系列。CPU 规格只有在区分 CPU 名称时才填入这两项，因为同一
	// generic 组内可能有多种 CPU。
	Product string
	Family  string
	// Memory 是设备规格中每张卡的显存，例如 24576Mi。CPU 规格留空。Webhook 可以从 ResourceFlavor 的说
	// 明中读取它，换算请求的显存配额。
	Memory string
	// Cores 是设备规格中每张卡的核心数，例如 9216，写入 ResourceFlavor 的说明。CPU 规格留空。
	Cores string
	// NodeLabels 匹配资源池的节点，包括管理标记、操作系统、架构、设备或 CPU 组及 .count。因此预留了某
	// 个规格的任务只会落到容量与该规格一致的节点。
	NodeLabels map[string]string
}

// OwnKey 返回规模计量所用的特征键。加速器 flavor 用加速器键，CPU flavor 用通用 CPU 键，
// .count 和 .capacity 都挂在该键下。
func (f NodeFlavor) OwnKey() string {
	if f.Acceleratable {
		return f.AcceleratorKey
	}
	return f.GeneralKey
}

// DerivedInstanceTypeIdentity 根据 CPU 厂商感知设置计算派生规格的名称和分组。关闭时，
// CPU 池合并为 gpustack--generic-<os>-<arch>，
// 加速器池按加速器键 aKey 合并为 gpustack--<aKey>-<os>-<arch>。开启后，两类池都按 CPU 键 gKey 拆分，
// 名称分别为 gpustack--<gKey>-<os>-<arch> 和 gpustack--<gKey>--<aKey>-<os>-<arch>。
func (f NodeFlavor) DerivedInstanceTypeIdentity(aware bool) (name, generalGroup, acceleratorGroup string) {
	generalGroup = GeneralGroupGeneric
	if aware {
		generalGroup = f.GeneralKey
	}
	if f.Acceleratable {
		acceleratorGroup = f.AcceleratorKey
		if aware {
			name = fmt.Sprintf("gpustack--%s--%s-%s-%s", f.GeneralKey, f.AcceleratorKey, f.OS, f.Arch)
		} else {
			name = fmt.Sprintf("gpustack--%s-%s-%s", f.AcceleratorKey, f.OS, f.Arch)
		}
		return name, generalGroup, acceleratorGroup
	}
	if aware {
		name = fmt.Sprintf("gpustack--%s-%s-%s", f.GeneralKey, f.OS, f.Arch)
	} else {
		name = fmt.Sprintf("gpustack--%s-%s-%s", GeneralGroupGeneric, f.OS, f.Arch)
	}
	return name, generalGroup, acceleratorGroup
}

// ExtractNodeFlavors 根据节点特征标签和容量构造 CPU 规格，并为每个加速器组构造相应的设备规格。
func ExtractNodeFlavors(node *core.Node) (flavors []NodeFlavor) {
	if node.Labels == nil {
		return nil
	}

	os := node.Labels[core.LabelOSStable]
	arch := node.Labels[core.LabelArchStable]

	// CPU flavor 用通用 CPU 键标识节点，以单节点核数区分规格。
	// ConstructNodeCapacityLabels 已把向上取整的 CPU 容量写入 .count，
	// 名称和节点选择器从同一标签取值。
	gKey := ExtractGeneralNodeKey(node)
	gNodeKey := GeneralFeatureLabelPrefix + gKey
	gCountKey := gNodeKey + ".count"
	if cpuCount, err := strconvx.Atoi[int64](node.Labels[gCountKey]); err == nil && cpuCount > 0 {
		manufacturer, _, _ := strings.Cut(gKey, "-")
		nodeLabels := map[string]string{
			systemname.ManagedLabelKey: "true",
			core.LabelOSStable:         os,
			core.LabelArchStable:       arch,
			gNodeKey:                   "true",
			// 同一 flavor 只匹配核数相同的节点，保证预留的资源规格一致。
			gCountKey: node.Labels[gCountKey],
		}

		flavors = append(flavors, NodeFlavor{
			Name:         formatNodeFlavorName(gKey, "", os, arch, cpuCount, false),
			GeneralKey:   gKey,
			OS:           os,
			Arch:         arch,
			Count:        cpuCount,
			Manufacturer: manufacturer,
			// 通用键反映实际 CPU 型号，因此记录对应的产品和系列。
			Product:    generalFeatureAnnotation(node, "name"),
			Family:     generalFeatureAnnotation(node, "family"),
			NodeLabels: nodeLabels,
		})
	}

	// 每种加速器特征键产生一个设备 flavor，以该型号上报的单节点设备数区分规格。
	for _, aKey := range ExtractAcceleratableNodeKeys(node) {
		aNodeKey := AcceleratableFeatureLabelPrefix + aKey
		aCountKey := aNodeKey + ".count"
		count, err := strconvx.Atoi[int64](node.Labels[aCountKey])
		if err != nil || count <= 0 {
			continue
		}
		manufacturer, _, _ := strings.Cut(aKey, "-")
		nodeLabels := map[string]string{
			systemname.ManagedLabelKey: "true",
			core.LabelOSStable:         os,
			core.LabelArchStable:       arch,
			// 选择器要求节点同时具备配对的 CPU 键。是否按 CPU 拆分资源池，由上层汇总时决定。
			gNodeKey:  "true",
			aNodeKey:  "true",
			aCountKey: node.Labels[aCountKey],
		}

		flavors = append(flavors, NodeFlavor{
			Name:           formatNodeFlavorName(gKey, aKey, os, arch, count, true),
			GeneralKey:     gKey,
			AcceleratorKey: aKey,
			OS:             os,
			Arch:           arch,
			Count:          count,
			Acceleratable:  true,
			Manufacturer:   manufacturer,
			Product:        node.Labels[aNodeKey+".product"],
			Family:         node.Labels[aNodeKey+".family"],
			Memory:         node.Labels[aNodeKey+".memory"],
			Cores:          node.Labels[aNodeKey+".cores"],
			NodeLabels:     nodeLabels,
		})
	}

	return flavors
}

// formatNodeFlavorName 将 CPU 键 gKey 写入 ResourceFlavor 名。
// 设备 flavor 还会在双连字符后写入加速器键 aKey，然后依次写入完整的系统名、架构和单节点数量。
// CPU 核数以 c 结尾，设备数以 d 结尾。前缀 gpustack-- 及键间的双连字符可与键内部的单连字符区分。
func formatNodeFlavorName(gKey, aKey, os, arch string, count int64, acceleratable bool) string {
	if acceleratable {
		return fmt.Sprintf("gpustack--%s--%s-%s-%s-%dd", gKey, aKey, os, arch, count)
	}
	return fmt.Sprintf("gpustack--%s-%s-%s-%dc", gKey, os, arch, count)
}

// generalFeatureAnnotation 读取 NFD NodeFeatureRule 记录的 feature.gpustack.ai/cpu-${name} 注解，各类 CPU
// 属性共用这一个读取入口。
func generalFeatureAnnotation(node *core.Node, name string) string {
	v := node.Annotations[FeatureLabelPrefix+"cpu-"+name]
	if strings.HasPrefix(v, "@") {
		return ""
	}
	return v
}

// FormatLocalQueueName 根据 ClusterQueue 名称生成 LocalQueue 名称。
//
// ClusterQueue 名称可能超过 kueue.x-k8s.io/queue-name 标签值允许的 63 个字符，因此对其计算 FNV-64a 哈希，
// 用于生成 LocalQueue 名称。
//
// 名称格式为 "gpustack-fnv64-${fnv64a-hex}"。
//
// 生成后的长度固定为 31 个字符。
func FormatLocalQueueName(clusterQueueName string) string {
	return LocalQueueNamePrefix + stringx.SumByFNV64a(clusterQueueName)
}

// LocalQueueNamePrefix 是 Operator 创建 LocalQueue 时使用的名称前缀。
const LocalQueueNamePrefix = "gpustack-fnv64-"

// GetAcceleratableCreditsResourceName 返回指定厂商在 Kueue 中计量加速器配额的资源名。
func GetAcceleratableCreditsResourceName(manufacturer string) core.ResourceName {
	return core.ResourceName(CreditsLabelPrefix + manufacturer)
}

// IsAcceleratableCreditsResourceName 判断资源名是否属于某个厂商的加速器配额。Kueue 将请求换算成这种单位后，
// 在 ClusterQueue 中统一计量。
func IsAcceleratableCreditsResourceName(name core.ResourceName) bool {
	manufacturer, ok := strings.CutPrefix(string(name), CreditsLabelPrefix)
	return ok && IsKnownAcceleratableManufacturer(manufacturer)
}
