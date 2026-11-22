package nodefeature

import (
	"regexp"
	"slices"
	"sort"
	"strings"

	"golang.org/x/exp/maps"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation"
	klog "k8s.io/klog/v2"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/utils/osx"
	"gpustack.ai/gpustack/pkg/utils/strconvx"
)

const (
	ManufacturerAMD       = "amd"
	ManufacturerAscend    = "ascend"
	ManufacturerCambricon = "cambricon"
	ManufacturerHygon     = "hygon"
	ManufacturerIluvatar  = "iluvatar"
	ManufacturerMetaX     = "metax"
	ManufacturerMThreads  = "mthreads"
	ManufacturerNVIDIA    = "nvidia"
	ManufacturerTHead     = "thead"
)

const (
	// GeneralManufacturerGeneric 是 NFD 缺少或无法识别 CPU 型号标签时的通用计算资源厂商。
	GeneralManufacturerGeneric = "generic"
)

const (
	// SharedResourceNameSuffix 是插件公布的共享份额资源键后缀。每卡可提供多个所有者名额，Kueue 据此折算额度。
	SharedResourceNameSuffix = ".shared"
	// SlicedResourceNameSuffix 是触发逻辑切片注入的粗粒度令牌后缀。节点令牌数由卡数和各卡切片上限决定，
	// 显存预算另行计量。
	SlicedResourceNameSuffix = ".sliced"
	// SlicedUnitsResourceNameSuffix 是按节点发布的软切 units 资源后缀，供 Kueue 统一计量。
	SlicedUnitsResourceNameSuffix = ".sliced.units"
	// SlicedCoresPercentageResourceNameSuffix 是软切算力百分比预算的后缀。
	// NodeCapacityReconciler 按设备组实际支持的超配能力发布容量。
	SlicedCoresPercentageResourceNameSuffix = ".sliced.cores-percentage"
	// SlicedMemoryPercentageResourceNameSuffix 是软切显存百分比预算后缀。每卡贡献 100，
	// webhook 将请求折算为调度使用的 units。
	SlicedMemoryPercentageResourceNameSuffix = ".sliced.memory-percentage"
	// SlicedMemoryMibResourceNameSuffix 是以 MiB 计的软切显存预算后缀。节点容量按卡数乘单卡显存计算。
	SlicedMemoryMibResourceNameSuffix = ".sliced.memory-mib"
	// PartitionedResourceNameSuffix 是插件对硬切模式卡发布的注入令牌后缀，
	// 实际分区的规格与额度在更细的资源键上登记。
	PartitionedResourceNameSuffix = ".partitioned"
	// PartitionedUnitsResourceNameSuffix 是分区的细粒度计量后缀，将不同规格的显存份额折算为 Kueue 额度。
	PartitionedUnitsResourceNameSuffix = ".partitioned.units"

	// VisibilityResourceNamePrefix 和 VisibilityResourceNameSuffix 构成 SSH sidecar 使用的可见性资源名，
	// 使它能看到工作容器已获分配的同一批卡。
	VisibilityResourceNamePrefix = "device.gpustack.ai/"
	VisibilityResourceNameSuffix = ".visibility"
)

const (
	// ResourceMaxUnits 是所有分配模式共用的整卡计量基准 D = 2^9 × 5^5 = 1,600,000。均分 N 份时每份为 D/N units，
	// 插件和 Devices 分配账本使用同一标尺。
	ResourceMaxUnits = 1_600_000
	// SharedResourceMaxSize 是每卡支持的最大共享所有者数量。
	SharedResourceMaxSize = 10
	// SlicedResourceMaxSize 是单卡逻辑切片数量的上限，取能整除 D 的二次幂。
	SlicedResourceMaxSize = 512
)

var (
	_ManufacturerPciVendorIDMap               map[string]string
	_ManufacturerAcceleratableResourceNameMap map[string]core.ResourceName
	_ManufacturerAcceleratableRuntimeNameMap  map[string]string
	_ManufacturerPartitionKindMap             map[string]string
	_AcceleratableResourceNameSet             sets.Set[core.ResourceName]
	_PartitionedProfileResourceNamePrefixMap  map[string]string
	_SlicedResourceSizes                      []int64
	_SlicedResourceOperatedSizesSet           sets.Set[string]
	_SlicedResourceUnitsPerSlice              map[int64]int64
)

// _AcceleratablePciClassPrefixes 列出加速器可能报告的 PCI 类别前缀，供 NFD 和控制器使用同一套分类。
var _AcceleratablePciClassPrefixes = []string{"02", "03", "0b", "12"}

func init() {
	// 建立厂商与 PCI 厂商编号的映射。
	_ManufacturerPciVendorIDMap = map[string]string{
		ManufacturerAMD:       "1002",
		ManufacturerAscend:    "19e5",
		ManufacturerCambricon: "cabc",
		ManufacturerHygon:     "1d94",
		ManufacturerIluvatar:  "1e3e",
		ManufacturerMetaX:     "9999",
		ManufacturerMThreads:  "1ed5",
		ManufacturerNVIDIA:    "10de",
		ManufacturerTHead:     "1ded",
	}
	for _, manufacturer := range maps.Keys(_ManufacturerPciVendorIDMap) {
		// 如果设置 GPUSTACK_<厂商>_PCI_VENDOR_ID，则覆盖默认 PCI 厂商编号。例如 AMD 使用 GPUSTACK_AMD_PCI_VENDOR_ID。
		if v := osx.Getenv("GPUSTACK_" + strings.ToUpper(manufacturer) + "_PCI_VENDOR_ID"); v != "" {
			_ManufacturerPciVendorIDMap[manufacturer] = v
		}
	}

	// 建立厂商与 Kubernetes 加速器资源名的映射，该名称也是相关资源键的基础。
	_ManufacturerAcceleratableResourceNameMap = map[string]core.ResourceName{
		ManufacturerAMD:       "amd.com/gpu",
		ManufacturerAscend:    "huawei.com/npu",
		ManufacturerCambricon: "cambricon.com/mlu",
		ManufacturerHygon:     "hygon.com/dcu",
		ManufacturerIluvatar:  "iluvatar.com/gpu",
		ManufacturerMetaX:     "metax-tech.com/gpu",
		ManufacturerMThreads:  "mthreads.com/gpu",
		ManufacturerNVIDIA:    "nvidia.com/gpu",
		ManufacturerTHead:     "alibabacloud.com/ppu",
	}
	for _, manufacturer := range maps.Keys(_ManufacturerAcceleratableResourceNameMap) {
		// 使用厂商对应的环境变量覆盖默认资源名，例如 GPUSTACK_AMD_ACCELERATABLE_RESOURCE_NAME。
		if v := osx.Getenv("GPUSTACK_" + strings.ToUpper(manufacturer) + "_ACCELERATABLE_RESOURCE_NAME"); v != "" {
			_ManufacturerAcceleratableResourceNameMap[manufacturer] = core.ResourceName(v)
		}
	}

	// 建立厂商与加速器容器运行时名称的映射，供 RuntimeClass 使用。
	_ManufacturerAcceleratableRuntimeNameMap = map[string]string{
		ManufacturerAMD:       "amd",
		ManufacturerAscend:    "ascend",
		ManufacturerCambricon: "cambricon",
		ManufacturerHygon:     "hygon",
		ManufacturerIluvatar:  "iluvatar",
		ManufacturerMetaX:     "metax",
		ManufacturerMThreads:  "mthreads",
		ManufacturerNVIDIA:    "nvidia",
	}
	for _, manufacturer := range maps.Keys(_ManufacturerAcceleratableResourceNameMap) {
		// 使用厂商对应的环境变量覆盖默认运行时名称，例如 GPUSTACK_AMD_ACCELERATABLE_RUNTIME_NAME。
		if v := osx.Getenv("GPUSTACK_" + strings.ToUpper(manufacturer) + "_ACCELERATABLE_RUNTIME_NAME"); v != "" {
			_ManufacturerAcceleratableRuntimeNameMap[manufacturer] = v
		}
	}

	// _ManufacturerPartitionKindMap 给出厂商的硬切技术名，供构造按规格计量的资源名，例如 NVIDIA 使用 mig。
	_ManufacturerPartitionKindMap = map[string]string{
		ManufacturerNVIDIA: "mig",
		// 平头哥管理库和 CLI 也使用 MIG 描述 PPU 分区，因此复用这一名称。
		ManufacturerTHead: "mig",
		// 海光管理库导出与 NVML 同名的多实例接口，规格的显示名称也使用 MIG 前缀，例如 MIG 2g.15gb。
		ManufacturerHygon: "mig",
	}
	for _, manufacturer := range maps.Keys(_ManufacturerPartitionKindMap) {
		// 用环境变量覆盖厂商的硬切技术名，例如 GPUSTACK_NVIDIA_PARTITION_KIND。
		if v := osx.Getenv("GPUSTACK_" + strings.ToUpper(manufacturer) + "_PARTITION_KIND"); v != "" {
			_ManufacturerPartitionKindMap[manufacturer] = v
		}
	}

	// 建立已知资源名集合，便于快速查找。
	_AcceleratableResourceNameSet = sets.New[core.ResourceName](maps.Values(_ManufacturerAcceleratableResourceNameMap)...)

	// 基于已配置的资源名称解析各厂商的硬切技术名。
	_PartitionedProfileResourceNamePrefixMap = make(map[string]string, len(_ManufacturerPartitionKindMap))
	for manufacturer, kind := range _ManufacturerPartitionKindMap {
		resName := _ManufacturerAcceleratableResourceNameMap[manufacturer]
		if resName == "" || kind == "" {
			continue
		}
		_PartitionedProfileResourceNamePrefixMap[manufacturer] = string(resName) + PartitionedResourceNameSuffix + "." + kind + "-"
	}

	// 列出逻辑切分支持的份数规格。
	for val := int64(1); val <= SlicedResourceMaxSize; val <<= 1 {
		_SlicedResourceSizes = append(_SlicedResourceSizes, val)
	}

	// 将切分份数转换成字符串集合以便快速匹配。
	_SlicedResourceOperatedSizesSet = sets.New[string]()
	for _, size := range _SlicedResourceSizes {
		if size < 2 {
			continue
		}
		_SlicedResourceOperatedSizesSet.Insert(strconvx.FormatInt(size, 10))
	}

	// 预计算每种切分份数对应的 D/size units。D 可被上限以内的各二次幂整除，因此结果是整数。
	_SlicedResourceUnitsPerSlice = make(map[int64]int64)
	for _, size := range _SlicedResourceSizes {
		_SlicedResourceUnitsPerSlice[size] = ResourceMaxUnits / size
	}
}

// GetPartitionKind 返回厂商自己的硬切技术名，供构造按规格发布的资源键。未声明硬切类型时返回空字符串。
func GetPartitionKind(manufacturer string) string {
	if _PartitionedProfileResourceNamePrefixMap[manufacturer] == "" {
		return ""
	}
	return _ManufacturerPartitionKindMap[manufacturer]
}

// GetPciVendorID 返回厂商在 PCI 总线上的标识。
func GetPciVendorID(manufacturer string) string {
	return _ManufacturerPciVendorIDMap[manufacturer]
}

// GetKnownAcceleratableManufacturers 返回已知加速器厂商列表。
func GetKnownAcceleratableManufacturers() []string {
	manus := maps.Keys(_ManufacturerAcceleratableResourceNameMap)
	sort.Strings(manus)
	return manus
}

// IsKnownAcceleratableManufacturer 判断名称是否属于已知加速器厂商。
func IsKnownAcceleratableManufacturer(manufacturer string) bool {
	return _ManufacturerAcceleratableResourceNameMap[manufacturer] != ""
}

// GetAcceleratablePciClassPrefixes 返回加速器相关 PCI 类别的前缀，例如显示控制器、处理加速器和协处理器。
// PCI 类别编号见 https://admin.pci-ids.ucw.cz/read/PD
func GetAcceleratablePciClassPrefixes() []string {
	return slices.Clone(_AcceleratablePciClassPrefixes)
}

// GetAcceleratableResourceName 根据厂商和分配模式返回插件公布的资源名。
// 整卡、共享、软切和硬切分别使用基础名、.shared、.sliced 和 .partitioned。可见性使用独立的 sidecar 资源名。
func GetAcceleratableResourceName(manufacturer string, mode workercore.DeviceAllocationMode) core.ResourceName {
	resName := _ManufacturerAcceleratableResourceNameMap[manufacturer]
	switch mode {
	default:
		return resName
	case workercore.DeviceAllocationModeShared:
		return resName + SharedResourceNameSuffix
	case workercore.DeviceAllocationModeSliced:
		return resName + SlicedResourceNameSuffix
	case workercore.DeviceAllocationModePartitioned:
		if GetPartitionKind(manufacturer) == "" {
			return ""
		}
		return resName + PartitionedResourceNameSuffix
	case workercore.DeviceAllocationModeVisibility:
		return core.ResourceName(VisibilityResourceNamePrefix + manufacturer + VisibilityResourceNameSuffix)
	}
}

// GetAcceleratableSlicedCoresPercentageResourceName 构造厂商的每卡算力百分比预算键，
// 例如 nvidia.com/gpu.sliced.cores-percentage。该键供节点预算检查使用。
func GetAcceleratableSlicedCoresPercentageResourceName(manufacturer string) core.ResourceName {
	return _ManufacturerAcceleratableResourceNameMap[manufacturer] + SlicedCoresPercentageResourceNameSuffix
}

// GetAcceleratableSlicedMemoryPercentageResourceName 返回给定制造商的每加速器 VRAM 比例预算键（例如 "
// nvidia.com/gpu.sliced.memory-percentage"）。这是 Gate-2 节点级资源。
func GetAcceleratableSlicedMemoryPercentageResourceName(manufacturer string) core.ResourceName {
	return _ManufacturerAcceleratableResourceNameMap[manufacturer] + SlicedMemoryPercentageResourceNameSuffix
}

// GetAcceleratableSlicedMemoryMibResourceName 返回给定制造商的每加速器绝对 VRAM 预算键，单位为单位字节（
// MiB）（例如 "nvidia.com/gpu.sliced.memory-mib"）。属于 Gate-2 节点级资源。
func GetAcceleratableSlicedMemoryMibResourceName(manufacturer string) core.ResourceName {
	return _ManufacturerAcceleratableResourceNameMap[manufacturer] + SlicedMemoryMibResourceNameSuffix
}

// GetAcceleratablePartitionedProfileResourceName 构造厂商的硬分区规格键。
// 例如 NVIDIA 的 3g.40gb 对应 nvidia.com/gpu.partitioned.mig-3g.40gb。
// 厂商未声明硬切或名称无法组成合法资源键时返回空值。
func GetAcceleratablePartitionedProfileResourceName(manufacturer, profile string) core.ResourceName {
	prefix := _PartitionedProfileResourceNamePrefixMap[manufacturer]
	if prefix == "" || profile == "" {
		return ""
	}
	name := prefix + PublishPartitionedProfileName(manufacturer, profile)
	if errs := validation.IsQualifiedName(name); len(errs) != 0 {
		// 避免把非法名称强行改写为合法键，否则无法反向定位真实硬件规格。
		klog.Warningf("excluding %s partition profile %q: it does not yield a valid resource name: %s",
			manufacturer, profile, strings.Join(errs, "; "))
		return ""
	}
	return core.ResourceName(name)
}

// _SeparatorlessPartitionedProfileManufacturerSet 列出不以点分隔几何数字的厂商。平头哥写作 MIG 4g48gb。
// 对外发布时统一为 4g.48gb，调用厂商库时保留其原写法。
var _SeparatorlessPartitionedProfileManufacturerSet = sets.New(ManufacturerTHead)

var (
	// _VendorPartitionedProfileGeometryRegex 匹配无点分隔的规格，并提取计算切片数与显存容量。
	_VendorPartitionedProfileGeometryRegex = regexp.MustCompile(`^([0-9]+)g([0-9]+)gb$`)
)

// PublishPartitionedProfileName 将厂商规格名转换为对外发布的格式。对省略分隔点的厂商把 4g48gb 改为 4g.48gb，
// 原本带点的名称保持不变。
func PublishPartitionedProfileName(manufacturer, profile string) string {
	if !_SeparatorlessPartitionedProfileManufacturerSet.Has(manufacturer) {
		return profile
	}
	m := _VendorPartitionedProfileGeometryRegex.FindStringSubmatch(profile)
	if m == nil {
		return profile
	}
	return m[1] + "g." + m[2] + "gb"
}

// PartitionedProfileOf 从已知厂商的 <基础名>.partitioned.<技术名>-<规格> 资源键中提取规格名。
// 它会校验基础资源名和技术名，.partitioned.units 属于额度键。
func PartitionedProfileOf(name core.ResourceName) (string, bool) {
	_, profile, ok := partitionedProfileOf(name)
	return profile, ok
}

// partitionedProfileOf 同时解析规格所属厂商和规格名，让各公开读取入口使用相同的校验规则。
func partitionedProfileOf(name core.ResourceName) (manufacturer, profile string, ok bool) {
	s := string(name)
	for m, prefix := range _PartitionedProfileResourceNamePrefixMap {
		p, cut := strings.CutPrefix(s, prefix)
		if cut && p != "" {
			return m, p, true
		}
	}
	return "", "", false
}

// IsKnownAcceleratableResourceName 判断名称是否属于 Operator 管理的四种加速器分配资源。
func IsKnownAcceleratableResourceName(name core.ResourceName) bool {
	switch ResourceFamilyOf(name) {
	case ResourceFamilyExclusive, ResourceFamilyShared, ResourceFamilySliced, ResourceFamilyPartitioned:
		return true
	}
	return false
}

// ResourceFamily 表示资源所属的加速器分配类别。同一类别包含粗粒度令牌、细粒度计量及可选的硬件规格键。
type ResourceFamily string

const (
	// ResourceFamilyNone 表示该资源不属于任何加速器类别，例如 cpu 或额度资源。
	ResourceFamilyNone ResourceFamily = "none"
	// ResourceFamilyExclusive 表示直接使用基础资源名的整卡模式。
	ResourceFamilyExclusive ResourceFamily = "exclusive"
	// ResourceFamilyShared 表示使用 <base>.shared 的共享模式。
	ResourceFamilyShared ResourceFamily = "shared"
	// ResourceFamilySliced 表示使用 <base>.sliced 及其子键的逻辑切分模式。
	ResourceFamilySliced ResourceFamily = "sliced"
	// ResourceFamilyPartitioned 表示使用 <base>.partitioned 及其子键的硬件分区模式。
	ResourceFamilyPartitioned ResourceFamily = "partitioned"
	// ResourceFamilyVisibility 是 SSH sidecar 复用同块设备的资源类别，
	// 资源名形如 device.gpustack.ai/<manufacturer>.visibility，独立于四种分配模式。
	ResourceFamilyVisibility ResourceFamily = "visibility"
)

// _ResourceFamilyFixedSuffixes 把固定后缀映射为资源类别。匹配时同时校验完整基础名，因此不依赖表中排列顺序。
var _ResourceFamilyFixedSuffixes = []struct {
	suffix string
	family ResourceFamily
}{
	{SlicedMemoryPercentageResourceNameSuffix, ResourceFamilySliced},
	{SlicedCoresPercentageResourceNameSuffix, ResourceFamilySliced},
	{SlicedMemoryMibResourceNameSuffix, ResourceFamilySliced},
	{PartitionedUnitsResourceNameSuffix, ResourceFamilyPartitioned},
	{SlicedUnitsResourceNameSuffix, ResourceFamilySliced},
	{PartitionedResourceNameSuffix, ResourceFamilyPartitioned},
	{SlicedResourceNameSuffix, ResourceFamilySliced},
	{SharedResourceNameSuffix, ResourceFamilyShared},
}

// ResourceFamilyOf 按完整的已知基础名和合法后缀对资源分类。Pod webhook 与逐卡准入用该结果检查分配模式，
// 名称不合法时返回 ResourceFamilyNone。
func ResourceFamilyOf(name core.ResourceName) ResourceFamily {
	s := string(name)

	if manufacturer, ok := strings.CutPrefix(s, VisibilityResourceNamePrefix); ok {
		manufacturer, ok = strings.CutSuffix(manufacturer, VisibilityResourceNameSuffix)
		if ok && IsKnownAcceleratableManufacturer(manufacturer) {
			return ResourceFamilyVisibility
		}
		return ResourceFamilyNone
	}

	// 名称末尾包含可变规格的资源键会单独校验基础名和规格后缀。
	if _, ok := PartitionedProfileOf(name); ok {
		return ResourceFamilyPartitioned
	}

	for _, e := range _ResourceFamilyFixedSuffixes {
		if base, ok := strings.CutSuffix(s, e.suffix); ok &&
			_AcceleratableResourceNameSet.Has(core.ResourceName(base)) {
			return e.family
		}
	}

	if _AcceleratableResourceNameSet.Has(name) {
		return ResourceFamilyExclusive
	}
	return ResourceFamilyNone
}

// GetAcceleratableRuntimeName 根据厂商返回加速器运行时名称，通常用作容器 RuntimeClass 名。
func GetAcceleratableRuntimeName(manufacturer string) string {
	return _ManufacturerAcceleratableRuntimeNameMap[manufacturer]
}

// NormalizePartitionedProfileName 去除规格名周围空白及厂商显示前缀，再转为小写几何描述，
// 例如将 MIG 1c.12g 转为 1c.12g。内部保留厂商自己的分隔写法，对外名称由 PublishPartitionedProfileName 转换。
func NormalizePartitionedProfileName(raw string) string {
	fields := strings.Fields(strings.ToLower(raw))
	if len(fields) == 0 {
		return ""
	}
	return fields[len(fields)-1]
}

const (
	// CreditsPerAccelerator 定义整张卡的整数额度 B，与总单位数 D（ResourceMaxUnits）相等。
	// 最小软切片为整卡的 1/SlicedResourceMaxSize，对应 B/SlicedResourceMaxSize，即 3125 个 credits。
	// .sliced.units 换算为 credits 的系数 B/D 恰好为 1。各种模式都能用整数表示额度，
	// 避免 Kueue 的 q.Value() 在处理非 CPU 资源时将不足 1 的额度向上取整为 1，造成切片借用量失真。
	CreditsPerAccelerator = ResourceMaxUnits
)

// AcceleratorsToCredits 用整卡数乘以 B 得到整数额度。
// ClusterQueue 的 NominalQuota 和换算后的工作负载请求据此采用相同单位。
func AcceleratorsToCredits(accelerators resource.Quantity) resource.Quantity {
	accelerators.Mul(CreditsPerAccelerator)
	return accelerators
}
