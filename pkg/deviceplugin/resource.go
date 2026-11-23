package deviceplugin

import (
	"fmt"
	"strconv"
	"strings"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/utils/strconvx"
)

// 资源是指 Kubernetes 识别一个设备的方式：它被检测到的设备组，以及该组内的自身 ID。
//
//   - 这是 Device-plugin 和控制器相关的术语。Device Manager 负责管理 Devices。Kubernetes 将其作为资源进行
//     使用。关于完整的架构层次，请参考 pkg/device 包的相关文档。
//   - 它指的是某个设备，而非加速器。当前这个包所服务的每一个设备都是加速器，但分层结构将其他设备——比如
//     InfiniBand 端口、连接端口——也包含其中，这些设备将通过同一种机制接入 Kubernetes。若将范围限定在当前
//     使用的子类型上，就必须取消这种限制才能接纳它们。
//   - 此包中每个设备相关的账本都以其为键，因此必须保持为可比较的值类型：添加切片、映射或指针字段会使其作
//     为映射键变得无效，并在编译时破坏这些账本。
type Resource struct {
	// Group 是设备组的 ID。
	Group string
	// Device 是该组内设备的 ID。
	Device string
}

// String 将整卡渲染为 "<group>:<device>"。它指的是整卡，而非单个令牌——ResourceToken.String 会添加索引，以
// 便 kubelet 能够匹配到设备 ID。
func (in Resource) String() string {
	return in.Group + ":" + in.Device
}

// DeviceIDs 根据分配模式生成一张卡应公布的设备 ID。切片和分区使用该卡的 poolSize，非正时没有令牌；
// 其他模式按各自规则生成，未定义令牌的模式返回空集合。
func (in Resource) DeviceIDs(mode workercore.DeviceAllocationMode, poolSize int32) []string {
	str := in.String() + ":"

	switch mode {
	case workercore.DeviceAllocationModeExclusive:
		return []string{str + "0000"}

	case workercore.DeviceAllocationModeShared:
		// 每个共享持有者对应一个 ID，编号间隔为整卡单位数除以最大持有者数量。
		const step = uint64(nodefeature.ResourceMaxUnits / nodefeature.SharedResourceMaxSize)
		devIDs := make([]string, 0, nodefeature.SharedResourceMaxSize)
		for i := uint64(0); i < nodefeature.ResourceMaxUnits; i += step {
			devIDs = append(devIDs, str+padIndex(i))
		}
		return devIDs

	case workercore.DeviceAllocationModeVisibility:
		// 可见性模式按每卡最多可容纳的切片数公布可互换的令牌。实际设备从同一 Pod 主容器的预留中获取，
		// kubelet 选中了哪个可见性令牌都不影响结果，也不额外消耗账本额度。
		devIDs := make([]string, nodefeature.SlicedResourceMaxSize)
		for i := 0; i < nodefeature.SlicedResourceMaxSize; i++ {
			devIDs[i] = str + padIndex(uint64(i))
		}
		return devIDs

	case workercore.DeviceAllocationModeSliced, workercore.DeviceAllocationModePartitioned:
		// 逻辑切片和硬件分区都使用可互换的令牌，每个并发名额一个。逻辑切片令牌用于触发设备注入，
		// 数量要足以覆盖并发上限。实际资源预算另由 Pod webhook 写入的 .sliced.units 约束。
		if poolSize <= 0 {
			return nil
		}
		devIDs := make([]string, poolSize)
		for i := int32(0); i < poolSize; i++ {
			devIDs[i] = str + padIndex(uint64(i))
		}
		return devIDs
	}

	return nil
}

// padIndex 以固定的四位数字形式显示每个设备 ID 所携带的令牌索引。该宽度由 IDs DeviceIDs 广告中的名称以及
// ResourceToken.String 中的名称共享。kubelet 所匹配的字符串与之前提供的字符串完全一致——因此两者在这一点上
// 绝不应出现矛盾。
func padIndex(idx uint64) string {
	return strconvx.PadZeroUint(idx, 4)
}

// ResourceToken 唯一标识一张卡上的一个可分配令牌，由物理资源和该卡上的令牌编号组成。
// kubelet 设备 ID 可解析为它，也可由它还原。
type ResourceToken struct {
	Resource

	// Index 用来区分同一卡上的令牌。切片、分区和可见性模式中，它只是令牌池下标，没有硬件含义。共享模式中，
	// 它是整卡额度内的偏移，步长为 ResourceMaxUnits / SharedResourceMaxSize，即 160000。
	Index uint64
}

// String 将令牌编码为 <group>:<device>:<index>，下标补零方式与 DeviceIDs 公布的一致。缺少下标只表示物理卡，
// 既不能由 ParseResourceToken 解析，也无法与 kubelet 提供的令牌匹配。
func (in ResourceToken) String() string {
	return in.Resource.String() + ":" + padIndex(in.Index)
}

// ParseResourceToken 把 kubelet 的设备 ID 解析为 ResourceToken。
func ParseResourceToken(id string) (ResourceToken, error) {
	ps := strings.Split(id, ":")
	if len(ps) != 3 {
		return ResourceToken{}, fmt.Errorf("invalid device ID format: %q", id)
	}
	if ps[0] == "" || ps[1] == "" {
		return ResourceToken{}, fmt.Errorf("group/device cannot be empty: %q", id)
	}
	idx, err := strconv.ParseUint(ps[2], 10, 64)
	if err != nil {
		return ResourceToken{}, fmt.Errorf("invalid index: %q", ps[2])
	}
	return ResourceToken{
		Resource: Resource{
			Group:  ps[0],
			Device: ps[1],
		},
		Index: idx,
	}, nil
}
