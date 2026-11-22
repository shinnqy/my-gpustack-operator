package nodefeature

import (
	core "k8s.io/api/core/v1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/device"
)

const (
	// RDMAFeatureLabelPrefix 用于作为该文件所写入的每一个标签的前缀，也是使该集合可以移除的关键：如果
	// 某个遍历过程不再报告这些键中的任何一个，就需要删除过时的键。而使用这个前缀是识别哪些键属于我们
	// 的唯一方式。
	RDMAFeatureLabelPrefix = FeatureLabelPrefix + "rdma."
)

// rdmaResourceNameBase 是 RDMA 资源键的基础：即运营商自己的设备前缀，该前缀下所有该运营商所发布的节点级资
// 源都归属于此，此外还包含品牌名称。它不包含制造商信息，因为网络接口属于节点，而非供应商。
const rdmaResourceNameBase = VisibilityResourceNamePrefix + "rdma"

// RDMAEndpointPoolSize 表示共享家族中一个 RDMA 端点所 advertised 的并发持有者数量。
//
// 该图来自被替代的生态系统，而非在此处编造：操作员通常会安装的共享 RDMA 设备插件在厂商自己的部署指南中设
// 定为 63 作为默认配置，因此基于该插件编写的工作负载在此处没有更严格的限制。64 是指下一个 2 的幂次数值，
// 也是该软件包其余部分的尺寸遵循的形态。
//
// 它不是：
//
//   - 这不是硬件限制。HCA 所拥有的队列对数量远多于此，同时多个进程同时使用一个 HCA 是常见的 RDMA 使用方式，
//     其隔离功能由固件和内核实现，而非由 kubelet 所拥有的令牌数量决定。参考默认值也是一种配置选择，因此与
//     之匹配是兼容性问题，而非物理限制。
//   - 不是配额。这些密钥是节点级资源，配额链从未将其归类为加速器系列，因此下游的任何系统都不会将此数字用
//     于容量或准入决策。
//
// 提升它没有任何成本，但允许更多容器运行在同一个端点上，而这正是端点的作用。避免从机器的形状来推导它是最
// 关键的：通过某个节点中的卡片数量得出的数值在下一个节点上就不适用了，而且会无声地失败，端点在更密集的机
// 器中部分时间后便无法被调度。
const RDMAEndpointPoolSize = 64

// GetRDMAResourceName 返回在给定分配模式下所服务的节点级 RDMA 资源键。共有三种： "device.gpustack.ai/rdma
// "、 "device.gpustack.ai/rdma.shared" 和 "device.gpustack.ai/rdma.partitioned"。
//
// 分片名称没有密钥，这种缺失正是有意为之。它曾经返回一个".sliced"密钥，其定义与共享密钥完全一致，使用相同
// 的 HCAs，且两者不会互相减少。一个密钥对应一个资源池，并非两个扩展资源对调度器而言的意义：一个端点随后在
// 每个名称下接纳了完整的持有者集合，因此那个看似设定的上限实际上并非上限。它所实现的对称性——让工作负载以
// 其加速器请求的相同后缀来声明其 RDMA 请求——并没有换来一个真实有效的数值。
//
// 可见性名称均为空，原因不同：可见性分配会为同一 Pod 中另一个容器所持有的端点命名，而不会写入任何 RDMA 分
// 配记录来响应该命名，因此该模式被视为未服务状态，而非部分服务状态。无论是“无”还是其他无法识别的模式，都
// 会返回空字符串，因为未知模式不应静默地命名整个功能键。
func GetRDMAResourceName(mode workercore.DeviceAllocationMode) core.ResourceName {
	switch mode {
	case workercore.DeviceAllocationModeExclusive:
		return rdmaResourceNameBase
	case workercore.DeviceAllocationModeShared:
		return rdmaResourceNameBase + SharedResourceNameSuffix
	case workercore.DeviceAllocationModePartitioned:
		return rdmaResourceNameBase + PartitionedResourceNameSuffix
	default:
		return ""
	}
}

// ConstructRDMANodeLabels 根据一次检测过程的参数集构建 RDMA 功能标签。
//
// 当连接检查未显示连接中断时，某个端点被视为具备 RDMA 能力。只有在未达成任何判断结果时，才会退化为仅检查
// 设备是否绑定状态。因此，`failed` 是保留 NodeRDMACapableLabelKey 的状态，而 `unverified` 则会发出该状态—
// —包括探测器为无法读取的树状结构所记录的未验证记录，这些记录中不包含任何绑定设备。若保留该状态，则“此端
// 口的状态无法被读取”会变为“此节点没有 RDMA”。
//
// 这种聚合是存在性的：当至少有一个端点具备能力时，标签才会被发出，而不是所有端点都具备能力时。因此，一个
// 拥有损坏的网卡而旁边有正常网卡的节点，仍然可以保留该网卡，并仍能处理 RDMA 工作负载。哪个端点损坏了，会
// 在 `Devices` 中，按接口和虚拟功能进行区分。节点标签无法体现这一点。
//
// 对于没有此类接口的节点，它返回的是一个空映射，而非 nil。调用方移除过时的键正是导致这种缺失生效的原因——
// 标签在它们被写入的对象上是可叠加的，因此不再被报告的键仍然存在。
func ConstructRDMANodeLabels(
	groups device.DevicesGroupList, interfaces []device.Interface,
) map[string]string {
	// 第 14 章补上此函数的实现。
	return nil
}
