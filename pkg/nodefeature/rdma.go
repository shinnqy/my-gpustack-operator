package nodefeature

import (
	"sort"
	"strings"

	core "k8s.io/api/core/v1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/kubemeta"
)

const (
	// RDMAFeatureLabelPrefix 是这组标签的共同前缀。
	// 探测器用它识别本轮已消失、需要从 NodeFeature 撤下的旧标签。
	RDMAFeatureLabelPrefix = FeatureLabelPrefix + "rdma."

	// NodeRDMACapableLabelKey 表示至少一个端点的链路没有被判为故障。
	// 工作负载可用自己的节点选择器或亲和性要求它。每个端点的绑定和链路状态保存在 Devices 中。
	NodeRDMACapableLabelKey = RDMAFeatureLabelPrefix + "capable"

	// NodeRDMADistanceLabelKey 保存节点上任意加速器与可用 RDMA 端点之间最短的总线距离。
	// 它供观察使用，具体容器拿到的设备由 kubelet 分配结果决定。
	NodeRDMADistanceLabelKey = RDMAFeatureLabelPrefix + "distance"

	// NodeRDMANumaLabelKey 保存可用 RDMA 端点所在的 NUMA 节点集合。用下划线分隔各值，
	// 例如 0_1。逗号会被标签清理器去掉，直接拼接会让两个节点看起来像同一个编号。
	NodeRDMANumaLabelKey = RDMAFeatureLabelPrefix + "numa"

	// rdmaNumaSeparator 用合法的标签字符分隔 NUMA 节点编号。
	rdmaNumaSeparator = "_"
)

// rdmaResourceNameBase 是 RDMA 资源键的基础：即运营商自己的设备前缀，该前缀下所有该运营商所发布的节点级资
// 源都归属于此，此外还包含品牌名称。它不包含制造商信息，因为网络接口属于节点，而非供应商。
const rdmaResourceNameBase = VisibilityResourceNamePrefix + "rdma"

// RDMAEndpointPoolSize 表示共享家族中一个 RDMA 端点向 kubelet 上报的并发持有者数量。
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

// ConstructRDMANodeLabels 根据一次节点探测生成 RDMA 能力、NUMA 和距离标签。
// 有明确链路状态时优先使用它，failed 的端点不参与聚合，unverified 仍可贡献能力标签。
// 没有链路记录时再看 RDMA 绑定标志。
//
// 只要有一个可用端点，节点便可保留能力标签。端点的具体差异留在 Devices 中。全部端点都不再可用时返回空映射，
// 由上报器撤下旧的 RDMA 标签。
func ConstructRDMANodeLabels(
	groups device.DevicesGroupList, interfaces []device.Interface,
) map[string]string {
	capable := rdmaEndpoints(interfaces)

	labels := map[string]string{}
	if len(capable) == 0 {
		return labels
	}
	labels[NodeRDMACapableLabelKey] = "true"

	// NUMA 标签必须完整表示整个集合。清理后的值若会改变，例如超过 63 个字符而被截断，就略过这个标签。
	// 逐端点的亲和信息仍保存在 Devices 中。
	if numa := rdmaNumaNodes(capable); numa != "" && kubemeta.SanitizeLabelValue(numa) == numa {
		labels[NodeRDMANumaLabelKey] = numa
	}
	// 距离描述的是一对设备。节点只有 RDMA 网卡却没有加速器时，只发布能从网卡本身得到的标签。
	if distance, ok := closestAcceleratorDistance(groups, capable); ok {
		labels[NodeRDMADistanceLabelKey] = distance.String()
	}

	for k := range labels {
		labels[k] = kubemeta.SanitizeLabelValue(labels[k])
	}
	return labels
}

// rdmaEndpoints 收集用于节点标签聚合的接口和虚拟功能。SR-IOV 的 VF 在库存中位于 PF 的条目下，
// 需逐一处理。VF 与 PF 处于同一 PCI 桥路径，NUMA 则优先用 VF 自己的读数，
// VF 未报告时沿用 PF 的值。
func rdmaEndpoints(interfaces []device.Interface) []device.Interface {
	endpoints := make([]device.Interface, 0, len(interfaces))
	for i := range interfaces {
		iface := interfaces[i]
		if rdmaUsable(iface.RDMA, iface.Link) {
			endpoints = append(endpoints, iface)
		}

		for j := range iface.VirtualFunctions {
			vf := &iface.VirtualFunctions[j]
			if !rdmaUsable(vf.RDMA, vf.Link) {
				continue
			}
			endpoint := iface
			// 这里已经得到一个独立端点，清空子列表可以避免随后再次遍历同一批 VF。
			endpoint.VirtualFunctions = nil
			if vf.NumaAffinity != "" {
				endpoint.NumaAffinity = vf.NumaAffinity
			}
			endpoints = append(endpoints, endpoint)
		}
	}

	return endpoints
}

// rdmaUsable 先看链路结论，只有明确失败才从节点标签聚合中排除。若探测器看到了 RDMA 目录却无法读取，
// unverified 仍能反映这一情况。完全没有链路记录时，才使用 RDMA 绑定标志。
func rdmaUsable(rdma bool, link *workercore.DeviceInterfaceLink) bool {
	if link != nil {
		return link.State != workercore.DeviceInterfaceLinkStateFailed
	}
	return rdma
}

// rdmaNumaNodes 收集非空 NUMA 节点编号，去重后按字符串排序并用下划线连接，
// 使同一批硬件在各轮探测中产生稳定的标签值。
func rdmaNumaNodes(capable []device.Interface) string {
	seen := make(map[string]bool, len(capable))
	nodes := make([]string, 0, len(capable))
	for i := range capable {
		numa := capable[i].NumaAffinity
		// 空值表示内核没有给出亲和信息，跳过它才能保留“未知”和实际编号 0 的区别。
		if numa == "" || seen[numa] {
			continue
		}
		seen[numa] = true
		nodes = append(nodes, numa)
	}
	sort.Strings(nodes)
	return strings.Join(nodes, rdmaNumaSeparator)
}

// closestAcceleratorDistance 遍历本节点所有加速器与可用端点的组合，
// 返回最短总线距离及是否存在可比较的设备对。这个结果描述整台节点，不能用来推断某次分配选择了哪块网卡。
func closestAcceleratorDistance(
	groups device.DevicesGroupList, capable []device.Interface,
) (device.TopologyDistance, bool) {
	closest, found := device.TopologyDistanceUnknown, false
	for gi := range groups {
		for ai := range groups[gi].Accelerators {
			topology := groups[gi].Accelerators[ai].Topology
			for ii := range capable {
				found = true
				if d := device.BusDistance(topology, capable[ii]); d < closest {
					closest = d
				}
			}
		}
	}
	return closest, found
}
