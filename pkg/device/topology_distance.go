package device

// TopologyDistance 表示两个设备的拓扑距离，名称和数值与 GPUStack 运行时使用的拓扑矩阵一致。
// 数值越小，设备越近。各级之间留有间隔，便于扩展时保持既有数值稳定。SELF 表示同一设备，UNK 表示当前信息不足。
type TopologyDistance int32

const (
	// TopologyDistanceSelf 表示设备与自身的距离。
	TopologyDistanceSelf TopologyDistance = 0
	// TopologyDistanceLink 表示通过厂商专用的高速互连直接相连。这样的设备也可能跨 NUMA 节点，
	// 单靠 PCI 总线坐标无法确认这种连接。
	TopologyDistanceLink TopologyDistance = 5
	// TopologyDistancePIX 表示两个设备之间至多经过一个 PCIe 桥。
	TopologyDistancePIX TopologyDistance = 10
	// TopologyDistancePXB 表示同一 PCIe 桥子树内经过多个桥，路径没有跨越主机桥。
	TopologyDistancePXB TopologyDistance = 20
	// TopologyDistancePHB 表示路径经过 PCIe 主机桥，通常经过 CPU。
	TopologyDistancePHB TopologyDistance = 30
	// TopologyDistanceNode 表示在同一 NUMA 节点内跨越 PCIe 主机桥之间的互连。
	// 它比跨 NUMA 节点更近。
	TopologyDistanceNode TopologyDistance = 40
	// TopologyDistanceSys 表示路径跨越 NUMA 节点之间的系统互连，例如 QPI 或 UPI。
	TopologyDistanceSys TopologyDistance = 50
	// TopologyDistanceUnknown 表示缺少作出判断所需的信息。它的数值大于所有已知距离，
	// 取最小值时会优先保留实际测得的等级。
	TopologyDistanceUnknown TopologyDistance = 100
)

// String 返回厂商拓扑矩阵使用的名称，供节点标签发布。未知值统一写为 UNK。
func (in TopologyDistance) String() string {
	switch in {
	case TopologyDistanceSelf:
		return "SELF"
	case TopologyDistanceLink:
		return "LINK"
	case TopologyDistancePIX:
		return "PIX"
	case TopologyDistancePXB:
		return "PXB"
	case TopologyDistancePHB:
		return "PHB"
	case TopologyDistanceNode:
		return "NODE"
	case TopologyDistanceSys:
		return "SYS"
	default:
		return "UNK"
	}
}

// BusDistance 用加速器和网卡上报的 PCI 与 NUMA 坐标判断总线距离。厂商专用互连需要独立的观测。
// 库存记录的 PCI 桥路径止于最外层桥，无法进一步区分 PHB 与 NODE 时按较远的 NODE 报告，
// 避免高估邻近程度。非 PCI 接口或关键坐标缺失时返回未知。
func BusDistance(accelerator Topology, iface Interface) TopologyDistance {
	if accelerator.PciBusID == "" || iface.PciBusID == "" ||
		accelerator.PciRootID == "" || iface.PciRootID == "" {
		return TopologyDistanceUnknown
	}

	// PciSwitches 从设备侧向外排列。两个设备的第一个桥相同，说明它们挂在同一个最内层 PCIe 桥后。
	if len(accelerator.PciSwitches) > 0 && len(iface.PciSwitches) > 0 &&
		accelerator.PciSwitches[0] == iface.PciSwitches[0] {
		return TopologyDistancePIX
	}

	// PciRootID 记录最外层 PCI 桥。它相同而最内层桥不同时，两条路径在同一桥子树内分开。
	if accelerator.PciRootID == iface.PciRootID {
		return TopologyDistancePXB
	}

	// 超出共同的桥子树后，剩下可比较的是 NUMA 节点。任一端都可能返回空亲和信息，不能把空值当成节点 0。
	if accelerator.NumaAffinity == "" || iface.NumaAffinity == "" {
		return TopologyDistanceUnknown
	}
	if accelerator.NumaAffinity == iface.NumaAffinity {
		return TopologyDistanceNode
	}
	return TopologyDistanceSys
}
