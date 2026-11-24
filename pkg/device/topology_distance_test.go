package device

import "testing"

// TestBusDistance 验证 PCI 和 NUMA 坐标能够支持的距离判断。总线距离不能据此产生专用互连等级。
// 坐标不足时必须返回未知，以免误报设备远近。
func TestBusDistance(t *testing.T) {
	accelerator := func(bus, root string, switches []string, numa string) Topology {
		return Topology{PciBusID: bus, PciRootID: root, PciSwitches: switches, NumaAffinity: numa}
	}
	iface := func(bus, root string, switches []string, numa string) Interface {
		return Interface{
			Name: "eth0", PciBusID: bus, PciRootID: root, PciSwitches: switches, NumaAffinity: numa,
		}
	}

	testCases := []struct {
		name string
		a    Topology
		i    Interface
		want TopologyDistance
	}{
		{
			name: "behind the same innermost bridge",
			a:    accelerator("0000:01:00.0", "0000:00:01.0", []string{"0000:00:01.0"}, "0"),
			i:    iface("0000:02:00.0", "0000:00:01.0", []string{"0000:00:01.0"}, "0"),
			want: TopologyDistancePIX,
		},
		{
			// 两条路径在最外层桥汇合，但在更靠近设备的位置分开，因此只共享 PciRootID。
			name: "inside one bridge subtree but not behind the same innermost bridge",
			a: accelerator("0000:03:00.0", "0000:00:02.0",
				[]string{"0000:01:01.0", "0000:00:02.0"}, "0"),
			i: iface("0000:04:00.0", "0000:00:02.0",
				[]string{"0000:01:02.0", "0000:00:02.0"}, "0"),
			want: TopologyDistancePXB,
		},
		{
			// 同一个 NUMA 节点上有不同 PCI 桥子树。当前坐标不能区分 PHB 与 NODE，采用较远的 NODE。
			name: "different bridge subtrees on the same NUMA node",
			a:    accelerator("0000:01:00.0", "0000:00:01.0", []string{"0000:00:01.0"}, "0"),
			i:    iface("0000:81:00.0", "0000:80:01.0", []string{"0000:80:01.0"}, "0"),
			want: TopologyDistanceNode,
		},
		{
			name: "different NUMA nodes",
			a:    accelerator("0000:01:00.0", "0000:00:01.0", []string{"0000:00:01.0"}, "0"),
			i:    iface("0000:81:00.0", "0000:80:01.0", []string{"0000:80:01.0"}, "1"),
			want: TopologyDistanceSys,
		},
		{
			// 这种网卡不挂在 PCI 总线上，缺少总线坐标时应当保留为未知。
			name: "an interface with no PCI coordinates",
			a:    accelerator("0000:01:00.0", "0000:00:01.0", []string{"0000:00:01.0"}, "0"),
			i:    iface("", "", nil, "0"),
			want: TopologyDistanceUnknown,
		},
		{
			name: "an accelerator with no PCI coordinates",
			a:    accelerator("", "", nil, "0"),
			i:    iface("0000:02:00.0", "0000:00:01.0", []string{"0000:00:01.0"}, "0"),
			want: TopologyDistanceUnknown,
		},
		{
			// 两条桥路径已分开，只剩 NUMA 可以比较。任一端未知时都不能推断它们在同一个 NUMA 节点。
			name: "different subtrees with an unknown NUMA node",
			a:    accelerator("0000:01:00.0", "0000:00:01.0", []string{"0000:00:01.0"}, ""),
			i:    iface("0000:81:00.0", "0000:80:01.0", []string{"0000:80:01.0"}, "0"),
			want: TopologyDistanceUnknown,
		},
		{
			// 两个设备都直接挂在根复合体上，路径中没有共享的桥，各自的地址就是各自的 PciRootID，因此要继续比较 NUMA。
			name: "no bridges at all",
			a:    accelerator("0000:00:1f.6", "0000:00:1f.6", nil, "0"),
			i:    iface("0000:00:19.0", "0000:00:19.0", nil, "0"),
			want: TopologyDistanceNode,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := BusDistance(tc.a, tc.i)
			if got != tc.want {
				t.Errorf("distance = %s (%d), want %s (%d)", got, got, tc.want, tc.want)
			}
			switch got {
			case TopologyDistanceLink:
				t.Error("the bus axis returned LINK, a level it cannot observe: an accelerator " +
					"interconnect is invisible in bus coordinates, and deriving it here would " +
					"rate two devices as peers for sitting near each other")
			case TopologyDistanceSelf:
				t.Error("the bus axis returned SELF for an accelerator and a NIC, which are " +
					"never the same device")
			case TopologyDistancePHB:
				t.Error("the bus axis returned PHB, which asserts a shared PCIe host bridge — " +
					"a component these coordinates do not carry")
			}
		})
	}
}
