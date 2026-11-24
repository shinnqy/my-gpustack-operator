package nodefeature

import (
	"strconv"
	"testing"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/device"
)

func linkState(state workercore.DeviceInterfaceLinkState) *workercore.DeviceInterfaceLink {
	return &workercore.DeviceInterfaceLink{State: state}
}

func rdmaNIC(
	name, bus, root, numa string, switches []string, link *workercore.DeviceInterfaceLink,
) device.Interface {
	return device.Interface{
		Name: name, Bus: "pci", PciBusID: bus, PciRootID: root, PciSwitches: switches,
		NumaAffinity: numa, RDMA: true, RDMADevice: "mlx5_0", Link: link,
	}
}

// plainNIC 构造一个挂在 PCI 总线上、没有绑定 RDMA 设备的网卡。
func plainNIC(name, bus, root, numa string, switches []string) device.Interface {
	return device.Interface{
		Name: name, Bus: "pci", PciBusID: bus, PciRootID: root, PciSwitches: switches,
		NumaAffinity: numa,
	}
}

// vfRDMA 构造已绑定 RDMA 设备的 VF，携带自身的链路状态和 NUMA 亲和信息。
// 它与 PF 共享 PCI 桥路径，NUMA 读数则可以不同。
func vfRDMA(
	bus, numa string, link *workercore.DeviceInterfaceLink,
) workercore.DeviceInterfaceVirtualFunction {
	return workercore.DeviceInterfaceVirtualFunction{
		PciBusID: bus, NumaAffinity: numa, RDMA: true, RDMADevice: "mlx5_1", Link: link,
	}
}

func withVFs(
	iface device.Interface, vfs ...workercore.DeviceInterfaceVirtualFunction,
) device.Interface {
	iface.SRIOV = true
	iface.VirtualFunctions = vfs
	return iface
}

func acceleratorAt(bus, root, numa string, switches []string) device.DevicesGroupList {
	return device.DevicesGroupList{{
		Manufacturer: ManufacturerNVIDIA,
		Accelerators: []device.Accelerator{{
			ID: "GPU-0",
			Topology: device.Topology{
				PciBusID: bus, PciRootID: root, PciSwitches: switches, NumaAffinity: numa,
			},
		}},
	}}
}

// TestConstructRDMANodeLabels 检查能力、NUMA 和最近距离三种标签。
// 明确失败和暂时无法核实的链路对能力标签有不同影响。
func TestConstructRDMANodeLabels(t *testing.T) {
	testCases := []struct {
		name       string
		groups     device.DevicesGroupList
		interfaces []device.Interface
		want       map[string]string
	}{
		{
			name:   "a verified link one bridge from the accelerator",
			groups: acceleratorAt("0000:01:00.0", "0000:00:01.0", "0", []string{"0000:00:01.0"}),
			interfaces: []device.Interface{
				rdmaNIC("eth0", "0000:02:00.0", "0000:00:01.0", "0", []string{"0000:00:01.0"},
					linkState(workercore.DeviceInterfaceLinkStateOK)),
			},
			want: map[string]string{
				NodeRDMACapableLabelKey:  "true",
				NodeRDMANumaLabelKey:     "0",
				NodeRDMADistanceLabelKey: "PIX",
			},
		},
		{
			// 本例只把链路改成失败，其余硬件条件与上一例相同。
			name:   "a failed link withholds every key",
			groups: acceleratorAt("0000:01:00.0", "0000:00:01.0", "0", []string{"0000:00:01.0"}),
			interfaces: []device.Interface{
				rdmaNIC("eth0", "0000:02:00.0", "0000:00:01.0", "0", []string{"0000:00:01.0"},
					linkState(workercore.DeviceInterfaceLinkStateFailed)),
			},
			want: map[string]string{},
		},
		{
			// 设备已绑定而链路暂时无法核实时，仍可保留能力标签。
			name:   "an unverified link on a bound device still emits the keys",
			groups: acceleratorAt("0000:01:00.0", "0000:00:01.0", "0", []string{"0000:00:01.0"}),
			interfaces: []device.Interface{
				rdmaNIC("eth0", "0000:02:00.0", "0000:00:01.0", "0", []string{"0000:00:01.0"},
					linkState(workercore.DeviceInterfaceLinkStateUnverified)),
			},
			want: map[string]string{
				NodeRDMACapableLabelKey:  "true",
				NodeRDMANumaLabelKey:     "0",
				NodeRDMADistanceLabelKey: "PIX",
			},
		},
		{
			// RDMA 目录存在却无法读取时，探测器记录 unverified，绑定标志保持为假。
			// 这个组合能检验链路结论是否优先于绑定标志。
			name:   "an unverified verdict with no bound device still emits the keys",
			groups: acceleratorAt("0000:01:00.0", "0000:00:01.0", "0", []string{"0000:00:01.0"}),
			interfaces: []device.Interface{
				func() device.Interface {
					iface := plainNIC("eth0", "0000:02:00.0", "0000:00:01.0", "0",
						[]string{"0000:00:01.0"})
					iface.Link = linkState(workercore.DeviceInterfaceLinkStateUnverified)
					return iface
				}(),
			},
			want: map[string]string{
				NodeRDMACapableLabelKey:  "true",
				NodeRDMANumaLabelKey:     "0",
				NodeRDMADistanceLabelKey: "PIX",
			},
		},
		{
			// VF 的 RDMA 目录同样可能存在却读不到，嵌套在 PF 下的这条记录也要参与判断。
			name:   "an unverified verdict on a virtual function still emits the keys",
			groups: acceleratorAt("0000:01:00.0", "0000:00:01.0", "0", []string{"0000:00:01.0"}),
			interfaces: []device.Interface{
				withVFs(
					plainNIC("eth0", "0000:02:00.0", "0000:00:01.0", "0", []string{"0000:00:01.0"}),
					workercore.DeviceInterfaceVirtualFunction{
						PciBusID: "0000:02:00.1",
						Link:     linkState(workercore.DeviceInterfaceLinkStateUnverified),
					},
				),
			},
			want: map[string]string{
				NodeRDMACapableLabelKey:  "true",
				NodeRDMANumaLabelKey:     "0",
				NodeRDMADistanceLabelKey: "PIX",
			},
		},
		{
			// 普通网卡既没有绑定 RDMA 设备，也没有链路判定，不应贡献 RDMA 标签。
			name:   "a plain NIC with no verdict emits nothing",
			groups: acceleratorAt("0000:01:00.0", "0000:00:01.0", "0", []string{"0000:00:01.0"}),
			interfaces: []device.Interface{
				plainNIC("eth0", "0000:02:00.0", "0000:00:01.0", "0", []string{"0000:00:01.0"}),
			},
			want: map[string]string{},
		},
		{
			// 一张坏卡旁还有正常网卡时仍能发布能力标签，最近距离和 NUMA 只用尚可用的端点计算。
			name:   "a broken link beside a working one describes the working one",
			groups: acceleratorAt("0000:01:00.0", "0000:00:01.0", "0", []string{"0000:00:01.0"}),
			interfaces: []device.Interface{
				rdmaNIC("eth0", "0000:02:00.0", "0000:00:01.0", "0", []string{"0000:00:01.0"},
					linkState(workercore.DeviceInterfaceLinkStateFailed)),
				rdmaNIC("eth1", "0000:81:00.0", "0000:80:01.0", "1", []string{"0000:80:01.0"},
					linkState(workercore.DeviceInterfaceLinkStateOK)),
			},
			want: map[string]string{
				NodeRDMACapableLabelKey:  "true",
				NodeRDMANumaLabelKey:     "1",
				NodeRDMADistanceLabelKey: "SYS",
			},
		},
		{
			// SR-IOV 可以只有 VF 能提供 RDMA。只遍历顶层的 PF 会遗漏这样的端点。
			name:   "a virtual function's RDMA makes the node capable",
			groups: acceleratorAt("0000:01:00.0", "0000:00:01.0", "0", []string{"0000:00:01.0"}),
			interfaces: []device.Interface{
				withVFs(
					plainNIC("eth0", "0000:02:00.0", "0000:00:01.0", "0", []string{"0000:00:01.0"}),
					vfRDMA("0000:02:00.1", "", linkState(workercore.DeviceInterfaceLinkStateOK)),
				),
			},
			want: map[string]string{
				NodeRDMACapableLabelKey:  "true",
				NodeRDMANumaLabelKey:     "0",
				NodeRDMADistanceLabelKey: "PIX",
			},
		},
		{
			// 如果唯一支持 RDMA 的 VF 明确报告链路失败，就没有其他端点可供此节点使用。
			name:   "a virtual function whose link failed leaves the node with nothing",
			groups: acceleratorAt("0000:01:00.0", "0000:00:01.0", "0", []string{"0000:00:01.0"}),
			interfaces: []device.Interface{
				withVFs(
					plainNIC("eth0", "0000:02:00.0", "0000:00:01.0", "0", []string{"0000:00:01.0"}),
					vfRDMA("0000:02:00.1", "", linkState(workercore.DeviceInterfaceLinkStateFailed)),
				),
			},
			want: map[string]string{},
		},
		{
			// VF 能上报自己的 NUMA 位置，标签应使用这个读数。PCI 桥仍与 PF 相同。
			name:   "a virtual function publishes its own NUMA node, not its parent's",
			groups: acceleratorAt("0000:01:00.0", "0000:00:01.0", "0", []string{"0000:00:01.0"}),
			interfaces: []device.Interface{
				withVFs(
					plainNIC("eth0", "0000:02:00.0", "0000:00:01.0", "0", []string{"0000:00:01.0"}),
					vfRDMA("0000:02:00.1", "1", linkState(workercore.DeviceInterfaceLinkStateOK)),
				),
			},
			want: map[string]string{
				NodeRDMACapableLabelKey:  "true",
				NodeRDMANumaLabelKey:     "1",
				NodeRDMADistanceLabelKey: "PIX",
			},
		},
		{
			// VF 没有 NUMA 读数时，沿用父接口提供的亲和信息。
			name:   "a virtual function with no NUMA affinity keeps its parent's",
			groups: acceleratorAt("0000:01:00.0", "0000:00:01.0", "0", []string{"0000:00:01.0"}),
			interfaces: []device.Interface{
				withVFs(
					plainNIC("eth0", "0000:02:00.0", "0000:00:01.0", "0", []string{"0000:00:01.0"}),
					vfRDMA("0000:02:00.1", "", linkState(workercore.DeviceInterfaceLinkStateOK)),
				),
			},
			want: map[string]string{
				NodeRDMACapableLabelKey:  "true",
				NodeRDMANumaLabelKey:     "0",
				NodeRDMADistanceLabelKey: "PIX",
			},
		},
		{
			name:   "RDMA on both sockets carries both NUMA nodes, ordered",
			groups: acceleratorAt("0000:01:00.0", "0000:00:01.0", "0", []string{"0000:00:01.0"}),
			interfaces: []device.Interface{
				rdmaNIC("eth1", "0000:81:00.0", "0000:80:01.0", "1", []string{"0000:80:01.0"},
					linkState(workercore.DeviceInterfaceLinkStateOK)),
				rdmaNIC("eth0", "0000:02:00.0", "0000:00:01.0", "0", []string{"0000:00:01.0"},
					linkState(workercore.DeviceInterfaceLinkStateOK)),
			},
			want: map[string]string{
				NodeRDMACapableLabelKey:  "true",
				NodeRDMANumaLabelKey:     "0_1",
				NodeRDMADistanceLabelKey: "PIX",
			},
		},
		{
			// 节点上只有 RDMA 接口，没有与它配对的加速器，因此不会生成距离标签。
			name:   "RDMA with no accelerator reports no distance",
			groups: nil,
			interfaces: []device.Interface{
				rdmaNIC("eth0", "0000:02:00.0", "0000:00:01.0", "0", []string{"0000:00:01.0"},
					linkState(workercore.DeviceInterfaceLinkStateOK)),
			},
			want: map[string]string{
				NodeRDMACapableLabelKey: "true",
				NodeRDMANumaLabelKey:    "0",
			},
		},
		{
			// 内核没有给接口提供 NUMA 亲和信息时，不能把它公布成节点 0。
			name:   "an interface with no NUMA affinity contributes no NUMA value",
			groups: nil,
			interfaces: []device.Interface{
				rdmaNIC("eth0", "0000:02:00.0", "0000:00:01.0", "", []string{"0000:00:01.0"},
					linkState(workercore.DeviceInterfaceLinkStateOK)),
			},
			want: map[string]string{NodeRDMACapableLabelKey: "true"},
		},
		{
			name:       "no RDMA anywhere emits nothing",
			groups:     acceleratorAt("0000:01:00.0", "0000:00:01.0", "0", []string{"0000:00:01.0"}),
			interfaces: []device.Interface{{Name: "eth0", Bus: "pci"}},
			want:       map[string]string{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := ConstructRDMANodeLabels(tc.groups, tc.interfaces)

			if len(got) != len(tc.want) {
				t.Fatalf("labels = %v, want %v", got, tc.want)
			}
			for k, v := range tc.want {
				if got[k] != v {
					t.Errorf("label %s = %q, want %q", k, got[k], v)
				}
			}
		})
	}
}

func TestConstructRDMANodeLabelsOmitsOversizedNUMASet(t *testing.T) {
	var interfaces []device.Interface
	for i := range 25 {
		numa := strconv.Itoa(i)
		interfaces = append(interfaces, device.Interface{RDMA: true, RDMADevice: "mlx5_" + numa, NumaAffinity: numa})
	}
	// 编号 0 到 23 连同分隔符共 61 字节。再加一个编号后超过 63 字节，整项省略，其他标签仍照常发布。
	before := ConstructRDMANodeLabels(nil, interfaces[:24])
	if len(before[NodeRDMANumaLabelKey]) != 61 {
		t.Fatalf("未超过长度上限的 NUMA 集合应完整发布，得到 %v", before)
	}
	after := ConstructRDMANodeLabels(nil, interfaces)
	if len(after) != 1 || after[NodeRDMACapableLabelKey] != "true" {
		t.Fatalf("NUMA 集合过长时只应保留 RDMA 能力标签，得到 %v", after)
	}
}
