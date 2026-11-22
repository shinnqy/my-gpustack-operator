package deviceplugin

import (
	deviceplugin "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/binding"
	"gpustack.ai/gpustack/pkg/utils/slicex"
)

// rdmaEndpoint 是网络接口的一个可分配量 RDMA 端点，其属性包括从库存记录中读取的广告信息和分配信息。
//
// 它是推导出来的，而非存储的：每次调用时都会从 Devices.spec.interfaces[] 中读取所有字段，因此检测器已修正
// 的事实无法保留在基于旧版本的响应中。
type rdmaEndpoint struct {
	// Resource 是用于定位端点的名称，由 endpointOf 构建。
	Resource Resource

	// RDMADevice 是绑定后的 RDMA 设备名称，其分配结果会转化为一个字符设备。从构造上来看，它是不空的：
	// 没有名称的端点没有可交付的内容，根本不会被作为端点返回。
	RDMADevice string

	// Topology 是端点的 NUMA 提示信息，由 numaTopologyOf 构建。当既无端点也无其父节点报告亲和性时，该
	// 值为空。
	Topology *deviceplugin.TopologyInfo

	// Link 是端点链接判定结果，由广告健康门进行读取。若未记录到判定结果，则记为“Nil”。
	Link *workercore.DeviceInterfaceLink
}

// endpointOf 返回了名为 RDMA 的资源的端点：其物理功能以及该端点本身。对于支持全功能模式的接口来说，两者是
// 相等的。而对于虚拟功能则有所不同，正是这种差异使得通过单一密钥即可同时访问两者。
//
// 这是一个定位器，而非记录器：响应所需的一切内容——RDMA 设备名称、NUMA 亲和性、连接状态——都会在需要时从
// Devices.spec.interfaces[] 中读取出来，因此一个令牌永远无法携带被检测器修正过的过时信息。
func endpointOf(iface *workercore.DeviceInterface, endpoint string) Resource {
	return Resource{Group: iface.Name, Device: endpoint}
}

// rdmaEndpoints 返回接口在分配模式下所声明的 RDMA 端点，该模式是从接口记录的状态中确定的，而非用户选择。
//
// 具有配置虚拟函数的 SR-IOV 物理功能仅支持分区模式，每个虚拟函数对应一个端点：该节点在进程开始之前已处于
// 该分区状态，运行时无法对其进行任何更改，因此该接口也不支持全功能模式。其他接口——即没有配置虚拟函数的物
// 理功能，或根本没有物理功能——则支持独占和共享模式，同样对应一个端点。
//
// 作为物理函数，以及配置了虚拟函数的事实，在记录中是两个不同的要素。两者都需要被读取：仅根据虚拟函数数量
// 来决定分支，会导致包含无物理函数元素的记录被放入 Partitioned 中。而仅读取该事实，则会导致存在无虚拟函数
// 的物理函数，而后者毫无价值。
//
// 在任何模式下，没有 RDMA 设备名称的端点都不会被返回：名称是分配器用于指向字符设备的标识，因此没有该名称
// 的端点则没有可交付的内容。可见性、None 以及任何无法识别的模式同样不会返回任何内容——可见性分配会为同一
// Pod 下的另一个容器中的端点指定名称，且不会写入任何 RDMA 分配记录来回应该请求，而未知的模式则不得静默采
// 用全功能分支。
func rdmaEndpoints(iface *workercore.DeviceInterface, mode workercore.DeviceAllocationMode) []rdmaEndpoint {
	partitioned := iface.SRIOV && len(iface.VirtualFunctions) > 0

	switch mode {
	case workercore.DeviceAllocationModePartitioned:
		if !partitioned {
			return nil
		}
		endpoints := make([]rdmaEndpoint, 0, len(iface.VirtualFunctions))
		for i := range iface.VirtualFunctions {
			vf := &iface.VirtualFunctions[i]
			if vf.RDMADevice == "" {
				continue
			}
			endpoints = append(endpoints, rdmaEndpoint{
				Resource:   endpointOf(iface, vf.Name),
				RDMADevice: vf.RDMADevice,
				Topology:   numaTopologyOf(vf.NumaAffinity, iface.NumaAffinity),
				Link:       vf.Link,
			})
		}
		return endpoints

	case workercore.DeviceAllocationModeExclusive,
		workercore.DeviceAllocationModeShared:
		if partitioned || iface.RDMADevice == "" {
			return nil
		}
		return []rdmaEndpoint{{
			Resource:   endpointOf(iface, iface.Name),
			RDMADevice: iface.RDMADevice,
			Topology:   numaTopologyOf(iface.NumaAffinity, ""),
			Link:       iface.Link,
		}}
	}

	return nil
}

// numaTopologyOf 根据某个 RDMA 端点的自身亲和性，构建其 NUMA 提示信息。当该端点的自身亲和性为空时，则回退
// 到父节点的亲和性——此时“空白”表示内核拒绝响应，而非其他答案。此外，一个虚拟函数位于与其父节点相同的物理
// 函数上。
//
// 当没有响应时，它返回 nil：空的亲和性表示 -1，即虚拟化主机为其所有设备可以报告的数值，且没有指定节点名为
// node 0，而在单一 Numa 节点策略下，这一数值将依据未测量的邻近性来决定准入情况。
//
// 亲和力是通过与加速器路径相同的范围解析器来读取的，因此两者在亲和力字符串的含义上不会产生分歧。
func numaTopologyOf(own, parent string) *deviceplugin.TopologyInfo {
	affinity := own
	if affinity == "" {
		affinity = parent
	}

	var topology *deviceplugin.TopologyInfo
	if numa := binding.StrRangeToList(affinity); len(numa) > 0 {
		topology = &deviceplugin.TopologyInfo{
			Nodes: slicex.Transform(numa, func(n int) *deviceplugin.NUMANode {
				return &deviceplugin.NUMANode{
					ID: int64(n),
				}
			}),
		}
	}
	return topology
}
