package amd

import (
	"fmt"
	"sync"

	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/binding"
	"gpustack.ai/gpustack/binding/hsa"
)

// 同一把锁保护 HSA 句柄与各物理卡的拓扑缓存。
//
// PlaceLogicalSliced 在设备插件的节点级分配锁内调用 readTopology。每次做 dlopen、遍历 agent 并进行
// 九次 cgo 查询，会连带阻塞本节点其他厂商的 Allocate。驱动运行期间 CU、着色器引擎与 XCC 的数量稳定，
// 因此每张卡只需成功查询一次。
//
// 仅缓存初始化成功的句柄。若记住一次瞬态失败，之后的软切分都要等到 Device Manager 重启才能恢复。
var (
	hsaMutex      sync.Mutex
	hsaLib        *hsa.HSA
	topologyCache = map[string]Topology{}
)

// readTopology 根据 PCI 总线地址和设备 UUID 读取 HSA 所报告的单卡拓扑。
//
// 先按 PCI BDF 匹配 agent，缺少这个键时再按 UUID 查找，这与 AMD 探测器的匹配顺序一致。找不到卡时应
// 明确报出其身份。返回全零拓扑会把问题误诊为设备没有计算单元。
//
// binding/hsa 将读取失败的数值字段保留为零，交给 Topology.Validate 判断该卡是否能够生成有效掩码。
func readTopology(pciBusID, cardUUID string) (Topology, error) {
	hsaMutex.Lock()
	defer hsaMutex.Unlock()

	if topo, cached := topologyCache[cardUUID]; cached {
		return topo, nil
	}

	if hsaLib == nil {
		// 向 binding 传入日志器，使 dlopen 或 dlsym 失败时留下诊断信息。
		lib := hsa.New(binding.WithLogger(klog.Background().WithName("amd").WithName("cumask")))
		if ret := lib.Init(); !ret.IsSuccess() {
			return Topology{}, fmt.Errorf("hsa init failed: %w", ret)
		}
		hsaLib = lib
	}

	agents := hsaLib.GetAgents()
	agent, ok := agents[pciBusID]
	if !ok {
		agent, ok = agents[cardUUID]
	}
	if !ok {
		return Topology{}, fmt.Errorf("no hsa agent reports card %s (pci bus id %q)", cardUUID, pciBusID)
	}

	topo := Topology{
		Name:    agent.Name,
		CU:      int(agent.ComputeUnitCount),
		SE:      int(agent.NumShaderEngines),
		SAPerSE: int(agent.NumShaderArraysPerSE),
		XCC:     int(agent.NumXcc),
	}
	topologyCache[cardUUID] = topo
	return topo, nil
}
