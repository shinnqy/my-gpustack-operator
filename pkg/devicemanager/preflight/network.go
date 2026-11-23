package preflight

import (
	"fmt"
	"time"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/devicemanager/detector"
)

type (
	// NetworkCheck 记录一个 RDMA 接口或 VF 的链路结论。沿用探测器的链路状态，供节点诊断使用。
	NetworkCheck struct {
		// Interface 是接口名。VF 使用父接口与 VF 总线地址共同标识。
		Interface string `json:"interface" yaml:"interface"`
		// RDMADevice 是内核绑定到该接口的 RDMA 设备名。
		RDMADevice string `json:"rdmaDevice" yaml:"rdmaDevice"`
		// State 是链路检查的结论。
		State workercore.DeviceInterfaceLinkState `json:"state" yaml:"state"`
		// Depth 为 declared，表示读取了节点接口清单，还没有执行跨节点通信测量。
		Depth device.PreflightDepth `json:"depth" yaml:"depth"`
		// Reason 保留链路检查器说明的原因，验证成功时为空。
		Reason string `json:"reason,omitempty" yaml:"reason,omitempty"`
	}

	// NetworkReport 汇总节点的 RDMA 链路信息。网络接口属于节点，可独立于加速器厂商报告。
	NetworkReport struct {
		// Timestamp 是读取链路状态的时间。
		Timestamp time.Time `json:"timestamp" yaml:"timestamp"`
		// Checks 按库存顺序列出 RDMA 接口及虚拟功能的检查结果。
		Checks []NetworkCheck `json:"checks,omitempty" yaml:"checks,omitempty"`
		// Note 说明枚举失败或清单中没有 RDMA 接口等情况，使空列表也有明确含义。
		Note string `json:"note,omitempty" yaml:"note,omitempty"`
	}
)

// PreflightNetwork 读取探测器的接口清单并报告 RDMA 链路状态，复用发现设备时的探测逻辑。
func PreflightNetwork() NetworkReport {
	interfaces, err := detector.DetectInterfaces()
	return networkReport(interfaces, err, time.Now())
}

// networkReport 将一次枚举结果及其错误转成报告，即使读取失败也能说明原因。
func networkReport(
	interfaces []workercore.DeviceInterface, err error, now time.Time,
) NetworkReport {
	report := NetworkReport{Timestamp: now}

	if err != nil {
		report.Note = "the network interfaces could not be enumerated, so nothing is claimed " +
			"about this node's RDMA links: " + err.Error()
		return report
	}

	virtualFunctions := 0
	for i := range interfaces {
		iface := &interfaces[i]
		if hasLinkToReport(iface.RDMA, iface.Link) {
			report.Checks = append(report.Checks,
				linkCheck(iface.Name, iface.RDMADevice, iface.Link))
		}

		// 同样遍历虚拟功能。SR-IOV 节点常通过 VF 向容器交付 RDMA，单看物理接口会遗漏链路状态。
		virtualFunctions += len(iface.VirtualFunctions)
		for j := range iface.VirtualFunctions {
			vf := &iface.VirtualFunctions[j]
			if !hasLinkToReport(vf.RDMA, vf.Link) {
				continue
			}
			report.Checks = append(report.Checks,
				linkCheck(vfInterfaceName(iface.Name, vf), vf.RDMADevice, vf.Link))
		}
	}

	if len(report.Checks) == 0 {
		report.Note = fmt.Sprintf(
			"none of the %d interfaces on this node, nor any of their %d virtual functions, has an "+
				"RDMA device bound, so there is no link to check; this is an answer about the "+
				"hardware, not a check that was skipped",
			len(interfaces), virtualFunctions,
		)
	}

	return report
}

// hasLinkToReport 筛出已绑定 RDMA 或已有链路诊断的条目，跳过已确认无关的普通接口。
func hasLinkToReport(rdma bool, link *workercore.DeviceInterfaceLink) bool {
	return rdma || link != nil
}

// linkCheck 把一条库存记录转成网络报告中的一行。
func linkCheck(
	name, rdmaDevice string, link *workercore.DeviceInterfaceLink,
) NetworkCheck {
	check := NetworkCheck{
		Interface:  name,
		RDMADevice: rdmaDevice,
		Depth:      device.PreflightDepthDeclared,
	}
	if link != nil {
		check.State, check.Reason = link.State, link.Reason
		return check
	}

	// 已绑定 RDMA 却没有链路结论时，保留条目并说明尚无可确认的状态。
	check.State = workercore.DeviceInterfaceLinkStateUnverified
	check.Reason = "the inventory carries no link state for this interface"
	return check
}

// vfInterfaceName 以物理接口和 VF 总线地址标识虚拟功能。没有网络设备名的 VF 也能据此定位。
func vfInterfaceName(ifaceName string, vf *workercore.DeviceInterfaceVirtualFunction) string {
	id := vf.PciBusID
	if id == "" {
		id = vf.Name
	}
	return ifaceName + "/" + id
}
