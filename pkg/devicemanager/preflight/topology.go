package preflight

import (
	"fmt"
	"strings"
	"time"

	"gpustack.ai/gpustack/pkg/device"
)

// TopologyPolicyUnknown 表示没有读到有效的 kubelet 拓扑策略，不根据默认值推测当前配置。
const TopologyPolicyUnknown = "unknown"

type (
	// TopologyReport 记录本节点 kubelet 的 TopologyManager 策略，
	// 说明申请 NUMA 亲和设备时 kubelet 采用的放置约束。
	TopologyReport struct {
		// Timestamp 是读取策略配置的时间。
		Timestamp time.Time `json:"timestamp" yaml:"timestamp"`
		// Policy 采用 kubelet 命令行或有效配置中的原始策略名。无法确认时为 unknown。
		Policy string `json:"policy" yaml:"policy"`
		// Depth 为 declared，此处读取配置，没有对运行中的 kubelet 发起设备分配以验证策略效果。
		Depth device.PreflightDepth `json:"depth" yaml:"depth"`
		// Note 解释策略为何未知，区分未设置、无法读取和配置冲突等原因。
		Note string `json:"note,omitempty" yaml:"note,omitempty"`
	}
)

// PreflightTopology 使用核实过的宿主机根目录读取 kubelet 策略，无需修改节点配置。无法访问根目录时直接报告未知。
func (p *Preflighter) PreflightTopology() TopologyReport {
	if err := p.host.Validate(); err != nil {
		return unknownTopology(time.Now(),
			"the kubelet's configuration was never looked for, because the host root did not "+
				"validate: "+err.Error())
	}
	return topologyReport(p.host.root, time.Now())
}

// topologyReport 与运行时端点共用配置读取器，按相同顺序处理命令行、配置文件和补充目录。策略无法确定时在报告中说明。
func topologyReport(root string, now time.Time) TopologyReport {
	reading := readKubeletSetting(root, topologyPolicySetting)

	switch {
	case reading.UnsearchableErr != nil:
		// 搜索本身未能进行时指明是哪个来源，避免将缺少宿主机进程表误报成配置文件权限问题。
		return unknownTopology(now, fmt.Sprintf(
			"the kubelet configuration search under %s could not run, so which topology "+
				"manager policy its kubelet runs is unknown rather than defaulted: %s",
			reading.UnsearchablePattern, reading.UnsearchableErr,
		))
	case reading.UnreadableErr != nil:
		// 匹配到文件却读不到时，在说明中保留它的路径，因为生效的策略可能就在这个文件中。
		return unknownTopology(now, fmt.Sprintf(
			"this host carries a kubelet configuration at %s that could not be read, so "+
				"which topology manager policy its kubelet runs is unknown rather than "+
				"defaulted: %s", reading.UnreadablePath, reading.UnreadableErr,
		))

	case reading.Conflict != nil:
		// 多个部署目录若提供不同策略，就无法判定哪一个属于运行中的 kubelet，报告配置冲突。
		return unknownTopology(now, fmt.Sprintf(
			"the kubelet configuration under %s names more than one topology manager policy "+
				"(%s), and only one of them belongs to the kubelet that is running, so no "+
				"policy is reported rather than a guessed one",
			reading.ConflictPattern, strings.Join(reading.Conflict, ", "),
		))

	case reading.Found:
		return TopologyReport{
			Timestamp: now, Policy: reading.Value, Depth: device.PreflightDepthDeclared,
		}

	default:
		// 已核对运行中 kubelet 的配置仍找不到策略，与没有运行中的 kubelet、只查到常见位置，是两种不同的未知情况。
		places := strings.Join(reading.Searched, ", ")
		if reading.CommandLineRead {
			return unknownTopology(now, fmt.Sprintf(
				"nothing this node's kubelet reads its configuration from (%s) names a topology "+
					"manager policy, so its default is not reported either: that would be a "+
					"value nobody read", places,
			))
		}
		return unknownTopology(now, fmt.Sprintf(
			"no kubelet is running here to name where its configuration is, and none of the "+
				"places one is kept (%s, the last one walked) names a topology manager policy; a "+
				"kubelet started here may still be given one on its command line, so its default "+
				"is not reported either: that would be a value nobody read", places,
		))
	}
}

// unknownTopology 返回带时间、声明层级和原因的未知策略结果。
func unknownTopology(now time.Time, note string) TopologyReport {
	return TopologyReport{
		Timestamp: now,
		Policy:    TopologyPolicyUnknown,
		Depth:     device.PreflightDepthDeclared,
		Note:      note,
	}
}
