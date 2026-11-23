// Package preflight 在没有工作负载时检查各厂商分配器依赖的节点条件，
// 例如 Ascend 共享开关、寒武纪 sMLU 模式或 NVIDIA MIG 能力。先读取实际库存，再核实驱动状态，
// 条件允许时运行持有设备的探针容器。会触碰驱动开关的检查单独从 preflight 入口启动。
package preflight

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime/debug"
	"strings"
	"time"

	yaml "gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/util/sets"
	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/devicemanager/detector"
)

var logger = klog.Background().WithName("preflight")

// 当某个厂商没有检查行时，在组说明中写出具体原因，区分没有设备、未能探测、无法锁定节点等情况。
const (
	noteNoPreflighter = "no allocation-time precondition is checked for this manufacturer"
	noteUnmeasured    = "the detect pass could not measure this manufacturer, " +
		"so none of its accelerators were preflighted"
	noteNoAccelerator = "no accelerator of this manufacturer was detected"
	noteNoCheck       = "this manufacturer's accelerators were preflighted and none of them " +
		"declared a capability to read"
	noteContended = "another preflight holds this node's lock, so nothing was read for this " +
		"manufacturer: two runs at once sweep each other's probe containers"
	noteUnlockable = "this node's preflight lock could not be taken, so nothing was read for this " +
		"manufacturer: the reason above says why, and it is not another preflight"
	noteDetectionFailed = "detection failed for this manufacturer, so the remaining questions are " +
		"unanswerable: whether these accelerators slice, and what happens to a slice, are questions " +
		"about accelerators this report cannot identify -- the detection reason above is the one to fix"
)

// capPreflightPanicked 标识某个厂商的检查意外触发了 panic，便于报告该组没有取得有效结果的原因。
const capPreflightPanicked = "preflight-panicked"

// capStaleSweep 标识预检未能完成遗留容器清理，这个故障属于探测流程。
const capStaleSweep = "stale-container-sweep"

// capDeviceHealth 标记探测器已经判为故障的卡。设备插件停发该卡的设备，预检也需要跳过它的能力探针。
const capDeviceHealth = "device-health"

// 区分没有发现加速器和根本无法探测。后者不能说明节点上是否有卡。
const (
	detectionNoAccelerator = "the detect pass found no accelerator of this manufacturer"
	detectionUnmeasured    = "the detect pass could not measure this manufacturer, " +
		"so whether its accelerators are present is unknown"
	detectionUnnamed = "the detect pass found accelerators it could not name: the part of this " +
		"manufacturer's driver stack that names them did not load, which leaves the group's id " +
		"empty as well -- and every resource flavor and instance type on this node is named after " +
		"that id"
)

type Preflighter struct {
	manufacturers sets.Set[string]
	dryRun        bool
	probeImage    string
	wantRuntime   string
	detector      *detector.Detector
	host          *hostExec
	// hostRoot 是宿主机根目录在容器内的挂载点，供厂商读取对应的宿主机文件。
	hostRoot string
	// hostRootErr 保留根目录校验结果，决定本轮能否持锁，也决定能否把宿主机路径交给厂商预检器。
	hostRootErr error
	// runtime 是在宿主机解析出的容器运行时，同一轮不同厂商共用。
	runtime *hostRuntime
	// noRuntime 保存未找到可用运行时的具体原因，使报告能区分没有安装 CLI 和 kubelet 使用的 CRI 不受支持。
	noRuntime string
	// sweepFailures 记录哪些运行时未能完成清理。遗留容器可能持有加速器，会影响随后在同一卡上取得的结果。
	sweepFailures []string
}

// New 根据配置建立预检器。探测器只调用一次 DetectAccelerator，用实际库存确定要检查的卡及其厂商地址。
func New(c *Config) (*Preflighter, error) {
	d, err := detector.New(&detector.Config{
		NoPCICheck:    c.NoPCICheck,
		Manufacturers: c.Manufacturers,
	})
	if err != nil {
		return nil, err
	}
	return &Preflighter{
		manufacturers: c.Manufacturers,
		dryRun:        c.DryRun,
		probeImage:    c.ProbeImage,
		wantRuntime:   c.Runtime,
		detector:      d,
		host:          newHostExec(c.HostRoot),
		hostRoot:      c.HostRoot,
	}, nil
}

// PreflightAccelerator 逐个厂商检查当前节点。每个请求的厂商都返回一组结果，即使没有可执行的检查也要给出原因。
func (p *Preflighter) PreflightAccelerator(ctx context.Context) device.PreflightGroupList {
	// 先验证宿主机根目录，再持锁清理带预检标签的遗留容器，避免并发预检误删对方仍在运行的探针。宿主机不可访问时无法安全串行化驱动开关写入，
	// 退回只读模式。显式 dry-run 也要校验根目录，防止误读容器中的同名文件。
	p.hostRootErr = p.host.Validate()
	if p.hostRootErr != nil {
		logger.Info("no usable host root, so every row that reads one reports that it never looked, "+
			"and this pass is downgraded to a dry run: the node cannot be locked, and a mode toggle "+
			"is a write that needs it",
			"error", p.hostRootErr.Error())
	}
	if !p.dryRun {
		if p.hostRootErr != nil {
			p.dryRun = true
		} else {
			release, err := lockHost(p.host.root)
			if err != nil {
				return p.contended(err)
			}
			defer release()
		}
	}

	// 先定位容器运行时，后续清理和启动都需要它。无法执行时保留原因并输出可复现的命令。
	if rt, err := p.host.ResolveRuntime(ctx, p.wantRuntime); err != nil {
		p.noRuntime = err.Error()
		logger.V(3).Info("no container runtime resolved on the host", "error", p.noRuntime)
	} else {
		p.runtime = rt
	}
	p.sweepStaleContainers(ctx)

	// 探针挂载分配器生成的文件时使用专用目录。即使进程被强制终止，遗留文件也不会被真实分配器计入 Pod 的占用。
	defer p.sweepRenderedArtifacts()

	grpList, unmeasured := p.detector.DetectAccelerator(ctx)

	detected := make(map[string]device.DevicesGroupList, len(grpList))
	for i := range grpList {
		m := grpList[i].Manufacturer
		detected[m] = append(detected[m], grpList[i])
	}

	out := make(device.PreflightGroupList, 0, p.manufacturers.Len())
	for _, m := range sets.List(p.manufacturers) {
		grp := p.preflightSurviving(ctx, m, detected[m], unmeasured.Has(m))
		// 清理结果属于整台节点。把错误附到每张实际设备的检查中，按设备过滤和判断退出码时都能看到。
		grp.Checks = append(grp.Checks, p.staleSweepChecks(detected[m])...)
		out = append(out, grp)
	}
	return out
}

// contended 将获取节点锁失败转成每个厂商的报告，区分已有预检持锁与权限等错误。没有取得锁就不会清理或启动容器。
func (p *Preflighter) contended(err error) device.PreflightGroupList {
	note := noteUnlockable
	if errors.Is(err, errContended) {
		note = noteContended
	}

	out := make(device.PreflightGroupList, 0, p.manufacturers.Len())
	for _, m := range sets.List(p.manufacturers) {
		out = append(out, device.PreflightGroup{
			Manufacturer: m,
			Timestamp:    time.Now(),
			Detection: device.PreflightDetection{
				State:  device.PreflightStateUnavailable,
				Depth:  device.PreflightDepthDeclared,
				Reason: err.Error(),
			},
			Note: note,
		})
	}
	return out
}

// preflightSurviving 将单个厂商驱动的 panic 限制在该厂商。其余厂商仍可检查，
// 出错厂商的卡会明确标为 unavailable。
func (p *Preflighter) preflightSurviving(
	ctx context.Context, manufacturer string, groups device.DevicesGroupList, unmeasured bool,
) (grp device.PreflightGroup) {
	// 在调用厂商驱动之前保留基础探测和宿主机交叉检查的结论，这两项已取得的结果不受后续厂商故障影响。
	det := detection(groups, unmeasured)
	if p.host != nil {
		crossCheckHost(ctx, p.host, manufacturer, &det)
	}

	defer func() {
		r := recover()
		if r == nil {
			return
		}
		logger.Error(nil, "a manufacturer's preflight panicked", "manufacturer", manufacturer,
			"panic", fmt.Sprint(r), "stack", string(debug.Stack()))
		// 丢弃驱动中途失败时尚未完成的能力结果。重建故障卡记录，保留已完成的检测及失败原因。
		grp = device.PreflightGroup{
			Manufacturer: manufacturer,
			Timestamp:    time.Now(),
			Detection:    det,
			Checks:       panickedChecks(groups, r),
			Note: "this manufacturer's preflight panicked and was contained: whatever it had " +
				"established is discarded, and every accelerator it was asked about is reported " +
				"unavailable below; the rest of this run is unaffected: " + fmt.Sprint(r),
		}
	}()
	return p.preflight(ctx, manufacturer, det, groups, unmeasured)
}

// staleSweepChecks 为每张可能受到遗留容器影响的卡生成不可用记录。清理成功时不增加检查项。
func (p *Preflighter) staleSweepChecks(groups device.DevicesGroupList) []device.PreflightCheck {
	if len(p.sweepFailures) == 0 {
		return nil
	}

	reason := "the stale-container sweep could not be completed, so a container an earlier run left " +
		"behind may still be holding this accelerator and every answer measured against it here is " +
		"unsafe to trust: " + strings.Join(p.sweepFailures, "; ")

	var checks []device.PreflightCheck
	for i := range groups {
		accels := groups[i].Accelerators
		for j := range accels {
			checks = append(checks, device.PreflightCheck{
				Accelerator: accels[j].ID,
				Capability:  capStaleSweep,
				Mode:        device.PreflightModeUnnamed,
				State:       device.PreflightStateUnavailable,
				Depth:       device.PreflightDepthDeclared,
				Reason:      reason,
			})
		}
	}
	return checks
}

// anyLogicallySliceable 判断该组是否有卡声明逻辑切片容量，只有这些卡需要准备容器探针的库文件。
func anyLogicallySliceable(groups device.DevicesGroupList) bool {
	for i := range groups {
		for j := range groups[i].Accelerators {
			if groups[i].Accelerators[j].Status.LogicalSliced.Count > 0 {
				return true
			}
		}
	}
	return false
}

// hostRootForManufacturers 只把校验通过的宿主机根目录交给厂商。空字符串表示无法读取宿主机文件。
func hostRootForManufacturers(root string, validationErr error) string {
	if validationErr != nil {
		return ""
	}
	return root
}

// panickedChecks 为每张受厂商驱动异常影响的卡生成 unavailable 记录，使退出状态也能反映故障。
func panickedChecks(groups device.DevicesGroupList, r any) []device.PreflightCheck {
	reason := "this manufacturer's preflight panicked and was contained, so none of this " +
		"accelerator's preconditions were established: " + fmt.Sprint(r)

	var checks []device.PreflightCheck
	for i := range groups {
		for j := range groups[i].Accelerators {
			checks = append(checks, device.PreflightCheck{
				Accelerator: groups[i].Accelerators[j].ID,
				Capability:  capPreflightPanicked,
				Mode:        device.PreflightModeUnnamed,
				State:       device.PreflightStateUnavailable,
				Depth:       device.PreflightDepthDeclared,
				Reason:      reason,
			})
		}
	}
	return checks
}

// unhealthyCount 统计该组在本轮探测中报告为故障的卡数。
func unhealthyCount(accels []device.Accelerator) int {
	unhealthy := 0
	for i := range accels {
		if accels[i].Status.Unhealthy {
			unhealthy++
		}
	}
	return unhealthy
}

// partitionHealthyGroups 复制各组的元数据和健康卡列表，保留原库存给检测结果计数。
// 每张故障卡各自生成一条 device-health 记录，组中没有健康卡时就不把该组交给探针。
func partitionHealthyGroups(
	groups device.DevicesGroupList,
) (device.DevicesGroupList, []device.PreflightCheck) {
	reason := "the detect pass reported this accelerator unhealthy, so the device plugin withholds " +
		"it from allocations and preflight skipped its capability checks"

	var (
		healthyGroups = make(device.DevicesGroupList, 0, len(groups))
		checks        []device.PreflightCheck
	)
	for i := range groups {
		healthy := make([]device.Accelerator, 0, len(groups[i].Accelerators))
		for j := range groups[i].Accelerators {
			accel := groups[i].Accelerators[j]
			if accel.Status.Unhealthy {
				checks = append(checks, device.PreflightCheck{
					Accelerator: accel.ID,
					Capability:  capDeviceHealth,
					Mode:        device.PreflightModeUnnamed,
					State:       device.PreflightStateUnavailable,
					Depth:       device.PreflightDepthDeclared,
					Reason:      reason,
				})
				continue
			}
			healthy = append(healthy, accel)
		}
		if len(healthy) == 0 {
			continue
		}
		grp := groups[i]
		grp.Accelerators = healthy
		healthyGroups = append(healthyGroups, grp)
	}
	return healthyGroups, checks
}

func (p *Preflighter) preflight(
	ctx context.Context, manufacturer string, det device.PreflightDetection,
	groups device.DevicesGroupList, unmeasured bool,
) device.PreflightGroup {
	grp := device.PreflightGroup{
		Manufacturer: manufacturer,
		Timestamp:    time.Now(),
		Detection:    det,
	}

	creator := supportedPreflighterCreators[manufacturer]
	switch {
	case creator == nil:
		// 当前平台没有该厂商的预检器时，直接说明情况，不根据本轮是否发现设备作额外推断。
		grp.Note = noteNoPreflighter
		return grp
	case unmeasured:
		grp.Note = noteUnmeasured
		return grp
	case len(groups) == 0:
		grp.Note = noteNoAccelerator
		return grp
	case grp.Detection.State == device.PreflightStateUnavailable:
		// 基础探测已经失败时，继续测量切片会把结果挂到无法识别的卡上，此处保留探测失败的原因。
		grp.Note = noteDetectionFailed
		return grp
	}

	// 先隔离故障卡，再对健康卡调用厂商预检器。这样既不会在故障卡上创建探针容器，也不会跳过同组的健康卡。
	healthyGroups, healthChecks := partitionHealthyGroups(groups)

	if len(healthyGroups) > 0 {
		// 按需加载对应厂商的库，节点无需初始化与本机设备无关的驱动。
		pf := creator(device.PreflighterOptions{
			Logger:   logger.V(3),
			DryRun:   p.dryRun,
			HostRoot: hostRootForManufacturers(p.hostRoot, p.hostRootErr),
		})
		read := pf.PreflightAccelerator(healthyGroups)
		grp.Checks = read.Checks

		// 厂商没有给出证据层级时按最浅的 declared 记录。漏填模式时用 unnamed 并附说明。后面实际执行的容器探针自行填写达到的层级。
		for i := range grp.Checks {
			c := &grp.Checks[i]
			if c.Depth == "" {
				c.Depth = device.PreflightDepthDeclared
			}
			if c.Mode == "" {
				c.Mode = device.PreflightModeUnnamed
				c.Reason = strings.TrimSpace(c.Reason + " (this check names no allocation mode, so it " +
					"cannot be compared with another manufacturer's answer for the same one)")
			}
		}

		// 只有存在能承载逻辑切片的健康卡时，才在宿主机准备这个厂商的库。两个容器检查共用同一份文件。
		var staged StageResult
		if anyLogicallySliceable(healthyGroups) {
			staged = p.stageLibFor(manufacturer)
		}

		// 即使厂商分配时不需要读取驱动，也可以用生产响应器生成注入并从容器内检查切片。
		grp.Checks = append(grp.Checks, p.measureSliced(ctx, manufacturer, pf, staged, healthyGroups)...)

		// 另行检查第二个容器实际获得的设备。这个问题与单个切片的读数相互独立。
		grp.Checks = append(grp.Checks, p.checkManagement(ctx, manufacturer, pf, staged, healthyGroups)...)

		// 厂商没有驱动前置条件的说明仍要保留，容器检查提供的其他结果不能说明驱动本身已经被查询过。
		grp.Note = read.Note
	}

	// 在厂商能力结果之后追加健康行。它描述整张卡，没有对应某个特定分配模式，即使组内没有健康卡也要写入。
	grp.Checks = append(grp.Checks, healthChecks...)

	// 没有检查项且厂商未提供说明时，补充原因，以免将空记录解读为检查通过。
	if grp.Note == "" && len(grp.Checks) == 0 {
		grp.Note = noteNoCheck
	}
	return grp
}

// detection 把本轮厂商探测转换成独立结论，分别处理已发现设备、未发现设备和无法探测。
func detection(groups device.DevicesGroupList, unmeasured bool) device.PreflightDetection {
	d := device.PreflightDetection{Depth: device.PreflightDepthDeclared}

	switch {
	case unmeasured:
		// 本轮读取出错优先处理，以免把此前的缓存误作本次发现的设备。
		d.State = device.PreflightStateUnavailable
		d.Reason = detectionUnmeasured
		return d
	case len(groups) == 0:
		d.State = device.PreflightStateNotDeclared
		d.Reason = detectionNoAccelerator
		return d
	}

	var (
		names   = make([]string, 0, len(groups))
		unnamed bool
	)
	for i := range groups {
		d.Accelerators += len(groups[i].Accelerators)
		if groups[i].Name == "" {
			unnamed = true
		}
		// 总数包括不健康设备，另列故障数，使读者同时能看到节点上的完整库存和需要排查的数量。
		unhealthy := unhealthyCount(groups[i].Accelerators)
		if unhealthy > 0 {
			names = append(names, fmt.Sprintf("%s x%d (%d unhealthy)",
				groups[i].Name, len(groups[i].Accelerators), unhealthy))
			continue
		}
		names = append(names, fmt.Sprintf("%s x%d", groups[i].Name, len(groups[i].Accelerators)))
	}
	// 未命名的组不能生成有效的组 ID，后续 ResourceFlavor 和 InstanceType 也无法正确命名，
	// 需要让探测报告反映这个失败。
	if unnamed {
		d.State = device.PreflightStateUnavailable
		d.Reason = detectionUnnamed
		return d
	}
	d.State = device.PreflightStateOK
	d.Detail = strings.Join(names, ", ")
	return d
}

// preflightResult 将各厂商加速器结果与节点级网络、拓扑信息写进同一份 YAML 文档，使读者能够一次读取所有部分。
type preflightResult struct {
	Accelerators device.PreflightGroupList `json:"accelerators" yaml:"accelerators"`
	Network      NetworkReport             `json:"network" yaml:"network"`
	Topology     TopologyReport            `json:"topology" yaml:"topology"`
}

// Report 先输出完整 YAML，再返回加速器的失败原因，使自动化能读取退出状态、人工也能查看失败现场。网络和拓扑是节点放置条件，
// 单独报告，不影响加速器分配能力的退出判断。
func Report(
	w io.Writer, grpList device.PreflightGroupList, network NetworkReport, topology TopologyReport,
) error {
	enc := yaml.NewEncoder(w)
	enc.SetIndent(4)
	if err := enc.Encode(preflightResult{
		Accelerators: grpList, Network: network, Topology: topology,
	}); err != nil {
		return fmt.Errorf("encode result: %w", err)
	}

	if failed := Failed(grpList); len(failed) != 0 {
		return errors.New("preflight failed: " + strings.Join(failed, "; "))
	}
	return nil
}

// Failed 收集探测或能力检查中所有 unavailable 的原因。硬件未声明某项能力或本机没有该厂商设备时无需判为失败。
func Failed(grpList device.PreflightGroupList) []string {
	var failed []string
	for i := range grpList {
		grp := &grpList[i]
		if grp.Detection.State == device.PreflightStateUnavailable {
			failed = append(failed, fmt.Sprintf("%s: detection: %s", grp.Manufacturer, grp.Detection.Reason))
		}
		for j := range grp.Checks {
			c := &grp.Checks[j]
			if c.State != device.PreflightStateUnavailable {
				continue
			}
			failed = append(failed,
				fmt.Sprintf("%s: %s on %s: %s", grp.Manufacturer, c.Capability, c.Accelerator, c.Reason))
		}
	}
	return failed
}
