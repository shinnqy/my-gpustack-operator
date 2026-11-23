package preflight

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"

	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/deviceplugin"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/utils/stringx"
)

// 对每张卡分别检查 sidecar 获得的设备可见性和两个切片能否同时使用，便于分别定位分配与共存问题。
const (
	capSidecarVisibility = "sidecar-visibility"
	capCoTenancy         = "co-tenancy"
)

// 预检用两种双容器 Pod，分别对应复用主容器设备的可见性 sidecar，以及另一个独立申请切片的租户。
const (
	preflightSidecarContainer  = "preflight-visibility"
	preflightCoTenantContainer = "preflight-cotenant"
)

// visibleDevicesEnvSuffix 用单数词干匹配各厂商可见设备的环境变量，包括海光的单数名称。其余厂商还需检查设备节点。
const visibleDevicesEnvSuffix = "_VISIBLE_DEVICE"

// 保存检查停在当前证据层级的原因。未能继续测量时，报告已经确认的内容。
const (
	sidecarNoDeeper = "the measured form needs the owner container to still be running when the " +
		"sidecar starts, and the container this command starts is a one-shot that exits as soon as " +
		"it has printed its evidence, so there would be nothing left for a sidecar to co-allocate from"
	partitionNotDriven = "this accelerator is partition-backed, so a sidecar is granted the " +
		"partition its owner holds rather than the accelerator itself. That response is served by " +
		"the responder's partition capability, which this pass does not drive: reaching it means " +
		"asserting the responder to the interface that also creates a hardware partition, and " +
		"preflight reads partitions and never creates one"
)

// checkManagement 对同一组健康卡检查 sidecar 可见性和多租户共存。逻辑切片走分配响应器，
// 物理分区只报告当前能够确认的条件。
func (p *Preflighter) checkManagement(
	ctx context.Context, manufacturer string, pf device.AcceleratorPreflighter,
	staged StageResult, groups device.DevicesGroupList,
) []device.PreflightCheck {
	injector, ok := pf.(deviceplugin.AcceleratorInjectionPreflighter)
	if !ok || p.host == nil {
		return nil
	}

	var checks []device.PreflightCheck
	for i := range groups {
		grp := &groups[i]
		for j := range grp.Accelerators {
			accel := &grp.Accelerators[j]
			switch {
			case accel.Status.LogicalSliced.Count > 0:
				checks = append(checks,
					p.sidecarVisibility(ctx, manufacturer, injector, grp, accel),
					p.coTenancy(ctx, manufacturer, injector, staged, grp, accel))
			case accel.Status.PhysicalSliced.Count > 0:
				checks = append(checks, partitionBackedRows(accel.ID)...)
			}
		}
	}
	return checks
}

// partitionBackedRows 解释物理分区卡的 sidecar 和共存检查范围。预检没有创建分区，
// 因此两项都只报告 declared，并各自标明对应的分配模式。
func partitionBackedRows(acceleratorID string) []device.PreflightCheck {
	modes := map[string]workercore.DeviceAllocationMode{
		capSidecarVisibility: workercore.DeviceAllocationModeVisibility,
		capCoTenancy:         workercore.DeviceAllocationModePartitioned,
	}

	rows := make([]device.PreflightCheck, 0, len(modes))
	for _, capability := range []string{capSidecarVisibility, capCoTenancy} {
		rows = append(rows, device.PreflightCheck{
			Accelerator: acceleratorID,
			Capability:  capability,
			Mode:        device.PreflightModeOf(modes[capability]),
			State:       device.PreflightStateOK,
			Depth:       device.PreflightDepthDeclared,
			Detail:      partitionNotDriven,
		})
	}
	return rows
}

// sidecarVisibility 按 kubelet 的调用顺序，先让主容器获得切片，
// 再检查 sidecar 是否只能看到它已持有的设备。
func (p *Preflighter) sidecarVisibility(
	ctx context.Context,
	manufacturer string,
	injector deviceplugin.AcceleratorInjectionPreflighter,
	grp *workercore.DevicesGroup,
	accel *workercore.Accelerator,
) device.PreflightCheck {
	row := device.PreflightCheck{
		Accelerator: accel.ID,
		Capability:  capSidecarVisibility,
		Mode:        device.PreflightModeOf(workercore.DeviceAllocationModeVisibility),
		State:       device.PreflightStateOK,
		Depth:       device.PreflightDepthSimulated,
	}

	owner, sidecar, err := p.simulateSidecarPair(ctx, manufacturer, injector, grp, accel)
	if err != nil {
		row.State = device.PreflightStateUnavailable
		row.Depth = device.PreflightDepthDeclared
		row.Reason = err.Error()
		return row
	}

	// 只要求 sidecar 的设备集合包含在主容器授权范围内。核心是发现多出来的卡。
	granted, seen := grantedDevices(owner), grantedDevices(sidecar)
	unheld := namesNotIn(seen, granted)
	unnamed := namesNotIn(granted, seen)
	switch {
	case len(granted) == 0:
		row.Detail = "the owner allocation names no visible device and no device node, so there was " +
			"nothing for the sidecar's own to be compared against. " + sidecarNoDeeper
	case len(seen) == 0:
		row.State = device.PreflightStateUnavailable
		row.Reason = "the owner was granted " + describeDevices(granted) +
			" and the sidecar allocation names no visible device and no device node at all, so a " +
			"sidecar on this node would be handed its owner's accelerator by nothing"
	case len(unheld) != 0:
		row.State = device.PreflightStateUnavailable
		row.Reason = "the sidecar allocation names " + describeDevices(unheld) +
			", which the owner was not granted (" + describeDevices(granted) +
			"), so a sidecar on this node would see a device its owner does not hold"
	case len(unnamed) != 0:
		row.Detail = "the sidecar allocation names " + describeDevices(seen) +
			", all of it the owner's own grant, and does not carry " + describeDevices(unnamed) +
			" -- which the owner's allocation adds for itself and a sidecar reaching the same " +
			"accelerator does not need. " + sidecarNoDeeper
	default:
		row.Detail = "the sidecar allocation names exactly what the owner was granted (" +
			describeDevices(granted) + "). " + sidecarNoDeeper
	}
	return row
}

// namesNotIn 依照输入顺序找出 a 中存在、b 中没有的设备名。
func namesNotIn(a, b []string) []string {
	var out []string
	for _, name := range a {
		if !slices.Contains(b, name) {
			out = append(out, name)
		}
	}
	return out
}

// coTenancy 为同一张卡申请两个独立切片。有可用运行时时同时启动两个容器，读取各自观察到的配额及共存证据。
func (p *Preflighter) coTenancy(
	ctx context.Context,
	manufacturer string,
	injector deviceplugin.AcceleratorInjectionPreflighter,
	staged StageResult,
	grp *workercore.DevicesGroup,
	accel *workercore.Accelerator,
) device.PreflightCheck {
	row := device.PreflightCheck{
		Accelerator: accel.ID,
		Capability:  capCoTenancy,
		Mode:        device.PreflightModeOf(workercore.DeviceAllocationModeSliced),
		State:       device.PreflightStateOK,
		Depth:       device.PreflightDepthSimulated,
	}

	first, second, err := p.simulateCoTenants(ctx, manufacturer, injector, grp, accel)
	if err != nil {
		row.State = device.PreflightStateUnavailable
		row.Depth = device.PreflightDepthDeclared
		row.Reason = err.Error()
		return row
	}

	const produced = "the allocator produced two independent slice injections for this accelerator"

	probe, known := sliceProbes[manufacturer]
	if !known {
		row.Detail = produced + "; no container probe has been established for " + manufacturer +
			", so the two slices were not started together"
		return row
	}

	image, err := ResolveProbeImage(manufacturer, grp.Family, grp.RuntimeVersion, p.probeImage)
	if err != nil {
		row.Detail = produced + "; they were not started together because " + err.Error()
		return row
	}

	for _, injection := range []*deviceplugin.ContainerAllocateResponse{first, second} {
		for k, v := range probe.LogEnv {
			injection.Envs[k] = v
		}
	}

	runs, err := p.runTogether(ctx, image, probe, staged, accel.ID, first, second)
	row.Command = togetherCommand(runs[0], runs[1])
	switch {
	case err != nil:
		// 容器未能启动时保留命令及原因。此时还没有关于两个切片能否共存的实测结论。
		row.State = device.PreflightStateOK
		row.Detail = produced + "; the two co-tenant containers could not both be started, so " +
			"nothing was measured: " + err.Error()
		if p.runtime != nil && p.runtime.NetworkWarning != "" {
			row.Detail += ". " + p.runtime.NetworkWarning
		}
		row.Evidence = string(runs[0].Output) + string(runs[1].Output)
		return row
	case runs[0].Emitted:
		row.Detail = produced + "; the container step was emitted rather than run because " +
			runs[0].Reason
		if p.dryRun {
			// dry-run 不会把文件放到宿主机上，因此输出复现命令时还要说明所需的挂载文件。
			row.Detail += dryRunRemediation(first, second)
		}
		return row
	}

	row.Evidence = "tenant 1:\n" + string(runs[0].Output) + "\ntenant 2:\n" + string(runs[1].Output)

	// 两个探针必须各自观察到对方仍在运行。仅仅几乎同时启动，不能证明它们曾经重叠。
	met := bytes.Contains(runs[0].Output, []byte(coTenantsMet)) &&
		bytes.Contains(runs[1].Output, []byte(coTenantsMet))

	firstQuota, firstAbsent := memoryQuota(first, probe, p.host.root)
	secondQuota, secondAbsent := memoryQuota(second, probe, p.host.root)
	firstOut, secondOut := readerSection(runs[0].Output), readerSection(runs[1].Output)
	switch {
	case runs[0].ExitError != "" || runs[1].ExitError != "":
		// 检查退出状态，避免把两个容器异常退出前打印的标记当作成功共存的证据。
		row.State = device.PreflightStateUnavailable
		row.Depth = device.PreflightDepthMeasured
		row.Reason = "both slice injections started, and a container then exited non-zero under " +
			"them, so this accelerator was not observed holding two slices that both stayed " +
			"alive -- " + coTenantExits(runs)
	case !met:
		row.Detail = "both slices were started and each ran, but neither waited out the other at " +
			"the barrier, so they were not observed holding this accelerator at the same time; " +
			"the containers' own output is carried as evidence"
	case probe.LoadEvidence != "" &&
		strings.Contains(firstOut, probe.LoadEvidence) && firstOut == secondOut:
		// 若两个租户读到了完全相同的驱动实例记录，不能证明有两个独立切片。
		row.Detail = produced + "; both tenants reported the same driver record, so one slice was " +
			"observed twice rather than two at once; the containers' own output is carried as evidence"
	case probe.LoadEvidence != "" &&
		(!strings.Contains(firstOut, probe.LoadEvidence) || !strings.Contains(secondOut, probe.LoadEvidence)):
		// 需要厂商驱动登记进程的探针，还要核实两个租户都取得了自己的记录。
		row.Detail = produced + "; at least one of the two containers produced no driver record, so " +
			"whatever it printed was printed outside a slice and the two were not observed holding " +
			"this accelerator together; the containers' own output is carried as evidence"
	case firstQuota == "" || secondQuota == "":
		// 如果注入配置承诺了显存上限，两个租户各自都应当能在读取结果中看到它。
		row.State = device.PreflightStateUnavailable
		row.Depth = device.PreflightDepthMeasured
		row.Reason = "two slices ran together on this accelerator and at least one of them bounds " +
			"nothing this run could read -- " + cmp.Or(firstAbsent, secondAbsent)
	case !reportsFigure(firstOut, firstQuota) || !reportsFigure(secondOut, secondQuota):
		row.Detail = "two slices ran together on this accelerator, capped at " + firstQuota +
			" and " + secondQuota + ", and at least one of them did not report its cap back, so the " +
			"two coexist and each holding its own quota was not observed; the containers' own " +
			"output is carried as evidence"
	default:
		row.Depth = device.PreflightDepthMeasured
		row.Detail = "two slices ran together on this accelerator, each reporting its own cap (" +
			firstQuota + " and " + secondQuota + ") rather than the whole accelerator"
	}
	return row
}

// readerSection 只返回厂商工具输出的部分，避免从注入日志中读到配置值就判为实测成功。
func readerSection(out []byte) string {
	_, readerOut := probeSections(string(out))
	return readerOut
}

// barrierComponent 将设备 ID 散列成同步目录名。驱动返回的原始 ID 可能含有路径分隔符，还可能在简单替换后重名。
func barrierComponent(acceleratorID string) string {
	return stringx.SumByFNV64a(acceleratorID)
}

// tenantName 为同步的两个探针生成可区分的租户名。
func tenantName(i int) string { return "tenant-" + strconv.Itoa(i+1) }

// coTenantExits 给出每个异常退出的探针及其退出状态，同时保留两方的故障信息。
func coTenantExits(runs []emitResult) string {
	failed := make([]string, 0, len(runs))
	for i := range runs {
		if runs[i].ExitError != "" {
			failed = append(failed, tenantName(i)+": "+runs[i].ExitError)
		}
	}
	return strings.Join(failed, "; ")
}

// runTogether 并行运行两个探针并分别收集结果，使检查能验证同卡资源的同时占用。
func (p *Preflighter) runTogether(
	ctx context.Context, image string, probe sliceProbe, staged StageResult, acceleratorID string,
	injections ...*deviceplugin.ContainerAllocateResponse,
) ([]emitResult, error) {
	results := make([]emitResult, len(injections))
	errs := make([]error, len(injections))
	force := forceEmitReason(p.dryRun, staged)

	// 两个容器共享一个可写同步目录，用来互相确认对方在线。目录位于本轮预检路径下，后续统一回收。
	hostBarrier := filepath.Join(deviceplugin.OperatorPreflightDir,
		string(deviceplugin.PreflightPodUID), "barrier", barrierComponent(acceleratorID))
	if force == "" {
		dir := filepath.Join(p.host.root, hostBarrier)
		if err := os.RemoveAll(dir); err != nil {
			return results, fmt.Errorf("clear the co-tenancy barrier directory: %w", err)
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return results, fmt.Errorf("create the co-tenancy barrier directory: %w", err)
		}
	}
	for i := range injections {
		injections[i].Mounts = append(injections[i].Mounts, &deviceplugin.Mount{
			ContainerPath: coTenancyBarrierDir,
			HostPath:      hostBarrier,
		})
	}

	var wg sync.WaitGroup
	for i := range injections {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = emitOrRun(ctx, p.host, p.runtime, p.noRuntime, force, containerRunSpec{
				Image:     image,
				Injection: injections[i],
				Runtime:   probe.Runtime,
				Label:     preflightLabel,
				Args: coTenantProbeShellCommand(probe.Reader, logEnvNames(probe),
					tenantName(i), tenantName((i+1)%len(injections))),
			})
		}(i)
	}
	wg.Wait()

	return results, errors.Join(errs...)
}

// togetherCommand 生成同时后台启动两个容器并等待两者退出的命令，可复现并发测量。
func togetherCommand(first, second emitResult) string {
	return first.Command + " &\n" + second.Command + "\nwait"
}

// simulateSidecarPair 按真实分配顺序生成主容器和 sidecar 的注入，后者复用主容器已经分得的设备。
func (p *Preflighter) simulateSidecarPair(
	ctx context.Context,
	manufacturer string,
	injector deviceplugin.AcceleratorInjectionPreflighter,
	grp *workercore.DevicesGroup,
	accel *workercore.Accelerator,
) (owner, sidecar *deviceplugin.ContainerAllocateResponse, err error) {
	alloc, err := newCoAllocation(manufacturer, grp, accel, preflightSidecarContainer)
	if err != nil {
		return nil, nil, err
	}
	// 可见性容器只申请设备可见性资源，无需再为同一张卡扣一份额度。
	if name := nodefeature.GetAcceleratableResourceName(
		manufacturer, workercore.DeviceAllocationModeVisibility,
	); name != "" {
		alloc.Second.Resources.Limits = core.ResourceList{
			name: *resource.NewQuantity(int64(len(alloc.Allocated)), resource.DecimalSI),
		}
	}

	pair, err := p.driveResponder(manufacturer, injector,
		func(open responderOpener) ([]*deviceplugin.ContainerAllocateResponse, error) {
			// 主容器和 sidecar 请求不同分配模式，所以各自从对应模式的响应器获取注入。
			slicedSrv, err := open(workercore.DeviceAllocationModeSliced)
			if err != nil {
				return nil, err
			}
			own, _, err := slicedResponse(ctx, slicedSrv, alloc.Pod, alloc.Owner, alloc.Devs, alloc.Allocated, nil)
			if err != nil {
				return nil, fmt.Errorf("drive the owner allocation: %w", err)
			}

			visibilitySrv, err := open(workercore.DeviceAllocationModeVisibility)
			if err != nil {
				return nil, err
			}
			side, err := visibilitySrv.GetContainerAllocateResponse(
				ctx, alloc.Pod, alloc.Second, alloc.Devs, alloc.Allocated,
			)
			if err != nil {
				return nil, fmt.Errorf("drive the sidecar allocation: %w", err)
			}
			return []*deviceplugin.ContainerAllocateResponse{own, side}, nil
		})
	if err != nil {
		return nil, nil, err
	}
	return pair[0], pair[1], nil
}

// simulateCoTenants 在同一个双容器 Pod 的上下文中为同一张卡安排两个独立切片，各自生成注入。
func (p *Preflighter) simulateCoTenants(
	ctx context.Context,
	manufacturer string,
	injector deviceplugin.AcceleratorInjectionPreflighter,
	grp *workercore.DevicesGroup,
	accel *workercore.Accelerator,
) (first, second *deviceplugin.ContainerAllocateResponse, err error) {
	alloc, err := newCoAllocation(manufacturer, grp, accel, preflightCoTenantContainer)
	if err != nil {
		return nil, nil, err
	}
	// 第二个租户申请与第一个相同的半卡额度，两者合计一张卡。
	alloc.Second.Resources = alloc.Owner.Resources

	pair, err := p.driveResponder(manufacturer, injector,
		func(open responderOpener) ([]*deviceplugin.ContainerAllocateResponse, error) {
			r, err := open(workercore.DeviceAllocationModeSliced)
			if err != nil {
				return nil, err
			}
			// 放置第二个切片时带上第一个的占用，避免将两个切片排到同一个物理位置。
			one, held, err := slicedResponse(ctx, r, alloc.Pod, alloc.Owner, alloc.Devs, alloc.Allocated, nil) //nolint:govet
			if err != nil {
				return nil, fmt.Errorf("drive the first tenant's allocation: %w", err)
			}
			two, _, err := slicedResponse(ctx, r, alloc.Pod, alloc.Second, alloc.Devs, alloc.Allocated,
				mergePlacements(nil, held))
			if err != nil {
				return nil, fmt.Errorf("drive the second tenant's allocation: %w", err)
			}
			return []*deviceplugin.ContainerAllocateResponse{one, two}, nil
		})
	if err != nil {
		return nil, nil, err
	}
	return pair[0], pair[1], nil
}

// coAllocation 汇集两种双容器测量共享的 Pod、库存及主容器分配结果。
type coAllocation struct {
	// Pod 包含预检用的两个容器。
	Pod *core.Pod
	// Owner 是先取得设备额度的主容器。
	Owner *core.Container
	// Second 是随后获得设备的 sidecar 或另一个租户。
	Second *core.Container
	// Devs 是两次分配共用的库存快照。
	Devs *workercore.Devices
	// Allocated 是主容器取得的额度，可见性分配也用这张表确定授权设备。
	Allocated map[deviceplugin.Resource]int32
}

// newCoAllocation 为一张加速器构建双容器输入，使响应器能按 kubelet 实际看到的 Pod 找到对应容器。
func newCoAllocation(
	manufacturer string,
	grp *workercore.DevicesGroup,
	accel *workercore.Accelerator,
	secondName string,
) (*coAllocation, error) {
	one := *grp
	one.Accelerators = []workercore.Accelerator{*accel}

	pod, _, devs, allocated, err := deviceplugin.NewPreflightAllocationRequest(
		[]workercore.DevicesGroup{one}, manufacturer, workercore.DeviceAllocationModeSliced, sliceQuota,
	)
	if err != nil {
		return nil, fmt.Errorf("build the allocation request: %w", err)
	}

	// 容器名包含设备 ID，使不同卡的探针文件各有自己的路径。
	pod.Spec.Containers[0].Name = probeContainerName(pod.Spec.Containers[0].Name, accel.ID)
	pod.Spec.Containers = append(pod.Spec.Containers,
		core.Container{Name: probeContainerName(secondName, accel.ID)})
	// 在追加容器后再取两个指针。append 可能重新分配底层数组，旧指针可能不再指向最终 Pod 中的容器。
	return &coAllocation{
		Pod:       pod,
		Owner:     &pod.Spec.Containers[0],
		Second:    &pod.Spec.Containers[1],
		Devs:      devs,
		Allocated: allocated,
	}, nil
}

// grantedDevices 从环境变量及设备节点提取容器获得的设备名，排序后便于比较。
func grantedDevices(injection *deviceplugin.ContainerAllocateResponse) []string {
	var names []string
	for k, v := range injection.GetEnvs() {
		if strings.HasSuffix(k, visibleDevicesEnvSuffix) ||
			strings.HasSuffix(k, visibleDevicesEnvSuffix+"S") {
			names = append(names, k+"="+v)
		}
	}
	for _, d := range injection.GetDevices() {
		names = append(names, d.GetHostPath())
	}
	slices.Sort(names)
	return names
}

// describeDevices 用可读文字显示设备清单，清单为空时明确说明。
func describeDevices(names []string) string {
	if len(names) == 0 {
		return "no device at all"
	}
	return strings.Join(names, ", ")
}
