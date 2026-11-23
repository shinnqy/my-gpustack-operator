package preflight

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/deviceplugin"
	"gpustack.ai/gpustack/pkg/nodefeature"
)

// 容器探针分别回答两个问题，切片运行库是否实际生效，以及容器观察到的显存上限是否与分配器相符。
const (
	capSlicedRuntimeLoaded = "sliced-runtime-loaded"
	capSlicedQuotaInForce  = "sliced-quota-in-force"
)

// sliceQuota 取半张卡。容器内的读数应与整卡容量明显不同，两个相同额度的租户也可以同时接受检查。
const sliceQuota = nodefeature.ResourceMaxUnits / 2

// preflightLabel 标记本命令创建的每个容器，进程意外退出后可按标签找到并清理遗留探针。
const preflightLabel = "gpustack.ai/preflight=true"

// sliceProbe 描述厂商切片在容器内的观察方式，包括运行时、配额来源和读取程序。探针核对实际注入的配置。
type sliceProbe struct {
	// Runtime 是该厂商探针需要的 OCI 运行时，使用普通容器运行时即可启动时留空。
	Runtime string
	// LogEnv 调高切片运行库的日志级别以观察其加载过程，读取实际配额时会关掉这些额外日志。
	LogEnv map[string]string
	// Reader 是容器内检查切片的厂商工具或 shell 片段，输出作为证据保存在结果中。
	Reader string
	// LoadEvidence 标识厂商驱动已为探针建立了切片实例。对于未挂载自研动态库的厂商，观察该记录来判断运行库是否生效。
	LoadEvidence string
	// MemoryQuotaEnvPrefix 是注入显存上限的环境变量名前缀，分配器会在后面追加卡号。
	MemoryQuotaEnvPrefix string
	// 另外一些厂商在挂载的文件中写入配额。以下字段指定挂载位置、目录内的文件名和保存配额的键，
	// 如 Ascend 的 memory-quota 或海光的 mem。
	MemoryQuotaConfigMount string
	MemoryQuotaConfigFile  string
	MemoryQuotaConfigKey   string
}

// clientExitMarker 在驱动尚未生成实例记录而后台客户端已退出时标明退出码，便于区分客户端启动失败和驱动未接纳切片。
const clientExitMarker = "gpustack-preflight-client-exit-"

// hygonSliceConf 是容器收到的海光切片配置路径，配额和用于匹配驱动实例的 PCI 地址、pipe ID 均来自这次分配。
const hygonSliceConf = "/etc/vdev/docker/vdev0.conf"

// sliceProbes 登记有容器内观测方法的五个厂商。
var sliceProbes = map[string]sliceProbe{
	nodefeature.ManufacturerNVIDIA: {
		Runtime:              "nvidia",
		LogEnv:               map[string]string{"LIBCUDA_LOG_LEVEL": "3"},
		Reader:               "nvidia-smi --query-gpu=memory.total --format=csv,noheader,nounits",
		MemoryQuotaEnvPrefix: "CUDA_DEVICE_MEMORY_LIMIT_",
	},
	nodefeature.ManufacturerAscend: {
		Runtime:                "ascend",
		LogEnv:                 map[string]string{"ENPU_LOG_LEVEL": "3"},
		Reader:                 "/opt/enpu/vcann-rt/tools/enpu-monitor",
		MemoryQuotaConfigMount: "/etc/enpu/vcann-rt/npu_info.config",
		MemoryQuotaConfigKey:   "memory-quota",
	},
	nodefeature.ManufacturerAMD: {
		LogEnv:               map[string]string{"LIBVROCM_LOG_LEVEL": "2"},
		Reader:               "/usr/local/vrocm/rocm-monitor",
		MemoryQuotaEnvPrefix: "VROCM_DEVICE_MEMORY_LIMIT_",
	},
	nodefeature.ManufacturerTHead: {
		LogEnv:               map[string]string{"LIBHGGC_LOG_LEVEL": "2"},
		Reader:               "/usr/local/vppu/ppu-monitor",
		MemoryQuotaEnvPrefix: "HGGC_DEVICE_MEMORY_LIMIT_",
	},
	// 海光的切片对实际进入 HIP 运行时的进程生效。hy-smi 查询物理卡时仍可能返回整卡容量。
	// 这里启动挂入的 DTK 客户端并设置其库路径，
	// 用配置中的 PCI 地址、pipe ID 和 kfd 中的 GPU ID 定位本次分配自己的驱动记录。对照启动前后的实例标识，
	// 避免用其他租户的记录判为成功。取驱动报告的字节数转换为 MiB，与其余厂商使用同一套配额核对逻辑。客户端提前退出时保留退出状态。
	nodefeature.ManufacturerHygon: {
		Reader: `conf=` + hygonSliceConf + `; ` +
			`set -- $(awk -F'[.: \t]+' '/^PciBusId:/ { print "0x"$3, "0x"$4, $5; exit }' "$conf" 2>/dev/null); ` +
			`loc=$(( ${1:-0} * 256 + ${2:-0} * 8 + ${3:-0} )); ` +
			`pipe=$(awk '/^pipe_id:/ { print $2; exit }' "$conf" 2>/dev/null); mine='*'; ` +
			`[ "$loc" != 0 ] && [ -n "$pipe" ] && ` +
			`for n in /sys/class/kfd/kfd/topology/nodes/*/; do ` +
			`grep -qx "location_id $loc" "$n/properties" 2>/dev/null || continue; ` +
			`g=$(cat "$n/gpu_id" 2>/dev/null); [ "${g:-0}" != 0 ] || continue; ` +
			`mine=$(printf '0x%x@%s' "$g" "$pipe"); break; done; ` +
			`before=$(grep -h '^Indentifier:' /sys/devices/virtual/kfd/kfd/vgpu/*/entry 2>/dev/null); ` +
			`claim=` + coTenancyBarrierDir + `; [ -d "$claim" ] || claim=$(mktemp -d 2>/dev/null || echo /tmp); ` +
			`log=$(mktemp 2>/dev/null || echo /dev/null); ` +
			`LD_LIBRARY_PATH=/opt/hygondriver/hip/lib:/opt/hygondriver/lib:/opt/hyhal/lib ` +
			`/opt/hygondriver/bin/BandwidthTest >"$log" 2>&1 & hip=$!; i=0; new=""; dead=0; ` +
			`while [ -z "$new" ]; do ` +
			`for e in /sys/devices/virtual/kfd/kfd/vgpu/$mine/entry; do ` +
			`[ -r "$e" ] || continue; id=$(grep -h '^Indentifier:' "$e" 2>/dev/null); ` +
			`[ -n "$id" ] || continue; case "$before" in *"$id"*) continue;; esac; ` +
			`mkdir "$claim/claimed-${id#Indentifier:}" 2>/dev/null || continue; new="$e"; break; done; ` +
			`[ -n "$new" ] && break; [ "$dead" = 1 ] && break; ` +
			`kill -0 "$hip" 2>/dev/null || { dead=1; continue; }; ` +
			`[ "$i" -lt 100 ] || break; sleep 0.1; i=$((i+1)); done; ` +
			`if [ -n "$new" ]; then ` +
			`awk -F: '{ print } /^Vram limit/ { printf "Vram limit MiB: %d\n", $2 / 1048576 }' "$new"; ` +
			`elif ! kill -0 "$hip" 2>/dev/null; then ` +
			`wait "$hip" 2>/dev/null; echo ` + clientExitMarker + `$?; tail -n 3 "$log" 2>/dev/null; fi; ` +
			`kill "$hip" 2>/dev/null || true`,
		LoadEvidence:           "Vgpu device:",
		MemoryQuotaConfigMount: filepath.Dir(hygonSliceConf),
		MemoryQuotaConfigFile:  filepath.Base(hygonSliceConf),
		MemoryQuotaConfigKey:   "mem",
	},
}

// stageLibFor 按厂商准备一次预加载库，供本轮该厂商的所有探针共用。通过厂商自身接口取得证据的运行时可能无需复制库。
func (p *Preflighter) stageLibFor(manufacturer string) StageResult {
	probe, probed := sliceProbes[manufacturer]
	if !probed || probe.LoadEvidence != "" || p.dryRun || p.host == nil {
		return StageResult{}
	}
	// 普通目录也可以写入文件。没有核实宿主机根目录时必须跳过复制，避免把容器本地文件当成节点挂载源。
	if err := p.host.Validate(); err != nil {
		return StageResult{Failed: true, Reason: err.Error()}
	}
	return StageLib(p.host.root, manufacturer)
}

// measureSliced 对每张支持逻辑切片的卡分别返回运行库与配额两项结果，同时记录此次环境实际达到的验证层级。
func (p *Preflighter) measureSliced(
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
			if accel.Status.LogicalSliced.Count == 0 {
				continue
			}
			checks = append(checks, p.measureAccelerator(ctx, manufacturer, injector, staged, grp, accel)...)
		}
	}
	return checks
}

// measureAccelerator 在一张卡上逐步生成分配响应、启动探针并核对读数，返回这张卡的两项结论。
func (p *Preflighter) measureAccelerator(
	ctx context.Context,
	manufacturer string,
	injector deviceplugin.AcceleratorInjectionPreflighter,
	staged StageResult,
	grp *workercore.DevicesGroup,
	accel *workercore.Accelerator,
) []device.PreflightCheck {
	injection, err := p.simulateInjection(ctx, manufacturer, injector, grp, accel)
	if err != nil {
		return unreachablePair(accel.ID, device.PreflightStateUnavailable,
			device.PreflightDepthDeclared, "", err.Error())
	}

	probe, known := sliceProbes[manufacturer]
	if !known {
		// 有容器探针时，空分配响应意味着根本没有内容可验证。此处分开处理，避免把一个普通容器的读数当成切片。
		if injectsNothing(injection) {
			return unreachablePair(accel.ID, device.PreflightStateUnavailable,
				device.PreflightDepthSimulated, "",
				"the allocator produced an injection for this slice that carries no environment "+
					"variable, mount, device or annotation, so a container granted it would receive "+
					"nothing that bounds it")
		}
		return unreachablePair(accel.ID, device.PreflightStateOK, device.PreflightDepthSimulated,
			"the allocator produced the slice injection; no container probe has been established for "+
				manufacturer+", so what the slice looks like from inside it was not observed", "")
	}

	image, err := ResolveProbeImage(manufacturer, grp.Family, grp.RuntimeVersion, p.probeImage)
	if err != nil {
		return unreachablePair(accel.ID, device.PreflightStateOK, device.PreflightDepthSimulated,
			"the allocator produced the slice injection; the container step was not taken because "+
				err.Error(), "")
	}

	for k, v := range probe.LogEnv {
		injection.Envs[k] = v
	}
	run, err := emitOrRun(ctx, p.host, p.runtime, p.noRuntime,
		forceEmitReason(p.dryRun, staged), containerRunSpec{
			Image:     image,
			Injection: injection,
			Runtime:   probe.Runtime,
			Label:     preflightLabel,
			Args:      probeShellCommand(probe.Reader, logEnvNames(probe)),
		})
	switch {
	case err != nil:
		// 启动失败时保留已经从分配响应得到的结论、命令和原因。只有实际运行后的表现才能说明切片能力。
		detail := "the allocator produced the slice injection, and the probe container could not be " +
			"started, so nothing was measured: " + err.Error()
		if p.runtime != nil && p.runtime.NetworkWarning != "" {
			detail += ". " + p.runtime.NetworkWarning
		}
		return unreachablePair(accel.ID, device.PreflightStateOK,
			device.PreflightDepthSimulated, detail, "").
			withCommand(run.Command).withEvidence(string(run.Output))
	case run.Emitted:
		detail := "the allocator produced the slice injection; the container step was emitted " +
			"rather than run because " + run.Reason
		if p.dryRun {
			// dry-run 没有把所需文件写到节点上，命令后要说明复现前需要准备哪些目录。
			detail += dryRunRemediation(injection)
		}
		return unreachablePair(accel.ID, device.PreflightStateOK, device.PreflightDepthSimulated,
			detail, "").withCommand(run.Command)
	}

	return judgeProbeOutput(accel.ID, p.host.root, injection, probe, run)
}

// simulateInjection 为一张实际探测到的卡调用厂商自己的分配响应器，取得它在分配时会给容器的注入。
func (p *Preflighter) simulateInjection(
	ctx context.Context,
	manufacturer string,
	injector deviceplugin.AcceleratorInjectionPreflighter,
	grp *workercore.DevicesGroup,
	accel *workercore.Accelerator,
) (*deviceplugin.ContainerAllocateResponse, error) {
	// 一次只传一张卡。把整个组交给响应器会让探针同时获得多张卡，无法把读数对应到结果中的那个设备 ID。
	pod, ctr, devs, allocated, err := sliceRequest(manufacturer, grp, accel)
	if err != nil {
		return nil, err
	}

	injections, err := p.driveResponder(manufacturer, injector,
		func(open responderOpener) ([]*deviceplugin.ContainerAllocateResponse, error) {
			responder, driveErr := open(workercore.DeviceAllocationModeSliced)
			if driveErr != nil {
				return nil, driveErr
			}
			injection, _, driveErr := slicedResponse(ctx, responder, pod, ctr, devs, allocated, nil)
			if driveErr != nil {
				return nil, driveErr
			}
			return []*deviceplugin.ContainerAllocateResponse{injection}, nil
		})
	if err != nil {
		return nil, err
	}
	return injections[0], nil
}

// slicedResponse 按厂商实际实现的入口生成切片响应。实现单独逻辑切片接口时还传入已有占用，保留本次放置结果。
func slicedResponse(
	ctx context.Context,
	responder deviceplugin.ContainerAllocateResponder,
	pod *core.Pod,
	ctr *core.Container,
	devs *workercore.Devices,
	allocated map[deviceplugin.Resource]int32,
	occupied deviceplugin.Placements,
) (*deviceplugin.ContainerAllocateResponse, deviceplugin.Placements, error) {
	logical, ok := responder.(deviceplugin.LogicalSlicedResponder)
	if !ok {
		injection, err := responder.GetContainerAllocateResponse(ctx, pod, ctr, devs, allocated)
		if err != nil {
			return nil, nil, fmt.Errorf("drive the slice responder: %w", err)
		}
		return injection, nil, nil
	}

	placements, err := logical.PlaceLogicalSliced(ctx, pod, ctr, devs, allocated, occupied)
	if err != nil {
		return nil, nil, fmt.Errorf("place the logical slice: %w", err)
	}
	injection, err := logical.GetLogicalSlicedResponse(ctx, pod, ctr, devs, allocated, placements)
	if err != nil {
		return nil, nil, fmt.Errorf("drive the slice responder: %w", err)
	}
	return injection, placements, nil
}

// mergePlacements 合并本次分配占用的区间，使下一次分配能避开此前所有切片。
func mergePlacements(into, add deviceplugin.Placements) deviceplugin.Placements {
	if len(add) == 0 {
		return into
	}
	if into == nil {
		into = deviceplugin.Placements{}
	}
	for res, runs := range add {
		into[res] = append(into[res], runs...)
	}
	return into
}

// sliceRequest 为一张卡的一份逻辑切片构建与生产分配器相同形状的输入。
func sliceRequest(manufacturer string, grp *workercore.DevicesGroup, accel *workercore.Accelerator) (
	*core.Pod, *core.Container, *workercore.Devices, map[deviceplugin.Resource]int32, error,
) {
	one := *grp
	one.Accelerators = []workercore.Accelerator{*accel}

	pod, ctr, devs, allocated, err := deviceplugin.NewPreflightAllocationRequest(
		[]workercore.DevicesGroup{one}, manufacturer, workercore.DeviceAllocationModeSliced, sliceQuota,
	)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("build the allocation request: %w", err)
	}
	ctr.Name = probeContainerName(ctr.Name, accel.ID)
	return pod, ctr, devs, allocated, nil
}

// probeContainerName 将可读的设备标识加入容器名并过滤不适合路径的字符。这样不同加速器生成的配额文件能同时保留。
func probeContainerName(base, acceleratorID string) string {
	folded := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-':
			return r
		default:
			return '-'
		}
	}, acceleratorID)
	if folded == "" {
		return base
	}
	return base + "-" + folded
}

// driveResponder 在临时写入目录中构造实际厂商响应器，取得注入后将挂载路径改写为宿主机可访问的位置。
func (p *Preflighter) driveResponder(
	manufacturer string,
	injector deviceplugin.AcceleratorInjectionPreflighter,
	drive func(open responderOpener) ([]*deviceplugin.ContainerAllocateResponse, error),
) ([]*deviceplugin.ContainerAllocateResponse, error) {
	// 重定向前保存宿主机路径，重定向生效时保存临时路径，便于稍后将注入中的路径还原。
	realLib, preflightPods := deviceplugin.OperatorLibDir, deviceplugin.OperatorPreflightDir

	var (
		restores []func()
		// swaps 保存每次重定向产生的临时路径与宿主机路径对，包括厂商私有路径。
		swaps [][2]string
		// rendered 单独保存临时 Pod 目录。只有响应器在这些目录里生成的文件需要复制到节点。
		rendered []string
	)
	// 先复制并改写路径，最后逆序恢复重定向。提前恢复会连同临时目录一起删除待复制的文件。
	defer func() {
		for i := len(restores) - 1; i >= 0; i-- {
			restores[i]()
		}
	}()

	open := func(mode workercore.DeviceAllocationMode) (deviceplugin.ContainerAllocateResponder, error) {
		responder, restore, err := injector.PreflightResponder(mode)
		if err != nil {
			return nil, fmt.Errorf("build the %s responder: %w", mode, err)
		}
		restores = append(restores, restore)
		rendered = append(rendered, deviceplugin.OperatorPodsDir)
		swaps = append(swaps,
			[2]string{deviceplugin.OperatorLibDir, realLib},
			[2]string{deviceplugin.OperatorPodsDir, preflightPods},
		)
		// 加入厂商自有的挂载路径，例如 NVIDIA HAMi-core 的锁目录。这些位置只有创建重定向的厂商知道。
		for scratch, host := range deviceplugin.PreflightRehosts() {
			swaps = append(swaps, [2]string{scratch, host})
		}
		return responder, nil
	}

	injections, err := drive(open)
	if err != nil {
		return nil, err
	}
	for _, injection := range injections {
		if injection == nil {
			return nil, fmt.Errorf("the %s responder produced no injection", manufacturer)
		}
		if injection.Envs == nil {
			injection.Envs = map[string]string{}
		}
	}

	// 厂商库树由 stageLibFor 单独准备，这里只需处理响应器为本轮分配生成的文件。
	if !p.dryRun {
		for _, scratchPods := range rendered {
			if err = copyTree(scratchPods, filepath.Join(p.host.root, preflightPods)); err != nil &&
				!errors.Is(err, os.ErrNotExist) {
				return nil, fmt.Errorf("stage what the responder rendered onto the host: %w", err)
			}
		}
	}

	for _, injection := range injections {
		for _, m := range injection.GetMounts() {
			m.HostPath = rehostPath(m.GetHostPath(), swaps...)
		}
	}
	return injections, nil
}

// responderOpener 在重定向有效期间按模式构造响应器，使主容器与 sidecar 能取到各自模式的注入。
type responderOpener func(workercore.DeviceAllocationMode) (deviceplugin.ContainerAllocateResponder, error)

// rehostPath 用活动映射将临时路径改写为宿主机路径，匹配时尊重目录边界。
func rehostPath(path string, swaps ...[2]string) string {
	for _, swap := range swaps {
		if rel, err := filepath.Rel(swap[0], path); err == nil && !strings.HasPrefix(rel, "..") {
			return filepath.Join(swap[1], rel)
		}
	}
	return path
}

// probeShellCommand 在同一容器内读取进程映射和厂商工具输出，用标记分隔两段。
// 这样能在一次真实分配中分别核对加载的库和生效的配额。
const (
	mapsBegin = "gpustack-preflight-maps-begin"
	mapsEnd   = "gpustack-preflight-maps-end"
)

// quiet 列出需要在读取配额前取消的日志变量。保留 LD_PRELOAD 使厂商读取工具仍在切片中运行，
// 同时避免日志里的配置数字混入读数。
func probeShellCommand(reader string, quiet []string) []string {
	script := "echo " + mapsBegin + "; cat /proc/self/maps; echo " + mapsEnd
	if reader != "" {
		if len(quiet) != 0 {
			script += "; unset " + strings.Join(quiet, " ")
		}
		script += "; " + reader + " || echo probe-reader-exit-$?"
	}
	return []string{"sh", "-c", "{ " + script + "; } 2>&1"}
}

// logEnvNames 按名称排序日志变量，使同一套注入生成稳定的命令。
func logEnvNames(probe sliceProbe) []string {
	return slices.Sorted(maps.Keys(probe.LogEnv))
}

// 两个租户通过挂载的目录互相表明已就绪。只有确认同时在线后才打印标记并读取设备，否则保留超时结果。
const (
	coTenancyBarrierDir  = "/gpustack-preflight-barrier"
	coTenantsMet         = "gpustack-preflight-co-tenants-met"
	coTenancyBarrierWait = 300 // 等待 30 秒，给另一侧拉取镜像和启动容器留出时间。
)

// coTenantProbeShellCommand 等待名为 peer 的另一个租户到达同步点，再执行本租户的容器内检查。
func coTenantProbeShellCommand(reader string, quiet []string, self, peer string) []string {
	selfPath := coTenancyBarrierDir + "/" + self
	peerPath := coTenancyBarrierDir + "/" + peer

	barrier := ": > " + selfPath + "; i=0; " +
		"while [ ! -e " + peerPath + " ] && [ $i -lt " + strconv.Itoa(coTenancyBarrierWait) + " ]; " +
		"do sleep 0.1; i=$((i+1)); done; " +
		"if [ -e " + peerPath + " ]; then echo " + coTenantsMet + "; fi"

	return []string{"sh", "-c", "{ " + barrier + "; } 2>&1; " + probeShellCommand(reader, quiet)[2]}
}

// probeSections 把动态库映射与厂商工具输出分开。缺少分隔标记时不跨段取值，以免误读日志。
func probeSections(evidence string) (mappings, readerOut string) {
	begin := strings.Index(evidence, mapsBegin)
	if begin < 0 {
		return "", ""
	}
	rest := evidence[begin+len(mapsBegin):]

	end := strings.Index(rest, mapsEnd)
	if end < 0 {
		return "", ""
	}
	return rest[:end], rest[end+len(mapsEnd):]
}

// judgeProbeOutput 将探针输出与本次注入的动态库和显存配额对照，分别给出是否实际加载、配额是否可见的结论。
func judgeProbeOutput(
	acceleratorID, hostRoot string,
	injection *deviceplugin.ContainerAllocateResponse,
	probe sliceProbe,
	run emitResult,
) []device.PreflightCheck {
	evidence := string(run.Output)
	mappings, readerOut := probeSections(evidence)
	// 需要驱动实例记录的厂商，两个检查都要用到这项证据。未进入切片时读到的值无法说明切片配额。
	noRecord := probe.LoadEvidence != "" && !strings.Contains(readerOut, probe.LoadEvidence)

	loaded := device.PreflightCheck{
		Accelerator: acceleratorID,
		Capability:  capSlicedRuntimeLoaded,
		Mode:        device.PreflightModeOf(workercore.DeviceAllocationModeSliced),
		State:       device.PreflightStateOK,
		Depth:       device.PreflightDepthMeasured,
		Command:     run.Command,
		Evidence:    evidence,
	}
	switch missing := unloadedObjects(injection, mappings); {
	case probe.LoadEvidence != "":
		// 该厂商不注入本项目的共享库，改用厂商读取程序输出的驱动记录验证切片生效。
		switch died := clientExit(readerOut); {
		case noRecord && run.ExitError != "":
			// 如果容器也异常退出，优先报告退出错误，使缺少驱动记录有可追查的原因。
			loaded.State = device.PreflightStateUnavailable
			loaded.Reason = "the container exited non-zero and the driver recorded no slicing " +
				"instance for it, so nothing established whether the vendor runtime takes the " +
				"slice up: " + run.ExitError
		case noRecord && died != "":
			// 即使探针 shell 正常收尾，里面的客户端仍可能提前失败。单独读取客户端退出标记。
			loaded.State = device.PreflightStateUnavailable
			loaded.Reason = "the client this probe starts to enter the slice exited with status " +
				died + " and the driver recorded no slicing instance for it, so nothing " +
				"established whether the vendor runtime takes the slice up"
		case noRecord:
			loaded.State = device.PreflightStateUnavailable
			loaded.Reason = "the allocator produced a slice for this accelerator and the driver " +
				"recorded no slicing instance for the container, so the vendor runtime did not " +
				"take the slice up"
		case run.ExitError != "":
			loaded.State = device.PreflightStateUnavailable
			loaded.Reason = "the driver recorded a slicing instance for the container, and the " +
				"container then exited non-zero under it: " + run.ExitError
		default:
			loaded.Detail = "the driver records a slicing instance for the container, so the " +
				"vendor runtime took the slice up"
		}
	case len(missing) == 0 && injectedObjects(injection) == 0:
		// 卡已声明支持逻辑切片，但分配结果没有带可加载的共享库，需要明确报告注入异常。
		loaded.State = device.PreflightStateUnavailable
		loaded.Reason = "the allocator produced a slice for this accelerator that mounts no shared " +
			"object, so there is no slicing runtime for the container to load"
	case len(missing) != 0:
		loaded.State = device.PreflightStateUnavailable
		loaded.Reason = "the container ran without " + strings.Join(missing, ", ") +
			" in its address space, so the injection mounted a slicing runtime that did not load"
	case run.ExitError != "":
		// 库已加载但容器在切片环境中异常退出，保留两件事及其错误原因。
		loaded.State = device.PreflightStateUnavailable
		loaded.Reason = "the slicing runtime the injection mounts loaded, and the container then " +
			"exited non-zero under it: " + run.ExitError
	default:
		loaded.Detail = "the slicing runtime the injection mounts is loaded in the container"
	}

	quota := device.PreflightCheck{
		Accelerator: acceleratorID,
		Capability:  capSlicedQuotaInForce,
		Mode:        device.PreflightModeOf(workercore.DeviceAllocationModeSliced),
		State:       device.PreflightStateOK,
		Depth:       device.PreflightDepthMeasured,
		Command:     run.Command,
		Evidence:    evidence,
	}
	limit, absent := memoryQuota(injection, probe, hostRoot)
	switch {
	case run.ExitError != "":
		// 配额读数来自同一容器，异常退出前出现的数字也不能单独证明探针成功。
		quota.State = device.PreflightStateUnavailable
		quota.Reason = "the container exited non-zero under the slicing runtime the injection " +
			"mounts, so whatever it reported about its cap was not observed to hold: " + run.ExitError
	case limit == "":
		// 注入应当携带可读取的配额。找不到配置上限时无法对照容器读数，需要报告原因。
		quota.State = device.PreflightStateUnavailable
		quota.Reason = "the allocator produced a slice whose per-accelerator memory cap this run " +
			"could not read, so nothing observed bounds this container -- " + absent
	case noRecord:
		// 驱动没有为容器登记切片时，输出里的数字不能作为该切片的显存配额。
		quota.Depth = device.PreflightDepthSimulated
		quota.Detail = "the injection asks for a cap of " + limit + " on this accelerator and the " +
			"driver recorded no slicing instance for the container, so nothing it printed was " +
			"printed inside a slice and the cap was not observed in force; the container's own " +
			"output is carried as evidence"
	case !reportsFigure(readerOut, limit):
		quota.Depth = device.PreflightDepthSimulated
		quota.Detail = "the injection caps this accelerator at " + limit +
			" and the container did not report that figure back, so the cap is applied but was not " +
			"observed in force; the container's own output is carried as evidence"
	default:
		quota.Detail = "the container reports " + limit +
			", which is the cap the injection set rather than the whole accelerator"
	}

	return []device.PreflightCheck{loaded, quota}
}

// injectsNothing 检查设备插件能使用的所有注入渠道是否都为空。
func injectsNothing(injection *deviceplugin.ContainerAllocateResponse) bool {
	return len(injection.GetEnvs()) == 0 &&
		len(injection.GetMounts()) == 0 &&
		len(injection.GetDevices()) == 0 &&
		len(injection.GetAnnotations()) == 0
}

// injectedObjects 统计注入挂载的共享库对象。
func injectedObjects(injection *deviceplugin.ContainerAllocateResponse) int {
	var n int
	for _, m := range injection.GetMounts() {
		if strings.HasSuffix(m.GetContainerPath(), ".so") {
			n++
		}
	}
	return n
}

// unloadedObjects 对照进程映射，列出虽然已挂载、但尚未出现在进程地址空间的共享库。
func unloadedObjects(injection *deviceplugin.ContainerAllocateResponse, mappings string) []string {
	mapped := sets.New[string]()
	for line := range strings.SplitSeq(mappings, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		mapped.Insert(fields[len(fields)-1])
	}

	var missing []string
	for _, m := range injection.GetMounts() {
		path := m.GetContainerPath()
		if strings.HasSuffix(path, ".so") && !mapped.Has(path) {
			missing = append(missing, path)
		}
	}
	return missing
}

// dryRunRemediation 列出只打印命令时尚未写入节点、但该命令将要挂载的宿主机目录。
func dryRunRemediation(injections ...*deviceplugin.ContainerAllocateResponse) string {
	mountsFrom := func(root string) bool {
		for _, injection := range injections {
			for _, m := range injection.GetMounts() {
				if strings.HasPrefix(m.GetHostPath(), root) {
					return true
				}
			}
		}
		return false
	}

	var named []string
	for _, root := range []string{
		deviceplugin.OperatorLibDir,
		filepath.Join(deviceplugin.OperatorPreflightDir, string(deviceplugin.PreflightPodUID)),
	} {
		if mountsFrom(root) {
			named = append(named, root)
		}
	}
	if len(named) == 0 {
		return ""
	}
	return ". The command mounts from " + strings.Join(named, " and ") +
		", which a dry run deliberately does not write: re-run without --dry-run, which writes it"
}

// clientExit 从厂商读取程序的输出中取出后台客户端退出状态，没有标记时返回空值。
func clientExit(readerOut string) string {
	_, rest, found := strings.Cut(readerOut, clientExitMarker)
	if !found {
		return ""
	}
	status, _, _ := strings.Cut(rest, "\n")
	return strings.TrimSpace(status)
}

// reportsFigure 按数字边界查找读数，避免把一个较长数值的一部分当成期望的显存上限。
func reportsFigure(readerOut, figure string) bool {
	if figure == "" {
		return false
	}
	for i := 0; ; {
		j := strings.Index(readerOut[i:], figure)
		if j < 0 {
			return false
		}
		start := i + j
		end := start + len(figure)
		beforeOK := start == 0 || !isASCIIDigit(readerOut[start-1])
		afterOK := end == len(readerOut) || !isASCIIDigit(readerOut[end])
		if beforeOK && afterOK {
			return true
		}
		i = start + 1
	}
}

func isASCIIDigit(b byte) bool { return b >= '0' && b <= '9' }

// memoryQuota 从该厂商实际注入的环境变量或配置文件读取单卡上限，转换成能与工具输出对照的数字。
func memoryQuota(
	injection *deviceplugin.ContainerAllocateResponse, probe sliceProbe, hostRoot string,
) (limit, absent string) {
	if probe.MemoryQuotaEnvPrefix != "" {
		for k, v := range injection.GetEnvs() {
			if strings.HasPrefix(k, probe.MemoryQuotaEnvPrefix) {
				return strings.TrimRight(v, "mMbBiI"), ""
			}
		}
		return "", "the injection carries no " + probe.MemoryQuotaEnvPrefix +
			"* variable, which is where this manufacturer's allocator puts the cap"
	}
	if probe.MemoryQuotaConfigMount == "" {
		return "", "this package knows no carrier for this manufacturer's cap"
	}

	for _, m := range injection.GetMounts() {
		if m.GetContainerPath() != probe.MemoryQuotaConfigMount {
			continue
		}
		// 有的厂商直接挂载配置文件，有的挂载所在目录。仅在目录形式下追加配置文件名。
		path := filepath.Join(hostRoot, m.GetHostPath())
		named := probe.MemoryQuotaConfigMount
		if probe.MemoryQuotaConfigFile != "" {
			path = filepath.Join(path, probe.MemoryQuotaConfigFile)
			named = filepath.Join(named, probe.MemoryQuotaConfigFile)
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return "", "the injection mounts " + named +
				", and the host copy it mounts from could not be read: " + err.Error()
		}
		for _, line := range strings.Split(string(body), "\n") {
			key, value, ok := cutConfigField(line)
			if !ok || key != probe.MemoryQuotaConfigKey {
				continue
			}
			// 配置可能包含 MiB 等单位，工具读数使用裸数字，因此只取数值部分。
			figure, _, _ := strings.Cut(value, " ")
			return figure, ""
		}
		return "", "the injection mounts " + named +
			", and it names no " + probe.MemoryQuotaConfigKey
	}
	return "", "the injection mounts no " + probe.MemoryQuotaConfigMount +
		", which is where this manufacturer's allocator renders the cap"
}

// cutConfigField 支持厂商配置使用的两种键值分隔符，返回规范化的键和值。
func cutConfigField(line string) (key, value string, ok bool) {
	line = strings.TrimSpace(line)
	eq, colon := strings.Index(line, "="), strings.Index(line, ":")
	switch {
	case eq < 0 && colon < 0:
		return "", "", false
	case eq >= 0 && (colon < 0 || eq < colon):
		key, value, ok = strings.Cut(line, "=")
	default:
		key, value, ok = strings.Cut(line, ":")
	}
	return strings.TrimSpace(key), strings.TrimSpace(value), ok
}

// checkPair 保存同一张卡的运行库与配额两行结果，使各个验证层级的报告形状一致。
type checkPair []device.PreflightCheck

// withCommand 把可复现的命令同时写入两项结果，包括无法实际启动容器的情况。
func (p checkPair) withCommand(command string) checkPair {
	for i := range p {
		p[i].Command = command
	}
	return p
}

// withEvidence 把容器已经输出的证据保存在两项结果中，也保留未执行完就退出的现场输出。
func (p checkPair) withEvidence(evidence string) []device.PreflightCheck {
	if evidence == "" {
		return p
	}
	for i := range p {
		p[i].Evidence = evidence
	}
	return p
}

// unreachablePair 在无法进一步运行探针时为两项检查保存同一层级及其原因。
func unreachablePair(
	acceleratorID string, state device.PreflightState, depth device.PreflightDepth, detail, reason string,
) checkPair {
	pair := make(checkPair, 0, 2)
	for _, capability := range []string{capSlicedRuntimeLoaded, capSlicedQuotaInForce} {
		pair = append(pair, device.PreflightCheck{
			Accelerator: acceleratorID,
			Capability:  capability,
			Mode:        device.PreflightModeOf(workercore.DeviceAllocationModeSliced),
			State:       state,
			Depth:       depth,
			Detail:      detail,
			Reason:      reason,
		})
	}
	return pair
}

// sweepRenderedArtifacts 删除本轮响应器写到宿主机专用预检目录的文件。真实 Pod 的分配目录独立，不依赖此次清理。
func (p *Preflighter) sweepRenderedArtifacts() {
	if p.dryRun || p.host == nil || p.host.Validate() != nil {
		return
	}
	dir := filepath.Join(p.host.root, deviceplugin.OperatorPreflightDir, string(deviceplugin.PreflightPodUID))
	if err := os.RemoveAll(dir); err != nil {
		logger.Error(err, "could not remove what this pass rendered", "path", dir)
	}
}

// sweepStaleContainers 先清理以前带有预检标签的容器，再启动本轮探针。运行时清理失败要记录，
// 否则被遗留容器占用的卡可能被误判。
func (p *Preflighter) sweepStaleContainers(ctx context.Context) {
	if p.runtime == nil || p.dryRun || p.host.Validate() != nil {
		return
	}

	// 检查可能持有旧探针的所有运行时，因为上一次运行可能通过 --runtime 选用了另一种工具。
	current, _ := probeRuntimeFor(p.runtime, "")

	for _, name := range []string{"docker", "nerdctl"} {
		if !p.host.Has(ctx, name) {
			continue
		}
		// 当前选中的运行时使用已解析的 socket 和命名空间，在同一个位置查找旧容器。
		rt := p.runtime
		if name != current {
			// 对其余运行时按本命令原本会使用的连接方式查找，覆盖以前显式指定运行时的情况。
			rt = p.host.describeRuntime(name, "")
		}

		err := p.sweepWith(ctx, name, rt)
		if err == nil {
			continue
		}

		// 对未被选用且完全不可达的运行时，区分无法查询守护进程和已找到容器却清理失败的情况。
		if name != current && errors.Is(err, errSweepUnreachable) {
			logger.Info("skipped the stale-container sweep of a runtime this host cannot reach, "+
				"which is therefore a runtime no earlier pass started a probe through",
				"runtime", name, "error", err.Error())
			continue
		}
		// 同时写日志并保存清理失败的原因。即使本轮没有探测到卡，没有检查行可以附加，日志中也能找到。
		logger.Error(err, "could not sweep the containers an earlier preflight may have left behind",
			"runtime", name)
		p.sweepFailures = append(p.sweepFailures, fmt.Sprintf("%s: %s", name, err.Error()))
	}
}

// errSweepUnreachable 表示没能从运行时查询到清单，用来区分查询失败和已确认残留容器却未能删除。
var errSweepUnreachable = errors.New("this runtime could not be asked what it is holding")

// sweepWith 向指定运行时查询并回收带本命令标签的容器，不能完成时保留出错的步骤。
func (p *Preflighter) sweepWith(ctx context.Context, name string, rt *hostRuntime) error {
	args := sweepRuntimeArgs(rt)

	out, err := p.host.Run(ctx, name,
		append(slices.Clone(args), "ps", "-aq", "--filter", "label="+preflightLabel)...)
	if err != nil {
		return fmt.Errorf("%w: %w", errSweepUnreachable, err)
	}

	ids := strings.Fields(string(out))
	if len(ids) == 0 {
		return nil
	}

	if _, err := p.host.Run(ctx, name,
		append(append(slices.Clone(args), "rm", "-f"), ids...)...); err != nil {
		return fmt.Errorf("could not remove %d stale container(s) it is holding: %w", len(ids), err)
	}
	logger.Info("removed containers an earlier preflight left behind",
		"runtime", name, "containers", len(ids))
	return nil
}

// sweepRuntimeArgs 为 containerd CLI 生成连接 socket 和命名空间参数，
// Docker 使用宿主机配置自行连接。
func sweepRuntimeArgs(rt *hostRuntime) []string {
	var args []string
	if rt.Socket != "" {
		args = append(args, "--address", rt.Socket)
	}
	if rt.Namespace != "" {
		args = append(args, "--namespace", rt.Namespace)
	}
	return args
}
