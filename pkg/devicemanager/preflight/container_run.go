package preflight

import (
	"bytes"
	"context"
	"errors"
	"maps"
	"os/exec"
	"slices"
	"strings"

	"gpustack.ai/gpustack/pkg/deviceplugin"
)

// containerRunSpec 汇总探针容器的镜像、分配器返回的注入内容和容器内命令。所有数据来自这一次待验证的分配。
type containerRunSpec struct {
	// Image 是探针容器使用的镜像。
	Image string
	// Injection 包含环境变量、挂载、设备和注解。容器运行时看到的是宿主机路径，临时文件必须先发布到宿主机的预检目录。
	Injection *deviceplugin.ContainerAllocateResponse
	// Runtime 是启动容器时选用的厂商 OCI 运行时，例如 NVIDIA 或 Ascend。调用它的宿主机 CLI 另行选择。
	Runtime string
	// Label 为所有探针容器设置相同的键值标记，以便下次预检识别上次中断后遗留的容器。
	Label string
	// Args 是镜像名之后传入的容器内命令及参数。
	Args []string
}

// emitResult 保存实际运行得到的结果。环境不支持执行时则保存可复现的命令及原因。
type emitResult struct {
	// Command 对每一个参数做 shell 引用，保存可原样执行的完整命令。
	Command string
	// Emitted 表示只输出了命令。单凭环境无法执行探针不能判定卡分配失败。
	Emitted bool
	// Reason 在只输出命令时说明原因。
	Reason string
	// Output 保存实际运行后取得的探针输出。
	Output []byte
	// ExitError 保存容器启动后异常退出的错误，用来区分运行时无法创建容器和探针在容器内失败。
	ExitError string
}

// buildContainerRunArgv 把注入的环境、挂载和设备转换为指定 CLI 的参数，
// 连同 socket、命名空间和自动清理选项组成一次性探针命令。
func buildContainerRunArgv(rtName, address, namespace string, spec containerRunSpec) []string {
	argv := []string{rtName}
	if address != "" {
		argv = append(argv, "--address", address)
	}
	if namespace != "" {
		argv = append(argv, "--namespace", namespace)
	}
	argv = append(argv, "run", "--rm")
	argv = append(argv, vendorRuntimeArgs(rtName, spec.Runtime)...)
	if spec.Label != "" {
		argv = append(argv, "--label", spec.Label)
	}

	injection := spec.Injection
	if injection == nil {
		injection = &deviceplugin.ContainerAllocateResponse{}
	}

	for _, k := range slices.Sorted(maps.Keys(injection.GetEnvs())) {
		argv = append(argv, "-e", k+"="+injection.GetEnvs()[k])
	}

	mounts := append([]*deviceplugin.Mount(nil), injection.GetMounts()...)
	slices.SortFunc(mounts, func(a, b *deviceplugin.Mount) int {
		return strings.Compare(a.GetContainerPath(), b.GetContainerPath())
	})
	for _, m := range mounts {
		v := m.GetHostPath() + ":" + m.GetContainerPath()
		if m.GetReadOnly() {
			v += ":ro"
		}
		argv = append(argv, "-v", v)
	}

	devices := append([]*deviceplugin.DeviceSpec(nil), injection.GetDevices()...)
	slices.SortFunc(devices, func(a, b *deviceplugin.DeviceSpec) int {
		return strings.Compare(a.GetContainerPath(), b.GetContainerPath())
	})
	for _, d := range devices {
		v := d.GetHostPath() + ":" + d.GetContainerPath()
		if d.GetPermissions() != "" {
			v += ":" + d.GetPermissions()
		}
		argv = append(argv, "--device", v)
	}

	for _, k := range slices.Sorted(maps.Keys(injection.GetAnnotations())) {
		argv = append(argv, "--annotation", k+"="+injection.GetAnnotations()[k])
	}

	argv = append(argv, spec.Image)
	argv = append(argv, spec.Args...)
	return argv
}

// dryRunReason 是预演模式下输出命令的原因。
const dryRunReason = "this is a dry run"

// forceEmitReason 汇总执行前就能确定的限制，例如 dry-run 或库文件未就绪。存在限制时输出命令和原因。
func forceEmitReason(dryRun bool, staged StageResult) string {
	switch {
	case dryRun:
		return dryRunReason
	case staged.Failed:
		return "the libraries the command mounts could not be staged onto the host: " + staged.Reason
	}
	return ""
}

// probeRuntimeFor 选择探针实际使用或报告中展示的 CLI。当前 CLI 无法传递厂商运行时和设备时，返回替代命令及原因。
func probeRuntimeFor(rt *hostRuntime, vendorRuntime string) (name, emitReason string) {
	name = rt.Name
	if name == "ctr" {
		name = "nerdctl"
		emitReason = "the resolved container runtime is ctr, which has no flag that passes a device " +
			"node to a container; the command above takes the step with nerdctl against the same containerd"
		if rt.NerdctlAbsent {
			// 走到 ctr 是因为机器上没有 nerdctl。输出使用 nerdctl 的命令时，需要说明宿主机缺少它。
			emitReason += " -- which this host does not carry, since that is why ctr was resolved: " +
				"install nerdctl to take the step, or take it from a host that has one"
		}
	}
	if _, ok := vendorRuntimeFlags[[2]string{name, vendorRuntime}]; vendorRuntime != "" && !ok {
		// 替代命令采用 Docker 参数格式，命令名也要写 docker，以保留厂商运行时选项。
		return "docker", "the resolved container runtime is " + rt.Name + ", which has no way to hand a " +
			"container to the " + vendorRuntime + " runtime this manufacturer's injection relies on for " +
			"its user-space driver; the command above is written for docker, which does"
	}
	return name, emitReason
}

// vendorRuntimeFlags 按宿主机 CLI 和厂商 OCI 运行时的组合给出对应的启动参数。
var vendorRuntimeFlags = map[[2]string][]string{
	{"docker", "nvidia"}:  {"--runtime", "nvidia"},
	{"docker", "ascend"}:  {"--runtime", "ascend"},
	{"nerdctl", "nvidia"}: {"--gpus", "all"},
}

// vendorRuntimeArgs 返回向该 CLI 指定厂商运行时所需的选项。厂商无需专用运行时时返回空列表。
func vendorRuntimeArgs(rtName, vendorRuntime string) []string {
	if vendorRuntime == "" {
		return nil
	}
	return vendorRuntimeFlags[[2]string{rtName, vendorRuntime}]
}

// emitOrRun 只构建一次命令。有条件时从宿主机执行，否则在报告中给出同一条可复现命令及原因。
func emitOrRun(
	ctx context.Context, host *hostExec, rt *hostRuntime, noRuntime, forceReason string,
	spec containerRunSpec,
) (emitResult, error) {
	rtName, address, namespace := hostRuntimes[0], "", ""
	var warning, cannotRun string
	if rt != nil {
		address, namespace, warning = rt.Socket, rt.Namespace, rt.NetworkWarning
		rtName, cannotRun = probeRuntimeFor(rt, spec.Runtime)

		// 切换到 Docker 命令时一并清除 containerd 专用的 socket 和命名空间参数。
		if !slices.Contains(containerdRuntimes, rtName) {
			address, namespace = "", ""
		}
	}

	argv := buildContainerRunArgv(rtName, address, namespace, spec)
	printed := shellQuoteJoin(host.Command(argv[0], argv[1:]...))
	if warning != "" {
		printed = "# " + warning + "\n" + printed
	}

	// 先记录运行时降级原因，使 dry-run 时也能解释为什么输出的 CLI 与节点上检测到的不同。
	var fallback string
	switch {
	case rt == nil:
		if noRuntime == "" {
			noRuntime = "no container runtime was found on the host: probed " + strings.Join(hostRuntimes, ", ")
		}
		fallback = noRuntime + "; the command above assumes " + rtName
	case cannotRun != "":
		fallback = cannotRun
	}

	switch {
	case forceReason != "":
		reason := forceReason
		if fallback != "" {
			reason += ", and it would have been emitted regardless: " + fallback
		}
		return emitResult{Command: printed, Emitted: true, Reason: reason}, nil
	case fallback != "":
		return emitResult{Command: printed, Emitted: true, Reason: fallback}, nil
	}

	if err := host.Validate(); err != nil {
		// 无法进入宿主机根目录时，输出命令和原因即可，调用方可继续报告其他项目。
		return emitResult{Command: printed, Emitted: true, Reason: err.Error()}, nil //nolint:nilerr
	}

	out, err := host.Run(ctx, argv[0], argv[1:]...)
	res := emitResult{Command: printed, Output: out}
	if err == nil {
		return res, nil
	}
	if containerRan(rtName, out, err) {
		res.ExitError = err.Error()
		return res, nil
	}
	return res, err
}

// runtimeRefusals 保存各容器运行时用于拒绝启动容器的退出码，方便区分环境错误和容器内错误。
var runtimeRefusals = map[string]int{
	"docker":  125,
	"nerdctl": 1,
}

// containerRan 通过探针输出标记和 CLI 退出码判断容器是否曾启动。只有已经运行的探针才能提供卡能力的证据。
func containerRan(runtime string, out []byte, err error) bool {
	if bytes.Contains(out, []byte(mapsBegin)) {
		return true
	}

	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		// 宿主机连命令都没能启动时，错误不含容器退出状态。
		return false
	}

	refusal, known := runtimeRefusals[runtime]
	if !known {
		// 对尚不清楚退出码约定的运行时，只有探针输出标记能证明容器启动过。
		return false
	}
	return exitErr.ExitCode() != refusal
}

// shellQuoteJoin 将每个参数转义后拼成一行，保留空格、引号等字符的原意。
func shellQuoteJoin(argv []string) string {
	quoted := make([]string, len(argv))
	for i, a := range argv {
		quoted[i] = shellQuote(a)
	}
	return strings.Join(quoted, " ")
}

// shellSafe 列出可在 shell 单词中直接使用的字符。
const shellSafe = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-./:=@"

// shellQuote 使用 POSIX 单引号形式保护含特殊字符的参数，包括空参数和内嵌单引号。
func shellQuote(s string) string {
	if s != "" && strings.Trim(s, shellSafe) == "" {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
