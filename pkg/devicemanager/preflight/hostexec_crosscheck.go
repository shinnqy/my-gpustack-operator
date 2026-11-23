package preflight

import (
	"context"
	"fmt"
	"os"
	"strings"

	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/nodefeature"
)

// hostVendorCLI 描述在宿主机上查询某厂商设备清单的方式。
type hostVendorCLI struct {
	name string
	args []string
	// match 标识代表一张物理卡的输出行。逐行匹配可跳过标题和分区等额外行。
	match string
	// mounts 列出容器看到同一批设备所需的挂载，供宿主机和容器结果不一致时排查。
	mounts []string
	// libDir 是厂商驱动库所在目录，用于检测路径存在但无法解析的情况。对未知目录布局保留空值。
	libDir string
}

// hostVendorCLIs 只收录已知输出格式的厂商工具，避免用错误的匹配规则把节点报告成零卡。
var hostVendorCLIs = map[string]hostVendorCLI{
	nodefeature.ManufacturerNVIDIA: {
		// NVIDIA 以物理 GPU 的 UUID 前缀计数。MIG 实例也有 UUID，按普通 UUID 字样匹配会重复计数。
		name: "nvidia-smi", args: []string{"-L"}, match: "(UUID: GPU-",
		mounts: []string{
			"/dev/nvidiactl", "/dev/nvidia<N> for each accelerator",
			"the NVIDIA container runtime, or the toolkit libraries it would have injected",
		},
	},
	nodefeature.ManufacturerAscend: {
		name: "npu-smi", args: []string{"info", "-l"}, match: "NPU ID",
		mounts: []string{
			"/dev/davinci_manager", "/dev/devmm_svm", "/dev/hisi_hdc",
			"/dev/davinci<N> for each accelerator", "/usr/local/Ascend/driver",
		},
	},
	nodefeature.ManufacturerAMD: {
		name: "rocm-smi", args: []string{"--showuniqueid"}, match: "Unique ID:",
		// AMD 除设备节点外还要挂载驱动库，容器可能有设备文件却加载不了厂商库。
		mounts: []string{
			"/dev/kfd", "/dev/dri",
			"the ROCm user-space libraries (/opt/rocm), which nothing injects for you",
		},
		// 使用 Device Manager DaemonSet 挂载的默认 ROCm 前缀。安装在其他位置时需要结合节点配置核对。
		libDir: "/opt/rocm/lib",
	},
}

// crossCheckHost 从已验证的宿主机根目录运行厂商 CLI，再与容器内探测结果比较，帮助定位设备可见性问题。
func crossCheckHost(
	ctx context.Context, host *hostExec, manufacturer string, detection *device.PreflightDetection,
) {
	cli, known := hostVendorCLIs[manufacturer]
	if !known {
		return
	}

	// 用与容器命令相同的转义规则生成可复查的宿主机命令，确保带空格的根目录仍为一个参数。
	view := &device.PreflightHostView{
		Command: shellQuoteJoin(host.Command(cli.name, cli.args...)),
	}
	detection.Host = view

	if err := host.Validate(); err != nil {
		view.Reason = err.Error()
		return
	}
	if !host.Has(ctx, cli.name) {
		view.Reason = "the host carries no " + cli.name + ", so it cannot be asked what it sees"
		return
	}

	out, err := host.Run(ctx, cli.name, cli.args...)
	if err != nil {
		view.Reason = err.Error()
		return
	}

	view.Detail = strings.TrimSpace(string(out))
	view.Accelerators = countMatchingLines(view.Detail, cli.match)

	// 关注宿主机发现设备而容器没有发现的情况。只有实际缺失的挂载才放入排查清单。
	if view.Accelerators <= detection.Accelerators {
		return
	}
	view.MissingMounts = absentHere(cli.mounts)
	if note := unresolvableLibDir(cli.libDir); note != "" {
		view.MissingMounts = append(view.MissingMounts, note)
	}

	// 原本已经读取失败的探测保留最初的原因，避免用另一条“未发现卡”的说明覆盖驱动错误。
	if detection.State == device.PreflightStateUnavailable {
		return
	}

	// 把可见性冲突也写进基础探测状态，使自动化调用能从退出码识别。
	detection.State = device.PreflightStateUnavailable
	detection.Reason = fmt.Sprintf(
		"the host's own %s reports %d accelerator(s) where this container detected %d, so what is "+
			"missing is this container's access to them rather than the hardware",
		cli.name, view.Accelerators, detection.Accelerators,
	)
}

// unresolvableLibDir 指出厂商库目录存在但链接或挂载失效的情况，补充缺少路径列表无法表达的诊断。
func unresolvableLibDir(dir string) string {
	if dir == "" {
		return ""
	}

	// Lstat 读取链接本身，Stat 尝试访问目标，二者结合可区分路径不存在和链接悬空。
	if _, err := os.Lstat(dir); err != nil {
		return ""
	}
	if _, err := os.Stat(dir); err == nil {
		return ""
	}
	target, err := os.Readlink(dir)
	if err != nil {
		return ""
	}

	return dir + " is mounted but does not resolve: it links to " + target +
		", which is not in this container -- mount " + dir +
		" itself, which resolves the link on the host"
}

// countMatchingLines 统计输出中包含指定标记的行数。
func countMatchingLines(out, match string) int {
	var n int
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, match) {
			n++
		}
	}
	return n
}

// absentHere 只保留容器实际缺失的路径，使报告指出真正要补的挂载。
func absentHere(mounts []string) []string {
	var absent []string
	for _, mount := range mounts {
		if strings.HasPrefix(mount, "/") && !strings.Contains(mount, " ") {
			if _, err := os.Stat(mount); err == nil {
				continue
			}
		}
		absent = append(absent, mount)
	}
	return absent
}
