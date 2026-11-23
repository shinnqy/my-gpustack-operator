package ascend

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"gpustack.ai/gpustack/pkg/device"
)

// dockerRuntimeCapability 使用厂商安装包的名称标识 Ascend Docker Runtime 检查项。
const dockerRuntimeCapability = "ascend-docker-runtime"

// dockerRuntimeInstallInfo 是安装程序记录版本号的文件。运行时包装器的 --version 会调用底层容器工具，
// 不能据此识别厂商版本。
const dockerRuntimeInstallInfo = "/usr/local/Ascend/Ascend-Docker-Runtime/ascend_docker_runtime_install.info"

// dockerRuntimeMajor950 是支持 A5 的首个 MindCluster 主版本。
// 只比较主版本也能兼容含 RC 等字符串的发行版本号。
const dockerRuntimeMajor950 = 26

// checkDockerRuntime 检查 A5 节点的厂商容器运行时版本。旧版能传入普通设备节点，但不具备 A5 所需的设备处理能力。
func checkDockerRuntime(path string) device.PreflightCheck {
	c := device.PreflightCheck{
		Capability: dockerRuntimeCapability,
		State:      device.PreflightStateUnavailable,
		Depth:      device.PreflightDepthDeclared,
	}

	version, err := readDockerRuntimeVersion(path)
	if err != nil {
		c.Reason = fmt.Sprintf("the installed ascend-docker-runtime version could not be read, so "+
			"whether this node serves A5 is unknown: %v", err)
		return c
	}

	major, err := mindClusterMajor(version)
	if err != nil {
		c.Reason = fmt.Sprintf("%s records an ascend-docker-runtime version this cannot read, so "+
			"which MindCluster release is installed is unknown: %v", path, err)
		return c
	}

	if major < dockerRuntimeMajor950 {
		c.Reason = fmt.Sprintf("ascend-docker-runtime %s predates MindCluster %d.0.0, where A5 "+
			"support landed: it maps the Ascend950 chip name to no device type it knows, so it falls "+
			"back to a manager-device list carrying /dev/devmm_svm, which A5 does not have -- an "+
			"allocation on this accelerator fails at container creation, naming that device rather "+
			"than this version", version, dockerRuntimeMajor950)
		return c
	}

	c.State = device.PreflightStateOK
	c.Detail = fmt.Sprintf("ascend-docker-runtime %s is MindCluster %d.0.0 or newer, so it resolves "+
		"ASCEND_VISIBLE_DEVICES to this node's A5 device nodes and mounts the UB fabric beside them",
		version, dockerRuntimeMajor950)
	return c
}

// readDockerRuntimeVersion 从安装信息的键值对中读取 version，原样保留例如 v7.3.0 的版本串。
func readDockerRuntimeVersion(path string) (string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}

	for _, line := range strings.Split(string(content), "\n") {
		key, value, found := strings.Cut(line, "=")
		if found && strings.TrimSpace(key) == "version" {
			return strings.TrimSpace(value), nil
		}
	}

	return "", fmt.Errorf("%s carries no version entry", path)
}

// mindClusterMajor 提取 MindCluster 版本串的主版本号，忽略后续版本段和预发行标记。
func mindClusterMajor(version string) (int, error) {
	major, _, _ := strings.Cut(strings.TrimPrefix(version, "v"), ".")

	n, err := strconv.Atoi(major)
	if err != nil {
		return 0, fmt.Errorf("%q begins with no release number", version)
	}

	return n, nil
}
