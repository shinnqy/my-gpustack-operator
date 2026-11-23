package preflight

import (
	"fmt"
	"strings"

	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/nodefeature"
)

// ascendProbeImages 按 CANN 主版本和芯片系列选择与注入库相配的厂商镜像，否则容器内的诊断程序可能无法装载。
var ascendProbeImages = map[string]string{
	"cann-8-910b": "quay.io/ascend/cann:8.5.0-910b-ubuntu22.04-py3.11",
	"cann-8-910c": "quay.io/ascend/cann:8.5.0-a3-ubuntu22.04-py3.11",
	"cann-9-910b": "quay.io/ascend/cann:9.1.0-910b-ubuntu22.04-py3.12",
	"cann-9-910c": "quay.io/ascend/cann:9.1.0-a3-ubuntu22.04-py3.12",
	"cann-9-950":  "quay.io/ascend/cann:9.1.0-950-ubuntu22.04-py3.12",
	"cann-9-310p": "quay.io/ascend/cann:9.1.0-310p-ubuntu22.04-py3.12",
}

// nvidiaProbeImages 按 CUDA 主版本选择镜像，使容器用户态与挂入的库和监控工具相匹配。
var nvidiaProbeImages = map[string]string{
	"cuda-12": "nvidia/cuda:12.9.2-cudnn-devel-ubi8",
	"cuda-13": "nvidia/cuda:13.0.3-cudnn-devel-ubi8",
}

// amdProbeImage 使用与打包阶段相同的基础系统。AMD 的切片库运行时通过 dlsym 查找 ROCr，无需预装专用基础镜像。
const amdProbeImage = "ubuntu:24.04"

// ResolveProbeImage 优先使用调用方提供的镜像。未指定时按厂商、系列和运行时版本匹配。没有已知镜像则说明缺少的输入。
func ResolveProbeImage(manufacturer, family, runtimeVersion, override string) (string, error) {
	if override != "" {
		return override, nil
	}

	switch manufacturer {
	case nodefeature.ManufacturerAscend:
		dir := ascendCANNDir(runtimeVersion, family)
		if img, ok := ascendProbeImages[dir]; ok {
			return img, nil
		}
		return "", fmt.Errorf(
			"no default probe image for ascend family %q on CANN runtime %q; pass --probe-image",
			family, runtimeVersion,
		)
	case nodefeature.ManufacturerNVIDIA:
		dir := nvidiaCUDADir(runtimeVersion)
		if img, ok := nvidiaProbeImages[dir]; ok {
			return img, nil
		}
		return "", fmt.Errorf(
			"no default probe image for nvidia CUDA runtime %q; pass --probe-image", runtimeVersion,
		)
	case nodefeature.ManufacturerAMD:
		return amdProbeImage, nil
	default:
		return "", fmt.Errorf("no default probe image for manufacturer %q; pass --probe-image", manufacturer)
	}
}

// ascendCANNDir 与 Ascend 分配器的库目录命名相同，以该目录名查找匹配镜像。
func ascendCANNDir(runtimeVersion, family string) string {
	return "cann-" + device.RuntimeMajor(runtimeVersion, "8") + "-" + strings.ToLower(family)
}

// nvidiaCUDADir 从运行时版本提取 NVIDIA 分配器也使用的 CUDA 库目录名称。
func nvidiaCUDADir(runtimeVersion string) string {
	return "cuda-" + device.RuntimeMajor(runtimeVersion, "12")
}
