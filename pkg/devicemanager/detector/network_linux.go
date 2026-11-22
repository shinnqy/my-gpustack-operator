package detector

import (
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

// sysfsRoot 是内核暴露设备模型及接口清单的入口位置。
const sysfsRoot = "/sys"

// DetectInterfaces 报告此工作进程的网络接口，每个接口都包含其 RDMA 链接状态。
//
// 根目录的命名是平台特定部分的全部内容。所有用于解析其下内容的代码都是有意实现与平台无关的：此文件无法在
// Linux 上编译，因此任何放入此处的内容也会被编译器、检查器以及开发平台上的测试所忽略。
func DetectInterfaces() ([]workercore.DeviceInterface, error) {
	return enumerateInterfaces(sysfsRoot)
}
