//go:build !linux

package binding

import (
	"runtime"
)

func getCPUSize() int {
	return runtime.NumCPU()
}

func getNumaNodeSize() int {
	return 1
}

func getCPUNumaNodeMapping() []int {
	mapping := make([]int, cpuSize)
	for i := 0; i < cpuSize; i++ {
		mapping[i] = 0
	}

	return mapping
}

// 非 Linux 平台缺少对应的 sysfs 数据，返回空字符串表示 NUMA 位置未知，避免将所有设备都归到节点 0。
func getNumaNodeByBDF(bdf string) string {
	return ""
}

func getPCIDevices(vendors, classPrefixes []string) PCIDevices {
	return PCIDevices{}
}

func getPCIDeviceNames(vendors []string) PCIDeviceNames {
	return PCIDeviceNames{}
}

func getLibFromEnv(libName string) string {
	return ""
}

func getLibFromLdCache(libName string) string {
	return ""
}
