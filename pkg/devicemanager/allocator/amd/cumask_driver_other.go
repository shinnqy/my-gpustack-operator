//go:build !linux

package amd

import (
	"errors"
)

// 用编译期类型检查保持两种平台的 readTopology 函数签名一致。
var _ func(string, string) (Topology, error) = readTopology

var errTopologyUnsupported = errors.New("hsa topology reader is not available on this platform")

// 非 Linux 平台返回不支持读取 HSA 拓扑。Device Manager 在 Linux 上使用真正的读取实现。Darwin 测试
// 若链接 binding/hsa，加载时会因找不到 HSA 符号而中止。这里仍可独立测试掩码的整数运算。
func readTopology(_, _ string) (Topology, error) {
	return Topology{}, errTopologyUnsupported
}
