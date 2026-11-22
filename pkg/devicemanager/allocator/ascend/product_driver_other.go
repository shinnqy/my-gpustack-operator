//go:build !linux

package ascend

import (
	"errors"

	klog "k8s.io/klog/v2"

	productascend "gpustack.ai/gpustack/pkg/devicemanager/product/ascend"
)

// newProductDriver 在非 Linux 平台上会返回一个空实现。设备管理器仅能在 Linux 上运行，将 cgo 绑定/ dcmi 链
// 接到 Darwin 测试二进制文件中（该文件会链接 Go 的插件包），但在未解析的 DCMI 符号的 dyld 加载过程中会失
// 败，因此真正的驱动程序（product_driver_linux.go）仅适用于 Linux 环境。这个空实现使得该包能够构建，并且
// 其无 dcmi 调用的调用者能够在 Darwin 环境下进行测试。
func newProductDriver(_ klog.Logger) productascend.Driver {
	return stubProductDriver{}
}

var errStubProductDriver = errors.New("dcmi topology reader is not available on this platform")

type stubProductDriver struct{}

func (stubProductDriver) MainboardID(_, _ int32) (uint32, error) { return 0, errStubProductDriver }

func (stubProductDriver) SuperPodType(_, _ int32) (uint32, error) { return 0, errStubProductDriver }
