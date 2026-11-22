//go:build !linux

package ascend

import (
	"errors"

	klog "k8s.io/klog/v2"
)

// newShareDriver 在非 Linux 平台上会返回一个空实现。设备管理器仅能在 Linux 上运行，将 cgo 绑定/dcmi 链接
// 到 Darwin 测试二进制文件中（该文件会链接 Go 的插件包），但在处理未解析的 DCMI 符号时会导致 dyld 加载失
// 败，因此真正的驱动程序（share_driver_linux.go）仅适用于 Linux 环境。这个空实现则允许该包在 Darwin 环境
// 下进行构建及其无 DCMI 的注入核心功能的测试。
func newShareDriver(_ klog.Logger) shareDriver {
	return stubShareDriver{}
}

var errStubShareDriver = errors.New("dcmi container-share driver is not available on this platform")

type stubShareDriver struct{}

func (stubShareDriver) GetShareEnabled(_, _ int32) (bool, error) { return false, errStubShareDriver }

func (stubShareDriver) SetShareEnabled(_, _ int32, _ bool) error { return errStubShareDriver }
