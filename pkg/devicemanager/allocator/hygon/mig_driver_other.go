//go:build !linux

package hygon

import (
	"errors"
)

// newMigDriver 在非 Linux 平台上会返回一个空实现。设备管理器仅在 Linux 上运行，该包的服务器会链接 Go 的插
// 件包，该插件会在 cgo 厂商绑定下的 dyld 加载过程中终止 Darwin 测试二进制文件——因此真正的驱动程序（
// mig_driver_linux.go）仅适用于 Linux。这个空实现使得该包能够构建其无驱动程序的记录与定位核心功能，并在开
// 发平台上通过表格进行测试。
func newMigDriver() migDriver {
	return stubMigDriver{}
}

var errStubMigDriver = errors.New("hygon multi-instance driver is not available on this platform")

type stubMigDriver struct{}

func (stubMigDriver) CardState(_, _ string) (migCardState, error) {
	return migCardState{}, errStubMigDriver
}

func (stubMigDriver) CreateInstance(_, _ string, _ migPlacement) (migInstance, error) {
	return migInstance{}, errStubMigDriver
}

func (stubMigDriver) DestroyInstance(string, migInstance) error { return errStubMigDriver }

func (stubMigDriver) ListInstances() ([]migLiveInstance, error) { return nil, errStubMigDriver }
