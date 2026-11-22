//go:build !linux

package thead

import (
	"errors"
)

// newMigDriver 在非 Linux 平台上会返回一个空实现。设备管理器仅能在 Linux 上运行，将 cgo 厂商管理绑定层链
// 接到 Darwin 测试二进制文件中时，由于未解析的厂商符号会导致 dyld 加载失败，因此实际驱动程序（
// mig_driver_linux.go）仅适用于 Linux。这个空实现使得该软件包能够构建，并且其无需厂商库的核心组件能够在开
// 发平台上进行测试。
func newMigDriver() migDriver {
	return stubMigDriver{}
}

var errStubMigDriver = errors.New("hgml mig driver is not available on this platform")

type stubMigDriver struct{}

func (stubMigDriver) CardState(_, _ string, _, _ int32) (migCardState, error) {
	return migCardState{}, errStubMigDriver
}

func (stubMigDriver) CreateInstance(_, _ string, _, _ int32, _ migPlacement) (migInstance, error) {
	return migInstance{}, errStubMigDriver
}

func (stubMigDriver) DestroyInstance(string, migInstance) error { return errStubMigDriver }

func (stubMigDriver) InstanceProcesses(string, migInstance) (int, error) { return 0, errStubMigDriver }

func (stubMigDriver) ListInstances() ([]migLiveInstance, error) { return nil, errStubMigDriver }

func (stubMigDriver) CardInstances(string) ([]migInstance, error) { return nil, errStubMigDriver }
