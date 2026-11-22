//go:build !linux

package nvidia

import (
	"errors"
)

// newMigDriver 在非 Linux 平台上会返回一个空实现。设备管理器仅能在 Linux 上运行，将 cgo 绑定/ nvml 链接到
// Darwin 测试二进制文件中（该文件会链接 Go 的插件包）时，由于未解析的 NVML 符号在 dyld 加载过程中会失败，
// 因此真正的驱动程序（mig_driver_linux.go）仅适用于 Linux 环境。这个空实现使得该包能够构建，并且其无 NVML
// 功能的核心模块能够在 Darwin 环境下进行测试。
func newMigDriver() migDriver {
	return stubMigDriver{}
}

var errStubMigDriver = errors.New("nvml mig driver is not available on this platform")

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
