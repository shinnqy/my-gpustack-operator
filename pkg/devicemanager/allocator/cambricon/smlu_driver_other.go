//go:build !linux

package cambricon

import (
	"errors"
)

// newSMLUDriver 在非 Linux 平台上会返回一个空实现。设备管理器仅在 Linux 上运行，因此真正的基于 cnDev 的驱
// 动程序（smlu_driver_linux.go）仅适用于 Linux。这个空实现使得该软件包能够构建，并且其无需 cnDev 的核心版
// 本能够在 Darwin 系统上进行测试。
func newSMLUDriver() smluDriver {
	return stubSMLUDriver{}
}

var errStubDriver = errors.New("cndev sMLU driver is not available on this platform")

type stubSMLUDriver struct{}

func (stubSMLUDriver) GetSMLUMode(string) (bool, error) { return false, errStubDriver }

func (stubSMLUDriver) SetSMLUMode(string, bool) error { return errStubDriver }

func (stubSMLUDriver) CreateProfile(string, int, int64) (int32, error) { return 0, errStubDriver }

func (stubSMLUDriver) DestroyProfile(string, int32) error { return errStubDriver }

func (stubSMLUDriver) CreateInstance(string, int32, string) (smluInstance, error) {
	return smluInstance{}, errStubDriver
}

func (stubSMLUDriver) DestroyInstance(_, _ string) error { return errStubDriver }

func (stubSMLUDriver) ListInstances() ([]smluInstance, error) { return nil, errStubDriver }

func (stubSMLUDriver) ListProfiles() ([]profileKey, error) { return nil, errStubDriver }
