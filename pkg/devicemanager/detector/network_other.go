//go:build !linux

package detector

import (
	"errors"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

// DetectInterfaces 报告称，该工作程序的接口无法枚举。
//
// 它返回错误信息，而非空列表，这种区别至关重要：空库存与真正没有接口的工作者无法区分，因此调用者会保留之
// 前记录的内容，而不是声称该平台无法处理。
func DetectInterfaces() ([]workercore.DeviceInterface, error) {
	return nil, errors.New("the network interface inventory is only available on linux")
}
