package ascend

import (
	"fmt"

	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/binding"
	"gpustack.ai/gpustack/binding/dcmi"
	productascend "gpustack.ai/gpustack/pkg/devicemanager/product/ascend"
)

// newProductDriver 返回真正的基于 dcmi 的产品读取器。它仅适用于 Linux，原因与 newShareDriver 相同：设备管
// 理器仅在 Linux 上运行，将 cgo 绑定/ dcmi 链接到 Darwin 测试二进制文件中会导致 dyld 在加载未解析的 DCMI
// 符号时失败，因此 Darwin 构建版本使用 product_driver_other.go 中的 stub 实现。
//
// 它保留自己的 dcmi 句柄，而不是借用容器共享驱动程序。包装器不会卸载其已持有的库，因此第二个初始化器无法
// 清空第一个的函数指针。将两者分开是能够分别进行伪造的关键。
func newProductDriver(logger klog.Logger) productascend.Driver {
	return &dcmiProductDriver{lib: dcmi.New(binding.WithLogger(logger)), logger: logger}
}

// dcmiProductDriver 实际上是 productascend.Driver，它根据 (卡片、卡片内设备) 这对名称来标识设备。其针对真
// 实驱动器的行为可通过 e2e 运行验证：一个 Darwin 测试二进制文件根本无法完成绑定/dcmi 操作。
type dcmiProductDriver struct {
	lib    *dcmi.DCMI
	logger klog.Logger
}

// 设备初始化库并解析句柄。
//
// 与容器共享驱动不同，它不会保留初始化结果：上述解析器仅记住答案，因此失败的操作会在下一次分配时重新尝试。
// 而成功的操作则会被记住，不会再次被请求。对于由包装器已持有的库来说，重新运行厂商的初始化非常便宜——它仅
// 单独重新运行初始化过程，而无需进行负载测试——因此在一个节点上最多只会发生几次这种情况。
func (d *dcmiProductDriver) device(cardID, deviceID int32) (dcmi.Device, error) {
	if ret := d.lib.Init(d.logger); !ret.IsSuccess() {
		return dcmi.Device{}, fmt.Errorf("dcmi init failed: %w", ret)
	}

	return d.lib.GetDeviceHandleByCardAndIndex(cardID, deviceID), nil
}

func (d *dcmiProductDriver) MainboardID(cardID, deviceID int32) (uint32, error) {
	dev, err := d.device(cardID, deviceID)
	if err != nil {
		return 0, err
	}
	mainboardID, ret := dev.GetMainboardId()
	if !ret.IsSuccess() {
		return 0, fmt.Errorf("dcmi get mainboard id: %w", ret)
	}

	return mainboardID, nil
}

func (d *dcmiProductDriver) SuperPodType(cardID, deviceID int32) (uint32, error) {
	dev, err := d.device(cardID, deviceID)
	if err != nil {
		return 0, err
	}
	info, ret := dev.GetSuperPodInfo()
	if !ret.IsSuccess() {
		return 0, fmt.Errorf("dcmi get super pod info: %w", ret)
	}

	return uint32(info.Super_pod_type), nil
}
