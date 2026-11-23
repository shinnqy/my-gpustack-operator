package hygon

import (
	"fmt"
	"time"

	klog "k8s.io/klog/v2"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/deviceplugin"
)

// noDriverNote 说明整卡、共享和逻辑切分的分配响应不需要实时读取驱动。分区路径另行查询驱动实例。
const noDriverNote = "the hygon allocator reads no driver to serve a whole-card, shared or sliced " +
	"allocation: those device sets are host device-node paths declared in this package, and the " +
	"sliced path derives its cu-mask and vram cap from the on-disk vdev.conf ledger it scans and " +
	"writes itself. only the partition path reads the multi-instance library, and only on a node " +
	"an administrator has put into multi-instance mode"

// NewPreflighter 创建海光预检器，常规模式报告无需驱动读取的原因，容器注入仍复用生产响应器。
func NewPreflighter(opts device.PreflighterOptions) device.AcceleratorPreflighter {
	return &preflighter{logger: opts.Logger.WithName(Manufacturer)}
}

type preflighter struct {
	logger klog.Logger
}

// PreflightAccelerator 在组说明中解释为何本轮没有逐卡驱动检查。
func (p *preflighter) PreflightAccelerator(_ device.DevicesGroupList) device.PreflightGroup {
	return device.PreflightGroup{
		Manufacturer: Manufacturer,
		Timestamp:    time.Now(),
		Note:         noDriverNote,
	}
}

// PreflightResponder 复用生产响应器，将配置文件写入临时目录。分区查询沿用真实驱动，预检期间禁止实际创建或销毁实例。
func (p *preflighter) PreflightResponder(
	mode workercore.DeviceAllocationMode,
) (deviceplugin.ContainerAllocateResponder, func(), error) {
	_, restore, err := deviceplugin.NewPreflightRedirect()
	if err != nil {
		return nil, nil, err
	}

	srv := newServer(p.logger, mode, &recordingMigDriver{read: newMigDriver()})

	responder, ok := srv.(deviceplugin.ContainerAllocateResponder)
	if !ok {
		restore()
		return nil, nil, fmt.Errorf("hygon %s server serves no container allocate response", mode)
	}
	return responder, restore, nil
}

// recordingMigDriver 转发驱动读取，拒绝创建硬件分区。探针没有创建过实例，回收请求按实例已释放处理。
type recordingMigDriver struct {
	read migDriver
}

var _ migDriver = (*recordingMigDriver)(nil)

func (d *recordingMigDriver) CardState(pciBusID, profile string) (migCardState, error) {
	return d.read.CardState(pciBusID, profile)
}

func (d *recordingMigDriver) CreateInstance(
	pciBusID, profile string, slot migPlacement,
) (migInstance, error) {

	return migInstance{}, fmt.Errorf(
		"hygon preflight: refusing to create a partition through the simulated responder",
	)
}

func (d *recordingMigDriver) DestroyInstance(string, migInstance) error {

	return nil
}

func (d *recordingMigDriver) ListInstances() ([]migLiveInstance, error) {
	return d.read.ListInstances()
}
