package cambricon

import (
	"fmt"

	"gpustack.ai/gpustack/binding/cndev"
)

// newSMLUDriver 返回真正的、由 cnDev 支持的 sMLU 驱动程序。该驱动仅适用于 Linux：设备管理器也仅在 Linux
// 上运行，若将 cnDev cgo 链接到 Darwin 测试二进制文件中，会在 dyld 加载阶段导致失败（无 .so 的宿主系统无
// 法解析 flat-namespace 符号），因此 Darwin 构建版本使用 smlu_driver_other.go 中的存根。
func newSMLUDriver() smluDriver {
	l := cndev.New()
	return &cndevSMLUDriver{lib: l, initRet: l.Init()}
}

// cndevSMLUDriver 就是真正的 smluDriver，它通过导出的 sMLU 包装层，利用 PCI 总线 ID 所指定的加速器，来驱
// 动 cnDev。
//
// sMLU 字段的精确语义和设备节点集合属于有记录的硬件开放问题（参见规范）：以下调用遵循制造商文档，但在实际
// 硬件上未经验证，因此所有操作都会经过该接口，只有当协议得到确认时才会改变这一类型。所有单元测试使用虚拟
// 驱动程序。
type cndevSMLUDriver struct {
	lib *cndev.CNDev
	// initRet 捕获了 cndevInit 的结果，因此当库无法加载/初始化时，第一次调用 device() 会报告出一个可
	// 解决的根本问题。
	initRet cndev.Return
}

func (d *cndevSMLUDriver) device(card string) (cndev.Device, error) {
	if !d.initRet.IsSuccess() {
		// 一个从未加载的库也不提供 sMLU API，因此该模式预检查必须像处理缺失的入口点一样进行读取，
		// 而不是试图绕过它进行写入。
		return cndev.Device{}, smluModeError("cndev init failed", d.initRet, d.initRet.IsAPIUnavailable())
	}
	// Handle 的查找操作与其他操作一样在库中进行，因此其失败也具有相同的意义：此处没有入口点，就意味着
	// 没有 API，而不是某种暂时性的状况。
	dev, ret := d.lib.GetDeviceHandleByPciBusId(card)
	if !ret.IsSuccess() {
		return cndev.Device{}, smluModeError(
			fmt.Sprintf("get cndev handle for %s", card), ret, ret.IsAPIUnavailable())
	}
	return dev, nil
}

func (d *cndevSMLUDriver) GetSMLUMode(card string) (bool, error) {
	dev, err := d.device(card)
	if err != nil {
		return false, err
	}
	mode, ret := dev.GetSMLUMode()
	if !ret.IsSuccess() {
		return false, smluModeError("get smlu mode", ret, ret.IsAPIUnavailable())
	}
	return mode.SmluMode == uint32(cndev.FEATURE_ENABLED), nil
}

func (d *cndevSMLUDriver) SetSMLUMode(card string, enabled bool) error {
	dev, err := d.device(card)
	if err != nil {
		return err
	}
	if ret := dev.SetSMLUMode(enabled); !ret.IsSuccess() {
		return smluModeError("set smlu mode", ret, ret.IsAPIUnavailable())
	}
	return nil
}

func (d *cndevSMLUDriver) CreateProfile(card string, coresPct int, memMiB int64) (int32, error) {
	dev, err := d.device(card)
	if err != nil {
		return 0, err
	}
	mluQuota, memorySize := smluSetFor(coresPct, memMiB)
	id, ret := dev.CreateSMluProfile(cndev.SMluSet{MluQuota: mluQuota, MemorySize: memorySize})
	if !ret.IsSuccess() {
		return 0, fmt.Errorf("card %s: create smlu profile: %w", card, ret)
	}
	return id, nil
}

func (d *cndevSMLUDriver) DestroyProfile(card string, profileID int32) error {
	dev, err := d.device(card)
	if err != nil {
		return err
	}
	if ret := dev.DestroySMluProfile(profileID); !ret.IsSuccess() {
		return fmt.Errorf("card %s: destroy smlu profile %d: %w", card, profileID, ret)
	}
	return nil
}

func (d *cndevSMLUDriver) CreateInstance(card string, profileID int32, name string) (smluInstance, error) {
	dev, err := d.device(card)
	if err != nil {
		return smluInstance{}, err
	}
	if ret := dev.CreateSMluInstance(uint32(profileID), name); !ret.IsSuccess() {
		return smluInstance{}, fmt.Errorf("card %s: create smlu instance %q: %w", card, name, ret)
	}
	// 读取创建的实例以恢复其设备节点。
	infos, ret := dev.GetAllSMluInstanceInfo()
	if !ret.IsSuccess() {
		return smluInstance{}, fmt.Errorf("card %s: list smlu instances after create: %w", card, ret)
	}
	for i := range infos {
		if infos[i].GetInstanceName() == name {
			return instanceFromInfo(card, infos[i]), nil
		}
	}
	return smluInstance{}, fmt.Errorf("card %s: created smlu instance %q not found", card, name)
}

func (d *cndevSMLUDriver) DestroyInstance(card, name string) error {
	dev, err := d.device(card)
	if err != nil {
		return err
	}
	if ret := dev.DestroySMluInstanceByName(name); !ret.IsSuccess() && ret != cndev.ERROR_NOT_FOUND {
		return fmt.Errorf("card %s: destroy smlu instance %q: %w", card, name, ret)
	}
	return nil
}

func (d *cndevSMLUDriver) ListInstances() ([]smluInstance, error) {
	if !d.initRet.IsSuccess() {
		return nil, fmt.Errorf("cndev init failed: %w", d.initRet)
	}
	count, ret := d.lib.GetDeviceCount()
	if !ret.IsSuccess() {
		return nil, fmt.Errorf("get cndev device count: %w", ret)
	}
	var out []smluInstance
	for i := 0; i < count; i++ {
		// 任何设备相关的错误会导致失败关闭：如果只列出部分内容，回收循环中的规格垃圾回收机制会将
		// 仍被引用的规格视为孤儿对象并将其销毁。
		dev, ret := d.lib.GetDeviceHandleByIndex(i)
		if !ret.IsSuccess() {
			return nil, fmt.Errorf("get cndev handle for device %d: %w", i, ret)
		}
		pcie, ret := dev.GetPCIeInfoV().V2()
		if !ret.IsSuccess() {
			return nil, fmt.Errorf("get pcie info for device %d: %w", i, ret)
		}
		card := pcie.GetBusId()
		infos, ret := dev.GetAllSMluInstanceInfo()
		if !ret.IsSuccess() {
			return nil, fmt.Errorf("list smlu instances on %s: %w", card, ret)
		}
		for j := range infos {
			out = append(out, instanceFromInfo(card, infos[j]))
		}
	}
	return out, nil
}

func (d *cndevSMLUDriver) ListProfiles() ([]profileKey, error) {
	if !d.initRet.IsSuccess() {
		return nil, fmt.Errorf("cndev init failed: %w", d.initRet)
	}
	count, ret := d.lib.GetDeviceCount()
	if !ret.IsSuccess() {
		return nil, fmt.Errorf("get cndev device count: %w", ret)
	}
	var out []profileKey
	for i := 0; i < count; i++ {
		dev, ret := d.lib.GetDeviceHandleByIndex(i)
		if !ret.IsSuccess() {
			return nil, fmt.Errorf("get cndev handle for device %d: %w", i, ret)
		}
		pcie, ret := dev.GetPCIeInfoV().V2()
		if !ret.IsSuccess() {
			return nil, fmt.Errorf("get pcie info for device %d: %w", i, ret)
		}
		card := pcie.GetBusId()
		ids, ret := dev.GetSMluProfileIds()
		if !ret.IsSuccess() {
			return nil, fmt.Errorf("list smlu profiles on %s: %w", card, ret)
		}
		for _, id := range ids {
			out = append(out, profileKey{card: card, profileID: id})
		}
	}
	return out, nil
}

// instanceFromInfo 根据加速器上的 cnDev SMluInfo 记录构建一个 smluInstance。配额数据从 MAX 槽位（索引 0）
// 读取。内存以字节为单位，再转换为 MiB 单位。
func instanceFromInfo(card string, info cndev.SMluInfo) smluInstance {
	return smluInstance{
		card:      card,
		name:      info.GetInstanceName(),
		profileID: info.ProfileId,
		coresPct:  int(info.MluQuota[0]),
		memMiB:    int64(info.MemorySize[0] >> 20),
		devNode:   info.GetDevNodeName(),
	}
}
