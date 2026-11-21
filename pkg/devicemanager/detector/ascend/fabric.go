package ascend

import (
	"encoding/hex"

	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/binding/dcmi"
	"gpustack.ai/gpustack/pkg/device"
	productascend "gpustack.ai/gpustack/pkg/devicemanager/product/ascend"
	"gpustack.ai/gpustack/pkg/utils/strconvx"
)

// fabricKindUB 标识昇腾的统一总线 UB，使跨节点比较只发生在相同互连类型之间。
const fabricKindUB = "ub"

// urmaDeviceCountMax 是厂商规定的 URMA 枚举上限，驱动返回更大的数量时不继续遍历。
const urmaDeviceCountMax = 128

// readFabric 读取单个 A5 加速器的 UB 超节点、产品形态和端点。可选拓扑读失败按信息日志记录。
// 已有成功坐标时复用，避免暂时故障撤掉节点的互连域。
func (in *ascend) readFabric(dev dcmi.Device, cardID, deviceID int32, logger klog.Logger) *device.Fabric {
	var spod *dcmi.SpodInfo
	if info, ret := dev.GetSuperPodInfo(); ret.IsSuccess() {
		spod = &info
	} else {
		logger.Info("could not read the fabric domain coordinates",
			"reason", ret.Error(),
			"consequence", "reusing whichever this accelerator last answered with, if any")
	}
	spod = in.rememberSuperPod(superPodKey{cardID: cardID, deviceID: deviceID}, spod)

	var productType productascend.Type
	if product, err := in.product.Resolve(cardID, deviceID); err != nil {
		logger.Info("skipping the fabric domain shape", "reason", err.Error())
	} else if productType = product.Type; productType == "" {
		logger.Info("the fabric domain has a shape this build has no name for", "productCode", product.Code)
	}

	return newFabric(spod, productType, readFabricEndpoints(dev, logger))
}

// superPodKey 使用 DCMI 的卡号和卡内设备索引定位加速器，在进程生命周期内保持稳定。
type superPodKey struct {
	cardID   int32
	deviceID int32
}

// rememberSuperPod 保存最近成功读出的超节点坐标。当前查询失败可沿用旧值，新设备无旧值时留空。
// 成功读到“不属于超节点”的标志时用新值覆盖旧坐标。
func (in *ascend) rememberSuperPod(key superPodKey, spod *dcmi.SpodInfo) *dcmi.SpodInfo {
	in.superPodsMu.Lock()
	defer in.superPodsMu.Unlock()

	if spod != nil {
		in.superPods[key] = *spod
		return spod
	}

	remembered, ok := in.superPods[key]
	if !ok {
		return nil
	}

	return &remembered
}

// readFabricEndpoints 遍历设备的各个 URMA 功能实体并汇总其 EID。单个实体不可读时仍保留其他实体给出的端点。
func readFabricEndpoints(dev dcmi.Device, logger klog.Logger) []string {
	count, ret := dev.GetUrmaDeviceCount()
	if !ret.IsSuccess() {
		logger.Info("skipping the fabric endpoints", "reason", ret.Error())
		return nil
	}
	if count > urmaDeviceCountMax {
		logger.Info("skipping the fabric endpoints, the urma device count is out of range",
			"count", count, "max", urmaDeviceCountMax)
		return nil
	}

	var endpoints []string
	for i := uint32(0); i < count; i++ {
		eids, ret := dev.GetEidList(i)
		if !ret.IsSuccess() {
			logger.Info("skipping one urma device's endpoints", "urmaDevice", i, "reason", ret.Error())
			continue
		}
		endpoints = append(endpoints, fabricEndpoints(eids)...)
	}

	return endpoints
}

// fabricEndpoints 将每个 EID 的全部 16 字节编码为不带分隔符的小写十六进制。
// 后续消费者会按位解出功能实体、裸片和端口，因此必须保持长度、顺序和大小写。
func fabricEndpoints(eids []dcmi.UrmaEidInfo) []string {
	endpoints := make([]string, 0, len(eids))
	for i := range eids {
		endpoints = append(endpoints, hex.EncodeToString(eids[i].Eid[:]))
	}

	return endpoints
}

// newFabric 将驱动结果转换为发布的 UB 记录。完全没有信息时返回 nil。域 ID 无法确定时，
// 已知的产品形态或端点仍可供通信计划使用。
func newFabric(spod *dcmi.SpodInfo, productType productascend.Type, endpoints []string) *device.Fabric {
	if spod == nil && productType == "" && len(endpoints) == 0 {
		return nil
	}

	fabric := &device.Fabric{
		Kind:      fabricKindUB,
		Type:      string(productType),
		Endpoints: endpoints,
	}
	// 先用 Type.InSuperPod 确认设备确实属于超节点，再发布域坐标。普通服务器同样可能成功响应查询，
	// 而某些 8P 服务器确实位于超节点中，必须结合型号与驱动返回值。
	if spod != nil && productType.InSuperPod(spod.Super_pod_id, spod.Scale_type) {
		fabric.ID = strconvx.FormatUint(uint64(spod.Super_pod_id), 10)
		// 域成员身份成立后，还要分别校验域大小、服务器序号和机箱坐标。无效标记应省略，
		// 避免将全 1 值误当成数十亿台服务器。
		if spod.Scale_type != productascend.InvalidSuperPodSize {
			fabric.MemberCount = spod.Scale_type
		}
		if spod.Server_id != productascend.InvalidSuperPodCoordinate {
			fabric.NodeIndex = strconvx.FormatUint(uint64(spod.Server_id), 10)
		}
		if spod.Chassis_id != productascend.InvalidSuperPodCoordinate {
			fabric.RackID = strconvx.FormatUint(uint64(spod.Chassis_id), 10)
		}
	}

	return fabric
}

// productDriver 将已有 DCMI 句柄接入产品形态解析器。这里不依赖 Go 的 plugin 包，Darwin 上也能编译 cgo 绑定。
type productDriver struct {
	lib *dcmi.DCMI
}

func (d productDriver) MainboardID(cardID, deviceID int32) (uint32, error) {
	id, ret := d.lib.GetDeviceHandleByCardAndIndex(cardID, deviceID).GetMainboardId()
	if !ret.IsSuccess() {
		return 0, ret
	}
	return id, nil
}

func (d productDriver) SuperPodType(cardID, deviceID int32) (uint32, error) {
	info, ret := d.lib.GetDeviceHandleByCardAndIndex(cardID, deviceID).GetSuperPodInfo()
	if !ret.IsSuccess() {
		return 0, ret
	}
	return uint32(info.Super_pod_type), nil
}
