package nvidia

import (
	"encoding/hex"

	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/binding/nvml"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/utils/strconvx"
)

// fabricKindNVLink 标识 NVLink 互连，仅与同协议的域标识比较。
const fabricKindNVLink = "nvlink"

// fabricInfo 统一保存两个 NVML 结构版本共有的注册数据。V2 额外提供的健康掩码不参与构造域记录。
type fabricInfo struct {
	clusterUUID [nvml.GPU_FABRIC_UUID_LEN]uint8
	status      nvml.Return
	cliqueID    uint32
	state       uint8
}

// readFabric 读取加速器在 NVLink 上注册的集群和可互访的 clique。该可选查询失败不影响卡的枚举。
// 能否发布域还要看注册状态及标识。
func (in *nvidia) readFabric(dev nvml.Device, uuid string, logger klog.Logger) *device.Fabric {
	read := readFabricInfo(dev, logger)
	info := in.rememberFabric(uuid, read)

	fabric := newFabric(info)
	// 只把本轮实际读取的状态写入诊断日志，避免把缓存的上一次结果当作新读数。
	if fabric == nil && read != nil {
		// 在日志里区分设备没有这种互连和注册未完成。两种情况都可能成功读取结构但没有可发布的域。
		logger.V(3).Info("recorded no fabric domain for an accelerator that is not registered with a fabric",
			"registration", fabricStateName(read.state),
			"status", read.status.String(),
			"clusterIdentified", read.clusterUUID != [nvml.GPU_FABRIC_UUID_LEN]uint8{})
	}

	return fabric
}

// fabricStateName 给日志中的注册状态取可读名称，便于区分无此能力和尚未开始注册。
func fabricStateName(state uint8) string {
	switch state {
	case nvml.GPU_FABRIC_STATE_NOT_SUPPORTED:
		return "not-supported"
	case nvml.GPU_FABRIC_STATE_NOT_STARTED:
		return "not-started"
	case nvml.GPU_FABRIC_STATE_IN_PROGRESS:
		return "in-progress"
	case nvml.GPU_FABRIC_STATE_COMPLETED:
		return "completed"
	}
	// 对未知枚举值保留原始数字，便于识别比当前实现更新的驱动状态。
	return "unrecognized-" + strconvx.FormatUint(uint64(state), 10)
}

// readFabricInfo 优先调用较新的 NVML 查询，再回退到已标记将弃用的 V1。这样既读取新版驱动，
// 也支持只有旧入口的节点。
func readFabricInfo(dev nvml.Device, logger klog.Logger) *fabricInfo {
	handler := dev.GetGpuFabricInfoV()

	newer, newerRet := handler.V2()
	if newerRet.IsSuccess() {
		return &fabricInfo{
			clusterUUID: newer.ClusterUuid,
			status:      nvml.Return(newer.Status),
			cliqueID:    newer.CliqueId,
			state:       newer.State,
		}
	}

	info, ret := handler.V1()
	if !ret.IsSuccess() {
		logger.V(3).Info("recorded no fabric domain for a card whose driver could not answer for it",
			"reason", ret.Error(), "newerEntryPoint", newerRet.Error())
		return nil
	}

	// 记录回退成功的情况。两个接口给上层的是同一种数据，这里是辨别驱动实际响应哪个版本的位置。
	logger.V(3).Info("read the fabric registration through the older entry point",
		"reason", newerRet.Error())

	return &fabricInfo{
		clusterUUID: info.ClusterUuid,
		status:      nvml.Return(info.Status),
		cliqueID:    info.CliqueId,
		state:       info.State,
	}
}

// rememberFabric 按 UUID 保存上一次成功的注册结果。一次读取失败沿用缓存，避免无依据地移除互连域。
// 成功读到未注册状态时，以新读数撤下旧域。
func (in *nvidia) rememberFabric(uuid string, info *fabricInfo) *fabricInfo {
	in.fabricsMu.Lock()
	defer in.fabricsMu.Unlock()

	if info != nil {
		in.fabrics[uuid] = *info
		return info
	}

	remembered, ok := in.fabrics[uuid]
	if !ok {
		return nil
	}

	return &remembered
}

// newFabric 只在注册结果能确定 NVLink 域时发布记录。此接口没有可独立使用的端点列表，缺少域时返回 nil。
func newFabric(info *fabricInfo) *device.Fabric {
	if info == nil {
		return nil
	}

	// 注册已完成后，集群 UUID 和 clique 编号才有意义。未开始或协商中的卡可能给出空值，
	// 把它们发布会让不同节点误判为同一域。
	if info.state != nvml.GPU_FABRIC_STATE_COMPLETED {
		return nil
	}
	// 注册完成后再检查状态码。已结束但失败的注册不能作为可用互连域。
	if info.status != nvml.SUCCESS {
		return nil
	}
	// 全零集群 UUID 代表未填充缓冲区。省略它，避免多台机器共享一个虚构域。
	if info.clusterUUID == ([nvml.GPU_FABRIC_UUID_LEN]uint8{}) {
		return nil
	}

	return &device.Fabric{
		Kind: fabricKindNVLink,
		// 按原顺序把全部 16 字节编码为无分隔符的 32 位小写十六进制。固定表示可跨 Worker 比较，
		// 也为长度受限的 Kubernetes 拓扑标签留出空间。
		ID: hex.EncodeToString(info.clusterUUID[:]),
		// clique 编号 0 是有效编号。同一集群 UUID 下，不同 clique 的加速器仍不能相互寻址。
		CliqueID: strconvx.FormatUint(uint64(info.cliqueID), 10),
	}
}
