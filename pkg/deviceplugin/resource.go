package deviceplugin

import (
	"gpustack.ai/gpustack/pkg/utils/strconvx"
)

// 资源是指 Kubernetes 识别一个设备的方式：它被检测到的设备组，以及该组内的自身 ID。
//
//   - 这是 Device-plugin 和控制器相关的术语。Device Manager 负责管理 Devices。Kubernetes 将其作为资源进行
//     使用。关于完整的架构层次，请参考 pkg/device 包的相关文档。
//   - 它指的是某个设备，而非加速器。当前这个包所服务的每一个设备都是加速器，但分层结构将其他设备——比如
//     InfiniBand 端口、连接端口——也包含其中，这些设备将通过同一种机制接入 Kubernetes。若将范围限定在当前
//     使用的子类型上，就必须取消这种限制才能接纳它们。
//   - 此包中每个设备相关的账本都以其为键，因此必须保持为可比较的值类型：添加切片、映射或指针字段会使其作
//     为映射键变得无效，并在编译时破坏这些账本。
type Resource struct {
	// Group 是设备组的 ID。
	Group string
	// Device 是该组内设备的 ID。
	Device string
}

// String 将整卡渲染为 "<group>:<device>"。它指的是整卡，而非单个令牌——ResourceToken.String 会添加索引，以
// 便 kubelet 能够匹配到设备 ID。
func (in Resource) String() string {
	return in.Group + ":" + in.Device
}

// padIndex 以固定的四位数字形式显示每个设备 ID 所携带的令牌索引。该宽度由 IDs DeviceIDs 广告中的名称以及
// ResourceToken.String 中的名称共享。kubelet 所匹配的字符串与之前提供的字符串完全一致——因此两者在这一点上
// 绝不应出现矛盾。
func padIndex(idx uint64) string {
	return strconvx.PadZeroUint(idx, 4)
}
