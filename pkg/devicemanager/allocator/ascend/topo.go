package ascend

import (
	productascend "gpustack.ai/gpustack/pkg/devicemanager/product/ascend"
)

// hcclTopoFilePathEnv 指定了 HCCL 读取节点互联网络拓扑结构的文件。只有 A5 代次具备此功能：厂商自研的设备
// 插件对其他卡类型会提前返回，运营商也遵循相同规则，以 family950 为关键参数。
const hcclTopoFilePathEnv = "HCCL_TOPO_FILE_PATH"

// hcclTopoFilePaths 是厂商的“产品与拓扑文件”表，由 ascend-device-plugin 提供，该表以产品形态为键，而非厂
// 商的纯数字作为键——详见 productascend.Type。
//
// 存放这些文件的目录是由 ascend-docker-runtime 挂载的，而非此分配器挂载，因此这里仅注入了 env 环境变量——
// 这也是为什么该文件永远不会被 stat 检查：它存在于容器中，而非设备管理器中。
var hcclTopoFilePaths = map[productascend.Type]string{
	productascend.TypeServer8P:  "/usr/local/Ascend/driver/topo/950/atlas_850_1.json",
	productascend.TypePod1D:     "/usr/local/Ascend/driver/topo/950/atlas_950_1.json",
	productascend.TypePod2D:     "/usr/local/Ascend/driver/topo/950/atlas_950_2.json",
	productascend.TypeServer16P: "/usr/local/Ascend/driver/topo/950/atlas_850_2.json",
	productascend.TypeServer32P: "/usr/local/Ascend/driver/topo/950/atlas_850_3.json",
	productascend.TypeCard1P:    "/usr/local/Ascend/driver/topo/950/atlas_350_1.json",
	productascend.TypeCard4P:    "/usr/local/Ascend/driver/topo/950/atlas_350_3.json",
}
