// 本文件保存所有引擎通用的注入信息，以及仅 vLLM 系列使用的产物：其配置文件。
// “vLLM 系列”指两个引擎，它们使用两个连接器名称，以及分别限定于不同键集合的两个读取器，
// 因此下文按读取器而非按系列说明各项取值。
//
// 下文配置针对自身不存储任何数据的 Mooncake 客户端渲染。这是一种角色设定而非容量设置：
// 拥有本地缓冲区且无全局段的客户端可执行 Get 和 Put，同时不贡献内存，这正是推理引擎所需的。
// 本文件的组织方式旨在防止意外声明其他形态，因此声明该角色的键会一并写入，绝不单独写入。
package inject

import (
	"fmt"

	"gpustack.ai/gpustack/pkg/systemname"
	"gpustack.ai/gpustack/pkg/utils/json"
)

// IMPORTANT: 本文件是：vLLM 系列用的 mooncake.json 配置文件生成器

const (
	// MetadataServer 是所有参与方为元数据面写入的固定值。它是常量而非地址：元数据面采用点对点
	// 方式，不存在需要命名的服务端，写成类似端点的字符串只会误导读者去寻找并不存在的组件。
	MetadataServer = "P2PHANDSHAKE"

	// GlobalSegmentSize 表示引擎容器贡献给资源池的宿主机内存：为零。资源池容量完全来自后端
	// 声明的成员，这些成员的 Pod 会在各自的 resources 中申请内存。若引擎容器贡献内存，会在不
	// 改变任何资源请求的情况下增大其实际内存占用，而本包的调用方绝不会修改 resources。
	GlobalSegmentSize = 0

	// LocalBufferSize 是客户端侧的中转缓冲区大小，采用 store 参考实现所使用的取值。它属于
	// 传输层中转缓冲，而非租户配额，因此作为常量写在这里且不读取任何 API 字段。
	//
	// 这里显式写入而非省略，是因为各层默认值相差数个数量级：文件读取器在 vLLM 上默认为 4 GiB
	// （`worker.py:75`），在 vLLM-Ascend 上默认为 1 GiB（`mooncake_backend.py:19`），而客户端
	// 自身默认值为 16 MiB，且最终生效的是文件读取器的值。缺省该键会导致每个容器在按模型大小
	// 规划的内存上限内额外占用数 GiB 宿主机内存，不同引擎只是具体数字不同，后果完全一致。
	LocalBufferSize = 128 * 1024 * 1024

	// ModeStandaloneStore 是纯客户端声明的拓扑模式。vLLM 在启动时会双向校验它与
	// GlobalSegmentSize 的一致性，因此二者始终成对写入。
	ModeStandaloneStore = "standalone-store"

	// DeviceName 是 RDMA 设备过滤条件，包括 RDMA 在内的所有路径均留空。留空表示“使用发现的
	// 全部设备”，这是同一资源池中所有宿主机唯一正确的配置：设备名因节点而异，此节点为 mlx5_0，
	// 下一节点可能是 erdma_0。文档中提到的字符串 "auto-discovery" 在客户端并未做特殊处理，
	// 会被当作过滤器去匹配一个任何主机都不存在的设备名。
	DeviceName = ""
)

// ClientConfigAnnotationKey 用于在使用配置文件的引擎对应的 Pod 上承载渲染后的客户端配置。
// 该文件是此 annotation 的 downwardAPI 投影，因此调用方无需创建 ConfigMap，无需相关 RBAC 权限，
// 也不会留下需要垃圾回收的资源：配置的生命周期与 Pod 完全一致。
//
// 它声明于此，而非放在调用方读取的 annotation 旁边，因为该键是渲染的输出结果。
// 拒绝用户传入该值的调用方会从此处读取该常量。
const ClientConfigAnnotationKey = "kvcache." + systemname.LabelPrefix + "client-config"

const (
	// ConfigVolumeName 和 ConfigMountPath 指定渲染文件在容器内的挂载卷名与路径。该挂载为只读：
	// 容器内没有任何理由写入该文件，而可写投影会让进程篡改已注入配置的记录。
	ConfigVolumeName = "gpustack-kvcache-config"
	ConfigMountPath  = "/etc/gpustack/kvcache"

	// ConfigFileName 是挂载目录内的文件名，ConfigFilePath 是两者的拼接结果——
	// 即引擎自身的路径变量所设置的值。
	ConfigFileName = "mooncake.json"
	ConfigFilePath = ConfigMountPath + "/" + ConfigFileName
)

// vllmClientConfig 表示 vLLM 自身拼写规范下的 Mooncake 配置文件。
//
// 该结构体即本 operator 输出的配置 schema。使用结构体而非 map 可以让每个输出键的增删都成为
// 显式代码变更。所选引擎镜像是否读取其中每个键，属于镜像维护者的兼容性责任。
//
// 两个读取器共同消费这一份文件，但可识别的键集合不同：vLLM-Ascend 不使用 `mode`。此处仍照常
// 渲染 `mode`，原因需要明确说明而非臆测：vLLM 同样不把 `mode` 传给 `store.setup()`
// （`worker.py:1040-1048`），它只是将其与 `global_segment_size` 做配对校验并打印日志。真正
// 驱动两款引擎的都是大小字段，且两者都会读取。只有当包含该键的读取器不对它采取额外动作时，
// 另一侧忽略该键才是安全的。
//
// `enable_offload` 是刻意不写入的唯一可读键：它选择的存储层级不由本注入流程配置，且其默认值
// 与我们期望写入的值一致。
//
// 配置文件的字段名与客户端 `setup()` 签名不同，此处以文件字段名为准：`master_server_address`
// 比函数参数多出四个字母，RDMA 过滤器在这里名为 `device_name` 而非 `rdma_devices`。
type vllmClientConfig struct {
	MetadataServer      string `json:"metadata_server"`
	MasterServerAddress string `json:"master_server_address"`
	Protocol            string `json:"protocol"`
	DeviceName          string `json:"device_name"`
	Mode                string `json:"mode"`
	GlobalSegmentSize   int64  `json:"global_segment_size"`
	LocalBufferSize     int64  `json:"local_buffer_size"`
	TenantID            string `json:"tenant_id,omitempty"`
}

// renderVLLMClientConfig 为解析完成的连接构建配置文件内容。
//
// 所有连接字段和角色字段均显式写入，不靠省略取默认值。阅读者不应被迫掌握引擎对缺省键的默认
// 行为才能预测容器表现，而且在 `global_segment_size` 与 `local_buffer_size` 这两项上，
// 引擎默认会占用无人请求的数 GiB 宿主机内存（vLLM 为 4 GiB，vLLM-Ascend 为 1 GiB）。
// 租户字段是唯一例外：空域会被省略，因为引擎会将其归一化为与缺省该键相同的默认值。
func renderVLLMClientConfig(conn Connection, tenant string) ([]byte, error) {
	cfg := vllmClientConfig{
		MetadataServer:      MetadataServer,
		MasterServerAddress: conn.MasterAddress,
		Protocol:            conn.Protocol,
		DeviceName:          DeviceName,
		Mode:                ModeStandaloneStore,
		GlobalSegmentSize:   GlobalSegmentSize,
		LocalBufferSize:     LocalBufferSize,
		TenantID:            tenant,
	}

	out, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("marshal the vllm client configuration: %w", err)
	}
	return out, nil
}
