// 本文件负责为 vLLM 系列引擎渲染配置。它以文件作为载体，因为该引擎没有其他途径：
// `MooncakeStoreConfig.load_from_config()` 会读取 MOONCAKE_CONFIG_PATH 并在未设置时抛出异常
// （`worker.py:144-151`），且该环境变量指定的是文件路径而非直接承载配置内容。
package inject

import (
	"fmt"

	core "k8s.io/api/core/v1"

	"gpustack.ai/gpustack/pkg/utils/json"
)

// IMPORTANT: 本文件是 vLLM & vLLM-Ascend 的"引擎侧 KV 交接配置"核心参数渲染器

const (
	// vllmConfigPathEnv 是 vLLM 用于读取配置文件位置的环境变量。它指定路径，
	// 而该引擎的配置内容本身从不直接放在环境变量中。
	vllmConfigPathEnv = "MOONCAKE_CONFIG_PATH"

	// vllmTransferConfigArg 用于选择连接器和角色。该参数没有对应的环境变量——它是从命令行解析的
	// EngineArgs 字段——因此配置该引擎必须追加命令行参数。
	vllmTransferConfigArg = "--kv-transfer-config"
	vllmKVEventsConfigArg = "--kv-events-config"
	// vllmBlockSizeArg 设置 vLLM 的 KV 缓存块粒度（在 v0.29.0/v0.30.0 的 `vllm/config/cache.py:72-76`
	// 中为 `CacheConfig.block_size: int = Field(default=None, gt=0)`，其中 V1 引擎解除了旧版 V0 32-token
	// 的 CUDA 上限，且 V1 CUDA FlashAttention/FlashInfer 后端支持包括 128 在内的 16 的倍数）。在 Mooncake
	// TCP 上，将其默认设为 128 token 可使每个提示词的 KV 传输与存储描述符数量减少 8 倍（例如在 64 层的 8K
	// 提示词上从 16,448 个降至 2,056 个描述符），并摊薄每个描述符的 TCP 帧开销。该参数通过 DefaultedArgs
	// 输出，因此任何模型或注意力后端需要其他粒度的工作负载，只需在自身参数中声明 `--block-size` 即可覆盖默认值。
	vllmBlockSizeArg        = "--block-size"
	defaultVLLMTCPBlockSize = "128"
	// VLLMKVEventsPort 是供托管路由器消费的 ZMQ 发布端端口。
	VLLMKVEventsPort int32 = 5557
	// VLLMKVEventsReplayPort 是发布端的重放端口。
	VLLMKVEventsReplayPort int32 = 5558
	// VLLMKVEventsTopic 是发布端与订阅端共同使用的主题。
	VLLMKVEventsTopic = "kv@"
	// VLLMMooncakeBootstrapPort 是 prefiller 暴露 Mooncake 传输握手服务的端口。
	VLLMMooncakeBootstrapPort int32 = 8998

	// vllmStoreConnector 是上游原版 vLLM 为 Mooncake 存储注册的名称
	// （见 v0.29.0 `kv_connector/factory.py:223-226`）。
	//
	// `create_connector` 会在注册表中查找该名称，未命中时抛出 `ValueError: Unsupported connector type`，
	// 因此引擎不识别的名称会使其在启动时终止。这并不意味着该常量与引擎无关：它只是某一个项目的注册表拼写，
	// 其他引擎的工厂能解析哪些拼写属于上游状态，本仓库既不控制也不观测。应通过按引擎选择的 vllmConnectorFor
	// 获取该名称，而非直接渲染。
	vllmStoreConnector = "MooncakeStoreConnector"

	// vllmAscendStoreConnector 是 vLLM-Ascend 为其自有存储注册的名称
	// （见 v0.19.1rc1 `vllm_ascend/distributed/kv_transfer/__init__.py:39-43`）。它在前两条还为
	// 同一个类注册了 `MooncakeConnectorStoreV1`；两个名称均可解析，选择此名称是为了表明该类的归属项目。
	//
	// 该连接器名称并不暗示任何关于租户支持的信息；租户支持由引擎镜像决定，在此不作为选择依据。
	vllmAscendStoreConnector = "AscendStoreConnector"

	// vllmAscendTransferConnector 是 vLLM-Ascend 为其点对点连接器注册的名称
	// （见 v0.23.0 `vllm_ascend/distributed/kv_transfer/__init__.py:29-31`），通过该项目的
	// `ascend_kv_connector` 入口点加载，而非按路径指定。
	//
	// 它与原版 vLLM 为同一用途注册的连接器不同：两者通过 kv_transfer_params 使用不同的握手字段——
	// 原版的 MooncakeConnector 需要 transfer_id 和引导地址，而此连接器需要 prefiller 的主机、端口、
	// 块 ID 和引擎 ID，这也是在两者之间代理的路由器必须匹配引擎的原因。
	vllmAscendTransferConnector = "MooncakeConnectorV1"

	// vllmAscendDriverPath 是 Ascend 传输分支用于读取各 NPU 网卡地址的宿主机驱动目录：
	// hccn_tool 位于该目录下，而引擎镜像只包含驱动库而不包含该工具。宿主机源路径与容器挂载点
	// 使用同一路径，这正是镜像自身驱动存根所在的位置。
	vllmAscendDriverPath = "/usr/local/Ascend/driver"

	// vllmAscendDriverVolumeName 指定该目录树在 Pod 上投射卷的名称。
	vllmAscendDriverVolumeName = "gpustack-ascend-driver"
)

// vllmConnectorFor 返回给定引擎自身工厂能够解析的连接器名称。
//
// vLLM 系列共享同一个渲染器，因为它们共享同一种载体——位于 MOONCAKE_CONFIG_PATH 的配置文件，
// 两端均由 `MooncakeStoreConfig.from_file` 读取且公共键含义一致。但它们不共享连接器注册表：
// 各项目注册各自的拼写，任意一方工厂可解析哪些拼写属于上游状态，本仓库既不控制也不观测，
// 因此必须按引擎选择名称，不能假定两者互通。
//
// 按引擎选对名称并不意味着不同厂商的两个角色就能共享同一缓存。两侧写入的存储键由各自项目的
// store 客户端拼装，本渲染器输出的任何配置都无法选择或对齐键格式。一旦两侧不一致，故障是静默的：
// 两个引擎均正常启动并提供服务，只是各自查不到对方写入的条目。此时引擎日志不会报错，因此跨厂商
// 缓存未命中不能说明本函数选错了名称。
func vllmConnectorFor(engine Engine) (string, error) {
	switch engine {
	case EngineVLLM:
		return vllmStoreConnector, nil
	case EngineVLLMAscend:
		return vllmAscendStoreConnector, nil
	default:
		// 不可达：Render 只会把上述两款引擎分发到这里。此处显式报错而不回退到原版 vLLM 名称，
		// 因为隐式回退会让新增的 vLLM 衍生引擎静默继承该拼写，而这正是需要避免的故障。
		return "", newRefusal(ReasonEngineUnknown,
			"engine %q has no vllm-family connector name", engine)
	}
}

// vllmKVRole 将声明的角色映射为连接器自身的词汇。未设置角色时映射为 `kv_both`，
// 对应未拆分 prefill/decode 的共享缓存形态：容器同时负责读取与写入。
func vllmKVRole(role Role) (string, error) {
	switch role {
	case RoleNone:
		return "kv_both", nil
	case RolePrefill:
		return "kv_producer", nil
	case RoleDecode:
		return "kv_consumer", nil
	default:
		return "", newRefusal(ReasonRoleUnknown, "role %q has no vllm connector role", role)
	}
}

// vllmTransferConfig 是 --kv-transfer-config 的 JSON 文档。使用结构体而非 map，
// 以便无法识别的键能在编译期暴露，与本包的 client-config 类型保持一致。
type vllmTransferConfig struct {
	KVConnector            string                    `json:"kv_connector"`
	KVRole                 string                    `json:"kv_role"`
	KVPort                 int32                     `json:"kv_port,omitempty"`
	KVConnectorExtraConfig *vllmConnectorExtraConfig `json:"kv_connector_extra_config,omitempty"`
}

type vllmConnectorExtraConfig struct {
	// CachePrefix 是存储连接器的键命名空间：vLLM v0.29.0 会将每个 Mooncake 存储键构造为
	// "[cache_prefix@]<model name>@tp_rank:N@...@<chunk hash>"。
	CachePrefix      string               `json:"cache_prefix,omitempty"`
	Connectors       []vllmTransferConfig `json:"connectors,omitempty"`
	MooncakeProtocol string               `json:"mooncake_protocol,omitempty"`
	Prefill          *vllmRoleParallelism `json:"prefill,omitempty"`
	Decode           *vllmRoleParallelism `json:"decode,omitempty"`
}

// vllmRoleParallelism 是 vLLM-Ascend 点对点连接器在 worker 启动时断言校验的单侧并行形态
// （见 v0.23.0 `vllm_ascend/distributed/kv_transfer/kv_p2p/mooncake_connector.py:2066-2084`）：
// 任一侧缺少 tp_size 或 dp_size 都会导致引擎崩溃，且 prefill 的 tp_size 必须大于等于 decode 的
// tp_size——若解析结果中 decode tp 更宽，则在渲染阶段直接拒绝，避免渲染后陷入崩溃循环。顺序校验
// 只针对 tp_size：dp_size 仅校验是否存在而后续不再读取，因此非对称 dp 配对可以正常启动，交由引擎
// 自行运行而不应由此处拒绝。数值由调用方从各自角色配置中解析得到，此处不额外推导并行度，未声明时
// 渲染为引擎自身的默认值 1。
type vllmRoleParallelism struct {
	TPSize int `json:"tp_size"`
	DPSize int `json:"dp_size"`
}

// vllmParallelismBlock 渲染单侧并行配置块，并将未声明的并行度映射为引擎默认值 1。
// 文档两侧均经过此函数，因此两者的差异仅来自调用方解析出的成对数值。
func vllmParallelismBlock(p Parallelism) *vllmRoleParallelism {
	p = p.orOne()

	return &vllmRoleParallelism{TPSize: p.TensorParallel, DPSize: p.DataParallel}
}

type vllmKVEventsConfig struct {
	EnableKVCacheEvents bool   `json:"enable_kv_cache_events"`
	Publisher           string `json:"publisher"`
	Endpoint            string `json:"endpoint"`
	ReplayEndpoint      string `json:"replay_endpoint"`
	BufferSteps         int    `json:"buffer_steps"`
	HWM                 int    `json:"hwm"`
	MaxQueueSize        int    `json:"max_queue_size"`
	Topic               string `json:"topic"`
}

// renderVLLMWithEncoderCache 生成 vLLM 系列容器所需的全部配置，并组合了编码器缓存分支。
// 包括选择连接器的命令行参数、指向配置文件的环境变量、承载该文件的投影卷及对应注解，
// 以及直传、事件发布与编码器缓存相关配置；若 EncoderCache 未指定连接器，则其余所有分支不受影响。
func renderVLLMWithEncoderCache(in Input, ec EncoderCache) (*Result, error) {
	// encoder 阶段仅渲染编码器缓存分支，绝不渲染任何 KV 分支，这是上游引擎自身的约束而非策略选择：
	// 纯 encoder 实例同样不分配 KV 缓存——`GPUModelRunner.get_kv_cache_spec` 对其返回 `{}`
	// （见 vLLM v0.30.0 `vllm/config/ec_transfer.py:112-119`）。若在此写入任何 KV 键，就会声明一个
	// 引擎无法执行的角色，尤其 store 分支还会向并无 KV 缓存可寻址的 Pod 投影客户端配置文件。
	encoderOnly := in.Role == RoleEncode
	if encoderOnly {
		// encoder 阶段的存在目的就是生产编码结果，若未声明编码器缓存分支，它既没有编码器缓存文档，
		// 也没有下方的任何 KV 键。返回空 Result 会启动一个没有任何传输配置的 vLLM 容器，产出的
		// 嵌入结果无人可读取。此处拒绝并沿用该分支出现之前的同一拒绝原因（当时角色映射未覆盖
		// encoder 而落入 default）。
		if ec.Connector == "" {
			return nil, newRefusal(ReasonRoleUnknown,
				"role %q has no vllm encoder-cache rendering: declare the encoder-cache leg the "+
					"stage runs, since it renders no KV configuration either", in.Role)
		}
		if in.KVTransfer {
			return nil, newRefusal(ReasonRoleUnsupported,
				"the encoder role has no KV transfer partner: it produces encodings, not "+
					"attention blocks")
		}
		if in.PublishKVEvents {
			return nil, newRefusal(ReasonRoleUnsupported,
				"the encoder role is not among the KV event publishers: it writes no KV cache "+
					"for one to describe")
		}
		if in.Connection.MasterAddress != "" {
			return nil, newRefusal(ReasonRoleUnsupported,
				"role %q holds no KV cache, so a shared store asked of it would leave the "+
					"encoder carrying a cache configuration that moves nothing", in.Role)
		}
	}

	var kvRole string
	if !encoderOnly {
		// IMPORTANT: 角色翻译
		// 	- RolePrefill → "kv_producer"（KV 生产者）
		// 	- RoleDecode → "kv_consumer"（KV 消费者）
		// 	- RoleNone（单角色共享缓存）→ "kv_both"（既读又写）
		role, err := vllmKVRole(in.Role)
		if err != nil {
			return nil, err
		}
		kvRole = role
	}

	// 这里只检查地址，而 Render 入口的校验写作“地址或传输协议任一非空”。对于能到达此处的所有输入，
	// 两者完全等价，因为入口校验已拒绝只带其中一项的半完整连接——到达这里的 Connection 要么两项
	// 都有，要么都为空。encoder 阶段绝不会带着连接到达此处，它已在上方被拒绝，与 SGLang 渲染器
	// 保持一致。
	hasStore := !encoderOnly && in.Connection.MasterAddress != ""

	// 连接器配置作为命令行上的 JSON 文档，由结构体序列化得到，以便不可识别的键在编译期报错。
	// 键顺序不属于契约——JSON 不定义顺序——因此比对时应反序列化而不是做字符串匹配。
	var transferConfigValue *vllmTransferConfig
	// 下方按厂商区分的直传分支还会补充该链路自身需要挂载到 Pod 上的卷与挂载点。
	var legVolumes []core.Volume
	var legVolumeMounts []core.VolumeMount
	var forceTCP bool
	if hasStore {
		// IMPORTANT: 共享 Store 分支

		// 选连接器名：
		// 	- 原版 vLLM 用 "MooncakeStoreConnector"
		// 	- vLLM-Ascend 用 "AscendStoreConnector"
		connector, err := vllmConnectorFor(in.Engine)
		if err != nil {
			return nil, err
		}
		transferConfigValue = &vllmTransferConfig{KVConnector: connector, KVRole: kvRole}
		// 仅在原版 store 连接器上渲染：只有它的键布局经过源码核对。vLLM-Ascend 的 store 连接器
		// 按自身规则构造键，写入一个可能被其忽略的前缀会制造出“已隔离”的假象。
		if in.CachePrefix != "" && in.Engine == EngineVLLM {
			transferConfigValue.KVConnectorExtraConfig = &vllmConnectorExtraConfig{CachePrefix: in.CachePrefix}
		}
	}
	if in.KVTransfer {
		// IMPORTANT: 点对点直传分支

		native := in.Engine == EngineVLLM
		if (!native && in.Engine != EngineVLLMAscend) || (in.Role != RolePrefill && in.Role != RoleDecode) {
			return nil, newRefusal(ReasonRoleUnsupported,
				"point-to-point transfer requires a native vLLM prefill or decode role")
		}
		var direct vllmTransferConfig
		if native {
			// IMPORTANT: 原版 vLLM

			// 此处刻意不对协议取值做白名单拦截。可用集合取决于引擎镜像内部编译的 mooncake 构建，
			// 本 operator 既不分发也无法检查该镜像：启用 HIP 编译的镜像可正常支持 "hip" 点对点传输，
			// 在此处拒绝会把某一份镜像的编译选项硬编码限制到其他镜像的连接器上。checkTransport 在
			// 另一侧遵循同一原则：对未实测的组合予以放行，避免基于未经验证的假设把能运行的引擎拦死。
			// 因此协议不匹配仍会在拥有真实事实的容器启动阶段报错，该规则对显式声明与默认值同等适用。
			direct = vllmTransferConfig{
				KVConnector: "MooncakeConnector", KVRole: kvRole,
				KVConnectorExtraConfig: &vllmConnectorExtraConfig{MooncakeProtocol: directLegProtocol(in)},
			}
			// 配置文档中的 protocol 键并不会被传输引擎读取，因此 tcp 链路还需通过它实际读取的开关
			// 来固定；directLegForcesTCP 负责判断何时这样做不会影响同进程内的其他部分。
			forceTCP = directLegForcesTCP(in)
		} else {
			// IMPORTANT: 华为昇腾 vLLM-Ascend 分支

			// Ascend 分支不渲染 protocol 键：其传输引擎以字面量 "ascend" 初始化
			// （见 v0.23.0 `vllm_ascend/distributed/kv_transfer/utils/mooncake_transfer_engine.py:26`），
			// 因此 spec.kvTransfer.protocol 在此没有落脚键，应忽略而非拒绝——准入阶段无法得知
			// 资源池厂商，在准入之后再拒绝只会陷入调谐报错循环。
			//
			// kv_port 是两端必须达成一致的唯一数值，且该一致性是结构性的：prefiller 以 consumer
			// 自身 kv_port 的偏移量来宣告其侧信道端口（`kv_p2p/mooncake_connector.py:1936-1945`），
			// 若两端数值不同，decoder 就会去拨号无人监听的端口。VLLMMooncakeBootstrapPort 即该
			// 统一端口；准入阶段已在两类厂商的被路由 prefiller 上保留该端口。
			//
			// prefill/decode 块携带双方角色各自声明的并行形态，由调用方从两边配置解析传入。连接器
			// 会在 worker 启动时断言检查这些数字，若配置块与实际运行的引擎不一致，则会把启动报错变成
			// 错误的块布局；因此两侧块都通过同一个辅助函数从同一份成对输入填充。
			//
			// 当 prefill 的 tp_size 小于 decode 的 tp_size 时，连接器也会在启动时对配对报错，因此
			// 在此处提前拒绝，而不是渲染出崩溃循环。该校验仅覆盖 tp_size 且仅限于本分支：原版直传分支
			// 支持两个方向映射，若对所有 vLLM 配对施加此限制，会破坏引擎本身可正常运行的部署。
			if prefill, decode := in.Parallelism.Prefill.orOne(), in.Parallelism.Decode.orOne(); prefill.TensorParallel < decode.TensorParallel {
				return nil, newRefusal(ReasonRoleUnsupported,
					"the vLLM-Ascend transfer leg requires the prefill tp_size to be at "+
						"least the decode one, but the pair declares prefill %d and decode %d; "+
						"narrow the decode role's tensor parallelism or widen the prefill role's",
					prefill.TensorParallel, decode.TensorParallel)
			}
			direct = vllmTransferConfig{
				KVConnector: vllmAscendTransferConnector,
				KVRole:      kvRole,
				KVPort:      VLLMMooncakeBootstrapPort,
				KVConnectorExtraConfig: &vllmConnectorExtraConfig{
					Prefill: vllmParallelismBlock(in.Parallelism.Prefill),
					Decode:  vllmParallelismBlock(in.Parallelism.Decode),
				},
			}
			// 两侧角色容器均以只读方式挂载宿主机驱动目录树。该链路的传输层会构建 Device RoCE 端点，
			// 并通过随驱动分发的 hccn_tool 读取每张 NPU 的网卡地址——引擎镜像只带了驱动库而未带该
			// 工具，若两处来源都不可达，引擎启动时会以 "Failed to get device ip from hccn.conf and
			// hccn_tool" 退出。读取 /etc/hccn.conf 虽也可行，但仅适用于保留该文件的宿主机；而工具
			// 可在所有装有驱动的宿主机上直接向驱动查询，因此挂载驱动目录树可同时覆盖两种环境。在所有
			// Ascend 容器化路径中，驱动属于宿主机（镜像提供 toolkit），因此该挂载不会遮蔽镜像自身
			// 内容。它仅随直传分支一并渲染：向租户侧工作负载投射宿主机路径的影响面远大于集群级
			// DaemonSet，因此跟随真正需要该能力的分支而非跟随厂商无条件挂载。采用 Directory 类型可
			// 让缺少驱动的节点在卷挂载阶段就明确指出缺失路径，把失败提前到部署期而非拖到启动期。
			hostPathDirectory := core.HostPathDirectory
			legVolumes = append(legVolumes, core.Volume{
				Name: vllmAscendDriverVolumeName,
				VolumeSource: core.VolumeSource{
					HostPath: &core.HostPathVolumeSource{Path: vllmAscendDriverPath, Type: &hostPathDirectory},
				},
			})
			legVolumeMounts = append(legVolumeMounts, core.VolumeMount{
				Name: vllmAscendDriverVolumeName, MountPath: vllmAscendDriverPath, ReadOnly: true,
			})
		}
		if !hasStore {
			// decode 分支仅渲染角色与协议：引导地址预期按请求通过 kv_transfer_params 传入。
			// 该行为已在真实集群运行中验证，上游逐请求处理路径尚未逐行阅读。
			transferConfigValue = &direct
		} else {
			// 内层 store 连接器的 kv_consumer/kv_both 角色在此固定写入：上游对内层连接器 kv_role
			// 的语义尚未完全核对。实机集群中曾观测到在 kv_both 角色上触发指向 MooncakeConnector
			// 的 Prometheus 指标注册断言，在一次 APIServer 重启后恢复。
			storeRole := "kv_consumer"
			if in.Role == RolePrefill {
				storeRole = "kv_both"
			}
			transferConfigValue = &vllmTransferConfig{
				KVConnector: "MultiConnector",
				KVRole:      kvRole,
				KVConnectorExtraConfig: &vllmConnectorExtraConfig{Connectors: []vllmTransferConfig{
					direct, *transferConfigValue,
				}},
			}
			transferConfigValue.KVConnectorExtraConfig.Connectors[1].KVRole = storeRole
		}
	}

	result := &Result{
		KVTransfer: in.KVTransfer,
	}
	if hasStore {
		config, err := renderVLLMClientConfig(in.Connection, in.Domain)
		if err != nil {
			return nil, err
		}
		result.TenantInjected = in.Domain != ""
		result.Env = []core.EnvVar{{Name: vllmConfigPathEnv, Value: ConfigFilePath}}
		result.Volumes = []core.Volume{
			{
				Name: ConfigVolumeName,
				VolumeSource: core.VolumeSource{
					DownwardAPI: &core.DownwardAPIVolumeSource{
						Items: []core.DownwardAPIVolumeFile{
							{
								Path: ConfigFileName,
								FieldRef: &core.ObjectFieldSelector{
									FieldPath: fmt.Sprintf("metadata.annotations['%s']",
										ClientConfigAnnotationKey),
								},
							},
						},
					},
				},
			},
		}
		result.VolumeMounts = []core.VolumeMount{
			{Name: ConfigVolumeName, MountPath: ConfigMountPath, ReadOnly: true},
		}
		result.PodAnnotations = map[string]string{
			ClientConfigAnnotationKey: string(config),
		}
	}
	// 追加在 store 分支之后，因为 store 分支直接对两个切片赋值：同时挂接共享池的配对需要把客户端
	// 配置卷和直传分支的挂载点并排保留。
	result.Volumes = append(result.Volumes, legVolumes...)
	result.VolumeMounts = append(result.VolumeMounts, legVolumeMounts...)
	if transferConfigValue != nil {
		transferDoc, err := json.Marshal(transferConfigValue)
		if err != nil {
			return nil, fmt.Errorf("marshal the vLLM connector configuration: %w", err)
		}
		result.Args = append(result.Args, vllmTransferConfigArg, string(transferDoc))
	}
	// 当请求未指定编码器连接器时该分支不输出任何内容，因此可无条件组合：由一处统一定义缺省分支的
	// 含义，追加空 Result 没有任何开销。放在 KV 配置文档之后以便两条传输链路在命令行上相邻呈现，
	// 同时放在 store 分支之后，因为 store 分支会直接给 Env 赋值，而此处是追加。
	leg, err := renderVLLMEncoderCache(in, ec)
	if err != nil {
		return nil, err
	}
	result.Args = append(result.Args, leg.Args...)
	result.Env = append(result.Env, leg.Env...)
	result.Ports = append(result.Ports, leg.Ports...)
	result.DefaultedArgs = append(result.DefaultedArgs, leg.DefaultedArgs...)
	if hasStore && in.Dtype != "" {
		result.Args = append(result.Args, KVCacheDtypeArg, in.Dtype)
	}
	if in.KVTransfer && in.Role == RolePrefill {
		// 容器端口在两类厂商上均声明：prefiller 的握手监听就位于此处，且防止用户自声明端口冲突的
		// 准入保留逻辑与厂商无关。环境变量仅属于原版 vLLM——它的连接器从环境变量读取引导端口，
		// 而 vLLM-Ascend 从上方配置文档的 kv_port 字段读取，不会查看该环境变量。
		if in.Engine == EngineVLLM {
			result.Env = append(result.Env, core.EnvVar{
				Name: "VLLM_MOONCAKE_BOOTSTRAP_PORT", Value: fmt.Sprint(VLLMMooncakeBootstrapPort),
			})
		}
		result.Ports = append(result.Ports, core.ContainerPort{
			Name: "mc-bootstrap", Protocol: core.ProtocolTCP,
			ContainerPort: VLLMMooncakeBootstrapPort,
		})
	}
	if forceTCP {
		// 以默认值而非托管键输出：传输引擎只检查该变量是否存在，已经自行设置该变量（任意值）的用户
		// 表达的正是同一诉求。
		result.DefaultedEnv = append(result.DefaultedEnv, core.EnvVar{Name: MooncakeForceTCPEnv, Value: "1"})
	}
	if in.Engine == EngineVLLM && ((hasStore && in.Connection.Protocol == "tcp") || forceTCP) {
		result.DefaultedEnv = append(result.DefaultedEnv, mooncakeTCPDefaultedEnv()...)
		result.DefaultedArgs = append(result.DefaultedArgs, []string{vllmBlockSizeArg, defaultVLLMTCPBlockSize})
	}
	if !in.PublishKVEvents {
		return result, nil
	}
	if in.KVEventsHost == "" {
		return nil, newRefusal(ReasonConnectionIncomplete,
			"the KV event publisher has no dialable host")
	}

	eventsDoc, err := json.Marshal(vllmKVEventsConfig{
		EnableKVCacheEvents: true,
		Publisher:           "zmq",
		Endpoint:            fmt.Sprintf("tcp://*:%d", VLLMKVEventsPort),
		ReplayEndpoint:      fmt.Sprintf("tcp://*:%d", VLLMKVEventsReplayPort),
		BufferSteps:         10_000,
		HWM:                 100_000,
		MaxQueueSize:        100_000,
		Topic:               VLLMKVEventsTopic,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal the vLLM KV event configuration: %w", err)
	}
	result.Args = append(result.Args, vllmKVEventsConfigArg, string(eventsDoc))
	result.Ports = append(result.Ports, KVEventsPorts()...)
	result.KVEvents = VLLMKVEvents(in.KVEventsHost)

	return result, nil
}
