// 本文件负责为 vLLM 系列引擎渲染 encoder/prefill/decode 部署中的编码器缓存分支：
// 即用于将多模态编码结果从 encoder 阶段传输到 prefiller 的参数、环境变量和端口。
//
// 它是独立于 KV 渲染之外的分支，因为这两种缓存互不相关。一种将注意力块从 prefill 端传向
// decode 端；另一种将编码结果从 encoder 阶段传向 prefill 端。因此一个 Pod 会渲染其中之一，
// 或（作为 prefiller 时）同时渲染两者，而 encoder 仅渲染此分支。
package inject

import (
	"fmt"
	"strconv"

	core "k8s.io/api/core/v1"

	"gpustack.ai/gpustack/pkg/utils/json"
)

// IMPORTANT: 本文件是 vLLM 的多模态 EncoderCache（编码器缓存）渲染分支

const (
	// vllmECTransferConfigArg 用于选择编码器缓存连接器和角色。与对应的 KV 参数一样，它是从命令行
	// 解析的 EngineArgs 字段（`ec_transfer_config`，见 vLLM v0.30.0 `vllm/engine/arg_utils.py:1717`），
	// 没有对应的环境变量，因此配置该分支必须追加命令行参数。
	vllmECTransferConfigArg = "--ec-transfer-config"

	// vllmECCPUConnector 是上游原版 vLLM 为编码器缓存连接器注册的名称，该连接器将编码结果卸载到
	// CPU mmap 区域并通过 NIXL 传递给对端（见 v0.30.0 `docs/features/ec_cpu_connector.md`）。
	//
	// 应通过调用方传入并经下方选择器校验的连接器名称来使用，而不是无条件渲染：任一引擎注册表能解析哪些
	// 拼写属于上游状态，本仓库既不控制也不观测。
	vllmECCPUConnector = "ECCPUConnector"

	// vllmECExampleConnector 是上游原版 vLLM 为编码器缓存连接器注册的名称，该连接器将编码结果以
	// safetensors 格式写入共享目录（见 v0.30.0 `example_connector.py:53-56`），即上游编码器示例
	// 所采用的实现（`examples/disaggregated/disaggregated_encoder/README.md:69-89`）。
	vllmECExampleConnector = "ECExampleConnector"

	// vllmECProducerRole 和 vllmECConsumerRole 是连接器自身对传输两端角色的拼写
	// （`ECProducer`/`ECConsumer`，见 v0.30.0 `vllm/config/ec_transfer.py:12-14`）。
	// 上游还接受 "ec_both"，但本渲染器从不指定该值：此处每个引擎实例对应其中一个角色，
	// 既生产又消费自身编码结果的 Pod 形态并不在本仓库的任何部署形态之列。
	vllmECProducerRole = "ec_producer"
	vllmECConsumerRole = "ec_consumer"

	// VLLMECSideChannelPort 是 NIXL 生产端绑定供消费端拉取的 ZMQ ROUTER socket 的端口。
	// 该值采用上游默认值（v0.30.0 `vllm/envs.py:231`），此处沿用经过验证的默认端口优于自行指定。
	// 它与本包渲染的所有其他端口均不重叠，因为其余端口各自承载不同协议：HTTP、指标和 KV 事件发布器
	// 各自占用独立端口，KV 传输握手占用第四个端口。
	VLLMECSideChannelPort int32 = 5601

	// vllmECSideChannelHostEnv 是该 socket 的绑定地址。上游仅在启用 NIXL 传输的生产端读取这一对
	// 变量（v0.30.0 `docs/features/ec_cpu_connector.md:108`），其默认值 "localhost" 仅在生产端与
	// 消费端共享主机时有效（`:105`）——而两个 Pod 不会共享主机，因此渲染器将其设为 Pod 自身的地址。
	vllmECSideChannelHostEnv = "VLLM_EC_SIDE_CHANNEL_HOST"

	// vllmECSideChannelPortEnv 是同一组配置中的端口环境变量。
	vllmECSideChannelPortEnv = "VLLM_EC_SIDE_CHANNEL_PORT"

	// vllmModelRunnerEnv 用于选择引擎的模型运行器。ECCPUConnector 必须使用第二个（V2）运行器，
	// 否则会在构造时抛出 ValueError（v0.30.0 `docs/features/ec_cpu_connector.md:10`），
	// 因此在该分支上此变量是连接器的前置条件而非可选项，属于托管键而非默认值。
	vllmModelRunnerEnv = "VLLM_USE_V2_MODEL_RUNNER"

	// vllmECPodIPFieldPath 是 kubelet 在容器启动时解析的字段路径，使渲染器无需预先知道
	// Pod 地址即可让侧信道绑定地址可路由。
	vllmECPodIPFieldPath = "status.podIP"

	// 下方的编码器实例参数是上游示例为编码器列出的四个参数（v0.30.0
	// `examples/disaggregated/disaggregated_encoder/README.md:38-46`）。这四个参数均作为默认值而非托管键：
	// 上游将前两个标记为必需，但如果工作负载已声明同一参数即表达了相同诉求，因此跳过该组默认值不会造成
	// 遗漏。它们仅在生产端渲染，因为每个参数都用于配置不运行语言模型的引擎——在 prefiller 上使用
	// --mm-encoder-only 会跳过正需要读取编码结果的语言模型本身。
	vllmEnforceEagerArg          = "--enforce-eager"
	vllmNoPrefixCachingArg       = "--no-enable-prefix-caching"
	vllmECMaxNumBatchedTokensArg = "--max-num-batched-tokens"
	vllmMMEncoderOnlyArg         = "--mm-encoder-only"

	// vllmECMaxNumBatchedTokens 是编码器示例设置的 token 调度预算。上游指出该参数对纯编码器实例
	// 并无实际意义，并建议将其设得足够高以绕过调度器限制，将真正的预算控制交给编码器缓存管理器（README:44）；
	// 示例中的取值为 114688（README:96）。它是默认值而非强制值，因此带有自定义预算的工作负载只需声明
	// 该参数，此默认值便会被跳过。
	vllmECMaxNumBatchedTokens = "114688"
)

// EncoderCache 表示部署中单个 Pod 运行的编码器缓存分支配置。
//
// 其零值表示不启用编码器缓存分支，即编码器缓存传输引入前本渲染器处理的所有输入形式。调用方直接指定
// 连接器而非由渲染器推导，因为两条路径需要不同的输入信息：NIXL 路径需要 CPU 内存区域大小，
// 共享目录路径需要目录路径，两者既无法互相推导，也无法从角色推导。
type EncoderCache struct {
	// Connector 指定引擎必须解析的连接器名称：ECCPUConnector 通过 NIXL 在 Pod 间传输编码结果，
	// ECExampleConnector 则通过两端共同挂载的目录进行传输。未识别的名称将被拒绝，因为引擎注册表
	// 无法解析的名称会导致其在启动时终止，且无法与缓存配置错误区分开。
	Connector string

	// CPUBytes 是 ECCPUConnector 用于卸载编码结果的 CPU mmap 区域总大小。此处不设默认值：
	// 当缺少该键时上游会抛出 ValueError（v0.30.0 `docs/features/ec_cpu_connector.md:99`），且该区域
	// 会与 Node 上的其他进程竞争宿主机内存，因此其大小应由调用方根据容量规划决定。
	CPUBytes int64

	// SharedStoragePath 是 ECExampleConnector 读写编码结果的目录。它是调用方挂载的路径，
	// 且两端必须指定同一路径：缺少该键时连接器会回退到 "/tmp"（见 v0.30.0 `example_connector.py:53-56`），
	// 而在 Pod 中该目录并非两端共享，未设置时虽看似已配置，实则无法传输任何数据。
	SharedStoragePath string
}

// vllmECTransferConfig 是 --ec-transfer-config 的 JSON 文档。使用结构体而非 map，以便无法
// 识别的键能在编译期报错，与本包已有的另外两类配置文档保持一致。JSON 规范不保证键顺序，
// 因此对其做比对时应反序列化后比较，而不是做纯字符串匹配。
type vllmECTransferConfig struct {
	ECConnector string `json:"ec_connector"`
	ECRole      string `json:"ec_role"`
	// EngineID 用于命名 NIXL agent。此处留空，因为上游默认会为每个实例生成新的 UUID4
	// （v0.30.0 `docs/features/ec_cpu_connector.md:90`），天然满足每 Pod 唯一；若在这里指定，
	// 同样必须保证每个 Pod 唯一，而本渲染器无法校验这一点。
	EngineID               string                      `json:"engine_id,omitempty"`
	ECConnectorExtraConfig *vllmECConnectorExtraConfig `json:"ec_connector_extra_config,omitempty"`
}

type vllmECConnectorExtraConfig struct {
	// ECEnableNIXL 是 JSON 布尔值而非字符串 "true"：extra config 不会做类型强转，传入非 bool
	// 值时会被字符串化后与 "true"、"1"、"yes" 比对（见 vLLM v0.30.0
	// `vllm/distributed/ec_transfer/ec_connector/cpu/scheduler/__init__.py:88-92`）。因此字符串
	// "true" 或数字 1 虽能碰巧生效，而 0 与 2 则会被当作关闭。这只是解析过程的偶然行为而非契约，
	// 该键在文档中定义为布尔开关，因此此处直接按布尔值渲染。
	ECEnableNIXL bool `json:"ec_enable_nixl,omitempty"`

	// ECCPUBytes 指定 CPU 内存区域大小。ECCPUConnector 必需该键，ECExampleConnector 不读取。
	ECCPUBytes int64 `json:"ec_cpu_bytes,omitempty"`

	// SharedStoragePath 是 ECExampleConnector 的工作目录。该连接器必需此键，ECCPUConnector 不读取。
	SharedStoragePath string `json:"shared_storage_path,omitempty"`
}

// vllmECRole 将声明的角色映射为编码器缓存连接器自身的词汇。
//
// 仅支持这两种角色，这种不对称正是设计本身：encoder 负责生产，只有 prefiller 负责消费，因为
// decoder 代理会把多模态部分发送给 encoder，再由 prefill 端读回其编码结果
// （v0.30.0 `examples/disaggregated/disaggregated_encoder/disagg_1e1p1d_example.sh:52-57`）。
// decode 端只消费注意力块而不消费编码结果，因此在此直接拒绝，而不是为它渲染一个永远不会被
// 调用的 "ec_consumer"。
func vllmECRole(role Role) (string, error) {
	switch role {
	case RoleEncode:
		return vllmECProducerRole, nil
	case RolePrefill:
		return vllmECConsumerRole, nil
	case RoleDecode:
		return "", newRefusal(ReasonRoleUnsupported,
			"the decode role consumes attention blocks, not encodings: only the encoder produces "+
				"them and only the prefill role reads them")
	default:
		return "", newRefusal(ReasonRoleUnsupported,
			"the encoder-cache leg has no end for role %q: a deployment runs it as a separate "+
				"encoder stage or as the prefill half that consumes it", role)
	}
}

// renderVLLMEncoderCache 单独生成编码器缓存分支的配置，由调用方叠加到角色的常规渲染结果上，
// 而非由本函数决定其外围结构。
//
// 返回的 Result 仅包含该分支自身的内容，既方便调用方按合适顺序追加，也便于测试单独持有该分支
// 并与本包声明托管的键集合做比对。
//
// 当未请求连接器时，所有角色（包括 encoder）在此均渲染为空结果。是否允许 encoder 在未声明
// 编码器缓存连接器的情况下进入渲染，不属于本函数的职责：本函数只负责回答已命名分支的渲染结果，
// 而 encoder 阶段的约束规则位于组合入口处，与其他角色规则放在一起。
func renderVLLMEncoderCache(in Input, ec EncoderCache) (*Result, error) {
	// 未指定连接器时直接返回空结果，不视为拒绝。早于该分支存在的所有输入都不会命名连接器，
	// 不涉及此分支的角色也不应因未发起请求而被拒绝。判空放在这里而非调用处，避免第二个组合点
	// 遗漏判断。
	if ec.Connector == "" {
		return &Result{}, nil
	}

	role, err := vllmECRole(in.Role)
	if err != nil {
		return nil, err
	}

	doc := &vllmECTransferConfig{ECRole: role}
	switch ec.Connector {
	case vllmECCPUConnector:
		// 在缺乏 NPU 拷贝通路的引擎上直接拒绝，而不是照常渲染让容器启动失败：整个传输过程是
		// 设备间拷贝，而 Ascend 构建不包含所需的 NIXL 后端。
		if in.Engine == EngineVLLMAscend {
			return nil, newRefusal(ReasonTransportUnsupported,
				"the NIXL encoder-cache connector has no NPU copy path on engine %q, so every "+
					"value the leg declares is legal and the pair still has no container that can "+
					"run it; name %s instead", in.Engine, vllmECExampleConnector)
		}
		if ec.CPUBytes <= 0 {
			return nil, newRefusal(ReasonConnectionIncomplete,
				"the NIXL encoder-cache connector needs the size of the CPU region to offload "+
					"encodings into, and it raises ValueError without the key; declare one on the "+
					"encoder-cache leg")
		}
		doc.ECConnector = vllmECCPUConnector
		doc.ECConnectorExtraConfig = &vllmECConnectorExtraConfig{
			ECEnableNIXL: true,
			ECCPUBytes:   ec.CPUBytes,
		}
	case vllmECExampleConnector:
		if ec.SharedStoragePath == "" {
			return nil, newRefusal(ReasonConnectionIncomplete,
				"the shared-directory encoder-cache connector needs the mount path both ends "+
					"share, and falls back to /tmp without it, which in a Pod is a directory "+
					"neither end can see; declare one on the encoder-cache leg")
		}
		doc.ECConnector = vllmECExampleConnector
		doc.ECConnectorExtraConfig = &vllmECConnectorExtraConfig{SharedStoragePath: ec.SharedStoragePath}
	default:
		return nil, newRefusal(ReasonEngineUnknown,
			"encoder-cache connector %q is not one the operator's decode proxy speaks: its sidecar "+
				"carries the shared-directory and NIXL encodings, so this leg would move nothing "+
				"whatever the engine's own registry resolves", ec.Connector)
	}

	encoded, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("marshal the vLLM encoder-cache configuration: %w", err)
	}
	result := &Result{Args: []string{vllmECTransferConfigArg, string(encoded)}}

	// 该模型运行器环境变量对所有角色都是连接器的前置条件，而不仅限于生产端。
	// ECCPUConnector.__init__ 在检查传入角色之前就会校验 use_v2_model_runner，为 false 时直接抛出
	// ValueError（见 vLLM v0.30.0 `vllm/distributed/ec_transfer/ec_connector/cpu/connector.py:40-44`），
	// 且工厂函数为消费端与生产端构造的是同一个类（`ec_connector/factory.py:36-56`，区分的是 worker
	// 或 scheduler，而非生产端或消费端）。因此缺少该变量会导致该分支上的 prefiller 在构造阶段反复崩溃。
	// 共享目录分支不设置该变量，因为 ECExampleConnector.__init__ 不读取它（`example_connector.py:54-67`）。
	if doc.ECConnector == vllmECCPUConnector {
		result.Env = []core.EnvVar{{Name: vllmModelRunnerEnv, Value: "1"}}
	}

	if role == vllmECConsumerRole {
		return result, nil
	}

	// 以下仅适用于生产端。侧信道变量仅由启用 NIXL 传输的生产端读取，消费端渲染它们只会宣告一个
	// 自身并不会监听的地址。
	if doc.ECConnector == vllmECCPUConnector {
		result.Env = append(result.Env,
			core.EnvVar{
				Name: vllmECSideChannelHostEnv,
				ValueFrom: &core.EnvVarSource{
					FieldRef: &core.ObjectFieldSelector{FieldPath: vllmECPodIPFieldPath},
				},
			},
			core.EnvVar{Name: vllmECSideChannelPortEnv, Value: strconv.Itoa(int(VLLMECSideChannelPort))},
		)
		result.Ports = []core.ContainerPort{{
			Name: "ec-side-channel", Protocol: core.ProtocolTCP, ContainerPort: VLLMECSideChannelPort,
		}}
	}

	result.DefaultedArgs = [][]string{
		{vllmEnforceEagerArg},
		{vllmNoPrefixCachingArg},
		{vllmECMaxNumBatchedTokensArg, vllmECMaxNumBatchedTokens},
		{vllmMMEncoderOnlyArg},
	}
	return result, nil
}

// EncoderCacheOwnedKeys 返回编码器缓存分支针对给定请求写入并自行决定的命令行参数和环境变量。
//
// 调用方使用此函数拒绝同时声明了其中任一项的工作负载。否则两份配置都会生效，引擎将采用最后解析到的
// 值，从而可能关闭 NIXL 传输、将侧信道指向消费端无法连接的地址，或在运行中的部署下偷偷更换连接器——
// 而引擎日志无法将这些情况与分支单纯失效区分开来。
//
// 返回的两个列表是精确集合而非固定集合，因为该分支写入的内容同时取决于连接器和角色：模型运行器在
// 两种角色下都属于 NIXL 分支，而侧信道仅属于生产端。两者分别返回是因为命令行参数和环境变量通过不同
// 机制进行拒绝校验，且每个返回值始终为列表（该类别下无托管键时为空列表），以便调用方无需归一化即可
// 直接与渲染输出比对。
//
// 未指定连接器的请求不拥有任何键，因为它不会渲染任何内容；而 model_deployment_connector.go 中
// 构建 operator 托管键集合的调用方始终会指定连接器，因此这些键在每次部署中均受托管。
//
// 之所以同时接收角色参数，是因为仅凭请求本身无法确定结果：同一个 NIXL 分支在消费端仅托管一个
// 环境变量，在生产端则托管三个。
func EncoderCacheOwnedKeys(role Role, ec EncoderCache) (args, env []string) {
	if ec.Connector == "" {
		return []string{}, []string{}
	}
	args = []string{vllmECTransferConfigArg}
	if ec.Connector != vllmECCPUConnector {
		return args, []string{}
	}
	env = []string{vllmModelRunnerEnv}
	if role == RoleEncode {
		env = append(env, vllmECSideChannelHostEnv, vllmECSideChannelPortEnv)
	}
	return args, env
}
