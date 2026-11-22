// 本文件负责为 vLLM 系列引擎渲染 encoder/prefill/decode 部署中的编码器缓存分支：
// 即用于将多模态编码结果从 encoder 阶段传输到 prefiller 的参数、环境变量和端口。
//
// 它是独立于 KV 渲染之外的分支，因为这两种缓存互不相关。一种将注意力块从 prefill 端传向
// decode 端；另一种将编码结果从 encoder 阶段传向 prefill 端。因此一个 Pod 会渲染其中之一，
// 或（作为 prefiller 时）同时渲染两者，而 encoder 仅渲染此分支。
package inject

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
