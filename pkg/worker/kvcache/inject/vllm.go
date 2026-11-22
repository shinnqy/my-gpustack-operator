// 本文件负责为 vLLM 系列引擎渲染配置。它以文件作为载体，因为该引擎没有其他途径：
// `MooncakeStoreConfig.load_from_config()` 会读取 MOONCAKE_CONFIG_PATH 并在未设置时抛出异常
// （`worker.py:144-151`），且该环境变量指定的是文件路径而非直接承载配置内容。
package inject

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

// renderVLLMWithEncoderCache 在 renderVLLM 的基础上组合了编码器缓存分支。若 EncoderCache
// 未指定连接器，则其余所有分支的输出与 renderVLLM 完全一致。
func renderVLLMWithEncoderCache(in Input, ec EncoderCache) (*Result, error) {
	// 在第 20 章实现此函数。
	var result0 *Result
	var result1 error
	return result0, result1
}
