// 本文件负责为 SGLang 渲染配置。其载体为环境变量，原因在于求值时机而非配置键覆盖范围。
//
// 本文件中的每一处上游源码位置均基于 v0.5.18 读取并注明符号名，因为目标文件在多个版本间
// 篇幅已增长至三倍：裸行号在文件变动后会默默失去准确指向，而符号名才能在文件移动后重新定位依据。
//
// SGLang 在 `_load_config`（v0.5.18 `mooncake_store.py:294-314`）中选择配置来源：依次为
// 启动参数的 extra config、配置文件、环境变量。前两者在调用方运行即准入阶段就已固定——此时
// Pod 尚无 IP——且两者都会逐键回退到 `envs.<NAME>.default`（v0.5.18 `mooncake_store.py:110-168`
// `from_file` 与 `:212-261` `load_from_extra_config`），它是 `EnvField.__init__`
// （v0.5.18 `environ.py:39-40`）中赋值的字面属性，而非读取进程环境变量的访问器（v0.5.18
// `environ.py:57` 的 `EnvField.get`，其通过 `:52-55` 的 `_resolve_default` 获取默认值）。
// 因此在前两种方式下，池中每个 Pod 的 `local_hostname` 都会解析为字面量 "localhost"。
//
// 该键表示网络地址：vLLM 通过本机 IP 自行计算（`rdma_utils.py:21-25`）。唯有环境变量能将
// 求值推迟到 kubelet 阶段，使指向 status.podIP 的 fieldRef 在容器启动时完成解析。等待上游
// 新版本也无济于事，因为该值并非架构缺失——在写入配置文件的那一刻它还根本不存在。
//
// 环境变量路径通过不设置 SGLANG_HICACHE_MOONCAKE_CONFIG_PATH 来启用，因此本渲染器不会
// 输出该变量。若用户自行设置该变量，则会平滑且正确地接管配置：显式配置应当优先于默认配置。
package inject

import ()

const (
	// SGLang 在 `load_from_env`（v0.5.18 `mooncake_store.py:170-210`）中通过 `.get()` 读取的
	// 环境变量。
	//
	// sglangMetadataServerEnv 存在一个值得注意的拼写陷阱：META_DATA 中间多了一个下划线，而常见
	// 拼写 METADATA 没有。此处拼写错误不会报错——该键只会默默回退到默认值——因此测试会按字节
	// 精确锁定该名称，而非直接引用本常量。
	sglangMasterEnv         = "MOONCAKE_MASTER"
	sglangMetadataServerEnv = "MOONCAKE_TE_META_DATA_SERVER"
	sglangProtocolEnv       = "MOONCAKE_PROTOCOL"
	sglangDeviceEnv         = "MOONCAKE_DEVICE"
	sglangGlobalSegmentEnv  = "MOONCAKE_GLOBAL_SEGMENT_SIZE"
	sglangLocalHostnameEnv  = "MOONCAKE_LOCAL_HOSTNAME"

	// sglangTenantEnv 传递 Binding 的复用域。所选镜像是否读取该变量属于镜像维护者的兼容性责任。
	sglangTenantEnv = "MOONCAKE_TENANT_ID"

	// sglangBackendArg 用于启用存储后端。与 vLLM 的 connector 参数一样，它没有对应的环境变量，
	// 因此该引擎也无法在不追加启动参数的情况下完成配置。
	sglangBackendArg   = "--hicache-storage-backend"
	sglangBackendValue = "mooncake"

	// sglangBackendExtraConfigArg 以 JSON 格式传递存储的额外配置。渲染结果中仅包含
	// extra_backend_tag，SGLang v0.5.18 会将其置于每个存储键的最前方
	// （`mem_cache/storage/mooncake_store/mooncake_store.py:419-430`）。既不包含
	// master_server_address 也不包含 client_server_address 的额外配置不会触发从其读取
	// 存储设置的加载器（`:294-314`），因此存储设置仍来自此处渲染的环境变量。
	sglangBackendExtraConfigArg = "--hicache-storage-backend-extra-config"

	// 分离（disaggregation）参数用于指定 prefill/decode 拆分模式。三者均为 ServerArgs 字段：
	// disaggregation_mode（v0.5.18 `server_args.py:3101-3105`，字面量 "null"/"prefill"/"decode"，
	// "null" 表示不拆分）、disaggregation_transfer_backend（v0.5.18 `server_args.py:3106-3113`，
	// 可选值 DISAGG_TRANSFER_BACKEND_CHOICES，默认 "mooncake"）以及 disaggregation_bootstrap_port
	// （v0.5.18 `server_args.py:3114-3118`，默认 8998，"prefill 服务器上的引导服务端口"）。
	//
	// 即使后端默认值已是 mooncake，此处仍显式写入，因为该值属于配对属性：路由器侧握手与
	// 引擎的传输引擎必须保持一致，若放任其使用默认值，某角色自带的参数就可能单方面改动一侧，而
	// 此处却无法指出被破坏的是哪一端。
	sglangDisaggregationModeArg          = "--disaggregation-mode"
	sglangDisaggregationBackendArg       = "--disaggregation-transfer-backend"
	sglangDisaggregationBackendMooncake  = "mooncake"
	sglangDisaggregationBootstrapPortArg = "--disaggregation-bootstrap-port"

	// sglangDisaggregationBackendMooncakeTCP 是固定使用 TCP 的同一 Mooncake 后端。SGLang 的
	// 分离钩子（v0.5.18 `arg_groups/pd_disaggregation_hook.py`）会在未设置 MC_FORCE_TCP 时
	// 将其设为开启，并将后端改回 mooncake 同时清除 IB 设备，因此这相当于 vLLM 渲染器通过
	// 环境变量实现的行为在 SGLang 引擎中的原生写法。配对中的路由器侧无论如何都使用 mooncake
	// 协议，仅引擎侧的传输引擎被固定为 TCP。
	sglangDisaggregationBackendMooncakeTCP = "mooncake_tcp"

	// sglangRetractionBackupArg 用于选择 decode 端在撤回请求时将 KV 缓存保存于何处（v0.5.18
	// `server_args.py` ServerArgs.disaggregation_decode_retraction_backup，可选值 cpu_tensor
	// 与 host_pool）。若不设置，MHA 池上的 decode 端会推断为 host_pool 并构建分层主机内存池
	// （v0.5.18 `mem_cache/kv_cache_builder.py`），而 SGLang 要求节点全局可用内存必须超过
	// 固定的 10 GiB 预留量加上内存池大小才允许构建（v0.5.18 `mem_cache/pool_host/base.py`
	// HICACHE_HOST_MEMORY_RESERVE_BYTES）。cpu_tensor 按请求保存 CPU 张量且不构建内存池，
	// 因此 decode 端可在空闲内存低于该阈值的节点上启动。
	sglangRetractionBackupArg      = "--disaggregation-decode-retraction-backup"
	sglangRetractionBackupCPUValue = "cpu_tensor"

	// sglangHierarchicalCacheArg 用于开启存储后端所挂载的分层缓存。在 v0.5.18 中，只要
	// 指定了存储后端，调度器就会启用存储预取（`managers/scheduler.py`），但只有开启该标志后
	// 预取所读取的树形缓存才会是分层缓存（`mem_cache/registry.py`）；仅指定后端会保留普通
	// RadixCache，首个请求便会在 hicache_storage_pass_prefix_keys 上抛出 AttributeError。
	sglangHierarchicalCacheArg = "--enable-hierarchical-cache"

	// sglangPageSizeArg 设置 SGLang 的 KVCache 页粒度（`arg_groups/fields/schedule.py`，
	// 默认值为 None，在 `model_executor/model_runner.py` 和 `mooncake_store.py` 中实际
	// 等效为 1 token）。当 page_size=1 时，HiCache Mooncake 存储对象（`_batch_io_v2`）与
	// P/D 传输描述符（`disaggregation/mooncake/conn.py`）都会退化为逐 token 操作
	// （`arg_groups/kv_shard_hook.py`）。在 Mooncake TCP 上将 --page-size 默认设为 64 可将
	// 描述符数量减少 64 倍，符合 SGLang 对 Mooncake 存储与 KV 分片的推荐设置，满足限制或要求
	// page_size == 64 的注意力后端（flashmla、trtllm_mla、tokenspeed_mla、cutedsl_mla、
	// dsa、hpc_ops），能整除 chunked_prefill_size，并确保 prefill 与 decode 两端使用
	// `disaggregation/common/conn.py` 所要求的相同 --page-size。该参数输出在 DefaultedArgs
	// 中，以便模型或注意力后端需要其他取值的工作负载（例如混合或线性注意力模型需要 `--page-size 1`）
	// 可以通过在自身参数中声明 `--page-size` 来覆盖默认值。
	sglangPageSizeArg        = "--page-size"
	defaultSGLangTCPPageSize = "64"

	// 配对双方的端口，以及为何其中只有一个固定端口号。encoder 在引擎自身的 --port 上处理请求：
	// 仅编码服务器在 server_args.port 上运行 HTTP 服务（v0.5.18 `disaggregation/encode_server.py:4110`），
	// 并以 NetworkAddress(host, server_args.port)（`:3875`）对外宣告，因此仅语言端拨号的
	// 地址就是调用方已为每个角色渲染的端口，不存在第二个端口号。
	//
	// 反向回传通道在 v0.5.18 中没有固定端口（此结论经实测而非假设）。仅语言端会在操作系统分配的
	// 端口上绑定一个 ZMQ PULL 套接字（v0.5.18 `utils/network.py:202-218`，"随机分配的 TCP 端口"），
	// 调度器侧接收器绑定一次（v0.5.18 `disaggregation/encode_receiver.py:1611-1620`），每个等待
	// 请求再各绑定一次（`:790-797`），随后在编码请求中将自身主机与该端口告知 encoder；encoder
	// 再向其 PUSH 数据（v0.5.18 `disaggregation/encode_server.py:3010-3038`，`_push_embedding_to_prefill`）。
	// 因此这一链路需要 operator 保障的是两个 Pod 之间的网络可达性，而非某个公开端口号，此处也
	// 不会杜撰常量来替代。在任何请求到来之前双方唯一共享的端口号是下方的引导端口，双方在启动时
	// 即已知晓。
	//
	// encoder 分离参数用于指定将编码器拆分至独立服务器的第三阶段部署模式。
	//
	// sglangEncoderOnlyArg 与 sglangLanguageOnlyArg 为 ServerArgs 布尔字段（v0.5.18
	// `server_args.py:3171-3173` 与 `:3174-3176`），布尔字段通过 action="store_true"
	// 注册（v0.5.18 `arg_groups/arg_utils.py:313-319`），因此二者均为不带值的开关参数。
	//
	// sglangEncoderURLsArg 为 List[str] 字段（v0.5.18 `server_args.py:3193-3195`），未显式
	// 指定 nargs 的列表字段默认使用 "+"（v0.5.18 `arg_groups/arg_utils.py:293-297`），因此
	// 单个标志会接收其后直到下一个以短横线开头条目为止的所有参数。这也是 encoder 分支最后渲染该
	// 标志的原因：若在其后继续写入其他标志的值，该值会被当作另一个地址读取，导致引擎拨号一个
	// 并不存在的 Service。
	sglangEncoderOnlyArg  = "--encoder-only"
	sglangLanguageOnlyArg = "--language-only"
	sglangEncoderURLsArg  = "--encoder-urls"

	// sglangEncoderBootstrapPortArg 指定 EncoderBootstrapServer 的端口（v0.5.18
	// `server_args.py:3196-3200`）。该服务运行在仅语言端的分词器管理进程中（v0.5.18
	// `managers/tokenizer_manager.py:673-687`），因此它是 prefill 端口而非 encoder 端口：
	// encoder 会向该端口注册，而仅语言端接收器会从该端口拉取当前 URL 列表。只要设置了
	// --language-only，无论是否有 encoder 注册，同一进程都会启动该服务，因此始终需要渲染此参数。
	sglangEncoderBootstrapPortArg = "--encoder-bootstrap-port"

	// SGLangEncoderBootstrapPort 即上述端口。上游默认值为 8997（v0.5.18 `server_args.py:3196-3200`），
	// 此处显式渲染的原因与上方 sglangBootstrapPortAnnotation 对配对端口的考量相同：依赖默认值
	// 会让各方各自重复声明该端口号，而且这也是 Pod 必须开放的端口。
	SGLangEncoderBootstrapPort int32 = 8997

	// sglangEncoderBootstrapPortName 为容器上该端口的名称。Kubernetes 端口名称最多允许 15 个
	// 字符，因此不能拼写为 "encoder-bootstrap"。
	sglangEncoderBootstrapPortName = "enc-bootstrap"

	// SGLangBootstrapPort 是分离部署的 prefill 端提供引导注册表的端口，decode 端通过它获取
	// 传输端点。上游默认值为 8998（v0.5.18 `server_args.py:3114-3118`，
	// ServerArgs.disaggregation_bootstrap_port）；此处显式渲染而非沿用默认值，是因为另有两方
	// 需要与之配对——网关从 prefill Pod 的注解中读取该端口，decode Pod 的路由 sidecar 从其
	// 环境变量中读取该端口——依赖默认值会导致各方各自重复声明该端口号。
	SGLangBootstrapPort int32 = 8998

	// sglangBootstrapPortAnnotation 向通过 Kubernetes 发现工作节点的路由器公布 prefill 端的
	// 引导端口。网关仅在 prefill Pod 上读取该注解（sgl-project/sglang@gateway-v0.3.1
	// `sgl-model-gateway/src/service_discovery.rs:54,136-141`，Config.bootstrap_port_annotation
	// 的默认值）。注解值为十进制字符串形式的端口号，与网关自身的服务发现测试写法一致。
	sglangBootstrapPortAnnotation = "sglang.ai/bootstrap-port"

	// sglangPodIPFieldPath 是 kubelet 在容器启动时解析的字段路径，这也是该引擎采用环境变量
	// 进行配置的根本原因。
	sglangPodIPFieldPath = "status.podIP"
)

// SGLangEncoderOwnedArgs 返回本渲染器写入的 encoder 分离参数列表，以便拒绝用户覆盖受管
// 参数的调用方无需重复硬编码参数拼写即可引用它们。
//
// 返回完整列表而非单个键，是因为该分支包含三个关键参数：分别将 Pod 置于 encoder 或仅语言
// 形态的两个开关，以及携带双方配对地址的标志。用户传入的任何同名参数都会落在命令行中
// operator 参数之后，因此若漏管其中任何一个，就可能使声明为 encoder 的角色实际以仅语言
// 服务器启动，而两端仍各自对外宣告原始角色。
//
// 引导端口被纳入该列表的原因与模型自身的分离模式相同：它是一项配对属性，由仅语言端写入
// 并供 encoder 向其注册时拨号使用。
func SGLangEncoderOwnedArgs() []string {
	return []string{
		sglangEncoderOnlyArg,
		sglangLanguageOnlyArg,
		sglangEncoderURLsArg,
		sglangEncoderBootstrapPortArg,
	}
}

// renderSGLang 生成 SGLang 容器所需的环境变量与启动参数。
//
// 它不会以任何拼写写入 `local_buffer_size`，这并非遗漏，而是经过验证的设计：
// SGLang 的配置读取器没有对应的键，它在两条 store 初始化路径（`setup_dummy` 与 `setup`，
// 并非同一函数的两次调用）中均硬编码了 16 MiB 的 `DEFAULT_LOCAL_BUFFER_SIZE`，并各自注释为
// "Zero copy interface does not need local buffer"（v0.5.18 `mooncake_store.py:28,464,514`）。
// 输出该字段只会写入一个没有任何组件会读取的值。
//
// 同时它也未写入 mode，且这一点尚未经过实测验证。本注释此前曾声称读取器没有任何拼写的 mode 键，
// 但实际上它存在一个名为 `standalone_storage` 的键（搜索 "mode" 无法找到）：
// `load_from_env` 会从 `MOONCAKE_STANDALONE_STORAGE` 绑定该字段，而 `sglang.srt.environ`
// 默认将其设为 `EnvBool(False)`（v0.5.18）。因此，本渲染器为 SGLang 生成的 store 形态
// 与 vLLM 不同——vLLM 的注解在相同的 `"global_segment_size":0` 旁带有 `"mode":"standalone-store"`，
// 且 `client_config.go` 说明这两者总是成对写入。
//
// 此处是否应该写入该字段尚未确定，因此暂不写入。在部署中设置 `MOONCAKE_STANDALONE_STORAGE=true`
// 只是改变了引擎报错的位置，并未消除故障：store 的 warmup 不再被触发，`setup_dummy` 转而在其自身签名上
// 抛出 TypeError。本段注释旨在明确这一差异；要彻底解决该问题，需要在能够通过该调用的引擎版本中实测确认。
//
// `global_segment_size` 会被显式写入，因为 SGLang 在缺省时默认将其设为 "4gb"（v0.5.18
// `environ.py:704`, `MOONCAKE_GLOBAL_SEGMENT_SIZE`）——这与 vLLM 的陷阱相同，因此同样显式写入零值。
// 注意 SGLang 在向下传递配置值之前会将其按 tensor-parallel rank 均分（v0.5.18
// `mooncake_store.py:413,415-416`, `tp_scale_factor`）；零值除以任意数仍为零，但后续若在此处
// 渲染非零值，必须将该乘法关系考虑在内。
//
// 以上每处引用均附带了版本号与符号名，因为本注释此前引用的纯行号已全部漂移——
// 该上游文件现已超过 1300 行——而过时的行号在代码评审中极易因看似经过核对而被放过。
func renderSGLang(in Input) (*Result, error) {
	// 在第 20 章中实现此函数。
	var result0 *Result
	var result1 error
	return result0, result1
}
