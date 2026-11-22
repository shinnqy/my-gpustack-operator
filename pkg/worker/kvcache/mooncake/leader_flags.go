package mooncake

const (
	// LeaderRPCPort 是 leader 提供 RPC 服务的端口，也是引擎客户端连接的端口。
	// 这里将其固定而未沿用构件自身的默认值，是因为 Service 和对外发布的端点
	// 必须明确指定端口号，若默认值发生变动会导致这些配置静默漂移。
	LeaderRPCPort = 50051
	// LeaderMetricsPort 是 leader 同时提供 Prometheus 指标暴露和 HTTP 管理 API
	// 的端口。单端口承载双接口是构件本身的设计，而非此处的简化处理，这也是对外
	// 发布的管理端点与指标抓取目标使用同一地址的原因。
	LeaderMetricsPort = 9003

	// LeaderPodNameEnv 和 LeaderPodNamespaceEnv 是在 Kubernetes 选举模式下
	// 渲染出的 argv 所引用的环境变量。运行该 argv 的 Workload 必须通过
	// downward API 定义这两个变量，引用才能正确解析。
	//
	// 它们沿用本仓库自身的命名，而非该标志文档中写明的默认来源 POD_NAME / POD_NAMESPACE。
	// 这不会带来任何缺失：相关标志均会显式渲染，因此构件永远不会回退去自行读取
	// 默认环境变量，而且本仓库中的其他所有组件也均采用这种拼写方式。
	LeaderPodNameEnv      = "KUBERNETES_POD_NAME"
	LeaderPodNamespaceEnv = "KUBERNETES_POD_NAMESPACE"
	// LeaderPodIPEnv 是第三个环境变量，与前两个一样，仅在启用 Kubernetes 选举时
	// 定义，因为只有选举逻辑会读取它。其具体作用参见 -rpc_address 标志。
	LeaderPodIPEnv = "KUBERNETES_POD_IP"

	// LeaderKVLeaseTTL 表示缓存对象受保护免于淘汰的时长，用于替代构件自身的 10 秒默认值。
	//
	// 租约在对象被读取时授予，而非写入时：完成写入时授予的是零时长租约，因此尚未被
	// 读取过的对象可立即被淘汰。因此该值限制的是最近读取集合（RECENTLY READ）的大小，
	// 而非写入速率；淘汰机制会直接跳过受保护对象，即使找不到其他可淘汰对象也不会回退选择它们。
	//
	// 10 秒过短，一个引擎副本刚读取的块，在下一个副本请求之前就可能再次变为可淘汰状态，
	// 而共享缓存存在的意义正是提供这种跨副本复用。5 分钟足以覆盖该需求，且只要在该窗口内
	// 读取的数据集未超出存储容量就不会产生额外开销；若超出，则说明存储容量低于自身工作集，
	// 而非该配置值导致的问题。
	//
	// 它采用时长字符串格式，因为该标志支持 ms、s、m 和 h 后缀。
	LeaderKVLeaseTTL = "5m"
)
