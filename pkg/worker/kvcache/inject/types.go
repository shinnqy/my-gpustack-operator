// Package inject 以基于值的纯函数形式渲染 Pod 引擎容器所需的 KV cache 客户端配置：
// 不依赖 Kubernetes 客户端、ctx 或集群类型。解析需要渲染的内容由准入 Webhook 负责；
// 将其转换为环境变量、启动参数、Volume 与挂载点则由本包承担，因此测试计划中的每个用例
// 都只需一行表格数据，既不需要集群也不需要真实引擎。
//
// 本包渲染所遵循的契约是引擎自身的配置文件 schema，而非 Mooncake 客户端的 setup() 函数签名。
// 两者在字段名上并不一致——例如 master_server_address 对应 master_server_addr，
// device_name 对应 rdma_devices——而文件读取器会忽略无法识别的键，因此按签名拼写写入的值
// 会被静默丢弃。
package inject

import (
	"fmt"
	"slices"
)

// Engine 表示正在为其渲染配置的推理引擎名称。
//
// 它始终由调用方显式声明，绝不从容器镜像名称推断：不同引擎接受完全不同的启动参数，
// 而改名或由厂商定制打包的镜像会导致错误注入，且除了缓存完全不生效外没有任何明显症状。
type Engine string

// 本包支持渲染的引擎列表。
const (
	EngineVLLM       Engine = "vllm"
	EngineVLLMAscend Engine = "vllm-ascend"
	EngineSGLang     Engine = "sglang"
)

// Engines 以稳定顺序返回本包支持渲染的所有引擎。
//
// 其范围大于用户允许指定的集合，参见 SelectableEngines。
func Engines() []Engine {
	return []Engine{EngineVLLM, EngineVLLMAscend, EngineSGLang}
}

// SelectableEngines 以稳定顺序返回用户允许在注解中指定的引擎值。
//
// 其中不包含 EngineVLLMAscend，这使得该注解与 ModelDeployment.spec.engine 保持一致
// （后者已先一步明确了该问题）：vllm_ascend 是加速卡后端为 CANN 时 runner 安装的软件包，
// 而非用户自行选择的引擎。在这里它同样是派生值——由 operator 根据资源池的加速器类型选定——
// 因此保持可渲染但不可由用户直接指定。这一拆分消除了两个 API 面对同一概念暴露不同取值集合的问题，
// 而非再造一个表面来描述这种差异。
func SelectableEngines() []string {
	return []string{"vLLM", "SGLang"}
}

// ParseEngine 转换引擎注解的值，并拒绝所有不允许用户指定的值。
//
// 遇到未知值时选择拒绝而非回退到默认值，是因为没有任何可以安全猜测的引擎：
// 不同引擎的参数完全不同，注入错误的参数集合会导致容器正常启动却完全不使用缓存。
func ParseEngine(value string) (Engine, error) {
	switch value {
	case "vLLM":
		return EngineVLLM, nil
	case "SGLang":
		return EngineSGLang, nil
	}

	// 在此处拒绝而非删除该常量，因为外部传入的值正是在这里进入系统的。
	// 错误信息中指明了替代值，因为拒绝它的理由也正是替代值正确的理由：由加速器决定底层软件包，
	// 因此需要指定的引擎名称就是基础引擎名。
	if value == string(EngineVLLMAscend) {
		return "", newRefusal(ReasonEngineUnknown,
			"engine %q is not one this annotation takes: it names the Python package vllm_ascend, "+
				"which the runner installs when the accelerator backend is CANN, rather than an "+
				"engine anybody picks. Set %q and declare the Ascend runtime with "+
				"kvcache.gpustack.ai/manufacturer=%q", value, "vLLM", "ascend")
	}

	return "", newRefusal(ReasonEngineUnknown,
		"engine %q is not one this operator can configure; set one of %v", value, SelectableEngines())
}

// Role 是调用方为其 Pod 声明的 prefill/decode 角色。
type Role string

// 各个角色以及表示无角色的值。RoleNone 作为显式常量而非直接使用空字符串，
// 使得渲染器按角色分支时不会出现未标注的分支。
//
// RoleEncode 表示部署可声明的 encoder 阶段：该类型在此拥有独立常量，使连接器的角色映射函数
// 可以直接返回它，而不必将其折叠进 RoleNone；同时 engineRoleSupport 中缺少对应条目，
// 正是准入阶段拒绝不支持该角色的引擎的依据。该角色不承载 KV cache：encoder-cache 链路
// 属于引擎自身的 encoder 分离渲染逻辑，而非其余角色所写入的 store 客户端。
const (
	RoleNone    Role = ""
	RolePrefill Role = "prefill"
	RoleDecode  Role = "decode"
	RoleEncode  Role = "encode"
)

// ParseRole 转换角色注解的值。未设置该注解是合法的，表示调用方未启用 prefill/decode 分离，
// 这也是共享缓存下的常规场景。
//
// Pod 注解永远不会携带 "Encode"：encoder 阶段是由 API server 写入的部署形态，而非 Pod 侧开关；
// 若在此处接受该值，手写的注解将能使容器绕过按所设角色执行的渲染器。
func ParseRole(value string) (Role, error) {
	switch value {
	case "":
		return RoleNone, nil
	case "Prefill":
		return RolePrefill, nil
	case "Decode":
		return RoleDecode, nil
	default:
		return "", newRefusal(ReasonRoleUnknown,
			"role %q is not recognised; set %q, %q, or leave the annotation off",
			value, "Prefill", "Decode")
	}
}

// engineRoleSupport 记录每个引擎的渲染逻辑所支持的 Role 集合。
//
// 将其定义为查找表而非分散在各个渲染器中的分支，是因为准入处理程序必须在渲染任何内容之前
// 就能检查该问题：引擎不支持的角色必须在用户仍可修正的阶段被拒绝，而不是等到容器启动时才失败。
// 若调用方只能通过调用 Render 才能得知结果，就不得不为此构造完整的 Input。
//
// 各渲染器直接读取此表而非各自重复声明，因此它是一个生效中的约束而非单纯的文档说明。
// 修改其中的条目会直接改变实际渲染行为。
var engineRoleSupport = map[Engine][]Role{
	// 两个 vLLM 系引擎共用 renderVLLMWithEncoderCache，其中的 vllmKVRole 将三种缓存角色
	// 分别映射为 kv_both、kv_producer 和 kv_consumer。
	//
	// 此处包含 RoleEncode 是因为该渲染器对其有明确处理：encoder 阶段作为 encoder-cache 链路的
	// 生产者一侧，会被渲染到完全不运行任何 KV cache 的容器上；而 prefill 一侧则作为消费者读取同一链路
	// （参考 vLLM v0.30.0 `vllm/config/ec_transfer.py:12-14`）。因此表中的条目表示渲染器能够处理该角色，
	// 而非意味着 encoder 提供 KV cache 服务。
	EngineVLLM:       {RoleNone, RolePrefill, RoleDecode, RoleEncode},
	EngineVLLMAscend: {RoleNone, RolePrefill, RoleDecode, RoleEncode},
	// SGLang 的 store 客户端本身不区分角色，但其分离部署参数会携带这两个角色，因此三者均可渲染。
	// 这两个角色仅有的渲染逻辑就是分离模式，且遵循成对声明规则：未成对的角色（包括注入 Webhook
	// 准入的所有 Pod）会仅渲染共享 store，与无角色时完全一致。渲染器真正拒绝的是相反的情况，
	// 即只有点对点链路却没有成对的角色。
	//
	// 此处包含 RoleEncode 是因为该引擎对其有明确支持：SGLang 通过 --encoder-only 从自身 server 中
	// 拆分出 encoder 阶段（v0.5.18 `server_args.py:3171-3173`），而非通过本包需要描述的连接器。
	// 因此表中的条目表示渲染器能够处理该角色，而非意味着 encoder 提供缓存服务——它完全不持有 KV cache。
	EngineSGLang: {RoleNone, RolePrefill, RoleDecode, RoleEncode},
}

// SupportsRole 返回指定引擎的渲染逻辑是否支持该角色。
//
// 对于未知引擎或不在支持集合内的角色，均返回 false：采取保守判定并拒绝渲染，
// 避免渲染出没有任何组件读取的配置。
func SupportsRole(engine Engine, role Role) bool {
	return slices.Contains(engineRoleSupport[engine], role)
}

// Connection 保存配置解析阶段已确定的、Pod 将要连接的资源池信息。
//
// 其中不包含元数据面地址和 RDMA 设备列表，这并非遗漏。元数据面是点对点通信的，
// 因此无需传递地址——所有参与方均写入同一个常量；设备过滤列表留空以使客户端按主机自动发现，
// 这是同一资源池的所有消费者唯一正确的配置方式。
type Connection struct {
	// MasterAddress 是资源池对外发布的客户端端点，格式为 host:port。它是推理引擎连接的目标地址，
	// 绝非仅用于配额账本且不对外发布的后端管理地址。
	MasterAddress string

	// Protocol 是已由调用方从后端 API 拼写映射为制品自身拼写的传输协议。
	// 它是后端全局配置而非按节点区分：一个成员组只渲染一个 DaemonSet，因此单个 Pod 模板无法按节点
	// 携带不同的传输协议。
	//
	// 它仅用于 store 客户端。点对点链路并不从中获取传输协议——两个数据面分别声明各自协议，
	// 原因见 defaultKVTransferProtocol；该链路读取它只是为了判断将自身固定为 tcp 是否会剥夺
	// store 的高速网络能力，原因见 directLegForcesTCP。
	Protocol string
}

// Parallelism 表示单个角色声明的并行形态，包含点对点传输配置文档所需的两个维度。
// 其零值表示未声明并行形态；orOne 会将其渲染为引擎自身的默认值 1，这与配置文档此前使用字面量时的
// 行为一致。
type Parallelism struct {
	// TensorParallel 是该角色声明的张量并行度。
	TensorParallel int

	// DataParallel 是该角色声明的数据并行度。
	DataParallel int
}

// ParallelismPair 表示 prefill/decode 配对中双方角色共同声明的并行形态。
// 两者作为一个整体传递，是因为传输配置文档会同时校验双方：如果渲染器只使用调用方提供的其中一侧
// 而对另一侧取默认值，就会生成错误的块布局而非直接拒绝。
type ParallelismPair struct {
	// Prefill 是 prefill 角色声明的并行形态。
	Prefill Parallelism

	// Decode 是 decode 角色声明的并行形态。
	Decode Parallelism
}

// Input 包含配置合成所需的全部信息，且均已完成解析与校验。
type Input struct {
	// Engine 用于选择对应的渲染器。
	Engine Engine

	// Role 是 prefill/decode 角色；若调用方未声明，则为 RoleNone。
	Role Role

	// Disaggregated 表示声明该角色的部署是否同时声明了 prefill/decode 配对的双方。
	//
	// 它控制引擎是否启用自身的分离模式，因为仅凭 Role 无法做到这一点：只持有一侧角色的部署
	// 没有可交接 KV 块的配对端，若仍以半边模式启动引擎，它会一直等待一个永远不会创建的对端，
	// 而路由层却会向其发送完整请求——同一对象的两个层级对“这是否为分离部署”给出了矛盾的回答。
	// 路由层已率先采用相同规则：只有当两侧选择器都存在时，其渲染器才进入分离模式，否则将孤立的
	// 单侧按非分离形态路由；本字段正是将该规则延伸到引擎侧。这一做法的代价也明确说明如下：
	// 刻意仅声明单侧以向共享 store 写入数据的部署，现在会以非分离模式运行引擎，而此前它会作为
	// 一个从未声明完整配对的半边启动。
	Disaggregated bool

	// Domain 是 Binding 声明的复用域。对于携带租户标识的引擎，非空值会被原样输出；
	// 空值（对应不维护租户账本的 master）则完全不渲染租户字段。由于该路径不检查容器镜像，
	// 因此不做引擎版本检查。
	Domain string

	// Connection 是资源池及其后端发布的连接信息。
	Connection Connection

	// CachePrefix 是 store 键所带有的权重标识前缀，为空表示不设置前缀。
	//
	// 两种引擎的 store 键中都不包含权重标识。vLLM 的 Mooncake store 键仅携带 --model 的最后一段路径，
	// SGLang 仅携带 served model name，两者均不包含 revision 或 digest，因此同一租户下服务不同权重的
	// 两个部署会相互读取对方的 KV 块——已在 vLLM 上针对同一仓库的两个不同 commit 实测验证过。
	// 该前缀即各引擎允许服务端设置的键命名空间：vLLM 对应 kv_connector_extra_config.cache_prefix，
	// SGLang 对应 extra_backend_tag。它仅在配置了 store 时渲染：点对点链路不使用任何键。
	//
	// 前缀中不得包含 "@"、"_" 或 ":"，这是两种引擎拼接键时使用的分隔符；
	// 若包含这些字符，Render 会拒绝渲染。
	CachePrefix string

	// Dtype 是 Binding 声明的数据元素类型，在 store 配置旁渲染为 KVCacheDtypeArg 并原样透传：
	// 可接受的拼写由各引擎自行决定，本包不做校验。为空时不渲染。
	//
	// 两种引擎的 store 键中均不包含 dtype，因此以不同 dtype 缓存同一提示词的两个引擎会写入相同的键。
	// 随后使用更宽 dtype 的读取方会将较窄的块加载为“成功”，却读入部分陈旧缓冲区的数据，因为两个引擎
	// 都不会校验实际读取的字节数与预期字节数。渲染 Binding 的值可使同一 Binding 上的所有引擎
	// 写入一致的元素类型，包括原本处于 "auto" 而随模型变化的引擎。
	Dtype string

	// KVTransfer 启用受管的 prefill/decode 路由器所使用的点对点连接器。
	// 当提供完整的 Connection 时，它与共享 store 组合渲染；否则单独渲染。
	// 两者是正交的输入而非互斥选项：可以同时开启。
	KVTransfer bool

	// KVTransferProtocol 是 Mooncake 拼写形式的直连链路传输协议，已由调用方从 ModelDeployment API 值
	// 完成映射。为空时渲染为 tcp。SGLang 会根据该值选择后端，而非直接透传。
	KVTransferProtocol string

	// Parallelism 是配对双方声明的并行形态，由调用方根据各自角色的配置解析得到。
	// 只有需要校验它的文档才会读取该字段——即 Ascend 传输链路的配置文档——且零值在其中会渲染为 1/1，
	// 与此前内联字面量的渲染结果完全一致，因此没有角色需要解析的调用方不会受到任何影响。
	Parallelism ParallelismPair

	// PublishKVEvents 指示引擎为路由器发布缓存放置事件。它由调用方按角色解析；
	// 为 false 时，普通连接器的渲染结果与此前逐字节完全一致。
	PublishKVEvents bool

	// KVEventsHost 是与发布器固定端口配对的可拨号主机地址。引擎监听通配符地址，
	// 而该地址无法作为端点发布给消费者。
	KVEventsHost string

	// EncoderURLs 是部署中各 encoder 副本的地址列表，按引擎自身的拼写方式对应每个副本或每个副本 Service。
	// 正是该字段使部署构成三角色形态，且仅有 prefill 一侧会读取它。
	//
	// 它作为输入传入而非在此处解析，是因为在副本的 Service 创建之前其地址并不存在，
	// 而 Service 是调用方在协调（reconcile）阶段而非准入阶段构造的对象。
	//
	// 为空时完全不渲染 encoder 链路，这正是所有不含 encoder 角色的部署所声明的形态，
	// 从而确保该形态的渲染结果与新增此字段之前完全一致。
	EncoderURLs []string

	// EncoderCache 是当前角色运行的 encoder-cache 链路，由调用方指定而非在此处派生。
	// 零值表示不启用该链路，此时不渲染任何内容，并使本结构体的其余所有字段保持与新增该链路前
	// 完全一致的渲染结果。
	//
	// 它作为输入传入是因为引擎所需解析的连接器不应由本包自行挑选：它对应准入阶段为该路由器、引擎
	// 和加速器所接受的条目，若渲染器自行选择，就可能与已被接受的对象不一致。
	// 上方的 EncoderURLs 则是改由直接携带地址的引擎所使用的同一链路的另一端。
	EncoderCache EncoderCache
}

// MooncakeForceTCPEnv 是使 Mooncake 传输引擎仅使用 TCP 的环境变量名。
// 它由传输引擎本身读取，而非由任何引擎的配置类读取，且仅检查是否存在（不关心值）。
const MooncakeForceTCPEnv = "MC_FORCE_TCP"

// Mooncake TCP 传输调优变量，在传输协议为 TCP 时由 vLLM 和 SGLang 进程内的 C++ Mooncake
// Transfer Engine（`from mooncake.engine import TransferEngine`）读取。
//
// 在多层长提示词的 KV 传输中，Mooncake 默认的每对端排队传输上限（1024）和单通道准入配额会溢出，
// 导致 Transport::submitTransfer 和 TcpTransport::allocateLocalBuffer 返回 ret=-1 失败，并使
// prefill KV 块滞留。将每对端传输队列和待准入队列提升至 65535、为每个对端打开 16 条并行 TCP
// socket 通道，并将准入超时延长至 60000 ms，可防止描述符队列溢出并平滑多流 TCP 吞吐。
// 这四个变量均通过 DefaultedEnv 输出，因此工作负载自身的声明会覆盖 operator 默认值。
const (
	MooncakeTCPMaxQueuedTransfersPerPeerEnv   = "MC_TCP_MAX_QUEUED_TRANSFERS_PER_PEER"
	MooncakeTCPMaxPendingAdmissionsPerPeerEnv = "MC_TCP_MAX_PENDING_ADMISSIONS_PER_PEER"
	MooncakeTCPLanesPerPeerEnv                = "MC_TCP_LANES_PER_PEER"
	MooncakeTCPAdmissionTimeoutMsEnv          = "MC_TCP_ADMISSION_TIMEOUT_MS"

	defaultMooncakeTCPMaxQueuedTransfersPerPeer   = "65535"
	defaultMooncakeTCPMaxPendingAdmissionsPerPeer = "65535"
	defaultMooncakeTCPLanesPerPeer                = "16"
	defaultMooncakeTCPAdmissionTimeoutMs          = "60000"
)

// KVCacheDtypeArg 是两类引擎用于读取 KV 缓存元素类型的命令行参数：vLLM v0.29.0
// `vllm/config/cache.py` 的 CacheDType 与 SGLang v0.5.18 `server_args.py` 的 kv_cache_dtype。
// 两者都会在启动时按各自的可选值校验，因此引擎不识别的拼写会直接终止容器，而不会被静默忽略。
const KVCacheDtypeArg = "--kv-cache-dtype"

// Reason 对拒绝原因进行分类。调用方依据它做分支判断；随附的消息面向人工阅读，
// 调整措辞不会破坏调用方逻辑。
type Reason string

// 本包拒绝渲染的原因。每一项都代表一种特定条件：在此条件下若强行渲染任何配置，
// 都会产生一个表面上已配置但实际并未生效的容器。
const (
	// ReasonEngineUnknown 表示引擎值不在允许的集合内。
	ReasonEngineUnknown Reason = "EngineUnknown"

	// ReasonRoleUnknown 表示角色值不在允许的集合内。
	ReasonRoleUnknown Reason = "RoleUnknown"

	// ReasonRoleUnsupported 表示引擎没有已知配置项支持的角色（或随角色一并请求的能力）：
	// 例如引擎无法表达的角色、声明值组合无法运行的角色配对（各自单独合法，但组合会在引擎启动时
	// 被拒绝），或者向既不渲染点对点传输也不渲染 KV 事件发布的引擎请求了相应能力。接收并忽略这类
	// 请求会导致静默错误，而这正是本包旨在避免的。
	ReasonRoleUnsupported Reason = "RoleUnsupported"

	// ReasonConnectionIncomplete 表示输入缺少渲染所必需的信息：Connection 缺少引擎启动必需的
	// 值、输入未请求存储、直连传输或事件发布中的任何一项、引擎缺少共享存储无法运行，或事件发布器
	// 没有可拨号的主机地址。encoder 角色不受前三项限制，因为它渲染的分支并非本包负责连接的缓存。
	ReasonConnectionIncomplete Reason = "ConnectionIncomplete"

	// ReasonTransportUnsupported 表示引擎的存储后端拒绝该池传输方式。它与 ReasonConnectionIncomplete
	// 区分开来，因为此时 Connection 是完整的：每个值都已提供且合法，只是两者的组合无法在任何容器中运行。
	ReasonTransportUnsupported Reason = "TransportUnsupported"

	// ReasonCachePrefixInvalid 表示缓存前缀中包含了引擎用于拼接存储键的分隔符。此类前缀拼接后
	// 可能生成与其他前缀相同的键，导致两个不同的权重身份在表面上均已配置的情况下互相读取对方的块。
	ReasonCachePrefixInvalid Reason = "CachePrefixInvalid"
)

// RefusalError 表示被拒绝的渲染结果，包含供调用方分支判断的原因以及指明被拒对象的消息。
type RefusalError struct {
	Reason  Reason
	Message string
}

// Error 实现 error 接口。
func (r *RefusalError) Error() string {
	return r.Message
}

// newRefusal 构造带类型的拒绝错误。格式化参数用于指明对象（如引擎、角色、域），
// 因为让阅读者无法采取行动的拒绝几乎与静默失败无异。
func newRefusal(reason Reason, format string, args ...any) error {
	return &RefusalError{
		Reason:  reason,
		Message: fmt.Sprintf(format, args...),
	}
}
