package worker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	core "k8s.io/api/core/v1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/worker/kvcache/inject"
	"gpustack.ai/gpustack/pkg/worker/kvcache/mooncake"
	"gpustack.ai/gpustack/pkg/worker/settings"
)

// 挂载路径、文件名及其拼接逻辑归属于 `inject` 包，不在此处重复声明。该包负责渲染文件，
// 而本包仅负责上报文件落盘位置，因此若再定义一对相同值的常量，就相当于对同一事实做了
// 两份定义——而且一旦发生漂移，出错的必然是这里，因为实际被校验的是渲染器。

// ModelDeploymentConnectorInput 包含 connector 合成所需的全部信息，均以纯值形式表示。
//
// 它接收纯值而非对象，使合成成为一个纯函数，无需 KVCachePoolBinding、KVCachePool 或
// 运行中的 master 即可测试。将 Binding 解析为该结构是另一项独立职责。
type ModelDeploymentConnectorInput struct {
	// Engine 决定渲染哪款引擎的参数键和载体，同时也是每项自有键决策的左半部分依据。
	Engine string

	// Kind 告知引擎当前角色的类型，也是使 prefill 角色与 decode 角色产生不同配置而非互为
	// 副本的唯一要素。
	//
	// 其零值对应分离部署之前的 deployment 形态，渲染结果与单角色 deployment 始终保持一致。
	Kind workercore.ModelDeploymentRoleKind

	// Disaggregated 上报声明该角色的 deployment 是否同时声明了 prefill/decode 配对的双方。
	// 该值仅从 spec 得出，因此准入与渲染不会对此产生分歧，同时也使引擎自身的拆分模式与
	// router 保持步调一致：单独的一方按未拆分路由，现在也按未拆分运行。无论是否拆分，store
	// 客户端上的角色区分标识仍遵循 Kind，因此声明用于向共享 pool 供数的单独一方仍会继续
	// 向其写入。
	Disaggregated bool

	// Manufacturer 是该角色所属 pool 的加速器厂商（采用本项目命名约定），例如 "nvidia" 或
	// "ascend"。它用于选择 CONNECTOR，而引擎本身并不做此选择。
	//
	// 为空表示 pool 的硬件尚未被观测到，此时会渲染厂商中立的 connector 而非报错：否则就会
	// 因为 status 尚未收敛而拒绝挂载缓存。
	Manufacturer string

	// Domain 是 Binding 声明的复用标识——当 pool 的 master 未维护用于隔离的租户账本时则
	// 为空：面对无账本的 master 时不传递任何租户标识，而非传递一个会被其折叠进默认租户的值。
	// 引擎构建版本是否读取渲染出的值，属于镜像维护方的兼容性职责。
	Domain string

	// CachePrefix 是存储键所使用的权重标识前缀，派生自
	// 部署的 ModelArtifact；若部署未指定 ModelArtifact，则为空。
	CachePrefix string

	// Dtype 是 Binding 的 spec.domain.dtype，若 operator 尚未接管其渲染对应的
	// 参数标志，则为空。
	Dtype string

	// MasterServerAddress 是从池中观测到的存储 master 地址。
	MasterServerAddress string

	// Protocols 是池的后端所提供的协议列表，保留 artifact 中的原始拼写并按
	// 组声明顺序排列：即每个成员组经由 `mooncake.MemberProtocols` 映射后的
	// 有效协议。之所以为列表，是因为不同组之间可能存在差异——例如一个使用 fabric 的
	// VRAM 组与一个使用 TCP 的 DRAM 组并存。准入阶段会拒绝将无约束的引擎新绑定到
	// 此类池；合成阶段则对已有绑定保留优先选择首项的规则。
	//
	// 该字段仅用于存储客户端。点对点链路不会读取它——两个数据平面各自独立
	// 声明，原因见 defaultKVTransferProtocol。空值表示无存储形态，渲染器会
	// 基于其自身原因予以拒绝。
	Protocols []string

	// PublishKVEvents 在产生缓存块的被路由角色上为 true。
	PublishKVEvents bool

	// KVEventsHost 是供消费者拨号连接的角色 Service 主机名，用于替代发布方的通配符
	// 绑定地址。
	KVEventsHost string

	// KVTransfer 将点对点 P/D 传输与共享存储组合在一起。两者是正交的输入，
	// 可以同时开启；彼此并非互斥分支。
	KVTransfer bool

	// RoutingSidecar 请求启用解码代理。它与 KVTransfer 并行传递而非由后者
	// 派生，因为两者现在的判定条件不同：每个准入的配对都会携带传输链路，而只有
	// 特定路由器会携带该代理。
	RoutingSidecar bool

	// KVTransferProtocol 是声明在 ModelDeployment 上、供点对点链路使用的传输协议。
	// 为空时将渲染为渲染器的默认值。该值会原样透传：可接受的协议集合取决于引擎镜像中
	// 的 mooncake 构建，而非本 operator。
	KVTransferProtocol string

	// Parallelism 是 prefill/decode 配对双方的已声明并行形态，由调用方根据各自
	// 角色的记录解析得到。它作为单个值传递，因为传输配置文档会对配对双方进行校验：
	// 若两个角色分别基于两次解析结果进行合成，可能会产生不同的块配置，而与所描述
	// 引擎不一致的块配置会导致块布局错误而非被拒绝。只有会对其进行校验的文档会读取
	// 该字段——即 Ascend 传输链路的文档——其零值在两侧均会渲染为引擎自身的默认值 1。
	Parallelism inject.ParallelismPair

	// EncoderCache 是该角色在通过名称指定连接器（而非地址）的引擎（即 vLLM 系列）上
	// 运行的 encoder-cache 链路。它作为输入传入而非自行推导，因为连接器不由本包决定：
	// 准入阶段会根据该路由器、引擎与加速器的矩阵行来确定它，若合成器自行选择，可能会
	// 渲染出对象从未获准入的链路。
	//
	// 其零值表示不启用该链路，所有未声明 Encode 角色的部署都会渲染为此形态，且渲染
	// 结果保持不变。
	EncoderCache inject.EncoderCache

	// EncoderURLs 是编码器阶段的地址列表，对应同一链路中另一类引擎的那一半：
	// SGLang 在语言侧接收这些 URL 并向下传递给编码器，而非通过名称指定连接器。
	// 它是部署级而非角色级的，因为声明了 Encode 角色的部署只有一组地址，而不读取
	// 它们的角色不会据此渲染任何内容。
	//
	// 空值表示没有编码器阶段，此时不渲染任何内容。
	EncoderURLs []string
}

// 该结构体过去携带的两个字段现已移除，且并未丢失任何能力。
//
// `MetadataServer` 过去作为一个输入字段，其唯一取值始终是字面量 P2PHANDSHAKE，
// 该范围内的元数据平面现在无条件采用该值，统一定义为 `inject.MetadataServer`。
//
// `DeviceName` 曾是 RDMA 设备过滤器，移除它是一次修复。它以前接收具体设备名
// （例如测试中传入的 `mlx5_0`），而具体设备名按设计就不适用于资源池：设备是按
// 主机命名的，一台主机上是 `mlx5_0`，下一台可能是 `erdma_0`，因此没有任何单一
// 名称能适用于一个池所跨越的所有主机。`inject.DeviceName` 在包括 RDMA 在内的
// 所有路径上均为空，意为“使用发现的全部设备”，这是唯一在所有环境中都正确的值。
// 该字段过去并非提供调优手段，而是提供了配置一个在某些主机上什么都匹配不到的
// 过滤器的途径。

// ModelDeploymentConnectorRender 是合成阶段为单个角色生成的结果。
type ModelDeploymentConnectorRender struct {
	// Args 会附加到引擎命令行中，位于角色自身的 ExtraArgs 之前。
	Args []string

	// Env 是 operator 所拥有的环境变量：用户条目若命名了其中任意名称，会在准入阶段
	// 被拒绝，而非进行合并。
	Env []core.EnvVar

	// DefaultedEnv 是仅在用户未提供对应变量时才由 operator 补充的环境变量。
	// 此处出现重复是无害的，因为覆盖语义明确（后者生效），用户的值会保留且不会
	// 触发拒绝。
	DefaultedEnv []core.EnvVar

	// DefaultedArgs 是仅在角色自身的 ExtraArgs 未指定同名标志时才由 operator 补充的
	// 参数组。每组均以其标志开头，只要标志被用户指定，就会丢弃整组参数：后者生效
	// 可以解决带值标志的覆盖问题，但无法解决仅用于开启某功能的开关标志，因此选择
	// 丢弃整组而非覆盖。
	DefaultedArgs [][]string

	// Volumes 和 VolumeMounts 用于将客户端配置挂载进容器。对于通过环境变量承载配置的
	// 引擎，两者均为空，因为挂载一个该引擎从不读取的文件会让人误以为存在实际并未
	// 生效的接线。
	//
	// 它们与下方的 PodAnnotations 成对存在。该卷是对对应注解的 downwardAPI 投影，
	// 因此只应用卷而不应用注解会挂载出一个空文件——容器能启动、表面上看似已配置，
	// 但实际不使用任何缓存。渲染器将这三者一并返回，本结构体也将它们保持在一起，
	// 任何地方都不得只应用其中的子集。
	Volumes      []core.Volume
	VolumeMounts []core.VolumeMount

	// PodAnnotations 必须落到接收上述字段的同一个 Pod 上。
	//
	// 它保存的是客户端配置本身而非配置的摘要，这样配置内容变更无需额外字段即可触发
	// Pod spec 哈希变化：该哈希的计算对象是 {Labels, Annotations, PodSpec}。
	// 若使用 ConfigMap，传给 Pod 的只是名称，当内容变更时 PodSpec 在字节层面仍完全
	// 相同——副本会继续保留旧配置，而对哈希的检查却会错误地显示通过。
	PodAnnotations map[string]string

	// Ports 是合成的连接器配置所额外开放的端口。
	Ports []core.ContainerPort

	// KVEvents 是渲染后的可拨号事件契约；当该角色不发布事件时为 nil。
	KVEvents *inject.KVEvents

	// KVTransfer 在 Args 包含点对点 P/D 连接器时为 true。
	KVTransfer bool

	// RoutingSidecar 在 decode 副本前置了代为执行握手的代理时为 true。该标志在
	// 此处决定，而非由渲染器根据 KVTransfer 推算，因为当第二个路由器也能承载
	// 传输链路时，两者的含义便不再一致。
	RoutingSidecar bool

	// binding 记录合成该连接器所依据的外部绑定输入，使显式的 Pod spec 哈希能够
	// 对绑定生成指纹，而无需对合成后的参数标志或默认环境变量求哈希。
	binding *modelDeploymentConnectorBinding
}

type modelDeploymentConnectorBinding struct {
	Domain              string                 `json:"domain,omitempty"`
	Dtype               string                 `json:"dtype,omitempty"`
	CachePrefix         string                 `json:"cachePrefix,omitempty"`
	MasterServerAddress string                 `json:"masterServerAddress,omitempty"`
	Protocol            string                 `json:"protocol,omitempty"`
	KVTransfer          bool                   `json:"kvTransfer,omitempty"`
	KVTransferProtocol  string                 `json:"kvTransferProtocol,omitempty"`
	PublishKVEvents     bool                   `json:"publishKVEvents,omitempty"`
	KVEventsHost        string                 `json:"kvEventsHost,omitempty"`
	RoutingSidecar      bool                   `json:"routingSidecar,omitempty"`
	Disaggregated       bool                   `json:"disaggregated,omitempty"`
	Parallelism         inject.ParallelismPair `json:"parallelism,omitempty"`
	EncoderCache        inject.EncoderCache    `json:"encoderCache,omitempty"`
	EncoderURLs         []string               `json:"encoderURLs,omitempty"`
}

// modelDeploymentRoutesManaged 现在仅作为 KV 事件开关使用：表示该部署前置了
// 托管路由器，且不在 Ascend 上。
//
// 下方三个决策彼此独立给出答案，将它们分开的关键正在于明确它们并不共享相同条件。
// KV 事件发布器用于向某个路由器的数据层提供事件，因此它跟随该路由器及对应引擎——
// 并避开 Ascend，因为 vLLM-Ascend 的渲染不支持事件发布器，渲染被拒会导致错误
// 循环，而不是得到一个无事件的部署。引擎侧传输链路跟随每一对获准入的配对，因为
// 无法向 decoder 传递缓存块的 prefiller 在任何路由器下都无法完成分离式推理。
// decode 代理仅跟随某一个路由器，因为它从只有该路由器才会写入的请求头中读取端点。
func modelDeploymentRoutesManaged(md *workercore.ModelDeployment, manufacturer string) bool {
	return md.Spec.Router != nil && manufacturer != nodefeature.ManufacturerAscend
}

// ModelDeploymentMaySynthesizeConnector 报告渲染阶段是否能为该部署的任意角色
// 合成连接器。它仅由 spec 决定，并作为渲染器自身按角色判断的前置门控：连接需要
// 有可连接的缓存，而传输链路、事件发布器与路由代理各自都需要路由器，因此两者
// 均未指定的部署不会渲染任何连接器，也不会渲染其携带的任何监听器。准入阶段也
// 读取同一结果以保持一致。
func ModelDeploymentMaySynthesizeConnector(md *workercore.ModelDeployment) bool {
	return md.Spec.KVCache != nil || md.Spec.Router != nil
}

// ModelDeploymentDeclaresBothHalves 报告该部署的角色中是否同时包含 prefill 角色
// 和 decode 角色。
//
// 这是路由器渲染器已经遵循的配对规则在引擎侧的表述：仅包含其中一半的部署没有可
// 交接缓存块的配对对象，而所有回答“这是否为分离部署”的层级——路由器的模式、
// 引擎的传输链路、引擎自身的分离参数——必须给出一致的答案，否则同一个对象就会
// 用一种方式处理请求、用另一种方式搬运缓存块。路由器渲染器会将孤立的一半按未
// 拆分形态路由；该谓词使引擎侧能与之保持一致。这一取舍是有意承担的代价：刻意
// 只声明一半以向共享存储写入缓存的部署，现在会以未拆分形态运行其引擎，并依靠
// 存储客户端上的角色区分器来保留该半边对缓存池的贡献。
//
// 该函数被导出，因为 webhook 的端口预留逻辑也读取同一规则：配对关系在准入时
// 即可确定——角色写在 spec 中，不像池的厂商那样不可知——因此拒绝逻辑与渲染
// 逻辑能够在该维度上精确一致，既不会拒绝渲染实际会绑定的端口，也不会放行渲染
// 不会使用的端口。
func ModelDeploymentDeclaresBothHalves(md *workercore.ModelDeployment) bool {
	var prefill, decode bool
	for i := range md.Spec.Roles {
		switch ModelDeploymentEffectiveRoleKind(&md.Spec.Roles[i]) {
		case workercore.ModelDeploymentRoleKindPrefill:
			prefill = true
		case workercore.ModelDeploymentRoleKindDecode:
			decode = true
		}
	}

	return prefill && decode
}

// IMPORTANT: 判断当前角色需不需要开启 PD 点对点直传（有没有 Router、是否成对声明 P/D、Ascend+vLLM 是否配了 llm-d-router）
//
// modelDeploymentUsesKVTransfer 报告该角色是否运行被路由 prefill/decode 配对的
// 引擎侧。
//
// 在 Ascend 上它按路由器区分，差异在于握手方式而非传输链路本身：vLLM-Ascend
// 注册的连接器要求按请求中继 prefiller 的主机、端口、块 ID 和引擎 ID，而此处
// 唯一接入了该机制的驱动是 llm-d-router 的 decode 代理。vLLM 路由器自行驱动
// 配对，使用的协议词汇会被 Ascend 连接器拒绝，因此在其下的配对不渲染传输链路
// 而非渲染一条不可用的链路——这是 Ascend 一直以来的形态，并非功能退化。
//
// 链路跟随配对而非单边角色：不成对的孤立角色没有可交接缓存块的对象，且其前置
// 路由器也已按未拆分形态路由它。强行渲染该链路只会导致引擎启动后一直等待
// 路由器永远不会分配的对端。
func modelDeploymentUsesKVTransfer(
	md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole, manufacturer string,
) bool {
	// IMPORTANT: 不是看到一个叫 Prefill 的角色就能无脑开启 KV 交接。如果没有路由器、或者只写了半边角色、或者硬件和路由器对不上号，强行开启会让引擎启动后死等一个永远不会来的搭档！

	if md.Spec.Router == nil {
		// IMPORTANT: 没有托管 Router，谁来帮请求在 Prefill 和 Decode 之间牵线搭桥？所以不能开 kv transfer
		return false
	}
	if manufacturer == nodefeature.ManufacturerAscend &&
		md.Spec.Engine.Name == workercore.ModelDeploymentEngineVLLM &&
		md.Spec.Router.Name != workercore.ModelDeploymentRouterLLMD {
		// IMPORTANT: Ascend（华为昇腾 NPU）+ vLLM 的特殊判断：
		// 	- 原版 vLLM（NVIDIA）和昇腾版 vLLM-Ascend 的底层连接器握手协议不一样！vLLM-Ascend 的连接器（MooncakeConnectorV1）要求每次请求时把 Prefill 的主机、端口、块 ID、引擎 ID 塞进请求里中继给 Decode。
		return false
	}
	if !ModelDeploymentDeclaresBothHalves(md) {
		// IMPORTANT: 必须同时声明了 Prefill 和 Decode 两半才开直传
		return false
	}

	kind := ModelDeploymentEffectiveRoleKind(role)
	// IMPORTANT: 只有 Prefill 和 Decode 需要传 KV
	return kind == workercore.ModelDeploymentRoleKindPrefill ||
		kind == workercore.ModelDeploymentRoleKindDecode
}

// modelDeploymentFrontsDecodeWithSidecar 报告 decode 副本是否配备路由代理。
//
// 它仅绑定到单一路由器，因为该代理实现的是该路由器自身的协议，而非分离式推理的
// 通用属性：它从 picker 写入的请求头中读取分配给当前请求的 prefiller，而另外
// 两个路由器均不写入该请求头。Ascend 并不排除在外：在该路由器下，代理同时也是
// 驱动传输链路的关键——没有其他组件会将 prefiller 的握手信息中继到 decoder 的
// 请求中——因此缺少代理的 Ascend 链路即使被渲染出来也永远不会被触发。
func modelDeploymentFrontsDecodeWithSidecar(
	md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole,
) bool {
	return md.Spec.Router != nil &&
		md.Spec.Router.Name == workercore.ModelDeploymentRouterLLMD &&
		ModelDeploymentEffectiveRoleKind(role) == workercore.ModelDeploymentRoleKindDecode
}

// 大小与拓扑常量均来自 `inject` 包，此处不再重复声明：`GlobalSegmentSize`、
// `LocalBufferSize` 与 `ModeStandaloneStore`。三者的取值已与本文件过去声明
// 的值逐一核对，完全一致，因此移除重复声明不会改变任何渲染结果。
//
// 每个常量在渲染它的一侧都附带了取值依据：segment size 声明的实际上是角色而非
// 容量（缺少该键会让每个副本按 4 GiB 默认值成为进程内存储成员），buffer 是存储
// 文档规定的中转缓冲区大小且必须为正数，而拓扑是 vLLM 双向校验的跨字段规则的
// 一半——因此它与 segment size 总是成对写入。

// modelDeploymentOwnedKeys 是由 operator 拥有的 (engine, key) 目录。
//
// 该数据同时由渲染器和校验 webhook 读取，从而保证拒绝规则与渲染逻辑在“归属权”
// 上绝不会出现分歧。新增引擎只需增加一个条目，无需改动其他代码。
//
// 编码器相关键取的是并集，这是本表结构决定的。上方的键均逐一列出，因为 operator
// 在该引擎的每次部署中都会写入它们。encoder-cache 链路则按 (role, connector)
// 写入不同的键集合，而本表没有这两列——它仅以引擎为键，因为拒绝检查可能在尚不
// 知道所判断的是哪个角色时就被调用。因此下方条目列出了该链路在任意请求下可能
// 写入的所有键，这与本仓库对 --kv-transfer-config 所做的权衡一致：没有编码器
// 阶段的部署同样不能自行设置该标志。
//
// 这一代价是明确且公开的：用户在未声明 Encode 角色的部署上设置这些键之一时，
// 即使渲染阶段原本不会写入该键也会被拒绝——但只要声明了 Encode 角色，该链路就会
// 被渲染，如果未将该键纳入所有权，用户条目就会在命令行上因后者生效而被静默覆盖。
var modelDeploymentOwnedKeys = map[string]struct {
	Args []string
	Env  []string
}{
	// 这一条覆盖整个 vLLM 系列及其所有后端。这正是此前将连接器挂在错误维度上的
	// 佐证：当该映射表过去单独设有 vllm-ascend 键时，其值与此处完全相同，因为
	// 受管键跟随引擎类型，只有连接器名称才跟随后端类型。
	workercore.ModelDeploymentEngineVLLM: {
		Args: append([]string{"--kv-transfer-config", "--kv-events-config"},
			modelDeploymentEncoderCacheOwnedArgs()...),
		Env: append([]string{"MOONCAKE_CONFIG_PATH", "VLLM_MOONCAKE_BOOTSTRAP_PORT"},
			modelDeploymentEncoderCacheOwnedEnv()...),
	},
	// SGLang 通过环境变量进行配置，因此其受管集合既涵盖引擎实际读取的变量，也涵盖
	// 会将其引导至其他加载器的两个键。此处所有权的目的是防止用户条目造成破坏，而非
	// 防止重复：该引擎按 extra-config、文件、环境变量的顺序选择配置源，且前两者
	// 各自的加载函数中按字段回退的默认值均为编译期字面量。因此用户一旦设置了其中
	// 任一配置源，就不只是覆盖某个值，而是会用默认值静默替换整份配置——即一个 4 GiB
	// 的段和 "localhost" 标识。
	//
	// MOONCAKE_TENANT_ID 由 operator 拥有且绝不可设为默认回退，这是此处唯一关乎
	// 安全性而非仅仅正确性的条目。tenant 即复用域，因此能够设置该变量的工作负载
	// 就能指定 API 禁止其使用的域。API 拒绝自声明域是该保证的持久化防线；此处则是
	// 另一半防线，因为该环境变量是通向同一取值的第二条路径，若不纳入所有权就会留下
	// 缺口。将其归为默认回退类是完全错误的，因为该类别允许用户的值生效。
	//
	// 此处所阻断的逃逸路径受限于此处未测量的因素。完整的攻击链路——伪造多个域、
	// 每个域获取独立配额账户、从而突破 Namespace 上限——要求每个域都拥有独立的配额
	// 账户，而当前存储的配额表现更像淘汰触发器而非硬性账本。该前提在 kv-cache 侧
	// 作为假设而非实测结果进行跟踪。域间数据隔离已通过测试（case-47），但那是另一项
	// 声明。无论如何，拥有该键的所有权都是正确的，因此无需等待该结论。
	//
	// 该键出现在此表中，是因为渲染器对该引擎总是会输出它；所选镜像是否读取该变量
	// 不在此处决定。
	//
	// 三个分离式推理相关键纳入所有权的原因与上述两个不同：它们不是加载器开关，而是
	// 配对双方的契约。prefiller 的 bootstrap 端口写入三个必须一致的位置——其自身
	// 参数、路由器据此发现它的注解，以及 decode Pod 代理读取的环境变量——而模式
	// 决定了 Pod 属于配对的哪一半。用户条目在同一命令行上排在 operator 条目之后，
	// 因此若不拥有该键，声明为其中一半的角色就可能以另一半的身份启动，而配对两端
	// 却仍在对外通告前者。
	workercore.ModelDeploymentEngineSGLang: {
		Args: append([]string{
			"--hicache-storage-backend", "--hicache-storage-backend-extra-config",
			"--disaggregation-mode", "--disaggregation-transfer-backend",
			"--disaggregation-bootstrap-port",
		}, inject.SGLangEncoderOwnedArgs()...),
		Env: []string{
			"SGLANG_HICACHE_MOONCAKE_CONFIG_PATH",
			"MOONCAKE_MASTER",
			"MOONCAKE_TE_META_DATA_SERVER",
			"MOONCAKE_PROTOCOL",
			"MOONCAKE_DEVICE",
			"MOONCAKE_GLOBAL_SEGMENT_SIZE",
			"MOONCAKE_LOCAL_HOSTNAME",
			"MOONCAKE_TENANT_ID",
		},
	},
}

// modelDeploymentEncoderCacheOwnedArgs 及其 Env 对应函数返回 vLLM encoder-cache
// 链路在所有可能被请求的 (role, connector) 组合下所写入键的并集。
//
// 它们通过向 `inject` 查询而非在此硬编码键名，因为该包才是实际渲染这些键的
// 地方：在此维护一份字面量列表会形成对所写内容的第二份声明，两者可能彼此
// 不一致——而这正是本表在拒绝检查另一侧存在的意义。
//
// 该链路所能渲染的两个已准入连接器和两个角色构成了完整作用域，因此并集仅对这
// 几项而非任意请求求取：调用方若指定第三个连接器，从 `inject` 本身不会得到
// 任何键，且在渲染前就会在准入阶段被拒绝。
func modelDeploymentEncoderCacheOwnedArgs() []string {
	return modelDeploymentEncoderCacheOwnedKeys(func(args, _ []string) ([]string, []string) { return args, nil })
}

func modelDeploymentEncoderCacheOwnedEnv() []string {
	return modelDeploymentEncoderCacheOwnedKeys(func(_, env []string) ([]string, []string) { return nil, env })
}

func modelDeploymentEncoderCacheOwnedKeys(pick func(args, env []string) ([]string, []string)) []string {
	var owned []string
	for _, connector := range []string{
		ModelDeploymentECConnectorExample, ModelDeploymentECConnectorCPU,
	} {
		for _, role := range []inject.Role{inject.RoleEncode, inject.RolePrefill} {
			args, env := pick(inject.EncoderCacheOwnedKeys(role, inject.EncoderCache{Connector: connector}))
			for _, key := range args {
				if !slices.Contains(owned, key) {
					owned = append(owned, key)
				}
			}
			for _, key := range env {
				if !slices.Contains(owned, key) {
					owned = append(owned, key)
				}
			}
		}
	}

	return owned
}

// ModelDeploymentModelMountPath 是以只读方式挂载声明制品目录的路径，也是告知引擎加载模型的
// 路径。ModelDeploymentModelCachePath 是由引擎自行下载 Hub 制品时的保存路径。当设置了
// spec.model.artifactRef 时，这两个路径均会被保留：角色在这些路径上、内部或其父目录挂载
// 自己的卷会遮蔽权重目录，或被权重挂载所遮蔽。
//
// ModelDeploymentCompileCacheMountPath 是托管角色编译缓存的默认容器挂载路径。与权重路径
// 不同，角色可以通过 additionalVolumes 在该路径、其内部或其外层挂载自己的卷；此时渲染器
// 会让出其默认的主机后端卷，改由用户的卷承载缓存根目录。
const (
	ModelDeploymentModelMountPath = "/var/lib/gpustack/model"
	ModelDeploymentModelCachePath = "/var/lib/gpustack/model-cache"

	// DefaultModelDeploymentCompileCacheHostDir 是所有托管角色编译缓存所在的主机目录，与
	// gpustack/gpustack 自身的默认 cache_dir 保持一致，使两者对缓存编译产物的存放位置达成一致。
	DefaultModelDeploymentCompileCacheHostDir = "/var/lib/gpustack/cache"

	// ModelDeploymentCompileCacheMountPath 是角色自身主机缓存子目录的挂载路径，同时也是渲染后
	// 引擎缓存根目录环境变量的值。
	ModelDeploymentCompileCacheMountPath = "/var/lib/gpustack/cache"

	// ModelDeploymentCompileCacheVolumeName 是托管 Pod 上由主机目录支持的编译缓存卷的名称。
	ModelDeploymentCompileCacheVolumeName = "gpustack-compile-cache"

	// DefaultModelDeploymentStartupTimeoutSeconds 是角色在未显式指定时获得的启动探针超时预算，
	// 使探针的失败阈值维持在本 operator 一贯渲染的 30 分钟。
	DefaultModelDeploymentStartupTimeoutSeconds int32 = 1800
)

// ModelDeploymentServedModelNameArg 是指定引擎所服务模型名称的标志。它是默认注入的而非由
// operator 独占，但其值被固定：托管角色可以显式声明该标志，但值必须与 spec.model.name
// 一致，因为路由器、路由器的 tokenizer 调用以及指标的 model 标签都按该名称匹配；经实测，
// 名称不匹配会在部署仍报告 Ready 的情况下静默失败。
const ModelDeploymentServedModelNameArg = "--served-model-name"

// modelDeploymentArtifactOwnedKeys 是在设置 spec.model.artifactRef 且为角色渲染命令行时，
// operator 在 modelDeploymentOwnedKeys 之外额外独占的键。
//
// 参数部分对应引擎加载的模型及其版本锁定：operator 会将模型渲染为位置路径或仓库名，并附带
// --revision，若后续出现同名标志则会静默覆盖其中之一；而 --download-dir 会把下载位置移出
// 受容量限制的缓存卷。环境变量部分则用于负责下载权重的引擎访问 Hub：包括从制品 Secret 读取
// 的令牌、管理员配置的端点以及缓存位置。
//
// 它是独立的一张表，而不是合并到另一张表中，因为另一张表是无条件生效的：未指定制品的部署
// 仍需自行传入 --revision 来锁定 Hub 下载，必须保留这种能力。
var modelDeploymentArtifactOwnedKeys = map[string]struct {
	Args []string
	Env  []string
}{
	workercore.ModelDeploymentEngineVLLM: {
		Args: []string{"--model", "--revision", "--tokenizer-revision", "--download-dir"},
		Env: []string{
			"HF_TOKEN", "HF_ENDPOINT", "HF_HOME",
			"MODELSCOPE_API_TOKEN", "MODELSCOPE_DOMAIN", "MODELSCOPE_CACHE", "VLLM_USE_MODELSCOPE",
		},
	},
	workercore.ModelDeploymentEngineSGLang: {
		Args: []string{"--model-path", "--revision", "--download-dir"},
		Env: []string{
			"HF_TOKEN", "HF_ENDPOINT", "HF_HOME",
			"MODELSCOPE_API_TOKEN", "MODELSCOPE_DOMAIN", "MODELSCOPE_CACHE", "SGLANG_USE_MODELSCOPE",
		},
	},
}

// ModelDeploymentArtifactOwnedArg 返回引擎解析器将命令行条目识别为哪个由制品独占的参数。
// 该判断仅在设置了 spec.model.artifactRef 时生效。
func ModelDeploymentArtifactOwnedArg(engine, arg string) (string, bool) {
	return ModelDeploymentResolveArg(engine, arg, modelDeploymentArtifactOwnedKeys[engine].Args)
}

// ModelDeploymentArtifactOwnsEnv 返回指定的环境变量名是否由制品独占。该判断仅在设置了
// spec.model.artifactRef 时生效。
func ModelDeploymentArtifactOwnsEnv(engine, name string) bool {
	return slices.Contains(modelDeploymentArtifactOwnedKeys[engine].Env, name)
}

// ModelDeploymentOwnsKVCacheDtype 根据 model-deployment-kv-cache-dtype-owned 设置，返回
// operator 是否将 Binding 的 dtype 渲染到引擎中并独占其对应的标志。渲染逻辑以及所有对该标志
// 的拒绝判断都以这同一结果为准。
//
// 读取失败时返回默认值而非 false。若失败时回退为 false，会导致一次协调中从所有绑定到池的
// Pod 上移除该标志并触发全部重建，然后在下一次协调时又重新加回。
func ModelDeploymentOwnsKVCacheDtype(ctx context.Context) bool {
	owned, err := strconv.ParseBool(settings.ModelDeploymentKVCacheDtypeOwned.ShouldValue(ctx))
	return err != nil || owned
}

// ModelDeploymentKVCacheOwnedArg 返回引擎解析器是否将命令行条目识别为用于渲染 Binding dtype
// 的标志。该判断仅在设置了 spec.kvCache 且 ModelDeploymentOwnsKVCacheDtype 为真时生效。
//
// 两个引擎系列对该标志的拼写相同，因此同一个键适用于所有引擎；但具体条目如何解析仍取决于
// 引擎本身，因为 vLLM 会重写下划线而 SGLang 不会。
func ModelDeploymentKVCacheOwnedArg(engine, arg string) (string, bool) {
	return ModelDeploymentResolveArg(engine, arg, []string{inject.KVCacheDtypeArg})
}

// ModelDeploymentServedModelNames 按引擎自身的解析方式返回命令行中赋予 served-name 标志的所有
// 值，以及该标志是否出现。
//
// vLLM 在该标志后接收一个或多个名称（直到下一个以 "-" 开头的条目），也支持用 "=" 形式传入
// 单个名称；SGLang 仅接收一个名称。函数会返回所有值，以便调用方既能拒绝错误的第一个名称，
// 也能拒绝多余的第二个名称。
func ModelDeploymentServedModelNames(engine string, args []string) ([]string, bool) {
	keys := []string{ModelDeploymentServedModelNameArg}
	var (
		names []string
		found bool
	)
	for i := 0; i < len(args); i++ {
		if _, ok := ModelDeploymentResolveArg(engine, args[i], keys); !ok {
			continue
		}
		found = true
		if _, value, inline := strings.Cut(args[i], "="); inline {
			names = append(names, value)
			continue
		}
		for i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
			i++
			names = append(names, args[i])
			if engine != workercore.ModelDeploymentEngineVLLM {
				break
			}
		}
	}

	return names, found
}

// modelDeploymentDefaultedEnvNames 是 operator 提供默认注入但不独占的环境变量名称集合。
//
// MC_TE_METRIC 用于开启传输引擎自身的指标；若缺少它，整个设计所依赖的命中率将完全无法
// 度量。它由传输引擎而非任何引擎的配置类读取，因此不依赖于配置类接受哪些键。用户可以关闭它。
//
// MC_FORCE_TCP 将 tcp 点对点链路固定到 TCP；渲染器仅在固定操作不会剥夺同一进程中存储
// 所属网络结构时才输出该变量。传输引擎仅检查该变量是否存在，因此用户自带的同名条目无论
// 值是什么都表达相同的意图，会予以保留。
//
// MC_TCP_MAX_QUEUED_TRANSFERS_PER_PEER、MC_TCP_MAX_PENDING_ADMISSIONS_PER_PEER、
// MC_TCP_LANES_PER_PEER 和 MC_TCP_ADMISSION_TIMEOUT_MS 用于在共享存储或点对点链路通过
// TCP 运行时，提高 Mooncake C++ Transfer Engine 的每对端 TCP 队列上限、socket 通道数和
// 准入超时。它们设为默认注入，以便工作负载可以在 role.Env 中覆盖其中任意一项。
//
// VLLM_CACHE_ROOT 和 SGLANG_CACHE_DIR 是各引擎存放已编译计算图和内核的位置，渲染器会
// 将其指向 ModelDeploymentCompileCacheMountPath。将它们设为默认注入而非独占的原因只有一个：
// 将缓存挂载到别处的用户，或所用镜像中的引擎采用不同变量拼写的用户，必须能够自行指定。
// role.Env 中的条目优先于渲染生成的值，且指定本 operator 未渲染的变量名不构成需拒绝的冲突。
//
// 运行时缓存变量设为默认注入的原因相同，此外还多一点：它们各自对应引擎启动期间会持续增长的
// 缓存目录，渲染出的值会将这部分增长转移到已挂载的缓存卷上，而这部分空间不计入 Pod 的
// ephemeral-storage 限制。
var modelDeploymentDefaultedEnvNames = slices.Concat([]string{
	"MC_TE_METRIC",
	inject.MooncakeForceTCPEnv,
	inject.MooncakeTCPMaxQueuedTransfersPerPeerEnv,
	inject.MooncakeTCPMaxPendingAdmissionsPerPeerEnv,
	inject.MooncakeTCPLanesPerPeerEnv,
	inject.MooncakeTCPAdmissionTimeoutMsEnv,
	"VLLM_CACHE_ROOT",
	"SGLANG_CACHE_DIR",
}, modelDeploymentRuntimeCacheEnvNames)

// ModelDeploymentOwnedArg 返回引擎自身解析器将命令行条目识别为哪个独占参数，从而使拒绝
// 错误能够指出不同拼写形式的条目实际对应到的键。
func ModelDeploymentOwnedArg(engine, arg string) (string, bool) {
	return ModelDeploymentResolveArg(engine, arg, modelDeploymentOwnedKeys[engine].Args)
}

// ModelDeploymentResolveArg 返回引擎自身解析器将命令行条目识别为 keys 中的哪一个；若未
// 识别为其中任何一项，则返回 false。
//
// 解析规则与引擎自身一致，与并行度解析所遵循的规则相同。vLLM 会将长标志中首个点号之前的
// 下划线重写为连字符，并将带点号的条目（如 "--kv-transfer-config.kv_role=x"）合并为点号前
// 标志对应的完整文档，追加在所有其他参数之后以覆盖 operator 的配置。两个引擎均按 argparse
// 的唯一前缀缩写规则解析长标志。SGLang 不做重写也不做合并。
//
// 任意键的前缀都会解析为该键，而不会统计引擎在该前缀下共注册了多少个标志，因为完整的标志表
// 属于引擎内部且此处未知。这种做法偏向保守拒绝：引擎认为有歧义的前缀在启动时本就会被拒绝，
// 而且 argparse 会优先匹配精确注册的拼写而非缩写，因此唯一可能被误解析的情况，是某个条目
// 恰好等于另一个同时也是某个键前缀的已注册标志。独占标志和监听标志是此处已知的已注册标志，
// 因此它们的精确拼写绝不会被当作更长键的前缀。
func ModelDeploymentResolveArg(engine, arg string, keys []string) (string, bool) {
	name := ModelDeploymentArgName(arg)
	if engine == workercore.ModelDeploymentEngineVLLM {
		name, _, _ = strings.Cut(modelDeploymentVLLMFlagKey(name), ".")
	}
	// 单独的 "--" 表示选项列表结束，而非某个选项的缩写。
	if !strings.HasPrefix(name, "--") || len(name) == len("--") {
		return "", false
	}

	if slices.Contains(keys, name) {
		return name, true
	}
	if slices.Contains(modelDeploymentOwnedKeys[engine].Args, name) ||
		slices.Contains(modelDeploymentListenArgs[engine], name) {
		return "", false
	}
	for _, key := range keys {
		if strings.HasPrefix(key, name) {
			return key, true
		}
	}

	return "", false
}

// modelDeploymentVLLMFlagKey 按 vLLM 自身的规则重写标志键：将前导 "--" 与首个点号之间的
// 所有下划线替换为连字符。非长标志的内容原样返回。
func modelDeploymentVLLMFlagKey(key string) string {
	if !strings.HasPrefix(key, "--") {
		return key
	}

	head, tail, dotted := strings.Cut(key, ".")
	head = strings.ReplaceAll(head, "_", "-")
	if !dotted {
		return head
	}

	return head + "." + tail
}

// ModelDeploymentOwnsEnv 返回指定名称的环境变量在此引擎上是否归 operator 独占。
//
// 配置路径变量是关键条目，之所以将其设为独占，重点在于它一旦被修改会造成破坏而非仅仅产生
// 重复：重新指向该路径会悄然将整套客户端配置（池地址、传输方式、元数据来源）替换为另一个
// 文件的内容，随后所有异常表象都会出现在偏离根因的另一层。
//
// rank 变量在所有引擎上统一独占，而非按引擎区分，因为它们描述的是 Pod 在 Kubernetes 中
// 被渲染出的拓扑形态，且没有任何引擎按名称读取它们。即使对于副本数为 1、根本不会渲染这些
// 变量的角色，它们也同样被独占：另一种方案是随字段值变化才启用的规则，但用户仅在扩大实例
// 规模后才遇到拒绝，体验比在首次写下该键时就拒绝更差。
func ModelDeploymentOwnsEnv(engine, name string) bool {
	return slices.Contains(modelDeploymentRankEnvNames, name) ||
		slices.Contains(modelDeploymentOwnedKeys[engine].Env, name)
}

// modelDeploymentRankEnvNames 是多 Member 实例向其主容器发布的环境变量列表。它声明在
// 归属检查旁而非渲染逻辑旁，因为将其组织成列表的原因正是归属检查——渲染器直接写入这三项。
var modelDeploymentRankEnvNames = []string{
	modelDeploymentLeaderAddressEnv,
	modelDeploymentReplicaSizeEnv,
	modelDeploymentMemberIndexEnv,
}

// ModelDeploymentArgName 将命令行条目归约为用于判定归属的标志名，使 "--kv-transfer-config=x"
// 与 "--kv-transfer-config x" 得到相同结果。非标志的条目归约为其自身，不会匹配任何独占项。
func ModelDeploymentArgName(arg string) string {
	name, _, _ := strings.Cut(arg, "=")

	return name
}

// IMPORTANT: 把 K8s API 的字段翻译成底层 inject.Input，调 inject.Render() 拿到引擎参数，并追加监控开关 MC_TE_METRIC=1 与指纹绑定记录。
//
// SynthesizeModelDeploymentConnector 渲染用于将单个角色的副本接入缓存的引擎参数、环境变量和
// 客户端 JSON。
//
// 它是纯函数：相同输入产生相同输出，不依赖客户端也不依赖时钟。它所需的关于池和域的一切
// 信息均作为值传入。
func SynthesizeModelDeploymentConnector(in ModelDeploymentConnectorInput) (ModelDeploymentConnectorRender, error) {
	// IMPORTANT: 翻译：引擎+厂商 → 内部引擎枚举
	engine, err := ModelDeploymentInjectEngine(in.Engine, in.Manufacturer)
	if err != nil {
		return ModelDeploymentConnectorRender{}, err
	}

	// 角色由 kind 映射到渲染器的词汇表。未设置的 kind 映射为 RoleNone，渲染结果与单角色部署
	// 一贯的产出完全一致，因此该映射在此无需特殊分支处理。
	//
	// 无法映射的 kind 会被直接拒绝，而非回退成 RoleNone。准入控制本应拦截此类 kind，能走到
	// 这里说明两者出现分歧，回退只会导致静默故障：decoder 被配置成普通单角色服务器，看似健康
	// 却在独立处理完整请求。
	// IMPORTANT: 翻译：API RoleKind → inject.Role
	role, ok := modelDeploymentInjectRole(in.Kind)
	if !ok {
		return ModelDeploymentConnectorRender{}, fmt.Errorf("unsupported role kind %q", in.Kind)
	}

	// 资源池提供的多个传输协议中究竟把哪一个交给引擎，是在此处而非连接解析阶段决定的：
	// 该决策依赖具体引擎，连接按 deployment 解析，而引擎随各个 role 变化。受限引擎选取首个
	// 接受的协议；不受限引擎则取首个以兼容在混用检查前已准入的旧绑定。
	// IMPORTANT: 从共享池支持的协议里挑一个当前引擎认的
	protocol, err := inject.MatchTransport(engine, in.Protocols)
	if err != nil {
		return ModelDeploymentConnectorRender{}, err
	}

	kvTransferProtocol := ""
	if in.KVTransferProtocol != "" {
		// IMPORTANT: 翻译：API 的 "TCP" → Mooncake 拼写的 "tcp"
		kvTransferProtocol = mooncake.TransportProtocol(in.KVTransferProtocol)
		if kvTransferProtocol == "" {
			return ModelDeploymentConnectorRender{}, fmt.Errorf("unsupported KV transfer protocol %q", in.KVTransferProtocol)
		}
	}

	res, err := inject.Render(inject.Input{
		Engine: engine,
		Role:   role,
		// 配对标志随 role 一并传入，而非在此处推导：引擎的拆分模式取决于 deployment 完整的角色
		// 集合，单个 role 本身无法得知另一半是否已声明。
		Disaggregated: in.Disaggregated,
		Domain:        in.Domain,
		CachePrefix:   in.CachePrefix,
		Dtype:         in.Dtype,
		// 编码器分支由外部完整传入而不在此组装：由哪个连接器承载该分支由准入规则决定，取自当前
		// 组合命中的匹配行。零值表示不启用编码器分支，输出与该分支存在前完全一致。
		EncoderCache: in.EncoderCache,
		EncoderURLs:  in.EncoderURLs,
		Connection: inject.Connection{
			MasterAddress: in.MasterServerAddress,
			Protocol:      protocol,
		},
		PublishKVEvents: in.PublishKVEvents,
		KVEventsHost:    in.KVEventsHost,
		KVTransfer:      in.KVTransfer,
		// 未设置时的默认值归渲染器所有；此处仅负责将显式声明的 API 协议值做映射。
		KVTransferProtocol: kvTransferProtocol,
		// 成对并行参数同样从外部透传而不在此处解析：解析需要读取整个 deployment 的所有角色，
		// 而合成函数只看到单个角色的输入。
		Parallelism: in.Parallelism,
	})
	if err != nil {
		return ModelDeploymentConnectorRender{}, err
	}

	var parallelism inject.ParallelismPair
	if engine == inject.EngineVLLMAscend && res.KVTransfer {
		parallelism = in.Parallelism
	}

	return ModelDeploymentConnectorRender{
		Args: res.Args,
		Env:  res.Env,
		// MC_TE_METRIC 在共享渲染器中没有也不应有对应项：共享渲染器输出引擎连接资源池所需的
		// 参数，而该环境变量用于满足本控制面自身的观测需求。它以默认值而非托管键输出，因此用户
		// 自行声明的值可无报错地覆盖它；渲染器自身的默认环境变量紧随其后遵循同一规则。
		DefaultedEnv:   append([]core.EnvVar{{Name: "MC_TE_METRIC", Value: "1"}}, res.DefaultedEnv...),
		DefaultedArgs:  res.DefaultedArgs,
		Volumes:        res.Volumes,
		VolumeMounts:   res.VolumeMounts,
		PodAnnotations: res.PodAnnotations,
		Ports:          res.Ports,
		KVEvents:       res.KVEvents,
		KVTransfer:     res.KVTransfer,
		// 直接透传而非在此推导：渲染器无法知晓前方挂接了哪款路由器，按直传链路反推 sidecar 正是
		// 本次拆分所要终止的耦合。
		RoutingSidecar: in.RoutingSidecar,
		binding: &modelDeploymentConnectorBinding{
			Domain:              in.Domain,
			Dtype:               in.Dtype,
			CachePrefix:         in.CachePrefix,
			MasterServerAddress: in.MasterServerAddress,
			Protocol:            protocol,
			KVTransfer:          res.KVTransfer,
			KVTransferProtocol:  kvTransferProtocol,
			PublishKVEvents:     in.PublishKVEvents,
			KVEventsHost:        in.KVEventsHost,
			RoutingSidecar:      in.RoutingSidecar,
			Disaggregated:       in.Disaggregated && (role == inject.RolePrefill || role == inject.RoleDecode),
			Parallelism:         parallelism,
			EncoderCache:        in.EncoderCache,
			EncoderURLs:         in.EncoderURLs,
		},
	}, nil
}

// ModelDeploymentInjectEngine 将本 API 的引擎值与角色制造商映射到共享渲染器的引擎枚举。
//
// 这两个枚举故意采用不同的形态。本 API 只有一个 `vllm` 值，因为在 CANN 上 runner 会为同一
// 声明的引擎安装 vllm_ascend 包，即由加速器决定实际运行哪个包，无需用户指定。而渲染器有
// 两个枚举值，因为这两个包注册的连接器名称不同。
//
// 此处的映射错误在租户输出中是不可见的：两个 vLLM 条目都会渲染 tenant_id，因此互换它们
// 不会改变任何租户断言能观察到的内容。它真正改变的是连接器名称，这也是对应测试专门校验
// 该字段的原因。
func ModelDeploymentInjectEngine(engine, manufacturer string) (inject.Engine, error) {
	switch engine {
	case workercore.ModelDeploymentEngineVLLM:
		if manufacturer == nodefeature.ManufacturerAscend {
			return inject.EngineVLLMAscend, nil
		}

		return inject.EngineVLLM, nil
	case workercore.ModelDeploymentEngineSGLang:
		return inject.EngineSGLang, nil
	default:
		return "", fmt.Errorf("unsupported engine %q", engine)
	}
}

// ModelDeploymentEngineUpstreamName 返回 runner 标签、路由器指标以及上游引擎配置所使用的
// 拼写。API 拼写并非上游制品名称。
func ModelDeploymentEngineUpstreamName(engine string) (string, bool) {
	switch engine {
	case workercore.ModelDeploymentEngineVLLM:
		return "vllm", true
	case workercore.ModelDeploymentEngineSGLang:
		return "sglang", true
	default:
		return "", false
	}
}

// ModelDeploymentRoleUpstreamKind 返回路由器和 Pod 选择器消费的角色类型拼写。
//
// "encode" 是角色标签过滤器与按角色 Service 选择器用于匹配 encoder 链路的拼写。
// 若此处遗漏该分支，encoder Pod 将缺失该标签，所有按此名称匹配的选择器都将看不到它。
func ModelDeploymentRoleUpstreamKind(kind workercore.ModelDeploymentRoleKind) (string, bool) {
	switch kind {
	case workercore.ModelDeploymentRoleKindServer:
		return "server", true
	case workercore.ModelDeploymentRoleKindPrefill:
		return "prefill", true
	case workercore.ModelDeploymentRoleKindDecode:
		return "decode", true
	case workercore.ModelDeploymentRoleKindEncode:
		return "encode", true
	default:
		return "", false
	}
}

// modelDeploymentRoleKindFromUpstream 读取 Pod 的 role-kind 标签所指定的类型。该标签承载的是
// 上游拼写，因此本函数负责将其转换回 API 类型。
//
// 无法识别的标签不代表任何类型，而不是 server 类型：若一个 Pod 的标签无法被本读取器归类，
// 说明它未表明自身身份；若读取器返回 "server"，就会让此类 Pod 误入仅限 server 的部署形态。
func modelDeploymentRoleKindFromUpstream(upstream string) workercore.ModelDeploymentRoleKind {
	switch upstream {
	case "server":
		return workercore.ModelDeploymentRoleKindServer
	case "prefill":
		return workercore.ModelDeploymentRoleKindPrefill
	case "decode":
		return workercore.ModelDeploymentRoleKindDecode
	case "encode":
		return workercore.ModelDeploymentRoleKindEncode
	default:
		return ""
	}
}

// ModelDeploymentEffectiveRoleKind 解析未设置的字段，返回角色实际生效的类型。
//
// CRD schema 会为其设置默认值，因此从 API server 传入的对象不会带有空类型；但所有在 Go 中
// 进程内构造该值的调用方都会出现空值，且这类调用方有好几个：webhook 规则、连接器映射以及
// 状态回写。若将零值视作一种独立类型，会导致它们对同一对象产生分歧，而状态回写正是问题
// 爆发之处：status.roles[].kind 为必填枚举字段，在此处完成解析才能保证写入状态的枚举值
// 合法。直接写入空类型会被拒绝，而状态写入失败会连带导致对象上的所有其他状态字段一并失败。
func ModelDeploymentEffectiveRoleKind(
	role *workercore.ModelDeploymentRole,
) workercore.ModelDeploymentRoleKind {
	if role.Kind == "" {
		return workercore.ModelDeploymentRoleKindServer
	}

	return role.Kind
}

// modelDeploymentPublishesKVEvents 返回该角色是否运行事件发布器。
//
// 它仅配合单一路由器与单一引擎启用。该发布器用于向该路由器的前缀缓存数据层提供数据，后者
// 按 Pod 订阅事件；另外两种路由器仅基于自身观测打分且不订阅任何内容，因此在它们下发布事件
// 会在每个副本上打开两个无人读取的 socket。
//
// ENCODE 不是 KV 生产者：编码器产出的是多模态嵌入而非 KV 缓存块，它打开的发布器在数据面
// 没有订阅方。拒绝为其启用发布器，可以防止 Encode 与 Prefill 配对的部署在只有一方存在
// 消费者时却在编码器上渲染出两个 socket。
func modelDeploymentPublishesKVEvents(
	md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole, manufacturer string,
) bool {
	kind := ModelDeploymentEffectiveRoleKind(role)
	if kind == workercore.ModelDeploymentRoleKindDecode || kind == workercore.ModelDeploymentRoleKindEncode {
		return false
	}

	return modelDeploymentRoutesManaged(md, manufacturer) &&
		md.Spec.Router.Name == workercore.ModelDeploymentRouterLLMD &&
		md.Spec.Engine.Name == workercore.ModelDeploymentEngineVLLM &&
		len(role.Command) == 0
}

// modelDeploymentInjectRole 将本 API 的角色类型映射到渲染器的角色枚举。
//
// 这两个枚举之所以分开，原因与引擎枚举相同。本 API 的 "server" 指代的是一种部署形态——
// 单个进程同时承担前后两半工作——而渲染器的 RoleNone 表示未做 prefill/decode 拆分。两者在
// 当前恰好重合，但表达的并非同一概念。
//
// 未设置的类型与 "server" 映射到同一目标：CRD schema 会为该字段设置默认值，因此从 API server
// 传入的对象不会为空，但直接在 Go 中构造该值的调用方可能会留空。若将零值视作未知类型，
// 就会拒绝所有此类调用方。
//
// ENCODE 拥有独立的值（inject.RoleEncode），绝不会被折叠进 RoleNone。返回这一专用值使得
// 连接器能够拒绝渲染器不支持的角色，引擎支持表也能在准入阶段再次拒绝它——两者基于同一
// 结论生效。
func modelDeploymentInjectRole(kind workercore.ModelDeploymentRoleKind) (inject.Role, bool) {
	switch kind {
	case "", workercore.ModelDeploymentRoleKindServer:
		return inject.RoleNone, true
	case workercore.ModelDeploymentRoleKindPrefill:
		return inject.RolePrefill, true
	case workercore.ModelDeploymentRoleKindDecode:
		return inject.RoleDecode, true
	case workercore.ModelDeploymentRoleKindEncode:
		return inject.RoleEncode, true
	default:
		return "", false
	}
}

// ModelDeploymentSupportsRoleKind 返回引擎的渲染逻辑是否支持该角色类型。
//
// 它存在的目的是让准入阶段能够拒绝引擎无法识别的类型（此时用户尚可修正），而不是渲染出在
// 容器启动时被引擎拒绝的配置。其结论直接取自渲染器自身的表而非副本：若另建一张表，今天
// 尚能一致，但在后续某个引擎版本上线后就会产生分歧，且期间不会有任何失败提示。
//
// 准入阶段尚无法获知资源池的加速器厂商——该信息要到很久之后才从节点观测到——而厂商正是将
// vLLM 值拆分为两个渲染器引擎的依据。因此，只有当该 API 值可能映射到的所有引擎都支持渲染
// 该类型时，才判定为支持。这是在观测到厂商后唯一不会出错的答案。
func ModelDeploymentSupportsRoleKind(engine string, kind workercore.ModelDeploymentRoleKind) bool {
	role, ok := modelDeploymentInjectRole(kind)
	if !ok {
		return false
	}

	for _, manufacturer := range []string{"", nodefeature.ManufacturerAscend} {
		injectEngine, err := ModelDeploymentInjectEngine(engine, manufacturer)
		if err != nil {
			return false
		}
		if !inject.SupportsRole(injectEngine, role) {
			return false
		}
	}

	return true
}

// 过去位于此处的按引擎区分的渲染器（一个用于 vLLM 系列配置文件，一个用于 SGLang 环境变量）
// 现已移除，改由 `pkg/worker/kvcache/inject` 统一渲染。它们原先承载的设计考量并未随之丢失，
// 而是保留在了该包对应的渲染器中——包括为什么 SGLang 的载体是环境变量（其配置路径在准入时
// 即已确定，彼时 Pod 尚无 IP，因此只有由 kubelet 求值的 fieldRef 才能携带 local_hostname），
// 以及为什么保持 SGLANG_HICACHE_MOONCAKE_CONFIG_PATH 未设置正是选用该路径的关键。
//
// 保留在此处的是属于本 API 而非渲染器的那一半职责：下方的独占键表依然会拒绝用户尝试设置
// 该配置路径或 extra-config 参数的行为，因为这两者都会把引擎引导至将 local_hostname 解析为
// 字面量的加载器。

// ModelDeploymentEngineCommand 渲染在指定模型上启动单个引擎的 argv。
//
// operator 拥有整条 argv，这是由模板类型本身而非风格偏好决定的：模板只携带 Command 且刻意不设
// Args，因此在镜像自带 entrypoint 之外无处追加参数。要么由 operator 构造整条命令行（基础命令、
// 合成的 connector 参数、角色的 ExtraArgs），要么由用户通过接管层级完整替换它；不存在 operator
// 向并非由自己构造的命令行追加参数的中间形态。
//
// 基础命令采用各引擎文档规定的入口：vLLM 安装了 "vllm" 命令行脚本，其 serve 子命令将模型作为
// 位置参数接收；SGLang 作为模块启动，通过 --model-path 指定模型。
//
// 该命令不随加速后端变化，因此不接收 manufacturer 参数：vllm_ascend 是 vLLM 插件并共用同一入口。
// 这是两个 vLLM 家族变体真正重合的地方，不同于独占键表（后者重合只是因为那些键本来就不属于后端属性）。
func ModelDeploymentEngineCommand(engine, model string) ([]string, error) {
	if model == "" {
		return nil, errors.New("model name is empty")
	}

	switch engine {
	case workercore.ModelDeploymentEngineVLLM:
		return []string{"vllm", "serve", model}, nil
	case workercore.ModelDeploymentEngineSGLang:
		return []string{
			"python3", "-m", "sglang.launch_server", "--model-path", model, "--enable-metrics",
		}, nil
	default:
		return nil, fmt.Errorf("unsupported engine %q", engine)
	}
}

// --kv-transfer-config 文档同样交由 `inject` 生成。它通过结构体而非 map 渲染相同的两个键，
// 从而将无法识别的键转化为编译期错误；并且它根据角色推导 kv_role，而不是硬编码 kv_both——
// 本版本唯一允许的 RoleNone 恰好推导出 kv_both。vLLM 会拒绝缺少 kv_role 的 kv_connector，
// 而 kv_both 是唯一同时合法的生产者与消费者角色值，这正是共享同一存储的副本所需要的。

// 本文件原先保存的协议映射现已归属 `mooncake.MemberProtocol`，属于负责该后端的包。两者在
// 每个枚举值上完全一致，因此删除旧映射不会改变任何渲染出的文档——仅有一处差异值得说明：旧版
// 会将未识别的值转为小写后透传，认为客户端会给出警告并继续执行；而保留下来的实现对于枚举之外
// 的值会返回空字符串，且 `inject.Render` 会拒绝空协议。带着 schema 不允许的协议流转到此处的
// 对象从未经过准入校验，明确报错拒绝比转为小写的猜测更为妥当。
