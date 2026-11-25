package worker

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	kueuectrlconst "sigs.k8s.io/kueue/pkg/controller/constants"

	worker "gpustack.ai/gpustack/api/worker/v1"
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/systemmeta"
	"gpustack.ai/gpustack/pkg/systemname"
	"gpustack.ai/gpustack/pkg/utils/json"
	"gpustack.ai/gpustack/pkg/utils/quantityx"
	"gpustack.ai/gpustack/pkg/utils/slicex"
	"gpustack.ai/gpustack/pkg/utils/stringx"
	"gpustack.ai/gpustack/pkg/worker/kvcache/inject"
	"gpustack.ai/gpustack/pkg/worker/settings"
)

const (
	// ModelDeploymentResourceType 是 ModelDeployment 渲染的每个对象都会携带的资源标记（resource note），
	// 以便 watch 逻辑将其与 Namespace 中的其他 Pod 区分开来。
	ModelDeploymentResourceType = "modeldeployments"
	// ModelDeploymentResourceNoteRole 标识被渲染对象所属的角色。
	ModelDeploymentResourceNoteRole = "role"

	// modelDeploymentLabelKeyName、modelDeploymentLabelKeyInstance 和
	// modelDeploymentLabelKeyComponent 是副本 Pod 携带的身份标签。
	//
	// 它们与资源标记（resource note）并存，因为 Service 只能按 label 选择而无法读取 annotation，
	// 且这些标签只承载不可变身份（属于哪个部署、哪个角色），绝不包含任何 spec 更新可能改变的内容。
	// 一旦 selector 发生变化，就会导致原先关联的 Pod 成为孤立对象。这三个完全相同的键及其含义同样用于挂载在 KVCache 后端 leader 前方的服务。
	modelDeploymentLabelKeyName      = "app.kubernetes.io/name"
	modelDeploymentLabelKeyInstance  = "app.kubernetes.io/instance"
	modelDeploymentLabelKeyComponent = "app.kubernetes.io/component"
	modelDeploymentLabelValueName    = "model-deployment"

	// modelDeploymentLabelKeyRoleKind 承载角色的有效类型（effective kind），使副本前端组件能够通过 selector 区分 prefiller 和 decoder。
	// 它是解析后的实际值，而非原始字段值：未指定 kind 的角色默认为 server，若按空字符串匹配 selector，则会漏掉所有默认形态的副本。
	//
	// 每个部署都会渲染该标签，包括仅含单个 server 角色的部署。
	// 若仅在分离式部署（disaggregation）下才出现该键，会导致“不存在 prefiller”和“该部署未标注角色类型”产生相同的查询结果。
	//
	// 该标签不包含在 selector 标签中。Service 的 selector 一旦更改就会使原先关联的 Pod 成为孤立对象；
	// 尽管目前角色类型不可变，不将其放入 selector 的原因与不放入入口标签相同：selector 只承载身份标识，而 kind 是角色的属性而非角色的名称。
	//
	// 添加该标签会触发所有现有部署滚动重建一次。由于 Pod spec 哈希覆盖了标签（参见 modelDeploymentPodSpecHash），
	// 在此键引入之前渲染的所有副本都会出现指纹过期，并由重建滚动策略（recreate rollout）进行替换。
	// 这种重建正是 recreate 策略所定义的行为，退出的副本会将其缓存块释放给同组其他副本。
	modelDeploymentLabelKeyRoleKind = "modeldeployment." + systemname.LabelPrefix + "role-kind"

	// modelDeploymentRouterCandidateLabel 将某个成员标记为其所属组的路由候选者（routing candidate）。
	//
	// 它表示的是一项路由事实，而非就绪资格认证：它说明该成员可被发现，但绝不代表它已通过探针检查。
	//
	// 属于本 operator 负责判定角色的成员，在其所属组具备资格（eligible）期间会携带此标签，并在与资格标签相同的写入操作中一同添加或移除。
	// 由本 operator 独立判定的角色包括：受保护的托管组（protected managed group）、encoder 以及普通的单成员 server。
	// 托管的 prefill 或 decode 对（pair）在本 operator 能够观测该 pair 时也属于此类，而探针路径不可用的 pair 则完全不由本 operator 判定。
	// 受保护类别与可判定类别是两个不同的维度：无论哪种情况，准入要求从首次启用时起即生效，且原生 selector 仍要求具备资格。
	//
	// 属于本 operator 无法判定角色的成员，只要所部署的形态允许其响应请求，就会一直携带该标签，
	// 这与它一贯的成员身份规则一致：对于本 operator 无法判定的路由，由其自身规则决定发现时机，该标记对此表达同样的语义。
	//
	// 该标签专为混合并集（mixed union）场景设计，在此场景下需要一个部署级 selector 来筛选要求各不相同的各角色候选者。
	// 此时不能使用资格标签，否则会遗漏不带该要求的角色；而按 kind 过滤也无法区分各自的要求，因为过滤器按 kind 生效，同一 kind 的两个角色会共享同一过滤条件。
	modelDeploymentRouterCandidateLabel = "modeldeployment." + systemname.LabelPrefix + "router-candidate"

	// modelDeploymentPodSpecHashAnnotation 记录用于构建副本成员的显式 spec 输入与外部资源绑定的指纹。
	modelDeploymentPodSpecHashAnnotation = "modeldeployment." + systemname.LabelPrefix + "pod-spec-hash"
	// modelDeploymentRenderVersionAnnotation 记录合成副本 Pod 时所使用的 operator 渲染版本。
	modelDeploymentRenderVersionAnnotation = "modeldeployment." + systemname.LabelPrefix + "render-version"
	// modelDeploymentMinRenderVersionAnnotation 是部署级别的 annotation，用于提高运行中副本可接受的最低渲染版本。
	modelDeploymentMinRenderVersionAnnotation = "modeldeployment." + systemname.LabelPrefix + "min-render-version"
	// modelDeploymentTemplateBindingHashAnnotation 将模板级外部绑定摘要从 renderModelDeploymentPodTemplate 传递到 stampModelDeploymentPod，并在落盘打标（stamp）时移除。
	modelDeploymentTemplateBindingHashAnnotation = "modeldeployment." + systemname.LabelPrefix + "template-binding-hash"

	// modelDeploymentRenderVersion 是当前 operator Pod 渲染基线版本。每当有变更修改了任意 ModelDeployment 形态的 Pod 渲染输出时，都应递增此常量。
	modelDeploymentRenderVersion = 4
	// modelDeploymentMinSupportedRenderVersion 是当前控制器仍可兼容而无需替换运行中 Pod 的最低渲染版本。
	modelDeploymentMinSupportedRenderVersion = 1

	// modelDeploymentDefaultPort 是角色未指定端口时副本提供服务的默认端口。
	// 该端口会被显式渲染进引擎自身的监听参数中，而不是交给引擎自行选择，因为各受支持的引擎对默认值的约定并不一致。
	// vLLM 会在所有网卡接口上监听 8000 端口；而 SGLang 仅在回环地址（loopback）上监听 30000 端口，导致 Service 端点和 kubelet 探针都无法访问。
	modelDeploymentDefaultPort int32 = 8000
	// modelDeploymentDefaultPortName 为容器及前端 Service 上该端口的名称。
	modelDeploymentDefaultPortName = "http"
	// 托管 decoder 的路由代理占用对外服务端口，而 vLLM 在其后方通过此端口监听。
	modelDeploymentInternalPort int32 = 8200
	// modelDeploymentMainContainerName 是运行角色自身镜像和命令的容器名称，也是唯一属于集合通信 rank 的容器：
	// Pod 中携带的所有其他容器均属于本 operator，不参与任何集合通信。
	modelDeploymentMainContainerName = "main"

	// modelDeploymentLeaderAddressEnv、modelDeploymentReplicaSizeEnv 和
	// modelDeploymentMemberIndexEnv 用于发布跨多个 Pod 的实例的 rank 拓扑布局。
	// 它们仅在副本规模大于 1 时出现，因此单 Pod 实例的渲染结果与该字段引入前完全一致。
	//
	// 与连接器的环境变量键不同，这些环境变量对所有引擎均由系统独占管理（owned），因为它们描述的是 Pod 在 Kubernetes 中的渲染形态，而非特定引擎专有的配置。
	// 这种独占权并非风格偏好：若允许用户定义同名条目，系统会按值（VALUE）合并到原本携带 fieldRef 的条目上，而同时包含两者的 EnvVar 会被 API server 拒绝——因此只要有一个键未受独占保护，合法的部署就会变成无法渲染的部署。
	modelDeploymentLeaderAddressEnv = "GPUSTACK_REPLICA_LEADER_ADDRESS"
	modelDeploymentReplicaSizeEnv   = "GPUSTACK_REPLICA_SIZE"
	modelDeploymentMemberIndexEnv   = "GPUSTACK_MEMBER_INDEX"

	// modelDeploymentEngineHostArg 和 modelDeploymentEnginePortArg 是决定引擎监听位置和端口的两个参数。
	// 它们采用“缺省填充”而非“独占覆盖”策略：若角色自身传入了其中任一参数，则保留其自定义值，这与非 Kubernetes 场景下 worker 对这两个参数的处理规则一致。
	modelDeploymentEngineHostArg = "--host"
	modelDeploymentEnginePortArg = "--port"
	// modelDeploymentEngineBindHost 表示 Pod 网络命名空间中的所有网卡接口，Service 端点和 kubelet 探针都会访问该地址。
	// 若让引擎使用其自身默认值，它可能只监听回环地址，导致只有容器内部的进程才能访问。
	modelDeploymentEngineBindHost = "0.0.0.0"
	// modelDeploymentEngineTLSCertArg 和 modelDeploymentEngineTLSKeyArg 是唯二会将引擎监听器切换为 TLS 模式的参数，
	// 此处刻意没有将整个 --ssl-* 参数族都纳入判定。
	//
	// 两个受支持的引擎都会将所有 ssl 参数直接透传给 uvicorn，而 uvicorn 仅在存在 `ssl_keyfile or ssl_certfile` 时才启用 TLS。
	// 若单独传入 --ssl-ca-certs、--ssl-ciphers、--ssl-keyfile-password 或 --ssl-cert-reqs，服务端仍只是一个普通 HTTP 服务器，
	// 因此若按整个参数前缀统一判定，就会为实际提供明文服务的监听器发布 https:// 地址。
	//
	// 如需复核此行为（由于这是引擎自身行为，此处代码无法强制约束）：请查看 uvicorn 的 Config.is_ssl，
	// 其调用路径分别位于 vllm 的 api_server.py（serve_http 调用）和 sglang 的 http_server.py（launch_server 调用）。
	// 此外，两个引擎各自还会打印一条关于是否启用 SSL 的日志，但其计算方式不同——vllm 要求同时存在 keyfile 和 certfile——而且日志均不决定监听器的实际行为。
	// 请以 uvicorn 的属性为准，而非日志输出。
	//
	// 此判定仅覆盖命令行参数范围。如果某引擎后续新增了能将同一监听器切换为 TLS 的环境变量，此处将无法感知，
	// 进而导致探针和对外发布的地址都将 TLS 监听器标记为 http://。目前两个引擎均不存在此类环境变量。
	modelDeploymentEngineTLSCertArg = "--ssl-certfile"
	modelDeploymentEngineTLSKeyArg  = "--ssl-keyfile"
	// modelDeploymentEngineTLSClientAuthArg 是门控无法始终兼容的唯一一个 TLS 相关参数：
	// 它可以要求提供客户端证书，而 kubelet 探针并没有证书可以出示。
	// 仅当上述两个参数之一已开启 TLS（否则 uvicorn 根本不会构建 TLS 上下文使该参数生效），且其取值不在下方允许的值范围内时，才会撤回门控。
	modelDeploymentEngineTLSClientAuthArg = "--ssl-cert-reqs"
	// modelDeploymentEngineTLSClientAuthNone 和 modelDeploymentEngineTLSClientAuthOptional 对应
	// ssl.CERT_NONE 和 ssl.CERT_OPTIONAL，即该参数下 kubelet 探针仍能正常完成的唯二取值：
	// 前者不要求证书，后者仅在客户端主动提供时才校验证书。
	//
	// 这两个常量构成的是白名单，而非“遇到 CERT_REQUIRED 则拒绝”的检查，两者的差异会在出现枚举外的值时显现。
	// Python ssl 模块恰好只定义了这两个值加上 CERT_REQUIRED，因此诸如 3 或 -1 属于本 operator 无法推理的配置；
	// 若只检查拒绝值，则会误将这些未知值视为安全并放行行为未知的监听器。
	// 凡是不在此白名单中的取值一律撤回门控，这与下文对不可读值的处理方式及其原因一致。
	modelDeploymentEngineTLSClientAuthNone     = 0
	modelDeploymentEngineTLSClientAuthOptional = 1

	// modelDeploymentProbePath 是所有三个门控共同读取的路由，也是两个受支持引擎都能给出有效响应的唯一路由——
	// 但它们这样做的原因截然相反，因此绝不能用其中一个引擎的行为来推论另一个引擎。
	//
	// 在 vLLM 上，该处理器实际上是一个存活检查（liveness check）：只要引擎未崩溃，它在任何状态下都会返回 200，仅在引擎死亡后才返回 503，
	// 甚至当服务器根本未加载任何引擎时也会直接返回 200。
	// 使它在这里成为就绪信号（readiness signal）的是处理器下方的一层机制：服务器 socket 在引擎构建之前就已绑定（bind），
	// 但直到引擎加载完成、uvicorn 开始服务时才会执行监听（listen）。在此之前，探针收不到任何可供判断的响应——连接会被直接拒绝。
	//
	// 在 SGLang 上，该处理器本身就是就绪检查（readiness check）：服务器从启动之初就开始监听，并在状态仍处于启动中时返回 503。
	//
	// 因此，TCP 探针在 vLLM 上是正确的，但在 SGLang 上却毫无意义（因为其端口会立即接受连接）。/v1/models 亦然。
	// 除此之外两者别无共通之处：vLLM 的 /ready 和 /readyz 仅存在于其数据并行（data-parallel）监督进程中，而不在角色运行的服务器进程中。
	modelDeploymentProbePath = "/health"

	// modelDeploymentDirectDecodeProbePath 是 direct-decode 形态下各探针读取的路由，
	// 在该形态中由路由 sidecar 占用各探针评估的 Service 端口。sidecar 在任何状态下都会自行响应
	// /health，因此该路径评估的是 sidecar 本身，一旦 Pod 启动就无法再反映引擎状态。
	// sidecar 会把其他所有路由转发给引擎，而在引擎完成加载并开始提供服务之前，其端口会拒绝连接，
	// sidecar 对此返回 503。因此通过 sidecar 读取 /v1/models 既能使探针继续访问
	// Service 前端的地址，又能让响应结果重新取决于引擎状态。
	//
	// 上述空洞问题不会随此路由带入该分支：direct-decode 形态仅针对 vLLM 渲染，
	// 而该引擎的 /v1/models 正是就绪信号。
	modelDeploymentDirectDecodeProbePath = "/v1/models"

	// modelDeploymentProbePeriodSeconds 为三个探针设定统一的执行周期，从而使它们的失败阈值相互可比。
	modelDeploymentProbePeriodSeconds int32 = 10
	// modelDeploymentProbeTimeoutSeconds 限制单次探测请求的超时时间。它高于 kubelet 默认的 1 秒，
	// 因为该路由由正在加载模型的同一进程提供服务，并且足够宽松，使在读取大权重分片期间到达的探测
	// 报告超时，而不会打断读取操作。
	modelDeploymentProbeTimeoutSeconds int32 = 5
	// modelDeploymentStartupFailureThreshold 表示未声明启动预算的副本在 kubelet 放弃前允许用于
	// 加载的周期数；按上述周期和 DefaultModelDeploymentStartupTimeoutSeconds 计算，该时间为 30 分钟。
	// 声明了预算的角色会获得由该预算推导出的独立阈值，而不使用此常量。
	//
	// 该预算刻意设置得较为宽松：加载时间会随模型规模和权重获取方式增长，按最小模型设定的阈值
	// 会把慢启动变成重启循环。超出所需宽限的代价仅在于本已异常的副本在异常状态多停留一段时间，
	// 而 Deployment 自身状态已能反映这一点。
	modelDeploymentStartupFailureThreshold int32 = 180
	// modelDeploymentReadinessFailureThreshold 表示曾正常提供服务的副本在连续失败多少次后
	// 会被移出 Service 的 endpoints。
	modelDeploymentReadinessFailureThreshold int32 = 3
	// modelDeploymentLivenessFailureThreshold 表示引擎在曾经响应后停止响应时，连续失败多少次会触发副本重启。
	//
	// 该阈值刻意大于就绪阈值，因为两种失败的代价不同。失去就绪状态只会将副本从 Service 中摘除，
	// 并在重新响应后恢复，因此短暂卡顿应先触发就绪失败。而重启会丢弃耗时上述启动预算才加载好的
	// 模型，因此只有持续时间超过就绪失败的故障才应触发重启。
	modelDeploymentLivenessFailureThreshold int32 = 6

	// 以下三个值在按地址连接编码器阶段的 prefiller 上替代上面对应的默认值。在编码器处理大媒体
	// 载荷或正在重启期间，其健康检查路由可能响应延迟甚至无响应。若使用默认值，编码器卡顿既会
	// 摘除健康的 prefiller，又会通过存活探针将其重启。更宽松的阈值在就绪与存活之间保持相同比例：
	// 按上述周期计算分别为 60 秒和 120 秒。
	modelDeploymentEncoderProbeTimeoutSeconds       int32 = 5
	modelDeploymentEncoderReadinessFailureThreshold int32 = 6
	modelDeploymentEncoderLivenessFailureThreshold  int32 = 12

	// modelDeploymentVLLMEngramConfigArg 和 modelDeploymentSGLangEngramHostTableEnv 是
	// 将 Engram 查找表放置在固定主机内存（pinned host DRAM）中的引擎设置。
	modelDeploymentVLLMEngramConfigArg      = "--engram-config"
	modelDeploymentSGLangEngramHostTableEnv = "SGLANG_ENABLE_DSV41_ENGRAM_HOST_TABLE"
)

// modelDeploymentListenArgs 是各引擎注册的监听和 TLS 参数：渲染器会通过 ModelDeploymentResolveArg
// 从命令行中回读这组参数，以获知引擎的监听位置和方式。该映射按引擎区分，因为各引擎注册集不同——
// SGLang 未注册 --ssl-cert-reqs，因此 "--ssl-cert" 即是其证书参数，而 vLLM 会认为该前缀存在歧义。
//
// 键的前缀会被识别为该键，只要引擎能够启动，这也是引擎自身的解析行为：两个引擎所注册的参数中
// 没有任何一个是此处任一键的严格前缀，因此引擎接受的前缀必然会解析为该键，而引擎判定为歧义的
// 前缀会在启动任何监听之前中止进程，从而没有可回读的监听信息。该前提属于引擎自身行为，此处并不
// 强制校验。升级引擎版本时应重新核查：针对每个键列出引擎解析器中作为其严格前缀的选项字符串
// （vLLM 的 make_arg_parser，SGLang 的 ServerArgs.add_cli_args）；若列表非空，则说明此处会将该拼写
// 误识别为错误的参数。
var modelDeploymentListenArgs = map[string][]string{
	workercore.ModelDeploymentEngineVLLM: {
		modelDeploymentEngineHostArg, modelDeploymentEnginePortArg,
		modelDeploymentEngineTLSCertArg, modelDeploymentEngineTLSKeyArg,
		modelDeploymentEngineTLSClientAuthArg,
	},
	workercore.ModelDeploymentEngineSGLang: {
		modelDeploymentEngineHostArg, modelDeploymentEnginePortArg,
		modelDeploymentEngineTLSCertArg, modelDeploymentEngineTLSKeyArg,
	},
}

// ModelDeploymentListenArg 报告引擎自身解析器会将某条命令行参数识别为哪个监听或 TLS 参数，
// 对其他任意参数则返回 ""。
func ModelDeploymentListenArg(engine, arg string) string {
	key, _ := ModelDeploymentResolveArg(engine, arg, modelDeploymentListenArgs[engine])

	return key
}

// ModelDeploymentRenderInput 包含渲染单个副本 Pod 所需的全部信息。
//
// InstanceType 和 connector 以值形式传入而非在此处读取，因为渲染必须保持为对其输入的纯函数：
// 调谐器在每次循环中都会收敛同一对象，若渲染过程主动调用客户端，就可能为同一 spec 返回两个
// 不同的 Pod，导致 Deployment 持续滚动更新。
type ModelDeploymentRenderInput struct {
	// Deployment 是正在渲染的对象。
	Deployment *workercore.ModelDeployment
	// Role 是该副本所属的 Deployment.Spec.Roles 条目。
	Role *workercore.ModelDeploymentRole
	// InstanceType 是该角色的 Pod 准入所依据的实例类型。它提供了加速器资源键的拼写方式以及
	// 用于推导主机资源请求的单卡单位资源。
	InstanceType *worker.InstanceType
	// Connector 是引擎连接资源池所需的配置。其零值会渲染出完全不带 connector 的副本，
	// 适用于 Binding 尚未解析完成的 Deployment。
	Connector ModelDeploymentConnectorRender
	// ECConnector 是告知解码 sidecar 使用的编码器缓存连接器，采用 sidecar 自身的拼写方式；
	// 当该 Deployment 不运行编码器阶段时为空。
	//
	// 它是由调用方解析出的矩阵行结果，而非在此处根据平台重新推导。矩阵行代表了准入阶段针对某组
	// 路由器/引擎/加速器组合的完整结论，而渲染器已在 Connector.Args 中为引擎侧携带了其副本；
	// 若在旁边再针对同一平台重新推导 sidecar 侧，就会形成对同一问题的两套 switch 逻辑，进而可能
	// 给出两个不同答案。空值不会渲染任何参数，sidecar 会将其解释为“跳过编码器阶段”——因此空值出现的
	// 两种原因（无 Encode 角色，或未准入的组合）最终渲染结果完全一致而非有所区别。
	ECConnector string
	// InterfaceResource 是根据角色有效网络协议选出的扩展资源。当角色未请求网卡接口时为空。
	InterfaceResource core.ResourceName
	// 此处没有 ConfigMap 名称，也没有需要命名的对象：客户端配置通过 Connector.PodAnnotations 传递，
	// 并以 downwardAPI 投射的方式进入容器。该结构体原先携带的字段从未被填充，因此它所控制的挂载属于
	// 死代码；Connector.PodAnnotations 才是实际载体。
	// RuntimeClassName 是使用加速器的副本所需的运行时类。由调谐器负责解析，
	// 因为确定该值需要检查集群中是否存在该类。
	RuntimeClassName string
	// GeneralResourcesOvercommit 对应 Instance 路径的过载超卖设置，用于决定推导出的 CPU 和内存
	// 是按完整大小请求还是按比例缩减。
	GeneralResourcesOvercommit bool
	// TCPTWReuse 对应 model-deployment-tcp-tw-reuse 设置。它是集群级别的设定，而非角色级别：
	// 哪些 Pod 会将其转化为 sysctl 由 modelDeploymentTakesTCPTWReuse 决定。
	TCPTWReuse bool
	// NativeSidecar 决定直接解码器（direct decoder）的路由代理以哪种形态渲染：当集群保留该字段时，
	// 渲染为原生 sidecar（即带有 restartPolicy: Always 的 init 容器）；否则渲染为传统的普通容器。
	// 该值由调谐器根据集群版本解析，而非由渲染器猜测，因为在 1.29 以下版本中该字段会被静默丢弃
	// 而不报错，导致带该字段渲染出的 Pod 永远无法离开 Init 阶段。
	NativeSidecar bool
	// Ordinal 是此次渲染对应的副本序号，会写入组元数据中。模板部分从不读取该值——只有打标（stamp）
	// 部分会读取——其零值渲染第一个副本，供未指定特定副本的调用方使用。
	Ordinal int

	// Artifact 表示 spec.model.artifactRef 指定的权重如何送达引擎，由调谐器负责解析。
	// 为 nil 时会按该字段引入前的方式渲染未指定制品的 Deployment；
	// 指定了制品的 Deployment 在其解析完成前完全不会进行渲染。
	Artifact *ModelDeploymentArtifactRender

	// Member 是此次渲染对应该副本中的第几个 Pod（从 0 开始计数）。它与 Ordinal 出于相同原因
	// 仅在打标（stamp）阶段写入而绝不进入模板：模板是同一角色所有 Pod 共享的内容，若模板中包含
	// 指向某个特定 Pod 的值，会导致同一副本的两个成员算出不同的哈希并被误判为持续滚动发布。
	//
	// 零值代表 leader，未指定具体成员的调用方会得到该值，它也是大小为 1 的副本唯一的成员。
	Member int
}

// modelDeploymentSelectorLabels 用于匹配角色下的各副本：仅包含 Deployment 与角色的标识，
// 不包含任何会因 spec 更新而变动的字段。
func modelDeploymentSelectorLabels(
	md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole,
) map[string]string {
	return map[string]string{
		modelDeploymentLabelKeyName:      modelDeploymentLabelValueName,
		modelDeploymentLabelKeyInstance:  md.Name,
		modelDeploymentLabelKeyComponent: role.Name,
	}
}

// IMPORTANT: 单 pod 封装函数
// renderModelDeploymentPod 渲染完整副本 Pod：先构建角色级模板，再写入该副本成员的组元数据与指纹。
func renderModelDeploymentPod(ctx context.Context, in ModelDeploymentRenderInput) (*core.Pod, error) {
	// 先早模板
	pod, err := renderModelDeploymentPodTemplate(ctx, in)
	if err != nil {
		return nil, err
	}

	// 再盖钢印（写 Kueue 组名、算 spec 哈希、写多机 DNS）
	stampModelDeploymentPod(pod, in.Deployment, in.Role, in.Ordinal, in.Member)

	return pod, nil
}

// IMPORTANT: 逐行装配出“角色模板”
//
// renderModelDeploymentPodTemplate 渲染副本 Pod 中由其角色 spec 声明的全部内容：
// labels、annotations、PodSpec 以及其中的所有容器。
//
// 它不会生成任何在同角色不同副本之间变化的内容，而拆分为模板与打标（stamp）两步正是为了确保
// 这一性质可被检查：Kueue 组元数据和 spec 哈希指纹在之后写入，因为按副本区分的组名在同角色
// 成员间各不相同，一旦指向单个成员的值泄漏进此函数，就会为每个副本渲染出不同的 Pod 且不会在
// 任何地方报错。connector 的 PodAnnotations 写入此处而非打标阶段，是因为它们携带的是整个角色
// 所有副本共享的池端点与域信息。
func renderModelDeploymentPodTemplate(ctx context.Context, in ModelDeploymentRenderInput) (*core.Pod, error) {
	md, role := in.Deployment, in.Role

	// 显式声明的角色镜像始终优先：角色级镜像用于覆盖部署级默认引擎镜像（例如私有构建），
	// 未声明角色镜像时则使用部署级引擎镜像。
	image := role.Image
	if image == "" {
		image = md.Spec.Engine.Image
	}
	if image == "" {
		// 经准入校验后不可达（schema 要求 spec.engine.image 非空）；显式报错以防 schema 约束变更时渲染出空镜像 Pod。
		return nil, fmt.Errorf("role %q names no image and the deployment engine names none either", role.Name)
	}

	// IMPORTANT: 工序 1：确定镜像与计算硬件资源（GPU/CPU/内存/磁盘）
	ress, err := deriveModelDeploymentResources(role, in.InstanceType)
	if err != nil {
		return nil, err
	}

	// IMPORTANT: 工序 2：拿 Kueue 队列入口
	// 入口队列直接从 InstanceType.Status.Entrance 读取而非重新计算，保证与创建 LocalQueue 的调谐器
	// 消费同一份事实来源，避免命名规则变化后 Pod 被路由到不存在的 LocalQueue。
	entrance := in.InstanceType.Status.Entrance
	if entrance == "" {
		// InstanceType 状态尚未计算完成时拒绝渲染而非回退：缺少 queue-name 标签的 Pod 会绕过 Kueue 直接进入 kubelet。
		return nil, fmt.Errorf(
			"instance type %q publishes no queue entrance yet", in.InstanceType.Name,
		)
	}

	// 替换命令行的角色拥有整条 argv，因此 operator 既不注入引擎参数，也不注入仅服务于受管参数的客户端环境变量。
	takeOver := len(role.Command) > 0

	command := role.Command
	// 依据连接器自身的 RoutingSidecar 标志（而非传输链路存在性）判断是否前置路由代理，
	// 并复核角色类型为 Decode，因为只有对应路由器的 decoder 才需要由代理接管对外端口。
	directDecode := !takeOver && in.Connector.RoutingSidecar &&
		ModelDeploymentEffectiveRoleKind(role) == workercore.ModelDeploymentRoleKindDecode

	// Service 目标端口、探针端口与容器端口均来自同一个推导函数，避免 Service 转发地址与探针探测地址漂移。
	servingPort := modelDeploymentTargetPort(md.Spec.Engine.Name, role, directDecode)
	// declaredPorts 在此处一次性计算，供下方去重连接器合成端口，并保证 direct-decode 场景下引擎内部端口
	// 避开角色声明的全部端口，而非仅避开 Service 入口端口。未声明端口时默认只开放 servingPort。
	declaredPorts := modelDeploymentContainerPorts(role)
	if len(role.Ports) == 0 {
		declaredPorts[0] = servingPort
	}
	// 接管模式下命令行非本 operator 构造，不假设其暴露标准健康检查，因此 gradable 为 false。
	var (
		scheme     core.URIScheme
		gradable   bool
		enginePort = servingPort.ContainerPort
	)
	if directDecode {
		enginePort = modelDeploymentInternalPort
		for modelDeploymentPortTaken(declaredPorts, enginePort) {
			enginePort++
		}
	}
	// IMPORTANT: 工序 3: 把“模型权重”翻译成挂载卷和启动参数
	if !takeOver {
		// 使用模型制品时，引擎从交付位置加载权重，并以 spec.model.name 对外提供服务；
		// 当模型参数变为挂载路径或仓库名时，通过默认 --served-model-name 保持服务名不变。
		model, defaulted := md.Spec.Model.Name, in.Connector.DefaultedArgs
		if in.Artifact != nil {
			// 返回引擎启动命令中指定的模型标识
			model = in.Artifact.model()
			defaulted = append(slices.Clone(defaulted), []string{ModelDeploymentServedModelNameArg, md.Spec.Model.Name})
		}
		// IMPORTANT: 工序 4：拼装推理引擎的启动命令与端口对齐
		command, err = ModelDeploymentEngineCommand(md.Spec.Engine.Name, model)
		if err != nil {
			return nil, err
		}
		if in.Artifact != nil {
			command = append(command, in.Artifact.args()...)
		}
		command = append(command, in.Connector.Args...)
		command = append(command, modelDeploymentDefaultedArgs(defaulted, role.ExtraArgs)...)
		command = append(command, role.ExtraArgs...)

		// IMPORTANT: 工序 5：协议探测与 K8s 三大健康探针
		//
		// 在填充监听参数之前先分析传输协议，且扫描与填充逻辑完全相同的参数列表。
		// 端口是否仍由 operator 控制取决于填充前命令行中已有的参数；若此处扫描的范围更窄，
		// 连接器带入的监听标志会被填充跳过却对门控不可见，导致两者判断不一致。
		scheme, gradable = modelDeploymentEngineTransport(md.Spec.Engine.Name, command)
		command = appendModelDeploymentBindArgs(md.Spec.Engine.Name, command, enginePort)
		listenPort, listenErr := modelDeploymentCommandPort(md.Spec.Engine.Name, command)
		if !directDecode && (listenErr != nil || listenPort != servingPort.ContainerPort) {
			// 引擎未监听在 Service 指向的端口上（只可能出现在角色声明端口与自身 --port 不一致且
			// 对象早于准入规则存储的场景）。此时门控无论指向哪一方都无法代表真实流量入口，直接撤回探针。
			gradable = false
		}
		if directDecode {
			enginePort, err = listenPort, listenErr
			if err != nil {
				return nil, fmt.Errorf("role %q cannot place its routing proxy: %w", role.Name, err)
			}
			for _, declared := range declaredPorts {
				if enginePort == declared.ContainerPort {
					return nil, fmt.Errorf(
						"role %q places its model server on port %d, which the role already "+
							"declares as %q", role.Name, enginePort, declared.Name,
					)
				}
			}
			gradable = true
		}
	}

	// 连接器的卷与挂载项由上游成对生成，且与下方写入的 Pod 注解作为一个整体生效：卷通过 downwardAPI
	// 投影其中一条注解，若只挂卷不写注解会得到空文件。对于通过环境变量配置的引擎，两者均为空，
	// 因此此处无需再按引擎分叉。
	//
	// 接管模式的角色既不获得连接器卷和注解，也不获得合成参数和客户端环境变量：
	// operator 没有构造其命令行，不能假定容器会消费这些缓存配置。
	vols, mounts := convertModelDeploymentAdditionalVolumes(role.AdditionalVolumes)
	if in.Artifact != nil {
		artifactVols, artifactMounts := in.Artifact.volumes(takeOver)
		vols = append(vols, artifactVols...)
		mounts = append(mounts, artifactMounts...)
	}
	if !takeOver {
		vols = append(vols, in.Connector.Volumes...)
		mounts = append(mounts, in.Connector.VolumeMounts...)
		// 除非角色自带卷已覆盖该路径，否则所有受管角色都在固定容器路径挂载编译缓存，并映射到宿主机上
		// 属于该角色的独立子目录。这样引擎只需一个固定的缓存根目录环境变量，无需感知具体权重、并行形状
		// 或标志组合，同时宿主机侧按角色哈希隔离，防止不同角色互相覆盖生成的核函数。若用户卷已经挂载在
		// 该路径（或其内/外层），operator 像 /dev/shm 一样让出默认宿主机卷。使用默认宿主机卷时，如果无法
		// 计算缓存子路径必须直接报错而不是静默省略缓存：让毫无关系的两个角色共享同一根目录正是隔离机制
		// 要防止的冲突。
		if !modelDeploymentCompileCacheMountOverridden(mounts) {
			compileCacheSubPath, err := BuildModelDeploymentCompileCacheSubPath(
				ModelDeploymentCompileCacheInput{
					Deployment:   md,
					Role:         role,
					InstanceType: in.InstanceType,
					Artifact:     in.Artifact,
				},
			)
			if err != nil {
				return nil, fmt.Errorf(
					"role %q cannot place its compilation cache: %w", role.Name, err,
				)
			}
			cacheVolume, cacheMount := modelDeploymentCompileCacheVolumes(compileCacheSubPath)
			vols = append(vols, cacheVolume)
			mounts = append(mounts, cacheMount)
		}
	}
	vols, mounts = modelDeploymentSharedMemoryVolumes(role, vols, mounts)

	probePath := modelDeploymentProbePath
	probeScheme := scheme
	if directDecode {
		probeScheme = core.URISchemeHTTP
		probePath = modelDeploymentDirectDecodeProbePath
	}
	// IMPORTANT: 工序 5：协议探测与 K8s 三大健康探针
	startupProbe, readinessProbe, livenessProbe := modelDeploymentProbes(
		servingPort, probeScheme, probePath, gradable,
		modelDeploymentProbesEncoderStage(md, role, takeOver), role.StartupTimeoutSeconds,
	)
	ports, err := appendModelDeploymentConnectorPorts(declaredPorts, in.Connector, takeOver)
	if err != nil {
		return nil, err
	}
	if directDecode {
		ports[0].Name = "model-server"
		ports[0].ContainerPort = enginePort
	}

	// IMPORTANT: 工序 6：环境变量分层、安全能力与内核参数
	mainC := core.Container{
		Name:            modelDeploymentMainContainerName,
		Image:           image,
		ImagePullPolicy: role.ImagePullPolicy,
		Command:         command,
		Resources: getResourceRequirements(
			ress, in.InstanceType, true, in.GeneralResourcesOvercommit, true, false,
		),
		Ports:          ports,
		Env:            mergeModelDeploymentEnv(md.Spec.Engine.Name, role, in.Connector, in.Artifact, takeOver),
		VolumeMounts:   mounts,
		StartupProbe:   startupProbe,
		ReadinessProbe: readinessProbe,
		LivenessProbe:  livenessProbe,
	}
	if in.Artifact != nil {
		// 将引擎下载缓存的上限叠加到主容器的 ephemeral-storage limit 上，同时保持 request 不变。
		in.Artifact.raiseEphemeralStorageLimit(&mainC, takeOver)
	}
	if role.Resources != nil && role.Resources.Interface != nil && role.Resources.Interface.Sign() > 0 {
		if in.InterfaceResource == "" {
			return nil, fmt.Errorf("role %q requests interfaces without a fabric resource", role.Name)
		}
		mainC.Resources.Limits[in.InterfaceResource] = role.Resources.Interface.DeepCopy()
	}
	if role.Privileged {
		mainC.SecurityContext = &core.SecurityContext{Privileged: ptr.To(true)}
	} else if modelDeploymentRoleNeedsEngramCapabilities(md.Spec.Engine.Name, role, command) {
		mainC.SecurityContext = &core.SecurityContext{
			Capabilities: &core.Capabilities{
				// IPC_LOCK 用于锁定 Engram 表注册给 UVA 访问的宿主机内存，SYS_RESOURCE 允许运行时按需提高锁内存上限。
				Add: []core.Capability{"IPC_LOCK", "SYS_RESOURCE"},
			},
		}
	}
	// 接管模式角色不注入排空 hook，原因与不注入探针相同：operator 未构造其命令行，无法保证容器提供 hook 所需的指标接口。
	if !takeOver {
		mainC.Lifecycle = &core.Lifecycle{
			// IMPORTANT: 工序 8：容器退出前的最后防线: preStop 排空钩子
			PreStop: modelDeploymentDrainHook(md.Spec.Engine.Name, enginePort, scheme,
				modelDeploymentTerminationGracePeriodSeconds(role)),
		}
	}

	pod := &core.Pod{
		ObjectMeta: meta.ObjectMeta{
			// 名称留空并仅渲染前缀：spec 声明的是可互换的副本数量，不为单个副本预设身份，
			// 区分各实例的随机后缀由 API server 在创建时分配。替换副本是一个新实例并获得新后缀，
			// 绝不沿用被替换副本的名称。
			GenerateName: md.Name + "-" + role.Name + "-",
			Namespace:    md.Namespace,
			Labels:       modelDeploymentPodLabels(md, role, entrance),
		},
		Spec: core.PodSpec{
			// 推理副本不是交互式 Shell 环境，无需访问 Kubernetes API，也不启用服务环境变量注入。
			AutomountServiceAccountToken: ptr.To(false),
			EnableServiceLinks:           ptr.To(false),
			// 滚动策略为重建替换，退出的副本由新创建的 Pod 替代而非原地以陈旧 spec 重启。
			RestartPolicy:    core.RestartPolicyAlways,
			ImagePullSecrets: role.ImagePullSecrets,
			Volumes:          vols,
			Containers:       []core.Container{mainC},
		},
	}
	// IMPORTANT: EPD 软亲和性。多模态部署有 Encode（视觉编码）、Prefill、Decode 三种角色。
	//
	// 同节点软亲和偏好属于模板的一部分，确保同角色所有副本携带一致的调度项并被 spec 哈希覆盖。
	// 它与创建时注入的节点级条件互补：后者写入 NodeAffinity，与 PodAffinity 互不重叠。
	if podAffinity := modelDeploymentEPDPodAffinity(md, role); podAffinity != nil {
		if pod.Spec.Affinity == nil {
			pod.Spec.Affinity = &core.Affinity{}
		}
		pod.Spec.Affinity.PodAffinity = podAffinity
	}
	// 排空 hook 所依据的宽限期与 hook 配套写入；接管模式副本仅在角色显式指定时写入，
	// 未指定时保持引入该特性前原有的 Pod 渲染结果。
	if !takeOver {
		pod.Spec.TerminationGracePeriodSeconds = ptr.To(modelDeploymentTerminationGracePeriodSeconds(role))
	} else if role.TerminationGracePeriodSeconds != nil {
		pod.Spec.TerminationGracePeriodSeconds = ptr.To(*role.TerminationGracePeriodSeconds)
	}
	if directDecode {
		manufacturer := in.InstanceType.Status.Detail.Manufacturer
		// IMPORTANT: Direct-Decode 路由代理
		sidecar := renderModelDeploymentRoutingSidecar(
			ctx, role, enginePort, scheme, md.Spec.Engine.Name, in.NativeSidecar,
			manufacturer, in.ECConnector,
		)
		if in.NativeSidecar {
			pod.Spec.InitContainers = []core.Container{sidecar}
		} else {
			// 传统形态将代理作为第一个普通容器放置：kubelet 按列表顺序启动普通容器，但这仅是事实顺序，
			// 不具备原生 sidecar 的 API 保证。两种形态下真正控制流量的门控都是引擎容器探针指向代理端口，
			// 因此在代理监听前 Pod 不会就绪。
			//
			// 传统形态失去的是关闭顺序保证：原生 sidecar 仅在所有普通容器退出后才被停止，而传统容器会与
			// 引擎同时收到终止信号，删除 Pod 时代理可能先于引擎排空结束而退出，导致在途交接重置并触发调用方
			// 重试到其他副本。引擎自身的排空 hook 无法保护传统形态的代理，且代理镜像没有 Shell 来运行独立 hook。
			// 失去启动顺序保证则没有实质影响：引擎不会主动连接代理，引擎就绪前到达代理的请求收到 503，
			// 引擎启动后即可自行恢复。
			pod.Spec.Containers = append([]core.Container{sidecar}, pod.Spec.Containers...)
		}
	}
	if in.RuntimeClassName != "" {
		pod.Spec.RuntimeClassName = ptr.To(in.RuntimeClassName)
	}
	// Pod 级 SecurityContext 仅在此处写入：角色没有 Pod 级安全字段，特权模式落在上方主容器上，无需合并。
	if in.TCPTWReuse && modelDeploymentTakesTCPTWReuse(md, role, in.Connector) {
		pod.Spec.SecurityContext = &core.PodSecurityContext{
			Sysctls: []core.Sysctl{{Name: modelDeploymentTCPTWReuseSysctl, Value: "1"}},
		}
	}

	// 此处不针对加速器型号渲染 nodeSelector。角色接受所属资源池分配的任意 flavor：
	// Kueue 按 PodSet 评估候选 flavor，不附加额外选择器时不会在单一资源池内人为缩小候选集。
	systemmeta.NoteResource(pod, ModelDeploymentResourceType, map[string]string{
		ModelDeploymentResourceNoteRole: role.Name,
	})
	kubemeta.ControlOnWithoutBlock(pod, md, workercore.SchemeGroupVersionKind("ModelDeployment"))

	// 连接器注解属于模板输出，必须落在打标计算指纹之前。客户端配置存放在注解中而非 ConfigMap，
	// 模板指纹覆盖了这些注解，因此池端点或域变化时会改变哈希并触发副本重建以获取新配置；
	// 若放到打标之后写入，指纹会感知不到配置变化，导致旧副本静默持有过期配置且无报错。
	//
	// 使用与投影卷相同的接管判断：替换命令行的角色不接收任何连接器配置，只给一半比完全不给更糟。
	if !takeOver && len(in.Connector.PodAnnotations) > 0 {
		if pod.Annotations == nil {
			pod.Annotations = make(map[string]string, len(in.Connector.PodAnnotations)+2)
		}
		for k, v := range in.Connector.PodAnnotations {
			pod.Annotations[k] = v
		}
	}
	if !takeOver {
		if pod.Annotations == nil {
			pod.Annotations = make(map[string]string, 5)
		}
		for key, value := range modelDeploymentMetricsAnnotations(enginePort, scheme) {
			pod.Annotations[key] = value
		}
	}
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string, 1)
	}
	pod.Annotations[modelDeploymentTemplateBindingHashAnnotation] = modelDeploymentBindingHash(in, *ress)

	return pod, nil
}

// modelDeploymentStampRouterCandidate 标记作为路由候选者的已渲染 Pod。
//
// 它读取的是已完成的 Pod 而非期望的 Role。Pod 反映了其实际运行的模式：接管了自身命令行的
// Pod 不会携带指标注解，因为该命令行并非由 operator 构建，无法保证容器会提供钩子读取的指标。
// 期望的 Role 描述要求该 Pod 运行什么，而 Pod 才代表实际正在运行的内容。
//
// 由 operator 控制的角色在创建时并非候选者。它尚未经过评估，仅当其所属组符合资格时才会向此类
// 成员写入该标记，因此受管的新建 Pod 初始无标记，随后与资格标签一同获得该标记。
//
// operator 无法评估的角色自创建起即为候选者，且仅由形态允许响应的成员携带该标记：
// leader-served 副本中的 leader，以及 External-DP 副本中的每个成员。
func modelDeploymentStampRouterCandidate(
	pod *core.Pod, md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole, member int,
) {
	if md.Spec.Router == nil {
		return
	}
	managed, _ := modelDeploymentMemberExecution(pod)
	effective := *role
	if managed {
		// 该副本运行的是 operator 生成的命令行，因此角色自身的 command 无法说明副本所处的模式。
		effective.Command = nil
	}
	if modelDeploymentGroupForwardActivatable(md, &effective) {
		return
	}
	// 非本 operator 构建的成员不携带受管负载均衡配置，这是明确确定的结果，而非缺失信息。
	externalDP := managed && modelDeploymentRoleExternalDP(md, &effective)
	switch modelDeploymentAnsweringShapeOf(externalDP, modelDeploymentRoleSize(&effective)) {
	case modelDeploymentAnsweringAll, modelDeploymentAnsweringSole:
	case modelDeploymentAnsweringLeader:
		if member != modelDeploymentLeaderMemberIndex {
			return
		}
	default:
		return
	}
	if pod.Labels == nil {
		pod.Labels = map[string]string{}
	}
	pod.Labels[modelDeploymentRouterCandidateLabel] = modelDeploymentEndpointEligibleValue
}

// modelDeploymentSharedMemoryVolumes 为角色添加默认的共享内存挂载，除非已显式配置。
func modelDeploymentSharedMemoryVolumes(
	role *workercore.ModelDeploymentRole, vols []core.Volume, mounts []core.VolumeMount,
) ([]core.Volume, []core.VolumeMount) {
	if !slices.ContainsFunc(mounts, func(m core.VolumeMount) bool { return m.MountPath == "/dev/shm" }) {
		size := resource.MustParse("16Gi")
		if role.ShmSize != nil {
			size = role.ShmSize.DeepCopy()
		}
		vols = append(vols, core.Volume{
			Name: "model-shm",
			VolumeSource: core.VolumeSource{EmptyDir: &core.EmptyDirVolumeSource{
				Medium: core.StorageMediumMemory, SizeLimit: &size,
			}},
		})
		mounts = append(mounts, core.VolumeMount{Name: "model-shm", MountPath: "/dev/shm"})
	}

	return vols, mounts
}

// modelDeploymentTCPTWReuseSysctl 允许内核为新的出站连接复用处于 TIME-WAIT 状态的端口。
// 该 sysctl 按网络命名空间隔离，仅影响 Pod 自身网络命名空间而不影响宿主机。
const modelDeploymentTCPTWReuseSysctl = "net.ipv4.tcp_tw_reuse"

// modelDeploymentTakesTCPTWReuse 报告在开关启用时角色 Pod 是否携带 modelDeploymentTCPTWReuseSysctl：
// 仅适用于由本 operator 渲染了传输链路的 SGLang prefill 角色。
//
// 发起连接的是 prefill 端。SGLang 的 Mooncake 传输引擎每次传输都会向 decode 端和各个存储成员
// 新建 TCP 连接并先关闭它，因此 TIME-WAIT 套接字累积在 prefill 端的网络命名空间中。该 sysctl
// 仅影响内核为出站连接挑选端口，被连接的一侧不会从中受益，而 decode 端不运行分层缓存也不会向存储写入。
//
// 此处不检查协议字段：operator 不给 SGLang 直连链路授予 RDMA 设备，所有实测形态均走 TCP；在不开
// 内核 TCP 连接的链路上该 sysctl 也无副作用。vLLM 复用长连接不会积累 TIME-WAIT，因此不加该 sysctl
// （避免对节点 kubelet 白名单增加额外要求）。接管命令行的角色同样不加，遵循与连接器一致的规则。
func modelDeploymentTakesTCPTWReuse(
	md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole,
	connector ModelDeploymentConnectorRender,
) bool {
	return md.Spec.Engine.Name == workercore.ModelDeploymentEngineSGLang &&
		len(role.Command) == 0 &&
		connector.KVTransfer &&
		ModelDeploymentEffectiveRoleKind(role) == workercore.ModelDeploymentRoleKindPrefill
}

func modelDeploymentMetricsAnnotations(port int32, scheme core.URIScheme) map[string]string {
	return map[string]string{
		"prometheus.io/scrape": "true",
		"prometheus.io/path":   "/metrics",
		"prometheus.io/port":   strconv.Itoa(int(port)),
		"prometheus.io/scheme": strings.ToLower(string(scheme)),
	}
}

// stampModelDeploymentPod 为已渲染的副本写入 Kueue 组元数据，随后写入显式的 spec 哈希指纹
// 以及 operator 渲染版本注解。
func stampModelDeploymentPod(
	pod *core.Pod, md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole,
	ordinal, member int,
) {
	// 规模大于 1 时，名称通过推导得出而非自动生成，整个多成员（multi-Member）形态正依赖于此：
	// 成员无法加入一个自己无法寻址的集合通信组，而通过 GenerateName 创建的 Pod 其名称无法被
	// 兄弟节点预测。每个读取方——本渲染器、收敛器以及兄弟节点的入口脚本——都无需读取外部状态，
	// 仅凭相同的四个值即可推导出完全一致的字符串。
	//
	// 规模为 1 时名称刻意保持自动生成。单副本的身份来自其 ordinal 标签而非名称，因此为单成员
	// 副本固定命名没有任何收益，反而会失去 GenerateName 带来的关键好处：新建操作不会与仍在
	// 排空（draining）的同 ordinal 旧 Pod 发生命名冲突，而这正是收敛器的创建门控所等待的状态。
	// 规模大于 1 时冲突无法避免——地址即名称——此时改由该门控负责处理。
	if modelDeploymentRoleSize(role) > 1 {
		pod.Name = modelDeploymentMemberName(md, role.Name, ordinal, member)
		pod.GenerateName = ""

		// hostname 和 subdomain 将该名称转换为 DNS 记录：此处指定的无头 Service 会为每个声明它的 Pod
		// 发布 <hostname>.<subdomain>，这是 core/v1 的原生行为，不依赖任何控制器。
		pod.Spec.Hostname = pod.Name
		pod.Spec.Subdomain = modelDeploymentReplicaServiceName(md, role.Name, ordinal)

		stampModelDeploymentRankEnv(pod, md, role, ordinal)
	}

	// Kueue 组元数据使单个副本成为独立的准入单元：组名按 (role, ordinal) 推导，因此每个 Pod
	// 加入其各自的成员组。
	//
	// 标签与注解必须一起应用，因为该组类型本身会同时返回两者——如果 Pod 仅携带成员关系标签而缺少
	// 总数信息，它加入的组将无法让 Kueue 获知规模，从而根本无法组成 Workload。
	group := ModelDeploymentPodGroup(md, role, ordinal, member)
	for k, v := range group.Labels {
		pod.Labels[k] = v
	}
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string, len(group.Annotations)+2)
	}
	for k, v := range group.Annotations {
		pod.Annotations[k] = v
	}

	bindingHash := pod.Annotations[modelDeploymentTemplateBindingHashAnnotation]
	delete(pod.Annotations, modelDeploymentTemplateBindingHashAnnotation)
	pod.Annotations[modelDeploymentPodSpecHashAnnotation] = modelDeploymentExplicitSpecHash(
		md, role, ordinal, member, bindingHash,
	)
	pod.Annotations[modelDeploymentRenderVersionAnnotation] = strconv.Itoa(modelDeploymentRenderVersion)
	modelDeploymentStampRouterCandidate(pod, md, role, member)
}

// stampModelDeploymentRankEnv 发布成员加入其集合通信组所需的三个事实：谁是 leader、
// 共有多少成员，以及当前是第几个成员。
//
// 它只发布事实，不拼装任何参数。引擎开启哪种并行模式以及跨越多少 rank，始终由作者决定：
// 成员总数是多个并行度之积，无法从单个数字反推拆解，因此此处的公式推导只会变成能跑起来
// 却错误的猜测。已声明的并行度确实会在别处被读取——传输文档的并行块会从角色自身配置、
// extra arguments 以及针对 vLLM 数据并行宽度的字面量 VLLM_DP_SIZE 条目中解析——但该读取仅把
// 作者的数字复制到另一份文档中，不会改变此处发布的任何成员关系事实。
//
// 索引通过 downward API 而非字面量传入，这保证了每个 ReplicaGroup 共用同一份模板：每个成员的
// 容器都声明相同的 `fieldRef`，因此这三条环境变量在副本的所有成员间逐字节一致，唯一不同的只有
// 打标阶段已经写入的标签。把索引写成字面量也能工作——线上 Pod 是与其自身成员索引的渲染结果而非
// 副本整体对比——但那会让容器 spec 变成按成员区分的文档，而“一份模板描述整个副本”这一
// Boundaries 不变量比省下两行代码更有价值。
//
// 仅为主容器写入该信息。路由代理是本 operator 自身的 sidecar，不加入任何集合通信组；
// 为其注入 rank 只会描述一段没有任何组件会使用的成员关系。
//
// 接管了自身命令行的角色同样会写入该信息，这一点与 connector 的规则不同。connector 的条目
// 指向同样由 operator 编写的配置文件，因此不读取该配置的接管 argv 用不到它们；而这些环境变量
// 描述的是 Pod 所运行的集群信息，自行拼装启动命令的入口脚本恰好需要它们。
func stampModelDeploymentRankEnv(
	pod *core.Pod, md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole, ordinal int,
) {
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name != modelDeploymentMainContainerName {
			continue
		}
		pod.Spec.Containers[i].Env = append(pod.Spec.Containers[i].Env,
			core.EnvVar{
				Name: modelDeploymentLeaderAddressEnv,
				Value: modelDeploymentMemberName(
					md, role.Name, ordinal, modelDeploymentLeaderMemberIndex,
				) +
					"." + modelDeploymentReplicaServiceName(md, role.Name, ordinal),
			},
			core.EnvVar{
				Name:  modelDeploymentReplicaSizeEnv,
				Value: strconv.Itoa(modelDeploymentRoleSize(role)),
			},
			core.EnvVar{
				Name: modelDeploymentMemberIndexEnv,
				ValueFrom: &core.EnvVarSource{FieldRef: &core.ObjectFieldSelector{
					FieldPath: "metadata.labels['" + modelDeploymentMemberIndexLabel + "']",
				}},
			},
		)

		return
	}
}

func modelDeploymentCommandPort(engine string, command []string) (int32, error) {
	for i := len(command) - 1; i >= 0; i-- {
		if ModelDeploymentListenArg(engine, command[i]) != modelDeploymentEnginePortArg {
			continue
		}
		_, value, inline := strings.Cut(command[i], "=")
		if !inline {
			if i+1 >= len(command) {
				return 0, fmt.Errorf("%s has no value", modelDeploymentEnginePortArg)
			}
			value = command[i+1]
		}
		port, err := strconv.ParseInt(value, 10, 32)
		if err != nil || port < 1 || port > 65535 {
			return 0, fmt.Errorf("%s has invalid value %q", modelDeploymentEnginePortArg, value)
		}
		return int32(port), nil
	}
	return 0, fmt.Errorf("%s is missing", modelDeploymentEnginePortArg)
}

// renderModelDeploymentRoutingSidecar 构建解码 Pod 的路由代理。它采用哪种握手协议取决于 ENGINE
// 和资源池的厂商，因为 connector 决定了 sidecar 如何向 prefiller 的 bootstrap 注册表请求传输端点，
// 而每种引擎提供的注册表各不相同：该值指定的是 sidecar 据以分发实现的 connector，而非同一协议的不同变体。
//
// BOOTSTRAP 端口是在两端写入的同一数值。prefiller 在其自身的启动参数中指定该端口（由 inject 包
// 根据其常量渲染），sidecar 则在此处读取它。在 Mooncake 下该端口作为命令行参数传递；在 SGLang 下
// 则不能，因为 sidecar 的 SGLang 端口是一个完全没有命令行参数的进程级常量，只能通过
// SGLANG_BOOTSTRAP_PORT 环境变量覆盖（llm-d/llm-d-router@v0.10.0
// `pkg/sidecar/proxy/connector_sglang.go:38-50`）。两端读取同一个常量，因此配对双方绝不会对
// 注册表所在位置产生分歧。
//
// ASCEND 不传端口参数，因为其配对模式没有可供指向的注册表：vLLM-Ascend connector 会在每次请求的
// kv_transfer_params 中把 prefiller 的主机和端口交给解码器，而 sidecar 的 nixlv2 模式原封不动地
// 转发该文档（llm-d/llm-d-router@v0.10.0 `pkg/sidecar/proxy/connector_nixlv2.go:162-170,405`）。
// 该 connector 值属于 sidecar 自身的常量词汇表（`pkg/sidecar/constants/constants.go:21`，
// KVConnectorNIXLV2），在此处直接拼写而非从引擎名称推导。
// 解码 sidecar 用于拉取编码器嵌入（embeddings）的编码器缓存连接器。这些是 sidecar 自身的术语，
// 而非它们对应的引擎连接器名称：引擎运行 ECCPUConnector 或 ECExampleConnector，而 sidecar 要么通过
// NIXL 拉取编码器输出，要么从两个角色共同挂载的目录中读取。
//
//	llm-d/llm-d-router@v0.11.0 `pkg/sidecar/constants/constants.go:35-39`，依据
//	`pkg/sidecar/proxy/options.go:180-183,671-673` 校验。
//
// 该集合是封闭的，集合之外的值会导致致命错误而非降级运行：sidecar 遇到未识别的 connector 会拒绝启动，
// 因此使用第三种拼写渲染出的解码器永远不会进入就绪状态，也永远不会响应请求。
const (
	// modelDeploymentECConnectorNIXL 是网络路径，适用于其复制路径受 NIXL 支持的平台。
	modelDeploymentECConnectorNIXL = "ec-nixl"
	// modelDeploymentECConnectorExample 是共享目录路径，不需要任何形式的复制路径，也是 Ascend 示例唯一运行的路径。
	modelDeploymentECConnectorExample = "ec-example"
)

// renderModelDeploymentRoutingSidecar 渲染 direct-decode 形态下的路由代理容器。
func renderModelDeploymentRoutingSidecar(
	ctx context.Context,
	role *workercore.ModelDeploymentRole, enginePort int32, engineScheme core.URIScheme,
	engine string, native bool, manufacturer, ecConnector string,
) core.Container {
	externalPort := modelDeploymentServicePort(role)
	args := []string{
		fmt.Sprintf("--port=%d", externalPort.ContainerPort),
		fmt.Sprintf("--model-server-port=%d", enginePort),
	}
	env := []core.EnvVar{{
		Name: "POD_IP",
		ValueFrom: &core.EnvVarSource{FieldRef: &core.ObjectFieldSelector{
			FieldPath: "status.podIP",
		}},
	}}
	switch {
	case engine == workercore.ModelDeploymentEngineSGLang:
		// IMPORTANT: SGLang 引擎分支，SGLang 内置了自己的一套 PD 分离握手协议（底层实际搬砖的传输库也可以配成 Mooncake 或 NIXL，但前面怎么对暗号用的是 SGLang 自己的 Bootstrap 协议）。
		args = append(args, "--kv-connector=sglang")
		env = append(env, core.EnvVar{
			Name:  "SGLANG_BOOTSTRAP_PORT",
			Value: strconv.Itoa(int(inject.SGLangBootstrapPort)),
		})
	case manufacturer == nodefeature.ManufacturerAscend:
		// IMPORTANT: nixlv2（华为昇腾 Ascend NPU 分支）
		args = append(args, "--kv-connector=nixlv2")
	default:
		// IMPORTANT: Mooncake 方案（默认分支）：vLLM + NVIDIA 等常规 GPU
		args = append(args,
			"--kv-connector=mooncake",
			// 告诉 Decode 里的 routing-proxy，去连接 Prefiller 的哪个端口交换显存地址和房间号
			fmt.Sprintf("--mooncake-bootstrap-port=%d", inject.VLLMMooncakeBootstrapPort))
	}
	args = append(args, "--secure-proxy=false")
	// encoder 链路与 KV 链路是独立的握手，由调用方按平台解析后传入，避免在此处再维护一套重复的平台分发分支。
	if ecConnector != "" {
		args = append(args, "--ec-connector="+ecConnector)
	}
	if engineScheme == core.URISchemeHTTPS {
		args = append(args, "--enable-tls=decoder", "--tls-insecure-skip-verify=decoder")
	}

	return core.Container{
		Name:            "routing-proxy",
		Image:           redirectedImage(ctx, settings.ModelDeploymentRoutingSidecarImage.ShouldValue(ctx)),
		ImagePullPolicy: core.PullIfNotPresent,
		Args:            args,
		// 容器级 RestartPolicy 是将 initContainer 声明为原生 sidecar 的关键，仅在 native 形态设置；
		// 传统普通容器不得设置该字段，由 Pod 级 RestartPolicy 负责重启。
		RestartPolicy: func() *core.ContainerRestartPolicy {
			if !native {
				return nil
			}
			return ptr.To(core.ContainerRestartPolicyAlways)
		}(),
		Ports: []core.ContainerPort{{
			Name: externalPort.Name, Protocol: core.ProtocolTCP,
			ContainerPort: externalPort.ContainerPort,
		}},
		Env: env,
		SecurityContext: &core.SecurityContext{
			AllowPrivilegeEscalation: ptr.To(false),
			RunAsNonRoot:             ptr.To(true),
		},
	}
}

// appendModelDeploymentConnectorPorts 将连接器合成的端口合并进容器声明端口列表。
// 若端口号与名称均已存在则去重跳过；若同端口号但名称不同，说明两个端点争用同一端口，直接报错拒绝渲染。
func appendModelDeploymentConnectorPorts(
	ports []core.ContainerPort, connector ModelDeploymentConnectorRender, takeOver bool,
) ([]core.ContainerPort, error) {
	if takeOver {
		return ports, nil
	}

	for _, synthesized := range connector.Ports {
		declared := false
		for _, p := range ports {
			if p.ContainerPort != synthesized.ContainerPort {
				continue
			}
			if p.Name != synthesized.Name {
				return nil, fmt.Errorf(
					"the connector publishes port %d as %q but the template declares it as %q",
					synthesized.ContainerPort, synthesized.Name, p.Name,
				)
			}
			declared = true
		}
		if !declared {
			ports = append(ports, synthesized)
		}
	}

	return ports, nil
}

// modelDeploymentPortTaken 报告指定端口号是否已出现在容器声明的端口列表中。
func modelDeploymentPortTaken(ports []core.ContainerPort, port int32) bool {
	for _, p := range ports {
		if p.ContainerPort == port {
			return true
		}
	}
	return false
}

// modelDeploymentPodLabels 组装副本 Pod 携带的全部标签：Service 选择器身份标签、引导至角色资源池的
// Kueue 队列入口标签、供上游路由区分的角色类型标签，以及所有受管对象统一的 part-of 标签。
//
// 队列入口标签刻意不进入 Service 选择器：它跟随角色所选的 InstanceType 变化，若放入选择器会孤立已有副本。
// 入口值由调用方从 InstanceType.Status.Entrance 读取后传入，确保与 LocalQueue 调谐器使用同一名称。
// 角色类型经由 ModelDeploymentEffectiveRoleKind 解析，保证该标签与 status.roles[].kind 始终一致。
func modelDeploymentPodLabels(
	md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole, entrance string,
) map[string]string {
	labels := modelDeploymentSelectorLabels(md, role)
	labels[kueuectrlconst.QueueLabel] = entrance
	kind, _ := ModelDeploymentRoleUpstreamKind(ModelDeploymentEffectiveRoleKind(role))
	labels[modelDeploymentLabelKeyRoleKind] = kind
	labels["app.kubernetes.io/part-of"] = "gpustack-operator-worker"

	return labels
}

// modelDeploymentContainerPorts 渲染副本暴露的端口。
//
// 未指定任何端口的 role 仍会获得一个默认端口，因为无法访问的副本无法提供任何服务，且前端的
// Service 需要一个目标端口。所有受支持引擎的 OpenAI 兼容服务器默认均监听 8000 端口。
func modelDeploymentContainerPorts(role *workercore.ModelDeploymentRole) []core.ContainerPort {
	if len(role.Ports) == 0 {
		return []core.ContainerPort{{
			Name:          modelDeploymentDefaultPortName,
			Protocol:      core.ProtocolTCP,
			ContainerPort: modelDeploymentDefaultPort,
		}}
	}

	return slicex.Transform(role.Ports, func(p workercore.ModelDeploymentPort) core.ContainerPort {
		return core.ContainerPort{
			Name:          modelDeploymentPortName(p),
			Protocol:      p.Protocol,
			ContainerPort: p.Port,
		}
	})
}

// modelDeploymentPortName 根据容器端口的协议和端口号推导其名称，因为容器端口名称必须符合 Kubernetes 的 DNS 标签规则，而用户提供的自由格式名称不一定满足该要求。Instance 路径也以相同方式推导端口名称。
func modelDeploymentPortName(port workercore.ModelDeploymentPort) string {
	return strings.ToLower(fmt.Sprintf("%s-%d", port.Protocol, port.Port))
}

// convertModelDeploymentAdditionalVolumes 将 role 声明的卷转换为 Pod 卷和容器挂载点。未指定源的条目会被跳过而不进行渲染：空源的卷会导致 API server 在每次 reconcile 时拒绝整个 Pod。
func convertModelDeploymentAdditionalVolumes(
	avs []workercore.ModelDeploymentAdditionalVolume,
) (vols []core.Volume, mounts []core.VolumeMount) {
	if len(avs) == 0 {
		return nil, nil
	}

	vols = make([]core.Volume, 0, len(avs))
	mounts = make([]core.VolumeMount, 0, len(avs))
	for i := range avs {
		av := &avs[i]

		var vs core.VolumeSource
		switch {
		case av.ConfigMap != nil:
			vs.ConfigMap = &core.ConfigMapVolumeSource{
				LocalObjectReference: *av.ConfigMap,
			}
		case av.Secret != nil:
			vs.Secret = &core.SecretVolumeSource{
				SecretName: av.Secret.Name,
			}
		case av.HostPath != nil:
			vs.HostPath = av.HostPath.DeepCopy()
		case av.PersistentVolumeClaim != nil:
			// claim 名称及其自身的只读属性按原样复制：Pod 卷只能引用 Pod 所在 Namespace 中的 claim，因此无需添加命名空间。
			vs.PersistentVolumeClaim = av.PersistentVolumeClaim.DeepCopy()
		default:
			continue
		}

		name := additionalVolumeName(i)
		vols = append(vols, core.Volume{
			Name:         name,
			VolumeSource: vs,
		})
		mounts = append(mounts, core.VolumeMount{
			Name:      name,
			MountPath: av.MountPath,
			ReadOnly:  av.ReadOnly,
			SubPath:   av.SubPath,
		})
	}

	return vols, mounts
}

// appendModelDeploymentBindArgs 在角色未指定监听地址或端口时补充默认绑定参数。
//
// 各引擎默认监听地址不一致（vLLM 监听全网卡 8000，SGLang 仅监听回环 30000 导致 Service 和 kubelet
// 探针都无法访问），显式填充使 Service 目标地址与引擎监听地址成为同一决策。
//
// 若命令行中已通过引擎可识别的任意拼写传入了对应标志，则原样保留，不重复追加。
func appendModelDeploymentBindArgs(engine string, command []string, port int32) []string {
	for _, kv := range [][2]string{
		{modelDeploymentEngineHostArg, modelDeploymentEngineBindHost},
		{modelDeploymentEnginePortArg, strconv.Itoa(int(port))},
	} {
		if modelDeploymentArgsName(engine, command, kv[0]) {
			continue
		}

		command = append(command, kv[0], kv[1])
	}

	return command
}

// modelDeploymentArgsName 报告命令行中是否已携带指定监听标志（兼容引擎解析器认可的各类拼写）。
func modelDeploymentArgsName(engine string, command []string, name string) bool {
	for _, arg := range command {
		if ModelDeploymentListenArg(engine, arg) == name {
			return true
		}
	}

	return false
}

// modelDeploymentEngineTransport 返回引擎监听器使用的协议方案，以及 operator 是否可以对该监听器设置探针门控。
//
// 只有两个参数会将监听器开启为 TLS，其余 --ssl-* 系列参数均不会。两种受支持的引擎都会将所有 ssl 参数原样传给 uvicorn 而自身不做判断，而 uvicorn 仅在配置了 `ssl_keyfile` 或 `ssl_certfile` 时才会启用 TLS。单独传入 CA 证书包、密码套件列表、密钥口令或客户端证书模式时，服务仍为普通 HTTP——因此若按整个参数族前缀判断，就会为明文监听器发布 https:// 地址，并撤回本可正常工作的门控。各引擎还会按各自不同的表达式打印 "SSL enabled" 日志；这些日志不具决定性，此处也不遵循它们。
//
// 这两个结果彼此独立，在扫描中途就确定其中之一会干扰另一项判断。只有在开启 TLS 后，是否要求客户端证书才有意义，且两个参数可能以任意顺序传入，因此需先收集全部信息再给出结论。
//
// 若存在已渲染的命令行，该函数会读取该命令行，因此必须在填充监听地址之前调用。填充逻辑会跳过已出现该参数的任意位置（base argv、connector 参数、额外参数），因此如果检查只读取更窄的列表，就会沿用 connector 提供的端口，转而对 operator 自己的默认地址施加门控。
//
// 撤回门控的情况：--host 会改变引擎监听的地址，之后 operator 不再干预，因而无法得知探针能否到达该地址，也就不能设置门控；但此类监听器使用的协议方案仍可观测，因此仍会上报。--port 不会撤回门控：系统会回读 role 指定的端口并让门控跟随该端口，仅在引擎端口与 Service 目标端口不一致时，调用方才会撤回门控。--ssl-cert-reqs 可能要求客户端证书，而 kubelet 探针无法提供客户端证书——但仅在实际启用 TLS 时才适用，因为未启用 TLS 时 uvicorn 不会构建可供该选项生效的 TLS 上下文。
//
// 其余所有 TLS 场景均保留门控：地址仍由 operator 控制，变化的只是传输层，且 kubelet 执行 HTTPS 探针时不验证服务端证书，因此自签名证书与公共证书同样可以通过探测。
func modelDeploymentEngineTransport(engine string, command []string) (core.URIScheme, bool) {
	var tls, clientAuth, moved bool

	for i, arg := range command {
		switch ModelDeploymentListenArg(engine, arg) {
		case modelDeploymentEngineHostArg:
			moved = true
		case modelDeploymentEngineTLSCertArg, modelDeploymentEngineTLSKeyArg:
			tls = true
		case modelDeploymentEngineTLSClientAuthArg:
			// 采用赋值而非逻辑或，从而使重复出现的参数以其最后一次出现的值为准，这与引擎自身解析器保留的值一致。
			clientAuth = modelDeploymentDemandsClientCert(command, i)
		}
	}

	scheme := core.URISchemeHTTP
	if tls {
		scheme = core.URISchemeHTTPS
	}

	return scheme, !moved && (!tls || !clientAuth)
}

// modelDeploymentDemandsClientCert 判断索引 i 处的 --ssl-cert-reqs 是否要求 kubelet 无法提供的客户端证书。
//
// 除已知允许放行的值外一律视为需要客户端证书，这是针对该问题更稳妥的保守策略。撤回门控只是让副本失去此前原本没有的就绪信号；而如果为随后会拒绝探针的监听器加上门控，则会导致副本在正常服务真实客户端的同时，每个启动预算周期都被 kubelet 重启一次。前者只是少了一项改进，后者则会造成服务中断——因此无法解析的值、缺失的值以及枚举范围之外的数值都会统一视为拒绝放行。
//
// 该值按整数解析而非按文本比较，因为引擎自身的解析器也是如此处理，对它而言 "02" 等同于 CERT_REQUIRED。
func modelDeploymentDemandsClientCert(command []string, i int) bool {
	_, value, ok := strings.Cut(command[i], "=")
	if !ok {
		if i+1 >= len(command) {
			// 该参数位于命令行末尾，因此没有任何值可供读取。
			return true
		}

		value = command[i+1]
	}

	mode, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return true
	}

	return mode != modelDeploymentEngineTLSClientAuthNone &&
		mode != modelDeploymentEngineTLSClientAuthOptional
}

// ModelDeploymentRoleArgs 返回副本命令行中由 role 贡献的部分，这也是唯一可能包含监听或 TLS 参数的部分。
//
// 该函数存在的目的是让仅持有 spec 的调用方（已发布的端点仅由 spec 推导）得出与已渲染命令行相同的结论。它同时也是并行度声明所解析的 token 流：引擎在两种层级下读取的正是这些 token，从而保证内部记录与生成配置绝不会来自不同的命令行。
//
// 该列表比渲染阶段扫描的范围更窄，两者之所以保持一致，完全依赖于一个本函数无法强制保证的不变量：引擎的 base argv 与 connector 渲染出的参数中不包含任何监听或 TLS 参数。一旦 connector 引入了此类参数，就会出现已发布地址仍显示 http://、副本却挂载了 HTTPS 探针的情况。因此，该不变量会直接针对 inject.Render 的实际输出进行断言，而不是在这里仅作口头保证。
//
// 接管命令行的 role 只贡献其完整的 argv，而不附加其他内容：extra arguments 不会追加到用户已替换的命令之后，因此在这里读取它们只会描述一条并未实际运行的命令行。
func ModelDeploymentRoleArgs(role *workercore.ModelDeploymentRole) []string {
	if len(role.Command) > 0 {
		return role.Command
	}

	return role.ExtraArgs
}

// modelDeploymentProbes 为副本主容器构建 startup、readiness、liveness 三个 HTTP 探针。
//
// 不可判定（如接管模式或要求客户端证书的 mTLS）或非 TCP 端口返回三个 nil。
// startup 失败阈值由角色声明的启动超时向上取整推导，避免覆盖大模型的长加载预算。
func modelDeploymentProbes(
	servingPort core.ContainerPort, scheme core.URIScheme, path string, gradable, probesEncoder bool,
	startupTimeoutSeconds *int32,
) (startup, readiness, liveness *core.Probe) {
	if !gradable {
		return nil, nil, nil
	}

	if servingPort.Protocol != core.ProtocolTCP {
		return nil, nil, nil
	}

	port := intstr.FromInt32(servingPort.ContainerPort)
	timeout := modelDeploymentProbeTimeoutSeconds
	readinessThreshold := modelDeploymentReadinessFailureThreshold
	livenessThreshold := modelDeploymentLivenessFailureThreshold
	if probesEncoder {
		timeout = modelDeploymentEncoderProbeTimeoutSeconds
		readinessThreshold = modelDeploymentEncoderReadinessFailureThreshold
		livenessThreshold = modelDeploymentEncoderLivenessFailureThreshold
	}
	gate := func(failureThreshold int32) *core.Probe {
		return &core.Probe{
			ProbeHandler: core.ProbeHandler{
				HTTPGet: &core.HTTPGetAction{
					Path:   path,
					Port:   port,
					Scheme: scheme,
				},
			},
			PeriodSeconds:    modelDeploymentProbePeriodSeconds,
			TimeoutSeconds:   timeout,
			FailureThreshold: failureThreshold,
		}
	}

	return gate(modelDeploymentStartupProbeFailureThreshold(startupTimeoutSeconds)),
		gate(readinessThreshold),
		gate(livenessThreshold)
}

// modelDeploymentStartupProbeFailureThreshold 根据角色声明的启动预算计算 startup 探针允许的失败周期数。
//
// 预算为空或非正数时回退到默认阈值，保证该字段引入前的存量对象继续获得原有门控，不会立刻重启。
func modelDeploymentStartupProbeFailureThreshold(startupTimeoutSeconds *int32) int32 {
	if startupTimeoutSeconds == nil || *startupTimeoutSeconds <= 0 {
		return modelDeploymentStartupFailureThreshold
	}

	periods := (*startupTimeoutSeconds + modelDeploymentProbePeriodSeconds - 1) / modelDeploymentProbePeriodSeconds

	return max(periods, 1)
}

// modelDeploymentProbesEncoderStage 报告该角色的探针是否评估一个依赖按地址访问编码器阶段的 prefiller。
// 其他角色以及不含编码器的部署维持默认探针参数，不改变其渲染结果。
func modelDeploymentProbesEncoderStage(
	md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole, takeOver bool,
) bool {
	return !takeOver &&
		modelDeploymentDeclaresEncoder(md) &&
		modelDeploymentEPDSplitsEncoderByAddress(md.Spec.Engine.Name) &&
		ModelDeploymentEffectiveRoleKind(role) == workercore.ModelDeploymentRoleKindPrefill
}

// mergeModelDeploymentEnv 将 operator 的环境变量与 role 的环境变量合并为一个列表。
//
// 结果顺序按权威优先级从高到低排列，以便查看已渲染的 Pod 时无需查表即可回答“是谁设置了该变量”：依次为 operator 拥有（owned）的变量、operator 仅设默认值（defaulted）的变量、以及用户追加的变量。owned 条目在准入阶段会被拒绝，因此此处跳过它们只是双重保险——但若渲染器放行了这些条目，失败就会推迟到引擎侧才暴露，远离真正引发问题的字段。
//
// 接管命令行的 role 不会获得任何 operator 环境变量。其 argv 从不引用这些变量指向的文件，设置它们只会描述一套没有任何进程读取的配置。
func mergeModelDeploymentEnv(
	engine string, role *workercore.ModelDeploymentRole,
	connector ModelDeploymentConnectorRender, artifact *ModelDeploymentArtifactRender, takeOver bool,
) []core.EnvVar {
	if takeOver {
		return mergeModelDeploymentUserEnv(nil, engine, role.Env, false)
	}

	var artifactEnv, artifactDefaulted []core.EnvVar
	if artifact != nil {
		artifactEnv, artifactDefaulted = artifact.env(takeOver, engine)
	}

	// 缓存根目录属于默认值而非 owned，分层机制正是为此而设。将缓存挂载到其他位置，或运行对变量拼写不同的引擎镜像时，用户必须能够自行指定并保留其配置：operator 仅保证默认路径生效，而这由上方挂载的卷来保证。
	//
	// 引擎的其他启动缓存也一并设为默认值，原因相同且还有另一点考量：这些缓存默认写入容器的可写层，而 Pod 的 ephemeral-storage 限制会计入该层占用，因此写往该处的缓存增长时会驱逐 Pod，而落在挂载缓存卷上的相同字节数则完全不计入该限制。
	defaulted := slices.Concat(
		connector.DefaultedEnv, artifactDefaulted,
		modelDeploymentCompileCacheDefaultedEnv(engine),
		modelDeploymentRuntimeCacheDefaultedEnv(engine),
	)

	env := make([]core.EnvVar, 0,
		len(connector.Env)+len(artifactEnv)+len(defaulted)+len(role.Env))
	env = append(env, connector.Env...)
	env = append(env, artifactEnv...)
	for _, e := range defaulted {
		if modelDeploymentUserSetsEnv(role.Env, e.Name) {
			continue
		}
		env = append(env, e)
	}

	return mergeModelDeploymentUserEnv(env, engine, role.Env, artifact != nil)
}

// mergeModelDeploymentUserEnv 将用户环境变量追加到 operator 已渲染的结果之后，允许高优先级条目按名称覆盖低优先级条目，但绝不允许替换 operator 拥有的变量。存在 artifact 时，artifact 拥有的变量名同样归 operator 所有；准入阶段会拒绝用户覆盖这些变量，在此丢弃它们可防止准入规则生效前已存储的对象覆盖 artifact 的配置。
func mergeModelDeploymentUserEnv(
	env []core.EnvVar, engine string, userEnv []workercore.ModelDeploymentEnvVar, artifact bool,
) []core.EnvVar {
	for i := range userEnv {
		if ModelDeploymentOwnsEnv(engine, userEnv[i].Name) ||
			(artifact && ModelDeploymentArtifactOwnsEnv(engine, userEnv[i].Name)) {
			continue
		}
		replaced := false
		for j := range env {
			if env[j].Name != userEnv[i].Name {
				continue
			}
			env[j].Value = userEnv[i].Value
			replaced = true

			break
		}
		if !replaced {
			env = append(env, core.EnvVar{Name: userEnv[i].Name, Value: userEnv[i].Value})
		}
	}

	return env
}

// modelDeploymentUserSetsEnv 判断用户是否提供了指定名称的环境变量。
func modelDeploymentUserSetsEnv(userEnv []workercore.ModelDeploymentEnvVar, name string) bool {
	for i := range userEnv {
		if userEnv[i].Name == name {
			return true
		}
	}

	return false
}

// modelDeploymentDefaultedArgs 将连接器的默认参数组展开为扁平列表，并跳过角色自身参数中已声明的标志。
func modelDeploymentDefaultedArgs(groups [][]string, userArgs []string) []string {
	var out []string
	for _, group := range groups {
		if slices.ContainsFunc(userArgs, func(arg string) bool {
			return ModelDeploymentArgName(arg) == group[0]
		}) {
			continue
		}
		out = append(out, group...)
	}

	return out
}

// modelDeploymentRoleNeedsEngramCapabilities 报告角色是否配置了需要 IPC_LOCK 与 SYS_RESOURCE 锁定
// 宿主机 DRAM 的 Engram 查找表。
func modelDeploymentRoleNeedsEngramCapabilities(
	engine string, role *workercore.ModelDeploymentRole, command []string,
) bool {
	if role == nil {
		return false
	}

	switch engine {
	case workercore.ModelDeploymentEngineVLLM:
		return modelDeploymentVLLMNeedsEngramCapabilities(command)
	case workercore.ModelDeploymentEngineSGLang:
		return modelDeploymentSGLangNeedsEngramCapabilities(role.Env)
	default:
		return false
	}
}

// modelDeploymentVLLMNeedsEngramCapabilities 扫描 vLLM 参数流中的 --engram-config 及其点号字段覆盖，
// 报告宿主机 CPU offload 是否处于开启状态。
func modelDeploymentVLLMNeedsEngramCapabilities(args []string) bool {
	seen := false
	cpuOffload := true

	for i := 0; i < len(args); i++ {
		if args[i] == "--" {
			break
		}
		if !strings.HasPrefix(args[i], "--") {
			continue
		}

		rawKey, val, hasVal := strings.Cut(args[i], "=")
		normKey := modelDeploymentVLLMFlagKey(rawKey)
		head, tail, dotted := strings.Cut(normKey, ".")
		if head != modelDeploymentVLLMEngramConfigArg {
			continue
		}

		if !hasVal && i+1 < len(args) && modelDeploymentConsumableValue(args[i+1]) {
			val = args[i+1]
			hasVal = true
			i++
		}

		seen = true
		if !dotted {
			cpuOffload = modelDeploymentVLLMEngramJSONCPUOffload(val)
			continue
		}

		if strings.ReplaceAll(tail, "-", "_") == "cpu_offload" {
			if !hasVal {
				cpuOffload = true
			} else {
				cpuOffload = !modelDeploymentFalsyString(val)
			}
		}
	}

	return seen && cpuOffload
}

// modelDeploymentVLLMEngramJSONCPUOffload 从 --engram-config 的 JSON 载荷中读取 cpu_offload。
// 字段缺失或载荷无法解析为 JSON 对象时默认返回 true，与 vLLM 的 EngramConfig.cpu_offload 默认值一致。
func modelDeploymentVLLMEngramJSONCPUOffload(raw string) bool {
	var cfg map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(raw)), &cfg); err != nil || cfg == nil {
		return true
	}

	val, exists := cfg["cpu_offload"]
	if !exists {
		val, exists = cfg["cpu-offload"]
	}
	if !exists {
		return true
	}

	switch v := val.(type) {
	case bool:
		return v
	case float64:
		return v != 0
	case string:
		return !modelDeploymentFalsyString(v)
	default:
		return true
	}
}

// modelDeploymentSGLangNeedsEngramCapabilities 报告 SGLang 的 Engram 宿主机表 offload 环境变量是否开启。
func modelDeploymentSGLangNeedsEngramCapabilities(env []workercore.ModelDeploymentEnvVar) bool {
	val, ok := modelDeploymentEnvValue(env, modelDeploymentSGLangEngramHostTableEnv)
	if !ok {
		return false
	}

	s := strings.ToLower(strings.TrimSpace(val))
	if b, err := strconv.ParseBool(s); err == nil {
		return b
	}

	switch s {
	case "yes", "y", "on":
		return true
	default:
		return false
	}
}

func modelDeploymentFalsyString(raw string) bool {
	s := strings.ToLower(strings.TrimSpace(raw))
	if b, err := strconv.ParseBool(s); err == nil {
		return !b
	}

	switch s {
	case "no", "n", "off":
		return true
	default:
		return false
	}
}

// deriveModelDeploymentResources 根据角色声明的加速器需求与 InstanceType 单位规格，推导完整的
// InstanceResources，以便复用第 7–11 章的统一容器资源生成函数 getResourceRequirements。
func deriveModelDeploymentResources(
	role *workercore.ModelDeploymentRole,
	instType *worker.InstanceType,
) (*workercore.InstanceResources, error) {
	ress := new(workercore.InstanceResources)
	if rr := role.Resources; rr != nil {
		ress.Accelerator = rr.Accelerator
		ress.AcceleratorSlicedMemoryPercentage = rr.AcceleratorSlicedMemoryPercentage
		ress.AcceleratorSlicedCoresPercentage = rr.AcceleratorSlicedCoresPercentage
		ress.AcceleratorPartitionedProfile = rr.AcceleratorPartitionedProfile
	}

	if err := sizeModelDeploymentHostResources(ress, instType); err != nil {
		return nil, err
	}

	// 与 Instance webhook 一致设置默认本地存储：15Gi，且绝不超过 InstanceType 所提供的上限。
	def := resource.NewQuantity(15<<30, resource.BinarySI) // 15Gi
	if instType.Spec.LocalStorage != "" {
		if maxStg, err := resource.ParseQuantity(instType.Spec.LocalStorage); err == nil && def.Cmp(maxStg) > 0 {
			def = &maxStg
		}
	}
	ress.LocalStorage = *def

	return ress, nil
}

// sizeModelDeploymentHostResources 按实例类型的单位规格与角色所占卡数或显存比例 计算 宿主机 CPU 与内存。
func sizeModelDeploymentHostResources(
	ress *workercore.InstanceResources, instType *worker.InstanceType,
) error {
	if !instType.Spec.Acceleratable {
		// 纯 CPU 实例类型按 1 份单位资源配比，与 Instance 路径处理未显式声明 CPU 的请求一致。
		cpu, ram, err := modelDeploymentUnitResources(instType, 1)
		if err != nil {
			return err
		}
		ress.CPU, ress.RAM = cpu, ram

		return nil
	}

	slicing := ress.AcceleratorSlicedMemoryPercentage != 0 || ress.AcceleratorSlicedCoresPercentage != 0
	partitioning := ress.AcceleratorPartitionedProfile != ""
	if (slicing || partitioning) && !instType.Status.Detail.AcceleratorReady() {
		// 所需卡份额尚无法读取；等待加速详情计算完成后可重试，绝不静默回退成整卡。
		return fmt.Errorf("instance type %s is not ready yet (accelerator detail not computed); retry", instType.Name)
	}

	partitionPct, sizeable := PartitionProfileMemoryPercent((*workercore.InstanceType)(instType),
		ress.AcceleratorPartitionedProfile)
	if !sizeable {
		return fmt.Errorf("instance type %s is not ready yet (partition profile %q cannot be sized "+
			"from the observed accelerator detail); retry", instType.Name, ress.AcceleratorPartitionedProfile)
	}

	switch {
	case partitionPct > 0:
		// 硬件分区占单卡的一定显存比例，宿主机资源按同一显存比例缩放，与同等大小的逻辑切分保持一致。
		return modelDeploymentSizeByPercent(ress, instType, partitionPct)
	case instType.Status.Detail.IsLogicallySliceable() && slicing:
		// 只填显存比例或算力比例其中一项时，另一项自动对齐，使单填显存比例也能获得同等算力份额，反之亦然。
		switch {
		case ress.AcceleratorSlicedMemoryPercentage > 0 && ress.AcceleratorSlicedCoresPercentage == 0:
			ress.AcceleratorSlicedCoresPercentage = ress.AcceleratorSlicedMemoryPercentage
		case ress.AcceleratorSlicedCoresPercentage > 0 && ress.AcceleratorSlicedMemoryPercentage == 0:
			ress.AcceleratorSlicedMemoryPercentage = ress.AcceleratorSlicedCoresPercentage
		}
		// 显存比例决定实际预留的卡份额；算力比例仅限制核心利用率而不缩放宿主机资源。
		return modelDeploymentSizeByPercent(ress, instType, int64(ress.AcceleratorSlicedMemoryPercentage))
	}

	// 整卡请求按卡数线性放大单位资源；在加速池上申请 0 卡的小模型仍需宿主机运行，按 1 份单位资源配比。
	cards := int64(1)
	if ress.Accelerator != nil && ress.Accelerator.Value() > 0 {
		cards = ress.Accelerator.Value()
	}
	cpu, ram, err := modelDeploymentUnitResources(instType, cards)
	if err != nil {
		return err
	}
	ress.CPU, ress.RAM = cpu, ram

	return nil
}

// modelDeploymentSizeByPercent 按单卡单位资源的百分比计算宿主机资源。
func modelDeploymentSizeByPercent(
	ress *workercore.InstanceResources, instType *worker.InstanceType, pct int64,
) error {
	cpu, err := quantityx.StringPercentMultiply(instType.Spec.UnitResources.CPU, pct)
	if err != nil {
		return fmt.Errorf("invalid CPU unit of instance type %s: %w", instType.Name, err)
	}
	ram, err := quantityx.StringPercentMultiply(instType.Spec.UnitResources.RAM, pct)
	if err != nil {
		return fmt.Errorf("invalid RAM unit of instance type %s: %w", instType.Name, err)
	}
	ress.CPU, ress.RAM = cpu, ram

	return nil
}

// modelDeploymentUnitResources 将单卡对应的宿主机单位资源按整卡倍数放大。
func modelDeploymentUnitResources(
	instType *worker.InstanceType, multiplier int64,
) (cpu, ram resource.Quantity, err error) {
	cpu, err = quantityx.StringMultiply(instType.Spec.UnitResources.CPU, multiplier)
	if err != nil {
		return cpu, ram, fmt.Errorf("invalid CPU unit of instance type %s: %w", instType.Name, err)
	}
	ram, err = quantityx.StringMultiply(instType.Spec.UnitResources.RAM, multiplier)
	if err != nil {
		return cpu, ram, fmt.Errorf("invalid RAM unit of instance type %s: %w", instType.Name, err)
	}

	return cpu, ram, nil
}

type modelDeploymentInstanceTypeHashInput struct {
	Name          string                               `json:"name,omitempty"`
	Entrance      string                               `json:"entrance,omitempty"`
	Manufacturer  string                               `json:"manufacturer,omitempty"`
	Acceleratable bool                                 `json:"acceleratable,omitempty"`
	UnitResources workercore.InstanceTypeUnitResources `json:"unitResources"`
	Resources     workercore.InstanceResources         `json:"resources"`
}

type modelDeploymentRoleSiblingHashInput struct {
	Name string                             `json:"name"`
	Kind workercore.ModelDeploymentRoleKind `json:"kind,omitempty"`
}

// modelDeploymentBindingHash 对输入角色模板渲染的已解析外部绑定求指纹，不包含隐式默认项与集群级全局设置。
func modelDeploymentBindingHash(
	in ModelDeploymentRenderInput, ress workercore.InstanceResources,
) string {
	takeOver := len(in.Role.Command) > 0

	var instType modelDeploymentInstanceTypeHashInput
	if in.InstanceType != nil {
		instType = modelDeploymentInstanceTypeHashInput{
			Name:          in.InstanceType.Name,
			Entrance:      in.InstanceType.Status.Entrance,
			Manufacturer:  in.InstanceType.Status.Detail.Manufacturer,
			Acceleratable: in.InstanceType.Spec.Acceleratable,
			UnitResources: in.InstanceType.Spec.UnitResources,
			Resources:     ress,
		}
	}

	var artifact *ModelDeploymentArtifactRender
	if in.Artifact != nil && (!takeOver || in.Artifact.Delivery != workercore.ModelDeploymentModelDeliveryEngine) {
		a := *in.Artifact
		a.Endpoint = ""
		a.HTTPSProxy = ""
		a.NoProxy = ""
		artifact = &a
	}

	var connector *modelDeploymentConnectorBinding
	var rawConnector *ModelDeploymentConnectorRender
	var ecConnector string
	if !takeOver {
		if in.Connector.binding != nil {
			connector = in.Connector.binding
		} else if len(in.Connector.Args) > 0 || len(in.Connector.Env) > 0 ||
			len(in.Connector.PodAnnotations) > 0 || len(in.Connector.Ports) > 0 ||
			len(in.Connector.Volumes) > 0 || len(in.Connector.VolumeMounts) > 0 ||
			in.Connector.KVTransfer || in.Connector.RoutingSidecar {
			c := in.Connector
			c.DefaultedEnv = nil
			c.DefaultedArgs = nil
			rawConnector = &c
		}
		if in.Connector.RoutingSidecar {
			ecConnector = in.ECConnector
		}
	}

	subject := struct {
		InstanceType      modelDeploymentInstanceTypeHashInput `json:"instanceType"`
		RuntimeClassName  string                               `json:"runtimeClassName,omitempty"`
		InterfaceResource core.ResourceName                    `json:"interfaceResource,omitempty"`
		Artifact          *ModelDeploymentArtifactRender       `json:"artifact,omitempty"`
		Connector         *modelDeploymentConnectorBinding     `json:"connector,omitempty"`
		RawConnector      *ModelDeploymentConnectorRender      `json:"rawConnector,omitempty"`
		ECConnector       string                               `json:"ecConnector,omitempty"`
	}{
		InstanceType:      instType,
		RuntimeClassName:  in.RuntimeClassName,
		InterfaceResource: in.InterfaceResource,
		Artifact:          artifact,
		Connector:         connector,
		RawConnector:      rawConnector,
		ECConnector:       ecConnector,
	}

	encoded, err := json.Marshal(subject)
	if err != nil {
		return "unhashable-binding-" + err.Error()
	}

	return stringx.SumBytesBySHA256(encoded)
}

// modelDeploymentExplicitSpecHash 对用户显式配置的 ModelDeployment 和 role spec，以及解析后的外部绑定摘要和副本席位坐标生成指纹。
func modelDeploymentExplicitSpecHash(
	md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole,
	ordinal, member int, bindingHash string,
) string {
	canonicalRole := *role.DeepCopy()
	canonicalRole.Kind = ModelDeploymentEffectiveRoleKind(&canonicalRole)
	if canonicalRole.Image == "" {
		canonicalRole.Image = md.Spec.Engine.Image
	}
	if len(canonicalRole.Command) == 0 {
		canonicalRole.TerminationGracePeriodSeconds = ptr.To(
			modelDeploymentTerminationGracePeriodSeconds(&canonicalRole),
		)
	} else {
		canonicalRole.ExtraArgs = nil
	}
	canonicalRole.Replicas = 0
	canonicalRole.ElasticEP = nil

	siblings := make([]modelDeploymentRoleSiblingHashInput, 0, len(md.Spec.Roles))
	for i := range md.Spec.Roles {
		siblings = append(siblings, modelDeploymentRoleSiblingHashInput{
			Name: md.Spec.Roles[i].Name,
			Kind: ModelDeploymentEffectiveRoleKind(&md.Spec.Roles[i]),
		})
	}

	var kvCache *workercore.ModelDeploymentKVCache
	if md.Spec.KVCache != nil {
		copied := *md.Spec.KVCache
		copied.Connector = ""
		kvCache = &copied
	}

	subject := struct {
		Name        string                                `json:"name"`
		Namespace   string                                `json:"namespace"`
		Engine      string                                `json:"engine,omitempty"`
		Model       workercore.ModelDeploymentModel       `json:"model"`
		Router      *workercore.ModelDeploymentRouter     `json:"router,omitempty"`
		KVCache     *workercore.ModelDeploymentKVCache    `json:"kvCache,omitempty"`
		KVTransfer  *workercore.ModelDeploymentKVTransfer `json:"kvTransfer,omitempty"`
		Role        workercore.ModelDeploymentRole        `json:"role"`
		Siblings    []modelDeploymentRoleSiblingHashInput `json:"siblings,omitempty"`
		Ordinal     int                                   `json:"ordinal"`
		Member      int                                   `json:"member"`
		BindingHash string                                `json:"bindingHash,omitempty"`
	}{
		Name:        md.Name,
		Namespace:   md.Namespace,
		Engine:      md.Spec.Engine.Name,
		Model:       md.Spec.Model,
		Router:      md.Spec.Router,
		KVCache:     kvCache,
		KVTransfer:  md.Spec.KVTransfer,
		Role:        canonicalRole,
		Siblings:    siblings,
		Ordinal:     ordinal,
		Member:      member,
		BindingHash: bindingHash,
	}

	encoded, err := json.Marshal(subject)
	if err != nil {
		return "unhashable-" + err.Error()
	}

	return stringx.SumBytesBySHA256(encoded)
}

// modelDeploymentEffectiveMinRenderVersion 计算 deployment 运行中副本可接受的最低渲染版本，并将其限制在 [modelDeploymentMinSupportedRenderVersion, modelDeploymentRenderVersion] 区间内。
func modelDeploymentEffectiveMinRenderVersion(md *workercore.ModelDeployment) int {
	floor := modelDeploymentMinSupportedRenderVersion
	if md == nil {
		return floor
	}
	if raw := md.Annotations[modelDeploymentMinRenderVersionAnnotation]; raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v > floor {
			floor = min(v, modelDeploymentRenderVersion)
		}
	}

	return floor
}

// modelDeploymentPodRenderVersion 返回 Pod 上标注的整数渲染版本；当注解不存在或无效时返回 0。
func modelDeploymentPodRenderVersion(pod *core.Pod) int {
	if pod == nil {
		return 0
	}
	v, err := strconv.Atoi(pod.Annotations[modelDeploymentRenderVersionAnnotation])
	if err != nil || v <= 0 {
		return 0
	}

	return v
}

// modelDeploymentPodMatchesCurrency 判断运行中的 Pod 是否匹配期望的显式 spec 哈希，且满足 deployment 的最低渲染版本下限。
func modelDeploymentPodMatchesCurrency(actual, desired *core.Pod, minRenderVersion int) bool {
	if actual == nil || desired == nil {
		return false
	}

	return actual.Annotations[modelDeploymentPodSpecHashAnnotation] ==
		desired.Annotations[modelDeploymentPodSpecHashAnnotation] &&
		modelDeploymentPodRenderVersion(actual) >= minRenderVersion
}

// modelDeploymentReplicaRenderVersionUniform 判断一个副本的所有成员是否具有相同的渲染版本。
func modelDeploymentReplicaRenderVersionUniform(members []*core.Pod) bool {
	if len(members) <= 1 {
		return true
	}
	first := modelDeploymentPodRenderVersion(members[0])
	for _, member := range members[1:] {
		if modelDeploymentPodRenderVersion(member) != first {
			return false
		}
	}

	return true
}
