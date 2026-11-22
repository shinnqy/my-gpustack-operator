package v1alpha1

import (
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	gpustack "gpustack.ai/gpustack/api/v1"
)

// ModelDeployment 是 worker.gpustack.ai 的架构定义。
//
// 它表示连接到同一个 KV cache 缓存池的 N 个推理引擎角色副本，使各副本能够命中彼此
// 缓存的前缀，而无需各自重复计算相同的 prefill。
//
// 它直接渲染生成 Pod。由于准入调度链以 Pod 为核心，直接渲染 Pod 即可复用所有
// 现有准入门槛，无需引入新的集成点。
//
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:crd-gen:resource:scope="Namespaced",categories=["gpustack"],shortName=["md"],subResources=["status"]
// +k8s:crd-gen:printcolumn:name="Engine",type="string",jsonPath=".spec.engine.name"
// +k8s:crd-gen:printcolumn:name="Roles",type="string",jsonPath=".status.roleSummary"
// +k8s:crd-gen:printcolumn:name="Phase",type="string",jsonPath=".status.phase"
// +k8s:crd-gen:printcolumn:name="Endpoint",type="string",jsonPath=".status.endpoint"
type ModelDeployment struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Spec   ModelDeploymentSpec   `json:"spec" protobuf:"bytes,2,name=spec"`
	Status ModelDeploymentStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

var _ runtime.Object = (*ModelDeployment)(nil)

// ModelDeploymentSpec 定义 ModelDeployment 的期望状态。
type ModelDeploymentSpec struct {
	// Model 指定引擎提供服务的模型。
	//
	// +required
	Model ModelDeploymentModel `json:"model" protobuf:"bytes,1,name=model"`

	// Engine 是该部署运行的推理引擎，决定了 operator 接管哪些参数键以及传输配置通过哪种载体
	// 传入。参数所有权按 (engine, key) 界定：某一引擎接管的键，在另一引擎上只是普通用户参数。
	//
	// 它并不决定 connector，connector 改为跟随角色的硬件：connector 是加速器后端的属性，
	// 因此运行该引擎的 Ascend 池和 NVIDIA 池会使用不同的 connector。
	//
	// +required
	Engine ModelDeploymentEngine `json:"engine" protobuf:"bytes,2,name=engine"`

	// KVCache 可选地将该部署挂载到共享 KV cache 池。未设置该字段的托管 vLLM
	// prefill/decode 部署仍会使用其 router 的点对点 connector，而不会渲染共享存储
	// connector 或其客户端配置。
	//
	// +optional
	KVCache *ModelDeploymentKVCache `json:"kvCache,omitempty" protobuf:"bytes,3,opt,name=kvCache"`

	// Roles 是该部署运行的引擎角色列表。单角色部署指定一个，prefill/decode 部署指定多个，
	// 这正是该字段从首个版本起就设计为列表的原因。
	//
	// 数量上限位于 validating webhook 而非此 schema 中：它跟随 Kueue 对
	// Workload.spec.podSets 的上限，这样拒绝请求时可以明确指出受限于谁，且同步调整该数值
	// 只需修改 webhook，而无需改动必须兼容所有已存储对象的 schema。此处不复述具体数值，因为
	// 实际生效的上限取决于集群当前运行的 Kueue 版本。
	//
	// +required
	// +k8s:validation:minItems=1
	// +listType=map
	// +listMapKey=name
	Roles []ModelDeploymentRole `json:"roles" protobuf:"bytes,4,rep,name=roles"`

	// Router 可选地在各角色前方部署一个请求路由器。
	//
	// 它是东西向流量管理组件，而非 prefill/decode 配对器，这一区别决定了其后方哪些部署形态是
	// 合法的。多个普通服务端实例就是其中之一：基于缓存视图进行评分的路由器能够以 Service 无法
	// 实现的方式在等价实例间择优，因此“不存在配对”并不是拒绝该配置的理由。多个 prefiller 配
	// 多个 decoder 同样合法。若规则仅准入一个 prefiller 和一个 decoder，那描述的是配对器，
	// 而非此字段。
	//
	// 在 Ascend 硬件上，prefill/decode 传输链路仅在单一路由器后方渲染：即 "llm-d-router"，
	// 其 decode proxy 会中继 Ascend connector 所等待的逐请求握手。"vllm-router" 使用的握手
	// 协议词汇会被该 connector 拒绝，因此在其后方的 Ascend 配对会渲染为两个相互独立、之间没有
	// 传输链路的完整引擎；而 "sglang-gateway" 在上述逻辑生效前就会被拒绝置于该引擎前方。无传输
	// 链路的形态是静默的：部署会进入 Ready 状态，请求也能正常响应，但两个角色之间永远不会交换任何
	// 块——部署能够正常应答恰恰使缺失的传输链路难以被察觉。
	//
	// 省略该字段表示不使用路由器，这也是一种受支持的正常形态而非故障配置：无论是否配置路由器，
	// 各个角色都可通过各自的 Service 独立寻址，因此在该字段出现之前编写的部署配置仍能按原有
	// 方式正常提供服务。
	//
	// +optional
	Router *ModelDeploymentRouter `json:"router,omitempty" protobuf:"bytes,5,opt,name=router"`

	// KVTransfer 用于调优托管 prefill/decode 配对中引擎到引擎的 KV 传输链路。
	//
	// 该字段与上方的 KVCache 是两个正交的维度，而非互斥的二选一分支，两者可同时设置。启用该
	// 传输链路的条件——即所有已准入的路由器与引擎组合（在 Ascend 硬件上仅限 "llm-d-router"；
	// 上方 Router 字段说明了每种组合以及静默无链路形态的表现）且角色类型为 prefill 或
	// decode——完全不读取 spec.kvCache；当两者同时设置时，它们会被合成为同一个 connector 和
	// 同一个 --kv-transfer-config：部署可以同时共享缓存池中的块，并将块直接从 prefill 传递
	// 给 decode。
	//
	// 该字段涵盖的链路绝不经过存储层，这也是其配置不来自 KVCacheBackend 的原因：那里的
	// spec.transport 定义的是存储成员运行的数据面，而这里是引擎到引擎的数据面，两个平面分开
	// 声明。即使完全不挂载任何池，部署也可以渲染该传输链路，这也是该字段不能放在 KVCache 下的
	// 另一个原因。
	//
	// +optional
	KVTransfer *ModelDeploymentKVTransfer `json:"kvTransfer,omitempty" protobuf:"bytes,6,opt,name=kvTransfer"`
}

// ModelDeployment 可运行的引擎常量，即 ModelDeploymentEngine.Name 枚举的取值。
//
// 这些常量声明在以 schema 限定取值集合的字段旁，便于阅读其中任一处时对照另一处。这里刻意
// 没有 "vllm-ascend"：`vllm_ascend` 是加速器后端为 CANN 时 runner 安装的 Python 包，而
// 非供用户选择的引擎；若在此命名它，会让 connector 看起来像是引擎的属性，但事实并非如此。
const (
	ModelDeploymentEngineVLLM   = "vLLM"
	ModelDeploymentEngineSGLang = "SGLang"
)

// ModelDeploymentModel 指定引擎提供服务的模型，并可选指定所加载的模型权重。
//
// 它本身不置备任何资源，这一点始终未变：ArtifactRef 引用由 ModelArtifact 置备的权重，
// 方式如同 spec.kvCache.poolRef 引用由 Binding 授权的池。该类型一贯的禁令依然有效：来源、
// URI、凭据或下载策略绝不会进入此对象，因为在此处引入权重置备配置块会成为迈向通用推理服务 CR
// 的第一步——而这正是本设计刻意避免的——同时还会使每个部署都成为凭据持有者。未设置
// ArtifactRef 时，权重仍与以往一样通过角色的附加卷或引擎自身的 hub 客户端获取。
type ModelDeploymentModel struct {
	// Name 是引擎对外提供服务的模型标识符，例如 "Qwen/Qwen2.5-72B-Instruct"。
	//
	// 无论是否设置 ArtifactRef，它都始终作为服务模型名称（served name）：路由器、路由器的
	// tokenizer 调用以及指标中的 model 标签均按此名称匹配，因此托管角色自身的
	// --served-model-name 必须与之相等，并由准入阶段强制校验。
	//
	// +required
	// +k8s:validation:minLength=1
	// +k8s:validation:maxLength=253
	Name string `json:"name" protobuf:"bytes,1,name=name"`

	// ArtifactRef 指定当前 Namespace 中持有权重的 ModelArtifact。其类型为
	// LocalObjectReference，从而在结构上无法表示跨 Namespace 引用，而无需仅靠校验拒绝。
	//
	//   - 它与该对象的其余字段一同冻结：部署所服务的权重是该部署自身身份的一部分。更换其他权重
	//     意味着创建一个新的部署，这也能防止 prefill/decode 配对在两套不同权重之间传递 KV。
	//   - 尚不存在或尚未解析完成的 artifact 仍会被准入：部署会在 status 中等待而不创建 Pod，
	//     因此 GitOps 工具无需严格编排两个对象的创建顺序。
	//   - 设置该字段后，每个托管角色的引擎都会从固定的本地路径（claim 模式）或锁定到已解析
	//     commit 的仓库（hub 模式）获取权重；若同时配置了 spec.kvCache，还会在存储键前缀中
	//     包含权重标识，使不同权重之间绝不共享 KV 块。
	//
	// +optional
	ArtifactRef *core.LocalObjectReference `json:"artifactRef,omitempty" protobuf:"bytes,2,opt,name=artifactRef"`
}

// ModelDeploymentEngine 定义部署运行的引擎及其使用的容器镜像。
//
// 镜像是部署级的默认值而非按角色分别指定的字段，因为同一部署的各个角色属于同一引擎构建，通常
// 共用同一镜像。需要不同镜像的角色可在其自身的 spec 中指定，从而覆盖此字段。
type ModelDeploymentEngine struct {
	// Name 选择要运行的引擎。
	//
	// 它是部署的身份标识，创建后即被冻结；而 Image 决定运行哪个构建版本，保持可编辑——此处
	// 明确说明这一点，以免读者误以为同一对象内的这两个字段会一并被冻结。
	//
	// +required
	// +k8s:validation:enum=["vLLM","SGLang"]
	Name string `json:"name" protobuf:"bytes,1,name=name"`

	// Image 是该部署中所有角色默认运行的容器镜像。
	//
	//   - 它是整个部署级别的默认值：指定了自有镜像的角色会改用其自身指定的镜像，而未指定的角色
	//     则回退到此字段。
	//   - operator 不负责构建镜像，因此该镜像标签必须存在于集群可拉取的镜像仓库中。
	//   - 最小长度 1 用于严格兜底必填约束。若无此限制，此处的必填字符串只要键存在即视为满足；
	//     此外该对象会经 mutating webhook 重新序列化：Engine 是值类型且该字段未设置
	//     omitempty，因此从请求中提取的未指定镜像的 patch 仍会携带一个空字符串。
	//
	// +required
	// +k8s:validation:minLength=1
	// +k8s:validation:maxLength=512
	Image string `json:"image" protobuf:"bytes,2,opt,name=image"`
}

// ModelDeploymentKVCache 将部署挂载到一个 KV cache 池。
//
// 复用域（reuse domain）不在此处声明，这是一项安全属性。存储层的租户就是复用域，因此每个
// 不同的域都是一个拥有独立配额账本的租户：若允许工作负载任意命名域，它就能在其 Namespace
// 中创建无限个租户，从而彻底绕过 Namespace 配额上限。因此，域的命名权归属于由管理员维护的
// KVCachePoolBinding。
type ModelDeploymentKVCache struct {
	// PoolRef 指定当前 Namespace 中的 KVCachePoolBinding。该 Binding 即为授权边界：管理员
	// 在某个 Namespace 中创建它，即授予该 Namespace 访问对应池的权限。其类型采用
	// LocalObjectReference 而非带命名空间的引用，从而使跨 Namespace 引用、直接指定集群作用域的
	// 池或填写裸端点 URL 在结构上无法表示，而不仅仅是被校验拒绝。
	//
	// +required
	PoolRef core.LocalObjectReference `json:"poolRef" protobuf:"bytes,1,name=poolRef"`

	// Connector 指定该部署所配置的 connector 实现名称。该值是部署自身携带的身份标识，而非
	// operator 推导出的设置："Mooncake" 仅表明这是哪一种 connector，目前没有任何逻辑读取该
	// 字段来生成配置。这里没有 "none" 选项——如需跳过合成配置，可通过完整替换启动命令来实现，
	// 这同时会将该角色标记为非托管并将 CacheAttached 置为 Unknown。
	//
	// KV 传输统一收敛于 Mooncake，这也是该枚举目前仅有一个值的原因。Mooncake 是支持异构
	// prefill 与 decode 的实现，而这也正是本 API 旨在表达的部署形态。NIXL 与 ROCm NIXL 仍
	// 保持可用路径，但本代码库未实际运行过它们，该字段的存在也不代表保证它们能够正常工作。
	//
	// 该预留仅存在于 schema 中，别无其他实现。没有任何代码会读取此字段：binding 解析传递的是
	// 域、端点和协议；connector 合成接收的是引擎、角色类型、硬件厂商与该连接信息；渲染器则按
	// 引擎进行分发。因此，这一区分字段是为未来引入第二种实现时的 API 而预留的，其对应的分发
	// 接缝目前尚不存在。
	//
	// 扩展该枚举涉及四项工作，而非一项：pkg/worker/kvcache 下的一个子包、此处的一个枚举项、
	// 一个渲染器，以及将该值一路传递到分发点的接线逻辑。最后一项并不包含在当前预留之中，这也是
	// 引入第二种实现需要一套完整开发工作而非仅仅添加一个常量的原因。
	//
	// 扩展后的枚举仅对新部署生效。该字段定义部署的身份属性，因此创建后即被冻结：现有部署只能
	// 通过重建来迁移到第二种 connector，而不能直接修改。此处明确说明这一点，以免“扩展枚举”被
	// 误解为适用于运行中部署的平滑迁移路径。
	//
	// +k8s:validation:default="Mooncake"
	// +k8s:validation:enum=["Mooncake"]
	Connector string `json:"connector,omitempty" protobuf:"bytes,2,opt,name=connector"`
}

// ModelDeploymentKVTransfer 承载 prefill 角色与 decode 角色之间点对点 KV 传输链路的
// 配置。它与共享存储相互组合而非互斥：同一部署可同时配置两条链路，合成后的 connector 会
// 将两者一并承载。
type ModelDeploymentKVTransfer struct {
	// Protocol 指定直连链路两端所使用的传输协议。它复用 KVCacheBackend 的传输取值及其
	// Mooncake 映射，但不包含 MUSA 和 MACA：这两者属于节点内 IPC 传输，而 prefill 与 decode
	// 可能运行在不同节点上。
	//
	//   - 刻意设计为部署级字段。协议是链路本身的属性，而非单端属性，若按角色分别配置只可能产生
	//     矛盾——同一条连接的两端指定了不同的值，从而导致传输阶段而非准入阶段才暴露失败。
	//   - 取值为声明式而非自动发现。枚举列出了受支持的传输协议族；引擎镜像仍须包含匹配的
	//     Mooncake 构建版本。CANN 会渲染为 "ascend"，ROCM 会渲染为 "hip"，采用与
	//     KVCacheBackend 成员相同的映射。
	//   - Auto 选择 TCP 并渲染为 "tcp"；它不会探测 worker 的网络结构。
	//   - 未设置时渲染为 "tcp"，即所有 Mooncake 构建版本都内置的传输协议。默认值留在渲染器中
	//     而非此 schema 中，因此存储对象忠实保留用户的原始配置。
	//   - TCP 为强制生效而非仅作请求，因为传输引擎会根据宿主机硬件自行选择传输方式，而不会读取
	//     所请求的协议。在 vLLM 上，该链路还会注入 MC_FORCE_TCP=1，且角色自身设置的值优先级
	//     更高。在 SGLang 上，该值映射到引擎的传输后端：TCP 渲染为 "mooncake_tcp"，其他值
	//     渲染为 "mooncake"，而不会透传该字段原值。当部署的共享存储运行非 tcp 传输协议时，以上
	//     两种固定设置均不会渲染，因为它们作用于整个进程，会导致存储客户端失去其底层网络；此时
	//     直连链路保留引擎自身的默认选择。
	//   - 仅在点对点链路上读取该字段：即所有已准入的路由器与引擎组合中的 prefill/decode 角色。
	//     在不渲染链路的情况下——例如未配置路由器，或 "vllm-router" 后方的 Ascend 配对——该值
	//     会被准入接收但不会渲染任何配置。"llm-d-router" 后方的 Ascend 配对会渲染该链路，但
	//     不会使用此字段的值：该引擎的传输链路硬编码了传输协议，因此声明的协议没有可写入的配置键。
	//     此处逐一列出每种静默忽略的情形，因为一个被接受却悄然无效的字段无异于无声违约。
	//   - 该字段可编辑，且修改会重启所有角色：其值会渲染进两端的启动参数中，因此变更会重建部署的
	//     每一个 Kueue pod group。当各角色分布在不同 InstanceType 上时，各个 pod group 会独立
	//     重建，在两端收敛之前，prefiller 与 decoder 之间会存在一段混合协议时间窗——这与修改
	//     引擎镜像时出现的时间窗相同。
	//
	// +optional
	// +k8s:validation:enum=["Auto","TCP","RDMA","EFA","CANN","ROCM"]
	Protocol string `json:"protocol,omitempty" protobuf:"bytes,1,opt,name=protocol"`
}

// ModelDeploymentRole 表示一个引擎角色及其副本集合。
//
// Replicas、InstanceType 和 Resources 均为结构化字段，且必须保持如此。它们是准入与调度的
// 输入——决定 Kueue PodSet 数量、flavor 选择以及队列核算的资源请求——因此若存在能够覆盖
// 其中任意一项的容器字段，就会导致准入可行性检查读取到与实际情况不符的账目。这也是下方容器
// 字段完全不携带任何资源请求的原因：加速器部分归属于 Resources，其余部分则由 InstanceType
// 推导，两者均不可在此处被覆盖。
//
// 编辑容器字段会滚动更新当前角色的副本，且仅影响当前角色——但有一个例外。prefill 或
// decode 角色声明的并行度——即 ExtraArgs 中的并行度参数以及 vLLM 的 VLLM_DP_SIZE 环境
// 变量——是唯一可能渲染进两个角色共同携带的配置文档中的容器字段：vLLM-Ascend 传输链路会
// 将相同的并行配置块写入两个角色的 Pod 中，因此在该链路上修改任一角色的并行度也会改写另一
// 角色的 Pod，从而触发配对双方的滚动更新。在没有共享文档渲染它们的场景下，并行度修改与其他
// 所有容器字段修改一样仅影响当前角色：每个副本都是一个独立的 Kueue pod group，因此它们会
// 逐个替换——每轮每个角色替换一个——而所有同级角色在此期间持续提供服务。修改 `replicas`
// 则完全不会触发滚动更新：它仅增减实例，所有保留下来的实例继续运行，保持准入时分配的加速器
// 以及持有的缓存。
//
// 角色集合在创建后即固定不变。准入阶段会拒绝新增、删除或重命名角色，也会拒绝修改已有角色的
// kind：name 和 kind 定义了角色“是什么”，而副本数、instanceType、resources 和启动命令定义
// 了它“当前如何运行”，可以与其他容器字段一样进行编辑。副本所在组依据部署名、角色名和序号
// 命名，因此修改一个角色的运行配置不会重命名同级角色的组。
//
// 非由本 operator 主动发起的实例离开不属于滚动更新。离开的副本会以新名称单独重建替换，而
// 同级副本继续提供服务——详见 docs/modules/model-deployment/deployment.md 中的
// "One group per replica" 与 "Rollout is a rolling replacement"。
type ModelDeploymentRole struct {
	// Name 标识该角色，同时也是该角色所转换成的 Kueue PodSet 的名称。
	//
	//   - 其格式采用 Kueue 自身的 PodSetReference 规范，在此强制校验是因为该名称会原样写入每个 Pod
	//     的 role-hash 注解中：Kueue 根据该注解将 pod group 内的 Pod 分组到各 PodSet 中，因此
	//     无法作为 PodSet 引用的名称会导致对应角色无法保留在分组结果中。
	//   - 唯一性由 Schema 保证。Roles 是以该字段为键的 list-map，因此 API server 会在任何
	//     Webhook 运行之前拒绝重复值；Webhook 保留相同规则仅作为该标记被移除时的兜底保障。
	//     若两个角色共用同一名称，它们会合并为单个 PodSet（数量为两者之和），从而产生静默合并而非报错。
	//
	// +required
	// +k8s:validation:minLength=1
	// +k8s:validation:maxLength=63
	// +k8s:validation:pattern="^[a-z0-9]([-a-z0-9]*[a-z0-9])?$"
	Name string `json:"name" protobuf:"bytes,1,name=name"`

	// Kind 是告知推理引擎该角色所属的类型。该枚举是封闭的，且不同于角色名称：Name 为自由格式，
	// 用于标识 PodSet，而 Kind 用于选择角色行为；若允许通过任意字符串表达语义，一次拼写错误就
	// 可能导致行为静默改变。仅当 Kind 为 Server 时，两个角色才允许共享相同类型并采用不同名称，
	// 因为多个 Server 是对等节点，而两个 Prefill 角色并非如此：任何消费这些角色的组件都无法表达
	// 第二个 Prefill 角色，因此声明两个 Prefill 会渲染出配置读取方无法触及的角色。该字段默认值为
	// Server（即分离部署出现之前已有部署形态），从而确保既有部署的渲染结果保持完全一致。
	//
	// +k8s:validation:default="Server"
	// +k8s:validation:enum=["Server","Prefill","Decode","Encode"]
	Kind ModelDeploymentRoleKind `json:"kind,omitempty" protobuf:"bytes,8,opt,name=kind,casttype=ModelDeploymentRoleKind"`

	// Replicas 指定该角色运行的独立推理服务实例数量。各实例彼此独立：每个实例单独启动、提供服务
	// 并被替换，相互之间不存在存在性依赖。
	//
	// 修改此数值会增减实例：调大时会在现有运行实例之外创建新实例；调小时则会移除部分实例。保留
	// 下来的实例不会重启，会继续不间断地提供服务并保留其持有的缓存。
	//
	// 该字段的上限是针对本 Operator（而非 Kubernetes）的限制。单次调谐会在写入任何对象之前先
	// 渲染该角色声明的全部实例，因此该数值会直接放大单次调谐的工作量；若放开至字段类型的完整
	// 取值范围，仅单个合法字段值就足以在 API server 限流创建请求之前耗尽 worker 资源。当前上限
	// 设定在任何实际服务部署都无法触及的水平。
	//
	// +k8s:validation:default=1
	// +k8s:validation:minimum=1
	// +k8s:validation:maximum=1024
	Replicas int32 `json:"replicas,omitempty" protobuf:"varint,2,opt,name=replicas"`

	// ReplicaSize 指定组成单个服务实例的 Pod 数量。同一实例中的 Pod 共担命运：一起启动、一起被
	// 替换，且不会单独提供服务——实例（而非 Pod）才是出现和消失的基本单元。
	//
	// 该字段可编辑。修改后会逐个替换该角色的副本：先删除一个旧副本及其 Workload，待该槽位释放后
	// 再创建新副本，且新副本重新通过准入后才允许删除下一个旧副本。其余未轮到的副本在被选中替换前
	// 保持原有已部署配置；此过程不保证持续不中断服务——单副本角色在替换期间会暂停服务。不触发
	// 替换的扩缩容请使用 replicas 字段。
	//
	// Elastic-EP 配置将该数值固定为 1；详见 ElasticEP。
	//
	// 当该值大于 1 时，同一实例内的多个 Pod 需要彼此的地址，因此多 Pod 实例会渲染出稳定名称，
	// 并向每个容器注入首个 Pod 的地址、当前实例大小以及各 Pod 自身的 rank。推理引擎如何利用
	// 这些信息（启用何种并行策略以及跨越多少个 rank）由部署作者自行决定。
	//
	// 此处 Go 标识符未命名为 Size，是因为 gogo protobuf 会在该类型上生成 Size() 方法，而 Go
	// 禁止字段与方法同名；对应的 API 字段名为 size。
	//
	// 其上限与 Replicas 的限制相同，且两者呈乘法关系：该值表示单个实例渲染出的 Pod 数量，因此
	// 单次调谐会在写入任何对象前先渲染 replicas 乘以该数值个 Pod。当前上限远高于加速器拓扑有
	// 意义的规模，同时远低于足以使单个合法字段值导致 worker 内存溢出的范围。
	//
	// +k8s:validation:default=1
	// +k8s:validation:minimum=1
	// +k8s:validation:maximum=64
	ReplicaSize int32 `json:"size,omitempty" protobuf:"varint,15,opt,name=size"`

	// InstanceType 是该角色 Pod 进行准入调度所针对的 InstanceType 名称，也是派生队列名称入口
	// 标签（queue-name entrance label）的依据。
	//
	// 该字段可编辑。将角色迁移到另一个 InstanceType 会执行同样的整组替换流程：先移除一个旧副本，
	// 再创建新副本，待新副本针对新类型发布的队列完成准入后，才允许下一个旧副本退出。既有 Pod
	// 在被删除前仍保持其准入时的类型。Elastic-EP 配置会在创建后冻结该字段。
	//
	// +required
	// +k8s:validation:minLength=1
	// +k8s:validation:maxLength=253
	InstanceType string `json:"instanceType" protobuf:"bytes,3,name=instanceType"`

	// Resources 定义该角色单个 Pod 对加速器的资源请求；它采用结构化字段的原因与 Replicas 和
	// InstanceType 相同：准入和调度逻辑需要读取该字段。
	//
	// 它仅包含资源请求中的加速器部分，因为只有这部分由工作负载自身决定。CPU、内存和临时存储均由
	// InstanceType 的单卡资源（UnitResources）乘以所请求的卡数推导得出，因此在这里根本无法
	// 表达——这比显式拒绝提供更强的约束，因为不存在的字段也无法被下方的容器字段覆盖。
	//
	// 仅凭 InstanceType 无法确定加速器请求量：其 UnitResources 仅定义单卡对应的资源规模，而
	// 单个 Pod 所需的卡数取决于所部署的模型，因此同一 InstanceType 上的两个部署通常会请求不同的
	// 卡数。
	//
	// 该字段可编辑。修改后会执行同样的整组替换流程，并在创建每个新副本时针对资源池对新请求进行
	// 准入。省略该字段时的行为与创建时一致：加速器请求会默认设为 1 张可加速类型的整卡。Elastic-EP
	// 配置会在创建时将该字段冻结为与 TP 绑定的整卡组。
	Resources *ModelDeploymentRoleResources `json:"resources,omitempty" protobuf:"bytes,4,opt,name=resources"`

	// Image 是要运行的容器镜像。通常留空即可，此时该角色使用部署配置的引擎镜像。需要运行私有
	// 构建版本、为该角色单独固定不同于引擎的镜像，或指定引擎镜像未包含的仓库地址时，可设置此字段。
	//
	// +optional
	// +k8s:validation:maxLength=512
	Image string `json:"image,omitempty" protobuf:"bytes,7,opt,name=image"`

	// ImagePullPolicy 是 Image 的镜像拉取策略。
	//
	// +optional
	ImagePullPolicy core.PullPolicy `json:"imagePullPolicy,omitempty" protobuf:"bytes,9,opt,name=imagePullPolicy"`

	// ImagePullSecrets 是用于拉取 Image 的 Secret 列表。
	//
	// +optional
	// +listType=atomic
	// +k8s:validation:maxItems=32
	ImagePullSecrets []core.LocalObjectReference `json:"imagePullSecrets,omitempty" protobuf:"bytes,10,rep,name=imagePullSecrets"`

	// Privileged 控制是否以特权模式运行容器。
	//
	// +optional
	Privileged bool `json:"privileged,omitempty" protobuf:"varint,11,opt,name=privileged"`

	// Ports 指定除推理引擎自身端口外需要额外暴露的容器端口。它们不会预留或选择传输引擎的
	// 运行时端口窗口。
	//
	// +optional
	// +patchMergeKey=port
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=port
	// +listMapKey=protocol
	Ports []ModelDeploymentPort `json:"ports,omitempty" patchStrategy:"merge" patchMergeKey:"port" protobuf:"bytes,12,rep,name=ports"`

	// AdditionalVolumes 指定除 Operator 自有卷之外需要额外挂载到容器中的卷。
	//
	// +optional
	// +listType=atomic
	AdditionalVolumes []ModelDeploymentAdditionalVolume `json:"additionalVolumes,omitempty" protobuf:"bytes,13,rep,name=additionalVolumes"` // nolint: lll

	// ShmSize 限制每个角色 Pod 中基于内存的 /dev/shm 卷大小，必须为正值。省略时默认渲染为
	// 16Gi。若 AdditionalVolumes 中显式指定了 /dev/shm 挂载，则以后者为准。已使用的共享
	// 内存会计入容器的内存限制，该配置并非内存预留。既有的弹性成员保持其原有挂载；新建和替换的
	// Pod 使用当前配置值。
	//
	// +optional
	ShmSize *resource.Quantity `json:"shmSize,omitempty" protobuf:"bytes,19,opt,name=shmSize"`

	// Command 会替换整个 argv，即接管模式（take-over）：用户完全掌控整条命令行，Operator
	// 不会生成任何引擎参数或客户端环境变量，该角色会被标记为 unmanaged，且 CacheAttached
	// 状态变为 Unknown。启动参数需直接合并写入 Command；这里刻意不提供 Args 字段，因为在
	// ExtraArgs 之外再引入第二个追加层级会导致优先级无法明确定义。
	//
	// 该字段支持任意方向的编辑：从托管模式切换为接管模式、从接管模式切回托管模式，以及在接管
	// 模式之间变更。每次编辑都会执行整组替换，已部署的 Pod 在被删除替换前会保持其创建时的模式，
	// 因此在滚动更新期间该角色可能同时存在两种模式的 Pod；缓存注入与状态计算会读取实际已部署
	// 的状态。Elastic-EP 配置会冻结此字段，由弹性渲染器全权控制 argv。
	//
	// +optional
	// +listType=atomic
	Command []string `json:"command,omitempty" protobuf:"bytes,14,rep,name=command"`

	// ExtraArgs 会追加在 Operator 生成的参数之后。若其中包含 Operator 管控的键名，请求会被
	// 拒绝而非合并：静默合并会导致同一个连接器参数出现两个值，且无法判断哪一个生效。
	//
	// 该字段命名为 ExtraArgs 而非 Args，因为 Args 容易被误解为完整 argv（这正是 Command 的
	// 语义）；这两个层级的区别在于 Operator 是否参与生成命令行内容。
	//
	// 设置 Command 时不会读取此列表：向角色作者已整体替换的 argv 中追加参数会侵入其完全掌控的
	// 命令行，因此在接管模式下完整命令行由 Command 独占，该字段不起任何作用。
	//
	// +listType=atomic
	ExtraArgs []string `json:"extraArgs,omitempty" protobuf:"bytes,5,rep,name=extraArgs"`

	// Env 采用相同的追加方式和拒绝规则。对于 Operator 仅提供默认值的环境变量，用户设置的值会
	// 覆盖默认值且不会被拒绝。
	//
	// 此处只保留单个列表而非额外叠加一层：原先的第二层级完全遵循相同的追加与冲突拒绝规则，
	// 嵌套结构反而暗示了并不存在的优先级关系。
	//
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	Env []ModelDeploymentEnvVar `json:"env,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,6,rep,name=env"`

	// Topology 可选地要求 Kueue 将每个副本的 Pod 组调度到同一指定层级的拓扑域内。该约束仅
	// 作用于当前角色自身的独立副本组，不要求其他角色或其他副本共享同一拓扑域。
	//
	// +optional
	Topology *ModelDeploymentRoleTopology `json:"topology,omitempty" protobuf:"bytes,16,name=topology"`

	// TerminationGracePeriodSeconds 是该角色退出副本从 Pod 被删除到被强制终止所获得的总宽限时间，
	// 会写入每个 Pod 的同名字段。未设置时渲染为 Kubernetes 默认值 30。
	//
	//   - 排空钩子（drain hook）的超时预算基于该值计算：钩子会等待引擎变为空闲，直至距该期限
	//     还剩 5s 时停止，并将最后 5s 留给引擎响应 SIGTERM 退出。因此调大该值会同时延长正在
	//     运行请求的等待时间与引擎的退出预留时间。
	//   - 更长的宽限期会占用加速器与配额。每次删除时，退出中的副本在进程结束前会持续占用这两者，
	//     而滚动更新在每个角色内会逐个等待退出的副本完成，因此该宽限期会在每次滚动更新中成倍叠加。
	//     空闲副本仍会在引擎退出后立即结束；宽限期是上限而非固定等待时长。
	//   - SGLang 的 prefill 或 decode 角色通常需要约 45：实测表明此类引擎在空闲状态下从触发删除
	//     到退出需耗时 26-28s，若上限为 30 则距强杀仅剩 2s。
	//   - 与所有会渲染到 Pod 的字段一样，修改该值会替换该角色的全部副本，而在此次滚动更新中退出
	//     的旧副本仍沿用其创建时设置的宽限期。
	//   - 设置了 Command 的接管模式角色会按原样将该值写入 Pod，且无论是否设置都不会注入排空钩子；
	//     未设置时其 Pod 保持 Kubernetes 默认值。
	//   - 下限用于在两端固定预留之间留出等待窗口：前 5s 引擎照常提供服务，后 5s 留给进程退出，
	//     设为 15 时中间剩余的 5s 刚好足以完成钩子退出所需的两次空闲状态读取。上限为 1 小时，远超
	//     单个副本服务的任何请求耗时；需要更长时间的请求应当由支持请求迁移的路由器处理，但目前
	//     支持的路由器均不具备该能力。
	//
	// +optional
	// +k8s:validation:minimum=15
	// +k8s:validation:maximum=3600
	TerminationGracePeriodSeconds *int64 `json:"terminationGracePeriodSeconds,omitempty" protobuf:"varint,17,opt,name=terminationGracePeriodSeconds"` // nolint: lll

	// ElasticEP 使该角色启用托管的 elastic-EP 配置：Operator 会为该部署渲染一个专用的逻辑
	// Ray 集群，包含仅使用 CPU 的控制面 head 节点，以及为每个 DP 引擎 rank 分配一个整卡 GPU
	// 的 TP 组。该字段是否出现即作为配置判别标志：未设置时，角色的渲染方式与该字段存在前完全一致；
	// 反复开关该配置则意味着不同的部署形态。该字段的存在性在创建时即被冻结。Width 支持扩容编辑；
	// 本版本准入控制拒绝缩容操作。
	//
	// 每个部署仅允许包含一个此类角色，以单实例单 Pod 的单一 Server 角色运行 vLLM 引擎。
	// 每个成员（包括预留的 API/DP-master 以及所有纯 Ray worker）都占用一个通过 ExtraArgs 声明的
	// 完整 TP 组。所有 GPU 均为整卡，不进行切分或分区。准入控制会将资源数量绑定至 TP 大小。
	// 辅助 head 不请求容器资源，且不计入 Width。
	//
	// +optional
	ElasticEP *ModelDeploymentRoleElasticEP `json:"elasticEp,omitempty" protobuf:"bytes,18,opt,name=elasticEp"`

	// StartupTimeoutSeconds 指定角色副本主容器在 kubelet 重启它之前通过启动探针的最大允许时间
	// （单位：秒）。未设置时默认为 1800（30 分钟），对应渲染出 periodSeconds 为 10 且
	// failureThreshold 为 180 的 startupProbe。
	//
	// +optional
	// +k8s:validation:default=1800
	// +k8s:validation:minimum=10
	// +k8s:validation:maximum=86400
	StartupTimeoutSeconds *int32 `json:"startupTimeoutSeconds,omitempty" protobuf:"varint,20,opt,name=startupTimeoutSeconds"` // nolint: lll
}

// ModelDeploymentRoleElasticEP 表示单个角色的 elastic-EP 配置。
//
// Width 是唯一可变字段，用于声明当前集合体应达到的规模，属于运行期状态问题。本版本允许增大
// 或保持不变；在 Elastic EP 缩容支持完善前，准入控制会拒绝减小该值。
// 创建后无法为角色开启或关闭此配置。
// 其张量并行度（TP）取自 ExtraArgs，且创建后不可变更。
// 普通角色字段的可编辑性不适用于此类角色：instanceType、resources 和 command 随配置一起
// 冻结，且 replicas 和 size 保持为 1。
type ModelDeploymentRoleElasticEP struct {
	// Width 是弹性集合体中 GPU 引擎的总数，包含预留的 API/DP-master 成员，其余 width-1 个为纯
	// Ray worker。它代表告知引擎运行的整体引擎规模，既不是 Pod 数量，也不代表 rank 映射关系：
	// 哪个成员持有哪个 rank 属于引擎自身的运行时状态，从不由该数值隐含决定。
	//
	// +k8s:validation:minimum=2
	// +k8s:validation:maximum=64
	Width int32 `json:"width" protobuf:"varint,1,name=width"`
}

// ModelDeploymentRoleTopology 表示单个独立副本组的拓扑调度请求。
type ModelDeploymentRoleTopology struct {
	// RequiredLevel 是已配置的 Kueue 拓扑层级名称，例如 topology.kubernetes.io/zone。
	// 留空表示不指定显式层级，由兼容的 TAS ResourceFlavor 自行选择层级结构。
	// +optional
	RequiredLevel string `json:"requiredLevel,omitempty" protobuf:"bytes,1,name=requiredLevel"`
}

// ModelDeploymentRoleKind 表示在 prefill/decode 分离部署中告知角色所承担的身份类型。
//
// 这些取值均对应推理引擎能够理解的角色概念，而非本 Operator 自创：每个值都对应引擎自身
// KV 传输配置中使用的区分标识。若引擎的渲染逻辑不支持某种类型，准入控制会直接拒绝，而不会
// 渲染出引擎在启动时报错的配置。
// +enum
type ModelDeploymentRoleKind string

const (
	// ModelDeploymentRoleKindServer 表示独立处理完整请求的角色，即在同一进程中同时执行 prefill
	// 和 decode。它是默认值，也是单角色部署唯一的合法类型；若与其他任何类型共存则会被拒绝，
	// 因为“普通 Server 加 Prefill”并非任何组件可消费的部署形态。
	ModelDeploymentRoleKindServer ModelDeploymentRoleKind = "Server"
	// ModelDeploymentRoleKindPrefill 表示负责计算 prompt 的 KV 块并将其传递出去、而自身不执行
	// decode 的角色。
	ModelDeploymentRoleKindPrefill ModelDeploymentRoleKind = "Prefill"
	// ModelDeploymentRoleKindDecode 表示消费 Prefill 角色生成的 KV 块并据此生成 token 的角色。
	ModelDeploymentRoleKindDecode ModelDeploymentRoleKind = "Decode"
	// ModelDeploymentRoleKindEncode 表示运行多模态编码器并将其输出传递给 Prefill 角色、而自身
	// 不生成 token 的角色。
	ModelDeploymentRoleKindEncode ModelDeploymentRoleKind = "Encode"
)

// ModelDeploymentPort 定义角色副本除引擎自身端口外额外暴露的一个端口。
//
// 该结构刻意不提供名称字段，并非遗漏。渲染出的容器端口名称是根据协议和端口号自动生成的，
// 若在此允许填写名称，就会出现 Schema 接收后却被悄然丢弃的情况——这正是本 API 在所有地方
// 都极力避免的“静默失信”模式。同一角色的两个端口通过端口号区分，Webhook 已强制要求端口
// 号唯一。
type ModelDeploymentPort struct {
	// Port 是要在副本上暴露的端口号。
	//
	// 其取值范围与容器端口本身一致。若不设此限制，0 或越界端口号会在此通过准入，随后在创建
	// 渲染出的 Pod 时被 API server 拒绝，表现为指向容器端口的每次调谐创建失败，而非指向本
	// 字段的准入错误。
	//
	// +k8s:validation:minimum=1
	// +k8s:validation:maximum=65535
	Port int32 `json:"port" protobuf:"varint,1,name=port"`

	// Protocol 是该端口使用的协议。
	//
	// +k8s:validation:enum=["TCP","UDP","SCTP"]
	// +k8s:validation:default="TCP"
	Protocol core.Protocol `json:"protocol,omitempty" protobuf:"bytes,2,opt,name=protocol"`
}

// ModelDeploymentEnvVar 定义追加到角色副本中的一个环境变量。
type ModelDeploymentEnvVar struct {
	// Name 是环境变量的名称；同一角色内的环境变量名称必须唯一。
	Name string `json:"name" protobuf:"bytes,1,name=name"`

	// Value 是环境变量的值。
	Value string `json:"value" protobuf:"bytes,2,name=value"`
}

// ModelDeploymentAdditionalVolume 定义除 Operator 自有卷之外需挂载到角色副本中的一个卷。
// 下方的卷来源字段必须设置其一；未指定任何来源的条目会在渲染时跳过，而非直接拒绝。
type ModelDeploymentAdditionalVolume struct {
	// MountPath 是卷在容器内的绝对挂载路径。不得与其他条目重复，也不得与 Operator 自有卷已
	// 占用的挂载路径冲突。
	//
	// +required
	// +k8s:validation:pattern="^(/[^/]+)+$"
	// +k8s:validation:maxLength=1024
	MountPath string `json:"mountPath" protobuf:"bytes,1,name=mountPath"`

	// ReadOnly 控制是否以只读方式挂载该卷。
	ReadOnly bool `json:"readOnly,omitempty" protobuf:"varint,2,opt,name=readOnly"`

	// SubPath 挂载卷内的相对路径，而非卷根目录。
	// 该路径不能为绝对路径，也不能包含 ".." 元素。
	//
	// 下方的正则仅校验前半部分。由于该引擎的正则表达式不支持负向前瞻，无法排除 ".." 元素，
	// 因此该部分由准入控制负责校验，详见 Webhook。这两部分都会在准入阶段被拒绝，而不会留给
	// API Server 在渲染 Pod 时拒收，否则报错只会是一次性的创建失败并指向 volumeMount，而非指向
	// 本字段的准入错误。
	//
	// +k8s:validation:pattern="^[^/].*$"
	// +k8s:validation:maxLength=1024
	SubPath string `json:"subPath,omitempty" protobuf:"bytes,3,opt,name=subPath"`

	// ConfigMap 是同一 Namespace 中待挂载 ConfigMap 的引用。
	ConfigMap *core.LocalObjectReference `json:"configMap,omitempty" protobuf:"bytes,4,opt,name=configMap"`

	// Secret 是同一 Namespace 中待挂载 Secret 的引用。
	Secret *core.LocalObjectReference `json:"secret,omitempty" protobuf:"bytes,5,opt,name=secret"`

	// HostPath 是待挂载的 Kubernetes Node 本地路径。它跨越了节点边界：该挂载访问的是节点
	// 自身的文件系统而非命名空间作用域对象，因此其暴露的内容取决于节点自身状态，而非本 API
	// 所能感知的任何对象。
	HostPath *core.HostPathVolumeSource `json:"hostPath,omitempty" protobuf:"bytes,6,opt,name=hostPath"`

	// PersistentVolumeClaim 是部署所在 Namespace 中待挂载 PersistentVolumeClaim 的引用。
	// claimName 为必填项，用于指定该 Namespace 中的 Claim；条目上只能设置一种卷来源。现有的
	// 挂载路径、subPath 和只读字段同样适用。
	PersistentVolumeClaim *core.PersistentVolumeClaimVolumeSource `json:"persistentVolumeClaim,omitempty" protobuf:"bytes,7,opt,name=persistentVolumeClaim"` // nolint: lll
}

// ModelDeploymentRoleResources 描述角色中单个 Pod 对加速器和网络接口的资源请求。
//
// 它刻意沿用了 InstanceResources 的加速器字段——相同的名称、相同的语义——而非为同一类请求
// 发明第二套词汇；同时刻意省略了该类型中的 CPU、RAM 和 LocalStorage 字段，因为这些资源在此处
// 是推导得出的，而非显式声明的。
type ModelDeploymentRoleResources struct {
	// Accelerator 是单个 Pod 请求的加速卡数量。
	//
	//   - 在支持加速器的 InstanceType 上若未设置，则在准入阶段（无论创建还是更新）默认设为 1，
	//     行为与 Instance 一致。该值会直接写入持久化对象，而非在渲染时应用，因此读回的值与
	//     准入时的值完全一致。
	//   - 显式设置的 0 会被保留，因为这是用户显式指定的值，在支持加速器的 InstanceType 上表示
	//     不请求该资源池队列所计量的任何加速器资源。当它是唯一使用该 InstanceType 的角色时会被
	//     接受；当有其他角色共享该 InstanceType 时会被拒绝：请求 0 配额的副本在准入并运行后
	//     不会被队列计费，却会消耗同一 InstanceType 上其他兄弟角色被计费的同一资源池。
	//   - 无需加速器运行的 Pod 应使用不支持加速器的 InstanceType，此时队列按 CPU 计量。
	Accelerator *resource.Quantity `json:"accelerator,omitempty" protobuf:"bytes,1,opt,name=accelerator"`

	// AcceleratorSlicedMemoryPercentage 是在启用切分的 InstanceType 上为每张加速卡请求的
	// 显存预算，取值范围为 [0,100] 的百分比。0 表示禁用切分，即转为独占整卡请求。在不提供
	// 切分能力的 InstanceType 上该字段会被忽略。
	//
	// +k8s:validation:minimum=0
	// +k8s:validation:maximum=100
	AcceleratorSlicedMemoryPercentage int32 `json:"acceleratorSlicedMemoryPercentage,omitempty" protobuf:"varint,2,opt,name=acceleratorSlicedMemoryPercentage"` // nolint: lll

	// AcceleratorSlicedCoresPercentage 是在启用切分的 InstanceType 上为每张加速卡请求的
	// 算力预算，取值范围为 [0,100] 的百分比，独立于显存百分比设置。
	//
	// +k8s:validation:minimum=0
	// +k8s:validation:maximum=100
	AcceleratorSlicedCoresPercentage int32 `json:"acceleratorSlicedCoresPercentage,omitempty" protobuf:"varint,3,opt,name=acceleratorSlicedCoresPercentage"` // nolint: lll

	// AcceleratorPartitionedProfile 是在提供硬件分区的 InstanceType 上请求的硬件分区规格，
	// 例如 "3g.40gb"。非空值与两个切分百分比字段互斥：同一张加速卡不能同时应用硬件分区和
	// 软件切分。在不提供分区的 InstanceType 上该字段会被忽略。
	//
	// +k8s:validation:maxLength=64
	AcceleratorPartitionedProfile string `json:"acceleratorPartitionedProfile,omitempty" protobuf:"bytes,4,opt,name=acceleratorPartitionedProfile"` // nolint: lll

	// Interface 是角色中单个 Pod 请求的网络接口数量，必须为整数。
	// 未设置或为 0 表示不请求。正数数量会选择对应有效缓存或直传协议的 DevicePlugin 资源。
	// 单个 RDMA 接口使用共享资源；多个 RDMA 接口使用独占资源。EFA 使用其自身的插件资源。
	// 互相冲突的有效协议会被直接拒绝，而不会在可用设备族中任选其一。与 accelerator 相同，
	// 整数要求和协议规则均由准入控制强制校验；Schema 本身不设上下界。
	//
	// +optional
	Interface *resource.Quantity `json:"interface,omitempty" protobuf:"bytes,5,opt,name=interface"`
}

// ModelDeploymentRouter 描述位于部署各角色前端的 Router，以及本 Operator 负责管理的
// 范围。
//
// 此处没有选择由谁运行 Router 的字段，这是有意为之的设计决策而非遗漏。本 Operator 负责
// 渲染并持有该 Router；当前不支持由集群中已运行的外部 Router 来承接部署流量。若在仅有一种
// 实现可渲染的情况下提供该选择字段，该字段将不携带任何信息（每个对象都只能填同一个值）；
// 后续若有需要，可作为带默认值的可选字段向后兼容地新增。若一开始就放出该选择字段并在日后
// 改变其取值语义，则会破坏兼容性。
type ModelDeploymentRouter struct {
	// Name 指定位于该部署前端的 Router 实现。
	//
	// 其取值遵循对应项目的原生拼写，而非本 API 的统一风格，这一差异在同一个词上就可见一斑：
	// 缓存后端类型上的传输协议拼写为 "Auto"，而此处的连接器拼写为 "auto"。大小写约定按 API
	// 类型各自独立，原因正如 ModelDeploymentRoleKind 所说明的——这些取值是外部工具能够理解的
	// 术语，而非本 Operator 自行发明的词汇。"llm-d-router" 正是 llm-d 项目在其模块路径中对该
	// Router 的拼写，因此此处沿用该拼写。
	//
	// 从 "llm-d" 更名而来无需转换逻辑，因为旧拼写从未在任何正式版本中发布：它仅存在于该字段
	// 引入到本次更名之间的 main 分支上，期间未切出任何发布版本，因此没有任何已发布的 CRD
	// 接受过旧拼写，所有由正式版本写入的对象均符合当前枚举值。运行该区间内未发布构建的集群
	// 不在此保障范围内，需手动修改此类对象。
	//
	// 扩展新的枚举值涉及四项工作，而非一项：此处新增一个枚举项、对应配置渲染器、该 Router 所需的
	// 对象集合，以及将该取值传递到分发点的接入逻辑。Schema 预留仅覆盖其中第一项，因此接入
	// 第二个 Router 是一项完整的开发工作，而不只是一个常量。
	//
	// 该取值同时还受引擎匹配规则约束，该规则由独立校验逻辑而非枚举本身表达："vllm-router" 和
	// "sglang-gateway" 分别是各自项目为其自身引擎设计的 Router，因此在另一方引擎前端使用时会被
	// 拒绝。"llm-d-router" 则支持任意一种引擎，因为上游为这两种引擎都提供了握手连接器和指标配置。
	//
	// +required
	// +k8s:validation:enum=["llm-d-router","vllm-router","sglang-gateway"]
	Name string `json:"name" protobuf:"bytes,1,name=name"`

	// Replicas 指定要运行的 Router Pod 数量。缺省表示 1。
	//
	// 该字段设为可选实属受限之举而非随意选择。Schema 默认值会在任何 Webhook 见到对象之前生效，
	// 因此默认设为 1 的普通 int32 传入时无法与用户显式填写的 1 区分开。在准入阶段保留这一
	// 区分度，才能让后续规则判断“是否有人显式请求过该配置”；一旦默认值抹除了这一差异，事后就
	// 无法再恢复。
	//
	// 多个副本是以缓存一致性换取高可用性。基于前缀缓存评分的 Router 会在每个副本内部维护各自
	// 的状态：上游报告称 radix tree 不会在副本间同步，因此命中率会下降 10% 到 20%；即便副本间
	// 交换事件，也只能改善负载估算，而不会让两个副本做出完全一致的路由决策。允许设置多于一个
	// 副本，但在此明示其代价，而非留待用户在监控面板上才发现。
	//
	// +optional
	// +k8s:validation:minimum=1
	Replicas *int32 `json:"replicas,omitempty" protobuf:"varint,2,opt,name=replicas"`

	// Image 覆盖 Router 的容器镜像。为空时 Operator 会根据 Name 自动拼装镜像，方式与角色未指定
	// 镜像时相同。
	//
	// +optional
	// +k8s:validation:maxLength=512
	Image string `json:"image,omitempty" protobuf:"bytes,3,opt,name=image"`

	// ExtraArgs 是传递给 Router 进程的附加启动参数。
	//
	// 由 Operator 自行推导的参数会被拒绝而非合并，以确保同一配置只有一个来源。受管参数目录按
	// Router 索引，因为按引擎索引、用于校验角色 extraArgs 的目录解决的是另一个问题，无法替代。
	//
	// +optional
	// +listType=atomic
	ExtraArgs []string `json:"extraArgs,omitempty" protobuf:"bytes,4,rep,name=extraArgs"`

	// ImagePullPolicy 是 Image 的拉取策略。
	//
	// +optional
	ImagePullPolicy core.PullPolicy `json:"imagePullPolicy,omitempty" protobuf:"bytes,5,opt,name=imagePullPolicy"`

	// ImagePullSecrets 是拉取 Image 时使用的 Secret 列表。
	//
	// +optional
	// +listType=atomic
	// +k8s:validation:maxItems=32
	ImagePullSecrets []core.LocalObjectReference `json:"imagePullSecrets,omitempty" protobuf:"bytes,6,rep,name=imagePullSecrets"`

	// RequestTimeoutSeconds 是 Router 在放弃请求前等待响应的超时时间（秒）。
	//
	// 未设置该字段时各 Router 的行为并不一致，本段注释正是为了说明这一点。省略该字段时不会渲染
	// 任何超时参数，因此各 Router 会保留各自的上游默认值："llm-d-router" 由其代理层承载超时，
	// 默认为一天；而另外两种 Router 通过命令行配置，默认为半小时。两者相差四十八倍，因此显式
	// 设置该字段是切换 Router 时保持配置一致的唯一方式——缺省会保留三种互不相同的上游默认行为，
	// 而不会替用户在其中做选择。
	//
	// 不接受 0 值。0 在代理层中表示“永久等待”，而在另外两种实现中并无特定含义；一个在三种实现中
	// 语义统一的字段不能承载某一种实现的特殊值。一天的上限在实践中已足够长，两者差异仅具理论
	// 意义；后续如有需要可随时下调下限，且不会破坏已有对象。
	//
	// +optional
	// +k8s:validation:minimum=1
	RequestTimeoutSeconds *int32 `json:"requestTimeoutSeconds,omitempty" protobuf:"varint,7,opt,name=requestTimeoutSeconds"`

	// DisaggregationThresholdTokens 指定尚未命中前缀缓存的提示词 Token 达到多少时，才值得将请求
	// 拆分到 prefill 与 decode 实例执行。低于该阈值时由 decode 副本独自处理完整请求。未设置时
	// 渲染 Router 自身的当前默认值。
	//
	// 设为 0 会完全禁用分离调度，而非表示“总是拆分”：决策逻辑在读取其他任何条件前遇到 0 阈值
	// 就会直接返回“不分离”。准入允许而非拒绝 0 值，因为这是上游明确定义的有效取值，且该字段为
	// 可选字段，从而使显式写 0 与省略该字段表达两种不同的意图。
	//
	// 该字段仅在 "llm-d-router" 下有效，在另外两种 Router 下会被拒绝而非静默忽略，因为允许写入
	// 却不产生任何渲染效果的字段形态是本 API 一贯拒绝的。由于 Name 在创建后不可变更，该拒绝
	// 条件是稳定的，对象不会因后续修改其他字段而变为非法。
	//
	// +optional
	// +k8s:validation:minimum=0
	DisaggregationThresholdTokens *int32 `json:"disaggregationThresholdTokens,omitempty" protobuf:"varint,8,opt,name=disaggregationThresholdTokens"`
}

// ModelDeployment 可指定的 Router 列表，对应 ModelDeploymentRouter.Name 枚举的取值集合。
//
// 声明在限定该集合的字段旁，便于阅读任一处时对照另一处。
const (
	ModelDeploymentRouterLLMD   = "llm-d-router"
	ModelDeploymentRouterVLLM   = "vllm-router"
	ModelDeploymentRouterSGLang = "sglang-gateway"
)

// ModelDeploymentStatus 定义 ModelDeployment 的观测状态。
//
// 它在每次协调时都会根据观测状态重建，因此过时的字段不会在与 Pod 状态不一致时残留。
type ModelDeploymentStatus struct {
	// Phase 汇总当前条件状态：Starting、Ready、Degraded、Deleting。Ready 表示所有角色的就绪数
	// 均等于期望数，且在声明了 Router 时该 Router 至少有一个就绪副本。Degraded 表示某个服务组件
	// 已就绪，但另一个必需组件尚未就绪。
	Phase string `json:"phase,omitempty" protobuf:"bytes,1,opt,name=phase"`

	// PhaseMessage 携带当前阶段的原因说明。
	PhaseMessage string `json:"phaseMessage,omitempty" protobuf:"bytes,2,opt,name=phaseMessage"`

	// Conditions 提供更细粒度的状态视图，每个维度对应一个条件：DomainRegistered、QuotaReserved、
	// CacheAttached、ReplicasUpToDate、RoleKindsReady、KVEventsPublishing、RouterReady、
	// WeightsReady。它们彼此独立——例如“配额已预留但缓存未挂载”是一种真实且可操作的状态——
	// 这是单个 phase 字符串无法表达的。
	//
	// KVEventsPublishing 反映的是已渲染的配置状态，而非对实时事件流的观测。因此，已配置但随后
	// 崩溃的发布端在有实时消费端观测到异常之前仍会保持为 True。
	//
	// WeightsReady 报告所有引擎角色的权重是否已可用：声明的制品已挂载，或引擎自行从模型中心下载
	// 制品已完成。只要该条件因非 Pod 启动中的原因保持为 False，就不会创建新副本，也不会触动
	// 正在运行的副本。
	//
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []gpustack.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,3,rep,name=conditions"` // nolint: lll

	// Endpoint 是客户端访问的服务地址。未配置 Router 时为部署级 Service，格式为
	// <scheme>://<name>.<namespace>.svc:<port>。配置了 Router 时为 Router 的 Service，且在
	// Router 拥有就绪副本前为空。
	//
	// 当某角色自身的参数使其监听器启用 TLS 时，角色端点使用 https。受管 Router 的端点则使用
	// Router 自身的传输协议（即 http）。客户端应直接读取此处给出的地址，而非自行假定协议。
	//
	// +k8s:validation:maxLength=512
	Endpoint string `json:"endpoint,omitempty" protobuf:"bytes,4,opt,name=endpoint"`

	// Roles 为每个声明的角色保留一条状态条目。
	//
	// +listType=map
	// +listMapKey=name
	Roles []ModelDeploymentRoleStatus `json:"roles,omitempty" protobuf:"bytes,5,rep,name=roles"`

	// KVCache 是该部署实际接入的复用域，读取自 Binding，使得区分缓存共享配置错误与单纯的冷缓存
	// 只需查看一个对象，而不必查看两个对象。
	//
	// 在 Binding 至少成功解析一次之前该字段保持缺失，而非置为空对象：空对象与所有字段恰好为空
	// 的域无法区分。一旦解析过一次，当后续 Binding 无法再解析时仍会保留上次读取到的域，因为
	// 副本此时仍在向该域写入；由 DomainRegistered 负责指示该读数已过时。
	KVCache *ModelDeploymentKVCacheStatus `json:"kvCache,omitempty" protobuf:"bytes,6,opt,name=kvCache"`

	// Router 是 spec.router 所请求的受管 Router 的观测契约。
	//
	// 当 spec.router 未配置时该字段保持缺失，而非置为空对象，原因与 KVCache 相同：空对象无法
	// 与所有字符串字段恰好为空的契约区分开。
	Router *ModelDeploymentRouterStatus `json:"router,omitempty" protobuf:"bytes,7,opt,name=router"`

	// RoleSummary 按角色类型统计当前就绪数量，供 kubectl 的 Roles 列展示。R 统计受管 Router
	// Pod 数；S、P、D 分别统计 server、prefill 和 decode 实例数。由于单个服务实例可能包含多个
	// Pod，引擎侧数字并不等同于 Pod 数量。
	RoleSummary string `json:"roleSummary,omitempty" protobuf:"bytes,8,opt,name=roleSummary"`

	// Model 回显 spec.model.artifactRef 解析到的模型权重信息以及向引擎交付的方式。
	//
	// 未设置 spec.model.artifactRef 时，以及制品尚未解析完成前，该字段均保持缺失，原因与
	// KVCache 相同：空对象无法与所有字段恰好为空的标识区分开。
	Model *ModelDeploymentModelStatus `json:"model,omitempty" protobuf:"bytes,9,opt,name=model"`

	// Retirement 是当前部署正在执行的退役操作的持久化预留记录，同一时间最多存在一个。没有退役
	// 操作时该字段保持缺失，而非置为空对象：操作状态保存在该对象中，空对象既不代表有操作也
	// 不代表状态有效。其标识绑定了准入时的 metadata.generation、角色名、副本序号以及目标成员和
	// Workload UID，从而保证控制器重启后可恢复执行，且冲突的操作不会静默接管该记录。
	//
	// +optional
	Retirement *ModelDeploymentRetirementStatus `json:"retirement,omitempty" protobuf:"bytes,10,opt,name=retirement"`
}

// ModelDeploymentRetirementStatus 是单次退役操作的持久化预留状态。
//
// 该预留记录即协议的执行进度：每一步状态、已消耗的预算边界与重试信息均保存于此，使控制器
// 在重启后能从观测到的状态恢复，而非重新决策。阶段预算同时读取 deadline 和 phaseStartedAt，
// 因此重启绝不会重置已经开始运行的阶段计时。
type ModelDeploymentRetirementStatus struct {
	// RoleName 是正在退役副本所属的角色名称。
	//
	// +required
	RoleName string `json:"roleName" protobuf:"bytes,1,name=roleName"`

	// ReplicaOrdinal 是正在退役的副本在其角色内的序号。0 是有效位置（即第一个副本），因此始终
	// 会被编码输出而不会被省略。
	//
	// +required
	ReplicaOrdinal int32 `json:"replicaOrdinal" protobuf:"varint,2,name=replicaOrdinal"`

	// ObservedGeneration 是准入该预留记录时所针对的期望状态 metadata.generation。
	//
	// +required
	ObservedGeneration int64 `json:"observedGeneration" protobuf:"varint,3,name=observedGeneration"`

	// TargetMemberUIDs 是该预留记录锁定的成员 Pod UID 列表。删除其中任一 Pod 时都会携带其 UID
	// 作为前置条件，确保不会误删同名的重建替代 Pod。
	//
	// +optional
	// +listType=atomic
	TargetMemberUIDs []string `json:"targetMemberUIDs,omitempty" protobuf:"bytes,4,rep,name=targetMemberUIDs"`

	// TargetWorkloadUID 是该预留记录锁定的 Workload UID。
	//
	// +required
	TargetWorkloadUID string `json:"targetWorkloadUID" protobuf:"bytes,5,name=targetWorkloadUID"`

	// State 是该预留记录当前所处的协议阶段。Aborted 表示在删除前预算已耗尽，此时所有成员和
	// Workload 均被保留；这是一种保留现状的终态，而非回滚。
	//
	// +required
	// +k8s:validation:enum=["Admitted","Disqualified","Withdrawing","Draining","Deleting","Settling","Aborted","Completed"]
	State ModelDeploymentRetirementState `json:"state" protobuf:"bytes,6,name=state,casttype=ModelDeploymentRetirementState"`

	// Reason 说明导致预留记录进入当前状态的原因，例如预算是在哪一步耗尽的。
	//
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,7,opt,name=reason"`

	// StartedAt 是该操作获得准入的时间。
	//
	// +required
	StartedAt meta.Time `json:"startedAt" protobuf:"bytes,8,name=startedAt"`

	// Deadline 是整体时间预算。每个阶段的截止时间取 min(deadline, 该阶段开始时间 + 阶段预算)，
	// 从而保证没有任何阶段会超出整个操作的生命期。
	//
	// +required
	Deadline meta.Time `json:"deadline" protobuf:"bytes,9,name=deadline"`

	// PhaseStartedAt 是当前阶段的开始时间。将其持久化可确保控制器重启绝不会重置已在运行中的
	// 阶段预算。
	//
	// +required
	PhaseStartedAt meta.Time `json:"phaseStartedAt" protobuf:"bytes,10,name=phaseStartedAt"`

	// LastConsumedRetryToken 是该预留记录最近一次消费的重试指令令牌。它在清除携带该令牌的
	// 注解之前先行持久化，从而保证即使在两者之间发生崩溃，令牌仍处于已消费状态且任何重放都
	// 成为空操作。为空表示尚未消费过任何令牌。
	//
	// +optional
	LastConsumedRetryToken string `json:"lastConsumedRetryToken,omitempty" protobuf:"bytes,11,opt,name=lastConsumedRetryToken"`
}

// ModelDeploymentRetirementState 是退役预留记录所处的协议阶段。
// +enum
type ModelDeploymentRetirementState string

const (
	// ModelDeploymentRetirementStateAdmitted 表示预留记录刚写入、尚未执行任何步骤的初始状态。
	ModelDeploymentRetirementStateAdmitted ModelDeploymentRetirementState = "Admitted"
	// ModelDeploymentRetirementStateDisqualified 表示在移除资格之后、Service 收敛期间的状态。
	ModelDeploymentRetirementStateDisqualified ModelDeploymentRetirementState = "Disqualified"
	// ModelDeploymentRetirementStateWithdrawing 等待目标的实际服务确认。
	ModelDeploymentRetirementStateWithdrawing ModelDeploymentRetirementState = "Withdrawing"
	// ModelDeploymentRetirementStateDraining 就地读取引擎的在途请求队列，所有成员保持不变。
	ModelDeploymentRetirementStateDraining ModelDeploymentRetirementState = "Draining"
	// ModelDeploymentRetirementStateDeleting 是已提交的删除步骤。
	ModelDeploymentRetirementStateDeleting ModelDeploymentRetirementState = "Deleting"
	// ModelDeploymentRetirementStateSettling 在删除后观测加速器释放与配额收敛。
	ModelDeploymentRetirementStateSettling ModelDeploymentRetirementState = "Settling"
	// ModelDeploymentRetirementStateAborted 表示在删除前预算耗尽：成员、Workload 与容量均已保留，
	// 健康实例无需重新滚动更新即可恢复资格。
	ModelDeploymentRetirementStateAborted ModelDeploymentRetirementState = "Aborted"
	// ModelDeploymentRetirementStateCompleted 是已完成退役的终态转换：它与清除预留
	// 在同一次处理中设置，因此状态读取方会通过该字段缺失来观测完成状态，而不会看到该值被存储。
	ModelDeploymentRetirementStateCompleted ModelDeploymentRetirementState = "Completed"
)

// ModelDeploymentModelStatus 是部署所提供服务的已解析权重标识。
//
// 每个字段都从 ModelArtifact 读取，从不在此声明，因此只需读取一个对象而非两个对象，
// 即可获知部署所使用的 commit 和内容。
type ModelDeploymentModelStatus struct {
	// Artifact 是此部署在同一 namespace 中引用的 ModelArtifact。
	//
	// +required
	Artifact string `json:"artifact" protobuf:"bytes,1,name=artifact"`

	// Revision 是 hub 类型制品解析到的 commit。对于 claim 类型制品则不存在。
	//
	// +optional
	Revision string `json:"revision,omitempty" protobuf:"bytes,2,opt,name=revision"`

	// ManifestDigest 是 hub 类型制品的内容地址。对于 claim 类型制品则不存在。
	//
	// +optional
	ManifestDigest string `json:"manifestDigest,omitempty" protobuf:"bytes,3,opt,name=manifestDigest"`

	// Delivery 表示权重到达引擎的方式："PVC" 表示将 claim 以只读方式挂载到固定路径；
	// "Engine" 表示由引擎自行下载固定的 commit；"Node" 表示由节点的 model-manager 插件
	// 实体化已验证的文件并以只读方式挂载到同一固定路径；"Image" 表示由 kubelet 将固定的 OCI 镜像
	// 拉取到只读镜像卷中。
	//
	// +required
	// +k8s:validation:enum=["PVC","Engine","Node","Image"]
	Delivery ModelDeploymentModelDelivery `json:"delivery" protobuf:"bytes,4,name=delivery,casttype=ModelDeploymentModelDelivery"`
}

// ModelDeploymentModelDelivery 表示部署的权重到达其引擎的方式。
// +enum
type ModelDeploymentModelDelivery string

const (
	// ModelDeploymentModelDeliveryPVC 将 claim 制品以只读方式挂载到固定路径。
	ModelDeploymentModelDeliveryPVC ModelDeploymentModelDelivery = "PVC"
	// ModelDeploymentModelDeliveryEngine 由引擎使用制品 Secret 中的令牌，
	// 将 hub 制品解析到的 commit 下载到受大小限制的缓存卷中。
	ModelDeploymentModelDeliveryEngine ModelDeploymentModelDelivery = "Engine"
	// ModelDeploymentModelDeliveryNode 由节点的 model-manager 插件将 hub 制品的已验证文件
	// 实体化到节点缓存中，并以只读方式挂载。
	ModelDeploymentModelDeliveryNode ModelDeploymentModelDelivery = "Node"
	// ModelDeploymentModelDeliveryImage 通过 Kubernetes 镜像卷以只读方式挂载镜像制品的 OCI 镜像：
	// 由 kubelet 在需要的节点上拉取固定镜像，不使用节点的插件缓存。
	ModelDeploymentModelDeliveryImage ModelDeploymentModelDelivery = "Image"
)

// ModelDeploymentRoleStatus 是单个角色观测到的就绪状态。
type ModelDeploymentRoleStatus struct {
	// Name 是此条目描述的角色名称。
	//
	// +required
	Name string `json:"name" protobuf:"bytes,1,name=name"`

	// Desired 表示 spec 请求的实例数，Ready 表示其中处于 Ready 状态的实例数。两者统计的都是
	// 实例而非 Pod，仅当一个实例只包含一个 Pod 时两者数值才相同：包含 2 个实例、每个实例 4 个 Pod
	// 的角色会报告 2，且仅当实例声明的所有 Pod 都 Ready 时该实例才为 Ready。这两个字段始终存在——
	// 它们基于成功获取的 Pod 列表统计，因此此处的 0 是观测到的 0；如果列表获取失败，则完全不写入状态。
	Desired int32 `json:"desired" protobuf:"varint,2,name=desired"`

	Ready int32 `json:"ready" protobuf:"varint,3,name=ready"`

	// QuotaReserved 表示该角色中持有配额预留的副本数。每个副本都是独立的 Kueue Workload，
	// 因此在容量陆续到位期间，角色可以处于 0 到 Desired 之间的任意数量；而如果所有副本共享
	// 同一个 Workload，则只能全有或全无，该统计值也无从谈起。
	//
	// 该字段始终存在，且其 0 值表示真实观测到的 0：该数值基于成功获取的 Pod 和 Workload 列表统计，
	// 若列表获取失败则完全不写入状态而非写入 0，因为“本轮未能观测到”和“没有副本持有配额”
	// 需要采取相反的动作——前者应等待，后者应排查——若两者都写为 0，读数就无法区分。
	QuotaReserved int32 `json:"quotaReserved" protobuf:"varint,7,name=quotaReserved"`

	// Unmanaged 为 true 表示角色替换了完整的命令行，因此 operator 未为其合成引擎参数
	// 或客户端环境变量。它始终存在，原因与计数字段相同：false 是常规情况，必须作为明确结果
	// 呈现，而不能作为缺失字段省略。
	Unmanaged bool `json:"unmanaged" protobuf:"varint,4,name=unmanaged"`

	// Kind 回显角色的类型，因此仅读取状态即可确定该条目描述的是分离部署中的哪一侧。
	// 它始终存在：每个角色都有类型（若用户未指定则会使用默认值），因此缺失值意味着
	// 状态是由不了解角色类型的组件写入的，而不是角色没有类型。
	//
	// 该枚举与 spec 字段使用的枚举相同且必须保持一致：此字段由解析了未设置情况后的
	// spec 字段写入，因此若写入方能产生此列表未列出的值，会导致后续对该对象的每次状态写入失败，
	// 连带使其他所有统计值都无法更新。校验标记放在该字段上，因为类型自身的枚举标记仅在 Go 层面生效，
	// 不会生成 schema 校验。
	//
	// +k8s:validation:enum=["Server","Prefill","Decode","Encode"]
	Kind ModelDeploymentRoleKind `json:"kind" protobuf:"bytes,5,name=kind,casttype=ModelDeploymentRoleKind"`

	// AssignedFlavors 是 Kueue 为该角色各副本的加速器额度分配的 ResourceFlavor 集合，
	// 已去重并排序。它是一个集合，因为每个副本都是独立的 Workload，Kueue 按 Workload 分配 flavor，
	// 所以同一角色的两个副本可能获得不同的分配——这是按角色单个 PodSet 无法产生的状态。
	//
	//   - 字段缺失表示没有任何已分配的副本指定了 flavor，而非空列表所暗示的“分配到了空”：
	//     “尚未分配”和“已分配但所在资源池不带任何加速器名称”在这里对应同一事实，
	//     省略该字段可避免被误读为第三种状态。
	//   - 单个条目表示该角色的所有已分配副本都使用该 flavor。多个条目表示不同副本被分配了
	//     不同的 flavor，这是需要排查的信号而非单一答案的降级形式：此处刻意不体现哪个副本
	//     持有哪个 flavor，因为按副本记录所依赖的序号是收敛器的内部槽位编号，而非本 API 做出的
	//     承诺，需要该信息的读取方应直接查看各副本自身的 Pod。
	//   - 已准入的角色在此处仍可能不报告任何内容，这是该字段的契约而非缺陷。该结果通过
	//     按加速器准入门控所使用的同一函数读取，该函数只关注加速器额度，因此准入到不带
	//     加速器的资源池上的角色只会为 `cpu` 分配 flavor，而此处为空。刻意保持两者一致：
	//     若此处报告了门控并不参与匹配的 flavor，反而比不报告更糟。
	//
	// +optional
	// +listType=atomic
	AssignedFlavors []string `json:"assignedFlavors,omitempty" protobuf:"bytes,6,rep,name=assignedFlavors"`

	// Parallelism 是角色自身参数所声明的并行度，并附带读数的来源信息。其中不会填入默认值：
	// 角色未声明的维度为 nil，显式的 1 和显式的本地 0 均保留为具体值，尚未进行的读取
	// 会报告 Unknown 并附带原因，而不是静默填 1。prefill/decode 传输文档从相同参数
	// 推导出的数值结果不受此视图影响。
	//
	// 它是可选字段，以便在此视图引入之前存储的角色仍可写入：字段缺失仅表示该事实，
	// 从不代表默认读数，operator 写入对象时会重新填充它。
	//
	// +optional
	Parallelism ModelDeploymentRoleParallelismStatus `json:"parallelism" protobuf:"bytes,8,name=parallelism"`

	// Endpoints 表示角色的端点资格状态及其实际服务确认情况。这是两个永远不会混为一谈的
	// 独立结果：资格是 operator 认定合格的集合，而服务状态是观测到 Router 仍在选择的集合。
	// 从未观测到的计数为 nil，0 仅表示观测到的事实。
	//
	// 它与 parallelism 一样是可选字段，原因也相同：在此视图引入之前存储的角色不携带
	// 这两个对象，缺失不能导致其下次写入被拒绝。
	//
	// +optional
	Endpoints ModelDeploymentRoleEndpointsStatus `json:"endpoints" protobuf:"bytes,9,name=endpoints"`

	// Groups 为该角色的每个分组提供一条记录，按副本序号排序。
	//
	// 它回答了上述计数字段无法回答的问题。这些计数字段只说明角色有多少实例以及其中有多少
	// 处于 Ready 状态；而分组条目则说明哪些成员组成一个调度组、该组处于什么状态，以及 Router
	// 是否确认正在为其提供服务。读取方若想了解哪个副本失败，或 Router 停止选择了哪个副本，
	// 就需要读取此条目，无法从计数中获得。
	//
	// 它是一个普通列表而非映射（map），因为每当有成员被替换时，分组的 generation 就会改变。
	// 若以 generation 为键，每次成员变更都会替换整条记录；若仅以序号为键，则可能让读取方
	// 将旧记录匹配到新成员集合。因此身份标识直接携带在条目内部。
	//
	// +optional
	// +listType=atomic
	Groups []ModelDeploymentGroupStatus `json:"groups,omitempty" protobuf:"bytes,10,rep,name=groups"`
}

// ModelDeploymentGroupStatus 是已发布的单个分组的资格状态。
//
// 它是对单个分组的描述，而非角色端点的计数。角色的计数字段仍然是派生视图，
// 不反映哪个分组包含哪个成员。
//
// 状态与服务确认是两个独立结果，永远不会合并为一个。状态和原因来自引擎侧——表明
// 整组判定条件中的哪一项阻滞了该组。服务确认则来自 Router 侧。成员健康但丢失 Router 视图
// 的分组，其状态为 Eligible 且服务确认为 Unknown；成员发生故障的分组状态为 Withdrawn。
// 这两种读数绝不会互相混淆。
type ModelDeploymentGroupStatus struct {
	// Role 是此分组所属的角色。
	//
	// +required
	Role string `json:"role" protobuf:"bytes,1,name=role"`

	// Ordinal 是此分组在其角色中所占用的副本槽位。
	//
	// +required
	Ordinal int32 `json:"ordinal" protobuf:"varint,2,name=ordinal"`

	// Generation 是以下结果所针对的成员集合：即排序后的成员 UID 集合。
	// 成员被替换后集合即发生变化，因此先前的验证不再适用于新成员集合。
	//
	// +required
	Generation string `json:"generation" protobuf:"bytes,3,name=generation"`

	// State 是分组的资格状态。
	//
	// 它通常反映资格判定的状态，但 Router 观测阻滞分组的情况除外：通过全部检查
	// 并正在等待有效 Router 观测的分组处于 AwaitingObservation；而通过全部检查并在观测丢失期间
	// 仍被保留的分组处于 Eligible，因为此时并非观测阻滞了它。
	//
	// +required
	// +k8s:validation:enum=["Eligible","Withdrawn","AwaitingMembers","AwaitingReady","AwaitingProbe","AwaitingObservation","Unsupported","Unknown"]
	State ModelDeploymentGroupState `json:"state" protobuf:"bytes,4,name=state,casttype=ModelDeploymentGroupState"`

	// Reason 指明阻滞分组的单一事实，或说明其符合资格。
	//
	// 它只指明一个事实，绝不同时指明两个。当整组判定条件中的某一项阻滞分组时，原因指向该项；
	// 当 Router 观测阻滞分组时，原因指向该观测。因此，成员故障与 Router 视图丢失不仅可以通过状态
	// 区分，也可以通过原因区分。
	//
	// +required
	Reason string `json:"reason" protobuf:"bytes,5,name=reason"`

	// Members 是分组当前的成员列表，按 seat 排序。
	//
	// +optional
	// +listType=atomic
	Members []ModelDeploymentGroupMemberStatus `json:"members,omitempty" protobuf:"bytes,6,rep,name=members"`

	// LastQualificationTime 是该分组针对当前 generation 最近一次进入已发布 Eligible 状态的时间。
	//
	// 它记录的是状态转换时刻，而非最近一次检查通过时间。保持 Eligible 的分组会保留原先
	// 获得的时间戳，在同一 generation 内被暂时阻滞的分组也会保留它，因此观测丢失不会抹除该时间。
	// 分组离开 Eligible 后再次回到该状态时会记录新的时间。新的 generation 不继承旧时间戳：
	// 它所代表的成员集合此前从未获得过资格。
	//
	// +optional
	LastQualificationTime *meta.Time `json:"lastQualificationTime,omitempty" protobuf:"bytes,7,opt,name=lastQualificationTime"`

	// Serving 是 Router 对该分组给出的结果。
	//
	// +required
	Serving ModelDeploymentServingStatus `json:"serving" protobuf:"bytes,8,name=serving"`

	// ServingReason 在 Router 结果不是确认状态时，说明其呈现当前结果的原因。
	//
	// 它在两种观测丢失情况下都会发布，因为 serving 字段本身必须信息完整：若读取方
	// 看到 Serving 却找不到原因，就无法区分“尚未观测到 Router”和“已观测到 Router 但未报告任何内容”。
	//
	// 它与 Reason 相互独立，因为两者描述不同的事实。Reason 描述阻滞分组的原因，
	// ServingReason 描述影响服务确认结果的原因，因此引擎故障的原因绝不会出现在此处，
	// Router 的原因也绝不会取代分组的原因。
	//
	// +optional
	ServingReason string `json:"servingReason,omitempty" protobuf:"bytes,9,opt,name=servingReason"`
}

// ModelDeploymentGroupMemberStatus 表示分组中单个成员的当前状态。
type ModelDeploymentGroupMemberStatus struct {
	// Name 是成员 Pod 的名称。
	//
	// +required
	Name string `json:"name" protobuf:"bytes,1,name=name"`

	// UID 是成员 Pod 的身份标识。被替换后的成员具有不同的 UID，这也是替换成员会导致
	// 成员集合进入不同 generation 的原因。
	//
	// +required
	UID string `json:"uid" protobuf:"bytes,2,name=uid"`

	// Seat 是成员声明的槽位编号；若成员未声明或声明无法读取，则该字段缺失，分组的 reason
	// 会指明该缺陷。
	//
	// 在以完整副本作为成员的分组中，seat 是该成员的准入序号，因为它在自身副本内部
	// 成员之间声明的 seat 根本不是该分组的 seat。该序号是 operator 自身的事实，而非引擎
	// 运行时的 rank：哪个成员持有哪个 rank 由引擎决定，operator 的编号从不暗示这一点。
	//
	// +optional
	Seat *int32 `json:"seat,omitempty" protobuf:"varint,3,opt,name=seat"`

	// Ready 是成员的就绪状态，取值为 True、False 或 Unknown。
	//
	// 从未报告过 Ready 条件的成员其状态为 Unknown 而非 False：无人上报过状态的成员
	// 不同于“已观测到且处于未就绪”的成员，无法区分这两者的诊断也就无法说明究竟发生了哪种情况。
	//
	// +required
	// +k8s:validation:enum=["True","False","Unknown"]
	Ready meta.ConditionStatus `json:"ready" protobuf:"bytes,4,opt,name=ready"`
}

// ModelDeploymentGroupState 是单个分组资格所处的状态。
//
// 前七个资格状态直接沿用 operator 自身的命名与拼写，因为用两套词汇表达同一件事
// 对读取方毫无益处。AwaitingObservation 是第八个状态，也并非第二套词汇：它命名了
// 资格判定本身无法表达的唯一阻滞情况，即分组正在等待有效的 Router 观测。
//
// 它不是 "Unknown"。Unknown 是 operator 用来表示分组被某个无法更细致命名的检查项
// 阻滞的名称，若将其复用于 Router 视图丢失，会导致两种情况呈现相同的读数，而无法区分
// 观测丢失与引擎故障的诊断也无法说明究竟发生了哪种情况。
//
// +enum
type ModelDeploymentGroupState string

const (
	// ModelDeploymentGroupStateEligible 表示所有检查项均已通过且无其他阻滞的分组。
	ModelDeploymentGroupStateEligible ModelDeploymentGroupState = "Eligible"
	// ModelDeploymentGroupStateWithdrawn 表示因明确故障而被撤回的分组。
	ModelDeploymentGroupStateWithdrawn ModelDeploymentGroupState = "Withdrawn"
	// ModelDeploymentGroupStateAwaitingMembers 表示成员集合尚未确立的分组。
	ModelDeploymentGroupStateAwaitingMembers ModelDeploymentGroupState = "AwaitingMembers"
	// ModelDeploymentGroupStateAwaitingReady 表示并非所有成员都处于 Ready=True 的分组。
	ModelDeploymentGroupStateAwaitingReady ModelDeploymentGroupState = "AwaitingReady"
	// ModelDeploymentGroupStateAwaitingProbe 表示无法观测到其前向路径的分组。
	ModelDeploymentGroupStateAwaitingProbe ModelDeploymentGroupState = "AwaitingProbe"
	// ModelDeploymentGroupStateAwaitingObservation 表示所有检查项均已通过、正在等待有效 Router 观测的分组。
	ModelDeploymentGroupStateAwaitingObservation ModelDeploymentGroupState = "AwaitingObservation"
	// ModelDeploymentGroupStateUnsupported 表示不存在对应观测形态的情况。
	ModelDeploymentGroupStateUnsupported ModelDeploymentGroupState = "Unsupported"
	// ModelDeploymentGroupStateUnknown 表示因当前 operator 无法命名的事实而被阻滞的分组。
	ModelDeploymentGroupStateUnknown ModelDeploymentGroupState = "Unknown"
)

// ModelDeploymentRoleParallelismStatus 是从为角色渲染的命令行中读取到的声明并行度。
//
// 在读取参数之前，每个声明的并行度均为 nil，模式集合为空，负载均衡模式为 Unknown，
// 来源为 Unknown（且 complete=false 并附带原因）——这与上述计数字段对未观测数值
// 保持如实反映的原则一致。
type ModelDeploymentRoleParallelismStatus struct {
	// Declared 携带角色参数中所声明的各并行维度值。
	//
	// +optional
	Declared ModelDeploymentParallelismDeclaredStatus `json:"declared" protobuf:"bytes,1,name=declared"`

	// Modes 是角色参数中出现的并行模式，以规范化的引擎模式名称为键。观测到的模式会携带
	// 其显式布尔值；键的缺失不代表 false。在读取参数之前为空。
	//
	// +optional
	// +mapType=atomic
	Modes map[string]bool `json:"modes,omitempty" protobuf:"bytes,2,rep,name=modes"`

	// LoadBalance 是由已声明的并行维度推导出的负载均衡形态。Unknown 表示无法从角色声明中
	// 推导得出；是否受支持的判断记录在 reason 字段中，绝不作为此处的模式值。
	//
	// +k8s:validation:enum=["Internal","External","Hybrid","MultiPort","Unknown"]
	LoadBalance ModelDeploymentLoadBalance `json:"loadBalance" protobuf:"bytes,3,name=loadBalance,casttype=ModelDeploymentLoadBalance"`

	// Source 是本次读取结果的来源。
	//
	// +optional
	Source ModelDeploymentParallelismSourceStatus `json:"source" protobuf:"bytes,4,name=source"`
}

// ModelDeploymentParallelismDeclaredStatus 记录角色参数中声明的各项并行度。
//
// 字段为 nil 表示角色未声明该并行度，绝不会被解读为 0 或 1。显式值会按原样保留，
// 包括显式声明的本地并行度 0，它表达了省略字段无法传达的语义。
type ModelDeploymentParallelismDeclaredStatus struct {
	// +optional
	TensorParallel *int32 `json:"tensorParallel,omitempty" protobuf:"varint,1,opt,name=tensorParallel"`

	// +optional
	PipelineParallel *int32 `json:"pipelineParallel,omitempty" protobuf:"varint,2,opt,name=pipelineParallel"`

	// +optional
	DataParallel *int32 `json:"dataParallel,omitempty" protobuf:"varint,3,opt,name=dataParallel"`

	// +optional
	DataParallelLocal *int32 `json:"dataParallelLocal,omitempty" protobuf:"varint,4,opt,name=dataParallelLocal"`

	// +optional
	PrefillContextParallel *int32 `json:"prefillContextParallel,omitempty" protobuf:"varint,5,opt,name=prefillContextParallel"`

	// +optional
	DecodeContextParallel *int32 `json:"decodeContextParallel,omitempty" protobuf:"varint,6,opt,name=decodeContextParallel"`

	// +optional
	ExpertParallel *int32 `json:"expertParallel,omitempty" protobuf:"varint,7,opt,name=expertParallel"`

	// +optional
	AttentionContextParallel *int32 `json:"attentionContextParallel,omitempty" protobuf:"varint,8,opt,name=attentionContextParallel"`

	// MoEDPSize 是角色声明的专家调度数据并行大小。
	//
	// +optional
	MoEDPSize *int32 `json:"moeDpSize,omitempty" protobuf:"varint,9,opt,name=moeDpSize"`

	// DWDPSize 是角色声明的数据内数据并行大小。
	//
	// +optional
	DWDPSize *int32 `json:"dwdpSize,omitempty" protobuf:"varint,10,opt,name=dwdpSize"`
}

// ModelDeploymentParallelismSourceStatus 是角色并行度读取结果的来源信息。
type ModelDeploymentParallelismSourceStatus struct {
	// Kind 表示读取结果的来源。不可读的来源标记为 Unknown，且 complete=false
	// 并附带原因，绝不会静默赋予默认并行度。
	//
	// +k8s:validation:enum=["ExtraArgs","Command","UnmanagedCommand","Unknown"]
	Kind ModelDeploymentParallelismSourceKind `json:"kind" protobuf:"bytes,1,name=kind,casttype=ModelDeploymentParallelismSourceKind"`

	// Complete 表示本次读取是否涵盖了契约所需读取的全部内容。只有完整的读取结果
	// 才能作为上述声明并行度的可信依据；不完整的读取会将这些字段保持为 nil，而非只填充部分值。
	Complete bool `json:"complete" protobuf:"varint,2,name=complete"`

	// UnreadableReason 说明无法确定的具体内容，例如尚未执行读取，或声明的参数标志在
	// 负载均衡形态上存在冲突。即使来源已完整读取，若某个派生结论无法判定，完整读取结果
	// 仍可能携带此原因。
	//
	// +optional
	UnreadableReason string `json:"unreadableReason,omitempty" protobuf:"bytes,3,opt,name=unreadableReason"`
}

// ModelDeploymentParallelismSourceKind 指定角色并行度读取结果的来源类型。
// +enum
type ModelDeploymentParallelismSourceKind string

const (
	// ModelDeploymentParallelismSourceKindExtraArgs 表示从角色自有的额外参数中读取。
	ModelDeploymentParallelismSourceKindExtraArgs ModelDeploymentParallelismSourceKind = "ExtraArgs"
	// ModelDeploymentParallelismSourceKindCommand 表示从角色提供的完整命令行中读取。
	ModelDeploymentParallelismSourceKindCommand ModelDeploymentParallelismSourceKind = "Command"
	// ModelDeploymentParallelismSourceKindUnmanagedCommand 表示从被替换且 operator
	// 未为其合成任何参数的命令行中读取。
	ModelDeploymentParallelismSourceKindUnmanagedCommand ModelDeploymentParallelismSourceKind = "UnmanagedCommand"
	// ModelDeploymentParallelismSourceKindUnknown 表示该来源未能产生可信的读取结果。
	ModelDeploymentParallelismSourceKindUnknown ModelDeploymentParallelismSourceKind = "Unknown"
)

// ModelDeploymentLoadBalance 是由声明的并行度推导出的负载均衡形态。
// +enum
type ModelDeploymentLoadBalance string

const (
	// ModelDeploymentLoadBalanceInternal 在单个引擎进程内部进行负载均衡。
	ModelDeploymentLoadBalanceInternal ModelDeploymentLoadBalance = "Internal"
	// ModelDeploymentLoadBalanceExternal 在引擎外部进行负载均衡，例如通过
	// 根据 rank 布局推导出的外部负载均衡器。
	ModelDeploymentLoadBalanceExternal ModelDeploymentLoadBalance = "External"
	// ModelDeploymentLoadBalanceHybrid 结合了内部与外部两段负载均衡。
	ModelDeploymentLoadBalanceHybrid ModelDeploymentLoadBalance = "Hybrid"
	// ModelDeploymentLoadBalanceMultiPort 在多个引擎监听端口之间进行负载均衡。
	ModelDeploymentLoadBalanceMultiPort ModelDeploymentLoadBalance = "MultiPort"
	// ModelDeploymentLoadBalanceUnknown 表示该声明无法推导出明确的负载均衡形态。
	ModelDeploymentLoadBalanceUnknown ModelDeploymentLoadBalance = "Unknown"
)

// ModelDeploymentRoleEndpointsStatus 表示单个角色的端点资格状态与实际服务确认结果。
//
// 此处 nil 不等同于 0，反之亦然：eligible 计数为 nil 表示合格集合尚未被观测到，
// eligible 为 0 表示已观测到且为空集，且只有 Confirmed 服务状态才能携带数值。
// 当下方按角色统计的计数处于未观测而非空集状态时，部署级别的 EndpointEligibility condition 会予以说明。
type ModelDeploymentRoleEndpointsStatus struct {
	// Eligible 是该角色合格列表中所包含的端点数量，仅当该列表完整时才设置。
	// 该字段仅表示具备资格，绝不代表已实际提供服务。
	//
	// +optional
	Eligible *int32 `json:"eligible,omitempty" protobuf:"varint,1,opt,name=eligible"`

	// Serving 是针对该角色端点所获取的实际服务确认状态。
	//
	// +optional
	Serving ModelDeploymentServingStatus `json:"serving" protobuf:"bytes,2,name=serving"`
}

// ModelDeploymentServingStatus 是角色端点的实际服务确认结果：即对完整且绑定身份的
// 各个 Router 视图取去重并集。它是实际观测结果，绝非对 eligible 计数的复述。
type ModelDeploymentServingStatus struct {
	// State 是确认结果。只有 Confirmed 表示已观测到并集；其他状态则用于区分
	// 未能提供数值的原因，且它们本身均不编码具体数值。
	//
	// +k8s:validation:enum=["Confirmed","NotConverged","Unknown","NotConfigured"]
	State ModelDeploymentServingState `json:"state" protobuf:"bytes,1,name=state,casttype=ModelDeploymentServingState"`

	// Value 是已确认的服务中端点数，仅当 State 为 Confirmed 时出现；其中的显式 0 值
	// 与其他任何数值一样具有真实含义。
	//
	// +optional
	Value *int32 `json:"value,omitempty" protobuf:"varint,2,opt,name=value"`
}

// ModelDeploymentServingState 是各 Router 服务视图并集的确认结论。
// +enum
type ModelDeploymentServingState string

const (
	// ModelDeploymentServingStateConfirmed 表示完整、新鲜且身份匹配的视图对 Value 达成一致。
	ModelDeploymentServingStateConfirmed ModelDeploymentServingState = "Confirmed"
	// ModelDeploymentServingStateNotConverged 表示完整视图对目标状态存在分歧。
	ModelDeploymentServingStateNotConverged ModelDeploymentServingState = "NotConverged"
	// ModelDeploymentServingStateUnknown 表示所需的某个视图缺失、过期或无法确定。
	ModelDeploymentServingStateUnknown ModelDeploymentServingState = "Unknown"
	// ModelDeploymentServingStateNotConfigured 表示该部署完全未声明 router，
	// 因此不存在结果缺失的 Router 进程。
	ModelDeploymentServingStateNotConfigured ModelDeploymentServingState = "NotConfigured"
)

// ModelDeploymentKVCacheStatus 表示当前部署所关联的 KVCache 复用域。
//
// 所有字段均从 Binding 读取，绝不在此处声明。将这些信息回显到当前对象上，使得排查缓存
// 未共享问题时只需查看一个对象而非两个；错误的 block size 或 dtype 会导致静默缓存污染，
// 即写入和读取均能成功，但张量数据却是错误的。
type ModelDeploymentKVCacheStatus struct {
	// Binding 是当前部署在同一 Namespace 下解析到的 KVCachePoolBinding。
	//
	// +required
	Binding string `json:"binding" protobuf:"bytes,1,name=binding"`

	// Pool 是 Binding 所指向的 KVCachePool。
	//
	// +required
	Pool string `json:"pool" protobuf:"bytes,2,name=pool"`

	// Domain 是复用标识，从 Binding 的不可变 domain 块中回显。
	//
	// +required
	Domain ModelDeploymentKVCacheDomain `json:"domain" protobuf:"bytes,3,name=domain"`
}

// ModelDeploymentKVCacheDomain 是从 Binding 回显的复用标识。
type ModelDeploymentKVCacheDomain struct {
	// Name 是复用标识，也是存储层的租户标识。回显相同名称的两个部署共享 KV，
	// 回显不同名称的两个部署则不共享。
	//
	// +required
	// +k8s:validation:maxLength=63
	Name string `json:"name" protobuf:"bytes,1,name=name"`

	// BlockSize 和 Dtype 定义了该复用域中数据块的构成规格。此处仅做回显而不做校验：
	// Binding 已强制要求并冻结这两项，因此若某个条目缺失其中之一，只可能是写入方的 bug。
	//
	// +required
	BlockSize int32 `json:"blockSize" protobuf:"varint,2,name=blockSize"`

	// +required
	Dtype string `json:"dtype" protobuf:"bytes,3,name=dtype"`
}

// ModelDeploymentRouterStatus 是配置 router 所依据的契约。
//
// 此处的每一个字符串都是 operator 实际渲染出的值，绝非文档中记录的默认值。
// 因此，读取本对象的消费方与 operator 配置的 router 不会在 selector、topic 或指标名称上
// 产生分歧；这正是发布本对象的价值所在，而非重复文档已有的说明。
type ModelDeploymentRouterStatus struct {
	// Name 回显 spec 中的配置，使仅持有本对象的读取方也能知晓下方契约是针对哪种实现生成的。
	//
	// +required
	Name string `json:"name" protobuf:"bytes,1,name=name"`

	// Endpoint 是 router 自身的地址，在 router 的 Service 拥有地址后提供。
	//
	// +k8s:validation:maxLength=512
	Endpoint string `json:"endpoint,omitempty" protobuf:"bytes,2,opt,name=endpoint"`

	// PoolEndpoint 是当前部署所关联的 KVCache 池地址，格式与其客户端所需形式一致。
	// 需要查询缓存池的 router 会用到该地址，operator 已预先完成从 Binding 中的解析工作。
	//
	// +k8s:validation:maxLength=512
	PoolEndpoint string `json:"poolEndpoint,omitempty" protobuf:"bytes,3,opt,name=poolEndpoint"`

	// Roles 为每个已声明的角色对应一个条目，其键集合与角色集合完全一致。router 会自行
	// 发现存活的副本，但它无法自行区分哪个 selector 对应解耦配对中的哪一侧。
	//
	// +listType=map
	// +listMapKey=name
	Roles []ModelDeploymentRouterRoleStatus `json:"roles,omitempty" protobuf:"bytes,4,rep,name=roles"`

	// Metrics 是各角色暴露的服务指标及其提供端口。它们作用于整个部署，
	// 因为这是引擎的属性，而引擎是部署级别的字段。
	Metrics *ModelDeploymentRouterMetrics `json:"metrics,omitempty" protobuf:"bytes,5,opt,name=metrics"`
}

// ModelDeploymentRouterRoleStatus 是 router 视角下的单个角色信息。
type ModelDeploymentRouterRoleStatus struct {
	// Name 是角色名称，与 spec.roles[].name 对应。
	Name string `json:"name" protobuf:"bytes,1,name=name"`

	// Kind 是角色的有效类型，用于区分 prefiller 与 decoder。此处提供解析后的值而非原始字段，
	// 因为未指定 kind 的角色默认为 server，消费方若按空字符串匹配将找不到任何结果。
	Kind ModelDeploymentRoleKind `json:"kind" protobuf:"bytes,2,name=kind,casttype=ModelDeploymentRoleKind"`

	// Selector 是精确匹配该角色下响应 API 的 Pod 的标签选择器——每个副本对应一个成员即其 leader
	// （当副本大小为 1 时即为该副本唯一的 Pod）——按原样发布，以便 router 基于实际观测到的
	// 字符串进行配置，而非依赖文档约定。
	//
	// 它面向的单元是 Pod 而非副本：一个副本可能跨多个 Pod，尽管角色在运行非 leader 成员，
	// 但它们不提供 API 服务，因此不会落入此 selector 的匹配范围。
	//
	// 使用 selector 的 router 在扩缩容时无需调整即可持续工作；而使用地址列表的 router 则无法适应，
	// 每次角色扩容或缩容都必须重新配置并重启。
	Selector map[string]string `json:"selector,omitempty" protobuf:"bytes,3,rep,name=selector"`

	// Endpoint 是该角色自身的地址。无论部署前端是否有 router，该地址均保持可达，
	// 以便在调试时可以直接访问配对中的单侧角色。
	//
	// +k8s:validation:maxLength=512
	Endpoint string `json:"endpoint,omitempty" protobuf:"bytes,4,opt,name=endpoint"`

	// KVEvents 是该角色发布缓存事件的位置；若角色配置为不发布，则此字段不存在。
	KVEvents *ModelDeploymentRouterKVEvents `json:"kvEvents,omitempty" protobuf:"bytes,5,opt,name=kvEvents"`
}

// ModelDeploymentRouterKVEvents 是消费方可访问的单个角色的缓存事件流。
//
// 绑定地址并非可拨号连接地址。引擎配置中使用的是发布端所绑定的地址，其中不包含主机信息；
// 而此处提供的是消费方用于连接的地址。若在此发布绑定字符串，则该值仅在发布 Pod 内部有效，
// 在其他任何地方都无法使用。
type ModelDeploymentRouterKVEvents struct {
	// Endpoint 是消费方订阅的事件流地址。
	//
	// +k8s:validation:maxLength=512
	Endpoint string `json:"endpoint" protobuf:"bytes,1,name=endpoint"`

	// ReplayEndpoint 是较晚加入的消费方用来请求其错过事件的端点。若消费方未连接该端点，
	// 面对非空缓存时其初始视图将为空。
	//
	// +k8s:validation:maxLength=512
	ReplayEndpoint string `json:"replayEndpoint,omitempty" protobuf:"bytes,2,opt,name=replayEndpoint"`

	// Topic 是发布端配置使用的 topic。消费方若订阅了不同的 topic，将收不到任何事件且不会报错。
	Topic string `json:"topic,omitempty" protobuf:"bytes,3,opt,name=topic"`
}

// ModelDeploymentRouterMetrics 是 router 用于评分的服务指标。
//
// 指标名称采用显式发布而非假定，因为它们取决于具体引擎，而当前 operator 明确知晓
// 其配置的是哪种引擎。若 router 使用了一个该引擎并未暴露的名称，它会对所有副本给出
// 相同的评分且不会报任何错误。
//
// 此处存在该字段仅表示 operator 配置了会暴露这些指标的引擎，并不代表它们在 router
// 运行的位置一定可达——而且也无法保证这一点，因为角色可以指定任意镜像。
type ModelDeploymentRouterMetrics struct {
	// Port 是提供指标服务的端口号。其下界约束并非装饰：与旁边的名称字段不同，
	// 这是消费方实际拨号连接的数值，若为 0 只会在连接阶段才失败。
	//
	// +required
	// +k8s:validation:minimum=1
	Port int32 `json:"port" protobuf:"varint,1,name=port"`

	// QueuedRequests 指定保存引擎等待准入请求的指标名称。
	QueuedRequests string `json:"queuedRequests" protobuf:"bytes,2,name=queuedRequests"`

	// RunningRequests 指定保存引擎当前正在处理请求的指标名称。
	RunningRequests string `json:"runningRequests" protobuf:"bytes,3,name=runningRequests"`

	// KVCacheUtilization 指定保存引擎 KVCache 使用率的指标名称。
	KVCacheUtilization string `json:"kvCacheUtilization" protobuf:"bytes,4,name=kvCacheUtilization"`
}

// ModelDeploymentList 包含 ModelDeployment 列表。
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ModelDeploymentList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Items []ModelDeployment `json:"items" protobuf:"bytes,2,rep,name=items"`
}

var _ runtime.Object = (*ModelDeploymentList)(nil)
