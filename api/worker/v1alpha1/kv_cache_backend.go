package v1alpha1

import (
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	gpustack "gpustack.ai/gpustack/api/v1"
)

// KVCacheBackend 是 worker.gpustack.ai 的 schema。
//
// 它声明哪些机器向一个 KV 缓存后端贡献何种存储介质，并报告该后端的观测状态。
// 它属于集群作用域（cluster-scoped）资源，因为它是特权物理资源：会指定 Node、申请主机内存与
// 主机路径，且在 RDMA 路径上需要 hostNetwork 和 /dev/infiniband。租户隔离属于另一个维度，
// 由上层负责。
//
// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:crd-gen:resource:scope="Cluster",categories=["gpustack"],shortName=["kvcb"],subResources=["status"]
// +k8s:crd-gen:printcolumn:name="Type",type="string",jsonPath=".spec.type"
// +k8s:crd-gen:printcolumn:name="Phase",type="string",jsonPath=".status.phase"
// +k8s:crd-gen:printcolumn:name="Endpoint",type="string",jsonPath=".status.endpoints[?(@.name=='Client')].address"
// +k8s:crd-gen:printcolumn:name="Capacity",type="string",jsonPath=".status.capacity.total"
// +k8s:crd-gen:printcolumn:name="Age",type="date",jsonPath=".metadata.creationTimestamp"
type KVCacheBackend struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Spec   KVCacheBackendSpec   `json:"spec" protobuf:"bytes,2,name=spec"`
	Status KVCacheBackendStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

var _ runtime.Object = (*KVCacheBackend)(nil)

// KVCacheBackendSpec 定义 KVCacheBackend 的期望规格。
type KVCacheBackendSpec struct {
	// Type 表示后端实现，决定数据放置、驱逐、副本复制与元数据的管理方式。
	// 它不是存储介质；数据的实际存放位置由 members[].medium 指定。
	//
	// 当前仅提供一个显式枚举值而非隐式假定，以便后续引入第二种实现时只需扩展枚举，
	// 而不必重新解释缺失字段的语义。
	//
	// +k8s:validation:default="Mooncake"
	// +k8s:validation:enum=["Mooncake"]
	Type string `json:"type,omitempty" protobuf:"bytes,1,opt,name=type"`

	// Image 是该后端所有角色运行的容器镜像。
	//
	//   - 若未设置，则使用集群范围的 "kv-cache-backend-image" Setting（通常配置本项目已验证的版本）。
	//     若在此处设置，则仅针对当前后端覆盖该 Setting。
	//   - 若两处均未设置，准入阶段将拒绝请求并在错误中同时指明两者。
	//   - 该镜像不会像 Device Manager 那样从 operator 自身镜像推导：master 与推理引擎客户端可能基于
	//     不同代的加速器构建，自动推导镜像会导致 master 与无法加载的运行时隐式配对。
	//
	// +k8s:validation:maxLength=512
	Image string `json:"image,omitempty" protobuf:"bytes,2,opt,name=image"`

	// ImagePullPolicy 是该后端所有角色拉取镜像时使用的策略。
	//
	//   - 若未设置，operator 会按 API server 的默认规则从镜像 tag 解析：对于 :latest 或未指定 tag 的镜像
	//     解析为 Always，其余情况解析为 IfNotPresent。每当镜像或本字段变更时都会重新解析；
	//     显式解析而非留空是为了确保渲染出的工作负载可与之对比一致。
	//   - 该字段不会继承集群范围的 "image-pull-policy" Setting，后者仅用于内置应用 chart 安装，
	//     不会作用于控制器渲染的资源。
	//
	// +k8s:validation:enum=["Always","IfNotPresent","Never"]
	ImagePullPolicy core.PullPolicy `json:"imagePullPolicy,omitempty" protobuf:"bytes,5,opt,name=imagePullPolicy,casttype=k8s.io/api/core/v1.PullPolicy"`

	// ImagePullSecrets 指定该后端所有角色拉取镜像所用的 Secret 名称。这些 Secret 位于工作负载
	// 运行的 Namespace（即本 operator 所在 Namespace）中。
	//
	//   - 若缺少该配置，将无法访问私有镜像仓库：没有任何角色使用携带凭据的 operator ServiceAccount，
	//     且集群范围的 "image-pull-secrets" Setting 仅作用于内置应用 chart。
	//   - 该列表采用原子替换语义（整体替换而非合并）。结构化 schema 仅能基于必填且非空的字段对列表
	//     建键，而 LocalObjectReference 的 name 两者均不满足。
	//
	// +listType=atomic
	// +k8s:validation:maxItems=32
	ImagePullSecrets []core.LocalObjectReference `json:"imagePullSecrets,omitempty" protobuf:"bytes,6,rep,name=imagePullSecrets"`

	// Connection 描述如何访问该后端：由本 operator 托管或接入外部后端。
	// 两者必须且只能设置其一，由 Webhook 强制校验。
	//
	// +required
	Connection KVCacheBackendConnection `json:"connection" protobuf:"bytes,3,name=connection"`

	// Transport 描述存储成员使用的数据面传输配置。
	//
	// 其默认值为空对象，且必须如此：结构化 schema 的默认值填充不会深入缺失的对象，因此如果 spec
	// 完全未提及 transport，将不会存储任何 protocol，进而导致 protocol 自身的默认值静默失效。
	//
	// +k8s:validation:default={}
	Transport KVCacheBackendTransport `json:"transport,omitempty" protobuf:"bytes,4,opt,name=transport"`
}

// KVCacheBackendConnection 定义访问后端的方式。必须且只能设置其中一个分支；Webhook 会拒绝
// 同时设置或均未设置的情况，因为空分支无法描述任何后端，而双分支则指向两个不同的后端。
type KVCacheBackendConnection struct {
	// Managed 表示由本 operator 部署并管理 leader 与存储成员。
	Managed *KVCacheBackendManaged `json:"managed,omitempty" protobuf:"bytes,1,opt,name=managed"`

	// External 指定由外部运维的后端。控制器不会为其渲染任何资源，reconciler 仅执行观测。
	External *KVCacheBackendExternal `json:"external,omitempty" protobuf:"bytes,2,opt,name=external"`
}

// KVCacheBackendManaged 描述由 operator 管理的后端形态：包含 leader 以及提供存储介质的成员组。
type KVCacheBackendManaged struct {
	// Leader 是所有成员与客户端通信的元数据服务。Leader 是本 API 中的命名，而非制品自身的命名：
	// 渲染出的启动参数与环境变量仍保留厂商原生的 "master" 拼写，两者映射由渲染器负责。
	//
	// +required
	Leader KVCacheBackendLeader `json:"leader" protobuf:"bytes,1,name=leader"`

	// Members 定义存储成员组列表。每一项用于选择节点、指定节点贡献的存储介质，并可在同一批节点上
	// 附加本地磁盘层。
	//
	// 成员组在本列表中的位置即为其身份标识：DaemonSet 名称、不可变 selector 标签以及成员的 HTTP
	// 端口均由该位置派生。这里本可以为成员组单独命名但刻意未这样做——DaemonSet 的 selector 不可变，
	// 若改用名称标识，重命名会导致所有成员被删除重建并丢失全部缓存。
	//
	//   - 允许在列表末尾追加成员组、从末尾移除成员组以及原地修改成员组（包括放宽 nodeSelector
	//     以扩充节点）。
	//   - 准入阶段会拒绝将成员组移动到其他位置：这会改变后续所有位置的含义，并使对应成员按另一组
	//     的 spec 重建，导致缓存丢失。
	//   - 有两种情况已知无法捕获，因为在没有独立名称时无法与普通编辑区分：调整顺序的同时修改同一
	//     成员组，以及移除一个成员组并由后续完全相同的成员组替补。这两种情况都会在列表中留下两个
	//     相同的成员组。
	//   - 若需下线某个成员组而不将其从列表中移除，可收窄其 nodeSelector 直至不匹配任何节点。
	//     该组将保留原有位置且不会触发任何重建。
	//
	// 上限 32 是安全边界，并非有效组数的上限：端口派生在 57455 组以内均有效，但不设上限的列表
	// 可能会渲染出超出合法范围的端口且没有任何报错。
	//
	// +required
	// +k8s:validation:minItems=1
	// +k8s:validation:maxItems=32
	// +listType=atomic
	Members []KVCacheBackendMember `json:"members" protobuf:"bytes,2,rep,name=members"`

	// ScaleIn 定义成员退出缩容时的行为。若未设置，成员将按普通 Pod 的方式停止：收到 SIGTERM 并
	// 等待其自身停机所需时间，不会等待正在读取其数据的读端完成。
	ScaleIn *KVCacheBackendScaleIn `json:"scaleIn,omitempty" protobuf:"bytes,3,opt,name=scaleIn"`
}

// KVCacheBackendScaleIn 定义成员退出时的行为。
//
// 该结构体仅包含时长而非策略枚举。唯一可能的另一种策略（在成员退出前迁移其数据）依赖存储的
// drain job API，但该 API 仅覆盖内存和 NVMe-oF 副本，且会在未处理磁盘层的情况下报告成功，
// 因此策略字段目前实际上只有一个可选值。待存在两种可用策略时再引入枚举；后续扩展枚举不构成
// 破坏性变更。
type KVCacheBackendScaleIn struct {
	// GracePeriodSeconds 是 operator 要求退出成员在注销其本地磁盘层后等待的时长。该值会作为端点
	// 的 grace_period_seconds 参数渲染到 preStop hook 中，其他任何位置均不读取它。这里使用普通
	// int32，因为未设置与零值语义相同：注销磁盘层后立即退出而不等待。
	//
	//   - 退出时仅会注销磁盘层。内存段会直接丢弃而非排空，因此缩容成员组会丢失其持有的内存缓存——
	//     对于内容可重新计算的缓存而言，这属于性能代价而非故障。
	//   - 没有任何 hook 会排空内存段，因为成员本身会拒绝卸载它。基于 Mooncake 0.3.13 在本
	//     operator 渲染的成员上实测：无论尝试何种宽限期，使用 leader 为该内存段发布的 id 调用
	//     成员的两个卸载接口均对启动参数请求挂载的内存段返回 500，且两个报错分别指明检索了不同的
	//     记录集；而在同一成员的同一会话中，同一接口对通过成员自身挂载接口挂载的内存段返回 200。
	//     因此这并非成员身份识别问题：即使传入完全精确的自有段 id 同样会被拒绝，在此提供 Pod 地址
	//     或 client id 等任何标识均无法改变这一结果。
	//   - 该等待不会保持磁盘层处于开放状态，因此若为等待正在进行的对端读取完成而调大该值，实际并
	//     不会生效。基于 Mooncake 0.3.13 实测：注销操作立即生效，随后进程无条件等待完整时长，
	//     导致对端在整个窗口期内读取磁盘键都会直接未命中，而非仅在窗口结束后未命中。其他后端镜像
	//     可能有不同表现；本 operator 仅保证按配置发送该值。
	//   - Pod 的 terminationGracePeriodSeconds 由本字段派生而非并行设置，以确保 kubelet 不会在
	//     等待期间提前终止容器。
	//   - 设置本字段无法保护同时触发缩容的那次编辑：Pod 按创建时的模板运行，因此退出成员仍沿用
	//     其启动时的宽限期配置。若要使宽限期在缩容时生效，请先仅修改本字段并等待成员重建完成
	//     （其 pod-spec-hash annotation 会更新），然后再收窄 selector。
	//   - 该字段的上限由入口接口自身约束，超出上限的值会被其以 HTTP 400 拒绝。
	//
	// +k8s:validation:minimum=0
	// +k8s:validation:maximum=3600
	GracePeriodSeconds int32 `json:"gracePeriodSeconds,omitempty" protobuf:"varint,1,opt,name=gracePeriodSeconds"`
}

// KVCacheBackendExternal 表示非本 operator 运行的外部后端。
//
// 两个此类对象可能指向同一个后端，而系统不会察觉。同一个 leader 可能通过多种地址拼写访问，
// 本 operator 刻意不在对象间比对地址：所有可用于比对的标识要么本身可编辑，要么要求 leader
// 在准入阶段即可达。避免两个对象指向同一个 leader 是使用方的责任；一旦发生冲突，会出现以下
// 三类问题，且没有任何对象会在创建时被拒绝：
//
//   - 共享复用域的配额会来回覆盖而无法收敛。唯一性仅在池指向同一 backend 对象的 Binding 之间
//     强制校验，因此通过两个 backend 对象访问同一 leader 的两个 Binding 都会以相同的域名称
//     通过准入，且各自池的 reconciler 在每次调谐时都会用自身的 quota.ceiling 覆盖对方。
//   - 配额偏小只会表现为命中率下降，而无其他提示。超出配额不会拒绝写入：存储会丢弃该租户自身
//     较旧的对象腾出空间并不可逆地重试，且不会更新任何计数器。
//   - 两个使用相同 domain.name 但 blockSize 或 dtype 不同的 Binding 会互相破坏对方的数据块——
//     这是三类问题中唯一会导致结果错误（而非仅性能下降）的问题。传递给推理引擎的复用标识只有
//     domain 名称本身，因此两种不同结构的缓存会落入同一标识下：读写均成功，但张量数据错误。
type KVCacheBackendExternal struct {
	// Endpoints 是外部运维后端的访问地址列表，每个具名角色对应一项。两个角色均为必填：本 operator
	// 读取 Admin 地址并发布 Client 地址，因此若外部后端仅指定其中一个，将导致指标抓取或所有推理
	// 引擎无地址可用。采用列表而非单个地址是为了在未来描述多 leader 后端时无需变更 API。
	//
	// 因此当前该列表固定包含两项，即一个 Client 与一个 Admin：条目按 name 建键，name 共两个取值，
	// 准入阶段会拒绝缺少任一角色的列表。
	//
	// +required
	// +k8s:validation:minItems=1
	// +listType=map
	// +listMapKey=name
	Endpoints []KVCacheBackendEndpoint `json:"endpoints" protobuf:"bytes,1,rep,name=endpoints"`
}

// KVCacheBackendEndpoint 可使用的名称常量。定义为常量是因为三处逻辑必须保持一致——管理员为
// 外部后端编写的对象、reconciler 为托管后端发布的端点，以及所有选用端点的消费方——任一处拼写
// 错误都会导致连接到无人监听的地址。
const (
	// KVCacheBackendEndpointNameClient 是供推理引擎连接的端点名称。
	KVCacheBackendEndpointNameClient = "Client"
	// KVCacheBackendEndpointNameAdmin 是供本 operator 读取的端点名称：同一端口同时提供
	// Prometheus 指标暴露与 HTTP 管理接口。
	KVCacheBackendEndpointNameAdmin = "Admin"
)

// KVCacheBackendEndpoint 表示后端的一个具名地址。同一类型同时用于 external 分支的输入与
// status 中的输出，便于使用者理解统一的数据结构。
type KVCacheBackendEndpoint struct {
	// Address 为 host:port 格式的地址。
	//
	// 长度上限为 259 而非 253：限制针对整个 host:port 字符串，而仅 host 部分就可能是长达 253 个
	// 字符的 DNS 子域名，因此需要为冒号与五位端口号预留空间。若设为 253，schema 会拒绝符合
	// Webhook 自身 host:port 规则的地址。
	//
	// +required
	// +k8s:validation:maxLength=259
	Address string `json:"address" protobuf:"bytes,1,name=address"`

	// Name 指明该地址的用途。Client 供推理引擎连接；Admin 为同时提供 Prometheus 指标暴露与
	// HTTP 管理接口的端口，由本 operator 读取。若消费方拿到错误端点会在连接阶段直接失败且难以定位，
	// 因此在 API 中显式区分而非依赖约定。
	//
	// +required
	// +k8s:validation:enum=["Client","Admin"]
	Name string `json:"name" protobuf:"bytes,2,name=name"`
}

// KVCacheBackendLeader 定义 leader 进程配置：包括副本数、新写入的分配策略，以及用于传递本
// API 未显式枚举参数的扩展入口。
type KVCacheBackendLeader struct {
	// Replicas 是运行的 leader 进程数，同一时刻仅有一个主节点提供服务，其余均为备用节点：它们不
	// 持有数据也不响应请求，仅用于故障接管。
	//
	//   - 副本数大于 1 时，ElectionBackend 必须为 Kubernetes。Webhook 会拒绝多副本搭配 None，
	//     若准入不可用，渲染器会将其限制为 1。
	//   - 修改副本数不会改变选主模式。默认的 Kubernetes 模式即使在单副本下也会运行选主，因此向上
	//     扩容不会重启首个 leader。
	//   - 增加副本数不会提升存储容量（容量由 members 提供）。此处设置上限旨在避免误解其可扩容，
	//     且故意在 Webhook 中保留了相同上限：当未安装 Webhook 时（此时额外的 leader 会被直接
	//     渲染而非被拒绝），这里的限制依然生效。如需调大上限请同步修改两处；放宽最大值不属于
	//     破坏性变更。
	//
	// +k8s:validation:default=1
	// +k8s:validation:minimum=1
	// +k8s:validation:maximum=5
	Replicas *int32 `json:"replicas,omitempty" protobuf:"varint,1,opt,name=replicas"`

	// MemberAddressing 选择开启选主后成员发现 master 的方式。两种方式通过不同路径连接到当前提供
	// 服务的 leader。修改该字段会滚动重建所有成员组。当 ElectionBackend 为 None 时，即使本字段
	// 设为 Lease，成员仍使用 Service。
	//
	//   - Lease：成员自行读取 Lease 当前的持有者，这需要访问 API server 的权限。
	//   - Service：成员通过 leader Service 访问，该 Service 仅发布就绪端点。备用节点处于未就绪
	//     状态；跨选主切换时的客户端重连行为仍需验证。
	//
	// 默认值为 Service。在一次故障切换对比测试中，两种方式耗时相差 0.13 秒，处于单次运行的误差
	// 范围内；两者均在 31.41 秒首次出现失败并在约 60.6 秒收敛，说明该对比主要由选主本身主导。
	// 调整选主时间参数后应重新验证。Service 路径的端点切换过程系根据结果推断，未直接观测。
	//
	// +k8s:validation:default="Service"
	// +k8s:validation:enum=["Lease","Service"]
	MemberAddressing string `json:"memberAddressing,omitempty" protobuf:"bytes,2,opt,name=memberAddressing"`

	// ElectionBackend 选择 leader 选主后端。Kubernetes 模式即使在单副本下也使用 Lease，因此扩容
	// 不会改变首个 leader 的启动参数。None 适用于镜像不支持 Kubernetes 选主的单副本场景；多副本
	// 搭配 None 会被拒绝。后续支持其他后端时可扩展该枚举。
	//
	// 开启 MultiTenancy 时，故障切换会导致最多一个 KVCachePool 调谐周期内的命中率下降。每个副本
	// 仅在自身启动时初始化租户配额策略，因此若在配额调高后发生备用节点接管，新主节点会沿用较旧、
	// 较低的配额上限；超出配额的写入会不可逆地驱逐该租户自身较旧的对象，且不会更新任何计数器。
	// 配额本身不会丢失：pool reconciler 会在下次调谐时把差额重新写回。
	//
	// +k8s:validation:default="Kubernetes"
	// +k8s:validation:enum=["None","Kubernetes"]
	ElectionBackend string `json:"electionBackend,omitempty" protobuf:"bytes,7,opt,name=electionBackend"`

	// AllocationStrategy 定义 leader 选择由哪个成员承接新写入的策略。Random 均匀分散写入；
	// FreeRatioFirst 优先选择可用空间比例更高的成员。
	//
	// 本枚举刻意仅保留任意池化存储通用的这两个选项，而非囊括当前制品参数所接受的全部取值：其余
	// 取值均绑定特定存储介质或局部性模型，可通过 ExtraArgs 传入，若纳入则会使本 API 与单一实现
	// 的术语耦合。后续扩展该枚举不构成破坏性变更。
	//
	// +k8s:validation:default="FreeRatioFirst"
	// +k8s:validation:enum=["Random","FreeRatioFirst"]
	AllocationStrategy string `json:"allocationStrategy,omitempty" protobuf:"bytes,3,opt,name=allocationStrategy"`

	// MultiTenancy 用于开启 leader 的按租户配额账本及其底层的租户隔离分片索引。关闭时，所有请求
	// 都会落入同一个默认租户且索引退化为普通 key 哈希，导致使用不同租户名的两个调用方会读到彼此的
	// 缓存。
	//
	// 将其设为独立字段而非放在 extraArgs 中，是因为其他 API 需要依据它做校验：在未启用配额账本的
	// 后端上创建 KVCachePool 时，准入虽会通过但会发出警告提示按租户配额未生效；若池已绑定某后端，
	// 从该后端撤回此开关会被拒绝；而且让 Webhook 去解析无 schema 约束的 "true"、"1" 或 "True"
	// 会越权判断用户输入的值域。相反，存储全局的 -quota_bytes 参数保留在 extraArgs 中，因为没有
	// 其他 API 需要解析它。
	//
	// 本字段默认为 true，因为只有启用账本，本 API 其余字段的语义才能真正成立：若不启用，
	// KVCachePoolBinding 的上限只会被记录而不会强制执行，且 master 仅能服务单一复用域，第二个
	// Binding 将被拒绝。Mooncake 自 0.3.12 起支持该开关，默认存储镜像版本为 0.3.13.post1。
	//
	// 省略本键与显式写入 `multiTenancy: false` 语义不同——前者采用默认值，后者表示停用账本：仅当
	// 显式设为 false 时才不渲染该开关。早于 Mooncake 0.3.12 的存储镜像不识别此开关且会导致
	// master 启动退出，因此使用此类镜像（包括本项目同时发布的 0.3.10.post2 变体）的后端需在此
	// 显式设为 false。请通过 KVCacheBackendLeader.MultiTenancyEnabled 读取该配置。
	//
	// +k8s:validation:default=true
	MultiTenancy *bool `json:"multiTenancy,omitempty" protobuf:"varint,4,opt,name=multiTenancy"`

	// ExtraArgs 将本 API 未枚举的命令行参数直接透传给 leader，排在派生参数之后。每项均为独立的
	// 参数 token（"-flag" 或 "-flag=value"），并按书写顺序原样渲染。若某项的键（去掉前导短横线后、
	// 第一个 "=" 之前的部分）与上述字段渲染出的参数冲突，则会在准入时被拒绝，因为同一参数有两个来源
	// 会导致渲染出的命令产生歧义。
	//
	// 此处的每个值都是全局可读的：它会原样存储在此集群作用域对象上，随后渲染进 leader 容器的 argv，
	// 在对象生命周期内对所有能访问该 Pod 或 Deployment 的人可见。凭据不应放在此处；由于本 operator
	// 不会渲染任何携带凭据的参数，此字段是凭据进入的唯一途径。
	//
	// +listType=atomic
	ExtraArgs []string `json:"extraArgs,omitempty" protobuf:"bytes,5,rep,name=extraArgs"`

	// ExtraEnv 将本 API 未枚举的环境变量直接透传给 leader 容器。leader 会从环境变量而非命令行参数
	// 读取少量配置（例如 store 的本地快照路径），当其中某些配置需要用到而本 API 未提供对应字段时，
	// 可通过此字段透传。
	//
	// 本 operator 已渲染的变量名会在准入时被拒绝，原因与 ExtraArgs 键冲突相同：Kubernetes 允许容器
	// 中同一变量名出现两次，并将最终生效值交由运行时决定，从而导致冲突甚至不会被报告。
	//
	// 此处的每个值都是全局可读的：它会原样存储在此集群作用域对象上，随后渲染进 leader 容器的环境变量，
	// 在对象生命周期内对所有能访问该 Pod 或 Deployment 的人可见。凭据不应放在此处；由于本 operator
	// 不会渲染任何携带凭据的变量，此字段是凭据进入的唯一途径。
	//
	// +listType=map
	// +listMapKey=name
	ExtraEnv []InstanceEnvVar `json:"extraEnv,omitempty" protobuf:"bytes,6,rep,name=extraEnv"`
}

// MultiTenancyEnabled 返回 leader 是否启用租户账本（tenant ledger）运行。未设置字段按 schema
// 默认值 true 处理，因此未经过 API server 的对象（例如测试中或 fake client 构造的对象）与经过
// API server 的对象返回一致的结果。
func (in KVCacheBackendLeader) MultiTenancyEnabled() bool {
	return in.MultiTenancy == nil || *in.MultiTenancy
}

// KubernetesElectionEnabled 为未经 API server 默认值填充构造的对象应用 schema 默认值。
func (in KVCacheBackendLeader) KubernetesElectionEnabled() bool {
	return in.ElectionBackend == "" || in.ElectionBackend == "Kubernetes"
}

// KVCacheBackendTransport 是 member 使用的数据面配置。
type KVCacheBackendTransport struct {
	// Protocol 指定要求 member 使用的传输协议。Auto 会解析为 TCP。
	//
	// 这些取值并非 store 自身的拼写。此处写入的值在传给 member 前会先经过转换，其中 8 个枚举值中有
	// 两个会彻底更换用词：CANN 渲染为 ascend，ROCM 渲染为 hip。因此，member 的环境变量、日志以及下文的
	// status.members[].protocol 报告的都是 store 的小写拼写，而非此处所写的值，将两者直接作为字符串
	// 比较会产生看似不同实则一致的差异。
	//
	//   - TCP 是通用兜底选项。RDMA、EFA、CANN、ROCM、MUSA 与 MACA 彼此处于同一层级，各自代表特定网络结构
	//     （fabric）或厂商专用的快速路径，而非 TCP 的别称：尤其是 EFA 通过 libfabric 的 SRD provider
	//     访问，不存在 RC queue pair，因此 RDMA 传输无法驱动它。MUSA 与 MACA 属于节点内 IPC 传输，并非
	//     主机网络结构（host fabric）：它们不需要 hostNetwork、capabilities 或设备资源。
	//   - member 最终是否按所请求协议启动，无法通过本 API 观测。status.members[].protocol 只是回显此处
	//     的请求值，而非报告实际结果；因此即使 member 在服务期间已回退为 store 的 tcp，该状态字段仍会
	//     显示为原网络结构名称。只有 member 自身的日志会说明数据面实际安装了哪种传输。
	//   - Auto 刻意不设计为按节点探测并自动升级为更快网络结构：每个 member 组渲染为单个 DaemonSet，其
	//     唯一的 Pod 模板无法针对不同节点携带不同传输协议，且升级到 RDMA 会授予 hostNetwork 和两项
	//     capabilities——特权必须显式请求，绝不由 operator 代为推断。
	//   - 进入此枚举意味着经实测已编译进 member 可运行的构建产物，这也是排除该产物配置解析器所接受的
	//     另外 8 个字符串的原因。这并不意味着已实测可实际传输字节：目前只有 TCP 经过了端到端验证。
	//     这也并不意味着本项目会发布携带该支持的镜像——MUSA 和 MACA 被刻意保留在枚举中，但在
	//     pack/mirrored-mooncake 中没有对应变体，因此使用这两者的组需要指定自己的镜像。该规则排除的是
	//     既无项目镜像变体、又无可用自建镜像的值；仅缺少项目变体本身并不构成排除理由。
	//   - 主机网络结构需要两项本 API 无法检查的前提：member 镜像必须携带其传输所链接的运行时（CANN
	//     需要 CANN toolkit，EFA 需要 libfabric），且 Node 必须运行对应的 device plugin，因为仅靠
	//     hostPath 挂载仍会被设备 cgroup 拒绝打开设备。生效的协议决定网络结构资源族；member 组的
	//     接口数量决定其请求量，对于 RDMA 还决定使用共享资源还是独占资源。
	//   - 修改此枚举的拼写存在已知且可接受的残留风险，与 Medium 枚举的情况相同。这些值曾为 Auto、TCP、
	//     RDMA、EFA、HIP 和 Ascend；HIP 与 Ascend 已被移除，替换为工具链名称 ROCM 与 CANN，其余值也
	//     更改了大小写。存储有旧值的对象将无法删除，因为 schema 校验仅在写入路径执行：对象可以正常读取，
	//     但每次更新都会被拒绝，包括控制器移除 finalizer 的操作。现有发布版本均不受影响——逐 tag 检查
	//     确认截至 v0.8.6 均不含此类型——但跟踪默认分支的集群会受影响，因为该分支曾包含旧拼写。清理此类
	//     对象是首个发布版本的工作：确认不存在遗留对象，或编写恢复流程。conversion webhook 在此处与
	//     别名映射一样无法解决问题：schema 枚举是已存储对象最先经过的关卡，因此放宽准入所接受的范围
	//     无法触及已被 API server 拒绝的对象。
	//   - 在 RDMA 和 EFA 上，该协议还隐含对应的设备资源名称，member 会请求 1 个该资源以便设备 cgroup
	//     允许其打开网络结构设备。对于运行本 operator 的集群，每种网络结构都有唯一正确的资源名——EFA 的
	//     plugin 由 AWS 提供，RDMA 资源则由本 operator 的 Device Manager 发布——因此资源名直接由该值
	//     派生而无需声明。若节点未通告该资源，member 将保持 Pending，这正是预期的拒绝行为：无法提供
	//     该网络结构的集群应当将 backend 设为 TCP，在此处明确报错好过在对象仍显示 RDMA 时，事后才从
	//     未报 error 却默默安装了 TCP 的 store 日志中发现问题。
	//
	// +k8s:validation:default="Auto"
	// +k8s:validation:enum=["Auto","TCP","RDMA","EFA","CANN","ROCM","MUSA","MACA"]
	Protocol string `json:"protocol,omitempty" protobuf:"bytes,1,opt,name=protocol"`
}

// KVCacheBackendMember 定义一组 store member：它选择的节点、每个节点贡献的存储介质以及贡献容量。
type KVCacheBackendMember struct {
	// NodeSelector 选择贡献该介质的节点。每个被选中的节点上运行一个 member；扩大选择器范围会新增
	// member，leader 会立即将其 segment 纳入后续分配，无需重启 leader 或 member。
	//
	// +required
	NodeSelector map[string]string `json:"nodeSelector" protobuf:"bytes,1,rep,name=nodeSelector"`

	// Medium 指定该 member 组所挂载的 segment 使用的介质类型：主机内存（DRAM）或设备显存（VRAM）。
	//
	// 它不仅是一个标识，更决定了渲染逻辑的分支。DRAM member 会将 CapacityPerMember 计入 Pod 的主机
	// 内存请求；而 VRAM member 不将其计入任何主机资源请求，因为其 segment 位于设备显存中，声明它即
	// 意味着直接分配显存。让 VRAM member 访问其设备所需的配置均由用户显式声明而非推断——即下方的
	// SecurityContext、HostPaths 和 RuntimeClassName，各自独立配置。该字段保持不可变——已挂载的
	// segment 不能在底层数据仍存在时改变介质类型——因此在声明该组时即须确定。
	//
	//   - 本地磁盘、NVMe-oF、DAX 设备与分布式文件系统均不属于 member 组，各自通过其他途径接入：本地
	//     磁盘通过下方的 localDisks 配置，NVMe-oF 作为无需 Pod 的目标坐标接入，而后两者直接在 leader
	//     自身进程中使用。
	//   - 收窄该枚举存在已知且可接受的残留风险。如果在已安装此 CRD 但未安装 webhook 的集群上曾使用
	//     过已被移除的枚举值创建对象，该对象将无法删除：schema 校验仅在写入路径执行，因此对象可以正常
	//     读取，但每次更新都会被拒绝，包括控制器移除 finalizer 的操作。受影响范围仅限开发集群，因为
	//     截至 v0.8.6 的所有 tag 均不包含此类型，清理此类对象是首个发布版本的工作——确认不存在遗留对象，
	//     或编写恢复流程。
	//
	// +required
	// +k8s:validation:enum=["DRAM","VRAM"]
	Medium string `json:"medium" protobuf:"bytes,2,name=medium"`

	// CapacityPerMember 指定单个 member（而非单个节点）的容量。它将作为该 member 的全局 segment 大小，
	// 并计入 member Pod 自身的资源请求中，从而使节点容纳不下的 member 保持 Pending，避免对节点造成
	// 超售。
	//
	//   - 在 LocalDisks 中声明了存储层的组，此处至少需要能容纳一个 bucket 的容量，因为该存储层以 bucket
	//     为单位写入。bucket 中的字节会一直保存在此 segment 中直到整个 bucket 写满，因此如果 segment
	//     小于单个 bucket，就永远无法同时容纳一个完整的 bucket，该存储层在任何负载下都会保持为空，且没有
	//     其他地方会报告这一问题。未配置存储层的组没有此下限要求。
	//   - 该字段命名为 "per member"，是因为已确定但尚未实施的形态为：每个节点按 NUMA 域拆分为多个
	//     member。目前每个被选中的节点仅运行一个 member。
	//   - 重新启动该设计的触发条件是：双路节点在多个 NUMA 节点上均报告了 RDMA 接口，且观测到该节点上的
	//     member 正在跨 CPU 插槽边界传输数据——除此之外，该路径上目前没有其他逻辑消费本 operator 已发现
	//     的 NUMA 亲和性信息。届时也不会采用“每个 NUMA 域一个组”的形态——组通过 nodeSelector 选择节点，
	//     而 NUMA 是节点内部的属性而非节点上的标签。
	//
	// +required
	CapacityPerMember resource.Quantity `json:"capacityPerMember" protobuf:"bytes,3,name=capacityPerMember"`

	// LocalBufferSize 是 member 客户端的本地暂存缓冲区大小，会与 CapacityPerMember 一同计入 Pod 的
	// 内存请求。
	LocalBufferSize resource.Quantity `json:"localBufferSize,omitempty" protobuf:"bytes,4,opt,name=localBufferSize"`

	// ExtraArgs 将本 API 未枚举的配置键直接透传给 member。每项写作独立的参数 token（"-key=value"），
	// 渲染时会去掉前导短横线并转换为入口命令自身的 "-D key=value" 覆盖参数——两侧各自使用对应二进制
	// 文档规定的命名空间。若某个键与上述字段派生出的键冲突，则会在准入时被拒绝。
	//
	// 此处的每个值都是全局可读的：它会原样存储在此集群作用域对象上，随后渲染进 member 容器的 argv，
	// 在对象生命周期内对所有能访问该 Pod 或 DaemonSet 的人可见。凭据不应放在此处；由于本 operator
	// 不会渲染任何携带凭据的参数，此字段是凭据进入的唯一途径。
	//
	// +listType=atomic
	ExtraArgs []string `json:"extraArgs,omitempty" protobuf:"bytes,5,rep,name=extraArgs"`

	// ExtraEnv 将本 API 未枚举的环境变量直接透传给 member 容器。
	//
	//   - 它并非 ExtraArgs 的另一种写法：两者作用于不同位置。ExtraArgs 会渲染为入口命令的 "-D key=value"
	//     配置覆盖参数，而该 store 的一整类设置（例如本地磁盘层的刷盘阈值、晋升行为以及其他驱逐调节
	//     参数）完全没有配置键，仅从环境变量读取。
	//   - 本 operator 已渲染的变量名会在准入时被拒绝，原因与 ExtraArgs 键冲突相同：Kubernetes 允许容器
	//     中同一变量名出现两次，并将最终生效值交由运行时决定，从而导致冲突甚至不会被报告。这包括本地
	//     磁盘层的 bucket 阈值（由本 operator 自行计算大小）；若需要调整这些阈值，应通过专门的 API 字段，
	//     而不能借此字段绕过。
	//
	// 该列表以 name 为键，schema 会拒绝两个条目共用同一 name。
	//
	// 此处的每个值都是全局可读的：它会原样存储在此集群作用域对象上，随后渲染进 member 容器的环境变量，
	// 在对象生命周期内对所有能访问该 Pod 或 DaemonSet 的人可见。凭据不应放在此处；由于本 operator
	// 不会渲染任何携带凭据的变量，此字段是凭据进入的唯一途径。
	//
	// +listType=map
	// +listMapKey=name
	ExtraEnv []InstanceEnvVar `json:"extraEnv,omitempty" protobuf:"bytes,6,rep,name=extraEnv"`

	// Image 仅针对当前 member 组覆盖 backend 的 Image。若未设置，该组将运行 backend 的 Image。
	//
	// 之所以需要此字段，是因为组拥有独立的 NodeSelector：两个组可能选择不同加速器厂商或代际的节点，
	// 而 store 客户端按厂商分别打包为不同的 wheel，各自携带其编译时启用的传输协议和链接的运行时。厂商
	// 运行时也同时对应存储介质：VRAM 组需要包含已编译进 VRAM segment 支持的构建版本，而标准 CPU
	// 默认镜像并不包含该支持。
	//
	// +k8s:validation:maxLength=512
	Image string `json:"image,omitempty" protobuf:"bytes,7,opt,name=image"`

	// LocalDisks 声明该组节点贡献的本地磁盘存储层，每个主机目录对应一项；空列表表示该组仅使用内存。
	// 声明条目即意味着开启该存储层：store 的 leader 通过此列表是否存在来判断开关，而非依赖自身单独的字段。
	//
	//   - 该存储层以 bucket 为单位写入，这也是本 operator 自行设定 bucket 大小的原因。store 在单个
	//     bucket 按字节数或对象数写满前不会执行落盘，因此在 store 默认的阈值（按饱和生产环境规格设定）
	//     下，尽管其他各项指标看起来正常，该存储层仍会一直保持为空。本 operator 实际渲染的这组阈值并
	//     未暴露在本 API 中，且 members[].extraEnv 会拒绝这些变量名。
	//   - 它是挂载在该组之上的一个存储层，而非独立的 member 组，这符合 store 的工作模型：leader 会将
	//     卸载任务路由到持有该 key 内存副本的客户端，因此不持有内存 segment 的 member 永远不会被选中，
	//     其冷存储层也永远不会被填充。
	//   - 若要查看该存储层实际占用的数据量而非声明容量，请读取 leader 自身的
	//     master_allocated_file_size_bytes；status.capacity 仅报告声明的容量。
	//   - 最多只能包含一个条目，这一限制源于状态契约而非某个渲染器的能力边界：status.capacity 对整个
	//     backend 只提供一组汇总数值，无法将存储层的字节数归因到具体某块磁盘，因此两个条目会使其中的
	//     任何一个都无法被准确描述。基于同样的原因（在组级别上），准入校验仅允许一个组携带此列表——
	//     若要放宽限制，必须与该状态结构一并调整。
	//
	// 该列表以 path 为键，schema 会拒绝两个条目指定同一个目录。
	//
	// +k8s:validation:maxItems=1
	// +listType=map
	// +listMapKey=path
	LocalDisks []KVCacheBackendMemberLocalDisk `json:"localDisks,omitempty" protobuf:"bytes,8,rep,name=localDisks"`

	// Transport 声明该组使用的数据面配置，仅针对本组覆盖 backend 的 spec.transport.protocol。若未
	// 设置，该组将继承 backend 的配置。
	//
	// 提供此覆盖字段是为了解决两种介质唯一存在差异的场景：VRAM 组通过高速网络结构访问对端节点，而
	// 旁边的 DRAM 组仍保持使用 TCP。
	Transport *KVCacheBackendMemberTransport `json:"transport,omitempty" protobuf:"bytes,9,opt,name=transport"`

	// FabricInterfaceCount 指定每个 member 所需的不同主机网络结构（host-fabric）接口数量。
	//
	// 该字段仅在组实际生效的协议为 RDMA 或 EFA 时适用，未设置时默认按 1 处理。其他所有协议均不会分配
	// 网络结构设备，因此在这类组上填写该数量不会产生效果，准入校验仅发出告警而非拒绝：将组从 RDMA 切回
	// TCP 的更新操作通常仍会保留该字段。0 不是合法数量：显式填写的值至少为 1。
	//
	// RDMA 数量为 1 时请求共享资源；数量大于 1 时请求独占资源，因为第二个共享 token 可能对应同一个端点
	// 上的第二次申请，从而导致多接口请求在未满足真实多网卡要求的情况下不报错地通过调度。
	//
	// 该数量会影响部署密度：数量为 1 的 member 可使用共享 token 容量，而数量大于 1 时，单个节点最多
	// 可调度的 member 数量受限于节点端点数除以该数量。
	//
	// EFA 容量因节点而异。若请求的数量超过 device plugin 通告的可用数量，member 将保持 Pending，从而
	// 使用户能够直观感知无法满足的多接口请求。
	//
	// +k8s:validation:minimum=1
	FabricInterfaceCount *int32 `json:"fabricInterfaceCount,omitempty" protobuf:"varint,13,opt,name=fabricInterfaceCount"`

	// 本结构体不提供按组配置的设备资源字段。member 的 segment 只是单个设备上的一次 cudaMalloc——经
	// 上游实测，拆分只是为了不超过传输层的注册上限，且该路径上没有任何逻辑用于选择特定设备——因此如果向
	// 调度器申请一个扩展资源（extended resource），为了记录单个设备显存中的一小部分，就会把整张加速器从
	// 推理任务中独占走，而在该节点上 member 又无法利用被占用的其余算力与显存。相反，member 组与推理
	// 引擎共置于同一设备上，并依据该引擎自身的显存占用比例来确定大小。

	// SecurityContext 是成员容器的 security context，它会合并到渲染器根据成员组有效协议推导出的
	// security context 之上，而非直接替换后者。
	//
	// 合并按字段进行：此处显式设置的字段会生效，未设置的字段保留渲染器填入的内容，而
	// capabilities.add 则取两侧的并集。之所以需要明确说明“取并集”，是因为若采用替换语义将悄然
	// 产生问题：host-fabric 成员组需要 IPC_LOCK 来锁定其注册的内存，并需要 SYS_RESOURCE 来
	// 提高锁定内存时触及的上限；若整体替换该值会同时丢弃这两项 capability，导致容器仍能正常
	// 启动和运行，直到注册阶段才失败。因此，该字段无法表达“丢弃其中一项”的语义；如果某个
	// 成员组不能持有这些 capability，应声明不请求它们的协议。
	//
	// 此设置等价于节点上的 ROOT 权限，且是有意为之：Privileged 或将 RunAsUser 设为 0 并配合
	// HostPaths 条目，会赋予成员容器与节点上进程相同的权限。该授权并不会越级扩大可配置者的
	// 权限范围——本对象本身就是集群作用域资源，正是因为它涉及特权物理资源，能够写入该对象
	// 的主体本就已控制整个集群。此处选择显式声明而非隐式推断，是为了让任何阅读该对象的人都
	// 能清楚看到授予了哪些权限。
	SecurityContext *core.SecurityContext `json:"securityContext,omitempty" protobuf:"bytes,10,opt,name=securityContext"`

	// HostPaths 将所选节点上的目录或文件挂载到成员容器中。
	//
	// 设置此字段的原因在于厂商的用户态驱动既不在容器镜像内，也不位于 /dev 下，因此任何设备
	// 授权都无法覆盖它：例如 Ascend 成员需要节点上的驱动目录树和 DCMI 库，除此之外只能依靠
	// 容器运行时主动注入。仅靠 Privileged 并不能解决这一问题——它开放的是节点的设备树（即
	// 设备节点所在位置），而不是用户态库所在的位置。
	//
	// 条目按书写顺序挂载。每个条目背后的 volume 依据其位置编号而非此处声明的任何名称命名，
	// 因此不会与其他条目或渲染器自有的 volume 发生名称冲突。
	//
	// 上方的 LocalDisks 并非此字段的另一种写法：该列表声明的是 leader 用于路由 offload 任务的
	// 分层容量，并附带基于其存在性推导出的注销 hook 与宽限期；而此处挂载的目录就只是普通挂载，
	// 不承担其他语义。
	//
	// +k8s:validation:maxItems=32
	HostPaths []KVCacheBackendMemberHostPath `json:"hostPaths,omitempty" protobuf:"bytes,11,rep,name=hostPaths"`

	// RuntimeClassName 选择成员 Pod 运行所使用的容器运行时，厂商运行时借此在不通过本字段
	// 逐一命名的情况下注入其驱动库与设备节点。
	//
	// 与 ModelDeployment 上的同类字段不同，这里是显式声明而非依据成员组的硬件信息自动查找，
	// 原因是成员组没有可查询的 InstanceType：它通过标签选择节点，而标签本身并不携带本
	// operator 可映射的厂商信息。对于已将厂商运行时设为默认运行时的集群，此处无需填写。
	//
	// 若集群中不存在对应名称的 RuntimeClass，API server 会直接拒绝该 Pod，从而使成员组在
	// 自身 Pod 准入阶段即停止，而不会在缺失运行时的情况下启动。这正是此处期望的显式失败行为。
	//
	// +k8s:validation:pattern="^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$"
	// +k8s:validation:maxLength=253
	RuntimeClassName string `json:"runtimeClassName,omitempty" protobuf:"bytes,12,opt,name=runtimeClassName"`
}

// KVCacheBackendMemberHostPath 表示从所选节点挂载到成员容器中的单个目录或文件。
//
// 仅支持 host path，不提供其他 volume 来源：成员组需要从镜像外部获取的只是节点自身的驱动
// 目录树与设备节点。ConfigMap、Secret 或 PVC 在此场景下均无已知用途，在缺乏实际需求时
// 引入额外的来源只会增加无人验证的校验面。
type KVCacheBackendMemberHostPath struct {
	// Path 是节点上的绝对路径。
	//
	// +required
	// +k8s:validation:pattern="^(/[^/]+)+$"
	// +k8s:validation:maxLength=1024
	Path string `json:"path" protobuf:"bytes,1,name=path"`

	// MountPath 是成员容器内的绝对路径，不得与其他条目的挂载路径或渲染器自有的路径重复。
	//
	// +required
	// +k8s:validation:pattern="^(/[^/]+)+$"
	// +k8s:validation:maxLength=1024
	MountPath string `json:"mountPath" protobuf:"bytes,2,name=mountPath"`

	// Type 是 kubelet 在挂载前执行的 host-path 类型检查。
	//
	// 若留空则对应空类型，此时 kubelet 的 mounter 会直接返回而不检查路径是否存在——从而导致
	// 缺失的路径在容器内变成一个空目录，成员也会照常启动。显式指定类型才能让这种情况触发
	// FailedMount，从而使 Pod 停在挂载阶段。
	//
	// +k8s:validation:enum=["","DirectoryOrCreate","Directory","FileOrCreate","File","Socket","CharDevice","BlockDevice"]
	Type *core.HostPathType `json:"type,omitempty" protobuf:"bytes,3,opt,name=type"`

	// ReadOnly 以只读方式挂载该路径。驱动目录树仅由成员读取、不会被任何进程写入，因此只读
	// 是合适且正确的配置；未将其设为默认值的原因在于 /dev 下的设备节点也会通过此字段挂载，
	// 而设备节点需要可写。
	ReadOnly bool `json:"readOnly,omitempty" protobuf:"varint,4,opt,name=readOnly"`
}

// KVCacheBackendMemberTransport 是成员组对后端数据平面的覆盖配置。
//
// 该结构仅包含 protocol 字段。FabricInterfaceCount 仍保留在成员定义上，以便成员组即使
// 在继承后端协议时，也能独立选择自身的资源数量与 RDMA 分配模式。
type KVCacheBackendMemberTransport struct {
	// Protocol 指定要求该成员组使用的传输协议，其可选值及“Auto 解析为 TCP”的规则与后端的
	// spec.transport.protocol 一致；设置后将在该成员组范围内替代后端配置——包括该字段关于
	// 拼写变更的残留风险，此处存储的值同样适用。
	//
	// +k8s:validation:default="Auto"
	// +k8s:validation:enum=["Auto","TCP","RDMA","EFA","CANN","ROCM","MUSA","MACA"]
	Protocol string `json:"protocol,omitempty" protobuf:"bytes,1,opt,name=protocol"`
}

// KVCacheBackendMemberLocalDisk 表示成员组节点提供的单个本地磁盘存储层。
//
// 只要成员组列表中存在任意条目，就会同时在两端启用该存储层：leader 仅凭列表非空就会
// 渲染将驱逐 key 写入磁盘的标志位，且会将其渲染为延迟模式（key 仅在被驱逐时写入，绝不在
// 存储时写入），因为这是 store 唯一受保护的模式。另一种模式没有对应字段也不应提供：它
// 会驱逐尚未刷盘 bucket 的唯一副本，因此不存在可与之配合的声明方式，也不会有遗漏配置的
// 风险。
type KVCacheBackendMemberLocalDisk struct {
	// Path 是每个所选节点上承载该存储层的目录，会按相同路径从宿主机挂载到成员容器中。此字段
	// 为必填且无默认值：在他人节点上替用户选定宿主机目录不是本 operator 可以代劳的决定，
	// 因为选错目录会占满 Kubernetes 无法纳入配额统计的文件系统。
	//
	//   - 声明本地磁盘层要求成员组镜像中必须包含 shell。init container 会在成员启动前检查
	//     该目录，以便在路径被复用时提醒管理员，而不是等到读出他人数据后才发现问题。该检查
	//     通过 `sh -c` 执行，若镜像中没有 shell，成员将完全无法启动。
	//   - 创建该目录并设置正确的属主是管理员的职责，而非本 operator 的职责；若成员容器无法
	//     写入该目录，将在启动时失败。这一省略是有意为之：若由 init container 执行 chown，则
	//     必须指定一个 uid，而 members[].image 允许不同成员组使用不同厂商的构建镜像；若改用
	//     chmod 0777，则会使该目录对节点上所有进程开放。只有当整个后端统一使用同一 uid 时，
	//     operator 才具备自动处理的条件。
	//
	// +required
	// +k8s:validation:maxLength=4096
	Path string `json:"path" protobuf:"bytes,1,name=path"`

	// Capacity 以字节为单位限制该存储层可存储的数据总量。若留空，则沿用 store 自身的上限且
	// 不渲染任何配置，这样当上游调整默认上限时只需排查上游变更，而不会由本 API 悄然重申旧值。
	//
	//   - 它同时也是驱逐机制（Eviction）所依据的基准容量，因此下方的 Eviction 必须配合此
	//     字段使用：store 的水位线配额默认为 0，该路径会将其视为“无配额”，从而在不驱逐任何
	//     数据的情况下直接返回。单个配置值会同时渲染到这两个上限中。
	//   - 设置的容量必须至少能容纳一个 bucket（即该存储层写入数据的基本单元）：一旦剩余空间
	//     不足以再放入一个 bucket，store 就会停止向其下发 offload 任务，因此更小的容量永远
	//     不会收到任何 key。bucket 大小由本 operator 决定，容量下限也随之变化。
	//   - 与 CapacityPerMember 不同，该值不计入 Pod 的资源请求。该存储层是宿主机目录，完全
	//     处于 kubelet ephemeral-storage 统计范围之外；若将其计入请求，就会预留一项无人
	//     管控的数值，甚至可能把成员 Pod 排除在真正拥有该磁盘的节点之外。监控该文件系统属于
	//     运维人员的职责。
	Capacity resource.Quantity `json:"capacity,omitempty" protobuf:"bytes,2,opt,name=capacity"`

	// KeyLimit 限制该存储层最多可持有的 key 数量。它是与 Capacity 并列的另一半限制，而非
	// 替代项：store 同时按字节数和 key 数量约束该存储层，任一项即将超出时都会停止下发 offload
	// 任务，并对本对象未指定的项沿用其自身上限。按与 Capacity 相同的规则，留空或为 0 时不渲染
	// 任何配置。它同样受限于单 bucket 下限，原因也相同——store 按整 bucket 所需的 key 数量
	// 进行校验，低于该下限的存储层永远无法接收任何一个 bucket。
	//
	// +k8s:validation:minimum=0
	KeyLimit int64 `json:"keyLimit,omitempty" protobuf:"varint,3,opt,name=keyLimit"`

	// Eviction 定义该存储层写满后的行为。若留空，则不渲染任何配置并沿用 store 自身行为，从而
	// 当上游变更默认行为时更容易排查，而不会由本 API 悄然重申旧值。
	Eviction *KVCacheBackendMemberLocalDiskEviction `json:"eviction,omitempty" protobuf:"bytes,4,opt,name=eviction"`

	// CleanAfterDelete 请求本 operator 在后端被删除时，清理该成员组 NodeSelector 当时选中的
	// 每个节点上的 Path 目录内容。该字段默认为 false；保持默认时，目录内已有内容将保留。
	//
	// 将其设为开关而非默认启用，是因为磁盘上的数据归管理员所有，本 operator 不应擅自替管理员
	// 决定删除。这也是它可行而创建目录不可行的原因：清空内容无需指定 uid，而创建目录需要。
	//
	//   - “当时”即全部承诺范围。系统不会保存 selector 的历史记录，且清理执行时成员 Pod 已
	//     不存在，因此在删除后端前缩小 NodeSelector 或移除 LocalDisks 条目，会导致被移出的
	//     节点保留原有数据且不会收到任何提示。应先删除后端再修改配置。
	//   - 被清除的是目录中的内容而非目录本身——该目录由节点准备方创建，可能是挂载点，且带有
	//     非本 operator 设定的属主。
	//   - 清理任务使用该成员组自身的镜像、在所选节点上运行，并复用后端的 imagePullSecrets，
	//     从而无需等待成员镜像之外的额外拉取。
	//   - 若某节点在超时时间内无法触达，其目录内容将被保留，且不会阻塞后端删除：若 finalizer
	//     一直等待已失联的节点，会导致对象无法被删除。该节点会收到一条写明遗留路径的警告
	//     Event。
	//   - 若某节点上存在另一个声明了重叠路径的 KVCacheBackend，则跳过该节点并发送同类
	//     Event。系统不禁止两个后端声明同一目录，而若为当前后端清空该目录会一并删掉另一个
	//     后端的在线数据。
	CleanAfterDelete bool `json:"cleanAfterDelete,omitempty" protobuf:"varint,5,opt,name=cleanAfterDelete"`
}

// KVCacheBackendMemberLocalDiskEviction 定义存储层写满后的处理方式：丢弃已有数据以腾出
// 空间，或停止接收新数据。
//
// 这两种并非同一旋钮下的不同参数，而是两种截然不同的结果：启用驱逐时，存储层会持续接受
// 写入，最旧的内容被淘汰；禁用驱逐时，存储层写满至 Capacity 后，store 将停止向其发送
// 任务，已有数据保持可读。
type KVCacheBackendMemberLocalDiskEviction struct {
	// Enabled 控制该存储层是否启用驱逐。该字段默认为 true，因此仅声明该配置块而不显式指定
	// 此字段时，表示启用而非禁用驱逐。
	//
	// 省略该字段与显式书写 `enabled: false` 含义不同，正是这一默认值使二者区分开来：只有
	// 显式设为 false 才会关闭驱逐。若无此默认值，仅为了设置 Watermark 而声明该配置块的用户
	// 将意外关闭存储层的驱逐能力。
	//
	// 将其关闭会同时渲染两项配置而非一项：将驱逐策略设为 "none"（store 对该语义的内置取值），
	// 以及将其水位线驱逐开关显式设为 false。二者属于不同层级，应确保无论哪一层判断都处于
	// 关闭状态。
	//
	// +k8s:validation:default=true
	Enabled *bool `json:"enabled,omitempty" protobuf:"varint,1,opt,name=enabled"`

	// Policy 指定条目的淘汰顺序。FIFO 优先淘汰最早写入的条目，LRU 优先淘汰最近最少读取的
	// 条目。该字段不得与 Enabled=false 同时设置，因为不淘汰任何条目时不存在淘汰顺序。
	//
	//   - 枚举值特意仅提供任意缓存都会支持的这两种策略，而非 store 解析器恰好能识别的所有
	//     字符串。枚举中不包含“不驱逐”之类的值：那是 Enabled 的职责；若再引入第三个同义值，
	//     准入校验就不得不处理它与 Enabled 之间的冲突裁决。
	//   - 若留空则不渲染任何配置并沿用 store 默认值（即 FIFO）。在此处这一点尤为重要：store
	//     遇到不认识的策略字符串时会直接静默映射为“完全不驱逐”——既无错误、无警告，也不会
	//     启动失败——因此仅当策略由本 API 从固定枚举中选定时才会下发该字段。
	//
	// +k8s:validation:enum=["FIFO","LRU"]
	Policy string `json:"policy,omitempty" protobuf:"bytes,2,opt,name=policy"`

	// Watermark 定义驱逐运行的区间：当存储层使用率超过 High 时触发驱逐，回落到 Low 以下时
	// 停止，两者均以 Capacity 的百分比表示。若留空，则不渲染任何配置并沿用 store 自身的水位线。
	//
	// 该字段依赖 Capacity（百分比以此为基准），且不得与 Enabled=false 同时设置。
	Watermark *KVCacheBackendMemberLocalDiskEvictionWatermark `json:"watermark,omitempty" protobuf:"bytes,3,opt,name=watermark"`
}

// KVCacheBackendMemberLocalDiskEvictionWatermark 表示驱逐动作所处的上下限区间。
//
// 两个阈值必须成对给出，而非在上一层配置块中分别单独设置，因为单独任意一个阈值都无法
// 表达本 operator 希望渲染的完整语义：仅设高水位线时，store 会将其与对象中未声明的低
// 水位线配对；只要该未声明的默认值不低于高水位线，成员就会在启动时直接拒绝这组配置，
// 而具体原因只会出现在容器日志里。
type KVCacheBackendMemberLocalDiskEvictionWatermark struct {
	// High 是触发驱逐的 Capacity 百分比。这里采用百分比而非容量值：store 本身按其配额的比例
	// 而非绝对大小接收该参数，且若在此处填写绝对大小，便会重复表达 Capacity 中已有的数值——
	// 一旦 Capacity 调整，该值就会悄然失效并出现不一致。
	//
	// +required
	// +k8s:validation:minimum=1
	// +k8s:validation:maximum=100
	High int32 `json:"high" protobuf:"varint,1,name=high"`

	// Low 是停止驱逐的 Capacity 百分比，必须严格小于 High。若两者相等，一旦写入超过阈值
	// 就会触发每次写入都伴随驱逐的抖动，而设置区间正是为了避免这种现象。成员启动时 store
	// 会拒绝此类配对，但准入阶段会提前在此拦截，让错误信息直接反馈给配置编写者。
	//
	// +required
	// +k8s:validation:minimum=1
	// +k8s:validation:maximum=100
	Low int32 `json:"low" protobuf:"varint,2,name=low"`
}

// KVCacheBackendStatus 定义 KVCacheBackend 的观测状态。
type KVCacheBackendStatus struct {
	// Phase 汇总各项 condition 的状态：Provisioning、Ready、Degraded、Error、Deleting。该
	// 字段源自 leader 自身的健康检查文档而非其 Pod phase——即使 leader 的 Pod 处于 Running
	// 状态，只要 leader 报告其服务尚未就绪，Phase 仍为 Provisioning 而非 Ready。
	Phase string `json:"phase,omitempty" protobuf:"bytes,1,opt,name=phase"`

	// PhaseMessage 说明当前 phase 的原因。
	PhaseMessage string `json:"phaseMessage,omitempty" protobuf:"bytes,2,opt,name=phaseMessage"`

	// Conditions 提供更细粒度的状态视图，每个维度对应一项 condition：LeaderAvailable、
	// MembersMounted、CapacityObserved、PoolWrites、Deletable、RolloutComplete，以及（仅在
	// 有明确判定依据时出现的）ElectionObserved 与 TierWasEmpty（后者仅当成员组声明了本地
	// 磁盘层时出现）。每项 condition 均源自实际观测到的文档。
	//
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []gpustack.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,3,rep,name=conditions"` // nolint: lll

	// Endpoints 是该后端的访问地址，每个命名角色对应一个条目——其结构与 external 分支的
	// 输入结构相同。托管后端由其自身的 Service 填充两类信息，外部后端则原样回显声明内容。
	//
	// +listType=map
	// +listMapKey=name
	Endpoints []KVCacheBackendEndpoint `json:"endpoints,omitempty" protobuf:"bytes,4,rep,name=endpoints"`

	// Capacity 表示 leader 报告的总容量与已分配容量。在首次采集成功前该字段保持缺失，且重新
	// 变为缺失与报告为 0 含义不同：若此处写入空对象，将无法区分容量为 0 与采集未返回任何结果。
	Capacity *KVCacheBackendCapacity `json:"capacity,omitempty" protobuf:"bytes,5,opt,name=capacity"`

	// Members 每个条目对应一个观测到的 store 成员。若 leader 所运行的 master 版本未提供
	// segment 列表接口，则该列表为空，此时 MembersMounted 为 Unknown。
	//
	// +listType=map
	// +listMapKey=segmentID
	Members []KVCacheBackendMemberStatus `json:"members,omitempty" protobuf:"bytes,6,rep,name=members"`

	// UsedBy 列出正在使用该后端的对象。只要 UsedBy 非空，finalizer 就会拒绝删除后端，因此
	// 该字段是用于执行删除保护的依据，而非单纯的展示信息。
	//
	// 该字段由使用方写入，而非由本后端自身的 reconciler 写入；reconciler 仅读取该字段并在
	// 其非空时阻塞清理流程。目前只有一个使用方会写入此处：KVCachePool 会以其类型名称
	// KVCachePool 认领所依赖的后端，并仅在从该后端的 master 上移除已注册信息后才释放该认领。
	// 由于后端及所有认领它的对象均为集群作用域资源，条目的 Namespace 均留空。
	// KVCacheObjectReference 的文档注释说明了为何其结构不同于两个核心引用类型。
	//
	// +listType=map
	// +listMapKey=kind
	// +listMapKey=namespace
	// +listMapKey=name
	UsedBy []KVCacheObjectReference `json:"usedBy,omitempty" protobuf:"bytes,7,rep,name=usedBy"`
}

// KVCacheBackendCapacity 表示 leader 所报告的后端容量。当采集失败时两个数值均保持缺失：
// 若写入 0 会被误读为空缓存，而保留上一次采集的值则会被误读为当前值。
type KVCacheBackendCapacity struct {
	Total *resource.Quantity `json:"total,omitempty" protobuf:"bytes,1,opt,name=total"`
	Used  *resource.Quantity `json:"used,omitempty" protobuf:"bytes,2,opt,name=used"`
}

// KVCacheBackendMemberStatus 表示一个被观测到的 store 成员。
type KVCacheBackendMemberStatus struct {
	// SegmentID 是 leader 报告的 segment 唯一标识符。
	//
	// +required
	SegmentID string `json:"segmentID" protobuf:"bytes,6,name=segmentID"`

	// ClientID 是成员进程启动时自行生成的标识符。与对外通告的地址不同，即使同一节点上运行
	// 多个 host-network 成员，该标识符仍能保持各自唯一。
	//
	// +required
	ClientID string `json:"clientID" protobuf:"bytes,7,name=clientID"`

	// SegmentName 是 leader 报告的成员对外通告地址。该值不保证唯一：调度到同一节点上的
	// 多个 host-network 成员会通告相同的地址。
	//
	// +required
	SegmentName string `json:"segmentName" protobuf:"bytes,1,name=segmentName"`

	// NodeName 是提供该成员介质的节点。
	NodeName string `json:"nodeName,omitempty" protobuf:"bytes,2,opt,name=nodeName"`

	// Medium 是该成员提供的介质类型，沿用选中其节点的分组配置。
	Medium string `json:"medium,omitempty" protobuf:"bytes,3,opt,name=medium"`

	// Protocol 是该成员挂载其分片时注册所使用的传输协议。
	//
	// 它并非观测结果。该值直接取自成员自身的挂载请求并经由 leader 的列表原样透传，因此
	// 不会与 spec.transport.protocol 产生分歧：即便成员请求使用主机 fabric 网络却因设备
	// 缺失而回退到 TCP，只要处于服务状态，此处仍会上报该 fabric 协议。因此，与 spec 一致
	// 并不代表请求已经生效；数据平面实际安装的传输协议仅记录在成员自身的日志中。
	Protocol string `json:"protocol,omitempty" protobuf:"bytes,4,opt,name=protocol"`

	// State 是由 LEADER 上报的成员状态，直接读取自 leader 自身的分片列表，而非根据
	// 成员 Pod 推断。存储层定义的状态按本 API 的大小写约定为：OK、Draining、Drained、
	// GracefullyUnmounting、Unmounting、Undefined。
	//
	//   - Draining 与两个 unmounting 状态用于标识缩容过程的过渡阶段，从而将正在退出的
	//     成员与已经失联的成员区分开来。这也正是该字段保留存储层原始术语而非加以汇总的原因。
	//   - 该字段不包含“unreached”哨兵值，因为没有任何流程会写入该值：读取列表失败时会
	//     保留此前的条目并通过 MembersMounted 条件加以说明，而不会将其改写为空白。此处内容
	//     是否为最新刷新结果由该条件判断，本字段不负责反映这一点。
	//   - 与 spec 侧的各类枚举不同，此处刻意不加枚举校验标记。该值域归属于存储层：如果
	//     新版本存储层新增了某种状态，加上枚举标记会导致整个 status 写入因校验失败而受阻
	//     （影响的是整个对象而非仅此字段），从而使其他所有 status 字段都停留在上一次的旧值。
	//     上方的 Phase 同样基于此原因保持开放。
	State string `json:"state,omitempty" protobuf:"bytes,5,opt,name=state"`
}

// KVCacheBackendList 包含 KVCacheBackend 的列表。
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type KVCacheBackendList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Items []KVCacheBackend `json:"items" protobuf:"bytes,2,rep,name=items"`
}

var _ runtime.Object = (*KVCacheBackendList)(nil)
