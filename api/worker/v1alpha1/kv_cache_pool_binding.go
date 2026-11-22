package v1alpha1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	gpustack "gpustack.ai/gpustack/api/v1"
)

// KVCachePoolBinding 是 worker.gpustack.ai 的架构定义。
//
// 它是实际的分配入口点：在某个命名空间中创建该对象，即为该命名空间授予池上的一份
// 配额，并注册其后续写入所用的唯一复用域，使配额与复用域都成为管理员可通过 RBAC
// 控制并审计的对象，而非由租户在工作负载中随意填写的字符串。
//
// 存储层的租户即对应此处的域，因此注册域会创建相应的配额账本，这也使命名域成为一种
// 特权操作，避免工作负载随意生成新租户。指向同一 Binding 的工作负载共享 KV；如果
// 某个命名空间需要两个复用边界，则创建两个 Binding，正如需要两个调度边界时会创建
// 两个 Kueue LocalQueue 一样。
//
// 它并非强制隔离边界，也不应被描述为隔离边界。存储层目前会接受调用方传入的任意
// tenant id，且通过集群内任意 Pod 均可访问的 Service 提供服务，本对象也不会派生
// 任何访问凭证——因此，知晓另一命名空间域名的负载目前仍可读写该域的缓存。Binding
// 管辖的是谁被授予了容量以及以何种名称使用。真正的强制隔离需要认证代理或网络隔离，
// 而这两者目前均尚未实现。
//
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:crd-gen:resource:scope="Namespaced",categories=["gpustack"],shortName=["kvcpb"],subResources=["status"]
// +k8s:crd-gen:printcolumn:name="Pool",type="string",jsonPath=".spec.poolRef.name"
// +k8s:crd-gen:printcolumn:name="Domain",type="string",jsonPath=".spec.domain.name"
// +k8s:crd-gen:printcolumn:name="BlockSize",type="integer",jsonPath=".spec.domain.blockSize"
// +k8s:crd-gen:printcolumn:name="Dtype",type="string",jsonPath=".spec.domain.dtype"
// +k8s:crd-gen:printcolumn:name="Effective",type="string",jsonPath=".status.effectiveQuota"
// +k8s:crd-gen:printcolumn:name="Usage",type="string",jsonPath=".status.usage"
// +k8s:crd-gen:printcolumn:name="Phase",type="string",jsonPath=".status.phase"
// +k8s:crd-gen:printcolumn:name="Age",type="date",jsonPath=".metadata.creationTimestamp"
type KVCachePoolBinding struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Spec   KVCachePoolBindingSpec   `json:"spec" protobuf:"bytes,2,name=spec"`
	Status KVCachePoolBindingStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

var _ runtime.Object = (*KVCachePoolBinding)(nil)

// KVCachePoolBindingSpec 定义 KVCachePoolBinding 的期望规格。
type KVCachePoolBindingSpec struct {
	// PoolRef 指定本命名空间获授的池。该字段由 webhook 强制设为不可变：
	// 修改 Binding 指向会静默迁移命名空间的配额授权，并将已写入的字节遗留在旧的 master 上。
	//
	// +required
	PoolRef KVCachePoolBindingPoolReference `json:"poolRef" protobuf:"bytes,1,name=poolRef"`

	// Domain 是本 Binding 注册的复用身份，为必填项。其名称直接映射为存储层的 tenant_id，
	// 不承担其他用途，因此注册域即会创建一个带有独立配额账本的租户。
	//
	// 它不是键前缀。在单个租户内部，存储键由推理引擎根据模型名称和 token 块哈希构造；
	// blockSize 和 dtype 均不属于键的组成部分。即便需要渲染前缀，使用的也是引用了
	// ModelArtifact 的 ModelDeployment 的权重身份（在 vLLM 中渲染为 cache_prefix，
	// 在 SGLang 中渲染为 extra_backend_tag），绝非此处的域名称。
	//
	// 此处刻意设计为恰好一个域而非列表，由 schema 直接约束基数，无需依赖 webhook 规则：
	// 一个 Binding 对应一个租户，Status 中的每项指标都是单一序列而非汇总值，也无需发明
	// 在多个域之间切分同一上限的规则。
	//
	// 该字段下的每一项均由 webhook 强制设为不可变。修改 Name 会使本命名空间指向另一
	// 账本并使旧账本成为孤儿；在缓存已预热的情况下修改 BlockSize 或 Dtype 则会导致
	// 静默数据损坏：写入会成功，读取也会成功，但读出的张量是错误的。
	//
	// +required
	Domain KVCachePoolBindingDomain `json:"domain" protobuf:"bytes,2,name=domain"`

	// Quota 是本命名空间向其池申请的配额，其结构与池自身声明的上限一致，从而在授权的
	// 供需双方保持同一概念的表述统一。
	//
	// +required
	Quota KVCachePoolBindingQuota `json:"quota" protobuf:"bytes,3,name=quota"`
}

// KVCachePoolBindingQuota 是本命名空间向其池申请的配额。
type KVCachePoolBindingQuota struct {
	// Ceiling 是本命名空间在其复用域内可消耗的容量上限，直接原样写入该单一租户的请求
	// 配额中，而非作为由本 operator 维护的总量。
	//
	//   - 它是申请量而非最终授权量。当所有申请量之和超过可分配容量时，池会按比例缩减
	//     每个租户的有效配额，Status.EffectiveQuota 才是实际获授的容量。
	//   - 超出上限会触发淘汰而非拒绝写入，这与字面含义恰恰相反。超过上限的写入不会被
	//     拒绝：存储层会淘汰本命名空间自身较旧的对象以腾出空间并重试。因此，上限设置
	//     过低会损耗本命名空间自身的缓存命中而非导致写入失败，且这种损耗不会体现在任何
	//     失败计数器上。只有当淘汰无法释放足够空间时（例如旧对象仍被未过期的读取租约
	//     持有），写入才会被拒绝。
	//   - 该字段为必填项，因为省略它会导致系统进入不可用状态：存储层没有默认策略，会
	//     拒绝没有任何策略的租户，因此缺少该字段的 Binding 虽能通过准入并上报 Ready，
	//     却会拒绝其工作负载写入的每一个字节。
	//   - 非正数值将在准入阶段被拒绝。
	//
	// +required
	Ceiling resource.Quantity `json:"ceiling" protobuf:"bytes,1,name=ceiling"`
}

// KVCachePoolBindingPoolReference 指定本 Binding 所授予的集群作用域池。
//
// 它不携带 namespace，这正是其设计本意：池是集群作用域资源，本就没有命名空间可填，
// 且此处的命名空间作用域对象绝不会跨命名空间边界进行读取。
type KVCachePoolBindingPoolReference struct {
	// +required
	Name string `json:"name" protobuf:"bytes,1,name=name"`
}

// KVCachePoolBindingDomain 是复用身份：决定缓存按什么进行区分，以及数据块具有
// 怎样的形状。
type KVCachePoolBindingDomain struct {
	// Name 是域名称，并原样作为存储层的 tenant_id。
	//
	//   - 若留空不填，API server 会在对象落盘前将其填充为 "default"。这与存储层为未指定
	//     名称的写入方分配的租户一致，从而使省略名称与未转发租户的引擎落入同一账本条目。
	//     对于未启用多租户的 master，这是最自然的选择：此时完全不会转发租户信息，该名称
	//     仅用于记录注册信息。
	//   - 在服务于该 Binding 所属池的同一 master 上，该名称不得被其他任何 Binding 占用，
	//     此项由 webhook 强制校验：同一 master 上两个指向相同域的 Binding 虽然可能有意
	//     共享缓存，却会在同一配额账本上发生冲突，而这绝非预期行为。唯一性按 master 而非
	//     按池约束，因为一个 master 可服务多个池且租户空间在 master 内全局唯一；同时它
	//     也不是集群全局唯一的，因为两个 master 各自维护独立的账本。
	//   - 可接受的格式为 DNS-1123 标签，由 webhook 校验。该格式严格属于 master 所接受
	//     的 tenant_id 范围，且与 Kubernetes 对象名称的格式一致，用户无需额外学习第二套
	//     命名规则。此处是唯一校验该格式的位置；所有下游消费方均直接复制该名称，不再重复
	//     校验。
	//
	// +optional
	// +k8s:validation:default="default"
	// +k8s:validation:maxLength=63
	Name string `json:"name,omitempty" protobuf:"bytes,1,opt,name=name"`

	// BlockSize 是单个缓存块所容纳的 token 数量。
	//
	// +required
	BlockSize int32 `json:"blockSize" protobuf:"varint,2,name=blockSize"`

	// Dtype 是缓存张量所使用的元素类型，采用推理引擎自身的小写拼写形式。
	//
	// 完整的可选值集合归属于管理工作负载的规范，因此本 API 不对其进行枚举（否则每增加
	// 一种新的引擎数据类型都会构成 API 变更），Webhook 仅校验其语法格式。字段名拼写
	// 与其 JSON 名称完全一致；若写成 DType 则不一致，openapi 生成器会将此类不匹配
	// 记录为受检 API 规则违规。
	//
	// +required
	// +k8s:validation:maxLength=32
	Dtype string `json:"dtype" protobuf:"bytes,3,name=dtype"`
}

// KVCachePoolBindingStatus 是当前 Namespace 对缓存池的自身视图：请求了多少配额、
// 缓存池实际授予了多少、当前已使用多少以及是否超出配额。
//
// 下方各项数值均只从单个租户的时间序列中读取，因为一个 Binding 只注册一个复用域，
// 而存储层的租户正是该复用域：此处不会对任何数值求和，也不会在背后隐藏第二个复用域。
//
// 当下方观测值未被观测到时，字段值为缺失（nil）而非 0，两者含义截然不同：
// 读数为 0 表示实际测量结果为 0，而缺失表示根本未进行测量。区分这两者正是为了避免
// 将未测量误读为 0。
type KVCachePoolBindingStatus struct {
	// Phase 汇总当前状态条件：Provisioning、Ready、Error、Deleting。
	Phase string `json:"phase,omitempty" protobuf:"bytes,1,opt,name=phase"`

	// PhaseMessage 携带当前 Phase 的原因说明。
	PhaseMessage string `json:"phaseMessage,omitempty" protobuf:"bytes,2,opt,name=phaseMessage"`

	// Conditions 是更细粒度的状态视图，每个维度对应一个 Condition。
	//
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []gpustack.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,3,rep,name=conditions"` // nolint: lll

	// RequestedQuota 是 Operator 代表当前 Namespace 向缓存池请求的配额：即为此 Binding
	// 的唯一租户写入的请求配额。每当 Operator 拒绝写入时（例如 master 返回未开启多租户，
	// 或策略来源无法被改写），该字段缺失，并附带一条 Condition 说明原因。
	RequestedQuota *resource.Quantity `json:"requestedQuota,omitempty" protobuf:"bytes,4,opt,name=requestedQuota"`

	// EffectiveQuota 是缓存池实际授予的配额。当所有租户请求配额之和超过缓存池
	// 可分配容量时，该值会低于 RequestedQuota：此时缓存池会按各租户请求的比例
	// 重新计算其有效配额。若缓存池没有已挂载的成员，则会向所有租户授予 0，该情况
	// 会携带独立的 Condition，而不会表现为普通的容量不足。授予为 0 会上报为 0；
	// 无法读取到配额时则保持字段缺失，因为两者代表不同的结果。
	EffectiveQuota *resource.Quantity `json:"effectiveQuota,omitempty" protobuf:"bytes,5,opt,name=effectiveQuota"`

	// Usage 是 master 上报的当前 Namespace 复用域的已占空间，具体对应哪项指标
	// 取决于 master 的版本而非本 API。
	//
	// 若 master 将已用字节数与预留空间分开暴露，则读取已提交字节数，刻意排除
	// 进行中的写入——否则突发并发写入会被误读为实际并未发生的消耗。若 master
	// 仅暴露单一计费数值（0.3.13 引入的形态），则在写入开始时即计入，因此
	// 进行中的预留量已包含在该数值中且无法减去。Binding 自身的 QuotaObserved
	// 消息会说明具体采用的是哪种统计口径。
	Usage *resource.Quantity `json:"usage,omitempty" protobuf:"bytes,6,opt,name=usage"`

	// OverQuota 为 true 表示 Usage 超过了 EffectiveQuota，但这并不意味着该复用域
	// 尝试写入了超出授权的数据量。存储层绝不会对超出授权的复用域记账，因此超额写入
	// 时 Usage 会停留在授权上限且该字段为 false；实际发生的是存储层会驱逐该复用域
	// 自身的对象以腾出空间并接纳写入，只有当占用授权的所有对象均被固定（pinned）且
	// 无对象可驱逐时，写入才会失败。
	//
	// 因此该字段仅报告一种情况：授权配额被重新削减至低于该复用域当前已持有的容量，
	// 即缓存池成员缩容或有新 Binding 加入触发按比例缩减时的情形。将其作为写入被拒
	// 的信号来等待，永远不会等到结果。
	//
	// 观测到 false（正常的健康状态）会直接上报为 false；仅当完全无法抓取该租户指标
	// 时字段才缺失。这两个读数最容易混淆，却代表截然相反的含义。
	OverQuota *bool `json:"overQuota,omitempty" protobuf:"varint,7,opt,name=overQuota"`

	// Blocks 和 HitRate 均从 master 与引擎观测获得，从不由用户声明。当抓取结果中
	// 不包含该租户时字段缺失，因为在预热的缓存上伪造一个 0 命中率比没有数值更糟。
	// 0 个块与“未在抓取结果中”同样是不同的事实：0 是一次测量结果，缺失则表示未测量。
	//
	// 当前 Operator 尚未写入这两个字段，因此它们始终缺失：这一代存储后端尚未按租户
	// 导出对象数量，且其命中率是按 master 而非按租户统计的。
	Blocks *int64 `json:"blocks,omitempty" protobuf:"varint,8,opt,name=blocks"`

	// HitRate 是以带格式模式约束的字符串保存的比率，绝不使用浮点数。关于为何对计算
	// 比率使用模式约束更安全，以及它对写入方有何约束，参见缓存池自身的 HitRate 说明。
	//
	// +k8s:validation:pattern="^(0(\\.[0-9]{1,4})?|1(\\.0{1,4})?)$"
	HitRate string `json:"hitRate,omitempty" protobuf:"bytes,9,opt,name=hitRate"`

	// UsedBy 列出当前 Namespace 中通过此 Binding 使用缓存池的工作负载——这始终是
	// 单一作用域查询，且 Finalizer 会基于非空的 UsedBy 拒绝删除操作。
	//
	// 该字段由使用方写入，而非由本 API 自身的 Reconciler 写入，Reconciler 仅读取
	// 它并据此执行删除拦截。写入该字段的资源类型为 ModelDeployment，其
	// spec.kvCache.poolRef 引用了同一 Namespace 下的某个 Binding，因此该列表为空
	// 并不能证明没有任何对象在使用缓存池。列表项的 Namespace 留空，因为可能出现的
	// 所有对象均位于此 Binding 所在的 Namespace 内。
	//
	// +listType=map
	// +listMapKey=kind
	// +listMapKey=namespace
	// +listMapKey=name
	UsedBy []KVCacheObjectReference `json:"usedBy,omitempty" protobuf:"bytes,10,rep,name=usedBy"`
}

// KVCachePoolBindingList 包含 KVCachePoolBinding 的列表。
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type KVCachePoolBindingList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Items []KVCachePoolBinding `json:"items" protobuf:"bytes,2,rep,name=items"`
}

var _ runtime.Object = (*KVCachePoolBindingList)(nil)
