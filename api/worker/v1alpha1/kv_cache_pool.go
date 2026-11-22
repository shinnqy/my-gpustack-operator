package v1alpha1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	gpustack "gpustack.ai/gpustack/api/v1"
)

// KVCachePool 是 worker.gpustack.ai 的架构定义。
//
// KVCacheBackend 声明物理缓存，而本资源声明授予哪些命名空间配额、配额大小以及使用何种
// 复用身份。它之于 KVCachePoolBinding，犹如 Kueue 中的 ClusterQueue 之于 LocalQueue：
// 作为单个后端上的配额域，以及其下各 Binding 所声明复用域的注册表。
//
// 它负责授权与记账，但不负责准入控制——此处没有任何机制阻止知晓复用域名称的 Pod 访问
// 存储层上的该域，KVCachePoolBinding 说明了此项授权的边界所在。其之所以为集群作用域，
// 原因与后端相同：只有管理员才能声明特权物理资源，池必须能够跨命名空间共享，且从
// 命名空间作用域对象发起跨命名空间引用属于反模式。数据平面隔离属于另一维度，应由存储层的
// 租户机制而非本对象的作用域来解决。
//
// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:crd-gen:resource:scope="Cluster",categories=["gpustack"],shortName=["kvcp"],subResources=["status"]
// +k8s:crd-gen:printcolumn:name="Quota",type="string",jsonPath=".spec.quota.total"
// +k8s:crd-gen:printcolumn:name="Usage",type="string",jsonPath=".status.usage.total"
// +k8s:crd-gen:printcolumn:name="Phase",type="string",jsonPath=".status.phase"
// +k8s:crd-gen:printcolumn:name="Endpoint",type="string",jsonPath=".status.clientEndpoint"
// +k8s:crd-gen:printcolumn:name="Age",type="date",jsonPath=".metadata.creationTimestamp"
type KVCachePool struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Spec   KVCachePoolSpec   `json:"spec" protobuf:"bytes,2,name=spec"`
	Status KVCachePoolStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

var _ runtime.Object = (*KVCachePool)(nil)

// KVCachePoolSpec 定义 KVCachePool 的期望规格。
type KVCachePoolSpec struct {
	// Backends 指定该池所依托的 KVCacheBackend 名称。每一项均为后端的 NAME，而非
	// 对象引用：旁边无需填写 kind 或 namespace。
	//
	//   - 仅允许填写一项，该规则由 webhook 而非 schema 强制执行，以便在拒绝时能附带
	//     明确原因：配额落账在单个 master 的按租户账本上，一个 master 无法统计存储在
	//     另一后端中的字节数。
	//   - 该字段由 webhook 强制保持不可变。将池迁移到另一个后端会使原 master 账本上的
	//     所有租户配额成为孤儿记录，且后续无法再进行清理。
	//   - 反向关系并非排他：同一个后端可被多个池引用，因此账本与渲染后的策略均按
	//     MASTER 而非按池维度收敛。
	//
	// +required
	// +listType=atomic
	Backends []string `json:"backends" protobuf:"bytes,1,rep,name=backends"`

	// Quota 是该池在其后端上声明的容量上限。
	//
	// +required
	Quota KVCachePoolQuota `json:"quota" protobuf:"bytes,2,name=quota"`
}

// KVCachePoolQuota 是该池声明的容量上限。
//
// 它是本 API 侧设定的数值，而非 master 上报的值。master 会上报自身的可分配容量，
// 两者可能不一致；当冲突达到极端情况（即在尚未挂载任何介质的后端上声明了上限）时，
// 会通过 Condition 予以提示，而不会静默忽略。
type KVCachePoolQuota struct {
	// Total 为必填项：未声明上限的池没有可写入任何账本的配额基数。
	//
	// +required
	Total resource.Quantity `json:"total" protobuf:"bytes,1,name=total"`
}

// KVCachePoolStatus 定义 KVCachePool 的观测状态。
type KVCachePoolStatus struct {
	// Phase 汇总各项条件的状态：Provisioning、Ready、Error、Deleting。
	Phase string `json:"phase,omitempty" protobuf:"bytes,1,opt,name=phase"`

	// PhaseMessage 携带当前 Phase 的原因说明。
	PhaseMessage string `json:"phaseMessage,omitempty" protobuf:"bytes,2,opt,name=phaseMessage"`

	// Conditions 提供更细粒度的状态视图，每个维度对应一项条件。
	//
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []gpustack.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,3,rep,name=conditions"` // nolint: lll

	// ClientEndpoint 是推理引擎连接的地址，直接回显后端的 Client 端点。
	//
	// 每当后端尚未发布任何端点时，该字段为空，并伴随一条说明原因的 Condition；同时绝不会
	// 根据后端自身的名称去拼接猜测 Service 名称来填充——如果猜出的地址恰巧能够解析，
	// 池就会静默地连接到错误的 master。
	//
	// 后端的 ADMIN 地址刻意不在任何地方转发暴露。该端口同时提供 Prometheus 指标暴露与
	// HTTP 管理 API，因此属于配额账本的写入面，而本对象为集群作用域，任何拥有池 RBAC
	// 权限的主体均可读取。
	//
	// +k8s:validation:maxLength=259
	ClientEndpoint string `json:"clientEndpoint,omitempty" protobuf:"bytes,4,opt,name=clientEndpoint"`

	// Usage 表示本池所属租户当前占用的空间。在一次抓取成功之前该字段不存在，
	// 字段缺失与上报为零具有完全不同的语义。
	Usage *KVCachePoolUsage `json:"usage,omitempty" protobuf:"bytes,5,opt,name=usage"`

	// Domains 是针对本池注册的复用身份表，每个 Binding 对应一条记录。
	//
	// 该列表具有权威性而非仅供参考：某条记录存在是因为有 Binding 声明了它，而不是因为
	// 工作负载自行上报了存在，因此只需遍历一次 Binding 索引即可构建该列表，无需 watch
	// 任何工作负载资源类型。
	//
	// +listType=map
	// +listMapKey=name
	Domains []KVCachePoolDomain `json:"domains,omitempty" protobuf:"bytes,6,rep,name=domains"`

	// UsedBy 列出占用本池的 Binding。非空的 UsedBy 是 finalizer 拒绝删除的依据，
	// 因此该字段是实际生效的约束输入，而不仅仅是展示信息。
	//
	// 与 Binding 自身的列表不同，本字段由本 API 的 reconciler 写入，该 reconciler 已通过
	// 索引列出了池对应的 Binding。因此两级 usedBy 各自拥有不同的写入方，且只有本字段是
	// 自洽维护的。
	//
	// +listType=map
	// +listMapKey=kind
	// +listMapKey=namespace
	// +listMapKey=name
	UsedBy []KVCacheObjectReference `json:"usedBy,omitempty" protobuf:"bytes,7,rep,name=usedBy"`
}

// KVCacheObjectReference 指定消费本组中另一对象的资源。它是本系列资源中所有 usedBy
// 字段统一采用的结构，读者只需了解一次即可。
//
// 该结构不含 API GROUP，这是一种刻意约束而非遗漏：usedBy 条目只能引用本 API 组内的
// 类型，在该组内仅凭 kind 和 name 即可唯一确定对象。正是这一约束使得列表能够正常
// 建立主键——core.TypedLocalObjectReference 带有无默认值的可选 apiGroup，结构化
// schema 拒绝将其用作列表 map 键；而如果仅按 kind 和 name 建键，则会静默合并
// 仅在组名上存在差异的两个对象。
//
// 三个字段均为必填项，且共同构成列表 map 键：结构化 schema 仅允许必填或有默认值的
// 字段作为键，若两个消费者合并为一条记录，就会导致消费者仍持有引用时 finalizer 便提前
// 释放。当被引用对象与持有者处于同一作用域时，Namespace 填入空字符串，因此此处空字符串
// 代表有效取值而非缺失。
type KVCacheObjectReference struct {
	// +required
	Kind string `json:"kind" protobuf:"bytes,1,name=kind"`

	// +required
	Namespace string `json:"namespace" protobuf:"bytes,2,name=namespace"`

	// +required
	Name string `json:"name" protobuf:"bytes,3,name=name"`
}

// KVCachePoolUsage 是由 master 上报的池内租户实际占用量。
type KVCachePoolUsage struct {
	// Total 是 master 为本池所拥有租户上报的占用量总和——绝非其整个账本的总量，因为
	// 共享后端会使账本总量大于单个池。未观测到的 total 保持缺失状态而非置为零，因为零值
	// 会被误读为空缓存。
	//
	// 占用量的具体含义取决于 master 的实现而非本 API，且发生过变更：将已用字节与预留空间
	// 分开暴露的 master 按已提交字节汇总；而暴露单一计费数值的 master（0.3.13 引入的形态）
	// 会在写入开始时即计入费用，因此进行中的预留空间已包含在此总量内。在不清楚后端所运行
	// 版本行为的情况下，切勿将其直接视为已提交用量。
	Total *resource.Quantity `json:"total,omitempty" protobuf:"bytes,1,opt,name=total"`
}

// KVCachePoolDomain 是注册到本池的一条复用身份记录：包括其名称、声明者以及
// master 上报的相关信息。
type KVCachePoolDomain struct {
	// Name 是复用身份标识，对应存储层的 tenant_id。
	//
	// +required
	// +k8s:validation:maxLength=63
	Name string `json:"name" protobuf:"bytes,1,name=name"`

	// Binding 是声明该域的对象。它是唯一可以声明域的地方，从而确保命名一个域属于
	// 受控的特权操作。
	//
	// +required
	Binding KVCachePoolBindingReference `json:"binding" protobuf:"bytes,2,name=binding"`

	// BlockSize 和 Dtype 直接回显自注册该域的 Binding，使注册表的读取方无需逐一拉取
	// 每个 Binding 即可获知该域的数据块规格。
	//
	// 与下方的观测指标不同，这两项均为必填：它们复制自已要求这两项必填的 Binding，若条目
	// 缺少其中任一项只能说明写入方存在 bug；如果将其设为可选，则意味着注册表允许出现
	// “该域数据块形状未知”的回答，而这在实际上是不存在的状态。
	//
	// Dtype 的拼写严格匹配其 JSON 名称。写成 DType 则会不一致，openapi 生成器会将此类
	// 大小写不匹配记录为受检的 API 规则违规。
	//
	// +required
	BlockSize int32 `json:"blockSize" protobuf:"varint,3,name=blockSize"`

	// +required
	Dtype string `json:"dtype" protobuf:"bytes,4,name=dtype"`

	// Blocks 和 HitRate 属于观测值而非声明值，当抓取结果中不包含该域时保持缺失状态。
	// 在热缓存上伪造一个零命中率比没有数值更糟，而块数为零与“未出现在抓取结果中”是
	// 两个完全不同的事实。
	//
	// 当前版本的 operator 均不写入这两个字段，因此它们始终保持缺失：现阶段的存储层
	// 不导出按域划分的对象计数，且其命中率是按 master 而非按域统计的。
	Blocks *int64 `json:"blocks,omitempty" protobuf:"varint,5,opt,name=blocks"`

	// HitRate 是以带正则约束的 STRING 形式保存的比率（绝不使用浮点数），与被测接口
	// 自身的格式保持一致。相比在透传的厂商字段上使用枚举，在此使用正则约束是安全的，因为
	// 该比率由本 operator 计算生成。但这也要求写入方必须严格按该格式输出：任何不符合正则
	// 的值都会导致整个 status 写入失败，使其他所有字段都停留在上一次的值上，而不仅仅是
	// 这一个字段。
	//
	// +k8s:validation:pattern="^(0(\\.[0-9]{1,4})?|1(\\.0{1,4})?)$"
	HitRate string `json:"hitRate,omitempty" protobuf:"bytes,6,opt,name=hitRate"`
}

// KVCachePoolBindingReference 指定一个 KVCachePoolBinding。
//
// 与 KVCacheObjectReference 不同，它不携带 kind，因为它并非 usedBy 引用：它是从
// 已注册域指回唯一声明对象的反向指针，而只有 KVCachePoolBinding 才能声明域。
// 由于 Binding 属于命名空间作用域，两个字段均为必填。
type KVCachePoolBindingReference struct {
	// +required
	Namespace string `json:"namespace" protobuf:"bytes,1,name=namespace"`

	// +required
	Name string `json:"name" protobuf:"bytes,2,name=name"`
}

// KVCachePoolList 包含 KVCachePool 的列表。
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type KVCachePoolList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Items []KVCachePool `json:"items" protobuf:"bytes,2,rep,name=items"`
}

var _ runtime.Object = (*KVCachePoolList)(nil)
