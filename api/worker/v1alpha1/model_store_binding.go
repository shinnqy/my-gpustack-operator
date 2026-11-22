package v1alpha1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	gpustack "gpustack.ai/gpustack/api/v1"
)

// ModelStoreBinding 是 worker.gpustack.ai 的 schema。
//
// 它是模型预取的授权配置点：在某个 Namespace 中创建该对象，即授予该 Namespace 在指定
// store 上的字节预算及固定权限，从而使这两项能力都成为管理员可通过 RBAC 控制和审计的授权，
// 而非租户默认拥有的权限。没有 Binding 的 Namespace 无法预热任何内容，同时 Binding 也是
// 上报其配额消耗的唯一位置。
//
// 授权必须是显式的。Binding 不会回退到任何默认节点池：Chart 中的默认节点池仅是部署配置，
// 而非授权依据，回退机制会为“谁可以在何处预热”制造第二个事实来源。
//
// 它不是强制执行边界，也不应被描述为强制边界。该预算仅用于控制预取准入与配额核算；
// 工作负载通过常规交付路径挂载的数据仍按原样计入节点水位线，而不会计入此对象。
//
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:crd-gen:resource:scope="Namespaced",categories=["gpustack"],shortName=["msb"],subResources=["status"]
// +k8s:crd-gen:printcolumn:name="Quota",type="string",jsonPath=".spec.quota.bytes"
// +k8s:crd-gen:printcolumn:name="AllowPinned",type="boolean",jsonPath=".spec.allowPinned"
// +k8s:crd-gen:printcolumn:name="Used",type="string",jsonPath=".status.usedBytes"
// +k8s:crd-gen:printcolumn:name="Phase",type="string",jsonPath=".status.phase"
// +k8s:crd-gen:printcolumn:name="Age",type="date",jsonPath=".metadata.creationTimestamp"
type ModelStoreBinding struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Spec   ModelStoreBindingSpec   `json:"spec" protobuf:"bytes,2,name=spec"`
	Status ModelStoreBindingStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

var _ runtime.Object = (*ModelStoreBinding)(nil)

// ModelStoreBindingSpec 定义授权内容。除 AllowPinned 从 false 变为 true 以及 Quota.Bytes
// 增大外，所有字段均不可变（由 Webhook 强制校验）：重新指向授权目标会遗留旧的配额账目，
// 而在已有消耗的情况下缩减预算会追溯性地导致已准入的预取任务超出预算。
type ModelStoreBindingSpec struct {
	// StoreRefs 指定该 Namespace 可预取进入的 ModelStore 名称，至少包含一个。
	// 若预取的目标集合落在所有已授权 store 之外，预取准入将予以拒绝。
	//
	// +required
	// +listType=atomic
	// +k8s:validation:minItems=1
	// +k8s:validation:maxItems=8
	StoreRefs []ModelStoreBindingStoreReference `json:"storeRefs" protobuf:"bytes,1,rep,name=storeRefs"`

	// Quota 是该 Namespace 的字节预算。
	//
	// +required
	Quota ModelStoreBindingQuota `json:"quota" protobuf:"bytes,2,name=quota"`

	// AllowPinned 表示该 Namespace 中的预取任务是否允许固定内容。默认为 false，且是启用
	// 固定的唯一途径：固定是管理员授予的能力，绝非租户默认权限。
	//
	// +optional
	AllowPinned *bool `json:"allowPinned,omitempty" protobuf:"varint,3,opt,name=allowPinned"`
}

// ModelStoreBindingStoreReference 指定授予该 Namespace 的集群级 ModelStore 名称。
//
// 它不包含 Namespace 字段，这正是设计所在：store 是集群级资源，本就无 Namespace 可指定，
// 且此处任何带有 Namespace 的对象都不会跨 Namespace 边界读取资源。
type ModelStoreBindingStoreReference struct {
	// +required
	Name string `json:"name" protobuf:"bytes,1,name=name"`
}

// ModelStoreBindingQuota 是该 Namespace 的字节预算，按文件系统使用量计量。
type ModelStoreBindingQuota struct {
	// Bytes 是该 Namespace 的预取总共可占用的字节数。计量依据为内容自身的
	// 大小，按摘要和持有该内容的每个节点分别计算，因此两个 Namespace 共享的目录树
	// 会全额计入各自配额——这种方式较为保守，也能防止租户通过预热其他租户已预热
	// 的内容来规避配额限制。增大配额可通过准入检查，减小配额将被拒绝。
	//
	// +required
	Bytes resource.Quantity `json:"bytes" protobuf:"bytes,1,name=bytes"`
}

// ModelStoreBindingStatus 是该 Namespace 对其配额授予情况的自身视图：当前使用量以及是否
// 超出配额。
//
// 下方每项观测指标在未测量时均表现为缺失（ABSENT）而非零，两者含义
// 不同：读数为零表示实际测量结果为零，而缺失字段则表示根本未进行
// 测量。
type ModelStoreBindingStatus struct {
	// Phase 汇总各项状态条件：Ready、OverQuota、Error。
	Phase string `json:"phase,omitempty" protobuf:"bytes,1,opt,name=phase"`

	// PhaseMessage 携带处于该 Phase 的原因。
	PhaseMessage string `json:"phaseMessage,omitempty" protobuf:"bytes,2,opt,name=phaseMessage"`

	// Conditions 提供更精细的视图，每个维度对应一个 Condition。
	//
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []gpustack.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,3,rep,name=conditions"` // nolint: lll

	// UsedBytes 是该 Namespace 的预取所占用的字节数，即任一预取指向的每个
	// 不同摘要在所有持有它的节点上的完整大小之和。尚未进行任何测量时该字段
	// 缺失，而零值代表实际测量结果而非未测量。
	//
	// +optional
	UsedBytes *resource.Quantity `json:"usedBytes,omitempty" protobuf:"bytes,4,opt,name=usedBytes"`
}

// ModelStoreBindingList 包含 ModelStoreBinding 的列表。
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ModelStoreBindingList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Items []ModelStoreBinding `json:"items" protobuf:"bytes,2,rep,name=items"`
}

var _ runtime.Object = (*ModelStoreBindingList)(nil)
