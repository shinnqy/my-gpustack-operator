package v1alpha1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	gpustack "gpustack.ai/gpustack/api/v1"
)

// ModelPrefetch 是 worker.gpustack.ai 的 schema。
//
// 它表达租户对单个模型制品的驻留意图：在任何挂载权重文件的 Pod 启动前，将制品的权重提前
// 保留在一组节点上。worker 采用与冷挂载相同的方式交付——每个目标节点一个预热 Pod、使用制品自身
// 的 CSI 卷、不请求加速器——使数据通过节点缓存的常规路径落盘，每个节点仅下载一次并经校验，
// 目录树可读后 Pod 即退出。Pod 本身就是交付操作；没有独立的任务状态。
//
// Placement 用于缩小目标节点集合：可以指定节点池的 InstanceTypes，或通过 nodeSelector 指定节点；
// 若两者都省略，则取该 Namespace 中引用此制品的部署所使用的 InstanceTypes。Retention 决定
// 后续行为：内容是否被固定以防止回收（管理员授予的能力），以及节点在最后一次使用后保留多长时间。
//
// 删除该对象即撤销此意图：预热 Pod 被清理，节点的固定标记被移除，一旦数据不再被引用且超过宽限期，
// 便由节点缓存自身的回收机制回收。仍在读取目录树的 Pod 不会受到任何破坏。
//
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:crd-gen:resource:scope="Namespaced",categories=["gpustack"],shortName=["mpf"],subResources=["status"]
// +k8s:crd-gen:printcolumn:name="Artifact",type="string",jsonPath=".spec.artifactRef.name"
// +k8s:crd-gen:printcolumn:name="Binding",type="string",jsonPath=".spec.bindingRef.name"
// +k8s:crd-gen:printcolumn:name="Ready",type="integer",jsonPath=".status.readyNodes"
// +k8s:crd-gen:printcolumn:name="Desired",type="integer",jsonPath=".status.desiredNodes"
// +k8s:crd-gen:printcolumn:name="Pinned",type="boolean",jsonPath=".spec.retention.pinned"
// +k8s:crd-gen:printcolumn:name="Age",type="date",jsonPath=".metadata.creationTimestamp"
type ModelPrefetch struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Spec   ModelPrefetchSpec   `json:"spec" protobuf:"bytes,2,name=spec"`
	Status ModelPrefetchStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

var _ runtime.Object = (*ModelPrefetch)(nil)

// ModelPrefetchSpec 定义驻留意图。
type ModelPrefetchSpec struct {
	// ArtifactRef 指定当前 Namespace 中用于预热目标集合权重的 ModelArtifact 名称。
	// 在渲染任何 Pod 之前必须先解析它：准入和交付阶段都会读取其摘要（digest）。
	//
	// +required
	ArtifactRef ModelPrefetchArtifactReference `json:"artifactRef" protobuf:"bytes,1,name=artifactRef"`

	// BindingRef 指定当前 Namespace 中承担字节配额的 ModelStoreBinding 名称。它必须
	// 授予覆盖目标集合的存储，且其预算在准入时限制预取规模。
	//
	// +required
	BindingRef ModelPrefetchBindingReference `json:"bindingRef" protobuf:"bytes,2,name=bindingRef"`

	// Placement 用于缩小目标节点集合。InstanceTypes 和 NodeSelector 互斥；
	// 同时设置会被拒绝，均省略时则从当前 Namespace 中引用该制品的部署推导目标集合。
	// 这些关联规则由 Webhook 强制执行。
	//
	// +optional
	Placement *ModelPrefetchPlacement `json:"placement,omitempty" protobuf:"bytes,3,opt,name=placement"`

	// MinReady 表示持有内容的目标节点达到多少个时预取即视为 Available；0 表示全部节点。
	//
	// +optional
	// +k8s:validation:default=0
	// +k8s:validation:minimum=0
	MinReady int32 `json:"minReady,omitempty" protobuf:"varint,4,opt,name=minReady"`

	// Retention 决定内容落盘后的保留策略。
	//
	// +optional
	Retention ModelPrefetchRetention `json:"retention,omitempty" protobuf:"bytes,5,opt,name=retention"`
}

// ModelPrefetchArtifactReference 指定预取对象所属 Namespace 中的制品名称。
type ModelPrefetchArtifactReference struct {
	// +required
	Name string `json:"name" protobuf:"bytes,1,name=name"`
}

// ModelPrefetchBindingReference 指定预取对象所属 Namespace 中的绑定名称。
type ModelPrefetchBindingReference struct {
	// +required
	Name string `json:"name" protobuf:"bytes,1,name=name"`
}

// ModelPrefetchPlacement 用于缩小制品预热的目标节点集合。
type ModelPrefetchPlacement struct {
	// InstanceTypes 通过调度链已发布的 InstanceType 对象指定要预热的节点池。
	// 目标集合为带有这些类型所对应 Flavor 的所有节点。
	//
	// +optional
	// +listType=atomic
	// +k8s:validation:minItems=1
	// +k8s:validation:maxItems=8
	InstanceTypes []string `json:"instanceTypes,omitempty" protobuf:"bytes,1,rep,name=instanceTypes"`

	// NodeSelector 直接选择目标节点，即“下载到这些节点”的形式。
	//
	// +optional
	NodeSelector *meta.LabelSelector `json:"nodeSelector,omitempty" protobuf:"bytes,2,opt,name=nodeSelector"`
}

// ModelPrefetchRetention 决定内容落盘后的保留策略。
type ModelPrefetchRetention struct {
	// Pinned 使内容在每个目标节点上免受节点缓存回收：被固定的摘要（digest）永远不会成为
	// 驱逐候选，但仍计入已用空间。它要求关联的 Binding 允许固定，否则准入时会被拒绝。
	//
	// +optional
	Pinned bool `json:"pinned,omitempty" protobuf:"varint,1,opt,name=pinned"`

	// TTLAfterLastUse 在节点副本超过该时长未被任何挂载使用后取消固定。它按节点上报已包含的
	// 小时粒度执行：保留策略不需要更高精度，更精细的记录只会成倍增加节点写入开销，
	// 而对决策并无实质区别。
	//
	// +optional
	TTLAfterLastUse *meta.Duration `json:"ttlAfterLastUse,omitempty" protobuf:"bytes,2,opt,name=ttlAfterLastUse"`
}

// ModelPrefetchStatus 是预取任务对交付状态的视图，从目标节点的上报聚合而来：
// 哪些节点已持有摘要、哪些正在拉取，以及是否已有足够节点就绪。
type ModelPrefetchStatus struct {
	// DesiredNodes 是当前 Placement 解析出的目标节点集合大小。
	//
	// +optional
	DesiredNodes int32 `json:"desiredNodes,omitempty" protobuf:"varint,1,opt,name=desiredNodes"`

	// ReadyNodes 是上报摘要已发布且可挂载的目标节点数量。
	//
	// +optional
	ReadyNodes int32 `json:"readyNodes,omitempty" protobuf:"varint,2,opt,name=readyNodes"`

	// DownloadingNodes 是仍在拉取该摘要的目标节点数量。
	//
	// +optional
	DownloadingNodes int32 `json:"downloadingNodes,omitempty" protobuf:"varint,3,opt,name=downloadingNodes"`

	// Conditions：Progressing 表示交付正在进行中；Available 表示 ReadyNodes 已达到
	// MinReady 阈值；Degraded 表示某个目标节点已放弃交付，具体原因见 message。
	//
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []gpustack.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,4,rep,name=conditions"` // nolint: lll
}

// ModelPrefetchList 包含 ModelPrefetch 列表。
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ModelPrefetchList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Items []ModelPrefetch `json:"items" protobuf:"bytes,2,rep,name=items"`
}

var _ runtime.Object = (*ModelPrefetchList)(nil)
