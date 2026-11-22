package v1alpha1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	gpustack "gpustack.ai/gpustack/api/v1"
)

// ModelStore 是 worker.gpustack.ai 的 schema。
//
// 它是单个节点池的缓存策略，即管理员在集群默认值之上按节点池配置的覆盖层：控制其选择器
// 匹配的每个节点的水位线和下载限制，仅覆盖显式设置的字段。未被任何 store 匹配的节点保留
// Settings 中的集群默认值，因此本对象无需重复声明这些默认值。
//
// 此处刻意不提供缓存根路径字段：它属于插件 DaemonSet 的 hostPath，在部署时确定，
// 按池区分路径需要部署独立的 DaemonSet，而不是在此设置字段。
//
// 两个 store 不得同时匹配同一个节点。worker 拒绝猜测：它会确定性地按字母序选取第一个名称，
// 将该选择记录在节点的 NodeModelStore.spec.store 中，并在两个 store 上同时设置
// SelectorOverlap 状态，以便在配置处直观暴露该错误配置。
//
// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:crd-gen:resource:scope="Cluster",categories=["gpustack"],shortName=["ms"],subResources=["status"]
// +k8s:crd-gen:printcolumn:name="Nodes",type="integer",jsonPath=".status.nodes"
// +k8s:crd-gen:printcolumn:name="High",type="integer",jsonPath=".spec.watermarks.highPercent"
// +k8s:crd-gen:printcolumn:name="Low",type="integer",jsonPath=".spec.watermarks.lowPercent"
// +k8s:crd-gen:printcolumn:name="Age",type="date",jsonPath=".metadata.creationTimestamp"
type ModelStore struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Spec   ModelStoreSpec   `json:"spec" protobuf:"bytes,2,name=spec"`
	Status ModelStoreStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

var _ runtime.Object = (*ModelStore)(nil)

// ModelStoreSpec 定义节点池的缓存策略。除选择器外所有字段均为可选：字段为 nil 时
// 在匹配节点上保留集群默认值。
type ModelStoreSpec struct {
	// NodeSelector 选择该节点池中的节点。两个 store 的选择器匹配同一节点属于错误配置，
	// 本 API 会上报该问题而非静默处理；参见 SelectorOverlap。
	//
	// +required
	NodeSelector meta.LabelSelector `json:"nodeSelector" protobuf:"bytes,1,name=nodeSelector"`

	// Watermarks 限制匹配节点的缓存文件系统使用率。为 nil 时保留集群默认值。
	//
	// +optional
	Watermarks *NodeModelStoreWatermarks `json:"watermarks,omitempty" protobuf:"bytes,2,opt,name=watermarks"`

	// Download 限制匹配节点的下载行为。为 nil 时保留集群默认值。
	//
	// +optional
	Download *ModelStoreDownload `json:"download,omitempty" protobuf:"bytes,3,opt,name=download"`
}

// ModelStoreDownload 按字段定义节点池的下载策略：省略的字段不作指定并保留集群默认值，
// 因此节点池可以提高并发度而无需重申带宽限制。显式设置字段即为真正覆盖——例如
// bytesPerSecond 为 0 表示“该节点池不限速”，而非“未指定”。
type ModelStoreDownload struct {
	// Concurrency 表示匹配节点同时执行的 HTTP 请求数；省略时应用集群默认值。
	//
	// +optional
	// +k8s:validation:minimum=1
	// +k8s:validation:maximum=64
	Concurrency *int32 `json:"concurrency,omitempty" protobuf:"varint,1,opt,name=concurrency"`

	// BytesPerSecond 表示匹配节点的下载速率上限；0 表示不限速。省略时应用集群默认值。
	//
	// +optional
	// +k8s:validation:minimum=0
	BytesPerSecond *int64 `json:"bytesPerSecond,omitempty" protobuf:"varint,2,opt,name=bytesPerSecond"`
}

// ModelStoreStatus 记录 worker 对该节点池的观测结果：匹配的节点数、这些节点缓存
// 占用的总量，以及选择器是否与其他 store 重叠。
//
// Conditions：Ready 表示 store 的策略已应用到所有匹配节点；SelectorOverlap 表示
// 至少有一个匹配节点同时也匹配了另一个 store 的选择器。
type ModelStoreStatus struct {
	// Nodes 是选择器当前匹配的节点数量。
	//
	// +optional
	Nodes int32 `json:"nodes,omitempty" protobuf:"varint,1,opt,name=nodes"`

	// Capacity 汇总匹配节点的 NodeModelStore 容量读数。在没有任何匹配节点上报之前该字段为空。
	//
	// +optional
	Capacity *ModelStoreCapacity `json:"capacity,omitempty" protobuf:"bytes,2,opt,name=capacity"`

	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []gpustack.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,3,rep,name=conditions"` // nolint: lll
}

// ModelStoreCapacity 表示该节点池的缓存占用统计，为所有匹配节点的总和。
type ModelStoreCapacity struct {
	// TotalBytes 是所有匹配节点的缓存文件系统总容量之和。
	//
	// +required
	TotalBytes int64 `json:"totalBytes" protobuf:"varint,1,name=totalBytes"`

	// StoredBytes 是所有匹配节点上已发布目录树与未完成下载所占用的总字节数。
	//
	// +required
	StoredBytes int64 `json:"storedBytes" protobuf:"varint,2,name=storedBytes"`
}

// ModelStoreList 包含 ModelStore 列表。
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ModelStoreList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Items []ModelStore `json:"items" protobuf:"bytes,2,rep,name=items"`
}

var _ runtime.Object = (*ModelStoreList)(nil)
