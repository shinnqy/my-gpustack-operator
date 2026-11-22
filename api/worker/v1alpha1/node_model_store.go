package v1alpha1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	gpustack "gpustack.ai/gpustack/api/v1"
)

// NodeModelStore 是 worker.gpustack.ai 的 API Schema。
//
// 它代表单个节点的模型缓存，以 Node 名称命名并归属于该 Node，并以与 Node
// 相同的方式承载节点契约的两端：worker 写入 spec，即它根据 Settings 计算出
// 的生效配置；该节点上的 model-manager 插件写入 status，即节点当前持有的
// 内容。status.models 报告缓存内容，就像 Node.status.images 报告镜像一样。
//
// 其中不包含任何租户名称。它是集群作用域的资源，因此在此处写入 Namespace、
// 制品、仓库或 Pod 都会造成跨租户信息泄露；每个条目仅包含摘要及其大小。
//
// 当节点的 CSINode 列出该插件的驱动（即 kubelet 自身记录的插件已注册状态）
// 时，worker 会创建该对象；当驱动因重启或滚动升级等原因短暂从 CSINode 中
// 消失时，worker 绝不会删除它。只有运行在同名节点上的插件 Pod 才能写入
// status，该限制由 Webhook 强制执行。
//
// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:crd-gen:resource:scope="Cluster",categories=["gpustack"],shortName=["nms"],subResources=["status"]
// +k8s:crd-gen:printcolumn:name="Ready",type="string",jsonPath=".status.conditions[?(@.type=='Ready')].status"
// +k8s:crd-gen:printcolumn:name="Used",type="integer",jsonPath=".status.capacity.usedPercent"
// +k8s:crd-gen:printcolumn:name="Age",type="date",jsonPath=".metadata.creationTimestamp"
type NodeModelStore struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Spec   NodeModelStoreSpec   `json:"spec" protobuf:"bytes,2,name=spec"`
	Status NodeModelStoreStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

var _ runtime.Object = (*NodeModelStore)(nil)

// NodeModelStoreSpec 是节点的生效配置，即 worker 对 Settings 的合并结果。
//
// 没有任何租户可写的对象会向其提供输入：如果租户能够选择特权节点进程的
// 连接目标，就可能诱使其请求任意地址。插件在使用配置前会再次校验，因为
// 手动编辑会绕过 worker 的检查。
type NodeModelStoreSpec struct {
	// PeerSync 为 nil 时默认启用：节点的插件可以拉取其他节点插件已发布
	// 的内容。
	//
	// +optional
	PeerSyncEnabled *bool `json:"peerSyncEnabled,omitempty" protobuf:"bytes,7,opt,name=peerSyncEnabled"`

	// Watermarks 限制缓存文件系统的使用率范围。
	//
	// +required
	Watermarks NodeModelStoreWatermarks `json:"watermarks" protobuf:"bytes,1,name=watermarks"`

	// Download 限制节点的下载行为。
	//
	// +required
	Download NodeModelStoreDownload `json:"download" protobuf:"bytes,2,name=download"`

	// Hub 定义节点访问模型中心的方式。
	//
	// +required
	Hub NodeModelStoreHub `json:"hub" protobuf:"bytes,3,name=hub"`

	// Kubelet 是节点生效的 kubelet 阈值，缓存上限由此推导得出，由 worker
	// 从 kubelet 的 configz 端点读取，该端点合并了 kubelet 的启动标志、配置文件和
	// drop-in 配置。当无法读取时该字段缺失，此时插件会采用 kubelet 的默认值并
	// 在状态中说明。
	//
	// 该字段仅由 worker 写入，其他组件均不写入。它作为观测值存放在 spec 而非
	// status 中，原因在于插件从 spec 读取完整配置并独占 status 的所有权，且
	// 读取 configz 需要 nodes/proxy 权限，而插件从不持有该权限（该权限可访问
	// 所有 kubelet 端点）。没有任何控制器会将其作为期望状态进行调谐：worker
	// 会刷新读数并写入读取结果，因此手动修改仅在下次读取前有效。
	//
	// +optional
	Kubelet *NodeModelStoreKubelet `json:"kubelet,omitempty" protobuf:"bytes,4,opt,name=kubelet"`

	// Store 指定节点生效配置所采用的单池策略所属的 ModelStore 名称：当有两个
	// ModelStore 的选择器同时匹配该节点时取按字母序排在最前的那个；均不匹配时
	// 为空，此时应用集群默认配置。该字段仅由 worker 写入，其他组件均不写入。
	//
	// +optional
	Store string `json:"store,omitempty" protobuf:"bytes,5,opt,name=store"`

	// Pinned 列出节点必须保留的 manifest 摘要：即保留策略使其固定且目标集合
	// 包含该节点的所有预取的并集。被固定的摘要绝不会成为回收候选项，但仍会
	// 计入使用量。prefetch 控制器负责写入该字段，而配置调谐器会原样保留它，从而
	// 使两者不会在同一对象上互相覆盖对方的工作。
	//
	// +optional
	// +listType=set
	// +k8s:validation:maxItems=256
	Pinned []string `json:"pinned,omitempty" protobuf:"bytes,6,rep,name=pinned"`
}

// NodeModelStoreKubelet 是节点生效 kubelet 配置中用于限制与 kubelet 共享
// 文件系统的缓存的部分：低于可用阈值时 kubelet 会驱逐 Pod，高于镜像阈值时
// 会回收镜像。
type NodeModelStoreKubelet struct {
	// NodefsAvailable 是 kubelet 报告的 evictionHard["nodefs.available"]，格式
	// 为诸如 "10%" 的百分比或诸如 "20Gi" 的容量值；若 kubelet 未设置则为空。
	//
	// +optional
	NodefsAvailable string `json:"nodefsAvailable,omitempty" protobuf:"bytes,1,opt,name=nodefsAvailable"`

	// ImagefsAvailable 是 evictionHard["imagefs.available"]，格式与前者相同；
	// 若 kubelet 未设置则为空。
	//
	// +optional
	ImagefsAvailable string `json:"imagefsAvailable,omitempty" protobuf:"bytes,2,opt,name=imagefsAvailable"`

	// ImageGCHighThresholdPercent 是 kubelet 开始回收镜像时的磁盘使用率阈值。
	//
	// +optional
	// +k8s:validation:minimum=0
	// +k8s:validation:maximum=100
	ImageGCHighThresholdPercent *int32 `json:"imageGCHighThresholdPercent,omitempty" protobuf:"varint,3,opt,name=imageGCHighThresholdPercent"` // nolint: lll
}

// NodeModelStoreWatermarks 是缓存文件系统上所有内容占用空间的使用率百分比。
type NodeModelStoreWatermarks struct {
	// HighPercent 是触发插件清理未引用内容的使用率阈值。在缓存与 kubelet 共享
	// 的文件系统上，插件可能会应用更低的阈值，以免 kubelet 因缓存占用而驱逐 Pod
	// 或回收镜像。
	//
	// +required
	// +k8s:validation:minimum=2
	// +k8s:validation:maximum=95
	HighPercent int32 `json:"highPercent" protobuf:"varint,1,name=highPercent"`

	// LowPercent 是单次回收清理后降至的使用率目标，低于 HighPercent。
	//
	// +required
	// +k8s:validation:minimum=1
	// +k8s:validation:maximum=94
	LowPercent int32 `json:"lowPercent" protobuf:"varint,2,name=lowPercent"`
}

// NodeModelStoreDownload 限制节点的所有下载任务（全局总量控制）。
type NodeModelStoreDownload struct {
	// Concurrency 是节点同时运行的 HTTP 请求数。大文件会按字节范围分片获取并
	// 共享此并发预算，因为实测表明单文件单连接会决定整个下载任务的墙钟耗时。
	//
	// +required
	// +k8s:validation:minimum=1
	// +k8s:validation:maximum=64
	Concurrency int32 `json:"concurrency" protobuf:"varint,1,name=concurrency"`

	// BytesPerSecond 是节点的下载速率限制；0 表示不限制。
	//
	// +optional
	// +k8s:validation:minimum=0
	BytesPerSecond int64 `json:"bytesPerSecond,omitempty" protobuf:"varint,2,opt,name=bytesPerSecond"`
}

// NodeModelStoreHub 定义节点访问模型中心的方式，与 ModelArtifact 控制器
// 及引擎下载时所采用的方式一致。
type NodeModelStoreHub struct {
	// HuggingFaceEndpoint 是 Hugging Face Hub 的基础 URL。
	//
	// +required
	// +k8s:validation:minLength=1
	HuggingFaceEndpoint string `json:"huggingFaceEndpoint" protobuf:"bytes,1,name=huggingFaceEndpoint"`

	// HTTPSProxy 是下载所经过的代理地址，为不含凭据的 http 或 https URL。
	//
	// +optional
	HTTPSProxy string `json:"httpsProxy,omitempty" protobuf:"bytes,2,opt,name=httpsProxy"`

	// NoProxy 是以逗号分隔、绕过 HTTPSProxy 的主机列表。
	//
	// +optional
	NoProxy string `json:"noProxy,omitempty" protobuf:"bytes,3,opt,name=noProxy"`

	// CABundleConfigMap 指定位于 operator 所在 Namespace 中的一个 ConfigMap 名称，
	// 其 "ca.crt" 键保存了除系统证书池外额外受信任的 PEM 证书。这里只传递名称
	// 而非证书内容本身，因为证书包体积可能超过当前对象所允许的大小限制。
	//
	// +optional
	CABundleConfigMap string `json:"caBundleConfigMap,omitempty" protobuf:"bytes,4,opt,name=caBundleConfigMap"`

	// ModelScopeEndpoint 是 ModelScope 模型中心的基础 URL。在旧版 worker 写入
	// 的 spec 中该字段为空，此类节点上的 ModelScope 制品会以该原因为由等待，
	// 而不会回退到其他模型中心进行解析。
	//
	// +optional
	ModelScopeEndpoint string `json:"modelScopeEndpoint,omitempty" protobuf:"bytes,5,opt,name=modelScopeEndpoint"`
}

// NodeModelStoreStatus 是插件上报的节点状态，在插件每次启动时根据节点
// 磁盘与挂载状态重建。仅当其中内容发生变化时才会写入，因此缓存内容未变动的
// 节点不会产生写入操作。
type NodeModelStoreStatus struct {
	// ObservedGeneration 是插件当前已应用的 spec generation。
	//
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty" protobuf:"varint,1,opt,name=observedGeneration"`

	// Capacity 是缓存文件系统的总容量与使用量。
	//
	// +optional
	Capacity *NodeModelStoreCapacity `json:"capacity,omitempty" protobuf:"bytes,2,opt,name=capacity"`

	// Models 是节点上的缓存内容，以摘要为键。当磁盘上的内容超出容纳上限时，
	// 会优先保留被引用的条目，其次按最近使用顺序保留，未列出的数量会在 Ready
	// Condition 的消息中统计并说明。
	//
	// +optional
	// +listType=map
	// +listMapKey=digest
	// +k8s:validation:maxItems=256
	Models []NodeModelStoreModel `json:"models,omitempty" protobuf:"bytes,3,rep,name=models"`

	// Conditions：Ready 表示插件正常提供服务并已应用 spec 对应 generation 的配置；
	// CapacityLow 表示使用率已高于高水位线且没有插件可移除的内容。
	//
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []gpustack.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,4,rep,name=conditions"` // nolint: lll
}

// NodeModelStoreCapacity 是缓存文件系统的总容量与使用量，其粒度较粗，仅在
// 内容变化时才发生改变。
type NodeModelStoreCapacity struct {
	// TotalBytes 是缓存文件系统的总大小。
	//
	// +required
	TotalBytes int64 `json:"totalBytes" protobuf:"varint,1,name=totalBytes"`

	// StoredBytes 是已发布的目录树和部分下载文件占用的字节数。
	//
	// +required
	StoredBytes int64 `json:"storedBytes" protobuf:"varint,2,name=storedBytes"`

	// UsedPercent 是文件系统上所有内容的总使用率，向下取整为 5 的倍数。
	//
	// +required
	UsedPercent int32 `json:"usedPercent" protobuf:"varint,3,name=usedPercent"`
}

// NodeModelStoreModel 表示节点上的一个摘要条目。
type NodeModelStoreModel struct {
	// Digest 是 manifest 摘要，即内容的寻址标识。
	//
	// +required
	Digest string `json:"digest" protobuf:"bytes,1,name=digest"`

	// State 取值为 Downloading、Ready 或 Failed。
	//
	// +required
	// +k8s:validation:enum=["Downloading","Ready","Failed"]
	State NodeModelStoreModelState `json:"state" protobuf:"bytes,2,name=state,casttype=NodeModelStoreModelState"`

	// SizeBytes 是内容的大小。
	//
	// +optional
	SizeBytes int64 `json:"sizeBytes,omitempty" protobuf:"varint,3,opt,name=sizeBytes"`

	// DownloadedBytes 是处于 Downloading 状态的条目在节点磁盘上已有的字节数，
	// 包含此前尝试留下的用于断点续传的字节，因此该值绝不会回退。该字段按阈值
	// 而非每字节写入：仅当条目的下载进度推进了 SizeBytes 的 5% 且距离上次写入
	// 节点状态已过去 30 秒时才更新。在非 Downloading 状态下该字段缺失。
	//
	// +optional
	// +k8s:validation:minimum=0
	DownloadedBytes int64 `json:"downloadedBytes,omitempty" protobuf:"varint,9,opt,name=downloadedBytes"`

	// Source 表示内容字节的来源：Hub 表示模型中心，Peer 表示其他节点的插件。
	// 该字段不包含任何仓库或端点名称。在该字段引入之前发布的内容此字段为空。
	//
	// +optional
	// +k8s:validation:enum=["Hub","Peer"]
	Source NodeModelStoreModelSource `json:"source,omitempty" protobuf:"bytes,10,opt,name=source,casttype=NodeModelStoreModelSource"` // nolint: lll

	// Referenced 表示有 Pod 正在挂载该内容。从不记录具体是哪个 Pod。
	//
	// +optional
	Referenced bool `json:"referenced,omitempty" protobuf:"varint,4,opt,name=referenced"`

	// LastUsedTime 是内容上次被挂载或卸载的时间，截断到小时粒度，避免日常使用
	// 频繁重写该对象。
	//
	// +optional
	LastUsedTime *meta.Time `json:"lastUsedTime,omitempty" protobuf:"bytes,5,opt,name=lastUsedTime"`

	// Reason 对 Failed 状态的条目进行分类：InvalidRequest、AccessDenied、
	// SourceUnavailable、IntegrityMismatch、InsufficientCapacity 或 Canceled。
	//
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,6,opt,name=reason"`

	// Message 以面向人类阅读的文字说明原因，其中不包含任何租户名称：说明 Reason
	// 的含义及诸如模型中心返回的 HTTP 状态码等细节。包含文件路径与 URL 的完整
	// 错误信息记录在插件日志中。该字段不会被任何组件解析，且绝不携带凭据。
	//
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,7,opt,name=message"`

	// RetryTime 是 Failed 状态条目允许下次尝试的最早时间。在此之前，对该摘要的
	// 挂载请求会直接返回而不触发下载，因为 kubelet 会重试每次失败的挂载，否则
	// 每次重试都会重新下载全部内容。
	//
	// +optional
	RetryTime *meta.Time `json:"retryTime,omitempty" protobuf:"bytes,8,opt,name=retryTime"`
}

// NodeModelStoreModelSource 表示节点内容的来源。
// +enum
type NodeModelStoreModelSource string

// NodeModelStoreModelSourceHub 表示从模型中心下载。
const NodeModelStoreModelSourceHub NodeModelStoreModelSource = "Hub"

// NodeModelStoreModelSourcePeer 表示从其他节点的插件下载。
const NodeModelStoreModelSourcePeer NodeModelStoreModelSource = "Peer"

// NodeModelStoreModelState 表示摘要在节点上的状态。
// +enum
type NodeModelStoreModelState string

const (
	// NodeModelStoreModelStateDownloading 表示正在物化中。
	NodeModelStoreModelStateDownloading NodeModelStoreModelState = "Downloading"
	// NodeModelStoreModelStateReady 表示已发布且可挂载。
	NodeModelStoreModelStateReady NodeModelStoreModelState = "Ready"
	// NodeModelStoreModelStateFailed 表示上次尝试失败，正在等待 RetryTime。
	NodeModelStoreModelStateFailed NodeModelStoreModelState = "Failed"
)

// NodeModelStoreList 包含 NodeModelStore 列表。
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type NodeModelStoreList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Items []NodeModelStore `json:"items" protobuf:"bytes,2,rep,name=items"`
}

var _ runtime.Object = (*NodeModelStoreList)(nil)
