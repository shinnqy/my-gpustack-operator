package v1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// ModelArtifactProgress 表示某个 ModelArtifact 的内容在集群各节点上的分布情况，
// 按需计算且永不持久化：汇总自各节点的 NodeModelStore，对于正在下载的节点，
// 在其插件响应时实时读取已下载字节数。
//
// 它是聚合视图且不包含任何节点名称：该资源是命名空间级别的，供租户读取，
// 暴露节点名称会把集群拓扑泄露给所有租户。单节点视图请使用 NodeModelStore。
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ModelArtifactProgress struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Timestamp 是该结果的计算时间。
	Timestamp meta.Time `json:"timestamp" protobuf:"bytes,2,opt,name=timestamp"`

	// ManifestDigest 和 SizeBytes 是该内容的信息，来自 artifact 的解析结果。
	ManifestDigest string `json:"manifestDigest,omitempty" protobuf:"bytes,3,opt,name=manifestDigest"`
	SizeBytes      int64  `json:"sizeBytes,omitempty" protobuf:"varint,4,opt,name=sizeBytes"`

	// Ready、Downloading 和 Failed 分别统计已发布该内容、正在下载该内容以及
	// 等待重试失败下载任务的节点数量。
	Ready       int32 `json:"ready" protobuf:"varint,5,opt,name=ready"`
	Downloading int32 `json:"downloading" protobuf:"varint,6,opt,name=downloading"`
	Failed      int32 `json:"failed" protobuf:"varint,7,opt,name=failed"`

	// DownloadingPercent 是正在下载节点（每个节点为完整副本）的平均进度，以整数百分比表示；
	// 当没有节点在下载时该字段不存在。
	//
	// +optional
	DownloadingPercent *int32 `json:"downloadingPercent,omitempty" protobuf:"varint,8,opt,name=downloadingPercent"`

	// DownloadingBytes 是正在下载的节点当前已持有的该内容字节数总和。
	DownloadingBytes int64 `json:"downloadingBytes" protobuf:"varint,9,opt,name=downloadingBytes"`

	// Live 统计本次响应中直接从其插件读取字节数的下载节点数；其余节点则使用
	// 该节点按 5% 阈值上次写入的值。
	Live int32 `json:"live" protobuf:"varint,10,opt,name=live"`

	// FailureReasons 按原因统计失败节点数，不包含错误消息或节点名称。
	//
	// +listType=atomic
	// +optional
	FailureReasons []ModelArtifactProgressReason `json:"failureReasons,omitempty" protobuf:"bytes,11,rep,name=failureReasons"`

	// Reason 说明无可统计数据的原因：artifact 尚未解析，或其 claim 来源
	// 直接从卷挂载而无需下载。
	//
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,12,opt,name=reason"`
}

var _ runtime.Object = (*ModelArtifactProgress)(nil)

// ModelArtifactProgressReason 表示具有相同失败原因的节点数量。
type ModelArtifactProgressReason struct {
	Reason string `json:"reason" protobuf:"bytes,1,opt,name=reason"`
	Count  int32  `json:"count" protobuf:"varint,2,opt,name=count"`
}
