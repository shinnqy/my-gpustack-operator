package v1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

// InstanceType 是 worker.gpustack.ai 的 schema。
//
// 在底层，一个 InstanceType 映射到一个 Kueue ClusterQueue，
// 且 InstanceType 的名称与该 ClusterQueue 的名称相同。
//
// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:apireg-gen:resource:scope="Cluster",categories=["gpustack"],shortName=["instype"],subResources=["status"]
type InstanceType workercore.InstanceType

var _ runtime.Object = (*InstanceType)(nil)

// InstanceTypeList 保存 InstanceType 的列表。
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type InstanceTypeList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Items []InstanceType `json:"items" protobuf:"bytes,2,rep,name=items"`
}

var _ runtime.Object = (*InstanceTypeList)(nil)
