package v1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

// Instance 是 worker.gpustack.ai 的结构定义。
//
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:apireg-gen:resource:scope="Namespaced",categories=["gpustack"],shortName=["inst"],subResources=["status"]
type Instance workercore.Instance

var _ runtime.Object = (*Instance)(nil)

// InstanceList 保存 Instance 的列表。
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type InstanceList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Items []Instance `json:"items" protobuf:"bytes,2,rep,name=items"`
}

var _ runtime.Object = (*InstanceList)(nil)
