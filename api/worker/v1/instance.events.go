package v1

import (
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// InstanceEvents 是 Instance 子资源的事件对象，
// 提供与底层 Kubernetes Pod 相关的事件信息。
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type InstanceEvents struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Items []core.Event `json:"items" protobuf:"bytes,2,rep,name=items"`
}

var _ runtime.Object = (*InstanceEvents)(nil)
