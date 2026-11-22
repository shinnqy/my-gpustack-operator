package v1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// InstanceSSHPublicKey 是 worker.gpustack.ai 的资源 schema。
//
// 在底层，InstanceSSHPublicKey 对应一个 Kubernetes Secret，
// 且 InstanceSSHPublicKey 的名称与该 Kubernetes Secret 的名称相同。
//
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:apireg-gen:resource:scope="Namespaced",categories=["gpustack"]
type InstanceSSHPublicKey struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Spec InstanceSSHPublicKeySpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
}

type InstanceSSHPublicKeySpec struct {
	// DisplayName 是 InstanceSSHPublicKey 的显示名称。
	//
	// +k8s:validation:maxLength=64
	DisplayName string `json:"displayName,omitempty" protobuf:"bytes,1,opt,name=displayName"`

	// Description 是 InstanceSSHPublicKey 的描述信息。
	//
	// +k8s:validation:maxLength=1024
	Description string `json:"description,omitempty" protobuf:"bytes,2,opt,name=description"`

	// Data 是 SSH 公钥数据。
	Data string `json:"data,omitempty" protobuf:"bytes,3,opt,name=data"`
}

var _ runtime.Object = (*InstanceSSHPublicKey)(nil)

// InstanceSSHPublicKeyList 保存 InstanceSSHPublicKey 的列表。
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type InstanceSSHPublicKeyList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Items []InstanceSSHPublicKey `json:"items" protobuf:"bytes,2,name=items"`
}

var _ runtime.Object = (*InstanceSSHPublicKeyList)(nil)
