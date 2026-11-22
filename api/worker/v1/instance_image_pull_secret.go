package v1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// InstanceImagePullSecret 是 worker.gpustack.ai 的资源 schema。
//
// 在底层，InstanceImagePullSecret 对应一个 Kubernetes Secret，
// 且 InstanceImagePullSecret 的名称与该 Kubernetes Secret 的名称相同。
//
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:apireg-gen:resource:scope="Namespaced",categories=["gpustack"]
type InstanceImagePullSecret struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Spec InstanceImagePullSecretSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
}

// InstanceImagePullSecretSpec 定义 InstanceImagePullSecret 的期望状态。
type InstanceImagePullSecretSpec struct {
	// DisplayName 是 InstanceImagePullSecret 的显示名称。
	//
	// +k8s:validation:maxLength=64
	DisplayName string `json:"displayName,omitempty" protobuf:"bytes,1,opt,name=displayName"`

	// Description 是 InstanceImagePullSecret 的描述信息。
	//
	// +k8s:validation:maxLength=1024
	Description string `json:"description,omitempty" protobuf:"bytes,2,opt,name=description"`

	// Registry 是镜像拉取凭证对应的镜像仓库地址。
	//
	// 该字段为只写输入，在创建或更新操作中必填。
	//
	// 必须在同一次操作中与 Username、Password 以及可选的 Email 一并提供。
	Registry string `json:"registry,omitempty" protobuf:"bytes,3,opt,name=registry"`

	// Username 是镜像拉取凭证的用户名。
	//
	// 该字段为只写输入，在创建或更新操作中必填。
	//
	// 必须在同一次操作中与 Registry、Password 以及可选的 Email 一并提供。
	Username string `json:"username,omitempty" protobuf:"bytes,4,opt,name=username"`

	// Password 是镜像拉取凭证的密码。
	//
	// 该字段为只写输入，在创建或更新操作中必填。
	//
	// 必须在同一次操作中与 Registry、Username 以及可选的 Email 一并提供。
	Password string `json:"password,omitempty" protobuf:"bytes,5,opt,name=password"`

	// Email 是镜像拉取凭证的邮箱地址。
	//
	// 该字段为只写输入，在创建或更新操作中必填。
	//
	// 可在同一次操作中与 Registry、Username 和 Password 一并提供。
	Email string `json:"email,omitempty" protobuf:"bytes,6,opt,name=email"`
}

var _ runtime.Object = (*InstanceSSHPublicKey)(nil)

// InstanceImagePullSecretList 包含 InstanceImagePullSecret 列表。
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type InstanceImagePullSecretList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Items []InstanceImagePullSecret `json:"items" protobuf:"bytes,2,name=items"`
}

var _ runtime.Object = (*InstanceImagePullSecretList)(nil)
