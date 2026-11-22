package v1

import (
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// InstancePersistentVolume 是 worker.gpustack.ai 的资源 schema。
//
// 在底层，InstancePersistentVolume 对应一个 Kubernetes PersistentVolumeClaim，
// 且 InstancePersistentVolume 的名称与该 Kubernetes PersistentVolumeClaim 的名称相同。
//
// +genclient
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:apireg-gen:resource:scope="Namespaced",categories=["gpustack"]
type InstancePersistentVolume struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Spec   InstancePersistentVolumeSpec   `json:"spec" protobuf:"bytes,2,name=spec"`
	Status InstancePersistentVolumeStatus `json:"status,omitempty" protobuf:"bytes,3,name=status"`
}

var _ runtime.Object = (*InstancePersistentVolume)(nil)

// InstancePersistentVolumeSpec 定义 InstancePersistentVolume 的期望状态。
type InstancePersistentVolumeSpec struct {
	// DisplayName 是 InstancePersistentVolume 的显示名称。
	//
	// +k8s:validation:maxLength=64
	DisplayName string `json:"displayName,omitempty" protobuf:"bytes,1,opt,name=displayName"`

	// Description 是 InstancePersistentVolume 的描述信息。
	//
	// +k8s:validation:maxLength=1024
	Description string `json:"description,omitempty" protobuf:"bytes,2,opt,name=description"`

	// Type 是用于置备对应 Kubernetes PersistentVolume 的 Kubernetes StorageClass 名称。
	//
	// 创建后不可变。
	Type *string `json:"type,omitempty" protobuf:"bytes,3,opt,name=type"`

	// Capacity 是 InstanceVolume 的容量。
	//
	// 创建后不可变。
	//
	// +default="20Gi"
	Capacity resource.Quantity `json:"capacity,omitempty" protobuf:"bytes,4,name=capacity"`

	// AccessMode 是 InstanceVolume 的访问模式。
	//
	// 创建后不可变。
	//
	// +k8s:validation:enum=["ReadWriteMany","ReadWriteOnce","ReadOnlyMany","ReadWriteOncePod"]
	AccessMode *core.PersistentVolumeAccessMode `json:"accessMode,omitempty" protobuf:"bytes,5,opt,name=accessMode"`
}

// InstancePersistentVolumeStatus 定义 InstancePersistentVolume 的观测状态。
type InstancePersistentVolumeStatus struct {
	// Phase 是 InstanceVolume 所处的阶段。
	Phase string `json:"phase,omitempty" protobuf:"bytes,1,opt,name=phase"`

	// Volume 是绑定到该 InstanceVolume 的 Kubernetes PersistentVolume 引用。
	Volume *core.ObjectReference `json:"volume,omitempty" protobuf:"bytes,2,opt,name=volume"`
}

// InstancePersistentVolumeList 包含 InstancePersistentVolume 列表。
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type InstancePersistentVolumeList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Items []InstancePersistentVolume `json:"items" protobuf:"bytes,2,name=items"`
}

var _ runtime.Object = (*InstancePersistentVolumeList)(nil)
