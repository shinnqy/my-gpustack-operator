package v1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// InstanceTypeFlavor 是对由 operator 管理的 Kueue ResourceFlavor 的聚合只读投影，
// 即构建 InstanceType 所依据的硬件组目录。每个不同的资源池（加速池或通用纯 CPU 池）
// 都会作为一个 InstanceTypeFlavor 呈现，从 flavor 的 note.gpustack.ai/* 注解解析、
// 去重并排序得到。它由聚合 apiserver 以只读方式（get、list 和 watch）提供：没有对应的
// 底层 CRD 也没有控制器，管理员可借此发现编写 InstanceType 所需的组字段和描述符字段。
// Get 会根据同一份聚合列表按名称解析单个 flavor。
//
// +genclient
// +genclient:nonNamespaced
// +genclient:onlyVerbs=get,list,watch
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:apireg-gen:resource:scope="Cluster",categories=["gpustack"],shortName=["instypeflavor"]
type InstanceTypeFlavor struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Spec InstanceTypeFlavorSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
}

var _ runtime.Object = (*InstanceTypeFlavor)(nil)

// InstanceTypeFlavorSpec 描述从 ResourceFlavor 注解聚合而来的单个资源池。其字段
// 与 InstanceTypeSpec 的标识字段保持一致——accelerator/general group + acceleratable +
// manufacturer/product/family + memory/cores——使目录与 InstanceType 的语义保持统一。
// 具体填充哪些组字段取决于 instance-type-aware-cpu-manufacturer：
// 关闭感知时，通用池仅携带 GeneralGroup="generic"，加速池仅携带 AcceleratorGroup；
// 开启感知时，两者都会在 GeneralGroup 中携带实际的 CPU 键。
type InstanceTypeFlavorSpec struct {
	// AcceleratorGroup 是加速池的加速器组（acceleratable 键），
	// 例如 "nvidia-a10g"；通用池则为空。
	AcceleratorGroup string `json:"acceleratorGroup,omitempty" protobuf:"bytes,1,opt,name=acceleratorGroup"`

	// Acceleratable 表示该资源池是否代表加速硬件；通用（纯 CPU）资源池为 false。
	// 它用于区分通用 flavor 与加速 flavor。
	Acceleratable bool `json:"acceleratable" protobuf:"varint,2,name=acceleratable"`

	// Manufacturer 是设备（或 CPU）制造商，例如 "nvidia"、"generic"。
	Manufacturer string `json:"manufacturer,omitempty" protobuf:"bytes,3,opt,name=manufacturer"`

	// Product 是产品名称，例如 "NVIDIA A10G"；通用池则为空。
	Product string `json:"product,omitempty" protobuf:"bytes,4,opt,name=product"`

	// Family 是产品系列，例如 "ampere"；通用池则为空。
	Family string `json:"family,omitempty" protobuf:"bytes,5,opt,name=family"`

	// Memory 是单颗加速器的显存大小，例如 "24576Mi"；通用池则为空。
	Memory string `json:"memory,omitempty" protobuf:"bytes,6,opt,name=memory"`

	// Cores 是单颗加速器的核心数量，例如 "9216"；通用池则为空。
	Cores string `json:"cores,omitempty" protobuf:"bytes,7,opt,name=cores"`

	// GeneralGroup 是资源池的通用（CPU）组：开启 instance-type-aware-cpu-manufacturer 时为
	// 实际的 CPU 键，未开启（合并后的）通用池则为哨兵值 "generic"；
	// 关闭感知时，加速池该字段为空。
	GeneralGroup string `json:"generalGroup,omitempty" protobuf:"bytes,8,opt,name=generalGroup"`
}

// InstanceTypeFlavorList 保存 InstanceTypeFlavor 的列表。
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type InstanceTypeFlavorList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Items []InstanceTypeFlavor `json:"items" protobuf:"bytes,2,rep,name=items"`
}

var _ runtime.Object = (*InstanceTypeFlavorList)(nil)
