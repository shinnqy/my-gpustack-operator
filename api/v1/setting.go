package v1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// Setting 是 gpustack.ai 的结构定义。
//
// 其所在 Namespace 为系统 Namespace，这意味着 Setting 为系统作用域资源。
//
// 在底层，Setting 映射到 Kubernetes Secret 中的一个键值对，
// 且 Setting 的名称与该 Secret 的 data 键名相同。
//
// +genclient
// +genclient:onlyVerbs=get,list,watch,apply,update,patch
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:apireg-gen:resource:scope="Namespaced",categories=["gpustack"],shortName=["set"]
type Setting struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Spec   SettingSpec   `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
	Status SettingStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

var _ runtime.Object = (*Setting)(nil)

// SettingSpec 定义了 Setting 的期望状态。
type SettingSpec struct {
	// Value 包含配置数据，
	// 作为只写输入字段提供。
	Value *string `json:"value,omitempty" protobuf:"bytes,1,opt,name=value"`
}

// SettingStatus 定义了 Setting 的观测状态。
type SettingStatus struct {
	// Description 是该设置的描述信息。
	Description string `json:"description,omitempty" protobuf:"bytes,1,opt,name=description"`

	// Editable 指示该设置是否可在 UI 上编辑。
	Editable bool `json:"editable" protobuf:"varint,2,name=editable"`

	// Sensitive 指示该设置是否为敏感信息。
	Sensitive bool `json:"sensitive" protobuf:"varint,3,name=sensitive"`

	// Value 是该设置的当前值，
	// 作为只读输出字段提供。
	//
	// 若该设置为敏感信息，则返回 "(sensitive)"。
	Value string `json:"value" protobuf:"bytes,4,name=value"`
	// Value_ 是 Value 的影子字段，
	// 仅供系统内部处理使用。
	//
	// 请勿对外暴露。
	Value_ string `json:"-"`
}

// SettingList 保存 Setting 的列表。
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type SettingList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Items []Setting `json:"items" protobuf:"bytes,2,rep,name=items"`
}

var _ runtime.Object = (*SettingList)(nil)
