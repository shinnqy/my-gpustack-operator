package v1

import meta "k8s.io/apimachinery/pkg/apis/meta/v1"

// Condition 包含此 API 资源当前状态某一方面的详细信息。
type Condition struct {
	// Type 为 Condition 的类型，采用 CamelCase 或 foo.example.com/CamelCase 格式。
	//
	// +required
	// +k8s:validation:pattern="^([a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*/)?(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])$"
	// +k8s:validation:maxLength=316
	Type string `json:"type" protobuf:"bytes,1,opt,name=type"`

	// Status 为 Condition 的状态，取值为 True、False 或 Unknown 之一。
	//
	// +required
	// +k8s:validation:enum=["True","False","Unknown"]
	Status meta.ConditionStatus `json:"status" protobuf:"bytes,2,opt,name=status"`

	// LastTransitionTime 是 Condition 上一次从一种状态转换到另一种状态的时间。
	// 这应当是底层状态发生变更的时间；若该时间未知，也可使用 API 字段变更的时间。
	//
	// +required
	// +k8s:validation:format="datetime"
	LastTransitionTime meta.Time `json:"lastTransitionTime" protobuf:"bytes,3,name=lastTransitionTime"`

	// ObservedGeneration 表示设置该 Condition 时所依据的 .metadata.generation。
	// 例如，若当前 .metadata.generation 为 12，而 .status.conditions[x].observedGeneration 为 9，
	// 则表明该 Condition 相对于实例的当前状态已过时。
	//
	// +k8s:validation:minimum=0
	ObservedGeneration int64 `json:"observedGeneration,omitempty" protobuf:"varint,4,opt,name=observedGeneration"`

	// Reason 包含一个程序化标识符，用于说明 Condition 上一次状态转换的原因。
	// 特定 Condition 类型的生成方可能会为此字段定义预期值及其含义，
	// 并约定这些值是否视为受保证的 API。
	// 该值应为 CamelCase 字符串。
	//
	// +k8s:validation:maxLength=1024
	// +k8s:validation:pattern="^$|^[A-Za-z]([A-Za-z0-9_,:]*[A-Za-z0-9_])?$"
	Reason string `json:"reason,omitempty" protobuf:"bytes,5,opt,name=reason"`

	// Message 是可读的描述信息，用于说明此次状态转换的细节。
	// 该字段可以为空字符串。
	//
	// +k8s:validation:maxLength=32768
	Message string `json:"message,omitempty" protobuf:"bytes,6,opt,name=message"`
}

// Status 包含 phase、phaseMessage 和 Condition 列表。
type Status struct {
	// Phase 是对所有 Condition 的汇总状态。
	Phase string `json:"phase,omitempty" protobuf:"bytes,1,opt,name=phase"`

	// PhaseMessage 是当前 phase 的说明信息。
	PhaseMessage string `json:"phaseMessage,omitempty" protobuf:"bytes,2,opt,name=phaseMessage"`

	// Conditions 保存该对象的 Condition 列表。
	//
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,3,rep,name=conditions"`
}
