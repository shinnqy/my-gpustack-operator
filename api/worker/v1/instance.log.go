package v1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// InstanceLog 是 Instance 用于提取日志的子资源，
// 其行为与 Kubernetes PodLog 相同。
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type InstanceLog struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
}

var _ runtime.Object = (*InstanceLog)(nil)

// TODO(thxCode): 为 *Options 类型支持 swagger 文档生成。

// InstanceLogOptions 是 InstanceLog 的选项，
// 其定义与 Kubernetes PodLogOptions 相同。
//
// +k8s:conversion-gen:explicit-from=net/url.Values
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type InstanceLogOptions struct {
	meta.TypeMeta `json:",inline"`

	// Follow 持续跟踪 Instance 的日志流。
	Follow bool `json:"follow,omitempty" protobuf:"varint,1,opt,name=follow"`

	// 从当前时间往前推算的相对时间（单位为秒），用于指定从何时开始显示日志。
	// 如果该时间早于 Instance 启动的时间，
	// 则仅返回自 Instance 启动以来的日志。
	// 如果该时间位于将来，则不返回任何日志。
	// sinceSeconds 和 sinceTime 只能指定其中一个。
	//
	SinceSeconds *int64 `json:"sinceSeconds,omitempty" protobuf:"varint,2,opt,name=sinceSeconds"`

	// 用于指定从何时开始显示日志的 RFC3339 时间戳。如果该时间
	// 早于 Instance 启动的时间，则仅返回自 Instance 启动以来的日志。
	// 如果该时间位于将来，则不返回任何日志。
	// sinceSeconds 和 sinceTime 只能指定其中一个。
	SinceTime *meta.Time `json:"sinceTime,omitempty" protobuf:"bytes,3,opt,name=sinceTime"`

	// 如果为 true，则在日志输出的每一行开头添加 RFC3339 或 RFC3339Nano 时间戳。
	Timestamps bool `json:"timestamps,omitempty" protobuf:"varint,4,opt,name=timestamps"`

	// 如果设置，则表示从日志末尾开始显示的行数。如果未指定，
	// 则从容器创建时或 sinceSeconds、sinceTime 指定的时间点开始显示日志。
	TailLines *int64 `json:"tailLines,omitempty" protobuf:"varint,5,opt,name=tailLines"`

	// 如果设置，则表示在终止日志输出前从服务端读取的字节数上限。
	// 这可能不会显示完整的最后一行日志，实际返回的字节数也可能略多或略少于指定的上限。
	LimitBytes *int64 `json:"limitBytes,omitempty" protobuf:"varint,6,opt,name=limitBytes"`
}
