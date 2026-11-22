package v1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

// ModelPrefetch 是命名空间模型驻留状态的 v1 视图：哪些模型正在哪些位置预热，
// 以及当前进度。
//
// 它将配置变更代理到 v1alpha1 资源。status 通过主对象报告聚合后的节点就绪情况；
// 公共 API 不提供 status 或 progress 子资源。
//
// +genclient
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:apireg-gen:resource:scope="Namespaced",categories=["gpustack"]
type ModelPrefetch workercore.ModelPrefetch

var _ runtime.Object = (*ModelPrefetch)(nil)

// ModelPrefetchList 保存 ModelPrefetch 的列表。
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ModelPrefetchList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	Items         []ModelPrefetch `json:"items" protobuf:"bytes,2,rep,name=items"`
}

var _ runtime.Object = (*ModelPrefetchList)(nil)
