package v1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

// ModelArtifact 是模型权重的 v1 视图：权重来自何处以及当前位于何处。
//
// 它对所有 verb 代理到底层 v1alpha1 资源，并提供 progress 子资源。它没有可写的
// STATUS 子资源：代理使用 worker 身份写入，而 status 守卫仅允许该身份写入
// ModelArtifact 的 status，挂载授权正是依据该 status 进行。status 通过主对象读取。
//
// +genclient
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:apireg-gen:resource:scope="Namespaced",categories=["gpustack"]
type ModelArtifact workercore.ModelArtifact

var _ runtime.Object = (*ModelArtifact)(nil)

// ModelArtifactList 保存 ModelArtifact 的列表。
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ModelArtifactList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	Items         []ModelArtifact `json:"items" protobuf:"bytes,2,rep,name=items"`
}

var _ runtime.Object = (*ModelArtifactList)(nil)
