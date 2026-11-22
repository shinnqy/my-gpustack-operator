package v1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

// ModelStore 为管理员选定的节点声明缓存策略。
//
// 配置变更会代理到 v1alpha1 资源。由控制器管理的 status 通过主对象只读访问；
// 公共 API 不提供 status 子资源。对主资源的写入会保留底层资源的 status。
//
// +genclient
// +genclient:noStatus
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:apireg-gen:resource:scope="Cluster",categories=["gpustack"],shortName=["ms"]
type ModelStore workercore.ModelStore

var _ runtime.Object = (*ModelStore)(nil)

// ModelStoreList 保存 ModelStore 的列表。
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ModelStoreList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Items []ModelStore `json:"items" protobuf:"bytes,2,rep,name=items"`
}

var _ runtime.Object = (*ModelStoreList)(nil)
