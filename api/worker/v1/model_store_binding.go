package v1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

// ModelStoreBinding 为命名空间授予模型缓存配额以及固定（pin）artifact 的权限。
//
// 配置变更会代理到 v1alpha1 资源。由控制器管理的 status 通过主对象只读访问；
// 公共 API 不提供 status 子资源。对主资源的写入会保留底层资源的 status。
//
// +genclient
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:apireg-gen:resource:scope="Namespaced",categories=["gpustack"],shortName=["msb"]
type ModelStoreBinding workercore.ModelStoreBinding

var _ runtime.Object = (*ModelStoreBinding)(nil)

// ModelStoreBindingList 保存 ModelStoreBinding 的列表。
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ModelStoreBindingList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Items []ModelStoreBinding `json:"items" protobuf:"bytes,2,rep,name=items"`
}

var _ runtime.Object = (*ModelStoreBindingList)(nil)
