package v1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

// NodeModelStore 是单个节点模型缓存的 v1 视图：节点持有的数据、下载进度
// 以及生效配置。其写入方（worker 与节点插件）写入 v1alpha1 资源；此视图提供
// get、list、watch 和 delete 操作，其中 delete 供监听该 API 组首选版本的
// 垃圾回收器使用。
//
// +genclient
// +genclient:nonNamespaced
// +genclient:onlyVerbs=get,list,watch,delete
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:apireg-gen:resource:scope="Cluster",categories=["gpustack"]
type NodeModelStore workercore.NodeModelStore

var _ runtime.Object = (*NodeModelStore)(nil)

// NodeModelStoreList 保存 NodeModelStore 的列表。
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type NodeModelStoreList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	Items         []NodeModelStore `json:"items" protobuf:"bytes,2,rep,name=items"`
}

var _ runtime.Object = (*NodeModelStoreList)(nil)
