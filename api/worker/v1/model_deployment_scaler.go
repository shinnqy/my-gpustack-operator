package v1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

// ModelDeploymentScaler 是基于指标驱动的副本规划器的 v1 视图。
//
// 配置变更会代理到 v1alpha1 资源。提供 status 子资源，因此无需通过 alpha 版本
// 即可读取规划器的决策。
//
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:apireg-gen:resource:scope="Namespaced",categories=["gpustack"],subResources=["status"]
type ModelDeploymentScaler workercore.ModelDeploymentScaler

var _ runtime.Object = (*ModelDeploymentScaler)(nil)

// ModelDeploymentScalerList 保存 ModelDeploymentScaler 的列表。
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ModelDeploymentScalerList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	Items         []ModelDeploymentScaler `json:"items" protobuf:"bytes,2,rep,name=items"`
}

var _ runtime.Object = (*ModelDeploymentScalerList)(nil)
