package systemname

import "gpustack.ai/gpustack/pkg/utils/osx"

// NamespaceName 表示用于存储系统资源的 Kubernetes Namespace 名称。
var NamespaceName string

func init() {
	NamespaceName = osx.Getenv("KUBERNETES_POD_NAMESPACE", "gpustack-system")
}

const (
	// LabelPrefix 是 gpustack 使用的所有 label 的前缀。
	LabelPrefix = "gpustack.ai/"

	// ManagedLabelKey 是用于标识资源是否由 gpustack 管理的 label key。
	ManagedLabelKey = LabelPrefix + "managed"
)
