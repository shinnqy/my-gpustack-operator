package kubemeta

import meta "k8s.io/apimachinery/pkg/apis/meta/v1"

// SanitizeObjectMeta 深拷贝元数据，生成可用于创建新对象的副本。
func SanitizeObjectMeta(src meta.ObjectMeta) meta.ObjectMeta {
	dst := src.DeepCopy()

	// 清除由 API 服务器或原控制器维护的身份、版本和所有权信息，避免新对象继承原对象的生命周期。
	dst.UID = ""
	dst.ResourceVersion = ""
	dst.Generation = 0
	dst.CreationTimestamp = meta.Time{}
	dst.DeletionTimestamp = nil
	dst.DeletionGracePeriodSeconds = nil
	dst.OwnerReferences = nil
	dst.Finalizers = nil
	dst.ManagedFields = nil
	return *dst
}
