package kubemeta

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// GetNamespacedNameKey 将 types.NamespacedName（也可传指针）或实现 metav1.Object 的对象转成带命名空间的字符串键。
// 其他输入返回空字符串。
func GetNamespacedNameKey(obj any) (key string) {
	switch t := obj.(type) {
	case types.NamespacedName:
		return t.String()
	case *types.NamespacedName:
		return t.String()
	case meta.Object:
		return (types.NamespacedName{
			Namespace: t.GetNamespace(),
			Name:      t.GetName(),
		}).String()
	}
	return ""
}
