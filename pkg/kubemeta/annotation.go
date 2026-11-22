package kubemeta

import (
	kvalidation "k8s.io/apimachinery/pkg/api/validation"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

// GetAnnotation 返回具有给定键的注释值。
func GetAnnotation(obj MetaObject, key string) (string, bool) {
	if obj == nil {
		panic("object is nil")
	}

	as := obj.GetAnnotations()
	if as == nil {
		return "", false
	}

	v, ok := as[key]
	return v, ok
}

// HasAnnotation 返回 true，表示具有给定键的注释存在。
func HasAnnotation(obj MetaObject, key string) bool {
	_, ok := GetAnnotation(obj, key)
	return ok
}

const _LastAppliedConfigAnnotation = "kubectl.kubernetes.io/last-applied-configuration"

// OverwriteLastAppliedAnnotation 会覆盖存在的最后应用配置注释。
//
// 通常为了满足 `kubectl apply ...` 的需求。
func OverwriteLastAppliedAnnotation(obj MetaObject) {
	if obj == nil {
		panic("object is nil")
	}

	as := obj.GetAnnotations()
	if as == nil {
		return
	} else if _, ok := as[_LastAppliedConfigAnnotation]; !ok {
		return
	}

	// 临时移除受管理的字段。
	mf := obj.GetManagedFields()
	defer func() {
		obj.SetManagedFields(mf)
	}()
	obj.SetManagedFields(nil)

	// 删除最后应用的旧注释。
	delete(as, _LastAppliedConfigAnnotation)
	if len(as) == 0 {
		obj.SetAnnotations(nil)
	} else {
		obj.SetAnnotations(as)
	}

	// 设置新的最后应用注释。
	lastApplied, err := runtime.Encode(unstructured.UnstructuredJSONScheme, obj)
	if err != nil {
		// 编码失败，忽略。
		return
	}
	as[_LastAppliedConfigAnnotation] = string(lastApplied)
	if err = kvalidation.ValidateAnnotationsSize(as); err != nil {
		// 太大，忽略。
		return
	}
	obj.SetAnnotations(as)
}

// ConfigureLastAppliedAnnotation 与 OverwriteLastAppliedAnnotation 类似，但它总是设置
// `last-applied-configuration` 注释。
//
// 通常为了满足 `kubectl apply ...` 的需求。
func ConfigureLastAppliedAnnotation(obj MetaObject) {
	if obj == nil {
		panic("object is nil")
	}

	as := obj.GetAnnotations()
	if as == nil {
		as = map[string]string{}
	}

	// 假设存在“最后应用配置”注释。
	as[_LastAppliedConfigAnnotation] = "{}"
	obj.SetAnnotations(as)
	// 然后覆盖它。
	OverwriteLastAppliedAnnotation(obj)
}
