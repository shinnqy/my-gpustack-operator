package kubemeta

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/ptr"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
)

// MetaObject 是包含元数据的对象的接口。
type MetaObject = ctrlcli.Object

// IsControlledBy 用于检查对象是否有一个设置为指定所有者的控制器所有者引用。
func IsControlledBy(obj, owner MetaObject) bool {
	ref := GetControllerOfNoCopy(obj)
	if ref == nil {
		return false
	}
	return ref.UID == owner.GetUID()
}

// IsControlledByGVK 用于检查该对象是否有一个控制器所有者引用，该引用被设置为指定的所有者 GVK。
func IsControlledByGVK(obj MetaObject, ownerGVK schema.GroupVersionKind) bool {
	ref := GetOwnerRefOfNoCopy(obj, ownerGVK)
	if ref == nil {
		return false
	}
	return ptr.Deref(ref.Controller, false)
}

// GetControllerOfNoCopy 如果对象有控制器，则返回指向控制器所有者的指针。
func GetControllerOfNoCopy(obj MetaObject) *meta.OwnerReference {
	refs := obj.GetOwnerReferences()
	for i := range refs {
		if refs[i].Controller != nil && *refs[i].Controller {
			return &refs[i]
		}
	}
	return nil
}

// NewControllerRef 会创建一个指向指定所有者的被阻塞所有者引用。
func NewControllerRef(owner MetaObject, gvk schema.GroupVersionKind) *meta.OwnerReference {
	return &meta.OwnerReference{
		APIVersion:         gvk.GroupVersion().String(),
		Kind:               gvk.Kind,
		Name:               owner.GetName(),
		UID:                owner.GetUID(),
		BlockOwnerDeletion: ptr.To(true),
		Controller:         ptr.To(true),
	}
}

// ControlOn 将 obj 的拥有者引用设置为给定的 GVK。
//
// ControlOn 会如果对象没有指定的类型拥有者，则添加一个新的拥有者引用。否则会替换现有的拥有者引用。
func ControlOn(obj, owner MetaObject, gvk schema.GroupVersionKind) {
	var (
		gv  = gvk.GroupVersion().String()
		ors = obj.GetOwnerReferences()
		ref = NewControllerRef(owner, gvk)
	)

	idx := -1
	for i := range ors {
		if ors[i].APIVersion == gv && ors[i].Kind == gvk.Kind {
			idx = i
			break
		}
	}
	if idx == -1 {
		// 追加。
		ors = append(ors, *ref)
	} else {
		// 替换。
		ors[idx] = *ref
	}
	obj.SetOwnerReferences(ors)
}

// ControlOnWithoutBlock 与 ControlOn 类似，它会将 obj 的拥有者引用设置为给定的 GVK，且无需等待阻塞完成。
//
// ControlOnWithoutBlock 如果对象没有指定的类型拥有者，则会添加一个新的拥有者引用。否则会替换现有的拥有者
// 引用。
func ControlOnWithoutBlock(obj, owner MetaObject, gvk schema.GroupVersionKind) {
	var (
		gv  = gvk.GroupVersion().String()
		ors = obj.GetOwnerReferences()
		ref = NewControllerRef(owner, gvk)
	)

	ref.BlockOwnerDeletion = ptr.To(false)

	idx := -1
	for i := range ors {
		if ors[i].APIVersion == gv && ors[i].Kind == gvk.Kind {
			idx = i
			break
		}
	}
	if idx == -1 {
		// 追加。
		ors = append(ors, *ref)
	} else {
		// 替换。
		ors[idx] = *ref
	}
	obj.SetOwnerReferences(ors)
}

// ControlOff 会将 obj 的拥有者引用移除至给定的 GVK，并报告是否发生了移除。
//
// ControlOff 会移除对象的所有者引用，如果对象具有指定类型的所有者。一个空的所有者会匹配任何指定类型的引用，
// 无论其指向何种所有者，这就是对未知、已消失或不再符合预期的所有者引用的处理方式。
func ControlOff(obj, owner MetaObject, gvk schema.GroupVersionKind) (removed bool) {
	var (
		gv  = gvk.GroupVersion().String()
		ors = obj.GetOwnerReferences()
	)

	for i := range ors {
		if ors[i].APIVersion == gv && ors[i].Kind == gvk.Kind &&
			(owner == nil || ors[i].UID == owner.GetUID()) {
			obj.SetOwnerReferences(append(ors[:i], ors[i+1:]...))
			return true
		}
	}
	return false
}

// GetOwnerRefOfNoCopy 如果对象具有指定的所有者类型，则返回指向该所有者引用的指针。
func GetOwnerRefOfNoCopy(obj MetaObject, gvk schema.GroupVersionKind) *meta.OwnerReference {
	var (
		gv  = gvk.GroupVersion().String()
		ors = obj.GetOwnerReferences()
	)
	for i := range ors {
		if ors[i].APIVersion == gv && ors[i].Kind == gvk.Kind {
			return &ors[i]
		}
	}
	return nil
}
