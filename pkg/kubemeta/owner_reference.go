package kubemeta

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/ptr"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
)

// MetaObject is the interface for the object with metadata.
type MetaObject = ctrlcli.Object

// IsControlledBy checks if the object has a controller owner reference set to the given owner.
func IsControlledBy(obj, owner MetaObject) bool {
	ref := GetControllerOfNoCopy(obj)
	if ref == nil {
		return false
	}
	return ref.UID == owner.GetUID()
}

// IsControlledByGVK checks if the object has a controller owner reference set to the given owner GVK.
func IsControlledByGVK(obj MetaObject, ownerGVK schema.GroupVersionKind) bool {
	ref := GetOwnerRefOfNoCopy(obj, ownerGVK)
	if ref == nil {
		return false
	}
	return ptr.Deref(ref.Controller, false)
}

// GetControllerOfNoCopy returns a pointer to the controller owner reference if obj has a controller.
func GetControllerOfNoCopy(obj MetaObject) *meta.OwnerReference {
	refs := obj.GetOwnerReferences()
	for i := range refs {
		if refs[i].Controller != nil && *refs[i].Controller {
			return &refs[i]
		}
	}
	return nil
}

// NewControllerRef creates a blocked owner reference pointing to the given owner.
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

// ControlOn sets the owner reference of obj to owner with the given GVK.
//
// ControlOn will append a new owner reference if obj does not have the given kind of owner,
// otherwise it will replace the existing owner reference.
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
		// Append.
		ors = append(ors, *ref)
	} else {
		// Replace.
		ors[idx] = *ref
	}
	obj.SetOwnerReferences(ors)
}

// ControlOnWithoutBlock is similar to ControlOn,
// which sets the owner reference of obj to owner with the given GVK without block.
//
// ControlOnWithoutBlock will append a new owner reference if obj does not have the given kind of owner,
// otherwise it will replace the existing owner reference.
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
		// Append.
		ors = append(ors, *ref)
	} else {
		// Replace.
		ors[idx] = *ref
	}
	obj.SetOwnerReferences(ors)
}

// GetOwnerRefOfNoCopy returns a pointer to the owner reference if obj has the given kind of owner.
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
