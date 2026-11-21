package kubemeta

import (
	kvalidation "k8s.io/apimachinery/pkg/api/validation"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

const _LastAppliedConfigAnnotation = "kubectl.kubernetes.io/last-applied-configuration"

// OverwriteLastAppliedAnnotation overwrites the last-applied-configuration annotation if it exists.
//
// It usually to satisfy the requirement of the `kubectl apply ...`.
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

	// Temporarily remove managed fields.
	mf := obj.GetManagedFields()
	defer func() {
		obj.SetManagedFields(mf)
	}()
	obj.SetManagedFields(nil)

	// Erase old last-applied annotation.
	delete(as, _LastAppliedConfigAnnotation)
	if len(as) == 0 {
		obj.SetAnnotations(nil)
	} else {
		obj.SetAnnotations(as)
	}

	// Set new last-applied annotation.
	lastApplied, err := runtime.Encode(unstructured.UnstructuredJSONScheme, obj)
	if err != nil {
		// Failed to encode, ignore.
		return
	}
	as[_LastAppliedConfigAnnotation] = string(lastApplied)
	if err = kvalidation.ValidateAnnotationsSize(as); err != nil {
		// Too large, ignore.
		return
	}
	obj.SetAnnotations(as)
}

// ConfigureLastAppliedAnnotation is similar to OverwriteLastAppliedAnnotation,
// but it always sets the last-applied-configuration annotation.
//
// It usually to satisfy the requirement of the `kubectl apply ...`.
func ConfigureLastAppliedAnnotation(obj MetaObject) {
	if obj == nil {
		panic("object is nil")
	}

	as := obj.GetAnnotations()
	if as == nil {
		as = map[string]string{}
	}

	// Imagine the last-applied-configuration annotation is existed.
	as[_LastAppliedConfigAnnotation] = "{}"
	obj.SetAnnotations(as)
	// Then overwrite it.
	OverwriteLastAppliedAnnotation(obj)
}
