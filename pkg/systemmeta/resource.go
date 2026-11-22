package systemmeta

import (
	"strings"

	"k8s.io/apimachinery/pkg/labels"

	"gpustack.ai/gpustack/pkg/systemname"
)

const (
	// ResourceTypeLabel 是用于标识托管资源系统类型的 label key。
	ResourceTypeLabel = "resource." + systemname.LabelPrefix + "type"

	// ResourceNoteAnnoPrefix 是用于标识资源备注的 annotation 前缀。
	ResourceNoteAnnoPrefix = "note." + systemname.LabelPrefix
)

// NoteResource 为资源添加标记：若资源类型（resType）不为空则添加 label，若 notes 不为空则设置 annotation。
//
// 资源类型是 gpustack 的内部标识，可用于指明托管资源的系统类型。
// 若资源已添加标记则返回 true，否则返回 false。
func NoteResource(obj MetaObject, resType string, notes map[string]string) bool {
	if obj == nil {
		panic("object is nil")
	}

	noted := true

	if resType != "" {
		ls := obj.GetLabels()
		if ls == nil {
			ls = make(map[string]string)
		}
		if ls[ResourceTypeLabel] != resType {
			ls[ResourceTypeLabel] = resType
			obj.SetLabels(ls)
			noted = false
		}
	}

	if len(notes) > 0 {
		as := obj.GetAnnotations()
		if as == nil {
			as = make(map[string]string)
		}
		for k, v := range notes {
			pk := ResourceNoteAnnoPrefix + k
			if as[pk] != v {
				as[pk] = v
				noted = false
			}
		}
		obj.SetAnnotations(as)
	}

	return noted
}

// DescribeResource 提取资源的详细信息，资源类型和 notes 可能为空。
func DescribeResource(obj MetaObject) (resType string, notes map[string]string) {
	if obj == nil {
		panic("object is nil")
	}

	ls := obj.GetLabels()
	if ls != nil {
		resType = ls[ResourceTypeLabel]
	}

	notes = make(map[string]string)
	as := obj.GetAnnotations()
	for k, v := range as {
		if strings.HasPrefix(k, ResourceNoteAnnoPrefix) {
			notes[k[len(ResourceNoteAnnoPrefix):]] = v
		}
	}
	return resType, notes
}

// DescribeResourceType 提取资源的资源类型，资源类型可能为空。
func DescribeResourceType(obj MetaObject) string {
	if obj == nil {
		panic("object is nil")
	}

	ls := obj.GetLabels()
	if ls != nil {
		return ls[ResourceTypeLabel]
	}

	return ""
}

// DescribeResourceNote 提取资源中指定 key 对应的备注。
func DescribeResourceNote(obj MetaObject, noteKey string) string {
	if obj == nil {
		panic("object is nil")
	}

	as := obj.GetAnnotations()
	for k, v := range as {
		if !strings.HasPrefix(k, ResourceNoteAnnoPrefix) {
			continue
		}
		if k[len(ResourceNoteAnnoPrefix):] == noteKey {
			return v
		}
	}
	return ""
}

// UnnoteResource 与 DescribeResource 类似，
// 但会移除资源的 label 和 annotation。
func UnnoteResource(obj MetaObject) (resType string, notes map[string]string) {
	if obj == nil {
		panic("object is nil")
	}

	ls := obj.GetLabels()
	if ls != nil {
		resType = ls[ResourceTypeLabel]
		delete(ls, ResourceTypeLabel)
		obj.SetLabels(ls)
	}

	notes = make(map[string]string)
	as := obj.GetAnnotations()
	if as != nil {
		for k := range as {
			if !strings.HasPrefix(k, ResourceNoteAnnoPrefix) {
				continue
			}
			notes[k[len(ResourceNoteAnnoPrefix):]] = as[k]
			delete(as, k)
		}
		obj.SetAnnotations(as)
	}

	return resType, notes
}

// GetResourcesLabelSelectorOfType 返回指定类型资源的 label selector。
func GetResourcesLabelSelectorOfType(resType string) labels.Selector {
	return labels.SelectorFromSet(labels.Set{
		ResourceTypeLabel: resType,
	})
}

// GetResourcesLabelSetOfType 返回指定类型资源的 label set。
func GetResourcesLabelSetOfType[T ~map[string]string](resType string) T {
	return map[string]string{
		ResourceTypeLabel: resType,
	}
}

// MatchResource 在给定资源与资源类型及 notes 匹配时返回 true。
func MatchResource(obj MetaObject, resType string, noteKeyAndValues ...string) bool {
	if obj == nil {
		panic("object is nil")
	}

	if DescribeResourceType(obj) != resType {
		return false
	}
	for i := 0; i < len(noteKeyAndValues); i += 2 {
		k := noteKeyAndValues[i]
		v := ""
		if i+1 < len(noteKeyAndValues) {
			v = noteKeyAndValues[i+1]
		}
		if v != DescribeResourceNote(obj, k) {
			return false
		}
	}
	return true
}

// EqualResourceTypeAndNotes 在两个资源具有相同的资源类型和 notes 时返回 true。
func EqualResourceTypeAndNotes(expected, actual MetaObject) bool {
	if expected == nil || actual == nil {
		panic("object is nil")
	}

	rtExpected, nsExpected := DescribeResource(expected)
	rtActual, nsActual := DescribeResource(actual)
	if rtExpected != rtActual {
		return false
	}

	for k, v := range nsExpected {
		if v2, ok := nsActual[k]; !ok || v != v2 {
			return false
		}
	}

	return true
}

// SyncResourceTypeAndNotes 将 actual 的资源类型和 notes 同步为 expected。
func SyncResourceTypeAndNotes(expected, actual MetaObject) {
	if expected == nil || actual == nil {
		panic("object is nil")
	}

	rtExpected, nsExpected := DescribeResource(expected)
	NoteResource(actual, rtExpected, nsExpected)
}
