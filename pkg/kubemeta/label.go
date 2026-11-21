package kubemeta

// GetLabel returns the value of the label with the given key.
func GetLabel(obj MetaObject, key string) (string, bool) {
	if obj == nil {
		panic("object is nil")
	}

	ls := obj.GetLabels()
	if ls == nil {
		return "", false
	}

	v, ok := ls[key]
	return v, ok
}

// IsLabeled returns true if the label with the given key and value exists.
func IsLabeled(obj MetaObject, key, value string) bool {
	v, ok := GetLabel(obj, key)
	return ok && v == value
}
