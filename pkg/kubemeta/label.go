package kubemeta

// GetLabel 返回具有给定键的标签值。
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

// SetLabel 按给定的键设置标签值。
//
// 如果标签已存在，直接更新它的值。
func SetLabel(obj MetaObject, key, value string) {
	if obj == nil {
		panic("object is nil")
	}

	ls := obj.GetLabels()
	if ls == nil {
		ls = map[string]string{}
	}

	ls[key] = value
	obj.SetLabels(ls)
}

// IsLabeled 返回 true，表示具有给定键和值的标签存在。
func IsLabeled(obj MetaObject, key, value string) bool {
	v, ok := GetLabel(obj, key)
	return ok && v == value
}

// SanitizeLabelValue 根据特定规则将给定字符串转换为有效的 Kubernetes 标签值。
// - 长度必须为 63 个字符或以下。
// - 除非为空，必须以字母数字字符（[a-z0-9A-Z]）开头和结尾。
// - 可以包含连字符（-）、下划线（_）、点（.）以及字母数字字符。
func SanitizeLabelValue(s string) string {
	if s == "" {
		return ""
	}

	const maxLength = 63

	buf := make([]rune, 0, min(len(s), maxLength))
	var pr rune
	for _, r := range s {
		switch {
		case isAlphanumericChar(r):
		case r == ' ':
			r = '-'
			fallthrough
		case isValidSignChar(r):
			if len(buf) == 0 {
				continue
			}
			if isValidSignChar(pr) {
				continue
			}
		default:
			continue
		}
		buf = append(buf, r)
		pr = r

		// 如果缓冲区达到最大长度，则停止处理。
		if len(buf) >= maxLength {
			break
		}
	}
	if len(buf) == 0 {
		return ""
	}

	// 删除末尾的非字母数字字符。
	if isValidSignChar(buf[len(buf)-1]) {
		buf = buf[:len(buf)-1]
	}

	return string(buf)
}

func isAlphanumericChar(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
}

func isValidSignChar(r rune) bool {
	return r == '-' || r == '_' || r == '.'
}
