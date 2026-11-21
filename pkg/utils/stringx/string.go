package stringx

import (
	"fmt"
	"strings"
)

// Join 将字符串片段连接成一个字符串。
func Join[T ~string](sep string, strs ...T) string {
	switch len(strs) {
	case 0:
		return ""
	case 1:
		return string(strs[0])
	}

	n := len(sep) * (len(strs) - 1)
	for i := 0; i < len(strs); i++ {
		n += len(strs[i])
	}

	var b strings.Builder

	b.Grow(n)
	b.WriteString(string(strs[0]))

	for i := range strs[1:] {
		b.WriteString(sep)
		b.WriteString(string(strs[i+1]))
	}

	return b.String()
}

// Strings 将实现 fmt.Stringer 的切片转换为字符串切片。
func Strings[T fmt.Stringer](v []T) []string {
	if len(v) == 0 {
		return nil
	}

	s := make([]string, len(v))
	for i := range v {
		s[i] = v[i].String()
	}

	return s
}

// ContainSubstrings 判断字符串是否包含候选子串中的任意一个。
func ContainSubstrings(s string, substrings ...string) bool {
	for i := range substrings {
		if strings.Contains(s, substrings[i]) {
			return true
		}
	}
	return false
}
