package stringx

import (
	"fmt"
	"strings"
)

// Join concatenates the elements of strs to create a single string.
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

// Strings converts a slice of fmt.Stringer to a slice of string.
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
