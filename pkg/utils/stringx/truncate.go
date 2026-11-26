package stringx

import (
	"unicode/utf8"
)

// TruncateRunes 将 s 截取至前 n 个 Unicode 码点，只有发生截断时才追加 tail。
//
// 这里按码点计数。按显示宽度截断时，组合符、零宽空格和控制字符的宽度可能为零，再长也不会触发截断。
// Kubernetes 的 maxLength 按字符计数，因此校验状态消息长度时应使用本函数。
//
// Go 的字符串按字节存储，必须在码点边界切开，才能保留完整的 UTF-8 字符。
func TruncateRunes(s string, n int, tail string) string {
	if n < 0 || utf8.RuneCountInString(s) <= n {
		return s
	}

	var w int
	for i := range s {
		if w == n {
			return s[:i] + tail
		}
		w++
	}
	return s + tail
}
