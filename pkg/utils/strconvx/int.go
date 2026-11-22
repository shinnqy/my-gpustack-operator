package strconvx

import (
	"strconv"
	"strings"

	"gpustack.ai/gpustack/pkg/utils/typex"
)

// Atoi 把字符串转换为指定泛型整数类型。
func Atoi[T typex.Integer](s string) (T, error) {
	i, err := strconv.Atoi(s)
	return T(i), err
}

// Itoa 将任意支持的整数类型转换为十进制字符串。
func Itoa[T typex.Integer](i T) string {
	return strconv.Itoa(int(i))
}

// FormatInt 按指定进制格式化泛型整数。
func FormatInt[T typex.SignedInteger](i T, base int) string {
	return strconv.FormatInt(int64(i), base)
}

// PadZeroUint 以十进制对 i 进行格式化，用零填充至 width 个字符宽度。如果数值的位数已达到 width，则不进行
// 填充，因此结果永远不会被截断。非正数的 width 则不会进行任何填充。
func PadZeroUint[T typex.UnsignedInteger](i T, width int) string {
	s := FormatUint(i, 10)
	if len(s) >= width {
		return s
	}
	return strings.Repeat("0", width-len(s)) + s
}

// ParseInt 按进制和位宽解析字符串，返回所需整数类型。
func ParseInt[T typex.SignedInteger](s string, base, bitSize int) (T, error) {
	i, err := strconv.ParseInt(s, base, bitSize)
	return T(i), err
}

// FormatUint 按指定进制格式化无符号整数。
func FormatUint[T typex.UnsignedInteger](i T, base int) string {
	return strconv.FormatUint(uint64(i), base)
}
