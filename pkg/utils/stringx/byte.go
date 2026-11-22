package stringx

import (
	"unsafe"
)

// FromBytes 将字节切片转换为字符串。
func FromBytes(b *[]byte) string {
	return unsafe.String(unsafe.SliceData(*b), len(*b))
}

// ToBytes 将字符串转换为字节切片，
// 转换后的切片元素不可修改。
func ToBytes(s *string) (bs []byte) {
	return unsafe.Slice(unsafe.StringData(*s), len(*s))
}
