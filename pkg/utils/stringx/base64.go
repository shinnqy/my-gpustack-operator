package stringx

import (
	"encoding/base64"
)

// EncodeBase64 对给定字符串进行编码，
// 然后输出标准格式。
func EncodeBase64(src string) string {
	bs := EncodeBase64Bytes(ToBytes(&src))
	return FromBytes(&bs)
}

// EncodeBase64Bytes 与 DecodeBase64 类似，
// 但返回字节切片。
func EncodeBase64Bytes(src []byte) []byte {
	enc := base64.RawURLEncoding
	dst := make([]byte, enc.EncodedLen(len(src)))
	enc.Encode(dst, src)

	return dst
}
