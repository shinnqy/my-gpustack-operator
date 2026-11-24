package stringx

import (
	"encoding/base64"
	"strings"
)

// DecodeBase64 解码给定字符串，支持带填充或不带填充的格式。
func DecodeBase64(str string) (string, error) {
	// 统一为标准编码格式。
	str = strings.ReplaceAll(str, "-", "+")
	str = strings.ReplaceAll(str, "_", "/")

	// 统一为无填充格式。
	str = strings.TrimRight(str, "=")

	bs, err := DecodeBase64Bytes(ToBytes(&str))
	if err != nil {
		return "", err
	}

	return FromBytes(&bs), nil
}

// DecodeBase64Bytes 解码给定字节切片。
func DecodeBase64Bytes(src []byte) ([]byte, error) {
	enc := base64.RawStdEncoding
	dst := make([]byte, enc.DecodedLen(len(src)))
	n, err := enc.Decode(dst, src)

	return dst[:n], err
}

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
