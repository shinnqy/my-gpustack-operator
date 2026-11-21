package stringx

import (
	"encoding/base64"
)

// EncodeBase64 encodes the given string,
// and then output standard format.
func EncodeBase64(src string) string {
	bs := EncodeBase64Bytes(ToBytes(&src))
	return FromBytes(&bs)
}

// EncodeBase64Bytes is similar to DecodeBase64,
// but returns bytes.
func EncodeBase64Bytes(src []byte) []byte {
	enc := base64.RawURLEncoding
	dst := make([]byte, enc.EncodedLen(len(src)))
	enc.Encode(dst, src)

	return dst
}
