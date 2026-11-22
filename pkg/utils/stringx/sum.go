package stringx

import (
	"crypto/sha256"
	"encoding/hex"
	"hash/fnv"
)

// SumByFNV64a 使用 FNV-64a 哈希算法对字符串计算摘要。
func SumByFNV64a(s string, ss ...string) string {
	h := fnv.New64a()

	_, _ = h.Write(ToBytes(&s))
	for i := range ss {
		_, _ = h.Write(ToBytes(&ss[i]))
	}

	sum := h.Sum(nil)
	return hex.EncodeToString(sum)
}

// SumBySHA256 使用 SHA256 哈希算法对字符串计算摘要。
func SumBySHA256(s string, ss ...string) string {
	h := sha256.New()

	_, _ = h.Write(ToBytes(&s))
	for i := range ss {
		_, _ = h.Write(ToBytes(&ss[i]))
	}

	sum := h.Sum(nil)
	return hex.EncodeToString(sum)
}

// SumBytesBySHA256 使用 SHA256 哈希算法对字节切片计算摘要。
func SumBytesBySHA256(bs []byte, bss ...[]byte) string {
	h := sha256.New()

	_, _ = h.Write(bs)
	for i := range bss {
		_, _ = h.Write(bss[i])
	}

	sum := h.Sum(nil)
	return hex.EncodeToString(sum)
}

// SumBytesBySHA224 使用 SHA224 哈希算法对字节切片计算摘要。
func SumBytesBySHA224(bs []byte, bss ...[]byte) string {
	h := sha256.New224()

	_, _ = h.Write(bs)
	for i := range bss {
		_, _ = h.Write(bss[i])
	}

	sum := h.Sum(nil)
	return hex.EncodeToString(sum)
}
