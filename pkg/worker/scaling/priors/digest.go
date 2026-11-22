package priors

import (
	"bytes"
	"encoding/json"
	"fmt"

	"gpustack.ai/gpustack/pkg/utils/stringx"
)

// canonicalJSON 对值进行编码，确保相同内容始终编码为相同的字节序列。
//
// 它会对对象键进行排序，因为摘要不能依赖于字段声明时的偶然顺序。
//
// 它使用标准库进行编码，因为共享的 JSON 包在 goccy 构建标签下会切换到另一个编码器，而该编码器会将较小指数输出为 1e-07，标准库则输出为 1e-7。摘要是需要存储的值，因此其字节内容不能依赖于二进制文件的构建方式。
func canonicalJSON(value any) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var generic any
	if err := decoder.Decode(&generic); err != nil {
		return nil, err
	}
	return json.Marshal(generic)
}

// Digest 返回值的内容摘要。
//
// 该摘要是稳定的，因此相同内容始终产生相同的摘要，目录发生变更时也清晰可见。
func Digest(value any) (string, error) {
	encoded, err := canonicalJSON(value)
	if err != nil {
		return "", fmt.Errorf("encode digest content: %w", err)
	}
	return "sha256:" + stringx.SumBytesBySHA256(encoded), nil
}

// ProfileDigest 返回 profile 内容的摘要。
func ProfileDigest(profile Profile) (string, error) {
	profile.Digest = ""
	return Digest(profile)
}

// CatalogDigest 返回 catalog 内容的摘要。
func CatalogDigest(catalog Catalog) (string, error) {
	catalog.Digest = ""
	return Digest(catalog)
}
