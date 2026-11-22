package cache

import (
	"golang.org/x/crypto/acme/autocert"

	"gpustack.ai/gpustack/pkg/utils/certs"
)

// NewDirCache 会返回一个包含指定目录的新 DirCache 实例。
func NewDirCache(dir string) certs.Cache {
	return autocert.DirCache(dir)
}
