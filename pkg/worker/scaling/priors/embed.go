package priors

import (
	"fmt"
	"sync"

	_ "embed"
)

// DefaultCatalogConfigMapKey 是省略 key 时 catalog 引用读取的 ConfigMap data key。
//
// 它对应 YAML 文件，因为 catalog 采用 YAML 编写，且指定单一格式的读取器只读取对应的单一文件。
const DefaultCatalogConfigMapKey = "priors.yaml"

// LegacyCatalogConfigMapKey 是 catalog 更改文件名之前所使用的 ConfigMap data key。
//
// 它作为回退键读取，因为在变更前写入的 ConfigMap 中仍保存着 operator 随附的 catalog。
const LegacyCatalogConfigMapKey = "priors.json"

// embeddedCatalog 是随 operator 一同发布的 catalog。
//
// 它可能不包含任何 profile，因为只有当数据源实测出解析器所需的依据时，profile 才会进入 catalog。空 catalog 会拒绝所有解析请求，在获得实测 profile 之前这是预期行为。
//
//go:embed priors.yaml
var embeddedCatalog []byte

var loadEmbeddedOnce = sync.OnceValues(func() (*Catalog, error) {
	catalog, err := ParseCatalog(embeddedCatalog)
	if err != nil {
		return nil, fmt.Errorf("parse embedded catalog: %w", err)
	}
	return catalog, nil
})

// LoadEmbedded 返回随 operator 一同发布的 catalog。
//
// 它仅在首次调用时解析和校验一次，从而使生成有误的 catalog 在此处即报错（而非延迟到首次解析时），同时避免每次调用都重复解析。
func LoadEmbedded() (*Catalog, error) {
	return loadEmbeddedOnce()
}
