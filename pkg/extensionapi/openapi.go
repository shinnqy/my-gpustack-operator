package extensionapi

import "k8s.io/kube-openapi/pkg/common"

// OpenAPIDefinitionGetter 是返回 OpenAPI 定义映射的函数类型。
type OpenAPIDefinitionGetter = func(ref common.ReferenceCallback) map[string]common.OpenAPIDefinition

// MergeOpenAPIDefinitionsGetter 将多个 OpenAPIDefinitionsGetter 合并为一个。
func MergeOpenAPIDefinitionsGetter(getters ...OpenAPIDefinitionGetter) OpenAPIDefinitionGetter {
	return func(ref common.ReferenceCallback) map[string]common.OpenAPIDefinition {
		ret := getters[0](ref)
		for i := 1; i < len(getters); i++ {
			for k, v := range getters[i](ref) {
				if _, ok := ret[k]; ok {
					continue
				}
				ret[k] = v
			}
		}
		return ret
	}
}
