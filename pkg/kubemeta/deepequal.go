package kubemeta

import "k8s.io/apimachinery/pkg/api/equality"

// DeepEqual 是对 equality.Semantic.DeepEqual 的封装，用于对任意两个对象进行深度相等性比较，
// 并考虑特定类型上的 Kubernetes 语义。
func DeepEqual(a, b any) bool {
	return equality.Semantic.DeepEqual(a, b)
}
