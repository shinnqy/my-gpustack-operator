package kubediscovery

import (
	utilversion "k8s.io/apimachinery/pkg/util/version"
)

// Feature 表示其可用性由集群版本决定的 Kubernetes 能力。
type Feature int

const (
	// FeatureNativeSidecar 是渲染原生 sidecar 时使用的 initContainers[].restartPolicy 字段。
	// SidecarContainers 特性门控在 Kubernetes 1.29 中默认开启；低于该版本时，API Server
	// 会直接丢弃该字段而不报错，从而使原生 sidecar 退化为普通的 init container。
	FeatureNativeSidecar Feature = iota + 1

	// FeatureImageVolume 是渲染镜像卷时使用的 volumes[].image 字段。
	// ImageVolume 特性门控在 Kubernetes 1.35 中默认开启（beta）并在 1.36 中进入 GA；
	// 低于该版本时需在 apiserver 上手动开启该门控，而不识别该字段的 apiserver 会直接
	// 丢弃它而不报错，导致 Pod 静默丢失其模型权重。
	FeatureImageVolume
)

// SupportsFeature 返回给定的 Kubernetes 版本是否支持该特性。
//
// 当版本为 nil 或无法解析时返回 false：无法读取的版本绝不应解锁无法确定最低版本
// 要求的能力。
func SupportsFeature(v *Version, f Feature) bool {
	if v == nil {
		return false
	}
	parsed, err := utilversion.ParseGeneric(v.GitVersion)
	if err != nil {
		return false
	}

	switch f {
	case FeatureNativeSidecar:
		return parsed.Major() > 1 || (parsed.Major() == 1 && parsed.Minor() >= 29)
	case FeatureImageVolume:
		return parsed.Major() > 1 || (parsed.Major() == 1 && parsed.Minor() >= 35)
	}
	return false
}
