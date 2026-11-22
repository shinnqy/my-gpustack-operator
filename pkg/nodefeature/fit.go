package nodefeature

import (
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"

	"gpustack.ai/gpustack/pkg/systemname"
)

// fit 标签按节点与加速卡型号发布该型号单卡仍可提供的最大余量。设置这些标签是为了让仅读取节点
// 总容量的调度器（例如 Kueue 的拓扑感知调度）能够跳过那些虽然总余量足够但分散在多张卡上、
// 单卡均无法满足请求的节点。它们仅用于过滤：不会对其执行任何资源扣减。
//
// 型号位于键的名称部分，因此对于长度不超过 63 个字符的任意加速设备键均合法；若采用
// "<aKey>.<suffix>" 的命名方式则可能超出限制。其域名独立于其他控制器监听的 "feature." 和
// "acceleratable." 前缀，从而确保每次分配引起的标签值变化不会唤醒这些控制器。
const (
	// FitLabelDomainSuffix 是所有 fit 标签域名的后缀。
	FitLabelDomainSuffix = "fit." + systemname.LabelPrefix

	// _FitSlicedMaxFreeUnitsLabelPrefix 是如下标签键的前缀：记录可提供逻辑切分且仍有空闲槽位的
	// 任意单卡上的最大空闲单元数。
	_FitSlicedMaxFreeUnitsLabelPrefix = "sliced-max-free-units." + FitLabelDomainSuffix
	// _FitSharedFreeCardsLabelPrefix 是如下标签键的前缀：记录可服务整卡共享系列且仍有空闲
	// 归属份额的加速卡数量。
	_FitSharedFreeCardsLabelPrefix = "shared-free-cards." + FitLabelDomainSuffix
)

// FitSlicedMaxFreeUnitsLabelKey 返回对应标签键，用于记录加速卡型号 aKey（"<manufacturer>-<id>"）中
// 可提供逻辑切分且仍有空闲槽位的任意单卡上的最大空闲单元数。
func FitSlicedMaxFreeUnitsLabelKey(aKey string) string {
	return _FitSlicedMaxFreeUnitsLabelPrefix + aKey
}

// FitSharedFreeCardsLabelKey 返回对应标签键，用于记录加速卡型号 aKey（"<manufacturer>-<id>"）中
// 可服务整卡共享系列且仍有空闲归属份额的加速卡数量。
func FitSharedFreeCardsLabelKey(aKey string) string {
	return _FitSharedFreeCardsLabelPrefix + aKey
}

// IsFitLabelKey 判断 key 是否为两种 fit 标签键之一、包含非空型号且符合合法标签键语法。
// 从管理员配置的 InstanceType 或 Devices 台账中取得的型号在其他位置未经过标签键语法校验，
// 若由格式非法的型号构造键名，会导致携带该键的 Node 补丁或节点亲和性配置无效，因此调用方会跳过此类键。
func IsFitLabelKey(key string) bool {
	for _, prefix := range []string{_FitSlicedMaxFreeUnitsLabelPrefix, _FitSharedFreeCardsLabelPrefix} {
		if aKey, ok := strings.CutPrefix(key, prefix); ok && aKey != "" {
			return len(validation.IsQualifiedName(key)) == 0
		}
	}
	return false
}

// EqualIgnoringFitLabels 判断在忽略双方所有 fit 标签后两个标签映射是否相等，从而让监听方
// 能够忽略仅变动了 fit 标签的 Node 更新事件。
func EqualIgnoringFitLabels(a, b map[string]string) bool {
	for k, av := range a {
		if IsFitLabelKey(k) {
			continue
		}
		if bv, ok := b[k]; !ok || av != bv {
			return false
		}
	}
	for k := range b {
		if IsFitLabelKey(k) {
			continue
		}
		if _, ok := a[k]; !ok {
			return false
		}
	}
	return true
}

// FilterFitLabels 返回标签映射中的 fit 标签。
func FilterFitLabels(labels map[string]string) map[string]string {
	out := make(map[string]string)
	for k, v := range labels {
		if IsFitLabelKey(k) {
			out[k] = v
		}
	}
	return out
}
