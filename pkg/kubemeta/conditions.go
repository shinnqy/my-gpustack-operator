package kubemeta

import (
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SetCondition 将 conditions 中对应的 condition 设置为 newCondition，并在本次调用
// 改变了 conditions 时返回 true。
// conditions 必须非 nil。
//  1. 如果指定类型的 condition 已存在（将现有 condition 的所有字段更新为 newCondition，
//     若新状态与旧状态不同则将 LastTransitionTime 设为当前时间）
//  2. 如果指定类型的 condition 不存在（若未设置 LastTransitionTime 则设为 now()，并将 newCondition 追加到切片中）
func SetCondition(conditions *[]meta.Condition, newCondition meta.Condition) (changed bool) {
	return apimeta.SetStatusCondition(conditions, newCondition)
}

// IsConditionTrue 在 conditions 中存在对应的 conditionType 且其状态为 True 时返回 true。
func IsConditionTrue(conditions []meta.Condition, conditionType string) bool {
	return apimeta.IsStatusConditionTrue(conditions, conditionType)
}
