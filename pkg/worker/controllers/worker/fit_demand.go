package worker

import (
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"gpustack.ai/gpustack/pkg/nodefeature"
)

// PodSetFitDemand 返回 PodSet 中单个 Pod 对单个节点加速卡的需求，使用与 node-devices
// AdmissionCheck 相同的解析器读取，从而确保据此构建的节点亲和性固定约束与该检查对各类
// 请求形态的需求判定一致。
//
//   - slicedUnits 是逻辑切片所需的单卡单位数；当 PodSet 未请求切片或其模板未携带
//     折叠单位数时为零。
//   - sharedCards 是 Pod 中单个容器所需的最大不同卡数（每张卡占一个归属份额）；当 PodSet
//     未请求共享份额时为零。取最大值而非求和，因为该检查允许同一 Pod 内的两个容器
//     持有同一张卡上的份额。
func PodSetFitDemand(ps *kueue.PodSet) (slicedUnits, sharedCards int32) {
	for _, d := range podSetFamilyDemands(ps, 1) {
		switch d.family {
		case nodefeature.ResourceFamilySliced:
			slicedUnits = d.unitsPerCard
		case nodefeature.ResourceFamilyShared:
			for _, need := range d.sharedNeeds {
				sharedCards = max(sharedCards, need.cards)
			}
		}
	}
	return slicedUnits, sharedCards
}
