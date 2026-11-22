package kuberequest

import (
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"gpustack.ai/gpustack/pkg/utils/quantityx"
)

// 超配基数表示一个整单位的 limit 在调度时折算为多少 request。limit 保存用户要求的上限，
// 较小的 request 允许多个 Pod 共享节点。
//
//	1 核 CPU → 800m 的 request，加速器节点上为 100m。
//	1 Gi 内存 → 128Mi 的 request。
//	1 Gi 存储 → 128Mi 的 request。
//
// 加速器节点通常受卡数限制，因此对同一 CPU 上限预留更少的 CPU，避免 CPU 请求阻碍放置。
var (
	_CPUOvercommitBaseQuantity                 = resource.MustParse("800m")
	_RAMOvercommitBaseQuantity                 = resource.MustParse("128Mi")
	_LocalStorageOvercommitBaseQuantity        = resource.MustParse("128Mi")
	_CPUOvercommitBaseQuantityForAcceleratable = resource.MustParse("100m")
)

// ScaleToOvercommit 从单项资源的 limit 计算超配时的 request。调用方保留原来的 limit，
// 并且必须传入同一资源名对应的数量，使规格控制器能用 ScaleBackOvercommit 按相同单位还原。
//
// 普通节点按每整核 800m 预留 CPU，加速器节点按 100m。Value() 对核心数向上取整，
// 再用原 limit 封顶，避免 API 服务器拒绝 request 大于 limit 的 Pod。
// 内存和临时存储按每整 Gi 预留 128Mi，1.5Gi 按 1Gi 计算，小于 1Gi 会得到零预留。其他资源直接返回原值。
func ScaleToOvercommit(
	resName core.ResourceName,
	val resource.Quantity,
	acceleratable bool,
) resource.Quantity {
	switch resName {
	case core.ResourceCPU:
		base := _CPUOvercommitBaseQuantity
		if acceleratable {
			base = _CPUOvercommitBaseQuantityForAcceleratable
		}
		req := quantityx.Multiply(base, val.Value())
		if req.Cmp(val) > 0 {
			return val
		}
		return req
	case core.ResourceMemory:
		return quantityx.Multiply(_RAMOvercommitBaseQuantity, val.Value()/quantityx.Gi)
	case core.ResourceEphemeralStorage:
		return quantityx.Multiply(_LocalStorageOvercommitBaseQuantity, val.Value()/quantityx.Gi)
	}
	return val
}

// ScaleBackOvercommit 把 ScaleToOvercommit 生成的 request 折算回 limit 单位。
// InstanceTypeReconciler 在计算 CPU 资源视图时先还原 Kueue 报告的已预留总量，再从容量中扣除，
// 使容量、剩余量和单次最大请求量的单位一致。
//
// 反算使用整数除法。每个 Pod 按整单位预留时，其贡献量恰是基数的整数倍，汇总后仍可准确还原。
//
//	普通节点 CPU 核数 = val.MilliValue() / 800。
//	加速器节点 CPU 核数 = val.MilliValue() / 100。
//	内存或存储的 Gi 数 = val.Value() / 128Mi。
//	其他资源直接返回原值。
//
// 调用方须按同一资源名将 limit 换算为 request。正向换算已丢弃的精度无法还原，
// 例如不足一 Gi 的内存或存储会被截为零，零点几核 CPU 可能经 Value() 向上取整。
// 如果 CPU request 被限制在小于基数的 limit，整数反算也会得到零核。
func ScaleBackOvercommit(
	resName core.ResourceName,
	val resource.Quantity,
	acceleratable bool,
) resource.Quantity {
	switch resName {
	case core.ResourceCPU:
		baseMilli := _CPUOvercommitBaseQuantity.MilliValue()
		if acceleratable {
			baseMilli = _CPUOvercommitBaseQuantityForAcceleratable.MilliValue()
		}
		return *resource.NewMilliQuantity((val.MilliValue()/baseMilli)*1000, resource.DecimalSI)
	case core.ResourceMemory:
		base := _RAMOvercommitBaseQuantity.Value()
		return *resource.NewQuantity((val.Value()/base)*quantityx.Gi, resource.BinarySI)
	case core.ResourceEphemeralStorage:
		base := _LocalStorageOvercommitBaseQuantity.Value()
		return *resource.NewQuantity((val.Value()/base)*quantityx.Gi, resource.BinarySI)
	}
	return val
}
