package worker

import (
	"fmt"
	"math/big"

	"k8s.io/apimachinery/pkg/api/resource"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

// modelDeploymentScalerDefaultLatencyQuantile 是当规格未指定时，扩缩器所遵循的
// 延迟分布分位数。
//
// 默认为第 95 百分位数（0.95），未指定分位数的扩缩器均以此为基准。
const modelDeploymentScalerDefaultLatencyQuantile = 0.95

// ModelDeploymentScalerLatencyQuantile 返回扩缩器所遵循的延迟分布分位数。
//
// 当规格中指定了分位数时读取该值，未指定时则使用第 95 百分位数。
//
// 该函数读取 Quantity 解析后的精确值，因为 Quantity 以固定精度保存数值，且每次比较
// 与状态记录都使用该解析值。超出该精度的分位数小数位会按舍入后的值保留。
//
// 对于所有携带延迟目标的算法该值均保持一致，因为预测算法将其延迟目标保存在嵌套的
// token 吞吐量规格中，而手动模式和无容量先验路径也在相同位置保存该目标。
//
// 该函数会拒绝不在单位区间内的分位数，因为无法代表有效百分位数的分位数无法作为约束目标。
func ModelDeploymentScalerLatencyQuantile(
	algorithm *workercore.ModelDeploymentScalerAlgorithmSpec,
) (float64, error) {
	if algorithm == nil {
		return modelDeploymentScalerDefaultLatencyQuantile, nil
	}

	var quantity *resource.Quantity
	switch algorithm.Type {
	case workercore.ModelDeploymentScalerAlgorithmTypeTokenThroughput:
		if algorithm.TokenThroughput != nil {
			quantity = algorithm.TokenThroughput.LatencyQuantile
		}
	case workercore.ModelDeploymentScalerAlgorithmTypePredictive:
		if algorithm.Predictive != nil {
			quantity = algorithm.Predictive.TokenThroughput.LatencyQuantile
		}
	default:
		return modelDeploymentScalerDefaultLatencyQuantile, nil
	}
	if quantity == nil {
		return modelDeploymentScalerDefaultLatencyQuantile, nil
	}

	value, ok := modelDeploymentScalerQuantileValue(quantity)
	if !ok {
		return 0, fmt.Errorf("the latency quantile %s is not a number", quantity.String())
	}
	if value <= 0 || value > 1 {
		return 0, fmt.Errorf(
			"the latency quantile must be greater than zero and no greater than one, got %s",
			quantity.String(),
		)
	}
	return value, nil
}

// modelDeploymentScalerQuantileValue 读取 Quantity 中保存的数值。
//
// 它读取 Quantity 中保存的精确十进制值并一次性舍入为最接近的浮点数，因为 Quantity
// 保存的是十进制数，而浮点数无法精确表示该值。
//
// 它不使用 Quantity 的近似读取方法，因为该方法会将数值乘以经过舍入的 10 的幂，
// 从而偏离 Quantity 实际保存的数值一个单位。以这种方式读取的 0.95 分位数与
// 第 95 百分位数的实际测量值并不相等，二者比较时会错误地拒绝本可回答问题的观测证据。
//
// 它采用与十进制读取器相同的舍入方式，从而确保扩缩器指定的分位数与该数值的
// 其他读取方所看到的分位数完全一致。
func modelDeploymentScalerQuantileValue(quantity *resource.Quantity) (float64, bool) {
	if quantity == nil {
		return 0, false
	}

	dec := quantity.AsDec()
	num := new(big.Int).Set(dec.UnscaledBig())
	den := big.NewInt(1)
	scale := int64(dec.Scale())
	if scale >= 0 {
		den.Exp(big.NewInt(10), big.NewInt(scale), nil)
	} else {
		num.Mul(num, new(big.Int).Exp(big.NewInt(10), big.NewInt(-scale), nil))
	}

	value, _ := new(big.Rat).SetFrac(num, den).Float64()
	return value, true
}
