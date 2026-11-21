// quantityx 为 Kubernetes resource.Quantity 提供上游类型未直接暴露的计算和格式化操作。
package quantityx

import (
	"math"
	"math/big"
	"strconv"

	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	Ki = 1 << 10
	Mi = 1 << 20
	Gi = 1 << 30
	Ti = 1 << 40
	Pi = 1 << 50
	Ei = 1 << 60
)

// Multiply 返回与倍数相乘后的新数量，保留输入值。倍数为 1 时直接返回原值。
func Multiply(quantity resource.Quantity, multiplier int64) resource.Quantity {
	if multiplier == 1 {
		return quantity
	}
	q := quantity.DeepCopy()
	q.Mul(multiplier)
	return q
}

// StringMultiply 先将字符串解析为 resource.Quantity，再按给定倍数计算。
func StringMultiply(quantityStr string, multiplier int64) (resource.Quantity, error) {
	q, err := resource.ParseQuantity(quantityStr)
	if err != nil {
		return resource.Quantity{}, err
	}
	return Multiply(q, multiplier), nil
}

// PercentMultiply 按 percent/100 缩放数量，向下取整到基本单位，确保正数的结果至少为 1。
func PercentMultiply(quantity resource.Quantity, percent int64) resource.Quantity {
	if percent >= 100 {
		return quantity
	}
	if percent < 0 {
		percent = 0
	}
	v := quantity.Value()
	// v 与 percent 相乘可能超出 int64，先用 big.Int 计算再向零截断。最终结果的绝对值不大于原值，可放回 int64。
	scaled := new(big.Int).Mul(big.NewInt(v), big.NewInt(percent))
	scaled.Quo(scaled, big.NewInt(100))
	s := scaled.Int64()
	if s < 1 && v > 0 {
		s = 1
	}
	return *resource.NewQuantity(s, quantity.Format)
}

// StringPercentMultiply 将字符串解析为 resource.Quantity，再按百分比缩放。
func StringPercentMultiply(quantityStr string, percent int64) (resource.Quantity, error) {
	q, err := resource.ParseQuantity(quantityStr)
	if err != nil {
		return resource.Quantity{}, err
	}
	return PercentMultiply(q, percent), nil
}

// OverflowsInt64 检查 Quantity.Value() 是否会溢出或给出错误结果。不同格式的越界数值可能回绕或按指数截断。
// 已经被解析器钳制到边界的数值只能检查解析后的值。
func OverflowsInt64(quantity resource.Quantity) bool {
	largest := resource.NewQuantity(math.MaxInt64, resource.DecimalSI)
	smallest := resource.NewQuantity(math.MinInt64, resource.DecimalSI)
	return quantity.Cmp(*largest) > 0 || quantity.Cmp(*smallest) < 0
}

type _Unit struct {
	scale  int64
	suffix string
}

var binaryUnits = [...]_Unit{
	{Ei, "Ei"},
	{Pi, "Pi"},
	{Ti, "Ti"},
	{Gi, "Gi"},
	{Mi, "Mi"},
	{Ki, "Ki"},
}

var decimalUnits = [...]_Unit{
	{1e18, "E"},
	{1e15, "P"},
	{1e12, "T"},
	{1e9, "G"},
	{1e6, "M"},
	{1e3, "k"},
}

// Format 按数量自身的格式选择二进制或十进制 SI 单位。
func Format(q resource.Quantity) string {
	if q.Format == resource.BinarySI {
		return FormatBinarySI(q)
	}
	return FormatDecimalSI(q)
}

// FormatBinarySI 选择合适的 Ki、Mi、Gi 等单位，对无法整除的值取近似便于展示。
// 需要无损往返时使用 quantity 自带的 String 方法。
func FormatBinarySI(q resource.Quantity) string {
	return formatScaled(q, binaryUnits[:])
}

// FormatDecimalSI 选用合适的 k、M、G 等十进制单位，并将显示值四舍五入到整数。
func FormatDecimalSI(q resource.Quantity) string {
	return formatScaled(q, decimalUnits[:])
}

func formatScaled(
	q resource.Quantity,
	units []_Unit,
) string {
	if q.IsZero() {
		return "0"
	}

	v := q.Value()
	sign := ""
	if v < 0 {
		sign = "-"
		v = -v
	}

	for _, u := range units {
		if v >= u.scale {
			n := (v + u.scale/2) / u.scale
			return sign + strconv.FormatInt(n, 10) + u.suffix
		}
	}
	return sign + strconv.FormatInt(v, 10)
}
