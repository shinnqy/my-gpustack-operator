// Package quantityx provides helpers around k8s.io/apimachinery/pkg/api/resource.Quantity
// that the upstream API does not expose.
package quantityx

import (
	"math"
	"math/big"

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

// Multiply multiplies quantity by multiplier and returns the result as a new
// quantity; the input is not modified. If multiplier is 1, quantity is returned
// unchanged.
func Multiply(quantity resource.Quantity, multiplier int64) resource.Quantity {
	if multiplier == 1 {
		return quantity
	}
	q := quantity.DeepCopy()
	q.Mul(multiplier)
	return q
}

// StringMultiply multiplies q by multiplier and returns the result.
// The input q is a string, which will be parsed into a resource.Quantity.
func StringMultiply(quantityStr string, multiplier int64) (resource.Quantity, error) {
	q, err := resource.ParseQuantity(quantityStr)
	if err != nil {
		return resource.Quantity{}, err
	}
	return Multiply(q, multiplier), nil
}

// PercentMultiply scales quantity by percent/100, flooring the result to an integer
// in the quantity's base unit, with a floor of 1 so a positive quantity never scales
// down to zero. percent is clamped to [0,100]; percent >= 100 returns quantity unchanged
// and a zero quantity stays zero. The input is not modified. Like Divide, it floors the
// base-unit value, so it is intended for integer/count quantities (e.g. CPU cores, Gi of
// RAM).
//
// A percent of 0 does not yield 0: a positive quantity still floors to 1, so a caller that
// treats 0 as "no scaling" (the full quantity) must map it to 100 (or any value >= 100).
func PercentMultiply(quantity resource.Quantity, percent int64) resource.Quantity {
	if percent >= 100 {
		return quantity
	}
	if percent < 0 {
		percent = 0
	}
	v := quantity.Value()
	// v*percent can overflow int64 for very large quantities, so scale through big.Int
	// (truncating toward zero, like integer division); the floored result is always <= |v|,
	// so it always fits back into int64.
	scaled := new(big.Int).Mul(big.NewInt(v), big.NewInt(percent))
	scaled.Quo(scaled, big.NewInt(100))
	s := scaled.Int64()
	if s < 1 && v > 0 {
		s = 1
	}
	return *resource.NewQuantity(s, quantity.Format)
}

// StringPercentMultiply scales q by percent/100 and returns the result.
// The input q is a string, which will be parsed into a resource.Quantity.
func StringPercentMultiply(quantityStr string, percent int64) (resource.Quantity, error) {
	q, err := resource.ParseQuantity(quantityStr)
	if err != nil {
		return resource.Quantity{}, err
	}
	return PercentMultiply(q, percent), nil
}

// OverflowsInt64 reports whether Quantity.Value() would misreport this quantity.
//
// It has to be asked, because Value() does not fail — it answers wrongly, and differently by how the
// number was written. Measured, at both ends:
//
//	9223372036854775808   Value() = -9223372036854775808
//	1e30                  Value() = 0
//	-1e30                 Value() = 0
//	-9223372036854775809  Value() = -1
//
// The first two are then read as "not positive", so a caller converting the quantity into a byte
// count sees an impossible amount of memory as no memory at all, with nothing raised anywhere.
//
// BOTH bounds are checked, because the name and this sentence promise that. Every caller today
// rejects a negative sign separately, so the lower bound catches nothing yet — but a function that
// answered only for one end would be wrong for the contract it states rather than merely narrow, and
// the sign check is the caller's rule, not this one's.
//
// A binary-suffixed form such as `8Ei` is already gone by the time this runs: ParseQuantity saturates
// it at the maximum, so the field holds that maximum and Value() reports it faithfully. There is
// nothing left here to detect and nothing to refuse.
func OverflowsInt64(quantity resource.Quantity) bool {
	largest := resource.NewQuantity(math.MaxInt64, resource.DecimalSI)
	smallest := resource.NewQuantity(math.MinInt64, resource.DecimalSI)
	return quantity.Cmp(*largest) > 0 || quantity.Cmp(*smallest) < 0
}
