package slicex

import (
	"gpustack.ai/gpustack/pkg/utils/typex"
)

// Transform 使用函数 f 对提供的切片 s 进行转换。对于 s 中的每个元素，f 以该元素为参数被调用，其返回值被纳
// 入最终的切片中。
func Transform[From, To any, S ~[]From](s S, f func(From) To) []To {
	if len(s) == 0 {
		return nil
	}
	ret := make([]To, len(s))
	for i := range s {
		ret[i] = f(s[i])
	}
	if len(ret) == 0 {
		return nil
	}
	return ret
}

// Filter 使用函数 f 对提供的 slice s 进行过滤。对于 s 中的每个元素，f 以该元素为参数被调用。如果 f 返回
// true，则该元素会被包含在结果 slice 中。
func Filter[From any, S ~[]From](s S, f func(From) bool) []From {
	if len(s) == 0 {
		return s
	}
	ret := make([]From, 0, len(s))
	for i := range s {
		if f(s[i]) {
			ret = append(ret, s[i])
		}
	}
	return ret
}

// 将函数 f 作用于切片 s 中的每个元素所得到的值进行求和。
func Sum[T any, S ~[]T, I typex.Integer](s S, f func(T) I) I {
	var sum I
	for i := range s {
		sum += f(s[i])
	}
	return sum
}

// 检查切片 s 中的任何元素是否满足函数 f 所定义的条件。
func Any[T any, S ~[]T](s S, f func(int) bool) bool {
	for i := range s {
		if f(i) {
			return true
		}
	}
	return false
}

// CompareTrueFirst 对布尔键进行排序，使得 true 排在 false 之前，返回 slices.SortFunc 所期望的三元结果：当
// a 为 true 且 b 不为 true 时结果为负数，当 b 为 true 且 a 不为 true 时结果为正数，当两者一致时结果为零。
func CompareTrueFirst(a, b bool) int {
	switch {
	case a == b:
		return 0
	case a:
		return -1
	default:
		return 1
	}
}
