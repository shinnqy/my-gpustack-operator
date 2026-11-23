package mathx

import "gpustack.ai/gpustack/pkg/utils/typex"

// CeilDiv 计算 a 除以 b 并向上取整，当 b 为 0 时返回 0。
// 任意非零的 a 都会得到非零结果。
func CeilDiv[I typex.Integer](a, b I) I {
	if b == 0 {
		return 0
	}
	return (a + b - 1) / b
}

// RoundDiv 将 a 除以 b 后就近取整，正数的半数向上取整。除数为零时返回零。
func RoundDiv[I typex.Integer](a, b I) I {
	if b == 0 {
		return 0
	}
	return (a + b/2) / b
}
