package funcx

// NoError 忽略给定的 error，
// 通常是用于链式函数调用的便捷辅助函数。
func NoError[T any](t T, _ error) T {
	return t
}

// MustNoError 与 NoError 类似，
// 但如果给定的 error 不为 nil 则会 panic，
// 通常是用于链式函数调用的便捷辅助函数。
func MustNoError[T any](t T, e error) T {
	if e != nil {
		panic(e)
	}
	return t
}
