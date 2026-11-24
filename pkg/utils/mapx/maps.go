package mapx

import "strings"

// Contain 在 a 包含 b 中所有键且对应值相同时返回 true。
func Contain[K, V comparable, M ~map[K]V](a, b M) bool {
	for k, bv := range b {
		if av, found := a[k]; !found || av != bv {
			return false
		}
	}
	return true
}

// FilterSlice 使用函数 f 对提供的 map m 进行过滤并映射为切片。
// 对于 m 中的每个键值对，都会以键和值作为参数调用 f。
// 如果 f 返回 true，则将返回的值包含在结果切片中。
func FilterSlice[K comparable, V, Y any, M ~map[K]V](m M, f func(K, V) (Y, bool)) []Y {
	if len(m) == 0 {
		return nil
	}
	ret := make([]Y, 0, len(m))
	for k, v := range m {
		if y, ok := f(k, v); ok {
			ret = append(ret, y)
		}
	}
	if len(ret) == 0 {
		return nil
	}
	return ret
}

// Filter 使用函数 f 对提供的 map m 进行过滤。
// 对于 m 中的每个键值对，都会以键和值作为参数调用 f。
func Filter[K comparable, V any, M ~map[K]V](m M, f func(K, V) bool) M {
	if len(m) == 0 {
		return nil
	}
	ret := make(map[K]V, len(m))
	for k, v := range m {
		if f(k, v) {
			ret[k] = v
		}
	}
	if len(ret) == 0 {
		return nil
	}
	return ret
}

// Merge 返回一个合并了所提供各 map 条目的新 map。
// 当多个 map 共享同一个键时，以最后一个 map 的值为准。
// 会跳过 nil 或空 map。当结果为空时返回 nil。
func Merge[K comparable, V any, M ~map[K]V](ms ...M) M {
	ret := make(map[K]V)
	for i := range ms {
		for k, v := range ms[i] {
			ret[k] = v
		}
	}
	if len(ret) == 0 {
		return nil
	}
	return ret
}

// EqualWithKey 报告两个 map 在每个指定键上是否包含相同的值。
// 当两个 map 都不包含某个键时返回 true（视为同样缺失）。
func EqualWithKey[K, V comparable, M ~map[K]V](a, b M, k K, ks ...K) bool {
	check := func(k K) bool {
		av, aok := a[k]
		bv, bok := b[k]
		if aok != bok {
			return false
		}
		return !aok || av == bv
	}
	if !check(k) {
		return false
	}
	for i := range ks {
		if !check(ks[i]) {
			return false
		}
	}
	return true
}

// EqualWithStringPrefix 报告两个 map 中
// 匹配指定前缀或任一指定前缀的键是否包含相同的键值对。
// 当两个 map 都不包含任何匹配任一前缀的键时返回 true。
func EqualWithStringPrefix[V comparable, M ~map[string]V](a, b M, prefix string, prefixes ...string) bool {
	match := func(k string) bool {
		if strings.HasPrefix(k, prefix) {
			return true
		}
		for i := range prefixes {
			if strings.HasPrefix(k, prefixes[i]) {
				return true
			}
		}
		return false
	}
	for k, av := range a {
		if !match(k) {
			continue
		}
		if bv, ok := b[k]; !ok || av != bv {
			return false
		}
	}
	for k := range b {
		if !match(k) {
			continue
		}
		if _, ok := a[k]; !ok {
			return false
		}
	}
	return true
}

// Slice 遍历映射，对每个键值对调用 f，将结果收集到切片。映射为空时返回 nil。
func Slice[K comparable, V, Y any, M ~map[K]V](m M, f func(K, V) Y) []Y {
	if len(m) == 0 {
		return nil
	}
	ret := make([]Y, 0, len(m))
	for k, v := range m {
		ret = append(ret, f(k, v))
	}
	if len(ret) == 0 {
		return nil
	}
	return ret
}
