package slicex

// Transform maps the provided slice s using the function f.
// For each element in s, f is called with the element as an argument,
// and the returned value is included in the resulting slice.
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

// Any checks if any element in the slice s satisfies the condition defined by the function f.
func Any[T any, S ~[]T](s S, f func(int) bool) bool {
	for i := range s {
		if f(i) {
			return true
		}
	}
	return false
}

// CompareTrueFirst orders a boolean key so that true sorts before false, returning the
// three-way result slices.SortFunc expects: negative when a is true and b is not, positive
// when b is true and a is not, zero when they agree.
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
