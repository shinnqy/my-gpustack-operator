package funcx

// NoError ignores the given error,
// it is usually a nice helper for chain function calling.
func NoError[T any](t T, _ error) T {
	return t
}

// MustNoError is similar to NoError,
// but it panics if the given error is not nil,
// it is usually a nice helper for chain function calling.
func MustNoError[T any](t T, e error) T {
	if e != nil {
		panic(e)
	}
	return t
}
