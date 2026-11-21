package varx

import (
	"sync"
)

// Once holds the value of the variable and ensures that the value is configured only once.
type Once[T any] struct {
	o sync.Once

	v T
}

// NewOnce returns the Once with the given value.
func NewOnce[T any](value T) *Once[T] {
	return &Once[T]{
		v: value,
	}
}

// Configure sets the value, multiple calls will be ignored.
func (i *Once[T]) Configure(value T) {
	i.o.Do(func() {
		i.v = value
	})
}

// Get returns the configured value,
// or returns initialized value with NewOnce if the variable is not configured yet.
func (i *Once[T]) Get() T {
	return i.v
}
