package varx

import (
	"sync"
)

// Once 保存变量的值，并确保该值仅被配置一次。
type Once[T any] struct {
	o sync.Once

	v T
}

// NewOnce 返回带有指定值的 Once。
func NewOnce[T any](value T) *Once[T] {
	return &Once[T]{
		v: value,
	}
}

// Configure 设置变量值，多次调用将被忽略。
func (i *Once[T]) Configure(value T) {
	i.o.Do(func() {
		i.v = value
	})
}

// Get 返回已配置的值；\n// 如果变量尚未配置，则返回通过 NewOnce 初始化的值。
func (i *Once[T]) Get() T {
	return i.v
}
