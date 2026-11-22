package datax

import (
	"sync/atomic"
)

// Snapshot 是一个无需竞赛的持有者，用于保存类型为 T 的最新值。
//
// 零值已准备使用。
type Snapshot[T any] struct {
	v atomic.Pointer[T]
}

// Store 会将快照替换为给定值。
func (s *Snapshot[T]) Store(v *T) {
	s.v.Store(v)
}

// Load 会返回最新存储的值，如果尚未存储任何内容则返回 nil。
func (s *Snapshot[T]) Load() *T {
	return s.v.Load()
}
