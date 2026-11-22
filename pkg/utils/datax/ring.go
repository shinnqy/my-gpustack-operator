package datax

import (
	"sync/atomic"
)

// _RingNode 是环形缓冲区中的节点，包含一个序列号和一个值。
type _RingNode[T any] struct {
	seq atomic.Uint64
	val T
}

type RingBuffer[T any] struct {
	mask uint64
	data []_RingNode[T]
	_    [32]byte
	head atomic.Uint64
	_    [56]byte
	tail atomic.Uint64
	_    [56]byte
}
