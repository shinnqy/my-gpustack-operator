package datax

import (
	"sync/atomic"
)

// _RingNode is a node in the ring, containing a sequence number and a value.
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
