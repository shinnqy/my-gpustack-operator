package bytex

import (
	"bytes"
	"sync"
)

const defaultSize = 32 * 1024

type (
	Bytes       = []byte
	BytesBuffer = *bytes.Buffer
)

var gp = sync.Pool{
	New: func() any {
		buf := make(Bytes, defaultSize)
		return &buf
	},
}

// GetBytes 从对象池中获取一个字节缓冲区，
// 可指定大小，
// 默认为 32k。
func GetBytes(size ...int) Bytes {
	buf := *gp.Get().(*Bytes)

	s := defaultSize
	if len(size) != 0 {
		s = size[0]
		if s <= 0 {
			s = defaultSize
		}
	}
	if cap(buf) >= s {
		return buf[:s]
	}

	gp.Put(&buf)
	return make(Bytes, s)
}

// GetBuffer 与 GetBytes 类似，
// 但返回由 bytes.Buffer 封装的字节缓冲区。
func GetBuffer(size ...int) BytesBuffer {
	return bytes.NewBuffer(GetBytes(size...)[:0])
}

// Put 将缓冲区（Bytes 或 BytesBuffer）归还到对象池。
func Put[T Bytes | BytesBuffer](buf T) {
	switch v := any(buf).(type) {
	case Bytes:
		gp.Put(&v)
	case BytesBuffer:
		bs := v.Bytes()
		gp.Put(&bs)
		v.Reset()
	}
}
