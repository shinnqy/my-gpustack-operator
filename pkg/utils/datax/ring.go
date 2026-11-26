package datax

import (
	"math/bits"
	"runtime"
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

func NewRingBuffer[T any](size uint64) *RingBuffer[T] {
	// 将容量向上取整到 2 的幂，槽位索引就可以用掩码计算。
	if size <= 0 {
		panic("size must be greater than 0")
	}
	if size&(size-1) != 0 {
		size = 1 << bits.Len64(size-1)
	}

	r := &RingBuffer[T]{
		mask: size - 1,
		data: make([]_RingNode[T], size),
	}
	for i := uint64(0); i < size; i++ {
		r.data[i].seq.Store(i)
	}

	return r
}

// Push 将新元素写入环形缓冲区的尾部。
func (r *RingBuffer[T]) Push(v T) {
	for {
		tail := r.tail.Load()
		head := r.head.Load()
		n := &r.data[tail&r.mask]

		seq := n.seq.Load()
		diff := int64(seq - tail)
		// diff 小于零表示槽位仍被占用，需要推进头指针以覆盖最旧的数据。等于零表示可以写入。
		// 大于零表示另一个 goroutine 正在入队，需要重试。
		if diff <= 0 {
			if r.tail.CompareAndSwap(tail, tail+1) {
				n.val = v
				n.seq.Store(tail + 1)
				// 缓冲区已满时推进头指针，覆盖最旧的元素。
				if tail-head >= r.mask+1 {
					r.head.CompareAndSwap(head, head+1)
				}
				return
			}
		}
		runtime.Gosched()
	}
}

// Clone 返回缓冲区的浅拷贝。
//
// 复制所有槽位并非一个原子操作。调用方必须保证 Clone 与 Push 不会同时执行。
func (r *RingBuffer[T]) Clone() *RingBuffer[T] {
	if r == nil {
		return nil
	}
	size := r.mask + 1
	dest := &RingBuffer[T]{
		mask: r.mask,
		data: make([]_RingNode[T], size),
	}
	copy(dest.data, r.data)
	dest.head.Store(r.head.Load())
	dest.tail.Store(r.tail.Load())
	return dest
}

// Reset 清空缓冲区，同时清除槽位中保存的值。
//
// 清零槽位可释放对旧对象的引用，重置序号后槽位即可再次使用。调用方必须将 Reset 与其他读写操作互斥执行。
func (r *RingBuffer[T]) Reset() {
	tail := r.tail.Load()
	var zero T
	span := r.mask + 1
	for i := uint64(0); i < span; i++ {
		r.data[i].val = zero
		r.data[i].seq.Store(tail + (i+span-tail%span)%span)
	}
	r.head.Store(tail)
}

// DropWhile 从头部移除连续满足条件的元素，并返回移除的数量。
//
// 样本按时间排列，过期样本集中在头部，只需向前扫描一次即可裁剪窗口，无需重新分配内存。
// 调用方必须将 DropWhile 与其他读写操作互斥执行。
func (r *RingBuffer[T]) DropWhile(predicate func(T) bool) int {
	dropped := 0
	for {
		head := r.head.Load()
		tail := r.tail.Load()
		if head == tail {
			return dropped
		}
		n := &r.data[head&r.mask]
		switch diff := int64(n.seq.Load() - head - 1); {
		case diff < 0:
			// 当前槽位为空，缓冲区中已没有可读元素。
			return dropped
		case diff > 0:
			// 另一个 goroutine 正在写入此槽位，稍后重试。
			runtime.Gosched()
			continue
		}
		if !predicate(n.val) {
			return dropped
		}
		if !r.head.CompareAndSwap(head, head+1) {
			continue
		}
		var zero T
		n.val = zero
		n.seq.Store(head + r.mask + 1)
		dropped++
	}
}

// At 按从头部开始的索引读取元素。
//
// 索引 0 对应最旧的元素，越界时返回 false。调用方必须将 At 与其他并发访问同步。
func (r *RingBuffer[T]) At(index int) (v T, ok bool) {
	if index < 0 {
		return v, false
	}
	head := r.head.Load()
	position := head + uint64(index)
	if position >= r.tail.Load() {
		return v, false
	}
	n := &r.data[position&r.mask]
	if n.seq.Load() != position+1 {
		return v, false
	}
	return n.val, true
}

// First 返回缓冲区中最旧的元素。
func (r *RingBuffer[T]) First() (v T, ok bool) {
	for {
		head := r.head.Load()
		n := &r.data[head&r.mask]

		seq := n.seq.Load()
		diff := int64(seq - head - 1)
		// diff 小于零表示槽位为空，返回 false。等于零表示可读。
		// 大于零表示另一个 goroutine 正在出队，需要重试。
		switch {
		case diff < 0:
			return v, false // 缓冲区为空。
		case diff == 0:
			return n.val, true
		}
		runtime.Gosched()
	}
}

// Data 将缓冲区中的元素复制到新切片并返回。
//
// 若其他 goroutine 在复制期间修改缓冲区，切片中的元素可能来自不同时间点。
func (r *RingBuffer[T]) Data() []T {
	head := r.head.Load()
	tail := r.tail.Load()

	result := make([]T, 0, tail-head)
	for i := head; i < tail; i++ {
		n := &r.data[i&r.mask]
		seq := n.seq.Load()
		if seq == i+1 {
			result = append(result, n.val)
		}
	}
	return result
}

// Len 返回缓冲区中的元素数。
func (r *RingBuffer[T]) Len() int {
	head := r.head.Load()
	tail := r.tail.Load()
	return int(tail - head)
}
