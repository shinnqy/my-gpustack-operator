package extensionapi

import (
	"runtime"

	"go.uber.org/atomic"
)

type WatchBookmark struct {
	rv *atomic.String
}

// NewWatchBookmark 创建一个新的 WatchBookmark。
func NewWatchBookmark() WatchBookmark {
	return WatchBookmark{
		rv: atomic.NewString("0"),
	}
}

// SwapResourceVersion 在新资源版本更大时将当前资源版本替换为新版本。
//
// 如果未发生替换，SwapResourceVersion 返回 false。
func (c WatchBookmark) SwapResourceVersion(newRv string) bool {
	for {
		oldRv := c.rv.Load()
		if oldRv == newRv ||
			len(oldRv) > len(newRv) ||
			(len(oldRv) == len(newRv) && oldRv > newRv) {
			return false
		}
		if c.rv.CompareAndSwap(oldRv, newRv) {
			return true
		}
		runtime.Gosched() // 让出调度权给其他 goroutine。
	}
}

// GetResourceVersion 返回最新的资源版本。
func (c WatchBookmark) GetResourceVersion() string {
	return c.rv.Load()
}

// DeepCopy 返回 WatchBookmark 的深拷贝。
func (c WatchBookmark) DeepCopy() WatchBookmark {
	return WatchBookmark{
		rv: atomic.NewString(c.rv.Load()),
	}
}
