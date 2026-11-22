package gox

import (
	"errors"
	"runtime"
	"sync"

	pond "github.com/alitto/pond/v2"
	klog "k8s.io/klog/v2"
)

var (
	once sync.Once
	gp   = NewPool(100)
)

// NewPool 使用给定的 factor 创建一个新的 goroutine 池。
//
// factor 用于基于可用 CPU 核心数计算池中的最大 worker 数量。
//
// factor 应为正整数；如果小于 10，将默认设置为 10。
func NewPool(factor int) pond.Pool {
	// NB(thxCode): Go 允许我们随意创建 goroutine，但如果创建过多 goroutine，
	// 会导致程序因内存不足而崩溃，因此我们需要通过池化来限制 goroutine 的数量。
	//
	// 池化的优势在于以空间换时间，并提高复用率。
	//
	// - MaxWorkers 是池应创建的 goroutine 总数，
	//   目前我们以可用 CPU 核心数作为基准值，
	//   然后乘以给定的 factor 得到该值。
	// - MaxQueueSize 是同时可接收的提交 goroutine 数量上限，
	//   我们取 MaxWorkers 的 150% 作为结果。
	factor = max(factor, 10)
	maxWorkers := runtime.GOMAXPROCS(0) * factor
	maxQueueSize := maxWorkers / 10 * 15
	return pond.NewPool(maxWorkers, pond.WithQueueSize(maxQueueSize))
}

// Configure 使用新的 factor 仅配置一次 goroutine 池，
// 多次调用将被忽略。
func Configure(factor int) {
	once.Do(func() {
		gp.Stop()
		gp = NewPool(factor)
	})
}

// Go 将任务作为 goroutine 提交。
func Go(f func()) {
	if _, ok := gp.TrySubmit(f); !ok {
		klog.Background().WithName("gopool").
			V(5).
			Info("goroutine pool full")
		gp.Submit(f)
	}
}

// IsHealthy 在池中拥有充足的 worker 时返回 nil。
func IsHealthy(atLeast ...int) error {
	watermark := 0
	if len(atLeast) > 0 {
		watermark = max(atLeast[0], 0)
	}
	idle := max(gp.MaxConcurrency()-int(gp.RunningWorkers()), 0)

	if idle > watermark {
		return nil
	}
	return errors.New("goroutine pool full")
}
