package gox

import (
	"context"
	"sync"

	pond "github.com/alitto/pond/v2"
	"go.uber.org/multierr"
	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/pkg/utils/loggerx"
)

type IWaitGroup interface {
	Wait() error
	Go(func() error)
}

type IContextWaitGroup interface {
	Wait() error
	Go(func(context.Context) error)
}

// Group 返回一个等待组，
// 该等待组在所有任务完成时关闭并聚合来自任务的 error。
func Group() IWaitGroup {
	lg := klog.Background().WithName("gopool")
	return &waitGroup{lg: lg}
}

type waitGroup struct {
	lg  klog.Logger
	g   sync.WaitGroup
	m   sync.Mutex
	err error
}

// Wait 阻塞直到所有任务完成，并聚合来自任务的 error。
func (g *waitGroup) Wait() error {
	g.g.Wait()
	return g.err
}

// Go 将任务作为 goroutine 提交。
func (g *waitGroup) Go(f func() error) {
	if f == nil {
		return
	}

	wf := func() (err error) {
		defer loggerx.RecoverWithGoroutineStackScanner(func(s loggerx.Scanner, e error) {
			g.lg.Error(e, "caught panic in task")
			for s.Scan() {
				g.lg.Error(nil, s.Text())
			}
			err = e
		})

		return f()
	}

	g.g.Add(1)
	Go(func() {
		defer g.g.Done()

		err := wf()
		if err != nil {
			g.m.Lock()
			g.err = multierr.Append(g.err, err)
			g.m.Unlock()
		}
	})
}

// newContextWaitGroup 返回具体的等待组类型，以便包内调用方能够访问
// IWaitGroup 接口未携带的派生 context。
func newContextWaitGroup(ctx context.Context) contextWaitGroup {
	return contextWaitGroup{
		lg: klog.Background().WithName("gopool"),
		g:  gp.NewGroupContext(ctx),
	}
}

type contextWaitGroup struct {
	lg klog.Logger
	g  pond.TaskGroup
}

// Wait 阻塞直到所有任务完成、
// 其中任一任务返回非 nil error，或与该等待组关联的 context 被取消。
func (g contextWaitGroup) Wait() error {
	return g.g.Wait()
}

// context 返回该等待组中的任务应运行的 context。
func (g contextWaitGroup) context() context.Context {
	return g.g.Context()
}

// Go 将任务作为 goroutine 提交。
func (g contextWaitGroup) Go(f func() error) {
	if f == nil {
		return
	}

	wf := func() (err error) {
		defer loggerx.RecoverWithGoroutineStackScanner(func(s loggerx.Scanner, e error) {
			g.lg.Error(e, "caught panic in task")
			for s.Scan() {
				g.lg.Error(nil, s.Text())
			}
			err = e
		})

		return f()
	}

	g.g.SubmitErr(wf)
}

// GroupWithContextIn 与 GroupWithContext 类似，但在提交时向每个任务传入等待组自身的派生
// context，是一种更紧凑的用法。
//
// 该 context 刻意不是调用方的 context。它会在调用方的 context 结束时终止，也会在
// 组内任一任务返回 error 时立即终止——因此仅监听自身 context 的任务在兄弟任务失败时
// 仍能返回。这正是让等待组能够上报该失败的原因：Wait 必须等所有任务都返回后才会返回，
// 因此如果一个任务一直在等待没人取消的 context，就会导致兄弟任务的 error 在调用方
// 存活期间一直无法返回。仅返回 nil 的任务不会触发取消，因此相互协作接力的任务组
// 仍会运行直至全部完成。
func GroupWithContextIn(ctx context.Context) IContextWaitGroup {
	g := newContextWaitGroup(ctx)

	return embeddedContextWaitGroup{g: g, c: g.context()}
}

type embeddedContextWaitGroup struct {
	g IWaitGroup
	c context.Context
}

// Wait 阻塞直到所有任务完成、
// 其中任一任务返回非 nil error，或与该等待组关联的 context 被取消。
func (g embeddedContextWaitGroup) Wait() error {
	return g.g.Wait()
}

// Go 将任务作为 goroutine 提交。
func (g embeddedContextWaitGroup) Go(f func(context.Context) error) {
	if f == nil {
		return
	}

	g.g.Go(func() error {
		return f(g.c)
	})
}
