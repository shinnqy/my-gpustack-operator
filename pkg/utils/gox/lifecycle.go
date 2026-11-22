package gox

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"

	"go.uber.org/multierr"
)

// 生命周期是组件的运行至停止过程：即每个任务启动时所处的上下文，以及其停止时执行的取消操作。
//
// 一个仅以调用者为终结点的组件其实不需要终结点。GroupWithContextIn 在调用者放弃或其中某个任务失败时，就会
// 自动终止任务。这适用于那些被除调用者之外的其他对象停止的组件——这是没有来自调用者的上下文所能表达的：取
// 消操作属于该组件，其`Stop`方法负责控制它，而被停止则不是需要报告的结果。如果监督者将这种停止视为失败并
// 据此采取行动，那也是合理的。
//
// 零值已准备就绪可供使用。一旦启动，不得复制该值，且一旦停止，则不会再次运行。
type Lifecycle struct {
	// mu 保护以下运行状态：Start 在开始时写入它，在退出时清除它，而 Stop 则通过另一个 goroutine 访问
	// 它。
	mu   sync.Mutex
	stop context.CancelFunc
	// 当 Start 返回后，done 状态即被关闭，这也是 Stop 所等待的：若在此之前返回，就会将仍在运行的任务
	// 标记为已停止，而它们所持有的内容正是替代者即将接手的内容。
	done chan struct{}
	// stopped 会保留在运行前被停止的 Stop，避免其丢失。被放入资源池的组件在被丢弃后仍能顺利开始运行，
	// 而此时开始的运行则不会受到取消操作的影响——这种泄漏现象的存在是为了防止其再次发生，通过旨在修复
	// 它的拆卸过程即可解决。
	stopped bool
}

// 启动操作会在由 ctx 派生的一个上下文下运行所有任务，并阻塞直到所有任务完成。
//
// 失败的任务会终止其余任务：其下的组在其中一项任务报告错误后，会取消它们共享的上下文。这也是能够报告失败
// 的原因——在所有任务都完成之前，无法报告任何内容。如果某个任务只关注自己的上下文，那么在其运行期间就会保
// 留其他任务的失败状态。仅完成任务的任务则被标记为已完成，其下的任务继续运行：一个组件应该拥有完成的任务，
// 而不是损坏的任务。而逐渐恶化而非失败的任务是常态。
//
// 它返回任务的失败情况、调用者的错误（仅当发生这种情况时），以及当运行因调用了 Stop 而结束时的 nil 值。作
// 为取消操作的错误不会被报告：要么是此生命周期自身的清理操作反馈回来，要么是子任务失败导致运行已结束，无
// 论哪种情况都有自己的报告。因此，无法报告真实失败情况的任务不应在其中包含 context.Canceled。发生 panic
// 的任务会被作为错误报告，包含 panic 及其栈信息，以及其子任务所报告的任何信息。
//
// 在已停止的生命周期中，它不会启动任何操作，并返回 nil。在正在运行的生命周期中，它同样不会启动任何操作，
// 并在该运行结束后に返回。因此，重新调用的调用者无法在同一个 Stop 下拥有两组任务。
func (l *Lifecycle) Start(ctx context.Context, tasks ...func(context.Context) error) error {
	caller := ctx
	ctx, stop := context.WithCancel(caller)
	defer stop()

	done, state := l.begin(stop)
	if state == runStateStopped {
		return nil
	}
	if state == runStateRunning {
		// 这个调用开始时没有任何内容，因此没有需要等待的自身行为——但在其运行的期间如果继续运行，
		// 则会被视为一个不存在的组件。而等待调用者操作则能持续超过该运行时间。在没有任何人取消的
		// 上下文中，它根本不会结束。
		select {
		case <-done:
		case <-caller.Done():
		}

		return caller.Err()
	}
	defer l.end(done)

	var (
		mu       sync.Mutex
		failures error
	)

	gp := GroupWithContextIn(ctx)
	for i := range tasks {
		task := tasks[i]
		gp.Go(func(ctx context.Context) error {
			err := runTask(ctx, task)
			if err != nil && !errors.Is(err, context.Canceled) {
				mu.Lock()
				failures = multierr.Append(failures, err)
				mu.Unlock()
			}

			return err
		})
	}

	// 该小组的报错信息表明运行已结束，而非具体原因——取消操作会掩盖任务所报告的任何信息，而一个资源池
	// 中从未运行的任务则完全不会产生任何结果。因此，上述任务的结果就是报告内容，而此报告旨在确保每一
	// 个任务都已完成。它仍被视为一种兜底机制，因为该小组的报错并未涵盖任何任务。
	if err := gp.Wait(); failures == nil && err != nil && !errors.Is(err, context.Canceled) {
		failures = err
	}

	mu.Lock()
	defer mu.Unlock()

	// 失败比调用者自己的取消操作更为严重。一个正在关闭的调用者已经知道这一点。失败是只有这种返回才能
	// 揭示出来的事情，而进程终止时发生的失败任务，正是值得关注的。
	if failures != nil {
		return failures
	}

	return caller.Err()
}

// Stop 会取消当前运行中所有任务，直到 Start 完成才会返回。Start 启动的进程仍会继续运行。它也会停止尚未开
// 始的运行，且生命周期机制在之后也不会再次运行。
//
// 停止一个实例两次并无成本。但停止一个未运行的实例却不是免费的：因为被停止的动作会被记录，所以从资源池中
// 排队等待启动的进程之后，启动操作将不会执行，也不会产生任何报告——这正是重点，因为那个启动操作是无人负责
// 取消的。
//
// 不得从任务内部调用它：它等待运行结束，而运行直到该任务返回才会结束。
func (l *Lifecycle) Stop() {
	// 取消操作会与在运行结束时关闭的通道一起被处理，因此下面等待的其实是被此 Stop 操作取消的该运行，
	// 而非该运行中已关闭的后续运行。
	l.mu.Lock()
	l.stopped = true
	stop, done := l.stop, l.done
	l.mu.Unlock()

	if stop == nil {
		return
	}

	stop()
	<-done
}

// runTask 执行一个任务，并将崩溃情况转化为它本应返回的错误。
//
// 在这里恢复，而不是将其留给团队处理，才是使恐慌事件可被报告的原因：团队只保留其任务产生的第一个错误，因
// 此每当一个子任务首先失败时，恐慌事件就会被忽略，而运行结果则被报告为仅该子任务的失败。
func runTask(ctx context.Context, task func(context.Context) error) (err error) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}

		// 已经是错误的 panic 值会被包裹处理，而不是格式化处理，因此调用者仍然可以根据发生 panic
		// 的原因进行匹配。
		if e, ok := r.(error); ok {
			err = fmt.Errorf("task panicked: %w\n%s", e, debug.Stack())

			return
		}

		err = fmt.Errorf("task panicked: %v\n%s", r, debug.Stack())
	}()

	return task(ctx)
}

// runState 是指属于该生命周期的启动运行所发现的内容。
type runState uint8

const (
	// runStateReady 表示运行已记录完成，可以继续进行。
	runStateReady runState = iota
	// runStateRunning 表示另一次运行持有此生命周期。
	runStateRunning
	// runStateStopped 表示此生命周期已停止，因此没有内容可运行。
	runStateStopped
)

// 它记录起跑的取消情况，并将结束于该位置的通道交还回去。在相同的停止锁下决定停止，就能避免停止被丢失：在
// 停止之外，当停止找不到可取消的对象时，或者当停止返回时，起跑就可以开始。
func (l *Lifecycle) begin(stop context.CancelFunc) (chan struct{}, runState) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.stopped {
		return nil, runStateStopped
	}
	if l.stop != nil {
		// 这是运行进程自己的通道，因此一个从零开始的调用仍然可以等待与运行进程冲突，而不是等待调
		// 用者。
		return l.done, runStateRunning
	}

	done := make(chan struct{})
	l.stop, l.done = stop, done

	return done, runStateReady
}

// end 会撤销已完成的运行，并释放所有等待中的 Stop 指令。
func (l *Lifecycle) end(done chan struct{}) {
	l.mu.Lock()
	l.stop, l.done = nil, nil
	l.mu.Unlock()

	close(done)
}
