//go:build !windows

package signalx

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	klog "k8s.io/klog/v2"
)

var registered = make(chan struct{})

// Handler 收到中断或终止信号时取消 context。作为容器 PID 1 时还负责回收孤儿进程。
func Handler() context.Context {
	close(registered) // 调用两次时会 panic。

	logger := klog.Background().WithName("signal").WithName("handler")

	sigs := []os.Signal{syscall.SIGINT, syscall.SIGTERM}
	isRootProc := syscall.Getpid() == 1

	if isRootProc { // 回收子进程。
		sigs = append(sigs, syscall.SIGCHLD)
	}

	sigChan := make(chan os.Signal, len(sigs))
	reapChan := make(chan os.Signal)
	ctx, cancel := context.WithCancel(context.Background())

	// 注册信号。
	signal.Notify(sigChan, sigs...)

	// 处理信号。
	go func() {
		var exited bool

		for sig := range sigChan {
			logger.V(4).
				Info("received", "signal", sig)

			if !exited && sig == syscall.SIGCHLD {
				select {
				case reapChan <- sig:
				default:
					// 如果通道已满则不阻塞。
				}

				continue
			}

			if exited {
				os.Exit(1)
			}

			logger.Info("exiting")
			cancel()
			exited = true

			close(reapChan)
		}
	}()

	if !isRootProc {
		return ctx
	}

	// 回收子进程。
	go func() {
		for range reapChan {
			const zombieTimeWait = 500 * time.Millisecond

			for {
				runtime.Gosched()

				// 使用 waitid 获取已退出的子进程，
				// 与 waitpid 不同，waitid 可以将子进程保留在可等待状态。
				var si Siginfo

				err := syscallWaitid(P_ALL, 0, &si, syscall.WEXITED|syscall.WNOWAIT, nil)
				for errors.Is(err, syscall.EINTR) { // 如果被中断则重试。
					err = syscallWaitid(P_ALL, 0, &si, syscall.WEXITED|syscall.WNOWAIT, nil)
				}

				pid := int(si.Pid)

				if pid <= 0 {
					if err != nil {
						logger.V(4).
							Error(err, "reap child process")
					}

					break // 无法捕获任何子进程，返回轮询。
				}

				if si.Exited() {
					// 由于 EXIT_ZOMBIE 位于 EXIT_DEAD 之前，
					// 等待一段时间以避免误回收处于过渡状态的进程。
					time.Sleep(zombieTimeWait)

					if !isZombieProcess(pid) {
						continue // 继续确认下一个子进程。
					}

					// 如果进程仍为僵尸进程则再次等待，
					// 以便该进程能被其父进程回收。
					time.Sleep(zombieTimeWait)
				}

				// 使用 wait4(waitpid) 回收异常退出的子进程。
				wpid, err := syscall.Wait4(pid, nil, syscall.WNOHANG, nil)
				for errors.Is(err, syscall.EINTR) { // 如果被中断则重试。
					wpid, err = syscall.Wait4(pid, nil, syscall.WNOHANG, nil)
				}

				if wpid <= 0 {
					continue // 无法回收候选进程，继续确认下一个。
				}

				logger.V(4).
					Info("reaped child process", "pid", wpid)
			}
		}
	}()

	return ctx
}

// isZombieProcess 在进程为僵尸进程时返回 true，
// 它通过读取 /proc/<pid>/stat 文件来检查进程是否为僵尸进程。
func isZombieProcess(pid int) bool {
	bs, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return false
	}

	return bs[strings.IndexRune(string(bs), ')')+2] == 'Z'
}
