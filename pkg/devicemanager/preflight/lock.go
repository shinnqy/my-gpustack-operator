package preflight

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"gpustack.ai/gpustack/pkg/deviceplugin"
)

// lockName 是预检专用目录中的锁文件名。锁由打开的文件描述符持有。
const lockName = "lock"

// errContended 专门表示另一轮预检已持有锁。权限、文件系统等问题各自保留原始错误。
var errContended = errors.New("another preflight is already running on this node")

// flock 使用系统的文件锁操作，可单独校验不同 errno 的处理。
var flock = syscall.Flock

// lockHost 获取节点级预检锁并返回释放函数。它使清理遗留容器和探测互斥，进程被终止时操作系统会释放描述符上的锁。
func lockHost(hostRoot string) (release func(), err error) {
	dir := filepath.Join(hostRoot, deviceplugin.OperatorPreflightDir)
	if err = os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create the preflight directory to lock: %w", err)
	}

	f, err := os.OpenFile(filepath.Join(dir, lockName), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open the preflight lock: %w", err)
	}

	if err = flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		// 只有锁已占用这一错误才说明另一轮正在运行。其他错误也会阻止本轮执行，但需要报告各自原因。
		if !errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("take the preflight lock on %s: %w",
				filepath.Join(deviceplugin.OperatorPreflightDir, lockName), err)
		}
		return nil, fmt.Errorf("%w: it holds %s. Two runs sweep each other's probe containers and "+
			"share one rendered-artifact tree, so this one would report failures that belong to "+
			"neither: %w", errContended,
			filepath.Join(deviceplugin.OperatorPreflightDir, lockName), err)
	}

	return func() {
		// 先显式解锁再关闭描述符，确保本轮清理完成后其他进程才有机会进入。
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}, nil
}
