package manager

import (
	"context"
	"errors"
	"sync/atomic"

	ctrl "sigs.k8s.io/controller-runtime"
)

// CtrlManager 保留 controller-runtime 提供的客户端、缓存和注册能力。
type CtrlManager struct {
	ctrl.Manager
}

type Manager struct {
	CtrlManager CtrlManager
	sentinel    _CtrlManagerSentinel
	stopped     atomic.Bool
}

func (m *Manager) Start(ctx context.Context) error {
	defer m.stopped.Store(true)
	return m.CtrlManager.Start(ctx)
}

// WaitForReady 等待管理器启动并完成缓存的首次同步。
func (m *Manager) WaitForReady(ctx context.Context) error {
	// sentinel 启动后一直关闭，所以先判断管理器是否已经退出。
	if m.stopped.Load() {
		return errors.New("controller manager has stopped")
	}

	// 等待管理器启动，或响应调用方取消。
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.sentinel.Done():
	}

	// 缓存完成首次同步后，读取方才能使用它。
	if !m.CtrlManager.GetCache().WaitForCacheSync(ctx) {
		return errors.New("cache is not synced yet")
	}

	return nil
}

// _CtrlManagerSentinel 在管理器启动时关闭信号通道。
type _CtrlManagerSentinel struct {
	done chan struct{}
}

func (s _CtrlManagerSentinel) Start(ctx context.Context) error {
	close(s.done)
	<-ctx.Done()
	return nil
}

func (s _CtrlManagerSentinel) NeedLeaderElection() bool {
	return false
}

func (s _CtrlManagerSentinel) Done() <-chan struct{} {
	return s.done
}
