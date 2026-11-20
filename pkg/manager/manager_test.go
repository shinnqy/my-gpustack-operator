package manager

import (
	"context"
	"errors"
	"strings"
	"testing"

	ctrl "sigs.k8s.io/controller-runtime"
	ctrlcache "sigs.k8s.io/controller-runtime/pkg/cache"
)

// _FakeCache 用固定结果模拟缓存是否已同步。
type _FakeCache struct {
	ctrlcache.Cache

	synced bool
}

func (c _FakeCache) WaitForCacheSync(context.Context) bool { return c.synced }

// _FakeCtrlManager 为 readiness 检查提供模拟缓存。
type _FakeCtrlManager struct {
	ctrl.Manager

	cache ctrlcache.Cache
}

func (m _FakeCtrlManager) GetCache() ctrlcache.Cache { return m.cache }

func TestManager_WaitForReady(t *testing.T) {
	// 检查尚未启动、缓存同步中和已经停止的管理器所报告的状态。
	testCases := []struct {
		name           string
		started        bool
		stopped        bool
		cacheSynced    bool
		cancelContext  bool
		wantErr        error
		wantErrMessage string
	}{
		{
			name:        "a started manager with a synced cache is ready",
			started:     true,
			cacheSynced: true,
		},
		{
			name:           "a stopped manager is not ready",
			started:        true,
			stopped:        true,
			cacheSynced:    true,
			wantErrMessage: "stopped",
		},
		{
			name:           "a started manager whose cache is not synced is not ready",
			started:        true,
			wantErrMessage: "not synced",
		},
		{
			name:          "a manager that has not started yet fails with the caller's context",
			cancelContext: true,
			wantErr:       context.Canceled,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			m := &Manager{
				CtrlManager: CtrlManager{
					Manager: _FakeCtrlManager{cache: _FakeCache{synced: tc.cacheSynced}},
				},
				sentinel: _CtrlManagerSentinel{done: make(chan struct{})},
			}
			m.stopped.Store(tc.stopped)
			if tc.started {
				// sentinel 在管理器启动时关闭，停止后也保持关闭。
				close(m.sentinel.done)
			}

			ctx := context.Background()
			if tc.cancelContext {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}

			err := m.WaitForReady(ctx)

			if tc.wantErr == nil && tc.wantErrMessage == "" {
				if err != nil {
					t.Fatalf("WaitForReady() error = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("WaitForReady() = nil, want an error")
			}
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Errorf("WaitForReady() error = %v, want %v", err, tc.wantErr)
			}
			if tc.wantErrMessage != "" && !strings.Contains(err.Error(), tc.wantErrMessage) {
				t.Errorf("WaitForReady() error = %q, want it to mention %q", err, tc.wantErrMessage)
			}
		})
	}
}
