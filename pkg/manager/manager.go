package manager

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/pprof"
	"runtime"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"k8s.io/apiserver/pkg/server/healthz"
	"k8s.io/apiserver/pkg/server/routes"
	"k8s.io/component-base/logs"
	ctrlhealthz "sigs.k8s.io/controller-runtime/pkg/healthz"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes"
	"gpustack.ai/gpustack/pkg/kubediscovery"
	"gpustack.ai/gpustack/pkg/utils/gox"
	"gpustack.ai/gpustack/pkg/utils/httpx"
)

type Manager struct {
	KubeClient  kubernetes.Interface
	CtrlManager CtrlManager

	sentinel _CtrlManagerSentinel
	stopped  atomic.Bool
}

// Prepare prepares the runtime for the manager,
// including installing CRDs and registering metric collectors.
func (m *Manager) Prepare(_ context.Context) error {
	// Register metric collectors.
	{
		reg := ctrlmetrics.Registry
		cs := []prometheus.Collector{
			collectors.NewBuildInfoCollector(),
			gox.NewStatsCollector(),
		}
		for i := range cs {
			err := reg.Register(cs[i])
			if err != nil {
				return fmt.Errorf("register metric collector: %w", err)
			}
		}
	}

	return nil
}

// Start starts the manager.
//
// Start sets up controllers and registers assistant routes,
// before starting the controller manager.
func (m *Manager) Start(ctx context.Context) error {
	cm := m.CtrlManager
	ms := cm.GetWebhookServer()

	// Register /metrics.
	ms.Register("/metrics", newMetricsHandler(ctrlmetrics.Registry))

	// Register /readyz.
	{
		p := "/readyz"
		h := &ctrlhealthz.Handler{
			Checks: map[string]ctrlhealthz.Checker{
				"ping": ctrlhealthz.Ping,
				"log":  healthz.LogHealthz.Check,
			},
		}
		ms.Register(p, http.StripPrefix(p, h))
	}

	// Register /livez.
	{
		p := "/livez"
		h := &ctrlhealthz.Handler{
			Checks: map[string]ctrlhealthz.Checker{
				"ping": ctrlhealthz.Ping,
				"log":  healthz.LogHealthz.Check,
				"gopool": func(r *http.Request) error {
					return gox.IsHealthy()
				},
				"loopback": func(r *http.Request) error {
					return kubediscovery.IsConnected(r.Context(), m.KubeClient.Discovery())
				},
				"informer": func(r *http.Request) error {
					if cm.GetCache().WaitForCacheSync(r.Context()) {
						return nil
					}
					return errors.New("informer cache is not synced yet")
				},
			},
		}
		ms.Register(p, http.StripPrefix(p, h))
	}

	// Register /debug.
	{
		runtime.SetBlockProfileRate(1)
		ms.Register("/debug/pprof/", httpx.LoopbackAccessHandlerFunc(pprof.Index))
		ms.Register("/debug/pprof/cmdline", httpx.LoopbackAccessHandlerFunc(pprof.Cmdline))
		ms.Register("/debug/pprof/profile", httpx.LoopbackAccessHandlerFunc(pprof.Profile))
		ms.Register("/debug/pprof/symbol", httpx.LoopbackAccessHandlerFunc(pprof.Symbol))
		ms.Register("/debug/pprof/trace", httpx.LoopbackAccessHandlerFunc(pprof.Trace))
		ms.Register("/debug/flags/v", httpx.LoopbackAccessHandlerFunc(routes.StringFlagPutHandler(logs.GlogSetter)))
	}

	// Start.
	//
	// The controller manager returning is terminal, and not only when it returns an error:
	// controller-runtime stops every controller and returns as soon as the leader lease is lost,
	// which a single slow apiserver round trip is enough to cause. Record that it happened,
	// because the process does not necessarily exit when it does — a sibling task that fails to
	// return on cancellation keeps the process, and its HTTP handlers, alive — and a process that
	// keeps answering its liveness probe while reconciling nothing is the worst of both outcomes:
	// Kubernetes sees a healthy Pod and the whole chain silently stops converging.
	defer m.stopped.Store(true)
	return cm.Start(ctx)
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
