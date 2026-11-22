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

// Prepare 准备管理器的运行时环境，
// 包括安装 CRD 和注册指标采集器。
func (m *Manager) Prepare(_ context.Context) error {
	// 注册指标采集器。
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

// Start 启动管理器。
//
// Start 在启动 controller manager 之前设置控制器并注册辅助路由。
func (m *Manager) Start(ctx context.Context) error {
	cm := m.CtrlManager
	ms := cm.GetWebhookServer()

	// 注册 /metrics。
	ms.Register("/metrics", newMetricsHandler(ctrlmetrics.Registry))

	// 注册 /readyz。
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

	// 注册 /livez。
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

	// 注册 /debug。
	{
		runtime.SetBlockProfileRate(1)
		ms.Register("/debug/pprof/", httpx.LoopbackAccessHandlerFunc(pprof.Index))
		ms.Register("/debug/pprof/cmdline", httpx.LoopbackAccessHandlerFunc(pprof.Cmdline))
		ms.Register("/debug/pprof/profile", httpx.LoopbackAccessHandlerFunc(pprof.Profile))
		ms.Register("/debug/pprof/symbol", httpx.LoopbackAccessHandlerFunc(pprof.Symbol))
		ms.Register("/debug/pprof/trace", httpx.LoopbackAccessHandlerFunc(pprof.Trace))
		ms.Register("/debug/flags/v", httpx.LoopbackAccessHandlerFunc(routes.StringFlagPutHandler(logs.GlogSetter)))
	}

	// Start。
	//
	// controller manager 返回即为终止状态，不仅限于返回 error 时：
	// 一旦失去 leader 租约，controller-runtime 就会立即停止所有控制器并返回，
	// 而单次缓慢的 apiserver 往返就足以触发这种情况。需要记录该事件的发生，
	// 因为此时进程未必会退出——如果有同级任务在收到取消信号后未能返回，
	// 进程及其 HTTP 处理程序就会继续存活——而一个只回应存活探针却不再执行任何调谐的进程
	// 会导致最坏的双重后果：Kubernetes 看到健康的 Pod，而整条调度链却悄无声息地停止收敛。
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
