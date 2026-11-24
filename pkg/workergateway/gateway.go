package workergateway

import (
	"context"
	"fmt"
	"net/http"
	"net/http/pprof"
	"runtime"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/apiserver/pkg/server/healthz"
	"k8s.io/apiserver/pkg/server/routes"
	"k8s.io/component-base/logs"
	"k8s.io/component-base/metrics/legacyregistry"
	klog "k8s.io/klog/v2"
	ctrlhealthz "sigs.k8s.io/controller-runtime/pkg/healthz"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"gpustack.ai/gpustack/pkg/utils/gox"
	"gpustack.ai/gpustack/pkg/utils/httpx"
	"gpustack.ai/gpustack/pkg/webserver"
	"gpustack.ai/gpustack/pkg/workergateway/manager"
	"gpustack.ai/gpustack/pkg/workergateway/service"
)

func init() {
	ctrlmetrics.Registry = struct {
		prometheus.Registerer
		prometheus.Gatherer
	}{
		Registerer: legacyregistry.Registerer(),
		Gatherer:   legacyregistry.DefaultGatherer,
	}
}

type WorkerGateway struct {
	// 管理远端 Worker 的订阅、缓存和查询。
	Manager manager.Manager

	// 接收网关请求的 Web 服务。
	Server webserver.Server
}

// Prepare 在启动监听前注册网关运行所需的指标采集器。
func (wg *WorkerGateway) Prepare(ctx context.Context) error {
	// 注册构建信息和 goroutine 池指标，沿用 Kubernetes 的指标注册表。
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

func (wg *WorkerGateway) Start(ctx context.Context) error {
	mu := wg.Server

	// 在 /metrics 提供已注册的指标。
	{
		h := promhttp.HandlerOpts{
			ErrorLog:      klog.NewStandardLogger("WARNING"),
			ErrorHandling: promhttp.HTTPErrorOnError,
		}
		mu.Register("/metrics", promhttp.HandlerFor(ctrlmetrics.Registry, h))
	}

	// 在 /readyz 检查服务与日志组件的就绪状态。
	{
		p := "/readyz"
		h := &ctrlhealthz.Handler{
			Checks: map[string]ctrlhealthz.Checker{
				"ping": ctrlhealthz.Ping,
				"log":  healthz.LogHealthz.Check,
			},
		}
		mu.Register(p, http.StripPrefix(p, h))
	}

	// 在 /livez 检查服务、日志组件和 goroutine 池是否能正常工作。
	{
		p := "/livez"
		h := &ctrlhealthz.Handler{
			Checks: map[string]ctrlhealthz.Checker{
				"ping": ctrlhealthz.Ping,
				"log":  healthz.LogHealthz.Check,
				"gopool": func(r *http.Request) error {
					return gox.IsHealthy()
				},
			},
		}
		mu.Register(p, http.StripPrefix(p, h))
	}

	// 注册限制本机访问的调试端点。
	{
		runtime.SetBlockProfileRate(1)
		mu.Register("/debug/pprof/", httpx.LoopbackAccessHandlerFunc(pprof.Index))
		mu.Register("/debug/pprof/cmdline", httpx.LoopbackAccessHandlerFunc(pprof.Cmdline))
		mu.Register("/debug/pprof/profile", httpx.LoopbackAccessHandlerFunc(pprof.Profile))
		mu.Register("/debug/pprof/symbol", httpx.LoopbackAccessHandlerFunc(pprof.Symbol))
		mu.Register("/debug/pprof/trace", httpx.LoopbackAccessHandlerFunc(pprof.Trace))
		mu.Register("/debug/flags/v", httpx.LoopbackAccessHandlerFunc(routes.StringFlagPutHandler(logs.GlogSetter)))
	}

	// 把 /apis/ 下的请求转给网关路由，内层路由只处理去掉 /apis 前缀后的路径。
	{
		svc, err := service.New(ctx, wg.Manager)
		if err != nil {
			return fmt.Errorf("create service: %w", err)
		}
		p := "/apis"
		mu.Register(p+"/", http.StripPrefix(p, svc.Index()))
	}

	klog.Info("starting worker gateway")
	return mu.Start(ctx)
}
