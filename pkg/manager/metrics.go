package manager

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	klog "k8s.io/klog/v2"
)

// newMetricsHandler 以 Prometheus 文本暴露格式提供给定 gatherer 的指标。
//
// 当某个采集器的数据源失败时，响应会降级为已采集到的指标，而不是全部丢弃：
// 该端点承载了多个独立数据源，因其中一个不可用而丢失整台节点的指标，
// 远不如上报其余指标并将该失败通过其自身的 gauge 体现出来。
// 只有在采集结果完全为空时才会使请求失败，因为此时已无内容可提供。
//
// 只有 device manager 会触及此注册路径：worker 会向共享管理器传入
// webserver.Null()（见 option.go），并在其聚合 API server 上注册自己的处理程序，
// worker-gateway 也拥有另一个独立的端点。该处理程序的影响范围仅限于单个二进制进程。
func newMetricsHandler(gatherer prometheus.Gatherer) http.Handler {
	return promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{
		ErrorLog:      klog.NewStandardLogger("WARNING"),
		ErrorHandling: promhttp.ContinueOnError,
	})
}
