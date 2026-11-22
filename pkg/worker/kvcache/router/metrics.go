package router

import "fmt"

var metricsByEngine = map[string]Metrics{
	"vllm": {
		Port:               8000,
		QueuedRequests:     "vllm:num_requests_waiting",
		RunningRequests:    "vllm:num_requests_running",
		KVCacheUtilization: "vllm:kv_cache_usage_perc",
		CacheInfo:          "vllm:cache_config_info",
	},
	// SGLang 从 v0.5.11 起以两个 gauge 指标导出其 KV cache 容量；更早的版本则将其作为标签
	// 放在 sglang:cache_config_info 上，这也是 picker 内置 SGLang 条目仍在读取的指标
	// （`llm-d/llm-d-router@v0.10.0:pkg/epp/framework/plugins/datalayer/extractor/metrics/factories.go:98-107`，
	// 上游在该版本之后将其替换为这两个 gauge）。v0.5.11 之前的版本两个 gauge 都不导出，
	// 因此其容量会读作 0，且每次抓取都会记录一条提取错误。
	"sglang": {
		Port:               8000,
		QueuedRequests:     "sglang:num_queue_reqs",
		RunningRequests:    "sglang:num_running_reqs",
		KVCacheUtilization: "sglang:token_usage",
		CacheBlockSize:     "sglang:page_size",
		CacheNumBlocks:     "sglang:num_pages",
	},
}

// MetricsForEngine 返回托管路由器所需的服务指标。
func MetricsForEngine(engine string) (Metrics, error) {
	metrics := metricsByEngine[engine]
	metrics.Engine = engine
	for _, required := range []struct {
		name  string
		value string
	}{
		{name: "queued requests", value: metrics.QueuedRequests},
		{name: "running requests", value: metrics.RunningRequests},
		{name: "KV cache utilization", value: metrics.KVCacheUtilization},
	} {
		if required.value == "" {
			return Metrics{}, fmt.Errorf("metric %q required by router %q is unavailable on engine %q",
				required.name, LLMD, engine)
		}
	}
	if metrics.Port < 1 {
		return Metrics{}, fmt.Errorf("metrics port required by router %q is unavailable on engine %q",
			LLMD, engine)
	}

	return metrics, nil
}
