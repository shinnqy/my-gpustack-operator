package worker

import (
	"net/http"
	"time"

	"gpustack.ai/gpustack/pkg/utils/httpx"
)

var defaultModelDeploymentScalerHTTPClient = &http.Client{
	Transport: httpx.Transport(httpx.TransportOptions().WithoutProxy()),
	Timeout:   samplerPodTimeout,
	CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

// modelDeploymentScalerPodSample 表示单个引擎 Pod 在一次采样轮次中上报的数据。
type modelDeploymentScalerPodSample struct {
	// restartCount 指明这些指标数据属于哪一次进程实例。
	restartCount int32
	// observedAt 是本轮读取该 Pod 指标的时间。
	observedAt time.Time

	// running、waiting 和 decodeTransfer 是请求数仪表盘指标（gauge）。
	running        float64
	waiting        float64
	decodeTransfer float64
	// prefillInflight 是 prefill 角色自身的积压量：即已接收但尚未调度的请求数。
	// 它是一个独立队列，常规的 waiting 指标并不统计这部分请求。
	prefillInflight float64
	// kvUsage 是引擎的缓存使用率，表示其已用缓存占总缓存的比例。
	kvUsage float64
	// kvUsageFound 表示是否上报了缓存使用率指标。
	kvUsageFound bool
	// requestsReadable 表示请求数仪表盘指标（gauge）是否可读。
	requestsReadable bool

	// promptTokens、generationTokens 和 cacheHits 是 token 计数器（counter）。
	promptTokens     float64
	generationTokens float64
	cacheHits        float64
	// cacheQueries 表示向前缀缓存查询 prompt 的次数。
	cacheQueries float64
	// cacheQueriesFound 表示是否上报了前缀缓存查询次数指标。
	cacheQueriesFound bool
	// effectiveInput 是引擎上报的实际执行了 prefill 的 prompt token 数。
	effectiveInput float64
	// effectiveInputFound 表示是否上报了该计数值。
	effectiveInputFound bool
	// effectiveDevice、effectiveHost 和 effectiveStorage 是缓存命中计数。
	effectiveDevice  float64
	effectiveHost    float64
	effectiveStorage float64
	// effectiveHitsFound 表示是否上报了任何缓存命中计数。
	effectiveHitsFound bool
	// tokensReadable 表示 token 计数器（counter）是否可读。
	tokensReadable bool

	// ttft 和 itl 是延迟直方图（histogram）。
	ttft modelDeploymentScalerHistogram
	itl  modelDeploymentScalerHistogram
	// ttftFound 和 itlFound 表示是否上报了对应的延迟直方图指标。
	ttftFound bool
	itlFound  bool
	// latencyReadable 表示两个延迟直方图是否均可读。
	latencyReadable bool
}

// modelDeploymentScalerHistogram 表示读取到的单个延迟直方图。
type modelDeploymentScalerHistogram struct {
	// sum 和 count 是直方图自身的总和与计数。
	sum   float64
	count float64
	// buckets 保存直方图导出的每个上界对应的累积计数。
	buckets map[float64]float64
}
