package worker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"sort"
	"strconv"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	core "k8s.io/api/core/v1"

	"gpustack.ai/gpustack/pkg/utils/httpx"
	"gpustack.ai/gpustack/pkg/worker/scaling/algorithm"
)

var defaultModelDeploymentScalerHTTPClient = &http.Client{
	Transport: httpx.Transport(httpx.TransportOptions().WithoutProxy()),
	Timeout:   samplerPodTimeout,
	CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

// modelDeploymentScalerRankLabels 列出多进程引擎可能用来标记指标序列的 rank 标签。
// rank 0 提供引擎自身的统计值。
var modelDeploymentScalerRankLabels = []string{"tp_rank", "pp_rank", "moe_ep_rank"}

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

// modelDeploymentScalerWindow 记录一个 Pod 从上一次采样到本次采样的变化。
type modelDeploymentScalerWindow struct {
	// state 表示窗口有效、过期或缺少读数。
	state algorithm.SignalState
	// reason 说明为什么这个窗口无法产生有效读数。
	reason string
	// seconds 是窗口持续的秒数。
	seconds float64
	// promptTokens 和 generationTokens 分别是窗口内观测到的输入与生成 token 增量。
	promptTokens     float64
	generationTokens float64
	// cacheMissedPromptTokens 是窗口内未命中缓存的输入 token 数。
	cacheMissedPromptTokens float64
	// ttft 和 itl 分别记录窗口内的首 token 延迟和 token 间延迟直方图。
	ttft modelDeploymentScalerHistogram
	itl  modelDeploymentScalerHistogram
	// gaugesReadable 表示能否读取该 Pod 的请求数瞬时指标。
	gaugesReadable bool
	// tokensReadable 表示能否读取该 Pod 的 token 计数器。
	tokensReadable bool
	// latencyReadable 表示能否读取该 Pod 的延迟直方图。
	latencyReadable bool
}

// modelDeploymentScalerPodRead 保存一轮采样中一个 Pod 的读取结果。
type modelDeploymentScalerPodRead struct {
	// pod 是本次抓取的 Pod。
	pod *core.Pod
	// sample 保存该 Pod 成功返回的样本。
	sample modelDeploymentScalerPodSample
	// err 保存抓取失败的原因。
	err error
}

// scrape 读取单个 Pod 暴露的指标。配置了自定义抓取函数时优先调用它。
func (s *modelDeploymentScalerSampler) scrape(ctx context.Context, pod *core.Pod) ([]byte, error) {
	if s.Scrape != nil {
		return s.Scrape(ctx, pod)
	}
	return s.scrapeHTTP(ctx, pod)
}

// scrapeHTTP 使用采样器的 HTTP 客户端访问引擎 Pod 的指标端点。
func (s *modelDeploymentScalerSampler) scrapeHTTP(ctx context.Context, pod *core.Pod) ([]byte, error) {
	url, ok := modelDeploymentScalerMetricsURL(pod)
	if !ok {
		return nil, fmt.Errorf("the pod advertises no metrics endpoint")
	}
	requestCtx, cancel := context.WithTimeout(ctx, samplerPodTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("build the metrics request: %w", err)
	}
	client := s.HTTPClient
	if client == nil {
		client = defaultModelDeploymentScalerHTTPClient
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("read the metrics endpoint: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("the metrics endpoint returned HTTP %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, samplerMaxBodyBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read the metrics body: %w", err)
	}
	if len(body) > samplerMaxBodyBytes {
		return nil, fmt.Errorf("the metrics body exceeds %d bytes", samplerMaxBodyBytes)
	}
	return body, nil
}

// modelDeploymentScalerMetricsURL 返回引擎 Pod 的指标地址。
//
// 渲染器把引擎实际监听的端口写入 Pod 注解。角色声明的端口可能与之不同，因此从 Pod 读取指标地址。
func modelDeploymentScalerMetricsURL(pod *core.Pod) (string, bool) {
	if pod.Status.PodIP == "" || pod.Annotations["prometheus.io/scrape"] != modelDeploymentEndpointEligibleValue {
		return "", false
	}
	port, err := strconv.ParseInt(pod.Annotations["prometheus.io/port"], 10, 32)
	if err != nil || port < 1 || port > 65535 {
		return "", false
	}
	if !modelDeploymentScalerPortDeclared(pod, int32(port)) {
		return "", false
	}
	scheme := pod.Annotations["prometheus.io/scheme"]
	if scheme == "" {
		scheme = "http"
	}
	if scheme != "http" && scheme != "https" {
		return "", false
	}
	path := pod.Annotations["prometheus.io/path"]
	if path == "" {
		path = "/metrics"
	}
	if path != "/metrics" {
		return "", false
	}
	return scheme + "://" + net.JoinHostPort(pod.Status.PodIP, strconv.FormatInt(port, 10)) + path, true
}

func modelDeploymentScalerPortDeclared(pod *core.Pod, port int32) bool {
	for i := range pod.Spec.Containers {
		for j := range pod.Spec.Containers[i].Ports {
			if pod.Spec.Containers[i].Ports[j].ContainerPort == port {
				return true
			}
		}
	}
	return false
}

// modelDeploymentScalerParseSample 将一个引擎 Pod 返回的指标文本解析为采样器保存的样本。
func modelDeploymentScalerParseSample(
	pod *core.Pod, body []byte, engine string, observedAt time.Time,
) (modelDeploymentScalerPodSample, error) {
	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(bytes.NewReader(body))
	if err != nil {
		return modelDeploymentScalerPodSample{}, fmt.Errorf("parse Prometheus metrics: %w", err)
	}

	sample := modelDeploymentScalerPodSample{
		restartCount: modelDeploymentScalerRestartCount(pod),
		observedAt:   observedAt,
	}
	switch engine {
	case "vllm":
		modelDeploymentScalerReadVLLM(&sample, families)
	case "sglang":
		modelDeploymentScalerReadSGLang(&sample, families)
	}
	return sample, nil
}

// modelDeploymentScalerRestartCount 读取 Pod 中引擎容器的重启次数。
func modelDeploymentScalerRestartCount(pod *core.Pod) int32 {
	for i := range pod.Status.ContainerStatuses {
		if pod.Status.ContainerStatuses[i].Name == modelDeploymentMainContainerName {
			return pod.Status.ContainerStatuses[i].RestartCount
		}
	}
	return 0
}

// modelDeploymentScalerReadVLLM 从一次指标响应中读取 vLLM 的时间序列。
func modelDeploymentScalerReadVLLM(
	sample *modelDeploymentScalerPodSample, families map[string]*dto.MetricFamily,
) {
	running, runningFound := modelDeploymentScalerSum(families, "vllm:num_requests_running", nil)
	waiting, waitingFound := modelDeploymentScalerSum(families, "vllm:num_requests_waiting", nil)
	sample.running = running
	sample.waiting = waiting
	sample.requestsReadable = runningFound && waitingFound
	sample.kvUsage, sample.kvUsageFound = modelDeploymentScalerSum(families, "vllm:kv_cache_usage_perc", nil)

	prompt, promptFound := modelDeploymentScalerSum(families, "vllm:prompt_tokens_total", nil)
	generation, generationFound := modelDeploymentScalerSum(families, "vllm:generation_tokens_total", nil)
	hits, _ := modelDeploymentScalerSum(families, "vllm:prefix_cache_hits_total", nil)
	queries, queriesFound := modelDeploymentScalerSum(families, "vllm:prefix_cache_queries_total", nil)
	sample.promptTokens = prompt
	sample.generationTokens = generation
	sample.cacheHits = hits
	sample.cacheQueries = queries
	sample.cacheQueriesFound = queriesFound
	sample.tokensReadable = promptFound && generationFound

	sample.ttft, sample.ttftFound = modelDeploymentScalerReadHistogram(families, "vllm:time_to_first_token_seconds")
	sample.itl, sample.itlFound = modelDeploymentScalerReadHistogram(families, "vllm:inter_token_latency_seconds")
	sample.latencyReadable = sample.ttftFound && sample.itlFound
}

// modelDeploymentScalerReadSGLang 从一次指标响应中读取 SGLang 的时间序列。
func modelDeploymentScalerReadSGLang(
	sample *modelDeploymentScalerPodSample, families map[string]*dto.MetricFamily,
) {
	running, runningFound := modelDeploymentScalerSum(families, "sglang:num_running_reqs", nil)
	waiting, waitingFound := modelDeploymentScalerSum(families, "sglang:num_queue_reqs", nil)
	sample.running = running
	sample.waiting = waiting
	sample.requestsReadable = runningFound && waitingFound
	sample.kvUsage, sample.kvUsageFound = modelDeploymentScalerSum(families, "sglang:token_usage", nil)
	sample.decodeTransfer, _ = modelDeploymentScalerSum(families, "sglang:num_decode_transfer_queue_reqs", nil)
	// Prefill 已接收但尚未调度的请求在 inflight 队列中。此时普通等待队列仍可能为空，
	// 只读取它会把忙碌的 Prefill 判为空闲。
	sample.prefillInflight, _ = modelDeploymentScalerSum(
		families, "sglang:num_prefill_inflight_queue_reqs", nil,
	)

	prompt, promptFound := modelDeploymentScalerSum(families, "sglang:prompt_tokens_total", nil)
	generation, generationFound := modelDeploymentScalerSum(families, "sglang:generation_tokens_total", nil)
	sample.promptTokens = prompt
	sample.generationTokens = generation
	sample.tokensReadable = promptFound && generationFound

	const effective = "sglang:prefill_effective_tokens_total"
	sample.effectiveInput, sample.effectiveInputFound = modelDeploymentScalerSum(
		families, effective, map[string]string{"mode": "input"},
	)
	device, deviceFound := modelDeploymentScalerSum(families, effective, map[string]string{"mode": "device_hit"})
	host, hostFound := modelDeploymentScalerSum(families, effective, map[string]string{"mode": "host_hit"})
	storage, storageFound := modelDeploymentScalerSum(families, effective, map[string]string{"mode": "storage_hit"})
	sample.effectiveDevice = device
	sample.effectiveHost = host
	sample.effectiveStorage = storage
	sample.effectiveHitsFound = deviceFound || hostFound || storageFound

	sample.ttft, sample.ttftFound = modelDeploymentScalerReadHistogram(families, "sglang:time_to_first_token_seconds")
	sample.itl, sample.itlFound = modelDeploymentScalerReadHistogram(families, "sglang:inter_token_latency_seconds")
	sample.latencyReadable = sample.ttftFound && sample.itlFound
}

// modelDeploymentScalerSum 对同一指标族中符合条件的序列求和。
//
// 多个 rank 会重复上报同一组计数器，因此只计入零号 rank。单进程引擎可能省略 rank 标签，这类序列也纳入求和。
func modelDeploymentScalerSum(
	families map[string]*dto.MetricFamily, name string, labels map[string]string,
) (float64, bool) {
	total := 0.0
	found := false
	for _, candidate := range []string{name, name + "_total"} {
		family := families[candidate]
		if family == nil {
			continue
		}
		for _, metric := range family.Metric {
			if !modelDeploymentScalerRankZero(metric) {
				continue
			}
			if !modelDeploymentScalerLabelsMatch(metric, labels) {
				continue
			}
			value := math.NaN()
			if metric.Gauge != nil {
				value = metric.Gauge.GetValue()
			} else if metric.Counter != nil {
				value = metric.Counter.GetValue()
			}
			if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
				continue
			}
			total += value
			found = true
		}
		if found {
			break
		}
	}
	return total, found
}

// modelDeploymentScalerRankZero 判断一条序列是否属于应采集的零号 rank。
//
// 没有 rank 标签时保留该序列，否则单进程引擎的指标会全部丢失。带 rank 标签时仅保留零号 rank，
// 以免引擎计数被重复累加。
func modelDeploymentScalerRankZero(metric *dto.Metric) bool {
	for _, label := range metric.Label {
		for _, rank := range modelDeploymentScalerRankLabels {
			if label.GetName() == rank && label.GetValue() != "0" {
				return false
			}
		}
	}
	return true
}

// modelDeploymentScalerLabelsMatch 检查一条序列是否包含全部指定的标签及其值。
func modelDeploymentScalerLabelsMatch(metric *dto.Metric, want map[string]string) bool {
	for name, value := range want {
		matched := false
		for _, label := range metric.Label {
			if label.GetName() == name && label.GetValue() == value {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

// modelDeploymentScalerReadHistogram 从指标响应中读取一项延迟直方图。
//
// 将通过 rank 过滤的序列按桶相加，还原被不同标签组合拆开的同一项直方图。
func modelDeploymentScalerReadHistogram(
	families map[string]*dto.MetricFamily, name string,
) (modelDeploymentScalerHistogram, bool) {
	family := families[name]
	if family == nil || family.GetType() != dto.MetricType_HISTOGRAM {
		return modelDeploymentScalerHistogram{}, false
	}
	histogram := modelDeploymentScalerHistogram{buckets: map[float64]float64{}}
	found := false
	for _, metric := range family.Metric {
		if !modelDeploymentScalerRankZero(metric) {
			continue
		}
		if metric.Histogram == nil {
			continue
		}
		histogram.sum += metric.Histogram.GetSampleSum()
		histogram.count += float64(metric.Histogram.GetSampleCount())
		for _, bucket := range metric.Histogram.Bucket {
			histogram.buckets[bucket.GetUpperBound()] += float64(bucket.GetCumulativeCount())
		}
		found = true
	}
	return histogram, found
}

// modelDeploymentScalerWindowOf 计算同一 Pod 相邻两次样本之间的变化。
func modelDeploymentScalerWindowOf(
	previous *modelDeploymentScalerPodSample,
	current modelDeploymentScalerPodSample,
	now time.Time,
	freshness time.Duration,
) modelDeploymentScalerWindow {
	window := modelDeploymentScalerWindow{
		state:           algorithm.SignalMissing,
		reason:          "the window over which the rate is read is not established yet",
		gaugesReadable:  current.requestsReadable,
		tokensReadable:  current.tokensReadable,
		latencyReadable: current.latencyReadable,
	}
	if previous == nil {
		return window
	}
	if current.restartCount > previous.restartCount {
		window.reason = "the engine container restarted since the last sample"
		return window
	}
	if modelDeploymentScalerCounterReset(*previous, current) {
		window.reason = "a counter went down since the last sample"
		return window
	}
	elapsed := now.Sub(previous.observedAt)
	if elapsed <= 0 {
		window.reason = "the last sample is not older than this one"
		return window
	}
	state := algorithm.SignalFresh
	if elapsed > freshness {
		state = algorithm.SignalStale
		window.reason = "the last sample is older than the freshness bound"
	}
	window.state = state
	window.seconds = elapsed.Seconds()
	window.promptTokens = current.promptTokens - previous.promptTokens
	window.generationTokens = current.generationTokens - previous.generationTokens
	window.cacheMissedPromptTokens = modelDeploymentScalerCacheMissedPromptTokens(*previous, current)
	var ttftEstablished, itlEstablished bool
	window.ttft, ttftEstablished = modelDeploymentScalerHistogramDelta(previous.ttft, current.ttft)
	window.itl, itlEstablished = modelDeploymentScalerHistogramDelta(previous.itl, current.itl)
	if !ttftEstablished || !itlEstablished {
		// 这两份直方图无法确定窗口内的增量。本次样本仍可作为下一窗口的起点，后续窗口会独立判定。
		window.state = algorithm.SignalMissing
		window.reason = "the latency histograms do not establish the change over the window"
	}
	return window
}

// modelDeploymentScalerCounterReset 检查本次样本是否有计数器小于上次读数。
//
// 累计计数回落意味着两次读数跨越了计数器重置，可能来自不同的引擎进程。用它们计算速率会产生虚假的负载变化。
func modelDeploymentScalerCounterReset(
	previous, current modelDeploymentScalerPodSample,
) bool {
	switch {
	case current.promptTokens < previous.promptTokens:
		return true
	case current.generationTokens < previous.generationTokens:
		return true
	case current.cacheHits < previous.cacheHits:
		return true
	case current.cacheQueriesFound && previous.cacheQueriesFound &&
		current.cacheQueries < previous.cacheQueries:
		return true
	case current.effectiveInput < previous.effectiveInput:
		return true
	case current.effectiveDevice < previous.effectiveDevice:
		return true
	case current.effectiveHost < previous.effectiveHost:
		return true
	case current.effectiveStorage < previous.effectiveStorage:
		return true
	case current.ttft.count < previous.ttft.count:
		return true
	case current.itl.count < previous.itl.count:
		return true
	case current.ttft.sum < previous.ttft.sum:
		return true
	case current.itl.sum < previous.itl.sum:
		return true
	}
	return false
}

// modelDeploymentScalerCacheMissedPromptTokens 计算窗口内未命中缓存的输入 token 数。
//
// 引擎提供有效预填充计数时，使用它记录的实际预填充量。缺少这项统计时，从输入 token 数中扣除缓存命中数。
//
// 保留原始命中数。用查询数限制命中数会抬高未命中量和推算速率，可能触发不必要的扩容。
// 查询计数用于记录缓存活动。
func modelDeploymentScalerCacheMissedPromptTokens(
	previous, current modelDeploymentScalerPodSample,
) float64 {
	if current.effectiveInputFound && previous.effectiveInputFound {
		return math.Max(0, current.effectiveInput-previous.effectiveInput)
	}
	hits := current.cacheHits - previous.cacheHits
	if current.effectiveHitsFound && previous.effectiveHitsFound {
		hits = (current.effectiveDevice - previous.effectiveDevice) +
			(current.effectiveHost - previous.effectiveHost) +
			(current.effectiveStorage - previous.effectiveStorage)
	}
	return math.Max(0, (current.promptTokens-previous.promptTokens)-hits)
}

// modelDeploymentScalerHistogramDelta 计算两份直方图之间的增量，并报告能否可靠地得出该增量。
//
// 新出现的桶没有可供相减的基线。本次读数包含引擎启动以来的全部观测，把基线当作零会高估窗口内的数量。
//
// 桶的增量不能超过窗口总数。桶按上界累积，因此较小上界的桶也不能多于较大上界的桶。
//
// 两次读取的桶边界必须一致，才能比较同一范围内的观测数。
func modelDeploymentScalerHistogramDelta(
	previous, current modelDeploymentScalerHistogram,
) (modelDeploymentScalerHistogram, bool) {
	delta := modelDeploymentScalerHistogram{
		sum:     current.sum - previous.sum,
		count:   current.count - previous.count,
		buckets: map[float64]float64{},
	}
	for bound, count := range current.buckets {
		baseline, carried := previous.buckets[bound]
		if !carried {
			return modelDeploymentScalerHistogram{}, false
		}
		if count < baseline {
			// 累计桶的计数应单调递增。读数回落说明两次采样跨越了计数器重置。
			return modelDeploymentScalerHistogram{}, false
		}
		delta.buckets[bound] = count - baseline
	}
	if len(previous.buckets) != len(current.buckets) {
		return modelDeploymentScalerHistogram{}, false
	}
	bounds := make([]float64, 0, len(delta.buckets))
	for bound := range delta.buckets {
		bounds = append(bounds, bound)
	}
	sort.Float64s(bounds)
	for i, bound := range bounds {
		if delta.buckets[bound] > delta.count {
			return modelDeploymentScalerHistogram{}, false
		}
		if i > 0 && delta.buckets[bound] < delta.buckets[bounds[i-1]] {
			return modelDeploymentScalerHistogram{}, false
		}
	}
	return delta, true
}

// modelDeploymentScalerHistogramMerge 将一份直方图累加到另一份直方图中。
func modelDeploymentScalerHistogramMerge(
	into, other modelDeploymentScalerHistogram,
) modelDeploymentScalerHistogram {
	merged := modelDeploymentScalerHistogram{
		sum:     into.sum + other.sum,
		count:   into.count + other.count,
		buckets: map[float64]float64{},
	}
	for bound, count := range into.buckets {
		merged.buckets[bound] = count
	}
	for bound, count := range other.buckets {
		merged.buckets[bound] += count
	}
	return merged
}

// modelDeploymentScalerQuantileMilliseconds 从直方图求指定分位数的延迟，单位为毫秒。
//
// 当目标分位数落在可确定范围的桶内时，按桶的读数计算。
//
// 缺少桶边界或目标分位数落入正无穷桶时，无法从观测中确定该分位数。此时返回不可用，以免均值被当作分位数。
func modelDeploymentScalerQuantileMilliseconds(
	histogram modelDeploymentScalerHistogram, quantile float64,
) (float64, bool) {
	if histogram.count <= 0 {
		return 0, false
	}
	bounds := make([]float64, 0, len(histogram.buckets))
	for bound := range histogram.buckets {
		bounds = append(bounds, bound)
	}
	sort.Float64s(bounds)
	target := quantile * histogram.count
	for _, bound := range bounds {
		if math.IsInf(bound, 1) {
			break
		}
		if histogram.buckets[bound] >= target {
			return bound * 1000, true
		}
	}
	return 0, false
}

// modelDeploymentScalerWorseState 取较差的信号状态，用于把各 Pod 的读数合并为角色状态。
//
// 缺失比过期更严重：过期读数至少记录了一次过去的观测。
func modelDeploymentScalerWorseState(
	states ...algorithm.SignalState,
) algorithm.SignalState {
	worst := algorithm.SignalFresh
	for _, state := range states {
		switch state {
		case algorithm.SignalMissing:
			return algorithm.SignalMissing
		case algorithm.SignalStale:
			worst = algorithm.SignalStale
		}
	}
	return worst
}
