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

// modelDeploymentScalerPodSample is what one engine Pod reported in one pass.
type modelDeploymentScalerPodSample struct {
	// restartCount says which process the numbers belong to.
	restartCount int32
	// observedAt is when the pass read the Pod.
	observedAt time.Time

	// running, waiting and decodeTransfer are the request gauges.
	running        float64
	waiting        float64
	decodeTransfer float64
	// prefillInflight is the prefill role's own backlog: the requests it has accepted and not yet
	// scheduled. It is a queue of its own, and the ordinary waiting gauge does not count it.
	prefillInflight float64
	// kvUsage is the engine's cache usage, as a fraction of the cache it holds.
	kvUsage float64
	// kvUsageFound says whether the cache usage was reported at all.
	kvUsageFound bool
	// requestsReadable says whether the request gauges were readable.
	requestsReadable bool

	// promptTokens, generationTokens and cacheHits are the token counters.
	promptTokens     float64
	generationTokens float64
	cacheHits        float64
	// cacheQueries is how many times the prefix cache was asked for a prompt.
	cacheQueries float64
	// cacheQueriesFound says whether the prefix cache was asked at all.
	cacheQueriesFound bool
	// effectiveInput is the prompt token count an engine reports as actually prefilled.
	effectiveInput float64
	// effectiveInputFound says whether that count was reported at all.
	effectiveInputFound bool
	// effectiveDevice, effectiveHost and effectiveStorage are the cache hit counts.
	effectiveDevice  float64
	effectiveHost    float64
	effectiveStorage float64
	// effectiveHitsFound says whether any cache hit count was reported.
	effectiveHitsFound bool
	// tokensReadable says whether the token counters were readable.
	tokensReadable bool

	// ttft and itl are the latency histograms.
	ttft modelDeploymentScalerHistogram
	itl  modelDeploymentScalerHistogram
	// ttftFound and itlFound say whether the latency histograms were reported.
	ttftFound bool
	itlFound  bool
	// latencyReadable says whether both latency histograms were readable.
	latencyReadable bool
}

// modelDeploymentScalerHistogram is one latency histogram as it was read.
type modelDeploymentScalerHistogram struct {
	// sum and count are the histogram's own totals.
	sum   float64
	count float64
	// buckets holds the cumulative count of every upper bound the histogram exports.
	buckets map[float64]float64
}
