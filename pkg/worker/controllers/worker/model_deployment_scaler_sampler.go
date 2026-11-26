package worker

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	core "k8s.io/api/core/v1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/utils/datax"
	"gpustack.ai/gpustack/pkg/worker/scaling/algorithm"
	"gpustack.ai/gpustack/pkg/worker/scaling/priors"
)

// modelDeploymentScalerMaxStabilizationSamples 是每个角色的稳定窗口最多保留的样本数。
//
// 稳定窗口需要整体读取，限制样本数可以控制开销，即使实际采样比预期更密集。
const modelDeploymentScalerMaxStabilizationSamples = 256

// 单次采样周期的边界约束。
const (
	// maxSamplerPodsPerRole 是单次采样对单个角色最多查询的就绪且可响应引擎 Pod 数量。
	//
	// 它是上限而非目标值：若某个角色的就绪 Pod 数超过此上限，会被报告为覆盖不完整，
	// 因为仅从上限以内 Pod 收集的数据不足以代表整个角色。
	maxSamplerPodsPerRole = 64

	// maxSamplerScrapeConcurrency 是单次采样周期内并发抓取的 Pod 数量上限。
	maxSamplerScrapeConcurrency = 32

	// samplerPodTimeout 限制单个 Pod 抓取的超时时间。
	samplerPodTimeout = 2 * time.Second

	// samplerPassTimeout 限制遍历所有角色 Pod 的整轮采样的超时时间。
	samplerPassTimeout = 8 * time.Second

	// samplerMaxBodyBytes 限制单次抓取允许读取的响应体大小上限。
	samplerMaxBodyBytes = 1 << 20

	// modelDeploymentScalerSamplerDefaultIntervalSeconds 是当 scaler 的 spec 未指定
	// 采样间隔时默认采用的采样间隔（秒）。
	modelDeploymentScalerSamplerDefaultIntervalSeconds = 15
)

// modelDeploymentScalerScrapeFunc 读取单个引擎 Pod 暴露的指标文本。
//
// 它接收 Pod 对象而非直接接收地址，因为允许从 Pod 读取什么取决于 Pod 本身：
// Pod 未对外宣告的地址不应被读取。
type modelDeploymentScalerScrapeFunc func(ctx context.Context, pod *core.Pod) ([]byte, error)

// modelDeploymentScalerSampler 读取 scaler 目标的引擎指标，并将其转换为每个角色的
// 单条观测结果。
//
// 它为每个 scaler 维护独立的采样环形缓冲区，而非复用指标子资源的窗口：scaler
// 需要的是信号在其自身采样间隔内的变化速率，而指标子资源针对不同窗口回答的是
// 另一类问题。
type modelDeploymentScalerSampler struct {
	// HTTPClient 用于请求引擎 Pod 的指标端点。
	//
	// 它是默认抓取函数使用的客户端；将其设为字段既便于部署侧限制传输层行为，也便于
	// 测试无需启动服务端即可返回采样结果。
	HTTPClient *http.Client

	// Scrape 读取单个引擎 Pod 暴露的指标文本。
	//
	// 设置该字段后会替代底层 HTTP 传输，以便测试直接用固定指标文本驱动一轮采样。
	// 它接收 Pod 并返回该 Pod 响应的指标内容。
	Scrape modelDeploymentScalerScrapeFunc

	// Clock 读取当前时间。
	//
	// 它是采样器自身的时钟，当调用方未提供本轮采样的评估时间时使用。
	Clock func() time.Time

	mu sync.Mutex
	// rings 保存采样器为每个 scaler 记录的状态，以 scaler 的 UID 为键。
	rings map[string]*modelDeploymentScalerRing
	// passing 记录当前正在执行采样的目标，以目标的 UID 为键。
	passing map[string]bool
	// lastPass 记录上一次对目标开始采样的时间，以目标的 UID 为键。
	//
	// 它构成了目标采样频率的下限间隔：scaler 每次调谐都会请求观测数据，而一轮采样
	// 会给引擎 Pod 带来抓取负载，因此需要限制频率。
	lastPass map[string]time.Time
}

// modelDeploymentScalerRing 保存采样器为单个 scaler 保留的记忆状态。
type modelDeploymentScalerRing struct {
	// pods 保存每个引擎 Pod 的最近一次采样，以 Pod 的 UID 为键。
	pods map[string]modelDeploymentScalerPodSample
	// roles 保存上一轮采样获取的各角色信息，以角色名称为键。
	roles map[string]modelDeploymentScalerRoleSample
	// windows 保存每个角色的滑动窗口，以角色名称为键。
	windows map[string]*modelDeploymentScalerRoleWindow
	// quantile 是已记录观测数据所对应的延迟分位数。
	//
	// 它是观测数据自身携带的分位数；由于不同分位数下的观测反映的是不同的分布，
	// 当分位数发生变化时，已记录的观测数据会被丢弃。
	quantile float64
	// targetKey 是用于索引该 scaler 目标的键。
	//
	// 它标识的是目标而非 scaler 本身，因为采样轮次按目标计数：指向同一目标的两个
	// scaler 会共享该目标的最小采样间隔限制。
	targetKey string
}

// newModelDeploymentScalerRing 创建空的采样缓存。
func newModelDeploymentScalerRing() *modelDeploymentScalerRing {
	return &modelDeploymentScalerRing{
		pods:    map[string]modelDeploymentScalerPodSample{},
		roles:   map[string]modelDeploymentScalerRoleSample{},
		windows: map[string]*modelDeploymentScalerRoleWindow{},
	}
}

// modelDeploymentScalerRoleWindow 是单个角色的滑动窗口。
//
// 它保存 scaler 在各采样周期之间所需的跨周期状态：稳定期历史和需求历史都是
// 基于角色最近样本的窗口，每次采样按窗口读取并追加一个新样本。将其保留在内存中，
// 可避免处于稳态的 scaler 在每个周期都向 apiserver 写入状态。
type modelDeploymentScalerRoleWindow struct {
	// stabilization 保存该角色的稳定期样本。
	stabilization *datax.RingBuffer[workercore.ModelDeploymentScalerStabilizationSample]
	// predictor 是该角色的预测器状态。
	predictor *algorithm.PredictorState
	// seeded 表示该窗口是否已从 scaler 的 status 中初始化填充。
	seeded bool
}

// modelDeploymentScalerRoleSample 表示采样器在一轮采样后掌握的单个角色状态。
//
// 它携带了观测值本身无法表达的背景信息，包括：信号未达到预期读数的原因、
// 计算速率所依据的窗口时长，以及该角色引擎容器的重启次数。能够报告等待指标原因
// 的 scaler，才能让调用方区分信号缺失与指标损坏。
type modelDeploymentScalerRoleSample struct {
	// Observation 是角色各信号所反映的观测值。
	Observation algorithm.RoleObservation
	// Reasons 说明各信号未能给出预期读数的原因。
	Reasons []string
	// WindowSeconds 是读取该角色各速率指标所使用的最短窗口时长（秒）。
	WindowSeconds float64
	// ContainerRestarts 是该角色所有引擎容器中单容器的最大重启次数。
	ContainerRestarts int32
	// ReadyPods 是在本轮采样中成功响应的就绪引擎 Pod 数量。
	ReadyPods int
}

// SampleRoleMetrics 抓取目标部署的引擎 Pod，为每个角色返回一份观测。
//
// 调用方提供目标部署、它持有的 Pod 和本轮计算时间，采样器将这些 Pod 报告的指标汇总为角色观测。
//
// 同一目标已有正在执行的采样时，返回上一轮的观测，避免多轮同时抓取同一批 Pod。
func (s *modelDeploymentScalerSampler) SampleRoleMetrics(
	ctx context.Context,
	scaler *workercore.ModelDeploymentScaler,
	md *workercore.ModelDeployment,
	pods []core.Pod,
	now time.Time,
) map[string]algorithm.RoleObservation {
	if scaler == nil || md == nil {
		return nil
	}
	if now.IsZero() {
		now = s.now()
	}

	scalerKey := string(scaler.UID)
	targetKey := string(md.UID)
	if targetKey == "" {
		targetKey = scalerKey
	}

	// 延迟分位数来自 Scaler 规格，省略时采用第 95 百分位。
	quantile, quantileErr := ModelDeploymentScalerLatencyQuantile(&scaler.Spec.Algorithm)

	if !s.beginPass(targetKey, now, s.samplingInterval(scaler)) {
		return s.cachedObservations(scalerKey, now, s.freshnessBound(scaler), quantile)
	}
	defer s.endPass(targetKey)

	return s.observe(ctx, scalerKey, targetKey, scaler, md, pods, now, quantile, quantileErr)
}

// Forget 清除采样器中与指定 scaler 相关的全部缓存状态。
//
// 它在 scaler 被删除时调用，因为环形缓冲区以 scaler 的 UID 为键：同名重建的
// scaler 拥有不同的 UID，会使用独立的新缓冲区，旧 scaler 留下的状态只会占用内存。
//
// 目标的最小采样间隔限制会予以保留，因为该限制归属于目标而非 scaler：指向同一
// 目标的第二个 scaler 仍受此采样频率下限的约束。
func (s *modelDeploymentScalerSampler) Forget(scaler *workercore.ModelDeploymentScaler) {
	if scaler == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := string(scaler.UID)
	ring, ok := s.rings[key]
	if !ok {
		return
	}
	targetKey := ring.targetKey
	delete(s.rings, key)
	if targetKey == "" {
		return
	}
	for _, other := range s.rings {
		if other.targetKey == targetKey {
			return
		}
	}
	delete(s.lastPass, targetKey)
}

// observe 抓取目标部署的 Pod，并为每个声明的角色生成观测。
func (s *modelDeploymentScalerSampler) observe(
	ctx context.Context,
	scalerKey string,
	targetKey string,
	scaler *workercore.ModelDeploymentScaler,
	md *workercore.ModelDeployment,
	pods []core.Pod,
	now time.Time,
	quantile float64,
	quantileErr error,
) map[string]algorithm.RoleObservation {
	engine := modelDeploymentScalerEngine(md)
	ready := s.readyEnginePods(md, pods)
	freshness := s.freshnessBound(scaler)
	previous := s.snapshot(scalerKey)

	observations := make(map[string]algorithm.RoleObservation, len(md.Spec.Roles))
	samples := make(map[string]modelDeploymentScalerPodSample, len(ready))
	roles := make(map[string]modelDeploymentScalerRoleSample, len(md.Spec.Roles))
	for i := range md.Spec.Roles {
		role := &md.Spec.Roles[i]
		roleCtx, cancel := context.WithTimeout(ctx, samplerPassTimeout)
		observation, read, sample := s.observeRole(
			roleCtx, role, engine, ready[role.Name], previous, now, freshness, quantile, quantileErr,
		)
		cancel()
		observations[role.Name] = observation
		roles[role.Name] = sample
		for uid, podSample := range read {
			samples[uid] = podSample
		}
	}
	s.commit(scalerKey, targetKey, samples, roles, quantile)
	return observations
}

// observeRole 汇总本角色各 Pod 在本轮返回的指标。
func (s *modelDeploymentScalerSampler) observeRole(
	ctx context.Context,
	role *workercore.ModelDeploymentRole,
	engine string,
	ready []*core.Pod,
	previous map[string]modelDeploymentScalerPodSample,
	now time.Time,
	freshness time.Duration,
	quantile float64,
	quantileErr error,
) (algorithm.RoleObservation, map[string]modelDeploymentScalerPodSample, modelDeploymentScalerRoleSample) {
	observation := algorithm.RoleObservation{
		Role:            role.Name,
		Kind:            modelDeploymentScalerRoleKind(role.Kind),
		CurrentReplicas: role.Replicas,
		ReadyReplicas:   int32(len(ready)),
		ObservedAt:      now,
		GaugeState:      algorithm.SignalMissing,
		ThroughputState: algorithm.SignalMissing,
		LatencyState:    algorithm.SignalMissing,
		// 延迟初始标记为未测量。尚未处理请求的角色没有延迟样本，把它当作零延迟会误导算法。
		TTFTUnmeasured: true,
		ITLUnmeasured:  true,
	}
	sample := modelDeploymentScalerRoleSample{ReadyPods: len(ready)}
	var reasons []string
	if len(ready) == 0 {
		sample.Reasons = []string{"the role has no ready engine pod"}
		return observation, nil, sample
	}

	// 就绪 Pod 数超过上限时，只抓取上限以内的 Pod，并标记为部分覆盖，因为这些读数只能描述角色的一部分。
	capped := len(ready) > maxSamplerPodsPerRole
	asked := ready
	if capped {
		asked = ready[:maxSamplerPodsPerRole]
		reasons = append(reasons, fmt.Sprintf(
			"the role has %d ready engine pods and the sampler asks about %d of them",
			len(ready), maxSamplerPodsPerRole,
		))
	}

	reads, passErr := s.scrapePods(ctx, asked, engine, now)
	if passErr != nil {
		reasons = append(reasons, fmt.Sprintf(
			"the engine pods could not all be read: %v", passErr,
		))
	}
	samples := make(map[string]modelDeploymentScalerPodSample, len(reads))
	windows := make([]modelDeploymentScalerWindow, 0, len(reads))
	for i := range reads {
		read := &reads[i]
		if read.err != nil {
			continue
		}
		samples[string(read.pod.UID)] = read.sample

		var prior *modelDeploymentScalerPodSample
		if previousSample, ok := previous[string(read.pod.UID)]; ok {
			prior = &previousSample
		}
		windows = append(windows, modelDeploymentScalerWindowOf(prior, read.sample, now, freshness))
	}

	ttft := modelDeploymentScalerHistogram{buckets: map[float64]float64{}}
	itl := modelDeploymentScalerHistogram{buckets: map[float64]float64{}}
	kvUsage, kvUsageCount := 0.0, 0
	gaugeState := algorithm.SignalFresh
	throughputState := algorithm.SignalFresh
	latencyState := algorithm.SignalFresh
	idle := true
	for i := range reads {
		read := &reads[i]
		if read.err != nil {
			gaugeState = algorithm.SignalMissing
			throughputState = algorithm.SignalMissing
			latencyState = algorithm.SignalMissing
			idle = false
			reasons = append(reasons, fmt.Sprintf(
				"the engine pod %s did not answer: %v", read.pod.Name, read.err,
			))
			continue
		}
		observation.RunningRequests += read.sample.running
		observation.WaitingRequests += read.sample.waiting
		// 将角色自己的队列加到等待数中：Decode 可能在等待 Prefill 交接的请求，
		// Prefill 则有已接收但尚未调度的请求。只看普通等待队列会低估负载。
		if observation.Kind == algorithm.DecodeRole {
			observation.WaitingRequests += read.sample.decodeTransfer
		}
		if observation.Kind == algorithm.PrefillRole {
			observation.WaitingRequests += read.sample.prefillInflight
		}
		if read.sample.kvUsageFound {
			kvUsage += read.sample.kvUsage
			kvUsageCount++
		}
	}
	if kvUsageCount > 0 {
		observation.KVCacheUsage = kvUsage / float64(kvUsageCount)
	}

	for i := range windows {
		window := &windows[i]
		if !window.gaugesReadable {
			gaugeState = algorithm.SignalMissing
			idle = false
			reasons = append(reasons, "an engine pod did not report its request gauges")
		}
		if !window.tokensReadable {
			throughputState = algorithm.SignalMissing
			idle = false
			reasons = append(reasons, "an engine pod did not report its token counters")
		}
		if !window.latencyReadable {
			// 新副本尚未收到请求时可能不导出延迟直方图。保留延迟信号的缺失状态，
			// 供依赖延迟的算法判断。其他算法仍可让空闲角色缩回下限。
			latencyState = algorithm.SignalMissing
			reasons = append(reasons, "an engine pod did not report its latency histograms")
		}
		switch window.state {
		case algorithm.SignalMissing:
			throughputState = algorithm.SignalMissing
			latencyState = algorithm.SignalMissing
			idle = false
			reasons = append(reasons, window.reason)
		case algorithm.SignalStale:
			throughputState = modelDeploymentScalerWorseState(throughputState, window.state)
			latencyState = modelDeploymentScalerWorseState(latencyState, window.state)
			idle = false
			reasons = append(reasons, window.reason)
		}
		if window.seconds <= 0 {
			idle = false
		}
		if window.promptTokens != 0 || window.generationTokens != 0 {
			idle = false
		}
		if window.ttft.count != 0 || window.itl.count != 0 {
			idle = false
		}
		ttft = modelDeploymentScalerHistogramMerge(ttft, window.ttft)
		itl = modelDeploymentScalerHistogramMerge(itl, window.itl)
		if window.seconds > 0 {
			observation.PromptTokensPerSecond += window.cacheMissedPromptTokens / window.seconds
			observation.GenerationTokensPerSecond += window.generationTokens / window.seconds
		}
	}

	// 当请求数为零且窗口内的计数都没有增长时，角色确实空闲。标为 Idle 后，算法可以将副本数降至下限。
	if idle && observation.RunningRequests == 0 && observation.WaitingRequests == 0 {
		observation.GaugeState = algorithm.SignalIdle
		observation.ThroughputState = algorithm.SignalIdle
		// 延迟保留独立的信号状态。空闲角色可能没有延迟样本，依赖延迟的算法仍需等待。
		// 按请求数决策的算法可以使用空闲读数。
		if latencyState == algorithm.SignalFresh {
			observation.LatencyState = algorithm.SignalIdle
		} else {
			observation.LatencyState = latencyState
		}
		observation.PromptTokensPerSecond = 0
		observation.GenerationTokensPerSecond = 0
		observation.TTFTMilliseconds = 0
		observation.ITLMilliseconds = 0
	} else {
		observation.GaugeState = gaugeState
		observation.ThroughputState = throughputState
		// 首 token 延迟和 token 间延迟均依据直方图读取。即使缺少 token 计数器，
		// 引擎已上报的延迟仍然可用。
		if latencyState == algorithm.SignalFresh {
			ttftValue, ttftReadable := modelDeploymentScalerQuantileMilliseconds(ttft, quantile)
			itlValue, itlReadable := modelDeploymentScalerQuantileMilliseconds(itl, quantile)
			switch {
			case quantileErr != nil:
				// 无法解析所需的分位数时，保留延迟未测量标记，避免填入没有依据的零值。
				latencyState = algorithm.SignalMissing
				reasons = append(reasons, quantileErr.Error())
			default:
				// Prefill 关注首 token 延迟，Decode 关注 token 间延迟。缺少另一项指标时，
				// 仍可使用该角色需要的读数。
				ttftReading, itlReading, held := modelDeploymentScalerLatencyReadings(
					observation.Kind, ttftValue, ttftReadable, itlValue, itlReadable,
				)
				observation.TTFTMilliseconds = ttftReading
				observation.ITLMilliseconds = itlReading
				observation.TTFTUnmeasured = !ttftReadable
				observation.ITLUnmeasured = !itlReadable
				if !held {
					// 直方图无法确定本角色要求的分位数，因此保留信号缺失状态，
					// 避免把未测量的延迟记为新鲜的零读数。
					latencyState = algorithm.SignalMissing
					reasons = append(reasons, fmt.Sprintf(
						"the %s cannot represent the %s quantile",
						modelDeploymentScalerLatencyMetricName(observation.Kind),
						priors.FormatLatencyQuantile(quantile),
					))
				}
			}
		}
		observation.LatencyState = latencyState
	}

	// 已成功采样且确认空闲的角色可以视为完整覆盖。若仅因没有延迟样本阻止缩容，
	// 空闲角色就会一直停在副本下限之上。
	observation.CompleteCoverage = len(windows) == len(asked) && !capped &&
		((throughputState == algorithm.SignalFresh && latencyState == algorithm.SignalFresh) || idle)

	sample.Observation = observation
	sample.Reasons = reasons
	sample.WindowSeconds = shortestWindow(windows)
	sample.ContainerRestarts = highestRestarts(reads)
	return observation, samples, sample
}

// modelDeploymentScalerLatencyReadings 返回角色的两项延迟读数，并标明所需的延迟是否可用。
//
// 按角色决定是否具备所需读数。Prefill 依赖首 token 延迟，Decode 依赖 token 间延迟。
// 另一项缺失不会使已经取得的读数失效。
//
// 缺少读数的延迟保留为零，算法将非正值视为未测量。
func modelDeploymentScalerLatencyReadings(
	kind algorithm.RoleKind,
	ttftValue float64, ttftReadable bool,
	itlValue float64, itlReadable bool,
) (float64, float64, bool) {
	ttft, itl := 0.0, 0.0
	if ttftReadable {
		ttft = ttftValue
	}
	if itlReadable {
		itl = itlValue
	}
	switch kind {
	case algorithm.RoleKindPrefill:
		return ttft, itl, ttftReadable
	case algorithm.RoleKindDecode:
		return ttft, itl, itlReadable
	default:
		return ttft, itl, ttftReadable || itlReadable
	}
}

// modelDeploymentScalerLatencyMetricName 返回该角色需要的延迟指标名称。
func modelDeploymentScalerLatencyMetricName(kind algorithm.RoleKind) string {
	switch kind {
	case algorithm.RoleKindPrefill:
		return "time to first token histogram"
	case algorithm.RoleKindDecode:
		return "inter token latency histogram"
	default:
		return "latency histograms"
	}
}

// shortestWindow 返回计算该角色各 Pod 速率时使用的最短窗口。
//
// 最短窗口提供的观测时间最少。抓取耗时过长或引擎刚重启时，这个窗口的速率最容易失真。
func shortestWindow(windows []modelDeploymentScalerWindow) float64 {
	shortest := 0.0
	for i := range windows {
		if windows[i].seconds <= 0 {
			continue
		}
		if shortest == 0 || windows[i].seconds < shortest {
			shortest = windows[i].seconds
		}
	}
	return shortest
}

// highestRestarts 返回角色内各引擎容器重启次数的最大值。
func highestRestarts(reads []modelDeploymentScalerPodRead) int32 {
	highest := int32(0)
	for i := range reads {
		if reads[i].err != nil {
			continue
		}
		if reads[i].sample.restartCount > highest {
			highest = reads[i].sample.restartCount
		}
	}
	return highest
}

// scrapePods 在单轮采样的并发数和时间限制内读取各 Pod。
//
// 样本统一使用本轮的采样时间戳。计算速率时需要两轮之间的间隔，单次 HTTP 请求的耗时不能充当这个间隔。
//
// 返回本轮遇到的第一个错误，同时继续抓取其余 Pod。某个 Pod 未响应时，仍保留其他 Pod 的结果。
func (s *modelDeploymentScalerSampler) scrapePods(
	ctx context.Context, pods []*core.Pod, engine string, now time.Time,
) ([]modelDeploymentScalerPodRead, error) {
	reads := make([]modelDeploymentScalerPodRead, len(pods))
	var group errgroup.Group
	group.SetLimit(maxSamplerScrapeConcurrency)
	for i := range pods {
		group.Go(func() error {
			body, err := s.scrape(ctx, pods[i])
			read := modelDeploymentScalerPodRead{pod: pods[i], err: err}
			if err == nil {
				read.sample, err = modelDeploymentScalerParseSample(pods[i], body, engine, now)
				read.err = err
			}
			reads[i] = read
			return read.err
		})
	}
	return reads, group.Wait()
}

// readyEnginePods 为每个角色选择要抓取的引擎 Pod。
//
// 每个就绪副本只抓取成员 0。多成员副本的引擎横跨多个 Pod，由成员 0 暴露指标，其他成员只承担分布式计算。
func (s *modelDeploymentScalerSampler) readyEnginePods(
	md *workercore.ModelDeployment, pods []core.Pod,
) map[string][]*core.Pod {
	byRole := make(map[string][]*core.Pod, len(md.Spec.Roles))
	for _, pod := range modelDeploymentReadyReplicas(md, pods) {
		role := modelDeploymentPodRole(pod)
		byRole[role] = append(byRole[role], pod)
	}
	return byRole
}

// modelDeploymentScalerEngine 返回与指标族名称对应的目标引擎标识。
func modelDeploymentScalerEngine(md *workercore.ModelDeployment) string {
	if mapped, ok := ModelDeploymentEngineUpstreamName(md.Spec.Engine.Name); ok {
		return mapped
	}
	return ""
}

// freshnessBound 返回样本仍然有效的最长时间。
func (s *modelDeploymentScalerSampler) freshnessBound(
	scaler *workercore.ModelDeploymentScaler,
) time.Duration {
	seconds := int32(modelDeploymentScalerSamplerDefaultIntervalSeconds)
	if scaler != nil && scaler.Spec.Sampling.IntervalSeconds > 0 {
		seconds = scaler.Spec.Sampling.IntervalSeconds
	}
	return 2 * time.Duration(seconds) * time.Second
}

// samplingInterval 返回 Scaler 两次采样之间的最短间隔。
//
// 使用 Scaler 声明的间隔限制抓取频率，同时也限定了它对新负载作出反应的频率。
func (s *modelDeploymentScalerSampler) samplingInterval(
	scaler *workercore.ModelDeploymentScaler,
) time.Duration {
	seconds := int32(modelDeploymentScalerSamplerDefaultIntervalSeconds)
	if scaler != nil && scaler.Spec.Sampling.IntervalSeconds > 0 {
		seconds = scaler.Spec.Sampling.IntervalSeconds
	}
	return time.Duration(seconds) * time.Second
}

// now 返回当前时间。
func (s *modelDeploymentScalerSampler) now() time.Time {
	if s.Clock != nil {
		return s.Clock()
	}
	return time.Now()
}

// beginPass 为当前目标申请开始一轮采样。
//
// 如果同一目标已有采样在执行，跳过本次抓取，避免向引擎发出重复请求。
//
// 上次抓取后还未达到 Scaler 的采样间隔时也会跳过。每次调谐都可能请求观测，
// 而每轮实际抓取都会访问目标的引擎 Pod。
func (s *modelDeploymentScalerSampler) beginPass(
	targetKey string, now time.Time, interval time.Duration,
) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.passing == nil {
		s.passing = map[string]bool{}
	}
	if s.passing[targetKey] {
		return false
	}
	if last, ok := s.lastPass[targetKey]; ok && now.Before(last.Add(interval)) {
		return false
	}
	s.passing[targetKey] = true
	if s.lastPass == nil {
		s.lastPass = map[string]time.Time{}
	}
	s.lastPass[targetKey] = now
	return true
}

// endPass 释放目标的采样占用。
func (s *modelDeploymentScalerSampler) endPass(targetKey string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.passing, targetKey)
}

// snapshot 读取上一轮留下的样本快照。
func (s *modelDeploymentScalerSampler) snapshot(
	scalerKey string,
) map[string]modelDeploymentScalerPodSample {
	s.mu.Lock()
	defer s.mu.Unlock()
	ring := s.rings[scalerKey]
	if ring == nil {
		return map[string]modelDeploymentScalerPodSample{}
	}
	snapshot := make(map[string]modelDeploymentScalerPodSample, len(ring.pods))
	for uid, sample := range ring.pods {
		snapshot[uid] = sample
	}
	return snapshot
}

// commit 保存本轮样本，供下一轮计算相邻样本的增量。
func (s *modelDeploymentScalerSampler) commit(
	scalerKey string,
	targetKey string,
	samples map[string]modelDeploymentScalerPodSample,
	roles map[string]modelDeploymentScalerRoleSample,
	quantile float64,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rings == nil {
		s.rings = map[string]*modelDeploymentScalerRing{}
	}
	ring, ok := s.rings[scalerKey]
	if !ok {
		ring = newModelDeploymentScalerRing()
		s.rings[scalerKey] = ring
	}
	ring.targetKey = targetKey
	ring.pods = samples
	ring.roles = roles
	ring.quantile = quantile
}

// StabilizationHistory 返回采样器为指定角色保留的稳定窗口历史。
//
// 首次读取该角色时从 Scaler 状态恢复历史，使窗口能够跨 Operator 重启延续。
func (s *modelDeploymentScalerSampler) StabilizationHistory(
	scaler *workercore.ModelDeploymentScaler, role string,
) []workercore.ModelDeploymentScalerStabilizationSample {
	if scaler == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.roleWindow(scaler, role).stabilization.Data()
}

// RecordStabilizationHistory 保存各角色的稳定窗口历史。
//
// 约束求解器已经按窗口删去了过期样本，这里用裁剪后的历史替换旧记录。
func (s *modelDeploymentScalerSampler) RecordStabilizationHistory(
	scaler *workercore.ModelDeploymentScaler,
	histories map[string][]workercore.ModelDeploymentScalerStabilizationSample,
) {
	if scaler == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for role, history := range histories {
		window := s.roleWindow(scaler, role)
		window.stabilization.Reset()
		for _, sample := range history {
			window.stabilization.Push(sample)
		}
	}
}

// PredictorState 返回采样器为一个角色保存的预测器状态。
//
// 首次读取该角色时，从 Scaler 状态恢复预测器，使其在 Operator 重启后可以继续计算。
func (s *modelDeploymentScalerSampler) PredictorState(
	scaler *workercore.ModelDeploymentScaler, role string,
) *algorithm.PredictorState {
	if scaler == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	window := s.roleWindow(scaler, role)
	if window.predictor == nil {
		window.predictor = scalerPredictor(scaler.Status.Predictors, role)
	}
	return window.predictor
}

// RecordPredictorStates 保存各角色的预测器状态。
//
// 只记录具有预测器状态的角色。未产生状态的算法无需保存。
func (s *modelDeploymentScalerSampler) RecordPredictorStates(
	scaler *workercore.ModelDeploymentScaler, states map[string]*algorithm.PredictorState,
) {
	if scaler == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for role, state := range states {
		if state == nil {
			continue
		}
		s.roleWindow(scaler, role).predictor = state
	}
}

// roleWindow 返回某个 Scaler 中指定角色的窗口。
//
// 首次访问时从 Scaler 状态恢复窗口，接续 Operator 重启前已经记录的观测。
func (s *modelDeploymentScalerSampler) roleWindow(
	scaler *workercore.ModelDeploymentScaler, role string,
) *modelDeploymentScalerRoleWindow {
	if s.rings == nil {
		s.rings = map[string]*modelDeploymentScalerRing{}
	}
	key := string(scaler.UID)
	ring, ok := s.rings[key]
	if !ok {
		ring = newModelDeploymentScalerRing()
		s.rings[key] = ring
	}
	window, ok := ring.windows[role]
	if !ok {
		window = &modelDeploymentScalerRoleWindow{
			stabilization: datax.NewRingBuffer[workercore.ModelDeploymentScalerStabilizationSample](
				modelDeploymentScalerMaxStabilizationSamples,
			),
		}
		ring.windows[role] = window
	}
	if !window.seeded {
		window.seeded = true
		for _, sample := range scalerStabilizationHistory(scaler.Status.Roles, role) {
			window.stabilization.Push(sample)
		}
	}
	return window
}

// RoleSamples 返回目标部署中每个角色上一轮采样的详情。
//
// 除观测值外，另行记录信号缺失或异常的原因、速率窗口时长和引擎重启次数，以便写入状态并解释建议。
func (s *modelDeploymentScalerSampler) RoleSamples(
	scaler *workercore.ModelDeploymentScaler,
) map[string]modelDeploymentScalerRoleSample {
	if scaler == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ring := s.rings[string(scaler.UID)]
	if ring == nil {
		return nil
	}
	roles := make(map[string]modelDeploymentScalerRoleSample, len(ring.roles))
	for role, sample := range ring.roles {
		sample.Reasons = append([]string(nil), sample.Reasons...)
		roles[role] = sample
	}
	return roles
}

// cachedObservations 读取上一轮观测。观测年龄超过有效期时，将其标记为过期。
//
// 无法开始新一轮抓取时返回带状态的缓存观测，调用方据此判断读数是否仍可用于决策。
//
// 缓存的分位数必须与当前要求一致。例如 p99 读数不能回答 p95 延迟。遇到这种情况要等下一轮重新采样。
func (s *modelDeploymentScalerSampler) cachedObservations(
	scalerKey string, now time.Time, freshness time.Duration, quantile float64,
) map[string]algorithm.RoleObservation {
	s.mu.Lock()
	defer s.mu.Unlock()
	ring := s.rings[scalerKey]
	if ring == nil {
		return nil
	}
	if ring.quantile != quantile {
		return nil
	}
	cached := make(map[string]algorithm.RoleObservation, len(ring.roles))
	for role, sample := range ring.roles {
		observation := sample.Observation
		if now.Sub(observation.ObservedAt) > freshness {
			observation.GaugeState = modelDeploymentScalerStaleState(observation.GaugeState)
			observation.ThroughputState = modelDeploymentScalerStaleState(observation.ThroughputState)
			observation.LatencyState = modelDeploymentScalerStaleState(observation.LatencyState)
		}
		cached[role] = observation
	}
	return cached
}

// modelDeploymentScalerStaleState 给已过期的信号设置相应状态。
//
// 原先缺失的信号继续标为缺失，因为从未取得过可供标记为过期的读数。
func modelDeploymentScalerStaleState(state algorithm.SignalState) algorithm.SignalState {
	if state == algorithm.SignalMissing {
		return state
	}
	return algorithm.SignalStale
}

// modelDeploymentScalerRoleKind 将角色类型转换为算法层使用的角色枚举。
func modelDeploymentScalerRoleKind(kind workercore.ModelDeploymentRoleKind) algorithm.RoleKind {
	switch kind {
	case workercore.ModelDeploymentRoleKindPrefill:
		return algorithm.PrefillRole
	case workercore.ModelDeploymentRoleKindDecode:
		return algorithm.DecodeRole
	case workercore.ModelDeploymentRoleKindEncode:
		return algorithm.EncodeRole
	case workercore.ModelDeploymentRoleKindServer:
		return algorithm.ServerRole
	default:
		return algorithm.RoleKindUnknown
	}
}
