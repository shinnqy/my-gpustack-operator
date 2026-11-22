package worker

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	app "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/registry/rest"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"

	worker "gpustack.ai/gpustack/api/worker/v1"
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/extensionapi"
	"gpustack.ai/gpustack/pkg/utils/httpx"
	workerctrl "gpustack.ai/gpustack/pkg/worker/controllers/worker"
)

const (
	modelDeploymentMetricsTimeout  = 8 * time.Second
	modelDeploymentScrapeTimeout   = 2 * time.Second
	modelDeploymentMetricsMaxBytes = 1 << 20
	modelDeploymentMetricsMaxPods  = 64
	modelDeploymentCounterMaxAge   = 5 * time.Minute

	// modelDeploymentMetricsConcurrency 的大小经过设定，即使每次拉取都耗时至自身上限，
	// 也能在请求预算的一半时间内拉取完上限以内的所有 Pod，将另一半预算留给发现阶段。
	// 如果并发拉取数更少，挂起的 Pod 会在排在其后的 Pod 被请求之前就耗尽预算。
	modelDeploymentMetricsConcurrency = 32
)

type modelDeploymentPreviousCounter struct {
	value modelDeploymentCounterPair
	at    time.Time
}

type modelDeploymentPreviousWindow struct {
	value modelDeploymentWindowPair
	at    time.Time
}

// ModelDeploymentMetricsHandler 处理已鉴权的 metrics 子资源。
type ModelDeploymentMetricsHandler struct {
	extensionapi.GetOperation
	APIReader  ctrlcli.Reader
	HTTPClient *http.Client

	mu       sync.Mutex
	previous map[string]modelDeploymentPreviousCounter
	windows  map[string]modelDeploymentPreviousWindow
}

func newModelDeploymentMetricsHandler(parent rest.Scoper, opts extensionapi.SetupOptions) *ModelDeploymentMetricsHandler {
	h := &ModelDeploymentMetricsHandler{
		APIReader: opts.Manager.GetAPIReader(),
		HTTPClient: &http.Client{
			Transport: httpx.Transport(httpx.TransportOptions().WithoutProxy()),
			Timeout:   modelDeploymentScrapeTimeout,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		previous: make(map[string]modelDeploymentPreviousCounter),
		windows:  make(map[string]modelDeploymentPreviousWindow),
	}
	h.GetOperation = extensionapi.WithSubResourceGet(parent, h)
	return h
}

var (
	_ rest.Storage = (*ModelDeploymentMetricsHandler)(nil)
	_ rest.Getter  = (*ModelDeploymentMetricsHandler)(nil)
)

func (h *ModelDeploymentMetricsHandler) New() runtime.Object { return &worker.ModelDeploymentMetrics{} }
func (h *ModelDeploymentMetricsHandler) Destroy()            {}

type modelDeploymentPodScrape struct {
	pod    core.Pod
	router bool
	value  modelDeploymentScrape
	at     time.Time
	err    error
}

func (h *ModelDeploymentMetricsHandler) OnGet(
	ctx context.Context, key types.NamespacedName, _ ctrlcli.GetOptions,
) (runtime.Object, error) {
	ctx, cancel := context.WithTimeout(ctx, modelDeploymentMetricsTimeout)
	defer cancel()
	md := &workercore.ModelDeployment{}
	if err := h.APIReader.Get(ctx, key, md); err != nil {
		return nil, err
	}
	list := &core.PodList{}
	if err := h.APIReader.List(ctx, list, ctrlcli.InNamespace(key.Namespace), ctrlcli.MatchingLabels{
		"app.kubernetes.io/name": "model-deployment", "app.kubernetes.io/instance": key.Name,
	}); err != nil {
		return nil, fmt.Errorf("list ModelDeployment Pods: %w", err)
	}
	slices.SortFunc(list.Items, func(a, b core.Pod) int { return compareNames(a.Name, b.Name) })
	selected := make([]core.Pod, 0, len(list.Items))
	for i := range list.Items {
		pod := &list.Items[i]
		owned, err := h.ownedMetricsPod(ctx, md, pod)
		if err != nil {
			return nil, err
		}
		if owned {
			selected = append(selected, *pod)
		}
	}
	if len(selected) == 0 {
		return nil, kerrors.NewServiceUnavailable("ModelDeployment has no owned metrics Pods")
	}
	result := &worker.ModelDeploymentMetrics{ObjectMeta: meta.ObjectMeta{Name: md.Name, Namespace: md.Namespace}}
	for i := range md.Spec.Roles {
		role := &md.Spec.Roles[i]
		if len(role.Command) != 0 {
			continue
		}
		found := 0
		for j := range selected {
			if selected[j].Labels["app.kubernetes.io/component"] == role.Name {
				found++
			}
		}
		expected := max(int(role.Replicas), 1) * max(int(role.ReplicaSize), 1)
		if found == 0 {
			result.Missing = append(result.Missing, worker.ModelDeploymentMetricMissing{
				Source: "engine/" + role.Name, Reason: "no owned metrics Pod",
			})
		} else if found < expected {
			result.Missing = append(result.Missing, worker.ModelDeploymentMetricMissing{
				Source: "engine/" + role.Name, Reason: fmt.Sprintf("%d of %d owned metrics Pods present", found, expected),
			})
		}
	}
	if md.Spec.Router != nil {
		found := 0
		for j := range selected {
			if selected[j].Labels["modeldeployment.gpustack.ai/router"] == md.Spec.Router.Name {
				found++
			}
		}
		expected := 1
		if md.Spec.Router.Replicas != nil {
			expected = max(int(*md.Spec.Router.Replicas), 1)
		}
		if found == 0 {
			result.Missing = append(result.Missing, worker.ModelDeploymentMetricMissing{
				Source: "router", Reason: "no owned metrics Pod",
			})
		} else if found < expected {
			result.Missing = append(result.Missing, worker.ModelDeploymentMetricMissing{
				Source: "router", Reason: fmt.Sprintf("%d of %d owned metrics Pods present", found, expected),
			})
		}
	}
	if len(selected) > modelDeploymentMetricsMaxPods {
		result.Partial = true
		result.Missing = append(result.Missing, worker.ModelDeploymentMetricMissing{
			Source: "pod-discovery", Reason: "Pod count exceeds the bounded scrape limit",
		})
		selected = selected[:modelDeploymentMetricsMaxPods]
	}
	reads := make([]modelDeploymentPodScrape, len(selected))
	var wg sync.WaitGroup
	sem := make(chan struct{}, modelDeploymentMetricsConcurrency)
	for i := range selected {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			reads[i] = h.scrapeMetricsPod(ctx, md, selected[i])
		}(i)
	}
	wg.Wait()
	readable := 0
	for i := range reads {
		read := &reads[i]
		if read.err != nil {
			result.Missing = append(result.Missing, worker.ModelDeploymentMetricMissing{
				Pod: read.pod.Name, Source: "scrape", Reason: read.err.Error(),
			})
			continue
		}
		readable++
		h.mergeMetrics(result, md, read)
	}
	if readable == 0 {
		return nil, kerrors.NewServiceUnavailable("all ModelDeployment metrics Pods are unreadable: " + reads[0].err.Error())
	}
	slices.SortFunc(result.Processing, func(a, b worker.ModelDeploymentMetricGauge) int { return compareNames(a.Name+a.Scope, b.Name+b.Scope) })
	slices.SortFunc(result.Queueing, func(a, b worker.ModelDeploymentMetricGauge) int { return compareNames(a.Name+a.Scope, b.Name+b.Scope) })
	slices.SortFunc(result.CacheHits, func(a, b worker.ModelDeploymentCacheHit) int { return compareNames(a.Pod+a.Scope, b.Pod+b.Scope) })
	slices.SortFunc(result.Latency, func(a, b worker.ModelDeploymentMetricWindow) int { return compareNames(a.Pod+a.Name, b.Pod+b.Name) })
	slices.SortFunc(result.Traffic, func(a, b worker.ModelDeploymentMetricWindow) int { return compareNames(a.Pod+a.Name, b.Pod+b.Name) })
	slices.SortFunc(result.Transfer, func(a, b worker.ModelDeploymentMetricWindow) int { return compareNames(a.Pod+a.Name, b.Pod+b.Name) })
	slices.SortFunc(result.Missing, func(a, b worker.ModelDeploymentMetricMissing) int {
		return compareNames(a.Pod+a.Source, b.Pod+b.Source)
	})
	result.Partial = result.Partial || modelDeploymentMissingIsPartial(result.Missing)
	result.Timestamp = meta.NewTime(time.Now())
	return result, nil
}

func compareNames(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func (h *ModelDeploymentMetricsHandler) ownedMetricsPod(
	ctx context.Context, md *workercore.ModelDeployment, pod *core.Pod,
) (bool, error) {
	if pod.Labels["modeldeployment.gpustack.ai/router"] != "" {
		if md.Spec.Router == nil || pod.Labels["modeldeployment.gpustack.ai/router"] != md.Spec.Router.Name {
			return false, nil
		}
		podOwner := meta.GetControllerOf(pod)
		if podOwner == nil || podOwner.Kind != "ReplicaSet" {
			return false, nil
		}
		rs := &app.ReplicaSet{}
		if err := h.APIReader.Get(ctx, ctrlcli.ObjectKey{Namespace: md.Namespace, Name: podOwner.Name}, rs); err != nil {
			if kerrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		if rs.UID != podOwner.UID {
			return false, nil
		}
		rsOwner := meta.GetControllerOf(rs)
		if rsOwner == nil || rsOwner.Kind != "Deployment" || rsOwner.Name != md.Name+"-router" {
			return false, nil
		}
		deployment := &app.Deployment{}
		if err := h.APIReader.Get(ctx, ctrlcli.ObjectKey{Namespace: md.Namespace, Name: rsOwner.Name}, deployment); err != nil {
			if kerrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		if deployment.UID != rsOwner.UID {
			return false, nil
		}
		owner := meta.GetControllerOf(deployment)
		return owner != nil && owner.Kind == "ModelDeployment" && owner.UID == md.UID, nil
	}
	role := pod.Labels["app.kubernetes.io/component"]
	validRole := false
	for i := range md.Spec.Roles {
		if md.Spec.Roles[i].Name == role && len(md.Spec.Roles[i].Command) == 0 {
			validRole = true
			break
		}
	}
	owner := meta.GetControllerOf(pod)
	return validRole && owner != nil && owner.Kind == "ModelDeployment" && owner.UID == md.UID, nil
}

func (h *ModelDeploymentMetricsHandler) scrapeMetricsPod(
	ctx context.Context, md *workercore.ModelDeployment, pod core.Pod,
) modelDeploymentPodScrape {
	read := modelDeploymentPodScrape{pod: pod, router: pod.Labels["modeldeployment.gpustack.ai/router"] != ""}
	if pod.Status.PodIP == "" || pod.Annotations["prometheus.io/scrape"] != "true" ||
		pod.Annotations["prometheus.io/path"] != "/metrics" {
		read.err = fmt.Errorf("pod has no ready advertised metrics endpoint")
		return read
	}
	port, err := strconv.ParseInt(pod.Annotations["prometheus.io/port"], 10, 32)
	if err != nil || port < 1 || port > 65535 {
		read.err = fmt.Errorf("pod advertises an invalid metrics port")
		return read
	}
	if !modelDeploymentMetricsPortDeclared(&pod, int32(port)) {
		read.err = fmt.Errorf("pod advertises a metrics port absent from its containers")
		return read
	}
	scheme := pod.Annotations["prometheus.io/scheme"]
	if scheme == "" {
		scheme = "http"
	}
	if scheme != "http" && scheme != "https" {
		read.err = fmt.Errorf("pod advertises an invalid metrics scheme")
		return read
	}
	url := scheme + "://" + net.JoinHostPort(pod.Status.PodIP, strconv.FormatInt(port, 10)) + "/metrics"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		read.err = err
		return read
	}
	response, err := h.HTTPClient.Do(req)
	if err != nil {
		read.err = err
		return read
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		read.err = fmt.Errorf("metrics endpoint returned HTTP %d", response.StatusCode)
		return read
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, modelDeploymentMetricsMaxBytes+1))
	if err != nil {
		read.err = err
		return read
	}
	if len(body) > modelDeploymentMetricsMaxBytes {
		read.err = fmt.Errorf("metrics response exceeds %d bytes", modelDeploymentMetricsMaxBytes)
		return read
	}
	engine := md.Spec.Engine.Name
	if mapped, ok := workerctrl.ModelDeploymentEngineUpstreamName(engine); ok {
		engine = mapped
	}
	router := ""
	if read.router {
		engine, router = "", md.Spec.Router.Name
	}
	read.value, read.err = parseModelDeploymentMetrics(body, engine, router)
	read.at = time.Now()
	return read
}

func modelDeploymentMetricsPortDeclared(pod *core.Pod, port int32) bool {
	for _, container := range append(slices.Clone(pod.Spec.Containers), pod.Spec.InitContainers...) {
		for _, declared := range container.Ports {
			if declared.ContainerPort == port {
				return true
			}
		}
	}
	return false
}

func (h *ModelDeploymentMetricsHandler) mergeMetrics(
	result *worker.ModelDeploymentMetrics, md *workercore.ModelDeployment, read *modelDeploymentPodScrape,
) {
	metricNames := map[string]string{}
	encode := false
	switch {
	case read.router:
		switch md.Spec.Router.Name {
		case "llm-d-router":
			metricNames["router-running"] = "llm_d_epp_request_running"
			metricNames["router-backends"] = "llm_d_epp_ready_endpoints"
		case "vllm-router":
			if isModelDeploymentPD(md) {
				for _, source := range []string{"vllm_router_running_requests", "vllm_router_active_workers"} {
					result.Missing = append(result.Missing, worker.ModelDeploymentMetricMissing{
						Pod: read.pod.Name, Source: source, Reason: modelDeploymentVLLMRouterPDNoProcessing,
					})
				}
				break
			}
			metricNames["router-running"] = "vllm_router_running_requests"
			metricNames["router-reported-workers"] = "vllm_router_active_workers"
		case "sglang-gateway":
			metricNames["router-running"] = "smg_worker_requests_active"
			metricNames["router-backends"] = "smg_worker_pool_size"
		}
	case md.Spec.Engine.Name == workercore.ModelDeploymentEngineVLLM:
		metricNames["running"] = "vllm:num_requests_running"
		metricNames["waiting"] = "vllm:num_requests_waiting"
	default:
		metricNames["running"] = "sglang:num_running_reqs"
		metricNames["waiting"] = "sglang:num_queue_reqs"
		if isModelDeploymentPD(md) && modelDeploymentPodRoleKind(md, &read.pod) == workercore.ModelDeploymentRoleKindDecode {
			metricNames["decode-transfer-waiting"] = "sglang:num_decode_transfer_queue_reqs"
		}
	}
	// 编码器不是语言模型服务器，下文将这两部分区分处理。
	// 这里仍会请求其请求量 gauge 指标，因为发布该指标的编码器正在上报真实的在途工作量，
	// 而编码器是否为瓶颈正是本快照要回答的问题。该角色无法提供的是请求穿过语言模型时的 token 统计。
	// 当前代码库尚未验证编码器具体导出哪些请求 gauge，因此缺失的指标会被记录下来而不计为读取失败，
	// 也不会直接丢弃：如果引擎确实发布了该指标，丢弃会静默舍弃真实信号。
	encode = !read.router &&
		modelDeploymentPodRoleKind(md, &read.pod) == workercore.ModelDeploymentRoleKindEncode
	for key, source := range metricNames {
		value, ok := read.value.gauges[key]
		if !ok {
			reason := "metric is absent"
			if encode {
				reason = modelDeploymentEncodeNoRequestGauge
			}
			result.Missing = append(result.Missing, worker.ModelDeploymentMetricMissing{
				Pod: read.pod.Name, Source: source, Reason: reason,
			})
			continue
		}
		scope := "engine"
		if read.router {
			scope = "router"
		} else {
			scope += "/" + read.pod.Labels["app.kubernetes.io/component"]
		}
		gauge := worker.ModelDeploymentMetricGauge{
			Name: key, Source: source, Scope: scope, Unit: "requests",
			Value: value, PodCount: 1, ObservedAt: meta.NewTime(read.at),
		}
		switch key {
		case "router-backends":
			gauge.Unit = "backends"
		case "router-reported-workers":
			gauge.Unit = "workers"
		}
		if key == "waiting" || key == "decode-transfer-waiting" {
			mergeModelDeploymentGauge(&result.Queueing, gauge)
		} else {
			mergeModelDeploymentGauge(&result.Processing, gauge)
		}
	}
	idle := h.mergeWindowMetrics(result, md, read)
	if read.router {
		return
	}
	// 下方所有指标源均属于语言模型，编码器不运行语言模型。编码器只生成编码器缓存输出而不生成 token，
	// 因此没有可命中的前缀缓存，也不响应 prompt；若要求它提供缓存命中率，
	// 会因一个对该角色毫无意义的指标序列而使每个健康的 EPD 快照被标记为部分完整。
	// token 窗口指标同理，已在上文对该角色跳过。
	if encode {
		return
	}
	// SGLang 配对中的 decode 侧从 prefill 侧接收 prompt 的缓存，自身从不执行 prefill，
	// 因此其 prefill token 计数器始终为零，不存在命中率。
	if md.Spec.Engine.Name == workercore.ModelDeploymentEngineSGLang && isModelDeploymentPD(md) &&
		modelDeploymentPodRoleKind(md, &read.pod) == workercore.ModelDeploymentRoleKindDecode {
		return
	}
	if len(read.value.counters) == 0 {
		result.Missing = append(result.Missing, worker.ModelDeploymentMetricMissing{
			Pod: read.pod.Name, Source: "cache-hit counters", Reason: "metric is absent",
		})
		return
	}
	var sglangHost, sglangStorage bool
	if md.Spec.Engine.Name == workercore.ModelDeploymentEngineSGLang {
		sglangHost, sglangStorage = modelDeploymentSGLangPodTiers(&read.pod)
	}
	for scope, current := range read.value.counters {
		previous, ok := h.replaceCounter(md, &read.pod, scope, current, read.at)
		// SGLang 从启动起即导出每个层级的命中计数器，而 Pod 未构建的层级其计数器会保持为零，
		// 这会被误读为实测未命中，而非该层级不存在。
		if md.Spec.Engine.Name == workercore.ModelDeploymentEngineSGLang && (scope == "host-prefix" && !sglangHost ||
			scope == "storage-prefix" && !sglangStorage) {
			reason := modelDeploymentSGLangNoHostTier
			if scope == "storage-prefix" {
				reason = modelDeploymentSGLangNoStorageTier
			}
			result.Missing = append(result.Missing, worker.ModelDeploymentMetricMissing{
				Pod: read.pod.Name, Source: scope, Reason: reason,
			})
			continue
		}
		// vLLM 无论是否配置 KV connector 都会导出外部前缀缓存计数器，但在没有 connector 时它们从不变化。
		// 有变化的查询计数器证明存在 connector，因此只有当未渲染任何 connector 的 Pod 上该计数器仍为零时，
		// 才判定为不支持。
		if scope == "external-store" && current.queries == 0 && !modelDeploymentPodRendersKVConnector(&read.pod) {
			result.Missing = append(result.Missing, worker.ModelDeploymentMetricMissing{
				Pod: read.pod.Name, Source: scope, Reason: modelDeploymentVLLMNoKVConnector,
			})
			continue
		}
		// vLLM 将任意 KV connector 加载的所有 token（包括点对点传输链路上的）都计为外部命中。
		// 这些计数器仅在挂载了共享存储池且不是配对中 decode 侧的 Pod 上才衡量共享存储：
		// decode 侧加载的是 prefill 侧发送的数据，而 prefill 侧自身链路从不加载任何数据。
		// 未配置 connector 的 Pod 保留上文的单一原因。
		if scope == "external-store" && (md.Spec.KVCache == nil || (isModelDeploymentPD(md) &&
			modelDeploymentPodRoleKind(md, &read.pod) == workercore.ModelDeploymentRoleKindDecode)) {
			result.Missing = append(result.Missing, worker.ModelDeploymentMetricMissing{
				Pod: read.pod.Name, Source: scope, Reason: modelDeploymentVLLMExternalNotStore,
			})
			continue
		}
		if !ok {
			result.Missing = append(result.Missing, worker.ModelDeploymentMetricMissing{
				Pod: read.pod.Name, Source: scope, Reason: "awaiting a second counter sample",
			})
			continue
		}
		hits := current.hits - previous.value.hits
		queries := current.queries - previous.value.queries
		if hits < 0 || queries < 0 || hits > queries {
			result.Missing = append(result.Missing, worker.ModelDeploymentMetricMissing{
				Pod: read.pod.Name, Source: scope, Reason: "counter reset or incompatible denominator",
			})
			continue
		}
		if queries == 0 {
			reason := "no queries in the sampling window"
			if idle {
				reason = modelDeploymentIdleWindow
			}
			result.Missing = append(result.Missing, worker.ModelDeploymentMetricMissing{
				Pod: read.pod.Name, Source: scope, Reason: reason,
			})
			continue
		}
		source := "vllm:prefix_cache"
		if scope == "external-store" {
			source = "vllm:external_prefix_cache"
		}
		if md.Spec.Engine.Name == workercore.ModelDeploymentEngineSGLang {
			source = "sglang:prefill_effective_tokens_total"
		}
		result.CacheHits = append(result.CacheHits, worker.ModelDeploymentCacheHit{
			Pod: read.pod.Name, Source: source, Scope: scope, Unit: "tokens",
			Hits: hits, Queries: queries, Rate: hits / queries,
			WindowSeconds: read.at.Sub(previous.at).Seconds(),
			PodCount:      1, ObservedAt: meta.NewTime(read.at),
		})
	}
}

// modelDeploymentPodRendersKVConnector 报告 vLLM 引擎 Pod 是否配置了 KV connector。
// operator 会通过这同一个参数渲染受管角色运行的每个 connector（无论是共享存储还是点对点 prefill/decode 链路），
// 且准入控制会拒绝在角色的额外参数中传入该参数。
func modelDeploymentPodRendersKVConnector(pod *core.Pod) bool {
	for _, container := range pod.Spec.Containers {
		for _, arg := range append(slices.Clone(container.Command), container.Args...) {
			if name, _, _ := strings.Cut(arg, "="); name == "--kv-transfer-config" {
				return true
			}
		}
	}
	return false
}

// modelDeploymentSGLangHostTierArgs 是在 v0.5.18 中用于选择会上报主机命中（即
// sglang:prefill_effective_tokens_total 的 host_hit 模式）的树形缓存的 SGLang 参数：
// split_cached_prefix_by_tier（managers/schedule_batch.py:203-215）将树形缓存的
// host_hit_length 计为主机命中，而选择链回退到的普通 RadixCache 不上报该指标。
// operator 仅渲染第一个参数，用户可自行添加其中任意一个。
var modelDeploymentSGLangHostTierArgs = []string{
	// server_args.py:2668 中 enable_hierarchical_cache 会选择 HiRadixCache（mem_cache/registry.py:124-132），
	// 其 match 会设置 host_hit_length（mem_cache/hiradix_cache.py:1754-1768）。
	"--enable-hierarchical-cache",
	// server_args.py:3010 中 enable_lmcache 会选择 LMCRadixCache（mem_cache/registry.py:138-148），
	// 其 match 会设置 host_hit_length（mem_cache/storage/lmcache/lmc_radix_cache.py:245）。
	"--enable-lmcache",
	// server_args.py:3022 中 enable_flexkv 会选择 FlexKV 缓存（mem_cache/registry.py:151-165），
	// 其 match 会设置 host_hit_length（mem_cache/storage/flexkv/flexkv_radix_cache.py:232）。
	"--enable-flexkv",
}

const (
	// modelDeploymentSGLangRadixBackendArg 指定已注册的树形缓存以替代选择链
	// （server_args.py:1742-1746，默认为 None；mem_cache/registry.py:223-236）。仅当其值为
	// modelDeploymentSGLangRadixBackendFlexKV 时才选择主机层级，这是 v0.5.18 注册的唯一后端
	// （mem_cache/storage/flexkv/__init__.py:82）；其他名称均为此处未知其层级的后端。
	modelDeploymentSGLangRadixBackendArg    = "--radix-cache-backend"
	modelDeploymentSGLangRadixBackendFlexKV = "flexkv"
	// modelDeploymentSGLangStorageBackendArg 在启动时启用存储层级（managers/scheduler.py:444），
	// 且在 v0.5.18 中仅该层级会设置 storage_hit 模式（managers/scheduler.py:3317-3325）。
	// 该参数归 operator 所有，并会在部署挂载 KVCache 池时准确渲染。
	modelDeploymentSGLangStorageBackendArg = "--hicache-storage-backend"
)

// modelDeploymentSGLangPodTiers 按 SGLang 自身的拼写规则匹配各个标志，
// 报告 SGLang 引擎 Pod 的参数是否构建了主机层级和存储层级。
func modelDeploymentSGLangPodTiers(pod *core.Pod) (host, storage bool) {
	engine := workercore.ModelDeploymentEngineSGLang
	for _, container := range pod.Spec.Containers {
		args := slices.Concat(container.Command, container.Args)
		for i, arg := range args {
			if _, ok := workerctrl.ModelDeploymentResolveArg(engine, arg, modelDeploymentSGLangHostTierArgs); ok {
				host = true
			}
			if _, ok := workerctrl.ModelDeploymentResolveArg(engine, arg, []string{modelDeploymentSGLangStorageBackendArg}); ok {
				storage = true
			}
			if _, ok := workerctrl.ModelDeploymentResolveArg(engine, arg, []string{modelDeploymentSGLangRadixBackendArg}); ok {
				_, value, inline := strings.Cut(arg, "=")
				if !inline && i+1 < len(args) {
					value = args[i+1]
				}
				host = host || value == modelDeploymentSGLangRadixBackendFlexKV
			}
		}
	}
	return host, storage
}

func isModelDeploymentPD(md *workercore.ModelDeployment) bool {
	hasPrefill, hasDecode := false, false
	for _, role := range md.Spec.Roles {
		hasPrefill = hasPrefill || role.Kind == workercore.ModelDeploymentRoleKindPrefill
		hasDecode = hasDecode || role.Kind == workercore.ModelDeploymentRoleKindDecode
	}
	return hasPrefill && hasDecode
}

func modelDeploymentPodRoleKind(md *workercore.ModelDeployment, pod *core.Pod) workercore.ModelDeploymentRoleKind {
	name := pod.Labels["app.kubernetes.io/component"]
	for _, role := range md.Spec.Roles {
		if role.Name == name {
			return role.Kind
		}
	}
	return ""
}

const (
	modelDeploymentLabeledCounterUnexported = "labeled failure or error counter is not exported before its first increment; " +
		"its paired counter was read in the same scrape"
	modelDeploymentLabeledHistogramUnexported = "labeled histogram is not exported before its first observation; " +
		"its paired histogram recorded no new observation in the same window"
	modelDeploymentVLLMRouterPDNoProcessing = "unsupported source: the vLLM router exports no router processing gauge " +
		"for a prefill/decode deployment"
	modelDeploymentVLLMNoKVConnector = "unsupported source: the Pod renders no KV connector, so vLLM queries no " +
		"external prefix cache"
	modelDeploymentVLLMExternalNotStore = "unsupported source: vLLM counts every token any KV connector loads as an " +
		"external prefix cache hit, and this Pod's are not the shared store's alone: it attaches no KV cache pool, " +
		"or it is the decode half of a pair, whose store hits cannot be told from the blocks the prefill half sends"
	modelDeploymentSGLangNoHostTier = "unsupported source: the Pod renders no SGLang argument that selects a tree " +
		"cache with a host tier, so SGLang counts no host hit"
	modelDeploymentSGLangNoStorageTier = "unsupported source: the Pod renders no SGLang storage backend, so SGLang " +
		"counts no storage hit"
	modelDeploymentIdleWindow = "idle sampling window: the Pod's TTFT histogram recorded no new request, so this " +
		"source has no new sample"
	modelDeploymentEncodeNoRequestGauge = "unsupported source: the Pod runs the Encode role, and the language-model " +
		"request queues are not a source an encoder is read as providing, so their absence is not a readable " +
		"source failing to contribute"
)

// modelDeploymentNotPartialReasons 列出了不会导致快照被标记为部分完整的原因：
// 健康的 Pod 尚无理由导出的带标签指标序列、已知路由或 Pod 当前渲染形态不提供的指标源，
// 以及在采样窗口内未处理任何请求的 Pod 的指标源（其没有新样本可提供）。
// 这些条目保留在 missing[] 中以免伪造数值，但均不表示可读指标源未能提供数据。
var modelDeploymentNotPartialReasons = []string{
	modelDeploymentLabeledCounterUnexported,
	modelDeploymentLabeledHistogramUnexported,
	modelDeploymentVLLMRouterPDNoProcessing,
	modelDeploymentVLLMNoKVConnector,
	modelDeploymentVLLMExternalNotStore,
	modelDeploymentSGLangNoHostTier,
	modelDeploymentSGLangNoStorageTier,
	modelDeploymentIdleWindow,
	modelDeploymentEncodeNoRequestGauge,
}

// modelDeploymentMissingIsPartial 报告是否缺失了必需的指标源。
func modelDeploymentMissingIsPartial(missing []worker.ModelDeploymentMetricMissing) bool {
	return slices.ContainsFunc(missing, func(m worker.ModelDeploymentMetricMissing) bool {
		return !slices.Contains(modelDeploymentNotPartialReasons, m.Reason)
	})
}

type modelDeploymentWindowDefinition struct {
	name, source, area, unit string
	histogram                bool
	// pairedWith 指定用于佐证带标签序列的基准序列。带标签的序列在首次递增或观测前不会导出任何内容。
	// 当失败或错误计数器缺失、而其配对序列在同一次抓取中被读到时，说明该 Pod 尚未发生过失败，
	// 而非指标源不可读。当直方图缺失、而其配对序列在同一窗口内未记录到新观测时，说明没有可观测事件；
	// 若配对序列已有变化而直方图缺失，则会遗漏真实样本，因此保留为缺失。
	// 只有配置了配对序列的条目才采用此判定；其他任何缺失的序列仍视为缺失指标源。
	pairedWith string
}

// modelDeploymentWindowDefinitions 列出单个 Pod 的窗口指标源。读取 TTFT 的列表会将其排在首位：
// Pod 响应的每个请求都会经过 TTFT，因此后续条目会参考其窗口来区分“未处理请求的 Pod”和“停止记录的 Pod”。
func modelDeploymentWindowDefinitions(md *workercore.ModelDeployment, router bool) []modelDeploymentWindowDefinition {
	if router {
		switch md.Spec.Router.Name {
		case "llm-d-router":
			// 路由器的错误计数器还会统计其请求计数器从未记录的请求（例如未指定模型的错误请求），
			// 因此错误计数器采用独立命名，以避免将两者混入同一错误率计算中。
			return []modelDeploymentWindowDefinition{
				{"ttft", "llm_d_epp_request_ttft_seconds", "latency", "seconds", true, ""},
				{"tpot", "llm_d_epp_request_streaming_tpot_seconds", "latency", "seconds", true, ""},
				{"requests", "llm_d_epp_request_total", "traffic", "requests/second", false, ""},
				{"request-errors", "llm_d_epp_request_error_total", "traffic", "errors/second", false, "requests"},
			}
		case "vllm-router":
			// 路由器的 prefill/decode 模式仅记录其 pd_* 序列。其错误计数器也会在从未到达请求计数器的
			// 拒绝请求上递增，因此两者没有共同分母，通过独立命名避免将它们用于错误率计算。
			if isModelDeploymentPD(md) {
				return []modelDeploymentWindowDefinition{
					{"pd-requests", "vllm_router_pd_requests_total", "traffic", "requests/second", false, ""},
					{"pd-errors", "vllm_router_pd_errors_total", "traffic", "errors/second", false, "pd-requests"},
				}
			}
			// 重试耗尽计数器与错误计数器一样按路由加标签，因此在请求首次耗尽重试次数前也不会导出。
			return []modelDeploymentWindowDefinition{
				{"successful-requests", "vllm_router_requests_total", "traffic", "requests/second", false, ""},
				{"errors", "vllm_router_request_errors_total", "traffic", "errors/second", false, "successful-requests"},
				{"retries-exhausted", "vllm_router_retries_exhausted_total", "traffic", "events/second", false, "successful-requests"},
			}
		case "sglang-gateway":
			return []modelDeploymentWindowDefinition{
				{"requests", "smg_router_requests_total", "traffic", "requests/second", false, ""},
				{"errors", "smg_router_request_errors_total", "traffic", "errors/second", false, "requests"},
				{"http-5xx-responses", "smg_http_responses_total", "traffic", "responses/second", false, ""},
			}
		}
	}
	if md.Spec.Engine.Name == workercore.ModelDeploymentEngineVLLM {
		return []modelDeploymentWindowDefinition{
			{"ttft", "vllm:time_to_first_token_seconds", "latency", "seconds", true, ""},
			{"tpot", "vllm:request_time_per_output_token_seconds", "latency", "seconds", true, ""},
			{"itl", "vllm:inter_token_latency_seconds", "latency", "seconds", true, ""},
		}
	}
	// SGLang 的 token 间延迟直方图带有标签，且仅当请求流式返回第二个 chunk 时才会观测记录，
	// 因此仅响应过非流式预热请求的服务器不会导出该指标。
	definitions := []modelDeploymentWindowDefinition{
		{"ttft", "sglang:time_to_first_token_seconds", "latency", "seconds", true, ""},
		{"itl", "sglang:inter_token_latency_seconds", "latency", "seconds", true, "ttft"},
	}
	if isModelDeploymentPD(md) {
		definitions = append(definitions,
			modelDeploymentWindowDefinition{"transfer-latency", "sglang:kv_transfer_latency_ms", "transfer", "milliseconds", true, ""},
			modelDeploymentWindowDefinition{"transfer-speed", "sglang:kv_transfer_speed_gb_s", "transfer", "GB/second", true, ""},
			modelDeploymentWindowDefinition{"transfer-size", "sglang:kv_transfer_total_mb", "transfer", "MB", true, ""},
			modelDeploymentWindowDefinition{"transfer-failures", "sglang:num_transfer_failed_reqs_total", "transfer", "errors/second", false, "transfer-size"},
		)
	}
	return definitions
}

// mergeWindowMetrics 报告 Pod 是否处于空闲状态：其 TTFT 直方图在采样窗口内未记录到新请求，
// 因此其所有窗口指标源均无新样本可观测。
func (h *ModelDeploymentMetricsHandler) mergeWindowMetrics(
	result *worker.ModelDeploymentMetrics, md *workercore.ModelDeployment, read *modelDeploymentPodScrape,
) bool {
	scope := "router"
	if !read.router {
		scope = "engine/" + read.pod.Labels["app.kubernetes.io/component"]
	}
	deltas := map[string]float64{}
	durations := map[string]float64{}
	starts := map[string]time.Time{}
	sources := map[string]string{}
	unobserved := func(name string) bool {
		count, ok := deltas[name]
		return ok && count == 0
	}
	for _, definition := range modelDeploymentWindowDefinitions(md, read.router) {
		sources[definition.name] = definition.source
		// 编码器不输出 token，因此不会观测到首 token 延迟、token 间间隔或每输出 token 耗时。
		// 这些指标由 prefill 和 decode 角色负责提供，若在此处要求编码器提供，
		// 会因三个该角色无法产生的指标序列而使每个健康的 EPD 快照被标记为部分完整。
		if definition.area == "latency" &&
			modelDeploymentPodRoleKind(md, &read.pod) == workercore.ModelDeploymentRoleKindEncode {
			continue
		}
		// SGLang 在发送 KV 传输的一侧（即 prefill 侧）观测 KV 传输。
		if definition.area == "transfer" && modelDeploymentPodRoleKind(md, &read.pod) != workercore.ModelDeploymentRoleKindPrefill {
			continue
		}
		// 同一侧会将首个 token 交给 decode，这也是 SGLang 记录 TTFT 的位置。
		if definition.name == "ttft" && md.Spec.Engine.Name == workercore.ModelDeploymentEngineSGLang && isModelDeploymentPD(md) &&
			modelDeploymentPodRoleKind(md, &read.pod) == workercore.ModelDeploymentRoleKindPrefill {
			continue
		}
		// 配对中的 prefill 侧仅返回首个 token，因此从不会观测到两个 token 之间的间隔，
		// 且其每输出 token 耗时始终为零；若要求这两个指标，会使每个配对快照被标记为部分完整，
		// 或发布毫无意义的均值。
		if (definition.name == "itl" || definition.name == "tpot") && isModelDeploymentPD(md) &&
			modelDeploymentPodRoleKind(md, &read.pod) == workercore.ModelDeploymentRoleKindPrefill {
			continue
		}
		current, ok := read.value.windows[definition.name]
		if !ok {
			reason := "metric is absent"
			_, seen := read.value.windows[definition.pairedWith]
			switch {
			case definition.pairedWith == "":
			case definition.histogram && unobserved(definition.pairedWith):
				reason = modelDeploymentLabeledHistogramUnexported
			case !definition.histogram && seen:
				reason = modelDeploymentLabeledCounterUnexported
			}
			result.Missing = append(result.Missing, worker.ModelDeploymentMetricMissing{
				Pod: read.pod.Name, Source: definition.source, Reason: reason,
			})
			continue
		}
		previous, ok := h.replaceWindow(md, &read.pod, definition.source, current, read.at)
		if !ok {
			result.Missing = append(result.Missing, worker.ModelDeploymentMetricMissing{
				Pod: read.pod.Name, Source: definition.source, Reason: "awaiting a second counter sample",
			})
			continue
		}
		window := read.at.Sub(previous.at).Seconds()
		count := current.count - previous.value.count
		sum := current.sum - previous.value.sum
		if count < 0 || sum < 0 {
			result.Missing = append(result.Missing, worker.ModelDeploymentMetricMissing{
				Pod: read.pod.Name, Source: definition.source, Reason: "counter reset",
			})
			continue
		}
		deltas[definition.name] = count
		durations[definition.name] = window
		starts[definition.name] = previous.at
		if count == 0 && definition.histogram {
			// 只有空闲 Pod 的延迟直方图才会没有新样本；若 TTFT 已变化而该直方图保持不动，
			// 说明它遗漏了 Pod 已处理的请求。
			reason := "no observations in the sampling window"
			if definition.area == "latency" && unobserved("ttft") {
				reason = modelDeploymentIdleWindow
			}
			result.Missing = append(result.Missing, worker.ModelDeploymentMetricMissing{
				Pod: read.pod.Name, Source: definition.source, Reason: reason,
			})
			continue
		}
		value := count / window
		if definition.histogram {
			value = sum / count
		}
		metric := worker.ModelDeploymentMetricWindow{
			Name: definition.name, Pod: read.pod.Name, Source: definition.source,
			Scope: scope, Unit: definition.unit, Value: value, Samples: count,
			WindowSeconds: window, ObservedAt: meta.NewTime(read.at),
		}
		switch definition.area {
		case "latency":
			result.Latency = append(result.Latency, metric)
		case "traffic":
			result.Traffic = append(result.Traffic, metric)
		case "transfer":
			result.Transfer = append(result.Transfer, metric)
		}
	}
	if requests, ok := deltas["requests"]; ok {
		if errors, hasErrors := deltas["errors"]; hasErrors && !starts["requests"].Equal(starts["errors"]) {
			result.Missing = append(result.Missing, worker.ModelDeploymentMetricMissing{
				Pod: read.pod.Name, Source: sources["requests"] + "+" + sources["errors"],
				Reason: "request and error counters have different sampling windows",
			})
		} else if hasErrors && requests > 0 && errors <= requests {
			result.Traffic = append(result.Traffic, worker.ModelDeploymentMetricWindow{
				Name: "error-ratio", Pod: read.pod.Name, Source: sources["requests"] + "+" + sources["errors"],
				Scope: scope, Unit: "fraction", Value: errors / requests, Samples: requests,
				WindowSeconds: durations["requests"], ObservedAt: meta.NewTime(read.at),
			})
		} else if hasErrors {
			reason := "no requests in the sampling window"
			if errors > requests {
				reason = "error counter exceeds request counter"
			}
			result.Missing = append(result.Missing, worker.ModelDeploymentMetricMissing{
				Pod: read.pod.Name, Source: sources["requests"] + "+" + sources["errors"], Reason: reason,
			})
		}
	}
	return unobserved("ttft")
}

func (h *ModelDeploymentMetricsHandler) replaceWindow(
	md *workercore.ModelDeployment, pod *core.Pod, source string,
	value modelDeploymentWindowPair, at time.Time,
) (modelDeploymentPreviousWindow, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.windows == nil {
		h.windows = make(map[string]modelDeploymentPreviousWindow)
	}
	for key, sample := range h.windows {
		if at.Sub(sample.at) > 2*modelDeploymentCounterMaxAge {
			delete(h.windows, key)
		}
	}
	key := string(md.UID) + "/" + string(pod.UID) + "/" + source
	previous, ok := h.windows[key]
	h.windows[key] = modelDeploymentPreviousWindow{value: value, at: at}
	return previous, ok && at.Sub(previous.at) > 0 && at.Sub(previous.at) <= modelDeploymentCounterMaxAge
}

func mergeModelDeploymentGauge(gauges *[]worker.ModelDeploymentMetricGauge, next worker.ModelDeploymentMetricGauge) {
	for i := range *gauges {
		current := &(*gauges)[i]
		if current.Name == next.Name && current.Source == next.Source && current.Scope == next.Scope {
			current.Value += next.Value
			current.PodCount++
			if next.ObservedAt.After(current.ObservedAt.Time) {
				current.ObservedAt = next.ObservedAt
			}
			return
		}
	}
	*gauges = append(*gauges, next)
}

func (h *ModelDeploymentMetricsHandler) replaceCounter(
	md *workercore.ModelDeployment, pod *core.Pod, scope string,
	value modelDeploymentCounterPair, at time.Time,
) (modelDeploymentPreviousCounter, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.previous == nil {
		h.previous = make(map[string]modelDeploymentPreviousCounter)
	}
	for key, sample := range h.previous {
		if at.Sub(sample.at) > 2*modelDeploymentCounterMaxAge {
			delete(h.previous, key)
		}
	}
	key := string(md.UID) + "/" + string(pod.UID) + "/" + scope
	previous, ok := h.previous[key]
	h.previous[key] = modelDeploymentPreviousCounter{value: value, at: at}
	return previous, ok && at.Sub(previous.at) > 0 && at.Sub(previous.at) <= modelDeploymentCounterMaxAge
}
