// Package router 为 ModelDeployment 管理的上游路由器渲染配置。
package router

import (
	"fmt"
	"slices"

	"sigs.k8s.io/yaml"
)

// IMPORTANT: 三款路由器的纯配置渲染入口 + llm-d-router 专用渲染器

// 本包为其渲染配置的路由器名称，对应 API 枚举的值。这里直接声明而非导入，是因为
// 拒绝消息会插值引用它们；如果从 API 导入，就会使本渲染器依赖它本应保持独立的类型包。
const (
	LLMD   = "llm-d-router"
	VLLM   = "vllm-router"
	SGLang = "sglang-gateway"
)

// Role 表示路由器可见的一类稳定的模型服务端点。
type Role struct {
	Kind         string
	RoleLabelKey string

	// Selector 以标签等式集合的形式指明该角色下响应 API 的 Pod。
	//
	// 此处采用 map 而非选择器字符串，因为通过 argv 配置的路由器只支持等式匹配并自行解析输入：
	// vLLM router 按第一个 "=" 拆分每个条目，不含 "=" 的条目会被静默丢弃且不输出任何日志
	// （`vllm-project/router@v0.1.15:src/main.rs:345-355` 中的 `CliArgs::parse_selector`，
	// 在 `src/service_discovery.rs:74-82` 中匹配）。若传入完整表达式，非等式条件会被悄然丢弃，
	// 因此一开始就只向渲染器提供它能够表达的数据结构。
	Selector map[string]string
}

// Metrics 命名路由器所消费的模型服务指标。
type Metrics struct {
	Engine             string
	Port               int32
	QueuedRequests     string
	RunningRequests    string
	KVCacheUtilization string

	// CacheInfo 命名一个 info 风格的 gauge，其 block_size 与 num_gpu_blocks 标签携带
	// KV cache 容量。CacheBlockSize 和 CacheNumBlocks 则分别命名直接携带这两个值的 gauge，
	// 适用于以该方式导出指标的引擎。每个引擎只填充其中一种形式或均留空。
	CacheInfo      string
	CacheBlockSize string
	CacheNumBlocks string
}

// KVEvents 配置按 Pod 发现 KV 事件发布端的参数。
type KVEvents struct {
	Engine     string
	Port       int32
	ReplayPort int32
	Topic      string
}

// Input 仅包含稳定的配置项。实时 Pod 地址与副本数不应出现在这里，否则普通的扩缩容都会导致
// 路由器重启。
type Input struct {
	Roles             []Role
	Metrics           Metrics
	ModelName         string
	TokenizerEndpoint string
	Namespace         string
	EndpointSelector  string
	KVEvents          KVEvents

	// EndpointLabels 是 EndpointSelector 的纯等式形式，指明该部署下所有响应 API 的 Pod
	// （不区分角色）。对于本包中的路由器，两者表达相同集合；同时保留两者是有意为之，原因见
	// Role.Selector 为何采用 map 而非字符串。
	EndpointLabels map[string]string

	// AttentionDataParallelSize 是部署的注意力数据并行宽度，来自引擎自身参数的声明，与
	// External DP 分类器读取的是同一宽度。当该值大于等于 2 时，SGLang gateway 会渲染
	// --dp-aware，从而按每个 attention rank 而非每个副本组注册 worker
	// （`pack/llm-router/patches/sglang-gateway/0003-attn-dp-aware-worker-registration.patch`）；
	// 为 0 或 1 时不渲染任何参数，其余路由器不读取该字段。
	AttentionDataParallelSize int32

	// EligibleLabelKey 指定当资格要求按角色而非部署级选择器承载时，阶段过滤器必须匹配的标签键。
	// 非空时，会对所有 Selector 已携带该键的角色（即门控可判定的角色集合）在其阶段过滤器中补充
	// 该条件；为空时所有阶段过滤器保持原样渲染。
	//
	// 该键由调用方传入而非在此处声明，因为它属于 operator 自身的词汇表，在本包中再写一份拼写
	// 容易产生漂移。
	EligibleLabelKey string

	// CandidateLabelKey 与 CandidateLabelValue 分别指定标记路由候选者的标签键与值。仅在服务发现
	// 采用候选标记模式（即部署级选择器需要同时覆盖资格要求不同的多个角色）时设置。
	//
	// 此时阶段过滤器必须接纳该角色类型的候选并集，而不是单个角色的资格要求：过滤器按角色类型
	// 生效，同一类型的多个角色共用同一个过滤器，若写入其中一个角色的资格要求，就会把同类型另一
	// 角色的候选者排除在外。候选标记由该类型所有可路由候选者共同携带，因而能完整接纳它们。
	//
	// 键为空时，所有阶段过滤器保持原有渲染方式不变。
	CandidateLabelKey   string
	CandidateLabelValue string

	// EncoderURLs 是部署中编码器阶段的 HTTP(S) 端点列表，供需要静态编码器列表的路由器使用。
	// 每项均为 encoder 角色的角色级 Service URL 而非单个副本地址：Service 代理全部副本，因此
	// 扩缩容不会改动该列表，也不会触发路由器重启。未声明 Encode 角色时为 nil。
	EncoderURLs []string

	// ListenPort 是路由器自身提供 API 的监听端口，即前置 Service 转发的目标端口。
	ListenPort int32

	// MetricsPort 是路由器暴露 Prometheus 指标的端口。
	MetricsPort int32

	// DiscoveryPort 是连接已发现 Pod 的端口，即模型服务器实际提供服务的端口。若任由路由器沿用
	// 默认值，它会去拨号并未监听的 80 端口。
	DiscoveryPort int32

	// RequestTimeoutSeconds 指定等待上游响应的超时时间（秒）。缺省时各路由器保持各自的默认值，
	// 它们之间并不统一（详见 API 字段说明）。
	RequestTimeoutSeconds *int32

	// DisaggregationThreshold 指定触发拆分的未命中缓存提示词 token 阈值。缺省时沿用渲染器自身
	// 默认值；显式设为 0 会关闭拆分，与上游决策器行为一致。
	DisaggregationThreshold *int32
}

// roleKindDecode 是本包接收到的 decode 角色类型拼写。
//
// 这里使用字符串是因为本包以字符串作为输入：角色类型已由调用方解析完毕，从而让路由器渲染器不
// 依赖 API 类型包。代价是该拼写需要通过约定与 API 枚举保持一致，因此本包内所有比对均复用这一
// 常量，避免出现第二份可能漂移的拼写。
const roleKindDecode = "decode"

// roleKindEncode 是本包接收到的 encode 角色类型拼写，与 roleKindDecode 出于相同原因在此命名。
const roleKindEncode = "encode"

// llmdMultimodalDecider 是让编码器阶段按需触发的插件：请求携带图片、音频或视频内容时激活，
// 纯文本请求则拒绝进入编码阶段，这就是无媒体路径的全部机制——此时 decode 阶段已选定 Pod，拒绝
// 后请求会直接走向 prefiller 与 decoder（见 llm-d/llm-d-router@v0.11.0
// `pkg/epp/framework/plugins/scheduling/profilehandler/disagg/always_disagg_mm_decider.go:29`
// 及 `docs/disaggregation.md` 中的 E/P/D 配置）。
const llmdMultimodalDecider = "always-disagg-multimodal-decider"

// llmdAlwaysDisaggPDDecider 将所有请求都拆分给 prefiller 与 decoder，不接受任何参数
// （见 llm-d/llm-d-router@v0.11.0
// `pkg/epp/framework/plugins/scheduling/profilehandler/disagg/always_disagg_pd_decider.go:28`，
// 注册于 `cmd/epp/runner/runner.go:612`）。
//
// SGLang 必须使用该插件，因为其 decode 端无法独立执行 prefill：处于 decode 模式的服务器会对
// 任何未携带 bootstrap room id 的请求返回 HTTP 400。若决策器因提示词较短、命中缓存或分词
// 失败而跳过 prefiller，请求就会发给一台无法作答的服务器。vLLM 的 decode 端加载了完整模型
// 并能就地执行 prefill，因此继续沿用基于前缀缓存的决策器。
const llmdAlwaysDisaggPDDecider = "always-disagg-pd-decider"

// llmdEngineSGLang 是本包接收到的 SGLang 引擎名称拼写。
const llmdEngineSGLang = "sglang"

// llmdStageFilterType 是按标签过滤端点的 picker 插件。在此命名以确保渲染器与读取其
// 自身输出的组件对拼写达成一致。
const llmdStageFilterType = "label-selector-filter"

// DocumentStageFilterDemands 报告已渲染的 picker 文档中是否包含一个 label-selector
// 过滤器，且其 matchLabels 要求某个标签等于某个值。
//
// 这里是向解析后的文档而非原始文本发问，因为同一段字符串里也渲染了用户提供的模型名称。
// 文档仅仅包含该标签的键并不构成一项要求，若将其当作要求则会激活无人决策过的关卡。
// stage filter 自身的 matchLabels 是文档中唯一声明对 Pod 要求的地方，因此这里只检查该处。
//
// 无法解析的文档以及未声明过滤器的文档不提出任何要求。
func DocumentStageFilterDemands(document, labelKey, value string) bool {
	var parsed struct {
		Plugins []struct {
			Type       string `json:"type"`
			Parameters struct {
				MatchLabels map[string]string `json:"matchLabels"`
			} `json:"parameters"`
		} `json:"plugins"`
	}
	if err := yaml.Unmarshal([]byte(document), &parsed); err != nil {
		return false
	}
	for _, plugin := range parsed.Plugins {
		if plugin.Type != llmdStageFilterType {
			continue
		}
		if plugin.Parameters.MatchLabels[labelKey] == value {
			return true
		}
	}

	return false
}

// Output 表示渲染完成的路由器配置。每个路由器要么通过启动时读取的配置文件进行配置，要么完全
// 通过命令行参数进行配置，因此两半中必须且只能填充其中之一，Render 会拒绝其他一切形态。
//
// 两半放在同一个类型中而非按路由器各定义返回类型，是因为调用方在运行期根据字段选择路由器，
// 否则每次渲染后都要做类型断言才能知道拿到了什么。
type Output struct {
	// Command 是要执行的程序，所有路由器都必须提供，因为同一个镜像包含三个路由器二进制。
	//
	// 此处显式渲染而不留给镜像默认值。镜像刻意不设 entrypoint：在镜像中指定其中一个会让其余
	// 两个只能靠覆盖 entrypoint 才能启动。因此在参数旁边直接给出二进制名，避免调用方只传参数
	// 时容器把第一个标志当作程序名执行，进而报出远离根因的 "executable file not found"。
	Command []string

	// Document 是路由器启动时读取的配置文件内容。如何投递给进程由调用方决定，不属于本包职责。
	Document string

	// Arguments 追加到路由器命令行，用于完全不读取配置文件的路由器。
	Arguments []string
}

// Render 返回所选上游路由器能够理解的配置。
//
// “二选一”的约束在此处统一校验而非分散在各个渲染器中，因为新增渲染器最常见的问题就是返回
// 零值：代码能编译、不渲染任何内容，并启动一个毫无配置却不报错的路由器。同时填充两半同样被
// 拒绝：路由器只会读取其中一半而静默忽略另一半，被忽略的部分就成了用户配置却从未生效的死配置。
func Render(name string, input Input) (Output, error) {
	render, ok := renderers[name]
	if !ok {
		return Output{}, fmt.Errorf("unsupported router %q", name)
	}

	output, err := render(input)
	if err != nil {
		return Output{}, err
	}
	switch {
	case len(output.Command) == 0:
		// 单独于下方的二选一分支检查，因为不论配置走文档还是走参数，每个路由器都必须有启动命令。
		// 遗漏命令会让容器启动后把第一个标志当成可执行文件名，从而报出指向该标志而非遗漏命令的错误。
		return Output{}, fmt.Errorf("router %q rendered no command", name)
	case output.Document == "" && len(output.Arguments) == 0:
		return Output{}, fmt.Errorf(
			"router %q rendered neither a configuration document nor arguments", name,
		)
	case output.Document != "" && len(output.Arguments) != 0:
		return Output{}, fmt.Errorf(
			"router %q rendered both a configuration document and arguments", name,
		)
	}

	return output, nil
}

var renderers = map[string]func(Input) (Output, error){
	LLMD:   renderLLMD,
	VLLM:   argvRenderer(VLLM),
	SGLang: argvRenderer(SGLang),
}

// IMPORTANT: 拼出 epp.yaml
func renderLLMD(input Input) (Output, error) {
	kinds := make(map[string]string, len(input.Roles))
	selectors := make(map[string]map[string]string, len(input.Roles))
	for _, role := range input.Roles {
		kinds[role.Kind] = role.RoleLabelKey
		selectors[role.Kind] = role.Selector
	}

	// SGLang 的 prefiller 会通过自身 --encoder-urls 直接拨号 encoder，因此在路由器看来
	// encoder 与 prefiller 同属一个阶段：先 encode+prefill，再 decode。此处移除 encoder，
	// 避免渲染出一个 decoder sidecar 并无对应连接器的阶段。
	sglang := input.Metrics.Engine == llmdEngineSGLang
	if sglang {
		delete(kinds, roleKindEncode)
	}

	orderedKinds := make([]string, 0, len(kinds))
	for kind := range kinds {
		orderedKinds = append(orderedKinds, kind)
	}
	slices.Sort(orderedKinds)

	// 提前计算该部署是否存在编码器阶段，因为下方两处决策都依赖它，避免在分支内重复声明遮蔽变量。
	_, encode := kinds[roleKindEncode]

	plugins := make([]any, 0, len(orderedKinds)+7)
	profiles := make([]any, 0, len(orderedKinds))
	for _, kind := range orderedKinds {
		filter := kind + "-pods"
		matchLabels := map[string]string{kinds[kind]: kind}
		// 可由门控判定的角色将资格要求放在此处的阶段过滤器中，而非部署级选择器中——后者是所有条件
		// 的单个逻辑与，无法表达“要求 encoder 具备资格但不要求 prefiller”。永远不会激活门控的角色
		// 根本不带该标签，若对其施加要求就会选不到任何 Pod；其 Selector 中不含该键，因此阶段过滤器
		// 保持不变，其 Pod 仍可被路由选中。
		switch {
		case input.CandidateLabelKey != "":
			// 候选标记模式按角色类型命名全部候选者，因此过滤器匹配候选标记而非资格标签。资格标签
			// 属于单个角色，而该过滤器按角色类型生效：在此要求资格标签会把同类型下无需门控的角色
			// 候选者误排除。
			matchLabels[input.CandidateLabelKey] = input.CandidateLabelValue
		default:
			if value, ok := selectors[kind][input.EligibleLabelKey]; ok {
				matchLabels[input.EligibleLabelKey] = value
			}
		}
		plugins = append(plugins, map[string]any{
			"name":       filter,
			"type":       llmdStageFilterType,
			"parameters": map[string]any{"matchLabels": matchLabels},
		})
		profilePlugins := []any{map[string]any{"pluginRef": filter}}
		if kind == roleKindEncode {
			// encoder 阶段不做打分，这一形态与上游保持一致：llm-d/llm-d-router@v0.11.0
			// `docs/disaggregation.md` 中的 E/P/D 配置仅给 encode profile 配置了角色过滤器。
			//
			// 这也是必然要求：encoder 不维护 KV cache，队列和 KV 缓存打分器读取它从不导出的指标，
			// 对所有候选者都会给出相同分数；前缀打分器依赖 token 重合度，而请求进入编码阶段前还
			// 没有 token。无法区分候选者的打分器不是弱偏好，而是每次媒体请求白白支付的开销。
			//
			// picker 仍然存在：上游会在加载时为未显式指定 picker 的 profile 自动追加
			// max-score-picker（`pkg/epp/config/loader/defaults.go:197-216`），且运行期会无条件
			// 解引用该 picker（`pkg/epp/scheduling/scheduler_profile.go:351`）。此处不重复写入，
			// 避免在参与配置哈希计算的文档里写两份相同信息。
			profiles = append(profiles, map[string]any{
				"name":    kind,
				"plugins": profilePlugins,
			})
			continue
		}
		profilePlugins = append(profilePlugins,
			map[string]any{"pluginRef": "queue-scorer", "weight": 2},
			map[string]any{"pluginRef": "kv-cache-utilization-scorer", "weight": 2},
			map[string]any{"pluginRef": "max-score-picker"})
		if kind != roleKindDecode {
			profilePlugins = append(profilePlugins,
				map[string]any{"pluginRef": "prefix-cache-scorer", "weight": 3})
		}
		profiles = append(profiles, map[string]any{
			"name":    kind,
			"plugins": profilePlugins,
		})
	}

	metricsSource := map[string]any{"type": "metrics-data-source"}
	if input.Metrics.Port > 0 {
		metricsSource["parameters"] = map[string]any{"port": input.Metrics.Port}
	}
	prefixProducer := "approx-prefix-cache-producer"
	if input.KVEvents.Port > 0 {
		prefixProducer = "precise-prefix-cache-producer"
		// 将不维护 KV cache 的所有角色从事件订阅集合中排除。排除条件按各角色自身的标签键构造，
		// 只有输入中实际存在的角色才会加入条件。这里在已有选择器后追加并做判空保护，防止空选择器
		// 拼出前导逗号（会被 Kubernetes 拒绝）。
		//
		// 固定先排除 decode 再排除 encode，因为该顺序会影响最终渲染出的 ConfigMap 内容及其哈希，
		// 调整顺序会导致集群中所有 prefill/decode 路由器无故滚动更新。
		//
		// 排除 encoder 的原因与排除 decode 一致：decoder 只消费 KV 不生产，encoder 根本不保存 KV，
		// 两者都不会发布事件；留在订阅集合里只会让订阅方去拨号引擎从未打开的端口。
		publisherSelector := input.EndpointSelector
		for _, kind := range []string{roleKindDecode, roleKindEncode} {
			key, ok := kinds[kind]
			if !ok {
				continue
			}
			exclusion := key + "!=" + kind
			if publisherSelector == "" {
				publisherSelector = exclusion
			} else {
				publisherSelector += "," + exclusion
			}
		}
		// 含 encoder 阶段的部署使用内置估算器，其余部署使用 HTTP 渲染器。指定 vLLM URL 会选用
		// llm-d-router 的 vllmHTTPRenderer，它会 POST 到 <url>/v1/chat/completions/render——
		// 该接口在 vLLM 0.30.0 仅在 --enable-scale-out 下开启，默认服务会返回 404 导致请求完全
		// 无法路由。不带参数的 token-producer 则选用内置估算器，可直接从 data: URL 读取 PNG/JPEG
		// 尺寸而无需 HTTP 调用。
		//
		// 在含 encoder 的场景下两者也不可互换：请求携带原始媒体发往 encoder，多模态决策器需要读取
		// 估算器产出的 MultiModalFeatures 才能判定启用编码阶段，而 HTTP 渲染器只能去问 prefiller，
		// 问错了对象。
		tokenProducer := map[string]any{"type": "token-producer"}
		if !encode {
			tokenProducer["parameters"] = map[string]any{
				"modelName": input.ModelName,
				"vllm":      map[string]any{"url": input.TokenizerEndpoint},
			}
		}
		plugins = append(plugins,
			tokenProducer,
			map[string]any{"type": "endpoint-notification-source"},
			map[string]any{
				"type": prefixProducer,
				"parameters": map[string]any{
					"kvEventsConfig": map[string]any{
						"topicFilter":  input.KVEvents.Topic,
						"concurrency":  4,
						"engineType":   input.KVEvents.Engine,
						"discoverPods": true,
						"podDiscoveryConfig": map[string]any{
							"podLabelSelector": publisherSelector,
							"podNamespace":     input.Namespace,
							"socketPort":       input.KVEvents.Port,
							"replaySocketPort": input.KVEvents.ReplayPort,
						},
					},
				},
			})
	} else {
		plugins = append(plugins, map[string]any{"type": prefixProducer})
	}
	prefixScorer := map[string]any{"type": "prefix-cache-scorer"}
	if prefixProducer == "precise-prefix-cache-producer" {
		prefixScorer["parameters"] = map[string]any{"prefixMatchInfoProducerName": prefixProducer}
	}
	plugins = append(plugins,
		map[string]any{"type": "queue-scorer"},
		map[string]any{"type": "kv-cache-utilization-scorer"},
		map[string]any{"type": "max-score-picker"},
		prefixScorer,
		metricsSource,
	)
	metricsExtractor := map[string]any{"type": "core-metrics-extractor"}
	if input.Metrics.Engine != "" {
		engineConfig := map[string]any{
			"name":                input.Metrics.Engine,
			"queuedRequestsSpec":  input.Metrics.QueuedRequests,
			"runningRequestsSpec": input.Metrics.RunningRequests,
			"kvUsageSpec":         input.Metrics.KVCacheUtilization,
		}
		// 在此处命名某款引擎会整体替换提取器内置的对应条目而非叠加
		// （`llm-d/llm-d-router@v0.10.0:pkg/epp/framework/plugins/datalayer/extractor/metrics/factories.go:185-194`），
		// 因此漏写的字段提取器绝不会读取。缓存容量指标仅在配合近似前缀生产者时写入，因为只有该插件
		// 用它来计算每个服务实例的前缀索引容量；在精确生产者旁写入无人读取的字段只会无谓触发路由器滚动。
		if prefixProducer == "approx-prefix-cache-producer" {
			for key, spec := range map[string]string{
				"cacheInfoSpec":      input.Metrics.CacheInfo,
				"cacheBlockSizeSpec": input.Metrics.CacheBlockSize,
				"cacheNumBlocksSpec": input.Metrics.CacheNumBlocks,
			} {
				if spec != "" {
					engineConfig[key] = spec
				}
			}
		}
		parameters := map[string]any{"engineConfigs": []any{engineConfig}}
		// 提取器根据 Pod 的 engine-type 标签识别引擎，没有该标签时按默认引擎读取；模型服务 Pod 均不
		// 携带该标签，因此所有 Pod 都会按默认引擎解析。若沿用上游默认值，SGLang 服务会被按 vLLM
		// 指标名去抓，因指标不存在导致所有端点都拿不到新鲜指标，队列与 KV 打分器给所有端点打同一分数。
		if input.Metrics.Engine != llmdDefaultMetricsEngine {
			parameters["defaultEngine"] = input.Metrics.Engine
		}
		metricsExtractor["parameters"] = parameters
	}
	plugins = append(plugins, metricsExtractor)

	if _, prefill := kinds["prefill"]; prefill {
		if _, decode := kinds["decode"]; decode {
			// 编码器阶段与 prefiller 放在一起装配，因为准入要求声明 encoder 时必须同时声明 prefill 与
			// decode：只带 encoder 而缺 prefiller 的部署早已被准入拒绝，不会到达此处。
			stageProfiles := map[string]string{"prefill": "prefill", "decode": "decode"}
			stageDeciders := map[string]string{"prefill": "prefix-based-pd-decider"}
			// 显式为 0 的阈值仍用于关闭拆分，且只有基于前缀的决策器遵循该阈值，因此“始终拆分”决策器
			// 仅在未关闭拆分时使用。
			alwaysSplit := sglang &&
				(input.DisaggregationThreshold == nil || *input.DisaggregationThreshold > 0)
			if alwaysSplit {
				stageDeciders["prefill"] = llmdAlwaysDisaggPDDecider
			}
			if encode {
				// 声明在 prefiller 决策器之前，与上游 E/P/D 配置顺序一致（llm-d/llm-d-router@v0.11.0
				// `docs/disaggregation.md` 中的 E/P/D Configuration）。
				plugins = append(plugins, map[string]any{"type": llmdMultimodalDecider})
				stageProfiles["encode"] = "encode"
				stageDeciders["encode"] = llmdMultimodalDecider
			}
			pdDeciderParams := map[string]any{
				"promptTokens": 0,
				// 0 在这里代表显式取值而非缺省：决策器遇到 0 阈值会在读取其余字段前直接返回“不拆分”，
				// 因此通过指针区分显式 0 与未设置。
				"nonCachedTokens": llmdNonCachedTokens(input.DisaggregationThreshold),
			}
			// 在含 encoder 的部署上必须明确告知 prefill 决策器由哪个生产者提供前缀匹配信息。启用精确
			// 生产者时，决策器从 prefixMatchInfoProducerName 读取 token 计数；若省略，它会回退查找
			// 近似生产者，而本文档并未定义近似生产者，导致路由器加载时创建一个未配置的实例并对每个请求
			// 打印 "failed to read prefix plugin state: not found"。
			if encode && prefixProducer == "precise-prefix-cache-producer" {
				pdDeciderParams["prefixMatchInfoProducerName"] = prefixProducer
			}
			pdDecider := map[string]any{
				"type":       "prefix-based-pd-decider",
				"parameters": pdDeciderParams,
			}
			if alwaysSplit {
				pdDecider = map[string]any{"type": llmdAlwaysDisaggPDDecider}
			}
			plugins = append(plugins,
				pdDecider,
				map[string]any{
					"type": "disagg-profile-handler",
					"parameters": map[string]any{
						"profiles": stageProfiles,
						"deciders": stageDeciders,
					},
				},
			)
		}
	}

	config := map[string]any{
		"apiVersion":         "llm-d.ai/v1alpha1",
		"kind":               "EndpointPickerConfig",
		"plugins":            plugins,
		"schedulingProfiles": profiles,
	}
	if prefixProducer == "precise-prefix-cache-producer" {
		config["dataLayer"] = map[string]any{"sources": []any{
			map[string]any{
				"pluginRef":  "metrics-data-source",
				"extractors": []any{map[string]any{"pluginRef": "core-metrics-extractor"}},
			},
			map[string]any{
				"pluginRef":  "endpoint-notification-source",
				"extractors": []any{map[string]any{"pluginRef": prefixProducer}},
			},
		}}
	}
	encoded, err := yaml.Marshal(config)
	if err != nil {
		return Output{}, fmt.Errorf("render llm-d configuration: %w", err)
	}

	// 仅输出文档部分：该路由器从文件读取插件、profile 和数据层配置，进程所需的命令行参数由对象
	// 渲染器写入，并非从此输入推导。
	return Output{Command: []string{"epp"}, Document: string(encoded)}, nil
}

// llmdDefaultMetricsEngine 是当配置未指定引擎时，picker 指标提取器为不含 engine-type 标签的 Pod
// 假定的默认引擎（见 `llm-d/llm-d-router@v0.10.0:pkg/epp/framework/plugins/datalayer/extractor/metrics/factories.go:148-149`、
// `:178-180` 与 `extractor.go:201-217`）。
//
// 对本身就是默认值的引擎刻意不显式写入：渲染出的文档会被哈希并写入路由器 Pod 注解，显式写出
// 上游已默认的值会导致所有既有 vLLM 路由器无谓滚动更新。代价是该常量需要与上游约定保持同步，
// 升级 picker 版本时应重新核对上述位置。
const llmdDefaultMetricsEngine = "vllm"

// llmdDefaultNonCachedTokens 是阈值未设置时 picker 决策器沿用的默认值，与本渲染器一贯输出的值一致。
const llmdDefaultNonCachedTokens int32 = 8

func llmdNonCachedTokens(threshold *int32) int32 {
	if threshold == nil {
		return llmdDefaultNonCachedTokens
	}

	return *threshold
}
