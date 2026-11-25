// 本文件为完全通过命令行参数配置的单进程路由器渲染配置：vLLM 自带路由器与 SGLang 模型网关。
// 两者均不读取配置文件，所有配置都通过命令行标志传入。
//
// 两者共享同一个渲染器而非各写一套，因为它们的命令行参数面高度一致：监听地址、端口、基于
// Kubernetes 服务发现的选择器/命名空间/目标端口、按角色选择器以及 Prometheus 地址和端口在两款
// 路由器上的拼写完全相同。少量差异可直接收敛为下方的 argvRouter 数据表。
//
// 上游源码位置均附带版本标签与符号名。这些标志在两个项目中都对应同一个 clap 结构体上的字段，
// 因此改名会体现为字段改名而非行号漂移。
package router

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"gpustack.ai/gpustack/pkg/utils/json"
)

const (
	// 两个项目拼写完全相同的标志，分别对应 `vllm-project/router@v0.1.15:src/main.rs:99-341` 与
	// `sgl-project/sglang@gateway-v0.3.1:sgl-model-gateway/src/main.rs:134-263` 中的 CliArgs 字段，
	// clap 根据字段名推导出以下拼写。
	argvRouterHostArg          = "--host"
	argvRouterPortArg          = "--port"
	argvRouterMetricsHostArg   = "--prometheus-host"
	argvRouterMetricsPortArg   = "--prometheus-port"
	argvRouterDiscoveryArg     = "--service-discovery"
	argvRouterDiscoveryNSArg   = "--service-discovery-namespace"
	argvRouterDiscoveryPortArg = "--service-discovery-port"
	argvRouterSelectorArg      = "--selector"
	argvRouterPrefillSelectArg = "--prefill-selector"
	argvRouterDecodeSelectArg  = "--decode-selector"
	argvRouterRequestTimeout   = "--request-timeout-secs"

	// argvRouterBindAddress 用于替代回环默认地址。虽然只有 vLLM router 默认把 API
	// （`vllm-project/router@v0.1.15:src/main.rs:98-99` 的 `CliArgs::host`）与指标服务
	// （`:239-240` 的 `CliArgs::prometheus_host`）绑定在 127.0.0.1，而 gateway 默认已绑定全网卡，
	// 此处仍为两者统一显式声明，避免某侧依赖巧合的默认值而在未来悄然失效。
	argvRouterBindAddress = "0.0.0.0"

	// argvRouterEPDConfigArg 承载 vLLM router 的编码器阶段配置，另一款路由器没有此标志。上游将其
	// 定义为接收 JSON 文档的单个可选字符串（PR 266，合入于
	// `pack/llm-router/patches/vllm-router/0001-epd-routing.patch` 的 `CliArgs::epd_config`），
	// 因此整个阶段对应单个参数而非可重复标志。
	argvRouterEPDConfigArg = "--epd-config"

	// argvRouterDPAwareArg 开启 SGLang gateway 的 attention-data-parallel worker 注册：开启后
	// gateway 会按每个 dp rank 展开本地 worker，按成员而非按组注册。上游自 sgl-project/sglang
	// a9c97c9f69 起支持，合入于 `pack/llm-router/patches/sglang-gateway/0003-attn-dp-aware-worker-registration.patch`，
	// 仅 gateway 具备该标志。
	argvRouterDPAwareArg = "--dp-aware"
)

// argvRouter 描述此类单进程路由器各自特有的差异项，未列出的部分均由公共渲染逻辑共享。
type argvRouter struct {
	// Binary 是同一镜像中该路由器对应的可执行程序名。
	//
	// 这里只写裸程序名而不写绝对路径，这样用户指定自定义镜像时只需把程序放在 PATH 下，而无需
	// 与本项目镜像里的路径完全一致。
	Binary string

	// DisaggregationArg 用于开启 prefill/decode 分离模式。两个项目对该开关的拼写不同，这也是
	// 分离模式下两者唯一的差异。
	DisaggregationArg string

	// Extra 是仅该路由器拥有的标志，无论是否拆分均会输出，用以固定本会被隐式继承的默认值。
	Extra []string
}

var argvRouters = map[string]argvRouter{
	VLLM: {
		Binary: "vllm-router",
		// `vllm-project/router@v0.1.15:src/main.rs:114-115` 的 `CliArgs::vllm_pd_disaggregation`。
		DisaggregationArg: "--vllm-pd-disaggregation",
		// 本项目使用的 KV 传输后端。上游默认将其设为 NIXL（`:339-340` 的 `CliArgs::kv_connector`、
		// `KvConnector::Nixl`），若沿用默认值，路由器会照常转发却永远不触发 Mooncake 传输——故障
		// 源于默认值而非代码缺陷。gateway 没有此标志，其传输后端由引擎自身命令行决定。
		Extra: []string{"--kv-connector=mooncake"},
	},
	SGLang: {
		Binary: "sgl-model-gateway",
		// `sgl-project/sglang@gateway-v0.3.1:sgl-model-gateway/src/main.rs:191-192` 的
		// `CliArgs::pd_disaggregation`。
		DisaggregationArg: "--pd-disaggregation",
		// 无需额外标志，也不传模型名。开启服务发现本身就会启用该 gateway 的 inference-gateway
		// 模式（`:1056-1060`），该模式按各 worker 自行上报的 model id 路由，而不是按启动参数路由。
		// 其 --model-path 会加载分词器（`:937` 的 `maybe_model_path`），会迫使路由器 Pod 启动时
		// 访问模型仓库去拉取一个路由并不依赖的名字。
	},
}

func argvRenderer(name string) func(Input) (Output, error) {
	return func(input Input) (Output, error) {
		return renderArgvRouter(name, input)
	}
}

// renderArgvRouter 为单进程路由器生成完整命令行参数。
//
// 这里显式声明的都是默认值会导致静默失败的标志：监听地址若留在回环网卡上，前置 Service 将
// 指向网络命名空间外不可达的进程；省略服务发现命名空间会让路由器尝试 watch 全集群，而被本
// operator 渲染出的命名空间级 Role 拒绝，最终一个端点都发现不到；省略目标端口则会去拨号路由器
// 自行假定的端口，而非模型服务实际监听的端口。
func renderArgvRouter(name string, input Input) (Output, error) {
	delta, known := argvRouters[name]
	if !known {
		return Output{}, fmt.Errorf("render %s: no argument surface for this router", name)
	}
	if input.Namespace == "" {
		return Output{}, fmt.Errorf("render %s: no discovery namespace", name)
	}
	if input.DiscoveryPort <= 0 {
		return Output{}, fmt.Errorf("render %s: no discovery port", name)
	}

	args := []string{
		argvRouterHostArg + "=" + argvRouterBindAddress,
		argvRouterPortArg + "=" + strconv.Itoa(int(input.ListenPort)),
		argvRouterMetricsHostArg + "=" + argvRouterBindAddress,
		argvRouterMetricsPortArg + "=" + strconv.Itoa(int(input.MetricsPort)),
	}
	// 缺省时沿用路由器自身默认值（两款单进程路由器均为半小时，而 Envoy 代理为一天）。不渲染代表
	// 使用默认值，显式渲染则让配置在切换路由器时保持一致。
	if input.RequestTimeoutSeconds != nil {
		args = append(args,
			argvRouterRequestTimeout+"="+strconv.Itoa(int(*input.RequestTimeoutSeconds)))
	}
	args = append(args, delta.Extra...)
	args = append(args,
		argvRouterDiscoveryArg,
		argvRouterDiscoveryNSArg+"="+input.Namespace,
		argvRouterDiscoveryPortArg+"="+strconv.Itoa(int(input.DiscoveryPort)),
	)

	prefill, decode := roleSelector(input, rolePrefill), roleSelector(input, roleKindDecode)
	if prefill != nil && decode != nil {
		// 分离形态。vLLM router 要求开启分离模式时必须提供查找两侧的方式之一，而基于服务发现的
		// 按角色选择器能在扩缩容后保持有效（`vllm-project/router@v0.1.15:src/main.rs:403-415`）。
		// 两个选择器按并集匹配：Pod 命中任一选择器即被纳入（`src/service_discovery.rs:88-95`）。
		args = append(args, delta.DisaggregationArg)
		// encoder 作为静态列表传入，且仅由 vLLM router 接收。其 --epd-config 是上游支持的完整
		// 编码器阶段接口（PR 266）：启动时读取一次的 JSON 文档，encoder_urls 不会动态刷新也不做
		// 健康检查，且上游文档注明动态 prefill/decode 发现与 EPD 的组合限制。因此两侧仍用选择器，
		// 仅编码器传入静态 Service URL 列表。SGLang gateway 在此不输出任何参数，它的 prefiller
		// 通过引擎命令行上的 --encoder-urls 自行拨号 encoder。
		//
		// 必须放在两个选择器之前：两者都是列表标志，会吞掉后续所有不以短横线开头的 token（见
		// selectorArgs），因此配置文档放在前面避免后续标志被误认为列表项。
		//
		// 保持不开启 gateway 模式：上游明确拒绝同时启用 EPD 与 inference-gateway 模式，本渲染器
		// 在任何输入下都不会渲染 --enable-igw。
		if name == VLLM && (declaresRole(input, roleKindEncode) || len(input.EncoderURLs) > 0) {
			config, err := epdConfigArg(name, input)
			if err != nil {
				return Output{}, err
			}
			args = append(args, config)
		}
		args = append(args, selectorArgs(argvRouterPrefillSelectArg, prefill)...)
		args = append(args, selectorArgs(argvRouterDecodeSelectArg, decode)...)

		return Output{Command: []string{delta.Binary}, Arguments: args}, nil
	}

	// 未拆分形态：用单个选择器覆盖所有响应请求的 Pod。
	//
	// 若此时存在 encoder 角色则直接拒绝，而不是把它排除在选择器之外：这个部署级选择器代表整个
	// 作答集合，encoder 位于其中却不能生成回答（它只接收媒体并返回供 prefiller 读取的嵌入），
	// 把它当作后端只会让请求永远无法完成。需要编码器时必须搭配 prefill 与 decode 走上方的分离形态。
	if declaresRole(input, roleKindEncode) {
		return Output{}, fmt.Errorf("render %s: a deployment-wide selector would offer the %s role as "+
			"a backend, so a deployment holding one needs a %s and a %s role beside it",
			name, roleKindEncode, rolePrefill, roleKindDecode)
	}
	// External DP 形态要求按成员注册 worker：当注意力并行宽度大于等于 2 时，不带该标志会让
	// gateway 只注册每组一个 worker 而遗漏从属成员。仅 SGLang gateway 具备该标志，且仅适用于
	// 未拆分形态——分离模式下同时启用 dp-aware 会被 gateway 拒绝（实测会产生无效的 decode 目标）。
	if name == SGLang && input.AttentionDataParallelSize >= 2 {
		args = append(args, argvRouterDPAwareArg)
	}
	deployment := selectorArgs(argvRouterSelectorArg, input.EndpointLabels)
	if deployment == nil {
		return Output{}, fmt.Errorf("render %s: no endpoint labels to discover by", name)
	}

	return Output{Command: []string{delta.Binary}, Arguments: append(args, deployment...)}, nil
}

// selectorArgs 将标签按排序后的顺序逐项渲染为独立参数。
//
// 选择器不能拼成逗号分隔的单个字符串：该标志接收参数列表（`num_args = 0..`），每项按第一个 "="
// 拆分，不含 "=" 的项会被静默丢弃（`vllm-project/router@v0.1.15:src/main.rs:345-355` 的
// `CliArgs::parse_selector`）。排序保证相同输入每次渲染出完全一致的命令行，避免 Deployment 反复滚动。
//
// 它们必须放在命令行末尾：列表型标志会一直消费后续参数直到下一个以短横线开头的标志，放在中间会
// 把后面的标志值误吞为选择器条目。
func selectorArgs(flag string, labels map[string]string) []string {
	if len(labels) == 0 {
		return nil
	}

	out := make([]string, 0, len(labels)+1)
	out = append(out, flag)
	for _, key := range slices.Sorted(maps.Keys(labels)) {
		out = append(out, key+"="+labels[key])
	}

	return out
}

// rolePrefill 是本包接收到的 prefill 角色类型拼写，与 roleKindDecode 出于相同原因采用字符串常量。
const rolePrefill = "prefill"

// declaresRole 报告部署是否声明了指定类型的角色，无论该角色的选择器内容如何。
//
// 它与 roleSelector 职责分离：roleSelector 回答“该角色匹配哪些 Pod”（用于构造标志），而本函数
// 回答“该角色是否存在”（用于构造拒绝判断）。即使角色声明了空选择器，它依然存在，不能将其当作
// 不存在而静默放行。
func declaresRole(input Input, kind string) bool {
	for i := range input.Roles {
		if input.Roles[i].Kind == kind {
			return true
		}
	}

	return false
}

func roleSelector(input Input, kind string) map[string]string {
	for i := range input.Roles {
		if input.Roles[i].Kind == kind && len(input.Roles[i].Selector) > 0 {
			return input.Roles[i].Selector
		}
	}

	return nil
}

// epdConfig 是 --epd-config 承载的 JSON 文档，仅包含编码器端点列表。上游解析时会拒绝未知字段，
// 因此用结构体而非 map，避免键名拼错酿成路由器启动失败而非渲染报错。
type epdConfig struct {
	EncoderURLs []string `json:"encoder_urls"`
}

// epdConfigArg 为 vLLM router 的编码器阶段渲染 --epd-config 参数。
//
// 该契约要求全有或全无：声明了 Encode 角色却没有端点会被拒绝（上游拒绝空 encoder_urls 启动），
// 传入端点却未声明 Encode 角色同样被拒绝（避免静默丢弃）。每个端点都必须带 http:// 或 https://
// 协议头，因为上游会在启动时严格校验。
//
// 此处刻意不写入 consumer_zmq_addrs：上游将其视为可选字段，仅用于向逐个注册的编码器缓存连接器
// 传入各 prefiller 地址；当两侧通过选择器发现时并不存在这份静态地址列表，写出空值反而会误导。
func epdConfigArg(name string, input Input) (string, error) {
	if !declaresRole(input, roleKindEncode) || len(input.EncoderURLs) == 0 {
		return "", fmt.Errorf("render %s: the %s role and its encoder endpoints must be declared together",
			name, roleKindEncode)
	}
	for _, url := range input.EncoderURLs {
		if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
			return "", fmt.Errorf("render %s: encoder endpoint %q is not an http(s) URL", name, url)
		}
	}
	encoded, err := json.Marshal(epdConfig{EncoderURLs: input.EncoderURLs})
	if err != nil {
		return "", fmt.Errorf("render %s: encoder stage configuration: %w", name, err)
	}

	return argvRouterEPDConfigArg + "=" + string(encoded), nil
}
