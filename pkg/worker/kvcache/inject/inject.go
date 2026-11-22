// 本文件是该包唯一的入口点，以及在分发给具体引擎前执行的各项前置校验。
package inject

import (
	"slices"
	"strings"

	core "k8s.io/api/core/v1"
)

// Result 表示渲染结果，即由调用方应用到本包不可见对象上的各组值。
//
// 本包不直接执行任何修改。调用方持有目标对象——准入阶段的 Pod 或其他场景下工作负载的 Pod 模板——
// 并决定每一部分如何落到对象上，从而使同一渲染器可服务于修改不同资源类型的调用方。
type Result struct {
	// Env 是目标容器所需的环境变量，作为期望值提供。
	//
	// 除 TenantEnvName 外，调用方会保留容器已声明的值。TenantEnvName 的值来自解析后的 Binding，
	// 必须覆盖所有重复声明，以确保工作负载无法自行选择其他复用域。
	Env []core.EnvVar

	// DefaultedEnv 是仅在目标容器未声明同名变量时才注入的环境变量。
	// 与 Env 不同，此处绝不覆盖容器已声明的值，因为每一项都是工作负载可以合理自行指定的默认值。
	DefaultedEnv []core.EnvVar

	// Args 按顺序追加到目标容器的 args 末尾。
	//
	// 两个引擎都需要一个没有对应环境变量的参数，因此该字段绝不会为空。调用方负责满足一个本包无法检查的前置条件：
	// 既未声明 command 也未声明 args 的容器绝不能接收这些参数，因为此时 Kubernetes 会将 args 视为完整命令行并丢弃镜像自带的入口命令。
	Args []string

	// DefaultedArgs 是追加在 Args 之后的参数组，每组均以其标志开头。当容器自身参数中已包含该标志
	// （无论是 "--flag value" 还是 "--flag=value" 形式）时，调用方应丢弃整组参数：每一组都是工作负载可合理自行指定的默认值，
	// 且对于仅用于开启功能的开关，后续追加条目无法将其关闭。
	DefaultedArgs [][]string

	// Volumes 添加到 Pod 的 spec 中，VolumeMounts 添加到目标容器中。对于以环境变量作为载体进行注入的引擎，两者均为空。
	Volumes      []core.Volume
	VolumeMounts []core.VolumeMount

	// PodAnnotations 必须设置到接收上述字段的同一个 Pod（或同一个 Pod 模板）上。
	// 文件载体是 annotation 的 downwardAPI 投影，因此投影与它所读取的 annotation 是同一件事的两个部分：
	// 只挂载卷而不设置 annotation 会导致挂载出空文件。
	PodAnnotations map[string]string

	// TenantInjected 表示本次渲染是否生成了租户标识。它表示一次操作而非结果：引擎构建是否
	// 遵循该值在此处无法得知，因此下游任何组件都不能将其当作隔离性的保证。
	//
	// 对于 TenantEnvName，调用方会覆盖所有工作负载声明，因此为 true 意味着解析得到的租户
	// 已写入容器。TenantEnvName 标识该例外情况。
	TenantInjected bool

	// TenantEnvName 是传递租户值的环境变量名，当租户改由文件传递（或未生成租户）时为空。
	// 调用方使用它将 Binding 的租户值覆盖工作负载中声明的同名环境变量。
	TenantEnvName string

	// Ports 是由生成的配置额外开放的容器端口。
	Ports []core.ContainerPort

	// KVEvents 是消费者可访问的事件流，当禁用事件发布时不存在。
	KVEvents *KVEvents

	// KVTransfer 报告渲染后的传输配置文档中是否包含点对点传输分支。
	KVTransfer bool
}

// KVEvents 是随引擎绑定配置一同生成的可拨号缓存事件契约。
//
// Endpoint 为按角色划分的 ClusterIP Service，建立的连接会固定到单个后端 Pod：当存在多个
// 副本时，消费者只能看到一个生产者的事件流。因此托管路由器通过按 Pod 发现机制而非此端点来
// 消费事件流。ZMQ 发布端绑定通配地址且不做任何认证，因此事件平面依赖网络隔离——它与数据平面
// 处于同一信任域中。
type KVEvents struct {
	Endpoint       string
	ReplayEndpoint string
	Topic          string
}

// KVEventsPorts 返回 vLLM ZMQ 发布端及其重放端点使用的端口。
func KVEventsPorts() []core.ContainerPort {
	return []core.ContainerPort{
		{Name: "kv-events", Protocol: core.ProtocolTCP, ContainerPort: VLLMKVEventsPort},
		{Name: "kv-replay", Protocol: core.ProtocolTCP, ContainerPort: VLLMKVEventsReplayPort},
	}
}

// cachePrefixSeparators 是 vLLM 和 SGLang 用于拼接存储键各部分的字符。
const cachePrefixSeparators = "@_:"

// Render 将已解析的输入转换为单个容器使用 KVCache 池所需的配置产物。
//
// 它是基于值的纯函数：不依赖 Kubernetes 客户端、不接受 ctx、也不读取集群状态。决定要渲染什么
// 属于调用方的职责；该函数仅负责将该决策转换为配置产物。
//
// 它会明确拒绝而非近似处理。此处的每一项拒绝都对应这样一种情况：若勉强渲染配置，容器虽能
// 正常启动且看似已配置完成，实则未使用缓存——这种故障从 Pod 外部不可见，因此值得显式报错失败。
//
// 复用域本身是一项输入，绝不会成为拒绝的理由。非空复用域始终会被渲染；引擎构建是否读取该值
// 属于镜像维护者的兼容性责任。
func Render(in Input) (*Result, error) {
	if !slices.Contains(Engines(), in.Engine) {
		return nil, newRefusal(ReasonEngineUnknown,
			"engine %q is not one this operator can configure; set one of %v", in.Engine, Engines())
	}

	hasStore := in.Connection.MasterAddress != "" || in.Connection.Protocol != ""
	if hasStore && in.Connection.MasterAddress == "" {
		return nil, newRefusal(ReasonConnectionIncomplete,
			"the pool published no client endpoint; there is no address for engine %q to connect to",
			in.Engine)
	}
	if hasStore && in.Connection.Protocol == "" {
		return nil, newRefusal(ReasonConnectionIncomplete,
			"the backend published no transport; it is written explicitly because the engines "+
				"disagree on the default, so omitting it would pick one of them at random")
	}
	// 在此处而非任一调用方拒绝，因为这里是两者的必经之处：Pod 准入 Webhook 根据注解渲染，而
	// ModelDeployment 调谐器根据推导出的加速器渲染，目前只有其中一方能识别 vLLM-Ascend。仅在
	// 能识别的调用方做检查会导致另一方放行该组合，而在两处都做检查则会变成同一张表的两份实现。
	if hasStore {
		if err := checkTransport(in.Engine, in.Connection.Protocol); err != nil {
			return nil, err
		}
	}
	// encoder 角色豁免于以下两项拒绝检查。该豁免针对角色而非引擎，因为 encoder 所渲染的分支
	// 并非本包所连接的缓存：该 Pod 上既没有存储，也没有点对点 KV 传输和事件流。给定引擎是否
	// 支持渲染该角色仍由该引擎的渲染器决定，它在写入任何内容前会再次对照 SupportsRole 进行校验。
	if !hasStore && !in.KVTransfer && !in.PublishKVEvents && in.Role != RoleEncode {
		return nil, newRefusal(ReasonConnectionIncomplete,
			"no shared store, point-to-point transfer, or KV event publisher was requested")
	}
	if !hasStore && in.Role != RoleEncode &&
		in.Engine != EngineVLLM && in.Engine != EngineVLLMAscend &&
		(in.Engine != EngineSGLang || !in.KVTransfer) {
		return nil, newRefusal(ReasonConnectionIncomplete,
			"engine %q requires a shared store connection", in.Engine)
	}
	// 点对点传输由整个 vLLM 系列以及 SGLang 各自以自己的参数词汇渲染；KV 事件发布则仅限
	// vLLM 原生引擎。引擎无法渲染的能力会在此处被直接拒绝，而非由该引擎的渲染器静默忽略。
	//
	// 选择拒绝而非丢弃，正是因为静默丢弃会导致本包致力于避免的故障：若向引擎请求其无法渲染的
	// 能力，引擎会正常启动、正常提供服务，却不会迁移任何块——而且 Pod 内没有任何状态能反映这一点。
	// 这种组合在现实中是可以触发的，并非理论假设：路由器的指标契约已涵盖 SGLang，因此用户如今
	// 就能写出基于 SGLang 池的托管路由器配置。
	//
	// vLLM 渲染器会基于同时覆盖角色的条件再次拒绝点对点传输。这并非与此处重复：此处的检查
	// 针对引擎本身且对所有调用方生效，而那处的检查针对既非 prefill 也非 decode 的角色，且仅在
	// 确定引擎为 vLLM 后才会触发。
	if in.KVTransfer && in.Engine != EngineVLLM && in.Engine != EngineVLLMAscend && in.Engine != EngineSGLang {
		return nil, newRefusal(ReasonRoleUnsupported,
			"engine %q renders no point-to-point transfer; asking for one would leave a container "+
				"that starts and moves nothing", in.Engine)
	}
	if in.PublishKVEvents && in.Engine != EngineVLLM {
		return nil, newRefusal(ReasonRoleUnsupported,
			"engine %q renders no KV event publishing; asking for it would leave a container that "+
				"starts and moves nothing", in.Engine)
	}

	if strings.ContainsAny(in.CachePrefix, cachePrefixSeparators) {
		return nil, newRefusal(ReasonCachePrefixInvalid,
			"cache prefix %q holds one of %q, the separators the engines join store keys with",
			in.CachePrefix, cachePrefixSeparators)
	}

	switch in.Engine {
	case EngineVLLM, EngineVLLMAscend:
		return renderVLLMWithEncoderCache(in, in.EncoderCache)
	case EngineSGLang:
		return renderSGLang(in)
	default:
		// 不可达分支：上方的 Engines 检查接受的取值与本 switch 覆盖的分支完全一致。
		return nil, newRefusal(ReasonEngineUnknown, "engine %q has no renderer", in.Engine)
	}
}
