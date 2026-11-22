package worker

import (
	"fmt"
	"strings"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/utils/strconvx"
	"gpustack.ai/gpustack/pkg/worker/kvcache/inject"
)

// 已准入配置行可以指定的连接器名称。这些名称采用上游命名，因为当该链路出现异常时，用户在
// 引擎日志中看到的正是这些名称。
//
// ECExampleConnector 通过两个角色共同挂载的目录传递编码器输出。它是 Ascend 示例中唯一运行
// 的编码器缓存连接器，也是唯一不需要任何形式拷贝路径的连接器。
const ModelDeploymentECConnectorExample = "ECExampleConnector"

// ModelDeploymentECConnectorCPU 是 decode sidecar 驱动的网络 connector，通过 NIXL 从 encoder
// 的响应中拉取 embeddings。
const ModelDeploymentECConnectorCPU = "ECCPUConnector"

// ModelDeploymentECCPUConnectorFirstVLLM 是包含网络 encoder-cache connector
// （ECCPUConnector 和 ECMooncakeConnector）的 vLLM 版本。
// https://github.com/vllm-project/vllm/blob/v0.30.0/docs/features/ec_cpu_connector.md
const ModelDeploymentECCPUConnectorFirstVLLM = "0.30.0"

// ModelDeploymentECConnectorSGLangURLs 标识 SGLang 基于地址的 encoder 链路，即通过
// --encoder-urls 和 --encoder-bootstrap-port 将 encoder 的输出传输到 prefiller。
//
// 它不是 encoder-cache connector：SGLang 不配置 connector，而是由 prefiller 自行连接
// encoder 的地址。该名称与 cache connector 放在同一列，因为该列完整回答了某一行如何传递
// encoder 输出的问题。
const ModelDeploymentECConnectorSGLangURLs = "encoder-urls"

// ModelDeploymentSGLangEncoderFirst 是包含 --encoder-only、--language-only、--encoder-urls
// 和 --encoder-bootstrap-port 的最低 SGLang 版本。
// https://github.com/sgl-project/sglang/blob/v0.5.18/docs/docs/advanced_features/epd_disaggregation.mdx
const ModelDeploymentSGLangEncoderFirst = "0.5.10"

// ModelDeploymentECExampleConnectorFirstAscend 是该行可运行的最低 vllm-ascend 版本，
// 同时也是包含共享链路所需两个修复的最低版本，而非仅包含其中之一：
//
//   - PR 16682（commit ed0138b1a）在 dummy run 中跳过 language-model forward，缺少该修复时
//     `--mm-encoder-only` 会在启动时中止引擎。首次合入于 v0.28.0.rc1。
//   - PR 18291（commit 23d65455b）将 encoder cache 加载固定到对应 rank 自身的 NPU 上，缺少
//     该修复时，tensor parallelism 大于 1 的 prefill 会加载到错误的设备上。首次合入于
//     v0.30.0rc1。
//
// 该行之前使用的 v0.23.0 两个修复都不包含：encoder 会在启动时崩溃，而并行 prefill 会读取
// 其他节点的 embeddings。v0.30.0rc2 与 v0.30.0rc1 代码相同，仅修复了 Dockerfile，因此最低
// 版本取两者中较早的一个。
//
// 该版本号拼写沿用上游项目自身写法，而非 SemVer 规范：vllm-ascend 在此处将候选版本标记为
// `v0.30.0rc1`，而在上一行标记为 `v0.29.0.rc1`，准入逻辑会同时识别这两种写法。
const ModelDeploymentECExampleConnectorFirstAscend = "0.30.0rc1"

// ModelDeploymentEPDRow 表示一个获准入的组合：配对各角色的 router、提供服务的 engine、
// 运行所在的加速器平台（platform）、将 encoder 输出传递到 prefiller 的 connector，以及该
// connector 首次出现的最低 engine 版本。
//
// 这四列对应用户的配置项，每一列都曾实际拒绝过不合法的组合：无法驱动 encoder 角色的 router、
// 没有 encoder 参数的 engine、不存在对应 connector 的 platform，以及该 platform 无法承载
// 的 connector。因此，每一行即为完整的准入判定结果；未出现在此表中的组合会被直接拒绝，而不
// 是准入后等到容器启动时才暴露问题。
//
// 该定义放在此处而非读取它的准入规则旁，是因为除了准入规则之外，渲染器（render）也会读取它。
// engine 链路和 decode sidecar 都需要获知该部署运行的 connector，并且二者必须拿到同一行的
// 判定结果，而不是各自根据 platform 推导——基于同一 platform 做两次推导意味着对同一个问题
// 得出两个答案。admission 包已经导入本包以读取有效角色类型（effective role kind）等共享策略，
// 因此若将行定义放在 admission 包中，会导致本包反向导入调用方，这在 Go 中是不允许的。
type ModelDeploymentEPDRow struct {
	// Router 是配对三个角色的 router。
	Router string
	// Engine 是各角色运行的 engine。
	Engine string
	// Platform 是每个角色的 InstanceType 都必须报告的加速器厂商平台。
	Platform string
	// Connector 是该行所渲染的 encoder-cache connector。
	Connector string
	// EngineFloor 是该行所指明的 connector 对应的最低 engine 版本。
	//
	// 准入阶段不再读取该字段。它曾用于 encoder 版本门禁的比对下限，而该门禁已随
	// spec.engine.version 一同移除：由于现在由用户指定镜像而非版本号，operator 不再知晓镜像
	// 内携带的版本。该字段表述的约束本身并未失效，只是转移了位置——connector 仍然需要镜像内包含
	// 该版本的引擎库，而现在由用户自行选择并核对镜像。保留该字段是因为它记录了关于 connector 的
	// 客观事实（相关常量仍引用了其上游来源），且未来新增字段可能会读取它；该版本要求本身已记录在
	// docs/modules/model-deployment/engine-versions.md 中，供用户参考和执行。
	EngineFloor string
	// SharedVolume 记录 encoder 链路是否通过挂载目录而非网络到达 prefiller，这也是将 claim 规则
	// 应用于该行的依据。
	SharedVolume bool
}

// modelDeploymentEPDMatrix 保存所有获准入的 (router, engine, platform) 组合。
//
// Ascend 和 ROCm 的 vLLM 行作为设计基线：基于上游代码实现，尚未在硬件上验证。NVIDIA vLLM
// 行（位于 llm-d-router 和 vllm-router 之后）以及 NVIDIA SGLang 行（位于 llm-d-router 和
// sglang-gateway 之后）已在硬件上完成验证。其余 AMD 和 Ascend 行则基于 operator 侧渲染
// 逻辑及上游 connector 对等性予以准入；其硬件验证跟踪于 issue #763。vllm-router 在 Ascend
// 上未获准入，因为 vLLM-Ascend 的 Mooncake KV connector 需要 nixlv2 握手，而只有
// llm-d-router 的 decode proxy 会转发该握手；未出现在此表中的组合一律拒绝而不予准入。
var modelDeploymentEPDMatrix = []ModelDeploymentEPDRow{
	{
		// Ascend 上的 vLLM，前端为 llm-d-router，encoder 的输出通过用户以共享 ReadWriteMany claim
		// 形式同时挂载到两个角色的目录进行传递。
		//
		// ECExampleConnector 是上游 Ascend 示例自身使用的 connector：
		// https://github.com/vllm-project/vllm-ascend/blob/v0.23.0/docs/source/user_guide/feature_guide/epd_disaggregation.md
		// 它也是此处唯一可用的选项，因为网络 connector 没有 NPU 拷贝路径，且 Ascend 的 EC 和 KV 链路
		// 基于 ascend 协议使用文件、MemCache 和 Mooncake，从不使用 UCX 或 NIXL。该插件自身的网络
		// connector（ECMemcacheConnector）在 v0.30.0rc1 引入，此处未列为一行：operator 尚不配置它，
		// 而 ECMooncakeConnector 在上游仍处于 open 状态。
		//
		// 最低版本为 ModelDeploymentECExampleConnectorFirstAscend，其中按编号说明了两个上游修复。
		// 该行的版本号对应 vllm-ascend 发行版而非上游 vLLM 版本：operator 为 Ascend 资源池组装的
		// runner 会安装该插件，因此用户填写的是该插件所标记的版本号。
		Router:       workercore.ModelDeploymentRouterLLMD,
		Engine:       workercore.ModelDeploymentEngineVLLM,
		Platform:     nodefeature.ManufacturerAscend,
		Connector:    ModelDeploymentECConnectorExample,
		EngineFloor:  ModelDeploymentECExampleConnectorFirstAscend,
		SharedVolume: true,
	},
	{
		// ROCm 上的 vLLM，前端为 llm-d-router，encoder 的输出由 decode sidecar 通过 NIXL 拉取，而非
		// 通过挂载目录传递。
		//
		// ECCPUConnector 是该平台选用的 connector，最低版本为引入网络 connector 的发行版。NIXL 从
		// v1.3.0 开始支持 ROCm（https://github.com/ai-dynamo/nixl/releases/tag/v1.3.0），因此
		// engine 最低版本与传输层最低版本在此处没有额外约束冲突。上游未发现针对 NIXL UCX 后端的
		// 必需 ROCm 测试，这也是该行虽获准入但标记为未验证的原因。
		//
		// 共享 claim 对该行没有任何作用，因此 claim 规则不适用：sidecar 会读取 encoder 的响应并自行
		// 拉取 embeddings。
		Router:      workercore.ModelDeploymentRouterLLMD,
		Engine:      workercore.ModelDeploymentEngineVLLM,
		Platform:    nodefeature.ManufacturerAMD,
		Connector:   ModelDeploymentECConnectorCPU,
		EngineFloor: ModelDeploymentECCPUConnectorFirstVLLM,
	},
	{
		// NVIDIA CUDA 上的 vLLM，前端为 llm-d-router，以与 ROCm 行相同的方式通过 NIXL 拉取 encoder
		// 的输出。已在 4 x NVIDIA L40S（驱动 580.173.02，CUDA 13.0，Qwen/Qwen2.5-VL-7B-Instruct，
		// vLLM 0.30.0）上验证通过：多模态请求通过 router 正常响应，且两端链路均处于工作状态。
		Router:      workercore.ModelDeploymentRouterLLMD,
		Engine:      workercore.ModelDeploymentEngineVLLM,
		Platform:    nodefeature.ManufacturerNVIDIA,
		Connector:   ModelDeploymentECConnectorCPU,
		EngineFloor: ModelDeploymentECCPUConnectorFirstVLLM,
	},
	// 这两行 vllm-router 配置将相同的 ECCPUConnector 链路与 vllm-router 自身的三阶段编排
	// （--epd-config={"encoder_urls":[...]}）配对。该 router 无需 decode sidecar 即可直接驱动
	// encode -> prefill -> decode 流程，在发起 decode 调用前剥离 ec_transfer_params，同时各
	// engine 角色之间通过 NIXL（ECCPUConnector）交换 embeddings，并通过 MooncakeConnector
	// 交换 KV cache。此处不包含 Ascend，因为 vLLM-Ascend 的 P/D KV 链路需要 nixlv2 握手，
	// 而只有 llm-d-router 的 decode sidecar 支持该协议。
	{
		Router:      workercore.ModelDeploymentRouterVLLM,
		Engine:      workercore.ModelDeploymentEngineVLLM,
		Platform:    nodefeature.ManufacturerAMD,
		Connector:   ModelDeploymentECConnectorCPU,
		EngineFloor: ModelDeploymentECCPUConnectorFirstVLLM,
	},
	{
		Router:      workercore.ModelDeploymentRouterVLLM,
		Engine:      workercore.ModelDeploymentEngineVLLM,
		Platform:    nodefeature.ManufacturerNVIDIA,
		Connector:   ModelDeploymentECConnectorCPU,
		EngineFloor: ModelDeploymentECCPUConnectorFirstVLLM,
	},
	// 随后的六行 SGLang 配置共用同一种链路与同一验证等级。SGLang 通过地址而非 encoder-cache
	// connector 传递 encoder 的输出：encoder 以 --encoder-only 运行，prefiller 以
	// --language-only 运行并通过 --encoder-urls 连接 encoder。这正是 connector 列所记录的
	// 内容，也是这些行既不配置 connector、不挂载 claim，也不对二者应用 claim 规则的原因。
	//
	// 其中 NVIDIA 行已完成端到端验证，其余平台尚未验证。NVIDIA 上的 SGLang 在 L40S 上已通过
	// 两种 router 的测试：文本提示词和图像提示词均返回 200 并生成文本，prefiller 成功连接了
	// encoder，且 prefiller 维持了放宽后的探针配置（readiness 6、liveness 12、timeout 5s）
	// 且零重启。请求应发送至部署对外暴露的入口，即 8000 端口上的 router Service：普通的
	// <md> Service 直接选择 prefiller 角色且不携带 bootstrap 信息，因此发往该处的请求会在任何
	// decoder 运行前被 SGLang 以 HTTP 400 拒绝。这是入口设计使然，而非该行的故障。AMD ROCm
	// 和 Ascend NPU 行尚未在硬件上验证，跟踪于 issue #763。
	{
		// Ascend 上的 SGLang，前端为 llm-d-router。
		Router:      workercore.ModelDeploymentRouterLLMD,
		Engine:      workercore.ModelDeploymentEngineSGLang,
		Platform:    nodefeature.ManufacturerAscend,
		Connector:   ModelDeploymentECConnectorSGLangURLs,
		EngineFloor: ModelDeploymentSGLangEncoderFirst,
	},
	{
		Router:      workercore.ModelDeploymentRouterLLMD,
		Engine:      workercore.ModelDeploymentEngineSGLang,
		Platform:    nodefeature.ManufacturerAMD,
		Connector:   ModelDeploymentECConnectorSGLangURLs,
		EngineFloor: ModelDeploymentSGLangEncoderFirst,
	},
	{
		Router:      workercore.ModelDeploymentRouterLLMD,
		Engine:      workercore.ModelDeploymentEngineSGLang,
		Platform:    nodefeature.ManufacturerNVIDIA,
		Connector:   ModelDeploymentECConnectorSGLangURLs,
		EngineFloor: ModelDeploymentSGLangEncoderFirst,
	},
	{
		// 位于 sglang-gateway 之后的同一链路，由其原生路由该角色对，而非经由 llm-d-router 的
		// decider。
		Router:      workercore.ModelDeploymentRouterSGLang,
		Engine:      workercore.ModelDeploymentEngineSGLang,
		Platform:    nodefeature.ManufacturerAscend,
		Connector:   ModelDeploymentECConnectorSGLangURLs,
		EngineFloor: ModelDeploymentSGLangEncoderFirst,
	},
	{
		Router:      workercore.ModelDeploymentRouterSGLang,
		Engine:      workercore.ModelDeploymentEngineSGLang,
		Platform:    nodefeature.ManufacturerAMD,
		Connector:   ModelDeploymentECConnectorSGLangURLs,
		EngineFloor: ModelDeploymentSGLangEncoderFirst,
	},
	{
		Router:      workercore.ModelDeploymentRouterSGLang,
		Engine:      workercore.ModelDeploymentEngineSGLang,
		Platform:    nodefeature.ManufacturerNVIDIA,
		Connector:   ModelDeploymentECConnectorSGLangURLs,
		EngineFloor: ModelDeploymentSGLangEncoderFirst,
	},
}

// ModelDeploymentEPDConnector 返回该组合运行的 encoder-cache connector；当组合未获准入时
// 返回空字符串。
//
// 对于未声明 Encode 角色的部署以及组合无对应行的部署，返回值均为空字符串，两者都不渲染
// encoder 阶段。这是因两个不同原因导致的相同渲染结果，且都不属于需要报告的异常：未获准入的
// 组合永远不会进入渲染阶段，因为准入检查会先将其拒绝。
//
// 这是渲染器读取的唯一来源。它接收组合三元组而非部署对象本身，因为组合即是一行的完整标识；
// 它返回单个字符串而非整行结构，以防调用方在拿到结果之外再从该行的其他列重新推导 connector。
func ModelDeploymentEPDConnector(router, engine, platform string) string {
	row, admitted := ModelDeploymentEPDRowFor(router, engine, platform)
	if !admitted {
		return ""
	}

	return row.Connector
}

// modelDeploymentEPDEncoderLeg 解析声明了 Encode 角色的部署中，单个角色需要获取哪些关于
// encoder 阶段的信息，以及该角色是否参与执行其中任何部分。
//
// 角色决定负责哪一端，行定义决定传输方式。encoder 是生产者，prefiller 是消费者；其他所有
// 角色都不从该链路渲染任何内容，因此下面的零值返回涵盖了 decode 和 server 角色，无需在
// 调用点单独过滤。
//
// 哪一端的信息不会写入返回的链路结构中：`inject` 会从调用方传入的角色对象中直接读取，因此
// 在此再次声明只会造成同一事实的重复表达。
//
// 对于未声明 encoder 的部署，返回为空，从而避免为从未请求 encoder 的部署开启 encoder 阶段。
// prefill/decode 对本身是一个独立配对，某行恰好针对其 engine 和 platform 存在，并不代表
// 当前部署运行了 encoder；若在此为 prefiller 赋予 connector，会导致它去查找根本没有进程
// 生产的 embeddings。其返回值与链路之外的角色类型同样为零值，因此将门禁判断放在部署级别
// 而非角色级别。
//
// 对于未获准入的组合，返回同样为空，以防止先于变更后 spec 运行的渲染逻辑去配置一条未获准入的
// 链路。准入阶段会拒绝此类部署，因此若渲染流程触及此类对象，说明该对象是在对应行被撤回之前
// 落盘的；返回空链路会按该机制存在之前的行为进行渲染，这是安全的降级方向。
func modelDeploymentEPDEncoderLeg(
	md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole, manufacturer string,
) inject.EncoderCache {
	// 在第 21 章中实现此函数。
	return inject.EncoderCache{}
}

// modelDeploymentDeclaresEncoder 报告该部署是否运行 encoder 阶段。
//
// 它读取的是有效角色类型（EFFECTIVE kind）而非声明的类型，以确保具有默认类型的角色与本文件
// 其他地方的解析方式一致。在推导任何 encoder 阶段信息之前都会先询问该结果，因为没有 encoder
// 的部署不存在可供描述的 encoder 阶段，其余各项返回值对它均无意义。
func modelDeploymentDeclaresEncoder(md *workercore.ModelDeployment) bool {
	for i := range md.Spec.Roles {
		if ModelDeploymentEffectiveRoleKind(&md.Spec.Roles[i]) ==
			workercore.ModelDeploymentRoleKindEncode {
			return true
		}
	}

	return false
}

// modelDeploymentCreateRoleOrder 返回单次调度创建 Pod 时发放各角色的顺序：保持声明顺序，
// 但在声明了 encoder 的部署中将 encoder 置于最后。
//
// 这一顺序决定了后续调度偏好的实际效果。prefiller 和 decoder 之间交互权重更高，因为它们在
// 每次请求中都要交换 KV，而调度偏好只能基于已存在的 Pod 打分。如果 encoder 排在最前，它会
// 先被调度落位，随后 prefiller 仅针对 encoder 所在节点打分并填满该节点，最终导致 decoder
// 落到另一节点。将 encoder 放在最后可以让交互更重的 prefiller 与 decoder 优先聚拢在一起。
// 除 encoder 外的角色保持原有的相对声明顺序；未声明 encoder 的部署则保持不变。
func modelDeploymentCreateRoleOrder(md *workercore.ModelDeployment) []*workercore.ModelDeploymentRole {
	ordered := make([]*workercore.ModelDeploymentRole, 0, len(md.Spec.Roles))
	var encoders []*workercore.ModelDeploymentRole
	for i := range md.Spec.Roles {
		role := &md.Spec.Roles[i]
		if ModelDeploymentEffectiveRoleKind(role) == workercore.ModelDeploymentRoleKindEncode {
			encoders = append(encoders, role)

			continue
		}
		ordered = append(ordered, role)
	}

	return append(ordered, encoders...)
}

// modelDeploymentEPDConnector 返回该部署整体所用的行对应 connector；若未运行获准入的
// encoder 阶段则返回空字符串。
//
// 该值作用于整个部署，而链路信息则按角色区分，因为二者被配置到不同的进程中：链路信息会交给
// 运行 encoder cache 的容器，仅 encoder 和 prefiller 持有；而 sidecar 标志由 decode
// sidecar 读取，decode 并不属于上述两个角色。若从角色自身的链路中读取该标志，相当于向
// decoder 询问一个恒为零值的问题，从而导致唯一读取该标志的 Pod 上无法渲染出该参数。
//
// 渲染器仅在 decode Pod 上运行该 sidecar，因此此处无条件设置该标志，仅在存在 sidecar 时
// 才会被读取。返回空值时不渲染任何内容，这适用于未声明 encoder 的部署以及无任何获准入行
// 匹配的部署。
func modelDeploymentEPDConnector(
	md *workercore.ModelDeployment, manufacturer string,
) string {
	if !modelDeploymentDeclaresEncoder(md) {
		return ""
	}

	router := ""
	if md.Spec.Router != nil {
		router = md.Spec.Router.Name
	}

	return ModelDeploymentEPDConnector(router, md.Spec.Engine.Name, manufacturer)
}

// modelDeploymentEPDSplitsEncoderByAddress 报告某 engine 是否通过地址而非 connector 连接到
// encoder 阶段。
//
// 两种传输方式都是真实存在的，且需要不同的输入。使用 connector 的 engine 通过命名 connector
// 来配置，由行定义指定具体名称；而将 encoder 从自身服务拆分出去的 engine 通过传入待拨号
// 地址来配置，不指定 connector，因此在表中也没有对应的 connector 行。若用行定义去卡控这类
// engine，会导致其 encoder 被渲染成未拆分的普通服务，无法响应 prefiller 的请求。
//
// 该判断基于 engine 所属家族本身，因为差异正源于此：同一代码包仅针对 SGLang 家族渲染地址
// 传输，而在这两种家族之外的 engine 不会配置任何 encoder 参数。
func modelDeploymentEPDSplitsEncoderByAddress(engine string) bool {
	return engine == workercore.ModelDeploymentEngineSGLang
}

// modelDeploymentEPDReachesEncoderStage 报告该部署的 engine 是否能够识别 encoder 阶段配置，
// 这是渲染器自身可达性检查所询问的问题。
//
// 对于既无获准入行也无地址传输方式的组合，encoder 角色在此处返回 false，这与引入 encoder
// 角色配置之前的行为一致：此类角色将按未拆分模式渲染。该路径仅在协调一个已被准入逻辑拒绝的
// 对象时触发（原因可能是组合未获准入，或对象在对应行被撤回前已存储）；保持原有方式渲染可以
// 避免无关的协调流程因一个不被消费的字段而失败。
func modelDeploymentEPDReachesEncoderStage(
	md *workercore.ModelDeployment, manufacturer string,
) bool {
	if !modelDeploymentDeclaresEncoder(md) {
		return false
	}

	return modelDeploymentEPDConnector(md, manufacturer) != "" ||
		modelDeploymentEPDSplitsEncoderByAddress(md.Spec.Engine.Name)
}

// modelDeploymentEPDEncoderURLs 返回 encoder 阶段的地址列表，格式符合基于地址（而非
// connector）的 engine 要求：每个 Service 对应一个 URL，而非每个副本一个。
//
// 该列表作用于整个部署而非单个角色，因为声明 Encode 角色的部署只有一组地址。不读取该列表的
// 角色不会从中渲染任何内容，因此将其传递给所有角色可以让同一份值贯穿渲染器，而无需引入可能
// 与实际消费角色不一致的按角色过滤逻辑。
//
// 主机名采用短 Service 名称，部署中的每个角色都可通过各自的名称解析搜索路径解析它，因为它们
// 共享同一 Namespace。仅协议头（scheme）会按 modelDeploymentRoleEndpoint 的方式推导，
// 从而使开启 TLS 的 encoder 在被 prefiller 访问时像被 router 访问时一样使用 https，而未
// 开启 TLS 的 encoder 则维持原有寻址方式。对于未声明 encoder 的部署，返回空切片，不渲染
// 任何内容。
func modelDeploymentEPDEncoderURLs(md *workercore.ModelDeployment) []string {
	if !modelDeploymentDeclaresEncoder(md) {
		return nil
	}

	for i := range md.Spec.Roles {
		if ModelDeploymentEffectiveRoleKind(&md.Spec.Roles[i]) !=
			workercore.ModelDeploymentRoleKindEncode {
			continue
		}

		role := &md.Spec.Roles[i]
		scheme, _ := modelDeploymentEngineTransport(md.Spec.Engine.Name, ModelDeploymentRoleArgs(role))
		port := modelDeploymentServicePort(role)

		return []string{strings.ToLower(string(scheme)) + "://" + md.Name + "-" + role.Name + ":" +
			strconvx.Itoa(int(port.ContainerPort))}
	}

	return nil
}

// ModelDeploymentECSidecarConnector 将已准入表项中的 connector 转换为 decode sidecar 调度时使用的拼写，
// 对其他值返回空字符串。
//
// 这两种拼写是有意区分的，且互不指代：表项中保存的是引擎侧的 connector 名称，因为引擎解析的是该名称，
// 用户在引擎日志中看到的也是它；而 sidecar 则使用其自身的两种拼写，因为那是其命令行标志所接受的值。
// 此处使用映射表而非推导规则，是因为这两个集合无需同步增长——已准入但 sidecar 不支持的 connector
// 会在准入阶段被拒绝，反之则允许 sidecar 指定此处没有任何引擎会解析的 connector。
//
// 这是两套词汇唯一交汇之处，从而使该链路的两端都能通过同一个表项完成配置，而无需任一端重新决定传输方式。
func ModelDeploymentECSidecarConnector(connector string) string {
	switch connector {
	case ModelDeploymentECConnectorExample:
		return modelDeploymentECConnectorExample
	case ModelDeploymentECConnectorCPU:
		return modelDeploymentECConnectorNIXL
	default:
		return ""
	}
}

// ModelDeploymentEPDMatrixSummary 渲染所有已准入的表项，以便在拒绝请求时将其列在被拒组合旁。
// 否则，被告知某组合未获准入的用户只能去查看该矩阵表，而该表对用户并不可见。
//
// 此摘要不列出任何引擎版本。表项中的 EngineFloor 表示 connector 对镜像版本的最低要求，但该要求如今
// 由文档而非准入控制来承载；如果在此处打印它，就会误导被拒用户以为填写一个满足要求的高版本就能通过准入，
// 实际上无论如何填写都不会通过。
func ModelDeploymentEPDMatrixSummary() string {
	rows := make([]string, 0, len(modelDeploymentEPDMatrix))
	for _, row := range modelDeploymentEPDMatrix {
		rows = append(rows, fmt.Sprintf("%s+%s+%s+%s", row.Router, row.Engine, row.Platform, row.Connector))
	}

	return strings.Join(rows, ", ")
}

// ModelDeploymentEPDRowFor 返回该组合对应的已准入表项以及是否已准入。
//
// 它返回的是副本，而非指向表内元素的指针，因为该表是有意设为未导出的，以防止任何调用方直接访问——
// 而通过导出函数传出的指针同样会暴露内部数据。如今导出接口跨越了两个包，如果返回指针，准入包中的
// 调用方就可能根据请求改写已准入表，导致后续所有准入判断都基于被篡改后的对象作出响应。按值返回
// 无法回写原表，因此无论调用方对返回值做什么操作，原表始终保持只读。
func ModelDeploymentEPDRowFor(router, engine, platform string) (ModelDeploymentEPDRow, bool) {
	for _, row := range modelDeploymentEPDMatrix {
		if row.Router == router && row.Engine == engine && row.Platform == platform {
			return row, true
		}
	}

	return ModelDeploymentEPDRow{}, false
}

// ModelDeploymentEPDMatrixNames 报告是否存在任何已准入表项满足给定谓词。
func ModelDeploymentEPDMatrixNames(match func(*ModelDeploymentEPDRow) bool) bool {
	for i := range modelDeploymentEPDMatrix {
		if match(&modelDeploymentEPDMatrix[i]) {
			return true
		}
	}

	return false
}

// ModelDeploymentEPDMatrixGap 返回某组合所缺失的列名，以便在拒绝时明确指出是哪一侧缺失，
// 而不只是笼统说明该组合不被允许。若该组合已准入，则返回空字符串。
func ModelDeploymentEPDMatrixGap(router, engine, platform string) string {
	if _, admitted := ModelDeploymentEPDRowFor(router, engine, platform); admitted {
		return ""
	}

	named := ModelDeploymentEPDMatrixNames

	switch {
	case router == "":
		return "no router is named, so nothing pairs the three roles"
	case !named(func(row *ModelDeploymentEPDRow) bool { return row.Router == router }):
		return fmt.Sprintf("router %q is not admitted for an encoder role", router)
	case !named(func(row *ModelDeploymentEPDRow) bool { return row.Router == router && row.Engine == engine }):
		return fmt.Sprintf("router %q is not admitted with engine %q", router, engine)
	default:
		return fmt.Sprintf(
			"platform %q is not admitted for encoder roles under router %q and engine %q",
			platform, router, engine,
		)
	}
}
