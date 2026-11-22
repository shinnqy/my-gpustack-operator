// 引擎 group-forward 观测：使非分离式多 rank 副本的 GroupForward 分支可被验证的合成请求。
//
// 副本的前向路径通过让其承载一次请求并观察该请求返回来验证。
// 探测向每个成员发送一个单 token 请求，通过引擎自身的推理服务路由，并携带由本控制器选定的请求标识符。
// 使响应成为有效证据而非仅证明存活的关键在于绑定关系：响应必须带有该标识符返回，
// 因此仅仅存活的服务端，或返回了其他请求响应的服务端，都不会被误判为成功完成了前向推理。
// 两种引擎都在其服务路由上暴露了这种绑定关系，且均未在健康检查路由上暴露——这正是此前无法验证该分支的原因。
//
// 该探测会消耗引擎一个 token，且仅在有收益时执行：它运行在处于 HELD 状态副本的准入资格评估期间，
// 即当前被暂留的组在准入或恢复时。已准入的副本不会被探测，因此正在提供服务的部署没有额外开销；
// 重新评估将在下一次 reconcile 时进行，而非在本次 reconcile 内重试。
//
// vLLM 探测的日志足迹是可接受的，且无法按请求屏蔽。本程序兼容的两个版本的 vLLM OpenAI 协议
// 均未提供按请求关闭指标的开关，因此探测会像其他请求一样出现在引擎的请求日志和成功计数器中。
// 若要隐藏这种极少发生的探测而关闭所有生产流量的请求日志，代价得不偿失。
// 因此标识符采用自描述格式，便于查看日志的运维人员过滤探测请求：它始终以
// modelDeploymentQualifyRIDPrefix 开头。SGLang 则支持按请求开关，探测会自动设置该开关。
package worker

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	core "k8s.io/api/core/v1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/utils/json"
)

// modelDeploymentQualifyRIDPrefix 标记本 operator 在引擎服务路由上发起的每一次请求，
// 以便任何查看引擎自身日志和计数器的人员都能识别该探测。
const modelDeploymentQualifyRIDPrefix = "gpustack-qualify-"

// modelDeploymentGroupForwardBound 是整个副本探测允许耗费的最大时长。它是所有被探测成员共享的
// 单一时间预算，而非每个成员各自拥有：各成员在此预算下顺序探测，因此靠前的成员若响应较慢，
// 会消耗后续成员原本可用的预算。该上限设定了单次流程对单个副本的总等待容忍度，
// 正如 router 观测预算限定单次采集的时长一样。
//
// 它只是调用方上限，而非引擎会遵守的截止时间，因为两种引擎的请求结构体均不携带超时字段：
// 本程序适配的两个版本中，vLLM 的 CompletionRequest 和 SGLang 的 GenerateReqInput 都没有该字段。
// 因此，处于饱和状态的成员会将请求排队而不是直接拒绝，唯一能防止繁忙引擎阻塞 reconcile 的就是此上限。
// 相对于单 token 的工作量，该预算刻意设得较为充裕，因为过早超时的代价是导致健康组在整轮中被暂留。
const modelDeploymentGroupForwardBound = 15 * time.Second

// modelDeploymentGroupForwardMaxResponseBytes 限制探测读取的响应大小。单个 token 的响应是一个很小的文档，
// 返回超大响应的成员意味着其响应无法完成有效绑定。
const modelDeploymentGroupForwardMaxResponseBytes = 1 << 20

// modelDeploymentGroupForwardFetch 向单个成员的引擎发起一次探测请求并返回响应体。
//
// 它被定义为函数而非直接发起网络连接，是因为本文件需要准确验证各类失败状态，
// 而真实的网络连接无法按需触发失败。fetch 为 nil 表示该分支没有传输实现，
// 因而无法执行观测，结果将记为 Unknown 并保持暂留。
type modelDeploymentGroupForwardFetch func(ctx context.Context, url string, body []byte) ([]byte, error)

// modelDeploymentGroupForwardEngine 定义了本 operator 对单个引擎所知的全部交互方式：
// 探测请求发送的路由、如何构造请求体，以及如何从响应中提取绑定关系。
//
// 若某引擎缺少对应条目，则表示该引擎没有经过验证的观测方法，该分支会明确指出引擎名称，而不是盲目猜测。
type modelDeploymentGroupForwardEngine struct {
	// Path 是探测请求通过 POST 发送到的引擎路由。
	Path string
	// Binding 用文字描述使响应能够证明其对应本 operator 所发请求的属性。
	Binding string
	// DisaggregatedReason 说明在该引擎的分离式部署形态下，真实请求无法证明的内容。
	// 它是引擎特定的，因为两种引擎无法完成证明的原因各不相同，仅告知"不支持 P/D"无法让阅读者了解具体障碍所在。
	DisaggregatedReason string
	// Body 为单个成员构造探测请求体。
	Body func(rid string) ([]byte, error)
	// Bound 解析响应。当响应与本 operator 自身的请求绑定且表明请求已运行完成时返回空字符串；
	// 否则返回该响应无法证明上述任一点的原因。它直接接收原始响应体，使格式错误的文档被判定为
	// 拒绝采信的理由，而不是需要调用方单独区分的解析错误。
	Bound func(body []byte, rid string) string
}

// modelDeploymentGroupForwardEngines 是按引擎划分的适配矩阵。未列入其中的引擎没有经过验证的观测方式，
// 该分支会直接指明该引擎，而非带着猜测的原因将其暂留。
var modelDeploymentGroupForwardEngines = map[string]modelDeploymentGroupForwardEngine{
	workercore.ModelDeploymentEngineVLLM: {
		// /v1/completions 路由的请求结构体包含 request_id，本程序兼容的两个版本的文档均说明其
		// "贯穿整个推理过程并在响应中返回"，其服务路径会将其作为带 "cmpl-" 前缀的响应 id 返回。
		// 此处省略 model 字段：它在两个版本中都是可选的，且一旦指定了引擎未托管的模型名称，
		// 就会使正常工作的组返回 404 并被永久暂留。
		Path:    "/v1/completions",
		Binding: `the response id is "cmpl-" + our request_id`,
		DisaggregatedReason: "vLLM's disaggregated transfer is a client-coordinated pair of " +
			"requests, so one request proves only the forward path of the instance that served it " +
			"and says nothing about the transfer between the prefill and decode roles; the " +
			"directed pair probe is the path that answers it, and it needs both groups' snapshots, " +
			"which this call site does not carry",
		Body: func(rid string) ([]byte, error) {
			return json.Marshal(map[string]any{
				"prompt":      modelDeploymentQualifyPrompt,
				"max_tokens":  1,
				"temperature": 0,
				"request_id":  rid,
			})
		},
		Bound: func(body []byte, rid string) string {
			var response struct {
				ID      string `json:"id"`
				Choices []struct {
					FinishReason *string `json:"finish_reason"`
				} `json:"choices"`
			}
			if json.Unmarshal(body, &response) != nil {
				return modelDeploymentQualifyUnreadableAnswer
			}
			if response.ID != "cmpl-"+rid {
				return modelDeploymentQualifyUnboundAnswer
			}
			if len(response.Choices) == 0 || response.Choices[0].FinishReason == nil {
				return modelDeploymentQualifyUnfinishedAnswer
			}
			if !modelDeploymentQualifyFinishRan(*response.Choices[0].FinishReason) {
				return modelDeploymentQualifyAbortedAnswer
			}

			return ""
		},
	},
	workercore.ModelDeploymentEngineSGLang: {
		// /generate 不接受 model 字段，因此不存在填错模型的风险。响应中带有 meta_info.id，
		// 即本 operator 发送的 rid，以及引擎为该请求记录的 finish_reason。
		Path:    "/generate",
		Binding: `the response meta_info.id is our rid`,
		DisaggregatedReason: "a prefill or decode group is verified by a directed pair probe, " +
			"which needs both groups' snapshots, and this call site carries only one of them; " +
			"the engine's own readiness request cannot stand in for it, because that request " +
			"takes the fake-bootstrap shortcut, which skips the all-reduce that would gate the " +
			"transfer on every tensor-parallel rank",
		Body: func(rid string) ([]byte, error) {
			return json.Marshal(map[string]any{
				"text":        modelDeploymentQualifyPrompt,
				"rid":         rid,
				"log_metrics": false,
				"sampling_params": map[string]any{
					"max_new_tokens": 1,
					"temperature":    0,
				},
			})
		},
		Bound: func(body []byte, rid string) string {
			var response struct {
				MetaInfo struct {
					ID string `json:"id"`
					// 引擎将 finish_reason 记录为以 type 为键的文档而非字符串，因此这里将其读取为原始 JSON：
					// 关键在于是否收到了结束原因，而非其具体结构。若使用 string 类型字段，
					// 将会拒绝该引擎返回的所有真实响应，并使正常工作的组被永久暂留。
					FinishReason json.RawMessage `json:"finish_reason"`
				} `json:"meta_info"`
			}
			if json.Unmarshal(body, &response) != nil {
				return modelDeploymentQualifyUnreadableAnswer
			}
			if response.MetaInfo.ID != rid {
				return modelDeploymentQualifyUnboundAnswer
			}
			reason := modelDeploymentQualifyFinishType(response.MetaInfo.FinishReason)
			if reason == "" {
				return modelDeploymentQualifyUnfinishedAnswer
			}
			if !modelDeploymentQualifyFinishRan(reason) {
				return modelDeploymentQualifyAbortedAnswer
			}

			return ""
		},
	},
}

// modelDeploymentQualifyPrompt 是探测发送的文本。在两种引擎上它都只对应单个 token 的工作量，
// 其具体内容并不重要：需要观测的是请求是否带着自身的标识符返回。
const modelDeploymentQualifyPrompt = "ok"

// 当响应不足以证明该组转发了请求时所给出的拒绝原因。每一条都指明了响应中缺失的要素，
// 因为无法采信的响应会导致暂留，而暂留必须说明原因。
const (
	// modelDeploymentQualifyUnreadableAnswer 表示本 operator 完全无法解析的响应。
	modelDeploymentQualifyUnreadableAnswer = "answered with a document this operator cannot read, " +
		"so the answer is not evidence that this group forwarded one"
	// modelDeploymentQualifyUnboundAnswer 表示携带了他人请求标识的响应。
	modelDeploymentQualifyUnboundAnswer = "answered, but not bound to this operator's own request, " +
		"so the answer is not evidence that this group forwarded one"
	// modelDeploymentQualifyUnfinishedAnswer 表示终止但未说明原因的响应。
	modelDeploymentQualifyUnfinishedAnswer = "answered, but it recorded no finish reason, so the " +
		"answer is not evidence that this group forwarded one"
	// modelDeploymentQualifyAbortedAnswer 表示请求未能运行至完成的响应。
	modelDeploymentQualifyAbortedAnswer = "answered, but its finish reason reports the request did " +
		"not complete, so the answer is not evidence that this group forwarded one"
)

// modelDeploymentQualifyNonceBytes 指定单个探测标识符携带的随机字节数。它足以保证两次尝试
// 不会发生碰撞，同时又足够短小，使标识符在引擎自身的日志行中仍保持可读性。
const modelDeploymentQualifyNonceBytes = 12

// modelDeploymentQualifyRequestID 构建单个探测请求携带的标识符。
//
// 每次尝试都会生成新的标识符，从而防止复用来自身前次尝试的响应体通过绑定校验：
// 前缀和成员自身的标识指明这是哪类探测以及发往哪个成员，nonce 则标识属于哪一次尝试。
func modelDeploymentQualifyRequestID(member *core.Pod) (string, error) {
	nonce, err := modelDeploymentQualifyRandom(modelDeploymentQualifyNonceBytes)
	if err != nil {
		return "", err
	}

	return modelDeploymentQualifyRIDPrefix + string(member.UID) + "-" + nonce, nil
}

// modelDeploymentQualifyRandom 返回从加密随机源读取的十六进制令牌。
//
// 这里从加密随机源而非计数器读取，是因为该标识符会随请求写入引擎的日志和请求计数器中，
// 可预测的标识符会让读取这些日志的一方能够伪造出可通过绑定校验的响应。
func modelDeploymentQualifyRandom(size int) (string, error) {
	buffer := make([]byte, size)
	if _, err := rand.Read(buffer); err != nil {
		return "", fmt.Errorf("the probe's random source failed: %w", err)
	}

	return hex.EncodeToString(buffer), nil
}

// modelDeploymentQualifyFinishRan 判断结束原因是否表明请求已运行至完成。
//
// 报告请求被中止或失败的原因不能证明该组完成了请求转发，因此会被拒绝而非当作有效原因接受。
// 表明输出被截断的原因则是有效证据：引擎确实运行了，并说明了停止位置。
func modelDeploymentQualifyFinishRan(reason string) bool {
	trimmed := strings.TrimSpace(reason)
	if trimmed == "" || trimmed == "null" {
		return false
	}

	switch strings.ToLower(trimmed) {
	case "abort", "aborted", "error", "failed":
		return false
	}

	return true
}

// modelDeploymentQualifyFinishType 从结束原因中提取类型。
//
// 在本 operator 为组读取的路由上，引擎将该原因记录为以 type 为键的文档，因此需要判断的是其类型。
// 若以纯字符串形式给出，则直接以其本身作为判断依据，因为格式无法识别的原因依然是一个结束原因。
func modelDeploymentQualifyFinishType(reason json.RawMessage) string {
	trimmed := strings.TrimSpace(string(reason))
	if trimmed == "" || trimmed == "null" {
		return ""
	}
	var typed struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(reason, &typed) == nil && typed.Type != "" {
		return typed.Type
	}

	return trimmed
}

// observeModelDeploymentGroupForward 为单个副本判定 group-forward 证据状态。
//
// 下方判断的顺序按照误判代价排序。单成员副本最先判定，因为这是绝对不能出错的形态。
// 本 operator 没有观测支持的形态会在任何网络调用之前判定，因为从未读取的观测不算作观测。
// 只有通过这两项检查的副本，才值得发起探测请求。
func observeModelDeploymentGroupForward(
	ctx context.Context, md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole,
	view modelDeploymentReplicaView, fetch modelDeploymentGroupForwardFetch,
) modelDeploymentGroupForward {
	engineName := md.Spec.Engine.Name
	engine, known := modelDeploymentGroupForwardEngines[engineName]

	// prefill 或 decode 组会在检查成员数量之前判定，且永远不会返回 NotApplicable。
	// 其服务路径是一个配对，因此即使只有一个成员的组也仍有传输需要验证，
	// 若称"无需验证"则相当于做出了未经过观测的断言。该场景应由定向配对探测来给出结论，
	// 而定向配对探测需要两个组的快照；当前调用点仅携带其中一个，因此该组会被暂留，并指明负责探测的路径名称。
	if workercore.ModelDeploymentRoleKindPrefill == ModelDeploymentEffectiveRoleKind(role) ||
		workercore.ModelDeploymentRoleKindDecode == ModelDeploymentEffectiveRoleKind(role) {
		if !known {
			return modelDeploymentGroupForwardUnsupportedFor(
				"engine " + engineName + " has no group-forward observation this operator has verified",
			)
		}
		return modelDeploymentGroupForwardUnsupportedFor(engine.DisaggregatedReason)
	}

	if len(view.Members) <= 1 {
		return modelDeploymentGroupForward{
			State:  modelDeploymentGroupForwardNotApplicable,
			Reason: "a replica of one member has no cross-member forward path to verify",
		}
	}

	// encoder 会在检查引擎之前判定，且直接给出明确结果而非 Unsupported。它运行多模态编码器
	// 并产生 encoder-cache 输出，不直接生成 token：因此不存在能通过绑定响应来证明其前向路径的
	// completion 请求，无论引擎或版本如何，本分支都没有问题需要向其询问。
	// 强行请求只会向无法作答的引擎发出无效请求，唯一能得到的结果只有 Unknown，
	// 而健康度谓词会将其判定为暂留一个健康的组。NotApplicable 表示该 role 不存在此类观测，
	// 这与引擎未提供观测支持是不同的事实，只有前者才能避免将 encoder 置于暂留状态。
	if workercore.ModelDeploymentRoleKindEncode == ModelDeploymentEffectiveRoleKind(role) {
		return modelDeploymentGroupForward{
			State: modelDeploymentGroupForwardNotApplicable,
			Reason: "a replica of the Encode role serves no tokens, so its forward path has no " +
				"cross-member request a bound answer could verify",
		}
	}

	if !known {
		return modelDeploymentGroupForwardUnsupportedFor(
			"engine " + engineName + " has no group-forward observation this operator has verified",
		)
	}

	if fetch == nil {
		return modelDeploymentGroupForward{
			State:  modelDeploymentGroupForwardUnknown,
			Reason: "no group-forward transport is wired, so no member could be asked",
		}
	}

	return probeModelDeploymentGroupForward(ctx, engine, view, fetch)
}

// modelDeploymentGroupForwardUnsupportedFor 构造带有具体阻碍原因的 Unsupported 结果，
// 集中在一处定义，避免不同调用点出现不一致的表述。
func modelDeploymentGroupForwardUnsupportedFor(reason string) modelDeploymentGroupForward {
	return modelDeploymentGroupForward{
		State:  modelDeploymentGroupForwardUnsupported,
		Reason: reason,
	}
}

// probeModelDeploymentGroupForward 探测副本中的每一个成员，并要求每个响应都与其对应的请求绑定。
//
// 必须探测每个成员，因为该分支针对的是整个组，单个成员成功并不能说明其他成员正常：
// 对端缺失的成员只能在其自身所属的集合通信范围内完成前向推理，
// 而跨成员连线失败的组中，恰恰只有那些仍能单独应答的成员才会表现正常。要求全部成员通过才能代表整个副本的状态。
//
// 只要有单个成员未绑定或未响应，结果即为 Unknown，绝不判为 Failed。此时引擎并未报告组不健康；
// 只是本 operator 耗尽了等待时间、遇到了排队，或读到了无法绑定的文档。
// 无法理解的探测结果属于证据缺失，明确的故障应由确定性的健康检查分支来判定。
func probeModelDeploymentGroupForward(
	ctx context.Context, engine modelDeploymentGroupForwardEngine,
	view modelDeploymentReplicaView, fetch modelDeploymentGroupForwardFetch,
) modelDeploymentGroupForward {
	probeCtx, cancel := context.WithTimeout(ctx, modelDeploymentGroupForwardBound)
	defer cancel()

	for _, member := range view.Members {
		rid, err := modelDeploymentQualifyRequestID(member)
		if err != nil {
			return modelDeploymentGroupForwardUnknownFor(
				"the probe's identifier for " + member.Name + " could not be generated: " + err.Error(),
			)
		}
		body, err := engine.Body(rid)
		if err != nil {
			return modelDeploymentGroupForwardUnknownFor(
				"the probe request for " + member.Name + " could not be rendered: " + err.Error(),
			)
		}

		url, err := modelDeploymentGroupForwardURL(member, engine.Path)
		if err != nil {
			return modelDeploymentGroupForwardUnknownFor(
				"member " + member.Name + " " + err.Error(),
			)
		}

		response, err := fetch(probeCtx, url, body)
		if err != nil {
			return modelDeploymentGroupForwardUnknownFor(
				"member " + member.Name + " did not answer the probe within the bound: " +
					err.Error() + "; an unanswered probe is an absence of evidence, not a fault",
			)
		}
		if refusal := engine.Bound(response, rid); refusal != "" {
			return modelDeploymentGroupForwardUnknownFor("member " + member.Name + " " + refusal)
		}
	}

	return modelDeploymentGroupForward{
		State:  modelDeploymentGroupForwardVerified,
		Reason: "every member returned the one-token request this operator sent, bound to its own identifier",
		// 该观测是在本次流程中读取的瞬时状态，且仅在副本处于暂留状态时读取，
		// 因此它本身不携带时效性状态：不会有后续流程依据其过期副本采取行动。
		Generation:  modelDeploymentGenerationOf(view),
		Observation: strconv.Itoa(len(view.Members)) + " members answered, each bound to its own request",
	}
}

func modelDeploymentGroupForwardUnknownFor(reason string) modelDeploymentGroupForward {
	return modelDeploymentGroupForward{
		State:  modelDeploymentGroupForwardUnknown,
		Reason: reason,
	}
}

// modelDeploymentGroupForwardURL 根据渲染器已写入的 annotation 构建成员自身的推理服务 URL，
// 因此探测无需依赖本 operator 未曾写入的任何信息。
//
// 该 annotation 是硬性要求，而接管型（take-over）role 并不具备它。渲染器仅为运行 operator
// 自身命令行的 role 写入 prometheus.io/port：对于被替换的自定义命令，operator 无法得知其服务端口，
// 因此不会写入。所以多成员接管型 role 无法被探测，结果记为 Unknown；此处不会从容器 spec 中猜测端口，
// 因为凭空构造命令从未声明的服务契约，会把证据缺失变成未经证实的断言。
//
// 该 annotation 指代的是引擎端口，未必总是对外入口端口。前置有路由代理的 decoder 会在两个端口上应答：
// 其 Service 指向的端口（由代理监听），以及其背后引擎自身的端口。该 annotation 记录的是引擎端口，
// 因此需要访问入口端口的调用方应通过 modelDeploymentGroupForwardURLOnPort 指定端口。
func modelDeploymentGroupForwardURL(member *core.Pod, path string) (string, error) {
	if member.Status.PodIP == "" {
		return "", fmt.Errorf("has no address to be asked at yet")
	}

	port := member.Annotations["prometheus.io/port"]
	if port == "" {
		return "", fmt.Errorf("does not carry the engine's serving port")
	}
	number, err := strconv.ParseInt(port, 10, 32)
	if err != nil {
		return "", fmt.Errorf("does not carry a serving port this operator can read")
	}

	return modelDeploymentGroupForwardURLOnPort(member, int32(number), path)
}

// modelDeploymentGroupForwardURLOnPort 使用调用方指定的端口构建向单个成员发起请求的 URL。
//
// 端口由调用方指定，因为位于路由代理后的成员会在两个端口上应答：其 Service 指向的端口（由代理监听），
// 以及背后引擎自身的端口。需要访问入口端口的调用方在此指定 Service 端口；需要直连引擎的则指定引擎自身端口。
func modelDeploymentGroupForwardURLOnPort(member *core.Pod, port int32, path string) (string, error) {
	if member.Status.PodIP == "" {
		return "", fmt.Errorf("has no address to be asked at yet")
	}
	if port < 1 || port > 65535 {
		return "", fmt.Errorf("does not carry a serving port this operator can ask at")
	}
	scheme := member.Annotations["prometheus.io/scheme"]
	if scheme == "" {
		scheme = "http"
	}

	return fmt.Sprintf("%s://%s:%d%s", scheme, member.Status.PodIP, port, path), nil
}

// modelDeploymentGroupForwardHostPort 返回成员自身的服务地址，格式为握手时用于指代端点的地址形式。
//
// 它是引擎自身的地址，从相同的 annotation 中读取，因此本 operator 向另一引擎指定的端点
// 与其直接请求的端点是同一个。对于前置无代理的 prefiller，这是应指定的正确地址；
// 而对于前置有代理的 decoder，直接请求该地址则是不正确的。
func modelDeploymentGroupForwardHostPort(member *core.Pod) (string, error) {
	if member.Status.PodIP == "" {
		return "", fmt.Errorf("has no address to be named yet")
	}
	port := member.Annotations["prometheus.io/port"]
	if port == "" {
		return "", fmt.Errorf("does not carry the engine's serving port")
	}

	return member.Status.PodIP + ":" + port, nil
}

// defaultGroupForwardFetch 是生产环境使用的传输实现：直接向由成员 Pod 自身 IP 及渲染器写入的
// 服务端口构成的 URL 发送普通 POST 请求，绝不经由 Service（否则只会采样单个副本而悄悄代表其余副本应答）。
//
// 它采用与 router 观测读取相同的模式，并刻意不使用排空读取器（drain reader）所用的 exec 通道：
// exec 通道会在容器内部执行操作，属于另一类任务。此处不会进入成员容器内部执行任何操作。
func defaultGroupForwardFetch(ctx context.Context, url string, body []byte) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(body)))
	if err != nil {
		return nil, fmt.Errorf("probe request could not be built: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("probe request failed: %w", err)
	}
	defer func() {
		_ = response.Body.Close()
	}()

	// 该上限同样约束响应大小。返回超大响应的成员无法与本 operator 的请求完成绑定，
	// 完整读取它只会在最终会被丢弃的文档上耗尽本次 reconcile 剩余的预算。
	payload, err := io.ReadAll(io.LimitReader(response.Body, modelDeploymentGroupForwardMaxResponseBytes))
	if err != nil {
		return nil, fmt.Errorf("probe response could not be read: %w", err)
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("engine answered %s", response.Status)
	}

	return payload, nil
}

// defaultGroupForwardPairFetch 执行定向配对中的一次请求。
//
// 它是独立于普通探测的传输实现，因为配对请求需要携带普通探测从不需要的请求头：
// 一种引擎通过请求头读取目标 rank，另一种引擎则通过请求头被告知要与哪个 prefiller 配对。
func defaultGroupForwardPairFetch(
	ctx context.Context, request modelDeploymentGroupForwardPairRequest,
) ([]byte, error) {
	method := request.Method
	if method == "" {
		method = http.MethodPost
	}
	probeRequest, err := http.NewRequestWithContext(
		ctx, method, request.URL, strings.NewReader(string(request.Body)),
	)
	if err != nil {
		return nil, fmt.Errorf("pair request could not be built: %w", err)
	}
	probeRequest.Header.Set("Content-Type", "application/json")
	for name, value := range request.Headers {
		probeRequest.Header.Set(name, value)
	}

	response, err := http.DefaultClient.Do(probeRequest)
	if err != nil {
		return nil, fmt.Errorf("pair request failed: %w", err)
	}
	defer func() {
		_ = response.Body.Close()
	}()

	payload, err := io.ReadAll(io.LimitReader(response.Body, modelDeploymentGroupForwardMaxResponseBytes))
	if err != nil {
		return nil, fmt.Errorf("pair response could not be read: %w", err)
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("engine answered %s", response.Status)
	}

	return payload, nil
}

// modelDeploymentGroupForwardPairBound 是单次定向配对探测的总允许耗时。它是本次尝试所探测的
// 所有 rank 配对共享的单一时间预算，正如普通探测在副本各成员间共享一个预算：响应较慢的
// rank 配对会消耗后续 rank 配对原本可用的预算。
const modelDeploymentGroupForwardPairBound = 60 * time.Second

// modelDeploymentGroupForwardPairMaxTokens 限制复制响应可使用的最大 token 数。响应本身只是一短行文本，
// 该上限用于防止探测演变为长文本生成。
const modelDeploymentGroupForwardPairMaxTokens = 80

// modelDeploymentGroupForwardPairPrefillerHeader 指定 decoder 路由代理从中读取 prefiller 的请求头名称。
//
// 它是 router 自身使用的请求头，即选择器在选定 prefiller 时写入、代理负责读取的同一个请求头。
// operator 写入该请求头以指定目标 prefiller，从而使原本自行选择的代理按本 operator 指定的配对路由。
const modelDeploymentGroupForwardPairPrefillerHeader = "x-prefiller-host-port"

// modelDeploymentGroupForwardPairRankHeader 指定用于读取 data-parallel rank 的请求头名称。
const modelDeploymentGroupForwardPairRankHeader = "X-Data-Parallel-Rank"

// modelDeploymentGroupForwardPairBootstrapPortAnnotation 公布 prefiller 的 bootstrap 端口，
// 供需要从中获取传输端点的组件使用。其值为十进制字符串形式的端口号。
const modelDeploymentGroupForwardPairBootstrapPortAnnotation = "sglang.ai/bootstrap-port"

// modelDeploymentGroupForwardPairShape 标识有向配对探测所使用的路径。
//
// 它采用枚举值而非布尔值，因为这两种形态回答的问题不同。点对点形态由自身驱动
// 两个端头并枚举两个分组的每一对 rank；代理形态则向 decode 分组的路由代理发起
// 一次请求，由代理在其前置的引擎内部自行挑选 rank。如果调用方无法区分二者，就
// 会把代理形态的 Verified 误判为覆盖全量 rank 的证明，而事实并非如此。
type modelDeploymentGroupForwardPairShape string

const (
	// modelDeploymentGroupForwardPairShapeHeadToHead 表示由本 operator 直接驱动
	// 两个端头并向两个分组的每一对 rank 发起请求的路径。
	modelDeploymentGroupForwardPairShapeHeadToHead modelDeploymentGroupForwardPairShape = "HeadToHead"

	// modelDeploymentGroupForwardPairShapeViaRoutingProxy 表示由 decode 分组的路由代理
	// 驱动该配对并在其前置的引擎内部挑选 rank 的路径。
	modelDeploymentGroupForwardPairShapeViaRoutingProxy modelDeploymentGroupForwardPairShape = "ViaRoutingProxy"
)

// modelDeploymentGroupForwardPairAttempt 是单次有向配对探测的类型化记录：包含其
// 绑定的两个分组成员代次、发送的请求标识符以及连接两条分支所用的房间号。
//
// 这里同时记录双方的代次而非仅记录结果所携带的那一个，因为一次探测同时代表两个
// 分组，读取者必须能够分辨具体是哪两个分组。
type modelDeploymentGroupForwardPairAttempt struct {
	// PrefillGeneration 是本次探测所针对的 prefill 分组的成员代次。
	PrefillGeneration modelDeploymentMembershipGeneration
	// DecodeGeneration 是本次探测所针对的 decode 分组的成员代次。
	DecodeGeneration modelDeploymentMembershipGeneration
	// Shape 是本次探测采用的形态，使读取者无需查看原因即可区分代理形态与覆盖全量
	// rank 形态的结果。
	Shape modelDeploymentGroupForwardPairShape
	// RequestIDs 是本次探测发送的请求标识符列表，每个被探测的配对对应一个。
	RequestIDs []string
	// BootstrapRooms 是本次探测用于连接两条分支的房间号列表，每个被探测的配对对应
	// 一个。代理路径不记录房间号，因为该路径下由代理自行挑选房间。
	BootstrapRooms []int64
	// Mooncake 保存两端响应中携带的 Mooncake 暴露指标行，每个序列一条，并以所属半区
	// 作为前缀。prefill 半区的序列是推送证据；连接器未导出成功读取的计数器，因此
	// 复述出的响应内容仍作为读取侧的证明。
	Mooncake []string
}

// modelDeploymentGroupForwardPairResult 是单次有向配对探测的结果：每个角色一个
// 结果，以及产生这些结果的探测记录。
type modelDeploymentGroupForwardPairResult struct {
	// Prefill 是 prefill 分组的结果。
	Prefill modelDeploymentGroupForward
	// Decode 是 decode 分组的结果。
	Decode modelDeploymentGroupForward
	// Attempt 是探测所发起请求与发送内容的类型化记录。
	Attempt modelDeploymentGroupForwardPairAttempt
}

// modelDeploymentGroupForwardPairRequest 表示有向配对中的单个请求：发往何处、
// 携带什么内容以及如何声明其目标 rank。
//
// 它与普通探测的请求结构分开定义，因为配对请求携带了普通探测从不需要的请求头：
// 有的引擎从请求头读取目标 rank，还有的引擎通过请求头被告知要与哪个 prefiller 配对。
type modelDeploymentGroupForwardPairRequest struct {
	// Method 是 HTTP 方法。空值表示 POST，即本 operator 探测的所有路由读取请求时
	// 所用的方法。
	Method string
	// URL 是请求发送的目标地址。
	URL string
	// Body 是请求体内容。不读取请求体的请求不携带该字段。
	Body []byte
	// Headers 是请求头集合。不需要请求头的请求不携带该字段。
	Headers map[string]string
}

// modelDeploymentGroupForwardPairFetch 执行有向配对中的单个请求并返回响应体。
//
// 将其定义为函数而非直接发起网络连接，是因为本文件需要准确处理各类失败状态，
// 而真实的连接无法按需触发失败。fetch 为 nil 表示该配对没有传输通道，因此不会
// 产生观测结果，其状态判定为 Unknown 并保持挂起。
type modelDeploymentGroupForwardPairFetch func(
	ctx context.Context, request modelDeploymentGroupForwardPairRequest,
) ([]byte, error)

// modelDeploymentGroupForwardPairLeg 表示有向配对中的一条请求分支：发往何处、
// 携带什么内容以及其响应必须证明什么。
type modelDeploymentGroupForwardPairLeg struct {
	// Name 是在原因说明中对该分支的称呼。
	Name string
	// Request 是该分支携带的请求。
	Request modelDeploymentGroupForwardPairRequest
	// Copied 表示该分支的响应必须包含复述的文本。未要求复述的分支仅检查其请求绑定：
	// prefiller 在移交 KV 后只返回部分生成结果，无法由此判断传输是否已经发生。
	Copied bool
}

// modelDeploymentGroupForwardPairReply 表示单个分支响应所传达的信息。
type modelDeploymentGroupForwardPairReply struct {
	// Unreadable 表示响应体无法按引擎自身的响应格式解析，因此无法判断其中任何信息，
	// 甚至无法确认是否携带了请求标识符。
	Unreadable bool
	// Bound 表示响应中携带了本 operator 发出的请求标识符。
	Bound bool
	// Reason 是引擎记录的结束原因（finish reason）。
	Reason string
	// Content 是响应生成的文本内容。
	Content string
}

// modelDeploymentGroupForwardPairInput 是构建单个配对请求所需的输入参数。
type modelDeploymentGroupForwardPairInput struct {
	// Model 是引擎对外服务的模型名称。
	Model string
	// RequestID 是该配对各分支携带的请求标识符。
	RequestID string
	// Expected 是 decoder 必须复述返回的预期文本。
	Expected string
	// RequestIDField 是引擎从请求体中读取请求标识符的字段名。
	RequestIDField string
	// ChatFields 是除标识符与消息列表之外，引擎的 chat 路由还会读取的附加字段。
	ChatFields map[string]any
	// PrefillURL 和 DecodeURL 分别是向两个端头发起请求的地址。当 decoder 前置有路由
	// 代理时，DecodeURL 为该代理的入口地址，否则为引擎自身的地址。
	PrefillURL string
	DecodeURL  string
	// BootstrapHost 是 prefiller 自身的地址，decode 侧通过该地址获取传输端点。
	BootstrapHost string
	// BootstrapPort 是 prefiller 提供传输端点服务的端口。
	BootstrapPort int
	// BootstrapAddress 是符合引擎握手协议格式要求的 prefiller 地址表示。
	BootstrapAddress string
	// BootstrapRoom 是连接两条分支所用的房间号。
	BootstrapRoom int64
	// PrefillRank 和 DecodeRank 是该配对所指向的数据并行（DP）rank。
	PrefillRank int
	DecodeRank  int
	// RemoteEngineID 是当引擎握手协议需要指定时，prefiller 在该 rank 上的引擎 ID。
	RemoteEngineID string
	// TransferID 是当引擎握手协议携带传输标识时，本次传输自身的标识符。
	TransferID string
}

// modelDeploymentGroupForwardPairEngine 封装了单个引擎在有向配对探测中所需的全部
// 信息：请求发送的路由、chat 路由读取的字段，以及单个 rank 配对两端头携带的请求。
//
// 引擎间的差异全部收敛于此而非放在探测逻辑中，因为探测逻辑必须准确判定各类失败
// 状态，若按引擎分叉流程就会引入第二个判定入口。
type modelDeploymentGroupForwardPairEngine struct {
	// Path 是请求发送的目标路由路径。
	Path string
	// RequestIDField 是引擎从请求体中读取请求标识符的字段名。
	RequestIDField string
	// IDPrefix 是引擎在响应的 id 字段中加在请求标识符前面的前缀。
	IDPrefix string
	// ChatFields 是除标识符与消息列表之外，引擎的 chat 路由还会读取的附加字段。
	ChatFields map[string]any
	// Mooncake 表示该引擎的传输连接器是否会导出需在配对旁记录的 Mooncake 指标序列。
	// 不提供此类指标的引擎不会被抓取，因此其观测记录中也不会包含未运行连接器的相关行。
	Mooncake bool
	// Heads 生成单个 rank 配对的两个端头各自携带的请求。
	Heads func(in modelDeploymentGroupForwardPairInput) ([]modelDeploymentGroupForwardPairLeg, error)
}

// modelDeploymentGroupForwardPairEngines 是有向配对探测的按引擎划分的配置矩阵。
// 未包含在其中的引擎没有经过验证的观测方法，探测时将直接指明该引擎而非猜测原因。
var modelDeploymentGroupForwardPairEngines = map[string]modelDeploymentGroupForwardPairEngine{
	workercore.ModelDeploymentEngineSGLang: {
		// chat 路由会接收请求标识符并在响应中返回它，同时也是实测配对所运行的路由。
		Path:           "/v1/chat/completions",
		RequestIDField: "rid",
		// 该引擎在响应的 id 字段中不会在请求标识符前添加任何前缀。
		IDPrefix: "",
		// 思考开关用于防止输出推理过程的模型把推理内容放进回答，从而导致无法匹配预期的复述文本。
		ChatFields: map[string]any{"chat_template_kwargs": map[string]any{"enable_thinking": false}},
		Heads: func(in modelDeploymentGroupForwardPairInput) ([]modelDeploymentGroupForwardPairLeg, error) {
			prefill, err := modelDeploymentGroupForwardPairSGLangBody(in, in.PrefillRank)
			if err != nil {
				return nil, err
			}
			decode, err := modelDeploymentGroupForwardPairSGLangBody(in, in.DecodeRank)
			if err != nil {
				return nil, err
			}

			return []modelDeploymentGroupForwardPairLeg{
				{
					Name:    "prefill",
					Request: modelDeploymentGroupForwardPairRequest{URL: in.PrefillURL, Body: prefill},
				},
				{
					Name: "decode",
					Request: modelDeploymentGroupForwardPairRequest{
						URL: in.DecodeURL, Body: decode,
					},
					Copied: true,
				},
			}, nil
		},
	},
	workercore.ModelDeploymentEngineVLLM: {
		Path:           "/v1/chat/completions",
		RequestIDField: "request_id",
		// 该引擎会在标识符前加上所响应补全类型的前缀。
		IDPrefix: "chatcmpl-",
		Mooncake: true,
		Heads: func(in modelDeploymentGroupForwardPairInput) ([]modelDeploymentGroupForwardPairLeg, error) {
			// prefiller rank 的引擎 ID 是握手阶段用于标识 prefiller 的名称，
			// 且当前 operator 不会读取 prefiller 自身的注册表来获取该 ID。需要该 ID 却无法获取时，
			// 配对会保持关闭，而不会猜测标识符。
			if in.RemoteEngineID == "" {
				return nil, fmt.Errorf(
					"the probe cannot discover the prefiller engine id for rank %d, and the "+
						"connector's handshake will not name the prefiller without it; the id is held "+
						"in the prefiller's own transfer registry", in.PrefillRank,
				)
			}
			prefill, err := modelDeploymentGroupForwardPairVLLMBody(in, map[string]any{
				"do_remote_decode":  true,
				"do_remote_prefill": false,
				"transfer_id":       in.TransferID,
			})
			if err != nil {
				return nil, err
			}
			decode, err := modelDeploymentGroupForwardPairVLLMBody(in, map[string]any{
				"do_remote_decode":      false,
				"do_remote_prefill":     true,
				"remote_bootstrap_addr": in.BootstrapAddress,
				"remote_engine_id":      in.RemoteEngineID,
				"transfer_id":           in.TransferID,
			})
			if err != nil {
				return nil, err
			}

			return []modelDeploymentGroupForwardPairLeg{
				{
					Name: "prefill",
					Request: modelDeploymentGroupForwardPairRequest{
						URL:     in.PrefillURL,
						Body:    prefill,
						Headers: map[string]string{modelDeploymentGroupForwardPairRankHeader: strconv.Itoa(in.PrefillRank)},
					},
				},
				{
					Name: "decode",
					Request: modelDeploymentGroupForwardPairRequest{
						URL: in.DecodeURL, Body: decode,
					},
					Copied: true,
				},
			}, nil
		},
	},
}

// observeModelDeploymentGroupForwardPair 将一个 prefill 组和一个 decode 组作为一个有向配对进行探测，
// 并同时给出两个角色的判定结果。
//
// 两个组作为单个配对进行探测，因为双方均无法单独提供服务：prefiller 计算 KV 而 decoder 消费它，
// 因此返回复制答案的请求能够同时证明双方正常，而未返回答案的请求则无法证明任何一方。
//
// 所有失败对两个角色都判定为 Unknown，而不会判定为 Failed。无法完成观测的配对不能说明
// 哪一方存在故障；因配对无法读取而将责任归咎于健康对端，会导致正常服务的组被下线。
func observeModelDeploymentGroupForwardPair(
	ctx context.Context, md *workercore.ModelDeployment,
	prefill, decode modelDeploymentReplicaView,
	fetch modelDeploymentGroupForwardPairFetch,
) modelDeploymentGroupForwardPairResult {
	// 探测形态取决于部署方式而非引擎：带有路由代理的 decode 角色通过该代理访问，由代理负责驱动
	// 传输阶段，因此 operator 只需向代理指定 prefiller 并发起一次请求。不带代理的 decode 角色
	// 则直接访问，此时由 operator 自行驱动两个主节点。
	decodeRole := modelDeploymentGroupForwardPairDecodeRole(md)
	shape := modelDeploymentGroupForwardPairShapeHeadToHead
	if decodeRole != nil && ModelDeploymentRoleFrontedByProxy(md, decodeRole) {
		shape = modelDeploymentGroupForwardPairShapeViaRoutingProxy
	}
	viaProxy := shape == modelDeploymentGroupForwardPairShapeViaRoutingProxy

	attempt := modelDeploymentGroupForwardPairAttempt{
		PrefillGeneration: modelDeploymentGenerationOf(prefill),
		DecodeGeneration:  modelDeploymentGenerationOf(decode),
		Shape:             shape,
	}

	engineName := md.Spec.Engine.Name
	engine, known := modelDeploymentGroupForwardPairEngines[engineName]
	if !known {
		return modelDeploymentGroupForwardPairHeld(attempt, modelDeploymentGroupForwardUnsupportedFor(
			"engine "+engineName+" has no group-forward observation this operator has verified",
		))
	}

	if fetch == nil {
		return modelDeploymentGroupForwardPairHeld(attempt, modelDeploymentGroupForward{
			State:  modelDeploymentGroupForwardUnknown,
			Reason: "no group-forward transport is wired, so neither side of the pair could be asked",
		})
	}

	prefillHead, reason := modelDeploymentGroupForwardPairHead(prefill)
	if reason != "" {
		return modelDeploymentGroupForwardPairHeld(attempt, modelDeploymentGroupForward{
			State:  modelDeploymentGroupForwardUnknown,
			Reason: "the prefill group has no address to answer for it: " + reason,
		})
	}
	decodeHead, reason := modelDeploymentGroupForwardPairHead(decode)
	if reason != "" {
		return modelDeploymentGroupForwardPairHeld(attempt, modelDeploymentGroupForward{
			State:  modelDeploymentGroupForwardUnknown,
			Reason: "the decode group has no address to answer for it: " + reason,
		})
	}

	prefillURL, err := modelDeploymentGroupForwardURL(prefillHead, engine.Path)
	if err != nil {
		return modelDeploymentGroupForwardPairHeld(attempt, modelDeploymentGroupForward{
			State:  modelDeploymentGroupForwardUnknown,
			Reason: "the prefill group has no address to be asked at: " + err.Error(),
		})
	}
	decodeURL, err := modelDeploymentGroupForwardPairDecodeURL(
		md, decodeRole, decodeHead, engine.Path, viaProxy,
	)
	if err != nil {
		return modelDeploymentGroupForwardPairHeld(attempt, modelDeploymentGroupForward{
			State:  modelDeploymentGroupForwardUnknown,
			Reason: "the decode group has no address to be asked at: " + err.Error(),
		})
	}

	// rank 数量来自各角色自身的声明配置，这是声明引擎数据并行宽度的唯一位置。
	// 未声明宽度时，默认采用引擎自身的默认值 1。
	pair, err := modelDeploymentDeclaredParallelismPair(md)
	if err != nil {
		return modelDeploymentGroupForwardPairHeld(attempt, modelDeploymentGroupForward{
			State:  modelDeploymentGroupForwardUnknown,
			Reason: "the roles' declared parallelism could not be read: " + err.Error(),
		})
	}
	prefillWidth := pair.Prefill.DataParallel
	if prefillWidth < 1 {
		prefillWidth = 1
	}
	decodeWidth := pair.Decode.DataParallel
	if decodeWidth < 1 {
		decodeWidth = 1
	}
	if reason := modelDeploymentGroupForwardPairWidthBound("prefill", prefillWidth, shape); reason != "" {
		return modelDeploymentGroupForwardPairHeld(attempt, modelDeploymentGroupForward{
			State: modelDeploymentGroupForwardUnknown, Reason: reason,
		})
	}
	if reason := modelDeploymentGroupForwardPairWidthBound("decode", decodeWidth, shape); reason != "" {
		return modelDeploymentGroupForwardPairHeld(attempt, modelDeploymentGroupForward{
			State: modelDeploymentGroupForwardUnknown, Reason: reason,
		})
	}

	probeCtx, cancel := context.WithTimeout(ctx, modelDeploymentGroupForwardPairBound)
	defer cancel()

	var mooncakeRead bool

	for prefillRank := 0; prefillRank < prefillWidth; prefillRank++ {
		for decodeRank := 0; decodeRank < decodeWidth; decodeRank++ {
			if viaProxy && (prefillRank > 0 || decodeRank > 0) {
				// 代理会在其前端所服务的引擎内部自行选择 rank，因此只需探测一个配对，而无需为每个 rank
				// 单独探测。声明的并行宽度不会改变这一点：无论宽度是多少，该形态都只发起一次请求。
				continue
			}
			reply, room, reason := modelDeploymentGroupForwardPairProbe(
				probeCtx, md, engine, prefillHead, prefillURL, decodeURL,
				prefillWidth, prefillRank, decodeRank, viaProxy, fetch,
			)
			if reason != "" {
				return modelDeploymentGroupForwardPairHeld(attempt, modelDeploymentGroupForward{
					State: modelDeploymentGroupForwardUnknown, Reason: reason,
				})
			}
			attempt.RequestIDs = append(attempt.RequestIDs, reply)
			if !viaProxy {
				// 每个直连配对都会记录 room，且 0 号 room 与其他 room 同等有效。代理形态不记录 room，
				// 因为由代理自行选择用于关联配对的 room，当前 operator 在该场景下不会构建 room。
				attempt.BootstrapRooms = append(attempt.BootstrapRooms, room)
			}
			if !mooncakeRead && engine.Mooncake {
				// 在首个配对完成且传输结果尚新时，从两端各读取一次 Mooncake 指标序列：prefill 端的
				// 指标导出包含发送端推送证据，而 decode 端包含其连接器对此的处理结果。两端的地址
				// 不随 rank 变化，且指标导出值为累计值，因此后续配对只会重复相同的指标行。
				mooncakeRead = true
				attempt.Mooncake = append(attempt.Mooncake,
					modelDeploymentGroupForwardPairMooncakeSeries(probeCtx, prefillURL, engine.Path, "prefill", fetch)...)
				attempt.Mooncake = append(attempt.Mooncake,
					modelDeploymentGroupForwardPairMooncakeSeries(probeCtx, decodeURL, engine.Path, "decode", fetch)...)
			}
		}
	}

	return modelDeploymentGroupForwardPairVerified(attempt, len(attempt.RequestIDs))
}

// modelDeploymentGroupForwardPairProbe 向单个配对的各链路发起请求并读取其响应。
func modelDeploymentGroupForwardPairProbe(
	ctx context.Context, md *workercore.ModelDeployment, engine modelDeploymentGroupForwardPairEngine,
	prefillHead *core.Pod, prefillURL, decodeURL string,
	prefillWidth, prefillRank, decodeRank int, viaProxy bool,
	fetch modelDeploymentGroupForwardPairFetch,
) (string, int64, string) {
	nonce, err := modelDeploymentQualifyRandom(modelDeploymentQualifyNonceBytes)
	if err != nil {
		return "", 0, "the pair's identifier could not be generated: " + err.Error()
	}
	requestID := modelDeploymentQualifyRIDPrefix + "pd-" + nonce

	expected := modelDeploymentGroupForwardPairExpected(prefillRank, decodeRank)

	input := modelDeploymentGroupForwardPairInput{
		Model:          md.Spec.Model.Name,
		RequestID:      requestID,
		Expected:       expected,
		RequestIDField: engine.RequestIDField,
		ChatFields:     engine.ChatFields,
		PrefillURL:     prefillURL,
		DecodeURL:      decodeURL,
		PrefillRank:    prefillRank,
		DecodeRank:     decodeRank,
	}

	var legs []modelDeploymentGroupForwardPairLeg
	if viaProxy {
		leg, err := modelDeploymentGroupForwardPairProxyLeg(input, prefillHead)
		if err != nil {
			return "", 0, err.Error()
		}
		legs = []modelDeploymentGroupForwardPairLeg{leg}
	} else {
		input.BootstrapHost = prefillHead.Status.PodIP
		port, err := modelDeploymentGroupForwardPairBootstrapPort(prefillHead)
		if err != nil {
			return "", 0, err.Error()
		}
		input.BootstrapPort = port
		input.BootstrapAddress = prefillHead.Status.PodIP + ":" + strconv.Itoa(port)
		input.BootstrapRoom, err = modelDeploymentGroupForwardPairRoom(prefillWidth, prefillRank)
		if err != nil {
			return "", 0, err.Error()
		}
		input.TransferID = "xfer-" + requestID
		legs, err = engine.Heads(input)
		if err != nil {
			return "", 0, "the pair's requests could not be rendered: " + err.Error()
		}
	}

	replies, failures := modelDeploymentGroupForwardPairSend(ctx, legs, fetch, requestID, engine.IDPrefix)
	for i := range legs {
		if failures[i] != nil {
			return "", 0, "the " + legs[i].Name + " leg did not answer within the bound: " +
				failures[i].Error() + "; a pair that could not be observed is an absence of " +
				"evidence, and it is not attributed to one side"
		}
		reply := replies[i]
		if reply.Unreadable {
			return "", 0, "the " + legs[i].Name + " leg answered, but its answer could not be read " +
				"as the engine's reply shape, so nothing in it is evidence the pair carried one"
		}
		if !reply.Bound {
			return "", 0, "the " + legs[i].Name + " leg answered, but not bound to this operator's own " +
				"request, so the answer is not evidence that the pair carried one"
		}
		if !legs[i].Copied {
			if !modelDeploymentQualifyFinishRan(reply.Reason) {
				return "", 0, "the " + legs[i].Name + " leg answered, but its finish reason reports " +
					"the request did not complete, so the answer is not evidence that the pair " +
					"carried one"
			}
			continue
		}
		if !modelDeploymentQualifyFinishRan(reply.Reason) {
			return "", 0, "the " + legs[i].Name + " leg answered, but its finish reason reports " +
				"the request did not complete, so the answer is not evidence that the pair carried one"
		}
		// 比对时读取响应中的可见文本。无论思考开关如何设置，该路由上的聊天模板都会将回答包裹在
		// <think>…</think> 块中（case-9 run2 的详细原因捕获到空包裹块之后存在逐字节一致的复制内容），
		// 该包裹块属于模板噪声，而非模型回答。比对前会先剥离该块；被拒绝的回答则在原因中保留原始内容，
		// 以便在后续分类中仍可看到该包裹块。
		if visible := modelDeploymentGroupForwardPairVisibleCopy(reply.Content); visible != expected {
			// 原因中会附上长度受限且未剥离包裹块的回答内容，以便根据本次运行证据区分模型输出偏移
			// 与上下文不完整的情况。如果原因中丢弃了回答内容，就只能通过重跑来区分回答偏移、
			// 读取异常或思考内容泄露等问题，而本探测正是为了避免此类重跑。
			return "", 0, "the " + legs[i].Name + " leg answered, but it did not carry the text the " +
				"request asked to be copied, so nothing shows the prefill's KV reached it (answer: \"" +
				modelDeploymentGroupForwardPairBounded(reply.Content, modelDeploymentGroupForwardPairAnswerBound) + "\")"
		}
	}

	return requestID, input.BootstrapRoom, ""
}

// modelDeploymentGroupForwardPairThinkWrapper 跨行匹配回答开头的前导 <think>…</think> 块，
// 并允许该块两侧存在空白字符：部分变体下聊天模板会在包裹块前输出前导换行或空格，
// 若锚定匹配不允许空白字符，就会保留包裹块并误拒其后的复制文本。
var modelDeploymentGroupForwardPairThinkWrapper = regexp.MustCompile(`(?s)^\s*<think>.*?</think>\s*`)

// modelDeploymentGroupForwardPairVisibleCopy 从回答中剥离前导 <think>…</think> 块并修剪剩余内容，
// 仅保留要求 decode 链路复制的文本。
//
// 该包裹块是模板噪声而非实际内容。无论思考开关如何设置，Qwen3 聊天模板都会在该路由上输出
// 此包裹块（case-9 run2 的详细原因捕获到空包裹块之后存在逐字节一致的复制内容），因此精确比对
// 会先移除该块再读取回答。被拒绝的回答在原因中保留原始内容，包裹块仍可见。
func modelDeploymentGroupForwardPairVisibleCopy(answer string) string {
	return strings.TrimSpace(modelDeploymentGroupForwardPairThinkWrapper.ReplaceAllString(answer, ""))
}

// modelDeploymentGroupForwardPairAnswerBound 是失败原因中所携带的被拒回答的最大字符数。
// 200 个字符足以区分复制偏移与无关回答，同时又足够简短，可使原因保持在单行内。
const modelDeploymentGroupForwardPairAnswerBound = 200

// modelDeploymentGroupForwardPairBounded 按 rune 边界将文本截断至最多 limit 个字符，
// 并在截断处附加省略号。引擎会使用多种文字回答，若截断位置拆分了单个字符，就会破坏
// 所记录的证据内容。
func modelDeploymentGroupForwardPairBounded(text string, limit int) string {
	runes := []rune(text)
	if len(runes) <= limit {
		return text
	}
	return string(runes[:limit]) + "…"
}

// modelDeploymentGroupForwardPairMooncakeSeries 读取单端的指标导出内容，并将其归约为
// 观测结果所记录的 Mooncake 指标序列。
//
// 推送不等同于拉取。vllm:mooncake_bytes_transferred 是发送端的直方图，其 _count 表示
// prefill 端已完成的传输次数；连接器未导出成功读取的计数器（vllm:mooncake_num_failed_recvs
// 仅统计拉取失败次数），因此这些指标序列仅作为推送证据记录，而复制返回的回答仍作为读取端证明。
func modelDeploymentGroupForwardPairMooncakeSeries(
	ctx context.Context, chatURL, enginePath, half string, fetch modelDeploymentGroupForwardPairFetch,
) []string {
	body, err := fetch(ctx, modelDeploymentGroupForwardPairRequest{
		Method: http.MethodGet,
		URL:    strings.TrimSuffix(chatURL, enginePath) + "/metrics",
	})
	if err != nil {
		return []string{half + ": the mooncake exposition did not answer: " + err.Error()}
	}
	series := make([]string, 0)
	for _, line := range strings.Split(string(body), "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "vllm:mooncake_") || strings.HasSuffix(trimmed, "_bucket") {
			continue
		}
		series = append(series, half+": "+trimmed)
	}
	if len(series) == 0 {
		return []string{half + ": the mooncake exposition carries no vllm:mooncake_ series"}
	}
	return series
}

// modelDeploymentGroupForwardPairSend 并发请求配对中的每一条链路并读取其响应。
//
// 各链路必须同时发出，因为配对仅在两端请求同时在途时才成立：decode 端会等待 prefiller，
// 若在 decoder 超时放弃后再请求 prefiller，所触发的将是一次已无法完成的传输。
func modelDeploymentGroupForwardPairSend(
	ctx context.Context, legs []modelDeploymentGroupForwardPairLeg,
	fetch modelDeploymentGroupForwardPairFetch, requestID, idPrefix string,
) ([]modelDeploymentGroupForwardPairReply, []error) {
	replies := make([]modelDeploymentGroupForwardPairReply, len(legs))
	failures := make([]error, len(legs))

	var group sync.WaitGroup
	for i := range legs {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			body, err := fetch(ctx, legs[index].Request)
			if err != nil {
				failures[index] = err
				return
			}
			replies[index] = modelDeploymentGroupForwardPairChatRead(body, requestID, idPrefix)
		}(i)
	}
	group.Wait()

	return replies, failures
}

// modelDeploymentGroupForwardPairChatRead 读取单条链路的响应。
//
// 绑定关系由引擎自身决定：响应中的 id 必须包含当前 operator 发送的标识符，引擎可在其前
// 附加任意前缀。只有当完整 id 符合本 operator 所请求的标识时，携带 id 的响应才视为已绑定。
func modelDeploymentGroupForwardPairChatRead(
	body []byte, requestID, idPrefix string,
) modelDeploymentGroupForwardPairReply {
	var response struct {
		ID      string `json:"id"`
		Choices []struct {
			FinishReason *string `json:"finish_reason"`
			Message      struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if json.Unmarshal(body, &response) != nil {
		return modelDeploymentGroupForwardPairReply{Unreadable: true}
	}

	reply := modelDeploymentGroupForwardPairReply{Bound: response.ID == idPrefix+requestID}
	if len(response.Choices) == 0 || response.Choices[0].FinishReason == nil {
		return reply
	}
	reply.Reason = *response.Choices[0].FinishReason
	reply.Content = strings.TrimSpace(response.Choices[0].Message.Content)

	return reply
}

// modelDeploymentGroupForwardPairChatFields 返回有向配对中每条链路除引擎自身握手参数之外
// 所携带的公共字段：模型名称、请求标识符、消息列表以及响应格式参数。
func modelDeploymentGroupForwardPairChatFields(in modelDeploymentGroupForwardPairInput) map[string]any {
	fields := map[string]any{
		"model":       in.Model,
		"messages":    []map[string]string{{"role": "user", "content": modelDeploymentGroupForwardPairCopy(in.Expected)}},
		"max_tokens":  modelDeploymentGroupForwardPairMaxTokens,
		"temperature": 0,
		"stream":      false,
	}
	if in.RequestIDField != "" {
		fields[in.RequestIDField] = in.RequestID
	}
	for name, value := range in.ChatFields {
		fields[name] = value
	}

	return fields
}

// modelDeploymentGroupForwardPairSGLangBody 构造有向配对中单条链路发送给在请求体中
// 指定 rank 并在请求中指定 prefiller 的引擎的请求体。
func modelDeploymentGroupForwardPairSGLangBody(
	in modelDeploymentGroupForwardPairInput, rank int,
) ([]byte, error) {
	fields := modelDeploymentGroupForwardPairChatFields(in)
	fields["bootstrap_host"] = in.BootstrapHost
	fields["bootstrap_port"] = in.BootstrapPort
	fields["bootstrap_room"] = in.BootstrapRoom
	fields["routed_dp_rank"] = rank

	return json.Marshal(fields)
}

// modelDeploymentGroupForwardPairVLLMBody 构造有向配对中单条链路发送给在请求自身
// transfer 参数中携带握手信息的引擎的请求体。
func modelDeploymentGroupForwardPairVLLMBody(
	in modelDeploymentGroupForwardPairInput, transfer map[string]any,
) ([]byte, error) {
	fields := modelDeploymentGroupForwardPairChatFields(in)
	fields["kv_transfer_params"] = transfer

	return json.Marshal(fields)
}

// modelDeploymentGroupForwardPairProxyLeg 构造发往 decoder 路由代理的请求。
//
// 传输阶段由代理负责驱动，因此 operator 不构建握手参数：它在路由器选择器使用的请求头中
// 指定 prefiller，随后由代理向 prefiller 自身的注册表查询传输端点。
//
// 该请求发往代理自身的端口（即 decode Service 所指向的前端入口端口），而 prefiller 使用其
// 自身的引擎地址标识，因为其前方没有代理。
//
// 此处不断言 rank 覆盖范围，该形态也无法证明这一点。配对由路由代理驱动，探测仅观测代理的
// 响应、本 operator 自身的请求标识符以及复制的文本。它无法观测 prefiller 的响应，因为代理
// 会协同执行 prefill 与 decode 并仅流式返回 decode 的回答；它也无法覆盖各个数据并行 rank：
// 用于关联配对的 room 以及前端所服务引擎内部的 rank 均由代理自行选择，本 operator 的 room
// 计算在此不起作用。此外，若缺少 prefiller 请求头，代理会跳过分离执行并仅转发至 decode，
// 因此单独由 decode 引擎也能产生带有复制文本的绑定响应。
func modelDeploymentGroupForwardPairProxyLeg(
	in modelDeploymentGroupForwardPairInput, prefillHead *core.Pod,
) (modelDeploymentGroupForwardPairLeg, error) {
	address, err := modelDeploymentGroupForwardHostPort(prefillHead)
	if err != nil {
		return modelDeploymentGroupForwardPairLeg{}, fmt.Errorf(
			"the prefill group has no address to name to the decode side: %w", err,
		)
	}
	body, err := json.Marshal(modelDeploymentGroupForwardPairChatFields(in))
	if err != nil {
		return modelDeploymentGroupForwardPairLeg{}, err
	}

	return modelDeploymentGroupForwardPairLeg{
		Name: "decode",
		Request: modelDeploymentGroupForwardPairRequest{
			URL:  in.DecodeURL,
			Body: body,
			Headers: map[string]string{
				modelDeploymentGroupForwardPairPrefillerHeader: address,
			},
		},
		Copied: true,
	}, nil
}

// modelDeploymentGroupForwardPairDecodeRole 返回部署的 decode 角色，即配对中 decode 链路
// 所请求的目标角色。
//
// 返回角色对象而非布尔标志，因为访问该角色所使用的端口取决于 operator 为其渲染的配置：
// 前置路由代理的 decoder 需访问其前端入口端口，而无代理的 decoder 则直接访问引擎自身地址。
func modelDeploymentGroupForwardPairDecodeRole(
	md *workercore.ModelDeployment,
) *workercore.ModelDeploymentRole {
	for i := range md.Spec.Roles {
		role := &md.Spec.Roles[i]
		if ModelDeploymentEffectiveRoleKind(role) == workercore.ModelDeploymentRoleKindDecode {
			return role
		}
	}

	return nil
}

// modelDeploymentGroupForwardPairDecodeURL 构建 decode 链路请求的目标地址。
//
// 前置代理的 decoder 需访问其前端入口端口，即其 Service 指向且由路由代理监听的端口。
// 此类成员上的 annotation 记录的是代理背后引擎的端口，若客户端读取该端口，将会请求一个
// 代理外部无法直接响应的端口。无代理的 decoder 则直接访问引擎自身的地址。
func modelDeploymentGroupForwardPairDecodeURL(
	md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole,
	decodeHead *core.Pod, path string, viaProxy bool,
) (string, error) {
	if !viaProxy || role == nil {
		return modelDeploymentGroupForwardURL(decodeHead, path)
	}

	return modelDeploymentGroupForwardURLOnPort(
		decodeHead, ModelDeploymentRoleServingPort(md, role), path,
	)
}

// modelDeploymentGroupForwardPairHead 返回组内提供引擎请求路由的成员。
//
// 按构造约定该成员即主节点，因为分离式部署组中的其他成员仅运行引擎工作进程，自身不提供
// 请求路由：由主节点将请求路由至 operator 指定的 rank，组内所有 rank 均通过该单一地址访问。
func modelDeploymentGroupForwardPairHead(view modelDeploymentReplicaView) (*core.Pod, string) {
	if len(view.Members) == 1 {
		return view.Members[0], ""
	}
	for _, member := range view.Members {
		if modelDeploymentAnsweringMemberLeader(member) {
			return member, ""
		}
	}

	return nil, "no member of the group carries the leader index"
}

// modelDeploymentGroupForwardPairBootstrapPort 读取 prefiller 传输端点所监听的端口。
//
// 该端口直接从 prefiller 自身的 annotation 中读取，这是向需要获取这些端点的调用方发布
// 该端口的唯一位置。未发布端口的 prefiller 无法进行配对，明确报错优于使用 operator 猜测
// 的端口号发起连接。
func modelDeploymentGroupForwardPairBootstrapPort(prefillHead *core.Pod) (int, error) {
	published := prefillHead.Annotations[modelDeploymentGroupForwardPairBootstrapPortAnnotation]
	if published == "" {
		return 0, fmt.Errorf(
			"the prefill head publishes no bootstrap port, so the decode side cannot be told where " +
				"to fetch the transfer endpoints from",
		)
	}
	port, err := strconv.Atoi(published)
	if err != nil || port < 1 || port > 65535 {
		return 0, fmt.Errorf("the prefill head publishes a bootstrap port this operator cannot read: %q",
			published)
	}

	return port, nil
}

// modelDeploymentGroupForwardPairRoom 为单个 rank 配对分配用于关联的 bootstrap room。
//
// room 的低位携带 prefill rank：room 对 prefiller 数据并行宽度取模的结果即为请求被路由到的
// rank，因此为某一个 rank 构建的 room 不会响应另一个 rank。
func modelDeploymentGroupForwardPairRoom(width, rank int) (int64, error) {
	buffer := make([]byte, 8)
	if _, err := rand.Read(buffer); err != nil {
		return 0, fmt.Errorf("the probe's random source failed: %w", err)
	}
	bits := binary.BigEndian.Uint64(buffer) >> 12

	return int64(bits*uint64(width) + uint64(rank)), nil
}

// modelDeploymentGroupForwardPairMaxWidth 是配对探测能够寻址的最大数据并行组宽度。
//
// 这是 room 算术本身的限制，而非策略上限。room 由一个随机 52 位数值乘以宽度再加上 rank
// 偏移生成，room 所指定的 rank 即 room 对宽度取模的值，因此最大的 room 为 2^52 * width - 1。
// 当宽度为 2048 时该值仍在 int64 的正数范围内，到 2049 时则会溢出。因此超过该上限的宽度
// 不予探测：此时 operator 构建的 room 将无法准确寻址其指定的 rank，而发往错误 rank 的请求
// 也无法证明配对的有效性。
const modelDeploymentGroupForwardPairMaxWidth = 2048

// modelDeploymentGroupForwardPairWidthBound 返回当声明的数据并行宽度超过本 operator 探测上限时
// 不予探测的原因；若宽度在上限以内，则返回空字符串。
//
// 原因中会指明具体一侧，因为两个组分别声明各自的宽度，调用方需要能够分辨是哪一侧被拒绝探测。
//
// 原因的表述随探测形态而异，因为两种形态受限于该上限的原因不同。直连形态会自行构建 room，
// 超过上限会导致 room 超出 rank 算术所能表示的范围；而代理形态从不构建 room（由代理选择），
// 因此在该形态下该上限仅代表探测自身支持的宽度限制，不再作其他隐含说明。
func modelDeploymentGroupForwardPairWidthBound(
	side string, width int, shape modelDeploymentGroupForwardPairShape,
) string {
	if width <= modelDeploymentGroupForwardPairMaxWidth {
		return ""
	}

	if shape == modelDeploymentGroupForwardPairShapeViaRoutingProxy {
		return "the " + side + " group declares a data-parallel width of " + strconv.Itoa(width) +
			", above this probe's supported limit of " +
			strconv.Itoa(modelDeploymentGroupForwardPairMaxWidth) +
			"; the limit applies to both pair shapes"
	}

	return "the " + side + " group declares a data-parallel width of " + strconv.Itoa(width) +
		", which is above the bound of " + strconv.Itoa(modelDeploymentGroupForwardPairMaxWidth) +
		" this operator probes; the bootstrap room is a random 52-bit value scaled by that width and " +
		"offset by the rank, so a width above the bound would carry the room outside the range the " +
		"rank arithmetic can address"
}

// modelDeploymentGroupForwardPairExpected 是单个配对的 decoder 必须原样复制返回的文本。
//
// 文本中包含配对标识，因为携带其他配对文本的回答不属于当前配对，且比对采用精确匹配：
// decoder 输出的任何其他内容都不算作有效复制。
func modelDeploymentGroupForwardPairExpected(prefillRank, decodeRank int) string {
	return fmt.Sprintf("探测P%dD%d：星河春风。", prefillRank, decodeRank)
}

// modelDeploymentGroupForwardPairCopy 生成用于校验 decoder 回答的提示词指令。
// 该指令要求仅输出引号内的文字而不包含任何其他内容，从而确保返回的回答可供本 operator
// 进行精确比对。
func modelDeploymentGroupForwardPairCopy(expected string) string {
	return "逐字复制引号内文字，只输出文字本身：『" + expected + "』 /no_think"
}

// modelDeploymentGroupForwardPairHeld 为两个角色返回相同的判定结果，用于表示无法完成观测的
// 配对：既不能说明任何一方有故障，也不能证明任何一方正常。
func modelDeploymentGroupForwardPairHeld(
	attempt modelDeploymentGroupForwardPairAttempt, result modelDeploymentGroupForward,
) modelDeploymentGroupForwardPairResult {
	return modelDeploymentGroupForwardPairResult{
		Prefill: result,
		Decode:  result,
		Attempt: attempt,
	}
}

// modelDeploymentGroupForwardPairVerified 为成功完成请求的配对向两个角色返回通过判定，
// 并各自附带所属组的 generation。
//
// 原因的表述随探测形态而异，因为两种形态所验证的内容不同。直连形态同时驱动了两个主节点，
// 能够从 decode 回答中确认传输已完成；而代理形态仅向 decode 组的路由代理发起了一次请求，
// 其回答只能说明代理存活、已绑定到本 operator 的请求标识符且返回了要求复制的文本。它无法
// 证明具体由哪对 rank 提供服务（因为 rank 由代理选择），也无法直接证明传输本身（因为传输阶段
// 由代理驱动）。
func modelDeploymentGroupForwardPairVerified(
	attempt modelDeploymentGroupForwardPairAttempt, pairs int,
) modelDeploymentGroupForwardPairResult {
	reason := "the prefill and decode legs both carried this operator's own request, and the " +
		"decode leg's answer shows the prefill's KV reached it"
	observation := fmt.Sprintf("%d rank pairs carried the request, each bound to its own identifier",
		pairs)
	if attempt.Shape == modelDeploymentGroupForwardPairShapeViaRoutingProxy {
		reason = "the decode group's routing proxy answered the probe while driving the pair with " +
			"the named prefiller, and its response matched this operator's own request identifier " +
			"and the exact text requested for copying"
		observation = "the routing proxy answered one request, bound to this operator's own identifier"
	}
	if len(attempt.Mooncake) > 0 {
		observation += "; the mooncake exposition: " + strings.Join(attempt.Mooncake, "; ")
		// 读取端没有成功计数器。连接器导出的指标序列仅统计发送端推送次数和拉取失败次数，因此
		// 观测说明会明确指出复制返回的回答才是拉取实际发生的证据，不能仅凭计数器判断。该说明
		// 仅随指标导出附带输出，因此不携带 Mooncake 指标序列的形态不会对未运行的连接器作任何说明。
		observation += "; the mooncake connector exports no read-side success counter " +
			"(vllm:mooncake_num_failed_recvs counts failed pulls only), so the copied answer is the read-side evidence"
	}

	return modelDeploymentGroupForwardPairResult{
		Prefill: modelDeploymentGroupForward{
			State:       modelDeploymentGroupForwardVerified,
			Reason:      reason,
			Generation:  attempt.PrefillGeneration,
			Observation: observation,
		},
		Decode: modelDeploymentGroupForward{
			State:       modelDeploymentGroupForwardVerified,
			Reason:      reason,
			Generation:  attempt.DecodeGeneration,
			Observation: observation,
		},
		Attempt: attempt,
	}
}
