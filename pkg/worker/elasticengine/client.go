// Package elasticengine 是 operator 用于访问引擎自身弹性宽度端点的客户端。
//
// 它只与固定版本 vLLM 的原生路由通信：下方两个缩扩容路由，以及 ObserveNativeWorld
// 用来探测前端实际执行范围的单个补全路由。源自 v0.29.0 serve/elastic_ep 路由器的原生契约为：
//
//	POST /scale_elastic_ep      {"new_data_parallel_size": positive int, "drain_timeout": positive int}
//	                            200 {"message": ...} | 400 | 408 | 500
//	POST /is_scaling_elastic_ep  200 {"is_scaling_elastic_ep": bool}
//
// 此处的成功仅代表一件事：引擎已接受该请求。它不代表实际生效的宽度，本包也无法提供
// 实际生效宽度。引擎上报的是它认为自己所处的宽度，这只是引擎的自述；只有随后实际提供
// 服务的请求，才是其正在服务的证据。scaling 标志也是如此，它仅表示缩扩容正在进行中，
// 绝不表示可以安全移除某个 worker。
//
// ObserveNativeWorld 是本包新增的唯一原生观测能力：围绕每个预期 rank 的一次真实前向
// 推理，在前后分别读取前端自身的 rank 校验信息。它测量引擎实际执行的边界以及哪些 rank
// 处理了 token。它不是成员关系证明，不涉及任何 Pod 或执行体名称，且前后校验一致也不能
// 排除前向推理期间发生变更的可能；控制器必须将其与自身的身份读取结合后才能据此采取行动。
//
// 本包中的所有操作均不重试。缩扩容是对集合通信组的变更操作，对可能已经生效的请求进行
// 重放会导致集合通信组最终处于非预期的宽度。不确定的响应会按不确定类型上报，由调用方的
// 状态机决定后续处理；重建逻辑归该状态机而非本客户端负责。
package elasticengine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"gpustack.ai/gpustack/pkg/utils/json"
)

const (
	// scalePath 是用于修改数据并行宽度的原生路由。
	scalePath = "/scale_elastic_ep"
	// isScalingPath 是用于上报缩扩容是否正在进行中的原生路由。
	isScalingPath = "/is_scaling_elastic_ep"
	// isScalingField 是原生响应携带的唯一字段。该字段必须存在且为布尔值，
	// 因为不包含该字段的响应没有表达任何有效信息。
	isScalingField = "is_scaling_elastic_ep"
	// ackField 是原生缩扩容响应携带的唯一字段。该字段必须存在且为字符串，
	// 因为不包含该字段的响应未确认该次缩扩容请求。
	ackField = "message"
)

// maxResponseBytes 限制单次响应的最大字节数。持续不断流式输出的成员会在此处被截断，
// 而不会任由其返回任意大小的响应。
const maxResponseBytes = 1 << 20

// Kind 表示调用失败的原因，按调用方需要采取的处理方式分类。
//
// 其中最关键的是不确定（ambiguous）类型。传输层丢弃请求、调用方取消请求，以及引擎已应用
// 请求但未能返回确认，这三种状态对于未事先记录意图的观察者而言毫无区别。将它们归为同一
// 类型，正是因为无法从失败本身推断出请求是否已生效。
type Kind int

const (
	// KindInvalidInput 表示本客户端拒绝发送的请求，或引擎因格式错误而拒绝的请求。未产生任何变更。
	KindInvalidInput Kind = iota
	// KindReconfiguring 表示引擎返回的 503：由于引擎当前已在重新配置中，暂不接受该请求。
	// 该请求未被应用。
	KindReconfiguring
	// KindNativeFailure 表示引擎返回的 408 或 500。请求已到达引擎且引擎报告无法完成，
	// 这与请求从未到达引擎属于不同事实。
	KindNativeFailure
	// KindAmbiguous 表示传输失败、请求被取消，或本客户端未能完整读取的任何响应。
	// 请求是否已应用不得而知，也绝不可擅自假设。
	KindAmbiguous
	// KindMalformed 表示已收到响应，但响应内容不符合该路由的约定格式。
	KindMalformed
)

// String 返回该类型的名称，用于错误原因字符串及日志记录。
func (k Kind) String() string {
	switch k {
	case KindInvalidInput:
		return "invalid input"
	case KindReconfiguring:
		return "engine is reconfiguring"
	case KindNativeFailure:
		return "engine reported failure"
	case KindAmbiguous:
		return "ambiguous"
	case KindMalformed:
		return "malformed response"
	default:
		return "unknown"
	}
}

// Error 是本包返回的所有已分类失败。
//
// 它携带状态码与路由信息，但不包含响应体。原生错误响应体由引擎自行决定内容，可能包含
// 模型名称、URL 或请求片段。决定下一步操作并不需要这些信息，而它们往往会泄露到日志中。
type Error struct {
	Kind Kind
	// Op 是被调用的路由。
	Op string
	// StatusCode 是引擎返回的状态码；若调用未产生状态码则为零。
	StatusCode int
	// Detail 是对错误原因的简短、非敏感描述。
	Detail string
	// Err 是用于构成错误链的底层原因。对于客户端在发送请求前作出的拒绝，该字段为 nil。
	Err error
}

func (e *Error) Error() string {
	if e.StatusCode != 0 {
		return fmt.Sprintf("elastic engine %s: %s (status %d): %s", e.Op, e.Kind, e.StatusCode, e.Detail)
	}

	return fmt.Sprintf("elastic engine %s: %s: %s", e.Op, e.Kind, e.Detail)
}

// invalid 构造一个在发送任何数据之前作出的拒绝错误。
func invalid(op, format string, args ...any) *Error {
	return &Error{Kind: KindInvalidInput, Op: op, Detail: fmt.Sprintf(format, args...)}
}

// Client 是针对单个引擎弹性端点配置的不可变客户端。
//
// 它在构造后不可变且可安全并发使用。所有字段均为只读，底层的 http.Client 本身也是
// 并发安全的，且没有任何方法会修改客户端状态，因此无需加锁也不会引入并发问题。
type Client struct {
	baseURL string
	http    *http.Client
	timeout time.Duration
}

// callContext 使用 operator 的超时时间限制单次调用。
//
// 超时设置会遵循父 ctx 中更早的截止时间，因此已自行设定上限的调用方会保留其更严格的
// 限制，两者中较早到期的截止时间始终优先。
func (c *Client) callContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, c.timeout)
}

// post 发送单次请求并返回受大小限制的响应体，同时对过程中的各类失败进行分类。
func (c *Client) post(ctx context.Context, op, path string, payload any) ([]byte, *Error) {
	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return nil, &Error{
				Kind: KindInvalidInput, Op: op,
				Detail: "the request body could not be encoded", Err: err,
			}
		}
		body = bytes.NewReader(encoded)
	}

	callCtx, cancel := c.callContext(ctx)
	defer cancel()

	request, err := http.NewRequestWithContext(
		callCtx, http.MethodPost, c.baseURL+path, body,
	)
	if err != nil {
		return nil, &Error{Kind: KindInvalidInput, Op: op, Detail: "the request could not be built", Err: err}
	}
	if payload != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	request.Header.Set("Accept", "application/json")

	response, err := c.http.Do(request)
	if err != nil {
		// 被取消或超时的调用属于不确定状态，而非明确失败。请求可能在客户端停止接收响应前
		// 就已生效，若将其上报为失败，可能会诱使调用方重试一个已经发生的变更。
		kind := KindAmbiguous
		detail := "the request could not be completed"
		if errors.Is(err, context.Canceled) {
			detail = "the caller cancelled the request"
		} else if errors.Is(err, context.DeadlineExceeded) {
			detail = "the call exceeded its bound"
		}

		return nil, &Error{Kind: kind, Op: op, Detail: detail, Err: err}
	}
	defer response.Body.Close()

	// 读取时即对响应体大小进行限制，使持续写入不停止的成员被及时截断，而不会无限
	// 膨胀响应数据。
	raw, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return nil, &Error{
			Kind: KindAmbiguous, Op: op,
			Detail: "the answer could not be read to its end", Err: err,
		}
	}
	if len(raw) > maxResponseBytes {
		return nil, &Error{
			Kind: KindMalformed, Op: op,
			Detail: fmt.Sprintf("the answer exceeded the %d byte bound", maxResponseBytes),
		}
	}

	switch {
	case response.StatusCode == http.StatusOK:
		return raw, nil
	case response.StatusCode == http.StatusServiceUnavailable:
		return nil, &Error{
			Kind: KindReconfiguring, Op: op, StatusCode: response.StatusCode,
			Detail: "the engine is not accepting requests while it reconfigures",
		}
	case response.StatusCode == http.StatusRequestTimeout,
		response.StatusCode >= http.StatusInternalServerError:
		return nil, &Error{
			Kind: KindNativeFailure, Op: op, StatusCode: response.StatusCode,
			Detail: "the engine received the request and reported that it could not complete it",
		}
	case response.StatusCode == http.StatusBadRequest:
		return nil, &Error{
			Kind: KindInvalidInput, Op: op, StatusCode: response.StatusCode,
			Detail: "the engine rejected the request as malformed",
		}
	default:
		return nil, &Error{
			Kind: KindMalformed, Op: op, StatusCode: response.StatusCode,
			Detail: "the engine answered with a status this client does not model",
		}
	}
}

// scaleRequest 是原生请求体，按原生字段命名。
type scaleRequest struct {
	NewDataParallelSize int `json:"new_data_parallel_size"`
	DrainTimeout        int `json:"drain_timeout"`
}

// Scale 请求引擎切换至新的数据并行宽度。
//
// 该请求仅发送一次。此处不设重试，也不存在将失败转为再次尝试的路径，因为本客户端
// 无法判断一次失败的调用是否已在服务端生效。
//
// 返回 nil error 表示引擎已按路由定义接受并完成了该请求，但这并不意味着引擎已在
// 新宽度下提供服务；将其等同于已生效的调用方是把确认回执误读为了既成事实。
func (c *Client) Scale(ctx context.Context, newDataParallelSize int, drainTimeout time.Duration) error {
	const op = scalePath

	// 在构造任何请求前先校验输入参数，因为被拒绝的请求不得触及引擎。原生路由对这类情况
	// 同样返回 400，若依赖服务端校验则会把本地参数错误变成一次不必要的网络往返。
	if newDataParallelSize <= 0 {
		return invalid(op, "the new data parallel size must be positive, got %d", newDataParallelSize)
	}
	if drainTimeout <= 0 {
		return invalid(op, "the drain timeout must be positive, got %s", drainTimeout)
	}
	seconds := int(drainTimeout / time.Second)
	if seconds <= 0 {
		return invalid(op, "the drain timeout of %s is under a second", drainTimeout)
	}

	raw, failure := c.post(ctx, op, scalePath, scaleRequest{
		NewDataParallelSize: newDataParallelSize,
		DrainTimeout:        seconds,
	})
	if failure != nil {
		return failure
	}

	// 此处显式返回 nil。直接返回 *Error 会将类型为 *Error 的 nil 装入非 nil 的 error
	// 接口中，导致调用方在引擎已确认的情况下误判为失败。
	if failure := acknowledge(op, raw); failure != nil {
		return failure
	}

	return nil
}

// acknowledge 依据路由约定校验原生 200 响应体。
//
// 该路由约定返回包含 message 字符串的单个 JSON 对象。即便响应体是合法 JSON，只要
// 不是该对象就未确认任何操作；若此处返回 nil error，就会凭一段该路由从不发送的文档
// 误导调用方认为变更已被接受。
func acknowledge(op string, raw []byte) *Error {
	answer, failure := decodeObject(op, raw)
	if failure != nil {
		return failure
	}

	message, present := answer[ackField]
	if !present {
		return &Error{
			Kind: KindMalformed, Op: op, Detail: "the acknowledgement does not carry " + ackField,
		}
	}
	// 在反序列化之前拒绝 null，因为将 null 反序列化到 string 只会得到空字符串而不会报任何 error。
	if string(message) == "null" {
		return &Error{
			Kind: KindMalformed, Op: op, Detail: ackField + " is null rather than a string",
		}
	}
	var text string
	if err := json.Unmarshal(message, &text); err != nil {
		return &Error{
			Kind: KindMalformed, Op: op,
			Detail: ackField + " is not a string", Err: err,
		}
	}

	return nil
}

// decodeObject 从响应中严格读取单个 JSON 对象。
//
// 此处刻意拒绝 null。将其解码到 map 会成功且使 map 为 nil，若调用方仅检查 error，
// 就会把静默的引擎响应误读为一个空字段对象。
func decodeObject(op string, raw []byte) (map[string]json.RawMessage, *Error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))

	var answer map[string]json.RawMessage
	if err := decoder.Decode(&answer); err != nil {
		return nil, &Error{
			Kind: KindMalformed, Op: op, Detail: "the answer is not a JSON object", Err: err,
		}
	}
	if answer == nil {
		return nil, &Error{
			Kind: KindMalformed, Op: op, Detail: "the answer is a JSON null rather than an object",
		}
	}

	// 拒绝响应体末尾附带的第二个 JSON 值。解码器读取第一个值后，若不额外检查就会忽略后续内容。
	if err := decoder.Decode(new(json.RawMessage)); !errors.Is(err, io.EOF) {
		return nil, &Error{
			Kind: KindMalformed, Op: op, Detail: "the answer carries a trailing JSON value",
		}
	}

	return answer, nil
}
