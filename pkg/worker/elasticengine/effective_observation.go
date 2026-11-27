package elasticengine

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"gpustack.ai/gpustack/pkg/utils/json"
	"gpustack.ai/gpustack/pkg/worker/elasticprofile"
)

const (
	// completionPath 是兼容 OpenAI 的原生补全路由。固定版本的引擎前端会在此检查数据并行 rank 请求头，
	// 因此可以用它观测实际并行宽度。
	completionPath = "/v1/completions"
	// rankHeader 指定引擎前端读取数据并行 rank 所用的请求头。
	rankHeader = "X-data-parallel-rank"
	// sentinelRank 的值为 -1，在任何有效宽度下都越界。用它发起请求，
	// 可以让前端在错误响应中给出当前执行的宽度边界。
	sentinelRank = -1
	// minObservedWorldWidth 是探测所支持的最小宽度。即使准入配置要求更宽的部署，
	// 单 rank 的实际并行组仍值得测量，因此这里从 1 开始。探测范围须覆盖配置允许的全部宽度。
	minObservedWorldWidth = 1
	// maxObservedWorldWidth 取受管配置允许的最大宽度，限制一次观测可以发出的逐 rank 请求数，
	// 防止引擎返回的意外数值放大探测流量。
	maxObservedWorldWidth = elasticprofile.WidthMax
	// forwardTokens 将每个 rank 的真实推理请求限制为生成一个 token。
	forwardTokens = 1
)

// boundaryPrefix 和 boundarySuffix 标识固定版本前端的越界错误文本，
// 宽度 N 位于 “data_parallel_rank -1 is out of range [0, N).” 的指定位置。
const (
	boundaryPrefix = "data_parallel_rank -1 is out of range [0, "
	boundarySuffix = ")."
)

// RankForward 保存定向到单个 rank 的推理结果，记录它是否提供了服务。
// 该 rank 具体属于哪个 Pod 仍由身份观测核实。
type RankForward struct {
	Rank   int
	Served bool
}

// NativeWorldObservation 保存原生观测的结果，包括引擎前端实际执行的宽度，
// 以及哪些 rank 成功处理了一个 token 的补全请求。
//
// 前后边界相同也无法排除宽度曾在中途变化后恢复。成员的 Pod 和 actor 身份须另外核对，
// 这份读数本身不足以授权删除。
type NativeWorldObservation struct {
	// ObservedWidth 是前后两次查询中前端实际执行的宽度。
	ObservedWidth int
	// Boundaries 按先后顺序保留两次边界读数。成功返回时二者相等。
	Boundaries [2]int
	// Forwards 按 rank 保存结果，[0, ObservedWidth) 中的每个 rank 对应一项。
	Forwards []RankForward
}

// observeRequest 定义观测器发送给补全端点的请求体，用固定的生成长度限制探测开销。
type observeRequest struct {
	Model     string `json:"model"`
	Prompt    string `json:"prompt"`
	MaxTokens int    `json:"max_tokens"`
	Stream    bool   `json:"stream"`
}

// [IMPORTANT: elastic EP] 证明引擎“真的”在宽度 W 上服务
//   - 第二步，调 vLLM 原生探测     ──▶ 发 rank=-1 查边界 == W
//
// ObserveNativeWorld 先用 rank -1 读取前端接受的范围，再让每个 rank 实际生成一个 token，最后重新读取边界。
//
// 调用方给出要验证的宽度。首次边界一致才开始逐 rank 探测，末次边界也相同才接受结果，
// 避免按未经验证的数值扩大流量。整个过程共用一个超时预算，服从调用方更早的截止时间，只发送只读探测请求。
//
// 返回的是引擎侧的服务证据。控制器还须核对完整的 Ray rank 到成员映射，并经由无缓存读取确认对象身份。
func (c *Client) ObserveNativeWorld(
	ctx context.Context, model, prompt string, expectedWidth int,
) (*NativeWorldObservation, *Error) {
	const op = "observe native world"

	if expectedWidth < minObservedWorldWidth || expectedWidth > maxObservedWorldWidth {
		return nil, invalid(op,
			"the expected width %d is outside the bounded range [%d, %d]",
			expectedWidth, minObservedWorldWidth, maxObservedWorldWidth)
	}
	if model == "" {
		return nil, invalid(op, "the model name is required")
	}

	// 每次边界查询和前向推理都共用同一个截止时间，取客户端超时与调用方截止时间中较早者。
	// 增加 rank 数或遇到慢响应都不会延长整轮预算。
	//
	// 一次观测会发送 expectedWidth + 2 个请求，调用方要按整轮请求数安排时间。
	// Scale 和 IsScaling 则各自独享一次调用预算。途中超时返回 KindAmbiguous，
	// 供调用方结合所请求的宽度和时间预算判断原因。
	obsCtx, cancel := c.callContext(ctx)
	defer cancel()

	// 遇到重定向时直接保留该响应，避免把其他地址的处理结果当作当前引擎的证据。
	httpc := *c.http
	httpc.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	httpc.Timeout = 0 // 所有探测共用外层观测的截止时间。

	payload := observeRequest{
		Model:     model,
		Prompt:    prompt,
		MaxTokens: forwardTokens,
		Stream:    false,
	}

	bound := func() (int, *Error) {
		status, raw, err := c.observePost(obsCtx, &httpc, op, sentinelRank, payload)
		if err != nil {
			return 0, err
		}
		if status == http.StatusOK {
			return 0, &Error{
				Kind: KindMalformed, Op: op, StatusCode: status,
				Detail: "the engine answered the out-of-range probe with 200, so the rank header is not enforced; the bound cannot be observed",
			}
		}
		if status != http.StatusBadRequest {
			return 0, probeStatusFailure(op, status,
				"the engine did not answer the out-of-range probe with the native validation error")
		}
		width, perr := parseObservedBound(raw)
		if perr != nil {
			return 0, &Error{Kind: KindMalformed, Op: op, StatusCode: status, Detail: perr.Error()}
		}
		if width != expectedWidth {
			return 0, &Error{
				Kind: KindMalformed, Op: op, StatusCode: status,
				Detail: fmt.Sprintf("the observed native width %d does not match the expected width %d", width, expectedWidth),
			}
		}
		return width, nil
	}

	first, err := bound()
	if err != nil {
		return nil, err
	}

	forwards := make([]RankForward, 0, first)
	for rank := 0; rank < first; rank++ {
		if serr := c.observeForward(obsCtx, &httpc, op, rank, payload); serr != nil {
			return nil, serr
		}
		forwards = append(forwards, RankForward{Rank: rank, Served: true})
	}

	second, err := bound()
	if err != nil {
		return nil, err
	}

	return &NativeWorldObservation{
		ObservedWidth: first,
		Boundaries:    [2]int{first, second},
		Forwards:      forwards,
	}, nil
}

// observePost 发送带 rank 请求头的补全探测，返回状态码和大小受限的响应体。边界查询依靠引擎的越界错误，
// 因此非 2xx 响应也原样交给调用处分类。
func (c *Client) observePost(
	ctx context.Context, httpc *http.Client, op string, rank int, payload observeRequest,
) (int, []byte, *Error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return 0, nil, &Error{
			Kind: KindInvalidInput, Op: op,
			Detail: "the probe body could not be encoded", Err: err,
		}
	}

	request, err := http.NewRequestWithContext(
		ctx, http.MethodPost, c.baseURL+completionPath, strings.NewReader(string(encoded)),
	)
	if err != nil {
		return 0, nil, &Error{
			Kind: KindInvalidInput, Op: op,
			Detail: "the probe request could not be built", Err: err,
		}
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set(rankHeader, strconv.Itoa(rank))

	response, err := httpc.Do(request)
	if err != nil {
		kind := KindAmbiguous
		detail := "the probe could not be completed"
		if ctxErr := ctx.Err(); ctxErr != nil {
			detail = "the observer's bound was reached"
		}
		return 0, nil, &Error{Kind: kind, Op: op, Detail: detail, Err: err}
	}
	defer response.Body.Close()

	raw, rerr := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if rerr != nil {
		return 0, nil, &Error{
			Kind: KindAmbiguous, Op: op,
			Detail: "the probe answer could not be read to its end", Err: rerr,
		}
	}
	if len(raw) > maxResponseBytes {
		return 0, nil, &Error{
			Kind: KindMalformed, Op: op, StatusCode: response.StatusCode,
			Detail: fmt.Sprintf("the probe answer exceeded the %d byte bound", maxResponseBytes),
		}
	}
	return response.StatusCode, raw, nil
}

// probeStatusFailure 沿用原生客户端的错误分类。503 表示正在重新配置，408 和其他 5xx 表示引擎处理失败，
// 其余无法识别的响应按协议错误处理。调用方提供具体说明。
func probeStatusFailure(op string, status int, detail string) *Error {
	switch {
	case status == http.StatusServiceUnavailable:
		return &Error{Kind: KindReconfiguring, Op: op, StatusCode: status, Detail: detail}
	case status == http.StatusRequestTimeout || status >= http.StatusInternalServerError:
		return &Error{Kind: KindNativeFailure, Op: op, StatusCode: status, Detail: detail}
	default:
		return &Error{Kind: KindMalformed, Op: op, StatusCode: status, Detail: detail}
	}
}

// parseObservedBound 按固定版本前端的越界错误格式提取宽度。错误对象必须恰好含有约定的四个字段，
// param 必须存在且为 null，宽度必须是规范的十进制正整数。对外只返回固定错误说明，避免泄露原始响应内容。
func parseObservedBound(raw []byte) (int, error) {
	var envelope struct {
		Error json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil || envelope.Error == nil {
		return 0, fmt.Errorf("the probe answer is not the native error object")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(envelope.Error, &fields); err != nil {
		return 0, fmt.Errorf("the probe answer is not the native error object")
	}
	if len(fields) != 4 {
		return 0, fmt.Errorf("the probe answer is not the native bad-request shape")
	}
	var code int
	var messageType, message string
	for name, dst := range map[string]any{"code": &code, "type": &messageType, "message": &message} {
		field, ok := fields[name]
		if !ok || json.Unmarshal(field, dst) != nil {
			return 0, fmt.Errorf("the probe answer is not the native bad-request shape")
		}
	}
	paramRaw, hasParam := fields["param"]
	if !hasParam || string(paramRaw) != "null" {
		return 0, fmt.Errorf("the probe answer is not the native bad-request shape")
	}
	if code != http.StatusBadRequest || messageType != "BadRequestError" {
		return 0, fmt.Errorf("the probe answer is not the native bad-request shape")
	}
	if !strings.HasPrefix(message, boundaryPrefix) || !strings.HasSuffix(message, boundarySuffix) {
		return 0, fmt.Errorf("the probe answer does not carry the native out-of-range message")
	}
	widthText := strings.TrimSuffix(strings.TrimPrefix(message, boundaryPrefix), boundarySuffix)
	if !canonicalWidth.MatchString(widthText) {
		return 0, fmt.Errorf("the probe answer's bound is not a canonical positive width")
	}
	width, err := strconv.Atoi(widthText)
	if err != nil || width < 1 {
		return 0, fmt.Errorf("the probe answer's bound is not a positive width")
	}
	return width, nil
}

// canonicalWidth 只接受与前端用 %d 输出的十进制正整数相同的形式。
var canonicalWidth = regexp.MustCompile(`^[1-9][0-9]*$`)

// observeForward 用指定 rank 的请求头发出真实的一 token 补全请求，验证该 rank 可提供服务。
func (c *Client) observeForward(
	ctx context.Context, httpc *http.Client, op string, rank int, payload observeRequest,
) *Error {
	status, raw, err := c.observePost(ctx, httpc, op, rank, payload)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return probeStatusFailure(op, status,
			fmt.Sprintf("rank %d did not serve the forward", rank))
	}
	var body struct {
		Object  string            `json:"object"`
		Choices []json.RawMessage `json:"choices"`
		Error   *json.RawMessage  `json:"error"`
		Usage   struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
			TotalTokens      int `json:"total_tokens"`
		} `json:"usage"`
	}
	if jsonErr := json.Unmarshal(raw, &body); jsonErr != nil {
		return &Error{
			Kind: KindMalformed, Op: op, StatusCode: status,
			Detail: fmt.Sprintf("rank %d's answer is not a completion object", rank),
		}
	}
	// 核验固定版本引擎的非流式响应。必须是补全对象，
	// choices 非空且第一项含字符串 text（允许空串或空白），无非空的 error 对象，
	// 且 usage 记录生成了一个 token。
	notCompletion := fmt.Sprintf("rank %d's answer is not the native completion shape", rank)
	if body.Error != nil || body.Object != "text_completion" || len(body.Choices) == 0 {
		return &Error{Kind: KindMalformed, Op: op, StatusCode: status, Detail: notCompletion}
	}
	var choice struct {
		Text *string `json:"text"`
	}
	if json.Unmarshal(body.Choices[0], &choice) != nil || choice.Text == nil {
		return &Error{Kind: KindMalformed, Op: op, StatusCode: status, Detail: notCompletion}
	}
	if body.Usage.CompletionTokens != forwardTokens {
		return &Error{Kind: KindMalformed, Op: op, StatusCode: status, Detail: notCompletion}
	}
	return nil
}
