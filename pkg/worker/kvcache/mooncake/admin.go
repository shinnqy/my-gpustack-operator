package mooncake

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"

	"gpustack.ai/gpustack/pkg/utils/json"
	"gpustack.ai/gpustack/pkg/utils/stringx"
)

// 管理面板的路径。每一个路径都位于主节点的指标端口上：那个端口同时服务于 Prometheus 界面和 HTTP 管理 API，
// 因此状态报告将其作为独立的命名端点进行展示，而非让读者去猜测。
const (
	adminPathHealth   = "/health"
	adminPathMetrics  = "/metrics"
	adminPathSegments = "/get_segments_detail"
)

// adminReadLimit 限制管理员响应内容。
//
// 这里列出的三条路径用于存储一个小型健康文档、几十个家庭的说明文件，以及每个挂载内存段对应的一个条目。其中
// 最大的文件大小会随集群规模增长，因此限制设置远高于当前文件大小，而不是紧贴于当前文件大小：每内存段大约
// 250 字节，这意味着有数万个内存段，同时仍保持有效边界。
const adminReadLimit = 8 << 20

// 可识别的故障。它们充当哨兵，因为调用方会在这些故障上执行分支操作：一个对应一个阶段，一个对应一个状态消息，
// 一个对应无法观测到的状态，其余的则对应重试操作——而一个只能看到“错误”的调用方，会将一个初始主节点报告为故障状态。
var (
	// ErrServicePlaneInactive 表示主节点声明自己尚未开始提供服务。
	//
	// 它仅适用于门限路由。/health 和/metrics 在每种状态下均返回 200，因此询问这些的调用者不会看到此结果
	// ——这正是重点：该事实在那里以 service_ready 的形式呈现，两者通过设计是一致的，因为门限测试的就是
	// `/health`所发布的字段。
	ErrServicePlaneInactive = errors.New("kvcache: leader's service plane is not active")

	// ErrMalformedBody 是一种未能解析的回复。特意将其与传输失败区分开：前者表明该主节点给出了意想不到的
	// 响应，后者则表明没有响应，只有前者值得让操作员直接查看。
	ErrMalformedBody = errors.New("kvcache: leader returned a body that does not parse")

	// ErrRouteNotServed 是响应 404 错误的主节点：其运行的 Master 版本没有该路由。这是该版本的一个事实，
	// 并非读取失败，其实际能到达的路由是内存段列表，而 Mooncake 从 0.3.12 提供该服务：在 0.3.10 和
	// 0.3.11 行中会出现 404 错误，而 /health 和 /metrics 则能正常运行。
	ErrRouteNotServed = errors.New("kvcache: leader does not serve this route")
)

// LeaderHealth 是主节点的/health 文档。
type LeaderHealth struct {
	// 状态是恒定的。主节点会无条件分配 "ok" 值，不会填充其他内容，因此即使在不提供推理服务的主节点上，
	// 也显示为“ok”，没有主节点视图，也不包含任何片段。此处保留此状态仅为了让记录文档的调用者知道已接收
	// 的内容；无法基于此状态进行任何分支操作。
	Status string

	// 角色和 HAState 描述了相关情况。ServiceReady 是文档中唯一的字段，也是此处所有就绪状态决策所依赖的内容。
	Role         string
	HAState      string
	ServiceReady bool

	// 在单主节点后端中，LeaderAddress 和 ViewVersion 不存在，因为没有需要报告的主节点视图。它们的缺失是
	// 正常的，并非错误。
	LeaderAddress string
	ViewVersion   *uint64
}

// leaderHealthBody 与接口文档一致。它与 LeaderHealth 是分开的，因此区分 "absent" 与 "empty" 的指针会保留在
// 边界处，而不会向内部扩展。
type leaderHealthBody struct {
	Status  string `json:"status"`
	Role    string `json:"role"`
	HAState string `json:"ha_state"`
	// 一个指针，与该包解码的其他布尔值不同，因为这一字段代表了整个就绪状态。作为一个普通的布尔值，不存
	// 在或空的 service_ready 与明确的 false 状态无法区分，因此如果内容为有效的 JSON 但完全不是本文档——
	// 即 `{}`，或者某个其他服务的健康端点在错误地址下响应——则会被识别为“已上线但未提供服务”，并会永久处
	// 于配置状态，等待一个不会发送任何数据的字段。
	ServiceReady  *bool   `json:"service_ready"`
	LeaderAddress *string `json:"leader_address"`
	ViewVersion   *uint64 `json:"view_version"`
}

// DecodeLeaderHealth 解码 /health 返回的健康文档。
func DecodeLeaderHealth(body []byte) (LeaderHealth, error) {
	var wire leaderHealthBody
	if err := json.Unmarshal(body, &wire); err != nil {
		return LeaderHealth{}, fmt.Errorf("%w: %s: %w", ErrMalformedBody, adminPathHealth, err)
	}

	// 没有该字段的文档并非真正的文档。此处的其他内容均为描述性信息，使用空字符串也是可以接受的；
	// service_ready 是该阶段启动后的状态判断，因此将其缺失视为错误会导致将无关 JSON 内容误判为处于运行
	// 状态且未提供推理服务的主节点——而实际上这只是一个阶段状态，因此根本不会将其视为错误。
	if wire.ServiceReady == nil {
		return LeaderHealth{}, fmt.Errorf("%w: %s: no service_ready", ErrMalformedBody,
			adminPathHealth)
	}

	health := LeaderHealth{
		Status:       wire.Status,
		Role:         wire.Role,
		HAState:      wire.HAState,
		ServiceReady: *wire.ServiceReady,
		ViewVersion:  wire.ViewVersion,
	}
	if wire.LeaderAddress != nil {
		health.LeaderAddress = *wire.LeaderAddress
	}
	return health, nil
}

// 该运算符的说明家族。计数器仅用于 PoolWrites 条件；它们的累积值不会复制到状态中。
const (
	metricTotalCapacityBytes     = "master_total_capacity_bytes"
	metricAllocatedBytes         = "master_allocated_bytes"
	metricTotalFileCapacityBytes = "master_total_file_capacity_bytes"
	metricAllocatedFileSizeBytes = "master_allocated_file_size_bytes"
	metricPutStartRequests       = "master_put_start_requests_total"
	metricPutEndRequests         = "master_put_end_requests_total"
	metricPutRevokeRequests      = "master_put_revoke_requests_total"
)

// LeaderCapacity 是报告中的数值。
//
// 每个数值都是一个指针，其区别正是这种类型存在的原因：一个未被说明的家族为“空”，而“空”并非零。零会被理解为
// 空缓存——这是一种真实且不同的状态——因此未被报告的家族无法扮演这一角色。
type LeaderCapacity struct {
	TotalBytes         *int64
	AllocatedBytes     *int64
	TotalFileBytes     *int64
	AllocatedFileBytes *int64
	PutStartRequests   *int64
	PutEndRequests     *int64
	PutRevokeRequests  *int64
}

// DecodeLeaderCapacity 从 Prometheus 指标文本中读取容量和 PUT 活动。
//
// 它直接解析文本，而非通过上游解析器进行，这是有意为之的权衡：上游解析器在首次使用前需要设置全局的度量名称
// 验证模式，而此过程已包含操作者的自身度量注册表。获取这些数值并不值得为了一个全局开关而让同一二进制文件中
// 的其他注册表继承它。
//
// 所选家族未标注标签。按片段划分的数据中，主节点同时包含各种标签和不同的名称，因此下面的名称查找过程将其排
// 除，没有特定的规则：已标注的样本的名称包含其标签集，而此处没有与之匹配的内容。
func DecodeLeaderCapacity(body []byte) (LeaderCapacity, error) {
	wanted := map[string]**int64{}
	capacity := LeaderCapacity{}
	wanted[metricTotalCapacityBytes] = &capacity.TotalBytes
	wanted[metricAllocatedBytes] = &capacity.AllocatedBytes
	wanted[metricTotalFileCapacityBytes] = &capacity.TotalFileBytes
	wanted[metricAllocatedFileSizeBytes] = &capacity.AllocatedFileBytes
	wanted[metricPutStartRequests] = &capacity.PutStartRequests
	wanted[metricPutEndRequests] = &capacity.PutEndRequests
	wanted[metricPutRevokeRequests] = &capacity.PutRevokeRequests

	var sawAnySample bool
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		name, value, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		sawAnySample = true

		target, ok := wanted[name]
		if !ok {
			continue
		}

		// 一个样本为 `name value [timestamp]`，而时间戳是可选的——因此 Cut 留在`value`中的内容可能包
		// 含两个数字。选择第一个字段而非整个剩余内容，这就是读取容量与拒绝完全有效的描述之间的区别。
		value = strings.TrimSpace(value)
		if i := strings.IndexAny(value, " \t"); i >= 0 {
			value = value[:i]
		}

		parsed, err := strconv.ParseFloat(value, 64)
		if err != nil {
			// 只有 `strconv` 的错误原因需要关注，因为其自身的 Error() 会把整个输入内容原封不动
			// 地返回——因此将其整体包裹起来，就能恢复上面那个有边界限制的字符串。对 *
			// strconv.NumError* 使用 errors.Unwrap，会返回 ErrSyntax 或 ErrRange。
			return LeaderCapacity{}, fmt.Errorf("%w: %s: %s carries %q: %w",
				ErrMalformedBody, adminPathMetrics, name, quoteExternal(value), errors.Unwrap(err))
		}
		// 在转换之前会进行范围检查，而非之后。ParseFloat 可以接受 NaN 和无穷大值，而将不适合存储在
		// int64 中的 float64 转换为数值在 Go 语言中则是未定义的——结果取决于架构，且本项目构建的两个
		// 版本存在差异：根据测量数据，相同的样本在 arm64 架构下给出最大 int64 值，在 amd64 架构下则
		// 给出最小值。因此，格式错误的数据会导致一个 worker 输出 8 艾字节的数据，而另一个则输出负容
		// 量。数据通过 HTTP 从外部后端选择的地址传输过来，因此不可依赖此操作方提供的数据。
		//
		// 上限为 2^63，并非 math.MaxInt64。这种比较的陷阱在于：MaxInt64 为 2^63-1，这是 float64 无
		// 法容纳的值，因此在此比较中将其转换后向上取整至 2^63，从而“大于”条件永远不会触发。与 2^63
		// 本身进行比较则准确无误。这会消耗约 1024 个 int64 值，所有这些值都超过 8 艾字节。拒绝处理
		// 小数样本，而非将其截断：int64(1.9) 会显示 1 作为数值，而主节点从未报告过该数值，仅在确认
		// 了容量的情况下才会显示。
		if math.IsNaN(parsed) || math.IsInf(parsed, 0) ||
			parsed < 0 || parsed >= float64(uint64(1)<<63) ||
			parsed != math.Trunc(parsed) {
			return LeaderCapacity{}, fmt.Errorf("%w: %s: %s carries %q, which is not a byte count",
				ErrMalformedBody, adminPathMetrics, name, quoteExternal(value))
		}
		asInt := int64(parsed)
		*target = &asInt
	}

	// 一个没有任何样本的实体并不属于一种表现。一个含有样本但不符合我们定义的家族的实体则是一个合理的结
	// 果——每个指标都缺失，调用者报告说他什么也没观察到，而不是观察到零。
	if !sawAnySample && len(strings.TrimSpace(string(body))) > 0 {
		return LeaderCapacity{}, fmt.Errorf("%w: %s: no sample line found",
			ErrMalformedBody, adminPathMetrics)
	}

	return capacity, nil
}

// SegmentDetail 是主节点内存段列表中的一项条目。
//
// 该列表包含十二个字段中的六个。分配器字节计数被有意保留下来：在这个范围内没有任何内容会显示每个成员的容量，
// 而一个没有读取器的字段会变得过时，而无人察觉。
type SegmentDetail struct {
	// ID 是该内存段的 UUID，在列表中是唯一的。
	ID string
	// ClientID 由成员进程生成一次，因此即使成员使用相同的地址进行宣传，它也能区分它们。
	ClientID string
	// 名称是主节点所认知的内存段，格式为“<成员地址>:<端口>”，而非成员配置时的简单地址。与 mooncake
	// 0.3.13 版本相比：端口在每次启动时都会重新绑定，并非 TEEndpoint 中的那个端口，后者的状态会在同一次
	// 重启中独立变化。因此，调用者选择成员的内存段时，仅会比较一半的 HOST 值；将整个值与 Pod IP 进行比
	// 较则无对应项，而“无匹配”则表现为静默状态。
	//
	// 这里没有节点参与——这是 TEEndpoint 的职责，见下文——此字段会作为主节点的字符串被发布到状态中。
	Name string
	// 状态是主节点的自身内存段状态，保持不变地传递。它在此处未被映射为封闭集合：此 API 发布的是存储系统
	// 报告的内容，因此后续版本中的状态会传递过去，而不是被压缩成适合的值。
	State string
	// 协议是注册成员所使用的传输方式，而非其启动时的传输方式。主节点会从挂载请求中复制出该协议，而
	// Decode 实例则将其传递过去，因此即使某个成员回退到 TCP，它仍然会列出所请求的互联网络。双方都没有承
	// 载实际安装的传输方式的界面。
	Protocol string
	// TEEndpoint 是成员的传输引擎地址，它决定了列表项如何与所属的 Pod 重新连接。
	TEEndpoint string
	// AllocatorUsedBytes 是该成员的当前分配量。如果为空，则表示主节点未报告该值。
	AllocatorUsedBytes *uint64
}

type segmentListingBody struct {
	TotalSegments uint64 `json:"total_segments"`
	// 用指针区分 segments 字段缺失和空列表。错误的外部端点也可能返回合法的空 JSON 对象。若将它当作空列
	// 表，就会清空成员并误报 NoSegments。只有字段确实存在时才能发布其结果。
	Segments *[]struct {
		SegmentID          string  `json:"segment_id"`
		ClientID           string  `json:"client_id"`
		SegmentName        string  `json:"segment_name"`
		Status             string  `json:"status"`
		Protocol           string  `json:"protocol"`
		TEEndpoint         string  `json:"te_endpoint"`
		AllocatorUsedBytes *uint64 `json:"allocator_used_bytes"`
	} `json:"segments"`
}

// DecodeSegmentListing 调用 /get_segments_detail。
//
// 空列表是一个合法的值，表示“主节点不知道有任何内存段”，它会被返回为一个空切片，而不是错误：一个没有挂载任
// 何内容的推理服务主节点是一种真实状态，调用者通过错误值为 null 来将其与失败读取情况区分开来。
//
// 缺少 segments 字段的响应不能用于更新成员。返回 {} 的外部端点可能指向了其他服务。真正的空列表写作
// `"segments": []`。
func DecodeSegmentListing(body []byte) ([]SegmentDetail, error) {
	var wire segmentListingBody
	if err := json.Unmarshal(body, &wire); err != nil {
		return nil, fmt.Errorf("%w: %s: %w", ErrMalformedBody, adminPathSegments, err)
	}

	// 缺失，因此这根本不是内存段列表。
	if wire.Segments == nil {
		return nil, fmt.Errorf("%w: %s: no segments field", ErrMalformedBody, adminPathSegments)
	}

	segments := make([]SegmentDetail, 0, len(*wire.Segments))
	seen := make(map[string]struct{}, len(*wire.Segments))
	for _, s := range *wire.Segments {
		if s.SegmentID == "" {
			return nil, fmt.Errorf("%w: %s: a segment carries no id",
				ErrMalformedBody, adminPathSegments)
		}
		if _, dup := seen[s.SegmentID]; dup {
			return nil, fmt.Errorf("%w: %s: two segments carry id %q",
				ErrMalformedBody, adminPathSegments, quoteExternal(s.SegmentID))
		}
		seen[s.SegmentID] = struct{}{}
		if s.ClientID == "" {
			return nil, fmt.Errorf("%w: %s: a segment carries no client id",
				ErrMalformedBody, adminPathSegments)
		}
		if s.SegmentName == "" {
			return nil, fmt.Errorf("%w: %s: a segment carries no name",
				ErrMalformedBody, adminPathSegments)
		}
		segments = append(segments, SegmentDetail{
			ID:                 s.SegmentID,
			ClientID:           s.ClientID,
			Name:               s.SegmentName,
			State:              s.Status,
			Protocol:           s.Protocol,
			TEEndpoint:         s.TEEndpoint,
			AllocatorUsedBytes: s.AllocatorUsedBytes,
		})
	}
	return segments, nil
}

// AdminClient 访问单个 Mooncake leader 的管理接口。
//
// 这是覆盖在解码器上的薄壳：解码器是意义的所在，仅通过已记录的实体即可对其进行测试，而这一做法则使传输结果
// 变成了调用者分支时的失败之一。
type AdminClient struct {
	// 地址是主节点的管理端点，状态发布时会显示，格式为主机：端口。
	Address string
	// HTTP 由调用方注入，并设置适当的超时，避免单次指标读取长期阻塞本轮调谐。
	HTTP *http.Client
}

// Health 请求 /health。无论 leader 是否正在服务，该端点都可返回当前状态，因此可作为就绪判断的依据。
func (c *AdminClient) Health(ctx context.Context) (LeaderHealth, error) {
	body, err := c.get(ctx, adminPathHealth)
	if err != nil {
		return LeaderHealth{}, err
	}
	return DecodeLeaderHealth(body)
}

// Capacity 请求 /metrics。
//
// leader 的服务面尚未就绪时，/metrics 也可能以 200 返回全零容量。调用方只有在 service_ready 成立后才发布这
// 些指标，以免把未挂载时的零误报为真实容量。
func (c *AdminClient) Capacity(ctx context.Context) (LeaderCapacity, error) {
	body, err := c.get(ctx, adminPathMetrics)
	if err != nil {
		return LeaderCapacity{}, err
	}
	return DecodeLeaderCapacity(body)
}

// Segments 请求 /get_segments_detail。服务面未就绪时返回 ErrServicePlaneInactive，早于 0.3.12 的 master 没
// 有该路由，返回 ErrRouteNotServed。
func (c *AdminClient) Segments(ctx context.Context) ([]SegmentDetail, error) {
	body, err := c.get(ctx, adminPathSegments)
	if err != nil {
		return nil, err
	}
	return DecodeSegmentListing(body)
}

// get 执行一次管理请求，并保留不同失败原因供调用方判断。
func (c *AdminClient) get(ctx context.Context, path string) ([]byte, error) {
	url := fmt.Sprintf("http://%s%s", c.Address, path)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request for %s: %w", path, err)
	}

	resp, err := c.HTTP.Do(req)
	if err != nil {
		// 底层传输错误原样返回，以便保留失败地址和系统调用信息。没有收到响应与收到无效内容、服务面
		// 尚未就绪，应分别处理。
		return nil, fmt.Errorf("request %s: %w", path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	// 有边界限制，因为远端不一定属于我们。外部后端地址标识了管理员所指定的进程，而错误的地址则可以指向
	// 任何东西——包括无限流化的内容。
	//
	// 读取的字节数超过上限，因此达到上限的情况与内容在此时结束的情况可以区分开来。不能将截断问题留给解
	// 码器：描述解析器是按行处理的，因此内容在中间被截断后，每个家族的内容仍然会被保留，并将部分内容作
	// 为观测到的容量进行发布。
	body, err := io.ReadAll(io.LimitReader(resp.Body, adminReadLimit+1))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	if len(body) > adminReadLimit {
		return nil, fmt.Errorf("%w: %s: the response is larger than %d bytes",
			ErrMalformedBody, path, adminReadLimit)
	}

	if resp.StatusCode == http.StatusServiceUnavailable {
		// 这些门限路由会在主节点服务平面启动前继续工作。这是一个阶段问题，而非故障，绝不应被折叠为
		// 404 错误——后者意味着主节点的版本根本不处理该路径，而等待行为也不会改变这一情况。
		return nil, fmt.Errorf("%w: %s: %s", ErrServicePlaneInactive, path, excerpt(body))
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("%w: %s: status 404: %s", ErrRouteNotServed, path, excerpt(body))
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("request %s: unexpected status %d: %s",
			path, resp.StatusCode, excerpt(body))
	}

	return body, nil
}

// adminErrorExcerpt 规定了失败的响应内容会被引用多少。
//
// 这些错误会转化为一个状态消息，而根据架构规范，该状态消息的大小上限为 32 KiB。超过此限制的内容会导致所有状
// 态写入操作无法通过验证——因此，只要对方继续以那种方式回应，控制器就无法报告它试图报告的错误。对方不一定是
// 我们的用户，因此它所包含内容的大小不由本操作方决定：该内容片段足够短，可以留出其他消息空间。
const adminErrorExcerpt = 512

// 该摘录会生成一个错误信息的响应体，具有边界限制且为单行格式。
func excerpt(body []byte) string {
	return quoteExternal(strings.TrimSpace(string(body)))
}

// quoteExternal 限定了一个来自管理员响应的字符串，该字符串最终会转化为一个错误，进而成为一条条件消息。
//
// 整体响应受到 adminReadLimit 的限制，其值为 8 MiB，且未说明这一限制是如何分配到各个字段中的：一个度量值样
// 本、一个内存段名称或一段内容可以容纳全部内容。使用的是“Unicode 码点”而非“字节”，因为“Unicode 码点”是该模
// 式限制所依据的计量单位，而且正是通过分隔 Unicode 码点来保持多字节字符的完整性。
func quoteExternal(text string) string {
	return stringx.TruncateRunes(text, adminErrorExcerpt, "… (truncated)")
}

// segmentStates 将商店中定义的内存段状态映射至此 API 的对应状态。
//
// 这是两个词汇表相遇的第三个位置，位于分配策略和传输之后，也是唯一一个将 INWARD 翻译为“生成”的——另外两个则
// 处理请求，这个则发布报告。
//
// 该存储器的六个状态：UNDEFINED、OK、DRAINING、DRAINED、GRACEFULLY_UNMOUNTING、UNMOUNTING。OK 保持其拼写不
// 变，因为它是一个首字母缩写，这正是该枚举组对这类名称的处理方式。
var segmentStates = map[string]string{
	"UNDEFINED":             "Undefined",
	"OK":                    "OK",
	"DRAINING":              "Draining",
	"DRAINED":               "Drained",
	"GRACEFULLY_UNMOUNTING": "GracefullyUnmounting",
	"UNMOUNTING":            "Unmounting",
}

// SegmentState 将报告状态转换为该 API 的命名格式。
//
// 该版本无法识别的状态将保持原样，不会被替换为备用值。存储库拥有此词汇表：添加状态的版本会报告真实信息，将
// 其 flatten 为 "Unknown" 则会导致新信息变成无信息。该字段没有使用枚举类型，因此能够正常通过。
func SegmentState(reported string) string {
	if mapped, ok := segmentStates[reported]; ok {
		return mapped
	}
	return reported
}
