package mooncake

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"gpustack.ai/gpustack/pkg/utils/json"
)

// adminPathTenantQuotas 是租户账本的单一路径。所有操作都通过该路径进行，采用不同的方法：列出和查询使用 GET
// 方法，创建或更新使用 PUT 方法，删除则使用 DELETE 方法。
//
// 该接口与 /health、/metrics 和 /get_segments_detail 使用相同的端口，因此这些方法会依赖于客户端已经读取的内
// 容，而不是它们自己的内容：一个连接对应一套规则——一个主体限制、一个摘录、一个重定向策略——而所拨打的地址则
// 来自用户提交的 CR。
const adminPathTenantQuotas = "/api/v1/tenant_quotas"

// 账本中那些可辨识的拒绝记录。它们作为守护者存在，原因与读取界面相同：调用方在它们上设置分支——一个用于指示
// 条件，一个用于指示操作对象，其余的则被重新尝试或作为错误报告。
//
// Master 会响应一个数字代码以及一个 HTTP 状态码。该状态码是下面映射所依赖的关键项，因为它是 HTTP 本身所保证
// 存在的部分——除了 409 这一例外情况外，它包含三个不相关的代码，因此还会根据消息体中的名称进行进一步拆分。
var (
	// ErrInvalidQuota 表示 Master 在 PUT 请求体中拒绝配额数值。根据检测结果，对于零值，返回 -600 / 400，
	// 意为“租户配额必须为正数”；对于负值，则返回“无效的 JSON 体：解析数字失败”。
	//
	// 达到该状态意味着存在错误或竞争情况，而非用户输入：每个拒绝路径在准入阶段就已经过预验证，因此
	// Master 拒绝的配额是此操作员绝不应渲染的内容。Master 自己的话会在消息中体现出来，因此可以诊断出问题所在。
	ErrInvalidQuota = errors.New("kvcache: the master refused the requested quota")

	// ErrInvalidTenantID 表示 Master 在查询中拒绝了 tenant_id——其错误等级为 -600 / 400，对于空值以及以
	// 下划线开头的 tenant_id，均会出现“缺失或无效 tenant_id”的错误。
	//
	// 它与 ErrInvalidQuota 不同，尽管 Master 用同一个代码回应这两种情况，因为这两种情况对于错误发生的位
	// 置描述不同：一种是指已准入的 Webhook 域名，另一种是指验证过的上限。
	ErrInvalidTenantID = errors.New("kvcache: the master refused the tenant id")

	// ErrMultiTenancyDisabled 为 -1011 / 409 UNAVAILABLE_IN_CURRENT_MODE：账本不存在，因为 Master 在启
	// 动时并未开启多租户功能。
	//
	// 这是整个配额功能的基础前提，因此调用者会取消该条件，而不会重试：除非更改后端，该运算符所写的任何
	// 内容都无法收敛。
	ErrMultiTenancyDisabled = errors.New("kvcache: the master is not running with multi-tenancy")

	// ErrTenantNotEmpty 对应 HTTP 409 和 master 的 -1702（TENANT_NOT_EMPTY），表示请求删除的租户仍持有对象。
	//
	// 它有自己的哨兵机制，因为它会落在那个可以释放最终化器的路径上。如果以其他方式理解，即一个仍保留缓
	// 存域的绑定对象，会被报告为处于多租户关闭状态的 Master——这会让操作员关注后端规范，但实际上，该域必
	// 须先进行排空操作。
	ErrTenantNotEmpty = errors.New("kvcache: the tenant still owns objects")

	// ErrUnavailableInCurrentStatus 为-1010 / 409 UNAVAILABLE_IN_CURRENT_STATUS：主节点因当前状态而拒绝
	// ——处于待机状态，或正处于主节点过渡阶段。
	//
	// 与其他两个 409s 不同，它是临时的，这正是它无法被合并进任何一组的原因：调用者会重新尝试使用这个，
	// 并为其他项设置条件。
	ErrUnavailableInCurrentStatus = errors.New("kvcache: the master cannot serve this in its current status")

	// ErrQuotaPolicyNotWritable 为 -1503 / 500 PERSISTENT_FAIL：Master 无法将更改写入其策略源，因此也无法应用该更改。
	//
	// 没有任何项被接受。两种管理员-API 写入路径在应用之前都会保留在连接器中，因此被拒绝的写入操作会让账
	// 本保持原样——以真实的 Master 系统为基准进行测量，在后续的列表中，失败的 PUT 操作租户会消失。内存和
	// 磁盘数据一致，这一点值得提及，因为相反的结论表明配额是有效的，只是不可持续，这将促使操作员重新启
	// 动某些操作，而非修复挂载问题。
	//
	// 它有自己的“哨兵”问题，因为这是一个既非 Master 的错，也非请求导致的失败：连接器文件无法写入，因此
	// 第一次配额写入在文件系统层面就失败了，而当时没有人去处理这个问题。如果将其报告为普通的写入失败，
	// 就会被认为只是会逐渐收敛的配额问题，但实际上永远不会如此。
	ErrQuotaPolicyNotWritable = errors.New("kvcache: the master cannot persist to its quota policy source")
)

// 通过 Master 代码中写入的实体，这三个因 HTTP 409 状态而被拒绝的请求。
//
// 每次拒绝都会返回 `{"success": false, "error_code": …, "error_message": …}` 的结果，其中 CODE 指的是
// artifact 的公开 ErrorCode 枚举值；而消息则只是该枚举值的拼写形式，因为这些处理程序本身不会返回任何消息。
// 如果某个处理程序后来返回了消息，就会让消息匹配器无法判断出需要从 Master 中排空且账本已关闭的租户情况——而
// 它不会留下任何迹象。
//
// 从 v0.3.12.post1 的源文件读取——types.h 用于定义数值，master_admin_service.cpp 用于定义
// ErrorCodeToHttpStatus 函数，该函数将三者映射为同一个状态。
const (
	masterCodeUnavailableInCurrentStatus = -1010
	masterCodeUnavailableInCurrentMode   = -1011
	masterCodeTenantNotEmpty             = -1702

	// masterCodePersistentFail 是指 Master 在写入其刚刚接受的策略时失败。它仅对应 500 状态，因为
	// ErrorCodeToHttpStatus 没有针对此情况的分类，因此状态描述为“Master 崩溃了”，而主体则说明了具体原因
	// ——这就是从代码中读取该信息的原因。
	masterCodePersistentFail = -1503

	// masterCodeObjectNotFound 是账本路由上唯一的代码，表示“没有此类租户”。ErrorCodeToHttpStatus 将四种
	// 代码映射到 404 状态——JOB_NOT_FOUND、SEGMENT_NOT_FOUND、OBJECT_NOT_FOUND 和 TENANT_NOT_REGISTERED—
	// —而 Master 之前的任何内容都会对应同一状态，无论该内容属于哪个路由。因此，仅从状态上无法判断具体发
	// 生了什么，这也是为什么判断缺失情况取决于代码，而非 404 状态。
	masterCodeObjectNotFound = -704
)

// TenantQuota 保存 Admin API 返回的一条租户账本记录。
//
// 该字段设置正是所测量构建版本（0.3.12.post1）所响应的内容。
//
// v0.3.13 回应的是另一组问题——它删除了 used_bytes、reserved_bytes 以及这两个计数项，转而使用一个
// charged_bytes 字段，并新增了 admission_closed 字段。这里没有任何扩展内容，因为没有字段显示它所做的更改：
// 账本最终聚焦于 RequestedBytes 和 HasExplicitPolicy 两个字段，这两个字段都用于构建发送数据。缺失的四个字段
// 会被解码为零，且不会从该类型中发布——每个租户的 METRICS 字段正是状态数据的来源，而那个 Decode 实例会处理这
// 两个版本的数据。如果进一步扩展这个类型，还会增加没有读取器的字段。
type TenantQuota struct {
	// TenantID 是条目的标识，即 Binding 声明的复用域名。
	TenantID string

	// RequestedBytes 是申请的额度，EffectiveBytes 是 master 实际授予的额度。总申请超过可分配容量时实际
	// 值会减小，master 上还没有任何已挂载内存段时，每个租户的实际额度都为零。
	RequestedBytes int64
	EffectiveBytes int64

	// UsedBytes 统计已经提交的用量，ReservedBytes 是进行中的 PutStart 预留。status 只报告前者，以免把并
	// 发写入暂时保留的空间记作已使用。
	UsedBytes     int64
	ReservedBytes int64

	// CommittedCount 和 MetadataObjectCount 是条目的对象数量。
	CommittedCount      int64
	MetadataObjectCount int64

	// OverQuota 是 master 对当前已持有数据超过实际配额的判断。master 会拒绝越过上限的新写入，已有租户通
	// 常在配额下调后才会进入此状态。
	OverQuota bool

	// HasExplicitPolicy 用于区分那些账本中带有特定政策的租户，以及那些在默认设置下被提供推理服务的租户。
	// 没有 quota.ceiling 的绑定关系则对应第二种情况，这两种情况不可混淆：一种是此操作器所设定的上限，另
	// 一种则是没有上限的情况。
	HasExplicitPolicy bool
}

type tenantQuotaListEnvelope struct {
	Data *[]tenantQuotaBody `json:"data"`
}

// tenantQuotaBody 与某个快照相同。它与 TenantQuota 是分开的，因此区分 "absent" 和 "empty" 的指针会保留在边
// 界处，而不会向内部扩散。
//
// JSON 标签沿用 master_admin_service.cpp 的 C++ 字段名。YLT_REFL 序列化时保留 snake_case。标签拼错一个字符，
// Go 解码就可能静默得到零值，所以只在协议边界使用这些字段名。
type tenantQuotaBody struct {
	// 对租户标识使用指针，以区分字段缺失和空值。来自错误端点的 {} 也符合 JSON 语法，不能把它当作一个用
	// 量和配额均为零的真实租户。
	TenantID            *string `json:"tenant_id"`
	RequestedQuotaBytes int64   `json:"requested_quota_bytes"`
	EffectiveQuotaBytes int64   `json:"effective_quota_bytes"`
	UsedBytes           int64   `json:"used_bytes"`
	ReservedBytes       int64   `json:"reserved_bytes"`
	CommittedCount      int64   `json:"committed_count"`
	MetadataObjectCount int64   `json:"metadata_object_count"`
	OverQuota           bool    `json:"over_quota"`
	HasExplicitPolicy   bool    `json:"has_explicit_policy"`
}

func (b tenantQuotaBody) toTenantQuota() TenantQuota {
	return TenantQuota{
		TenantID:            *b.TenantID,
		RequestedBytes:      b.RequestedQuotaBytes,
		EffectiveBytes:      b.EffectiveQuotaBytes,
		UsedBytes:           b.UsedBytes,
		ReservedBytes:       b.ReservedBytes,
		CommittedCount:      b.CommittedCount,
		MetadataObjectCount: b.MetadataObjectCount,
		OverQuota:           b.OverQuota,
		HasExplicitPolicy:   b.HasExplicitPolicy,
	}
}

// DecodeTenantQuotaList 解码 master 返回的完整租户账本。
//
// 一个空的账本是一个合法的值——一个多租户的 Master，目前还没有人编写过相关策略——因此会返回一个空的切片。如果
// 一个对象不是条目列表，则属于错误情况，而非空账本，因为这两种情况都会引导调用者走向相反的方向：前者表明所
// 有所需的条目都缺失且必须写入，后者则表明该地址无法响应账本。
func DecodeTenantQuotaList(body []byte) ([]TenantQuota, error) {
	var wire tenantQuotaListEnvelope
	if err := json.Unmarshal(body, &wire); err != nil {
		return nil, fmt.Errorf("%w: %s: %w", ErrMalformedBody, adminPathTenantQuotas, err)
	}
	// 缺失或空的数据并非空的账本。JSON 中的 null 在没有任何错误的情况下会解析为 nil，因此这个指针是区分
	// “Master 未发送列表”与“Master 发送了空列表”的唯一依据，而只有后者属于该运算符可能收敛的对象。
	if wire.Data == nil {
		return nil, fmt.Errorf("%w: %s: no data in the body",
			ErrMalformedBody, adminPathTenantQuotas)
	}

	entries := make([]TenantQuota, 0, len(*wire.Data))
	for _, entry := range *wire.Data {
		if entry.TenantID == nil {
			return nil, fmt.Errorf("%w: %s: an entry carries no tenant_id",
				ErrMalformedBody, adminPathTenantQuotas)
		}
		entries = append(entries, entry.toTenantQuota())
	}
	return entries, nil
}

// ListTenantQuotas 读取 master 账本上的全部租户条目。
//
// 这就是收敛机制的工作方式：目标集合由绑定数据计算得出，而 Master 中保存的、没有任何绑定声明的数据，就是
// DELETE 操作所针对的内容。每次处理时只列出一次，而非按租户进行查询，这样无论绑定数量如何，都只需一个请求即可。
func (c *AdminClient) ListTenantQuotas(ctx context.Context) ([]TenantQuota, error) {
	body, _, err := c.tenantQuotaDo(ctx, http.MethodGet, "", nil)
	if err != nil {
		return nil, err
	}
	return DecodeTenantQuotaList(body)
}

// PutTenantQuota 会创建或更新一条记录，将 requestedBytes 设置为该租户的 requested_quota_bytes。
//
// 其设计是无条件支持的：认定某项记录已包含该数值且无需写入，由持有相关账本的调用者决定。每次写入前先读取的
// 客户端，会使不会产生任何变化的操作次数翻倍。
//
// 这里的错误是 OBJECT_NOT_FOUND，且仅在此处出现。tenantQuotaDo 报告称该错误为“不存在但未失败”，因为对于 GET
// 请求来说，这意味着一个租户在准入阶段及首次写入之前；对于 DELETE 请求来说，则意味着该条目已消失——但此路径
// 属于 upsert 操作，因此不存在可以描述其缺失的租户。该报告指出请求从未到达负责写入的处理器：错误的地址、
// Master 前的代理服务器，或没有这些路径的构建过程。将此报告为成功会使得 pass 调用 ledger 的 converged 及资
// 源池的 Ready 状态成立，而配额则未被写入。任何其他代码的 404 错误都会在更低层级被拒绝，适用于所有方法。
func (c *AdminClient) PutTenantQuota(ctx context.Context, tenantID string, requestedBytes int64) error {
	payload, err := json.Marshal(map[string]int64{"requested_quota_bytes": requestedBytes})
	if err != nil {
		return fmt.Errorf("build quota body for %q: %w", tenantID, err)
	}

	_, found, err := c.tenantQuotaRequest(ctx, http.MethodPut, tenantID, payload)
	if err != nil {
		return err
	}
	if !found {
		// 刻意地没有那些哨兵：它既不属于所提到的两个前提条件之一，因此控制器在账本轴上将其标记为无
		// 法访问的账本，事实就是如此。
		return fmt.Errorf("PUT %s: the master answered 404, so the quota of %q was not written",
			adminPathTenantQuotas, tenantID)
	}
	return nil
}

// DeleteTenantQuota 删除一个租户条目。条目已不存在时也视为成功。
//
// Master 对于它不认识的租户会返回 OBJECT_NOT_FOUND 响应，而这正是删除操作所期望的状态。如果将其报告为失败，
// 就会留下一个能够完美报告错误的路径，且会一直如此：该条目将永远无法恢复。而不携带该代码的 404 错误则会被当
// 作错误处理，因为调用者正是那个在成功时由终结器释放的对象。
//
// 被移除的条目会重新出现在答案数据中，因此这里没有进行解码：描述内容说明了刚刚被删除的内容，而这个操作符没
// 有用于获取该快照的读取器。这个残骸声明它是可选的，原因也是它可能不存在。
//
// 仍拥有对象的租户会因 ErrTenantNotEmpty 被拒绝，而非被删除。这是 Master 自身的先决条件，这是提示调用者等待
// 而非继续重试的信号。
func (c *AdminClient) DeleteTenantQuota(ctx context.Context, tenantID string) error {
	_, _, err := c.tenantQuotaRequest(ctx, http.MethodDelete, tenantID, nil)
	return err
}

// tenantQuotaRequest 指向账本中的一条记录。
//
// 空的 tenantID 在此被拒绝，不会被发送。在写入方法中，Master 无论如何都会返回 400 错误，但在 GET 操作中，查
// 询将完全不存在，Master 会处理整个账本——因此，如果调用者请求一个名为空名的租户，将会收到一个列表，需要将其
// 解码为一条记录，并告知调用者 Master 的响应体存在错误。
func (c *AdminClient) tenantQuotaRequest(
	ctx context.Context, method, tenantID string, payload []byte,
) ([]byte, bool, error) {
	if tenantID == "" {
		return nil, false, fmt.Errorf("%w: %s: no tenant id was given",
			ErrInvalidTenantID, adminPathTenantQuotas)
	}
	return c.tenantQuotaDo(ctx, method, url.Values{"tenant_id": []string{tenantID}}.Encode(), payload)
}

// tenantQuotaDo 执行一次账本请求，并将结果转化为可识别的失败。它返回的布尔值表示租户是否存在：当值为 false
// 且出现 nil 错误时，即为 Master 的 404 错误。
func (c *AdminClient) tenantQuotaDo(
	ctx context.Context, method, query string, payload []byte,
) ([]byte, bool, error) {
	target := fmt.Sprintf("http://%s%s", c.Address, adminPathTenantQuotas)
	if query != "" {
		target += "?" + query
	}

	var reader io.Reader
	if payload != nil {
		reader = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, target, reader)
	if err != nil {
		return nil, false, fmt.Errorf("build %s request for %s: %w", method, adminPathTenantQuotas, err)
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.HTTP.Do(req)
	if err != nil {
		// 传输故障会像在读取路径上那样独立存在：没有响应，既不是格式错误的实体，也不是拒绝信号。
		return nil, false, fmt.Errorf("request %s %s: %w", method, adminPathTenantQuotas, err)
	}
	defer func() { _ = resp.Body.Close() }()

	// 与读取路径相同的限制和同一字节的超调，因为都是同一个端口，原因也相同：即管理员在 CR 中写入的地址名称。
	body, err := io.ReadAll(io.LimitReader(resp.Body, adminReadLimit+1))
	if err != nil {
		return nil, false, fmt.Errorf("read %s %s: %w", method, adminPathTenantQuotas, err)
	}
	if len(body) > adminReadLimit {
		return nil, false, fmt.Errorf("%w: %s: the response is larger than %d bytes",
			ErrMalformedBody, adminPathTenantQuotas, adminReadLimit)
	}

	switch resp.StatusCode {
	case http.StatusOK:
		return body, true, nil
	case http.StatusNotFound:
		// 缺席，而非失败——但仅当 Master 在代码中明确说明时才会如此。404 错误来自四个相关代码的触发，
		// 以及位于其前的任何不服务于该路径的组件：一个代理、一个错误的地址，或没有配置账本路由的构
		// 建过程。将其理解为缺席，最后这一组最为危险，因为调用此函数的每个调用者都将缺席视为一个确
		// 定的结果，而非一个失败的疑问。GetTenantQuota 报告租户未知，DeleteTenantQuota 则报告条目已
		// 删除，这使得 Binding 的终结器能够释放仍存在的账本条目。
		if !objectNotFound(body) {
			return nil, false, fmt.Errorf(
				"request %s %s: the master answered 404 without OBJECT_NOT_FOUND, so this is a "+
					"route that does not exist rather than a tenant that does not: %s",
				method, adminPathTenantQuotas, excerpt(body),
			)
		}
		return nil, false, nil
	case http.StatusServiceUnavailable:
		// 账本路由机制与服务平面上的 `/`get_segments_detail 类似，因此由起始主节点负责处理。这是一
		// 个阶段性的操作，调用方需要重试。
		//
		// 它携带了 error_code -1011 这一代码，与 409 相同的代码表示多租户功能已关闭，因为 Master 通
		// 过同一个辅助组件进行写入操作。因此，在询问代码之前会先查询状态，而不是反过来：仅通过代码
		// 读取的起始主节点会被报告为需要有人去后端进行重新配置的状态。
		return nil, false, fmt.Errorf("%w: %s: %s",
			ErrServicePlaneInactive, adminPathTenantQuotas, excerpt(body))
	case http.StatusBadRequest:
		// -600，这是 Master 在同一个代码下对两个无关输入所返回的结果：在 v0.3.12.post1 中，无论是被
		// 拒绝的配额还是被拒绝的 tenant_id，都会以 ErrorCode::INVALID_PARAMS 的形式返回——
		// ParseAdminTenantId 在 id 为空或无效时也会返回该值，而 HandleUpsertTenantQuota 中的零配额
		// 分支也会返回该值。目前没有代码层面的区分方式可以切换，因此这个消息不是快捷方式，而是唯一
		// 的提示：经测量，每个 tenant-id 的拒绝都会标记该字段，而配额拒绝则不会。
		refusal := ErrInvalidQuota
		if strings.Contains(strings.ToLower(string(body)), "tenant_id") {
			refusal = ErrInvalidTenantID
		}
		return nil, false, fmt.Errorf("%w: %s %s: %s",
			refusal, method, adminPathTenantQuotas, excerpt(body))
	case http.StatusConflict:
		// 三个无关代码具有相同的状态——-1011、-1010 和 -1702——其中有一个会进入终结器所等待的删除路径，
		// 因此仅从状态来看可能无法决定。
		return nil, false, conflictError(method, body)
	case http.StatusInternalServerError:
		// ErrorCodeToHttpStatus 会将所有没有特定情况的代码映射到此状态，因此仅从状态本身就能判断是“
		// Master 故障”。其中有一个代码是此操作员需要以不同方式报告的挂载故障，而具体描述部分则决定
		// 了它与其他情况的差异。
		if persistentFail(body) {
			return nil, false, fmt.Errorf("%w: %s %s: %s",
				ErrQuotaPolicyNotWritable, method, adminPathTenantQuotas, excerpt(body))
		}
		return nil, false, fmt.Errorf("request %s %s: unexpected status %d: %s",
			method, adminPathTenantQuotas, resp.StatusCode, excerpt(body))
	default:
		return nil, false, fmt.Errorf("request %s %s: unexpected status %d: %s",
			method, adminPathTenantQuotas, resp.StatusCode, excerpt(body))
	}
}

// objectNotFound 判断 HTTP 404 是否确实表示 master 找不到该租户。
//
// 一个无法解析的实体故意未能通过测试：Master 会拒绝每一次尝试，因此没有来自账本处理器的回应。缺失正是该客户
// 端会触发 404 错误的情况，而错误地应对这一情况则不会产生任何反应——因此举证责任落在了实体身上。
func objectNotFound(body []byte) bool {
	var wire struct {
		ErrorCode int32 `json:"error_code"`
	}
	return json.Unmarshal(body, &wire) == nil && wire.ErrorCode == masterCodeObjectNotFound
}

// persistentFail 判断 HTTP 500 是否表示 master 无法持久化已经接收的策略。
func persistentFail(body []byte) bool {
	var wire struct {
		ErrorCode int32 `json:"error_code"`
	}
	return json.Unmarshal(body, &wire) == nil && wire.ErrorCode == masterCodePersistentFail
}

// conflictError 根据响应体的 error_code 识别 HTTP 409 对应的具体拒绝原因。
//
// 一个无法解析的实体，或者属于三个名称中都不存在的代码，被刻意设定为 NONE，不属于那些哨兵：猜测会为某个对象
// 设定条件，而这是 Master 从未给出过的理由。Master 自己的话仍然存在于信息中，这就是使其可以被诊断的原因。
func conflictError(method string, body []byte) error {
	var wire struct {
		ErrorCode int32 `json:"error_code"`
	}
	if err := json.Unmarshal(body, &wire); err == nil {
		var refusal error
		switch wire.ErrorCode {
		case masterCodeUnavailableInCurrentMode:
			refusal = ErrMultiTenancyDisabled
		case masterCodeTenantNotEmpty:
			refusal = ErrTenantNotEmpty
		case masterCodeUnavailableInCurrentStatus:
			refusal = ErrUnavailableInCurrentStatus
		}
		if refusal != nil {
			return fmt.Errorf("%w: %s %s: %s", refusal, method, adminPathTenantQuotas, excerpt(body))
		}
	}

	return fmt.Errorf("request %s %s: unexpected conflict: %s",
		method, adminPathTenantQuotas, excerpt(body))
}
