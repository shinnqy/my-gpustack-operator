package mooncake

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// 每条租户级指标仅使用 tenant_id 标签，其值就是 Binding 声明的复用域。每个 Binding 只读取自己的指标序列，无需跨租户求和。
//
// 读取两个代次，因为该图像符合后端规范的定义，且两者均被使用。0.3.12.post1 会生成八个这样的代次；0.3.13 则
// 移除了四个并增加了一个。构建过程只会生成一个或另一个代次，绝不会同时生成两个，因此每个代次都保持为指针状
// 态，缺失的代次也仅仅是不存在——同时读取两个会消耗两个情况，但能获得一个占用量，否则在新版本构建中就会悄然缺失。
const (
	metricTenantRequestedBytes = "mooncake_tenant_quota_requested_bytes"
	metricTenantEffectiveBytes = "mooncake_tenant_quota_effective_bytes"
	metricTenantOverQuota      = "mooncake_tenant_quota_over_quota"
	metricTenantExplicitPolicy = "mooncake_tenant_quota_explicit_policy"

	// 仅支持 0.3.12.post1。它将占用情况分为两部分，并统计对象数量。
	metricTenantUsedBytes           = "mooncake_tenant_quota_used_bytes"
	metricTenantReservedBytes       = "mooncake_tenant_quota_reserved_bytes"
	metricTenantCommittedCount      = "mooncake_tenant_quota_committed_count"
	metricTenantMetadataObjectCount = "mooncake_tenant_quota_metadata_object_count"

	// 仅支持 0.3.13 版本。在 PutStart 处收取一次占用费用，因此其中已包含 metricTenantReservedBytes 原本
	// 单独计算的数值，此外还有一个表示未超过配额限制的拒绝标志。
	metricTenantChargedBytes    = "mooncake_tenant_quota_charged_bytes"
	metricTenantAdmissionClosed = "mooncake_tenant_quota_admission_closed"
)

// 这些全局指标没有任何标签。它们描述的是 Master 整体情况，而非某个租户的具体状况。其中第一个指标就说明了整
// 个资源池都是空的：一个没有挂载内存段的 Master 没有可分配的资源，因此每个租户的有效配额都是零，任何写入操
// 作都无法成功，尽管所有对象看起来都配置正确。
const (
	metricTenantAllocatableCapacityBytes = "mooncake_tenant_quota_allocatable_capacity_bytes"
	metricTenantRequestedBytesSum        = "mooncake_tenant_quota_requested_bytes_sum"
	metricTenantEffectiveBytesSum        = "mooncake_tenant_quota_effective_bytes_sum"
)

// tenantLabel 是每租户指标系列唯一的标签名。
const tenantLabel = "tenant_id"

// TenantSample 保存一次采集得到的单个租户的各项读数。
//
// 每个图都是一个指针，这种区别正是该类型存在的全部原因：Master 只序列化它所观察到的标签组合，因此文档中缺失
// 的数据可能意味着从未触发过，而非数值为零。如果标注为零，“此租户尚未写作”和“此租户的使用情况未被报告”会处
// 于相同的状态——而在热缓存中制造一个虚假的零比没有数字更糟糕。
type TenantSample struct {
	// RequestedBytes 是所请求的量，而 EffectiveBytes 是 Master 实际授予的量。当所有租户的请求总和超过可
	// 分配量时，后者会按比例减少。
	RequestedBytes *int64
	EffectiveBytes *int64

	// UsedBytes 表示已使用的消耗量；ReservedBytes 则代表正在进行的 PutStart 预留资源。状态报告中第一个
	// 值为“ALONE”——若将第二个值也包含进去，就会将同时进行的写入操作误判为从未发生的消耗行为。
	// 0.3.12.post1 仅适用于此；详见“Occupancy”部分。
	UsedBytes     *int64
	ReservedBytes *int64

	// CommittedCount 和 MetadataObjectCount 是此租户的对象数量。0.3.12.post1 仅限使用——0.3.13 停止了它
	// 们的导出，也没有任何内容替代它们。
	CommittedCount      *int64
	MetadataObjectCount *int64

	// ChargedBytes 是 0.3.13 的单一占用数值，取代了上述数值。该数值在 PutStart 时刻被计费，因此正在进行
	// 的预订已包含在该数值中，无需进行减除操作。
	ChargedBytes *int64

	// AdmissionClosed 是 0.3.13 用于表示 master 拒绝该租户新写入的指标。即使没有超额，租户的用量记录不
	// 可信时也会关闭准入，增大配额也不会解除这种状态。
	AdmissionClosed *bool

	// OverQuota 是 Master 的裁决结果，表示该租户使用的资源量超过了其有效配额，而这一情况仅发生在配额在
	// 其之下重新调整时：所收取的字节数从未超过授予量，因为 Master 拒绝收取费用，也不允许总量超标。因此，
	// 超过配额的写入操作被判定为错误，上述 AdmissionClosed 也不应作为替代判断标准——超过配额的写入操作既
	// 不是超过配额，也不是准入行为，而是 Master 正在清除该租户自己的对象并允许该写入操作进行。
	OverQuota *bool

	// ExplicitPolicy 将账本中某租户所持有的书面配额与在 Master 默认配置下运行的配额区分开来。无上限限制
	// 的绑定会产生第二种情况，这是一种需要报告的状态，而非需要隐藏的状态。
	ExplicitPolicy *bool
}

// Occupancy 返回租户已占用的字节数，并说明其中是否包含尚未完成的写入。
//
// 两代产品对于可测量的内容存在分歧，而这一分歧在这一点上得到了解决，因此调用者无需知道正在处理的是哪个版本。
// 0.3.12.post1 报告了除预留部分之外的已提交字节数，因此第一个返回值排除了这些部分，故“正在处理中”为假。
// 0.3.13 报告了从 PutStart 中收取的一个数值，因此预留部分包含在其中，“正在处理中”为真——发布该数字的调用者必
// 须明确说明，因为同时发生的写入操作会在任何操作提交之前增加该数值。
//
// 当两者都存在时，更旧的那一对更为优选：这是更精确的答案，迄今为止没有观察到同时具备两者的情况。
func (s TenantSample) Occupancy() (bytes *int64, inflight bool) {
	if s.UsedBytes != nil {
		return s.UsedBytes, false
	}
	return s.ChargedBytes, s.ChargedBytes != nil
}

// TenantQuotaMetrics 保存一次采样中出现的各租户指标及 master 的全局指标。
//
// 这是一份用于整个池的文档。一次通过会读取它一次，而每个 Binding 会从中提取出自己租户的序列，这样就能使请求
// 数量与 Binding 数量相互独立。
type TenantQuotaMetrics struct {
	// 租户通过 tenant_id 进行关联。文档中未提及的租户在此并非键，这种缺失正是报告中所说的：参见“租户”部分。
	Tenants map[string]TenantSample

	// AllocatableCapacityBytes 为 master 可在租户之间分配的容量。启动初期若尚无成员挂载内存段，它会为零，
	// 此时所有租户的实际额度也都是零。
	AllocatableCapacityBytes *int64

	// RequestedBytesSum 和 EffectiveBytesSum 是 Master 自身在所有已知租户中的总数据量，包括属于同一
	// Master 下其他资源池的租户。没有任何与 Binding 相关的数据是从这些数值中推导出来的；读取这些数值是
	// 为了让操作方能够看到第二个数值低于第一个数值，这正是处于压力下的资源池的实际情况。
	RequestedBytesSum *int64
	EffectiveBytesSum *int64

	// TotalCapacityBytes 是 Master 级别的容量指标，从同一数据源中读取得到。与 AllocatableCapacityBytes
	// 不同，无论租户子系统是否运行，该值都会被输出，因此对于没有租户账本的 Master 来说，这就是唯一的容量数值。
	TotalCapacityBytes *int64
}

// Tenant 返回指定租户的采样值，并标明该租户是否出现在指标文档中。
//
// bool 字段是关键：调用者必须能够区分“已报告，所有数值均为零”和“未报告”，因为前者是普通租户，而后者是
// Master 从未听说过的租户——这就是尚未写入配额值的 Binding 所呈现的样态。
func (m TenantQuotaMetrics) Tenant(tenantID string) (TenantSample, bool) {
	sample, ok := m.Tenants[tenantID]
	return sample, ok
}

// TenantQuotaMetrics 请求 /metrics，从响应中读取租户相关指标。
//
// AdminClient.Capacity 读取同一个端点。master 在同一份指标文本中同时公布全局容量和每租户数据，两个调用方各
// 自解析所需的序列。
func (c *AdminClient) TenantQuotaMetrics(ctx context.Context) (TenantQuotaMetrics, error) {
	body, err := c.get(ctx, adminPathMetrics)
	if err != nil {
		return TenantQuotaMetrics{}, err
	}
	return DecodeTenantQuotaMetrics(body)
}

// DecodeTenantQuotaMetrics 从同一份 Prometheus 指标文本中解析两个版本的租户序列和三个全局指标。
//
// 沿用 DecodeLeaderCapacity 的处理方式直接解析指标文本。上游解析器要求首次使用前设置进程级指标名称校验模式，
// 而本进程也维护着控制器自己的指标注册表。
//
// 遇到不完整的样本行就返回错误。否则响应在传输途中截断时，解析器可能把已收到的前半段当作完整采样，误报后续租户不存在。
func DecodeTenantQuotaMetrics(body []byte) (TenantQuotaMetrics, error) {
	metrics := TenantQuotaMetrics{Tenants: map[string]TenantSample{}}

	globals := map[string]**int64{
		metricTenantAllocatableCapacityBytes: &metrics.AllocatableCapacityBytes,
		metricTenantRequestedBytesSum:        &metrics.RequestedBytesSum,
		metricTenantEffectiveBytesSum:        &metrics.EffectiveBytesSum,
		metricTotalCapacityBytes:             &metrics.TotalCapacityBytes,
	}

	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		name, labels, value, err := splitSample(line)
		if err != nil {
			return TenantQuotaMetrics{}, err
		}

		if target, ok := globals[name]; ok {
			// 一个全局指标没有标签。如果带有标签的，那便是另一个系列，只是恰好使用了相同的名称。
			// 将其作为全局数值读取时，就会将某个租户的总额误认为是 Master 的值。
			if labels != "" {
				continue
			}
			parsed, err := parseSampleCount(name, value)
			if err != nil {
				return TenantQuotaMetrics{}, err
			}
			*target = &parsed
			continue
		}

		if !isTenantSeries(name) {
			continue
		}

		tenantID, err := sampleTenantID(name, labels)
		if err != nil {
			return TenantQuotaMetrics{}, err
		}

		sample := metrics.Tenants[tenantID]
		if err := assignTenantSample(&sample, name, value); err != nil {
			return TenantQuotaMetrics{}, err
		}
		metrics.Tenants[tenantID] = sample
	}

	return metrics, nil
}

func isTenantSeries(name string) bool {
	switch name {
	case metricTenantRequestedBytes, metricTenantEffectiveBytes, metricTenantUsedBytes,
		metricTenantReservedBytes, metricTenantCommittedCount, metricTenantMetadataObjectCount,
		metricTenantChargedBytes, metricTenantAdmissionClosed,
		metricTenantOverQuota, metricTenantExplicitPolicy:
		return true
	default:
		return false
	}
}

// assignTenantSample 把一条指标序列的数值写入所属租户的样本。
func assignTenantSample(sample *TenantSample, name, value string) error {
	switch name {
	case metricTenantOverQuota, metricTenantExplicitPolicy, metricTenantAdmissionClosed:
		flag, err := parseSampleFlag(name, value)
		if err != nil {
			return err
		}
		switch name {
		case metricTenantOverQuota:
			sample.OverQuota = &flag
		case metricTenantExplicitPolicy:
			sample.ExplicitPolicy = &flag
		case metricTenantAdmissionClosed:
			sample.AdmissionClosed = &flag
		}
		return nil
	}

	count, err := parseSampleCount(name, value)
	if err != nil {
		return err
	}
	switch name {
	case metricTenantRequestedBytes:
		sample.RequestedBytes = &count
	case metricTenantEffectiveBytes:
		sample.EffectiveBytes = &count
	case metricTenantUsedBytes:
		sample.UsedBytes = &count
	case metricTenantReservedBytes:
		sample.ReservedBytes = &count
	case metricTenantCommittedCount:
		sample.CommittedCount = &count
	case metricTenantMetadataObjectCount:
		sample.MetadataObjectCount = &count
	case metricTenantChargedBytes:
		sample.ChargedBytes = &count
	}
	return nil
}

// splitSample 将一行描述文本拆分为其名称、标签块和值三个部分。
//
// 一个样本为 `name[{labels}] value [timestamp]`，其中时间戳是可选的——因此值指的是名称之后的第一个字段，而非
// 整个内容。不符合该结构的行将被拒绝：关于为何截断的文档不能被视为短文档，请参考 Decode 实例的注释。
func splitSample(line string) (name, labels, value string, err error) {
	// 在分隔空格处进行切割，该空格位于标签块之外。这位操作员编写的租户 ID 没有空格，但账本中却包含其他
	// 地方生成的 ID，在第一个空格处进行切割会将 `{tenant_id="foo bar"}` 一分为二——没有闭合花括号的头部，
	// 被拒绝，随之而来的是整个结构以及所有管理的绑定数据。这与 splitLabels 中的逗号问题相同，只是提前了一个阶段。
	head, tail, ok := cutOutsideLabels(line)
	if !ok {
		return "", "", "", fmt.Errorf("%w: %s: %q is not a sample line",
			ErrMalformedBody, adminPathMetrics, quoteExternal(line))
	}

	value = strings.TrimSpace(tail)
	if i := strings.IndexAny(value, " \t"); i >= 0 {
		value = value[:i]
	}

	name = head
	if open := strings.IndexByte(head, '{'); open >= 0 {
		if !strings.HasSuffix(head, "}") {
			return "", "", "", fmt.Errorf("%w: %s: %q has no closing brace",
				ErrMalformedBody, adminPathMetrics, quoteExternal(head))
		}
		name, labels = head[:open], head[open+1:len(head)-1]
	}
	return name, labels, value, nil
}

// cutOutsideLabels 会在不属于其标签块的第一个空格处切割样本行，因此包含空格的标签 VALUE 不会在未被分割的位置中断行。
func cutOutsideLabels(line string) (head, tail string, found bool) {
	var quoted, escape bool
	for i := 0; i < len(line); i++ {
		switch {
		case escape:
			escape = false
		case line[i] == '\\':
			escape = true
		case line[i] == '"':
			quoted = !quoted
		case (line[i] == ' ' || line[i] == '\t') && !quoted:
			return line[:i], line[i+1:], true
		}
	}
	return line, "", false
}

// splitLabels 会根据分隔标签的逗号将标签块拆分，而引号内的内容则保持不变。
//
// 在这里使用“,”进行简单拆分是不合适的，因为该包在其他地方已有相关说明：Master 的账本中包含了该集群中没有人
// 创建的条目，且其标识符并非 DNS-1123 标签。外部租户名称中的一个逗号会导致 `tenant_id="foo,bar"` 被拆分为两
// 个片段，无法正确解析；而且由于没有可用的标识符，该名称会被拒绝而非跳过，这将导致整个抓取操作失败，并连带
// 影响 Master 上所有受管理的 Binding 的数据。
func splitLabels(labels string) []string {
	var (
		out            []string
		start          int
		quoted, escape bool
	)
	for i := 0; i < len(labels); i++ {
		switch {
		case escape:
			escape = false
		case labels[i] == '\\':
			escape = true
		case labels[i] == '"':
			quoted = !quoted
		case labels[i] == ',' && !quoted:
			out = append(out, labels[start:i])
			start = i + 1
		}
	}
	return append(out, labels[start:])
}

// sampleTenantID 从每租户指标中读取 tenant_id 标签。
//
// 没有该标签的租户家庭样本会被拒绝而非删除：该标签是表明其属于哪个系列的唯一标识，因此没有该标签的样本要么
// 不是此 Master 系列，要么是被损坏的文件，这两种情况都意味着无法依赖该数据说明哪个租户缺失。
func sampleTenantID(name, labels string) (string, error) {
	for _, label := range splitLabels(labels) {
		key, quoted, ok := strings.Cut(strings.TrimSpace(label), "=")
		if !ok || strings.TrimSpace(key) != tenantLabel {
			continue
		}
		quoted = strings.TrimSpace(quoted)
		if len(quoted) < 2 || !strings.HasPrefix(quoted, `"`) || !strings.HasSuffix(quoted, `"`) {
			break
		}
		// Master 在输出时 escapes 了反斜杠、双引号和换行符，因此输入时这些字符不会被 escape。这位操
		// 作员创建的租户 ID 无法包含任何内容——它是一个 DNS-1123 标签——但 Master 的账本中也有此处没
		// 有人创建的记录。
		unquoted := strings.NewReplacer(`\\`, `\`, `\"`, `"`, `\n`, "\n").
			Replace(quoted[1 : len(quoted)-1])
		if unquoted == "" {
			break
		}
		return unquoted, nil
	}

	return "", fmt.Errorf("%w: %s: %s carries no usable %s label",
		ErrMalformedBody, adminPathMetrics, name, tenantLabel)
}

// parseSampleCount 将一个样本值解析为非负整数。
//
// 它拒绝任何未经验证的 int64 转换后可能变成的虚假数值。ParseFloat 可以接受 NaN 和无穷大值，而将不适合 int64
// 的 float64 转换为 int64 在 Go 语言中属于未定义行为——根据测量结果，同一样本在 arm64 架构下能达到 int64 的
// 最大值，在 amd64 架构下则能达到最小值，而这两种数值都是本项目所支持的范围。
//
// 两条路径，两个边界。普通整数可以精确读取，其值可以是 int64 所能承载的任何数值；而以 float 形式表示的数值
// 则被限制在 2^53 以内，超过此范围后 float64 根本无法存储整数。样本选择哪条路径，取决于其表示形式，而非其大小。
func parseSampleCount(name, value string) (int64, error) {
	// 首先按准确的拼写来写，因为这些样本是字节计数，而 float64 无法存储 2^53 的整数字符串：从
	// ParseFloat 返回的 9007199254740993 被显示为 ...992，这是完整的、在范围内的数值，但是错误的——它通
	// 过了所有下方的校验，并被作为 Master 从未报告的数值发布。ParseInt 会原样接受普通整数，而 float 路
	// 径则用于展示相同数值的其他拼写形式（1.5e+09、12.0）。
	if exact, err := strconv.ParseInt(value, 10, 64); err == nil {
		if exact < 0 {
			return 0, fmt.Errorf("%w: %s: %s carries %q, which is not a count",
				ErrMalformedBody, adminPathMetrics, name, quoteExternal(value))
		}
		return exact, nil
	}

	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		// 仅出现 strconv 的错误原因：其自身的 Error() 会将整个输入内容返回，这将使得上述被引用的字
		// 符串失去无限制特性，并受到限制。
		return 0, fmt.Errorf("%w: %s: %s carries %q: %w",
			ErrMalformedBody, adminPathMetrics, name, quoteExternal(value), errors.Unwrap(err))
	}
	// 这里的限制是 2^53，而不是 2^63，且两条路径的边界设定有意为之。仅由浮点数表示的样本只能到达这一行，
	// 因为它不是普通整数，因此 2^53 以上的数值不可信：`9007199254740993.0`解析后为...992，这是一个整数，
	// 非负且属于 int64 范围，但作为数值不会被 Master 报告。上方确切的边界没有限制，因为 ParseInt 会原样
	// 读取整个 int64 范围——这就是快速路径存在的原因。拒绝不可表示的浮点数表示法是双方之间的安全妥协：一
	// 个代表如此大数量的 Master 总能将其表示为数字。
	if math.IsNaN(parsed) || math.IsInf(parsed, 0) ||
		parsed < 0 || parsed >= float64(uint64(1)<<53) ||
		parsed != math.Trunc(parsed) {
		return 0, fmt.Errorf("%w: %s: %s carries %q, which is not a count",
			ErrMalformedBody, adminPathMetrics, name, quoteExternal(value))
	}
	return int64(parsed), nil
}

// parseSampleFlag 会读取一个样本值，该值作为布尔值，代表 0 或 1 中的某一个。其他值则会被拒绝，不会被视为真
// 值：既非 0 也非 1 的标记表明文档并非此读取器所认为的那样，而 over_quota 则决定一个命名空间是否被通知其域
// 的权限超过当前授予的权限。
func parseSampleFlag(name, value string) (bool, error) {
	switch value {
	case "0":
		return false, nil
	case "1":
		return true, nil
	default:
		return false, fmt.Errorf("%w: %s: %s carries %q, which is not 0 or 1",
			ErrMalformedBody, adminPathMetrics, name, quoteExternal(value))
	}
}
