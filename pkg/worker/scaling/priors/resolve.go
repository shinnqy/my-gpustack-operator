package priors

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// DefaultHeadroom 是请求未设置裕量时应用于实测容量的裕量。
//
// 它是一项工程安全裕量，并非用于证明队列长度或副本数保持在固定倍数以下。
const DefaultHeadroom = 0.75

// Request 表示一次容量先验解析请求。
type Request struct {
	// Role 是该容量所对应的服务角色。
	Role RoleKind

	// Identity 是 profile 必须匹配的部署身份。
	Identity Identity

	// ServingConfig 是 profile 必须匹配的服务配置。
	ServingConfig ServingConfig

	// Topology 是 profile 必须匹配的单个扩缩容副本形态。
	Topology Topology

	// Constraints 是延迟目标与裕量。
	Constraints Constraints
}

// Constraints 是解析出的先验必须满足的要求。
type Constraints struct {
	// TTFTTargetMilliseconds 是该角色必须低于的首 Token 时间（TTFT）目标。
	TTFTTargetMilliseconds float64

	// ITLTargetMilliseconds 是该角色必须低于的 Token 间延迟（ITL）目标。
	ITLTargetMilliseconds float64

	// Headroom 是应用于实测容量的裕量。
	//
	// 必须位于 (0,1] 区间内。未设置时默认为 DefaultHeadroom。
	Headroom float64

	// LatencyQuantile 是请求需满足的延迟分位数。
	//
	// 为零时表示省略，此时解析器要求使用 DefaultLatencyQuantile。
	//
	// 设置时必须位于 (0,1] 区间内，因为该范围之外的数值不代表任何有效分位数。
	//
	// 它适用于所有必需的延迟指标，因为单个请求不能将两个指标约束在两个
	// 不同的分位数上。
	LatencyQuantile float64
}

// Resolution 表示已解析的容量先验结果。
type Resolution struct {
	// Shape 是该数值来源的形态。
	Shape string

	// WorkloadRevision 是运行该测量时的工作负载修订版本。
	WorkloadRevision string

	// GPUsPerReplica 是该测量中单个扩缩容副本所使用的 GPU 数量。
	GPUsPerReplica int

	// MeasuredReplicas 是该测量运行所使用的副本数量。
	MeasuredReplicas int

	// SeedTokensPerSecond 是单个副本的实测容量。
	SeedTokensPerSecond float64

	// EffectiveTokensPerSecond 是实际使用的容量值。
	EffectiveTokensPerSecond float64

	// Basis 记录选用该数值的原因。
	Basis Basis
}

// Basis 记录选用某个容量先验的原因。
//
// 它的存在是为了让状态字段能够说明为何选用或拒绝某个数值。
type Basis struct {
	// LatencyBounds 描述所选种子点上的直接实测边界。
	LatencyBounds []LatencyBound

	// SourceID 指定导入该 profile 的来源。
	SourceID string

	// SourceRevision 是该来源的固定修订版本。
	SourceRevision string

	// ProfileDigest 是该数值来源 profile 的摘要。
	ProfileDigest string

	// Metric 是读取容量时使用的吞吐量分子指标。
	Metric string

	// TokenCountConvention 说明分子统计了哪些 Token。
	TokenCountConvention string

	// Quantile 是读取约束时使用的延迟分位数。
	//
	// 它是数据源实际测量的分位数，而非请求所要求的目标分位数。数据源测量的
	// 更高分位数可从上界约束所请求的分位数。
	Quantile string

	// RequestedQuantile 是请求要求满足的延迟分位数。
	//
	// 它是调用方请求的分位数，而非数据源实测的分位数，以便状态字段能够区分
	// 请求了哪个分位数以及实际测量了哪个分位数。
	RequestedQuantile float64

	// Denominator 说明实测速率的统计分母。
	Denominator string

	// WorkloadID 是进行该测量时所处的工作负载包络。
	WorkloadID string

	// Headroom 是所应用的裕量。
	Headroom float64

	// Conservative 表示使用了同一指标的更高分位数作为上界。
	Conservative bool

	// FeasiblePoints 是满足约束的实测负载点数量。
	FeasiblePoints int

	// Boundary 说明容量边界是如何确定的。
	Boundary string
}

// 确定容量边界的方式。
const (
	// BoundaryHigherLoadFailure 表示高于可行边界的负载点未能满足延迟约束。
	BoundaryHigherLoadFailure = "HigherLoadSLOFailure"

	// BoundaryExplicitSaturation 表示某个可行负载点记录了显式的饱和测量值。
	BoundaryExplicitSaturation = "ExplicitSaturation"
)

// LatencyBound 记录用于检查所选种子点上单个延迟指标的统计量。
type LatencyBound struct {
	// Metric 指定被测量的延迟指标。
	Metric LatencyMetric
	// Quantile 是用作延迟边界的直接实测分位数。
	Quantile float64
	// Conservative 在使用更高分位数约束所需分位数时为 true。
	Conservative bool
}

// roleRequirement 表示某个角色对 profile 的需求。
type roleRequirement struct {
	// metric 是角色用于除以其需求量的吞吐量分子指标。
	metric ThroughputMetric

	// convention 是分子必须采用的 Token 计数约定。
	convention TokenCountConvention

	// latencies 是该角色必须保持在目标值以下的延迟指标。
	latencies []LatencyMetric
}

// anyMeasuredQuantile 是证据检查所要求的分位数。
//
// 之所以存在，是因为证据检查不得依赖于某次请求恰好指定的分位数。只要
// profile 在任意分位数上测量了延迟即视为具备证据，而需要更高分位数的请求
// 会自行判断该测量结果是否达到要求。
const anyMeasuredQuantile = 0.0

// FormatLatencyQuantile 格式化延迟分位数，使其在反序列化读回后保持同一数值。
//
// 它保留数值携带的每一位数字，因为若状态将 0.00001 写为 0.0000 则报告了
// 无人请求的分位数，将 0.97505 写为 0.9750 则报告了另一个分位数。
func FormatLatencyQuantile(quantile float64) string {
	return strconv.FormatFloat(quantile, 'f', -1, 64)
}

// constraintOutcome 表示实测延迟对目标约束的判定结果。
type constraintOutcome int

const (
	// constraintUndecided 表示该测量结果无法判定目标是否满足。
	//
	// 当更高的分位数未达到目标时即为此结果，因为高于所请求分位数的指标仅构成
	// 上界，无法说明所请求的分位数本身是否未达到目标。
	constraintUndecided constraintOutcome = iota

	// constraintMet 表示测量结果证明目标已满足。
	constraintMet

	// constraintMissed 表示测量结果证明目标未满足。
	//
	// 它要求必须是所请求的分位数本身，因为只有所请求的分位数
	// 才能证明该分位数未达到目标。
	constraintMissed
)

// requestedQuantile 返回请求所约束的延迟分位数。
//
// 它拒绝 (0,1] 范围之外的分位数（因为该范围外不表示任何分位数），
// 也拒绝 NaN 或无穷大（因为两者都不是有效分位数）。
func requestedQuantile(constraints Constraints) (float64, *RefusalError) {
	quantile := constraints.LatencyQuantile
	if quantile == 0 {
		return DefaultLatencyQuantile, nil
	}
	if math.IsNaN(quantile) || math.IsInf(quantile, 0) || quantile <= 0 || quantile > 1 {
		return 0, newRefusal(
			RefusalCapacityPriorUnresolved,
			"the latency quantile must be greater than zero and no greater than one",
			map[string]string{"latencyQuantile": fmt.Sprintf("%v", quantile)},
		)
	}
	return quantile, nil
}

// quantileIs 判断测得的分位数是否为所请求的分位数。
//
// 它对两个分位数进行精确比较，因为无论多么接近，高于请求分位数的
// 值都属于不同的分位数。测得的 0.9500000005 并不是所请求的 0.95，
// 测量它的数据源不能证明所请求的分位数未达到目标。
func quantileIs(measured, requested float64) bool {
	return measured == requested
}

// isConservative 判断测得的分位数是否从上方界定所请求的分位数。
//
// 对于所请求的分位数本身它返回 false，因为在请求分位数上的测量值
// 就是该分位数本身的测量结果，而非其上界。
func isConservative(measured, requested float64) bool {
	return measured > requested
}

// worseOutcome 返回两个结果中起决定作用的那一个。
//
// 它将已证明失败排在未定测量之上，并将未定测量排在已证明通过之上，
// 因为只要有一个指标确切未达到目标就足以判定该点，而无法判定的
// 指标不会推翻其他指标的结论。
func worseOutcome(current, candidate constraintOutcome) constraintOutcome {
	switch {
	case current == constraintMissed:
		return current
	case candidate == constraintMissed:
		return candidate
	case current == constraintUndecided:
		return current
	default:
		return candidate
	}
}

// requirementFor 返回某角色对 profile 的需求。
//
// 此处有意采用穷举方式，因为解析器未知的角色不能被赋予容量值。
func requirementFor(role RoleKind) (roleRequirement, bool) {
	switch role {
	case RoleKindServer:
		return roleRequirement{
			metric:     ThroughputMetricCacheMissedTotalTokensPerSecond,
			convention: TokenCountConventionCacheMissedInputPlusOutput,
			latencies:  []LatencyMetric{LatencyMetricTTFT, LatencyMetricITL},
		}, true
	case RoleKindPrefill:
		return roleRequirement{
			metric:     ThroughputMetricCacheMissedInputTokensPerSecond,
			convention: TokenCountConventionCacheMissedInput,
			latencies:  []LatencyMetric{LatencyMetricTTFT},
		}, true
	case RoleKindDecode:
		return roleRequirement{
			metric:     ThroughputMetricOutputTokensPerSecond,
			convention: TokenCountConventionOutput,
			latencies:  []LatencyMetric{LatencyMetricITL},
		}, true
	default:
		return roleRequirement{}, false
	}
}

// countsPromptTokens 判断该角色的分子是否计入 prompt token。
//
// 这对缓存约定很重要，因为计入 prompt token 的分子只能匹配
// prompt token 均为缓存未命中的测量结果。
func countsPromptTokens(role RoleKind) bool {
	return role == RoleKindServer || role == RoleKindPrefill
}

// loadAxisIncoherence 返回 profile 的负载点在所变化变量上的不一致之处，
// 若一致则返回空字符串。
//
// 并发数和请求速率是不可直接比较的数值，因此一个 profile 的所有点
// 必须只变化同一种负载类型、同一种客户端模型和同一种负载单位，且
// 客户端模型必须与工作负载声明的到达模型一致。混合了这些维度的 profile
// 不存在可供读取容量边界的负载水平。
func loadAxisIncoherence(profile Profile) string {
	if len(profile.Points) == 0 {
		return ""
	}
	first := profile.Points[0].Load
	for _, point := range profile.Points[1:] {
		load := point.Load
		if load.Kind != first.Kind {
			return fmt.Sprintf("the load points mix the %s and %s load kinds", first.Kind, load.Kind)
		}
		if load.ClientModel != first.ClientModel {
			return fmt.Sprintf("the load points mix the %s and %s client models", first.ClientModel, load.ClientModel)
		}
		if load.Unit != first.Unit {
			return fmt.Sprintf("the load points mix the %s and %s load units", first.Unit, load.Unit)
		}
	}
	if first.ClientModel != profile.Workload.ArrivalModel {
		return fmt.Sprintf(
			"the load points were offered with the %s client model while the workload declares %s",
			first.ClientModel, profile.Workload.ArrivalModel,
		)
	}
	return ""
}

// qualifiedPoints 返回在所请求分位数或更高分位数上直接测量了角色所需
// 全部延迟统计量的合格点。
//
// 它是能够承载容量的测量点的唯一定义，因为 CheckEligibility 和 Resolve
// 必须就哪些点计入负载覆盖率、可行集和边界达成一致。缺少某个统计量的点
// 仅作诊断用途：它保留在 profile 的原始证据中，但不计入任何结果。
// 对同一统计量测量了两次且值不同的点会作为冲突报告，由调用方直接拒绝
// 而非过滤忽略。
//
// 它接收证据必须达到的分位数。证据检查传入 anyMeasuredQuantile，因为
// 在任意分位数上测量过延迟的 profile 都携带了证据；而解析时则传入请求
// 所要求的分位数。
func qualifiedPoints(
	requirement roleRequirement,
	eligible []pointEvaluation,
	requested float64,
) ([]pointEvaluation, bool) {
	qualified := make([]pointEvaluation, 0, len(eligible))
	conflict := false
	for _, evaluation := range eligible {
		counts := true
		for _, metric := range requirement.latencies {
			_, _, found, conflicting := boundStatistic(evaluation.point, metric, requested)
			if conflicting {
				conflict = true
				counts = false
				break
			}
			if !found {
				counts = false
			}
		}
		if counts {
			qualified = append(qualified, evaluation)
		}
	}
	return qualified, conflict
}

// Resolve 返回请求的容量先验。
//
// 它采用拒绝而非猜测的策略，使无法使用结果的调用方能够了解
// 缺少了哪些证据。
func (c *Catalog) Resolve(request Request) (Resolution, error) {
	if c == nil {
		return Resolution{}, newRefusal(
			RefusalCapacityPriorUnresolved,
			"no catalog is loaded",
			nil,
		)
	}

	headroom := request.Constraints.Headroom
	if headroom == 0 {
		headroom = DefaultHeadroom
	}
	if headroom <= 0 || headroom > 1 {
		return Resolution{}, newRefusal(
			RefusalCapacityPriorUnresolved,
			"headroom must be greater than zero and no greater than one",
			map[string]string{"headroom": fmt.Sprintf("%v", headroom)},
		)
	}

	quantile, refusal := requestedQuantile(request.Constraints)
	if refusal != nil {
		return Resolution{}, refusal
	}

	profile, refusal := c.selectProfile(request)
	if refusal != nil {
		return Resolution{}, refusal
	}

	if mismatch := profileMismatches(profile, request); mismatch != "" {
		return Resolution{}, newRefusal(
			RefusalCapacityPriorUnresolved,
			fmt.Sprintf("profile %s does not match the deployment: %s", profile.Shape(), mismatch),
			map[string]string{"shape": profile.Shape().String(), "mismatch": mismatch},
		)
	}

	resolution, refusal := resolveProfile(c, profile, request, headroom, quantile)
	if refusal != nil {
		return Resolution{}, refusal
	}
	return resolution, nil
}

// Select 返回与 shape 匹配的 profile。
//
// 当没有匹配的 profile 或有多个 profile 匹配时它会拒绝返回，因为匹配到
// 两条测量结果的部署没有证据表明应适用哪一条。
func (c *Catalog) Select(shape Shape) (Profile, *RefusalError) {
	return c.selectProfile(Request{
		Role: shape.Role,
		Identity: Identity{
			Model:    ModelIdentity{Name: shape.Model, Quantization: shape.Quantization},
			Engine:   EngineIdentity{Image: shape.Engine},
			Hardware: HardwareIdentity{Accelerator: shape.Hardware},
		},
		Topology: shape.Topology,
	})
}

// selectProfile 查找请求可使用的唯一 profile。
//
// 它按六元组进行匹配，因为 shape 是 profile 的标识，且部署在说明
// catalog 中的其他信息之前会先声明其 shape。
//
// 当有多个 profile 匹配时它会拒绝返回，因为匹配到两条测量结果的部署
// 没有证据表明应适用哪一条。
func (c *Catalog) selectProfile(request Request) (Profile, *RefusalError) {
	want := request.Shape()
	var matched []Profile
	for _, profile := range c.Profiles {
		if shapeMatches(want, profile.Shape()) {
			matched = append(matched, profile)
		}
	}

	switch len(matched) {
	case 0:
		return Profile{}, newRefusal(
			RefusalCapacityPriorUnresolved,
			fmt.Sprintf("no catalog profile matches the deployment shape %s", want),
			map[string]string{"role": string(request.Role), "shape": want.String()},
		)
	case 1:
		return matched[0], nil
	default:
		shapes := make([]string, 0, len(matched))
		for _, profile := range matched {
			shapes = append(shapes, profile.Shape().String())
		}
		sort.Strings(shapes)
		return Profile{}, newRefusal(
			RefusalCapacityPriorUnresolved,
			"several catalog profiles match the deployment shape",
			map[string]string{"shapes": strings.Join(shapes, ",")},
		)
	}
}

// profileMismatches 返回 profile 与请求不一致的字段。
//
// 当两者一致时返回空字符串。
func profileMismatches(profile Profile, request Request) string {
	mismatched := make([]string, 0, 12)
	mismatched = append(mismatched, identityMismatch(profile.Identity, request.Identity)...)
	mismatched = append(mismatched, servingConfigMismatch(profile.ServingConfig, request.ServingConfig)...)
	mismatched = append(mismatched, topologyMismatch(profile.Topology.Normalized(), request.Topology.Normalized())...)
	return formatMismatch(mismatched)
}

// CheckEligibility 报告 profile 无法承载容量的原因。
//
// 它不依赖延迟目标，因为延迟目标属于具体部署而非测量结果。生成器
// 用它来确保 catalog 中不包含永远无法解析的 profile。
//
// 它也不依赖延迟分位数，因为分位数属于请求而非测量结果。在低于默认
// 分位数的位置测量过延迟的 profile 仍然携带了证据，而需要更高分位数
// 的请求会自行判断该测量是否达到要求。
//
// 它会报告发现的所有缺失项，因为缺少一项证据的 profile 通常还缺少
// 其他多项，调用方需要完整列表才能知道下一步应补充测量什么。
func CheckEligibility(profile Profile) *RefusalError {
	requirement, known := requirementFor(profile.Role)
	if !known {
		return newRefusal(
			RefusalCapacityPriorUnresolved,
			fmt.Sprintf("the %s role needs no capacity prior", profile.Role),
			map[string]string{"role": string(profile.Role), "shape": profile.Shape().String()},
		)
	}

	detail := map[string]string{"shape": profile.Shape().String()}
	var findings []string

	// 不要求环境信息完整，因为公开数据源很少会完整记录整个容器环境，
	// 数据源未观测到的环境信息不能说明未记录的变量情况。
	// 部署仍会与测量实际记录的环境信息进行匹配，因此记录中所包含的变量
	// 依然会被比对。

	if incoherent := loadAxisIncoherence(profile); incoherent != "" {
		return newRefusal(
			RefusalIncompatibleMetric,
			fmt.Sprintf("profile %s %s", profile.Shape(), incoherent),
			detail,
		)
	}

	for _, point := range profile.Points {
		if point.Throughput.Metric != requirement.metric {
			findings = append(findings, fmt.Sprintf("measures %s instead of %s", point.Throughput.Metric, requirement.metric))
			detail["numerator"] = string(point.Throughput.Metric)
			break
		}
	}
	if detail["numerator"] == "" {
		for _, point := range profile.Points {
			if point.Throughput.TokenCountConvention != requirement.convention {
				findings = append(findings, fmt.Sprintf("counts %s instead of %s", point.Throughput.TokenCountConvention, requirement.convention))
				detail["tokenCountConvention"] = string(point.Throughput.TokenCountConvention)
				break
			}
		}
	}

	eligible := eligiblePoints(profile)
	if len(eligible) == 0 {
		findings = append(findings, fmt.Sprintf("measures no load point with the %s numerator", requirement.metric))
		detail["eligiblePoints"] = "0"
	} else {
		qualified, conflict := qualifiedPoints(requirement, eligible, anyMeasuredQuantile)
		if conflict {
			findings = append(findings, "measures the same latency statistic twice with different values")
			detail["conflict"] = "true"
		}
		for _, metric := range requirement.latencies {
			if !measuresStatistic(eligible, metric, anyMeasuredQuantile) {
				findings = append(findings, fmt.Sprintf("does not measure the %s latency", metric))
				detail["missingMetric"] = string(metric)
				break
			}
		}
		if countsPromptTokens(profile.Role) && profile.Workload.CacheBehavior != CacheBehaviorAllMisses {
			findings = append(findings, "does not establish that its prompt tokens were cache misses")
			detail["cacheBehavior"] = string(profile.Workload.CacheBehavior)
		}
		levels := distinctLoadLevels(qualified)
		if len(levels) < 2 {
			findings = append(findings, "has fewer than two distinct qualified measured load levels")
			detail["loadLevels"] = fmt.Sprintf("%d", len(levels))
		}
	}

	if len(findings) == 0 {
		return nil
	}

	reason := RefusalIncompatibleMetric
	if detail["numerator"] == "" && detail["tokenCountConvention"] == "" &&
		detail["missingMetric"] == "" && detail["cacheBehavior"] == "" && detail["conflict"] == "" {
		reason = RefusalInsufficientLoadCoverage
	}
	return newRefusal(reason, fmt.Sprintf("profile %s %s", profile.Shape(), findings[0]), detail)
}

// pointEvaluation 表示解析器对单个测量点得出的评估结果。
type pointEvaluation struct {
	// point 是被评估的测量点。
	point MeasuredPoint

	// eligible 表示该点测量了角色所需的数据。
	eligible bool

	// perReplica 是为单个副本重建的该点吞吐量。
	perReplica float64

	// reason 说明该点不符合条件的原因。
	reason string
}

// eligiblePoints 返回 profile 中测量了角色所需数据的点。
func eligiblePoints(profile Profile) []pointEvaluation {
	evaluations := make([]pointEvaluation, 0, len(profile.Points))
	for _, point := range profile.Points {
		evaluation := evaluatePoint(profile, point)
		if evaluation.eligible {
			evaluations = append(evaluations, evaluation)
		}
	}
	return evaluations
}

// evaluatePoint 计算单个测量点是否能够承载容量。
//
// 此处不检查分子和 token 计数约定，因为它们必须在整个 profile 范围内
// 保持一致，且在读取各个点之前就已检查完毕。
func evaluatePoint(profile Profile, point MeasuredPoint) pointEvaluation {
	evaluation := pointEvaluation{point: point}

	if point.Throughput.Unit != UnitTokensPerSecond {
		evaluation.reason = fmt.Sprintf("measured %s instead of %s", point.Throughput.Unit, UnitTokensPerSecond)
		return evaluation
	}
	if !finitePositive(point.Throughput.Value) {
		evaluation.reason = "measured no finite positive throughput"
		return evaluation
	}
	if !finitePositive(point.Load.Value) {
		evaluation.reason = "measured no finite positive load"
		return evaluation
	}

	perReplica, ok := perReplicaTokensPerSecond(point.Throughput, profile)
	if !ok {
		evaluation.reason = fmt.Sprintf("cannot reconstruct one replica from a %s denominator", point.Throughput.Denominator)
		return evaluation
	}

	evaluation.perReplica = perReplica
	evaluation.eligible = true
	return evaluation
}

// resolveProfile 从单个 profile 推导出容量。
func resolveProfile(
	catalog *Catalog,
	profile Profile,
	request Request,
	headroom float64,
	requested float64,
) (Resolution, *RefusalError) {
	requirement, _ := requirementFor(profile.Role)

	if refusal := CheckEligibility(profile); refusal != nil {
		return Resolution{}, refusal
	}

	targets, refusal := latencyTargets(request, requirement)
	if refusal != nil {
		return Resolution{}, refusal
	}

	eligible := eligiblePoints(profile)

	qualified, conflict := qualifiedPoints(requirement, eligible, requested)
	if conflict {
		return Resolution{}, newRefusal(
			RefusalIncompatibleMetric,
			fmt.Sprintf("profile %s measured the same latency statistic twice with different values", profile.Shape()),
			map[string]string{"shape": profile.Shape().String()},
		)
	}

	for _, metric := range requirement.latencies {
		if !measuresStatistic(eligible, metric, requested) {
			return Resolution{}, newRefusal(
				RefusalIncompatibleMetric,
				fmt.Sprintf("profile %s does not measure the %s latency at the requested quantile", profile.Shape(), metric),
				map[string]string{
					"shape":             profile.Shape().String(),
					"missingMetric":     string(metric),
					"requestedQuantile": fmt.Sprintf("%g", requested),
				},
			)
		}
	}

	levels := distinctLoadLevels(qualified)
	if len(levels) < 2 {
		return Resolution{}, newRefusal(
			RefusalInsufficientLoadCoverage,
			fmt.Sprintf("profile %s has fewer than two distinct qualified measured load levels", profile.Shape()),
			map[string]string{
				"shape":             profile.Shape().String(),
				"loadLevels":        fmt.Sprintf("%d", len(levels)),
				"requestedQuantile": fmt.Sprintf("%g", requested),
			},
		)
	}

	feasible := make([]pointEvaluation, 0, len(qualified))
	failed := make([]pointEvaluation, 0, len(qualified))
	for _, evaluation := range qualified {
		outcome, _, _ := satisfiesConstraints(evaluation.point, requirement, targets, requested)
		switch outcome {
		case constraintMet:
			feasible = append(feasible, evaluation)
		case constraintMissed:
			failed = append(failed, evaluation)
		}
	}

	if len(feasible) == 0 {
		if len(failed) == 0 {
			// 没有任何点可被判定，因为所有达到所请求分位数的测量结果
			// 都来自更高分位数且未达到目标。
			return Resolution{}, newRefusal(
				RefusalCapacityPriorUnresolved,
				fmt.Sprintf("profile %s cannot decide the latency targets at the requested quantile", profile.Shape()),
				map[string]string{
					"shape":             profile.Shape().String(),
					"requestedQuantile": fmt.Sprintf("%g", requested),
				},
			)
		}
		return Resolution{}, newRefusal(
			RefusalCapacityPriorUnresolved,
			fmt.Sprintf("profile %s has no measured load point that satisfies the latency targets", profile.Shape()),
			map[string]string{
				"shape":             profile.Shape().String(),
				"requestedQuantile": fmt.Sprintf("%g", requested),
			},
		)
	}

	boundary, found := capacityBoundary(feasible, failed)
	if !found {
		return Resolution{}, newRefusal(
			RefusalInsufficientLoadCoverage,
			fmt.Sprintf("profile %s has no evidence of the capacity boundary", profile.Shape()),
			map[string]string{"shape": profile.Shape().String()},
		)
	}

	seed := 0.0
	seedPoint := MeasuredPoint{}
	for _, evaluation := range feasible {
		if evaluation.perReplica > seed {
			seed = evaluation.perReplica
			seedPoint = evaluation.point
		}
	}

	basis := Basis{
		Metric:               string(seedPoint.Throughput.Metric),
		TokenCountConvention: string(seedPoint.Throughput.TokenCountConvention),
		Denominator:          string(seedPoint.Throughput.Denominator),
		WorkloadID:           profile.Workload.ID,
		Headroom:             headroom,
		FeasiblePoints:       len(feasible),
		Boundary:             boundary,
		ProfileDigest:        profile.Digest,
		RequestedQuantile:    requested,
	}
	highestQuantile := 0.0
	for _, metric := range requirement.latencies {
		_, quantile, _, _ := boundStatistic(seedPoint, metric, requested)
		highestQuantile = max(highestQuantile, quantile)
		basis.LatencyBounds = append(basis.LatencyBounds, LatencyBound{
			Metric: metric, Quantile: quantile, Conservative: isConservative(quantile, requested),
		})
	}
	if highestQuantile > 0 {
		basis.Quantile = FormatLatencyQuantile(highestQuantile)
		basis.Conservative = isConservative(highestQuantile, requested)
	}
	for _, source := range catalog.Sources {
		if source.ID == profile.SourceID {
			basis.SourceID = source.ID
			basis.SourceRevision = source.Revision
			break
		}
	}

	return Resolution{
		Shape:                    profile.Shape().String(),
		WorkloadRevision:         profile.Workload.Revision,
		GPUsPerReplica:           profile.Topology.GPUsPerReplica,
		MeasuredReplicas:         profile.MeasuredReplicas,
		SeedTokensPerSecond:      seed,
		EffectiveTokensPerSecond: headroom * seed,
		Basis:                    basis,
	}, nil
}

// latencyTargets 返回每个必需延迟指标所受约束的目标值。
//
// 缺少目标时它会拒绝解析，因为依赖 catalog 容量的角色必须声明
// 需要控制在其下的延迟上限。
//
// 每个必需指标都遵循相同的分位数，因为一次请求不能将两个指标分别
// 约束到两个不同的分位数，且分位数由调用方统一选定一次。
func latencyTargets(request Request, requirement roleRequirement) (map[LatencyMetric]float64, *RefusalError) {
	targets := map[LatencyMetric]float64{}
	for _, metric := range requirement.latencies {
		target := 0.0
		switch metric {
		case LatencyMetricTTFT:
			target = request.Constraints.TTFTTargetMilliseconds
		case LatencyMetricITL:
			target = request.Constraints.ITLTargetMilliseconds
		}
		if target <= 0 {
			return nil, newRefusal(
				RefusalCapacityPriorUnresolved,
				fmt.Sprintf("a role that relies on a catalog capacity requires the %s latency target", metric),
				map[string]string{"missingTarget": string(metric)},
			)
		}
		targets[metric] = target
	}
	return targets, nil
}

// perReplicaTokensPerSecond 为单个副本重建测得的速率。
//
// 重建方式由分母决定，因为数据源已经声明了统计口径，解析器
// 不得自行选择乘数。
func perReplicaTokensPerSecond(throughput Throughput, profile Profile) (float64, bool) {
	switch throughput.Denominator {
	case DenominatorPerGPU:
		if profile.Topology.GPUsPerReplica <= 0 {
			return 0, false
		}
		return throughput.Value * float64(profile.Topology.GPUsPerReplica), true
	case DenominatorPerReplica:
		return throughput.Value, true
	case DenominatorWholeSystem:
		if profile.MeasuredReplicas <= 0 {
			return 0, false
		}
		return throughput.Value / float64(profile.MeasuredReplicas), true
	default:
		return 0, false
	}
}

// satisfiesConstraints 对照延迟目标检查合格点。
//
// 它只处理合格点，因为资格筛选已经确保每个必需统计量都在所请求
// 分位数或更高分位数上被直接测量且无冲突。不满足条件的点即为不可行，
// 而非缺少测量数据。
//
// 它区分已证明的失败和未定的测量。在所请求分位数上未达标的测量结果
// 可证明失败；而在更高分位数上未达标的测量结果则不能，因为更高的
// 分位数只是所请求分位数的上界，无法说明所请求分位数是否未达标。
//
// 此处不读取点中记录的 SLO 结果，因为它既未记录判定所依据的分位数，
// 也未记录判定所对照的目标值；一个未写明自身判定条件的结果不能用于
// 裁定不同的目标。
func satisfiesConstraints(
	point MeasuredPoint,
	requirement roleRequirement,
	targets map[LatencyMetric]float64,
	requested float64,
) (outcome constraintOutcome, bound float64, conservative bool) {
	outcome = constraintMet
	for _, metric := range requirement.latencies {
		value, quantile, found, conflicting := boundStatistic(point, metric, requested)
		if !found || conflicting {
			// 对合格点而言此分支不可达；保留它是为了在日后资格筛选逻辑变更时
			// 保持循环的严谨性。
			outcome = worseOutcome(outcome, constraintUndecided)
			continue
		}
		if quantile > bound {
			bound = quantile
		}
		if isConservative(quantile, requested) {
			conservative = true
		}
		if value <= targets[metric] {
			continue
		}
		if quantileIs(quantile, requested) {
			// 所请求的分位数本身未达到目标，因此该测量结果证明失败。
			outcome = worseOutcome(outcome, constraintMissed)
			continue
		}
		// 更高的分位数未达到目标，但这并不能证明所请求的分位数未达到目标。
		outcome = worseOutcome(outcome, constraintUndecided)
	}
	return outcome, bound, conservative
}

// boundStatistic 返回在所请求分位数处界定某延迟指标的值。
//
// 当数据源直接测量了所请求的分位数时，它会选用该测量值，因为这就是
// 实际测量结果而非其上界。
//
// 当数据源未直接测量所请求的分位数，但分别在其下方和上方各测得一个
// 分位数时，它会在两者之间插值，因为发布多点分位曲线的数据源同样
// 隐含了区间内的数值。插值结果按所请求分位数上报，从而能在满足和
// 失败两个方向上作出判定，避免测量结果处于未定状态。
//
// 当下方没有测量值时，它会选择高于所请求分位数的最小已测分位数，
// 因为这是数据源测得的最紧上界。
//
// 当数据源在同一分位数上对同一统计量测得两个不同的值时，它会报告
// 冲突，因为自相矛盾的数据源无法判定约束。
//
// 当低分位数与高分位数的数值出现倒挂时，它会拒绝插值，因为曲线在
// 两个自有测量点之间下降的数据源无法给出其中间的数值。
func boundStatistic(
	point MeasuredPoint,
	metric LatencyMetric,
	requested float64,
) (value, quantile float64, found, conflict bool) {
	for i, latency := range point.Latencies {
		if latency.Metric != metric {
			continue
		}
		for _, other := range point.Latencies[i+1:] {
			if other.Metric != metric {
				continue
			}
			if quantileIs(other.Quantile, latency.Quantile) && other.Value != latency.Value {
				return 0, 0, false, true
			}
		}
	}
	for _, latency := range point.Latencies {
		if latency.Metric != metric {
			continue
		}
		if quantileIs(latency.Quantile, requested) {
			return latency.Value, latency.Quantile, true, false
		}
	}
	lower, higher := bracketingQuantiles(point, metric, requested)
	if lower != nil && higher != nil && lower.Value <= higher.Value {
		interpolated := lower.Value + (requested-lower.Quantile)/(higher.Quantile-lower.Quantile)*(higher.Value-lower.Value)
		return interpolated, requested, true, false
	}
	for _, latency := range point.Latencies {
		if latency.Metric != metric {
			continue
		}
		if latency.Quantile <= 0 || latency.Quantile < requested {
			continue
		}
		if !found || latency.Quantile < quantile {
			found = true
			value = latency.Value
			quantile = latency.Quantile
		}
	}
	return value, quantile, found, false
}

// bracketingQuantiles 返回从上下两侧夹逼所请求分位数的测量值。
//
// 下界测量值是低于所请求分位数中的最大者，上界测量值是高于所请求
// 分位数中的最小者，因为所请求分位数正位于这两者之间。
//
// 若未能双侧包夹（缺少任一侧的测量），则不返回有效结果，因为单侧的
// 测量仅能从上方界定所请求的分位数，而无法从两侧夹逼。
func bracketingQuantiles(
	point MeasuredPoint, metric LatencyMetric, requested float64,
) (lower, higher *Latency) {
	for i := range point.Latencies {
		latency := &point.Latencies[i]
		if latency.Metric != metric || latency.Quantile <= 0 {
			continue
		}
		switch {
		case latency.Quantile < requested:
			if lower == nil || latency.Quantile > lower.Quantile {
				lower = latency
			}
		case latency.Quantile > requested:
			if higher == nil || latency.Quantile < higher.Quantile {
				higher = latency
			}
		}
	}
	return lower, higher
}

// measuresStatistic 判断是否存在任何点在所请求分位数或更高分位数上
// 测量了某个延迟指标。
//
// 证据检查向其传入 anyMeasuredQuantile，因为 profile 所携带的证据
// 不得依赖于某次请求恰好要求的分位数。
func measuresStatistic(eligible []pointEvaluation, metric LatencyMetric, requested float64) bool {
	for _, evaluation := range eligible {
		if _, _, found, conflict := boundStatistic(evaluation.point, metric, requested); found && !conflict {
			return true
		}
	}
	return false
}

// capacityBoundary 报告容量边界是如何确定的。
//
// 它接受高于可行边界且确切未满足约束的负载点，或记录了显式饱和
// 测量值的可行点。无法判定的点不能确立边界，因为未定测量结果
// 并不代表失败。
//
// 它只读取合格点，因为诊断点既不能算作可行负载水平，也不能算作
// 失败负载水平。
func capacityBoundary(feasible, failed []pointEvaluation) (string, bool) {
	highestFeasible := 0.0
	for _, evaluation := range feasible {
		if evaluation.point.Load.Value > highestFeasible {
			highestFeasible = evaluation.point.Load.Value
		}
	}

	for _, evaluation := range failed {
		if evaluation.point.Load.Value <= highestFeasible {
			continue
		}
		return BoundaryHigherLoadFailure, true
	}

	for _, evaluation := range feasible {
		if evaluation.point.Saturation != nil && evaluation.point.Saturation.Measured {
			return BoundaryExplicitSaturation, true
		}
	}
	return "", false
}

// distinctLoadLevels 返回一组点所包含的已测负载水平。
func distinctLoadLevels(eligible []pointEvaluation) []string {
	levels := map[string]struct{}{}
	for _, evaluation := range eligible {
		levels[loadLevelKey(evaluation.point.Load)] = struct{}{}
	}
	keys := make([]string, 0, len(levels))
	for key := range levels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// loadLevelKey 生成已测负载水平的标识键。
func loadLevelKey(load LoadLevel) string {
	return fmt.Sprintf("%s/%g", load.Kind, load.Value)
}

// finitePositive 判断一个值是否为大于零的有限数。
func finitePositive(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0) && value > 0
}
