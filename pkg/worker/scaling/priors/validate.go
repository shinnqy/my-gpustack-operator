package priors

import (
	"fmt"
	"math"
	"strings"
)

// ParseCatalog 读取并校验目录。
//
// 解析过程采用严格模式，因为读取方无法识别的字段就无法进行校验；
// 同时限制了大小，因为从 ConfigMap 读取的目录绝不能使控制器的内存无限增长。
//
// 该函数解析 YAML，因为 YAML 是 JSON 的超集，一个读取器即可同时支持两种格式。
// 它读取的是扁平化的目录文档，返回的则是供解析器使用的目录结构。
func ParseCatalog(data []byte) (*Catalog, error) {
	if len(data) > MaxCatalogBytes {
		return nil, fmt.Errorf("catalog is %d bytes, which is over the %d byte limit", len(data), MaxCatalogBytes)
	}

	document, err := ParseCatalogDocument(data)
	if err != nil {
		return nil, err
	}
	catalog, err := toCatalog(document)
	if err != nil {
		return nil, err
	}
	if err := Validate(*catalog); err != nil {
		return nil, err
	}
	return catalog, nil
}

// field 表示校验过程所检查的命名值。
type field struct {
	name  string
	value string
}

// requireNonEmpty 返回包含第一个空字段名称的 error。
//
// 字段按传入顺序依次检查，从而确保每次运行报错时都指向同一个字段。
func requireNonEmpty(fields ...field) error {
	for _, entry := range fields {
		if strings.TrimSpace(entry.value) == "" {
			return fmt.Errorf("%s is empty", entry.name)
		}
	}
	return nil
}

// Validate 校验目录。
//
// 该函数会拒绝重复的 shape、不受支持的 schema 版本以及无效的测量数据，
// 因为包含上述任意问题的目录都无法确定哪条测量数据可用。
func Validate(catalog Catalog) error {
	if catalog.SchemaVersion != SchemaVersion {
		return fmt.Errorf("catalog schema version %q is not supported", catalog.SchemaVersion)
	}
	if len(catalog.Profiles) > MaxProfiles {
		return fmt.Errorf("catalog holds %d profiles, which is over the %d profile limit", len(catalog.Profiles), MaxProfiles)
	}

	sourceIDs := map[string]struct{}{}
	for _, source := range catalog.Sources {
		if _, seen := sourceIDs[source.ID]; seen {
			return fmt.Errorf("source %q appears more than once", source.ID)
		}
		sourceIDs[source.ID] = struct{}{}

		if err := requireNonEmpty(
			field{"source id", source.ID},
			field{"source url", source.URL},
			field{"source revision", source.Revision},
			field{"source license", source.License},
			field{"source adapter version", source.AdapterVersion},
		); err != nil {
			return err
		}
		switch source.Outcome {
		case SourceOutcomeAccepted:
			if source.RefusalError != nil {
				return fmt.Errorf("source %q was accepted but carries a refusal", source.ID)
			}
		case SourceOutcomeRefused:
			if source.RefusalError == nil {
				return fmt.Errorf("source %q was refused but carries no reason", source.ID)
			}
		default:
			return fmt.Errorf("source %q carries the unknown outcome %q", source.ID, source.Outcome)
		}
		for _, artifact := range source.Artifacts {
			if err := requireNonEmpty(
				field{"artifact path", artifact.Path},
				field{"artifact digest", artifact.SHA256},
			); err != nil {
				return fmt.Errorf("source %q: %w", source.ID, err)
			}
		}
	}

	shapes := map[string]int{}
	for index, profile := range catalog.Profiles {
		shape := profile.Shape()
		if first, seen := shapes[shape.String()]; seen {
			return fmt.Errorf(
				"profiles[%d] and profiles[%d] normalize to the same shape %s",
				first, index, shape,
			)
		}
		shapes[shape.String()] = index

		if profile.SourceID != "" {
			if _, known := sourceIDs[profile.SourceID]; !known {
				return fmt.Errorf("profile %s names the unknown source %q", shape, profile.SourceID)
			}
		}
		if err := validateProfile(profile); err != nil {
			return fmt.Errorf("profile %s: %w", shape, err)
		}
	}

	digest, err := CatalogDigest(catalog)
	if err != nil {
		return fmt.Errorf("compute catalog digest: %w", err)
	}
	if catalog.Digest != digest {
		return fmt.Errorf("catalog digest %q does not match the content digest %q", catalog.Digest, digest)
	}
	return nil
}

// validateProfile 校验单个 profile。
//
// 该函数要求必须包含标识信息与拓扑信息，因为未标明所测部署的 profile
// 无法确定哪条测量数据可用。
//
// 该函数不强制要求推理服务配置，因为目录记录的是测量数据而非配置：
// 未声明服务配置的 profile 不施加任何约束，而声明了某项配置的 profile
// 则仅针对该项进行比较。
func validateProfile(profile Profile) error {
	if _, known := requirementFor(profile.Role); !known {
		return fmt.Errorf("carries the unknown role %q", profile.Role)
	}
	if err := requireNonEmpty(
		field{"model name", profile.Identity.Model.Name},
		field{"model revision", profile.Identity.Model.Revision},
		field{"model tokenizer", profile.Identity.Model.Tokenizer},
		field{"model quantization", profile.Identity.Model.Quantization},
		field{"engine image", profile.Identity.Engine.Image},
		field{"hardware accelerator", profile.Identity.Hardware.Accelerator},
		field{"hardware form factor", profile.Identity.Hardware.FormFactor},
		field{"workload id", profile.Workload.ID},
		field{"workload revision", profile.Workload.Revision},
		field{"workload modality", profile.Workload.Modality},
	); err != nil {
		return err
	}
	if profile.Identity.Hardware.MemoryGB <= 0 {
		return fmt.Errorf("hardware memory %d is not positive", profile.Identity.Hardware.MemoryGB)
	}
	if profile.Topology.GPUsPerReplica <= 0 {
		return fmt.Errorf("gpus per replica %d is not positive", profile.Topology.GPUsPerReplica)
	}
	if profile.MeasuredReplicas <= 0 {
		return fmt.Errorf("measured replicas %d is not positive", profile.MeasuredReplicas)
	}
	// 张量并行、流水线并行、数据并行和本地数据并行宽度为必填项，
	// 因为扩缩容副本由这些宽度共同定义，必须防止双方都省略该字段时因宽度为 0 而误匹配。
	//
	// 上下文并行和专家并行宽度允许为 0，因为未使用它们的被测系统会以宽度 0 来明确表示。
	for _, entry := range []struct {
		name  string
		width int
	}{
		{"tensor parallel", profile.Topology.TensorParallel},
		{"pipeline parallel", profile.Topology.PipelineParallel},
		{"data parallel", profile.Topology.DataParallel},
		{"local data parallel", profile.Topology.LocalDataParallel},
	} {
		if entry.width <= 0 {
			return fmt.Errorf("%s width %d is not positive", entry.name, entry.width)
		}
	}
	for _, entry := range []struct {
		name  string
		width int
	}{
		{"context parallel", profile.Topology.ContextParallel},
		{"expert parallel", profile.Topology.ExpertParallel},
	} {
		if entry.width < 0 {
			return fmt.Errorf("%s width %d is negative", entry.name, entry.width)
		}
	}
	switch profile.Workload.CacheBehavior {
	case CacheBehaviorAllMisses, CacheBehaviorIncludesHits, CacheBehaviorUnknown:
	default:
		return fmt.Errorf("workload cache behavior %q is not known", profile.Workload.CacheBehavior)
	}
	if err := CheckEffectiveLaunchParameters(
		profile.ServingConfig.LaunchParameters,
		profile.ServingConfig.EffectiveLaunchParameters,
	); err != nil {
		return fmt.Errorf("the effective launch parameters do not describe the launch: %w", err)
	}
	switch profile.Workload.ArrivalModel {
	case ClientModelFixedConcurrency, ClientModelOpenLoop:
	default:
		return fmt.Errorf("workload arrival model %q is not known", profile.Workload.ArrivalModel)
	}
	if len(profile.Points) == 0 {
		return fmt.Errorf("holds no measured load point")
	}
	if len(profile.Points) > MaxPointsPerProfile {
		return fmt.Errorf("holds %d measured load points, which is over the %d point limit", len(profile.Points), MaxPointsPerProfile)
	}

	for _, point := range profile.Points {
		if err := validatePoint(point); err != nil {
			return err
		}
	}

	if incoherent := loadAxisIncoherence(profile); incoherent != "" {
		return fmt.Errorf("load axis is incoherent: %s", incoherent)
	}

	// 摘要不会自动派生，因为目录由人工编写，读取方需要能够据此判断
	// 当前内容是否与实际测量时的内容一致。
	digest, err := ProfileDigest(profile)
	if err != nil {
		return fmt.Errorf("compute profile digest: %w", err)
	}
	if profile.Digest != digest {
		return fmt.Errorf("digest %q does not match the content digest %q", profile.Digest, digest)
	}
	return nil
}

// validatePoint 校验单个实测负载点。
func validatePoint(point MeasuredPoint) error {
	switch point.Load.Kind {
	case LoadKindConcurrency, LoadKindRequestRate:
	default:
		return fmt.Errorf("load kind %q is not known", point.Load.Kind)
	}
	switch point.Load.ClientModel {
	case ClientModelFixedConcurrency, ClientModelOpenLoop:
	default:
		return fmt.Errorf("load client model %q is not known", point.Load.ClientModel)
	}
	if err := requireNonEmpty(field{"load unit", point.Load.Unit}); err != nil {
		return err
	}
	if !isFinite(point.Load.Value) || point.Load.Value <= 0 {
		return fmt.Errorf("load value %v is not a finite positive number", point.Load.Value)
	}

	switch point.Throughput.Metric {
	case ThroughputMetricCacheMissedInputTokensPerSecond,
		ThroughputMetricOutputTokensPerSecond,
		ThroughputMetricCacheMissedTotalTokensPerSecond,
		ThroughputMetricInputTokensPerSecondIncludingCacheHits,
		ThroughputMetricTotalTokensPerSecond:
	default:
		return fmt.Errorf("throughput metric %q is not known", point.Throughput.Metric)
	}
	switch point.Throughput.TokenCountConvention {
	case TokenCountConventionCacheMissedInput,
		TokenCountConventionOutput,
		TokenCountConventionCacheMissedInputPlusOutput,
		TokenCountConventionInputIncludingCacheHits,
		TokenCountConventionInputPlusOutput:
	default:
		return fmt.Errorf("token count convention %q is not known", point.Throughput.TokenCountConvention)
	}
	switch point.Throughput.Denominator {
	case DenominatorPerGPU, DenominatorPerReplica, DenominatorWholeSystem:
	default:
		return fmt.Errorf("throughput denominator %q is not known", point.Throughput.Denominator)
	}
	if err := requireNonEmpty(
		field{"throughput unit", point.Throughput.Unit},
		field{"token count convention", string(point.Throughput.TokenCountConvention)},
	); err != nil {
		return err
	}
	if !isFinite(point.Throughput.Value) || point.Throughput.Value <= 0 {
		return fmt.Errorf("throughput value %v is not a finite positive number", point.Throughput.Value)
	}

	if len(point.Latencies) == 0 {
		return fmt.Errorf("holds no latency statistic")
	}
	for _, latency := range point.Latencies {
		switch latency.Metric {
		case LatencyMetricTTFT, LatencyMetricITL, LatencyMetricTPOT:
		default:
			return fmt.Errorf("latency metric %q is not known", latency.Metric)
		}
		if latency.Unit != UnitMilliseconds {
			return fmt.Errorf("latency unit %q is not %q", latency.Unit, UnitMilliseconds)
		}
		if !isFinite(latency.Quantile) || latency.Quantile <= 0 || latency.Quantile > 1 {
			return fmt.Errorf("latency quantile %v is not within (0,1]", latency.Quantile)
		}
		if !isFinite(latency.Value) || latency.Value < 0 {
			return fmt.Errorf("latency value %v is not a finite number", latency.Value)
		}
		if latency.SampleCount < 0 {
			return fmt.Errorf("latency sample count %d is negative", latency.SampleCount)
		}
	}

	if point.Requests.Successful < 0 || point.Requests.Failed < 0 {
		return fmt.Errorf("request counts are negative")
	}
	if !isFinite(point.Duration.WarmUpSeconds) || point.Duration.WarmUpSeconds < 0 {
		return fmt.Errorf("warm-up duration %v is not a finite number", point.Duration.WarmUpSeconds)
	}
	if !isFinite(point.Duration.MeasuredSeconds) || point.Duration.MeasuredSeconds < 0 {
		return fmt.Errorf("measured duration %v is not a finite number", point.Duration.MeasuredSeconds)
	}
	return nil
}

// isFinite 判断数值是否既非无穷大也非 NaN。
func isFinite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}
