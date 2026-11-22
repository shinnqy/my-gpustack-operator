package priors

import (
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"

	"sigs.k8s.io/yaml"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

// CatalogDocument 表示按原始书写格式呈现的目录文档。
//
// 它采用扁平文档结构，因为目录在由机器解析前首先供人工阅读，
// 当每个 profile 将模型、引擎和硬件各自独立成行时，人工阅读效率更高。
//
// 它不包含摘要和来源信息，因为对包含摘要的文档再计算摘要会导致嵌套摘要，
// 而读取方无法校验的来源信息只能被动信任。目录的摘要在加载时计算，
// 测量数据的来源即为读取该数据的文件名。
type CatalogDocument = workercore.ModelDeploymentScalerCapacityPriorCatalog

// ProfileDocument 表示按原始书写格式呈现的单个 profile。
type ProfileDocument = workercore.ModelDeploymentScalerCapacityPriorProfile

// TopologyDocument 表示按原始书写格式呈现的单个扩缩容副本拓扑形态。
//
// 它显式列出测量结果所声明的并行宽度，因为数据源未声明的宽度默认为 1，
// 而读取方必须明确哪些宽度经过了实际测量。
type TopologyDocument = workercore.ModelDeploymentScalerCapacityPriorTopology

// PointDocument 表示按原始书写格式呈现的单个实测负载点。
//
// 它显式列出测量产出的三项速率，因为读取方通常会先关注 prompt token、
// 生成 token 及总吞吐，再关注其中任意单一指标。
type PointDocument = workercore.ModelDeploymentScalerCapacityPriorPoint

// LatencyQuantiles 保存单个负载点的实测延迟分位数，键名为 p25、p50、p70、
// p90、p95 或 p99 等百分位名称。
//
// 数据源未公布的分位数保持缺省，因为零延迟属于有效测量值，而缺失值不是。
// 缺失分位数的语义由读取方自行决定。
type LatencyQuantiles = map[string]float64

// ParseCatalogDocument 读取并解析目录文档。
//
// 该解析过程采用严格模式，因为读取方不识别的字段无法进行校验；同时它按
// YAML 解析，因为 YAML 是 JSON 的超集，单一读取器即可同时支持两种格式。
func ParseCatalogDocument(data []byte) (CatalogDocument, error) {
	var document CatalogDocument
	if err := yaml.UnmarshalStrict(data, &document); err != nil {
		return CatalogDocument{}, fmt.Errorf("decode catalog: %w", err)
	}
	return document, nil
}

// toCatalog 将文档转换为供解析器读取的目录结构。
func toCatalog(d CatalogDocument) (*Catalog, error) {
	if d.APIVersion != CatalogAPIVersion {
		return nil, fmt.Errorf("catalog apiVersion %q is not supported, want %q", d.APIVersion, CatalogAPIVersion)
	}
	if d.Kind != CatalogKind {
		return nil, fmt.Errorf("catalog kind %q is not supported, want %q", d.Kind, CatalogKind)
	}
	catalog := Catalog{SchemaVersion: d.APIVersion}
	for _, profile := range d.Profiles {
		converted, err := toProfile(profile)
		if err != nil {
			return nil, fmt.Errorf("profile %s: %w", DocumentShape(profile), err)
		}
		catalog.Profiles = append(catalog.Profiles, converted)
	}
	digest, err := CatalogDigest(catalog)
	if err != nil {
		return nil, fmt.Errorf("compute catalog digest: %w", err)
	}
	catalog.Digest = digest
	return &catalog, nil
}

// toProfile 将单个书写格式的 profile 转换为供解析器读取的 profile。
//
// 它会补全文档中未声明的字段，因为文档记录的是测量结果而非配置：
// 测量结果仅说明副本的运行速度，而不包含其所用的推理服务配置。
func toProfile(d ProfileDocument) (Profile, error) {
	role := RoleKind(d.Role)
	if _, known := requirementFor(role); !known {
		return Profile{}, fmt.Errorf("carries the unknown role %q", d.Role)
	}
	shape := DocumentShape(d)
	profile := Profile{
		Role: role,
		Identity: Identity{
			Model: ModelIdentity{
				Name:         CanonicalModelName(d.Model),
				Revision:     UnspecifiedRevision,
				Tokenizer:    CanonicalModelName(d.Model),
				Quantization: CanonicalQuantization(d.Quantization),
			},
			Engine: EngineIdentity{
				// 文档指定的是引擎系列而非容器镜像，因为镜像包含的 tag 和 digest
				// 无法反映其背后使用的实际引擎。
				Image: d.Engine,
			},
			Hardware: NormalizeHardwareIdentity(HardwareIdentity{Accelerator: d.Hardware}),
		},
		ServingConfig:    ServingConfig{},
		Topology:         NormalizeTopologyDocument(d.Topology),
		MeasuredReplicas: 1,
		Workload: Workload{
			// Workload 以负载形态命名，因为文档本身未指定 Workload 名称，
			// 且测量结果以其测量对象来标识。
			ID: shape.String(),
			// 文档不固定权重 commit 和 Workload 版本，因为公开基准测试仅发布模型名称
			// 与扫描参数。扁平目录文档仅包含按全未命中（all-miss）prompt 计数口径
			// 标定的 profile（inputTokensPerSecond 与 totalTokensPerSecond 统计的是
			// 缓存未命中的 prompt token）。
			Revision:      UnspecifiedRevision,
			Modality:      "text",
			CacheBehavior: CacheBehaviorAllMisses,
			ArrivalModel:  ClientModelFixedConcurrency,
		},
	}
	for _, point := range d.Points {
		converted, err := toPoint(point, role)
		if err != nil {
			return Profile{}, err
		}
		profile.Points = append(profile.Points, converted)
	}
	if len(profile.Points) >= 2 {
		highest := 0
		for i := 1; i < len(profile.Points); i++ {
			if profile.Points[i].Load.Value > profile.Points[highest].Load.Value {
				highest = i
			}
		}
		// 当多点扫描中的每个实测点均满足延迟目标时，最高并发点即为可行曲线的上限。
		profile.Points[highest].Saturation = &Saturation{
			Measured: true,
			Note:     "highest concurrency point of the measured sweep",
		}
	}
	digest, err := ProfileDigest(profile)
	if err != nil {
		return Profile{}, fmt.Errorf("compute profile digest: %w", err)
	}
	profile.Digest = digest
	return profile, nil
}

// toPoint 将文档中的一个负载点转换为解析器读取的点。
//
// 它会选取该角色所需的速率，因为文档中包含测量产生的三个速率，而解析器只请求其中一个。
func toPoint(d PointDocument, role RoleKind) (MeasuredPoint, error) {
	requirement, _ := requirementFor(role)
	rate := d.TotalTokensPerSecond
	switch requirement.metric {
	case ThroughputMetricCacheMissedInputTokensPerSecond:
		rate = d.InputTokensPerSecond
	case ThroughputMetricOutputTokensPerSecond:
		rate = d.OutputTokensPerSecond
	}
	point := MeasuredPoint{
		Load: LoadLevel{
			Kind:        LoadKindConcurrency,
			Value:       float64(d.Concurrency),
			ClientModel: ClientModelFixedConcurrency,
			Unit:        "requests",
		},
		Throughput: Throughput{
			Metric:               requirement.metric,
			Value:                rate,
			Unit:                 UnitTokensPerSecond,
			Denominator:          DenominatorPerReplica,
			TokenCountConvention: requirement.convention,
		},
	}
	ttftReadings, err := latencyReadings(LatencyMetricTTFT, d.TTFTMilliseconds)
	if err != nil {
		return MeasuredPoint{}, err
	}
	itlReadings, err := latencyReadings(LatencyMetricITL, d.ITLMilliseconds)
	if err != nil {
		return MeasuredPoint{}, err
	}
	point.Latencies = append(point.Latencies, ttftReadings...)
	point.Latencies = append(point.Latencies, itlReadings...)
	return point, nil
}

// latencyReadings 将单个指标记录的分位数转换为按分位数升序排列的延迟读数。
func latencyReadings(metric LatencyMetric, quantiles LatencyQuantiles) ([]Latency, error) {
	if len(quantiles) == 0 {
		return nil, nil
	}
	readings := make([]Latency, 0, len(quantiles))
	for key, value := range quantiles {
		level, err := parseQuantileKey(key)
		if err != nil {
			return nil, fmt.Errorf("%s latency quantile %q: %w", metric, key, err)
		}
		readings = append(readings, Latency{
			Metric:   metric,
			Quantile: level,
			Value:    value,
			Unit:     UnitMilliseconds,
		})
	}
	sort.Slice(readings, func(i, j int) bool {
		return readings[i].Quantile < readings[j].Quantile
	})
	for i := 1; i < len(readings); i++ {
		if readings[i].Quantile == readings[i-1].Quantile {
			return nil, fmt.Errorf("%s latency quantiles resolve to the same quantile %v", metric, readings[i].Quantile)
		}
	}
	return readings, nil
}

// parseQuantileKey 将 p25、p50、p70、p90、p95、p99 或 p99.9 等分位数键转换为 (0, 1] 区间内的分位水平。
func parseQuantileKey(key string) (float64, error) {
	if !strings.HasPrefix(key, "p") {
		return 0, fmt.Errorf("quantile key %q must start with \"p\"", key)
	}
	rest := strings.TrimPrefix(key, "p")
	if rest == "" {
		return 0, fmt.Errorf("quantile key %q names no percentile", key)
	}
	percentile, err := strconv.ParseFloat(rest, 64)
	if err != nil || !isFinite(percentile) || percentile <= 0 || percentile > 100 {
		return 0, fmt.Errorf("quantile key %q must name a percentile in (0, 100]", key)
	}
	if canonical := strconv.FormatFloat(percentile, 'f', -1, 64); canonical != rest {
		return 0, fmt.Errorf("quantile key %q is not in canonical form %q", key, "p"+canonical)
	}
	rat, ok := new(big.Rat).SetString(rest)
	if !ok {
		return 0, fmt.Errorf("quantile key %q is not a decimal percentile", key)
	}
	rat.Mul(rat, big.NewRat(1, 100))
	level, _ := rat.Float64()
	return level, nil
}
