package worker

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"unicode/utf8"

	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/worker/scaling/priors"
)

// ModelDeploymentScalerCapacityPriors 包含各独立生效的算法及其依据。
type ModelDeploymentScalerCapacityPriors struct {
	Algorithms map[string]workercore.ModelDeploymentScalerAlgorithmSpec
	Status     []workercore.ModelDeploymentScalerResolvedCapacityPriorStatus
}

type modelDeploymentScalerCapacityField struct {
	name  string
	value **resource.Quantity
}

// ModelDeploymentScalerNeedsCapacityPrior 判断实际服务角色是否缺少容量配置。
// 当目标缺失时，会保守地检查每一种服务类型。未配置容量先验的 scaler 会手动声明
// 所有容量，因此从不需要容量先验。
func ModelDeploymentScalerNeedsCapacityPrior(scaler *workercore.ModelDeploymentScaler, target *workercore.ModelDeployment) bool {
	if !modelDeploymentScalerPriorRequested(scaler) {
		return false
	}
	roles := []workercore.ModelDeploymentRole{
		{Kind: workercore.ModelDeploymentRoleKindServer},
		{Kind: workercore.ModelDeploymentRoleKindPrefill},
		{Kind: workercore.ModelDeploymentRoleKindDecode},
	}
	if target != nil {
		roles = target.Spec.Roles
	}
	for _, role := range roles {
		effective := scaler.Spec.Algorithm.DeepCopy()
		for _, field := range modelDeploymentScalerCapacityFields(effective, ModelDeploymentEffectiveRoleKind(&role)) {
			if *field.value == nil {
				return true
			}
		}
	}
	return false
}

// ResolveModelDeploymentScalerCapacityPriors 仅在深拷贝上填充缺失的角色容量。
// 目录拒绝错误带有明确类型；API 故障则保留原始类型，以供准入控制和重试使用。
func ResolveModelDeploymentScalerCapacityPriors(
	ctx context.Context, reader ctrlcli.Reader, scaler *workercore.ModelDeploymentScaler, target *workercore.ModelDeployment,
) (*ModelDeploymentScalerCapacityPriors, error) {
	result := &ModelDeploymentScalerCapacityPriors{Algorithms: make(map[string]workercore.ModelDeploymentScalerAlgorithmSpec)}
	// scaler 所依据的分位数仅解析一次，因为先验解析与在线采样必须针对同一指标问题作答。
	quantile, quantileErr := ModelDeploymentScalerLatencyQuantile(&scaler.Spec.Algorithm)
	if quantileErr != nil {
		return nil, modelDeploymentScalerPriorRefusal("", "%v", quantileErr)
	}
	var catalog *priors.Catalog
	for _, role := range target.Spec.Roles {
		effective := scaler.Spec.Algorithm.DeepCopy()
		if !modelDeploymentScalerPriorRequested(scaler) {
			result.Algorithms[role.Name] = *effective
			continue
		}
		fields := modelDeploymentScalerCapacityFields(effective, ModelDeploymentEffectiveRoleKind(&role))
		missing := false
		for _, field := range fields {
			if *field.value == nil {
				missing = true
				continue
			}
			if (*field.value).Sign() <= 0 || !modelDeploymentScalerFinite((*field.value).AsApproximateFloat64()) {
				return nil, modelDeploymentScalerPriorRefusal(role.Name, "explicit capacity field %s must be finite and positive", field.name)
			}
		}
		status := workercore.ModelDeploymentScalerResolvedCapacityPriorStatus{Name: role.Name}
		var resolution priors.Resolution
		if missing {
			if catalog == nil {
				var err error
				catalog, err = modelDeploymentScalerLoadCatalog(ctx, reader, scaler)
				if err != nil {
					return nil, err
				}
			}
			var err error
			resolution, status, err = modelDeploymentScalerResolveRolePrior(ctx, reader, scaler, target, &role, effective, catalog, quantile)
			if err != nil {
				return nil, err
			}
		}
		for _, field := range fields {
			source := workercore.ModelDeploymentScalerCapacityFieldSourceExplicit
			if *field.value == nil {
				quantity, err := modelDeploymentScalerPriorQuantity(resolution.EffectiveTokensPerSecond)
				if err != nil {
					return nil, modelDeploymentScalerPriorRefusal(role.Name, "effective capacity cannot be represented: %v", err)
				}
				*field.value = quantity
				source = workercore.ModelDeploymentScalerCapacityFieldSourceCatalog
			}
			status.Fields = append(status.Fields, workercore.ModelDeploymentScalerResolvedCapacityFieldStatus{
				Field: field.name, Source: source, EffectiveTokensPerSecond: (*field.value).DeepCopy(),
			})
		}
		fingerprint, err := modelDeploymentScalerPriorFingerprint(target, &role, effective, status)
		if err != nil {
			return nil, err
		}
		status.CompatibilityFingerprint = fingerprint
		result.Algorithms[role.Name] = *effective
		result.Status = append(result.Status, status)
	}
	return result, nil
}

const maxCachedConfigMapCatalogs = 32

var (
	configMapCatalogCacheMu sync.RWMutex
	configMapCatalogCache   = make(map[[32]byte]*priors.Catalog, maxCachedConfigMapCatalogs)
)

func parseCachedConfigMapCatalog(data []byte) (*priors.Catalog, error) {
	sum := sha256.Sum256(data)
	configMapCatalogCacheMu.RLock()
	cached, ok := configMapCatalogCache[sum]
	configMapCatalogCacheMu.RUnlock()
	if ok {
		return cached, nil
	}
	catalog, err := priors.ParseCatalog(data)
	if err != nil {
		return nil, err
	}
	configMapCatalogCacheMu.Lock()
	if len(configMapCatalogCache) >= maxCachedConfigMapCatalogs {
		for key := range configMapCatalogCache {
			delete(configMapCatalogCache, key)
			if len(configMapCatalogCache) < maxCachedConfigMapCatalogs {
				break
			}
		}
	}
	configMapCatalogCache[sum] = catalog
	configMapCatalogCacheMu.Unlock()
	return catalog, nil
}

func modelDeploymentScalerLoadCatalog(ctx context.Context, reader ctrlcli.Reader, scaler *workercore.ModelDeploymentScaler) (*priors.Catalog, error) {
	ref := scaler.Spec.Algorithm.CapacityPrior.CatalogRef
	if ref == nil {
		return priors.LoadEmbedded()
	}
	cm := new(core.ConfigMap)
	if err := reader.Get(ctx, ctrlcli.ObjectKey{Namespace: scaler.Namespace, Name: ref.Name}, cm); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, modelDeploymentScalerPriorRefusal("", "catalog ConfigMap %q does not exist", ref.Name)
		}
		return nil, err
	}
	key := ref.Key
	if key == "" {
		key = priors.DefaultCatalogConfigMapKey
	}
	catalogBytes, found, err := modelDeploymentScalerCatalogData(cm, key)
	if err != nil {
		return nil, err
	}
	if !found {
		// 还会尝试另一个键名，因为在目录改名前写入的 ConfigMap 仍保存着 operator
		// 随附的目录数据，若读取器拒绝该键名，就会连带拒绝对应的部署。
		for _, fallback := range []string{priors.DefaultCatalogConfigMapKey, priors.LegacyCatalogConfigMapKey} {
			if fallback == key {
				continue
			}
			catalogBytes, found, err = modelDeploymentScalerCatalogData(cm, fallback)
			if err != nil {
				return nil, err
			}
			if found {
				key = fallback
				break
			}
		}
	}
	if !found {
		return nil, modelDeploymentScalerPriorRefusal("", "catalog ConfigMap %q lacks key %q", ref.Name, key)
	}
	catalog, err := parseCachedConfigMapCatalog(catalogBytes)
	if err != nil {
		return nil, modelDeploymentScalerPriorRefusal("", "catalog ConfigMap %q is invalid: %.512s", ref.Name, err)
	}
	return catalog, nil
}

// modelDeploymentScalerCatalogData 返回单个目录键对应的字节数据。
//
// 若同一个键同时存在于 Data 和 BinaryData 中，则会拒绝读取，因为两份目录
// 副本无法视为同一份目录。
func modelDeploymentScalerCatalogData(cm *core.ConfigMap, key string) ([]byte, bool, error) {
	data, present := cm.Data[key]
	binary := cm.BinaryData[key]
	if present && binary != nil {
		return nil, false, modelDeploymentScalerPriorRefusal("", "catalog key %q exists in both data and binaryData", key)
	}
	if present {
		return []byte(data), true, nil
	}
	if binary != nil {
		return binary, true, nil
	}
	return nil, false, nil
}

func modelDeploymentScalerResolveRolePrior(
	ctx context.Context, reader ctrlcli.Reader, scaler *workercore.ModelDeploymentScaler, target *workercore.ModelDeployment,
	role *workercore.ModelDeploymentRole, effective *workercore.ModelDeploymentScalerAlgorithmSpec, catalog *priors.Catalog,
	quantile float64,
) (priors.Resolution, workercore.ModelDeploymentScalerResolvedCapacityPriorStatus, error) {
	status := workercore.ModelDeploymentScalerResolvedCapacityPriorStatus{Name: role.Name}
	// 目录的选择取决于部署所声明的配置，因为一个 profile 对应一种部署形态，而部署
	// 本身声明了其运行的形态。
	declaration, err := modelDeploymentScalerPriorDeclarationOf(ctx, reader, target, role)
	if err != nil {
		return priors.Resolution{}, status, err
	}
	shape := declaration.shape(modelDeploymentScalerRoleKind(ModelDeploymentEffectiveRoleKind(role)))
	profile, refusal := catalog.Select(shape)
	if refusal != nil {
		refusal.Role = role.Name
		return priors.Resolution{}, status, refusal
	}
	request, err := modelDeploymentScalerPriorRequest(target, role, &profile, declaration)
	if err != nil {
		return priors.Resolution{}, status, err
	}
	token := effective.TokenThroughput
	if effective.Type == workercore.ModelDeploymentScalerAlgorithmTypePredictive {
		token = &effective.Predictive.TokenThroughput
	}
	if token.TTFTTargetMilliseconds != nil {
		request.Constraints.TTFTTargetMilliseconds = token.TTFTTargetMilliseconds.AsApproximateFloat64()
	}
	if token.ITLTargetMilliseconds != nil {
		request.Constraints.ITLTargetMilliseconds = token.ITLTargetMilliseconds.AsApproximateFloat64()
	}
	// 解析过程以 scaler 请求的分位数为约束，解析器则返回其能够验证的实测分位数。
	request.Constraints.LatencyQuantile = quantile
	if headroom := scaler.Spec.Algorithm.CapacityPrior.Headroom; headroom != nil {
		value := headroom.AsApproximateFloat64()
		if !modelDeploymentScalerFinite(value) || value <= 0 || value > 1 {
			return priors.Resolution{}, status, modelDeploymentScalerPriorRefusal(role.Name, "headroom must be in (0,1]")
		}
		request.Constraints.Headroom = value
	}
	resolution, err := catalog.Resolve(request)
	if err != nil {
		var refusal *priors.RefusalError
		if errors.As(err, &refusal) {
			refusal.Role = role.Name
		}
		return priors.Resolution{}, status, err
	}
	basis := resolution.Basis
	status.SourceID, status.ProfileDigest = basis.SourceID, basis.ProfileDigest
	status.Metric, status.Quantile, status.WorkloadID = basis.Metric, basis.Quantile, basis.WorkloadID
	status.Denominator, status.CatalogDigest, status.SourceRevision = basis.Denominator, catalog.Digest, basis.SourceRevision
	status.WorkloadRevision = resolution.WorkloadRevision
	if resolution.GPUsPerReplica > math.MaxInt32 || resolution.MeasuredReplicas > math.MaxInt32 {
		return priors.Resolution{}, status, modelDeploymentScalerPriorRefusal(role.Name, "measured replica denominator exceeds the supported status range")
	}
	status.GPUsPerReplica, status.MeasuredReplicas = int32(resolution.GPUsPerReplica), int32(resolution.MeasuredReplicas)
	status.Headroom, err = modelDeploymentScalerPriorQuantity(basis.Headroom)
	if err != nil {
		return priors.Resolution{}, status, modelDeploymentScalerPriorRefusal(role.Name, "headroom cannot be represented: %v", err)
	}
	status.SeedTokensPerSecond, err = modelDeploymentScalerPriorQuantity(resolution.SeedTokensPerSecond)
	if err != nil {
		return priors.Resolution{}, status, modelDeploymentScalerPriorRefusal(role.Name, "seed capacity cannot be represented: %v", err)
	}
	for _, value := range []string{status.SourceID, status.SourceRevision, status.WorkloadID, status.WorkloadRevision} {
		if utf8.RuneCountInString(value) > 253 {
			return priors.Resolution{}, status, modelDeploymentScalerPriorRefusal(role.Name, "catalog provenance identifiers must not exceed 253 characters")
		}
	}
	selectedQuantile := 0.0
	for _, bound := range basis.LatencyBounds {
		selectedQuantile = max(selectedQuantile, bound.Quantile)
		status.LatencyBounds = append(status.LatencyBounds, workercore.ModelDeploymentScalerResolvedLatencyBoundStatus{
			Metric: string(bound.Metric), Quantile: priors.FormatLatencyQuantile(bound.Quantile), Conservative: bound.Conservative,
		})
	}

	// 实测分位数与请求分位数分开记录，因为目录若仅测得高于请求所需分位数的数据，
	// 其含义与直接测得请求分位数的结果并不相同。
	status.Quantile = priors.FormatLatencyQuantile(selectedQuantile)
	status.RequestedQuantile = priors.FormatLatencyQuantile(quantile)
	return resolution, status, nil
}

func modelDeploymentScalerPriorFingerprint(
	target *workercore.ModelDeployment, role *workercore.ModelDeploymentRole,
	effective *workercore.ModelDeploymentScalerAlgorithmSpec, status workercore.ModelDeploymentScalerResolvedCapacityPriorStatus,
) (string, error) {
	canonicalRole := role.DeepCopy()
	canonicalRole.Replicas = 0
	annotations := make(map[string]string)
	for _, key := range []string{
		workercore.ModelDeploymentScalerModelIDAnnotationKey, workercore.ModelDeploymentScalerHardwareIDAnnotationKey,
		workercore.ModelDeploymentScalerPrecisionAnnotationKey, workercore.ModelDeploymentScalerPrefillHardwareIDAnnotationKey,
		workercore.ModelDeploymentScalerDecodeHardwareIDAnnotationKey,
	} {
		annotations[key] = target.Annotations[key]
	}
	// 排序可消除声明顺序的差异，同时不改变有序启动参数本身的顺序。
	canonicalAlgorithm := effective.DeepCopy()
	return priors.Digest(struct {
		TargetUID   string
		Role        *workercore.ModelDeploymentRole
		Model       workercore.ModelDeploymentModel
		Engine      workercore.ModelDeploymentEngine
		Router      *workercore.ModelDeploymentRouter
		KVCache     *workercore.ModelDeploymentKVCache
		KVTransfer  *workercore.ModelDeploymentKVTransfer
		Annotations map[string]string
		Algorithm   *workercore.ModelDeploymentScalerAlgorithmSpec
		Evidence    workercore.ModelDeploymentScalerResolvedCapacityPriorStatus
	}{
		string(target.UID), canonicalRole, target.Spec.Model, target.Spec.Engine, target.Spec.Router, target.Spec.KVCache,
		target.Spec.KVTransfer, annotations, canonicalAlgorithm, status,
	})
}

func modelDeploymentScalerFinite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

// modelDeploymentScalerPriorRequested 判断 scaler 是否请求了容量先验。
//
// 只要存在该配置块即视为发起请求，因为块内没有用于关闭解析的开关。未配置该块的
// scaler 会手动声明全部容量。
func modelDeploymentScalerPriorRequested(scaler *workercore.ModelDeploymentScaler) bool {
	return scaler.Spec.Algorithm.CapacityPrior != nil
}

func modelDeploymentScalerCapacityFields(
	algorithm *workercore.ModelDeploymentScalerAlgorithmSpec, kind workercore.ModelDeploymentRoleKind,
) []modelDeploymentScalerCapacityField {
	if kind == workercore.ModelDeploymentRoleKindEncode || algorithm.Type == workercore.ModelDeploymentScalerAlgorithmTypeProportional {
		return nil
	}
	var fields []modelDeploymentScalerCapacityField
	var token *workercore.ModelDeploymentScalerTokenThroughputSpec
	prefix := "tokenThroughput."
	if algorithm.Type == workercore.ModelDeploymentScalerAlgorithmTypePredictive {
		if algorithm.Predictive == nil {
			algorithm.Predictive = &workercore.ModelDeploymentScalerPredictiveSpec{}
		}
		predictive := algorithm.Predictive
		switch kind {
		case workercore.ModelDeploymentRoleKindServer:
			fields = append(fields, modelDeploymentScalerCapacityField{"predictive.serverTokenCapacityPerReplica", &predictive.ServerTokenCapacityPerReplica})
		case workercore.ModelDeploymentRoleKindPrefill:
			fields = append(fields, modelDeploymentScalerCapacityField{
				"predictive.prefillTokenCapacityPerReplica", &predictive.PrefillTokenCapacityPerReplica,
			})
		case workercore.ModelDeploymentRoleKindDecode:
			fields = append(fields, modelDeploymentScalerCapacityField{"predictive.decodeTokenCapacityPerReplica", &predictive.DecodeTokenCapacityPerReplica})
		}
		token, prefix = &predictive.TokenThroughput, "predictive.tokenThroughput."
	} else {
		if algorithm.TokenThroughput == nil {
			algorithm.TokenThroughput = &workercore.ModelDeploymentScalerTokenThroughputSpec{}
		}
		token = algorithm.TokenThroughput
	}
	switch kind {
	case workercore.ModelDeploymentRoleKindServer:
		fields = append(fields, modelDeploymentScalerCapacityField{prefix + "serverTargetTokensPerSecond", &token.ServerTargetTokensPerSecond})
	case workercore.ModelDeploymentRoleKindPrefill:
		fields = append(fields, modelDeploymentScalerCapacityField{prefix + "prefillTargetTokensPerSecond", &token.PrefillTargetTokensPerSecond})
	case workercore.ModelDeploymentRoleKindDecode:
		fields = append(fields, modelDeploymentScalerCapacityField{prefix + "decodeTargetTokensPerSecond", &token.DecodeTargetTokensPerSecond})
	}
	return fields
}

func modelDeploymentScalerPriorRefusal(role, format string, args ...any) error {
	return &priors.RefusalError{Reason: priors.RefusalCapacityPriorUnresolved, Message: fmt.Sprintf("role %q: "+format, append([]any{role}, args...)...)}
}

func modelDeploymentScalerPriorQuantity(value float64) (*resource.Quantity, error) {
	if !modelDeploymentScalerFinite(value) || value <= 0 {
		return nil, fmt.Errorf("capacity must be finite and positive")
	}
	quantity, err := resource.ParseQuantity(strconv.FormatFloat(value, 'g', -1, 64))
	if err != nil {
		return nil, err
	}
	represented := quantity.AsApproximateFloat64()
	if !modelDeploymentScalerFinite(represented) || represented <= 0 || math.Abs(represented-value)/value > 1e-12 {
		return nil, fmt.Errorf("capacity exceeds Quantity precision or range")
	}
	return &quantity, nil
}
