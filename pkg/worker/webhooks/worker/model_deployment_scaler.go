package worker

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrladmission "sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/webhook"
	workerctrl "gpustack.ai/gpustack/pkg/worker/controllers/worker"
	"gpustack.ai/gpustack/pkg/worker/scaling/priors"
)

// 当对象省略对应字段时，scaler 所使用的初始默认值。
//
// 这里的每个默认值都是算法运行所必需的值，因此省略其中任一项都会被 schema 拒绝，
// 而不是以零值代替。对于这些字段中的一部分而言，0 是一个有意义的计数；而对另一些
// 字段而言，0 则表示缺失。正因如此，每个默认值都必须显式命名而非隐式推导。
const (
	// modelDeploymentScalerDefaultIntervalSeconds 是 scaler 检查目标对象的周期（秒）。
	modelDeploymentScalerDefaultIntervalSeconds = 15

	// modelDeploymentScalerDefaultPendingTimeoutSeconds 是已提交的扩容请求在未获准入前
	// 可保持挂起的最长时间（秒），超时后将被撤回。
	modelDeploymentScalerDefaultPendingTimeoutSeconds = 300

	// modelDeploymentScalerDefaultStableWindowSeconds 是计算稳态（stable）扩缩容建议所用的时间窗口（秒）。
	modelDeploymentScalerDefaultStableWindowSeconds = 180

	// modelDeploymentScalerDefaultPanicWindowSeconds 是计算紧急（panic）扩缩容建议所用的时间窗口（秒）。
	modelDeploymentScalerDefaultPanicWindowSeconds = 60

	// modelDeploymentScalerDefaultMaxStepReplicas 是单轮扩缩容最多可增加的副本数。
	modelDeploymentScalerDefaultMaxStepReplicas = 4

	// modelDeploymentScalerDefaultMaxStepPercent 是单轮扩缩容最多可增加的副本百分比。
	modelDeploymentScalerDefaultMaxStepPercent = 100

	// modelDeploymentScalerDefaultScaleDownStabilizationSeconds 是保留缩容建议用于稳定判断的时间窗口（秒）。
	modelDeploymentScalerDefaultScaleDownStabilizationSeconds = 300

	// modelDeploymentScalerDefaultCooldownAfterAbortSeconds 是下线（retirement）中止后 role 需保持冷却的时间（秒）。
	modelDeploymentScalerDefaultCooldownAfterAbortSeconds = 300

	// modelDeploymentScalerDefaultDecodePostScaleUpGraceSeconds 是 Decode role 扩容后需保持的宽限期（秒）。
	modelDeploymentScalerDefaultDecodePostScaleUpGraceSeconds = 120

	// modelDeploymentScalerDefaultMinReplicas 是单个 role 允许保留的最小副本数。
	modelDeploymentScalerDefaultMinReplicas = 1

	// modelDeploymentScalerDefaultCatalogKey 是在省略 key 时，catalog 引用默认读取的 ConfigMap 数据键。
	modelDeploymentScalerDefaultCatalogKey = priors.DefaultCatalogConfigMapKey
)

// modelDeploymentScalerMaxReplicas 是单个 role 允许扩缩容到的最大副本数。
//
// 该数值限制了可行副本向量的搜索范围，因此它属于搜索空间的属性而非算法本身的限制：
// 每个 role 仅从最多包含这么多整数的区间内选取副本数，构成搜索向量空间。
const modelDeploymentScalerMaxReplicas = 1024

// modelDeploymentScalerFeasibilityBudget 是搜索可行副本向量时在放弃前最多可尝试的向量数量。
//
// 该搜索是精确搜索，而预算上限用于防止准入请求在无人实际使用的规模形态上耗尽资源。
// 当搜索耗尽预算时，不会直接拒绝该对象，而是判定为无法决定，因为拒绝一个实际上
// 可行的配置会导致对象被困住，且其所有者无法通过编辑对象来修复问题。
const modelDeploymentScalerFeasibilityBudget = 1 << 14

// ModelDeploymentScalerWebhook 为 ModelDeploymentScaler 填充默认值并执行校验。
//
// 当目标存在时它会读取目标对象，因为指向目标不存在的 role 或不可扩缩容的 role 的
// scaler 等于在做无意义规划。当目标不存在时则跳过读取，因为允许在创建目标之前
// 先编写 scaler。
//
// nolint: lll
// +k8s:webhook-gen:validating:group="worker.gpustack.ai",version="v1alpha1",resource="modeldeploymentscalers",scope="Namespaced"
// +k8s:webhook-gen:validating:operations=["CREATE","UPDATE"],failurePolicy="Fail",sideEffects="None",matchPolicy="Equivalent",timeoutSeconds=10
// +k8s:webhook-gen:mutating:group="worker.gpustack.ai",version="v1alpha1",resource="modeldeploymentscalers",scope="Namespaced"
// +k8s:webhook-gen:mutating:operations=["CREATE","UPDATE"],failurePolicy="Fail",sideEffects="None",matchPolicy="Equivalent",timeoutSeconds=10
type ModelDeploymentScalerWebhook struct {
	webhook.DefaultValidator

	// APIReader 用于读取目标 ModelDeployment。此处刻意不使用带缓存的客户端：
	// 若缓存存在延迟，可能会拿一个已不存在的目标状态来校验 scaler。
	APIReader ctrlcli.Reader
}

func (r *ModelDeploymentScalerWebhook) SetupWebhook(_ context.Context, opts webhook.SetupOptions) (runtime.Object, error) {
	r.APIReader = opts.Manager.GetAPIReader()

	return &workercore.ModelDeploymentScaler{}, nil
}

var (
	_ ctrladmission.Validator[runtime.Object] = (*ModelDeploymentScalerWebhook)(nil)
	_ ctrladmission.Defaulter[runtime.Object] = (*ModelDeploymentScalerWebhook)(nil)
)

// Default 填充 scaler 省略的默认值。
//
// 模式默认设为 Shadow，因此刚创建的 scaler 在所有者明确要求其生效前仅做观测而不产生
// 任何变更。这区分了“已安装规划器”与“授权规划器操作目标”两个阶段。
//
// 当省略算法类型时，会根据实际填写的配置块自动推断，从而避免仅填写了 predictive
// 配置块的 scaler 在未察觉的情况下执行 token throughput 算法并忽略 predictive 块。
// 若完全未配置任何算法块，则默认使用 token throughput 算法，因为只有该算法的所有必需参数
// 均有默认值。
func (r *ModelDeploymentScalerWebhook) Default(ctx context.Context, obj runtime.Object) error {
	s := obj.(*workercore.ModelDeploymentScaler)
	if !s.DeletionTimestamp.IsZero() {
		return nil
	}
	_, _, err := r.defaultModelDeploymentScaler(ctx, s)
	return err
}

func (r *ModelDeploymentScalerWebhook) defaultModelDeploymentScaler(
	ctx context.Context, s *workercore.ModelDeploymentScaler,
) (*workercore.ModelDeployment, bool, error) {
	if s.Spec.Mode == "" {
		s.Spec.Mode = workercore.ModelDeploymentScalerModeShadow
	}

	if s.Spec.Sampling.IntervalSeconds == 0 {
		s.Spec.Sampling.IntervalSeconds = modelDeploymentScalerDefaultIntervalSeconds
	}
	if s.Spec.Sampling.PendingTimeoutSeconds == 0 {
		s.Spec.Sampling.PendingTimeoutSeconds = modelDeploymentScalerDefaultPendingTimeoutSeconds
	}

	if s.Spec.Algorithm.Type == "" {
		s.Spec.Algorithm.Type = modelDeploymentScalerAlgorithmTypeOf(s.Spec.Algorithm)
	}
	defaultModelDeploymentScalerCapacityPrior(s.Spec.Algorithm.CapacityPrior)
	suppressDefaultSLOs, md, targetRead, err := r.shouldSuppressDefaultSLOs(ctx, s)
	if err != nil {
		return nil, false, err
	}
	switch s.Spec.Algorithm.Type {
	case workercore.ModelDeploymentScalerAlgorithmTypeProportional:
		if s.Spec.Algorithm.Proportional == nil {
			s.Spec.Algorithm.Proportional = &workercore.ModelDeploymentScalerProportionalSpec{}
		}
	case workercore.ModelDeploymentScalerAlgorithmTypeTokenThroughput:
		if s.Spec.Algorithm.TokenThroughput == nil {
			s.Spec.Algorithm.TokenThroughput = &workercore.ModelDeploymentScalerTokenThroughputSpec{}
		}
		defaultModelDeploymentScalerTokenThroughput(s.Spec.Algorithm.TokenThroughput, suppressDefaultSLOs)
	case workercore.ModelDeploymentScalerAlgorithmTypePredictive:
		if s.Spec.Algorithm.Predictive == nil {
			s.Spec.Algorithm.Predictive = &workercore.ModelDeploymentScalerPredictiveSpec{}
		}
		defaultModelDeploymentScalerTokenThroughput(&s.Spec.Algorithm.Predictive.TokenThroughput, suppressDefaultSLOs)
	}

	for i := range s.Spec.Roles {
		if s.Spec.Roles[i].MinReplicas == 0 {
			s.Spec.Roles[i].MinReplicas = modelDeploymentScalerDefaultMinReplicas
		}
	}

	if s.Spec.Behavior.ScaleUp.MaxStepReplicas == 0 {
		s.Spec.Behavior.ScaleUp.MaxStepReplicas = modelDeploymentScalerDefaultMaxStepReplicas
	}
	if s.Spec.Behavior.ScaleUp.MaxStepPercent == 0 {
		s.Spec.Behavior.ScaleUp.MaxStepPercent = modelDeploymentScalerDefaultMaxStepPercent
	}
	if s.Spec.Behavior.ScaleDown.StabilizationWindowSeconds == 0 {
		s.Spec.Behavior.ScaleDown.StabilizationWindowSeconds = modelDeploymentScalerDefaultScaleDownStabilizationSeconds
	}
	if s.Spec.Behavior.ScaleDown.CooldownAfterAbortSeconds == 0 {
		s.Spec.Behavior.ScaleDown.CooldownAfterAbortSeconds = modelDeploymentScalerDefaultCooldownAfterAbortSeconds
	}
	if s.Spec.Behavior.ScaleDown.DecodePostScaleUpGraceSeconds == 0 {
		s.Spec.Behavior.ScaleDown.DecodePostScaleUpGraceSeconds = modelDeploymentScalerDefaultDecodePostScaleUpGraceSeconds
	}

	if s.Spec.Rebalance.Policy == "" {
		s.Spec.Rebalance.Policy = workercore.ModelDeploymentScalerRebalancePolicyOpposing
	}

	return md, targetRead, nil
}

// modelDeploymentScalerAlgorithmTypeOf 根据所填写的配置块选择 scaler 使用的算法类型。
func modelDeploymentScalerAlgorithmTypeOf(
	algorithm workercore.ModelDeploymentScalerAlgorithmSpec,
) workercore.ModelDeploymentScalerAlgorithmType {
	switch {
	case algorithm.Proportional != nil:
		return workercore.ModelDeploymentScalerAlgorithmTypeProportional
	case algorithm.Predictive != nil:
		return workercore.ModelDeploymentScalerAlgorithmTypePredictive
	default:
		return workercore.ModelDeploymentScalerAlgorithmTypeTokenThroughput
	}
}

// defaultModelDeploymentScalerCapacityPrior 填充容量先验（capacity prior）配置块中省略的默认值。
func defaultModelDeploymentScalerCapacityPrior(cp *workercore.ModelDeploymentScalerCapacityPriorSpec) {
	if cp == nil {
		return
	}
	if cp.CatalogRef != nil && cp.CatalogRef.Key == "" {
		cp.CatalogRef.Key = modelDeploymentScalerDefaultCatalogKey
	}
	if cp.Headroom == nil {
		cp.Headroom = ptr.To(resource.MustParse("0.75"))
	}
}

// shouldSuppressDefaultSLOs 报告是否必须抑制延迟目标的默认值，以确保在通过 catalog
// 解析先验容量时，缺少手动容量配置的 role 必须显式指定延迟目标。
func (r *ModelDeploymentScalerWebhook) shouldSuppressDefaultSLOs(
	ctx context.Context, s *workercore.ModelDeploymentScaler,
) (bool, *workercore.ModelDeployment, bool, error) {
	if !workerctrl.ModelDeploymentScalerNeedsCapacityPrior(s, nil) {
		return false, nil, false, nil
	}
	if r.APIReader == nil || strings.TrimSpace(s.Spec.TargetRef.Name) == "" {
		return true, nil, false, nil
	}
	md, err := r.readModelDeploymentScalerTarget(ctx, s)
	if err != nil {
		return false, nil, false, err
	}
	return workerctrl.ModelDeploymentScalerNeedsCapacityPrior(s, md), md, true, nil
}

// defaultModelDeploymentScalerTokenThroughput 填充 token throughput 算法所需的默认值。
// 当通过 catalog 解析容量先验时，会保留延迟 SLO 默认值为未设置状态，以便校验阶段
// 强制要求显式配置延迟目标。
func defaultModelDeploymentScalerTokenThroughput(
	tt *workercore.ModelDeploymentScalerTokenThroughputSpec, suppressDefaultSLOs bool,
) {
	if tt.StableWindowSeconds == 0 {
		tt.StableWindowSeconds = modelDeploymentScalerDefaultStableWindowSeconds
	}
	if tt.PanicWindowSeconds == 0 {
		tt.PanicWindowSeconds = modelDeploymentScalerDefaultPanicWindowSeconds
	}
	if tt.PanicThreshold == nil {
		tt.PanicThreshold = ptr.To(resource.MustParse("2.0"))
	}
	if suppressDefaultSLOs {
		return
	}
	if tt.TTFTTargetMilliseconds == nil {
		tt.TTFTTargetMilliseconds = ptr.To(resource.MustParse("500"))
	}
	if tt.ITLTargetMilliseconds == nil {
		tt.ITLTargetMilliseconds = ptr.To(resource.MustParse("50"))
	}
}

func (r *ModelDeploymentScalerWebhook) ValidateCreate(
	ctx context.Context, obj runtime.Object,
) (ctrladmission.Warnings, error) {
	s := obj.(*workercore.ModelDeploymentScaler)

	warnings, errs := r.validateModelDeploymentScaler(ctx, s, false)
	if len(errs) > 0 {
		return nil, kerrors.NewInvalid(s.GroupVersionKind().GroupKind(), s.Name, errs)
	}

	return warnings, nil
}

func (r *ModelDeploymentScalerWebhook) ValidateUpdate(
	ctx context.Context, oldObj, newObj runtime.Object,
) (ctrladmission.Warnings, error) {
	s := newObj.(*workercore.ModelDeploymentScaler)
	if !s.DeletionTimestamp.IsZero() {
		return nil, nil
	}
	old, _ := oldObj.(*workercore.ModelDeploymentScaler)

	warnings, errs := r.validateModelDeploymentScaler(ctx, s, true)
	errs = append(errs, validateModelDeploymentScalerTargetRefImmutable(old, s)...)
	if len(errs) > 0 {
		return nil, kerrors.NewInvalid(s.GroupVersionKind().GroupKind(), s.Name, errs)
	}

	return warnings, nil
}

// validateModelDeploymentScaler 包含校验 scaler 所需的全部规则。
//
// 不依赖目标对象的规则优先执行，需要目标对象的规则在目标不存在时跳过。
// 因为允许在目标创建之前先编写 scaler，若在目标尚不存在时去读取并校验它，
// 会错误地拒绝一个仅仅是提前创建的合法对象。
func (r *ModelDeploymentScalerWebhook) validateModelDeploymentScaler(
	ctx context.Context, s *workercore.ModelDeploymentScaler, isUpdate bool,
) (ctrladmission.Warnings, field.ErrorList) {
	var (
		warnings ctrladmission.Warnings
		errs     field.ErrorList
	)

	// 校验对象以其最终落盘存储的形态为准，即经过默认值填充后的对象。
	// 由 defaulting webhook 填充的字段会以其即将承载的值接受校验，因此规则绝不会
	// 因某个即将被自动填充的值而拒绝对象，两端也不会对“省略字段的 scaler 语义”产生分歧。
	effective := s.DeepCopy()
	md, targetRead, err := r.defaultModelDeploymentScaler(ctx, effective)
	if err != nil {
		return nil, append(errs, field.InternalError(field.NewPath("spec"), err))
	}

	errs = append(errs, validateModelDeploymentScalerMode(effective)...)
	errs = append(errs, validateModelDeploymentScalerTargetRef(effective)...)
	errs = append(errs, validateModelDeploymentScalerSampling(effective)...)
	errs = append(errs, validateModelDeploymentScalerRoles(effective)...)
	errs = append(errs, validateModelDeploymentScalerRatios(effective)...)
	errs = append(errs, validateModelDeploymentScalerBehavior(effective)...)
	errs = append(errs, validateModelDeploymentScalerRebalance(effective)...)
	errs = append(errs, validateModelDeploymentScalerAlgorithm(effective)...)

	if len(errs) > 0 {
		return nil, errs
	}

	errs = append(errs, validateModelDeploymentScalerRatioFeasibility(effective)...)

	if !targetRead {
		md, err = r.readModelDeploymentScalerTarget(ctx, s)
		if err != nil {
			return nil, append(errs, field.InternalError(field.NewPath("spec", "targetRef", "name"), err))
		}
	}
	if md == nil {
		if workerctrl.ModelDeploymentScalerNeedsCapacityPrior(effective, nil) {
			if sloErrs := validateModelDeploymentScalerUnresolvedTargetSLOs(effective); len(sloErrs) > 0 {
				return nil, append(errs, sloErrs...)
			}
		}
		if effective.Spec.Algorithm.CapacityPrior != nil {
			warnings = append(warnings, fmt.Sprintf(
				"target ModelDeployment %q does not exist yet; capacity prior compatibility will be verified by the controller once the target exists",
				effective.Spec.TargetRef.Name,
			))
		}
		return warnings, errs
	}

	targetWarnings, targetErrs := r.validateModelDeploymentScalerAgainstTarget(ctx, effective, md, isUpdate)
	warnings = append(warnings, targetWarnings...)
	errs = append(errs, targetErrs...)
	return warnings, errs
}

// readModelDeploymentScalerTarget 读取 scaler 指向的目标对象，或在目标不存在时进行报告。
func (r *ModelDeploymentScalerWebhook) readModelDeploymentScalerTarget(
	ctx context.Context, s *workercore.ModelDeploymentScaler,
) (*workercore.ModelDeployment, error) {
	if r.APIReader == nil {
		return nil, fmt.Errorf("the scaler target cannot be read, because this handler has no reader")
	}

	md := new(workercore.ModelDeployment)
	err := r.APIReader.Get(ctx, ctrlcli.ObjectKey{Namespace: s.Namespace, Name: s.Spec.TargetRef.Name}, md)
	if err != nil {
		if kerrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read the target ModelDeployment %q: %w", s.Spec.TargetRef.Name, err)
	}

	return md, nil
}

// validateModelDeploymentScalerMode 校验 scaler 的执行模式。
func validateModelDeploymentScalerMode(s *workercore.ModelDeploymentScaler) field.ErrorList {
	switch s.Spec.Mode {
	case workercore.ModelDeploymentScalerModeShadow, workercore.ModelDeploymentScalerModeActive:
		return nil
	case "":
		return field.ErrorList{field.Required(field.NewPath("spec", "mode"),
			"a scaler either observes (Shadow) or acts (Active)")}
	default:
		return field.ErrorList{field.Invalid(field.NewPath("spec", "mode"), s.Spec.Mode,
			"a mode is either Shadow or Active")}
	}
}

// validateModelDeploymentScalerTargetRef 拒绝未指定目标的 scaler。
func validateModelDeploymentScalerTargetRef(s *workercore.ModelDeploymentScaler) field.ErrorList {
	var errs field.ErrorList

	if strings.TrimSpace(s.Spec.TargetRef.Name) == "" {
		errs = append(errs, field.Required(
			field.NewPath("spec", "targetRef", "name"),
			"this scaler plans for the ModelDeployment it names, and a scaler that names none "+
				"plans for nothing",
		))
	}

	return errs
}

// validateModelDeploymentScalerTargetRefImmutable 拒绝将 scaler 指向另一个目标的修改。
//
// 目标是 scaler 所管理的对象，绑定关系跟随目标而非 scaler：修改目标引用会导致
// 旧目标残留无人可更新的绑定，而新目标却没有绑定。若要管理另一个目标，应当创建
// 第二个 scaler。
func validateModelDeploymentScalerTargetRefImmutable(
	old, s *workercore.ModelDeploymentScaler,
) field.ErrorList {
	errs := make(field.ErrorList, 0, 1)

	if old == nil || old.Spec.TargetRef.Name == s.Spec.TargetRef.Name {
		return errs
	}

	errs = append(errs, field.Invalid(
		field.NewPath("spec", "targetRef", "name"), s.Spec.TargetRef.Name,
		"the target of a scaler is fixed once it is set: a different target is a different "+
			"scaler, because the binding follows the target and the old one would be left "+
			"holding a binding nobody may write",
	))

	return errs
}

// validateModelDeploymentScalerSampling 校验 scaler 运行所依赖的两个采样时间窗口。
func validateModelDeploymentScalerSampling(s *workercore.ModelDeploymentScaler) field.ErrorList {
	errs := make(field.ErrorList, 0, 2)

	path := field.NewPath("spec", "sampling")
	if s.Spec.Sampling.IntervalSeconds < 5 || s.Spec.Sampling.IntervalSeconds > 300 {
		errs = append(errs, field.Invalid(
			path.Child("intervalSeconds"), s.Spec.Sampling.IntervalSeconds,
			"the interval is how often the scaler looks at its target and how often it writes, "+
				"so it is between 5 and 300 seconds",
		))
	}
	if s.Spec.Sampling.PendingTimeoutSeconds < 60 || s.Spec.Sampling.PendingTimeoutSeconds > 1200 {
		errs = append(errs, field.Invalid(
			path.Child("pendingTimeoutSeconds"), s.Spec.Sampling.PendingTimeoutSeconds,
			"the pending timeout is how long a committed scale-up may stay unadmitted before it "+
				"is retracted, so it is between 60 and 1200 seconds",
		))
	}

	return errs
}

// validateModelDeploymentScalerRoles 校验 scaler 所规划的各 role 的副本数上下界。
func validateModelDeploymentScalerRoles(s *workercore.ModelDeploymentScaler) field.ErrorList {
	var errs field.ErrorList

	path := field.NewPath("spec", "roles")
	seen := map[string]int{}
	for i := range s.Spec.Roles {
		bounds := &s.Spec.Roles[i]
		rolePath := path.Index(i)

		if first, taken := seen[bounds.Name]; taken {
			errs = append(errs, field.Duplicate(rolePath.Child("name"), bounds.Name))
			errs = append(errs, field.Invalid(rolePath.Child("name"), bounds.Name,
				fmt.Sprintf("role %q is already declared at index %d", bounds.Name, first)))
			continue
		}
		seen[bounds.Name] = i

		if bounds.MinReplicas < 1 {
			errs = append(errs, field.Invalid(rolePath.Child("minReplicas"), bounds.MinReplicas,
				"a role runs at least one replica"))
		}
		if bounds.MaxReplicas < 1 {
			errs = append(errs, field.Invalid(rolePath.Child("maxReplicas"), bounds.MaxReplicas,
				"a role runs at least one replica"))
		}
		if bounds.MaxReplicas > modelDeploymentScalerMaxReplicas {
			errs = append(errs, field.Invalid(rolePath.Child("maxReplicas"), bounds.MaxReplicas,
				fmt.Sprintf("a role runs at most %d replicas", modelDeploymentScalerMaxReplicas)))
		}
		if bounds.MinReplicas > bounds.MaxReplicas {
			errs = append(errs, field.Invalid(rolePath.Child("minReplicas"), bounds.MinReplicas,
				fmt.Sprintf("the floor of role %q is above its ceiling of %d",
					bounds.Name, bounds.MaxReplicas)))
		}
	}

	return errs
}

// validateModelDeploymentScalerRatios 校验各 role 之间的比例约束。
//
// 比例约束必须引用 scaler 纳入规划的 role，因为纳入规划的 role 与未纳入规划的 role
// 之间的比例无法通过任何副本向量来维持。
func validateModelDeploymentScalerRatios(s *workercore.ModelDeploymentScaler) field.ErrorList {
	var errs field.ErrorList

	known := map[string]bool{}
	for i := range s.Spec.Roles {
		known[s.Spec.Roles[i].Name] = true
	}

	path := field.NewPath("spec", "ratios")
	seen := map[string]int{}
	for i := range s.Spec.Ratios {
		ratio := &s.Spec.Ratios[i]
		ratioPath := path.Index(i)

		if ratio.NumeratorRole == ratio.DenominatorRole {
			errs = append(errs, field.Invalid(ratioPath.Child("numeratorRole"), ratio.NumeratorRole,
				"a ratio compares two roles, and this one compares a role with itself"))
			continue
		}
		if !known[ratio.NumeratorRole] {
			errs = append(errs, field.Invalid(ratioPath.Child("numeratorRole"), ratio.NumeratorRole,
				"this role is not one of the roles this scaler plans for"))
		}
		if !known[ratio.DenominatorRole] {
			errs = append(errs, field.Invalid(ratioPath.Child("denominatorRole"), ratio.DenominatorRole,
				"this role is not one of the roles this scaler plans for"))
		}

		key := ratio.NumeratorRole + "/" + ratio.DenominatorRole
		if first, taken := seen[key]; taken {
			errs = append(errs, field.Duplicate(ratioPath, key))
			errs = append(errs, field.Invalid(ratioPath, key,
				fmt.Sprintf("this ratio is already declared at index %d", first)))
			continue
		}
		seen[key] = i

		errs = append(errs, validateModelDeploymentScalerRatioQuantities(ratio, ratioPath)...)
	}

	return errs
}

// validateModelDeploymentScalerRatioQuantities 校验单个比例约束中的数值配置。
//
// 目标比例必须位于区间范围内，因为超出区间的目标比例是任何向量都无法同时满足的，
// 而排除了目标比例的区间也是毫无意义的约束。
func validateModelDeploymentScalerRatioQuantities(
	ratio *workercore.ModelDeploymentScalerRoleRatio, ratioPath *field.Path,
) field.ErrorList {
	errs := make(field.ErrorList, 0, 5)

	errs = append(errs, validateModelDeploymentScalerPositiveQuantity(
		ratioPath.Child("target"), ratio.Target, "the target of a ratio is how many replicas the "+
			"numerator role holds per replica of the denominator role",
	)...)
	errs = append(errs, validateModelDeploymentScalerPositiveQuantity(
		ratioPath.Child("min"), ratio.Min, "the lower bound of a ratio is how few replicas the "+
			"numerator role may hold per replica of the denominator role",
	)...)
	errs = append(errs, validateModelDeploymentScalerPositiveQuantity(
		ratioPath.Child("max"), ratio.Max, "the upper bound of a ratio is how many replicas the "+
			"numerator role may hold per replica of the denominator role",
	)...)

	minimum, minimumSet := modelDeploymentScalerRatioValue(ratio.Min)
	maximum, maximumSet := modelDeploymentScalerRatioValue(ratio.Max)
	if minimumSet && maximumSet && minimum.Cmp(maximum) > 0 {
		errs = append(errs, field.Invalid(ratioPath.Child("max"), ratio.Max.String(),
			"the upper bound of a ratio is below its lower bound, so no ratio satisfies it"))
	}

	target, targetSet := modelDeploymentScalerRatioValue(ratio.Target)
	if targetSet && minimumSet && target.Cmp(minimum) < 0 {
		errs = append(errs, field.Invalid(ratioPath.Child("target"), ratio.Target.String(),
			"the target of a ratio is below its lower bound, so no vector reaches it"))
	}
	if targetSet && maximumSet && target.Cmp(maximum) > 0 {
		errs = append(errs, field.Invalid(ratioPath.Child("target"), ratio.Target.String(),
			"the target of a ratio is above its upper bound, so no vector reaches it"))
	}

	return errs
}

// validateModelDeploymentScalerPositiveQuantity 拒绝非正数的数量值。
func validateModelDeploymentScalerPositiveQuantity(
	path *field.Path, quantity *resource.Quantity, what string,
) field.ErrorList {
	var errs field.ErrorList

	if quantity == nil {
		return errs
	}
	if quantity.Sign() <= 0 {
		errs = append(errs, field.Invalid(path, quantity.String(),
			what+", so it is a positive number"))
	}

	return errs
}

// modelDeploymentScalerRatioValue 将数量值读取为精确有理数，并指示该值是否已设置。
//
// 此处必须使用精确有理数，因为比例边界需要与两个整数的商进行比较，若经过舍入
// 会导致边界判定出错。
func modelDeploymentScalerRatioValue(quantity *resource.Quantity) (*big.Rat, bool) {
	if quantity == nil {
		return nil, false
	}

	dec := quantity.AsDec()
	num := new(big.Int).Set(dec.UnscaledBig())
	den := big.NewInt(1)
	scale := int64(dec.Scale())
	if scale >= 0 {
		den.Exp(big.NewInt(10), big.NewInt(scale), nil)
	} else {
		num.Mul(num, new(big.Int).Exp(big.NewInt(10), big.NewInt(-scale), nil))
	}

	return new(big.Rat).SetFrac(num, den), true
}

// validateModelDeploymentScalerBehavior 校验 scaler 的扩缩容节奏控制规则。
func validateModelDeploymentScalerBehavior(s *workercore.ModelDeploymentScaler) field.ErrorList {
	var errs field.ErrorList

	path := field.NewPath("spec", "behavior", "scaleUp")
	if s.Spec.Behavior.ScaleUp.MaxStepReplicas < 0 {
		errs = append(errs, field.Invalid(path.Child("maxStepReplicas"),
			s.Spec.Behavior.ScaleUp.MaxStepReplicas, "a step cap is a count of replicas"))
	}
	if s.Spec.Behavior.ScaleUp.MaxStepPercent < 0 || s.Spec.Behavior.ScaleUp.MaxStepPercent > 1000 {
		errs = append(errs, field.Invalid(path.Child("maxStepPercent"),
			s.Spec.Behavior.ScaleUp.MaxStepPercent, "a step cap in percent is between 0 and 1000"))
	}
	errs = append(errs, validateModelDeploymentScalerStepPolicy(
		path.Child("stepPolicy"), s.Spec.Behavior.ScaleUp.StepPolicy,
	)...)

	downPath := field.NewPath("spec", "behavior", "scaleDown")
	errs = append(errs, validateModelDeploymentScalerStepPolicy(
		downPath.Child("stepPolicy"), s.Spec.Behavior.ScaleDown.StepPolicy,
	)...)

	return errs
}

// validateModelDeploymentScalerStepPolicy 校验单条步长限制策略。
func validateModelDeploymentScalerStepPolicy(
	path *field.Path, policy *workercore.ModelDeploymentScalerStepPolicy,
) field.ErrorList {
	var errs field.ErrorList

	if policy == nil {
		return errs
	}

	switch policy.Type {
	case workercore.ModelDeploymentScalerStepPolicyTypePods,
		workercore.ModelDeploymentScalerStepPolicyTypePercent:
	case "":
		errs = append(errs, field.Required(path.Child("type"),
			"a step policy says whether its value counts replicas or percent"))
	default:
		errs = append(errs, field.Invalid(path.Child("type"), policy.Type,
			"a step policy counts either replicas or percent"))
	}
	if policy.Value <= 0 {
		errs = append(errs, field.Invalid(path.Child("value"), policy.Value,
			"a step cap is a positive count"))
	}

	return errs
}

// validateModelDeploymentScalerRebalance 校验 scaler 的重均衡（rebalance）顺序配置。
func validateModelDeploymentScalerRebalance(s *workercore.ModelDeploymentScaler) field.ErrorList {
	var errs field.ErrorList

	switch s.Spec.Rebalance.Policy {
	case workercore.ModelDeploymentScalerRebalancePolicyOpposing,
		workercore.ModelDeploymentScalerRebalancePolicyPriority,
		workercore.ModelDeploymentScalerRebalancePolicyDisabled:
	case "":
		errs = append(errs, field.Required(field.NewPath("spec", "rebalance", "policy"),
			"a rebalance policy says whether another role may give resources back"))
	default:
		errs = append(errs, field.Invalid(field.NewPath("spec", "rebalance", "policy"),
			s.Spec.Rebalance.Policy, "a rebalance policy is Opposing, Priority or Disabled"))
	}

	known := map[string]bool{}
	for i := range s.Spec.Roles {
		known[s.Spec.Roles[i].Name] = true
	}

	path := field.NewPath("spec", "rebalance", "priority")
	seen := map[string]int{}
	for i, role := range s.Spec.Rebalance.Priority {
		if !known[role] {
			errs = append(errs, field.Invalid(path.Index(i), role,
				"this role is not one of the roles this scaler plans for"))
			continue
		}
		if first, taken := seen[role]; taken {
			errs = append(errs, field.Duplicate(path.Index(i), role))
			errs = append(errs, field.Invalid(path.Index(i), role,
				fmt.Sprintf("role %q is already listed at index %d", role, first)))
			continue
		}
		seen[role] = i
	}

	return errs
}

// validateModelDeploymentScalerAlgorithm 校验 scaler 运行的算法及其参数。
func validateModelDeploymentScalerAlgorithm(s *workercore.ModelDeploymentScaler) field.ErrorList {
	var errs field.ErrorList

	path := field.NewPath("spec", "algorithm")
	switch s.Spec.Algorithm.Type {
	case workercore.ModelDeploymentScalerAlgorithmTypeProportional:
		if s.Spec.Algorithm.TokenThroughput != nil {
			errs = append(errs, field.Invalid(path.Child("tokenThroughput"), s.Spec.Algorithm.TokenThroughput,
				"this block is for the TokenThroughput algorithm, and this scaler runs Proportional"))
		}
		if s.Spec.Algorithm.Predictive != nil {
			errs = append(errs, field.Invalid(path.Child("predictive"), s.Spec.Algorithm.Predictive,
				"this block is for the Predictive algorithm, and this scaler runs Proportional"))
		}
		if s.Spec.Algorithm.CapacityPrior != nil {
			errs = append(errs, field.Invalid(path.Child("capacityPrior"), s.Spec.Algorithm.CapacityPrior,
				"this block is for the TokenThroughput and Predictive algorithms, and this scaler runs Proportional"))
		}
	case workercore.ModelDeploymentScalerAlgorithmTypeTokenThroughput:
		if s.Spec.Algorithm.Proportional != nil {
			errs = append(errs, field.Invalid(path.Child("proportional"), s.Spec.Algorithm.Proportional,
				"this block is for the Proportional algorithm, and this scaler runs TokenThroughput"))
		}
		if s.Spec.Algorithm.Predictive != nil {
			errs = append(errs, field.Invalid(path.Child("predictive"), s.Spec.Algorithm.Predictive,
				"this block is for the Predictive algorithm, and this scaler runs TokenThroughput"))
		}
	case workercore.ModelDeploymentScalerAlgorithmTypePredictive:
		if s.Spec.Algorithm.Proportional != nil {
			errs = append(errs, field.Invalid(path.Child("proportional"), s.Spec.Algorithm.Proportional,
				"this block is for the Proportional algorithm, and this scaler runs Predictive"))
		}
		if s.Spec.Algorithm.TokenThroughput != nil {
			errs = append(errs, field.Invalid(path.Child("tokenThroughput"), s.Spec.Algorithm.TokenThroughput,
				"this block is for the TokenThroughput algorithm, and this scaler runs Predictive"))
		}
	case "":
		errs = append(errs, field.Required(path.Child("type"),
			"this scaler names no algorithm"))
	default:
		errs = append(errs, field.Invalid(path.Child("type"), s.Spec.Algorithm.Type,
			"an algorithm is Proportional, TokenThroughput or Predictive"))
	}

	errs = append(errs, validateModelDeploymentScalerProportional(s)...)
	errs = append(errs, validateModelDeploymentScalerTokenThroughput(s)...)
	errs = append(errs, validateModelDeploymentScalerPredictive(s)...)
	errs = append(errs, validateModelDeploymentScalerCapacityPrior(s)...)

	return errs
}

// validateModelDeploymentScalerCapacityPrior 校验容量先验（capacity prior）配置块的结构性字段。
func validateModelDeploymentScalerCapacityPrior(s *workercore.ModelDeploymentScaler) field.ErrorList {
	cp := s.Spec.Algorithm.CapacityPrior
	if cp == nil {
		return nil
	}

	var errs field.ErrorList
	path := field.NewPath("spec", "algorithm", "capacityPrior")

	if cp.CatalogRef != nil {
		refPath := path.Child("catalogRef")
		if strings.TrimSpace(cp.CatalogRef.Name) == "" {
			errs = append(errs, field.Required(refPath.Child("name"),
				"the catalog reference must name a ConfigMap"))
		} else if why := validation.IsDNS1123Subdomain(cp.CatalogRef.Name); len(why) > 0 {
			errs = append(errs, field.Invalid(refPath.Child("name"), cp.CatalogRef.Name,
				fmt.Sprintf("a ConfigMap name must be a valid DNS subdomain: %s", strings.Join(why, "; "))))
		}

		if strings.TrimSpace(cp.CatalogRef.Key) == "" {
			errs = append(errs, field.Required(refPath.Child("key"),
				"the catalog reference must name a ConfigMap data key"))
		} else if why := validation.IsConfigMapKey(cp.CatalogRef.Key); len(why) > 0 {
			errs = append(errs, field.Invalid(refPath.Child("key"), cp.CatalogRef.Key,
				fmt.Sprintf("a ConfigMap key must be a valid data key: %s", strings.Join(why, "; "))))
		}
	}

	if cp.Headroom != nil {
		headroom, ok := modelDeploymentScalerRatioValue(cp.Headroom)
		if !ok || headroom.Sign() <= 0 {
			errs = append(errs, field.Invalid(path.Child("headroom"), cp.Headroom.String(),
				"headroom is a positive fraction of the seed capacity"))
		} else if headroom.Cmp(big.NewRat(1, 1)) > 0 {
			errs = append(errs, field.Invalid(path.Child("headroom"), cp.Headroom.String(),
				"headroom is a fraction of the seed capacity, so it is at most one"))
		}
	}

	return errs
}

// validateModelDeploymentScalerProportional 校验 proportional 算法的参数。
func validateModelDeploymentScalerProportional(s *workercore.ModelDeploymentScaler) field.ErrorList {
	var errs field.ErrorList

	proportional := s.Spec.Algorithm.Proportional
	if proportional == nil {
		return errs
	}

	path := field.NewPath("spec", "algorithm", "proportional")
	switch proportional.Metric {
	case workercore.ModelDeploymentScalerProportionalMetricQueueDepth,
		workercore.ModelDeploymentScalerProportionalMetricRunningRequests,
		workercore.ModelDeploymentScalerProportionalMetricWaitingRequests,
		workercore.ModelDeploymentScalerProportionalMetricPromptTokensPerSecond,
		workercore.ModelDeploymentScalerProportionalMetricGenerationTokensPerSecond,
		workercore.ModelDeploymentScalerProportionalMetricTotalTokensPerSecond:
	case "":
		errs = append(errs, field.Required(path.Child("metric"),
			"the proportional algorithm scales on a signal, and this one names none"))
	default:
		errs = append(errs, field.Invalid(path.Child("metric"), proportional.Metric,
			"the proportional algorithm scales on a signal this operator does not publish"))
	}

	if proportional.Tolerance != nil {
		tolerance, _ := modelDeploymentScalerRatioValue(proportional.Tolerance)
		if tolerance.Cmp(big.NewRat(1, 1)) > 0 {
			errs = append(errs, field.Invalid(path.Child("tolerance"), proportional.Tolerance.String(),
				"the tolerance is a fraction of the target, so it is at most one"))
		} else if tolerance.Sign() <= 0 {
			errs = append(errs, field.Invalid(path.Child("tolerance"), proportional.Tolerance.String(),
				"the tolerance is a positive fraction of the target"))
		}
	}

	known := map[string]bool{}
	for i := range s.Spec.Roles {
		known[s.Spec.Roles[i].Name] = true
	}

	targetsPath := path.Child("roleTargets")
	seen := map[string]int{}
	for i := range proportional.RoleTargets {
		target := &proportional.RoleTargets[i]
		targetPath := targetsPath.Index(i)

		if !known[target.Name] {
			errs = append(errs, field.Invalid(targetPath.Child("name"), target.Name,
				"this role is not one of the roles this scaler plans for"))
		}
		if first, taken := seen[target.Name]; taken {
			errs = append(errs, field.Duplicate(targetPath.Child("name"), target.Name))
			errs = append(errs, field.Invalid(targetPath.Child("name"), target.Name,
				fmt.Sprintf("role %q already has a target at index %d", target.Name, first)))
			continue
		}
		seen[target.Name] = i

		errs = append(errs, validateModelDeploymentScalerPositiveQuantity(
			targetPath.Child("target"), &target.Target,
			"the target of a role is the signal one replica of it is expected to carry",
		)...)
	}

	return errs
}

// validateModelDeploymentScalerTokenThroughput 校验 token throughput 算法的参数。
func validateModelDeploymentScalerTokenThroughput(s *workercore.ModelDeploymentScaler) field.ErrorList {
	var errs field.ErrorList

	path := field.NewPath("spec", "algorithm", "tokenThroughput")
	if tt := s.Spec.Algorithm.TokenThroughput; tt != nil {
		errs = append(errs, validateModelDeploymentScalerTokenThroughputSpec(path, tt)...)
	}
	if predictive := s.Spec.Algorithm.Predictive; predictive != nil {
		errs = append(errs, validateModelDeploymentScalerTokenThroughputSpec(
			field.NewPath("spec", "algorithm", "predictive", "tokenThroughput"),
			&predictive.TokenThroughput,
		)...)
	}

	return errs
}

// validateModelDeploymentScalerTokenThroughputSpec 校验单个 token throughput 配置块携带的参数。
func validateModelDeploymentScalerTokenThroughputSpec(
	path *field.Path, tt *workercore.ModelDeploymentScalerTokenThroughputSpec,
) field.ErrorList {
	errs := make(field.ErrorList, 0, 6)

	errs = append(errs, validateModelDeploymentScalerPositiveQuantity(
		path.Child("prefillTargetTokensPerSecond"), tt.PrefillTargetTokensPerSecond,
		"the prefill target is the prompt-token rate one prefill replica sustains",
	)...)
	errs = append(errs, validateModelDeploymentScalerPositiveQuantity(
		path.Child("decodeTargetTokensPerSecond"), tt.DecodeTargetTokensPerSecond,
		"the decode target is the generation-token rate one decode replica sustains",
	)...)
	errs = append(errs, validateModelDeploymentScalerPositiveQuantity(
		path.Child("serverTargetTokensPerSecond"), tt.ServerTargetTokensPerSecond,
		"the server target is the token rate one server replica sustains",
	)...)
	errs = append(errs, validateModelDeploymentScalerPositiveQuantity(
		path.Child("panicThreshold"), tt.PanicThreshold,
		"the panic threshold is the growth rate over the ready replicas that starts a panic",
	)...)
	errs = append(errs, validateModelDeploymentScalerPositiveQuantity(
		path.Child("ttftTargetMilliseconds"), tt.TTFTTargetMilliseconds,
		"the time to first token target is the latency a role is expected to keep under",
	)...)
	errs = append(errs, validateModelDeploymentScalerPositiveQuantity(
		path.Child("itlTargetMilliseconds"), tt.ITLTargetMilliseconds,
		"the inter-token latency target is the latency a role is expected to keep under",
	)...)
	// 分位数按该 Quantity 所持有的精确值读取，因为所有比较逻辑都使用其解析后的精确数值。
	if tt.LatencyQuantile != nil {
		quantilePath := path.Child("latencyQuantile")
		quantile, ok := modelDeploymentScalerRatioValue(tt.LatencyQuantile)
		switch {
		case !ok:
			errs = append(errs, field.Invalid(quantilePath, tt.LatencyQuantile.String(),
				"the latency quantile must be a number"))
		case quantile.Sign() <= 0:
			errs = append(errs, field.Invalid(quantilePath, tt.LatencyQuantile.String(),
				"the latency quantile is a quantile, so it is greater than zero"))
		case quantile.Cmp(big.NewRat(1, 1)) > 0:
			errs = append(errs, field.Invalid(quantilePath, tt.LatencyQuantile.String(),
				"the latency quantile is a fraction of the distribution, so it is at most one"))
		}
	}
	if tt.StableWindowSeconds != 0 && (tt.StableWindowSeconds < 30 || tt.StableWindowSeconds > 600) {
		errs = append(errs, field.Invalid(
			path.Child("stableWindowSeconds"), tt.StableWindowSeconds,
			"the stable window is between 30 and 600 seconds",
		))
	}
	if tt.PanicWindowSeconds != 0 && (tt.PanicWindowSeconds < 10 || tt.PanicWindowSeconds > 300) {
		errs = append(errs, field.Invalid(
			path.Child("panicWindowSeconds"), tt.PanicWindowSeconds,
			"the panic window is between 10 and 300 seconds",
		))
	}

	return errs
}

// validateModelDeploymentScalerPredictive 校验 predictive 算法的参数。
func validateModelDeploymentScalerPredictive(s *workercore.ModelDeploymentScaler) field.ErrorList {
	predictive := s.Spec.Algorithm.Predictive
	if predictive == nil {
		return nil
	}

	errs := make(field.ErrorList, 0, 6)

	path := field.NewPath("spec", "algorithm", "predictive")
	errs = append(errs, validateModelDeploymentScalerPositiveQuantity(
		path.Child("prefillTokenCapacityPerReplica"), predictive.PrefillTokenCapacityPerReplica,
		"the prefill capacity is the prompt tokens one Prefill replica holds",
	)...)
	errs = append(errs, validateModelDeploymentScalerPositiveQuantity(
		path.Child("decodeTokenCapacityPerReplica"), predictive.DecodeTokenCapacityPerReplica,
		"the decode capacity is the generation tokens one Decode replica holds",
	)...)
	errs = append(errs, validateModelDeploymentScalerPositiveQuantity(
		path.Child("serverTokenCapacityPerReplica"), predictive.ServerTokenCapacityPerReplica,
		"the server capacity is the tokens one Server replica holds",
	)...)
	errs = append(errs, validateModelDeploymentScalerPositiveQuantity(
		path.Child("qLevel"), predictive.QLevel,
		"qLevel is the level process noise",
	)...)
	errs = append(errs, validateModelDeploymentScalerPositiveQuantity(
		path.Child("qTrend"), predictive.QTrend,
		"qTrend is the trend process noise",
	)...)
	errs = append(errs, validateModelDeploymentScalerPositiveQuantity(
		path.Child("measurementNoise"), predictive.MeasurementNoise,
		"measurementNoise is the observation noise",
	)...)

	return errs
}

// validateModelDeploymentScalerRatioFeasibility 拒绝因上下界与比例约束冲突而不存在任何
// 可行副本向量的 scaler。
//
// 一组无法同时满足的上下界与比例约束，会使 scaler 永远无法提交任何扩缩容决策：
// 它选出的每一个向量都会违反给定规则，从而要么永久锁死目标，要么为了调整目标而打破规则。
//
// 搜索在整数域上进行，因为副本数必须是整数，仅在实数域上满足比例的向量无法由
// operator 提交执行。搜索过程是精确的：它枚举受比例耦合的各 role 的副本向量，
// 并在搜索过程中依据每条比例约束不断收紧其余 role 的范围。
func validateModelDeploymentScalerRatioFeasibility(s *workercore.ModelDeploymentScaler) field.ErrorList {
	var errs field.ErrorList

	components := modelDeploymentScalerComponents(s)
	for _, component := range components {
		budget := modelDeploymentScalerFeasibilityBudget
		if component.feasible(&budget) {
			continue
		}
		if budget <= 0 {
			// 搜索无法做出判定。此时若直接拒绝，可能会误拒实际上可行的配置，
			// 且所有者无法通过编辑对象来修复此类问题。
			continue
		}
		errs = append(errs, field.Invalid(
			field.NewPath("spec", "roles"), component.roleNames(),
			"no replica vector inside these bounds satisfies every ratio at the same time, so "+
				"this scaler could never commit anything",
		))
	}

	return errs
}

// modelDeploymentScalerRatioBand 是以精确有理数区间表示的一条已声明比例约束。
type modelDeploymentScalerRatioBand struct {
	numerator   string
	denominator string
	minimum     *big.Rat
	maximum     *big.Rat
}

// modelDeploymentScalerComponent 是一组副本数相互关联约束的 role 集合。
//
// 未被任何比例约束提及的 role 各自独立成组且始终可行，因为在其自身上下界内的
// 任意副本数都能满足全部规则。
type modelDeploymentScalerComponent struct {
	roles  []string
	low    []int64
	high   []int64
	ratios []modelDeploymentScalerRatioBand
	index  map[string]int
}

// roleNames 列出组件中的角色名称，用于构建拒绝信息。
func (c *modelDeploymentScalerComponent) roleNames() string {
	return strings.Join(c.roles, ", ")
}

// modelDeploymentScalerComponents 按比例耦合关系将 scaler 的角色划分为若干集合。
func modelDeploymentScalerComponents(s *workercore.ModelDeploymentScaler) []*modelDeploymentScalerComponent {
	parent := make([]int, len(s.Spec.Roles))
	for i := range parent {
		parent[i] = i
	}

	position := map[string]int{}
	for i := range s.Spec.Roles {
		position[s.Spec.Roles[i].Name] = i
	}

	var bands []modelDeploymentScalerRatioBand
	for i := range s.Spec.Ratios {
		ratio := &s.Spec.Ratios[i]
		numerator, ok := position[ratio.NumeratorRole]
		if !ok {
			continue
		}
		denominator, ok := position[ratio.DenominatorRole]
		if !ok {
			continue
		}
		modelDeploymentScalerUnion(parent, numerator, denominator)

		band := modelDeploymentScalerRatioBand{
			numerator:   ratio.NumeratorRole,
			denominator: ratio.DenominatorRole,
		}
		if minimum, set := modelDeploymentScalerRatioValue(ratio.Min); set {
			band.minimum = minimum
		}
		if maximum, set := modelDeploymentScalerRatioValue(ratio.Max); set {
			band.maximum = maximum
		}
		bands = append(bands, band)
	}

	groups := map[int]*modelDeploymentScalerComponent{}
	for i := range s.Spec.Roles {
		root := modelDeploymentScalerFind(parent, i)
		component, ok := groups[root]
		if !ok {
			component = &modelDeploymentScalerComponent{index: map[string]int{}}
			groups[root] = component
		}
		component.index[s.Spec.Roles[i].Name] = len(component.roles)
		component.roles = append(component.roles, s.Spec.Roles[i].Name)
		component.low = append(component.low, int64(s.Spec.Roles[i].MinReplicas))
		component.high = append(component.high, int64(s.Spec.Roles[i].MaxReplicas))
	}

	for _, band := range bands {
		numerator := groups[modelDeploymentScalerFind(parent, position[band.numerator])]
		denominator := groups[modelDeploymentScalerFind(parent, position[band.denominator])]
		if numerator != denominator {
			continue
		}
		numerator.ratios = append(numerator.ratios, band)
	}

	components := make([]*modelDeploymentScalerComponent, 0, len(groups))
	for _, component := range groups {
		components = append(components, component)
	}

	return components
}

// modelDeploymentScalerFind 返回角色所属集合的根节点。
func modelDeploymentScalerFind(parent []int, i int) int {
	for parent[i] != i {
		parent[i] = parent[parent[i]]
		i = parent[i]
	}
	return i
}

// modelDeploymentScalerUnion 合并两个角色集合。
func modelDeploymentScalerUnion(parent []int, a, b int) {
	rootA, rootB := modelDeploymentScalerFind(parent, a), modelDeploymentScalerFind(parent, b)
	if rootA == rootB {
		return
	}
	parent[rootB] = rootA
}

// feasible 报告在上下界内是否存在满足所有比例约束的副本数向量。
func (c *modelDeploymentScalerComponent) feasible(budget *int) bool {
	if len(c.ratios) == 0 {
		return true
	}

	// 优先尝试两个显而易见的边界向量，因为若角色集合本身已满足约束，可直接命中其中之一而无需运行搜索。
	for _, candidate := range [][]int64{c.uniform(c.low), c.uniform(c.high)} {
		if c.holds(candidate) {
			return true
		}
	}

	return c.search(0, c.low, c.high, budget)
}

// uniform 复制一个边界向量，以便搜索过程对其进行测试而不修改组件的原始边界。
func (c *modelDeploymentScalerComponent) uniform(values []int64) []int64 {
	vector := make([]int64, len(c.roles))
	copy(vector, values)
	return vector
}

// holds 报告给定向量是否满足组件的所有比例约束。
func (c *modelDeploymentScalerComponent) holds(vector []int64) bool {
	for _, band := range c.ratios {
		numerator := vector[c.index[band.numerator]]
		denominator := vector[c.index[band.denominator]]
		if denominator == 0 {
			return false
		}
		quotient := new(big.Rat).SetFrac(big.NewInt(numerator), big.NewInt(denominator))
		if band.minimum != nil && quotient.Cmp(band.minimum) < 0 {
			return false
		}
		if band.maximum != nil && quotient.Cmp(band.maximum) > 0 {
			return false
		}
	}
	return true
}

// search 尝试查找可行向量，每次为一个角色赋值，并随搜索推进根据比例约束收紧其余角色的上下界。
func (c *modelDeploymentScalerComponent) search(pos int, low, high []int64, budget *int) bool {
	if pos >= len(c.roles) {
		return c.holds(low)
	}
	if *budget <= 0 {
		return false
	}

	index := c.index[c.roles[pos]]
	for value := low[index]; value <= high[index]; value++ {
		*budget--
		if *budget <= 0 {
			return false
		}

		nextLow := append([]int64(nil), low...)
		nextHigh := append([]int64(nil), high...)
		nextLow[index], nextHigh[index] = value, value
		if !c.propagate(index, nextLow, nextHigh) {
			continue
		}
		if c.search(pos+1, nextLow, nextHigh, budget) {
			return true
		}
	}

	return false
}

// propagate 收紧受已知副本数约束的其余角色的上下界。
//
// 该过程绝不会修改已选定的副本数。收紧已选定角色的区间只会使其变空，而空区间表示该分支被剪枝，而非改写已选副本数。
func (c *modelDeploymentScalerComponent) propagate(index int, low, high []int64) bool {
	for _, band := range c.ratios {
		numerator := c.index[band.numerator]
		denominator := c.index[band.denominator]
		switch {
		case numerator == index:
			if !c.tightenDenominator(low, high, denominator, numerator, band) {
				return false
			}
		case denominator == index:
			if !c.tightenNumerator(low, high, numerator, denominator, band) {
				return false
			}
		}
	}
	return true
}

// tightenNumerator 在分母已知的情况下约束比例中分子的上下界。
//
// 比例区间限定商值位于上下界之间，因此分子必然位于上下界分别乘以分母所得范围之间。
func (c *modelDeploymentScalerComponent) tightenNumerator(
	low, high []int64, numerator, denominator int, band modelDeploymentScalerRatioBand,
) bool {
	value := big.NewRat(low[denominator], 1)
	if band.minimum != nil {
		bound := modelDeploymentScalerCeil(new(big.Rat).Mul(band.minimum, value))
		if bound > low[numerator] {
			low[numerator] = bound
		}
	}
	if band.maximum != nil {
		bound := modelDeploymentScalerFloor(new(big.Rat).Mul(band.maximum, value))
		if bound < high[numerator] {
			high[numerator] = bound
		}
	}
	return low[numerator] <= high[numerator]
}

// tightenDenominator 在分子已知的情况下约束比例中分母的上下界。
//
// 比例区间限定商值位于上下界之间，因此分母必然位于分子除以上界与分子除以下界所得范围之间。
func (c *modelDeploymentScalerComponent) tightenDenominator(
	low, high []int64, denominator, numerator int, band modelDeploymentScalerRatioBand,
) bool {
	value := big.NewRat(low[numerator], 1)
	if band.maximum != nil {
		bound := modelDeploymentScalerCeil(new(big.Rat).Quo(value, band.maximum))
		if bound > low[denominator] {
			low[denominator] = bound
		}
	}
	if band.minimum != nil {
		bound := modelDeploymentScalerFloor(new(big.Rat).Quo(value, band.minimum))
		if bound < high[denominator] {
			high[denominator] = bound
		}
	}
	return low[denominator] <= high[denominator]
}

// modelDeploymentScalerCeil 对有理数向上取整。
func modelDeploymentScalerCeil(ratio *big.Rat) int64 {
	quotient, remainder := new(big.Int).QuoRem(ratio.Num(), ratio.Denom(), new(big.Int))
	if remainder.Sign() > 0 {
		quotient.Add(quotient, big.NewInt(1))
	}
	return quotient.Int64()
}

// modelDeploymentScalerFloor 对有理数向下取整。
func modelDeploymentScalerFloor(ratio *big.Rat) int64 {
	quotient, _ := new(big.Int).QuoRem(ratio.Num(), ratio.Denom(), new(big.Int))
	return quotient.Int64()
}

// validateModelDeploymentScalerAgainstTarget 包含需要结合目标对象才能判定的校验规则。
//
// scaler 所规划的角色必须属于目标对象，因此引用目标对象不存在的角色将无法进行有效规划，而引用目标对象不支持扩缩容的角色则会使扩缩容建议失去意义。
func (r *ModelDeploymentScalerWebhook) validateModelDeploymentScalerAgainstTarget(
	ctx context.Context, s *workercore.ModelDeploymentScaler, md *workercore.ModelDeployment, isUpdate bool,
) (ctrladmission.Warnings, field.ErrorList) {
	var (
		warnings ctrladmission.Warnings
		errs     field.ErrorList
	)

	declared := map[string]*workercore.ModelDeploymentRole{}
	for i := range md.Spec.Roles {
		declared[md.Spec.Roles[i].Name] = &md.Spec.Roles[i]
	}

	rolesPath := field.NewPath("spec", "roles")
	for i := range s.Spec.Roles {
		bounds := &s.Spec.Roles[i]
		rolePath := rolesPath.Index(i)

		role, ok := declared[bounds.Name]
		if !ok {
			errs = append(errs, field.Invalid(rolePath.Child("name"), bounds.Name,
				"the target does not have a role with this name, so there is nothing to scale"))
			continue
		}
		if len(role.Command) > 0 {
			errs = append(errs, field.Invalid(rolePath.Child("name"), bounds.Name,
				"this role runs a command of its own, and a scaler cannot tell what that command "+
					"serves, so it cannot tell when the role is busy"))
		}
		if role.ElasticEP != nil {
			errs = append(errs, field.Invalid(rolePath.Child("name"), bounds.Name,
				"this role runs the elastic-EP profile, which spreads its width across the "+
					"members of one replica, so another replica would add no capacity"))
		}
		errs = append(errs, validateModelDeploymentScalerRoleNames(md, role, bounds, rolePath)...)
	}

	if s.Spec.Algorithm.CapacityPrior != nil {
		catalogWarnings, catalogErrs := r.validateModelDeploymentScalerCatalogRoles(ctx, s, md, declared, isUpdate)
		warnings = append(warnings, catalogWarnings...)
		errs = append(errs, catalogErrs...)
		return warnings, errs
	}

	errs = append(errs, validateModelDeploymentScalerAlgorithmRoles(s, declared)...)

	return warnings, errs
}

// validateModelDeploymentScalerCatalogRoles 为请求容量先验的 scaler 校验角色耦合关系、显式延迟目标以及目录先验解析。
func (r *ModelDeploymentScalerWebhook) validateModelDeploymentScalerCatalogRoles(
	ctx context.Context, s *workercore.ModelDeploymentScaler, md *workercore.ModelDeployment,
	declared map[string]*workercore.ModelDeploymentRole, isUpdate bool,
) (ctrladmission.Warnings, field.ErrorList) {
	var (
		warnings ctrladmission.Warnings
		errs     field.ErrorList
	)

	algorithmName := "the token throughput algorithm"
	tt := s.Spec.Algorithm.TokenThroughput
	ttPath := field.NewPath("spec", "algorithm", "tokenThroughput")
	if s.Spec.Algorithm.Type == workercore.ModelDeploymentScalerAlgorithmTypePredictive {
		algorithmName = "the predictive algorithm"
		if s.Spec.Algorithm.Predictive != nil {
			tt = &s.Spec.Algorithm.Predictive.TokenThroughput
		}
		ttPath = field.NewPath("spec", "algorithm", "predictive", "tokenThroughput")
	}

	coupled := modelDeploymentScalerCoupledToKinds(s, declared,
		workercore.ModelDeploymentRoleKindPrefill, workercore.ModelDeploymentRoleKindDecode)

	rolesPath := field.NewPath("spec", "roles")
	for i := range s.Spec.Roles {
		bounds := &s.Spec.Roles[i]
		role, ok := declared[bounds.Name]
		if !ok {
			continue
		}
		if workerctrl.ModelDeploymentEffectiveRoleKind(role) == workercore.ModelDeploymentRoleKindEncode && !coupled[bounds.Name] {
			errs = append(errs, field.Invalid(rolesPath.Index(i).Child("name"), bounds.Name,
				"an encode role is scaled by the role it is coupled to, and this one is "+
					"coupled to no prefill or decode role, so "+algorithmName+" has nothing to "+
					"scale it from"))
		}
	}

	if !workerctrl.ModelDeploymentScalerNeedsCapacityPrior(s, md) {
		return warnings, errs
	}

	for i := range s.Spec.Roles {
		bounds := &s.Spec.Roles[i]
		role, ok := declared[bounds.Name]
		if !ok {
			continue
		}
		kind := workerctrl.ModelDeploymentEffectiveRoleKind(role)
		if kind == workercore.ModelDeploymentRoleKindEncode || !modelDeploymentScalerRoleNeedsCapacityPrior(s, kind) {
			continue
		}
		if (kind == workercore.ModelDeploymentRoleKindPrefill || kind == workercore.ModelDeploymentRoleKindServer) &&
			(tt == nil || tt.TTFTTargetMilliseconds == nil) {
			errs = append(errs, field.Required(
				ttPath.Child("ttftTargetMilliseconds"),
				fmt.Sprintf("role %q (%s) resolves capacity from a catalog prior and requires an explicit ttftTargetMilliseconds", bounds.Name, kind),
			))
		}
		if (kind == workercore.ModelDeploymentRoleKindDecode || kind == workercore.ModelDeploymentRoleKindServer) &&
			(tt == nil || tt.ITLTargetMilliseconds == nil) {
			errs = append(errs, field.Required(
				ttPath.Child("itlTargetMilliseconds"),
				fmt.Sprintf("role %q (%s) resolves capacity from a catalog prior and requires an explicit itlTargetMilliseconds", bounds.Name, kind),
			))
		}
	}

	if len(errs) > 0 {
		return warnings, errs
	}

	if _, err := workerctrl.ResolveModelDeploymentScalerCapacityPriors(ctx, r.APIReader, s, md); err != nil {
		var refusal *priors.RefusalError
		if errors.As(err, &refusal) {
			if isUpdate {
				warnings = append(warnings, fmt.Sprintf(
					"capacity prior cannot be resolved (%s); admitting update so the scaler can be repaired or placed in Shadow mode",
					refusal.Error(),
				))
				return warnings, errs
			}
			errs = append(errs, field.Invalid(
				field.NewPath("spec", "algorithm", "capacityPrior"),
				s.Spec.Algorithm.CapacityPrior,
				refusal.Error(),
			))
			return warnings, errs
		}
		errs = append(errs, field.InternalError(field.NewPath("spec", "algorithm", "capacityPrior"), err))
	}

	return warnings, errs
}

// modelDeploymentScalerRoleNeedsCapacityPrior 报告某种角色类型在 scaler 算法配置中是否缺少必需的容量字段。
func modelDeploymentScalerRoleNeedsCapacityPrior(
	s *workercore.ModelDeploymentScaler, kind workercore.ModelDeploymentRoleKind,
) bool {
	probe := &workercore.ModelDeployment{
		Spec: workercore.ModelDeploymentSpec{
			Roles: []workercore.ModelDeploymentRole{{Kind: kind}},
		},
	}
	return workerctrl.ModelDeploymentScalerNeedsCapacityPrior(s, probe)
}

// validateModelDeploymentScalerUnresolvedTargetSLOs 在目标对象尚未创建前即创建 scaler 且容量先验解析抑制了默认延迟目标时，若 scaler 遗漏了任意未指定容量类型所需的延迟目标，则拒绝该 scaler。
func validateModelDeploymentScalerUnresolvedTargetSLOs(
	s *workercore.ModelDeploymentScaler,
) field.ErrorList {
	var (
		errs   field.ErrorList
		tt     *workercore.ModelDeploymentScalerTokenThroughputSpec
		ttPath *field.Path
	)
	switch s.Spec.Algorithm.Type {
	case workercore.ModelDeploymentScalerAlgorithmTypeTokenThroughput:
		tt = s.Spec.Algorithm.TokenThroughput
		ttPath = field.NewPath("spec", "algorithm", "tokenThroughput")
	case workercore.ModelDeploymentScalerAlgorithmTypePredictive:
		if s.Spec.Algorithm.Predictive != nil {
			tt = &s.Spec.Algorithm.Predictive.TokenThroughput
		}
		ttPath = field.NewPath("spec", "algorithm", "predictive", "tokenThroughput")
	default:
		return nil
	}

	needsServer := modelDeploymentScalerRoleNeedsCapacityPrior(s, workercore.ModelDeploymentRoleKindServer)
	needsPrefill := modelDeploymentScalerRoleNeedsCapacityPrior(s, workercore.ModelDeploymentRoleKindPrefill)
	needsDecode := modelDeploymentScalerRoleNeedsCapacityPrior(s, workercore.ModelDeploymentRoleKindDecode)

	needsTTFT := needsServer || needsPrefill
	needsITL := needsServer || needsDecode
	if needsTTFT && (tt == nil || tt.TTFTTargetMilliseconds == nil) {
		errs = append(errs, field.Required(
			ttPath.Child("ttftTargetMilliseconds"),
			"a scaler that resolves capacity from a catalog prior before its target exists requires an explicit ttftTargetMilliseconds",
		))
	}
	if needsITL && (tt == nil || tt.ITLTargetMilliseconds == nil) {
		errs = append(errs, field.Required(
			ttPath.Child("itlTargetMilliseconds"),
			"a scaler that resolves capacity from a catalog prior before its target exists requires an explicit itlTargetMilliseconds",
		))
	}
	return errs
}

// validateModelDeploymentScalerRoleNames 拒绝会使生成对象名称超出集群长度限制的副本数上界。
//
// 副本名称取决于其序号，因此必须确保允许最大副本数的上界能够容纳。此处按实际允许的最大值测算而非按最坏情况预留，因为按最坏情况预留会挤压所有部署的名称可用字符数以覆盖实际无人使用的极大副本数。
func validateModelDeploymentScalerRoleNames(
	md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole,
	bounds *workercore.ModelDeploymentScalerRoleBounds, rolePath *field.Path,
) field.ErrorList {
	var errs field.ErrorList

	ordinal := int(bounds.MaxReplicas) - 1
	if ordinal < 0 {
		ordinal = 0
	}

	group := fmt.Sprintf("%s-%s-r%d", md.Name, role.Name, ordinal)
	if why := validation.IsDNS1123Label(group); len(why) > 0 {
		errs = append(errs, field.Invalid(rolePath.Child("maxReplicas"), bounds.MaxReplicas,
			fmt.Sprintf("at %d replicas a replica of this role is named %q (%d characters), which "+
				"cannot be the name of the Pod group or of the Workload that reserves its quota: "+
				"%s. Shorten the role or the deployment, or declare fewer replicas",
				bounds.MaxReplicas, group, len(group), strings.Join(why, "; "))))
	}

	if int(role.ReplicaSize) > 1 {
		member := fmt.Sprintf("%s-%s-r%d-m%d", md.Name, role.Name, ordinal, int(role.ReplicaSize)-1)
		if why := validation.IsDNS1123Label(member); len(why) > 0 {
			errs = append(errs, field.Invalid(rolePath.Child("maxReplicas"), bounds.MaxReplicas,
				fmt.Sprintf("at %d replicas a member of this role is named %q (%d characters), "+
					"which cannot be a Pod name: %s. Shorten the role or the deployment, or "+
					"declare fewer replicas",
					bounds.MaxReplicas, member, len(member), strings.Join(why, "; "))))
		}
	}

	return errs
}

// validateModelDeploymentScalerAlgorithmRoles 包含将扩缩容算法与所规划角色类型耦合起来的校验规则。
//
// 算法按角色类型读取对应指标信号，因此算法没有信号来源的角色类型无法纳入规划。Encode 角色是例外：它依据所耦合的角色而非自身信号进行扩缩容，因此需要配置与 Prefill 或 Decode 角色之间的比例关系。
func validateModelDeploymentScalerAlgorithmRoles(
	s *workercore.ModelDeploymentScaler, declared map[string]*workercore.ModelDeploymentRole,
) field.ErrorList {
	var errs field.ErrorList

	switch s.Spec.Algorithm.Type {
	case workercore.ModelDeploymentScalerAlgorithmTypeTokenThroughput:
		tt := s.Spec.Algorithm.TokenThroughput
		if tt == nil {
			return errs
		}
		errs = append(errs, validateModelDeploymentScalerTokenThroughputRoles(s, declared,
			func(kind workercore.ModelDeploymentRoleKind) *resource.Quantity {
				switch kind {
				case workercore.ModelDeploymentRoleKindPrefill:
					return tt.PrefillTargetTokensPerSecond
				case workercore.ModelDeploymentRoleKindDecode:
					return tt.DecodeTargetTokensPerSecond
				default:
					return tt.ServerTargetTokensPerSecond
				}
			}, "the token throughput algorithm")...)
	case workercore.ModelDeploymentScalerAlgorithmTypePredictive:
		predictive := s.Spec.Algorithm.Predictive
		if predictive == nil {
			return errs
		}
		errs = append(errs, validateModelDeploymentScalerTokenThroughputRoles(s, declared,
			func(kind workercore.ModelDeploymentRoleKind) *resource.Quantity {
				switch kind {
				case workercore.ModelDeploymentRoleKindPrefill:
					return predictive.TokenThroughput.PrefillTargetTokensPerSecond
				case workercore.ModelDeploymentRoleKindDecode:
					return predictive.TokenThroughput.DecodeTargetTokensPerSecond
				default:
					return predictive.TokenThroughput.ServerTargetTokensPerSecond
				}
			}, "the predictive algorithm")...)
		errs = append(errs, validateModelDeploymentScalerPredictiveCapacities(s, declared)...)
	case workercore.ModelDeploymentScalerAlgorithmTypeProportional:
		errs = append(errs, validateModelDeploymentScalerProportionalRoles(s, declared)...)
	}

	return errs
}

// validateModelDeploymentScalerTokenThroughputRoles 包含如下规则：若角色类型在算法中没有对应的目标配置，则予以拒绝。
func validateModelDeploymentScalerTokenThroughputRoles(
	s *workercore.ModelDeploymentScaler, declared map[string]*workercore.ModelDeploymentRole,
	target func(workercore.ModelDeploymentRoleKind) *resource.Quantity, algorithm string,
) field.ErrorList {
	var errs field.ErrorList

	coupled := modelDeploymentScalerCoupledToKinds(s, declared,
		workercore.ModelDeploymentRoleKindPrefill, workercore.ModelDeploymentRoleKindDecode)

	rolesPath := field.NewPath("spec", "roles")
	for i := range s.Spec.Roles {
		bounds := &s.Spec.Roles[i]
		rolePath := rolesPath.Index(i)

		role, ok := declared[bounds.Name]
		if !ok {
			continue
		}
		kind := role.Kind
		if kind == "" {
			kind = workercore.ModelDeploymentRoleKindServer
		}

		if kind == workercore.ModelDeploymentRoleKindEncode {
			if !coupled[bounds.Name] {
				errs = append(errs, field.Invalid(rolePath.Child("name"), bounds.Name,
					"an encode role is scaled by the role it is coupled to, and this one is "+
						"coupled to no prefill or decode role, so "+algorithm+" has nothing to "+
						"scale it from"))
			}
			continue
		}

		if target(kind) == nil {
			errs = append(errs, field.Invalid(rolePath.Child("name"), bounds.Name,
				fmt.Sprintf("this role is a %s role, and %s needs the target of that kind to "+
					"know how much one replica of it serves", kind, algorithm)))
		}
	}

	return errs
}

// validateModelDeploymentScalerPredictiveCapacities 包含如下规则：预测算法需要获取其规划的每一种角色类型的容量。
func validateModelDeploymentScalerPredictiveCapacities(
	s *workercore.ModelDeploymentScaler, declared map[string]*workercore.ModelDeploymentRole,
) field.ErrorList {
	var errs field.ErrorList

	predictive := s.Spec.Algorithm.Predictive
	if predictive == nil {
		return errs
	}

	rolesPath := field.NewPath("spec", "roles")
	for i := range s.Spec.Roles {
		bounds := &s.Spec.Roles[i]
		rolePath := rolesPath.Index(i)

		role, ok := declared[bounds.Name]
		if !ok {
			continue
		}
		kind := role.Kind
		if kind == "" {
			kind = workercore.ModelDeploymentRoleKindServer
		}

		var capacity *resource.Quantity
		switch kind {
		case workercore.ModelDeploymentRoleKindPrefill:
			capacity = predictive.PrefillTokenCapacityPerReplica
		case workercore.ModelDeploymentRoleKindDecode:
			capacity = predictive.DecodeTokenCapacityPerReplica
		case workercore.ModelDeploymentRoleKindEncode:
			continue
		default:
			capacity = predictive.ServerTokenCapacityPerReplica
		}

		if capacity == nil {
			errs = append(errs, field.Invalid(rolePath.Child("name"), bounds.Name,
				fmt.Sprintf("this role is a %s role, and the predictive algorithm needs the "+
					"capacity of that kind to know how many replicas the demand needs", kind)))
		}
	}

	return errs
}

// validateModelDeploymentScalerProportionalRoles 包含如下规则：每个参与扩缩容的角色要么自身拥有目标配置，要么耦合到拥有目标配置的角色。
func validateModelDeploymentScalerProportionalRoles(
	s *workercore.ModelDeploymentScaler, declared map[string]*workercore.ModelDeploymentRole,
) field.ErrorList {
	var errs field.ErrorList

	proportional := s.Spec.Algorithm.Proportional
	if proportional == nil {
		return errs
	}

	targeted := map[string]bool{}
	for i := range proportional.RoleTargets {
		targeted[proportional.RoleTargets[i].Name] = true
	}
	coupled := modelDeploymentScalerCoupledToTargeted(s, targeted)

	rolesPath := field.NewPath("spec", "roles")
	for i := range s.Spec.Roles {
		bounds := &s.Spec.Roles[i]
		rolePath := rolesPath.Index(i)
		if _, ok := declared[bounds.Name]; !ok {
			continue
		}
		if targeted[bounds.Name] || coupled[bounds.Name] {
			continue
		}
		errs = append(errs, field.Invalid(rolePath.Child("name"), bounds.Name,
			"the proportional algorithm divides what it observed by the target of a role, and "+
				"this role has no target of its own and is coupled to no role that has one"))
	}

	return errs
}

// modelDeploymentScalerCoupledToKinds 列出通过比例关系耦合到指定类型角色的所有角色名称。
//
// 只有与不同类型角色耦合才有效，因为与同类型另一个角色耦合并不会引入算法可区分读取的新信号。
func modelDeploymentScalerCoupledToKinds(
	s *workercore.ModelDeploymentScaler, declared map[string]*workercore.ModelDeploymentRole,
	kinds ...workercore.ModelDeploymentRoleKind,
) map[string]bool {
	wanted := map[string]bool{}
	for _, kind := range kinds {
		wanted[string(kind)] = true
	}

	kindOf := func(name string) string {
		role, ok := declared[name]
		if !ok {
			return ""
		}
		if role.Kind == "" {
			return string(workercore.ModelDeploymentRoleKindServer)
		}
		return string(role.Kind)
	}

	coupled := map[string]bool{}
	for i := range s.Spec.Ratios {
		ratio := &s.Spec.Ratios[i]
		if wanted[kindOf(ratio.NumeratorRole)] {
			coupled[ratio.DenominatorRole] = true
		}
		if wanted[kindOf(ratio.DenominatorRole)] {
			coupled[ratio.NumeratorRole] = true
		}
	}

	return coupled
}

// modelDeploymentScalerCoupledToTargeted 列出通过比例关系耦合到自身带有目标配置的角色的所有角色名称。
func modelDeploymentScalerCoupledToTargeted(
	s *workercore.ModelDeploymentScaler, targeted map[string]bool,
) map[string]bool {
	coupled := map[string]bool{}
	for i := range s.Spec.Ratios {
		ratio := &s.Spec.Ratios[i]
		if targeted[ratio.NumeratorRole] {
			coupled[ratio.DenominatorRole] = true
		}
		if targeted[ratio.DenominatorRole] {
			coupled[ratio.NumeratorRole] = true
		}
	}

	return coupled
}
