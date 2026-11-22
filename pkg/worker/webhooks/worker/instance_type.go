package worker

import (
	"context"
	"strings"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrladmission "sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/systemname"
	"gpustack.ai/gpustack/pkg/utils/strconvx"
	"gpustack.ai/gpustack/pkg/webhook"
	"gpustack.ai/gpustack/pkg/worker/settings"
)

// QueueEntranceLabelKey 在 InstanceType 上记录位于其底层 ClusterQueue 前端的命名空间级
// LocalQueue 名称（即 Workload 的 "kueue.x-k8s.io/queue-name" 值）。Pod webhook 通过该标签
// 反向查找 InstanceType 以读取观测到的单卡显存（Status.Detail.Memory），而绝不信任用户可写的 LocalQueue。
// Default webhook 会将其标记为 nodefeature.FormatLocalQueueName(<InstanceType name>)。
const QueueEntranceLabelKey = "schedule." + systemname.LabelPrefix + "queue-entrance"

// InstanceTypeWebhook 对 v1alpha1.InstanceType 执行默认值填充与校验。
//
// defaulting webhook 仅写入调度区分标签与入口标签；观测到的硬件描述符由 reconciler 计算并写入
// Status.Detail，而不会在准入阶段补充到 spec 上。validating webhook 在创建时要求提供管理员可写的
// 输入字段，并在更新时冻结 spec。
//
// nolint: lll
// +k8s:webhook-gen:validating:group="worker.gpustack.ai",version="v1alpha1",resource="instancetypes",scope="Cluster"
// +k8s:webhook-gen:validating:operations=["CREATE","UPDATE"],failurePolicy="Fail",sideEffects="None",matchPolicy="Equivalent",timeoutSeconds=10
// +k8s:webhook-gen:mutating:group="worker.gpustack.ai",version="v1alpha1",resource="instancetypes",scope="Cluster"
// +k8s:webhook-gen:mutating:operations=["CREATE","UPDATE"],failurePolicy="Fail",sideEffects="None",matchPolicy="Equivalent",timeoutSeconds=10
type InstanceTypeWebhook struct {
	Client    ctrlcli.Client
	APIReader ctrlcli.Reader
}

func (r *InstanceTypeWebhook) SetupWebhook(_ context.Context, opts webhook.SetupOptions) (runtime.Object, error) {
	r.Client = opts.Manager.GetClient()
	r.APIReader = opts.Manager.GetAPIReader()

	return &workercore.InstanceType{}, nil
}

var (
	_ ctrladmission.Validator[runtime.Object] = (*InstanceTypeWebhook)(nil)
	_ ctrladmission.Defaulter[runtime.Object] = (*InstanceTypeWebhook)(nil)
	_ webhook.ReceiveDeletionUpdate           = (*InstanceTypeWebhook)(nil)
)

// ReceiveDeletionUpdate 使该 webhook 在 InstanceType 处于删除过程中时仍持续校验其更新。
// validateInstanceTypeSpecImmutable 对新旧两个 spec 执行掩码相等性比较且不读取其他内容，
// 从而确保在 reconciler 排空该类型的 ClusterQueue 期间（而不仅仅在存活期间）spec 保持冻结。
//
// 同一个标记也会放行 mutating 部分，而在此处这不会导致任何拒绝：Default 在所有路径上都返回 nil，
// 且其在 CREATE 上注册的 failurePolicy 为 Fail，因此每个已存储的 InstanceType 都已经过该处理，
// 重新推导其标签是幂等的。
func (r *InstanceTypeWebhook) ReceiveDeletionUpdate() {}

func (r *InstanceTypeWebhook) ValidateCreate(_ context.Context, obj runtime.Object) (ctrladmission.Warnings, error) {
	it := obj.(*workercore.InstanceType)

	if errs := validateInstanceTypeSpec(it); len(errs) > 0 {
		return nil, kerrors.NewInvalid(workercore.Kind("InstanceType"), it.Name, errs)
	}
	return nil, nil
}

func (r *InstanceTypeWebhook) ValidateUpdate(_ context.Context, oldObj, newObj runtime.Object) (ctrladmission.Warnings, error) {
	itOld, it := oldObj.(*workercore.InstanceType), newObj.(*workercore.InstanceType)

	// 更新时仅强制执行不可变性校验：除 displayName 和 inactive 外，spec 均处于冻结状态，
	// 因此 validateInstanceTypeSpec 在创建时检查的所有输入都已冻结为其存储值。
	// 在此处重新运行创建时校验不会增加任何保护，反而会卡住那些在规则收紧前就已存储的存量对象
	// （例如单位 CPU 不为 1 的纯 CPU 类型），甚至阻塞对 displayName/inactive 的编辑或 operator 自身的 inactive 回填。
	if errs := validateInstanceTypeSpecImmutable(itOld, it); len(errs) > 0 {
		return nil, kerrors.NewInvalid(workercore.Kind("InstanceType"), itOld.Name, errs)
	}
	return nil, nil
}

func (r *InstanceTypeWebhook) ValidateDelete(_ context.Context, _ runtime.Object) (ctrladmission.Warnings, error) {
	return nil, nil
}

// Default 为 InstanceType 打上元数据标签，使其从创建之初即可被选中。它根据 spec 标识派生
// (acceleratable, group) 特征键，并与 kubernetes.io/os|arch 一起，打上资源池中 Devices 与
// ResourceFlavor 所携带的调度区分标签（同时清理因 group 或 acceleratable 变更而遗留的旧特征键），
// 此外还会添加供 Pod webhook 反向查找的入口标签（nodefeature.FormatLocalQueueName(name)）。
// 该方法不写入硬件描述符：观测到的描述符位于 Status.Detail 中，由 reconciler 随硬件变化计算得出，
// 而非在此处快照到 spec 上。
func (r *InstanceTypeWebhook) Default(ctx context.Context, obj runtime.Object) error {
	it := obj.(*workercore.InstanceType)

	// 将 CPU 分组默认设为通用哨兵值，使资源池在管理员未配置该项时也能聚合归并；
	// 仅当开启感知开关时，具体的 CPU 分组才有意义。
	if it.Spec.GeneralGroup == "" {
		it.Spec.GeneralGroup = nodefeature.GeneralGroupGeneric
	}
	// 加速类型需要指定 accelerator group 才能调度到对应的资源池；
	// 此处不打上退化的标签，而是留给 validating webhook 拒绝。
	if it.Spec.Acceleratable && it.Spec.AcceleratorGroup == "" {
		return nil
	}

	// 根据 spec 标识与感知设置生成资源池的调度标签。此处读取该设置（worker 进程携带本地设置索引器）；
	// 校验逻辑则保持与设置无关。
	cpuAware := settings.InstanceTypeAwareCPUManufacturer.ShouldValueBool(ctx)
	sched := nodefeature.PoolScheduleLabels(
		it.Spec.Acceleratable, cpuAware,
		it.Spec.GeneralGroup, it.Spec.AcceleratorGroup,
		it.Spec.OS, it.Spec.Arch,
	)

	// 在 InstanceType 上打上调度标签，使其可通过与 Devices 和 ResourceFlavor 相同的区分标签被选中。
	// 移除因 group 或 acceleratable 变更而遗留的旧特征键——特征键的名称会变化，
	// 而 os/arch 以及 acceleratable 布尔值的键名固定，直接覆盖即可。
	if it.Labels == nil {
		it.Labels = make(map[string]string, len(sched)+1)
	}
	for k := range it.Labels {
		if _, want := sched[k]; want {
			continue
		}
		if strings.HasPrefix(k, nodefeature.GeneralFeatureLabelPrefix) ||
			strings.HasPrefix(k, nodefeature.AcceleratableFeatureLabelPrefix) {
			delete(it.Labels, k)
		}
	}
	for k, v := range sched {
		it.Labels[k] = v
	}
	// 广播前置的 LocalQueue 名称，以便 Pod webhook 反向查找该 InstanceType，
	// 获取观测到的单卡显存（Status.Detail.Memory）。
	it.Labels[QueueEntranceLabelKey] = nodefeature.FormatLocalQueueName(it.Name)

	return nil
}

// validateInstanceTypeSpec 收集必填项错误：启用加速时的 accelerator group、非空的 os/arch，
// 以及单元规格的格式规范检查。它不读取任何可编辑的设置——校验逻辑与
// instance-type-aware-cpu-manufacturer 无关；mutating webhook 会将空的 GeneralGroup
// 默认设为 "generic"，因此此处从不要求必填 GeneralGroup。
func validateInstanceTypeSpec(it *workercore.InstanceType) field.ErrorList {
	var errs field.ErrorList
	if it.Spec.Acceleratable && it.Spec.AcceleratorGroup == "" {
		errs = append(errs, field.Required(field.NewPath("spec", "acceleratorGroup"),
			"must be specified when acceleratable is true"))
	}
	if it.Spec.OS == "" {
		errs = append(errs, field.Required(field.NewPath("spec", "os"), "must be specified"))
	}
	if it.Spec.Arch == "" {
		errs = append(errs, field.Required(field.NewPath("spec", "arch"), "must be specified"))
	}
	return append(errs, validateInstanceTypeUnitSpec(it)...)
}

// validateInstanceTypeUnitSpec 强制校验单元规格：三个字段均须设置且格式合法——unitRAM 与
// localStorage 必须为带区分大小写 "Gi" 后缀的正整数。unitCPU 规则根据 acceleratable 分支：
// 可加速类型接受任意无单位正整数，而非加速（纯 CPU）类型仅接受恰好为 1 核，
// 以匹配纯 CPU 请求的计量方式（请求的 CPU 核数即单元数）。
func validateInstanceTypeUnitSpec(it *workercore.InstanceType) field.ErrorList {
	cpu, ram, localStg := it.Spec.UnitResources.CPU, it.Spec.UnitResources.RAM, it.Spec.LocalStorage

	var errs field.ErrorList
	if it.Spec.Acceleratable {
		if extractPositiveNumberFromString(cpu) == "" {
			errs = append(errs, field.Invalid(field.NewPath("spec", "unitResources", "cpu"),
				cpu, "must be a positive integer with no unit suffix"))
		}
	} else if n, err := strconvx.ParseInt[int32](cpu, 10, 32); err != nil || n != 1 {
		errs = append(errs, field.Invalid(field.NewPath("spec", "unitResources", "cpu"),
			cpu, "must be 1 when acceleratable is false"))
	}
	if extractPositiveNumberFromQuantity(ram, "Gi") == "" {
		errs = append(errs, field.Invalid(field.NewPath("spec", "unitResources", "ram"),
			ram, "must be a positive integer with a case-sensitive \"Gi\" suffix"))
	}
	if extractPositiveNumberFromQuantity(localStg, "Gi") == "" {
		errs = append(errs, field.Invalid(field.NewPath("spec", "localStorage"),
			localStg, "must be a positive integer with a case-sensitive \"Gi\" suffix"))
	}
	return errs
}

// extractPositiveNumberFromString 在 v 为不带单位后缀的正整数时原样返回 v，
// 否则返回 ""（包括空输入）。
func extractPositiveNumberFromString(v string) string {
	n, err := strconvx.ParseInt[int32](v, 10, 32)
	if err == nil && n > 0 {
		return v
	}
	return ""
}

// extractPositiveNumberFromQuantity 剥离给定后缀并返回纯正整数，
// 当后缀缺失或剩余部分不是正整数时返回 ""（包括空输入）。
func extractPositiveNumberFromQuantity(v, suffix string) string {
	b, ok := strings.CutSuffix(v, suffix)
	if ok {
		return extractPositiveNumberFromString(b)
	}
	return ""
}

// validateInstanceTypeSpecImmutable 拒绝对象创建后对 spec 的任何修改，除非修改的是
// 管理员可编辑字段：DisplayName（重命名）、Description（自由文本注解）以及 Inactive
// （启用/停用）。spec 完全可比较，因此先在两侧掩去这些字段，再用一次相等性比较校验其余部分——
// 这样新增的 spec 字段会自动受到不可变约束保护。
func validateInstanceTypeSpecImmutable(itOld, it *workercore.InstanceType) field.ErrorList {
	oldSpec, newSpec := itOld.Spec, it.Spec
	oldSpec.DisplayName, newSpec.DisplayName = "", ""
	oldSpec.Description, newSpec.Description = "", ""
	oldSpec.Inactive, newSpec.Inactive = false, false
	if oldSpec != newSpec {
		return field.ErrorList{field.Forbidden(field.NewPath("spec"),
			"is immutable except displayName, description and inactive")}
	}
	return nil
}
