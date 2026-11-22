package worker

import (
	"context"
	"fmt"
	"slices"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrladmission "sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/webhook"
)

// ModelStoreBindingWebhook 用于校验 v1alpha1.ModelStoreBinding。
//
// 该 Webhook 仅执行校验，负责检查 schema 无法覆盖的约束：授权引用的每个 store 均存在、以数量字符串表示的配额为正值，以及不可变性约束——防止授权在准入已放行占用的情况下被重新指向或缩减配额。
//
// nolint: lll
// +k8s:webhook-gen:validating:group="worker.gpustack.ai",version="v1alpha1",resource="modelstorebindings",scope="Namespaced"
// +k8s:webhook-gen:validating:operations=["CREATE","UPDATE"],failurePolicy="Fail",sideEffects="None",matchPolicy="Equivalent",timeoutSeconds=10
type ModelStoreBindingWebhook struct {
	Client    ctrlcli.Client
	APIReader ctrlcli.Reader
}

func (r *ModelStoreBindingWebhook) SetupWebhook(
	_ context.Context, opts webhook.SetupOptions,
) (runtime.Object, error) {
	r.Client = opts.Manager.GetClient()
	r.APIReader = opts.Manager.GetAPIReader()

	return &workercore.ModelStoreBinding{}, nil
}

var (
	_ ctrladmission.Validator[runtime.Object] = (*ModelStoreBindingWebhook)(nil)
	_ webhook.ReceiveDeletionUpdate           = (*ModelStoreBindingWebhook)(nil)
)

// ReceiveDeletionUpdate 使本 Webhook 在 Binding 处于删除过程中时仍继续校验其更新。
//
// 冻结约束恰恰在删除窗口期最为关键：在授权被收回的同时对其进行编辑，正是事后审计需要排查的情况。
func (r *ModelStoreBindingWebhook) ReceiveDeletionUpdate() {}

func (r *ModelStoreBindingWebhook) ValidateCreate(
	ctx context.Context, obj runtime.Object,
) (ctrladmission.Warnings, error) {
	binding := obj.(*workercore.ModelStoreBinding)

	errs := validateModelStoreBindingSpec(binding)
	if len(errs) == 0 {
		// 仅当对象自身结构合法时，才执行跨对象校验。
		errs = append(errs, r.validateModelStoreRefsExist(ctx, binding)...)
	}
	if len(errs) > 0 {
		return nil, kerrors.NewInvalid(workercore.Kind("ModelStoreBinding"), binding.Name, errs)
	}

	return nil, nil
}

func (r *ModelStoreBindingWebhook) ValidateUpdate(
	ctx context.Context, oldObj, newObj runtime.Object,
) (ctrladmission.Warnings, error) {
	oldBinding, newBinding := oldObj.(*workercore.ModelStoreBinding), newObj.(*workercore.ModelStoreBinding)

	errs := validateModelStoreBindingSpec(newBinding)
	errs = append(errs, validateModelStoreBindingImmutable(oldBinding, newBinding)...)
	if len(errs) == 0 && modelStoreRefsMoved(oldBinding, newBinding) {
		// 仅当授权指向了此前未引用的 store 时才重新读取检查。由于 storeRefs 当前不可变，该分支暂不可达——保留此保护是为了在未来放宽不可变限制时能自动恢复存在性校验，而不是静默跳过。
		errs = append(errs, r.validateModelStoreRefsExist(ctx, newBinding)...)
	}
	if len(errs) > 0 {
		return nil, kerrors.NewInvalid(workercore.Kind("ModelStoreBinding"), newBinding.Name, errs)
	}

	return nil, nil
}

func (r *ModelStoreBindingWebhook) ValidateDelete(
	_ context.Context, _ runtime.Object,
) (ctrladmission.Warnings, error) {
	// 收回授权始终是被允许的：对应 Namespace 的 prefetch 将失去授权依据并由记账控制器上报，这如实反映了授权被撤销后的状态。
	return nil, nil
}

// validateModelStoreBindingSpec 包含仅依据对象自身即可判定的所有校验规则。
func validateModelStoreBindingSpec(binding *workercore.ModelStoreBinding) field.ErrorList {
	specPath := field.NewPath("spec")

	var errs field.ErrorList

	for i, ref := range binding.Spec.StoreRefs {
		if ref.Name == "" {
			errs = append(errs, field.Required(specPath.Child("storeRefs").Index(i).Child("name"),
				"a grant names the store it covers"))
		}
	}

	// 配额为零意味着授予了缓存权限却会以同一报错拒绝所有 prefetch：这种被允许的状态无法正常工作，因此不予通过。
	if binding.Spec.Quota.Bytes.Sign() <= 0 {
		errs = append(errs, field.Invalid(specPath.Child("quota", "bytes"), binding.Spec.Quota.Bytes.String(),
			"must be greater than 0: it is the byte budget every prefetch of this namespace draws on"))
	}

	return errs
}

// validateModelStoreBindingImmutable 保持授权配置稳定不变，仅允许两条扩容路径：allowPinned 从 false 变为 true 可在不迁移授权的情况下放宽权限，而调高配额可提供更多同类资源。即使尚未产生实际数据占用，调低配额也会被拒绝，因为记账统计的是 prefetch 预计占用的容量，若事后导致已准入的 prefetch 超出预算，会表现为操作员并未犯下的操作错误。
func validateModelStoreBindingImmutable(old, next *workercore.ModelStoreBinding) field.ErrorList {
	specPath := field.NewPath("spec")

	var errs field.ErrorList

	if !slices.EqualFunc(old.Spec.StoreRefs, next.Spec.StoreRefs,
		func(a, b workercore.ModelStoreBindingStoreReference) bool { return a.Name == b.Name }) {
		errs = append(errs, field.Invalid(specPath.Child("storeRefs"), next.Spec.StoreRefs,
			"is immutable: the grant is what a namespace's prefetch admission is checked against, and "+
				"re-pointing it would strand the old accounting while admitting against the new"))
	}

	switch cmp := next.Spec.Quota.Bytes.Cmp(old.Spec.Quota.Bytes); {
	case cmp < 0:
		errs = append(errs, field.Invalid(specPath.Child("quota", "bytes"), next.Spec.Quota.Bytes.String(),
			fmt.Sprintf("cannot shrink: prefetches admitted against %s would turn over budget by an "+
				"edit; grow the budget instead when the namespace needs more", old.Spec.Quota.Bytes.String())))
	case cmp == 0:
		// 授权未发生变更，属于常规更新（如 status 写入或无实际变更的重新提交）。
	}

	if modelStoreBindingAllowsPinned(old) && !modelStoreBindingAllowsPinned(next) {
		errs = append(errs, field.Invalid(specPath.Child("allowPinned"), next.Spec.AllowPinned,
			"cannot go back to false: prefetches pinned under this grant would lose the pin they were "+
				"admitted on; take the grant away by deleting the Binding, which is audited"))
	}

	return errs
}

// modelStoreBindingAllowsPinned 返回生效的 allowPinned 值，nil 视为默认值 false。
func modelStoreBindingAllowsPinned(binding *workercore.ModelStoreBinding) bool {
	return binding.Spec.AllowPinned != nil && *binding.Spec.AllowPinned
}

// validateModelStoreRefsExist 拒绝引用不存在 store 的授权：基于不存在 store 的 Binding 会报告 Ready，却在每次 prefetch 时因找不到该 store 而拒绝请求。
func (r *ModelStoreBindingWebhook) validateModelStoreRefsExist(
	ctx context.Context, binding *workercore.ModelStoreBinding,
) field.ErrorList {
	path := field.NewPath("spec", "storeRefs")

	var errs field.ErrorList

	for i, ref := range binding.Spec.StoreRefs {
		if ref.Name == "" {
			continue // 结构校验已报告该错误
		}
		err := r.Client.Get(ctx, ctrlcli.ObjectKey{Name: ref.Name}, new(workercore.ModelStore))
		if kerrors.IsNotFound(err) {
			// storeRefs 不可变且校验策略为 failurePolicy=Fail，因此缓存哪怕滞后一拍都会永久拒绝指向已存在 store 的授权。在拒绝前先绕过缓存重新读取确认。
			err = r.APIReader.Get(ctx, ctrlcli.ObjectKey{Name: ref.Name}, new(workercore.ModelStore))
		}
		switch {
		case err == nil:
		case kerrors.IsNotFound(err):
			errs = append(errs, field.NotFound(path.Index(i).Child("name"), ref.Name))
		default:
			errs = append(errs, field.InternalError(path.Index(i).Child("name"),
				fmt.Errorf("get model store %q: %w", ref.Name, err)))
		}
	}

	return errs
}

// modelStoreRefsMoved 报告本次更新是否指定了此前未包含的 store 列表。
func modelStoreRefsMoved(old, next *workercore.ModelStoreBinding) bool {
	return !slices.EqualFunc(old.Spec.StoreRefs, next.Spec.StoreRefs,
		func(a, b workercore.ModelStoreBindingStoreReference) bool { return a.Name == b.Name })
}
