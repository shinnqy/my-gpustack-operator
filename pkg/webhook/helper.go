package webhook

import (
	"context"
	"fmt"
	"net/http"

	"github.com/davecgh/go-spew/spew"
	"go.uber.org/multierr"
	admreg "k8s.io/api/admissionregistration/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrladmission "sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes"
	"gpustack.ai/gpustack/pkg/kubeclientset"
	"gpustack.ai/gpustack/pkg/kubeclientset/review"
	"gpustack.ai/gpustack/pkg/kubemeta"
)

type (
	_DefaultWebhookHandler interface {
		ctrladmission.Defaulter[runtime.Object]
		DefaultPath() string
	}
	_ValidatorWebhookHandler interface {
		ctrladmission.Validator[runtime.Object]
		ValidatePath() string
	}
	// ReceiveDeletionUpdate 使 Webhook 退出删除保护机制。默认情况下，对于带有删除时间戳的对象，\n// 处理器的 Default 和 ValidateUpdate 会被跳过，从而确保清除 finalizer 的更新不会被拒绝；\n// 实现此标记接口的处理器在对象删除期间仍会继续收到这些调用。\n//\n// 实现前必须满足：当更新操作未改动 spec 时，处理器 ValidateUpdate 可能触及的每一条规则\n// 都必须仅凭传入的两个对象即可判定。仅当跨对象读取受限于其所检查字段发生变更时，\n// 才与该标记兼容，因为清除 finalizer 的更新不会改动这些字段。\n//\n// 限制：这是一个同时覆盖两部分的单一决策，无法在保留 Default 保护的同时仅让 ValidateUpdate 退出。\n// 因此，如果处理器的默认值设置逻辑需要读取其他对象并在其缺失时拒绝请求，\n// 则无论其校验逻辑多么纯粹都必须保留该保护——否则对象在销毁时将永远无法释放。
	ReceiveDeletionUpdate interface {
		ReceiveDeletionUpdate()
	}
	ConfigurationsGetter func(string, admreg.WebhookClientConfig) (
		*admreg.ValidatingWebhookConfiguration,
		*admreg.MutatingWebhookConfiguration,
	)
	HTTPServeMux interface {
		Handle(string, http.Handler)
	}
)

// ExecuteSetup 执行给定的 setup 以注册 Webhook API。
func ExecuteSetup(ctx context.Context, mgr ctrl.Manager, mux HTTPServeMux, setups []Setup) error {
	scheme := mgr.GetScheme()
	for i := range setups {
		switch setups[i].(type) {
		default:
			continue
		case _DefaultWebhookHandler:
		case _ValidatorWebhookHandler:
		}

		opts := SetupOptions{Manager: mgr}
		obj, err := setups[i].SetupWebhook(ctx, opts)
		if err != nil {
			return fmt.Errorf("webhook setup: %s: %w", spew.Sdump(setups[i]), err)
		}

		// 除非处理器显式退出，否则在删除期间跳过默认值设置与更新校验。
		_, keepDeletionUpdate := setups[i].(ReceiveDeletionUpdate)
		if d, ok := setups[i].(_DefaultWebhookHandler); ok {
			var defaulter ctrladmission.Defaulter[runtime.Object] = d
			if !keepDeletionUpdate {
				defaulter = deletionGuardedDefaulter{delegate: d}
			}
			dh := ctrladmission.WithCustomDefaulter(scheme, obj, defaulter).WithRecoverPanic(true)
			mux.Handle(d.DefaultPath(), dh)
		}
		if v, ok := setups[i].(_ValidatorWebhookHandler); ok {
			var validator ctrladmission.Validator[runtime.Object] = v
			if !keepDeletionUpdate {
				validator = deletionGuardedValidator{delegate: v}
			}
			vh := ctrladmission.WithCustomValidator(scheme, obj, validator).WithRecoverPanic(true)
			mux.Handle(v.ValidatePath(), vh)
		}
	}

	return nil
}

// objectBeingDeleted 返回 obj 是否带有删除时间戳。
func objectBeingDeleted(obj runtime.Object) bool {
	accessor, err := apimeta.Accessor(obj)
	if err != nil {
		return false
	}
	return accessor.GetDeletionTimestamp() != nil
}

// deletionGuardedDefaulter 在对象处于删除过程中时跳过默认值设置，\n// 避免仅用于清除 finalizer 的更新因依赖可能已不存在的状态而被默认值逻辑拒绝。
type deletionGuardedDefaulter struct {
	delegate ctrladmission.Defaulter[runtime.Object]
}

func (g deletionGuardedDefaulter) Default(ctx context.Context, obj runtime.Object) error {
	if objectBeingDeleted(obj) {
		return nil
	}
	return g.delegate.Default(ctx, obj)
}

// deletionGuardedValidator 在对象处于删除过程中时跳过更新校验，\n// 避免仅用于清除 finalizer 的更新被更新校验逻辑拒绝。创建和删除校验仍按原样委托执行。
type deletionGuardedValidator struct {
	delegate ctrladmission.Validator[runtime.Object]
}

func (g deletionGuardedValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (ctrladmission.Warnings, error) {
	return g.delegate.ValidateCreate(ctx, obj)
}

func (g deletionGuardedValidator) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (ctrladmission.Warnings, error) {
	if objectBeingDeleted(newObj) {
		return nil, nil
	}
	return g.delegate.ValidateUpdate(ctx, oldObj, newObj)
}

func (g deletionGuardedValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (ctrladmission.Warnings, error) {
	return g.delegate.ValidateDelete(ctx, obj)
}

// InstallConfigurations 安装 Webhook 配置。
func InstallConfigurations(
	ctx context.Context,
	prefix string,
	cli kubernetes.Interface,
	cc admreg.WebhookClientConfig,
	getters []ConfigurationsGetter,
) error {
	err := review.CanDoUpdate(ctx,
		cli.AuthorizationV1().SelfSubjectAccessReviews(),
		review.Simples{
			{
				Group:    admreg.SchemeGroupVersion.Group,
				Version:  admreg.SchemeGroupVersion.Version,
				Resource: "validatingwebhookconfigurations",
			},
			{
				Group:    admreg.SchemeGroupVersion.Group,
				Version:  admreg.SchemeGroupVersion.Version,
				Resource: "mutatingwebhookconfigurations",
			},
		},
		review.WithCreateIfNotExisted(),
	)
	if err != nil {
		return err
	}

	vwCli := cli.AdmissionregistrationV1().ValidatingWebhookConfigurations()
	mwCli := cli.AdmissionregistrationV1().MutatingWebhookConfigurations()

	vwc, mwc := MergeConfigurations(cc, prefix, getters)
	// 对齐函数可使竞争失败的写入方重试而非直接返回冲突：\n// 每个副本在选主前都会安装配置，若缺少对齐逻辑，并发启动时会导致其中一方失败。
	if vwc != nil {
		vwcAlignFn := func(aVwc *admreg.ValidatingWebhookConfiguration) (_ *admreg.ValidatingWebhookConfiguration, skip bool, err error) {
			skip = true
			// 对齐 Webhook 配置。
			if !kubemeta.DeepEqual(aVwc.Webhooks, vwc.Webhooks) {
				aVwc.Webhooks = vwc.DeepCopy().Webhooks
				skip = false
			}
			return aVwc, skip, err
		}
		_, err := kubeclientset.Update(ctx, vwCli, vwc,
			kubeclientset.WithCreateIfNotExisted[*admreg.ValidatingWebhookConfiguration](),
			kubeclientset.WithUpdateAlign(vwcAlignFn))
		if err != nil {
			return fmt.Errorf("install validating webhook configuration %q: %w",
				vwc.GetName(), err)
		}
	}
	if mwc != nil {
		mwcAlignFn := func(aMwc *admreg.MutatingWebhookConfiguration) (_ *admreg.MutatingWebhookConfiguration, skip bool, err error) {
			skip = true
			// 对齐 Webhook 配置。
			if !kubemeta.DeepEqual(aMwc.Webhooks, mwc.Webhooks) {
				aMwc.Webhooks = mwc.DeepCopy().Webhooks
				skip = false
			}
			return aMwc, skip, err
		}
		_, err := kubeclientset.Update(ctx, mwCli, mwc,
			kubeclientset.WithCreateIfNotExisted[*admreg.MutatingWebhookConfiguration](),
			kubeclientset.WithUpdateAlign(mwcAlignFn))
		if err != nil {
			return fmt.Errorf("install mutating webhook configuration %q: %w",
				mwc.GetName(), err)
		}
	}

	return nil
}

// DeleteConfigurations 删除以 prefix 命名的 Webhook 配置，\n// 即 "<prefix>-validation" 和 "<prefix>-mutation"。不存在的配置视为已删除，\n// 且即使其中一个删除失败，仍会尝试删除另一个。
func DeleteConfigurations(ctx context.Context, prefix string, cli kubernetes.Interface) error {
	var errs []error
	if err := cli.AdmissionregistrationV1().ValidatingWebhookConfigurations().
		Delete(ctx, prefix+"-validation", meta.DeleteOptions{}); err != nil && !kerrors.IsNotFound(err) {
		errs = append(errs, fmt.Errorf("delete validating webhook configuration %q: %w",
			prefix+"-validation", err))
	}
	if err := cli.AdmissionregistrationV1().MutatingWebhookConfigurations().
		Delete(ctx, prefix+"-mutation", meta.DeleteOptions{}); err != nil && !kerrors.IsNotFound(err) {
		errs = append(errs, fmt.Errorf("delete mutating webhook configuration %q: %w",
			prefix+"-mutation", err))
	}
	return multierr.Combine(errs...)
}

// MergeConfigurations 合并来自各 getter 的 Webhook 配置，并返回校验型和变更型 Webhook 配置。
func MergeConfigurations(
	cc admreg.WebhookClientConfig,
	prefix string,
	getters []ConfigurationsGetter,
) (
	*admreg.ValidatingWebhookConfiguration,
	*admreg.MutatingWebhookConfiguration,
) {
	// NB(thxCode): 可在此处添加更多 Webhook 配置 getter。\n// 合并来自所有 getter 的 Webhook 配置。
	var (
		vret = make([]*admreg.ValidatingWebhookConfiguration, len(getters))
		vwsc int
		mret = make([]*admreg.MutatingWebhookConfiguration, len(getters))
		mwsc int
	)
	for i := range getters {
		vwc, mwc := getters[i](prefix, cc)
		if vwc != nil {
			vret[i] = vwc
			vwsc += len(vwc.Webhooks)
		}
		if mwc != nil {
			mret[i] = mwc
			mwsc += len(mwc.Webhooks)
		}
	}

	var (
		vwc *admreg.ValidatingWebhookConfiguration
		mwc *admreg.MutatingWebhookConfiguration
	)
	if vwsc != 0 {
		vwc = &admreg.ValidatingWebhookConfiguration{
			ObjectMeta: meta.ObjectMeta{
				Name: prefix + "-validation",
			},
		}
		for i := range vret {
			if vret[i] == nil {
				continue
			}
			vwc.Webhooks = append(vwc.Webhooks, vret[i].Webhooks...)
		}
	}
	if mwsc != 0 {
		mwc = &admreg.MutatingWebhookConfiguration{
			ObjectMeta: meta.ObjectMeta{
				Name: prefix + "-mutation",
			},
		}
		for i := range mret {
			if mret[i] == nil {
				continue
			}
			mwc.Webhooks = append(mwc.Webhooks, mret[i].Webhooks...)
		}
	}

	return vwc, mwc
}
