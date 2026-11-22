package webhook

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrladmission "sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

type (
	// SetupOptions 是设置 Webhook 的选项。
	SetupOptions struct {
		// Manager 是 controller-runtime 管理器。
		Manager ctrl.Manager
	}

	// Setup 是用于初始化 Webhook 的接口。
	Setup interface {
		// SetupWebhook 在成功时返回受该 Webhook 影响的对象。\n//\n// SetupWebhook 在 Cache 启动前调用，\n// 因此不应执行任何依赖 Cache 已启动的操作；\n// 但可以配置 Cache，例如调用 IndexField 等。
		SetupWebhook(context.Context, SetupOptions) (runtime.Object, error)
	}
)

// DefaultValidator 实现了 admission.Validator，\n// 用于组合和覆盖所需的方法。
type DefaultValidator struct{}

var _ ctrladmission.Validator[runtime.Object] = (*DefaultValidator)(nil)

func (DefaultValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (ctrladmission.Warnings, error) {
	return nil, nil
}

func (DefaultValidator) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (ctrladmission.Warnings, error) {
	return nil, nil
}

func (DefaultValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (ctrladmission.Warnings, error) {
	return nil, nil
}
