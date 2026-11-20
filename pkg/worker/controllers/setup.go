package controllers

import (
	"context"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"gpustack.ai/gpustack/pkg/controller"
	"gpustack.ai/gpustack/pkg/manager"
	"gpustack.ai/gpustack/pkg/worker/apis"
)

func Setup(ctx context.Context, mgr manager.CtrlManager) error {
	return controller.ExecuteSetup(ctx, mgr, []controller.Setup{&devicesObserver{}})
}

// devicesObserver 从缓存读取 Devices 的最新版本，验证事件已进入 reconcile。
type devicesObserver struct {
	client client.Client
}

func (r *devicesObserver) SetupController(_ context.Context, opts controller.SetupOptions) error {
	r.client = opts.Manager.GetClient()
	return ctrl.NewControllerManagedBy(opts.Manager).
		Named("devices-observer").
		For(apis.NewDevices()).
		Complete(r)
}

func (r *devicesObserver) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	devices := apis.NewDevices()
	if err := r.client.Get(ctx, req.NamespacedName, devices); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	log.FromContext(ctx).V(1).Info("observed Devices", "resourceVersion", devices.GetResourceVersion())
	return ctrl.Result{}, nil
}
