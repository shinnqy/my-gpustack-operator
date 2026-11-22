package controllers

import (
	"context"

	"gpustack.ai/gpustack/pkg/controller"
	"gpustack.ai/gpustack/pkg/deviceplugin"
	"gpustack.ai/gpustack/pkg/manager"
)

// setups 是控制器设置函数的列表。
var setups = []controller.Setup{
	new(deviceplugin.DevicesReconciler),
}

// Get 返回指定类型的控制器设置。
func Get[T any]() T {
	for i := range setups {
		if s, ok := setups[i].(T); ok {
			return s
		}
	}
	var zero T
	return zero
}

// Setup 用于设置所有控制器。
func Setup(ctx context.Context, mgr manager.CtrlManager) error {
	return controller.ExecuteSetup(ctx, mgr, setups)
}
