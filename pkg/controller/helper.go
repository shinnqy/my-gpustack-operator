package controller

import (
	"context"
	"fmt"

	"gpustack.ai/gpustack/pkg/manager"
)

type SetupOptions struct {
	Manager manager.CtrlManager
}

type Setup interface {
	SetupController(context.Context, SetupOptions) error
}

// ExecuteSetup 在启动缓存前逐个注册控制器，首个失败会中止启动。
func ExecuteSetup(ctx context.Context, mgr manager.CtrlManager, setups []Setup) error {
	for i := range setups {
		opts := SetupOptions{Manager: mgr}
		if err := setups[i].SetupController(ctx, opts); err != nil {
			return fmt.Errorf("controller setup: %T: %w", setups[i], err)
		}
	}
	return nil
}
