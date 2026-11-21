package controller

import (
	"context"
	"fmt"

	"github.com/davecgh/go-spew/spew"

	"gpustack.ai/gpustack/pkg/manager"
)

// ExecuteSetup 在启动缓存前逐个注册控制器，首个失败会中止启动。
func ExecuteSetup(ctx context.Context, mgr manager.CtrlManager, setups []Setup) error {
	for i := range setups {
		opts := SetupOptions{
			Manager: mgr,
		}
		err := setups[i].SetupController(ctx, opts)
		if err != nil {
			return fmt.Errorf("controller setup: %s: %w", spew.Sdump(setups[i]), err)
		}
	}
	return nil
}
