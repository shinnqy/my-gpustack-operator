package controller

import (
	"context"

	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"

	"gpustack.ai/gpustack/pkg/manager"
)

type (
	// SetupOptions 是用于初始化控制器的选项。
	SetupOptions struct {
		// Manager 是 controller-runtime 管理器。
		Manager manager.CtrlManager
	}

	// Setup 是控制器初始化的接口。
	Setup interface {
		ctrlreconcile.Reconciler
		// SetupController 初始化控制器。
		//
		// SetupController 在 Cache 启动之前调用，
		// 因此不应执行任何需要 Cache 已启动才能进行的操作。
		// 相反，可以在此配置 Cache，例如调用 IndexField 等。
		SetupController(ctx context.Context, opts SetupOptions) error
	}
)
