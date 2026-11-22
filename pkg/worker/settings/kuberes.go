package settings

import (
	"context"

	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes"
)

// Initialize 初始化设置相关的 Kubernetes 资源。
func Initialize(ctx context.Context, cli kubernetes.Interface) error {
	return settings.Initialize(ctx, cli)
}
