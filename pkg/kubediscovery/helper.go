package kubediscovery

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/version"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/pkg/utils/waitx"
)

// WaitUntilConnected 等待直到连接上 Kubernetes 集群，
// 若 context 被取消则返回 error。
func WaitUntilConnected(ctx context.Context, discoveryCli discovery.DiscoveryInterface) error {
	return waitx.RetryUntilSuccess(ctx, time.Second, true,
		func(ctx context.Context) error {
			err := IsConnected(ctx, discoveryCli)
			if err != nil {
				klog.V(3).ErrorS(err, "connect to Kubernetes, retrying...")
			}
			return err
		},
	)
}

// IsConnected 在 Kubernetes 集群已连接时返回 nil。
func IsConnected(ctx context.Context, discoveryCli discovery.DiscoveryInterface) error {
	_, err := GetVersion(ctx, discoveryCli)
	return err
}

// IsConnectedWithRestConfig 与 IsConnected 类似，但接收 rest.Config 作为输入。
func IsConnectedWithRestConfig(ctx context.Context, restCfg *rest.Config) error {
	_, err := GetVersionWithRestConfig(ctx, restCfg)
	return err
}

// Version 表示 Kubernetes 版本信息。
type Version = version.Info

// GetVersion 返回 Kubernetes 版本信息。
func GetVersion(_ context.Context, discoveryCli discovery.DiscoveryInterface) (*Version, error) {
	info, err := discoveryCli.ServerVersion()
	if err != nil {
		return nil, fmt.Errorf("get server version: %w", err)
	}

	return info, nil
}

// GetVersionWithRestConfig 使用给定的 REST 配置返回 Kubernetes 版本信息。
func GetVersionWithRestConfig(ctx context.Context, restCfg *rest.Config) (*Version, error) {
	discoveryCli, err := discovery.NewDiscoveryClientForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("create discovery client: %w", err)
	}

	return GetVersion(ctx, discoveryCli)
}
