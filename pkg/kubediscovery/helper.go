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

// WaitUntilConnected waits until the Kubernetes to be connected,
// or returns an error if the context is canceled.
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

// IsConnected returns nothing if the Kubernetes cluster is connected.
func IsConnected(ctx context.Context, discoveryCli discovery.DiscoveryInterface) error {
	_, err := GetVersion(ctx, discoveryCli)
	return err
}

// IsConnectedWithRestConfig is similar to IsConnected, accepts rest.Config as input.
func IsConnectedWithRestConfig(ctx context.Context, restCfg *rest.Config) error {
	_, err := GetVersionWithRestConfig(ctx, restCfg)
	return err
}

// Version represents the Kubernetes version information.
type Version = version.Info

// GetVersion returns the Kubernetes version information.
func GetVersion(_ context.Context, discoveryCli discovery.DiscoveryInterface) (*Version, error) {
	info, err := discoveryCli.ServerVersion()
	if err != nil {
		return nil, fmt.Errorf("get server version: %w", err)
	}

	return info, nil
}

// GetVersionWithRestConfig returns the Kubernetes version information with the given REST config.
func GetVersionWithRestConfig(ctx context.Context, restCfg *rest.Config) (*Version, error) {
	discoveryCli, err := discovery.NewDiscoveryClientForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("create discovery client: %w", err)
	}

	return GetVersion(ctx, discoveryCli)
}
