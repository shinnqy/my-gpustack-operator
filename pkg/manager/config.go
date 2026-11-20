package manager

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	klog "k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

type Config struct {
	RestConfig *rest.Config
	Scheme     *runtime.Scheme
}

func (c *Config) Apply(ctx context.Context) (*Manager, error) {
	ctrl.SetLogger(klog.Background().WithName("ctrl"))
	raw, err := ctrl.NewManager(c.RestConfig, ctrl.Options{
		Scheme:         c.Scheme,
		MapperProvider: apiutil.NewDynamicRESTMapper,
		BaseContext: func() context.Context {
			return ctx
		},
		// Devices 使用非结构化对象。启用缓存后，控制器读取的是 informer 同步的视图。
		Client:  client.Options{Cache: &client.CacheOptions{Unstructured: true}},
		Metrics: metricsserver.Options{BindAddress: "0"},
	})
	if err != nil {
		return nil, fmt.Errorf("create controller manager: %w", err)
	}

	ctrlManager := CtrlManager{Manager: raw}
	sentinel := _CtrlManagerSentinel{done: make(chan struct{})}
	if err := ctrlManager.Add(sentinel); err != nil {
		return nil, fmt.Errorf("register controller manager sentinel: %w", err)
	}

	return &Manager{CtrlManager: ctrlManager, sentinel: sentinel}, nil
}
