package worker

import (
	"context"
	"errors"
	"fmt"

	"golang.org/x/sync/errgroup"
	corev1 "k8s.io/api/core/v1"
	genericapiserver "k8s.io/apiserver/pkg/server"
	klog "k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"gpustack.ai/gpustack/pkg/manager"
	"gpustack.ai/gpustack/pkg/worker/apis"
	"gpustack.ai/gpustack/pkg/worker/controllers"
)

type Worker struct {
	Manager         *manager.Manager
	APIServer       *genericapiserver.GenericAPIServer
	installer       client.Client
	routingCABundle []byte
	namespace       string
	service         string
}

func (w *Worker) Prepare(ctx context.Context) (err error) {
	defer func() {
		if err != nil {
			_ = w.APIServer.SecureServingInfo.Listener.Close()
		}
	}()

	// Kubernetes 聚合层通过部署提供的 Service 访问 Worker。
	key := client.ObjectKey{Namespace: w.namespace, Name: w.service}
	service := &corev1.Service{}
	if err := w.installer.Get(ctx, key, service); err != nil {
		return fmt.Errorf("read Worker routing Service %s: %w", key, err)
	}
	if len(service.Spec.Ports) == 0 {
		return fmt.Errorf("Worker routing Service %s has no ports", key)
	}

	if err := apis.InstallCRDs(ctx, w.installer); err != nil {
		return fmt.Errorf("install CRDs: %w", err)
	}
	if err := apis.InstallServices(ctx, w.installer, w.namespace, w.service, service.Spec.Ports[0].Port, w.routingCABundle); err != nil {
		return fmt.Errorf("install extension API services: %w", err)
	}
	return nil
}

func (w *Worker) Start(ctx context.Context) error {
	cm := w.Manager.CtrlManager
	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		// 聚合 API 可访问后，才注册控制器并启动 informer。
		klog.Info("waiting for extension API services to be ready")
		if err := apis.WaitForServicesReady(ctx, cm.GetAPIReader()); err != nil {
			return fmt.Errorf("wait for extension API services to be ready: %w", err)
		}
		if err := controllers.Setup(ctx, cm); err != nil {
			return fmt.Errorf("setup controllers: %w", err)
		}

		klog.Info("starting controller manager")
		err := w.Manager.Start(ctx)
		if err == nil && ctx.Err() == nil {
			return errors.New("controller manager stopped unexpectedly")
		}
		return err
	})
	group.Go(func() error {
		// API server 与等待 APIService 的协程并发运行。
		err := w.APIServer.PrepareRun().RunWithContext(ctx)
		if err == nil && ctx.Err() == nil {
			return errors.New("extension API server stopped unexpectedly")
		}
		return err
	})
	return group.Wait()
}
