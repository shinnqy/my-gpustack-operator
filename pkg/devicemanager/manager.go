package devicemanager

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/component-base/metrics/legacyregistry"
	klog "k8s.io/klog/v2"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"gpustack.ai/gpustack/pkg/devicemanager/allocator"
	"gpustack.ai/gpustack/pkg/devicemanager/controllers"
	"gpustack.ai/gpustack/pkg/devicemanager/detector"
	"gpustack.ai/gpustack/pkg/devicemanager/exporter"
	"gpustack.ai/gpustack/pkg/manager"
	"gpustack.ai/gpustack/pkg/utils/gox"
)

func init() {
	ctrlmetrics.Registry = struct {
		prometheus.Registerer
		prometheus.Gatherer
	}{
		Registerer: legacyregistry.Registerer(),
		Gatherer:   legacyregistry.DefaultGatherer,
	}
}

type Manager struct {
	Manager   *manager.Manager
	Detector  *detector.Detector
	Allocator *allocator.Allocator
	Exporter  *exporter.Poller
}

// 先准备管理器，再注册 Prometheus 指标采集器
// Prepare 准备服务所需的运行环境并注册指标采集器。
func (m *Manager) Prepare(ctx context.Context) error {
	err := m.Manager.Prepare(ctx)
	if err != nil {
		return err
	}

	// 把本节点的 Instance 指标注册到共享管理器已有的 /metrics 端点，复用现有的端口和指标注册表。
	err = ctrlmetrics.Registry.Register(
		exporter.NewCollector(m.Exporter, m.Detector.MonitorSnapshot),
	)
	if err != nil {
		return fmt.Errorf("register instance metrics collector: %w", err)
	}

	return nil
}

// 启动后台任务并等待它们结束
func (m *Manager) Start(ctx context.Context) error {
	cm := m.Manager.CtrlManager
	ms := cm.GetWebhookServer()

	// 注册读取最新加速器监控快照的 HTTP 端点。
	ms.Register(MonitorSnapshotPath, newMonitorSnapshotHandler(m.Detector.MonitorSnapshot))

	// IMPORTANT: 四个 gp.go: 读硬件、看 Kubernetes 对象、启动插件、采指标可以同时进行
	// 启动各组件。
	gp := gox.GroupWithContextIn(ctx)
	gp.Go(func(ctx context.Context) error {
		// IMPORTANT: 探测硬件
		klog.Info("starting detector")
		return m.Detector.Start(ctx)
	})
	gp.Go(func(ctx context.Context) error {
		// IMPORTANT: 跑设备分配器
		// NB(thxCode) 分配器需要先等管理器就绪，否则创建所需资源时可能失败。
		klog.Info("waiting for manager ready")
		err := m.Manager.WaitForReady(ctx)
		if err != nil {
			return fmt.Errorf("wait for manager ready: %w", err)
		}
		klog.Info("starting allocator")
		return m.Allocator.Start(ctx)
	})
	gp.Go(func(ctx context.Context) error {
		// IMPORTANT: 采容器指标
		// NB(thxCode) 指标采集器通过控制器注册的字段索引和 informer 缓存列出 Pod，两者都准备好后
		// 才能开始采集。
		klog.Info("waiting for manager ready")
		err := m.Manager.WaitForReady(ctx)
		if err != nil {
			return fmt.Errorf("wait for manager ready: %w", err)
		}
		klog.Info("starting instance metrics exporter")
		return m.Exporter.Start(ctx)
	})
	gp.Go(func(ctx context.Context) error {
		// IMPORTANT: 跑 K8s ControllerManager
		klog.Info("starting controller manager")
		err := controllers.Setup(ctx, cm)
		if err != nil {
			return fmt.Errorf("setup controllers: %w", err)
		}
		return m.Manager.Start(ctx)
	})
	return gp.Wait()
}
