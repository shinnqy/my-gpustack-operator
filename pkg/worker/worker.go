package worker

import (
	"context"
	"fmt"
	"net/http"
	"net/http/pprof"
	"runtime"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	admreg "k8s.io/api/admissionregistration/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/healthz"
	"k8s.io/apiserver/pkg/server/routes"
	"k8s.io/client-go/dynamic"
	"k8s.io/component-base/logs"
	"k8s.io/component-base/metrics/legacyregistry"
	klog "k8s.io/klog/v2"
	apireg "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	"k8s.io/utils/ptr"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"gpustack.ai/gpustack/pkg/kubediscovery"
	"gpustack.ai/gpustack/pkg/manager"
	"gpustack.ai/gpustack/pkg/system"
	"gpustack.ai/gpustack/pkg/utils/gox"
	"gpustack.ai/gpustack/pkg/utils/httpx"
	"gpustack.ai/gpustack/pkg/worker/apis"
	"gpustack.ai/gpustack/pkg/worker/controllers"
	"gpustack.ai/gpustack/pkg/worker/extensionapis"
	"gpustack.ai/gpustack/pkg/worker/extensionroutes"
	"gpustack.ai/gpustack/pkg/worker/kuberess"
	"gpustack.ai/gpustack/pkg/worker/settings"
	"gpustack.ai/gpustack/pkg/worker/webhooks"
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

type Worker struct {
	RoutingPort     int32
	RoutingCaBundle []byte
	Manufacturers   []string
	Manager         *manager.Manager
	APIServer       *genericapiserver.GenericAPIServer
}

// routingServiceReference 返回前置代理各个扩展 API service 的系统路由 Service。
func (w *Worker) routingServiceReference() apireg.ServiceReference {
	return apireg.ServiceReference{
		Namespace: kuberess.SystemNamespaceName,
		Name:      kuberess.SystemRoutingServiceName,
		Port:      ptr.To(w.RoutingPort),
	}
}

func (w *Worker) Prepare(ctx context.Context) error {
	err := w.Manager.Prepare(ctx)
	if err != nil {
		return err
	}

	lpCli := system.LoopbackKubeClient.Get()

	// 安装系统 Namespace。
	err = kuberess.InstallSystemNamespace(ctx, lpCli)
	if err != nil {
		return fmt.Errorf("install system namespace: %w", err)
	}

	// 如需要，安装伪系统路由 Service。
	if !system.LoopbackKubeInside.Get() && system.LoopbackKubeNearby.Get() {
		// NB(thxCode): 还需要同时启用回环 Kubernetes APIServer 的 `--enable-aggregator-routing` 标志。
		err = kuberess.InstallFakeSystemRoutingService(ctx, lpCli, w.RoutingPort)
		if err != nil {
			return fmt.Errorf("install fake system routing service: %w", err)
		}
	}

	// 初始化 CRDs。
	err = apis.InstallCRDs(ctx, lpCli)
	if err != nil {
		return fmt.Errorf("install CRDs: %w", err)
	}

	// Kubernetes 聚合层通过部署提供的 Service 访问 Worker。
	err = apis.InstallServices(ctx, lpCli, w.routingServiceReference(), w.RoutingCaBundle)
	if err != nil {
		return fmt.Errorf("install extension API services: %w", err)
	}

	// 安装 Webhook 配置。
	{
		cc := admreg.WebhookClientConfig{
			Service: &admreg.ServiceReference{
				Namespace: kuberess.SystemNamespaceName,
				Name:      kuberess.SystemRoutingServiceName,
				Port:      ptr.To(w.RoutingPort),
			},
		}
		// 如果部署在回环 Kubernetes 集群附近而非集群内部，
		// 可以使用主 IP 地址访问 Webhook 服务器。
		if !system.LoopbackKubeInside.Get() && system.LoopbackKubeNearby.Get() {
			// NB(thxCode): 启动多个实例时，仅有一个实例生效。
			ep := fmt.Sprintf("https://%s:%d", system.PrimaryIP.Get(), w.RoutingPort)
			cc = admreg.WebhookClientConfig{
				URL: ptr.To(ep),
			}
		}
		// 安装 Webhook 配置。
		cc.CABundle = w.RoutingCaBundle
		err = webhooks.Install(ctx, lpCli, cc)
		if err != nil {
			return fmt.Errorf("install webhook configurations: %w", err)
		}
	}

	// 初始化设置。
	err = settings.Initialize(ctx, lpCli)
	if err != nil {
		return fmt.Errorf("initialize settings: %w", err)
	}

	// 安装应用组件。
	err = kuberess.InstallApplications(ctx, w.Manufacturers)
	if err != nil {
		return fmt.Errorf("install applications: %w", err)
	}

	// 安装调度链所需的两个自定义资源，但任何 Helm chart 都无法直接携带它们：
	// 它们的 CRD 分别属于 Node Feature Discovery 和 Kueue，而 Helm 在创建任何资源前会先映射整个清单，因此 chart
	// 无法保证 CRD 在依赖它的自定义资源之前创建。两者均保留在 --disable-applications 开关之外 —— 无论由谁部署
	// NFD 和 Kueue，缺少它们调度链都无法启动 —— 且两者都会持续重试直至对应的 CRD 就绪。
	err = kuberess.InstallCPUInfoNodeFeatureRule(ctx, w.Manufacturers)
	if err != nil {
		return fmt.Errorf("install cpu info node feature rule: %w", err)
	}

	err = kuberess.InstallNodeDevicesAdmissionCheck(ctx)
	if err != nil {
		return fmt.Errorf("install node devices admission check: %w", err)
	}

	err = kuberess.InstallModelDeploymentJointAdmissionCheck(ctx)
	if err != nil {
		return fmt.Errorf("install model deployment joint admission check: %w", err)
	}

	return nil
}

func (w *Worker) Start(ctx context.Context) error {
	cm := w.Manager.CtrlManager
	mu := w.APIServer.Handler.NonGoRestfulMux

	// 在开始监听前注册两个聚合 API 组的资源处理器。
	err := extensionapis.Setup(ctx, w.APIServer, cm)
	if err != nil {
		return fmt.Errorf("setup extension API handlers: %w", err)
	}

	// 集群中的 webhook 配置在 Prepare 安装，这里注册相应的 HTTP 路由。
	err = webhooks.Setup(ctx, cm, mu)
	if err != nil {
		return fmt.Errorf("setup webhooks: %w", err)
	}

	// 注册 /metrics。
	mu.Handle("/metrics", legacyregistry.Handler())

	// 将 Manager 检查放到存活探针。就绪探针必须能先于 Manager 启动通过。
	{
		err = w.APIServer.AddLivezChecks(10*time.Second,
			healthz.NamedCheck("gopool", func(r *http.Request) error {
				return gox.IsHealthy()
			}),
			healthz.NamedCheck("loopback", func(r *http.Request) error {
				return kubediscovery.IsConnectedWithRestConfig(r.Context(), dynamic.ConfigFor(cm.GetConfig()))
			}),
			healthz.NamedCheck("manager", func(r *http.Request) error {
				return w.Manager.WaitForReady(r.Context())
			}),
		)
		if err != nil {
			return fmt.Errorf("add livez checks: %w", err)
		}
	}

	// 注册 /debug。
	{
		runtime.SetBlockProfileRate(1)
		mu.Handle("/debug/pprof/", httpx.LoopbackAccessHandlerFunc(pprof.Index))
		mu.Handle("/debug/pprof/cmdline", httpx.LoopbackAccessHandlerFunc(pprof.Cmdline))
		mu.Handle("/debug/pprof/profile", httpx.LoopbackAccessHandlerFunc(pprof.Profile))
		mu.Handle("/debug/pprof/symbol", httpx.LoopbackAccessHandlerFunc(pprof.Symbol))
		mu.Handle("/debug/pprof/trace", httpx.LoopbackAccessHandlerFunc(pprof.Trace))
		mu.Handle("/debug/flags/v", httpx.LoopbackAccessHandlerFunc(routes.StringFlagPutHandler(logs.GlogSetter)))
	}

	// 注册扩展 APIs。
	mu.NotFoundHandler(extensionroutes.Index())

	// 启动。
	gp := gox.GroupWithContextIn(ctx)
	gp.Go(func(ctx context.Context) error {
		// 聚合 API 可访问后，才注册控制器并启动 informer。
		klog.Info("waiting for extension API services to be ready")
		err := apis.WaitForServicesReady(ctx, system.LoopbackKubeClient.Get())
		if err != nil {
			return fmt.Errorf("wait for extension API services to be ready: %w", err)
		}

		// NB(thxCode): 在此处安装部分资源，
		// 不等待任何资源就绪，完成后直接继续。
		// ...

		// 配置控制器。
		klog.Info("starting controller manager")
		err = controllers.Setup(ctx, cm)
		if err != nil {
			return fmt.Errorf("setup controllers: %w", err)
		}
		return w.Manager.Start(ctx)
	})
	gp.Go(func(ctx context.Context) error {
		// API server 与等待 APIService 的协程并发运行。
		klog.Info("starting api server")
		return w.APIServer.PrepareRun().RunWithContext(ctx)
	})
	gp.Go(func(ctx context.Context) error {
		// Prepare 只安装一次。运行期间 CRD 被误删时也要恢复，控制器才能继续监听。
		klog.Info("starting custom resource definitions ensurer")
		// 退出时可能同时带有重试期间的错误，用 context 判断是否在正常关闭。
		err := apis.EnsureCRDs(ctx, system.LoopbackKubeClient.Get())
		if err != nil && ctx.Err() == nil {
			return fmt.Errorf("ensure CRDs: %w", err)
		}
		return nil
	})
	gp.Go(func(ctx context.Context) error {
		klog.Info("starting extension API services ensurer")
		err := apis.EnsureServices(ctx, system.LoopbackKubeClient.Get(),
			w.routingServiceReference(), w.RoutingCaBundle)
		w.deregisterOnTeardown()
		if err != nil && ctx.Err() == nil {
			return fmt.Errorf("ensure extension API services: %w", err)
		}
		return nil
	})
	return gp.Wait()
}

// deregisterOnTeardown 在因系统 Namespace 正在被删除而触发关闭时，移除本次安装的集群作用域对象 —— 包括由该
// 系统 Namespace 支撑的所有 API service 以及该 worker 注册的 Webhook 配置。否则这两类对象都会在带命名空间的
// 路由 Service 消失后继续存在：陈旧的 API service 会因其已无法提供的 discovery 服务而卡住 Namespace 的删除，
// 陈旧的 Webhook 配置则会向已不存在的 Service 发起准入调用从而导致准入失败（gpustack-operator#123）。
// API service 按底层 Namespace 而非 worker 自身维护的列表进行匹配，因为通过 chart 安装的 APIService —— 即
// Kueue 的 visibility 对 —— 也会以同样方式卡住删除，而直接删除 Namespace 不会触发 chart 卸载来清理它们。普通
// 重启或升级会保留系统 Namespace，因此此时不会移除任何对象 —— 存活副本的 ensurer 或下一次启动会保持其持续提供
// 服务。尽力而为且有超时限制：绝不能阻塞关闭流程。
func (w *Worker) deregisterOnTeardown() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	lpCli := system.LoopbackKubeClient.Get()
	ns, err := lpCli.CoreV1().Namespaces().Get(ctx, kuberess.SystemNamespaceName, meta.GetOptions{})
	switch {
	case err == nil && ns.DeletionTimestamp == nil:
		// Namespace 仍然存在：属于重启或升级，而非卸载清理。
		return
	case err != nil && !kerrors.IsNotFound(err):
		// 读取失败时无法区分卸载清理与重启 —— 保留现有注册项，避免从存活的安装中误删配置。
		klog.InfoS("leaving the extension API services and webhook configurations registered: cannot read the system namespace", "err", err)
		return
	}

	if err = apis.DeleteServicesBackedBy(ctx, lpCli, kuberess.SystemNamespaceName); err != nil {
		klog.InfoS("failed to delete the aggregated API services on teardown", "err", err)
	}
	if err = webhooks.Delete(ctx, lpCli); err != nil {
		klog.InfoS("failed to delete webhook configurations on teardown", "err", err)
	}
}
