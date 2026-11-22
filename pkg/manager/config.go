package manager

import (
	"context"
	"fmt"
	"net/http"
	"time"

	kmeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/rest"
	klog "k8s.io/klog/v2"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlcache "sigs.k8s.io/controller-runtime/pkg/cache"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlapiutil "sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	ctrlcfg "sigs.k8s.io/controller-runtime/pkg/config"
	ctrlmetricsrv "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes"
	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes/scheme"
	"gpustack.ai/gpustack/pkg/system"
	"gpustack.ai/gpustack/pkg/systemname"
	"gpustack.ai/gpustack/pkg/webserver"
)

type Config struct {
	InformerCacheResyncPeriod time.Duration
	LoopbackKubeConfigPath    string
	LoopbackKubeRestConfig    rest.Config
	LoopbackKubeHTTPClient    *http.Client
	LoopbackKubeClient        kubernetes.Interface
	KubeLeaderElection        bool
	KubeLeaderElectionID      string
	KubeLeaderLease           time.Duration
	KubeLeaderRenewTimeout    time.Duration

	// 内部赋值字段。
	// 在调用 Apply 之前覆写这些字段。
	WebhookServer webserver.Server
	// CacheByObject 按对象类型缩小 informer 缓存的监听范围。若组件的 RBAC 权限仅覆盖
	// 单个 Namespace 或单个对象，必须在此声明，否则其 informer 发起的集群级 list 请求
	// 会被拒绝，导致缓存永远无法完成同步。
	CacheByObject map[ctrlcli.Object]ctrlcache.ByObject
}

func (c *Config) Apply(ctx context.Context) (*Manager, error) {
	// controller-runtime 和 Manager 各使用一个带名称的日志器。
	ctrl.SetLogger(klog.Background().V(3).WithName("ctrl"))

	ctrlMgrOpts := ctrl.Options{
		// 通用配置。
		GracefulShutdownTimeout: ptr.To(30 * time.Second),
		Scheme:                  scheme.Scheme,
		Logger:                  klog.Background().WithName("ctrlmgr"),

		// 上下文配置。
		BaseContext: func() context.Context {
			return ctx
		},

		// Mapper 配置。
		MapperProvider: func(config *rest.Config, _ *http.Client) (kmeta.RESTMapper, error) {
			// 显式传入已有的 HTTP client，让 discovery 和对象请求复用连接。
			return ctrlapiutil.NewDynamicRESTMapper(config, c.LoopbackKubeHTTPClient)
		},

		// 客户端配置。
		Client: ctrlcli.Options{
			// 复用 HTTP 客户端。
			HTTPClient: c.LoopbackKubeHTTPClient,
		},
		NewClient: func(config *rest.Config, options ctrlcli.Options) (ctrlcli.Client, error) {
			// 聚合 API 需要向存储版本转发 Watch。
			return ctrlcli.NewWithWatch(config, options)
		},

		// 缓存配置。
		Cache: ctrlcache.Options{
			// 复用 HTTP 客户端。
			HTTPClient: c.LoopbackKubeHTTPClient,
			// informer 按这个周期重新同步；ByObject 限定各类对象的监听范围。
			SyncPeriod: ptr.To(c.InformerCacheResyncPeriod),
			ByObject:   c.CacheByObject,
		},

		// 控制器配置。
		Controller: ctrlcfg.Controller{
			// 控制器启动前等待监听缓存完成同步的超时时间。
			CacheSyncTimeout: 2 * time.Minute,
			// 从 panic 中恢复。
			RecoverPanic: ptr.To(true),
			// NB(thxCode): 默认启用控制器的 leader election，
			// 此配置不影响 manager 本身的 leader election。
			NeedLeaderElection: ptr.To(true),
		},

		// Controller manager 的 leader election 配置。
		LeaderElectionReleaseOnCancel: true,
		LeaderElectionNamespace:       systemname.NamespaceName,
		LeaderElectionID:              c.KubeLeaderElectionID,
		LeaderElection:                c.KubeLeaderElection,
		LeaseDuration:                 ptr.To(c.KubeLeaderLease),
		RenewDeadline:                 ptr.To(c.KubeLeaderRenewTimeout),
		RetryPeriod:                   ptr.To(2 * time.Second),

		// 禁用默认 Webhook 服务器。
		WebhookServer: c.WebhookServer,
		// 禁用默认指标服务。
		Metrics: ctrlmetricsrv.Options{BindAddress: "0"},
		// 禁用默认健康检查服务。
		HealthProbeBindAddress: "0",
		// 禁用默认性能分析服务。
		PprofBindAddress: "0",
	}

	// 创建 controller manager 并对其进行包装。
	var ctrlManager CtrlManager
	{
		rawCtrlManager, err := ctrl.NewManager(rest.CopyConfig(&c.LoopbackKubeRestConfig), ctrlMgrOpts)
		if err != nil {
			return nil, fmt.Errorf("create controller manager: %w", err)
		}
		ctrlManager = CtrlManager{
			Manager:       rawCtrlManager,
			options:       ctrlMgrOpts,
			httpClient:    c.LoopbackKubeHTTPClient,
			indexedFields: sets.Set[string]{},
		}
	}

	// 把启动信号作为 runnable 注册，让 WaitForReady 能等待实际启动。
	sentinel := _CtrlManagerSentinel{done: make(chan struct{})}
	err := ctrlManager.Add(sentinel)
	if err != nil {
		return nil, fmt.Errorf("add controller manager sentinel: %w", err)
	}

	// 普通读取可以命中缓存；需要确认最新状态时走直连 APIReader。
	system.ConfigureLoopbackCtrlRuntime(ctrlManager.GetClient(), ctrlManager.GetAPIReader())

	return &Manager{
		KubeClient:  c.LoopbackKubeClient,
		CtrlManager: ctrlManager,
		sentinel:    sentinel,
	}, nil
}
