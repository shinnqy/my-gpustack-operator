package modelmanager

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	authenticationv1 "k8s.io/api/authentication/v1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	toolscache "k8s.io/client-go/tools/cache"
	klog "k8s.io/klog/v2"
	ctrlcache "sigs.k8s.io/controller-runtime/pkg/cache"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes"
	"gpustack.ai/gpustack/pkg/manager"
	"gpustack.ai/gpustack/pkg/modelmanager/download"
	"gpustack.ai/gpustack/pkg/modelmanager/driver"
	"gpustack.ai/gpustack/pkg/modelmanager/gc"
	"gpustack.ai/gpustack/pkg/modelmanager/materialize"
	"gpustack.ai/gpustack/pkg/modelmanager/metrics"
	"gpustack.ai/gpustack/pkg/modelmanager/peer"
	"gpustack.ai/gpustack/pkg/modelmanager/report"
	"gpustack.ai/gpustack/pkg/modelmanager/store"
	"gpustack.ai/gpustack/pkg/modelstore"
	"gpustack.ai/gpustack/pkg/system"
	"gpustack.ai/gpustack/pkg/systemname"
	"gpustack.ai/gpustack/pkg/utils/gox"
	"gpustack.ai/gpustack/pkg/utils/version"
)

// Manager 是单个节点上的 model-manager 插件主体：整合 CSI 服务、物化器、垃圾回收器与状态上报器。
type Manager struct {
	Manager *manager.Manager
	Store   *store.Store

	NodeName   string
	KubeletDir string
	CSISocket  string

	// PeerSync 非空时对外提供本节点已发布的目录树，并允许物化器从其他节点拉取。
	PeerSync *peer.Sync

	ready atomic.Bool
}

// Prepare 注册 Prometheus 指标并准备底层管理器。
func (m *Manager) Prepare(ctx context.Context) error {
	if err := m.Manager.Prepare(ctx); err != nil {
		return err
	}
	for _, c := range metrics.Collectors() {
		if err := ctrlmetrics.Registry.Register(c); err != nil {
			return fmt.Errorf("register metric: %w", err)
		}
	}

	return nil
}

// Start 启动 CSI 服务与管理器。在缓存完成同步且从节点实际挂载重建引用前，挂载调用一律返回 Unavailable。
func (m *Manager) Start(ctx context.Context) error {
	cm := m.Manager.CtrlManager

	downloader := download.New(nil, 1, 0)
	downloader.Received = func(n int64) { metrics.DownloadBytes.WithLabelValues("hub").Add(float64(n)) }
	materializer := &materialize.Materializer{Store: m.Store, Now: time.Now, Base: ctx}
	// collector（GC）需要知道三件事：磁盘用了多少 (gc.Statfs)、谁正在挂着 (m.referenced)、谁正在下载 (materializer.Downloading)
	collector := &gc.Collector{
		Store:       m.Store,
		Usage:       func() (gc.Usage, error) { return gc.Statfs(m.Store.Root()) },
		Referenced:  m.referenced,
		Downloading: materializer.Downloading,
		Now:         time.Now,
	}
	kubeletCap, err := m.kubeletCap()
	if err != nil {
		return err
	}
	reporter := m.newReporter(cm, collector, materializer, downloader, kubeletCap)
	collector.Watermarks = reporter.Watermarks
	collector.Pinned = reporter.Pinned
	materializer.Environment = reporter.Environment
	if m.PeerSync != nil {
		puller, err := m.newPeerPuller(reporter)
		if err != nil {
			return err
		}
		materializer.Peers = puller
	}
	materializer.Reserve = collector.Reserve
	materializer.Changed = reporter.Trigger

	cm.GetWebhookServer().Register(modelstore.DownloadsPath, newDownloadsHandler(materializer.Progress))

	d := &driver.Driver{
		Name:         modelstore.DriverName,
		Version:      version.Get(),
		NodeID:       m.NodeName,
		KubeletDir:   m.KubeletDir,
		Store:        m.Store,
		Artifacts:    cm.GetCache(),
		Ready:        m.ready.Load,
		Mounter:      driver.HostMounter{},
		Materializer: materializer,
		Events:       reporter,
	}

	// 4 个并发运行的协程组
	gp := gox.GroupWithContextIn(ctx)
	gp.Go(func(ctx context.Context) error {
		// 协程 1：启动 controller-runtime Manager（建立与 API Server 的 Watch 连接和 HTTP 服务器）
		klog.Info("starting controller manager")
		return m.Manager.Start(ctx)
	})

	if m.PeerSync != nil {
		gp.Go(func(ctx context.Context) error {
			// 协程 2（可选）：若开启 PeerSync，启动 HTTPS Peer 服务端，供其他节点来本机拉模型。
			return m.servePeers(ctx)
		})
	}
	gp.Go(func(ctx context.Context) error {
		// 协程 3（最关键的“冷启动门禁”！）
		if err := m.Manager.WaitForReady(ctx); err != nil {
			return fmt.Errorf("wait for manager ready: %w", err)
		}
		if err := m.rebuild(); err != nil {
			return fmt.Errorf("rebuild references: %w", err)
		}
		// 每次挂载都从该 informer 读取 ModelArtifact：在对外服务前启动并同步，避免挂载等待首次同步。
		if _, err := cm.GetCache().GetInformer(ctx, &workercore.ModelArtifact{}, ctrlcache.BlockUntilSynced(true)); err != nil {
			return fmt.Errorf("watch ModelArtifacts: %w", err)
		}
		// 本节点的 NodeModelStore 由 worker 在插件注册后创建，其 spec 与 CA bundle 均可在运行时变更，
		// 任一变更都需要重新应用并上报；而对象自身的 status 更新是本插件写入的回声，予以过滤。
		for _, w := range []struct {
			obj     ctrlcli.Object
			changed func(oldObj, newObj any) bool
		}{
			{&workercore.NodeModelStore{}, ownObjectChanged},
			{&core.ConfigMap{}, func(any, any) bool { return true }},
		} {
			inf, err := cm.GetCache().GetInformer(ctx, w.obj)
			if err != nil {
				return fmt.Errorf("watch %T: %w", w.obj, err)
			}
			trigger := func(any) { reporter.Trigger("") }
			if _, err := inf.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
				AddFunc: trigger,
				UpdateFunc: func(o, n any) {
					if w.changed(o, n) {
						trigger(n)
					}
				},
				DeleteFunc: trigger,
			}); err != nil {
				return fmt.Errorf("watch %T: %w", w.obj, err)
			}
		}
		m.ready.Store(true)
		klog.Info("serving mounts")
		return reporter.Run(ctx)
	})
	gp.Go(func(ctx context.Context) error {
		return serveCSI(ctx, m.CSISocket, d)
	})

	return gp.Wait()
}

// newReporter 构建本节点对象的状态上报器。读取走 API Reader 而非缓存：上报负责收敛生效配置，
// 避免 informer 停滞时导致配置永久卡在旧版本。
func (m *Manager) newReporter(
	cm manager.CtrlManager, collector *gc.Collector, materializer *materialize.Materializer,
	downloader *download.Downloader, kubeletCap func(*workercore.NodeModelStoreKubelet, uint64) *gc.Cap,
) *report.Reporter {
	return &report.Reporter{
		Reader: cm.GetAPIReader(), Client: cm.GetClient(), NodeName: m.NodeName, Namespace: systemname.NamespaceName,
		Store: m.Store, Collector: collector, Downloading: materializer.Downloading, Progress: materializer.Progress, Downloader: downloader,
		KubeletCap: kubeletCap, Now: time.Now,
	}
}

// rebuild 根据节点当前挂载表和账本重建引用，并清理中断删除留下的垃圾目录。
func (m *Manager) rebuild() error {
	if err := m.Store.EmptyTrash(); err != nil {
		return err
	}
	device, err := store.Device(m.Store.Root())
	if err != nil {
		return err
	}
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	mounts, err := store.ParseMountInfo(f)
	if err != nil {
		return err
	}
	rebuilt, err := m.Store.Rebuild(mounts, device)
	if err != nil {
		return err
	}
	klog.InfoS("rebuilt references", "kept", len(rebuilt.Kept), "corrected", len(rebuilt.Corrected),
		"adopted", len(rebuilt.Adopted), "dropped", len(rebuilt.Dropped))

	return nil
}

// referenced 从账本返回当前仍有挂载目标引用的摘要集合。
// 账本读取失败时必须返回错误，绝不返回空集合，以免回收器误删正在使用的目录树。
func (m *Manager) referenced() (map[string]bool, error) {
	refs, err := m.Store.Refs()
	if err != nil {
		return nil, fmt.Errorf("read the references: %w", err)
	}
	set := make(map[string]bool, len(refs))
	for _, r := range refs {
		set[r.Hex] = true
	}

	return set, nil
}

// kubeletCap 在缓存目录与 kubelet pods 目录位于同一文件系统时返回高水位上限计算函数；
// 缓存独占文件系统时返回 nil，直接按配置水位执行。阈值来自节点 spec，插件不直接读取 kubelet 配置文件。
func (m *Manager) kubeletCap() (func(*workercore.NodeModelStoreKubelet, uint64) *gc.Cap, error) {
	cache, err := store.Device(m.Store.Root())
	if err != nil {
		return nil, err
	}
	kubelet, err := store.Device(filepath.Join(m.KubeletDir, "pods"))
	if err != nil {
		return nil, err
	}
	if cache != kubelet {
		return nil, nil
	}
	return func(k *workercore.NodeModelStoreKubelet, total uint64) *gc.Cap {
		c := gc.KubeletCap(k, total)
		return &c
	}, nil
}

// newPeerPuller 构建物化器使用的 peer 拉取器：通过 API Reader 发现持有摘要的节点，
// 并将 peer 接收字节与 Hub 字节分开统计。每次尝试均从上报器当前 spec 读取开关状态。
func (m *Manager) newPeerPuller(reporter *report.Reporter) (*peer.Puller, error) {
	client, err := peer.NewClient(m.PeerSync.Auth)
	if err != nil {
		return nil, err
	}

	return &peer.Puller{
		Discover: func(ctx context.Context, digest string) ([]*peer.Source, error) {
			if !reporter.PeerSyncEnabled() {
				return nil, nil
			}

			return peer.Discover(ctx, m.Manager.CtrlManager.GetAPIReader(), peer.DiscoverOptions{
				Namespace:        systemname.NamespaceName,
				SelfNode:         m.NodeName,
				Port:             m.PeerSync.Port,
				StreamsPerSource: m.PeerSync.StreamsPerSource,
				Client:           client,
			}, digest)
		},
		Received: func(node string, n int64) {
			metrics.DownloadBytes.WithLabelValues("peer").Add(float64(n))
		},
	}, nil
}

// servePeers 在 peer 端口上响应其他节点插件的拉取请求，直至 ctx 结束。
func (m *Manager) servePeers(ctx context.Context) error {
	lis, err := peer.Listen(m.PeerSync.Port, m.PeerSync.Auth)
	if err != nil {
		return err
	}
	admit, err := m.PeerSync.Auth.Admit(tokenReviews{cli: system.LoopbackKubeClient.Get()})
	if err != nil {
		return err
	}
	srv := &peer.Server{Store: m.Store, Admit: admit, MaxStreams: m.PeerSync.MaxServingStreams}
	klog.InfoS("serving peers", "port", m.PeerSync.Port, "auth", m.PeerSync.Auth.Mode)

	return srv.Serve(ctx, lis)
}

// tokenReviews 通过 API Server 审核 peer 请求携带的 Bearer 令牌，与 worker 状态 webhook 方式一致。
type tokenReviews struct {
	cli kubernetes.Interface
}

func (t tokenReviews) Create(
	ctx context.Context, review *authenticationv1.TokenReview, opts meta.CreateOptions,
) (*authenticationv1.TokenReview, error) {
	return t.cli.AuthenticationV1().TokenReviews().Create(ctx, review, opts)
}

// serveCSI 在 Unix 套接字上提供 CSI Identity 与 Node 服务，直至 ctx 结束。
func serveCSI(ctx context.Context, socket string, d *driver.Driver) error {
	if err := os.MkdirAll(filepath.Dir(socket), 0o750); err != nil {
		return err
	}
	if err := os.Remove(socket); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("remove the stale socket: %w", err)
	}
	lis, err := net.Listen("unix", socket)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", socket, err)
	}

	srv := grpc.NewServer()
	csi.RegisterIdentityServer(srv, d)
	csi.RegisterNodeServer(srv, d)
	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()
	klog.InfoS("serving CSI", "socket", socket)

	return srv.Serve(lis)
}
