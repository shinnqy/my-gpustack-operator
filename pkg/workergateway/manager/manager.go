package manager

import (
	"context"
	"fmt"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	core "k8s.io/api/core/v1"
	kmeta "k8s.io/apimachinery/pkg/api/meta"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	klog "k8s.io/klog/v2"
	"k8s.io/utils/ptr"

	worker "gpustack.ai/gpustack/api/worker/v1"
	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes"
	"gpustack.ai/gpustack/pkg/utils/events/topic"
	"gpustack.ai/gpustack/pkg/utils/gox"
	"gpustack.ai/gpustack/pkg/worker/apis"
)

type (
	// ProcessObjectFunc 接收来源集群和一个 Kubernetes 对象。返回错误会中止迭代，并把错误传给调用方。
	ProcessObjectFunc = func(cluster string, obj runtime.Object) error

	// WorkerEventType 使用 Kubernetes Watch 的事件类型。
	WorkerEventType = watch.EventType

	// WorkerEvent 保存某个 Worker 的 informer 观察到的变化。
	WorkerEvent struct {
		// Type 表示新增、修改或删除事件。
		Type WorkerEventType `json:"type"`
		// Cluster 标识事件来自哪个集群。
		Cluster string `json:"cluster"`
		// Object 是发生变化的对象。删除事件的 Object 为 nil 时，表示要清除该集群在此资源类型下的所有对象。
		Object any `json:"object"`
	}

	// IteratorOptions 指定跨 Worker 读取对象时使用的过滤和复制选项。
	IteratorOptions struct {
		// Namespace 限定要读取的命名空间。留空表示读取所有命名空间。
		Namespace string
		// DeepCopy 为真时先复制对象再传给处理函数。为假时处理函数收到的可能是缓存对象，必须只读。
		DeepCopy bool
		// LabelSelector 只保留标签匹配的对象。留空表示不按标签过滤。
		LabelSelector labels.Set
	}

	// Manager 管理集群订阅，并按调用方指定的范围遍历各集群的对象。
	Manager interface {
		// SubscribeWorker 为指定集群订阅一组资源类型，每种类型分别启动 informer。GVK 列表为空时仍能走实时 List，
		// 但该集群不会通过 informer 产生 Watch 事件。force 会先结束之前的订阅。
		SubscribeWorker(ctx context.Context, cluster, token string, gvks []schema.GroupVersionKind, force bool) error
		// UnsubscribeWorker 结束指定集群的订阅，包括还在等待就绪的尝试。
		UnsubscribeWorker(ctx context.Context, cluster string)
		// IterateWorkers 遍历所选集群中指定类型的对象，对满足条件的对象调用处理函数。
		IterateWorkers(ctx context.Context, clusters []string, gvk schema.GroupVersionKind, opts IteratorOptions, processor ProcessObjectFunc) error
	}
)

func (we *WorkerEvent) DeepCopy() *WorkerEvent {
	if we == nil {
		return nil
	}

	ro, ok := we.Object.(runtime.Object)
	if ok {
		return &WorkerEvent{
			Type:    we.Type,
			Cluster: we.Cluster,
			Object:  ro.DeepCopyObject(),
		}
	}

	return &WorkerEvent{
		Type:    we.Type,
		Cluster: we.Cluster,
		Object:  we.Object,
	}
}

const (
	WorkerEventAdded    = watch.Added
	WorkerEventModified = watch.Modified
	WorkerEventDeleted  = watch.Deleted
)

// defaultReadinessCheckTimeout 是单轮就绪检查的默认时限。即使代理已经接收连接却一直等不到 Worker 响应，
// 本轮也能及时结束，进入退避重试。
const defaultReadinessCheckTimeout = 30 * time.Second

// newReadinessBackoff 创建每个集群独享的重试节奏。前几轮较快，随后逐步延长到最多一分钟。Step 会修改计数，
// 因此每个订阅各持有一份。
func newReadinessBackoff() wait.Backoff {
	return wait.Backoff{
		Duration: 2 * time.Second,
		Factor:   2,
		Cap:      time.Minute,
		Steps:    math.MaxInt32,
	}
}

type (
	Cluster = string

	_Manager struct {
		sync.RWMutex

		Logger  klog.Logger
		Context context.Context
		Workers map[Cluster]*_Worker
		// Pending 按集群保存订阅尝试，早于 Worker 注册。重复请求据此复用已有尝试，取消操作也能终止正在等待就绪的任务。
		Pending             map[Cluster]*_Pending
		ConstructRestConfig ConstructRestConfigFunc
		ResyncPeriod        time.Duration
		// 这三项由 New 设置。一次就绪检查受超时限制，重试间隔由退避控制。测试可以分别控制检查结果和时间。
		IsServicesReady       func(context.Context, kubernetes.Interface) error
		ReadinessCheckTimeout time.Duration
		ReadinessBackoff      wait.Backoff
	}

	// _Pending 表示一次订阅尝试。通过比较指针，较早的后台任务结束时不会误删后来的订阅。
	_Pending struct {
		Cancel context.CancelFunc
	}

	_Worker struct {
		Context   context.Context
		Cancel    context.CancelFunc
		Cluster   string
		Client    kubernetes.Interface
		Informers map[schema.GroupVersionKind]cache.SharedIndexInformer
		AllReady  atomic.Bool
	}
)

// New 以管理器上下文和连接配置创建集群管理器。
func New(ctx context.Context, config *Config) (Manager, error) {
	// 零时长会让发出的检查立即超时，未指定有效值时采用默认时限。
	readinessCheckTimeout := config.ReadinessCheckTimeout
	if readinessCheckTimeout <= 0 {
		readinessCheckTimeout = defaultReadinessCheckTimeout
	}

	return &_Manager{
		Logger:              klog.FromContext(ctx).WithName("manager"),
		Context:             ctx,
		ConstructRestConfig: config.ConstructRestConfig,
		ResyncPeriod:        config.ResyncPeriod,
		Workers:             make(map[Cluster]*_Worker),
		Pending:             make(map[Cluster]*_Pending),

		IsServicesReady:       apis.IsServicesReady,
		ReadinessCheckTimeout: readinessCheckTimeout,
		ReadinessBackoff:      newReadinessBackoff(),
	}, nil
}

func (wm *_Manager) SubscribeWorker(ctx context.Context, cluster, token string, gvks []schema.GroupVersionKind, force bool) error {
	logger := wm.Logger.WithValues("cluster", cluster)

	if force {
		wm.UnsubscribeWorker(ctx, cluster)
	}

	wkCtx, wkCancel := context.WithCancel(klog.NewContext(wm.Context, logger))

	// 先按集群取得占位，再构建连接。这样重复订阅只会启动一条就绪检查循环，取消请求也能中止等待中的订阅。
	pending, claimed := wm.claimPending(cluster, wkCancel)
	if !claimed {
		wkCancel()
		logger.V(2).Info("worker already subscribed or pending, skip")
		return nil
	}

	cfg, err := wm.ConstructRestConfig(cluster, token)
	if err != nil {
		wm.releasePending(cluster, pending)
		wkCancel()
		logger.Error(err, "construct rest config")
		return fmt.Errorf("construct rest config for cluster %q: %w", cluster, err)
	}

	cli, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		wm.releasePending(cluster, pending)
		wkCancel()
		logger.Error(err, "construct kubernetes client")
		return fmt.Errorf("construct kubernetes client for cluster %q: %w", cluster, err)
	}

	gox.Go(func() {
		// 只要这条循环仍负责该集群，就继续持有占位，包括 Worker 已注册并正在运行 informer 的阶段。
		defer wm.releasePending(cluster, pending)

		backoff := wm.ReadinessBackoff
		reported := false

		for {
			select {
			case <-wkCtx.Done():
				return
			default:
			}

			logger.V(2).Info("checking worker api services")
			checkCtx, checkCancel := context.WithTimeout(wkCtx, wm.ReadinessCheckTimeout)
			err := wm.IsServicesReady(checkCtx, cli)
			checkCancel()
			if err != nil {
				// 这里检查订阅上下文。单次检查自身超时只表示本轮失败，应继续重试。订阅被取消才结束整个循环。
				if wkCtx.Err() != nil {
					// 在检查期间已取消订阅，直接退出即可。
					return
				}
				if reported {
					// 同一段连续失败只把第一次记为错误，后续尝试继续执行并使用较低级别的日志。
					logger.V(2).Info("worker api services still not ready", "err", err)
				} else {
					logger.Error(err, "check api services ready")
					reported = true
				}

				select {
				case <-wkCtx.Done():
					return
				case <-time.After(backoff.Step()):
				}
				continue
			}
			backoff = wm.ReadinessBackoff
			reported = false

			wm.Lock()
			// 就绪检查成功时，当前尝试可能刚好被取消或替换。持锁核对占位和上下文，只有仍然拥有此集群的尝试可以注册 Worker。
			if wkCtx.Err() != nil || wm.Pending[cluster] != pending {
				wm.Unlock()
				wkCancel()
				logger.V(2).Info("attempt no longer owns the cluster, skip")
				return
			}
			wk := newWorker(wkCtx, wkCancel, cluster, cli, wm.ResyncPeriod, gvks)
			wm.Workers[cluster] = wk
			wm.Unlock()

			logger.Info("subscribing worker", "gvks", gvks)
			// 正常取消订阅时，informer 的运行返回上下文取消。只有订阅仍有效时才将 Subscribe 的错误记为异常。
			if err := wk.Subscribe(); err != nil && wkCtx.Err() == nil {
				logger.Error(err, "subscribe worker")
			}

			wm.Lock()
			// 只移除本轮注册的 Worker。强制重新订阅可能已经写入新对象，旧任务收尾时必须保留新对象。
			if wm.Workers[cluster] == wk {
				delete(wm.Workers, cluster)
			}
			wm.Unlock()
		}
	})

	return nil
}

func (wm *_Manager) UnsubscribeWorker(ctx context.Context, cluster string) {
	wm.Lock()

	// 在锁内立即释放占位，使后续订阅可以马上取得该集群，无须等旧任务完成退出。
	pending, isPending := wm.Pending[cluster]
	delete(wm.Pending, cluster)

	wk, hasWorker := wm.Workers[cluster]
	delete(wm.Workers, cluster)
	wm.Unlock()

	if !isPending && !hasWorker {
		return
	}
	// 在锁外取消 Worker 和后台尝试。它们已从管理器索引移除，而取消 Worker 可能向事件总线发送通知。
	if hasWorker {
		wk.Unsubscribe(ctx)
	}
	if isPending {
		// 即使尚未成功注册 Worker，也要终止对应的就绪检查，避免它继续访问已取消的集群。
		pending.Cancel()
	}
	if hasWorker {
		wm.Logger.Info("unsubscribed worker", "cluster", cluster)
	} else {
		wm.Logger.Info("canceled pending worker subscription", "cluster", cluster)
	}
}

// claimPending 为一次订阅占用集群名。如果已有 Worker 或进行中的尝试，返回 false。
func (wm *_Manager) claimPending(cluster string, cancel context.CancelFunc) (*_Pending, bool) {
	wm.Lock()
	defer wm.Unlock()

	if _, ok := wm.Workers[cluster]; ok {
		return nil, false
	}
	if _, ok := wm.Pending[cluster]; ok {
		return nil, false
	}

	pending := &_Pending{Cancel: cancel}
	wm.Pending[cluster] = pending
	return pending, true
}

// listClusters 从当前已注册 Worker 取得集群名快照并排序，使遍历顺序稳定，也避免单独维护容易过期的集群列表。
func (wm *_Manager) listClusters() []Cluster {
	wm.RLock()
	clusters := make([]Cluster, 0, len(wm.Workers))
	for cluster := range wm.Workers {
		clusters = append(clusters, cluster)
	}
	wm.RUnlock()

	// 快照已经归当前调用所有，释放锁后再排序，避免阻塞其他订阅或取消请求。
	slices.Sort(clusters)

	return clusters
}

// releasePending 只释放本次尝试仍然持有的占位。
func (wm *_Manager) releasePending(cluster string, pending *_Pending) {
	wm.Lock()
	defer wm.Unlock()

	if wm.Pending[cluster] == pending {
		delete(wm.Pending, cluster)
	}
}

func (wm *_Manager) IterateWorkers(
	ctx context.Context,
	clusters []string,
	gvk schema.GroupVersionKind,
	opts IteratorOptions,
	processor ProcessObjectFunc,
) error {
	if len(clusters) == 0 {
		clusters = wm.listClusters()
	}
	if gvk.Empty() {
		return nil
	}
	if processor == nil {
		return nil
	}

	var labelSel labels.Selector
	if len(opts.LabelSelector) != 0 {
		labelSel = labels.SelectorFromValidatedSet(opts.LabelSelector)
		if labelSel.Empty() {
			labelSel = nil
		}
	}

	for _, cluster := range clusters {
		logger := wm.Logger.WithValues("cluster", cluster, "gvk", gvk)

		wm.RLock()
		wk, ok := wm.Workers[cluster]
		if !ok {
			logger.Info("worker not found, skip")
			wm.RUnlock()
			continue
		}
		inf, hasInformer := wk.Informers[gvk]
		cli := wk.Client
		wm.RUnlock()

		// 已为该 GVK 建立 informer 时从缓存读取。该 Worker 未订阅此 GVK 时，查找对应的实时 List 实现。
		// 两种读取方式都不存在时跳过。
		var objs []any
		switch {
		case hasInformer:
			if !inf.HasSynced() {
				logger.Info("informer not synced, skip")
				continue
			}
			if opts.Namespace == "" {
				objs = inf.GetIndexer().List()
			} else {
				objs, _ = inf.GetIndexer().ByIndex(cache.NamespaceIndex, opts.Namespace)
			}
		default:
			lister, ok := defaultListerFactories[gvk]
			if !ok {
				logger.Info("no informer or lister for gvk, skip")
				continue
			}
			ros, err := lister(ctx, cli, opts.Namespace)
			if err != nil {
				// 一个集群的实时 List 失败时跳过它，继续返回其他可访问集群的结果。
				logger.Error(err, "list objects, skip")
				continue
			}
			objs = ros
		}

		for _, obj := range objs {
			ro, ok := obj.(runtime.Object)
			if !ok {
				logger.Error(nil, "assert object to runtime.Object")
				continue
			}
			mo, err := kmeta.Accessor(ro)
			if err != nil {
				logger.Error(err, "access object meta")
				continue
			}
			if labelSel != nil && !labelSel.Matches(labels.Set(mo.GetLabels())) {
				continue
			}

			var oro runtime.Object
			if opts.DeepCopy {
				oro = ro.DeepCopyObject()
				oro.GetObjectKind().SetGroupVersionKind(gvk)
			} else {
				oro = ro
			}

			if err = processor(cluster, oro); err != nil {
				logger.Error(err, "process object")
				return fmt.Errorf("process object: %w", err)
			}
		}
	}

	return nil
}

// WorkerEventTopic 根据 GVK 得到事件主题，使同一资源类型的发布端与 Watch 消费者使用相同的名字。
func WorkerEventTopic(gvk schema.GroupVersionKind) topic.Topic {
	return topic.Topic("workergateway/" + gvk.String())
}

// newWorker 为本次订阅支持的每个 GVK 建立一个共享 informer。重复的 GVK 只建一次，未登记工厂的 GVK 会被跳过。
func newWorker(
	ctx context.Context,
	cancel context.CancelFunc,
	cluster string,
	cli kubernetes.Interface,
	resyncPeriod time.Duration,
	gvks []schema.GroupVersionKind,
) *_Worker {
	logger := klog.FromContext(ctx)

	wk := &_Worker{
		Context:   ctx,
		Cancel:    cancel,
		Cluster:   cluster,
		Client:    cli,
		Informers: make(map[schema.GroupVersionKind]cache.SharedIndexInformer),
	}

	seen := make(map[schema.GroupVersionKind]struct{}, len(gvks))
	for _, gvk := range gvks {
		if _, ok := seen[gvk]; ok {
			continue
		}
		seen[gvk] = struct{}{}

		factory, ok := defaultInformerFactories[gvk]
		if !ok {
			logger.Info("no informer factory registered for gvk, skip", "gvk", gvk)
			continue
		}
		inf := factory(cli, resyncPeriod)
		registerEventHandler(ctx, inf, cluster, gvk)
		wk.Informers[gvk] = inf
	}

	return wk
}

func registerEventHandler(
	ctx context.Context,
	inf cache.SharedIndexInformer,
	cluster string,
	gvk schema.GroupVersionKind,
) {
	t := WorkerEventTopic(gvk)
	publishEvent := func(et watch.EventType, obj any) {
		ro, ok := obj.(runtime.Object)
		if !ok {
			if dfu, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				ro, _ = dfu.Obj.(runtime.Object)
			}
		}
		if ro == nil {
			return
		}
		_ = topic.Publish(ctx, t, &WorkerEvent{
			Type:    et,
			Cluster: cluster,
			Object:  ro,
		})
	}

	handler := cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { publishEvent(WorkerEventAdded, obj) },
		UpdateFunc: func(_, obj any) { publishEvent(WorkerEventModified, obj) },
		DeleteFunc: func(obj any) { publishEvent(WorkerEventDeleted, obj) },
	}
	opts := cache.HandlerOptions{
		Logger: ptr.To(klog.FromContext(ctx).WithValues("gvk", gvk).V(4)),
	}

	_, _ = inf.AddEventHandlerWithOptions(handler, opts)
}

func (w *_Worker) Subscribe() error {
	gp := gox.GroupWithContextIn(w.Context)

	for gvk := range w.Informers {
		inf := w.Informers[gvk]
		gp.Go(func(ctx context.Context) error {
			inf.RunWithContext(ctx)
			return ctx.Err()
		})
	}

	gp.Go(func(ctx context.Context) error {
		logger := klog.FromContext(ctx)
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			allSynced := true
			for _, inf := range w.Informers {
				if !inf.HasSynced() {
					allSynced = false
					break
				}
			}
			if !allSynced {
				time.Sleep(1 * time.Second)
				continue
			}
			w.AllReady.Store(true)
			logger.V(2).Info("synced all informers")
			return ctx.Err()
		}
	})

	// 有 informer 时，运行任务保持到订阅取消，并将任务错误交还外层。没有 informer 时，缓存同步任务可立即成功，这时仍需等待取消，
	// 保证只提供实时 List 的 Worker 持续注册。
	if err := gp.Wait(); err != nil {
		return err
	}
	<-w.Context.Done()
	return nil
}

func (w *_Worker) Unsubscribe(ctx context.Context) {
	w.Cancel()

	for gvk := range w.Informers {
		t := WorkerEventTopic(gvk)
		_ = topic.Publish(ctx, t, &WorkerEvent{
			Type:    WorkerEventDeleted,
			Cluster: w.Cluster,
			Object:  nil,
		})
	}
}

var defaultInformerFactories = map[schema.GroupVersionKind]func(kubernetes.Interface, time.Duration) cache.SharedIndexInformer{
	worker.SchemeGroupVersionKind("Devices"): func(cli kubernetes.Interface, p time.Duration) cache.SharedIndexInformer {
		return NewSharedIndexInformerWithOptions(cli.WorkerV1().Devices(), &worker.Devices{}, p)
	},
	worker.SchemeGroupVersionKind("InstanceTypeFlavor"): func(cli kubernetes.Interface, p time.Duration) cache.SharedIndexInformer {
		return NewSharedIndexInformerWithOptions(cli.WorkerV1().InstanceTypeFlavors(), &worker.InstanceTypeFlavor{}, p)
	},
	worker.SchemeGroupVersionKind("InstanceType"): func(cli kubernetes.Interface, p time.Duration) cache.SharedIndexInformer {
		return NewSharedIndexInformerWithOptions(cli.WorkerV1().InstanceTypes(), &worker.InstanceType{}, p)
	},
	worker.SchemeGroupVersionKind("Instance"): func(cli kubernetes.Interface, p time.Duration) cache.SharedIndexInformer {
		return NewSharedIndexInformerWithOptions(cli.WorkerV1().Instances(core.NamespaceAll), &worker.Instance{}, p)
	},
	worker.SchemeGroupVersionKind("InstancePersistentVolumeType"): func(cli kubernetes.Interface, p time.Duration) cache.SharedIndexInformer {
		return NewSharedIndexInformerWithOptions(cli.WorkerV1().InstancePersistentVolumeTypes(), &worker.InstancePersistentVolumeType{}, p)
	},
	worker.SchemeGroupVersionKind("InstancePersistentVolume"): func(cli kubernetes.Interface, p time.Duration) cache.SharedIndexInformer {
		return NewSharedIndexInformerWithOptions(cli.WorkerV1().InstancePersistentVolumes(core.NamespaceAll), &worker.InstancePersistentVolume{}, p)
	},
	worker.SchemeGroupVersionKind("InstanceImagePullSecret"): func(cli kubernetes.Interface, p time.Duration) cache.SharedIndexInformer {
		return NewSharedIndexInformerWithOptions(cli.WorkerV1().InstanceImagePullSecrets(core.NamespaceAll), &worker.InstanceImagePullSecret{}, p)
	},
	worker.SchemeGroupVersionKind("InstanceSSHPublicKey"): func(cli kubernetes.Interface, p time.Duration) cache.SharedIndexInformer {
		return NewSharedIndexInformerWithOptions(cli.WorkerV1().InstanceSSHPublicKeys(core.NamespaceAll), &worker.InstanceSSHPublicKey{}, p)
	},
}

// _RuntimeObjectLister 描述 typed client 的 List 方法。
// 返回的是对应资源的 Kubernetes 列表对象。
type _RuntimeObjectLister[T runtime.Object] interface {
	List(ctx context.Context, opts meta.ListOptions) (T, error)
}

// listAll 请求 API Server 缓存中的列表，再通过 EachListItem 逐个取出对象，
// 交给 IterateWorkers 的公共处理流程。
func listAll[T runtime.Object](ctx context.Context, lister _RuntimeObjectLister[T]) ([]any, error) {
	list, err := lister.List(ctx, meta.ListOptions{ResourceVersion: "0"})
	if err != nil {
		return nil, err
	}

	var objs []any
	err = kmeta.EachListItem(list, func(obj runtime.Object) error {
		objs = append(objs, obj)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return objs, nil
}

// defaultListerFactories 为每个已知 Worker GVK 提供实时查询入口。即便订阅的 GVK 集合为空或只包含部分资源，
// 其他已知类型仍可查询。
var defaultListerFactories = map[schema.GroupVersionKind]func(context.Context, kubernetes.Interface, string) ([]any, error){
	worker.SchemeGroupVersionKind("Devices"): func(ctx context.Context, cli kubernetes.Interface, _ string) ([]any, error) {
		return listAll(ctx, cli.WorkerV1().Devices())
	},
	worker.SchemeGroupVersionKind("InstanceTypeFlavor"): func(ctx context.Context, cli kubernetes.Interface, _ string) ([]any, error) {
		return listAll(ctx, cli.WorkerV1().InstanceTypeFlavors())
	},
	worker.SchemeGroupVersionKind("InstanceType"): func(ctx context.Context, cli kubernetes.Interface, _ string) ([]any, error) {
		return listAll(ctx, cli.WorkerV1().InstanceTypes())
	},
	worker.SchemeGroupVersionKind("Instance"): func(ctx context.Context, cli kubernetes.Interface, ns string) ([]any, error) {
		return listAll(ctx, cli.WorkerV1().Instances(ns))
	},
	worker.SchemeGroupVersionKind("InstancePersistentVolumeType"): func(ctx context.Context, cli kubernetes.Interface, _ string) ([]any, error) {
		return listAll(ctx, cli.WorkerV1().InstancePersistentVolumeTypes())
	},
	worker.SchemeGroupVersionKind("InstancePersistentVolume"): func(ctx context.Context, cli kubernetes.Interface, ns string) ([]any, error) {
		return listAll(ctx, cli.WorkerV1().InstancePersistentVolumes(ns))
	},
	worker.SchemeGroupVersionKind("InstanceImagePullSecret"): func(ctx context.Context, cli kubernetes.Interface, ns string) ([]any, error) {
		return listAll(ctx, cli.WorkerV1().InstanceImagePullSecrets(ns))
	},
	worker.SchemeGroupVersionKind("InstanceSSHPublicKey"): func(ctx context.Context, cli kubernetes.Interface, ns string) ([]any, error) {
		return listAll(ctx, cli.WorkerV1().InstanceSSHPublicKeys(ns))
	},
}
