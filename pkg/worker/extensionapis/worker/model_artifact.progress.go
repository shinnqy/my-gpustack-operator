package worker

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/registry/rest"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	worker "gpustack.ai/gpustack/api/worker/v1"
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/deviceplugin"
	"gpustack.ai/gpustack/pkg/extensionapi"
	"gpustack.ai/gpustack/pkg/modelstore"
	"gpustack.ai/gpustack/pkg/utils/ctrlclix"
	"gpustack.ai/gpustack/pkg/utils/httpx"
	"gpustack.ai/gpustack/pkg/utils/json"
	workerctrl "gpustack.ai/gpustack/pkg/worker/controllers/worker"
	"gpustack.ai/gpustack/pkg/worker/kuberess"
)

const (
	// _ModelArtifactProgressTimeout 限制单次 progress 响应中固定部分的耗时：包括读取
	// artifact、列举 store 及聚合操作，不含实时扇出查询。
	_ModelArtifactProgressTimeout = 5 * time.Second
	// _ModelArtifactProgressNodeTimeout 限制单个节点的实时读取耗时；超时的节点将沿用其
	// 上次写入的值。
	_ModelArtifactProgressNodeTimeout = 2 * time.Second
	// _ModelArtifactProgressMaxNodes 限制单次响应的实时读取节点上限，_ModelArtifactProgressParallel
	// 限制并发读取数。
	_ModelArtifactProgressMaxNodes  = 64
	_ModelArtifactProgressParallel  = 16
	_ModelArtifactProgressMaxBytes  = 1 << 20
	_ModelArtifactProgressNotResolv = "the artifact is not resolved yet, so it has no content to count"
	_ModelArtifactProgressClaim     = "a claim source is mounted from its volume and never downloaded"
)

// modelArtifactProgressTimeout 限制完整单次响应的总耗时：固定部分加上实时扇出的最坏情况
// （节点按每批 _ModelArtifactProgressParallel 个分波次执行，每波受限于
// _ModelArtifactProgressNodeTimeout），从而确保完整扇出也能在截止时间内完成，而不会
// 一开始就被取消。
func modelArtifactProgressTimeout() time.Duration {
	waves := (_ModelArtifactProgressMaxNodes + _ModelArtifactProgressParallel - 1) / _ModelArtifactProgressParallel
	return _ModelArtifactProgressTimeout + time.Duration(waves)*_ModelArtifactProgressNodeTimeout
}

// ModelArtifactProgressHandler 处理 v1.ModelArtifact 对象的 "progress" 子资源：统计
// artifact 内容在各节点上的分布与进度，每次请求时实时计算而不持久化。
//
// 它会读取该 artifact、包含其摘要的 NodeModelStore，以及每个正在下载该 artifact 的节点上
// 插件的运行中下载字节数（在节点 status 按 5% 步长更新的间隔之间实时变化）。未能及时响应的
// 插件将保留其节点已存储的值。返回结果为不具名任何节点的聚合视图。
type ModelArtifactProgressHandler struct {
	extensionapi.GetOperation

	// APIReader 用于读取 artifact 和插件 Pod；Client 从 worker 缓存中列举 NodeModelStore。
	APIReader ctrlcli.Reader
	Client    ctrlcli.Reader
	// Downloads 从指定 URL 读取插件正在进行的下载信息；为 nil 时使用共享客户端。
	Downloads func(ctx context.Context, url string) (*modelstore.Downloads, error)
	Now       func() time.Time
}

func newModelArtifactProgressHandler(parent rest.Scoper, opts extensionapi.SetupOptions) *ModelArtifactProgressHandler {
	h := &ModelArtifactProgressHandler{APIReader: opts.Manager.GetAPIReader(), Client: opts.Manager.GetClient()}
	h.GetOperation = extensionapi.WithSubResourceGet(parent, h)

	return h
}

var (
	_ rest.Storage = (*ModelArtifactProgressHandler)(nil)
	_ rest.Getter  = (*ModelArtifactProgressHandler)(nil)
)

func (h *ModelArtifactProgressHandler) New() runtime.Object { return &worker.ModelArtifactProgress{} }

func (h *ModelArtifactProgressHandler) Destroy() {}

func (h *ModelArtifactProgressHandler) OnGet(ctx context.Context, key types.NamespacedName, _ ctrlcli.GetOptions) (runtime.Object, error) {
	ctx, cancel := context.WithTimeout(ctx, modelArtifactProgressTimeout())
	defer cancel()

	ma := new(workercore.ModelArtifact)
	if err := h.APIReader.Get(ctx, key, ma, ctrlclix.WithoutQuorum); err != nil {
		return nil, err
	}
	out := &worker.ModelArtifactProgress{
		ObjectMeta: meta.ObjectMeta{Name: ma.Name, Namespace: ma.Namespace},
		Timestamp:  meta.NewTime(h.now()),
	}
	switch r := ma.Status.Resolved; {
	case ma.Spec.Source.PersistentVolumeClaim != nil:
		out.Reason = _ModelArtifactProgressClaim
		return out, nil
	case r == nil || r.ManifestDigest == "":
		out.Reason = _ModelArtifactProgressNotResolv
		return out, nil
	default:
		out.ManifestDigest, out.SizeBytes = r.ManifestDigest, r.SizeBytes
	}

	stores := new(workercore.NodeModelStoreList)
	if err := h.Client.List(ctx, stores,
		ctrlcli.MatchingFields{workerctrl.IndexingNodeModelStoreByModelDigest: out.ManifestDigest}); err != nil {
		return nil, fmt.Errorf("list node model stores: %w", err)
	}
	entries := modelstore.NodeEntries(stores.Items, out.ManifestDigest)
	out.Live = h.readLive(ctx, out.ManifestDigest, entries)

	a := modelstore.AggregateEntries(entries)
	out.Ready, out.Downloading, out.Failed = a.Ready, a.Downloading, a.Failed
	out.DownloadingBytes = a.DownloadedBytes
	if p, ok := a.DownloadingPercent(); ok {
		out.DownloadingPercent = &p
	}
	for _, reason := range a.Reasons() {
		out.FailureReasons = append(out.FailureReasons, worker.ModelArtifactProgressReason{Reason: reason, Count: a.FailureReasons[reason]})
	}

	return out, nil
}

// readLive 在插件及时响应时，用实时读取值替换正在下载节点的已存储字节数，并返回成功响应的
// 节点数量。实时读数绝不会使节点进度倒退。
func (h *ModelArtifactProgressHandler) readLive(
	ctx context.Context, digest string, entries map[string]workercore.NodeModelStoreModel,
) int32 {
	var nodes []string
	for node, m := range entries {
		if m.State == workercore.NodeModelStoreModelStateDownloading && len(nodes) < _ModelArtifactProgressMaxNodes {
			nodes = append(nodes, node)
		}
	}
	if len(nodes) == 0 {
		return 0
	}
	pods, err := h.pluginPods(ctx)
	if err != nil {
		ctrllog.FromContext(ctx).Error(err, "list the model-manager plugins; progress keeps the stored values")
		return 0
	}

	var (
		mu   sync.Mutex
		live int32
		wg   sync.WaitGroup
		sem  = make(chan struct{}, _ModelArtifactProgressParallel)
	)
	for _, node := range nodes {
		pod := pods[node]
		if pod == nil {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			nctx, cancel := context.WithTimeout(ctx, _ModelArtifactProgressNodeTimeout)
			defer cancel()
			url := "https://" + net.JoinHostPort(pod.Status.PodIP, strconv.Itoa(pluginSecurePortOf(pod))) + modelstore.DownloadsPath
			ds, err := h.downloads(nctx, url)
			if err != nil {
				ctrllog.FromContext(ctx).V(2).Info("read a plugin's downloads", "node", node, "error", err.Error())
				return
			}
			for _, d := range ds.Downloads {
				if d.Digest != digest {
					continue
				}
				mu.Lock()
				m := entries[node]
				m.DownloadedBytes = max(m.DownloadedBytes, d.DownloadedBytes)
				if d.SizeBytes > 0 {
					m.SizeBytes = d.SizeBytes
				}
				entries[node] = m
				live++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	return live
}

// pluginPods 按节点名称返回每个节点上就绪的 model-manager 插件 Pod。
func (h *ModelArtifactProgressHandler) pluginPods(ctx context.Context) (map[string]*core.Pod, error) {
	list := new(core.PodList)
	if err := h.APIReader.List(ctx, list,
		ctrlcli.InNamespace(kuberess.SystemNamespaceName),
		ctrlcli.MatchingLabels{deviceplugin.ComponentLabelKey: modelstore.PluginComponent},
		ctrlclix.WithoutQuorum,
	); err != nil {
		return nil, err
	}
	pods := map[string]*core.Pod{}
	for i := range list.Items {
		p := &list.Items[i]
		if p.DeletionTimestamp == nil && p.Status.PodIP != "" && p.Spec.NodeName != "" && deviceplugin.IsPodReady(p) {
			pods[p.Spec.NodeName] = p
		}
	}

	return pods, nil
}

// pluginSecurePortOf 返回插件容器的命名安全端口，或返回 chart 的默认端口。
func pluginSecurePortOf(pod *core.Pod) int {
	for i := range pod.Spec.Containers {
		for j := range pod.Spec.Containers[i].Ports {
			if pod.Spec.Containers[i].Ports[j].Name == modelstore.PluginSecurePortName {
				return int(pod.Spec.Containers[i].Ports[j].ContainerPort)
			}
		}
	}
	return modelstore.DefaultPluginSecurePort
}

func (h *ModelArtifactProgressHandler) now() time.Time {
	if h.Now != nil {
		return h.Now()
	}
	return time.Now()
}

func (h *ModelArtifactProgressHandler) downloads(ctx context.Context, url string) (*modelstore.Downloads, error) {
	if h.Downloads != nil {
		return h.Downloads(ctx, url)
	}
	return fetchPluginDownloads(ctx, url)
}

// _pluginDownloadsClient 读取插件的下载任务：每个进程使用一个长连接 Transport。
var _pluginDownloadsClient = &http.Client{
	Transport: httpx.Transport(httpx.TransportOptions().WithoutProxy().WithTLSClientConfig(
		&tls.Config{InsecureSkipVerify: true}, // nolint: gosec
	)),
}

// fetchPluginDownloads 通过 GET 获取插件正在进行的下载任务。与 device manager 一样，
// 该插件在同一信任域内提供自签名证书；其响应不包含机密信息，且由于未校验对端身份，
// 响应体大小受到限制。代理已禁用：Pod 间流量绝不能经过代理。
func fetchPluginDownloads(ctx context.Context, url string) (*modelstore.Downloads, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := _pluginDownloadsClient.Do(req)
	if err != nil {
		return nil, err
	}
	body := io.LimitReader(resp.Body, _ModelArtifactProgressMaxBytes)
	defer func() {
		_, _ = io.Copy(io.Discard, body)
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %s", resp.Status)
	}
	ds := new(modelstore.Downloads)
	if err := json.NewDecoder(body).Decode(ds); err != nil {
		return nil, fmt.Errorf("decode the downloads: %w", err)
	}

	return ds, nil
}
