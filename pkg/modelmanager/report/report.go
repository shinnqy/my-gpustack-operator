// Package report 将节点的生效配置应用到插件中，并在节点缓存状态发生变化时将其写入
// NodeModelStore 的 status：状态变化立即上报，下载进度则按阈值节流上报。
package report

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"net/http"
	"sort"
	"sync"
	"time"

	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	klog "k8s.io/klog/v2"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"

	gpustack "gpustack.ai/gpustack/api/v1"
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/modelartifact"
	"gpustack.ai/gpustack/pkg/modelmanager/download"
	"gpustack.ai/gpustack/pkg/modelmanager/gc"
	"gpustack.ai/gpustack/pkg/modelmanager/materialize"
	"gpustack.ai/gpustack/pkg/modelmanager/metrics"
	"gpustack.ai/gpustack/pkg/modelmanager/store"
	"gpustack.ai/gpustack/pkg/modelstore"
)

// 状态条件名称与对应原因常量。
const (
	ConditionReady       = "Ready"
	ConditionCapacityLow = "CapacityLow"

	ReasonServing              = "Serving"
	ReasonInvalidConfiguration = "InvalidConfiguration"
	ReasonCapacityLow          = "NothingToRemove"
	ReasonCapacityFine         = "WithinWatermarks"

	// MaxModels 限制 status.models 的条目数，与 CRD schema 保持一致。
	MaxModels = 256
	// caBundleKey 是 CA 证书 ConfigMap 中的键名，与 ModelArtifact 控制器约定一致。
	caBundleKey = "ca.crt"
)

// DefaultInterval 是无事件触发时，上报器定期执行回收与状态上报的默认周期。
const DefaultInterval = 5 * time.Minute

// 进度写入节流规则。随每批到达字节变化的字段（条目的 downloadedBytes 以及下载进行时的 capacity.storedBytes）
// 仅在距上次写入已过 progressInterval 且存在某下载推进幅度达到其总大小 progressStep 时单独写入，
// 或者伴随其他字段的状态变化一并写入。这样无论下载速度多快，进度写入次数都不超过 1/progressStep，
// 频率也不高于每 progressInterval 一次。
const (
	progressInterval = 30 * time.Second
	progressStep     = 0.05
	// ProgressTick 是上报器轮询在途下载进度的周期，确保进度达到阈值时及时上报而非等待下一个无关事件。
	ProgressTick = 10 * time.Second
)

// conflictRetry 是状态更新遇到乐观锁冲突后延迟重试的间隔，避免立即拿同一份旧副本再次冲突。
const conflictRetry = time.Second

// Reporter 负责将 NodeModelStore 的 spec 配置应用到插件、触发缓存回收并将节点现状写回 status。
// 同时实现驱动的 Events 回调与物化器的 Changed 回调。
type Reporter struct {
	// Reader 用于读取本节点的 NodeModelStore 与 CA ConfigMap。使用直连 API Reader 而非 informer 缓存：
	// 上报负责使配置收敛，若依赖缓存则 informer 卡顿时会导致配置停滞。Client 用于写入 status。
	Reader    ctrlcli.Reader
	Client    ctrlcli.Client
	NodeName  string
	Namespace string

	Store       *store.Store
	Collector   *gc.Collector
	Downloading func() map[string]bool
	// Progress 返回每个在途下载的实时进度（按摘要索引）；为 nil 时不上报进度。
	Progress   func() map[string]materialize.DownloadProgress
	Downloader *download.Downloader
	// KubeletCap 在缓存与 kubelet 共享文件系统时根据 kubelet 阈值计算高水位上限；独立文件系统时为 nil。
	KubeletCap func(k *workercore.NodeModelStoreKubelet, totalBytes uint64) *gc.Cap
	Now        func() time.Time
	Interval   time.Duration

	mu        sync.Mutex
	spec      *workercore.NodeModelStoreSpec
	specErr   error
	client    *http.Client
	ca        []byte
	capTotal  uint64
	high, low int32
	capNote   string
	trigger   chan struct{}
	// lastWrite 记录本插件上次成功写入 status 的时间，仅由 Report 维护。
	lastWrite time.Time
}

var _ materialize.Environment = (*Reporter)(nil).Environment

func (r *Reporter) init() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.trigger == nil {
		r.trigger = make(chan struct{}, 1)
	}
}

func (r *Reporter) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// Trigger 请求尽快执行一次上报；多次触发会自动合并。
func (r *Reporter) Trigger(string) {
	r.init()
	select {
	case r.trigger <- struct{}{}:
	default:
	}
}

// Mounted 记录摘要的使用时间并触发上报。
func (r *Reporter) Mounted(hex string) { r.used(hex) }

// Unmounted 记录摘要的使用时间并触发上报。
func (r *Reporter) Unmounted(hex string) { r.used(hex) }

func (r *Reporter) used(hex string) {
	now := r.now()
	if err := r.Store.UpdateDigest(hex, func(rec *store.DigestRecord) { rec.LastUsedTime = now }); err != nil {
		klog.ErrorS(err, "record a digest's use", "digest", hex)
	}
	r.Trigger(hex)
}

// Watermarks 返回供垃圾回收使用的生效高低水位，或尚未生效的原因：配置尚未应用或节点配置校验失败。
// 若此前已生效的配置在更新后校验失败，不再沿用旧水位线，与节点上报的配置未生效状态保持一致。
func (r *Reporter) Watermarks() (int32, int32, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch {
	case r.specErr != nil:
		return 0, 0, "the node's configuration fails its check, so nothing is collected under the previous one"
	case r.spec == nil:
		return 0, 0, "the node's configuration has not been applied yet, so there are no watermarks to collect against"
	}

	return r.high, r.low, ""
}

// Pinned 返回节点配置要求固定保留的摘要集合：永不作为回收候选，但仍计入文件系统使用量。
// 在配置生效前返回 nil。
func (r *Reporter) Pinned() map[string]bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.spec == nil || len(r.spec.Pinned) == 0 {
		return nil
	}
	out := make(map[string]bool, len(r.spec.Pinned))
	for _, digest := range r.spec.Pinned {
		if hex := store.HexOf(digest); hex != "" {
			out[hex] = true
		}
	}

	return out
}

// Environment 按节点当前生效配置返回指定类型的 Hub 客户端和下载器；
// 若配置尚未到达、校验失败或无法访问目标 Hub，则返回 InvalidRequest 错误。
func (r *Reporter) Environment(_ context.Context, kind string) (materialize.Hub, *download.Downloader, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch {
	// 节点配置不含任何租户信息，整条错误均可作为公开 Detail。
	case r.specErr != nil:
		msg := r.specErr.Error()
		return nil, nil, &download.Error{Reason: download.ReasonInvalidRequest, Message: msg, Detail: msg}
	case r.spec == nil:
		const msg = "the node's configuration has not arrived yet"
		return nil, nil, &download.Error{Reason: download.ReasonInvalidRequest, Message: msg, Detail: msg}
	}

	switch kind {
	case materialize.HubModelScope:
		// 旧版本 worker 写入的 spec 中该字段可能为空：要求 ModelScope 的制品直接显式等待升级，而不错用其他 Hub。
		if r.spec.Hub.ModelScopeEndpoint == "" {
			msg := "the node's configuration names no ModelScope endpoint: it was written by an older worker; upgrade the worker"
			return nil, nil, &download.Error{Reason: download.ReasonInvalidRequest, Message: msg, Detail: msg}
		}

		return &modelartifact.ModelScope{Endpoint: r.spec.Hub.ModelScopeEndpoint, Client: listingClient(r.client)}, r.Downloader, nil
	case materialize.HubHuggingFace:
		return &modelartifact.HuggingFace{Endpoint: r.spec.Hub.HuggingFaceEndpoint, Client: listingClient(r.client)}, r.Downloader, nil
	default:
		msg := fmt.Sprintf("the node's configuration does not know the hub kind %q", kind)
		return nil, nil, &download.Error{Reason: download.ReasonInvalidRequest, Message: msg, Detail: msg}
	}
}

// PeerSyncEnabled 报告节点当前配置是否允许从其他节点插件拉取内容；配置未到达前仅从 Hub 拉取。
func (r *Reporter) PeerSyncEnabled() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.spec != nil && (r.spec.PeerSyncEnabled == nil || *r.spec.PeerSyncEnabled)
}

func httpClient(hub workercore.NodeModelStoreHub, caBundle []byte) (*http.Client, error) {
	client, err := modelartifact.NewHTTPClient(modelartifact.HTTPClientOptions{
		HTTPSProxy: hub.HTTPSProxy, NoProxy: hub.NoProxy, CABundle: caBundle,
	})
	if err != nil {
		return nil, err
	}
	// 下载器自行控制分片停滞超时；设置全局超时会误掐断大分片下载。
	client.Timeout = 0

	return client, nil
}

// listingClient 基于下载客户端浅拷贝一份恢复单次请求超时的客户端，用于小体量的清单列举请求。
func listingClient(c *http.Client) *http.Client {
	l := *c
	l.Timeout = modelartifact.DefaultRequestTimeout
	return &l
}

// Run 在每次触发及每个定时周期执行上报，直到 ctx 结束。
func (r *Reporter) Run(ctx context.Context) error {
	r.init()
	interval := r.Interval
	if interval <= 0 {
		interval = DefaultInterval
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	pt := time.NewTicker(ProgressTick)
	defer pt.Stop()
	for {
		if err := r.Report(ctx); err != nil {
			klog.ErrorS(err, "report the node's model cache")
		}
	wait:
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-t.C:
				break wait
			case <-r.trigger:
				break wait
			case <-pt.C:
				// 只有存在进行中的下载时才需要按进度 tick 刷新。
				if len(r.Downloading()) > 0 {
					break wait
				}
			}
		}
	}
}

// Report 读取并应用节点配置、执行一次回收，并在 status 发生变化时写回 API Server。
func (r *Reporter) Report(ctx context.Context) error {
	nms := new(workercore.NodeModelStore)
	if err := r.Reader.Get(ctx, ctrlcli.ObjectKey{Name: r.NodeName}, nms); err != nil {
		if kerrors.IsNotFound(err) {
			// kubelet 在本节点注册 CSI 驱动后由 worker 创建该对象。
			return nil
		}
		return err
	}
	r.apply(ctx, nms)

	res, err := r.Collector.Collect(ctx)
	if err != nil {
		return fmt.Errorf("collect: %w", err)
	}
	status, err := r.status(nms, res)
	if err != nil {
		return err
	}
	// 使用语义等价比较：从 API 读回的时间可能带本地时区，但语义上仍与 UTC 相等。
	if equality.Semantic.DeepEqual(status, nms.Status) {
		return nil
	}
	if onlyProgressMoved(status, nms.Status) && !r.progressDue(status, nms.Status) {
		return nil
	}
	nms.Status = status
	if err := r.Client.Status().Update(ctx, nms); err != nil {
		if kerrors.IsConflict(err) {
			time.AfterFunc(conflictRetry, func() { r.Trigger("") })
			return nil
		}
		return fmt.Errorf("update status: %w", err)
	}
	r.lastWrite = r.now()

	return nil
}

// onlyProgressMoved 判断 next 与 old 是否仅在进度字段上有差异：即条目的 downloadedBytes，
// 以及存在在途下载时的 capacity.storedBytes。没有在途下载时 storedBytes 变化说明发生了清理，属于状态变更。
func onlyProgressMoved(next, old workercore.NodeModelStoreStatus) bool {
	written := make(map[string]int64, len(old.Models))
	for _, m := range old.Models {
		written[m.Digest] = m.DownloadedBytes
	}
	held := *next.DeepCopy()
	downloading := false
	for i := range held.Models {
		m := &held.Models[i]
		if m.State == workercore.NodeModelStoreModelStateDownloading {
			downloading = true
		}
		if b, ok := written[m.Digest]; ok {
			m.DownloadedBytes = b
		}
	}
	if downloading && held.Capacity != nil && old.Capacity != nil {
		held.Capacity.StoredBytes = old.Capacity.StoredBytes
	}

	return equality.Semantic.DeepEqual(held, old)
}

// progressDue 判断仅包含进度变化的写入是否达到上报阈值：距上次写入已满 progressInterval，
// 且至少有一个下载较上次写入推进了不低于其总大小 progressStep 的比例。
func (r *Reporter) progressDue(next, old workercore.NodeModelStoreStatus) bool {
	if r.now().Sub(r.lastWrite) < progressInterval {
		return false
	}
	written := make(map[string]int64, len(old.Models))
	for _, m := range old.Models {
		written[m.Digest] = m.DownloadedBytes
	}
	for _, m := range next.Models {
		if m.SizeBytes > 0 && float64(m.DownloadedBytes-written[m.Digest]) >= progressStep*float64(m.SizeBytes) {
			return true
		}
	}

	return false
}

// apply 再次校验 nms.Spec 并将其应用为插件配置，防止手工编辑对象绕过 worker 侧校验。
func (r *Reporter) apply(ctx context.Context, nms *workercore.NodeModelStore) {
	spec := nms.Spec
	err := modelstore.Validate(spec)
	var ca []byte
	if err == nil && spec.Hub.CABundleConfigMap != "" {
		cm := new(core.ConfigMap)
		if gerr := r.Reader.Get(ctx, ctrlcli.ObjectKey{Namespace: r.Namespace, Name: spec.Hub.CABundleConfigMap}, cm); gerr != nil {
			err = fmt.Errorf("read the CA bundle ConfigMap %q: %w", spec.Hub.CABundleConfigMap, gerr)
		} else if ca = []byte(cm.Data[caBundleKey]); len(ca) == 0 {
			err = fmt.Errorf("the CA bundle ConfigMap %q has no %s", spec.Hub.CABundleConfigMap, caBundleKey)
		}
	}

	var (
		total    uint64
		usageErr error
	)
	if err == nil && r.KubeletCap != nil {
		var u gc.Usage
		u, usageErr = r.Collector.Usage()
		total = u.Total
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if usageErr != nil {
		// 上限随文件系统总容量计算，单次读取失败不会改变磁盘大小：保留上次读数使水位上限继续生效且不重建客户端。
		// 若此前从未成功读过容量，则暂不应用配置，避免在未受限的水位线下误回收。
		total = r.capTotal
		if total == 0 {
			err = fmt.Errorf("read the cache's filesystem for the kubelet cap: %w", usageErr)
		}
	}
	if err == nil && r.spec != nil && equality.Semantic.DeepEqual(*r.spec, spec) && bytes.Equal(r.ca, ca) &&
		r.capTotal == total {
		// 配置未变化：沿用现有 HTTP 客户端、连接池及下载器限额。
		r.specErr = nil
		return
	}
	var client *http.Client
	if err == nil {
		client, err = httpClient(spec.Hub, ca)
	}
	r.specErr = err
	if err != nil {
		return
	}
	high, low, note := spec.Watermarks.HighPercent, spec.Watermarks.LowPercent, ""
	if total > 0 {
		c := r.KubeletCap(spec.Kubelet, total)
		high, low, note = gc.Effective(high, low, c)
		// 无论上限是否实际拉低了水位，均说明回退默认值或被忽略阈值的情况，因为 kubelet 仍按自身配置执行驱逐。
		if spec.Kubelet == nil {
			note = joinNotes("the node's effective kubelet configuration could not be read, so the cap on the "+
				"filesystem the cache shares with kubelet assumes kubelet's defaults", note)
		}
		if c != nil && c.Ignored != "" && note == "" {
			note = "the cap on the filesystem the cache shares with kubelet set a threshold aside: " + c.Ignored
		}
	}
	r.spec, r.client, r.ca, r.capTotal, r.high, r.low, r.capNote = &spec, client, ca, total, high, low, note
	r.Downloader.SetClient(client)
	r.Downloader.SetLimits(int(spec.Download.Concurrency), spec.Download.BytesPerSecond)
}

func (r *Reporter) status(nms *workercore.NodeModelStore, res gc.Result) (workercore.NodeModelStoreStatus, error) {
	models, omitted, stored, err := r.models()
	if err != nil {
		return workercore.NodeModelStoreStatus{}, err
	}
	metrics.StoredBytes.Set(float64(stored))
	metrics.CapacityBytes.Set(float64(res.Usage.Total))

	r.mu.Lock()
	specErr, capNote := r.specErr, r.capNote
	r.mu.Unlock()

	ready := condition(ConditionReady, meta.ConditionTrue, ReasonServing,
		joinNotes(capNote, omittedNote(omitted), skippedNote(res.Skipped)))
	if specErr != nil {
		ready = condition(ConditionReady, meta.ConditionFalse, ReasonInvalidConfiguration,
			joinNotes("no download starts until the configuration is fixed: "+specErr.Error(), skippedNote(res.Skipped)))
	}
	low := condition(ConditionCapacityLow, meta.ConditionFalse, ReasonCapacityFine, "")
	if res.CapacityLow {
		low = condition(ConditionCapacityLow, meta.ConditionTrue, ReasonCapacityLow,
			"the cache's filesystem is above the high watermark and every model on it is in use")
	}

	return workercore.NodeModelStoreStatus{
		ObservedGeneration: nms.Generation,
		Capacity: &workercore.NodeModelStoreCapacity{
			TotalBytes:  int64(min(res.Usage.Total, math.MaxInt64)), // nolint: gosec
			StoredBytes: stored,
			UsedPercent: int32(math.Floor(res.Usage.Percent()/5) * 5),
		},
		Models:     models,
		Conditions: carryTransitions(nms.Status.Conditions, []gpustack.Condition{ready, low}, r.now()),
	}, nil
}

// models 汇总节点持有的全部模型条目与总占用空间；条目超出上限时优先保留被引用和最近使用的条目。
func (r *Reporter) models() ([]workercore.NodeModelStoreModel, int, int64, error) {
	markers, err := r.Store.Published()
	if err != nil {
		return nil, 0, 0, err
	}
	refs, err := r.Store.Refs()
	if err != nil {
		return nil, 0, 0, err
	}
	records, err := r.Store.Digests()
	if err != nil {
		return nil, 0, 0, err
	}
	partials, err := r.Store.Partials()
	if err != nil {
		return nil, 0, 0, err
	}
	referenced := map[string]bool{}
	for _, ref := range refs {
		referenced[ref.Hex] = true
	}
	downloading := r.Downloading()
	var progress map[string]materialize.DownloadProgress
	if r.Progress != nil {
		progress = r.Progress()
	}

	var stored int64
	seen := map[string]bool{}
	var models []workercore.NodeModelStoreModel
	for _, m := range markers {
		hex := store.HexOf(m.Digest)
		seen[hex] = true
		stored += m.SizeBytes
		models = append(models, workercore.NodeModelStoreModel{
			Digest: m.Digest, State: workercore.NodeModelStoreModelStateReady, SizeBytes: m.SizeBytes,
			Source:     workercore.NodeModelStoreModelSource(m.Source),
			Referenced: referenced[hex], LastUsedTime: hourOf(records[hex].LastUsedTime),
		})
	}
	for _, p := range partials {
		stored += p.SizeBytes
	}
	for hex := range downloading {
		if seen[hex] {
			continue
		}
		seen[hex] = true
		p := progress[hex]
		models = append(models, workercore.NodeModelStoreModel{
			Digest: "sha256:" + hex, State: workercore.NodeModelStoreModelStateDownloading,
			SizeBytes: p.SizeBytes, DownloadedBytes: max(p.DownloadedBytes, 0), Source: p.Source,
			LastUsedTime: hourOf(records[hex].LastUsedTime),
		})
	}
	for hex, rec := range records {
		if seen[hex] || rec.Reason == "" || rec.Reason == download.ReasonCanceled {
			continue
		}
		// 本地账本保留纳秒精度，但 metav1.Time 经 API 往返后只保留秒级精度；不截断会导致每次比对都不等而重复写入。
		retry := meta.NewTime(rec.RetryTime.UTC().Truncate(time.Second))
		models = append(models, workercore.NodeModelStoreModel{
			Digest: "sha256:" + hex, State: workercore.NodeModelStoreModelStateFailed,
			LastUsedTime: hourOf(rec.LastUsedTime), Reason: rec.Reason, Message: rec.Message, RetryTime: &retry,
		})
	}

	sort.SliceStable(models, func(i, j int) bool {
		a, b := models[i], models[j]
		if a.Referenced != b.Referenced {
			return a.Referenced
		}
		at, bt := timeOf(a.LastUsedTime), timeOf(b.LastUsedTime)
		if !at.Equal(bt) {
			return at.After(bt)
		}
		return a.Digest < b.Digest
	})
	omitted := 0
	if len(models) > MaxModels {
		omitted = len(models) - MaxModels
		models = models[:MaxModels]
	}

	return models, omitted, stored, nil
}

// hourOf 将使用时间向下截断至小时，避免普通挂载/卸载频繁触发对象写入。
func hourOf(t time.Time) *meta.Time {
	if t.IsZero() {
		return nil
	}
	h := meta.NewTime(t.UTC().Truncate(time.Hour))
	return &h
}

func timeOf(t *meta.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return t.Time
}

func condition(typ string, status meta.ConditionStatus, reason, message string) gpustack.Condition {
	return gpustack.Condition{Type: typ, Status: status, Reason: reason, Message: message}
}

// carryTransitions 在条件状态未变时沿用原有的 LastTransitionTime，使未变化的上报与已存储状态完全相等从而跳过写入。
func carryTransitions(old, next []gpustack.Condition, now time.Time) []gpustack.Condition {
	for i := range next {
		next[i].LastTransitionTime = meta.NewTime(now.UTC().Truncate(time.Second))
		for _, o := range old {
			if o.Type == next[i].Type && o.Status == next[i].Status {
				next[i].LastTransitionTime = o.LastTransitionTime
			}
		}
	}

	return next
}

func omittedNote(n int) string {
	if n == 0 {
		return ""
	}
	return fmt.Sprintf("%d more models are on the node than status lists", n)
}

func skippedNote(reason string) string {
	if reason == "" {
		return ""
	}
	return "collection skipped: " + reason
}

func joinNotes(notes ...string) string {
	var out string
	for _, n := range notes {
		switch {
		case n == "":
		case out == "":
			out = n
		default:
			out += "; " + n
		}
	}
	return out
}
