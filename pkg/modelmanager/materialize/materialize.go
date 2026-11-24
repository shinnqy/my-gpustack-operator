// Package materialize 负责执行节点的模型物化：每个摘要同一时刻最多运行一次尝试，
// 依次重新计算清单、预留磁盘空间、下载并逐字节校验每个文件，最后原子发布；
// 失败后按摘要退避重试，并在无等待方时自动取消。
package materialize

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	klog "k8s.io/klog/v2"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/modelartifact"
	"gpustack.ai/gpustack/pkg/modelmanager/download"
	"gpustack.ai/gpustack/pkg/modelmanager/driver"
	"gpustack.ai/gpustack/pkg/modelmanager/metrics"
	"gpustack.ai/gpustack/pkg/modelmanager/peer"
	"gpustack.ai/gpustack/pkg/modelmanager/store"
	"gpustack.ai/gpustack/pkg/utils/json"
	"gpustack.ai/gpustack/pkg/utils/stringx"
)

// 物化器的默认时间与并发参数。
const (
	// DefaultWaiterWindow 是最后一个挂载请求之后任务继续运行的等待窗口：
	// 大于 kubelet 最长的挂载重试间隔（2 分 2 秒），确保仍在等待的 Pod 总会在窗口内再次询问。
	DefaultWaiterWindow = 5 * time.Minute
	// DefaultBackoff 和 DefaultMaxBackoff 分别是首次失败后的退避等待时间及其上限，每次连续失败翻倍。
	DefaultBackoff    = time.Minute
	DefaultMaxBackoff = time.Hour
	// DefaultCheckInterval 是运行中的尝试检查是否还有等待方的轮询间隔。
	DefaultCheckInterval = 30 * time.Second
	// fileParallelism 限制单次尝试同时下载的文件数，节点级 HTTP 请求并发上限由下载器控制。
	fileParallelism = 16
)

// Hub 是节点侧访问模型仓库 Hub 的抽象接口。
type Hub interface {
	ListManifest(ctx context.Context, repository, commit, token string, filter modelartifact.Filter) (modelartifact.Manifest, error)
	FileURL(repository, commit, path string) string
}

// 来源可声明的 Hub 类型，Environment 据此构造对应的 Hub 客户端。
const (
	HubHuggingFace = "huggingFace"
	HubModelScope  = "modelscope"
)

// HubKindOf 返回制品来源对应的 Hub 类型；未声明 Hub 来源时返回 ""。
func HubKindOf(ma *workercore.ModelArtifact) string {
	switch {
	case ma.Spec.Source.HuggingFace != nil:
		return HubHuggingFace
	case ma.Spec.Source.ModelScope != nil:
		return HubModelScope
	default:
		return ""
	}
}

// HubRepository 返回制品来源声明的仓库标识，无 Hub 来源时返回 ""。
func HubRepository(ma *workercore.ModelArtifact) string {
	switch {
	case ma.Spec.Source.HuggingFace != nil:
		return ma.Spec.Source.HuggingFace.Repository
	case ma.Spec.Source.ModelScope != nil:
		return ma.Spec.Source.ModelScope.Repository
	default:
		return ""
	}
}

// Environment 根据节点当前生效配置返回指定类型的 Hub 客户端和下载器；
// 若配置无效或无法访问对应 Hub，则返回 InvalidRequest 错误。
type Environment func(ctx context.Context, kind string) (Hub, *download.Downloader, error)

// Materializer 实现 driver.Materializer 接口。
type Materializer struct {
	Store       *store.Store
	Environment Environment
	// Reserve 在缓存文件系统上为总大小 sizeBytes、当前已写入 written() 字节的下载预留空间，
	// 与其他在途下载一同受高水位约束，直到调用返回的 release；空间不足时返回 InsufficientCapacity 错误。
	// 为 nil 时不执行预留。
	Reserve func(ctx context.Context, hex string, sizeBytes int64, written func() int64) (func(), error)
	// Changed 在摘要状态变更时通知调用方刷新状态上报。
	Changed func(hex string)
	// Peers 启用时优先从其他已发布该摘要的节点插件拉取文件，无法完整交付时再回退到 Hub。
	// 为 nil 时仅从 Hub 拉取。
	Peers *peer.Puller
	Now   func() time.Time
	// Base 是所有物化尝试派生上下文的父 Context，便于插件停止时一并中止下载；为 nil 时使用 context.Background。
	Base context.Context

	WaiterWindow  time.Duration
	Backoff       time.Duration
	MaxBackoff    time.Duration
	CheckInterval time.Duration

	mu   sync.Mutex
	jobs map[string]*job
}

var _ driver.Materializer = (*Materializer)(nil)

// job 表示针对单个摘要的一次正在运行的物化尝试。
type job struct {
	hex      string
	cancel   context.CancelCauseFunc
	asked    atomic.Int64 // 最近一次挂载询问的 Unix 纳秒时间戳
	received atomic.Int64
	peer     atomic.Int64 // 已接收字节中来自其他节点的字节数
	total    atomic.Int64

	mu sync.Mutex
	// sources 按制品 UID 记录提出请求的各个来源及其仓库与凭据，按首次加入顺序排列。
	sources map[string]*source
	order   []string
}

// source 表示单个制品获取内容的途径：Hub 类型、仓库、提交号、过滤规则及所属 Namespace 传入的凭据。
// 凭据仅会用于对应仓库的请求。
type source struct {
	kind       string
	repository string
	commit     string
	filter     modelartifact.Filter
	token      atomic.Value
}

func (s *source) currentToken() string {
	t, _ := s.token.Load().(string)
	return t
}

// errNoWaiters 在等待窗口内没有任何挂载请求询问时用于取消尝试。
var errNoWaiters = errors.New("no mount asked for it")

func (m *Materializer) now() time.Time {
	if m.Now != nil {
		return m.Now()
	}
	return time.Now()
}

// Ensure 启动或加入对 req.Hex 的物化任务，并返回当前进度。
func (m *Materializer) Ensure(_ context.Context, req driver.Request) driver.Progress {
	if m.Store.IsPublished(req.Hex) {
		return driver.Progress{Published: true}
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.jobs == nil {
		m.jobs = map[string]*job{}
	}
	if j, ok := m.jobs[req.Hex]; ok {
		j.join(req, m.now())
		return j.progress()
	}
	// 在上述快路径检查与加锁之间，可能已有尝试完成发布并退出运行集合。
	if m.Store.IsPublished(req.Hex) {
		return driver.Progress{Published: true}
	}

	rec, err := m.Store.ReadDigest(req.Hex)
	if err != nil {
		return driver.Progress{Code: codes.Unavailable, Message: fmt.Sprintf("read the node's ledger: %v", err)}
	}
	if rec.Reason != "" && m.now().Before(rec.RetryTime) {
		return backingOff(req.Hex, rec)
	}

	base := m.Base
	if base == nil {
		base = context.Background()
	}
	ctx, cancel := context.WithCancelCause(base)
	j := &job{hex: req.Hex, cancel: cancel, sources: map[string]*source{}}
	j.join(req, m.now())
	m.jobs[req.Hex] = j
	go m.run(ctx, j)

	return j.progress()
}

// DownloadProgress 描述单个运行中物化任务的进度。
type DownloadProgress struct {
	// DownloadedBytes 是本次尝试当前已持有的字节数：包含断点续传继承的部分与新到达的字节。
	// SizeBytes 为清单总字节数，在列出清单前为 0。
	DownloadedBytes int64
	SizeBytes       int64
	Source          workercore.NodeModelStoreModelSource
}

// Progress 返回每个正在运行物化任务的进度（按摘要索引）。
func (m *Materializer) Progress() map[string]DownloadProgress {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]DownloadProgress, len(m.jobs))
	for hex, j := range m.jobs {
		out[hex] = DownloadProgress{
			DownloadedBytes: j.received.Load(), SizeBytes: j.total.Load(), Source: j.source(),
		}
	}

	return out
}

// source 判断本次尝试的主要字节来源：当超过一半字节来自其他节点插件时记为 Peer，其余情况记为 Hub。
func (j *job) source() workercore.NodeModelStoreModelSource {
	if 2*j.peer.Load() > j.received.Load() {
		return workercore.NodeModelStoreModelSourcePeer
	}

	return workercore.NodeModelStoreModelSourceHub
}

// Downloading 返回当前正在物化的摘要集合。
func (m *Materializer) Downloading() map[string]bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	running := make(map[string]bool, len(m.jobs))
	for hex := range m.jobs {
		running[hex] = true
	}

	return running
}

func (j *job) join(req driver.Request, now time.Time) {
	j.asked.Store(now.UnixNano())
	ma := req.Artifact
	uid := string(ma.UID)

	j.mu.Lock()
	defer j.mu.Unlock()
	s, ok := j.sources[uid]
	if !ok {
		s = &source{
			kind:       HubKindOf(ma),
			repository: HubRepository(ma),
			commit:     ma.Status.Resolved.Revision,
			filter:     modelartifact.Filter{Allow: ma.Spec.AllowPatterns, Ignore: ma.Spec.IgnorePatterns},
		}
		j.sources[uid] = s
		j.order = append(j.order, uid)
	}
	// 保存本次挂载传入的最新凭据，确保轮换后的 Secret 能作用于下一次请求。
	s.token.Store(req.Token)
}

func (j *job) snapshotSources() []*source {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]*source, 0, len(j.order))
	for _, uid := range j.order {
		out = append(out, j.sources[uid])
	}

	return out
}

func (j *job) progress() driver.Progress {
	short := j.hex[:12]
	total := j.total.Load()
	if total == 0 {
		return driver.Progress{Code: codes.Aborted, Message: fmt.Sprintf("materializing sha256:%s: listing the files", short)}
	}

	return driver.Progress{Code: codes.Aborted, Message: fmt.Sprintf("materializing sha256:%s: %s of %s received",
		short, humanBytes(j.received.Load()), humanBytes(total))}
}

func backingOff(hex string, rec store.DigestRecord) driver.Progress {
	code := codes.Unavailable
	if rec.Reason == download.ReasonInsufficientCapacity {
		code = codes.ResourceExhausted
	}

	return driver.Progress{Code: code, Message: fmt.Sprintf("materializing sha256:%s failed, %s: %s; the next attempt starts at %s",
		hex[:12], rec.Reason, rec.Message, rec.RetryTime.UTC().Format(time.RFC3339))}
}

// run 执行一次完整的物化尝试，从分配尝试编号直到发布成功或记录失败。
func (m *Materializer) run(ctx context.Context, j *job) {
	start := m.now()
	m.changed(j.hex)

	stopWatch := m.watchWaiters(ctx, j)
	err := m.attempt(ctx, j)
	stopWatch()
	j.cancel(nil)

	// 在尝试移出运行集合前先把结果写入账本，确保随后发现任务已结束的挂载请求总能读到退避记录。
	if err := m.record(j.hex, err, start); err != nil {
		klog.ErrorS(err, "record the attempt's outcome", "digest", j.hex)
	}
	m.mu.Lock()
	delete(m.jobs, j.hex)
	m.mu.Unlock()
	m.changed(j.hex)
}

// watchWaiters 在等待窗口内没有任何挂载请求继续询问时取消本次尝试。已写入磁盘的文件会保留供下次续传。
func (m *Materializer) watchWaiters(ctx context.Context, j *job) func() {
	interval := m.CheckInterval
	if interval <= 0 {
		interval = DefaultCheckInterval
	}
	window := m.WaiterWindow
	if window <= 0 {
		window = DefaultWaiterWindow
	}
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				if m.now().Sub(time.Unix(0, j.asked.Load())) > window {
					j.cancel(errNoWaiters)
					return
				}
			}
		}
	}()

	return func() { close(done) }
}

func (m *Materializer) attempt(ctx context.Context, j *job) error {
	a, err := m.Store.NewAttempt(j.hex)
	if err != nil {
		return err
	}

	var lastErr error
	for _, src := range j.snapshotSources() {
		// Expected 身份没有可列举的 commit，也没有可请求的 Hub：持有该已发布目录树的 peer 是唯一来源。
		if src.commit == "" {
			lastErr = m.fromPeersOnly(ctx, j, a, src)
			break
		}
		hub, dl, err := m.Environment(ctx, src.kind)
		if err != nil {
			// 一种 Hub 类型的配置故障不影响其他类型的来源：继续尝试剩余来源后再上报。
			lastErr = err
			continue
		}
		lastErr = m.fromSource(ctx, j, a, hub, dl, src)
		// 只有当前来源的凭据被拒才值得换另一个来源重试：不同来源授权后拿到的是同一份内容，其他错误只会重复。
		if lastErr == nil || reasonOf(lastErr) != download.ReasonAccessDenied {
			break
		}
	}
	if lastErr != nil {
		return lastErr
	}

	return nil
}

// errNoSourceForAnchored 构造 Expected 身份在没有任何 peer 持有目录树时的明确失败错误：
// 摘要写入不含租户信息的 Detail，排查建议留在插件日志中。
func errNoSourceForAnchored(hex string) *download.Error {
	return &download.Error{
		Reason: download.ReasonSourceUnavailable,
		Message: fmt.Sprintf("no node holds sha256:%s and the artifact is anchored to its digest without "+
			"a hub: seed one node while the hub is reachable, or have a peer serve the tree", hex),
		Detail: "no peer holds sha256:" + hex,
	}
}

// fromPeersOnly 仅通过 peer 物化 Expected 身份：清单来自 peer 的已发布列表并先绑定校验摘要，
// 所有文件字节也全部来自 peer，不存在 Hub 回退路径。
func (m *Materializer) fromPeersOnly(ctx context.Context, j *job, a *store.Attempt, src *source) error {
	if !m.Peers.Enabled() {
		noSource := errNoSourceForAnchored(j.hex)
		noSource.Detail += ", and peer pulling is not configured"

		return noSource
	}
	manifest, err := m.Peers.FetchManifest(ctx, "sha256:"+j.hex)
	if err != nil {
		// 等待窗口触发或插件停止导致的取消属于取消结果，而非来源失败，不应消耗退避次数。
		if cause := context.Cause(ctx); errors.Is(cause, errNoWaiters) {
			const msg = "no mount asked for it within the waiter window"
			return &download.Error{Reason: download.ReasonCanceled, Message: msg, Detail: msg}
		}
		// 分类结果保持明确的无来源错误；底层具体原因输出到插件日志供排查。
		klog.V(1).InfoS("the peers-only listing found no source", "digest", "sha256:"+j.hex, "error", err.Error())

		return errNoSourceForAnchored(j.hex)
	}

	return m.fetchTree(ctx, j, a, manifest, nil, nil, src)
}

// hubFileURL 返回文件在 Hub 上的下载地址；无 Hub（Expected 身份）时返回空字符串，
// 逐文件循环将只走 peer 或返回无来源错误。
func hubFileURL(hub Hub, src *source, path string) string {
	if hub == nil {
		return ""
	}

	return hub.FileURL(src.repository, src.commit, path)
}

func (m *Materializer) fromSource(
	ctx context.Context, j *job, a *store.Attempt, hub Hub, dl *download.Downloader, src *source,
) error {
	manifest, err := hub.ListManifest(ctx, src.repository, src.commit, src.currentToken(), src.filter)
	if err != nil {
		return manifestError(err)
	}
	if manifest.Digest != "sha256:"+j.hex {
		return &download.Error{Reason: download.ReasonIntegrityMismatch, Message: fmt.Sprintf(
			"the hub lists %s at commit %s as %s, the artifact's digest is sha256:%s",
			src.repository, src.commit, manifest.Digest, j.hex,
		), Detail: "the hub lists " + manifest.Digest}
	}

	return m.fetchTree(ctx, j, a, manifest, hub, dl, src)
}

func (m *Materializer) fetchTree(
	ctx context.Context, j *job, a *store.Attempt, manifest modelartifact.Manifest, hub Hub, dl *download.Downloader, src *source,
) error {
	j.total.Store(manifest.SizeBytes)

	stateDir := filepath.Join(a.Dir(), "state")
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return err
	}
	// 在启动首个下载前校验全部路径，文件父目录则在各自下载协程中创建：
	// 一旦下载启动，本次尝试只通过 g.Wait 结束，保证记录结果后不再有任何协程继续写入尝试目录。
	dests := make([]string, len(manifest.Entries))
	checkpoints := make([]string, len(manifest.Entries))
	for i, e := range manifest.Entries {
		dest, err := a.FilePath(e.Path)
		if err != nil {
			return &download.Error{Reason: download.ReasonIntegrityMismatch, Message: err.Error()}
		}
		dests[i] = dest
		checkpoints[i] = filepath.Join(stateDir, stringx.SumBySHA256(e.Path))
	}

	// 在此处一次性统计尝试目录已持有的可续传字节，后续由各下载回调精确增量更新：
	// 该计数既用于上报进度，也用于空间预留时避免重复计算已落盘字节。
	// 切换到其他来源重试时，也从上一来源已校验落盘的字节数继续累加。
	var carried int64
	for i, e := range manifest.Entries {
		carried += download.ResumableOffset(dests[i], checkpoints[i], e.Size)
	}
	j.received.Store(carried)

	if m.Reserve != nil {
		// 尝试目录已落盘的部分（包括续传继承的字节）已计入文件系统使用量，因此只为剩余字节预留空间，
		// 并持续持有该预留直到本次尝试结束（无论最终是否发布成功）。
		release, err := m.Reserve(ctx, j.hex, manifest.SizeBytes, j.received.Load)
		if err != nil {
			return err
		}
		defer release()
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(fileParallelism)
	manifestDigest := "sha256:" + j.hex
	for i, e := range manifest.Entries {
		f := download.File{
			Path: e.Path, Size: e.Size, Digest: e.Digest,
			URL:        hubFileURL(hub, src, e.Path),
			Dest:       dests[i],
			Checkpoint: checkpoints[i],
			Token:      src.currentToken,
			Received:   func(n int64) { j.received.Add(n) },
		}
		g.Go(func() error {
			if err := os.MkdirAll(filepath.Dir(f.Dest), 0o755); err != nil {
				return err
			}
			if m.pullFromPeers(gctx, j, manifestDigest, f) {
				return nil
			}
			// Expected 身份不存在 Hub 回退：所有 peer 均未持有该文件时属于无来源错误而非完整性错误。
			if hub == nil {
				return errNoSourceForAnchored(j.hex)
			}

			return dl.Fetch(gctx, f)
		})
	}
	if err := g.Wait(); err != nil {
		if cause := context.Cause(ctx); errors.Is(cause, errNoWaiters) {
			const msg = "no mount asked for it within the waiter window"
			return &download.Error{Reason: download.ReasonCanceled, Message: msg, Detail: msg}
		}
		return err
	}
	// 续传状态目录放在 tree 旁而非 tree 内部，发布前清除以免随已发布目录树保留。
	if err := os.RemoveAll(stateDir); err != nil {
		return err
	}

	full := peer.Listing{Digest: manifest.Digest}
	for _, e := range manifest.Entries {
		full.Entries = append(full.Entries, peer.ManifestFile{Path: e.Path, Size: e.Size, Digest: e.Digest})
	}
	if b, err := json.Marshal(full); err == nil {
		if err := os.WriteFile(filepath.Join(a.Dir(), "manifest.json"), b, 0o600); err != nil {
			return fmt.Errorf("store the published manifest: %w", err)
		}
	}

	return a.Publish(store.Marker{
		Digest: manifest.Digest, ManifestFormat: modelartifact.ManifestHeader,
		SizeBytes: manifest.SizeBytes, FileCount: manifest.FileCount, PublishedTime: m.now(),
		Source: string(j.source()),
	})
}

// pullFromPeers 尝试从其他持有该摘要的节点拉取文件 f，并返回是否拉取成功。
// 若拉取失败，将本轮来自 peer 的字节计数扣回：回退到 Hub 时会从上次有效检查点继续或重来。
func (m *Materializer) pullFromPeers(ctx context.Context, j *job, digest string, f download.File) bool {
	if !m.Peers.Enabled() {
		return false
	}
	var fromPeers atomic.Int64
	withPeerCounting := f
	withPeerCounting.Received = func(n int64) {
		fromPeers.Add(n)
		if f.Received != nil {
			f.Received(n)
		}
	}
	if err := m.Peers.FetchFile(ctx, digest, withPeerCounting); err != nil {
		j.received.Add(-fromPeers.Load())
		klog.V(1).InfoS("a file the peers could not deliver goes to the hub", "digest", digest,
			"path", f.Path, "error", err.Error())

		return false
	}
	j.peer.Add(fromPeers.Load())

	return true
}

// record 将尝试结果写入账本：成功时清空失败计数；失败时按连续失败次数指数退避直到上限；
// 被取消或节点配置尚不可用时不施加退避，让下一次挂载能够立即启动。
// 配置无效并非源站问题，worker 修正节点 spec 后即可恢复，退避反而会白白阻塞挂载一分钟。
func (m *Materializer) record(hex string, err error, start time.Time) error {
	return m.Store.UpdateDigest(hex, func(rec *store.DigestRecord) { m.outcome(hex, rec, err, start) })
}

func (m *Materializer) outcome(hex string, rec *store.DigestRecord, err error, start time.Time) {
	now := m.now()
	switch reason := reasonOf(err); {
	case err == nil:
		rec.Failures, rec.Reason, rec.Message, rec.RetryTime = 0, "", "", time.Time{}
		rec.LastUsedTime = now
		metrics.Materializations.WithLabelValues("published").Inc()
		metrics.PublishDuration.Observe(now.Sub(start).Seconds())
		klog.InfoS("published", "digest", hex, "seconds", now.Sub(start).Seconds())
	case reason == download.ReasonCanceled, reason == download.ReasonInvalidRequest:
		rec.Reason, rec.Message, rec.RetryTime = reason, publicMessage(err), now
		metrics.Materializations.WithLabelValues(reason).Inc()
		klog.V(1).InfoS("materialization stopped", "digest", hex, "reason", reason, "error", messageOf(err))
	default:
		rec.Failures++
		rec.Reason, rec.Message = reason, publicMessage(err)
		rec.RetryTime = now.Add(m.backoff(rec.Failures))
		metrics.Materializations.WithLabelValues(reason).Inc()
		klog.InfoS("materialization failed", "digest", hex, "reason", reason, "error", messageOf(err),
			"retryTime", rec.RetryTime)
	}
}

func (m *Materializer) backoff(failures int) time.Duration {
	base, ceiling := m.Backoff, m.MaxBackoff
	if base <= 0 {
		base = DefaultBackoff
	}
	if ceiling <= 0 {
		ceiling = DefaultMaxBackoff
	}
	d := base
	for i := 1; i < failures && d < ceiling; i++ {
		d *= 2
	}

	return min(d, ceiling)
}

func (m *Materializer) changed(hex string) {
	if m.Changed != nil {
		m.Changed(hex)
	}
}

// manifestError 对拉取清单时的错误分类：保留 Hub 拒绝访问与源站不可用的原因，
// 其余 Hub 返回了结果但与制品不符的情况归类为完整性不匹配。
func manifestError(err error) error {
	detail := httpStatusPattern.FindString(err.Error())
	switch r := modelartifact.ReasonOf(err); r {
	case modelartifact.ReasonAccessDenied, modelartifact.ReasonSourceUnavailable:
		return &download.Error{Reason: r, Message: err.Error(), Detail: detail}
	default:
		return &download.Error{Reason: download.ReasonIntegrityMismatch, Message: err.Error(), Detail: detail}
	}
}

// httpStatusPattern 从清单获取失败消息中提取 Hub 返回的 HTTP 状态码，该部分不含仓库名。
var httpStatusPattern = regexp.MustCompile(`\bHTTP [0-9]{3}\b`)

// reasonMeanings 用不含租户信息的措辞说明每种失败原因的含义。
var reasonMeanings = map[string]string{
	download.ReasonInvalidRequest:       "the node cannot run the download with its configuration",
	download.ReasonAccessDenied:         "the hub refused the credential, or a file does not exist",
	download.ReasonSourceUnavailable:    "the hub could not be reached or failed",
	download.ReasonIntegrityMismatch:    "the content, or the hub's file list, does not match the artifact's digest",
	download.ReasonInsufficientCapacity: "the node's cache has no room for it",
	download.ReasonCanceled:             "the attempt was canceled",
}

// publicMessage 生成写入节点状态和挂载错误的公开失败说明。由于同一摘要会跨租户共享且状态可被跨租户读取，
// 消息中绝不出现仓库名、URL、文件名或 Namespace，仅包含原因说明与不含租户信息的细节（如 HTTP 状态码）；
// 完整错误仅输出到插件日志。
func publicMessage(err error) string {
	msg := reasonMeanings[reasonOf(err)]
	if msg == "" {
		msg = "the attempt failed"
	}
	if d := download.DetailOf(err); d != "" {
		msg += ": " + d
	}

	return msg
}

func reasonOf(err error) string {
	return download.ReasonOf(err)
}

func messageOf(err error) string {
	if e, ok := errors.AsType[*download.Error](err); ok {
		return e.Message
	}

	return err.Error()
}

func humanBytes(n int64) string {
	const unit = 1 << 10
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
