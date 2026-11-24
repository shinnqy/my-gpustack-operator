package download

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/time/rate"
)

// Downloader 的默认调优参数。
const (
	// DefaultSegmentBytes 是单次请求拉取大文件时的字节分片大小。
	DefaultSegmentBytes = 64 << 20
	// DefaultWindow 是允许领先于当前已哈希位置的在途或已完成分片数量。
	// 配合 DefaultSegmentBytes，可将单个文件待哈希的数据量限制在 256 MiB 内，
	// 使其停留在页缓存中，避免哈希阶段回读磁盘。
	DefaultWindow = 4
	// DefaultStallTimeout 是单次请求无任何字节到达时的停滞超时时间，超时后从上次接收位置重试。
	DefaultStallTimeout = 30 * time.Second
	// DefaultRetries 是单个分片在宣告文件失败前允许的最大重试次数。
	DefaultRetries = 5
	// DefaultRetryDelay 是分片首次重试前的等待间隔，随重试次数递增，上限为其十倍。
	DefaultRetryDelay = time.Second
	// DefaultCheckpointBytes 是同步文件并记录已哈希检查点的字节间隔，便于重启后续传而非重头下载。
	DefaultCheckpointBytes = 256 << 20
)

// readChunk 是每次读取响应体并执行限速的块大小。
const readChunk = 32 << 10

// File 表示清单中待下载的单个文件。
type File struct {
	// Path 是清单内的相对路径，报错信息以此标识文件。
	Path string
	// Size 和 Digest 来自清单。
	Size   int64
	Digest string
	// URL 是 Hub 提供该文件的下载地址。
	URL string
	// Dest 是目标落盘路径，Checkpoint 是位于发布目录树之外的检查点文件路径。
	Dest       string
	Checkpoint string
	// Received 在设置后会在每批字节到达时被回调。续传继承的字节不会通过它重复上报：
	// 调用方在开始下载前先通过 ResumableOffset 计入初值，使初值加回调累加值正好等于当前持有字节数。
	// 若 Hub 不支持续传迫使文件从头下载，则会以负数回调扣除已计入的初值与新到达字节。
	// 大文件的多个分片会并发到达，因此该函数会被并发调用，必须保证并发安全。
	Received func(n int64)
	// Token 返回发起请求当下该文件所在仓库的凭据，以便轮换后的 Secret 能作用于下一次请求。
	// 公开仓库返回 ""。凭据只会随提供它的仓库请求发送。每个分片请求都会并发调用它，须保证并发安全。
	Token func() string
}

// Downloader 在节点级配额下下载文件：跨所有进行中的下载最多同时发起 Concurrency 个请求，
// 且总速率不超过 BytesPerSecond。
type Downloader struct {
	// Client 用于发送请求，已按节点的代理和 CA 配置构建。
	Client *http.Client

	SegmentBytes    int64
	Window          int
	StallTimeout    time.Duration
	Retries         int
	RetryDelay      time.Duration
	CheckpointBytes int64

	// Received 在设置后会从所有并发请求中接收到达字节数统计，必须保证并发安全。
	Received func(n int64)

	mu      sync.Mutex
	streams chan struct{}
	limiter *rate.Limiter
}

// New 返回使用默认调优参数和指定限额的下载器。
func New(client *http.Client, concurrency int, bytesPerSecond int64) *Downloader {
	d := &Downloader{
		Client:          client,
		SegmentBytes:    DefaultSegmentBytes,
		Window:          DefaultWindow,
		StallTimeout:    DefaultStallTimeout,
		Retries:         DefaultRetries,
		RetryDelay:      DefaultRetryDelay,
		CheckpointBytes: DefaultCheckpointBytes,
	}
	d.SetLimits(concurrency, bytesPerSecond)

	return d
}

// SetLimits 替换并发与带宽限额；已在运行的请求保留其当前持有的令牌。
func (d *Downloader) SetLimits(concurrency int, bytesPerSecond int64) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.streams = make(chan struct{}, max(concurrency, 1))
	d.limiter = nil
	if bytesPerSecond > 0 {
		d.limiter = rate.NewLimiter(rate.Limit(bytesPerSecond), max(int(bytesPerSecond), readChunk))
	}
}

// SetClient 替换 HTTP 客户端；已发出的请求保留其原有客户端。
func (d *Downloader) SetClient(c *http.Client) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.Client = c
}

func (d *Downloader) limits() (chan struct{}, *rate.Limiter) {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.streams, d.limiter
}

// Fetch 将 f 下载到 f.Dest，从其检查点续传，并在所有字节按序通过清单校验后返回。
// 校验失败的文件及其检查点会被删除。此前已完整落盘并校验的文件直接复核，不再重新下载。
func (d *Downloader) Fetch(ctx context.Context, f File) error {
	v, err := newVerifier(f.Digest, f.Size)
	if err != nil {
		return err
	}
	out, err := os.OpenFile(f.Dest, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return d.diskError(f, err)
	}
	defer func() { _ = out.Close() }()

	offset, err := v.resume(out, f.Checkpoint)
	if err != nil {
		return d.diskError(f, err)
	}
	if offset < f.Size {
		if err := d.fetchRanges(ctx, f, out, v, offset); err != nil {
			if ReasonOf(err) == ReasonIntegrityMismatch {
				_ = out.Close()
				_ = os.Remove(f.Dest)
				removeCheckpoint(f.Checkpoint)
			}
			return err
		}
	}
	if err := out.Sync(); err != nil {
		return d.diskError(f, err)
	}
	if err := v.check(f.Path); err != nil {
		_ = out.Close()
		_ = os.Remove(f.Dest)
		removeCheckpoint(f.Checkpoint)
		return err
	}

	// 校验通过的文件保留末尾检查点，以便在发布前中断的尝试恢复时直接跳过该文件，避免重复下载。
	return d.checkpoint(f, out, v)
}

// errRangesUnsupported 表示 Hub 对非零起始偏移的 Range 请求返回了整个文件。
var errRangesUnsupported = errors.New("the hub does not honor byte ranges")

// fetchRanges 下载 [offset, size) 区间并按序计算哈希。首个分片单独请求以探测 Hub 是否支持 Range：
// 若返回整个文件则在单次响应中读完；若不支持断点续传则截断文件从头开始。
// 剩余部分在滑动窗口内并行拉取分片，待前方连续字节就绪后依次哈希。
func (d *Downloader) fetchRanges(ctx context.Context, f File, out *os.File, v *verifier, offset int64) error {
	segment := max(d.SegmentBytes, 1)

	reached, err := d.fetchSegment(ctx, f, out, offset, min(offset+segment, f.Size))
	if errors.Is(err, errRangesUnsupported) && offset > 0 {
		if err := out.Truncate(0); err != nil {
			return d.diskError(f, err)
		}
		// 文件中原有 reached 字节：调用方初值计入了续传偏移，后续到达字节也已实时上报，截断后全部清零扣回。
		if f.Received != nil {
			f.Received(-reached)
		}
		fresh, verr := newVerifier(f.Digest, f.Size)
		if verr != nil {
			return verr
		}
		*v = *fresh
		offset = 0
		reached, err = d.fetchSegment(ctx, f, out, 0, min(segment, f.Size))
	}
	if err != nil {
		return d.stopped(f, out, v, err)
	}
	if _, err := io.Copy(v, io.NewSectionReader(out, offset, reached-offset)); err != nil {
		return d.diskError(f, err)
	}
	if reached >= f.Size {
		return nil
	}

	return d.fetchParallel(ctx, f, out, v, reached)
}

func (d *Downloader) fetchParallel(ctx context.Context, f File, out *os.File, v *verifier, offset int64) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	segment := max(d.SegmentBytes, 1)
	var starts []int64
	for s := offset; s < f.Size; s += segment {
		starts = append(starts, s)
	}
	window := max(d.Window, 1)

	done := make([]chan struct{}, len(starts))
	for i := range done {
		done[i] = make(chan struct{})
	}
	// slots 每完成一个分片的哈希才放行一个新分片，从而将当前文件已下载但未哈希的字节限制在窗口大小内。
	slots := make(chan struct{}, window)
	for range window {
		slots <- struct{}{}
	}

	var wg sync.WaitGroup
	wg.Go(func() {
		for i, start := range starts {
			select {
			case <-slots:
			case <-ctx.Done():
				return
			}
			end := min(start+segment, f.Size)
			wg.Go(func() {
				if _, err := d.fetchSegment(ctx, f, out, start, end); err != nil {
					cancel(err)
					return
				}
				close(done[i])
			})
		}
	})

	var err error
	lastCheckpoint := v.offset
	for i, start := range starts {
		select {
		case <-done[i]:
		case <-ctx.Done():
			err = context.Cause(ctx)
		}
		if err != nil {
			break
		}
		end := min(start+segment, f.Size)
		if _, err = io.Copy(v, io.NewSectionReader(out, start, end-start)); err != nil {
			err = d.diskError(f, err)
			break
		}
		if v.offset-lastCheckpoint >= d.CheckpointBytes && v.offset < f.Size {
			if err = d.checkpoint(f, out, v); err != nil {
				break
			}
			lastCheckpoint = v.offset
		}
		slots <- struct{}{}
	}
	cancel(err)
	wg.Wait()

	if err == nil {
		return nil
	}

	return d.stopped(f, out, v, err)
}

// stopped 保存当前已哈希的进度用于续传，并分类下载中止的原因。
func (d *Downloader) stopped(f File, out *os.File, v *verifier, err error) error {
	_ = d.checkpoint(f, out, v)
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return detailedf(ReasonCanceled, "canceled", "%s: canceled", f.Path)
	}
	if errors.Is(err, errRangesUnsupported) {
		return errorf(ReasonSourceUnavailable, "%s: %v", f.Path, err)
	}

	return err
}

// checkpoint 先同步文件落盘再写入已哈希位置。必须先落盘：若哈希状态覆盖了崩溃后可能丢失的字节，
// 就会误校验已不在磁盘上的内容。
func (d *Downloader) checkpoint(f File, out *os.File, v *verifier) error {
	if f.Checkpoint == "" {
		return nil
	}
	if err := out.Sync(); err != nil {
		return d.diskError(f, err)
	}

	return v.saveCheckpoint(f.Checkpoint)
}

// fetchSegment 将 [start, end) 拉取到 out 中，遇到停滞或瞬时故障时从上次接收位置重试，
// 返回实际到达的偏移位置：通常为 end，或在 Hub 对首段返回整个文件时为文件末尾。
func (d *Downloader) fetchSegment(ctx context.Context, f File, out *os.File, start, end int64) (int64, error) {
	var lastErr error
	for attempt := 0; attempt <= max(d.Retries, 0); attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return start, context.Cause(ctx)
			case <-time.After(d.RetryDelay * time.Duration(min(attempt, 10))):
			}
		}
		reached, err := d.fetchRange(ctx, f, out, start, end)
		progressed := reached > start
		start = reached
		switch {
		case err == nil && start >= end:
			return start, nil
		case err == nil:
			lastErr = errorf(ReasonSourceUnavailable, "%s: the hub ended a range %d bytes short", f.Path, end-start)
		case errors.Is(err, errRangesUnsupported), ReasonOf(err) == ReasonAccessDenied,
			ReasonOf(err) == ReasonIntegrityMismatch, ReasonOf(err) == ReasonInsufficientCapacity, ctx.Err() != nil:
			return start, err
		default:
			lastErr = err
		}
		if progressed {
			// 只要取得了进展就重置重试计数：只有反复无进展失败的分片才会宣告整个文件失败。
			attempt = 0
		}
	}

	return start, detailedf(ReasonSourceUnavailable, DetailOf(lastErr), "%s: bytes %d to %d failed %d times: %v",
		f.Path, start, end, d.Retries+1, lastErr)
}

// fetchRange 发送单次 [start, end) 范围请求并将到达的字节写入对应偏移处，返回实际到达位置。
func (d *Downloader) fetchRange(ctx context.Context, f File, out *os.File, start, end int64) (int64, error) {
	streams, limiter := d.limits()
	select {
	case streams <- struct{}{}:
	case <-ctx.Done():
		return start, context.Cause(ctx)
	}
	defer func() { <-streams }()

	reqCtx, cancelReq := context.WithCancel(ctx)
	defer cancelReq()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, f.URL, nil)
	if err != nil {
		return start, errorf(ReasonInvalidRequest, "%s: %v", f.Path, err)
	}
	req.Header.Set("User-Agent", "gpustack-operator")
	req.Header.Set("Range", "bytes="+strconv.FormatInt(start, 10)+"-"+strconv.FormatInt(end-1, 10))
	if f.Token != nil {
		if token := f.Token(); token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
	}

	// 停滞定时器在 StallTimeout 内无数据到达（无论是无响应还是响应体卡住）时仅取消本次请求。
	// 客户端本身不设总超时，以免在慢速链路上掐断大分片传输。
	stall := time.AfterFunc(d.StallTimeout, cancelReq)
	defer stall.Stop()

	resp, err := d.client().Do(req)
	if err != nil {
		return start, detailedf(ReasonSourceUnavailable, transportDetail(err), "%s: GET %s: %v", f.Path, f.URL, unwrapURLError(err))
	}
	defer func() { _ = resp.Body.Close() }()
	stall.Reset(d.StallTimeout)

	switch {
	case resp.StatusCode == http.StatusPartialContent:
		rangeStart, total, ok := parseContentRange(resp.Header.Get("Content-Range"))
		switch {
		case !ok || rangeStart != start:
			return start, errorf(ReasonSourceUnavailable, "%s: the hub answered another range %q", f.Path,
				resp.Header.Get("Content-Range"))
		case total >= 0 && total != f.Size:
			return start, errorf(ReasonIntegrityMismatch, "%s: the hub serves %d bytes, the manifest says %d",
				f.Path, total, f.Size)
		}
	case resp.StatusCode == http.StatusOK && start == 0:
		// Hub 忽略 Range 返回了完整文件：直接从本次响应读完整份内容。
		if resp.ContentLength >= 0 && resp.ContentLength != f.Size {
			return start, errorf(ReasonIntegrityMismatch, "%s: the hub serves %d bytes, the manifest says %d",
				f.Path, resp.ContentLength, f.Size)
		}
		end = f.Size
	case resp.StatusCode == http.StatusOK:
		return start, errRangesUnsupported
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden ||
		resp.StatusCode == http.StatusNotFound:
		return start, detailedf(ReasonAccessDenied, "HTTP "+strconv.Itoa(resp.StatusCode),
			"%s: the file does not exist or is not accessible with the credential (HTTP %d)", f.Path, resp.StatusCode)
	default:
		return start, detailedf(ReasonSourceUnavailable, "HTTP "+strconv.Itoa(resp.StatusCode), "%s: HTTP %d", f.Path, resp.StatusCode)
	}

	buf := make([]byte, readChunk)
	pos := start
	for pos < end {
		want := min(int64(len(buf)), end-pos)
		if limiter != nil {
			// 等待节点自身的带宽限速不属于 Hub 停滞：无论低限速等待多久，等待期间均暂停停滞定时器。
			stall.Stop()
			if err := limiter.WaitN(ctx, int(want)); err != nil {
				return pos, context.Cause(ctx)
			}
			stall.Reset(d.StallTimeout)
		}
		n, readErr := io.ReadFull(resp.Body, buf[:want])
		if n > 0 {
			stall.Reset(d.StallTimeout)
			if _, err := out.WriteAt(buf[:n], pos); err != nil {
				return pos, d.diskError(f, err)
			}
			pos += int64(n)
			if d.Received != nil {
				d.Received(int64(n))
			}
			if f.Received != nil {
				f.Received(int64(n))
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
				if pos < end {
					return pos, errorf(ReasonSourceUnavailable, "%s: the hub closed the range early", f.Path)
				}
				break
			}
			if ctx.Err() != nil {
				return pos, context.Cause(ctx)
			}
			return pos, errorf(ReasonSourceUnavailable, "%s: read: %v", f.Path, readErr)
		}
	}

	return pos, nil
}

// client 返回带有重定向安全策略的 Client 副本：当重定向离开原主机或从 https 降级为 http 时移除凭据。
// Go 默认会向子域名转发 Authorization，而 Hub 的内容主机可能是子域名；
// 带签名的重定向目标不需要调用方凭据，而协议降级会导致凭据明文传输。
func (d *Downloader) client() *http.Client {
	d.mu.Lock()
	c := *d.Client
	d.mu.Unlock()
	next := c.CheckRedirect
	c.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) > 0 && (req.URL.Host != via[0].URL.Host ||
			via[0].URL.Scheme == "https" && req.URL.Scheme != "https") {
			req.Header.Del("Authorization")
		}
		if next != nil {
			return next(req, via)
		}
		if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		return nil
	}

	return &c
}

// parseContentRange 解析 "bytes <start>-<end>/<total>"，其中 "*" 对应的 total 为 -1。
func parseContentRange(header string) (start, total int64, ok bool) {
	spec, found := strings.CutPrefix(header, "bytes ")
	if !found {
		return 0, 0, false
	}
	span, size, found := strings.Cut(spec, "/")
	first, _, found2 := strings.Cut(span, "-")
	if !found || !found2 {
		return 0, 0, false
	}
	start, err := strconv.ParseInt(first, 10, 64)
	if err != nil {
		return 0, 0, false
	}
	if size == "*" {
		return start, -1, true
	}
	total, err = strconv.ParseInt(size, 10, 64)
	if err != nil {
		return 0, 0, false
	}

	return start, total, true
}

// diskError 对本地磁盘写入错误分类：磁盘满归为容量不足，其余错误非 Hub 原因直接原样包装返回。
func (d *Downloader) diskError(f File, err error) error {
	if errors.Is(err, syscall.ENOSPC) {
		return detailedf(ReasonInsufficientCapacity, "no space left on the cache's filesystem",
			"%s: no space left on the cache's filesystem", f.Path)
	}

	return fmt.Errorf("%s: %w", f.Path, err)
}

// transportDetail 返回无响应请求的故障类别，不暴露 URL。
func transportDetail(err error) string {
	var (
		netErr   net.Error
		certErr  *tls.CertificateVerificationError
		unknownA x509.UnknownAuthorityError
		hostErr  x509.HostnameError
		recErr   tls.RecordHeaderError
	)
	switch {
	case errors.As(err, &certErr), errors.As(err, &unknownA), errors.As(err, &hostErr), errors.As(err, &recErr):
		return "a TLS failure"
	case errors.As(err, &netErr) && netErr.Timeout():
		return "a timeout"
	default:
		return "a connection failure"
	}
}

// unwrapURLError 剥离传输错误中的请求 URL：重定向后的目标地址可能包含签名信息。
func unwrapURLError(err error) error {
	if ue, ok := errors.AsType[*url.Error](err); ok {
		return ue.Err
	}

	return err
}
