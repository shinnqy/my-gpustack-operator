package peer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	modelartifact "gpustack.ai/gpustack/pkg/modelartifact"
	"gpustack.ai/gpustack/pkg/modelmanager/download"
	"gpustack.ai/gpustack/pkg/utils/json"
)

// Puller 的默认参数。
const (
	// DefaultSegmentBytes 是单次请求拉取大文件时的字节分片大小。
	DefaultSegmentBytes = 64 << 20
	// peerCooldown 是某来源失败后跨文件与跨尝试的冷却时长，避免一个故障 peer 被并行文件反复重试。
	peerCooldown = time.Minute
	// readChunk 是每次读取响应体的块大小。
	readChunk = 32 << 10
)

// Puller 发现持有某摘要的 peer 来源并依次拉取文件。
// 失败的来源会暂时冷却并切换到下一个候选；所有候选耗尽后是否回退到 Hub 由调用方决定。
type Puller struct {
	// Discover 按顺序返回已就绪持有该摘要的候选节点来源。
	Discover func(ctx context.Context, digest string) ([]*Source, error)
	// SegmentBytes 是单次请求的字节分片大小；为 0 时使用默认值。
	SegmentBytes int64
	// Received 在收到字节时并发回调并附带来源节点名；为 nil 时不计数。
	// peer 字节数在此处与 Hub 下载器分开统计。
	Received func(node string, n int64)

	mu      sync.Mutex
	cooling map[string]time.Time
}

// Enabled 报告 peer 拉取是否已配置启用。
func (p *Puller) Enabled() bool { return p != nil && p.Discover != nil }

// FetchFile 从持有目标摘要的 peer 拉取 file，并对照重新绑定到根摘要的清单逐字节校验。
// 无论此前由谁下载，都会从已有检查点继续并在传输过程中持续写检查点，使中途 peer 故障仅损失上次检查点后的字节。
// 任一候选失败均轮换至下一个候选；所有候选均失败时由调用方回退到 Hub。
func (p *Puller) FetchFile(ctx context.Context, digest string, file download.File) error {
	treeHex, ok := HexOf(digest)
	if !ok {
		return fmt.Errorf("digest %q is not a manifest digest", digest)
	}
	sources, err := p.Discover(ctx, digest)
	if err != nil {
		return err
	}
	var lastErr error
	for _, src := range sources {
		if p.sitsOut(src) {
			continue
		}
		err = p.fetchFrom(ctx, src, treeHex, file)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		p.setAside(src)
		lastErr = err
	}
	if lastErr == nil {
		lastErr = errors.New("no candidate was usable")
	}

	return fmt.Errorf("%s: %w", file.Path, lastErr)
}

// FetchManifest 从已发布目标目录树的 peer 拉取清单，重组为规范清单并绑定校验摘要后返回。
// 对于没有 Hub 来源、仅锚定摘要的制品，调用方会优先调用此方法；若无可用来源，则直接返回明确的无源错误。
func (p *Puller) FetchManifest(ctx context.Context, digest string) (modelartifact.Manifest, error) {
	treeHex, ok := HexOf(digest)
	if !ok {
		return modelartifact.Manifest{}, fmt.Errorf("digest %q is not a manifest digest", digest)
	}
	sources, err := p.Discover(ctx, digest)
	if err != nil {
		return modelartifact.Manifest{}, err
	}
	var lastErr error
	for _, src := range sources {
		if p.sitsOut(src) {
			continue
		}
		manifest, err := p.fetchListing(ctx, src, treeHex)
		if err == nil {
			return manifest, nil
		}
		if ctx.Err() != nil {
			return modelartifact.Manifest{}, context.Cause(ctx)
		}
		p.setAside(src)
		lastErr = err
	}
	if lastErr == nil {
		lastErr = errors.New("no candidate was usable")
	}

	return modelartifact.Manifest{}, fmt.Errorf("no node holds %s: %w", digest, lastErr)
}

// fetchFrom 从单个来源拉取完整文件：先重组清单并核对目录树根摘要，确认无误后再按序拉取分片、
// 计算哈希并写入检查点。
func (p *Puller) fetchFrom(ctx context.Context, src *Source, treeHex string, file download.File) error {
	manifest, err := p.fetchListing(ctx, src, treeHex)
	if err != nil {
		return err
	}
	var entry modelartifact.ManifestEntry
	found := false
	for _, e := range manifest.Entries {
		if e.Path == file.Path {
			entry, found = e, true

			break
		}
	}
	if !found {
		return fmt.Errorf("the manifest does not name %s", file.Path)
	}
	if entry.Size != file.Size || entry.Digest != file.Digest {
		// 清单已与正确的根摘要完成绑定，因此其条目为准；调用方由同一规范清单构造，不一致说明存在异常。
		return fmt.Errorf("the manifest says %d bytes %s, the attempt says %d %s",
			entry.Size, entry.Digest, file.Size, file.Digest)
	}

	hasher, offset, err := download.Resume(file.Digest, file.Size, file.Dest, file.Checkpoint, checkpointInterval)
	if err != nil {
		return err
	}
	if offset < file.Size {
		if err := p.fetchRanges(ctx, src, treeHex, file, hasher, offset); err != nil {
			return err
		}
	}
	if err := hasher.Verify(file.Dest); err != nil {
		return err
	}

	return nil
}

// fetchRanges 按字节分片拉取文件的 [offset, size) 区间并按序送入哈希器。
func (p *Puller) fetchRanges(
	ctx context.Context, src *Source, treeHex string, file download.File,
	hasher *download.StreamHasher, offset int64,
) error {
	out, err := os.OpenFile(file.Dest, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer func() { _ = out.Close() }()

	segment := p.segmentBytes()
	for start := offset; start < file.Size; start += segment {
		end := min(start+segment, file.Size)
		if err := p.fetchOne(ctx, src, treeHex, file, out, start, end, hasher); err != nil {
			return err
		}
	}

	return nil
}

// fetchOne 发送单个范围请求，将到达数据写入文件对应偏移并按序送入哈希器。
func (p *Puller) fetchOne(
	ctx context.Context, src *Source, treeHex string, file download.File, out *os.File, start, end int64,
	hasher *download.StreamHasher,
) error {
	release, err := src.take(ctx)
	if err != nil {
		return err
	}
	defer release()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fileURL(src.BaseURL, treeHex, file.Path), nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "gpustack-operator")
	req.Header.Set("Range", "bytes="+strconv.FormatInt(start, 10)+"-"+strconv.FormatInt(end-1, 10))
	resp, err := src.Client.Do(req)
	if err != nil {
		return fmt.Errorf("the peer did not answer: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	switch {
	case resp.StatusCode == http.StatusPartialContent:
		if got := contentRangeStart(resp.Header.Get("Content-Range")); got != start {
			return fmt.Errorf("the peer answered another range %q", resp.Header.Get("Content-Range"))
		}
	case resp.StatusCode == http.StatusOK && start == 0:
		if resp.ContentLength >= 0 && resp.ContentLength != end {
			return fmt.Errorf("the peer serves %d bytes, the manifest says %d", resp.ContentLength, end)
		}
	case resp.StatusCode == http.StatusOK:
		return errors.New("the peer does not honor byte ranges")
	default:
		return fmt.Errorf("the peer answered HTTP %d", resp.StatusCode)
	}

	buf := make([]byte, min(readChunk, end-start))
	pos := start
	for pos < end {
		want := int64(len(buf))
		if left := end - pos; left < want {
			want = left
		}
		n, readErr := io.ReadFull(resp.Body, buf[:want])
		if n > 0 {
			if _, err := out.WriteAt(buf[:n], pos); err != nil {
				return err
			}
			if err := hasher.Hash(buf[:n]); err != nil {
				return err
			}
			pos += int64(n)
			if file.Received != nil {
				file.Received(int64(n))
			}
			if p.Received != nil {
				p.Received(src.NodeName, int64(n))
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
				if pos < end {
					return errors.New("the peer closed the range early")
				}

				break
			}

			return readErr
		}
	}

	return nil
}

// fetchListing 向单个来源请求目录树清单并将其重新绑定到目录树摘要：
// 将条目重新编码为规范清单并核对根摘要是否与目标摘要一致。在拉取任何文件字节前拦截被截断或篡改的列表。
func (p *Puller) fetchListing(ctx context.Context, src *Source, treeHex string) (modelartifact.Manifest, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, listingURL(src.BaseURL, treeHex), nil)
	if err != nil {
		return modelartifact.Manifest{}, err
	}
	req.Header.Set("User-Agent", "gpustack-operator")
	resp, err := src.Client.Do(req)
	if err != nil {
		return modelartifact.Manifest{}, fmt.Errorf("the peer did not answer: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return modelartifact.Manifest{}, fmt.Errorf("the peer answered HTTP %d", resp.StatusCode)
	}
	var l listing
	if err := json.NewDecoder(io.LimitReader(resp.Body, manifestLimit)).Decode(&l); err != nil {
		return modelartifact.Manifest{}, fmt.Errorf("the peer's listing does not parse: %w", err)
	}
	entries := make([]modelartifact.ManifestEntry, 0, len(l.Entries))
	for _, e := range l.Entries {
		entries = append(entries, modelartifact.ManifestEntry{Path: e.Path, Size: e.Size, Digest: e.Digest})
	}
	manifest, err := modelartifact.NewManifest(entries)
	if err != nil {
		return modelartifact.Manifest{}, fmt.Errorf("the peer's listing is not a manifest: %w", err)
	}
	if manifest.Digest != "sha256:"+treeHex {
		return modelartifact.Manifest{}, fmt.Errorf("the peer's listing hashes to %s, the content is %s: refused",
			manifest.Digest, "sha256:"+treeHex)
	}

	return manifest, nil
}

// manifestLimit 限制单次清单响应体的大小上限。
const manifestLimit = 64 << 20

// setAside 让刚失败的来源进入冷却期。
func (p *Puller) setAside(src *Source) {
	p.mu.Lock()
	if p.cooling == nil {
		p.cooling = map[string]time.Time{}
	}
	p.cooling[src.BaseURL] = time.Now().Add(peerCooldown)
	p.mu.Unlock()
}

// sitsOut 报告该来源当前是否处于冷却期。
func (p *Puller) sitsOut(src *Source) bool {
	p.mu.Lock()
	until, ok := p.cooling[src.BaseURL]
	p.mu.Unlock()

	return ok && time.Now().Before(until)
}

func (p *Puller) segmentBytes() int64 {
	if p.SegmentBytes > 0 {
		return p.SegmentBytes
	}

	return DefaultSegmentBytes
}

// checkpointInterval 指定 peer 拉取时每哈希 64 MiB 写一次检查点。
const checkpointInterval = 64 << 20

// fileURL 构造单个文件的 peer 下载地址。
func fileURL(base, treeHex, name string) string {
	u, err := parseBase(base)
	if err != nil {
		return base
	}

	return u.JoinPath("peer/v1/trees", treeHex, "files", filepath.ToSlash(name)).String()
}

// listingURL 构造目录树清单的 peer 查询地址。
func listingURL(base, treeHex string) string {
	u, err := parseBase(base)
	if err != nil {
		return base
	}

	return u.JoinPath("peer/v1/trees", treeHex).String()
}

func parseBase(base string) (*url.URL, error) {
	return url.Parse(base)
}

// contentRangeStart 从 "bytes <start>-<end>/<total>" 中解析起始偏移量。
func contentRangeStart(header string) int64 {
	spec, found := strings.CutPrefix(header, "bytes ")
	if !found {
		return -1
	}
	span, _, found := strings.Cut(spec, "/")
	first, _, found2 := strings.Cut(span, "-")
	if !found || !found2 {
		return -1
	}
	start, err := strconv.ParseInt(first, 10, 64)
	if err != nil {
		return -1
	}

	return start
}
