package modelartifact

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"gpustack.ai/gpustack/pkg/utils/httpx"
	"gpustack.ai/gpustack/pkg/utils/json"
)

// DefaultHuggingFaceMaxEntries 限制单次解析读取的目录树条目数（文件与目录总和）。
// 该上限远高于任何正常模型仓库，可防止异常仓库拖慢调谐循环并耗尽 Hub 的速率配额。
const DefaultHuggingFaceMaxEntries = 100000

// huggingFaceMaxResponseBytes 限制单个响应体的大小。包含一千个条目的目录树分页只有几百 KB。
const huggingFaceMaxResponseBytes = 16 << 20

// huggingFaceCommitPattern 是完整 commit 的格式，revision 端点对所有可接受的 revision 均返回此格式。
var huggingFaceCommitPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)

// huggingFaceMaskedOID 是受限仓库对未获授权调用方返回 LFS 文件 SHA-256 时的掩码值，
// 而目录树本身仍返回 200。
var huggingFaceMaskedOID = strings.Repeat("*", 64)

// HuggingFace 用于解析 Hugging Face 仓库。
//
// 每个方法都显式接收 token 而非在实例中持有，以便客户端在多个 Namespace 之间共享，
// 且绝不会在单次调用之外保留某个 Namespace 的凭据。
type HuggingFace struct {
	// Endpoint 是 Hub 的基础 URL，例如 "https://huggingface.co"。
	Endpoint string
	// Client 发送请求；参见 NewHTTPClient。
	Client *http.Client
	// MaxEntries 限制单次解析的目录树条目数。为零时使用 DefaultHuggingFaceMaxEntries。
	MaxEntries int
}

// Resolution 是按指定 revision 解析仓库后的产物。
type Resolution struct {
	// Commit 是 revision 所解析到的完整 40 位 commit。
	Commit string
	// Manifest 是 Commit 下所有文件的规范清单。
	Manifest Manifest
}

// Filter 是制品的 allow 和 ignore 过滤模式；参见 FilterEntries。零值表示选择所有文件。
type Filter struct {
	Allow  []string
	Ignore []string
}

// Resolve 将 revision 解析为 commit，并构建由 filter 选中的该 commit 下文件的清单。
//
// 它使用 revision 端点而非 refs 端点，因为只有前者会将附注标签（annotated tag）解引用到其指向的 commit；
// 后者返回的是标签对象本身。
func (h *HuggingFace) Resolve(ctx context.Context, repository, revision, token string, filter Filter) (Resolution, error) {
	var info struct {
		SHA string `json:"sha"`
	}
	infoURL := h.apiURL("models", repository, "revision", revision)
	if err := h.getJSON(ctx, infoURL, token, &info); err != nil {
		return Resolution{}, err
	}
	if !huggingFaceCommitPattern.MatchString(info.SHA) {
		return Resolution{}, sourceErrorf(ReasonSourceUnavailable,
			"the Hub answered revision %q of %q with a commit %q that is not 40 hexadecimal digits",
			revision, repository, info.SHA)
	}

	manifest, err := h.ListManifest(ctx, repository, info.SHA, token, filter)
	if err != nil {
		return Resolution{}, err
	}

	return Resolution{Commit: info.SHA, Manifest: manifest}, nil
}

// ListManifest 从目录树列表的所有分页中构建 commit 下由 filter 选中的文件清单。
// 节点会使用挂载该制品的 Pod 的凭据以这种方式重新计算清单，并将摘要与控制器发布的摘要进行比对。
func (h *HuggingFace) ListManifest(ctx context.Context, repository, commit, token string, filter Filter) (Manifest, error) {
	entries, err := h.listTree(ctx, repository, commit, token)
	if err != nil {
		return Manifest{}, err
	}
	if len(entries) == 0 {
		return Manifest{}, sourceErrorf(ReasonEmptyManifest, "commit %s of %q holds no file", commit, repository)
	}
	entries = FilterEntries(entries, filter.Allow, filter.Ignore)
	if len(entries) == 0 {
		return Manifest{}, sourceErrorf(ReasonEmptyManifest,
			"no file of commit %s of %q matches the allow and ignore patterns", commit, repository)
	}
	manifest, err := NewManifest(entries)
	if err != nil {
		return Manifest{}, sourceErrorf(ReasonInvalidManifest, "commit %s of %q: %v", commit, repository, err)
	}

	return manifest, nil
}

// huggingFaceTreeEntry 是目录树列表中的一个条目。
type huggingFaceTreeEntry struct {
	Type string `json:"type"`
	OID  string `json:"oid"`
	Size int64  `json:"size"`
	Path string `json:"path"`
	LFS  *struct {
		OID  string `json:"oid"`
		Size int64  `json:"size"`
	} `json:"lfs"`
}

// listTree 读取 commit 下递归目录树的所有分页。不使用 model-info 端点的 siblings 字段：
// 该列表不支持分页，且 huggingface_hub 本身在文件超过一千个后也不再信任该列表。
func (h *HuggingFace) listTree(ctx context.Context, repository, commit, token string) ([]ManifestEntry, error) {
	limit := h.MaxEntries
	if limit <= 0 {
		limit = DefaultHuggingFaceMaxEntries
	}

	var (
		entries []ManifestEntry
		seen    int
		next    = h.apiURL("models", repository, "tree", commit) + "?recursive=true"
	)
	for next != "" {
		var page []huggingFaceTreeEntry
		resp, err := h.do(ctx, http.MethodGet, next, token, false)
		if err != nil {
			return nil, err
		}
		link := resp.Header.Get("Link")
		err = decodeHuggingFaceBody(resp, &page)
		if err != nil {
			return nil, err
		}

		seen += len(page)
		if seen > limit {
			return nil, sourceErrorf(ReasonManifestTooLarge,
				"commit %s of %q lists more than %d tree entries", commit, repository, limit)
		}
		for _, e := range page {
			entry, isFile, err := huggingFaceManifestEntry(e)
			if err != nil {
				return nil, err
			}
			if isFile {
				entries = append(entries, entry)
			}
		}

		next, err = h.nextPage(link)
		if err != nil {
			return nil, err
		}
	}

	return entries, nil
}

// huggingFaceManifestEntry 将树条目转换为清单条目，并在条目为目录时返回 false。
func huggingFaceManifestEntry(e huggingFaceTreeEntry) (ManifestEntry, bool, error) {
	switch e.Type {
	case "directory":
		return ManifestEntry{}, false, nil
	case "file":
	default:
		return ManifestEntry{}, false, sourceErrorf(ReasonInvalidManifest,
			"tree entry %q has the unknown type %q", e.Path, e.Type)
	}

	if e.LFS == nil {
		return ManifestEntry{Path: e.Path, Size: e.Size, Digest: DigestGitSHA1 + ":" + e.OID}, true, nil
	}
	if e.LFS.OID == huggingFaceMaskedOID {
		return ManifestEntry{}, false, sourceErrorf(ReasonAccessDenied,
			"the repository is gated and the credential has not been granted access: the Hub masks "+
				"the digest of %q", e.Path)
	}
	if e.LFS.Size != e.Size {
		return ManifestEntry{}, false, sourceErrorf(ReasonInvalidManifest,
			"tree entry %q has size %d but LFS size %d", e.Path, e.Size, e.LFS.Size)
	}

	return ManifestEntry{Path: e.Path, Size: e.Size, Digest: DigestSHA256 + ":" + e.LFS.OID}, true, nil
}

// nextPage 返回 Link 响应头中标记为 rel="next" 的 URL；若未指定则返回 ""。
//
// 指向其他主机的下一页链接会被拒绝而非跟随：token 会随每一页请求发送，
// 分页不应成为将 token 发送到已配置端点之外任何地址的理由。
func (h *HuggingFace) nextPage(link string) (string, error) {
	for part := range strings.SplitSeq(link, ",") {
		target, params, ok := strings.Cut(strings.TrimSpace(part), ";")
		if !ok || !strings.Contains(strings.ReplaceAll(params, " ", ""), `rel="next"`) {
			continue
		}
		target = strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(target), "<"), ">")
		next, err := url.Parse(target)
		if err != nil {
			return "", sourceErrorf(ReasonSourceUnavailable, "the Hub answered an unreadable next page %q", target)
		}
		base, err := url.Parse(h.Endpoint)
		if err != nil {
			return "", err
		}
		next = base.ResolveReference(next)
		if next.Host != base.Host {
			return "", sourceErrorf(ReasonSourceUnavailable,
				"the Hub answered a next page on another host %q", next.Host)
		}

		return next.String(), nil
	}

	return "", nil
}

// ValidToken 报告 Hub 是否接受该 token。Hub 会静默忽略公开仓库上的无效 token，
// 因此这是发现此类错误配置的唯一途径。
func (h *HuggingFace) ValidToken(ctx context.Context, token string) (bool, error) {
	resp, err := h.do(ctx, http.MethodGet, h.apiURL("whoami-v2"), token, false)
	if err != nil {
		return false, err
	}
	httpx.Close(resp)

	switch {
	case resp.StatusCode == http.StatusUnauthorized:
		return false, nil
	case resp.StatusCode/100 == 2:
		return true, nil
	default:
		return false, classifyHuggingFace(resp, "the token check")
	}
}

// Revalidate 检查 token 在指定 commit 下是否仍可读取该仓库：对单个文件发起不跟随重定向的 HEAD 请求。
// revision 和 tree 端点无法回答此问题，因为对于无权访问的调用方，受限（gated）仓库对这两个端点仍会返回 200。
//
// 目标文件取自该 commit 根目录一页列表中的首个文件（按路径），因此无论仓库规模多大，该检查仅需两次请求。
// 若根目录不含文件，则回退到递归列表中的首个文件。
func (h *HuggingFace) Revalidate(ctx context.Context, repository, commit, token string) error {
	var page []huggingFaceTreeEntry
	if err := h.getJSON(ctx, h.apiURL("models", repository, "tree", commit), token, &page); err != nil {
		return err
	}

	file := firstHuggingFaceFile(page)
	if file == "" {
		entries, err := h.listTree(ctx, repository, commit, token)
		if err != nil {
			return err
		}
		for _, e := range entries {
			if file == "" || e.Path < file {
				file = e.Path
			}
		}
		if file == "" {
			return sourceErrorf(ReasonEmptyManifest, "commit %s of %q holds no file", commit, repository)
		}
	}

	resp, err := h.do(ctx, http.MethodHead, h.resolveURL(repository, commit, file), token, true)
	if err != nil {
		return err
	}
	httpx.Close(resp)
	if resp.StatusCode/100 == 2 || resp.StatusCode/100 == 3 {
		return nil
	}

	return classifyHuggingFace(resp, "the access check")
}

func firstHuggingFaceFile(page []huggingFaceTreeEntry) string {
	first := ""
	for _, e := range page {
		if e.Type == "file" && (first == "" || e.Path < first) {
			first = e.Path
		}
	}

	return first
}

func (h *HuggingFace) apiURL(segments ...string) string {
	escaped := make([]string, 0, len(segments)+1)
	escaped = append(escaped, strings.TrimSuffix(h.Endpoint, "/"), "api")
	for i, s := range segments {
		// 仓库保留其所有者分隔符；其余每个分段（尤其是 revision）无论包含何种字符，
		// 均作为单个路径分段处理。
		if i == 1 && segments[0] == "models" {
			escaped = append(escaped, escapeRepository(s))
			continue
		}
		escaped = append(escaped, url.PathEscape(s))
	}

	return strings.Join(escaped, "/")
}

func (h *HuggingFace) resolveURL(repository, commit, file string) string {
	parts := strings.Split(file, "/")
	for i := range parts {
		parts[i] = url.PathEscape(parts[i])
	}

	return strings.Join([]string{
		strings.TrimSuffix(h.Endpoint, "/"), escapeRepository(repository), "resolve", commit, strings.Join(parts, "/"),
	}, "/")
}

func escapeRepository(repository string) string {
	owner, name, ok := strings.Cut(repository, "/")
	if !ok {
		return url.PathEscape(repository)
	}

	return url.PathEscape(owner) + "/" + url.PathEscape(name)
}

func (h *HuggingFace) getJSON(ctx context.Context, target, token string, into any) error {
	resp, err := h.do(ctx, http.MethodGet, target, token, false)
	if err != nil {
		return err
	}

	return decodeHuggingFaceBody(resp, into)
}

// do 发送一次请求。传输层失败归类为 SourceUnavailable；状态码由调用方分类处理。
func (h *HuggingFace) do(ctx context.Context, method, target, token string, noRedirect bool) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, target, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "gpustack-operator")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	client := h.Client
	if noRedirect {
		c := *client
		c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		client = &c
	}

	resp, err := client.Do(req)
	if err != nil {
		// URL 是传输层错误中唯一包含请求信息的部分，且不携带凭据；
		// 这里重建错误，以确保请求的其他信息不会随错误一同传出。
		return nil, sourceErrorf(ReasonSourceUnavailable, "%s %s: %v", method, target, unwrapURLError(err))
	}

	return resp, nil
}

func unwrapURLError(err error) error {
	if ue, ok := errors.AsType[*url.Error](err); ok {
		return ue.Err
	}

	return err
}

func decodeHuggingFaceBody(resp *http.Response, into any) error {
	defer httpx.Close(resp)
	if resp.StatusCode != http.StatusOK {
		return classifyHuggingFace(resp, resp.Request.URL.Path)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, huggingFaceMaxResponseBytes+1))
	if err != nil {
		return sourceErrorf(ReasonSourceUnavailable, "read %s: %v", resp.Request.URL.Path, err)
	}
	if len(body) > huggingFaceMaxResponseBytes {
		return sourceErrorf(ReasonSourceUnavailable, "%s answered more than %d bytes", resp.Request.URL.Path,
			huggingFaceMaxResponseBytes)
	}
	if err := json.Unmarshal(body, into); err != nil {
		return sourceErrorf(ReasonSourceUnavailable, "%s answered an unreadable body: %v", resp.Request.URL.Path, err)
	}

	return nil
}

// classifyHuggingFace 根据 Hub 的实测响应将非成功响应映射为对应原因：
//
//   - 404 且 X-Error-Code 为 RevisionNotFound 表示仓库不存在该 revision。
//   - 401（无论是否携带错误码）、403、其他 404 以及 X-Error-Code 为 GatedRepo 均归类为
//     AccessDenied。未提供 token 时，不存在的仓库与调用方无权读取的私有仓库均返回相同的 401，
//     因此错误信息不做猜测区分。
//   - 其余情况（包括 5xx 和 429）均归类为 SourceUnavailable。
func classifyHuggingFace(resp *http.Response, what string) error {
	code := resp.Header.Get("X-Error-Code")
	switch {
	case resp.StatusCode == http.StatusNotFound && code == "RevisionNotFound":
		return sourceErrorf(ReasonRevisionNotFound, "%s: the repository has no such revision", what)
	case code == "GatedRepo":
		return sourceErrorf(ReasonAccessDenied,
			"%s: the repository is gated and the credential has not been granted access", what)
	case resp.StatusCode == http.StatusUnauthorized, resp.StatusCode == http.StatusForbidden,
		resp.StatusCode == http.StatusNotFound:
		return sourceErrorf(ReasonAccessDenied,
			"%s: the repository does not exist or is not accessible with the credential (HTTP %d%s)",
			what, resp.StatusCode, errorCodeSuffix(code))
	default:
		return sourceErrorf(ReasonSourceUnavailable, "%s: HTTP %d%s", what, resp.StatusCode, errorCodeSuffix(code))
	}
}

func errorCodeSuffix(code string) string {
	if code == "" {
		return ""
	}

	return ", " + code
}
