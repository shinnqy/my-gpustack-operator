package modelartifact

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"gpustack.ai/gpustack/pkg/utils/httpx"
	"gpustack.ai/gpustack/pkg/utils/json"
)

// ModelScope 用于解析 ModelScope 仓库。
//
// 每个方法均通过参数接收 token 而非持有 token，从而使客户端可在多个 Namespace 间共享，
// 且不会在单次调用之外保留任何 Namespace 的凭据。
type ModelScope struct {
	// Endpoint 是 Hub 的基础 URL，例如 "https://www.modelscope.cn"。
	Endpoint string
	// Client 用于发送请求；参见 NewHTTPClient。
	Client *http.Client
	// MaxEntries 限制单次解析的树条目上限。为零时使用
	// DefaultHuggingFaceMaxEntries，两个 Hub 共用此上限。
	MaxEntries int

	// lsRemote 向仓库的 git 端点查询按 ref 名称索引的 refs。测试中会替换该函数；
	// 默认实现直接通过 HTTP smart 协议通信。
	lsRemote func(ctx context.Context, gitURL, token string) (map[string]string, error)
}

// modelScopeCommitPattern 为完整 commit 的格式，完整 commit 按原样使用，
// 仅当 revision 为分支或标签时才通过 git 进行交叉校验。
var modelScopeCommitPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)

// resolveRevision 将分支或标签解析为 Hub 索引与 git 共同确认的 commit，
// 对完整 commit 则按原样采用。
//
// commits 端点未经文档说明，且在 Ref 参数拼写错误时会返回 master，
// 因此绝不单独信任其结果：对于分支，返回值必须与 git 中同名 ref 指向的 commit 一致；
// 对于附注标签，必须与 git 中该标签解引用（peeled）条目的 commit 一致。
// 若出现不一致则拒绝解析：存在偏差时宁可拒绝，绝不返回错误内容。
func (m *ModelScope) resolveRevision(ctx context.Context, repository, revision, token string) (string, error) {
	if modelScopeCommitPattern.MatchString(revision) {
		return revision, nil
	}

	var answer struct {
		Commit []struct {
			Id string `json:"Id"`
		} `json:"Commit"`
	}
	target := m.apiURL("models", repository, "commits") + "?Ref=" + url.QueryEscape(revision) + "&PageSize=1"
	if err := m.getJSON(ctx, target, token, &answer); err != nil {
		return "", err
	}
	if len(answer.Commit) == 0 {
		return "", sourceErrorf(ReasonRevisionNotFound, "the repository has no branch or tag %q", revision)
	}
	commit := answer.Commit[0].Id
	if !modelScopeCommitPattern.MatchString(commit) {
		return "", sourceErrorf(ReasonSourceUnavailable,
			"the hub answered revision %q of %q with a commit %q that is not 40 hexadecimal digits",
			revision, repository, commit)
	}

	refs, err := m.runLsRemote(ctx, repository, token)
	if err != nil {
		return "", err
	}
	against := modelScopeRefCommit(refs, revision)
	switch {
	case against == "":
		return "", sourceErrorf(ReasonSourceUnavailable,
			"the hub's git names no ref for %q of %q: the commits endpoint answered %s, and the two must agree",
			revision, repository, commit)
	case against != commit:
		return "", sourceErrorf(ReasonSourceUnavailable,
			"the hub's index and its git disagree about %q of %q: the commits endpoint answers %s, git answers %s",
			revision, repository, commit, against)
	}

	return commit, nil
}

// modelScopeRefCommit 选取 git 为 revision 指定的 commit：附注标签的解引用条目、
// 轻量标签本身或分支的 ref。此处刻意不回退到 HEAD：commits 端点在 Ref 拼写错误时会返回 master，
// 而指向 master 的 HEAD 若作为回退目标，恰好会印证这种静默重写，无法将其拦截。
func modelScopeRefCommit(refs map[string]string, revision string) string {
	for _, name := range []string{
		"refs/tags/" + revision + "^{}", "refs/tags/" + revision, "refs/heads/" + revision,
	} {
		if commit, ok := refs[name]; ok {
			return commit
		}
	}

	return ""
}

// modelScopePageBound 是 repo/files 接口发生静默截断的列表大小上限（经实测，PoC-D），
// 无论查询中指定何种分页参数均是如此：恰好返回此数量条目的分页即意味发生了截断，
// 只能通过遍历直接子项来恢复完整目录树。
const modelScopePageBound = 3000

// Resolve 将 revision 解析为 commit，并构建经 filter 筛选后的该 commit 文件清单。
func (m *ModelScope) Resolve(ctx context.Context, repository, revision, token string, filter Filter) (Resolution, error) {
	commit, err := m.resolveRevision(ctx, repository, revision, token)
	if err != nil {
		return Resolution{}, err
	}

	manifest, err := m.ListManifest(ctx, repository, commit, token, filter)
	if err != nil {
		return Resolution{}, err
	}

	return Resolution{Commit: commit, Manifest: manifest}, nil
}

// ListManifest 根据 Hub 的文件列表，构建 commit 下经 filter 筛选的文件清单。
// 节点使用挂载该制品的 Pod 所持凭据按此方式重新计算清单，并与控制器发布的摘要比对。
// 每个文件的摘要均为 Hub 自身提供的 sha256，因此 ModelScope 清单中不含 gitsha1 行。
func (m *ModelScope) ListManifest(ctx context.Context, repository, commit, token string, filter Filter) (Manifest, error) {
	entries, err := m.listRepoFiles(ctx, repository, commit, token)
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

// listRepoFiles 列出 commit 下的所有文件，并从 API 的静默截断中恢复完整列表。
//
// 若递归分页返回的条目数恰好等于 modelScopePageBound，则说明发生了截断：此时遍历会改为列出
// 该层级的直接子项并逐个进入其子目录，仅对发生截断的子树继续拆分。若某一层级的直接子项数量就达到上限，
// 则完全无法枚举；部分清单会导致静默的错误结果，因此直接拒绝。
func (m *ModelScope) listRepoFiles(ctx context.Context, repository, commit, token string) ([]ManifestEntry, error) {
	limit := m.MaxEntries
	if limit <= 0 {
		limit = DefaultHuggingFaceMaxEntries
	}

	var entries []ManifestEntry
	var walk func(dir string) error
	walk = func(dir string) error {
		page, truncated, err := m.listFiles(ctx, repository, commit, dir, true, token)
		if err != nil {
			return err
		}
		if !truncated {
			for _, e := range page {
				entry, ok, err := modelScopeManifestEntry(e)
				if err != nil {
					return err
				}
				if ok {
					entries = append(entries, entry)
					if len(entries) > limit {
						return sourceErrorf(ReasonManifestTooLarge,
							"commit %s of %q lists more than %d tree entries", commit, repository, limit)
					}
				}
			}
			return nil
		}

		children, truncated, err := m.listFiles(ctx, repository, commit, dir, false, token)
		if err != nil {
			return err
		}
		if truncated {
			return sourceErrorf(ReasonSourceUnavailable,
				"directory %q of commit %s lists %d or more direct children, which the API cannot enumerate",
				dir, commit, modelScopePageBound)
		}
		for _, e := range children {
			if e.Type == modelScopeTypeTree {
				if err := walk(e.Path); err != nil {
					return err
				}
				continue
			}
			entry, ok, err := modelScopeManifestEntry(e)
			if err != nil {
				return err
			}
			if ok {
				entries = append(entries, entry)
				if len(entries) > limit {
					return sourceErrorf(ReasonManifestTooLarge,
						"commit %s of %q lists more than %d tree entries", commit, repository, limit)
				}
			}
		}
		return nil
	}
	if err := walk(""); err != nil {
		return nil, err
	}

	return entries, nil
}

// modelScopeTypeTree 是目录条目的类型；每个文件均为 blob。
const modelScopeTypeTree = "tree"

// modelScopeFileEntry 是 repo/files 分页中的单个条目。
type modelScopeFileEntry struct {
	Type   string `json:"Type"`
	Path   string `json:"Path"`
	Size   int64  `json:"Size"`
	Sha256 string `json:"Sha256"`
}

// modelScopeManifestEntry 将列表条目转换为清单条目，并在条目为目录时返回 false。
// 每个文件都必须携带 Hub 的 sha256：ModelScope 不提供其他摘要格式，
// 因此缺少 sha256 的条目是本清单格式无法表示的清单。
func modelScopeManifestEntry(e modelScopeFileEntry) (ManifestEntry, bool, error) {
	switch e.Type {
	case modelScopeTypeTree:
		return ManifestEntry{}, false, nil
	case "blob":
	default:
		return ManifestEntry{}, false, sourceErrorf(ReasonInvalidManifest,
			"entry %q is of type %q, which is neither a blob nor a tree", e.Path, e.Type)
	}
	if len(e.Sha256) != 64 || strings.ContainsFunc(e.Sha256, func(r rune) bool {
		return (r < '0' || r > '9') && (r < 'a' || r > 'f')
	}) {
		return ManifestEntry{}, false, sourceErrorf(ReasonInvalidManifest,
			"entry %q names no usable Sha256, which every manifest line requires", e.Path)
	}

	return ManifestEntry{Path: e.Path, Size: e.Size, Digest: DigestSHA256 + ":" + e.Sha256}, true, nil
}

// listFiles 列出单个层级：当 recursive 为 true 时列出 root 下的整棵子树，
// 否则列出 root 的直接子项。无论其中包含什么内容，条目数达到上限的分页均视为截断。
func (m *ModelScope) listFiles(
	ctx context.Context, repository, commit, root string, recursive bool, token string,
) ([]modelScopeFileEntry, bool, error) {
	target := m.apiURL("models", repository, "repo", "files") + "?Revision=" + url.QueryEscape(commit)
	if root != "" {
		target += "&Root=" + url.QueryEscape(root)
	}
	if recursive {
		target += "&Recursive=true"
	}

	var answer struct {
		Files []modelScopeFileEntry `json:"Files"`
	}
	if err := m.getJSON(ctx, target, token, &answer); err != nil {
		return nil, false, err
	}

	return answer.Files, len(answer.Files) >= modelScopePageBound, nil
}

// Revalidate 检查 token 在指定 commit 下是否仍可读取该仓库：对仓库端点中首个文件
// 发起一次不跟随重定向的 HEAD 请求。
//
// 根据 Hub 实测表现：对可访问文件发起 HEAD 请求会返回 200——对 LFS 文件亦然
// （其 GET 请求会重定向至 CDN，因此返回重定向同样可确认权限）——而缺少 commit、路径或仓库，
// 以及未获授权的受限（gated）仓库均会返回不带信封结构的 404。
// 首个文件来自一次根目录列表请求，因此无论仓库规模多大，该检查仅需两次请求。
func (m *ModelScope) Revalidate(ctx context.Context, repository, commit, token string) error {
	first, err := m.firstRootFile(ctx, repository, commit, token)
	if err != nil {
		return err
	}

	target := m.fileURL(repository, commit, first)
	resp, err := m.doNoRedirect(ctx, http.MethodHead, target, token)
	if err != nil {
		return err
	}
	defer httpx.Close(resp)
	switch {
	case resp.StatusCode == http.StatusOK:
		return nil
	case resp.StatusCode >= 300 && resp.StatusCode < 400:
		// 重定向指向仅对可读仓库授予的 CDN 路径。
		return nil
	default:
		return classifyModelScope(resp.StatusCode, 0, resp.Request.URL.Path)
	}
}

// firstRootFile 确定用于重新校验发起 HEAD 请求的目标文件：从一次直接子项列表中
// 按路径选取根目录的第一个文件；若根目录不含文件，则回退到递归列表。
func (m *ModelScope) firstRootFile(ctx context.Context, repository, commit, token string) (string, error) {
	page, _, err := m.listFiles(ctx, repository, commit, "", false, token)
	if err != nil {
		return "", err
	}
	first := ""
	for _, e := range page {
		if e.Type != modelScopeTypeTree && (first == "" || e.Path < first) {
			first = e.Path
		}
	}
	if first != "" {
		return first, nil
	}

	page, _, err = m.listFiles(ctx, repository, commit, "", true, token)
	if err != nil {
		return "", err
	}
	for _, e := range page {
		if e.Type != modelScopeTypeTree && (first == "" || e.Path < first) {
			first = e.Path
		}
	}
	if first == "" {
		return "", sourceErrorf(ReasonEmptyManifest, "commit %s of %q holds no file to revalidate against", commit, repository)
	}

	return first, nil
}

// FileURL 返回仓库在指定 commit 下某个文件的下载地址。Hub 对小文件直接返回字节内容，
// 对 LFS 文件则重定向到支持字节范围请求的带签名内容寻址 CDN 路径。
func (m *ModelScope) FileURL(repository, commit, path string) string {
	return m.fileURL(repository, commit, path)
}

func (m *ModelScope) fileURL(repository, commit, path string) string {
	return m.apiURL("models", repository, "repo") +
		"?Revision=" + url.QueryEscape(commit) + "&FilePath=" + url.QueryEscape(path)
}

// ValidToken 报告 Hub 是否接受该 token：即 users/me 是否返回正常响应。Hub 会静默忽略
// 公开仓库上的无效 token，因此这是发现此类错误配置的唯一途径。
func (m *ModelScope) ValidToken(ctx context.Context, token string) (bool, error) {
	target := strings.TrimSuffix(m.Endpoint, "/") + "/openapi/v1/users/me"
	resp, err := m.doNoRedirect(ctx, http.MethodGet, target, token)
	if err != nil {
		return false, err
	}
	defer httpx.Close(resp)
	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusUnauthorized, http.StatusForbidden:
		return false, nil
	default:
		return false, sourceErrorf(ReasonSourceUnavailable,
			"the token check against %s: HTTP %d", target, resp.StatusCode)
	}
}

// gitURL 是仓库对应的 git 端点，即托管同一仓库 ID 的 Hub 主机地址。
func (m *ModelScope) gitURL(repository string) (string, error) {
	base, err := url.Parse(m.Endpoint)
	if err != nil {
		return "", sourceErrorf(ReasonSourceUnavailable, "the endpoint %q does not parse: %v", m.Endpoint, err)
	}
	base.Path = strings.TrimSuffix(base.Path, "/") + "/" + strings.TrimSuffix(escapeRepository(repository), "/") + ".git"
	base.RawQuery, base.Fragment = "", ""

	return base.String(), nil
}

// runLsRemote 为单个仓库执行 git 交叉校验。
func (m *ModelScope) runLsRemote(ctx context.Context, repository, token string) (map[string]string, error) {
	run := m.lsRemote
	if run == nil {
		run = m.lsRemoteOverHTTP
	}
	gitURL, err := m.gitURL(repository)
	if err != nil {
		return nil, err
	}
	refs, err := run(ctx, gitURL, token)
	if err != nil {
		return nil, sourceErrorf(ReasonSourceUnavailable, "the git cross-check of %s: %v", gitURL, err)
	}

	return refs, nil
}

// lsRemoteOverHTTP 通过 HTTP smart 协议向仓库的 git 端点查询 refs——与 `git ls-remote` 所请求的接口相同——
// 从而使交叉校验无需在镜像中内置 git 二进制文件，也无需启动子进程：使用的客户端由 Settings 配置，
// 私有仓库以 oauth2 作为用户名、token 作为密码进行认证，凭据置于 HTTP 请求头中，
// 而非其他进程可读取的形式。
func (m *ModelScope) lsRemoteOverHTTP(ctx context.Context, gitURL, token string) (map[string]string, error) {
	target := gitURL + "/info/refs?service=git-upload-pack"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, err
	}
	// 该端点仅响应 git 客户端并对其余请求返回 421（经实测），因此请求需标明自身所用的协议。
	req.Header.Set("User-Agent", "git/2.39")
	req.Header.Set("Accept", "application/x-git-upload-pack-advertisement")
	if token != "" {
		req.SetBasicAuth("oauth2", token)
	}

	resp, err := m.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", http.MethodGet, target, unwrapURLError(err))
	}
	defer httpx.Close(resp)
	switch {
	case resp.StatusCode == http.StatusUnauthorized, resp.StatusCode == http.StatusForbidden:
		return nil, sourceErrorf(ReasonAccessDenied,
			"%s: the repository does not exist or is not accessible with the credential (HTTP %d)",
			target, resp.StatusCode)
	case resp.StatusCode != http.StatusOK:
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/x-git-upload-pack-advertisement") {
		return nil, fmt.Errorf("the answer is %q, not a git advertisement", ct)
	}

	return parseGitAdvertisement(resp.Body)
}

// parseGitAdvertisement 读取 smart 协议通告：格式为 `<commit> <ref>` 的 pkt-line，
// 其中首行在 NUL 字节后携带服务端能力列表，附注标签的 commit 由带 `^{}` 后缀的解引用条目给出。
// 无法解析的数据包均视为错误——绝不允许被截断的响应缩减交叉校验范围。
func parseGitAdvertisement(r io.Reader) (map[string]string, error) {
	buf, err := io.ReadAll(io.LimitReader(r, modelScopeMaxResponseBytes+1))
	if err != nil {
		return nil, err
	}
	if len(buf) > modelScopeMaxResponseBytes {
		return nil, fmt.Errorf("the advertisement exceeds %d bytes", modelScopeMaxResponseBytes)
	}

	refs := make(map[string]string)
	for i := 0; i < len(buf); {
		if len(buf)-i < 4 {
			return nil, fmt.Errorf("the advertisement ends inside a packet length")
		}
		n, err := strconv.ParseInt(string(buf[i:i+4]), 16, 32)
		if err != nil {
			return nil, fmt.Errorf("unparsable packet length %q", string(buf[i:min(i+4, len(buf))]))
		}
		i += 4
		if n == 0 {
			continue // flush 数据包
		}
		if n < 4 {
			return nil, fmt.Errorf("unparsable packet length %q", string(buf[i-4:i]))
		}
		if i+int(n)-4 > len(buf) {
			return nil, fmt.Errorf("the advertisement ends inside a packet")
		}
		line := strings.TrimSuffix(string(buf[i:i+int(n)-4]), "\n")
		i += int(n) - 4
		if strings.HasPrefix(line, "# service=") {
			continue
		}
		if z := strings.IndexByte(line, 0); z >= 0 {
			line = line[:z] // 首个 ref 在 NUL 字节后携带服务端能力列表
		}
		commit, ref, ok := strings.Cut(line, " ")
		if !ok || !modelScopeCommitPattern.MatchString(commit) || ref == "" {
			return nil, fmt.Errorf("unparsable advertisement line %q", line)
		}
		refs[ref] = commit
	}
	if len(refs) == 0 {
		return nil, fmt.Errorf("the advertisement names no ref")
	}

	return refs, nil
}

// modelScopeMaxResponseBytes 限制单个响应体的大小。单条目的 commits 分页通常为几百字节；
// 包含 3000 个条目的 repo/files 分页通常为几百千字节。
const modelScopeMaxResponseBytes = 16 << 20

// modelScopeEnvelope 是所有响应（包括错误响应）的统一结构：真正区分 revision 不存在与权限被拒的
// 是响应体中的 Code 字段，而非 HTTP 状态码。
type modelScopeEnvelope struct {
	Code    int             `json:"Code"`
	Message string          `json:"Message"`
	Data    json.RawMessage `json:"Data"`
}

func (m *ModelScope) apiURL(segments ...string) string {
	escaped := make([]string, 0, len(segments)+1)
	escaped = append(escaped, strings.TrimSuffix(m.Endpoint, "/"), "api", "v1")
	for _, s := range segments {
		escaped = append(escaped, escapeRepository(s))
	}

	return strings.Join(escaped, "/")
}

func (m *ModelScope) getJSON(ctx context.Context, target, token string, into any) error {
	resp, err := m.do(ctx, http.MethodGet, target, token)
	if err != nil {
		return err
	}

	return decodeModelScope(resp, into)
}

// do 发送一次请求。传输层失败归类为 SourceUnavailable；状态码由调用方分类处理。
func (m *ModelScope) do(ctx context.Context, method, target, token string) (*http.Response, error) {
	return m.doRequest(ctx, method, target, token, false)
}

// doNoRedirect 发送一次不跟随重定向的请求：重定向本身即代表关于访问权限的响应，而非需要跟随的页面。
func (m *ModelScope) doNoRedirect(ctx context.Context, method, target, token string) (*http.Response, error) {
	return m.doRequest(ctx, method, target, token, true)
}

func (m *ModelScope) doRequest(ctx context.Context, method, target, token string, noRedirect bool) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, target, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "gpustack-operator")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	client := m.Client
	if noRedirect {
		c := *client
		c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		client = &c
	}

	resp, err := client.Do(req)
	if err != nil {
		// URL 是传输错误中唯一指明请求的部分，且不携带凭据；此处重建 error，避免请求的其他信息随之传出。
		return nil, sourceErrorf(ReasonSourceUnavailable, "%s %s: %v", method, target, unwrapURLError(err))
	}

	return resp, nil
}

// decodeModelScope 读取单个响应的信封结构。非 200 状态码，或 HTTP 状态为 200 但其自身 Code 不为 200 的
// 响应都会被归类为错误；仅当响应正常时才将其 Data 解码到目标对象中。
func decodeModelScope(resp *http.Response, into any) error {
	defer httpx.Close(resp)
	body, err := io.ReadAll(io.LimitReader(resp.Body, modelScopeMaxResponseBytes+1))
	if err != nil {
		return sourceErrorf(ReasonSourceUnavailable, "read %s: %v", resp.Request.URL.Path, err)
	}
	if len(body) > modelScopeMaxResponseBytes {
		return sourceErrorf(ReasonSourceUnavailable, "%s answered more than %d bytes", resp.Request.URL.Path,
			modelScopeMaxResponseBytes)
	}

	var env modelScopeEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		if resp.StatusCode != http.StatusOK {
			return classifyModelScope(resp.StatusCode, 0, resp.Request.URL.Path)
		}
		return sourceErrorf(ReasonSourceUnavailable, "%s answered an unreadable body: %v", resp.Request.URL.Path, err)
	}
	if resp.StatusCode != http.StatusOK || env.Code != http.StatusOK {
		return classifyModelScope(resp.StatusCode, env.Code, resp.Request.URL.Path)
	}
	if into == nil || len(env.Data) == 0 {
		return nil
	}
	if err := json.Unmarshal(env.Data, into); err != nil {
		return sourceErrorf(ReasonSourceUnavailable, "%s answered an unreadable body: %v", resp.Request.URL.Path, err)
	}

	return nil
}

// classifyModelScope 根据 ModelScope 的实测行为（PoC-D）将非成功响应映射为对应原因：所有访问失败
// 均返回 404，因此优先依据信封中的 Code 进行分类，并以 HTTP 状态码作为兜底。
//
//   - 10990101004 表示仓库在该修订版本下没有文件树：映射为 RevisionNotFound。
//   - 10010205001（未找到）和 10010200001（无权限——私有仓库未提供令牌或受限仓库未获授权）
//     均映射为 AccessDenied，其余所有 404、401 和 403 亦然：仓库中心对不存在的仓库与调用方
//     无权读取的仓库返回相同的响应，因此错误消息不对此作猜测。
//   - 其他所有情况（包括 5xx 和 429）均映射为 SourceUnavailable。
func classifyModelScope(status, code int, what string) error {
	switch {
	case code == 10990101004:
		return sourceErrorf(ReasonRevisionNotFound, "%s: the repository has no file tree at the revision", what)
	case code == 10010205001, code == 10010200001:
		return sourceErrorf(ReasonAccessDenied,
			"%s: the repository does not exist or is not accessible with the credential (HTTP %d, code %d)",
			what, status, code)
	case status == http.StatusUnauthorized, status == http.StatusForbidden, status == http.StatusNotFound:
		return sourceErrorf(ReasonAccessDenied,
			"%s: the repository does not exist or is not accessible with the credential (HTTP %d, code %d)",
			what, status, code)
	default:
		return sourceErrorf(ReasonSourceUnavailable, "%s: HTTP %d, code %d", what, status, code)
	}
}
