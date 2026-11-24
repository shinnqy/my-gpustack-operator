// Package store 管理节点磁盘上的模型缓存：包括目录布局、填充缓存的下载尝试、
// 已校验目录树的原子发布，以及记录引用和失败退避的节点本地账本。
//
// 缓存根目录布局：
//
//	layout                          目录布局版本；遇到其他版本的根目录将拒绝打开
//	published/<hex>/marker          已发布目录树的标记文件，在 rename 之前写入并落盘同步
//	published/<hex>/tree/...        只读模型文件；消费方实际挂载的源目录
//	partial/<hex>/<attempt>/        单次尝试的标记与目录树，完成后整体 rename 进 published/
//	trash/                          正在删除的已发布目录树，先从 published/ 中 rename 移出
//	ledger/refs/<volume>.json       记录哪个挂载目标挂载了哪个摘要以及所属 Pod
//	ledger/digests/<hex>.json       按摘要记录最近尝试编号、最近使用时间和失败退避信息
//
// 消费方只会从 published/ 挂载，且目录树仅在所有字节校验完毕且 marker 同步落盘后通过一次 rename 进入
// published/，因此消费方绝不会看到未完成的半成品目录树。<hex> 为清单摘要的十六进制部分：它是节点计算出的
// 内容地址，而非租户任意指定的名称。
package store

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"gpustack.ai/gpustack/pkg/utils/json"
)

// Layout 是 layout 文件的内容。后续布局演进会变更该值，从而避免一种布局写入的目录被误读为另一种布局。
const Layout = "gpustack-model-store v1"

const (
	layoutFile   = "layout"
	markerFile   = "marker"
	treeDir      = "tree"
	publishedDir = "published"
	partialDir   = "partial"
	trashDir     = "trash"
	ledgerDir    = "ledger"
	refsDir      = "refs"
	digestsDir   = "digests"
)

// 已发布内容的权限模式：任何受限 Pod 运行用户均可读取，但任何用户均不可写入。
const (
	publishedFileMode fs.FileMode = 0o444
	publishedDirMode  fs.FileMode = 0o555
	workingFileMode   fs.FileMode = 0o644
	workingDirMode    fs.FileMode = 0o755
)

var hexDigestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// ErrLayout 表示缓存根目录中存在不兼容的布局版本。
var ErrLayout = errors.New("the cache root holds another layout")

// HexOf 返回 "sha256:<hex>" 清单摘要中的十六进制部分；格式不符时返回 ""。
func HexOf(digest string) string {
	h, ok := strings.CutPrefix(digest, "sha256:")
	if !ok || !hexDigestPattern.MatchString(h) {
		return ""
	}

	return h
}

// Store 表示单个节点的缓存根目录，并且是该目录唯一的写入方。
type Store struct {
	root string
	// claimMu 协调挂载引用登记与目录树删除的顺序：Claim 和 RemoveUnreferenced 均持有该锁，
	// 确保目录树不会在确认已发布之后、写入挂载引用之前被删除。
	claimMu sync.Mutex
	// digestMu 串行化对摘要记录的更新。
	digestMu sync.Mutex
}

// Open 初始化 root 并返回对应的 Store。空目录或新目录会写入当前布局版本；若根目录持有其他布局版本，
// 则直接拒绝而不改动原内容：布局检查在创建任何子目录前执行。
func Open(root string) (*Store, error) {
	path := filepath.Join(root, layoutFile)
	b, err := os.ReadFile(path)
	fresh := errors.Is(err, fs.ErrNotExist)
	switch {
	case fresh:
	case err != nil:
		return nil, err
	case strings.TrimSpace(string(b)) != Layout:
		return nil, fmt.Errorf("%w: %q, want %q", ErrLayout, strings.TrimSpace(string(b)), Layout)
	}

	for _, d := range []string{
		publishedDir, partialDir, trashDir, filepath.Join(ledgerDir, refsDir),
		filepath.Join(ledgerDir, digestsDir),
	} {
		if err := os.MkdirAll(filepath.Join(root, d), workingDirMode); err != nil {
			return nil, err
		}
	}
	if fresh {
		if err := writeFileSync(path, []byte(Layout+"\n")); err != nil {
			return nil, err
		}
	}

	return &Store{root: root}, nil
}

// Root 返回缓存根目录路径。
func (s *Store) Root() string { return s.root }

// PublishedTree 返回 hex 对应的供消费方挂载的目录路径。
func (s *Store) PublishedTree(hex string) string {
	return filepath.Join(s.root, publishedDir, hex, treeDir)
}

// PublishedManifestPath 返回已发布目录树旁存放清单文件的路径，与 marker 同级：
// peer 列表接口用该文件作答，由创建该目录树的发布过程写入。早期尚未存储清单时发布的目录树没有该文件。
func (s *Store) PublishedManifestPath(hex string) string {
	return filepath.Join(s.root, publishedDir, hex, "manifest.json")
}

// Marker 是已发布目录树的元数据记录。
type Marker struct {
	// Digest 是目录树校验所依据的清单摘要。
	Digest string `json:"digest"`
	// Attempt 是发布该目录树的尝试编号。
	Attempt int64 `json:"attempt"`
	// ManifestFormat 是计算摘要所使用的清单格式首行。
	ManifestFormat string `json:"manifestFormat"`
	// SizeBytes 和 FileCount 分别为目录树的总字节数和文件数。
	SizeBytes int64 `json:"sizeBytes"`
	FileCount int64 `json:"fileCount"`
	// PublishedTime 是发布时间。
	PublishedTime time.Time `json:"publishedTime"`
	// Source 记录目录树字节数据的来源，如 SourceHub；早期尚未记录来源时发布的目录树该字段为空。
	Source string `json:"source,omitempty"`
}

// SourceHub 表示从模型仓库 Hub 下载的目录树。
const SourceHub = "Hub"

// IsPublished 报告 hex 是否已有已发布的目录树。该函数仅检查 marker 文件是否存在，
// 因此无论模型树多大，挂载时的检查都只需一次 stat。
func (s *Store) IsPublished(hex string) bool {
	_, err := os.Stat(filepath.Join(s.root, publishedDir, hex, markerFile))
	return err == nil
}

// ReadMarker 读取 hex 对应的 marker 记录。
func (s *Store) ReadMarker(hex string) (Marker, error) {
	var m Marker
	b, err := os.ReadFile(filepath.Join(s.root, publishedDir, hex, markerFile))
	if err != nil {
		return m, err
	}
	err = json.Unmarshal(b, &m)

	return m, err
}

// Published 列出所有已发布目录树的 marker。缺少 marker 的目录不视为已发布目录树，不会被列出。
func (s *Store) Published() ([]Marker, error) {
	entries, err := os.ReadDir(filepath.Join(s.root, publishedDir))
	if err != nil {
		return nil, err
	}
	markers := make([]Marker, 0, len(entries))
	for _, e := range entries {
		if !hexDigestPattern.MatchString(e.Name()) {
			continue
		}
		m, err := s.ReadMarker(e.Name())
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("published tree %s: %w", e.Name(), err)
		}
		markers = append(markers, m)
	}

	return markers, nil
}

// trashPublished 将 hex 的已发布目录树重命名移入 trash/ 并返回新路径；若不存在已发布目录树则返回 ""。
func (s *Store) trashPublished(hex string) (string, error) {
	src := filepath.Join(s.root, publishedDir, hex)
	dst := filepath.Join(s.root, trashDir, hex+"-"+strconv.FormatInt(time.Now().UnixNano(), 10))
	if err := os.Rename(src, dst); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return "", nil
		}
		return "", err
	}

	return dst, syncDir(filepath.Join(s.root, publishedDir))
}

// RemoveUnreferenced 在没有任何引用指向 hex 时移除其已发布目录树，并返回是否实际移除了它。
// 该方法在与 Claim 写入引用相同的锁保护下读取引用，因此刚在上一刻声明引用的挂载会保住该目录树；
// 若引用无法读取同样会保留它。只有移出 published/ 的 rename 操作处于锁内：移出后新挂载已看不到该目录树，
// 实际文件删除在释放锁后执行，避免删除大型模型目录时阻塞节点上的所有挂载请求。
func (s *Store) RemoveUnreferenced(hex string) (bool, error) {
	trash, err := s.trashUnreferenced(hex)
	if err != nil || trash == "" {
		return false, err
	}
	if err := removeAllWritable(trash); err != nil {
		return false, err
	}

	return true, nil
}

func (s *Store) trashUnreferenced(hex string) (string, error) {
	s.claimMu.Lock()
	defer s.claimMu.Unlock()

	refs, err := s.Refs()
	if err != nil {
		return "", err
	}
	for _, r := range refs {
		if r.Hex == hex {
			return "", nil
		}
	}
	if !s.IsPublished(hex) {
		return "", nil
	}

	return s.trashPublished(hex)
}

// EmptyTrash 清理上次被中断的删除操作遗留在 trash/ 中的内容。
func (s *Store) EmptyTrash() error {
	entries, err := os.ReadDir(filepath.Join(s.root, trashDir))
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := removeAllWritable(filepath.Join(s.root, trashDir, e.Name())); err != nil {
			return err
		}
	}

	return nil
}

// Partial 表示磁盘上遗留的单次下载尝试目录。
type Partial struct {
	Hex     string
	Attempt int64
	// SizeBytes 是该尝试目录下所有文件占用的字节总数。
	SizeBytes int64
	// ModTime 是目录中最近一次文件修改时间。
	ModTime time.Time
}

// Partials 列出磁盘上所有尝试目录。
func (s *Store) Partials() ([]Partial, error) {
	digests, err := os.ReadDir(filepath.Join(s.root, partialDir))
	if err != nil {
		return nil, err
	}
	var partials []Partial
	for _, d := range digests {
		// 遍历期间正在发布或启动的尝试可能把目录重命名移走，因此跳过中途消失的目录，
		// 避免单次并发移动导致列表读取整体失败。
		attempts, err := os.ReadDir(filepath.Join(s.root, partialDir, d.Name()))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, a := range attempts {
			n, err := strconv.ParseInt(a.Name(), 10, 64)
			if err != nil || !hexDigestPattern.MatchString(d.Name()) {
				continue
			}
			p := Partial{Hex: d.Name(), Attempt: n}
			walkErr := filepath.WalkDir(filepath.Join(s.root, partialDir, d.Name(), a.Name()),
				func(_ string, e fs.DirEntry, err error) error {
					if errors.Is(err, fs.ErrNotExist) {
						return nil
					}
					if err != nil {
						return err
					}
					info, err := e.Info()
					if errors.Is(err, fs.ErrNotExist) {
						return nil
					}
					if err != nil {
						return err
					}
					if !e.IsDir() {
						p.SizeBytes += info.Size()
					}
					if info.ModTime().After(p.ModTime) {
						p.ModTime = info.ModTime()
					}
					return nil
				})
			if walkErr != nil {
				return nil, walkErr
			}
			partials = append(partials, p)
		}
	}

	return partials, nil
}

// RemovePartial 删除指定尝试目录，并在该摘要的父目录变空时一并删除它。
func (s *Store) RemovePartial(hex string, attempt int64) error {
	if err := os.RemoveAll(s.attemptDir(hex, attempt)); err != nil {
		return err
	}
	// 目录非空时 os.Remove 会失败，这正是期望行为：说明其中还有其他尝试目录。
	_ = os.Remove(filepath.Join(s.root, partialDir, hex))

	return nil
}

func (s *Store) attemptDir(hex string, attempt int64) string {
	return filepath.Join(s.root, partialDir, hex, strconv.FormatInt(attempt, 10))
}

// Attempt 表示对某摘要的一次物化尝试：拥有独立目录并由节点内永不复用的递增编号隔离，
// 避免被取代的旧尝试延迟写入并污染另一次尝试待发布的目录树。
type Attempt struct {
	store  *Store
	Hex    string
	Number int64
}

// NewAttempt 为 hex 开启下一次尝试。若磁盘上留有前一次尝试的文件，会将最近一次尝试直接 rename 到新目录，
// 以便下载断点续传而非重新开始；该操作只重命名不拷贝，且由调用方确保当前没有同摘要的旧尝试在运行。
func (s *Store) NewAttempt(hex string) (*Attempt, error) {
	if !hexDigestPattern.MatchString(hex) {
		return nil, fmt.Errorf("digest %q is not 64 hexadecimal digits", hex)
	}
	previous, err := s.latestPartial(hex)
	if err != nil {
		return nil, err
	}
	var number int64
	if err := s.UpdateDigest(hex, func(rec *DigestRecord) {
		number = max(rec.LastAttempt, previous) + 1
		rec.LastAttempt = number
	}); err != nil {
		return nil, err
	}

	a := &Attempt{store: s, Hex: hex, Number: number}
	dir := s.attemptDir(hex, number)
	if previous > 0 {
		if err := os.Rename(s.attemptDir(hex, previous), dir); err != nil {
			return nil, err
		}
		// 移除被中断的发布留下的 marker：文件内容必须先再次校验。
		if err := os.Remove(filepath.Join(dir, markerFile)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return nil, err
		}
		// 将被封存为只读的文件恢复可写权限，以便续传重新打开写入。
		if err := unseal(filepath.Join(dir, treeDir)); err != nil {
			return nil, err
		}
	}
	if err := os.MkdirAll(filepath.Join(dir, treeDir), workingDirMode); err != nil {
		return nil, err
	}

	return a, nil
}

func (s *Store) latestPartial(hex string) (int64, error) {
	entries, err := os.ReadDir(filepath.Join(s.root, partialDir, hex))
	if errors.Is(err, fs.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var latest int64
	for _, e := range entries {
		if n, err := strconv.ParseInt(e.Name(), 10, 64); err == nil && n > latest {
			latest = n
		}
	}

	return latest, nil
}

// Dir 返回本次尝试的工作目录路径。
func (a *Attempt) Dir() string { return a.store.attemptDir(a.Hex, a.Number) }

// FilePath 返回清单路径在本次尝试中的落盘路径。清单格式本身已约定路径为相对路径且不含空段、"." 或 ".."，
// 此处再次校验，拒绝任何可能逃逸尝试目录树的路径，而非盲目拼接。
func (a *Attempt) FilePath(manifestPath string) (string, error) {
	tree := filepath.Join(a.Dir(), treeDir)
	p := filepath.Join(tree, filepath.FromSlash(manifestPath))
	rel, err := filepath.Rel(tree, p)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) ||
		filepath.ToSlash(rel) != manifestPath {
		return "", fmt.Errorf("manifest path %q escapes the tree", manifestPath)
	}

	return p, nil
}

// Publish 封存本次尝试并将其发布为 hex 的正式目录树：先将所有文件设为只读并 fsync，
// 按目录由深到浅封存并 sync，再写入并 sync marker，最后通过一次 rename 移入 published/ 并 sync 父目录。
// 仅允许已校验的内容到达此处；该函数不重读校验字节。
func (a *Attempt) Publish(m Marker) error {
	dir := a.Dir()
	tree := filepath.Join(dir, treeDir)
	var dirs []string
	err := filepath.WalkDir(tree, func(path string, e fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		switch {
		case e.IsDir():
			dirs = append(dirs, path)
			return nil
		case !e.Type().IsRegular():
			return fmt.Errorf("%s is not a regular file", path)
		}
		f, err := os.OpenFile(path, os.O_RDONLY, 0)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		// 在 sync 之前封存只读权限，确保权限变更与文件数据同样持久化落盘。
		if err := f.Chmod(publishedFileMode); err != nil {
			return err
		}
		return f.Sync()
	})
	if err != nil {
		return err
	}
	// 从最深层子目录开始逆序处理，确保每个目录都在其内部内容全部落盘后才被设为只读。
	for _, d := range slices.Backward(dirs) {
		if err := os.Chmod(d, publishedDirMode); err != nil {
			return err
		}
		if err := syncDir(d); err != nil {
			return err
		}
	}

	m.Attempt = a.Number
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if err := writeFileSync(filepath.Join(dir, markerFile), b); err != nil {
		return err
	}
	if err := syncDir(dir); err != nil {
		return err
	}

	dst := filepath.Join(a.store.root, publishedDir, a.Hex)
	if err := os.Rename(dir, dst); err != nil {
		return fmt.Errorf("publish %s: %w", a.Hex, err)
	}
	if err := syncDir(filepath.Join(a.store.root, publishedDir)); err != nil {
		return err
	}
	_ = os.Remove(filepath.Join(a.store.root, partialDir, a.Hex))

	return nil
}

func writeFileSync(path string, b []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, workingFileMode)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}

	return syncDir(filepath.Dir(path))
}

func syncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()

	return d.Sync()
}

// unseal 将 Publish 封存为只读的目录树恢复为工作权限模式。插件以 root 运行时本身仍可写入只读文件，
// 解除封存可确保非 root 调用方在续传尝试时也能正常工作。
func unseal(path string) error {
	err := filepath.WalkDir(path, func(p string, e fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		mode := workingFileMode
		if e.IsDir() {
			mode = workingDirMode
		}
		return os.Chmod(p, mode)
	})
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}

	return err
}

// removeAllWritable 删除包含只读目录的目录树。插件以 root 运行时可直接删除，
// 先恢复目录可写权限可确保非 root 调用方（包括单元测试）也能正常清理。
func removeAllWritable(path string) error {
	_ = filepath.WalkDir(path, func(p string, e fs.DirEntry, err error) error {
		if err == nil && e.IsDir() {
			_ = os.Chmod(p, workingDirMode)
		}
		return nil
	})

	return os.RemoveAll(path)
}
