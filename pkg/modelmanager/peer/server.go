package peer

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/pkg/modelmanager/store"
	certcache "gpustack.ai/gpustack/pkg/utils/certs/cache"
	"gpustack.ai/gpustack/pkg/utils/certs/fakecert"
	"gpustack.ai/gpustack/pkg/utils/gox"
	"gpustack.ai/gpustack/pkg/utils/httpx"
	"gpustack.ai/gpustack/pkg/utils/json"
	"gpustack.ai/gpustack/pkg/utils/osx"
)

// PathPrefix 是 peer 端点所在端口上的路由前缀。
const PathPrefix = "/peer/v1/"

// peer 服务端暴露的路由模板。
const (
	// TreePath 提供单个已发布目录树的清单列表。
	TreePath = PathPrefix + "trees/{hex}"
	// FilePath 按字节范围提供已发布目录树中的单个文件。
	FilePath = PathPrefix + "trees/{hex}/files/{path...}"
)

// Server 向其他节点的插件以只读方式提供本节点已发布的目录树，按文件支持字节范围读取，
// 并通过 MaxStreams 限制并发响应数量。
type Server struct {
	Store *store.Store
	// Admit 决定是否放行单个请求；为 nil 时放行所有请求。
	Admit func(*http.Request) error
	// MaxStreams 限制同时服务的文件响应数，超出时后续请求排队等待。
	MaxStreams int
	Now        func() time.Time

	streams chan struct{}
}

// ManifestFile 是已存储清单中的单个文件条目。
type ManifestFile struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	Digest string `json:"digest"`
}

// Listing 是已存储的清单结构：包含目录树摘要及其文件条目。
// 物化器在发布时将其落盘，peer 服务端通过列表接口返回该结构，
// 供拉取方重组规范清单并重新核对根摘要后再信任任何文件字节。
type Listing struct {
	Digest  string         `json:"digest"`
	Entries []ManifestFile `json:"entries"`
}

// listing 是清单接口响应类型的包内别名。
type listing = Listing

// Handler 返回 peer 服务的 HTTP 处理器。认证在处理器内执行而不单靠 TLS 层，
// 使被拒绝的凭据能够收到明确的 HTTP 响应而非直接握手失败。
func (s *Server) Handler() http.Handler {
	if s.MaxStreams > 0 {
		s.streams = make(chan struct{}, s.MaxStreams)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+TreePath, s.serveTree)
	mux.HandleFunc("GET "+FilePath, s.serveFile)

	return mux
}

// serveTree 返回单个已发布目录树旁存放的清单文件。早期未存储清单的目录树不作为列表来源。
func (s *Server) serveTree(w http.ResponseWriter, r *http.Request) {
	if err := s.admit(w, r); err != nil {
		return
	}
	hex, ok := HexOf("sha256:" + r.PathValue("hex"))
	if !ok {
		httpxRefused(w, http.StatusBadRequest, "not a manifest digest")

		return
	}
	m, err := s.Store.ReadMarker(hex)
	if err != nil {
		httpxRefused(w, http.StatusNotFound, "no such published tree")

		return
	}
	b, err := os.ReadFile(s.Store.PublishedManifestPath(hex))
	if err != nil {
		httpxRefused(w, http.StatusNotFound, "the tree carries no manifest")

		return
	}
	var l Listing
	if err := json.Unmarshal(b, &l); err != nil || l.Digest != m.Digest {
		klog.ErrorS(err, "a stored manifest does not parse or disagrees with its marker", "digest", m.Digest)
		httpxRefused(w, http.StatusInternalServerError, "the stored manifest is unreadable")

		return
	}
	httpx.PureJSON(w, http.StatusOK, l)
}

// serveFile 以整文件或字节范围方式响应已发布目录树中的单个文件。
func (s *Server) serveFile(w http.ResponseWriter, r *http.Request) {
	if err := s.admit(w, r); err != nil {
		return
	}
	hex, ok := HexOf("sha256:" + r.PathValue("hex"))
	if !ok {
		httpxRefused(w, http.StatusBadRequest, "not a manifest digest")

		return
	}
	name, err := treeFile(s.Store.PublishedTree(hex), r.PathValue("path"))
	if err != nil {
		httpxRefused(w, http.StatusNotFound, "no such file in the tree")

		return
	}
	f, err := os.Open(name)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			httpxRefused(w, http.StatusNotFound, "no such file in the tree")

			return
		}
		klog.ErrorS(err, "open a published file for a peer", "digest", hex)
		httpxRefused(w, http.StatusInternalServerError, "opening the file failed")

		return
	}
	defer func() { _ = f.Close() }()

	// 每个槽位同时只处理一个文件响应：停滞的读取方占住自身槽位，其余请求等待，以此实现服务端并发上限。
	if s.streams != nil {
		select {
		case s.streams <- struct{}{}:
			defer func() { <-s.streams }()
		case <-r.Context().Done():
			return
		}
	}
	now := s.now()
	http.ServeContent(w, r, path.Base(name), now, f)
}

// admit 执行准入认证，并在拒绝时写回错误响应。
func (s *Server) admit(w http.ResponseWriter, r *http.Request) error {
	if s.Admit == nil {
		return nil
	}
	if err := s.Admit(r); err != nil {
		var authErr *AuthError
		if errors.As(err, &authErr) {
			httpxRefused(w, http.StatusUnauthorized, authErr.Msg)

			return err
		}
		klog.ErrorS(err, "admit a peer request")
		httpxRefused(w, http.StatusInternalServerError, "admission failed")

		return err
	}

	return nil
}

// treeFile 将相对路径解析为目录树内的具体文件路径，拒绝任何越界访问。
func treeFile(root, name string) (string, error) {
	cleaned := path.Clean("/" + filepath.ToSlash(name))
	if cleaned == "/" {
		return "", fmt.Errorf("%q is not a file", name)
	}
	full := filepath.Join(root, filepath.FromSlash(cleaned[1:]))
	if full != root && !strings.HasPrefix(full, root+string(filepath.Separator)) {
		return "", fmt.Errorf("%q leaves the tree", name)
	}

	return full, nil
}

// now 返回处理器时钟，便于测试替换。
func (s *Server) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}

	return time.Now()
}

// Listen 打开 peer 服务端的 TLS 监听器：证书采用与插件安全端口一致的动态自签名证书。
func Listen(port int, auth AuthOptions) (net.Listener, error) {
	if port <= 0 || port > 65535 {
		return nil, fmt.Errorf("peer port %d is out of range", port)
	}
	certManager := &fakecert.DynamicManager{Cache: certcache.NewDirCache(osx.TempDir("peer-tls"))}
	tlsCfg, err := auth.ServerTLSConfig(certManager.GetCertificate)
	if err != nil {
		return nil, err
	}
	lis, err := tls.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port), tlsCfg)
	if err != nil {
		return nil, fmt.Errorf("listen on the peer port %d: %w", port, err)
	}

	return lis, nil
}

// Serve 在 lis 上持续服务 peer 请求，直至 ctx 取消并优雅关闭。
func (s *Server) Serve(ctx context.Context, lis net.Listener) error {
	srv := &http.Server{
		Handler:           s.Handler(),
		ReadHeaderTimeout: 30 * time.Second,
	}
	gp := gox.GroupWithContextIn(ctx)
	gp.Go(func(ctx context.Context) error {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		return srv.Shutdown(shutdownCtx)
	})
	gp.Go(func(_ context.Context) error {
		if err := srv.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}

		return nil
	})

	return gp.Wait()
}

// readFileLimited 读取可能被其他进程重写的小文件（例如投影令牌文件）。
func readFileLimited(name string) ([]byte, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	return io.ReadAll(io.LimitReader(f, 1<<20))
}

// httpxRefused 返回不包含任何租户信息的拒绝响应体。
func httpxRefused(w http.ResponseWriter, code int, msg string) {
	httpx.PureJSON(w, code, map[string]string{"error": msg})
}
