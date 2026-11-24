package peer

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gpustack.ai/gpustack/pkg/modelartifact"
	"gpustack.ai/gpustack/pkg/modelmanager/store"
	"gpustack.ai/gpustack/pkg/utils/json"
	"gpustack.ai/gpustack/pkg/utils/stringx"
)

// writableAgain 撤销 Publish 施加的只读权限封存，使测试临时目录在清理阶段可被删除。
func writableAgain(root string) func() {
	return func() {
		_ = filepath.WalkDir(root, func(p string, d os.DirEntry, _ error) error {
			if d.IsDir() {
				_ = os.Chmod(p, 0o755)
			} else {
				_ = os.Chmod(p, 0o644)
			}

			return nil
		})
	}
}

// publishTree 将指定文件集合发布为目录树并返回其十六进制摘要。
// 摘要由文件内容派生，同时像真实发布流程一样把带单文件摘要的清单落盘。
func publishTree(t *testing.T, s *store.Store, files map[string]string) string {
	t.Helper()
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	t.Cleanup(writableAgain(s.Root()))
	entries := make([]modelartifact.ManifestEntry, 0, len(files))
	for _, name := range names {
		body := files[name]
		entries = append(entries, modelartifact.ManifestEntry{
			Path: name, Size: int64(len(body)), Digest: "sha256:" + stringx.SumBySHA256(body),
		})
	}
	manifest, err := modelartifact.NewManifest(entries)
	require.NoError(t, err)
	treeHex := strings.TrimPrefix(manifest.Digest, "sha256:")
	a, err := s.NewAttempt(treeHex)
	require.NoError(t, err)
	for _, e := range manifest.Entries {
		dest, err := a.FilePath(e.Path)
		require.NoError(t, err)
		require.NoError(t, os.MkdirAll(filepath.Dir(dest), 0o755))
		require.NoError(t, os.WriteFile(dest, []byte(files[e.Path]), 0o644))
	}
	require.NoError(t, a.Publish(store.Marker{
		Digest: manifest.Digest, SizeBytes: manifest.SizeBytes, FileCount: manifest.FileCount,
	}))
	full := listing{Digest: manifest.Digest}
	for _, e := range manifest.Entries {
		full.Entries = append(full.Entries, ManifestFile{Path: e.Path, Size: e.Size, Digest: e.Digest})
	}
	stored, err := json.Marshal(full)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(s.PublishedManifestPath(treeHex), stored, 0o644))

	return treeHex
}

func serve(t *testing.T, s *Server) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)

	return ts
}

func TestServerServesPublishedTreesOnly(t *testing.T) {
	st, err := store.Open(t.TempDir())
	require.NoError(t, err)
	hex := publishTree(t, st, map[string]string{"config.json": "{}", "sub/weights.bin": "0123456789"})
	ts := serve(t, &Server{Store: st})

	t.Run("a listing answers the tree's files", func(t *testing.T) {
		resp, err := ts.Client().Get(ts.URL + PathPrefix + "trees/" + hex)
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		var l listing
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&l))
		assert.Equal(t, "sha256:"+hex, l.Digest)
		assert.Len(t, l.Entries, 2)
	})

	t.Run("a whole file answers its bytes", func(t *testing.T) {
		resp, err := ts.Client().Get(ts.URL + PathPrefix + "trees/" + hex + "/files/sub/weights.bin")
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, "0123456789", string(body))
	})

	t.Run("a range answers its slice", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, ts.URL+PathPrefix+"trees/"+hex+"/files/sub/weights.bin", nil)
		require.NoError(t, err)
		req.Header.Set("Range", "bytes=2-5")
		resp, err := ts.Client().Do(req)
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()
		require.Equal(t, http.StatusPartialContent, resp.StatusCode)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, "2345", string(body))
	})

	t.Run("a tree that is not published is not found", func(t *testing.T) {
		other := publishTree(t, st, map[string]string{"x": "x"})
		removed, err := st.RemoveUnreferenced(other)
		require.NoError(t, err)
		require.True(t, removed)
		resp, err := ts.Client().Get(ts.URL + PathPrefix + "trees/" + other)
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()
		assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	})

	t.Run("a path that leaves the tree is refused", func(t *testing.T) {
		resp, err := ts.Client().Get(ts.URL + PathPrefix + "trees/" + hex + "/files/../../marker")
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()
		// 路由多路复用器会在请求进入处理器之前拒绝含 .. 的路径；无论哪一层拦截，目录树之外的文件都不会被返回。
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})

	t.Run("a digest that is not hex is refused", func(t *testing.T) {
		resp, err := ts.Client().Get(ts.URL + PathPrefix + "trees/not-hex")
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})
}
