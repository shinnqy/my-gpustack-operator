package materialize

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/modelartifact"
	"gpustack.ai/gpustack/pkg/modelmanager/download"
	"gpustack.ai/gpustack/pkg/modelmanager/driver"
	"gpustack.ai/gpustack/pkg/modelmanager/peer"
	"gpustack.ai/gpustack/pkg/modelmanager/store"
	"gpustack.ai/gpustack/pkg/utils/json"
	"gpustack.ai/gpustack/pkg/utils/stringx"
)

const testCommit = "0123456789abcdef0123456789abcdef01234567"

// testHub 按路径前缀提供仓库文件并支持 Range 请求，同时记录每个请求的路径与凭据。
type testHub struct {
	server *httptest.Server

	mu       sync.Mutex
	files    map[string]map[string][]byte // 仓库 -> 路径 -> 内容
	requests []struct{ path, auth string }
}

func newTestHub(t *testing.T, files map[string]map[string][]byte) *testHub {
	t.Helper()
	h := &testHub{files: files}
	h.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.mu.Lock()
		h.requests = append(h.requests, struct{ path, auth string }{r.URL.Path, r.Header.Get("Authorization")})
		h.mu.Unlock()
		repo, path, ok := h.locate(r.URL.Path)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		body := h.files[repo][path]
		http.ServeContent(w, r, path, time.Time{}, bytes.NewReader(body))
	}))
	t.Cleanup(h.server.Close)
	return h
}

// locate 拆分 "/<repo owner>/<repo name>/resolve/<commit>/<path>"。
func (h *testHub) locate(urlPath string) (repo, path string, ok bool) {
	parts := strings.SplitN(strings.TrimPrefix(urlPath, "/"), "/", 5)
	if len(parts) != 5 || parts[2] != "resolve" || parts[3] != testCommit {
		return "", "", false
	}
	return parts[0] + "/" + parts[1], parts[4], true
}

func (h *testHub) manifest(repo string) modelartifact.Manifest {
	entries := make([]modelartifact.ManifestEntry, 0, len(h.files[repo]))
	for p, b := range h.files[repo] {
		entries = append(entries, modelartifact.ManifestEntry{Path: p, Size: int64(len(b)), Digest: "sha256:" + stringx.SumBytesBySHA256(b)})
	}
	m, err := modelartifact.NewManifest(entries)
	if err != nil {
		panic(err)
	}
	return m
}

func (h *testHub) ListManifest(_ context.Context, repo, commit, _ string, _ modelartifact.Filter) (modelartifact.Manifest, error) {
	if commit != testCommit {
		return modelartifact.Manifest{}, &modelartifact.SourceError{Reason: modelartifact.ReasonRevisionNotFound, Message: "no such commit"}
	}
	return h.manifest(repo), nil
}

func (h *testHub) FileURL(repo, commit, path string) string {
	return h.server.URL + "/" + repo + "/resolve/" + commit + "/" + path
}

func (h *testHub) recorded() []struct{ path, auth string } {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]struct{ path, auth string }(nil), h.requests...)
}

type testEnv struct {
	m     *Materializer
	hub   *testHub
	store *store.Store
}

func newTestEnv(t *testing.T, files map[string]map[string][]byte) *testEnv {
	t.Helper()
	root := t.TempDir()
	t.Cleanup(func() {
		_ = filepath.WalkDir(root, func(p string, e os.DirEntry, err error) error {
			if err == nil && e.IsDir() {
				_ = os.Chmod(p, 0o755)
			}
			return nil
		})
	})
	st, err := store.Open(root)
	require.NoError(t, err)
	hub := newTestHub(t, files)
	now := time.Date(2026, 9, 25, 6, 0, 0, 0, time.UTC)
	dl := download.New(hub.server.Client(), 8, 0)
	dl.RetryDelay = time.Millisecond
	return &testEnv{
		hub: hub, store: st,
		m: &Materializer{
			Store:         st,
			Environment:   func(context.Context, string) (Hub, *download.Downloader, error) { return hub, dl, nil },
			Now:           func() time.Time { return now },
			CheckInterval: 10 * time.Millisecond,
		},
	}
}

func (e *testEnv) artifact(repo, uid, digest string) *workercore.ModelArtifact {
	return &workercore.ModelArtifact{
		ObjectMeta: meta.ObjectMeta{UID: types.UID(uid)},
		Spec: workercore.ModelArtifactSpec{Source: workercore.ModelArtifactSource{
			HuggingFace: &workercore.ModelArtifactHubSource{Repository: repo},
		}},
		Status: workercore.ModelArtifactStatus{Resolved: &workercore.ModelArtifactResolved{Revision: testCommit, ManifestDigest: digest}},
	}
}

func (e *testEnv) request(repo, uid, token string) driver.Request {
	digest := e.hub.manifest(repo).Digest
	return driver.Request{Hex: store.HexOf(digest), Artifact: e.artifact(repo, uid, digest), Token: token}
}

// waitIdle 等待指定 hex 不再有正在运行的尝试。
func (e *testEnv) waitIdle(t *testing.T, hex string) {
	t.Helper()
	require.Eventually(t, func() bool { return !e.m.Downloading()[hex] }, 10*time.Second, 5*time.Millisecond)
}

func repoFiles() map[string]map[string][]byte {
	return map[string]map[string][]byte{
		"owner/repo": {"config.json": []byte(`{"a":1}`), "sub/model.safetensors": bytes.Repeat([]byte("w"), 3000)},
	}
}

func TestEnsureMaterializesAndPublishes(t *testing.T) {
	env := newTestEnv(t, repoFiles())
	req := env.request("owner/repo", "uid-a", "token-a")

	p := env.m.Ensure(context.Background(), req)
	assert.False(t, p.Published)
	assert.Equal(t, codes.Aborted, p.Code)
	assert.Contains(t, p.Message, "materializing sha256:"+req.Hex[:12])
	env.waitIdle(t, req.Hex)

	require.True(t, env.store.IsPublished(req.Hex))
	b, err := os.ReadFile(filepath.Join(env.store.PublishedTree(req.Hex), "sub", "model.safetensors"))
	require.NoError(t, err)
	assert.Equal(t, bytes.Repeat([]byte("w"), 3000), b)
	assert.NoDirExists(t, filepath.Join(env.store.PublishedTree(req.Hex), "state"), "resume state never travels with the tree")
	m, err := env.store.ReadMarker(req.Hex)
	require.NoError(t, err)
	assert.Equal(t, int64(2), m.FileCount)
	assert.Equal(t, modelartifact.ManifestHeader, m.ManifestFormat)
	assert.Equal(t, store.SourceHub, m.Source, "the tree records where its bytes came from")
	stored, err := os.ReadFile(env.store.PublishedManifestPath(req.Hex))
	require.NoError(t, err, "the stored manifest is what a peer's listing serves")
	var l peer.Listing
	require.NoError(t, json.Unmarshal(stored, &l))
	assert.Equal(t, "sha256:"+req.Hex, l.Digest)
	assert.NotEmpty(t, l.Entries)

	assert.True(t, env.m.Ensure(context.Background(), req).Published)
	for _, r := range env.hub.recorded() {
		assert.Equal(t, "Bearer token-a", r.auth)
	}
}
