package modelartifact

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gpustack.ai/gpustack/pkg/utils/json"
)

const (
	testCommit = "0123456789abcdef0123456789abcdef01234567"
	testToken  = "test-token-value-never-in-errors"
)

// fakeHub 用按 HTTP 方法和路径登记的处理函数提供各用例的 Hub 响应。
type fakeHub struct {
	server *httptest.Server
	routes map[string]http.HandlerFunc
	// auth 按路径记录请求带来的 Authorization 头。
	auth map[string]string
	hits atomic.Int64
}

func newFakeHub(t *testing.T, routes map[string]http.HandlerFunc) *fakeHub {
	t.Helper()
	h := &fakeHub{routes: routes, auth: map[string]string{}}
	h.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.hits.Add(1)
		h.auth[r.URL.EscapedPath()] = r.Header.Get("Authorization")
		route, ok := h.routes[r.Method+" "+r.URL.EscapedPath()]
		if !ok {
			w.Header().Set("X-Error-Code", "EntryNotFound")
			w.WriteHeader(http.StatusNotFound)
			return
		}
		route(w, r)
	}))
	t.Cleanup(h.server.Close)

	return h
}

func (h *fakeHub) client() *HuggingFace {
	return &HuggingFace{Endpoint: h.server.URL, Client: h.server.Client()}
}

func jsonAnswer(v any) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(v)
	}
}

func treeFile(path string, size int64, oid string) map[string]any {
	return map[string]any{"type": "file", "path": path, "size": size, "oid": oid}
}

func treeLFSFile(path string, size int64, sha string) map[string]any {
	e := treeFile(path, size, strings.Repeat("9", 40))
	e["lfs"] = map[string]any{"oid": sha, "size": size, "pointerSize": 134}
	return e
}

var (
	testConfigFile = treeFile("config.json", 659, strings.Repeat("b", 40))
	testWeightFile = treeLFSFile("model.safetensors", 100, strings.Repeat("a", 64))
	testTokenFile  = treeFile("sub/tokenizer.json", 7, strings.Repeat("c", 40))
	testSubDir     = map[string]any{"type": "directory", "path": "sub", "oid": strings.Repeat("d", 40), "size": 0}
)

const (
	revisionPath = "/api/models/owner/repo/revision/"
	treePath     = "/api/models/owner/repo/tree/" + testCommit
)

func revisionRoute(revision string) string {
	return "GET " + revisionPath + revision
}

func TestHuggingFaceResolve(t *testing.T) {
	want, err := NewManifest([]ManifestEntry{
		{Path: "config.json", Size: 659, Digest: DigestGitSHA1 + ":" + strings.Repeat("b", 40)},
		{Path: "model.safetensors", Size: 100, Digest: DigestSHA256 + ":" + strings.Repeat("a", 64)},
		{Path: "sub/tokenizer.json", Size: 7, Digest: DigestGitSHA1 + ":" + strings.Repeat("c", 40)},
	})
	require.NoError(t, err)

	cases := []struct {
		name     string
		revision string
		routes   func(hub *fakeHub) map[string]http.HandlerFunc
	}{
		{
			name:     "a branch over one page",
			revision: "main",
			routes: func(*fakeHub) map[string]http.HandlerFunc {
				return map[string]http.HandlerFunc{
					revisionRoute("main"): jsonAnswer(map[string]string{"sha": testCommit}),
					"GET " + treePath:     jsonAnswer([]any{testConfigFile, testWeightFile, testSubDir, testTokenFile}),
				}
			},
		},
		{
			name:     "a revision holding a slash is one path segment",
			revision: "refs/pr/1",
			routes: func(*fakeHub) map[string]http.HandlerFunc {
				return map[string]http.HandlerFunc{
					revisionRoute("refs%2Fpr%2F1"): jsonAnswer(map[string]string{"sha": testCommit}),
					"GET " + treePath:              jsonAnswer([]any{testConfigFile, testWeightFile, testTokenFile}),
				}
			},
		},
		{
			name:     "every page the Link header names, in any order",
			revision: "main",
			routes: func(hub *fakeHub) map[string]http.HandlerFunc {
				return map[string]http.HandlerFunc{
					revisionRoute("main"): jsonAnswer(map[string]string{"sha": testCommit}),
					"GET " + treePath: func(w http.ResponseWriter, r *http.Request) {
						switch r.URL.Query().Get("cursor") {
						case "":
							w.Header().Set("Link", fmt.Sprintf(`<%s%s?recursive=true&cursor=p2>; rel="next"`, hub.server.URL, treePath))
							_ = json.NewEncoder(w).Encode([]any{testTokenFile, testSubDir})
						case "p2":
							w.Header().Set("Link", `<`+treePath+`?recursive=true&cursor=p3>; rel="next"`)
							_ = json.NewEncoder(w).Encode([]any{testWeightFile})
						default:
							_ = json.NewEncoder(w).Encode([]any{testConfigFile})
						}
					},
				}
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			hub := newFakeHub(t, nil)
			hub.routes = c.routes(hub)

			got, err := hub.client().Resolve(context.Background(), "owner/repo", c.revision, testToken, Filter{})
			require.NoError(t, err)
			assert.Equal(t, testCommit, got.Commit)
			assert.Equal(t, want, got.Manifest)
			for path, auth := range hub.auth {
				assert.Equal(t, "Bearer "+testToken, auth, path)
			}
		})
	}
}

func TestHuggingFaceResolveFiltered(t *testing.T) {
	cases := []struct {
		name       string
		filter     Filter
		wantPaths  int64
		wantReason string
	}{
		{name: "no pattern keeps every file", wantPaths: 3},
		{name: "an allow pattern keeps its files", filter: Filter{Allow: []string{"*.json"}}, wantPaths: 2},
		{name: "a directory ignore pattern drops the directory", filter: Filter{Ignore: []string{"sub/"}}, wantPaths: 2},
		{
			name:       "a filter keeping nothing is an empty manifest",
			filter:     Filter{Allow: []string{"*.gguf"}},
			wantReason: ReasonEmptyManifest,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			hub := newFakeHub(t, map[string]http.HandlerFunc{
				revisionRoute("main"): jsonAnswer(map[string]string{"sha": testCommit}),
				"GET " + treePath:     jsonAnswer([]any{testConfigFile, testWeightFile, testSubDir, testTokenFile}),
			})

			got, err := hub.client().Resolve(context.Background(), "owner/repo", "main", testToken, c.filter)
			if c.wantReason != "" {
				require.Error(t, err)
				assert.Equal(t, c.wantReason, ReasonOf(err))
				assert.NotContains(t, err.Error(), testToken)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, c.wantPaths, got.Manifest.FileCount)

			listed, err := hub.client().ListManifest(context.Background(), "owner/repo", testCommit, testToken, c.filter)
			require.NoError(t, err)
			assert.Equal(t, got.Manifest, listed, "a node's recomputation must equal the resolution")
		})
	}
}
