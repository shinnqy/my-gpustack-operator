package modelartifact

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gpustack.ai/gpustack/pkg/utils/json"
)

const modelScopeCommit = "186d8559ad54c32cf47dc3a8225f993742c507b8"

// newFakeModelScope 让 ModelScope 客户端访问测试 HTTP 服务。用例提供明确的远端引用和读取错误，
// 以控制版本交叉核对的结果。
func newFakeModelScope(t *testing.T, routes map[string]http.HandlerFunc, refs map[string]string, gitErr error) *ModelScope {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		route, ok := routes[r.Method+" "+r.URL.EscapedPath()]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"Code":10010205001,"Message":"not found"}`))
			return
		}
		route(w, r)
	}))
	t.Cleanup(server.Close)

	m := &ModelScope{Endpoint: server.URL, Client: server.Client()}
	m.lsRemote = func(_ context.Context, _, _ string) (map[string]string, error) {
		if gitErr != nil {
			return nil, gitErr
		}
		return refs, nil
	}

	return m
}

// modelScopeCommitsRoute 构造与源站一致的 commits 响应，Data.Commit 包含查询到的提交，
// 找不到引用时返回空列表。
func modelScopeCommitsRoute(ids ...string) http.HandlerFunc {
	type commit struct {
		Id string `json:"Id"`
	}
	commits := make([]commit, 0, len(ids))
	for _, id := range ids {
		commits = append(commits, commit{Id: id})
	}
	return jsonAnswer(map[string]any{
		"Code":    200,
		"Message": "success",
		"Data":    map[string]any{"Commit": commits, "TotalCount": len(commits)},
	})
}

func modelScopeCommitsPath(repository string) string {
	return "/api/v1/models/" + repository + "/commits"
}

func TestModelScopeResolveRevision(t *testing.T) {
	const branchCommit = "13448952f850140b43c5e14d0e19f7e7f8cb3c47"

	t.Run("a commit is taken as is, without asking the hub", func(t *testing.T) {
		m := newFakeModelScope(t, nil, nil, nil)
		got, err := m.resolveRevision(context.Background(), "qwen/Qwen2.5-0.5B-Instruct", modelScopeCommit, "")
		require.NoError(t, err)
		assert.Equal(t, modelScopeCommit, got)
	})

	t.Run("a branch is resolved and cross-checked", func(t *testing.T) {
		m := newFakeModelScope(t,
			map[string]http.HandlerFunc{
				"GET " + modelScopeCommitsPath("qwen/Qwen2.5-0.5B-Instruct"): modelScopeCommitsRoute(branchCommit),
			},
			map[string]string{
				"HEAD":              branchCommit,
				"refs/heads/master": branchCommit,
			}, nil)
		got, err := m.resolveRevision(context.Background(), "qwen/Qwen2.5-0.5B-Instruct", "master", "")
		require.NoError(t, err)
		assert.Equal(t, branchCommit, got)
	})

	t.Run("a disagreement between the index and git refuses the resolution", func(t *testing.T) {
		other := strings.Repeat("f", 40)
		m := newFakeModelScope(t,
			map[string]http.HandlerFunc{
				"GET " + modelScopeCommitsPath("qwen/Qwen2.5-0.5B-Instruct"): modelScopeCommitsRoute(branchCommit),
			},
			map[string]string{"refs/heads/master": other}, nil)
		_, err := m.resolveRevision(context.Background(), "qwen/Qwen2.5-0.5B-Instruct", "master", "")
		require.Error(t, err)
		se := &SourceError{}
		require.True(t, errors.As(err, &se))
		assert.Equal(t, ReasonSourceUnavailable, se.Reason)
		assert.Contains(t, se.Message, "disagree")
		assert.Contains(t, se.Message, branchCommit)
		assert.Contains(t, se.Message, other)
	})

	t.Run("an annotated tag is cross-checked against the peeled entry", func(t *testing.T) {
		tagObject := strings.Repeat("b", 40)
		m := newFakeModelScope(t,
			map[string]http.HandlerFunc{
				"GET " + modelScopeCommitsPath("qwen/Qwen2.5-0.5B-Instruct"): modelScopeCommitsRoute(modelScopeCommit),
			},
			map[string]string{
				"refs/tags/v1.0":    tagObject,
				"refs/tags/v1.0^{}": modelScopeCommit,
			}, nil)
		got, err := m.resolveRevision(context.Background(), "qwen/Qwen2.5-0.5B-Instruct", "v1.0", "")
		require.NoError(t, err)
		assert.Equal(t, modelScopeCommit, got)
	})

	t.Run("a tag whose index names the tag object rather than the commit disagrees", func(t *testing.T) {
		tagObject := strings.Repeat("b", 40)
		m := newFakeModelScope(t,
			map[string]http.HandlerFunc{
				"GET " + modelScopeCommitsPath("qwen/Qwen2.5-0.5B-Instruct"): modelScopeCommitsRoute(tagObject),
			},
			map[string]string{
				"refs/tags/v1.0":    tagObject,
				"refs/tags/v1.0^{}": modelScopeCommit,
			}, nil)
		_, err := m.resolveRevision(context.Background(), "qwen/Qwen2.5-0.5B-Instruct", "v1.0", "")
		require.Error(t, err)
		assert.Equal(t, ReasonSourceUnavailable, ReasonOf(err))
	})

	t.Run("a branch git does not name refuses", func(t *testing.T) {
		m := newFakeModelScope(t,
			map[string]http.HandlerFunc{
				"GET " + modelScopeCommitsPath("qwen/Qwen2.5-0.5B-Instruct"): modelScopeCommitsRoute(branchCommit),
			},
			map[string]string{"refs/heads/other": branchCommit}, nil)
		_, err := m.resolveRevision(context.Background(), "qwen/Qwen2.5-0.5B-Instruct", "master", "")
		require.Error(t, err)
		assert.Equal(t, ReasonSourceUnavailable, ReasonOf(err))
		assert.Contains(t, ReasonOf(err), ReasonSourceUnavailable)
	})

	t.Run("git failing is SourceUnavailable, never a pass-through", func(t *testing.T) {
		m := newFakeModelScope(t,
			map[string]http.HandlerFunc{
				"GET " + modelScopeCommitsPath("qwen/Qwen2.5-0.5B-Instruct"): modelScopeCommitsRoute(branchCommit),
			},
			nil, errors.New("git: exit status 128"))
		_, err := m.resolveRevision(context.Background(), "qwen/Qwen2.5-0.5B-Instruct", "master", "")
		require.Error(t, err)
		assert.Equal(t, ReasonSourceUnavailable, ReasonOf(err))
		assert.Contains(t, ReasonOf(err), ReasonSourceUnavailable)
	})

	t.Run("a misspelled branch whose index silently answers master is caught", func(t *testing.T) {
		// 源站索引可能把拼错的分支返回为 master。远端没有同名的 masterr 分支时，不能用 HEAD 来确认索引返回的提交。
		m := newFakeModelScope(t,
			map[string]http.HandlerFunc{
				"GET " + modelScopeCommitsPath("qwen/Qwen2.5-0.5B-Instruct"): modelScopeCommitsRoute("13448952f850140b43c5e14d0e19f7e7f8cb3c47"),
			},
			map[string]string{"HEAD": "13448952f850140b43c5e14d0e19f7e7f8cb3c47", "refs/heads/master": "13448952f850140b43c5e14d0e19f7e7f8cb3c47"}, nil)
		_, err := m.resolveRevision(context.Background(), "qwen/Qwen2.5-0.5B-Instruct", "masterr", "")
		require.Error(t, err)
		assert.Equal(t, ReasonSourceUnavailable, ReasonOf(err))
		assert.Contains(t, ReasonOf(err), ReasonSourceUnavailable)
	})

	t.Run("a ref the index does not know is RevisionNotFound", func(t *testing.T) {
		m := newFakeModelScope(t,
			map[string]http.HandlerFunc{
				"GET " + modelScopeCommitsPath("qwen/Qwen2.5-0.5B-Instruct"): modelScopeCommitsRoute(),
			},
			nil, nil)
		_, err := m.resolveRevision(context.Background(), "qwen/Qwen2.5-0.5B-Instruct", "no-such", "")
		require.Error(t, err)
		assert.Equal(t, ReasonRevisionNotFound, ReasonOf(err))
	})

	t.Run("an index answer that is not a commit is SourceUnavailable", func(t *testing.T) {
		m := newFakeModelScope(t,
			map[string]http.HandlerFunc{
				"GET " + modelScopeCommitsPath("qwen/Qwen2.5-0.5B-Instruct"): modelScopeCommitsRoute("not-a-commit"),
			},
			map[string]string{"refs/heads/master": modelScopeCommit}, nil)
		_, err := m.resolveRevision(context.Background(), "qwen/Qwen2.5-0.5B-Instruct", "master", "")
		require.Error(t, err)
		assert.Equal(t, ReasonSourceUnavailable, ReasonOf(err))
	})
}

// modelScopeFilesRoute 按 Root 返回 repo/files 的内容，并记录每次查询，以核对逐目录查找的范围。
func modelScopeFilesRoute(repository string, pages map[string][]map[string]any, served *[]url.Values) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if served != nil {
			*served = append(*served, r.URL.Query())
		}
		root := r.URL.Query().Get("Root")
		page, ok := pages[root]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"Code":10990101004,"Message":"no file tree"}`))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"Code":    200,
			"Message": "success",
			"Data":    map[string]any{"Files": page, "TotalCount": len(page)},
		})
	}
}

func modelScopeFilesPath(repository string) string {
	return "/api/v1/models/" + repository + "/repo/files"
}

func msFile(path string, size int64, sha string) map[string]any {
	return map[string]any{"Type": "blob", "Path": path, "Size": size, "Sha256": sha}
}

func msDir(path string) map[string]any {
	return map[string]any{"Type": "tree", "Path": path, "Size": 0}
}

func shaOf(s string) string {
	return strings.Repeat(s, 64)
}

func TestModelScopeListManifest(t *testing.T) {
	t.Run("a small tree lists whole, files only, and hashes as an independent v1 manifest", func(t *testing.T) {
		route := modelScopeFilesRoute("qwen/repo", map[string][]map[string]any{
			"": {
				msFile("README.md", 3, shaOf("a")),
				msDir("sub"),
				msFile("sub/tokenizer.json", 7, shaOf("b")),
			},
		}, nil)
		m := newFakeModelScope(t, map[string]http.HandlerFunc{"GET " + modelScopeFilesPath("qwen/repo"): route}, nil, nil)

		manifest, err := m.ListManifest(context.Background(), "qwen/repo", modelScopeCommit, "", Filter{})
		require.NoError(t, err)
		want, err := NewManifest([]ManifestEntry{
			{Path: "README.md", Size: 3, Digest: DigestSHA256 + ":" + shaOf("a")},
			{Path: "sub/tokenizer.json", Size: 7, Digest: DigestSHA256 + ":" + shaOf("b")},
		})
		require.NoError(t, err)
		assert.Equal(t, want, manifest)
	})

	t.Run("a truncated root listing is re-listed per directory", func(t *testing.T) {
		// 根目录的递归结果恰好达到 3000 项，可能已被截断。应改按直接子目录读取完整文件树。
		root := make([]map[string]any, 0, modelScopePageBound)
		for i := 0; i < modelScopePageBound; i++ {
			root = append(root, msFile(fmt.Sprintf("gone/file-%04d", i), 1, shaOf("a")))
		}
		direct := []map[string]any{msDir("docs"), msDir("data"), msFile("loose.txt", 1, shaOf("e"))}
		docs := []map[string]any{msFile("docs/readme.md", 5, shaOf("b"))}
		for i := 0; i < 1498; i++ {
			docs = append(docs, msFile(fmt.Sprintf("docs/f-%04d", i), 1, shaOf("a")))
		}
		data := []map[string]any{msFile("data/set.bin", 9, shaOf("c"))}
		for i := 0; i < 1498; i++ {
			data = append(data, msFile(fmt.Sprintf("data/f-%04d", i), 1, shaOf("a")))
		}
		var served []url.Values
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			served = append(served, r.URL.Query())
			page := root
			switch {
			case r.URL.Query().Get("Recursive") != "true":
				page = direct
			case r.URL.Query().Get("Root") == "docs":
				page = docs
			case r.URL.Query().Get("Root") == "data":
				page = data
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"Code": 200, "Message": "success",
				"Data": map[string]any{"Files": page, "TotalCount": len(page)},
			})
		}))
		t.Cleanup(server.Close)
		m := &ModelScope{Endpoint: server.URL, Client: server.Client()}

		manifest, err := m.ListManifest(context.Background(), "qwen/repo", modelScopeCommit, "", Filter{})
		require.NoError(t, err)
		assert.Equal(t, int64(len(docs)+len(data)+1), manifest.FileCount)
		// 只有达到截断边界的子树才需要继续拆分。用请求记录核对各目录的递归查询和直接子项查询次数。
		rootWhole, rootDirect := 0, 0
		children := make([]string, 0, 2)
		for _, q := range served {
			switch root := q.Get("Root"); root {
			case "":
				if q.Get("Recursive") == "true" {
					rootWhole++
				} else {
					rootDirect++
				}
			default:
				children = append(children, root)
				assert.Equal(t, "true", q.Get("Recursive"))
			}
		}
		assert.Equal(t, 1, rootWhole)
		assert.Equal(t, 1, rootDirect)
		assert.ElementsMatch(t, []string{"docs", "data"}, children)
	})

	t.Run("a directory with 3000 direct children refuses", func(t *testing.T) {
		root := make([]map[string]any, 0, modelScopePageBound)
		for i := 0; i < modelScopePageBound; i++ {
			root = append(root, msDir(fmt.Sprintf("d-%04d", i)))
		}
		route := modelScopeFilesRoute("qwen/repo", map[string][]map[string]any{"": root}, nil)
		m := newFakeModelScope(t, map[string]http.HandlerFunc{"GET " + modelScopeFilesPath("qwen/repo"): route}, nil, nil)

		_, err := m.ListManifest(context.Background(), "qwen/repo", modelScopeCommit, "", Filter{})
		require.Error(t, err)
		assert.Equal(t, ReasonSourceUnavailable, ReasonOf(err))
		se := &SourceError{}
		require.True(t, errors.As(err, &se))
		assert.Contains(t, se.Message, "3000")
	})

	t.Run("a tree past MaxEntries refuses without walking the rest", func(t *testing.T) {
		files := make([]map[string]any, 0, 8)
		for i := 0; i < 8; i++ {
			files = append(files, msFile(fmt.Sprintf("f-%02d", i), 1, shaOf("a")))
		}
		route := modelScopeFilesRoute("qwen/repo", map[string][]map[string]any{"": files}, nil)
		m := newFakeModelScope(t, map[string]http.HandlerFunc{"GET " + modelScopeFilesPath("qwen/repo"): route}, nil, nil)
		m.MaxEntries = 5

		_, err := m.ListManifest(context.Background(), "qwen/repo", modelScopeCommit, "", Filter{})
		require.Error(t, err)
		assert.Equal(t, ReasonManifestTooLarge, ReasonOf(err))
	})

	t.Run("an entry without a usable Sha256 is an invalid manifest", func(t *testing.T) {
		route := modelScopeFilesRoute("qwen/repo", map[string][]map[string]any{
			"": {{"Type": "blob", "Path": "README.md", "Size": 3}},
		}, nil)
		m := newFakeModelScope(t, map[string]http.HandlerFunc{"GET " + modelScopeFilesPath("qwen/repo"): route}, nil, nil)

		_, err := m.ListManifest(context.Background(), "qwen/repo", modelScopeCommit, "", Filter{})
		require.Error(t, err)
		assert.Equal(t, ReasonInvalidManifest, ReasonOf(err))
	})

	t.Run("an unexpected entry type is an invalid manifest", func(t *testing.T) {
		route := modelScopeFilesRoute("qwen/repo", map[string][]map[string]any{
			"": {{"Type": "symlink", "Path": "link", "Size": 1, "Sha256": shaOf("a")}},
		}, nil)
		m := newFakeModelScope(t, map[string]http.HandlerFunc{"GET " + modelScopeFilesPath("qwen/repo"): route}, nil, nil)

		_, err := m.ListManifest(context.Background(), "qwen/repo", modelScopeCommit, "", Filter{})
		require.Error(t, err)
		assert.Equal(t, ReasonInvalidManifest, ReasonOf(err))
	})

	t.Run("an empty commit and a fully filtered one are EmptyManifest", func(t *testing.T) {
		route := modelScopeFilesRoute("qwen/repo", map[string][]map[string]any{
			"": {msDir("docs")},
		}, nil)
		m := newFakeModelScope(t, map[string]http.HandlerFunc{"GET " + modelScopeFilesPath("qwen/repo"): route}, nil, nil)

		_, err := m.ListManifest(context.Background(), "qwen/repo", modelScopeCommit, "", Filter{})
		require.Error(t, err)
		assert.Equal(t, ReasonEmptyManifest, ReasonOf(err))

		_, err = m.ListManifest(context.Background(), "qwen/repo", modelScopeCommit, "",
			Filter{Allow: []string{"*.safetensors"}})
		require.Error(t, err)
		assert.Equal(t, ReasonEmptyManifest, ReasonOf(err))
	})

	t.Run("the resolve path pins the commit the revision resolved to", func(t *testing.T) {
		const branchCommit = "13448952f850140b43c5e14d0e19f7e7f8cb3c47"
		commits := modelScopeCommitsRoute(branchCommit)
		var served []url.Values
		files := modelScopeFilesRoute("qwen/repo", map[string][]map[string]any{
			"": {msFile("README.md", 3, shaOf("a"))},
		}, &served)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.EscapedPath() == modelScopeCommitsPath("qwen/repo") {
				commits(w, r)
				return
			}
			files(w, r)
		}))
		t.Cleanup(server.Close)
		m := &ModelScope{Endpoint: server.URL, Client: server.Client()}
		m.lsRemote = func(_ context.Context, _, _ string) (map[string]string, error) {
			return map[string]string{"refs/heads/master": branchCommit}, nil
		}

		got, err := m.Resolve(context.Background(), "qwen/repo", "master", "", Filter{})
		require.NoError(t, err)
		assert.Equal(t, branchCommit, got.Commit)
		require.Len(t, served, 1)
		assert.Equal(t, branchCommit, served[0].Get("Revision"))
	})
}
