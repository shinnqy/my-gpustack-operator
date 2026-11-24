package peer

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	modelartifact "gpustack.ai/gpustack/pkg/modelartifact"
	"gpustack.ai/gpustack/pkg/modelmanager/store"
	"gpustack.ai/gpustack/pkg/utils/json"
	"gpustack.ai/gpustack/pkg/utils/stringx"
)

// peerServer 表示测试中的单个 peer 节点，包含已发布目录树摘要、文件内容及其 HTTP 服务。
type peerServer struct {
	*httptest.Server
	hex     string
	content string
}

func newPeerServer(t *testing.T, content string) *peerServer {
	t.Helper()
	st, err := store.Open(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(writableAgain(st.Root()))

	// 目录树摘要由其中文件的规范清单计算得出，与真实发布流程生成标记的方式一致。
	manifest, err := modelartifact.NewManifest([]modelartifact.ManifestEntry{{
		Path: "weights.bin", Size: int64(len(content)), Digest: "sha256:" + stringx.SumBySHA256(content),
	}})
	require.NoError(t, err)
	treeHex := strings.TrimPrefix(manifest.Digest, "sha256:")
	a, err := st.NewAttempt(treeHex)
	require.NoError(t, err)
	treePath, err := a.FilePath("weights.bin")
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(treePath), 0o755))
	require.NoError(t, os.WriteFile(treePath, []byte(content), 0o644))
	require.NoError(t, a.Publish(store.Marker{
		Digest: manifest.Digest, SizeBytes: manifest.SizeBytes, FileCount: manifest.FileCount,
	}))
	stored, err := json.Marshal(listing{Digest: manifest.Digest, Entries: []ManifestFile{
		{Path: "weights.bin", Size: manifest.SizeBytes, Digest: manifest.Entries[0].Digest},
	}})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(st.PublishedManifestPath(treeHex), stored, 0o644))

	inner := (&Server{Store: st}).Handler()
	ts := httptest.NewServer(inner)
	t.Cleanup(ts.Close)

	return &peerServer{Server: ts, hex: treeHex, content: content}
}

func (ps *peerServer) source(t *testing.T, name string) *Source {
	t.Helper()
	client, err := NewClient(AuthOptions{Mode: AuthNone})
	require.NoError(t, err)

	return &Source{NodeName: name, BaseURL: ps.URL, Client: client}
}

func TestPullerFetchManifest(t *testing.T) {
	ps := newPeerServer(t, strings.Repeat("abcdef", 100))
	source := ps.source(t, "peer-1")
	p := &Puller{Discover: func(context.Context, string) ([]*Source, error) {
		return []*Source{source}, nil
	}}
	manifest, err := p.FetchManifest(context.Background(), "sha256:"+ps.hex)
	require.NoError(t, err)
	assert.Equal(t, "sha256:"+ps.hex, manifest.Digest, "清单必须重新绑定到目录树根摘要")
	require.Len(t, manifest.Entries, 1)
	assert.Equal(t, "weights.bin", manifest.Entries[0].Path)
	assert.Equal(t, int64(len(ps.content)), manifest.Entries[0].Size)

	dead := newPeerServer(t, strings.Repeat("abcdef", 100))
	dead.Close()
	p = &Puller{Discover: func(context.Context, string) ([]*Source, error) { return nil, nil }}
	_, err = p.FetchManifest(context.Background(), "sha256:"+dead.hex)
	require.Error(t, err, "没有 peer 持有目标目录树时应明确返回无源错误")
}
