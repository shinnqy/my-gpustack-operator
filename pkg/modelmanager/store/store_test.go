package store

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testHex = strings.Repeat("a", 64)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = removeAllWritable(s.Root()) })
	return s
}

func writeAttemptFile(t *testing.T, a *Attempt, path, content string) {
	t.Helper()
	p, err := a.FilePath(path)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(p), workingDirMode))
	require.NoError(t, os.WriteFile(p, []byte(content), workingFileMode))
}

func TestPublish(t *testing.T) {
	s := openTestStore(t)
	a, err := s.NewAttempt(testHex)
	require.NoError(t, err)
	writeAttemptFile(t, a, "config.json", "{}")
	writeAttemptFile(t, a, "sub/model.safetensors", "weights")
	assert.False(t, s.IsPublished(testHex), "an attempt is not published")

	require.NoError(t, a.Publish(Marker{Digest: "sha256:" + testHex, ManifestFormat: "gpustack-manifest v1", SizeBytes: 9, FileCount: 2}))

	assert.True(t, s.IsPublished(testHex))
	m, err := s.ReadMarker(testHex)
	require.NoError(t, err)
	assert.Equal(t, a.Number, m.Attempt)
	assert.Equal(t, "sha256:"+testHex, m.Digest)
	assert.NoDirExists(t, a.Dir())

	tree := s.PublishedTree(testHex)
	b, err := os.ReadFile(filepath.Join(tree, "sub", "model.safetensors"))
	require.NoError(t, err)
	assert.Equal(t, "weights", string(b))
	for path, want := range map[string]fs.FileMode{
		tree:                               publishedDirMode,
		filepath.Join(tree, "sub"):         publishedDirMode,
		filepath.Join(tree, "config.json"): publishedFileMode,
		filepath.Join(tree, "sub", "model.safetensors"): publishedFileMode,
	} {
		info, err := os.Stat(path)
		require.NoError(t, err)
		assert.Equal(t, want, info.Mode().Perm(), path)
	}

	markers, err := s.Published()
	require.NoError(t, err)
	require.Len(t, markers, 1)
	assert.Equal(t, int64(2), markers[0].FileCount)
}

func TestPublishRefusesAnythingButRegularFiles(t *testing.T) {
	s := openTestStore(t)
	a, err := s.NewAttempt(testHex)
	require.NoError(t, err)
	writeAttemptFile(t, a, "config.json", "{}")
	p, err := a.FilePath("link")
	require.NoError(t, err)
	require.NoError(t, os.Symlink("/etc/passwd", p))

	require.Error(t, a.Publish(Marker{Digest: "sha256:" + testHex}))
	assert.False(t, s.IsPublished(testHex))
}
