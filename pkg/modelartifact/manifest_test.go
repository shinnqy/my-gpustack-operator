package modelartifact

import (
	rand "math/rand/v2"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testSHA256  = DigestSHA256 + ":" + strings.Repeat("a", 64)
	testGitSHA1 = DigestGitSHA1 + ":" + strings.Repeat("b", 40)
)

func manifestEntry(path string, size int64, digest string) ManifestEntry {
	return ManifestEntry{Path: path, Size: size, Digest: digest}
}

func TestManifestEncoding(t *testing.T) {
	cases := []struct {
		name    string
		entries []ManifestEntry
		want    string
		wantErr string
	}{
		{
			name: "sorted by the bytes of the path whatever the input order",
			entries: []ManifestEntry{
				manifestEntry("b/x.json", 2, testGitSHA1),
				manifestEntry("B.json", 1, testGitSHA1),
				manifestEntry("a.safetensors", 3, testSHA256),
			},
			want: "gpustack-manifest v1\n" +
				testGitSHA1 + " 1 B.json\n" +
				testSHA256 + " 3 a.safetensors\n" +
				testGitSHA1 + " 2 b/x.json\n",
		},
		{
			name:    "a space in the path is kept verbatim in the last field",
			entries: []ManifestEntry{manifestEntry("my file.bin", 0, testSHA256)},
			want:    "gpustack-manifest v1\n" + testSHA256 + " 0 my file.bin\n",
		},
		{
			name: "a non-ASCII path is not normalized and sorts by its bytes",
			entries: []ManifestEntry{
				manifestEntry("é.json", 1, testGitSHA1),
				manifestEntry("é.json", 1, testGitSHA1),
			},
			want: "gpustack-manifest v1\n" +
				testGitSHA1 + " 1 é.json\n" +
				testGitSHA1 + " 1 é.json\n",
		},
		{
			name: "no files is the header alone",
			want: "gpustack-manifest v1\n",
		},
		{
			name:    "a duplicate path",
			entries: []ManifestEntry{manifestEntry("a", 1, testSHA256), manifestEntry("a", 1, testSHA256)},
			wantErr: "duplicate path",
		},
		{name: "an absolute path", entries: []ManifestEntry{manifestEntry("/a", 1, testSHA256)}, wantErr: "segment"},
		{name: "a dot-dot segment", entries: []ManifestEntry{manifestEntry("a/../b", 1, testSHA256)}, wantErr: "segment"},
		{name: "a dot segment", entries: []ManifestEntry{manifestEntry("./a", 1, testSHA256)}, wantErr: "segment"},
		{name: "an empty segment", entries: []ManifestEntry{manifestEntry("a//b", 1, testSHA256)}, wantErr: "segment"},
		{name: "a trailing slash", entries: []ManifestEntry{manifestEntry("a/", 1, testSHA256)}, wantErr: "segment"},
		{name: "an empty path", entries: []ManifestEntry{manifestEntry("", 1, testSHA256)}, wantErr: "UTF-8"},
		{name: "a newline in the path", entries: []ManifestEntry{manifestEntry("a\nb", 1, testSHA256)}, wantErr: "control character"},
		{name: "a DEL in the path", entries: []ManifestEntry{manifestEntry("a\x7fb", 1, testSHA256)}, wantErr: "control character"},
		{name: "invalid UTF-8", entries: []ManifestEntry{manifestEntry("a\xffb", 1, testSHA256)}, wantErr: "UTF-8"},
		{
			name:    "an unknown algorithm",
			entries: []ManifestEntry{manifestEntry("a", 1, "md5:"+strings.Repeat("a", 32))},
			wantErr: "malformed digest",
		},
		{
			name:    "uppercase hexadecimal",
			entries: []ManifestEntry{manifestEntry("a", 1, DigestSHA256+":"+strings.Repeat("A", 64))},
			wantErr: "malformed digest",
		},
		{
			name:    "a short digest",
			entries: []ManifestEntry{manifestEntry("a", 1, DigestGitSHA1+":"+strings.Repeat("b", 39))},
			wantErr: "malformed digest",
		},
		{
			name:    "a masked digest",
			entries: []ManifestEntry{manifestEntry("a", 1, DigestSHA256+":"+strings.Repeat("*", 64))},
			wantErr: "malformed digest",
		},
		{name: "a negative size", entries: []ManifestEntry{manifestEntry("a", -1, testSHA256)}, wantErr: "negative size"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			input := slices.Clone(c.entries)
			got, err := NewManifest(c.entries)
			assert.Equal(t, input, c.entries, "构造清单时应保留传入切片的顺序")
			if c.wantErr != "" {
				require.ErrorIs(t, err, ErrInvalidManifest)
				assert.ErrorContains(t, err, c.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, c.want, string(got.Encoded))
			assert.Equal(t, int64(len(c.entries)), got.FileCount)
		})
	}
}

func TestManifestDigestSensitivity(t *testing.T) {
	base := []ManifestEntry{
		manifestEntry("config.json", 659, testGitSHA1),
		manifestEntry("model.safetensors", 100, testSHA256),
	}
	cases := []struct {
		name    string
		mutate  func([]ManifestEntry) []ManifestEntry
		changed bool
	}{
		{name: "reversed order", mutate: func(e []ManifestEntry) []ManifestEntry { slices.Reverse(e); return e }},
		{name: "a size changed", mutate: func(e []ManifestEntry) []ManifestEntry { e[1].Size++; return e }, changed: true},
		{
			name: "a digest changed",
			mutate: func(e []ManifestEntry) []ManifestEntry {
				e[0].Digest = DigestGitSHA1 + ":" + strings.Repeat("c", 40)
				return e
			},
			changed: true,
		},
		{name: "a path changed", mutate: func(e []ManifestEntry) []ManifestEntry { e[0].Path = "Config.json"; return e }, changed: true},
		{name: "a file removed", mutate: func(e []ManifestEntry) []ManifestEntry { return e[:1] }, changed: true},
	}

	want, err := NewManifest(base)
	require.NoError(t, err)
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := NewManifest(c.mutate(slices.Clone(base)))
			require.NoError(t, err)
			assert.Equal(t, c.changed, got.Digest != want.Digest)
		})
	}
}

// TestManifestCrossImplementation 使用固定提交的 Hugging Face 文件树，
// 核对 Go 编码与原项目独立生成的 SHA-256 基准值。
func TestManifestCrossImplementation(t *testing.T) {
	cases := []struct {
		name      string
		tree      string
		digest    string
		fileCount int64
		sizeBytes int64
	}{
		{
			name:      "Qwen2.5-0.5B-Instruct at 7ae55760",
			tree:      "testdata/manifest/qwen2.5-0.5b-instruct-7ae55760.tree.json",
			digest:    "testdata/manifest/qwen2.5-0.5b-instruct-7ae55760.digest",
			fileCount: 10,
			sizeBytes: 999604126,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			entries := loadTreeEntries(t, c.tree)
			wantDigest, err := os.ReadFile(c.digest)
			require.NoError(t, err)
			rand.New(rand.NewPCG(1, 2)).Shuffle(len(entries), func(i, j int) {
				entries[i], entries[j] = entries[j], entries[i]
			})

			got, err := NewManifest(entries)
			require.NoError(t, err)
			assert.Equal(t, strings.TrimSpace(string(wantDigest)), got.Digest)
			assert.Equal(t, c.fileCount, got.FileCount)
			assert.Equal(t, c.sizeBytes, got.SizeBytes)
		})
	}
}
