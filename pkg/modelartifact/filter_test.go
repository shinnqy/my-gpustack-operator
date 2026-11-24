package modelartifact

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gpustack.ai/gpustack/pkg/utils/json"
)

func TestFilterEntries(t *testing.T) {
	entries := []ManifestEntry{
		manifestEntry(".gitattributes", 1, testGitSHA1),
		manifestEntry("config.json", 1, testGitSHA1),
		manifestEntry("model.safetensors", 3, testSHA256),
		manifestEntry("Model.SAFETENSORS", 3, testSHA256),
		manifestEntry("original/consolidated.00.pth", 5, testSHA256),
		manifestEntry("original/params.json", 1, testGitSHA1),
		manifestEntry("sub/original/x.bin", 2, testSHA256),
	}
	cases := []struct {
		name   string
		allow  []string
		ignore []string
		want   []string
	}{
		{
			name: "no pattern keeps every entry",
			want: []string{
				".gitattributes", "config.json", "model.safetensors", "Model.SAFETENSORS",
				"original/consolidated.00.pth", "original/params.json", "sub/original/x.bin",
			},
		},
		{
			name:  "a star crosses slashes, so *.json selects nested files too",
			allow: []string{"*.json"},
			want:  []string{"config.json", "original/params.json"},
		},
		{
			name:  "** is a star like any other",
			allow: []string{"**"},
			want: []string{
				".gitattributes", "config.json", "model.safetensors", "Model.SAFETENSORS",
				"original/consolidated.00.pth", "original/params.json", "sub/original/x.bin",
			},
		},
		{
			name:   "original/* ignores the directory at the root only",
			ignore: []string{"original/*"},
			want: []string{
				".gitattributes", "config.json", "model.safetensors", "Model.SAFETENSORS",
				"sub/original/x.bin",
			},
		},
		{
			name:  "a trailing slash on an allow pattern is the directory's contents",
			allow: []string{"original/"},
			want:  []string{"original/consolidated.00.pth", "original/params.json"},
		},
		{
			name:   "a trailing slash on an ignore pattern is the directory's contents",
			ignore: []string{"original/"},
			want: []string{
				".gitattributes", "config.json", "model.safetensors", "Model.SAFETENSORS",
				"sub/original/x.bin",
			},
		},
		{
			name:  "matching is case-sensitive on the allow side",
			allow: []string{"*.safetensors"},
			want:  []string{"model.safetensors"},
		},
		{
			name:   "matching is case-sensitive on the ignore side",
			ignore: []string{"*.SAFETENSORS"},
			want: []string{
				".gitattributes", "config.json", "model.safetensors",
				"original/consolidated.00.pth", "original/params.json", "sub/original/x.bin",
			},
		},
		{
			name:   "ignore wins over allow",
			allow:  []string{"*.json", "*.safetensors"},
			ignore: []string{"original/"},
			want:   []string{"config.json", "model.safetensors"},
		},
		{
			name:  "a pattern that matches nothing keeps nothing",
			allow: []string{"*.gguf"},
			want:  []string{},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := []string{}
			for _, e := range FilterEntries(entries, c.allow, c.ignore) {
				got = append(got, e.Path)
			}
			assert.Equal(t, c.want, got)
		})
	}
}

// TestFilterDigestCrossImplementation 将过滤后的清单与独立实现记录的摘要比较，
// 并核对不传过滤模式时的编码保持一致。
func TestFilterDigestCrossImplementation(t *testing.T) {
	entries := loadTreeEntries(t, "testdata/manifest/qwen2.5-0.5b-instruct-7ae55760.tree.json")
	unfiltered, err := NewManifest(entries)
	require.NoError(t, err)

	cases := []struct {
		name      string
		allow     []string
		ignore    []string
		digest    string
		fileCount int64
		same      bool
	}{
		{
			name:      "no pattern is the unfiltered manifest, byte for byte",
			digest:    "testdata/manifest/qwen2.5-0.5b-instruct-7ae55760.digest",
			fileCount: 10,
			same:      true,
		},
		{
			name:      "allow json and safetensors, ignore the tokenizer files",
			allow:     []string{"*.json", "*.safetensors"},
			ignore:    []string{"tokenizer*"},
			digest:    "testdata/manifest/qwen2.5-0.5b-instruct-7ae55760.filtered.digest",
			fileCount: 4,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			wantDigest, err := os.ReadFile(c.digest)
			require.NoError(t, err)

			got, err := NewManifest(FilterEntries(entries, c.allow, c.ignore))
			require.NoError(t, err)
			assert.Equal(t, strings.TrimSpace(string(wantDigest)), got.Digest)
			assert.Equal(t, c.fileCount, got.FileCount)
			if c.same {
				assert.Equal(t, unfiltered.Encoded, got.Encoded)
			} else {
				assert.NotEqual(t, unfiltered.Digest, got.Digest)
			}
		})
	}
}

// loadTreeEntries 将固定版本的 Hugging Face 文件树转换成清单条目。
func loadTreeEntries(t *testing.T, path string) []ManifestEntry {
	t.Helper()

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var tree []struct {
		Type string `json:"type"`
		OID  string `json:"oid"`
		Size int64  `json:"size"`
		Path string `json:"path"`
		LFS  *struct {
			OID string `json:"oid"`
		} `json:"lfs"`
	}
	require.NoError(t, json.Unmarshal(raw, &tree))

	var entries []ManifestEntry
	for _, e := range tree {
		if e.Type != "file" {
			continue
		}
		digest := DigestGitSHA1 + ":" + e.OID
		if e.LFS != nil {
			digest = DigestSHA256 + ":" + e.LFS.OID
		}
		entries = append(entries, manifestEntry(e.Path, e.Size, digest))
	}

	return entries
}
