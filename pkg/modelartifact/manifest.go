// Package modelartifact 包含构成 ModelArtifact 标识的核心内容：规范化清单（manifest）格式，
// 以及将 Hugging Face、ModelScope 模型引用解析为该清单的客户端。
//
// 该包不导入控制器中的任何内容，因此节点侧用于按清单摘要校验下载内容的组件
// 可以使用与控制器发布时完全相同的定义。
package modelartifact

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"gpustack.ai/gpustack/pkg/utils/stringx"
)

// ManifestHeader 是规范清单的首行。格式变化时更新版本，使摘要也能区分两种格式。
const ManifestHeader = "gpustack-manifest v1"

// 该格式支持的文件级摘要算法，即冒号前的拼写形式。
const (
	// DigestSHA256 是文件内容的 SHA-256 摘要，对应 Hugging Face 上 LFS 文件的 oid。
	DigestSHA256 = "sha256"
	// DigestGitSHA1 是文件的 git blob id，对应 Hugging Face 上非 LFS 文件的 oid。
	DigestGitSHA1 = "gitsha1"
)

// manifestDigestHexLength 列出节点能验证的文件摘要算法及其十六进制字符数。未知算法直接拒绝，
// 避免发布无法复核的身份。
var manifestDigestHexLength = map[string]int{
	DigestSHA256:  64,
	DigestGitSHA1: 40,
}

// ErrInvalidManifest 是清单条目校验失败时包装的错误，调用方可据此区分内容无效和无法访问源站。
var ErrInvalidManifest = errors.New("invalid manifest")

// ManifestEntry 是清单中的单个文件。
type ManifestEntry struct {
	// Path 是相对于仓库根目录、以斜杠分隔的路径，保持源端返回的原始字节。
	// 该路径永不规范化：磁盘上的文件必须保留源端提供的原始名称。
	Path string
	// Size 是以字节为单位的文件大小。
	Size int64
	// Digest 的格式为 "<algorithm>:<lowercase hexadecimal>"，其中算法为 DigestSHA256 或
	// DigestGitSHA1。
	Digest string
}

// Manifest 是规范化清单以及 ModelArtifact 据此发布的统计数值。
type Manifest struct {
	// Encoded 为规范化编码结果：首行为头信息，随后按路径字节升序每文件占一行
	// "<digest> <size> <path>"，每行均以 LF 结尾。
	Encoded []byte
	// Digest 为 "sha256:" 后接 Encoded 的 SHA-256 摘要，即清单摘要。
	Digest string
	// FileCount 为文件数量。
	FileCount int64
	// SizeBytes 为所有文件大小的总和。
	SizeBytes int64
	// Entries 为按编码顺序排列的文件列表，即节点下载并校验的对象。
	Entries []ManifestEntry
}

// NewManifest 将 entries 编码为规范化清单并计算其摘要。
//
// 输入顺序不影响结果。遇到无效条目或同一路径对应两个条目时，将拒绝整个清单
// 而非丢弃条目：静默遗漏文件的清单会为并非该 commit 实际内容的产物发布摘要。
func NewManifest(entries []ManifestEntry) (Manifest, error) {
	sorted := slices.Clone(entries)
	// Go 的字符串比较按字节进行。对合法 UTF-8，这也与 Unicode 码点顺序一致。
	slices.SortFunc(sorted, func(a, b ManifestEntry) int { return strings.Compare(a.Path, b.Path) })

	var (
		b    strings.Builder
		size int64
	)
	b.WriteString(ManifestHeader)
	b.WriteByte('\n')
	for i, e := range sorted {
		if err := validateManifestEntry(e); err != nil {
			return Manifest{}, err
		}
		if i > 0 && sorted[i-1].Path == e.Path {
			return Manifest{}, fmt.Errorf("%w: duplicate path %q", ErrInvalidManifest, e.Path)
		}
		b.WriteString(e.Digest)
		b.WriteByte(' ')
		b.WriteString(strconv.FormatInt(e.Size, 10))
		b.WriteByte(' ')
		b.WriteString(e.Path)
		b.WriteByte('\n')
		size += e.Size
	}

	encoded := []byte(b.String())

	return Manifest{
		Encoded:   encoded,
		Digest:    DigestSHA256 + ":" + stringx.SumBytesBySHA256(encoded),
		FileCount: int64(len(sorted)),
		SizeBytes: size,
		Entries:   sorted,
	}, nil
}

func validateManifestEntry(e ManifestEntry) error {
	if err := validateManifestPath(e.Path); err != nil {
		return err
	}
	if e.Size < 0 {
		return fmt.Errorf("%w: path %q has a negative size %d", ErrInvalidManifest, e.Path, e.Size)
	}
	algorithm, value, ok := strings.Cut(e.Digest, ":")
	n, known := manifestDigestHexLength[algorithm]
	if !ok || !known || len(value) != n || !isLowerHex(value) {
		return fmt.Errorf("%w: path %q has a malformed digest %q", ErrInvalidManifest, e.Path, e.Digest)
	}

	return nil
}

// validateManifestPath 只接受合法 UTF-8 的仓库相对路径。拒绝控制字符、空路径段以及 . 和 ..，
// 使条目能原样放在节点的模型目录内。
func validateManifestPath(p string) error {
	if p == "" || !utf8.ValidString(p) {
		return fmt.Errorf("%w: path %q is empty or not valid UTF-8", ErrInvalidManifest, p)
	}
	for _, r := range p {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("%w: path %q contains a control character", ErrInvalidManifest, p)
		}
	}
	for seg := range strings.SplitSeq(p, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return fmt.Errorf("%w: path %q has an empty, \".\" or \"..\" segment", ErrInvalidManifest, p)
		}
	}

	return nil
}

func isLowerHex(s string) bool {
	for i := range len(s) {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}

	return true
}
