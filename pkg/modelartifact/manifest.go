// Package modelartifact 包含构成 ModelArtifact 标识的核心内容：规范化清单（manifest）格式，
// 以及将 Hugging Face 源解析为该清单的客户端。
//
// 该包不导入控制器中的任何内容，因此节点侧用于按清单摘要校验下载内容的组件
// 可以使用与控制器发布时完全相同的定义。
package modelartifact

import (
	"fmt"
)

// 该格式支持的文件级摘要算法，即冒号前的拼写形式。
const (
	// DigestSHA256 是文件内容的 SHA-256 摘要，对应 Hugging Face 上 LFS 文件的 oid。
	DigestSHA256 = "sha256"
	// DigestGitSHA1 是文件的 git blob id，对应 Hugging Face 上非 LFS 文件的 oid。
	DigestGitSHA1 = "gitsha1"
)

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
	// 在第 16 章实现此函数。
	return Manifest{}, fmt.Errorf("manifest construction is pending")
}
