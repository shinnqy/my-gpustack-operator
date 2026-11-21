// Package modelartifact holds what a ModelArtifact's identity is made of: the canonical manifest
// format, and the client that resolves a Hugging Face source into one.
//
// It imports nothing from the controllers, so node-side code that verifies downloads against a
// manifest digest can use the same definition the controller published.
package modelartifact

import (
	"fmt"
)

// The per-file digest algorithms the format admits, as they are spelled before the colon.
const (
	// DigestSHA256 is the SHA-256 of the file's content: an LFS file's oid on Hugging Face.
	DigestSHA256 = "sha256"
	// DigestGitSHA1 is the git blob id of the file: a non-LFS file's oid on Hugging Face.
	DigestGitSHA1 = "gitsha1"
)

// ManifestEntry is one file of a manifest.
type ManifestEntry struct {
	// Path is the slash-separated path relative to the repository root, in the bytes the source
	// returned. It is never normalized: the files on disk must carry the names the source serves.
	Path string
	// Size is the file size in bytes.
	Size int64
	// Digest is "<algorithm>:<lowercase hexadecimal>", the algorithm one of DigestSHA256 and
	// DigestGitSHA1.
	Digest string
}

// Manifest is a canonical manifest and the figures a ModelArtifact publishes from it.
type Manifest struct {
	// Encoded is the canonical encoding: the header line, then one "<digest> <size> <path>" line
	// per file in ascending byte order of the path, every line terminated by LF.
	Encoded []byte
	// Digest is "sha256:" followed by the SHA-256 of Encoded, which is the manifest digest.
	Digest string
	// FileCount is the number of files.
	FileCount int64
	// SizeBytes is the sum of the files' sizes.
	SizeBytes int64
	// Entries are the files, in the encoding's order: what a node downloads and verifies.
	Entries []ManifestEntry
}

// NewManifest encodes entries into the canonical manifest and computes its digest.
//
// The input order never matters. An invalid entry, or two entries with one path, refuses the whole
// manifest rather than dropping the entry: a manifest silently missing a file would publish a
// digest for content that is not the commit's.
func NewManifest(entries []ManifestEntry) (Manifest, error) {
	// Implement this function in chapter 16.
	return Manifest{}, fmt.Errorf("manifest construction is pending")
}
