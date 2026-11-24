package download

import (
	"crypto/sha1" // nolint: gosec // 非 LFS 文件的 git blob id 按 git 规范使用 SHA-1。
	"crypto/sha256"
	"encoding"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"strconv"
	"strings"

	"gpustack.ai/gpustack/pkg/modelartifact"
	"gpustack.ai/gpustack/pkg/utils/json"
)

// verifier 按字节顺序对单个文件计算哈希，并判断其是否与清单条目一致。
type verifier struct {
	algorithm string
	want      string
	size      int64
	h         hash.Hash
	// offset 记录已完成哈希的内容字节数。
	offset int64
}

func newVerifier(digest string, size int64) (*verifier, error) {
	algorithm, want, ok := strings.Cut(digest, ":")
	if !ok {
		return nil, errorf(ReasonIntegrityMismatch, "digest %q has no algorithm", digest)
	}
	v := &verifier{algorithm: algorithm, want: want, size: size}
	switch algorithm {
	case modelartifact.DigestSHA256:
		v.h = sha256.New()
	case modelartifact.DigestGitSHA1:
		// git blob id 在文件内容之前先对包含长度的头部做哈希。
		v.h = sha1.New() // nolint: gosec
		_, _ = v.h.Write([]byte("blob " + strconv.FormatInt(size, 10) + "\x00"))
	default:
		return nil, errorf(ReasonIntegrityMismatch, "digest %q has an unknown algorithm", digest)
	}

	return v, nil
}

func (v *verifier) Write(p []byte) (int, error) {
	n, err := v.h.Write(p)
	v.offset += int64(n)

	return n, err
}

// check 校验已哈希的长度与摘要是否匹配，不匹配时返回完整性错误。
func (v *verifier) check(path string) error {
	if v.offset != v.size {
		return errorf(ReasonIntegrityMismatch, "%s: received %d bytes, the manifest says %d", path, v.offset, v.size)
	}
	if got := hex.EncodeToString(v.h.Sum(nil)); got != v.want {
		return errorf(ReasonIntegrityMismatch, "%s: the content hashes to %s:%s, the manifest says %s:%s",
			path, v.algorithm, got, v.algorithm, v.want)
	}

	return nil
}

// ResumableOffset 返回位于 dest、检查点位于 path 的文件可从先前尝试继承的字节数：
// 检查点对磁盘上的文件有效时返回其偏移量，否则返回 0。判定规则与 resume 一致但不修改文件，
// 便于调用方在开始下载前统计已继承的字节数。
func ResumableOffset(dest, path string, size int64) int64 {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	var cp checkpoint
	if json.Unmarshal(b, &cp) != nil || cp.Offset < 0 || cp.Offset > size {
		return 0
	}
	if info, err := os.Stat(dest); err != nil || info.Size() < cp.Offset {
		return 0
	}

	return cp.Offset
}

// checkpoint 记录断点续传的起点：Offset 之前的字节均已计算哈希并在写入检查点前同步落盘，
// 确保恢复的哈希状态对应磁盘上真实存在的字节。
type checkpoint struct {
	Offset int64  `json:"offset"`
	State  []byte `json:"state"`
}

// saveCheckpoint 将已哈希的位置写入位于发布目录树之外的 path。
// 检查点文件本身不做 fsync：若机器崩溃导致丢失最新检查点，文件会从更早的位置或起点重新续传，
// 仅损失部分进度，绝不会信任可能未落盘的字节。
func (v *verifier) saveCheckpoint(path string) error {
	state, err := v.h.(encoding.BinaryMarshaler).MarshalBinary()
	if err != nil {
		return err
	}
	b, err := json.Marshal(checkpoint{Offset: v.offset, State: state})
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}

	return os.Rename(tmp, path)
}

// resume 根据 path 处的检查点返回文件 f 应继续下载的起始偏移量。它只信任检查点：
// 并行下载写入到已哈希位置之后的字节前可能存在空洞，因此会被截断。
// 若当前构建无法反序列化检查点中的哈希状态，则从磁盘重新对已达检查点的字节计算哈希；
// 没有检查点的文件从头开始。
func (v *verifier) resume(f *os.File, path string) (int64, error) {
	var cp checkpoint
	b, err := os.ReadFile(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return 0, f.Truncate(0)
	case err != nil:
		return 0, err
	}
	if json.Unmarshal(b, &cp) != nil || cp.Offset < 0 || cp.Offset > v.size {
		return 0, f.Truncate(0)
	}
	if info, err := f.Stat(); err != nil || info.Size() < cp.Offset {
		return 0, f.Truncate(0)
	}
	if err := f.Truncate(cp.Offset); err != nil {
		return 0, err
	}

	if err := v.h.(encoding.BinaryUnmarshaler).UnmarshalBinary(cp.State); err == nil {
		v.offset = cp.Offset
		return cp.Offset, nil
	}
	fresh, err := newVerifier(v.algorithm+":"+v.want, v.size)
	if err != nil {
		return 0, err
	}
	*v = *fresh
	if _, err := io.Copy(v, io.NewSectionReader(f, 0, cp.Offset)); err != nil {
		return 0, fmt.Errorf("re-hash the resumed part of %s: %w", f.Name(), err)
	}

	return cp.Offset, nil
}

func removeCheckpoint(path string) {
	_ = os.Remove(path)
}
