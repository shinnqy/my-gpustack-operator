package store

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"time"

	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/pkg/utils/json"
	"gpustack.ai/gpustack/pkg/utils/stringx"
)

// 账本仅存放在节点本地磁盘上。它是节点自身的私有记录而非 API 对象，因此可以记录引用所属的 Pod，
// 但绝不保存任何凭据。

// Ref 记录某个目标路径从某个摘要的已发布目录树建立了只读 bind mount。
type Ref struct {
	// VolumeID 是 kubelet 为该卷分配的 ID。
	VolumeID string `json:"volumeID"`
	// TargetPath 是 kubelet 请求挂载的目标路径。
	TargetPath string `json:"targetPath"`
	// Hex 是清单摘要的十六进制部分。
	Hex string `json:"hex"`
	// PodNamespace、PodName 和 PodUID 记录已知的挂载目标所属 Pod 信息。
	PodNamespace string `json:"podNamespace,omitempty"`
	PodName      string `json:"podName,omitempty"`
	PodUID       string `json:"podUID,omitempty"`
}

var volumeIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

func (s *Store) refPath(volumeID string) (string, error) {
	if !volumeIDPattern.MatchString(volumeID) {
		return "", fmt.Errorf("volume ID %q is not a file name", volumeID)
	}

	return filepath.Join(s.root, ledgerDir, refsDir, volumeID+".json"), nil
}

// WriteRef 写入引用记录 r，覆盖同一卷此前的记录。
func (s *Store) WriteRef(r Ref) error {
	path, err := s.refPath(r.VolumeID)
	if err != nil {
		return err
	}
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}

	return writeFileSync(path, b)
}

// Claim 在 r.Hex 对应的目录树已发布时记录引用 r，并返回该目录树是否已发布。
// 它与 RemoveUnreferenced 持有同一把锁，因此声明成功的目录树在引用移除前不会被回收。
// 挂载流程会先声明引用再执行 bind mount：若引用无法写入，挂载直接失败，避免留下回收器看不见的已挂载目录树。
func (s *Store) Claim(r Ref) (bool, error) {
	s.claimMu.Lock()
	defer s.claimMu.Unlock()

	if !s.IsPublished(r.Hex) {
		return false, nil
	}
	if err := s.WriteRef(r); err != nil {
		return false, err
	}

	return true, nil
}

// ReadRef 读取 volumeID 的引用记录；不存在时返回 false。
func (s *Store) ReadRef(volumeID string) (Ref, bool, error) {
	var r Ref
	path, err := s.refPath(volumeID)
	if err != nil {
		return r, false, err
	}
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return r, false, nil
	}
	if err != nil {
		return r, false, err
	}
	if err := json.Unmarshal(b, &r); err != nil {
		return r, false, err
	}

	return r, true, nil
}

// RemoveRef 删除 volumeID 的引用记录；记录不存在时不视为错误。
func (s *Store) RemoveRef(volumeID string) error {
	path, err := s.refPath(volumeID)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}

	return nil
}

// Refs 按目标路径排序返回所有引用记录。
func (s *Store) Refs() ([]Ref, error) {
	paths, err := filepath.Glob(filepath.Join(s.root, ledgerDir, refsDir, "*.json"))
	if err != nil {
		return nil, err
	}
	refs := make([]Ref, 0, len(paths))
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if errors.Is(err, fs.ErrNotExist) {
			// 列出路径之后卷已被卸载：引用已不存在，跳过即可。
			continue
		}
		if err != nil {
			return nil, err
		}
		var r Ref
		if err := json.Unmarshal(b, &r); err != nil {
			return nil, fmt.Errorf("%s: %w", p, err)
		}
		refs = append(refs, r)
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].TargetPath < refs[j].TargetPath })

	return refs, nil
}

// DigestRecord 是账本中按摘要维护的状态记录。
type DigestRecord struct {
	// LastAttempt 是已分配过的最大尝试编号，保证编号永不复用。
	LastAttempt int64 `json:"lastAttempt,omitempty"`
	// LastUsedTime 是最近一次挂载或卸载该摘要的时间。
	LastUsedTime time.Time `json:"lastUsedTime,omitzero"`
	// Failures 记录连续失败的尝试次数；成功一次即清零。
	Failures int `json:"failures,omitempty"`
	// Reason 和 Message 描述最近一次失败原因；RetryTime 是允许下次重试的最早时间。
	Reason    string    `json:"reason,omitempty"`
	Message   string    `json:"message,omitempty"`
	RetryTime time.Time `json:"retryTime,omitzero"`
}

func (s *Store) digestPath(hex string) (string, error) {
	if !hexDigestPattern.MatchString(hex) {
		return "", fmt.Errorf("digest %q is not 64 hexadecimal digits", hex)
	}

	return filepath.Join(s.root, ledgerDir, digestsDir, hex+".json"), nil
}

// ReadDigest 读取 hex 的摘要记录；尚无记录时返回零值。
func (s *Store) ReadDigest(hex string) (DigestRecord, error) {
	var rec DigestRecord
	path, err := s.digestPath(hex)
	if err != nil {
		return rec, err
	}
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return rec, nil
	}
	if err != nil {
		return rec, err
	}
	err = json.Unmarshal(b, &rec)

	return rec, err
}

// WriteDigest 写入 hex 的摘要记录。
func (s *Store) WriteDigest(hex string, rec DigestRecord) error {
	path, err := s.digestPath(hex)
	if err != nil {
		return err
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}

	return writeFileSync(path, b)
}

// UpdateDigest 对 hex 的记录执行 fn 并写回磁盘。所有更新共享同一把锁，
// 使挂载使用时间更新与下载尝试结果能够合并，不会互相覆盖各自先前读到的旧副本。
func (s *Store) UpdateDigest(hex string, fn func(*DigestRecord)) error {
	s.digestMu.Lock()
	defer s.digestMu.Unlock()

	rec, err := s.ReadDigest(hex)
	if err != nil {
		return err
	}
	fn(&rec)

	return s.WriteDigest(hex, rec)
}

// Digests 返回所有已有记录的摘要映射。
func (s *Store) Digests() (map[string]DigestRecord, error) {
	paths, err := filepath.Glob(filepath.Join(s.root, ledgerDir, digestsDir, "*.json"))
	if err != nil {
		return nil, err
	}
	recs := make(map[string]DigestRecord, len(paths))
	for _, p := range paths {
		hex := filepath.Base(p[:len(p)-len(".json")])
		if !hexDigestPattern.MatchString(hex) {
			// 非本存储写入的文件：单个杂项文件不应导致节点的每次上报失败。
			klog.V(1).InfoS("skip a file that is not a digest record", "path", p)
			continue
		}
		rec, err := s.ReadDigest(hex)
		if err != nil {
			return nil, err
		}
		recs[hex] = rec
	}

	return recs, nil
}

// RemoveDigest 在内容已清除后删除 hex 的摘要记录。
func (s *Store) RemoveDigest(hex string) error {
	path, err := s.digestPath(hex)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}

	return nil
}

// EphemeralVolumeID 按 kubelet 为 CSI 内联卷生成的格式计算卷 ID："csi-" 后接 Pod UID 与卷名拼接后的
// 十六进制 SHA-256。这只是 kubelet 的内部约定而非 CSI 协议契约，仅用于在账本丢失后接管已有挂载时命名引用，
// 没有任何逻辑依赖重新推算该值。
func EphemeralVolumeID(podUID, volumeName string) string {
	return fmt.Sprintf("csi-%s", stringx.SumBySHA256(podUID+volumeName))
}
