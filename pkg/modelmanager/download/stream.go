package download

import "os"

// 非 Hub 拉取方与 Hub 下载器共用的流式哈希校验接口：字节到达时按顺序计算哈希，
// 按字节间隔保存检查点，下载中断后可从最近检查点续传而无需从头开始。字节来源由调用方决定。

// StreamHasher 按到达顺序对单个文件的字节计算哈希并定期记录检查点，以便同一文件的后续尝试能够续传。
type StreamHasher struct {
	v              *verifier
	checkpointPath string
	interval       int64
	next           int64
}

// NewStreamHasher 开始对摘要为 digest、大小为 size 的文件计算哈希，每完成 checkpointBytes 字节
// 将状态写入 checkpointPath；checkpointBytes 小于等于 0 时仅在 Verify 确认文件完整后写检查点。
func NewStreamHasher(digest string, size int64, checkpointPath string, checkpointBytes int64) (*StreamHasher, error) {
	v, err := newVerifier(digest, size)
	if err != nil {
		return nil, err
	}

	return &StreamHasher{v: v, checkpointPath: checkpointPath, interval: checkpointBytes, next: checkpointBytes}, nil
}

// Resume 恢复同一文件先前保存在 checkpointPath 的哈希状态，将 dest 截断到已哈希位置，
// 并返回需要继续拉取的起始偏移量。若当前构建无法反序列化哈希状态，则从磁盘重新哈希已落盘部分；
// 没有检查点时从 0 开始。
func Resume(digest string, size int64, dest, checkpointPath string, checkpointBytes int64) (*StreamHasher, int64, error) {
	s, err := NewStreamHasher(digest, size, checkpointPath, checkpointBytes)
	if err != nil {
		return nil, 0, err
	}
	f, err := os.OpenFile(dest, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = f.Close() }()
	offset, err := s.v.resume(f, checkpointPath)
	if err != nil {
		return nil, 0, err
	}
	if s.interval > 0 && offset > 0 {
		s.next = (offset/s.interval + 1) * s.interval
	}

	return s, offset, nil
}

// Hash 按字节顺序继续哈希下一段数据，并在跨越间隔边界时写入检查点。
// 无论数据来自何处，调用方均按字节序送入，且边界检查点覆盖的字节均已写入磁盘对应偏移处。
func (s *StreamHasher) Hash(p []byte) error {
	if _, err := s.v.Write(p); err != nil {
		return err
	}
	if s.interval <= 0 {
		return nil
	}
	checkpointed := false
	for s.v.offset >= s.next {
		checkpointed = true
		s.next += s.interval
	}
	if !checkpointed {
		return nil
	}

	return s.v.saveCheckpoint(s.checkpointPath)
}

// Verify 完成文件 dest 的最终校验：长度与摘要必须与已哈希内容一致，并写入覆盖整个文件的最终检查点，
// 以便后续同一文件的尝试可直接跳过下载。
func (s *StreamHasher) Verify(dest string) error {
	if err := s.v.check(dest); err != nil {
		return err
	}

	return s.v.saveCheckpoint(s.checkpointPath)
}
