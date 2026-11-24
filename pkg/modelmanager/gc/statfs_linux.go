package gc

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// Statfs 读取 path 所在文件系统的容量与使用量。Used 计入所有非特权写入方不可用的块（包含保留块），
// 与 kubelet 计算文件系统可用空间的方式一致，确保缓存水位线与 kubelet 驱逐阈值口径统一。
func Statfs(path string) (Usage, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return Usage{}, fmt.Errorf("statfs %s: %w", path, err)
	}
	bsize := uint64(st.Bsize) // nolint: gosec // 块大小恒为正数。

	return Usage{Total: st.Blocks * bsize, Used: (st.Blocks - st.Bavail) * bsize}, nil
}
