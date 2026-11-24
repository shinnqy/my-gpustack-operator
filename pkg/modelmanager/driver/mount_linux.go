package driver

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"

	"gpustack.ai/gpustack/pkg/modelmanager/store"
)

// HostMounter 在插件的 mount 命名空间内执行挂载，并通过 Bidirectional 挂载传播与宿主机共享 kubelet pods 目录。
type HostMounter struct{}

// IsMountPoint 通过解析 mountinfo 报告 path 是否为挂载点：同一文件系统内的目录 bind mount
// 与父目录拥有相同设备号，无法仅靠 stat 判断。
func (HostMounter) IsMountPoint(path string) (bool, error) {
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return false, err
	}
	defer func() { _ = f.Close() }()
	mounts, err := store.ParseMountInfo(f)
	if err != nil {
		return false, err
	}
	clean := filepath.Clean(path)
	for _, m := range mounts {
		if m.MountPoint == clean {
			return true, nil
		}
	}

	return false, nil
}

// BindReadOnly 将 source 以只读且禁用 set-user-ID 与设备文件的方式 bind mount 到 target。
// bind mount 只能在 remount 步骤设置只读标志，因此需要两次 mount 调用；
// 若第二步失败则立即卸载回滚，避免留下可写挂载。
func (HostMounter) BindReadOnly(source, target string) error {
	if err := unix.Mount(source, target, "", unix.MS_BIND, ""); err != nil {
		return fmt.Errorf("bind %s onto %s: %w", source, target, err)
	}
	flags := uintptr(unix.MS_BIND | unix.MS_REMOUNT | unix.MS_RDONLY | unix.MS_NOSUID | unix.MS_NODEV)
	if err := unix.Mount("", target, "", flags, ""); err != nil {
		if uerr := unix.Unmount(target, 0); uerr != nil {
			return fmt.Errorf("make %s read-only: %w; and the writable bind is left: %w", target, err, uerr)
		}
		return fmt.Errorf("make %s read-only: %w", target, err)
	}

	return nil
}

// IsReadOnly 根据 statfs 返回的标志判断 path 处的挂载是否为只读且禁用了 set-user-ID 和设备文件。
func (HostMounter) IsReadOnly(path string) (bool, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return false, fmt.Errorf("statfs %s: %w", path, err)
	}
	want := int64(unix.ST_RDONLY | unix.ST_NOSUID | unix.ST_NODEV)

	return st.Flags&want == want, nil
}

// Unmount 卸载 target；目标本身未挂载时不视为错误。
func (HostMounter) Unmount(target string) error {
	if err := unix.Unmount(target, 0); err != nil && !errors.Is(err, unix.EINVAL) && !errors.Is(err, unix.ENOENT) {
		return fmt.Errorf("unmount %s: %w", target, err)
	}

	return nil
}
