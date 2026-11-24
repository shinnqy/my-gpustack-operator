package store

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// Device 返回 path 所在文件系统的 "major:minor" 设备号，与 mountinfo 中的格式一致。
func Device(path string) (string, error) {
	var st unix.Stat_t
	if err := unix.Stat(path, &st); err != nil {
		return "", fmt.Errorf("stat %s: %w", path, err)
	}

	return fmt.Sprintf("%d:%d", unix.Major(uint64(st.Dev)), unix.Minor(uint64(st.Dev))), nil // nolint: unconvert
}
