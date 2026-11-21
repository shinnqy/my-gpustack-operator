package osx

import "golang.org/x/sys/unix"

// GetRelease 读取操作系统内核版本，例如 5.15.0-1051-azure。
func GetRelease() string {
	var uts unix.Utsname
	if err := unix.Uname(&uts); err != nil {
		return ""
	}
	for i, b := range uts.Release {
		if b == 0 {
			return string(uts.Release[:i])
		}
	}
	return string(uts.Release[:])
}
