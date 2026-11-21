//go:build !linux

package osx

// GetRelease 读取操作系统内核版本，例如 5.15.0-1051-azure。
func GetRelease() string {
	return ""
}
