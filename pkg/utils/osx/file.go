package osx

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// InlineTilde replaces the leading ~ with the home directory.
func InlineTilde(path string) string {
	if path == "" {
		return path
	}
	if strings.HasPrefix(path, "~"+string(filepath.Separator)) {
		hd, err := os.UserHomeDir()
		if err == nil {
			path = filepath.Join(hd, path[2:])
		}
	}
	return path
}

// Exists checks if the given path exists.
func Exists(path string, checks ...func(os.FileInfo) bool) bool {
	p := filepath.Clean(path)
	p = InlineTilde(p)

	stat, err := os.Lstat(p)
	if err != nil {
		return false
	}

	for i := range checks {
		if checks[i] == nil {
			continue
		}

		if !checks[i](stat) {
			return false
		}
	}

	return true
}

// ExistsDir checks if the given path exists and is a directory.
func ExistsDir(path string) bool {
	return Exists(path, func(stat os.FileInfo) bool {
		return stat.Mode().IsDir()
	})
}

// ExistsFile checks if the given path exists and is a regular file.
func ExistsFile(path string) bool {
	return Exists(path, func(stat os.FileInfo) bool {
		return stat.Mode().IsRegular()
	})
}

// ExistsDevice checks if the given path exists and is a device.
func ExistsDevice(path string) bool {
	return Exists(path, func(stat os.FileInfo) bool {
		return stat.Mode()&os.ModeDevice != 0
	})
}

// TempFile creates a temporary file with the given pattern.
func TempFile(pattern string) string {
	f, err := os.CreateTemp("", pattern)
	if err != nil {
		panic(fmt.Errorf("create temp file: %w", err))
	}

	defer func() { _ = f.Close() }()

	return f.Name()
}

// SubTempDir is different to TempDir.
//
// TempDir creates a temporary directory randomly,
// but SubTempDir creates a subdirectory under the temporary directory with the given path.
func SubTempDir(path string) string {
	n := filepath.Join(os.TempDir(), filepath.Clean(path))
	err := os.MkdirAll(n, 0o700)
	if err != nil {
		panic(fmt.Errorf("create temp subdir: %w", err))
	}

	return n
}

// Close closes the given io.Closer without error.
func Close(c io.Closer) {
	if c == nil {
		return
	}
	_ = c.Close()
}

// IsEmptyFile checks if the given file is empty.
func IsEmptyFile(file string) bool {
	s, err := os.Lstat(file)
	if err != nil {
		return errors.Is(err, os.ErrNotExist)
	}
	if !s.Mode().IsRegular() {
		return false
	}
	return s.Size() == 0
}

// DurableRemove removes the given file or directory,
// and also syncs the parent directory to ensure the removal is durable.
func DurableRemove(path string) error {
	p := filepath.Clean(path)
	p = InlineTilde(p)

	err := os.Remove(p)
	if err != nil {
		return err
	}
	return syncDir(filepath.Dir(p))
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	defer Close(d)

	err = d.Sync()
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}
