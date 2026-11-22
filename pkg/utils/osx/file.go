package osx

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// InlineTilde 会将开头的 ~ 替换为家目录。
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

// ExistsParentDir 用于检查给定路径的父目录是否存在。
func ExistsParentDir(path string) bool {
	p := filepath.Clean(path)
	p = InlineTilde(p)

	return ExistsDir(filepath.Dir(p))
}

// 检查给定路径是否存在。
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

// ExistsDir 用于检查给定路径是否存在且为目录。
func ExistsDir(path string) bool {
	return Exists(path, func(stat os.FileInfo) bool {
		return stat.Mode().IsDir()
	})
}

// ExistsFile 用于检查给定路径是否存在，且为普通文件。
func ExistsFile(path string) bool {
	return Exists(path, func(stat os.FileInfo) bool {
		return stat.Mode().IsRegular()
	})
}

// ExistsSocket 用于检查给定路径是否存在且为套接字。
func ExistsSocket(path string) bool {
	return Exists(path, func(stat os.FileInfo) bool {
		return stat.Mode()&os.ModeSocket != 0
	})
}

// ExistsDevice 用于检查给定路径是否存在且为设备。
func ExistsDevice(path string) bool {
	return Exists(path, func(stat os.FileInfo) bool {
		return stat.Mode()&os.ModeDevice != 0
	})
}

// MkdirAll 与 os.MkdirAll 类似，但支持以 ~/ 作为主目录，并强制将叶子目录的权限位与后续的 Chmod 操作保持一
// 致，因此模式不会因 umask 进程而降低（os.MkdirAll 会将 umask 应用于权限参数）。
func MkdirAll(name string, perm os.FileMode) error {
	p := filepath.Clean(name)
	p = InlineTilde(p)

	if err := os.MkdirAll(p, perm); err != nil {
		return err
	}

	return os.Chmod(p, perm)
}

// TempFile 会按照给定的模式创建一个临时文件。
func TempFile(pattern string) string {
	f, err := os.CreateTemp("", pattern)
	if err != nil {
		panic(fmt.Errorf("create temp file: %w", err))
	}

	defer func() { _ = f.Close() }()

	return f.Name()
}

// TempDir 会按照给定的模式创建一个临时目录。
func TempDir(pattern string) string {
	n, err := os.MkdirTemp("", pattern)
	if err != nil {
		panic(fmt.Errorf("create temp dir: %w", err))
	}

	return n
}

// SubTempDir 与 TempDir 不同。
//
// TempDir 会随机创建一个临时目录，而 SubTempDir 则会在临时目录下按照给定路径创建一个子目录。
func SubTempDir(path string) string {
	n := filepath.Join(os.TempDir(), filepath.Clean(path))
	err := os.MkdirAll(n, 0o700)
	if err != nil {
		panic(fmt.Errorf("create temp subdir: %w", err))
	}

	return n
}

// Close 能够无误地关闭给定的 io.Closer。
func Close(c io.Closer) {
	if c == nil {
		return
	}
	_ = c.Close()
}

// IsEmptyFile 用于检查指定的文件是否为空。
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

// Remove 功能会删除指定的文件或目录，并且在删除前还支持额外的检查。
func Remove(path string, checks ...func(os.FileInfo) error) error {
	p := filepath.Clean(path)
	p = InlineTilde(p)

	stat, err := os.Lstat(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for i := range checks {
		if checks[i] == nil {
			continue
		}

		if err = checks[i](stat); err != nil {
			return err
		}
	}

	return os.Remove(p)
}

// RemoveSocket 会移除指定的套接字，如果路径不是套接字，则返回错误。
func RemoveSocket(path string) error {
	return Remove(path, func(stat os.FileInfo) error {
		if stat.Mode()&os.ModeSocket == 0 {
			return fmt.Errorf("not a socket")
		}
		return nil
	})
}

// DurableRemove 会移除指定的文件或目录，并同步父目录以确保移除的持久性。
func DurableRemove(path string) error {
	p := filepath.Clean(path)
	p = InlineTilde(p)

	err := os.Remove(p)
	if err != nil {
		return err
	}
	return syncDir(filepath.Dir(p))
}

// DurableWrite 会原子地用数据替换路径下的文件，并使其具有持久性：它会在目标文件旁创建一个临时文件，同步该
// 文件，将其重命名为正确的名称，并同步父目录。因此，并发读取器会看到之前的内容或全新的内容，而不会是部分
// 记录。一旦调用返回，替换后的文件在异常关闭后仍能保留。
//
// 两个同步机制都具有承载功能。当数据指向的块被重命名时，记录会保留下来。而数据块则不会。因此，在写回窗口
// 内的崩溃情况会导致新名称被发布在长度被截断或为零的文件中——这样的文件虽然存在，但无法解析，也无法与故意
// 这样编写的文件相区分。
//
// 父目录必须已存在。与 WriteFile 不同，它不会创建任何内容，因此无法代表调用者决定目录的权限。临时文件以目
// 标目录的名称作为名称，因此崩溃后留下的文件会表明它原本被替换了什么。
//
// 错误并不总是意味着目标对象未被修改，重新尝试的调用者需要知道它得到了什么。直到包括重命名操作在内的每一
// 次失败都不会改变原有内容，也不会留下临时文件。最终目录同步失败则是个例外：替换内容已经发布并可读取，唯
// 一不确定的是在异常关闭情况下重命名操作是否会保留下来。无论哪种情况，重新尝试都是安全的。
func DurableWrite(path string, data []byte, perm os.FileMode) error {
	p := filepath.Clean(path)
	p = InlineTilde(p)

	dir := filepath.Dir(p)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(p)+".*.tmp")
	if err != nil {
		return err
	}
	name := tmp.Name()

	// 下面的每个步骤都通过句柄进行，而不是通过路径，因此没有一个能落在其他写入者以该名称存放的文件上，
	// 任何失败的操作都会将临时文件带离。
	fail := func(err error) error {
		Close(tmp)
		_ = os.Remove(name)
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		return fail(err)
	}
	// 模式设置应在同步之前进行，而非之后：同步操作会清空 inode 以及数据，因此写入磁盘的模式也是该保障
	// 的一部分。在之后进行 chmod 操作会导致在文件发布时出现崩溃，因为文件会以临时文件自身的创建模式（
	// 0600）而非权限模式存储——仅对记录而言，这是无害的，因为只有写入者能读取，但对于容器必须读取的文
	// 件来说则不然。
	if err := tmp.Chmod(perm); err != nil {
		return fail(err)
	}
	if err := tmp.Sync(); err != nil {
		return fail(err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(name)
		return err
	}

	if err := os.Rename(name, p); err != nil {
		_ = os.Remove(name)
		return err
	}
	return syncDir(dir)
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
