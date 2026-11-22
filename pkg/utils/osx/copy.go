package osx

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

type CopyOptions func(*copyOptions)

// CopyWithModifier 配置用于在复制文件时修改内容的修改器，
// 仅在不使用缓冲区时生效。
func CopyWithModifier(m func([]byte) ([]byte, error)) CopyOptions {
	return func(o *copyOptions) {
		o.modify = m
	}
}

type copyOptions struct {
	ignore        bool
	replace       bool
	shadow        bool
	preserveTimes bool
	preserveOwner bool
	perm          *os.FileMode
	buffer        bool
	modify        func([]byte) ([]byte, error)
}

func Copy(src, dst string, opts ...CopyOptions) error {
	var o copyOptions

	for i := range opts {
		if opts[i] == nil {
			continue
		}

		opts[i](&o)
	}

	srcInfo, err := os.Lstat(src)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) && o.ignore {
			return nil
		}

		return err
	}

	var cp func(string, string, os.FileInfo, copyOptions) error

	for {
		switch m := srcInfo.Mode(); {
		default:
			return fmt.Errorf("unsupported src mode: %v", m)
		case m&os.ModeSymlink != 0:
			if src, err = os.Readlink(src); err != nil {
				return fmt.Errorf("read origin of link src: %w", err)
			}

			if srcInfo, err = os.Lstat(src); err != nil {
				return fmt.Errorf("stat origin of link src: %w", err)
			}

			continue
		case m.IsDir():
			cp = copyFromDir
		case m.IsRegular():
			cp = copyFromFile
		}

		break
	}

	dstInfo := srcInfo

	if s, err := os.Lstat(dst); err == nil {
		if o.replace {
			if err = os.RemoveAll(dst); err != nil {
				return fmt.Errorf("cannot clean dst: %w", err)
			}
		}

		dstInfo = s
	}

	if o.shadow {
		if err = os.Symlink(src, dst); err != nil {
			return fmt.Errorf("cannot shadow dst: %w", err)
		}

		return nil
	}

	defer func() {
		m := dstInfo.Mode()
		if o.perm != nil {
			m = *o.perm
		}

		if err = os.Chmod(dst, m); err != nil {
			err = fmt.Errorf("cannot change perm: %w", err)
		}
	}()

	if err = cp(src, dst, srcInfo, o); err != nil {
		return fmt.Errorf("cannot copy: %w", err)
	}

	if o.preserveTimes {
		aTime, mTime, _ := fileTimes(srcInfo)
		if err = os.Chtimes(dst, aTime, mTime); err != nil {
			return fmt.Errorf("cannot preserve times: %w", err)
		}
	}

	if o.preserveOwner {
		uid, gid := fileOwner(srcInfo)
		if err = os.Lchown(dst, uid, gid); err != nil {
			return fmt.Errorf("cannot preserve owner: %w", err)
		}
	}

	return nil
}

func copyFromDir(src, dst string, srcInfo os.FileInfo, o copyOptions) error {
	// 切换目录权限以便复制。
	if err := os.Mkdir(dst, 0o666); err != nil {
		if !os.IsExist(err) {
			return err
		}

		if err = os.Chmod(dst, 0o666); err != nil {
			return err
		}
	}

	return filepath.Walk(src, func(srcSub string, srcSubInfo fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		sub, err := filepath.Rel(src, srcSub)
		if err != nil {
			return err
		}
		dstSub := filepath.Join(dst, sub)

		switch m := srcSubInfo.Mode(); {
		default:
			// 不支持的源模式。
			return nil
		case m&os.ModeSymlink != 0:
			if srcSub, err = os.Readlink(srcSub); err != nil {
				return err
			}

			if err = os.Symlink(srcSub, dstSub); err != nil {
				return err
			}
		case m.IsDir():
			if err = os.Mkdir(dstSub, srcSubInfo.Mode()); err != nil && !os.IsExist(err) {
				return err
			}
		case m.IsRegular():
			if err = copyFromFile(srcSub, dstSub, srcSubInfo, o); err != nil {
				return err
			}
		}

		if o.preserveTimes {
			aTime, mTime, _ := fileTimes(srcInfo)
			if err = os.Chtimes(dstSub, aTime, mTime); err != nil {
				return err
			}
		}

		if o.preserveOwner {
			uid, gid := fileOwner(srcInfo)
			if err = os.Lchown(dstSub, uid, gid); err != nil {
				return err
			}
		}

		if srcSubInfo.Mode()&os.ModeSymlink == 0 {
			if err = os.Chmod(dstSub, srcSubInfo.Mode()); err != nil {
				return err
			}
		}

		return nil
	})
}

func copyFromFile(src, dst string, scrInfo os.FileInfo, o copyOptions) error {
	dstDir := filepath.Dir(dst)
	if !Exists(dstDir) {
		if err := os.MkdirAll(dstDir, 0o666); err != nil {
			return err
		}
	}

	if o.buffer {
		// 使用缓冲区复制。
		srcFile, err := os.Open(src)
		if err != nil {
			return err
		}

		defer func() { _ = srcFile.Close() }()

		dstFile, err := os.Create(dst)
		if err != nil {
			return err
		}

		defer func() { _ = dstFile.Close() }()

		_, err = io.Copy(srcFile, dstFile)

		return err
	}

	content, err := os.ReadFile(src)
	if err != nil {
		return err
	}

	if o.modify != nil {
		content, err = o.modify(content)
		if err != nil {
			return err
		}
	}

	return os.WriteFile(dst, content, 0o600)
}
