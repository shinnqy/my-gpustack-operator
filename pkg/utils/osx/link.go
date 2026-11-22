package osx

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type LinkOptions func(*linkOptions)

// LinkEvenIfNotFound 链接 src，
// 如果未找到则使用给定权限创建 src。
func LinkEvenIfNotFound(isFile bool, perm os.FileMode) LinkOptions {
	return func(o *linkOptions) {
		o.create = true
		o.createFile = isFile

		if perm != 0 {
			o.createPerm = &perm
		}
	}
}

// LinkInReplace 在找到 dst 时将其删除。
func LinkInReplace() LinkOptions {
	return func(o *linkOptions) {
		o.replace = true
	}
}

type linkOptions struct {
	create        bool
	createFile    bool
	createPerm    *os.FileMode
	replace       bool
	real          bool
	preserveTimes bool
	preserveOwner bool
}

func Link(src, dst string, opts ...LinkOptions) error {
	var o linkOptions

	for i := range opts {
		if opts[i] == nil {
			continue
		}

		opts[i](&o)
	}

	srcInfo, err := os.Lstat(src)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) || !o.create {
			return err
		}
		m := os.FileMode(0o666)

		if o.createPerm != nil {
			m = *o.createPerm
		}

		if !o.createFile {
			if err = os.MkdirAll(src, m); err != nil {
				return fmt.Errorf("cannot create src dir: %w", err)
			}
		} else {
			dstDir := filepath.Dir(dst)
			if !Exists(dstDir) {
				if err = os.MkdirAll(dstDir, 0o666); err != nil {
					return fmt.Errorf("cannot create dir of dst: %w", err)
				}
			}

			if _, err = os.Create(src); err != nil {
				return fmt.Errorf("cannot create src file: %w", err)
			}

			if err = os.Chmod(src, m); err != nil {
				return fmt.Errorf("cannot change src file mod: %w", err)
			}
		}
		srcInfo, _ = os.Stat(src)
	}

	if _, err := os.Lstat(dst); err == nil {
		if !o.replace {
			return errors.New("dst is not empty")
		}

		if err = os.Remove(dst); err != nil {
			return fmt.Errorf("cannot clean dst: %w", err)
		}
	} else {
		dstDir := filepath.Dir(dst)
		if !Exists(dstDir) {
			if err = os.MkdirAll(dstDir, 0o666); err != nil {
				return fmt.Errorf("cannot create dir of dst: %w", err)
			}
		}
	}

	if o.real {
		for srcInfo.Mode()&os.ModeSymlink != 0 {
			if src, err = os.Readlink(src); err != nil {
				return fmt.Errorf("read origin of link src: %w", err)
			}
			if srcInfo, err = os.Lstat(src); err != nil {
				return fmt.Errorf("stat origin of link src: %w", err)
			}
		}
	}

	if err = os.Symlink(src, dst); err != nil {
		return fmt.Errorf("cannot link dst: %w", err)
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
