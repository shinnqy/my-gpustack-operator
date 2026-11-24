package deviceplugin

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	// sysfsAttrMaxBytes 限定一次读取的属性大小。sysfs 属性应是短值，超长内容会作为错误返回。
	sysfsAttrMaxBytes = 64 << 10

	sysfsInfinibandDir = "class/infiniband"

	// sysfsVerbsClassName 是内核给 verbs 字符设备使用的类名。它既出现在 class 目录中，
	// 也出现在硬件父设备下的 <parent>/infiniband_verbs/uverbsN 路径中。
	sysfsVerbsClassName = "infiniband_verbs"
	sysfsVerbsClassDir  = "class/" + sysfsVerbsClassName

	// devInfinibandDir 是 Linux 放置 RDMA 字符设备的目录。
	// sysfs 中的 uverbsN 与这个目录中的设备节点同名。
	devInfinibandDir = "/dev/infiniband"

	ibdevAttr        = "ibdev"
	verbsEntryPrefix = "uverbs"
)

// outsideRootError 表示解析符号链接后路径落在 sysfs 根目录以外。调用方遇到它要终止查找，
// 不能将其当作普通的属性缺失。
type outsideRootError struct {
	rel      string
	resolved string
	root     string
}

func (e *outsideRootError) Error() string {
	return fmt.Sprintf("sysfs path %q resolves to %q, outside %q", e.rel, e.resolved, e.root)
}

func isOutsideRoot(err error) bool {
	var outside *outsideRootError
	return errors.As(err, &outside)
}

// verbsDevicePath 把库存中的 RDMA 设备名解析成 /dev/infiniband 下的 uverbs 路径。
// 优先扫描 class/infiniband_verbs/uverbsN/ibdev，
// 寻找内容与 RDMA 设备名相同的条目。若这个布局没有给出匹配，
// 再沿 class/infiniband/<设备名>/device 找到硬件父设备，
// 查找其 infiniband_verbs 子目录。
//
// 根目录作为参数传入，以便复用同一套 Linux 路径解析做测试。此处根据 sysfs 推导路径，容器运行时负责打开设备。
func verbsDevicePath(sysfsRoot, name string) (string, error) {
	if name == "" {
		return "", errors.New("the RDMA device name is empty")
	}
	root, err := filepath.EvalSymlinks(sysfsRoot)
	if err != nil {
		return "", fmt.Errorf("resolve the sysfs root %q: %w", sysfsRoot, err)
	}

	entry, unresolvable, err := scanVerbsEntries(root, filepath.Join(root, sysfsVerbsClassDir), name)
	if err != nil {
		return "", err
	}
	if entry != "" {
		return filepath.Join(devInfinibandDir, entry), nil
	}

	// 第二种布局以 RDMA 设备的类条目为起点，沿 device 链接找到内核挂载 verbs 字符设备的硬件父目录。
	anchor := filepath.Join(root, sysfsInfinibandDir, name, "device")
	parent, aerr := resolveInside(root, anchor)
	switch {
	case aerr == nil:
		entry, unresolvable2, err := scanVerbsEntries(
			root, filepath.Join(parent, sysfsVerbsClassName), name,
		)
		if err != nil {
			return "", err
		}
		if entry != "" {
			return filepath.Join(devInfinibandDir, entry), nil
		}
		unresolvable = errors.Join(unresolvable, unresolvable2)
	case isOutsideRoot(aerr):
		return "", aerr
	case os.IsNotExist(aerr) && !presentNoFollow(anchor) && !danglingClassEntry(root, name):
		// 该布局可以没有硬件父设备。如果本来就没有 device 链接，并且 RDMA 类条目也没有悬空，就继续按正常缺失处理。
	default:
		// 如果链接存在却无法解析，就无法确定其指向的硬件目录里是否有匹配项，应保留这个错误。
		unresolvable = errors.Join(unresolvable, fmt.Errorf(
			"resolve the device link of %q: %w", filepath.Join(sysfsInfinibandDir, name), aerr,
		))
	}

	if unresolvable != nil {
		return "", fmt.Errorf(
			"whether RDMA device %q has a verbs character device could not be established "+
				"under %q: %w",
			name, sysfsRoot, unresolvable,
		)
	}
	return "", fmt.Errorf(
		"no verbs character device for RDMA device %q under %q: tried both sysfs layouts, "+
			"%q (the verbs class directory) and the %q directory under %q (the RDMA "+
			"device's hardware parent)",
		name, sysfsRoot, sysfsVerbsClassDir, sysfsVerbsClassName,
		filepath.Join(sysfsInfinibandDir, name, "device"),
	)
}

// scanVerbsEntries 在给定目录查找 ibdev 属性等于目标设备名的 uverbs 条目。
// 其他条目读失败时先记录，后面找到有效匹配仍可返回。最终没有匹配时，需要区分“确认没有”和“有候选但读不到”。
// 一旦链接越出根目录就直接报错。
func scanVerbsEntries(realRoot, dir, name string) (entry string, unresolvable, err error) {
	if _, derr := resolveInside(realRoot, dir); derr != nil {
		if isOutsideRoot(derr) {
			return "", nil, derr
		}
		if !os.IsNotExist(derr) || presentNoFollow(dir) {
			return "", fmt.Errorf("resolve the uverbs candidates under %s: %w", dir, derr), nil
		}
		// 节点可能未提供这一种 sysfs 布局，目录确实不存在时可继续检查另一种布局。
		return "", nil, nil
	}

	entries, rerr := os.ReadDir(dir)
	if rerr != nil {
		return "", fmt.Errorf("list %s: %w", dir, rerr), nil
	}
	for _, candidate := range entries {
		if !strings.HasPrefix(candidate.Name(), verbsEntryPrefix) {
			continue
		}
		ibdev, aerr := readAttr(realRoot, filepath.Join(dir, candidate.Name(), ibdevAttr))
		if aerr != nil {
			if isOutsideRoot(aerr) {
				return "", nil, aerr
			}
			unresolvable = errors.Join(unresolvable, aerr)
			continue
		}
		if ibdev == name {
			return candidate.Name(), nil, nil
		}
	}
	return "", unresolvable, nil
}

// resolveInside 解析路径中的符号链接，并核对最终位置仍在指定根目录下。sysfs 类条目本身可能就是符号链接。
func resolveInside(realRoot, path string) (string, error) {
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	if resolved != realRoot && !strings.HasPrefix(resolved, realRoot+string(os.PathSeparator)) {
		return "", &outsideRootError{rel: path, resolved: resolved, root: realRoot}
	}
	return resolved, nil
}

// readAttr 读取单个 sysfs 属性，限制最大长度并去掉换行等首尾空白。
func readAttr(realRoot, path string) (string, error) {
	p, err := resolveInside(realRoot, path)
	if err != nil {
		return "", err
	}
	f, err := os.Open(p)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()

	buf, err := io.ReadAll(io.LimitReader(f, sysfsAttrMaxBytes+1))
	if err != nil {
		return "", err
	}
	if len(buf) > sysfsAttrMaxBytes {
		return "", fmt.Errorf("sysfs attribute %q exceeds %d bytes", path, sysfsAttrMaxBytes)
	}
	return strings.TrimSpace(string(buf)), nil
}

// presentNoFollow 使用 Lstat 判断路径自身是否存在，以区分布局缺失和现有链接无法解析。
func presentNoFollow(path string) bool {
	_, err := os.Lstat(path)
	return err == nil
}

// danglingClassEntry 检查 RDMA 类条目本身是否为符号链接。仅在上层查找已失败后调用，
// 以区分悬空类链接和确实没有硬件父目录的设备。
func danglingClassEntry(root, name string) bool {
	info, err := os.Lstat(filepath.Join(root, sysfsInfinibandDir, name))
	return err == nil && info.Mode()&os.ModeSymlink != 0
}
