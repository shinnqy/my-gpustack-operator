package store

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Mount 表示 /proc/self/mountinfo 中的一行，仅保留引用重建所需的字段。
type Mount struct {
	// Device 是挂载源所在文件系统的 "major:minor" 设备号。
	Device string
	// Root 是挂载到 MountPoint 的目录在其文件系统内的路径：对于 bind mount 即为其源目录。
	// 即使账本丢失，也可据此将挂载目标反向追溯回对应摘要。
	Root string
	// MountPoint 是挂载点在命名空间中的可见路径。
	MountPoint string
}

// ParseMountInfo 解析 mountinfo 的每一行。
func ParseMountInfo(r io.Reader) ([]Mount, error) {
	var mounts []Mount
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		if len(f) < 5 {
			return nil, fmt.Errorf("short mountinfo line %q", sc.Text())
		}
		mounts = append(mounts, Mount{Device: f[2], Root: unescapeMountInfo(f[3]), MountPoint: unescapeMountInfo(f[4])})
	}

	return mounts, sc.Err()
}

// unescapeMountInfo 还原 mountinfo 对空格、制表符、换行符和反斜杠使用的八进制转义。
func unescapeMountInfo(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+4 <= len(s) {
			if v, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(v))
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}

	return b.String()
}

var (
	// publishedRootPattern 匹配缓存文件系统内部 bind mount 源目录路径：仅当缓存与其他目录共享
	// 文件系统时，前面才带有缓存根在文件系统中的前缀路径。
	publishedRootPattern = regexp.MustCompile(`/` + publishedDir + `/([0-9a-f]{64})/` + treeDir + `$`)
	// csiTargetPattern 匹配 kubelet 为 CSI 卷分配的目标路径：
	// <kubelet>/pods/<pod UID>/volumes/kubernetes.io~csi/<volume>/mount。
	csiTargetPattern = regexp.MustCompile(`/pods/([^/]+)/volumes/kubernetes\.io~csi/([^/]+)/mount$`)
)

// Rebuilt 记录启动时重建引用表的结果分类。
type Rebuilt struct {
	// Kept 表示账本中有记录且实际挂载的摘要与账本一致。
	Kept []Ref
	// Corrected 表示账本中有记录但实际挂载了其他摘要；以实际挂载为准，因为那是消费方看到的内容。
	Corrected []Ref
	// Adopted 表示节点上存在实际挂载但账本中缺少对应记录。
	Adopted []Ref
	// Dropped 表示账本中有记录但对应挂载已不存在。
	Dropped []Ref
}

// Rebuild 根据节点当前挂载表和本地账本重建引用关系，并同步重写账本。
// device 是缓存文件系统的 "major:minor" 设备号，用于排除路径恰好以 published/<hex>/tree 结尾的无关挂载。
func (s *Store) Rebuild(mounts []Mount, device string) (Rebuilt, error) {
	mounted := map[string]string{}
	for _, m := range mounts {
		if m.Device != device || !csiTargetPattern.MatchString(m.MountPoint) {
			continue
		}
		if g := publishedRootPattern.FindStringSubmatch(m.Root); g != nil {
			mounted[m.MountPoint] = g[1]
		}
	}

	ledger, err := s.Refs()
	if err != nil {
		return Rebuilt{}, err
	}
	var out Rebuilt
	seen := map[string]bool{}
	for _, r := range ledger {
		seen[r.TargetPath] = true
		hex, ok := mounted[r.TargetPath]
		switch {
		case !ok:
			out.Dropped = append(out.Dropped, r)
			if err := s.RemoveRef(r.VolumeID); err != nil {
				return out, err
			}
		case hex != r.Hex:
			r.Hex = hex
			out.Corrected = append(out.Corrected, r)
			if err := s.WriteRef(r); err != nil {
				return out, err
			}
		default:
			out.Kept = append(out.Kept, r)
		}
	}

	targets := make([]string, 0, len(mounted))
	for t := range mounted {
		if !seen[t] {
			targets = append(targets, t)
		}
	}
	sort.Strings(targets)
	for _, t := range targets {
		g := csiTargetPattern.FindStringSubmatch(t)
		r := Ref{TargetPath: t, Hex: mounted[t], PodUID: g[1], VolumeID: EphemeralVolumeID(g[1], g[2])}
		out.Adopted = append(out.Adopted, r)
		if err := s.WriteRef(r); err != nil {
			return out, err
		}
	}

	return out, nil
}
