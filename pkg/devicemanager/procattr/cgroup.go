package procattr

import (
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"
)

const (
	// podSegmentPrefix 是 cgroup 驱动程序在 Pod UID 前面所使用的标识：cgroupfs 驱动程序会写成“pod<uid
	// >”格式，而 systemd 驱动程序则会在其切片名称末尾加上“-pod<uid>”。
	podSegmentPrefix = "pod"
	// sliceSuffix 和 scopeSuffix 标识了 kubelet 创建的两种系统 d 单元类型——每个 Pod 对应一个切片，每
	// 个容器对应一个范围。
	sliceSuffix = ".slice"
	scopeSuffix = ".scope"
	// uuidLength 是指 UUID 的标准连字符形式的长度，这正是普通 Pod 的 UID 的规格。
	uuidLength = 36
	// hashedUIDLength 是静态 Pod 的 UID 长度，kubelet 是从 Pod 的名称中获取该长度，而非分配一个 UUID：
	// 即 32 个不分隔的十六进制数字。
	hashedUIDLength = 32
	// minContainerIDLength 是作为容器 ID 被接受的最短十六进制字符串长度。其设定是为了避免一些偶然出现
	// 的短十六进制片段——如队列深度、索引等——被误判为容器 ID，同时该值刻意低于运行时实际写入的 64 个字
	// 符数。
	minContainerIDLength = 8
)

// containerScopePrefixes 是 systemd-driver 容器作用域在容器 ID 之前使用的运行时前缀。如果没有任何前缀的作
// 用域，则不会进行解析，而是会被猜测：否则就会将任意单元名称当作容器 ID 处理。
var containerScopePrefixes = []string{"cri-containerd-", "containerd-", "crio-", "docker-"}

// parseCgroup 会解析进程 cgroup 文件的内容，提取出 Pod 和容器名称，或者说明无法解析的原因。只有当原因为空
// 时，返回的身份信息才有意义。
//
// 会读取每一行数据，而不仅仅是第一行，因为 cgroup v1 会为进程分配一行数据给每个控制器层级，且这些层级必须
// 保持一致。这种一致性是经过验证而非假设：不同的控制器命名不同的 Pods，这是半迁移化进程的典型特征。选择其
// 中一个作为归属则只是将随机与测量混为一谈。命名相同 Pod 的线路与容器会合并为单一的归属，因此 v1 进程不会
// 因每个控制器而被计数一次。
func parseCgroup(content string) (Identity, Reason) {
	podUIDs, containerIDs := sets.New[string](), sets.New[string]()
	for _, line := range strings.Split(content, "\n") {
		// cgroup 行格式为“hierarchy-ID:controller-list:path”。v2 版本则写入单行“0::<path>”。任何
		// 更短的内容均不是 cgroup 行，不会产生任何影响。
		fields := strings.SplitN(strings.TrimSpace(line), ":", 3)
		if len(fields) != 3 {
			continue
		}
		pods, containers := parseCgroupPath(fields[2])
		podUIDs.Insert(pods...)
		containerIDs.Insert(containers...)
	}

	switch {
	case podUIDs.Len() == 0:
		return Identity{}, ReasonNoPodComponent
	case podUIDs.Len() > 1:
		return Identity{}, ReasonAmbiguousPod
	case containerIDs.Len() == 0:
		return Identity{}, ReasonNoContainerComponent
	case containerIDs.Len() > 1:
		return Identity{}, ReasonAmbiguousContainer
	}

	return Identity{
		PodUID:      podUIDs.UnsortedList()[0],
		ContainerID: containerIDs.UnsortedList()[0],
	}, reasonNone
}

// parseCgroupPath 会收集单个 cgroup 路径名称所对应的不同的 Pod UIDs，以及这些 UIDs 下方第一个 `UID` 所对
// 应的不同的容器 `ID`。
//
// 容器候选对象仅来自 Pod 段以下的内存段，这使得出现在容器自身名称中的 Pod UID 是安全的：它永远不会出现在
// 需要查找容器的位置，且 Pod 也永远不会通过子串被查找。
func parseCgroupPath(path string) (podUIDs, containerIDs []string) {
	segments := strings.Split(strings.Trim(path, "/"), "/")

	podAt := -1
	for i := range segments {
		uid, ok := parsePodUID(segments[i])
		if !ok {
			continue
		}
		podUIDs = append(podUIDs, uid)
		if podAt < 0 {
			podAt = i
		}
	}
	if podAt < 0 {
		return podUIDs, nil
	}

	for _, segment := range segments[podAt+1:] {
		if id, ok := parseContainerID(segment); ok {
			containerIDs = append(containerIDs, id)
		}
	}
	return podUIDs, containerIDs
}

// parsePodUID 会读取单个路径段名称中的 Pod UID，该名称可能采用 cgroup 驱动器的拼写形式：cgroupfs 驱动器的
// 纯“pod<uid>”，或 systemd 驱动器的“kubepods-<qos>-pod<uid>.slice”。其中 UID 中的连字符被转写为下划线，因
// 为 systemd 单元名称使用连字符作为分隔符。
//
// 该匹配项在两种拼写形式的 UID 两端都被锚定，因此仅包含“pod<uid>”的内存段——即一个缓存目录、一个以 Pod 命
// 名的容器单元——则不会产生任何结果。
func parsePodUID(segment string) (string, bool) {
	if uid, ok := strings.CutPrefix(segment, podSegmentPrefix); ok {
		return canonicalPodUID(uid)
	}

	slice, ok := strings.CutSuffix(segment, sliceSuffix)
	if !ok {
		return "", false
	}
	// 切片名称中仅最后一个用连字符分隔的组件可以包含 UID。其前面的所有内容均为所属的切片层级结构（"
	// kubepods-besteffort-"）。
	if i := strings.LastIndex(slice, "-"); i >= 0 {
		slice = slice[i+1:]
	}
	uid, ok := strings.CutPrefix(slice, podSegmentPrefix)
	if !ok {
		return "", false
	}
	return canonicalPodUID(uid)
}

// canonicalPodUID 可以验证 cgroup 路径中任何形式的 Pod UID，并以 Kubernetes 自身报告的格式返回该 UID。因
// 此，基于该 UID 的引用与从 API 中读取到的 Pod UID 相匹配，无论该路径是由哪种 cgroup 驱动程序生成的。
//
// 真实节点上存在两种格式。普通 Pod 的 UID 是 UUID 格式，systemd 驱动将其中的连字符转写为下划线，因为连字
// 符是 systemd 在单元名称中的分隔符。静态 Pod 的 UID 则完全不是 UUID 格式——kubelet 从 Pod 的名称中派生出
// 它，而 API 会原封不动地返回这 32 个未分隔的数字，因此它们会被原样返回。
//
// 拒绝第二个拼写会比仅仅不完整更糟糕。一个静态的 Pod 是节点已知的 Pod，没有 Instance 拥有它，因此识别其
// UID 正是能够安全将其排除在进程之外的关键。如果无法识别，则会导致设备上的所有数据都变得不可测量。
func canonicalPodUID(uid string) (string, bool) {
	separated := len(uid) == uuidLength
	if !separated && len(uid) != hashedUIDLength {
		return "", false
	}

	canonical := []byte(strings.ToLower(uid))
	for i := range canonical {
		switch {
		case separated && isUUIDSeparatorPosition(i):
			if canonical[i] != '-' && canonical[i] != '_' {
				return "", false
			}
			canonical[i] = '-'
		case !isHexDigit(canonical[i]):
			return "", false
		}
	}
	return string(canonical), true
}

// isUUIDSeparatorPosition 用于指示一个 UUID 是否在给定的偏移量位置带有分隔符。检查的是位置，而非字符，因
// 此无论分隔符出现在何处，都会被拒绝，而不会被视为另一个非数字的位置。
func isUUIDSeparatorPosition(i int) bool {
	return i == 8 || i == 13 || i == 18 || i == 23
}

// parseContainerID 解析单个路径段所代表的容器 ID：可能是 cgroupfs 驱动器的原始 ID，也可能是 systemd 驱动
// 器的 "<runtime>-<id>.scope"。该 ID 必须是一个完整的十六进制字符串，因为这就是运行时生成的格式，它还能防
// 止其他名称的 Scope 被误判为容器。
func parseContainerID(segment string) (string, bool) {
	id := segment
	if scope, ok := strings.CutSuffix(segment, scopeSuffix); ok {
		id = ""
		for _, prefix := range containerScopePrefixes {
			if rest, ok := strings.CutPrefix(scope, prefix); ok {
				id = rest
				break
			}
		}
	}

	// 在验证之前进行标准化，就像 canonicalPodUID 那样：isHexDigit 仅接受小写数字，因此先进行验证会拒
	// 绝标准化所设计的、可接受的大写拼写形式，并将其判定为“非容器”，而非仅因大小写差异而拒绝。
	id = strings.ToLower(id)
	if len(id) < minContainerIDLength {
		return "", false
	}
	for i := range len(id) {
		if !isHexDigit(id[i]) {
			return "", false
		}
	}
	return id, true
}

func isHexDigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')
}
