// Package procattr 属性将主机进程绑定到 Kubernetes Pod 上，并将其容器化，该容器运行在 Pod 内部，通过读取
// /proc 下进程的 cgroup 归属信息实现。
//
// 厂商加速器库能够报告哪些主机`PID`持有某个设备，以及它们持有的设备数量，但无法说明这些进程属于何种工作负
// 载。该工具包提供了这种关联中缺失的那一半信息，其规则是：即使有错误但合理的归属也优于没有归属。任何模糊
// 的 cgroup 结构——两个 Pod 祖先、不同 Pod 的 cgroup v1 控制器、仅出现在较长内存段中的 Pod UID——都会被拒绝，
// 并给出明确的原因，而非通过偏好来解决。
//
// 结果有三个，而不是两个，第三个原因就是为什么这个包可以被用来计算总和。归属于某个容器的进程会贡献其使用
// 量。属于 Pod 的进程会被节点识别，且不会支持任何 Instance，排除这些进程也不会产生任何变化。那些根本无法
// 分类的进程会使得设备上的所有数值都无法测量——因为缺少一个项的总和并不等于更小的数值，而是错误的数字，调
// 用者也无法仅凭数值来区分两者。
package procattr

import (
	"errors"
	"io/fs"
	"path"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"
)

// 身份指的是主机进程的 Kubernetes 身份：它所属的 Pod，以及该 Pod 内的容器。
//
// 需要两个部分。加速器分配会按容器进行持久化存储，之后才会进行整个 Pod 范围内的汇总，因此仅有一个 Pod UID
// 无法说明哪个分配负责某个进程，也无法确认“每个切片容器对应一个加速器”的约束条件是否成立。
type Identity struct {
	// PodUID 是 Pod 的 UID，无论使用哪种 cgroup 驱动程序读取路径，其格式均为小写字母并带连字符。
	PodUID string
	// ContainerID 是容器的运行时 ID，根据 cgroup 路径的表示方式，它就是纯 ID，没有 systemd 单元的运行
	// 时前缀，也没有 Pod 状态所报告的 "<runtime>://" 方案。
	ContainerID string
}

// 结果是指归因对某个过程得出的结论。
type Outcome uint8

const (
	// OutcomeUnattributed 表示该过程无法被分类。这是零值，因此从未被填充的 Result 无法被视为可归因的。
	// Reason 会说明拒绝的原因，调用者必须将该样本中的过程设备视为不可测量。
	OutcomeUnattributed Outcome = iota
	// OutcomeAttributed 表示该过程属于由 Identity 命名的容器。
	OutcomeAttributed
	// OutcomeExcluded 表示该进程属于某个节点正在运行的 Pod，且该 Pod 不支持任何 Instance。虽然其使用
	// 是真实的，但它不是任何人的份额，也不存在依赖于它的份额数据，因此将其移除后，设备剩余的指标仍可
	// 被测量。
	OutcomeExcluded
)

// 原因名称表明归因拒绝生成身份信息。这些值是稳定的字符串，因为它们作为标签发布在 Device Manager 的能力指
// 标上：有明确原因但无数据的状态可以诊断出来，没有原因且无数据的状态则属于未知情况。
type Reason string

const (
	// reasonNone 表示没有拒绝，在 Result 内部不会传递给调用者。
	reasonNone Reason = ""
	// ReasonNoPodComponent 表示进程 cgroup 路径中没有名为 Pod 的内存段，这是主机上的进程而非容器中的
	// 进程所呈现的面貌。
	ReasonNoPodComponent Reason = "no_pod_component"
	// ReasonAmbiguousPod 指的是名为多个 Pod 的路径——或在 cgroup v1 中，一系列控制器路径。
	ReasonAmbiguousPod Reason = "ambiguous_pod"
	// ReasonNoContainerComponent 表示某个 Pod 已命名，但下方没有容器。在单个进程中运行整个 Pod 的沙箱
	// 运行时环境如下所示，因此无法将其费用计入容器的分配中。
	ReasonNoContainerComponent Reason = "no_container_component"
	// ReasonAmbiguousContainer 表示在 Pod 下方有不止一个容器被命名。
	ReasonAmbiguousContainer Reason = "ambiguous_container"
	// ReasonUnknownPod 指的是一个语法上有效的 Pod UID 对应的 cgroup 路径，该节点的 Pod 索引并未包含该
	// 路径。该索引只是一个可能尚未更新的快照，因此该进程无法被归属到任何 Instance 中。
	ReasonUnknownPod Reason = "unknown_pod"
	// ReasonUnknownContainer 表示该 Pod 属于已知的 Instance，但容器 ID 与状态报告中的任何容器都不匹配。
	// 在此无法做出安全的猜测。
	ReasonUnknownContainer Reason = "unknown_container"
	// ReasonExited 表示 /proc 中没有该进程的条目，即进程在厂商查询与本次读取之间结束的常规过程。
	ReasonExited Reason = "exited"
	// ReasonPermission 表示 /proc 拒绝了读取。与 ReasonExited 不同，此情况会拒绝节点上的每个进程，直
	// 到部署问题得到解决，因此两者是不同的。
	ReasonPermission Reason = "permission"
	// ReasonUnreadable 表示 /proc 既未返回数据，也未出现上述两种错误中的任何一种。
	ReasonUnreadable Reason = "unreadable"
	// ReasonZombie 表示该进程仅被保留用于其退出状态，因此它不包含任何内容，且名为该进程的厂商行仍被视
	// 为过时的。
	ReasonZombie Reason = "zombie"
	// ReasonUnstable 表示在读取过程中进程的身份发生了变化，或者 PID 的架构被重新用于不同的 Pod。
	ReasonUnstable Reason = "unstable"
	// ReasonMediated 表示该进程代表其他 Pods 管理设备，因此它的使用既不属于该 Pod 自身，也无法在所服
	// 务的 Pods 之间分配。
	ReasonMediated Reason = "mediated"
	// ReasonInvisible 表示厂商从该进程无法访问的进程命名空间报告了 PIDs，因此这些 PID 根本不在 /proc
	// 中。这是部署问题，而非竞争问题。
	ReasonInvisible Reason = "pid_namespace_invisible"
)

// Result 是针对某个流程所得出的结论归属。
type Result struct {
	// 结果就是结论。
	Outcome Outcome
	// 当 Outcome 为 OutcomeAttributed 时，身份信息才会被填充。
	Identity Identity
	// 仅当 Outcome 为 OutcomeUnattributed 时，Reason 才会被填充。
	Reason Reason
}

// Pod 是指节点正在运行的某个 Pod 的相关信息。
type Pod struct {
	// Instance 报告该 Pod 是否支持某个 Instance。那些不支持的 Pod 仍然值得保留：知道某个进程属于某个
	// Instance，正是让该进程在替代设备样本时不会造成致命问题，从而确保其安全。
	Instance bool
	// Containers 保存了 Pod 中容器的运行时 IDs，其状态报告采用 `"<runtime>://"` 方案，且已进行了去重
	// 处理，因此可以直接与 cgroup 路径对应的内容进行比较。
	Containers sets.Set[string]
}

// PodIndex 保存了节点正在运行的每个 Pod，按 Pod UID 进行键序排列。
//
// 索引对应的 UID 未知，并非不存在：该索引只是一个快照，可能尚未包含刚刚启动的 Pod，因此未列出的 Pod 中的
// 进程无法被归属，而非假设没有`实例`。
type PodIndex map[string]Pod

// mediatingProcesses 是那些代表其他进程管理设备的可执行文件，其匹配规则与 /proc/<pid>/comm 相关。
//
// 这些名称按照内核报告的方式存储——被截断为 15 个字符——因此一个条目同时涵盖了 MPS 控制守护进程以及它所生成
// 的 MPS 服务器。
var mediatingProcesses = sets.New("nvidia-cuda-mps")

// Resolver 属性处理流程通过读取/proc 树来实现。它在调用之间不保留状态，因此调用者传入的 Pod 索引始终指向
// 当前有效的那个样本。
type Resolver struct {
	fsys fs.FS
}

// New 从 fsys 中获取 Resolver 的读取进程身份，该操作由位于主机 /proc 根目录的节点上的调用者执行：
//
// procattr.New(os.DirFS("/proc"))
func New(fsys fs.FS) *Resolver {
	return &Resolver{fsys: fsys}
}

// 根据厂商库为设备报告的每个 PID 属性，与节点运行的 Pods 进行比对。结果中每个不同的 PID 对应一个条目。
//
// 一起解析这一组 PID，可以发现进程命名空间配错的情况。如果驱动报告的所有 PID 都不在 /proc 中，应检查
// Device Manager 是否使用了宿主机的 PID 命名空间，而不能把每个进程都算作刚好退出。
func (r *Resolver) Resolve(index PodIndex, pids []uint32) map[uint32]Result {
	results := make(map[uint32]Result, len(pids))
	observed := false
	for _, pid := range pids {
		if _, seen := results[pid]; seen {
			continue
		}
		result, existed := r.resolveOne(index, pid)
		observed = observed || existed
		results[pid] = result
	}

	// 每个报告的 PID 都缺失于 /proc 中，且从未观察到其存在：厂商所报告的内容来自读者无法看到的进程命
	// 名空间。仅有的一致性并不能作为证据——一个在以下两次读取之间结束的单独进程，会留下完全相同的理由—
	// —因此，只要有一次成功的读取，就能排除这种可能性，避免将普通的竞争情况报告为部署故障。
	if !observed && allExited(results) {
		for pid, result := range results {
			result.Reason = ReasonInvisible
			results[pid] = result
		}
	}
	return results
}

// resolveOne 用于标记一个进程，并报告该进程是否被观察到存在。
//
// 校验的顺序是错误答案可能造成的损失最大的顺序：身份必须存在，才能被信任。在查询之前，也必须被信任。
func (r *Resolver) resolveOne(index PodIndex, pid uint32) (Result, bool) {
	identity, reason := r.readIdentity(pid)
	switch {
	case reason == ReasonExited, reason == ReasonPermission, reason == ReasonUnreadable:
		// 无法读取的 /proc 条目无法提供关于该进程的任何信息。
		return Result{Reason: reason}, false
	case reason != reasonNone:
		// 条目已被读取。仅其内容无法使用。
		return Result{Reason: reason}, true
	}

	if reason := r.readLiveness(pid); reason != reasonNone {
		return Result{Reason: reason}, true
	}

	if reason := r.readMediation(pid); reason != reasonNone {
		return Result{Reason: reason}, true
	}

	// 请再次确认身份，因此本次关闭的窗口会覆盖之前所有读取的内容。在厂商查询与当前位置之间被重新分配
	// 给另一个 Pod 的 PID，否则将由当前持有它的人负责。若产生分歧，则会拒绝该行记录。
	//
	// 失败的第二次读取并不构成冲突——可能只是进程已经结束，而结束并不会改变其运行的 Pod 信息——因此只有
	// 成功的、不同的读取才会被拒绝。剩余的窗口期，即第一次读取之前完成的重用操作，无法仅通过数字 PID
	// 来关闭，其范围由两次读取之间的差距决定。
	if second, reason := r.readIdentity(pid); reason == reasonNone && second != identity {
		return Result{Reason: ReasonUnstable}, true
	}

	pod, known := index[identity.PodUID]
	switch {
	case !known:
		return Result{Reason: ReasonUnknownPod}, true
	case !pod.Instance:
		return Result{Outcome: OutcomeExcluded}, true
	case !pod.Containers.Has(identity.ContainerID):
		return Result{Reason: ReasonUnknownContainer}, true
	}
	return Result{Outcome: OutcomeAttributed, Identity: identity}, true
}

// readIdentity 用于读取并解析进程的 cgroup 归属信息。
func (r *Resolver) readIdentity(pid uint32) (Identity, Reason) {
	content, err := r.readProcFile(pid, "cgroup")
	if err != nil {
		return Identity{}, readFailureReason(err)
	}
	return parseCgroup(string(content))
}

// readLiveness 拒绝处理内核仅用于其退出状态的进程。此类进程不占用任何设备内存，因此将其标记为厂商行的记录
// 属于过时的记录，其数值也不属于该样本的真实值。
func (r *Resolver) readLiveness(pid uint32) Reason {
	content, err := r.readProcFile(pid, "stat")
	if err != nil {
		return readFailureReason(err)
	}
	state, ok := parseProcState(string(content))
	if !ok {
		return ReasonUnreadable
	}
	if state == 'Z' {
		return ReasonZombie
	}
	return reasonNone
}

// readMediation 会拒绝那些代表其他 Pods 进程管理设备的进程。
//
// 将整卡的使用情况归咎于其自身的 Pod 是不对的，从设备的进程列表中也无法将其分配到它所服务的 Pods 中——那些
// Pods 的进程根本不会出现在那里。因此，对整卡而言，诚实的答案是“不可测量”，这就是此处拒绝回答所得到的结果。
func (r *Resolver) readMediation(pid uint32) Reason {
	content, err := r.readProcFile(pid, "comm")
	if err != nil {
		return readFailureReason(err)
	}
	if mediatingProcesses.Has(strings.TrimSpace(string(content))) {
		return ReasonMediated
	}
	return reasonNone
}

func (r *Resolver) readProcFile(pid uint32, name string) ([]byte, error) {
	return fs.ReadFile(r.fsys, path.Join(strconv.FormatUint(uint64(pid), 10), name))
}

// readFailureReason 类用于分类 `/proc` 读取失败情况。前两种情况被区分开，因为它们的含义截然不同：在厂商查
// 询与本次读取之间结束的进程属于普通的竞争现象，下一次样本采集可以从中恢复。而权限失败则会拒绝节点上的所
// 有进程，直到部署变更为止。
func readFailureReason(err error) Reason {
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return ReasonExited
	case errors.Is(err, fs.ErrPermission):
		return ReasonPermission
	}
	return ReasonUnreadable
}

// parseProcState 返回 /proc/<pid>/stat 行中的状态字段。扫描从最后一个 `)` 之后开始，因为其前面的字段是可
// 执行文件名，而文件名中可能包含空格和括号。
func parseProcState(stat string) (byte, bool) {
	i := strings.LastIndex(stat, ")")
	if i < 0 {
		return 0, false
	}
	rest := strings.TrimLeft(stat[i+1:], " ")
	if rest == "" {
		return 0, false
	}
	return rest[0], true
}

// allExited 报告每个进程是否未出现在 /proc 树中，这正是进程命名空间在外部看来的样子。
func allExited(results map[uint32]Result) bool {
	if len(results) == 0 {
		return false
	}
	for _, result := range results {
		if result.Reason != ReasonExited {
			return false
		}
	}
	return true
}
