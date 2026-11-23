package deviceplugin

import (
	"os"
	"path/filepath"

	"k8s.io/apimachinery/pkg/util/sets"
)

// IMPORTANT: 用来解决的问题是：
// 在逻辑切片模式下，设备插件会在宿主机固定目录（OperatorPodsDir）下，为每个 Pod 创建一个以 Pod UID 命名的子目录（放显存/算力限制配置文件和共享锁）。
// Pod 删掉后，kubelet 不会通知设备插件去清理这些宿主机目录（Device Plugin 协议压根没有 Release 回调！）。久而久之宿主机磁盘就会堆满垃圾目录。

// podDirGCMaxMisses 是 Pod UID 连续缺席多少轮协调后才允许回收其工作目录。
const podDirGCMaxMisses = 3

// podDirGC 每轮重扫切片工作目录，并与存活 UID 比较，因此进程重启后也能恢复清理。
// 只有连续多轮找不到同一 Pod 才删除，避免缓存短暂漏项导致误删。
type podDirGC struct {
	dir    string
	misses map[string]int // 记录每个 Pod UID 连续缺席的次数。
}

// newPodDirGC 创建目录回收器，从已有子目录登记 Pod UID，也能清理进程启动前遗留的孤儿目录。
func newPodDirGC(dir string) *podDirGC {
	g := &podDirGC{dir: dir, misses: make(map[string]int)}
	for uid := range g.scanPodUIDs() {
		g.misses[uid] = 0
	}
	return g
}

// scanPodUIDs 列出工作目录下以 Pod UID 命名的子目录。
func (g *podDirGC) scanPodUIDs() sets.Set[string] {
	uids := sets.New[string]()
	entries, err := os.ReadDir(g.dir)
	if err != nil {
		return uids
	}
	for _, e := range entries {
		if e.IsDir() {
			uids.Insert(e.Name())
		}
	}
	return uids
}

// reconcile 将磁盘目录与当前存活 UID 进行比较。重新出现的 UID 清零计数，
// 连续缺席达到 podDirGCMaxMisses 次才删除对应目录。
func (g *podDirGC) reconcile(livePodUIDs []string) {
	live := sets.New[string](livePodUIDs...)
	onDisk := g.scanPodUIDs()

	for uid := range onDisk {
		if live.Has(uid) {
			g.misses[uid] = 0
			continue
		}
		g.misses[uid]++
		if g.misses[uid] >= podDirGCMaxMisses {
			// 只有删除成功才移除跟踪项。权限或临时 I/O 错误发生时保留计数，让下轮直接重试。
			if err := os.RemoveAll(filepath.Join(g.dir, uid)); err == nil {
				delete(g.misses, uid)
			}
		}
	}

	// 对于已由其他途径删除的目录，一并清除跟踪项。
	for uid := range g.misses {
		if !onDisk.Has(uid) {
			delete(g.misses, uid)
		}
	}
}
