package hygon

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"
	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/pkg/utils/osx"
)

// migReclaimer 释放那些持有者已消失的分区。
//
// 它的存在是因为没有其他选择。设备插件协议没有释放回调，因此 Pod 的消失不会在驱动程序中释放任何资源：为它
// 准备的 GPU 实例会一直保留到节点生命周期结束。对于厂商来说，这比容量泄漏更糟糕——在任何实例存在时，无法关
// 闭多`实例`模式，因此无法将积累有废弃分区的节点恢复为完整卡片服务。
//
// `reconcile`机制基于层级：每次迭代都会传递当前的 Pod 集合，并销毁那些没有 Pod 的节点。不同迭代之间不会保
// 留任何信息，因此错过标记将导致延迟，而不会做出任何决策。
type migReclaimer struct {
	drv     migDriver
	podsDir string
	logger  klog.Logger
	// 现在以时钟的 grace 窗口为基准进行测量，用于测试的可注射物。
	now func() time.Time
}

// migReclaimGrace 表示新写入的归属记录被单独保留的时间长度。
//
// 本次通过的实时 Pod 集合是一个快照，在该快照之后写入的记录无法被识别——该 Pod 虽然处于实时状态，但尚不在
// 集合中。没有这个，大量已准入的节点就会失去其分区：以三个同时被准入的 Pod 为例，它们在启动过程中各自的实
// 例都被销毁，记录也被移除，然后全部被分配到同一个过时的分区。没有更便宜的键信号可用，因为在构建快照的分
// 配注释之前，就会先写入记录。
//
// 窗口的代价仅在于真正被废弃的分区上的延迟，下一轮处理会处理这个问题。
const migReclaimGrace = 2 * time.Minute

func newMigReclaimer(drv migDriver, podsDir string, logger klog.Logger) *migReclaimer {
	return &migReclaimer{drv: drv, podsDir: podsDir, logger: logger, now: time.Now}
}

// `reconcile`会破坏所有 Pod 已消失的分区，然后收集那些没有记录声称的分区。
//
// 这两部分是分开的，因为它们在确定性程度上有所不同。一个用于指代已死亡的 Pod 的记录是明确的：指的是属于该
// 分区但已不存在的事物。而没有记录的实例则不是——它同样可能是一个正在被创建的事物，由尚未写入记录的预留资
// 源所产生——因此只有当记录集足够完整以得到证明时，它才会被收集起来。
func (r *migReclaimer) reconcile(livePodUIDs []string) {
	live := sets.New(livePodUIDs...)

	// 每个名为“本次”的记录实例，无论是否存活或刚刚被释放，都适用。下面的“孤儿清理”操作仅针对那些没有
	// 任何记录提及的内容：本次已被销毁的实例并非“孤儿”，要求驱动程序再次销毁它，会与同时进行的保留操
	// 作产生冲突，因为后者已使用相同的 ID。
	entries, corrupt := scanMigMarkers(r.podsDir)
	seen := make(map[string]sets.Set[uint32], len(entries))
	for i := range entries {
		m := entries[i].marker
		r.note(seen, m)
		if live.Has(m.PodUID) {
			continue
		}
		if r.withinGrace(entries[i].path) {
			// 在本次任务交付后的快照中撰写，因此该快照并未提及此事。
			continue
		}
		r.release(entries[i])
	}

	r.sweepRuntimeDirs()

	if len(corrupt) != 0 {
		// 无法读取的记录可能归属于任何实例，因此不可能显示为未被占用。每次报告一次就值得了：一个
		// 从未清理该记录的运营商，其被废弃的分区将永远无法被收集。
		r.logger.Info("skipped the orphan-partition sweep because some ownership records are unreadable",
			"records", len(corrupt))
		return
	}
	r.collectOrphans(seen)
}

// withinGrace 用于报告是否有一条记录年龄太轻，无法由当前数据集进行评判。无法识别年龄的记录被视为年轻记录：
// 拒绝销毁是安全的选择。
func (r *migReclaimer) withinGrace(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return true
	}
	return r.now().Sub(info.ModTime()) < migReclaimGrace
}

// 注意记录：在本次过程中，某些所有权记录将此分区列为了自己的所有。
func (r *migReclaimer) note(seen map[string]sets.Set[uint32], m migMarker) {
	if seen[m.PciBusID] == nil {
		seen[m.PciBusID] = sets.New[uint32]()
	}
	seen[m.PciBusID].Insert(m.GiID)
}

// 释放操作会破坏一个被废弃的分区，并使其记录丢失。
//
// 只有当驾驶员确认分区已消失后，记录才会被删除：在实例仍然存在的情况下删除记录，使得该实例成为无主状态，
// 无法被任何人提及，而下面的清理操作则成为唯一能让人注意到它的东西。
func (r *migReclaimer) release(entry migMarkerEntry) {
	m := entry.marker
	logger := r.logger.WithValues(
		"pod", m.PodUID, "container", m.Container, "card", m.Card, "gpuInstance", m.GiID,
	)

	release := lockMigCard(m.PciBusID)
	defer release()

	if err := r.drv.DestroyInstance(m.PciBusID, m.instance()); err != nil {
		if errors.Is(err, errInstanceInUse) {
			// 一个进程的生命周期超过了其 Pod 的删除时间。只需等待即可：驱动程序拒绝从正在运行
			// 的工作负载中销毁该 Pod，这是合理的。
			logger.V(3).Info("a partition whose pod is gone is still in use; leaving it for the next pass")
			return
		}
		logger.Error(err, "failed to destroy the partition of a pod that is gone")
		return
	}

	if err := osx.Remove(entry.path); err != nil {
		logger.Error(err, "destroyed an abandoned partition but could not drop its ownership record")
		return
	}
	logger.Info("destroyed the partition of a pod that is gone")
}

// collectOrphans 会销毁没有所有权记录指向的活跃分区。
//
// 这些就是创建操作与其记录之间的崩溃后留下的残留物。预订路径在下一个针对同一规格的请求到来时采用该路径，
// 这处理了常见情况。而这一路径则处理其余情况——如果某个规格的孤儿对象不再被请求，原本会保留其切片直到节点
// 重建为止。
//
// 在任何操作被破坏之前，需先重新读取位于加速器锁下的所有权信息。调用者所生成的快照仅属于预过滤结果。若没
// 有这一步骤，此扫描操作将破坏那些仍在进行分配操作的分区：预留操作会在其调用者完成容器响应构建之前写入记
// 录并释放锁，因此未受保护的扫描操作会看到无人认领的活跃实例，进而将其破坏。这并非一个短暂的窗口期——在单
// 个节点上，每秒观察三次这种情况，该节点有三个 Pods，每个 Pods 都会失去刚刚获得的分区——而且由于驱动程序会
// 立即重新使用已释放的实例 ID，这种情况更为常见。
func (r *migReclaimer) collectOrphans(seen map[string]sets.Set[uint32]) {
	live, err := r.drv.ListInstances()
	if err != nil {
		r.logger.V(3).Error(err, "could not enumerate partitions, so none were collected this pass")
		return
	}

	for i := range live {
		card, inst := live[i].PciBusID, live[i].Instance
		if seen[card].Has(inst.GiID) {
			continue
		}
		logger := r.logger.WithValues("card", card, "gpuInstance", inst.GiID)

		release := lockMigCard(card)
		err := r.destroyIfStillUnclaimed(card, inst)
		release()
		switch {
		case err == nil:
			logger.Info("collected a partition no allocation claims")
		case errors.Is(err, errMigInstanceClaimed):
			logger.V(3).Info("a partition was claimed while this pass was deciding; leaving it alone")
		case errors.Is(err, errInstanceInUse):
			logger.V(3).Info("a partition no allocation claims is in use; leaving it for the next pass")
		default:
			logger.Error(err, "failed to collect a partition no allocation claims")
		}
	}
}

// sweepRuntimeDirs 会删除容器运行时在厂商实例注册表中可能留下的目录，这些目录会永久污染以它们命名的实例
// ID。
//
// 驱动程序在这里写入纯文件，每个计算实例对应一个文件。容器则将其作为绑定挂载使用。当运行时要求绑定一个不
// 存在的源路径时，会创建一个目录——因此，在分配期间被销毁的实例会在其名称下留下一个目录。之后，驱动程序便
// 无法再写入该名称：尝试创建与该名称对应的计算实例时，会因“资源不足”而失败。由于销毁后 ID 会被重新分配，
// 这种失败会持续存在，直到引发它的原因消失。在某个节点上观察到，两个 ID 变得永久无法创建。
//
// 仅会删除目录，且只能是空的目录。这里的文件是驱动程序自己的记录，与此功能无关。非空的目录则是另一回事，
// 会被报告而不是被删除。
func (r *migReclaimer) sweepRuntimeDirs() {
	ciDir := filepath.Join(migConfigDir, "ci")
	entries, err := os.ReadDir(ciDir)
	if err != nil {
		if !os.IsNotExist(err) {
			r.logger.V(3).Error(err, "could not read the instance registry to sweep it", "dir", ciDir)
		}
		return
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		path := filepath.Join(ciDir, entry.Name())
		if err := os.Remove(path); err != nil {
			r.logger.Error(err,
				"a directory occupies an instance registry name, so the driver can never create that "+
					"instance again, and it could not be removed", "path", path)
			continue
		}
		r.logger.Info("removed a directory a container runtime left on an instance registry name",
			"path", path)
	}
}

// errMigInstanceClaimed 报告称，在调用者的扫描与此次锁定发生之间，出现了某个候选者的所有权记录，因此该候
// 选者并非孤儿。
var errMigInstanceClaimed = errors.New("partition is claimed")

// destroyIfStillUnclaimed 在持有加速器锁的情况下重新读取所有权记录，只有当当前没有任何对象声称拥有该实例
// 时，才会销毁它。
//
// 无法读取的记录使得所有权无法确定，这也是出于同样的原因：调用者拒绝接受整个扫描结果——一个可能存在但无法
// 读取的所有者实例，不应被当作孤儿实例。
func (r *migReclaimer) destroyIfStillUnclaimed(pciBusID string, inst migInstance) error {
	entries, corrupt := scanMigMarkers(r.podsDir)
	r.sweepRuntimeDirs()

	if len(corrupt) != 0 {
		return fmt.Errorf("%w: some ownership records are unreadable", errMigInstanceClaimed)
	}
	for i := range entries {
		m := entries[i].marker
		if m.PciBusID == pciBusID && m.GiID == inst.GiID {
			return errMigInstanceClaimed
		}
	}
	return r.drv.DestroyInstance(pciBusID, inst)
}
