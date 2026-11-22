package thead

import (
	"errors"
	"os"
	"path/filepath"

	"k8s.io/apimachinery/pkg/util/sets"
	klog "k8s.io/klog/v2"
)

// reclaimMaxMisses 用于降低活跃性判断的干扰：在连续进行多次重调平操作后，某个 Pod（或被耗尽资源的加速器中
// 的孤儿节点）必须处于缺失或闲置状态，否则其分区将被销毁。因此，短暂的列表间隔不会恢复其活跃状态。与同步
// 间隔相比，它还是“创建前标记”的防护机制：在崩溃后重试时，Allocate 会重新绑定其 GPU 实例，且此过程在
// reclaimMaxMisses × 次同步内完成，因此孤儿节点收集器不会销毁分区，正在进行的重试操作仍拥有该分区。
const reclaimMaxMisses = 3

// reclaimMaxDestroyMisses 定义了在循环显示给操作者可见的日志之前，连续多少次协调任何操作可以阻止分区被回
// 收。实现该功能的三种方式：一种是将破坏操作视为繁忙而拒绝，一种是因为预检查发现进程先运行了而从未尝试破
// 坏，还有一种是因为根本无法询问分区能否破坏（destroyBlockedBy，其答案即日志名称，因为这三种情况对应不同
// 的需求）。同时不会清除延迟机制，因此每次都会重新做出决策。兄弟加速器永远不会被阻塞（每个加速器都有独立
// 锁定）。Devices.Status 每次协调后都会根据 Spec + Pod 注释重新构建，因此状态条件会被覆盖。日志是操作者可
// 见的界面。
const reclaimMaxDestroyMisses = 8

// reclaimMaxCorruptHoldMisses 定义了连续多少次协调操作可以保留该不可归属的损坏路径上的节点，该路径的自身
// 路径名称中没有任何 Pod，因此无法提供活跃性证据来移除该节点——在循环发现可被操作员查看的日志之前，该节点
// 会被保留。这种保留状态永远不会被释放（因为没有任何可释放的对象），因此与上述缓冲机制不同，此限制值的改
// 变不会影响任何决策：这种保留状态之所以存在，是因为它是整个节点的永久状态（在任何加速器上都不会被采用，
// 在任何加速器上也不会成为孤儿节点），并且不能让节点默默退化。这与“繁忙销毁”限制值的情况类似：一旦出现操
// 作员可见的日志，这种保留状态就会成立，因为每次协调操作后 Devices.Status 都会整体重建，且状态条件会被强
// 制满足。
const reclaimMaxCorruptHoldMisses = 8

// errCorruptPathUnattributable 是上述限制条件所报告的错误：在 Pod 工作根目录下的路径既无法被解析为标记，
// 也不指向其所属的 Pod 或加速器。该错误以错误形式呈现，而非消息文本形式，因此操作员可查看的日志看起来就像
// 繁忙的销毁日志一样。
var errCorruptPathUnattributable = errors.New("unreadable path names neither a pod nor a card")

// errCorruptMarkerHeldByLivePod 是实时所有者持有报告中的错误：这是一个无法读取的拥有记录，其对应的 Pod 仍
// 在运行，因此没有活体证据可以将其移除，也无法进行重建。出于上述错误的相同原因，该错误以错误形式呈现，而
// 非作为消息文本。
var errCorruptMarkerHeldByLivePod = errors.New("unreadable marker of a live pod")

// cardMissPrefix 命名空间用于记录每个加速器的孤儿集合缺失计数，该计数与每个 Pod 的计数器位于同一计数映射
// 中。Pod 的 UIDs 为 UUIDs，因此不会与这个前缀产生冲突。该前缀字符串是这些映射中的键命名空间，而非词汇表—
// —它所键定的计数是针对每个加速器的。
const cardMissPrefix = "card:"

// corruptMissPrefix 为同一错过映射中每个损坏标记路径的错过计数器定义了命名空间，因此移除无法解析的标记会
// 像其他活跃性判断一样被延迟处理（不会在单次临时处理中生效）。Pod UIDs 是 UUIDs，因此永远不会与其冲突。
const corruptMissPrefix = "corrupt:"

// corruptHeldMissPrefix 为同一路径的持有通过计数器定义了命名空间，该计数器无法与上述键共享：每当 Pod 仍存
// 活时，该计数器就会重置为零——因为退场延迟处理需要这种重置，因此那些超过短暂列表间隔的 Pod 其记录永远不会
// 被退场——而在此处的持有计数也永远不会达到任何边界值。Pod UIDs 是 UUIDs，因此这两个前缀不会产生冲突。
const corruptHeldMissPrefix = "corrupt-held:"

// reclaimer 是基于级别的分区回收循环状态，由协调器所设置的活跃 Pod-UID 集合以及定期同步机制驱动。物理分片
// 资源池没有 Release 回调，因此 Pod 的 GPU/compute 实例便在此处被释放。每次协调过程都会重新扫描标记并重新
// 列出驱动程序，因此无需内存中的实例注册表，即可在重启时实现自我修复。该进程以单线程方式运行（调用者需串
// 行调用 reconcile 函数），因此其计数器映射无需加锁。仅需要每个加速器级别的锁来协调并发分配操作。
type reclaimer struct {
	driver  migDriver
	podsDir string
	logger  klog.Logger
	// liveClaims 根据加速器 UUID，返回当前通过注解所声称的、处于活跃状态（非终止）的 Pods 的内存切片
	// 位置——即归属自自我校验源，因此错误归属的标记（分配 Pod 的启发式方法可能会将分配绑定到错误的同配
	// 置 Pod 上）不会破坏由正在运行的 Pod 所持有的实例。该标记被注入，以便在没有 Kubernetes 客户端的
	// 情况下对表格进行测试。
	liveClaims func() (map[string][]migPlacement, error)
	misses     map[string]int // pod UID / "card:<uuid>" / "corrupt:<path>" -> 连续缺失或闲置的重新调整
	inUse      map[string]int // pod UID / “card:<uuid>" -> 连续进行修复，以解除被阻塞的情况
}

// newReclaimer 通过驱动缝线、Pod 根节点和活索赔源来构建回收循环。这些依赖项属于参数而非包状态，因此该循环
// 由表格测试驱动。
func newReclaimer(
	driver migDriver, podsDir string, logger klog.Logger, liveClaims func() (map[string][]migPlacement, error),
) *reclaimer {
	return &reclaimer{
		driver: driver, podsDir: podsDir, logger: logger, liveClaims: liveClaims,
		misses: make(map[string]int), inUse: make(map[string]int),
	}
}

// 该功能用于针对某个实时节点 UID 快照，对分区与磁盘上的标记进行协调。每次实时性判断都会因连续
// reclaimMaxMisses 次未完成的协调操作而延迟执行，因此短暂的列表差异不会恢复实时状态。每次销毁操作都在其加
// 速器锁下运行（而非节点级互斥锁），因此不会在其他加速器并行执行创建+标记窗口时发生竞争。该功能通过两次协
// 调操作来完成。
// directions:
//   - 一个其 Pod 已死亡的标记器 -> 销毁其分区（即计算实例和 GPU 实例），除非正在运行的 Pod 仍声称该归属（
//     归因自检）。繁忙的销毁是一个有上限、可重试的部分失败（去抖未清除），会在上限处显示日志。
//   - 无标记 GPU 实例（在创建与标记写入之间发生崩溃，或属于非带内类型）只有在其加速器被完全释放后才会被销
//     毁（没有活跃的 Pod 对其进行声明或标记）——GPU 实例没有运营商标签，因此无法按 pod 进行分配。
//
// 在任何销毁操作之前，两个方向都会提出一个问题：即进程是否正在该分区上运行。任何带有进程的进程都不会被销
// 毁，无论其记录的所有者处于何种状态——参见 destroyBlockedBy。
//
// 无法解析的标记不仅未被记录：其 GPU 实例不在解析后的所有权集合中，因此当正在运行的 Pod 仍持有它时，它看
// 起来就像是一个无法被回收的孤儿。因此，该加速器的资源将被阻止被释放（根据加速器规则以失败关闭方式处理，
// 通过共享的 ownershipUnknownOnCard 谓词，分配路径也会参考该谓词），并且当其 Pod 被移除——从路径中读取到该
// 情况——时，损坏的文件本身也会被废弃，这使得加速器能够收敛，而不是在节点生命周期内泄露分区。
//
// 两种读取方式均会失败：无论是标记扫描的视图还是实时声明集，都不能被空值替代，因此任一失败都会导致整个操
// 作失败，而不是基于它无法信任的视图进行操作。
func (r *reclaimer) reconcile(livePodUIDs []string) {
	live := sets.New[string](livePodUIDs...)

	markers, corrupt := scanMarkers(r.podsDir)

	// 归属自检查需要实时声明集。若没有，则失败关闭（跳过整个流程），否则可能破坏正在运行的 Pod 所包含
	// 实例。
	claims, cerr := r.liveClaims()
	if cerr != nil {
		r.logger.Error(cerr, "reclaim: read live pod claims, skipping this pass")
		return
	}

	// 实时实例列表支持“孤儿清理”操作。若没有该列表，系统将关闭（跳过操作），而不会根据未经过验证的视
	// 图进行处理——被识别为缺失的分区其标记将被移除，标记为“已消失”，而没有标记的分区则会被泄漏过去。
	instances, lerr := r.driver.ListInstances()
	if lerr != nil {
		r.logger.Error(lerr, "reclaim: list partitions, skipping this pass")
		return
	}

	// 那些被触碰的标记在本次中仍然具有相关性。其余的将在最后被剔除。
	touched := sets.New[string]()

	// 加速器在任何处于活跃状态的 Pod 标记或声明其存在时均为“实时”状态，因此没有标记的孤儿实例也会被保
	// 留下来（可能是处于活跃状态的 Pod 在标记出现前的实例）。markedGI 会索引每个由标记所拥有的 GPU 实
	// 例，从而能够识别没有标记的实例。
	liveOnCard := make(map[string]bool)
	for card, ps := range claims {
		if len(ps) > 0 {
			liveOnCard[card] = true
		}
	}
	markedGI := make(map[string]map[uint32]bool)
	byPod := make(map[string][]markerEntry)
	for i := range markers {
		m := markers[i].marker
		byPod[m.PodUID] = append(byPod[m.PodUID], markers[i])
		if markedGI[m.Card] == nil {
			markedGI[m.Card] = make(map[uint32]bool)
		}
		markedGI[m.Card][m.GiID] = true
		if live.Has(m.PodUID) {
			liveOnCard[m.Card] = true
		}
	}

	// 每个 Pod 的存活性判断 + 延迟机制：死亡 Pod 的标记在超时后会被回收。
	for uid, entries := range byPod {
		touched.Insert(uid)
		if live.Has(uid) {
			r.misses[uid] = 0
			r.inUse[uid] = 0
			continue
		}
		r.misses[uid]++
		if r.misses[uid] < reclaimMaxMisses {
			continue
		}
		r.destroyPod(uid, entries, claims)
	}

	// 移除那些 Pod 已消失的损坏标记。它会在每个 Pod 的决策之后（一个已失效的 Pod 的可解析标记会被同一
	// 份证据重新利用）以及孤儿集合之前运行，但本次的损坏列表仍然保留着其加速器，位于下方：这种移除行
	// 为仅会在下一次扫描中被发现，这是有意为之的——一旦文件被证明已消失，加速器就会被释放，而不是仅仅
	// 因为尝试过移除它。
	r.pruneCorruptMarkers(corrupt, live, touched)

	// 孤儿集合：没有标记的 GPU 实例仅在加速器完全耗尽时会被销毁。
	orphansByCard := make(map[string][]migInstance)
	for i := range instances {
		li := instances[i]
		if markedGI[li.Card][li.Inst.GiID] {
			continue // 由标记器拥有。在上方处理
		}
		orphansByCard[li.Card] = append(orphansByCard[li.Card], li.Inst)
	}
	for card, orphans := range orphansByCard {
		key := cardMissPrefix + card
		touched.Insert(key)
		// 一个所有权集包含无法解析标记的加速器，其状态不可知，不会被视为已耗尽：这些“孤儿”对象中
		// 的某一个可能是该不可读记录所拥有的实例，摧毁它会导致运行中的容器中的分区被移除，而该容
		// 器的唯一所有权记录已被截断。延迟机制被重置，因此当加速器的所有权再次可读取时，它将从零
		// 开始重新计数。
		if liveOnCard[card] || ownershipUnknownOnCard(corrupt, card) {
			r.misses[key] = 0
			r.inUse[key] = 0
			continue
		}
		r.misses[key]++
		if r.misses[key] < reclaimMaxMisses {
			continue
		}
		r.destroyOrphans(key, card, orphans)
	}

	for k := range r.misses {
		if !touched.Has(k) {
			delete(r.misses, k)
		}
	}
	for k := range r.inUse {
		if !touched.Has(k) {
			delete(r.inUse, k)
		}
	}
}

// destroyPod 会销毁一个已死亡的 Pod 的分区：这些标记按加速器分组，每个加速器仅被占用一次——每个加速器需要
// 一次锁定操作和一次驱动程序重新读取，无论该 Pod 中有多少个容器在其上创建了分区——只有该 Pod 自身的标记文
// 件才会被移除。在 destroyPod 操作之前有三个保护机制。
//   - 归因自我校验——如果正在运行的 Pod 声称拥有该位置，则标记被错误归因（死 Pod 的标记出现在活 Pod 的实例
//     上），因此该标记永远不会被删除。
//   - 进程检查——运行了某个进程的分区被原样保留，包括标记等，因此后续处理时又能找到它。
//   - 身份验证——GPU 实例 ID 必须同时包含标记所记录的实例信息，既包含原始厂商规格 ID，也包含分区身份字符串。
//     此验证会重新读取加速器锁内的驱动实时数据，而非依赖无锁快照：该快照可能会因整个分配周期而失效，而带
//     外销毁及 ID 重用可能导致同一 ID 下出现不同的、可能处于运行状态的实例。若匹配失败，过时的标记将被丢
//     弃，无需进行任何销毁操作。已消失的实例只需移除其标记即可。若重新读取失败，则对每个加速器进行跳过处
//     理，而非在未验证的视图下进行销毁操作。
//
// 剩余的繁忙拒绝是一种有上限的可重试失败：Pod 的未命中计数不会清除（下次重试），当重试次数超过上限时，会
// 显示出可供操作者查看的日志。仅当 Pod 的所有分区都被回收后，未命中/使用的计数才会清除。
func (r *reclaimer) destroyPod(uid string, entries []markerEntry, claims map[string][]migPlacement) {
	ok := true
	var blocked error

	// 首先按加速器分组：归属自检过程不需要调用者调用或锁定，因此在此处进行过滤，而经过该过程后剩余的
	// 内容则会在该加速器的自身锁定下被逐一销毁。
	byCard := make(map[string][]markerEntry)
	for i := range entries {
		m := entries[i].marker
		if placementOverlapsAny(migPlacement{Start: m.Start, Length: m.Length}, claims[m.Card]) {
			r.logger.Info("reclaim: placement is claimed by a running pod, skipping destroy (attribution conflict)",
				"podUID", uid, "card", m.Card, "giID", m.GiID)
			ok = false
			continue
		}
		byCard[m.Card] = append(byCard[m.Card], entries[i])
	}

	for card, cardEntries := range byCard {
		done, cardBlocked := r.destroyMarkedInstancesOnCard(uid, card, cardEntries)
		if !done {
			ok = false
		}
		if cardBlocked != nil {
			blocked = worseBlock(blocked, cardBlocked)
		}
	}

	if blocked != nil {
		r.inUse[uid]++
		if r.inUse[uid] == reclaimMaxDestroyMisses {
			r.logger.Error(blocked,
				"reclaim: a partition is still not reclaimable after bounded retries; its placement stays "+
					"occupied until this clears",
				"podUID", uid, "attempts", r.inUse[uid])
		}
	} else {
		r.inUse[uid] = 0
	}

	if ok {
		delete(r.misses, uid)
		delete(r.inUse, uid)
		r.logger.Info("reclaim: reclaimed dead pod's partitions", "podUID", uid, "partitions", len(entries))
	}
}

// destroyMarkedInstancesOnCard 会在某个加速器上销毁死亡节点分区中的每一个实例，操作在该加速器的锁范围内进
// 行，并在关键区域内部重新验证每个记录的 GPU 实例 ID 是否仍携带着记录的原始规格 ID 和身份字符串。它会报告
// 该加速器上的所有标记是否都已消失，以及是否有任何销毁操作被判定为繁忙而被拒绝。被重新使用的 ID 会被保留，
// 不会被销毁：只有过时的标记会被丢弃，因为该 ID 下的实例现在属于其他人。
//
// 驱动程序的实时集合在整个加速器范围内被读取一次，而非每个标记点分别读取一次，这才是关键：多个容器属于同
// 一个 Pod 在单个加速器上的情况很常见，而这种枚举操作是跨节点的且成本较高的。仅读取一次并不会导致问题：锁
// 在整个组中都被持有，因此此循环之外的任何操作都无法在标记点之间改变加速器状态——只有此循环自身的销毁操作
// 会生效，且每个标记点都会根据其记录的身份进行验证，而不是根据兄弟节点的销毁残留状态进行验证。
func (r *reclaimer) destroyMarkedInstancesOnCard(uid, card string, entries []markerEntry) (done bool, blocked error) {
	unlock := lockCard(card)
	defer unlock()

	instances, lerr := r.driver.CardInstances(card)
	if lerr != nil {
		r.logger.Error(lerr, "reclaim: re-read this card's partitions before destroy, skipping it",
			"podUID", uid, "card", card, "partitions", len(entries))
		return false, nil
	}

	done = true
	for i := range entries {
		m := entries[i].marker
		inst, present := findLiveGi(instances, m.GiID)
		switch {
		case present && (inst.UUID != m.MigUUID || inst.ProfileID != m.ProfileID):
			r.logger.Info("reclaim: gpu-instance id reused by a different partition, dropping stale marker without destroy",
				"podUID", uid, "card", card, "giID", m.GiID,
				"markerUUID", m.MigUUID, "liveUUID", inst.UUID,
				"markerProfileID", m.ProfileID, "liveProfileID", inst.ProfileID)
		case present:
			if berr := r.destroyBlockedBy(card, m.instance()); berr != nil {
				// 保持标记和分区：这是一份记录，使得后续的访问者能够在清除任何块后再次找
				// 到这个分区。如果被视为被阻塞，那么被永久保留的分区就会穿过与被拒绝时相
				// 同的有界操作可见表面。
				done = false
				blocked = worseBlock(blocked, berr)
				continue
			}
			if derr := r.driver.DestroyInstance(card, m.instance()); derr != nil {
				done = false
				if errors.Is(derr, errInstanceInUse) {
					blocked = worseBlock(blocked, derr)
					continue
				}
				r.logger.Error(derr, "reclaim: destroy partition", "podUID", uid, "card", card, "giID", m.GiID)
				continue
			}
		}
		// 分区被破坏、已消失，或属于其他人：请移除标记。
		if !r.removeMarker(entries[i].path) {
			done = false
		}
	}
	return done, blocked
}

// destroyBlockedBy 用于说明为何某个分区必须保持原状，若可销毁则值为 `nil`。在每次执行此循环的操作中，该值
// 都会被询问两次，其返回值决定了有界日志名称的含义：对于在分区上运行的进程，则为 errInstanceInUse。对于无
// 法被询问的分区，则为查询自身的错误。这些是不同的错误情况，需要不同的解决方式——要么找到该进程，要么让容
// 器拥有访问驱动器的权限——调用者没有其他方法来区分它们，因为下面每遍处理的行内容并未在输出中显示。
//
// 包含活跃进程的分区，无论记录如何表明，都是某人的工作加速器。而孤儿分区则最为关键：定义在此操作器之外的
// 分区默认没有标记，因此被释放的加速器扫描操作会将其从正在运行的进程中移除。这样的分区保持完整，在下一轮
// 处理中会被重新检查，因此当最后一个进程退出后，它就会被回收。
//
// 除了驾驶员自身的拒绝之外，首先询问还能带来的是：拆解操作从未开始：销毁一个 GPU 实例会先清除其计算实例，
// 因此一个实例中同时有闲置和忙碌的计算实例会失去闲置的那个实例，而只有那样才会被拒绝。这就是该问题被解决
// 后所消除的。仅有的拒绝行为根本无法解决这一问题。
//
// 一次跳过也算作处于使用状态，因此一个永远静止的分区会穿过有界算子可见表面，在 reclaimMaxDestroyMisses 处
// 触发事件。它必须如此：这次跳过本身会被记录在该日志器的详细程度中，而已发布的详细程度中并未显示此信息。
// 此外，任何不会被释放的放置位置都不应安静地保持占用状态。
//
// 不询问的行为分为两种，它们导向不同的结果。没有要询问的内容——没有分区设备来处理该分区——这是没有计算实例
// 的 GPU 实例的读取方式，也是每个分区向容器读取时，驱动的分区设备被隐藏的情况。销毁操作也会在此进行，因为
// 在第一种情况下，该分区只是常规的回收目标。在第二种情况下，销毁操作也是按分区进行的，会自行失败。有询问
// 但没有回答的情况则被视为正在使用状态：对状态未知的分区开始销毁操作，会导致一个原本空闲的计算实例从分区
// 中移除，而该分区实际上处于忙碌状态，这就是先询问所旨在防止的情况。
//
// 无论何种情况，驱动程序自身的拒绝机制（errInstanceInUse）都作为后备措施，适用于介于此操作与销毁之间的一
// 系列过程。
func (r *reclaimer) destroyBlockedBy(card string, inst migInstance) error {
	procs, err := r.driver.InstanceProcesses(card, inst)
	switch {
	case errors.Is(err, errNoAddressableDevice):
		r.logger.Info("reclaim: no partition device addresses this partition, leaving the destroy to the driver",
			"card", card, "giID", inst.GiID, "err", err.Error())
		return nil
	case err != nil:
		r.logger.Info("reclaim: this partition could not be asked about its processes, skipping destroy",
			"card", card, "giID", inst.GiID, "err", err.Error())
		return err
	case procs == 0:
		return nil
	default:
		r.logger.Info("reclaim: a process is running on this partition, skipping destroy",
			"card", card, "giID", inst.GiID, "processes", procs)
		return errInstanceInUse
	}
}

// worseBlock 用于确定在多次阻塞情况下，哪些被阻塞的分区会导致限制报告。无法被移除的分区会排在某个进程之前：
// 第一个属于需要调查的故障，第二个则是正常执行工作负载的分区，如果直接命名该工作负载，就会掩盖故障问题。
func worseBlock(held, next error) error {
	if held == nil || (errors.Is(held, errInstanceInUse) && !errors.Is(next, errInstanceInUse)) {
		return next
	}
	return held
}

// findLiveGi 会返回具有 giID 的 GPU 实例，如果某个加速器包含该实例的话。
func findLiveGi(instances []migInstance, giID uint32) (migInstance, bool) {
	for i := range instances {
		if instances[i].GiID == giID {
			return instances[i], true
		}
	}
	return migInstance{}, false
}

// removeMarker 会移除标记文件（以及其现在为空的容器/Pod 目录），因此同级容器中的活标记永远不会被删除，该
// 函数会报告移除是否成功。
func (r *reclaimer) removeMarker(path string) bool {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		r.logger.Error(err, "reclaim: remove marker", "path", path)
		return false
	}
	removeIfEmpty(filepath.Dir(path))
	removeIfEmpty(filepath.Dir(filepath.Dir(path)))
	return true
}

// pruneCorruptMarkers 会移除那些 Pod 已消失、无法解析的标记，因此被截断的记录在仍能代表某种意义时，其加速
// 器也会随之消失。损坏的文件内容毫无意义，但其路径却能指向其 Pod，这本身就足以作为证据：一旦 Pod 在连续多
// 次 reclaimMaxMisses 操作后从活跃集合中消失——而此循环中的其他存活性判断也使用相同的延迟机制，因此短暂的
// 列表空缺不会移除活跃 Pod 的记录——该文件就会被删除。它所遮蔽的分区随后便成为没有标记的孤儿分区，收集器在
// 加速器耗尽后才会处理它，这样加速器就能收敛，而不是让分区在节点的生命周期内持续存在。无需使用加速器锁：
// 因为 Pod 已死亡，所以没有并发分配操作会写入该路径。
//
// 故意保留两个案例无限期保留：
//   - 一个损坏的标记器，其 Pod 仍然处于活动状态——它仍然记录着其 Pod 所依赖的所有权。
//   - 一个损坏的路径，其上的 Pod 无法被读取（存在移动错误，或标记深度处不是标记文件）——没有所有者，没有可
//     操作的活跃性证据，这是需要修复的文件系统故障，而非需要删除的所有者记录。
//
// 第二种情况是永久性的，其成本涉及整个节点，而非单个加速器：这样的路径名称也不会指向任何加速器，因此
// ownershipUnknownOnCard 会使得所有加速器处于不可读状态——既无任何采用行为，也无任何孤立的收集行为——只要它
// 保持不可读状态就如此。没有办法在不破坏无法处理的状态的情况下解决这一问题，因此这种状态必须被改变。它绝
// 不能只是默默存在。
//
// 两种保持模式都不再被忽视：每种模式都对其连续通过次数进行统计，并在 reclaimMaxCorruptHoldMisses 处生成一
// 份可供操作者可查看的日志，记录路径与修复方案，因此只有具备能力的执行者才能完成修复。第一种模式需要与第
// 二种模式相同的要求，因为它仅属于暂时状态——Pod 仍然存活，因此这种保持状态会被视为其退出后将被清除的内容，
// 实际上并非如此：请参考 holdLiveOwnersMarker。
func (r *reclaimer) pruneCorruptMarkers(corrupt []string, live, touched sets.Set[string]) {
	for _, path := range corrupt {
		uid, ok := podUIDFromMarkerPath(r.podsDir, path)
		if !ok {
			r.holdUnattributablePath(path, touched)
			continue
		}
		key := corruptMissPrefix + path
		touched.Insert(key)
		if live.Has(uid) {
			r.misses[key] = 0
			r.holdLiveOwnersMarker(path, uid, touched)
			continue
		}
		r.misses[key]++
		if r.misses[key] < reclaimMaxMisses {
			continue
		}
		if r.removeMarker(path) {
			delete(r.misses, key)
			r.logger.Info("reclaim: removed unparseable marker of a dead pod", "path", path, "podUID", uid)
		}
	}
}

// holdLiveOwnersMarker 保留了一个无法解析的“活跃 Pod”标记——它仍然记录着其“Pod”所依赖的所有权信息——并统计
// 连续几次“hold”操作使得加速器得以使用，这些记录会在 reclaimMaxCorruptHoldMisses 处呈现给操作员查看的日志。
// 与不可归属的“hold”操作一样，它不会改变任何决策：无论哪种情况，该记录都会被保留。该计数数据存储在自己的
// 键空间中，因为对于这条路径来说，退场延迟的键在每次操作都会被重置为 0。该键会被修改，以便计数能延续到下
// 一次操作，一旦路径结束，该键就会消失。
//
// 这个限制条件值得在本案中投入使用，因为它属于瞬态状态：Pod 仍然处于活动状态，因此这种保留状态看起来像是
// Pod 退出后会被清除的。但实际上并非如此。只要 Pod 存在，加速器中任何剩余的资源都无法被利用，也无法被重新
// 占用。而且 Pod 也无法被重新接纳，因为重新创建的容器会先读取自己的记录，对于非“缺失”的解析失败情况会直接
// 失败——因此重启 Pod 也无法清除该状态，操作者所持有的任何东西也无法重建记录，因为识别分区的 ID 仅存在于文
// 件中。删除 Pod 才能释放加速器：随后进行的退场操作会根据路径清除该记录。
func (r *reclaimer) holdLiveOwnersMarker(path, uid string, touched sets.Set[string]) {
	key := corruptHeldMissPrefix + path
	touched.Insert(key)
	r.misses[key]++
	r.logger.Info("reclaim: unparseable marker of a live pod, holding its card closed",
		"path", path, "podUID", uid, "passes", r.misses[key])
	if r.misses[key] != reclaimMaxCorruptHoldMisses {
		return
	}
	card, ok := cardFromMarkerPath(path)
	if !ok {
		card = "ALL (the file name encodes no card)"
	}
	r.logger.Error(errCorruptMarkerHeldByLivePod,
		"reclaim: an unreadable partition ownership record of a RUNNING pod is holding its card closed: while it "+
			"persists no leftover partition on that card is adopted and none is reclaimed, and the pod cannot be "+
			"re-admitted either, because a container's reserve reads its own record first and fails closed on an "+
			"unreadable one — so restarting the pod cannot clear this, and the record cannot be rebuilt from "+
			"anything outside it; delete the pod to release the card",
		"path", path, "podUID", uid, "card", card, "passes", r.misses[key])
}

// holdUnattributablePath 保留一个损坏的路径，该路径的拥有者无法被读取——这里没有任何可依赖的活跃性证据——同
// 时记录该路径连续被持有的次数，在 reclaimMaxCorruptHoldMisses 处会生成一份可供操作者可查看的日志。该计数
// 与“退场”的延迟机制共享同一损坏路径键空间下的计数映射。某个路径在其整个生命周期内可被归属或不可归属，因
// 此这两种含义不会在同一个键上混合。该路径会被访问，使计数能够延续到下一次操作，而一旦该路径被移除，计数
// 也会随之消失。
func (r *reclaimer) holdUnattributablePath(path string, touched sets.Set[string]) {
	key := corruptMissPrefix + path
	touched.Insert(key)
	r.misses[key]++
	r.logger.Info("reclaim: unparseable marker path names no pod, holding its cards closed",
		"path", path, "passes", r.misses[key])
	if r.misses[key] == reclaimMaxCorruptHoldMisses {
		r.logger.Error(errCorruptPathUnattributable,
			"reclaim: an unreadable path under the pod work root names neither a pod nor a card, so partition "+
				"ownership cannot be proven on ANY card of this node: no leftover partition is adopted and none is "+
				"reclaimed while it persists; this is a filesystem fault that will not clear by itself — repair or "+
				"remove the path",
			"path", path, "passes", r.misses[key])
	}
}

// destroyOrphans 会移除完全耗尽资源的加速器上无标记的 GPU 实例（即没有存活的 Pod）。它会重新扫描加速器锁
// 下的标记，如果加速器现在带有任何标记，则会失败：在此同一锁下，`create+marker` 是原子操作，因此自无锁快
// 照后出现的标记意味着分配已完成，加速器不再完全耗尽资源——其遗留实例将等待后续处理。无法解析的标记导致加
// 速器的所有权无法确定，也会以相同方式失败，通过处理过程中使用的共享谓词进行判断：这两个检查并非冗余，因
// 为处理的结论基于锁未覆盖的快照，而本次处理实际上能保留记录所属的分区。运行中的遗留实例也会被跳过：无论
// 是否位于该操作器之外，它仍被使用，而耗尽资源的加速器对此无能为力。剩余的忙碌拒绝情况属于有边界的可重试
// 失败，其日志表面与按 Pod 处理的路径相同。只有当所有移除操作成功时，错过计数才会被清除。
func (r *reclaimer) destroyOrphans(missKey, card string, orphans []migInstance) {
	unlock := lockCard(card)
	defer unlock()

	entries, corrupt := scanMarkers(r.podsDir)
	if len(ownedGiIDsOnCard(entries, card)) > 0 || ownershipUnknownOnCard(corrupt, card) {
		return
	}

	ok := true
	var blocked error
	destroyed := 0
	for _, inst := range orphans {
		if berr := r.destroyBlockedBy(card, inst); berr != nil {
			ok = false
			blocked = worseBlock(blocked, berr)
			continue
		}
		if derr := r.driver.DestroyInstance(card, inst); derr != nil {
			ok = false
			if errors.Is(derr, errInstanceInUse) {
				blocked = worseBlock(blocked, derr)
				continue
			}
			r.logger.Error(derr, "reclaim: destroy marker-less partition on drained card", "card", card, "giID", inst.GiID)
			continue
		}
		destroyed++
	}

	if blocked != nil {
		r.inUse[missKey]++
		if r.inUse[missKey] == reclaimMaxDestroyMisses {
			r.logger.Error(blocked,
				"reclaim: a marker-less partition on a drained card is still not reclaimable after bounded "+
					"retries; its placement stays occupied until this clears",
				"card", card, "attempts", r.inUse[missKey])
		}
	} else {
		r.inUse[missKey] = 0
	}

	if ok {
		delete(r.misses, missKey)
		delete(r.inUse, missKey)
		if destroyed > 0 {
			r.logger.Info("reclaim: reclaimed marker-less partitions on drained card", "card", card, "count", destroyed)
		}
	}
}

// removeIfEmpty 仅在目录中没有条目时才会移除它，因此回收一个容器不会使某个兄弟节点的标记变成孤立状态。
func removeIfEmpty(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) > 0 {
		return
	}
	_ = os.Remove(dir)
}
