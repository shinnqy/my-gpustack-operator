package nvidia

import (
	"errors"
	"os"
	"path/filepath"

	"k8s.io/apimachinery/pkg/util/sets"
	klog "k8s.io/klog/v2"
)

// reclaimMaxMisses 是连续缺失阈值。只有连续三轮都未见 Pod，或都确认卡已排空，才开始回收，避免一次
// informer 列表不全就误删分区。周期性重同步为 60 秒，创建 GI 后尚未写入标记就崩溃的调用也有时间重试。
// 这一策略与设备插件的 podDirGC 一致。
const reclaimMaxMisses = 3

// reclaimMaxDestroyMisses 是连续回收受阻时的告警阈值。NVML_ERROR_IN_USE、检测到活跃进程、读取进程状
// 态失败都会计数。达到阈值会记录一次原因，后续各轮仍会重试。同卡操作串行，不会占住其他卡。
// Devices.Status 每轮重建，因此把告警写入日志。 destroyBlockedBy 返回明确的阻塞原因。
const reclaimMaxDestroyMisses = 8

// reclaimMaxCorruptHoldMisses 限制连续多少轮暂缓回收后必须告警。如果损坏路径连所属 Pod 都无法识别，
// 回收器就没有删除它的依据。阈值只控制日志；只要问题存在，受影响卡上的接管和无主分区回收仍会暂停。
// 告警与持续 NVML_ERROR_IN_USE 一样只在达到阈值时写一次。
const reclaimMaxCorruptHoldMisses = 8

// errCorruptPathUnattributable 表示无法解析 Pod 工作根目录下的路径，也无法从路径识别所属 Pod 或卡。
// 持续达到阈值时，日志会带上这个错误。
var errCorruptPathUnattributable = errors.New("unreadable path names neither a pod nor a card")

// errCorruptMarkerHeldByLivePod 表示标记已损坏但所属 Pod 仍在运行。仅凭存活状态，既不能移除该标记，
// 也无法重建其中的分区身份。
var errCorruptMarkerHeldByLivePod = errors.New("unreadable marker of a live pod")

// cardMissPrefix 区分按物理卡计数的无主实例回收记录与按 Pod UID 计数的记录，二者共用 misses 表。
const cardMissPrefix = "card:"

// corruptMissPrefix 用标记路径跟踪其所有者连续缺失的轮数。一次短暂缺失不应导致旧文件被删除。
const corruptMissPrefix = "corrupt:"

// corruptHeldMissPrefix 跟踪本应保留的损坏文件已经阻塞了多少轮。同一 Pod 仍存活时，清除文件用的缺失
// 计数每轮都归零，因此告警计数必须使用独立的键。
const corruptHeldMissPrefix = "corrupt-held:"

// reclaimer 是基于级别的 MIG 回收循环的状态，由协调器设置的广播实时 Pod-UID 以及周期性同步计时器（
// deviceplugin.RunReclaimLoop）驱动。分片资源池没有 Release 回调，因此 Pod 的 GPU/compute 实例便在此处被
// 释放。每次协调都会重新扫描标记并重新列出驱动程序，因此它在重启时能够自我修复，无需内存中的实例注册表。
// 该循环以单线程方式运行（循环以串行方式调用协调操作），因此其计数器映射无需加锁。只有每个加速器级别的锁
// 能够与并发的分配器进行协调。
type reclaimer struct {
	driver  migDriver
	podsDir string
	logger  klog.Logger
	// liveClaims 根据加速器 UUID，返回了通过注解当前被占用的物理切片位置——即归属自自我校验源的 Pods。
	// 因此，错误归属的标记（最旧的 getAllocatingPod 启发式规则可能会将分配操作绑定到错误的同配置 Pod
	// 上）不会破坏正在运行的 Pod 所持有的实例。该标记被注入，以便在没有 Kubernetes 客户端的情况下对循
	// 环进行表测试。
	liveClaims func() (map[string][]migPlacement, error)
	// 缺失情况由 pod UID、cardMissPrefix + 加速器 UUID，或损坏路径前缀 + 路径来定义，并统计连续缺失或
	// 闲置的协调情况。
	misses map[string]int
	// inUse 表示按 Pod UID 计算的、连续进行的重新平衡操作次数，这些操作阻止了某个分区的回收——可能是被
	// 拒绝的销毁操作、因进程正在运行而被预检查跳过的操作，也可能是因无法获取分区信息而被跳过的操作。
	inUse map[string]int
}

func newReclaimer(driver migDriver, podsDir string, logger klog.Logger, liveClaims func() (map[string][]migPlacement, error)) *reclaimer {
	return &reclaimer{
		driver: driver, podsDir: podsDir, logger: logger, liveClaims: liveClaims,
		misses: make(map[string]int), inUse: make(map[string]int),
	}
}

// reconcile 根据存活 Pod 的 UID、注解、所有权标记和驱动中的实例列表决定是否回收 MIG 分区。
// 对有标记的分区，连续多轮未见所属 Pod 才尝试销毁，并在卡锁内再次确认当前持有者及实例身份。
// 无标记的遗留分区须等对应的卡排空。仍有计算进程或无法判明归属时保留实例，后续轮次再检查。
func (r *reclaimer) reconcile(livePodUIDs []string) {
	live := sets.New[string](livePodUIDs...)

	markers, corrupt := scanMarkers(r.podsDir)

	// 先从存活 Pod 的注解读取分区认领记录。读取失败就跳过本轮，以免销毁仍有工作负载使用的实例。
	claims, cerr := r.liveClaims()
	if cerr != nil {
		r.logger.Error(cerr, "reclaim: read live pod claims, skipping this pass")
		return
	}

	// 先列出驱动中的实例，供本轮查找无标记分区。枚举失败就跳过本轮。对有标记实例做身份核对时，还会在对应
	// 卡的锁内重新读取，防止本轮快照已经过期。
	instances, lerr := r.driver.ListInstances()
	if lerr != nil {
		r.logger.Error(lerr, "reclaim: list mig instances, skipping this pass")
		return
	}

	// touched 记录本轮仍需跟踪的键，轮末清理其余计数。
	touched := sets.New[string]()

	// 只要存活 Pod 在卡上还有标记或认领记录，就保留该卡上没有标记的 GI，其中一个可能正处于已创建、标记还
	// 没写入的阶段。markedGI 则索引所有标记已认领的 GI。
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

	// 以 Pod UID 判断存活性，连续缺失达到阈值才进入销毁流程。
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

	// 先处理已能解析的标记，再清理所属 Pod 已消失的损坏标记，最后检查无标记的分区。本轮仍按扫描时得到的
	// corrupt 列表保护相应的卡，下一轮确认文件确实消失后才放行。
	r.pruneCorruptMarkers(corrupt, live, touched)

	// 只有整张卡上已没有活跃认领，才回收无标记的 GI。
	orphansByCard := make(map[string][]migInstance)
	for i := range instances {
		li := instances[i]
		if markedGI[li.Card][li.Inst.GiID] {
			continue // 已有标记认领的 GI 在上面按 Pod 处理。
		}
		orphansByCard[li.Card] = append(orphansByCard[li.Card], li.Inst)
	}
	for card, orphans := range orphansByCard {
		key := cardMissPrefix + card
		touched.Insert(key)
		// 卡上存在归属不明的损坏标记时，看起来无主的 GI 可能仍归某个运行中的容器所有。本轮保留实例并重置计数，
		// 待归属可判定后重新计算连续空闲的轮数。
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

// destroyPod 逐张卡处理已离开 Pod 的分区。每个标记都须经过以下核对，成功后只移除相应标记文件。
// - 归属核对。如果运行中的 Pod 仍认领同一区间，这个已结束 Pod 的标记可能指错了实例，保留分区。
// - 当前持有者核对。如果另一个 Pod 的标记已认领同一 GI 编号，只删除被取代的旧标记。分配在卡锁内同步
// 写标记，informer 中的 Pod 注解可能更晚更新，因此也要在卡锁内重扫标记。
// - 进程检查。分区上仍有计算进程时暂缓销毁，留下标记以便下轮继续检查。
// - 身份检查。在卡锁内重新读取实时 GI 列表。GI 已消失或数字 ID 已被另一实例复用时只移除旧标记。读取
// 失败时跳过这张卡。
//
// 若驱动返回 NVML_ERROR_IN_USE，保留 Pod 的缺失计数并在下轮重试。连续受阻达到阈值时记录管理员可见的
// 日志；该 Pod 的所有分区都处理完毕后才清除计数。
func (r *reclaimer) destroyPod(uid string, entries []markerEntry, claims map[string][]migPlacement) {
	ok := true
	var blocked error

	// 先按物理卡分组。与存活 Pod 所占区间冲突的标记无需获取 NVML 句柄或卡锁，其余标记按卡加锁处理。
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
				"reclaim: a mig instance is still not reclaimable after bounded retries; its placement stays occupied until this clears",
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

// destroyMarkedInstancesOnCard 在单张卡的锁内处理同一已结束 Pod 的标记，重新验证每个 GI 的实时身份和
// 区间。返回是否已清除该卡上的全部相关标记，以及是否有阻塞原因。若 ID 已被其他分区复用，保留实时实例，
// 只清除旧标记。
//
// 本轮开始时的驱动快照可能已经过期。例如外部工具删除 GI 后，NVML 可以把原编号交给新实例。只有拿到卡
// 锁后重新读取实时状态，才能避免用旧快照误删另一个 Pod 的分区。
//
// 每卡只重新枚举一次。一个 Pod 的多个容器可能都在同一卡上分区，锁覆盖这一整组标记；每条标记都与它自
// 己保存的身份进行核对。
//
// 同一把卡锁内还要重新扫描标记。被释放的区间若立即再次分配，最先可用区间、GI/CI 数字编号、按名称生成
// 的 MIG UUID 都可能完全相同。此时用于识别新的持有者的是另一个 Pod 刚写入的标记。
//
// 核对新持有者时以锁内重新读到的标记为准。Pod 注解和本轮的存活 UID 列表来自 informer，可能落后于同步
// 写入的标记，不能再用这些旧数据过滤新标记。
func (r *reclaimer) destroyMarkedInstancesOnCard(uid, card string, entries []markerEntry) (done bool, blocked error) {
	unlock := lockCard(card)
	defer unlock()

	instances, lerr := r.driver.CardInstances(card)
	if lerr != nil {
		r.logger.Error(lerr, "reclaim: re-read this card's mig instances before destroy, skipping it",
			"podUID", uid, "card", card, "partitions", len(entries))
		return false, nil
	}
	scanned, corrupt := scanMarkers(r.podsDir)
	if supersedingCorruptOnCard(corrupt, r.podsDir, card, uid) {
		// 不可读的标记也可能是取代旧分配的新记录。无法查明它属于哪个 GI 时，本轮保留整张卡上的候选分区。待回
		// 收循环确认相应 Pod 已离开并移除文件后，再重新判断。
		r.logger.Info("reclaim: a corrupt marker on this card may claim one of these instances, holding the card for this pass",
			"podUID", uid, "card", card, "partitions", len(entries))
		return false, nil
	}
	superseded := supersedingGiIDsOnCard(scanned, card, uid)

	done = true
	for i := range entries {
		m := entries[i].marker
		inst, present := findLiveGi(instances, m.GiID)
		switch {
		case present && superseded[m.GiID]:
			r.logger.Info("reclaim: another pod's marker owns this gpu instance, dropping the superseded marker without destroy",
				"podUID", uid, "card", card, "giID", m.GiID, "migUUID", m.MigUUID,
				"placement", migPlacement{Start: m.Start, Length: m.Length})
		case present && !inst.matchesMarker(m):
			r.logger.Info("reclaim: gpu-instance id reused by a different instance, dropping stale marker without destroy",
				"podUID", uid, "card", card, "giID", m.GiID,
				"markerUUID", m.MigUUID, "liveUUID", inst.UUID,
				"markerPlacement", migPlacement{Start: m.Start, Length: m.Length}, "livePlacement", inst.Placement)
		case present:
			if berr := r.destroyBlockedBy(card, m.instance()); berr != nil {
				// 保留分区时也保留标记，下一轮仍可据此检查。
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
				r.logger.Error(derr, "reclaim: destroy gpu instance", "podUID", uid, "card", card, "giID", m.GiID)
				continue
			}
			// 销毁日志写入具体分区的卡、GI、CI、MIG UUID 和区间。若容器后来报告设备已无法打开，可按该身份回查是
			// 谁执行了回收。
			r.logger.Info("reclaim: destroyed a dead pod's partition",
				"podUID", uid, "card", card, "giID", m.GiID, "ciID", m.CiID, "migUUID", m.MigUUID,
				"placement", migPlacement{Start: m.Start, Length: m.Length})
		}
		// 实例已销毁、先前已消失或已属于他人时，都可以移除眼前这份旧标记。
		if !r.removeMarker(entries[i].path) {
			done = false
		}
	}
	return done, blocked
}

// destroyBlockedBy 在销毁前检查这个分区上的计算进程。返回 nil 表示可尝试驱动销毁，错误说明本轮应保留
// 它的原因。 如果仍有计算进程，返回 errInstanceInUse。
//
// 有活跃进程就保留整个分区，包括外部工具创建的分区。下轮再次检查，最后一个进程退出后才能继续回收。
//
// 先问进程状态可以避免部分拆除。DestroyInstance 会先销毁各 CI，再销毁 GI；若一个 GI 同时包含空闲和忙
// 碌的 CI，直接执行可能先删掉空闲 CI，之后才因另一 CI 正忙而失败。
//
// 因检查结果而跳过也计入连续受阻次数。达到 reclaimMaxDestroyMisses 时报告仍占用该区间的原因。
//
// errNoAddressableDevice 表示没有可查询的 MIG 设备句柄，例如 GI 尚未创建 CI，或者驱动的 MIG 设备在容
// 器内不可见。这种情况允许让驱动自行判断能否销毁。若有设备可查询但调用失败，则保留分区，避免在进程状
// 态未知时开始拆除。
//
// 在检查结束后也可能有新进程进入，驱动此时仍可返回 errInstanceInUse 阻止销毁。
func (r *reclaimer) destroyBlockedBy(card string, inst migInstance) error {
	procs, err := r.driver.InstanceProcesses(card, inst)
	switch {
	case errors.Is(err, errNoAddressableDevice):
		r.logger.Info("reclaim: no mig device addresses this partition, leaving the destroy to the driver",
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

// worseBlock 为同轮多个阻塞原因选择告警内容。查询失败比工作负载正常占用更需要排查，因此优先报告前者。
func worseBlock(held, next error) error {
	if held == nil || (errors.Is(held, errInstanceInUse) && !errors.Is(next, errInstanceInUse)) {
		return next
	}
	return held
}

// findLiveGi 从指定卡的实时枚举结果中查找给定 GI 编号。
func findLiveGi(instances []migInstance, giID uint32) (migInstance, bool) {
	for i := range instances {
		if instances[i].GiID == giID {
			return instances[i], true
		}
	}
	return migInstance{}, false
}

// supersedingGiIDsOnCard 找出同一张卡上由其他 Pod 的标记认领的 GI 编号。清理旧 Pod 时若见到第二份认
// 领，就保留该编号对应的实例。
//
// 此处只核对同步写入的其他标记。另一个 Pod 是否已离开，由它自己的回收流程判断。用本轮开始时的 UID 快
// 照过滤标记，可能漏掉刚刚完成分配的新 Pod。
//
// 旧持有者先删除自己已被取代的标记。即便两个已结束 Pod 的标记同时指向同一 GI，最终剩下的那份仍可按普
// 通流程处理。
func supersedingGiIDsOnCard(entries []markerEntry, cardUUID, reclaimingPodUID string) map[uint32]bool {
	claimed := make(map[uint32]bool)
	for i := range entries {
		m := entries[i].marker
		if m.Card == cardUUID && m.PodUID != reclaimingPodUID {
			claimed[m.GiID] = true
		}
	}
	return claimed
}

// supersedingCorruptOnCard 检查损坏文件是否可能是其他 Pod 对该卡的认领。这与 supersedingGiIDsOnCard
// 对可读标记所做的检查相对应。因无法从损坏内容恢复 GI 编号，只能按卡暂缓销毁。
//
// 即便内容已经损坏，路径仍可能表明卡和 Pod 的身份，按以下情况判断。
//
// - 无法从路径识别卡时，损坏记录可能属于任意卡，所有卡都要保留。
// - 路径指向当前卡，并且属于另一 Pod 或无法识别所有者时，暂缓销毁。
// - 路径指向当前卡和正在清理的 Pod 时，不将其视为另一 Pod 的认领，以免本 Pod 自己的损坏文件导致永久
// 无法回收。
func supersedingCorruptOnCard(corrupt []string, podsDir, cardUUID, reclaimingPodUID string) bool {
	for _, p := range corrupt {
		card, ok := cardFromMarkerPath(p)
		if !ok {
			return true
		}
		if card != cardUUID {
			continue
		}
		if owner, ok := podUIDFromMarkerPath(podsDir, p); !ok || owner != reclaimingPodUID {
			return true
		}
	}
	return false
}

// matchesMarker 核对 MIG UUID、区间起点和长度，用来识别 NVML 将 GI 编号复用给不同分区的情况。
//
// 同一位置被释放后再创建时，MIG UUID 和区间都有可能重现。要区分旧分区和同一位置上的继任者，还要通过
// supersedingGiIDsOnCard 查询另一个 Pod 是否已经写入认领标记。
//
// AllocatedPhysicalID 可帮助定位分区，仅有这个值无法证明找到的分区仍属于最初的 Pod。
func (in migInstance) matchesMarker(m migMarker) bool {
	return in.UUID == m.MigUUID &&
		in.Placement.Start == m.Start &&
		in.Placement.Length == m.Length
}

// removeMarker 删除指定标记，再清理已经为空的容器和 Pod 目录；同目录中其他容器的有效标记会保留。返回
// 是否删除成功。
func (r *reclaimer) removeMarker(path string) bool {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		r.logger.Error(err, "reclaim: remove marker", "path", path)
		return false
	}
	removeIfEmpty(filepath.Dir(path))
	removeIfEmpty(filepath.Dir(filepath.Dir(path)))
	return true
}

// pruneCorruptMarkers 根据路径中的 Pod UID 处理损坏标记。该 UID 连续 reclaimMaxMisses 轮都未出现后就
// 删除文件。它原先遮挡的分区随后可按无标记实例处理，待卡排空后回收。所属 Pod 已离开，此路径不再有并
// 发 Allocate 写入。
//
// 以下两类文件继续保留。
// - 如果所属 Pod 仍在运行，损坏文件仍可能是该 Pod 的唯一所有权记录。
// - 如果遍历失败的路径连 Pod UID 都无法确定，回收器无法判断其所有者是否存活，需要修复文件系统。
//
// 后一种路径也无法识别设备。只要该路径仍不可读，ownershipUnknownOnCard 就会暂停整台节点上遗留分区的
// 接管和无标记实例的回收。
//
// 两类持续保留情况分别按路径计数，达到 reclaimMaxCorruptHoldMisses 时记录一次管理员可见的日志，写明
// 文件和处理方式。 存活所有者的特例见 holdLiveOwnersMarker。
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

// holdLiveOwnersMarker 保留仍在运行的 Pod 的损坏标记，并累计实际阻塞轮数。用于等待 Pod 删除的缺失计
// 数在每轮存活时都归零，因此告警计数使用另一组键；路径消失后两个计数都会清理。 持续达到
// reclaimMaxCorruptHoldMisses 时报告路径和修复方式。
//
// 只重启这个 Pod 的容器仍会遇到同一份不可读的标记，分配重试无法重新绑定。标记里原有的身份也不能从其
// 它状态完整恢复。所属 Pod 被删除后，回收器才能根据路径和连续缺失轮数清理标记。
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
		"reclaim: an unreadable MIG ownership record of a RUNNING pod is holding its card closed: while it "+
			"persists no leftover partition on that card is adopted and none is reclaimed, and the pod cannot be "+
			"re-admitted either, because a container's reserve reads its own record first and fails closed on an "+
			"unreadable one — so restarting the pod cannot clear this, and the record cannot be rebuilt from "+
			"anything outside it; delete the pod to release the card",
		"path", path, "podUID", uid, "card", card, "passes", r.misses[key])
}

// holdUnattributablePath 保留无法识别所有者的损坏路径，并累计它阻塞节点的轮数。达到阈值时报告一次具
// 体路径。直到文件系统问题消失，回收器都没有安全清除该路径的依据。 告警阈值由
// reclaimMaxCorruptHoldMisses 指定。
func (r *reclaimer) holdUnattributablePath(path string, touched sets.Set[string]) {
	key := corruptMissPrefix + path
	touched.Insert(key)
	r.misses[key]++
	r.logger.Info("reclaim: unparseable marker path names no pod, holding its cards closed",
		"path", path, "passes", r.misses[key])
	if r.misses[key] == reclaimMaxCorruptHoldMisses {
		r.logger.Error(errCorruptPathUnattributable,
			"reclaim: an unreadable path under the pod work root names neither a pod nor a card, so MIG ownership "+
				"cannot be proven on ANY card of this node: no leftover partition is adopted and none is reclaimed "+
				"while it persists; this is a filesystem fault that will not clear by itself — repair or remove the "+
				"path",
			"path", path, "passes", r.misses[key])
	}
}

// destroyOrphans 清理已排空卡上的无标记 GI。取得卡锁后再次扫描，只要新发现该卡的标记，或有损坏路径可
// 能属于它，就终止本轮。检查每个分区的实时进程，忙碌的分区继续保留。对持续返回 NVML_ERROR_IN_USE 的
// 情况达到阈值会记录日志，成功处理全部实例后清除计数。
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
			r.logger.Error(derr, "reclaim: destroy orphan gpu instance on drained card", "card", card, "giID", inst.GiID)
			continue
		}
		// 记录被销毁分区的完整身份。若某次分配刚刚授予分区而早先快照漏掉了标记，可据此追查该次回收。
		r.logger.Info("reclaim: destroyed a marker-less partition on a drained card",
			"card", card, "giID", inst.GiID, "ciID", inst.CiID, "migUUID", inst.UUID,
			"placement", inst.Placement)
		destroyed++
	}

	if blocked != nil {
		r.inUse[missKey]++
		if r.inUse[missKey] == reclaimMaxDestroyMisses {
			r.logger.Error(blocked,
				"reclaim: a marker-less mig instance on a drained card is still not reclaimable after bounded "+
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
			r.logger.Info("reclaim: reclaimed marker-less orphans on drained card", "card", card, "count", destroyed)
		}
	}
}

// removeIfEmpty 只删除空目录，防止删除一个容器的记录时影响其他容器。
func removeIfEmpty(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) > 0 {
		return
	}
	_ = os.Remove(dir)
}
