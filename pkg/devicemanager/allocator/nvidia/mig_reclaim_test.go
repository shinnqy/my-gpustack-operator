package nvidia

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gpustack.ai/gpustack/pkg/deviceplugin"
)

// noClaims 是空的归属自校验源：没有正在运行的 Pod 拥有任何放置权。
func noClaims() (map[string][]migPlacement, error) { return nil, nil }

// seedMarkedInstance 在加速器上初始化一个活跃实例，并写入其所有权标记文件，从而让这对组件能够模拟出
// Pod 所创建的、已记录的 MIG 分区。
func seedMarkedInstance(t *testing.T, drv *fakeMigDriver, podUID, card string, giID uint32) {
	t.Helper()
	inst := migInstance{GiID: giID, CiID: giID, ComputeSlices: 1, Placement: migPlacement{Start: 0, Length: 2}, UUID: "MIG-" + card}
	drv.seedLive(card, inst)
	require.NoError(t, writeMarker(markerPath(podUID, "c", card), migMarker{
		PodUID: podUID, Container: "c", Card: card, Profile: "1g.10gb",
		GiID: giID, CiID: giID, MigUUID: inst.UUID, ComputeSlices: 1, Start: 0, Length: 2,
	}))
}

func TestReclaim_DestroysDeadPodAfterDebounce(t *testing.T) {
	redirectLogicalSliceDirs(t)
	drv := newFakeMigDriver()
	seedMarkedInstance(t, drv, "pod-dead", testGPUUUID0, 1)
	r := newReclaimer(drv, deviceplugin.OperatorPodsDir, logr.Discard(), noClaims)

	// pod-dead 未出现在活跃集合中，但在去抖之前的一系列处理中仍然存在。
	for i := 0; i < reclaimMaxMisses-1; i++ {
		r.reconcile(nil)
	}
	assert.Empty(t, drv.destroyed, "not destroyed before the debounce")
	_, err := parseMarker(markerPath("pod-dead", "c", testGPUUUID0))
	require.NoError(t, err, "marker still present before the debounce")

	// reclaimMaxMisses-th 轮会销毁实例并移除标记文件。
	r.reconcile(nil)
	require.Len(t, drv.destroyed, 1)
	assert.Equal(t, uint32(1), drv.destroyed[0].GiID)
	_, err = parseMarker(markerPath("pod-dead", "c", testGPUUUID0))
	require.Error(t, err, "marker removed after reclaim")
}

func TestReclaim_KeepsLivePod(t *testing.T) {
	redirectLogicalSliceDirs(t)
	drv := newFakeMigDriver()
	seedMarkedInstance(t, drv, "pod-live", testGPUUUID0, 1)
	r := newReclaimer(drv, deviceplugin.OperatorPodsDir, logr.Discard(), noClaims)

	for i := 0; i < reclaimMaxMisses+2; i++ {
		r.reconcile([]string{"pod-live"})
	}
	assert.Empty(t, drv.destroyed, "a live pod's instance is never reclaimed")
	_, err := parseMarker(markerPath("pod-live", "c", testGPUUUID0))
	require.NoError(t, err)
}

// TestReclaim_InUseBoundedRetryAndCondition 断言存在剩余的 NVML_ERROR_IN_USE 错误，该错误不会破坏实
// 例，每次尝试都会继续重试（去抖机制不会清除），在限制条件下仅一次展示可操作者可见的异常状态，当进程
// 结束时会进行回收。
func TestReclaim_InUseBoundedRetryAndCondition(t *testing.T) {
	redirectLogicalSliceDirs(t)
	drv := newFakeMigDriver()
	drv.inUseGiIDs = map[uint32]bool{1: true}
	seedMarkedInstance(t, drv, "pod-dead", testGPUUUID0, 1)

	var conditions int
	logger := funcr.New(func(_, args string) {
		if strings.Contains(args, "not reclaimable after bounded retries") {
			conditions++
		}
	}, funcr.Options{})
	r := newReclaimer(drv, deviceplugin.OperatorPodsDir, logger, noClaims)

	for i := 0; i < reclaimMaxMisses+reclaimMaxDestroyMisses+2; i++ {
		r.reconcile(nil)
	}
	assert.Empty(t, drv.destroyed, "an in-use instance is never destroyed")
	_, err := parseMarker(markerPath("pod-dead", "c", testGPUUUID0))
	require.NoError(t, err, "the marker is retained while the instance is in use")
	assert.Equal(t, 1, conditions, "the operator-visible condition fires exactly once at the bound")

	// 当剩余进程退出后，下一轮操作会对其进行回收。
	drv.inUseGiIDs = nil
	r.reconcile(nil)
	require.Len(t, drv.destroyed, 1)
	_, err = parseMarker(markerPath("pod-dead", "c", testGPUUUID0))
	require.Error(t, err)
}

// TestReclaim_MisAttributedMarkerNotDestroyed 验证回收器会再次核对归属。即使 getAllocatingPod 曾把标
// 记绑定到错误的 Pod，只要另一个运行中的 Pod 仍认领该区间，就保留分区。
func TestReclaim_MisAttributedMarkerNotDestroyed(t *testing.T) {
	redirectLogicalSliceDirs(t)
	drv := newFakeMigDriver()
	seedMarkedInstance(t, drv, "pod-dead", testGPUUUID0, 1)
	claims := func() (map[string][]migPlacement, error) {
		return map[string][]migPlacement{testGPUUUID0: {{Start: 0, Length: 2}}}, nil
	}
	r := newReclaimer(drv, deviceplugin.OperatorPodsDir, logr.Discard(), claims)

	for i := 0; i < reclaimMaxMisses+3; i++ {
		r.reconcile(nil)
	}
	assert.Empty(t, drv.destroyed, "a placement a running pod claims is never destroyed")
	_, err := parseMarker(markerPath("pod-dead", "c", testGPUUUID0))
	require.NoError(t, err, "the mis-attributed marker is retained")
}

// TestReclaim_FailClosedOnClaimsError 断言了 liveClaims 读取错误，导致整个流程中断——自检查无法运行，
// 因此不会面临销毁风险。
func TestReclaim_FailClosedOnClaimsError(t *testing.T) {
	redirectLogicalSliceDirs(t)
	drv := newFakeMigDriver()
	seedMarkedInstance(t, drv, "pod-dead", testGPUUUID0, 1)
	claims := func() (map[string][]migPlacement, error) {
		return nil, assert.AnError
	}
	r := newReclaimer(drv, deviceplugin.OperatorPodsDir, logr.Discard(), claims)

	for i := 0; i < reclaimMaxMisses+3; i++ {
		r.reconcile(nil)
	}
	assert.Empty(t, drv.destroyed, "a claims read error fails closed (no destroy)")
}

// TestReclaim_FailClosedOnListError 断言了 ListInstances 错误，导致整个处理流程被跳过——没有实时状态
// 视图的话，所有权标记文件的检查就无法进行，因此不会产生破坏风险。
func TestReclaim_FailClosedOnListError(t *testing.T) {
	redirectLogicalSliceDirs(t)
	drv := newFakeMigDriver()
	drv.listErr = assert.AnError
	seedMarkedInstance(t, drv, "pod-dead", testGPUUUID0, 1)
	r := newReclaimer(drv, deviceplugin.OperatorPodsDir, logr.Discard(), noClaims)

	for i := 0; i < reclaimMaxMisses+3; i++ {
		r.reconcile(nil)
	}
	assert.Empty(t, drv.destroyed, "a ListInstances error fails closed (no destroy)")
}

// TestReclaim_StaleMarkerGiIdReuseNotDestroyed 断言了身份验证：一个已死亡的容器中的旧标记，其 GI 标
// 识符和 NVML 已被用于另一个（存活的）实例，且这两个实例的占用时间不重叠——这种基于放置区间的归属验证
// 无法发现这种情况——该标记会被丢弃，而不会破坏存活的实例，因为该标记所记录的 MIG-设备 UUID 与存活实
// 例的不再匹配。
func TestReclaim_StaleMarkerGiIdReuseNotDestroyed(t *testing.T) {
	redirectLogicalSliceDirs(t)
	drv := newFakeMigDriver()
	// GI 1 现在是处于活动状态的 Pod 实例，位于第 4 个时隙（UUID：MIG-new），由其自身的标记文件记录。
	drv.seedLive(testGPUUUID0, migInstance{GiID: 1, CiID: 1, ComputeSlices: 1, Placement: migPlacement{4, 2}, UUID: "MIG-new"})
	require.NoError(t, writeMarker(markerPath("pod-new", "c", testGPUUUID0), migMarker{
		PodUID: "pod-new", Container: "c", Card: testGPUUUID0, Profile: "1g.10gb",
		GiID: 1, CiID: 1, MigUUID: "MIG-new", ComputeSlices: 1, Start: 4, Length: 2,
	}))
	// 死亡 Pod 的陈旧标记被记录在 GI 1 中，使用旧的 UUID，并存储在不同的、不重叠的 slot 中。
	require.NoError(t, writeMarker(markerPath("pod-dead", "c", testGPUUUID0), migMarker{
		PodUID: "pod-dead", Container: "c", Card: testGPUUUID0, Profile: "1g.10gb",
		GiID: 1, CiID: 1, MigUUID: "MIG-old", ComputeSlices: 1, Start: 0, Length: 2,
	}))
	claims := func() (map[string][]migPlacement, error) {
		return map[string][]migPlacement{testGPUUUID0: {{Start: 4, Length: 2}}}, nil
	}
	r := newReclaimer(drv, deviceplugin.OperatorPodsDir, logr.Discard(), claims)

	for i := 0; i < reclaimMaxMisses+1; i++ {
		r.reconcile([]string{"pod-new"})
	}
	assert.Empty(t, drv.destroyed, "a reused GI id (UUID mismatch) never destroys the live instance")
	_, err := parseMarker(markerPath("pod-dead", "c", testGPUUUID0))
	require.Error(t, err, "the stale marker is dropped")
	_, err = parseMarker(markerPath("pod-new", "c", testGPUUUID0))
	require.NoError(t, err, "the live pod's marker is intact")
}

// migSeed 是指用于在加速器上进行种子处理的单个活跃 GPU 实例，因为 orphan sweep 认为它没有可解析的标
// 记文件来拥有它。
type migSeed struct {
	card string
	giID uint32
}

// seedInstances 将每个 migSeed 作为实时实例，在其加速器上的不同时隙中启动。
func seedInstances(drv *fakeMigDriver, seeds []migSeed) {
	for i, s := range seeds {
		drv.seedLive(s.card, migInstance{
			GiID: s.giID, CiID: s.giID, ComputeSlices: 1,
			Placement: migPlacement{Start: int32(i) * 2, Length: 2}, UUID: "MIG-" + s.card,
		})
	}
}

// TestReclaim_PartitionRunningAProcessIsKept 断言了此循环每次执行前进行的进程检查。携带计算进程的分
// 区在两种情况下都会被保留——既可以是被删除的 Pod 的记录状态，也可以是在其他资源被释放后的无标记孤儿
// 状态，这种状态会导致该分区被从用户中移除。它被静静地保留着：没有问题，因此不会报告任何错误。
//
// 未能进行分区的操作会导致分裂成两部分，且这两部分走向相反方向。没有 MIG 设备地址的分区中没有任何可
// 询问的内容——没有携带计算实例的 GPU 实例，或者每个分区作为容器时，驱动程序的 MIG 设备对它们来说是隐
// 藏的——于是销毁过程便开始。那些驱动程序不会回答的分区会被保留：对状态未知的分区进行销毁操作，就能将
// 闲置的计算实例从繁忙的实例中移除。
func TestReclaim_PartitionRunningAProcessIsKept(t *testing.T) {
	cases := []struct {
		name string
		// 被标记的分区由死 Pod 的所有权标记文件所拥有；否则，扫描过程会将其识别为无标记的孤儿节点。
		marked        bool
		processes     int
		processesErr  error
		wantDestroyed bool
	}{
		{name: "a dead pod's partition running a process is kept", marked: true, processes: 1},
		{name: "a dead pod's idle partition is reclaimed", marked: true, wantDestroyed: true},
		{name: "an orphan running a process is kept on a drained card", processes: 2},
		{name: "an idle orphan is reclaimed on a drained card", wantDestroyed: true},
		{
			name:   "a dead pod's partition no mig device addresses is left to the driver",
			marked: true, processesErr: fmt.Errorf("probe: %w", errNoAddressableDevice), wantDestroyed: true,
		},
		{
			name:         "an orphan no mig device addresses is left to the driver",
			processesErr: fmt.Errorf("probe: %w", errNoAddressableDevice), wantDestroyed: true,
		},
		{
			name:   "a dead pod's partition the driver would not answer about is kept",
			marked: true, processesErr: assert.AnError,
		},
		{
			name:         "an orphan the driver would not answer about is kept",
			processesErr: assert.AnError,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			redirectLogicalSliceDirs(t)
			drv := newFakeMigDriver()
			drv.processesByGiID = map[uint32]int{1: c.processes}
			drv.processesErr = c.processesErr
			if c.marked {
				seedMarkedInstance(t, drv, "pod-dead", testGPUUUID0, 1)
			} else {
				seedInstances(drv, []migSeed{{card: testGPUUUID0, giID: 1}})
			}

			var errorRecords int
			logger := funcr.New(func(_, args string) {
				if strings.Contains(args, `"error"=`) {
					errorRecords++
				}
			}, funcr.Options{})
			r := newReclaimer(drv, deviceplugin.OperatorPodsDir, logger, noClaims)

			for i := 0; i < reclaimMaxMisses+3; i++ {
				r.reconcile(nil)
			}

			if c.wantDestroyed {
				require.Len(t, drv.destroyed, 1)
				assert.Equal(t, uint32(1), drv.destroyed[0].GiID)
			} else {
				assert.Empty(t, drv.destroyed, "a partition in use is never destroyed")
			}
			if c.marked {
				_, err := parseMarker(markerPath("pod-dead", "c", testGPUUUID0))
				if c.wantDestroyed {
					require.Error(t, err, "the marker goes with the partition")
				} else {
					require.NoError(t, err, "the marker is kept, so a later pass finds the partition again")
				}
			}
			assert.Zero(t, errorRecords, "a partition in use is not a failure to report")
		})
	}
}

// TestReclaim_ProcessHeldPartitionReachesTheBound 断言，预检查过程不会让操作员面临该循环所设定的限制。
// 跳过销毁操作意味着驱动程序从未拒绝该操作，因此使用计数器不会增加，受限制日志也不会因为该条件而触发
// ——即由泄露的进程长期持有分区，占据其放置区间。因此，跳过操作也被视为正在使用状态：日志在达到限制时
// 仅触发一次，与被拒绝的销毁操作所导致的情况一致。
func TestReclaim_ProcessHeldPartitionReachesTheBound(t *testing.T) {
	redirectLogicalSliceDirs(t)
	drv := newFakeMigDriver()
	drv.processesByGiID = map[uint32]int{1: 1}
	seedMarkedInstance(t, drv, "pod-dead", testGPUUUID0, 1)

	var bounded int
	logger := funcr.New(func(_, args string) {
		if strings.Contains(args, "not reclaimable after bounded retries") {
			bounded++
		}
	}, funcr.Options{})
	r := newReclaimer(drv, deviceplugin.OperatorPodsDir, logger, noClaims)

	for i := 0; i < reclaimMaxMisses+reclaimMaxDestroyMisses+2; i++ {
		r.reconcile(nil)
	}
	assert.Empty(t, drv.destroyed, "the destroy is never attempted while a process holds it")
	_, err := parseMarker(markerPath("pod-dead", "c", testGPUUUID0))
	require.NoError(t, err, "the marker is retained")
	assert.Equal(t, 1, bounded, "the operator-visible log fires exactly once at the bound")

	// 当进程退出后，下一轮操作会对其进行回收，这也就实现了放置区间的确定。
	drv.processesByGiID = nil
	r.reconcile(nil)
	require.Len(t, drv.destroyed, 1)
	_, err = parseMarker(markerPath("pod-dead", "c", testGPUUUID0))
	require.Error(t, err)
}

// TestReclaim_ProcessHeldOrphanReachesTheBound 是针对该循环执行的其他销毁操作的相同断言。这两种路径
// 分别统计各路使用过的次数——一个死亡的 Pod 的 UID，以及正在被清理的孤儿加速器的 UID——且每种路径都会
// 生成自己的日志，因此当一种路径统计而另一种不统计时，就会使一个被保留的分区无法被操作员访问到。这是
// 负责处理该操作员之外创建的分区的路径：它从未有过可被识别的标记，因此只能作为孤儿状态被访问到。
func TestReclaim_ProcessHeldOrphanReachesTheBound(t *testing.T) {
	redirectLogicalSliceDirs(t)
	drv := newFakeMigDriver()
	seedInstances(drv, []migSeed{{card: testGPUUUID0, giID: 1}})
	drv.processesByGiID = map[uint32]int{1: 1}

	var bounded int
	logger := funcr.New(func(_, args string) {
		if strings.Contains(args, "marker-less mig instance on a drained card is still not reclaimable") {
			bounded++
		}
	}, funcr.Options{})
	r := newReclaimer(drv, deviceplugin.OperatorPodsDir, logger, noClaims)

	for i := 0; i < reclaimMaxMisses+reclaimMaxDestroyMisses+2; i++ {
		r.reconcile(nil)
	}
	assert.Empty(t, drv.destroyed, "the destroy is never attempted while a process holds it")
	assert.Equal(t, 1, bounded, "the operator-visible log fires exactly once at the bound")

	// 当进程退出后，下一轮操作会对其进行回收，这也就实现了放置区间的确定。
	drv.processesByGiID = nil
	r.reconcile(nil)
	require.Len(t, drv.destroyed, 1)
	assert.Equal(t, uint32(1), drv.destroyed[0].GiID)
}

// TestReclaim_BoundNamesWhatBlockedTheReclaim 用于说明操作员实际看到的日志中标注了正确的错误原因。无
// 论是在分区上运行的进程，还是无法获取其进程信息的分区，都会阻碍销毁操作，并计入相同的限制范围内，但
// 它们所需要满足的条件却截然不同——要么找到该进程，要么让容器能够访问驱动程序——而区分这两种情况的说明
// 语句则位于提供的详细内容下方。如果将查询失败报告为 errInstanceInUse 错误，就会让操作员去寻找不存在
// 的进程。
func TestReclaim_BoundNamesWhatBlockedTheReclaim(t *testing.T) {
	cases := []struct {
		name string
		// processes 指定分区上运行的进程数，processesErr 可以让查询本身失败。
		processes    int
		processesErr error
		// wantNamed 必须出现在有边界的日志中，wantNotNamed 则不得出现。
		wantNamed    string
		wantNotNamed string
	}{
		{
			name:      "a process holding it is reported as in use",
			processes: 1, wantNamed: errInstanceInUse.Error(),
		},
		{
			name:         "a query failure is reported as itself, not as a process",
			processesErr: assert.AnError,
			wantNamed:    assert.AnError.Error(), wantNotNamed: errInstanceInUse.Error(),
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			redirectLogicalSliceDirs(t)
			drv := newFakeMigDriver()
			drv.processesByGiID = map[uint32]int{1: c.processes}
			drv.processesErr = c.processesErr
			seedMarkedInstance(t, drv, "pod-dead", testGPUUUID0, 1)

			var bounded []string
			logger := funcr.New(func(_, args string) {
				if strings.Contains(args, "not reclaimable after bounded retries") {
					bounded = append(bounded, args)
				}
			}, funcr.Options{})
			r := newReclaimer(drv, deviceplugin.OperatorPodsDir, logger, noClaims)

			for i := 0; i < reclaimMaxMisses+reclaimMaxDestroyMisses+2; i++ {
				r.reconcile(nil)
			}
			require.Len(t, bounded, 1, "the operator-visible log fires exactly once at the bound")
			assert.Contains(t, bounded[0], c.wantNamed, "the log names what actually blocked the reclaim")
			if c.wantNotNamed != "" {
				assert.NotContains(t, bounded[0], c.wantNotNamed, "and does not name a cause it cannot know")
			}
			assert.Empty(t, drv.destroyed, "the destroy is never attempted while the partition is blocked")
		})
	}
}

// TestReclaim_CorruptMarkerHoldsCardClosed 断言：某个加速器所持有的无法解析的标记文件永远不会被视为
// 已耗尽资源；否则该实例将完全像没有标记的孤儿对象一样，在运行中的容器下被销毁。这种保留行为针对每个
// 加速器进行（其他加速器中的孤儿对象仍会被收集，因此一个损坏的文件不会限制节点的容量），且处于活跃状
// 态的损坏标记文件会被保留而非废弃。没有任何 Pod 名称的损坏路径也会被无限期保留：由于没有所有者，也
// 没有任何活跃性证明可以对其进行废弃处理。
func TestReclaim_CorruptMarkerHoldsCardClosed(t *testing.T) {
	cases := []struct {
		name string
		// corruptPod 拥有 corrupt 标记文件；strayPath 将其写入容器目录上方一级目录中，该目录中的路径不会以
		// Pod 命名。
		corruptPod  string
		corruptFile string
		strayPath   bool
		live        []string
		seeds       []migSeed
		// wantDestroyed 列出了在以下步骤之后被回收的 GPU-实例 ID。
		wantDestroyed []uint32
	}{
		{
			name:        "a live pod's corrupt marker keeps its instance off the orphan sweep",
			corruptPod:  "pod-live",
			corruptFile: markerFileName(testGPUUUID0),
			live:        []string{"pod-live"},
			seeds:       []migSeed{{card: testGPUUUID0, giID: 1}},
		},
		{
			name:        "a corrupt marker naming no accelerator holds every accelerator",
			corruptPod:  "pod-live",
			corruptFile: markerFileName(""),
			live:        []string{"pod-live"},
			seeds:       []migSeed{{card: testGPUUUID0, giID: 1}, {card: testGPUUUID1, giID: 2}},
		},
		{
			name:          "a sibling accelerator's orphan is still collected",
			corruptPod:    "pod-live",
			corruptFile:   markerFileName(testGPUUUID1),
			live:          []string{"pod-live"},
			seeds:         []migSeed{{card: testGPUUUID0, giID: 1}, {card: testGPUUUID1, giID: 2}},
			wantDestroyed: []uint32{1},
		},
		{
			name:        "a corrupt path naming no pod is held indefinitely",
			corruptPod:  "pod-dead",
			corruptFile: markerFileName(testGPUUUID0),
			strayPath:   true,
			seeds:       []migSeed{{card: testGPUUUID0, giID: 1}},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			redirectLogicalSliceDirs(t)
			drv := newFakeMigDriver()
			seedInstances(drv, c.seeds)
			dir := deviceplugin.PodWorkDir(c.corruptPod, "c")
			if c.strayPath {
				dir = filepath.Join(deviceplugin.OperatorPodsDir, c.corruptPod)
			}
			path := writeCorruptMarker(t, dir, c.corruptFile)
			r := newReclaimer(drv, deviceplugin.OperatorPodsDir, logr.Discard(), noClaims)

			for i := 0; i < reclaimMaxMisses*2+2; i++ {
				r.reconcile(c.live)
			}

			destroyed := make([]uint32, 0, len(drv.destroyed))
			for _, inst := range drv.destroyed {
				destroyed = append(destroyed, inst.GiID)
			}
			assert.ElementsMatch(t, c.wantDestroyed, destroyed)
			assert.FileExists(t, path, "the corrupt marker is kept while it can still stand for an owner")
		})
	}
}

// TestReclaim_UnattributableCorruptPathBoundedLog 断言了这个循环无法释放的保留项并非静默状态：一个既
// 不是 Pod 也不是加速器名称的损坏路径会导致整个节点持续失败关闭（没有活跃性证据可以对其进行废弃），
// 且循环仅对这条路径进行了一次记录，位于边界处——与 IN_USE 路径使用的界面相同，因为状态条件会被
// wholesale Devices.Status 的重建操作所覆盖。
func TestReclaim_UnattributableCorruptPathBoundedLog(t *testing.T) {
	redirectLogicalSliceDirs(t)
	drv := newFakeMigDriver()
	seedInstances(drv, []migSeed{{card: testGPUUUID0, giID: 1}})
	// 位于容器目录上方一级的名为 marker的文件：其路径名称不包含 Pod 信息，且其自身名称也无法归属于任何加
	// 速器。
	path := writeCorruptMarker(t, filepath.Join(deviceplugin.OperatorPodsDir, "pod-dead"), markerFileName(""))

	var bounded int
	logger := funcr.New(func(_, args string) {
		if strings.Contains(args, "will not clear by itself") {
			bounded++
		}
	}, funcr.Options{})
	r := newReclaimer(drv, deviceplugin.OperatorPodsDir, logger, noClaims)

	for i := 0; i < reclaimMaxCorruptHoldMisses+3; i++ {
		r.reconcile(nil)
	}

	assert.FileExists(t, path, "an unattributable path is kept: there is no evidence to retire it on")
	assert.Empty(t, drv.destroyed, "and it keeps every card off the orphan sweep while it persists")
	assert.Equal(t, 1, bounded, "the operator-visible log fires exactly once at the bound")
}

// TestReclaim_LiveOwnersCorruptMarkerBoundedLog 表明兄弟持有状态并非处于静默状态。那些无法解析的记录
// 会被保留，因为其中的 Pod 仍处于运行状态——该 Pod 依赖于其所记录的拥有权——在 Pod 运行期间，此循环中
// 的任何操作都无法将其释放，因此这种状态被定义为短暂且非永久的。因此，它获得了相同的表面呈现：一份仅
// 在该特定边界处、仅对操作者可见的日志，记录了加速器、Pod 以及路径。该记录所做的事情在边界之前和之后
// 均保持不变。
func TestReclaim_LiveOwnersCorruptMarkerBoundedLog(t *testing.T) {
	redirectLogicalSliceDirs(t)
	drv := newFakeMigDriver()
	seedInstances(drv, []migSeed{{card: testGPUUUID0, giID: 1}})
	path := writeCorruptMarker(t, deviceplugin.PodWorkDir("pod-live", "c"), markerFileName(testGPUUUID0))

	var bounded int
	logger := funcr.New(func(_, args string) {
		if strings.Contains(args, "delete the pod to release the card") {
			bounded++
		}
	}, funcr.Options{})
	r := newReclaimer(drv, deviceplugin.OperatorPodsDir, logger, noClaims)
	live := []string{"pod-live"}

	for i := 0; i < reclaimMaxCorruptHoldMisses-1; i++ {
		r.reconcile(live)
	}
	assert.Zero(t, bounded, "the operator-visible log does not fire before the bound")
	assert.FileExists(t, path, "and the record is kept meanwhile")

	r.reconcile(live)
	assert.Equal(t, 1, bounded, "it fires at the bound")

	for i := 0; i < 3; i++ {
		r.reconcile(live)
	}
	assert.Equal(t, 1, bounded, "and exactly once, however long the hold stands")
	assert.FileExists(t, path, "the record is still kept: its live pod depends on the ownership it records")
	assert.Empty(t, drv.destroyed, "and its card stays off the orphan sweep throughout")
}

// TestReclaim_CorruptMarkerOfDeadPodConverges 断言：锁定机制会自行清除，而不会在节点生命周期内导致分
// 区泄漏：一个已损坏且对应的 Pod 已消失的标记文件，仅基于这一事实（其路径指向该 Pod）在与其他决策所
// 采用的去抖机制相同的时间后会被移除，它所遮蔽的分区随后就会成为真正的孤儿节点，直到加速器自身的去抖
// 机制结束。这种移除行为会在下一次处理中被观察到，而不会在移除该文件的那次处理中发生。
func TestReclaim_CorruptMarkerOfDeadPodConverges(t *testing.T) {
	cases := []struct {
		name        string
		corruptFile string
	}{
		{name: "corrupt marker naming its card", corruptFile: markerFileName(testGPUUUID0)},
		{name: "corrupt marker naming no card", corruptFile: markerFileName("")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			redirectLogicalSliceDirs(t)
			drv := newFakeMigDriver()
			seedInstances(drv, []migSeed{{card: testGPUUUID0, giID: 1}})
			path := writeCorruptMarker(t, deviceplugin.PodWorkDir("pod-dead", "c"), c.corruptFile)
			r := newReclaimer(drv, deviceplugin.OperatorPodsDir, logr.Discard(), noClaims)

			for i := 0; i < reclaimMaxMisses-1; i++ {
				r.reconcile(nil)
			}
			assert.FileExists(t, path, "the corrupt marker is not retired before the debounce")

			r.reconcile(nil)
			assert.NoFileExists(t, path, "the corrupt marker of a dead pod is retired after the debounce")
			assert.Empty(t, drv.destroyed, "the pass that removed the file still holds the card closed")

			// 记录消失后，被遮蔽的分区便成为被关闭加速器上单纯的孤儿状态。
			for i := 0; i < reclaimMaxMisses-1; i++ {
				r.reconcile(nil)
			}
			assert.Empty(t, drv.destroyed, "the card restarts its own drained debounce from scratch")

			r.reconcile(nil)
			require.Len(t, drv.destroyed, 1, "the shadowed partition is collected once the card drains")
			assert.Equal(t, uint32(1), drv.destroyed[0].GiID)
		})
	}
}

// TestReclaim_OrphanKeptWhileCardHasMarker 断言了被耗尽加速器的保护机制：当加速器仍携带任何标记时，
// 没有标记的孤儿对象不会被垃圾回收（GC），因为这样的加速器并未完全被耗尽资源。
func TestReclaim_OrphanKeptWhileCardHasMarker(t *testing.T) {
	redirectLogicalSliceDirs(t)
	drv := newFakeMigDriver()
	drv.inUseGiIDs = map[uint32]bool{1: true} // 被标记 Pod的GI 1正在使用中无法释放
	seedMarkedInstance(t, drv, "pod-stuck", testGPUUUID0, 1)
	// 无标记的孤儿资源共享加速器。
	drv.seedLive(testGPUUUID0, migInstance{GiID: 2, CiID: 2, ComputeSlices: 1, Placement: migPlacement{2, 2}, UUID: "MIG-orphan"})
	r := newReclaimer(drv, deviceplugin.OperatorPodsDir, logr.Discard(), noClaims)

	for i := 0; i < reclaimMaxMisses+2; i++ {
		r.reconcile(nil)
	}
	assert.Empty(t, drv.destroyed, "an orphan is held while the card still carries any marker (not fully drained)")
}

// TestReclaim_OrphanGCOnlyOnDrainedCard 断言：当任何存活的 Pod 位于加速器上时，无标记的 GPU 实例会被
// 保留；且仅在加速器完全耗尽、超过去抖阈值后才会被回收一次。
func TestReclaim_OrphanGCOnlyOnDrainedCard(t *testing.T) {
	t.Run("kept while a live pod is on the accelerator", func(t *testing.T) {
		redirectLogicalSliceDirs(t)
		drv := newFakeMigDriver()
		seedMarkedInstance(t, drv, "pod-live", testGPUUUID0, 1)
		// 无标记孤儿（即 GI-create与marker-write之间的崩溃）共享加速器。
		drv.seedLive(testGPUUUID0, migInstance{GiID: 2, CiID: 2, ComputeSlices: 1, Placement: migPlacement{2, 2}, UUID: "MIG-orphan"})
		r := newReclaimer(drv, deviceplugin.OperatorPodsDir, logr.Discard(), noClaims)

		for i := 0; i < reclaimMaxMisses+3; i++ {
			r.reconcile([]string{"pod-live"})
		}
		assert.Empty(t, drv.destroyed, "an orphan on a card hosting a live pod is kept")
	})

	t.Run("gc'd after the accelerator drains and the debounce", func(t *testing.T) {
		redirectLogicalSliceDirs(t)
		drv := newFakeMigDriver()
		// 一个没有标记标记的孤儿对象，位于其他为空的处理器上。
		drv.seedLive(testGPUUUID0, migInstance{GiID: 2, CiID: 2, ComputeSlices: 1, Placement: migPlacement{2, 2}, UUID: "MIG-orphan"})
		r := newReclaimer(drv, deviceplugin.OperatorPodsDir, logr.Discard(), noClaims)

		for i := 0; i < reclaimMaxMisses-1; i++ {
			r.reconcile(nil)
		}
		assert.Empty(t, drv.destroyed, "not gc'd before the drained-card debounce")

		r.reconcile(nil)
		require.Len(t, drv.destroyed, 1, "gc'd once the card is drained past the debounce")
		assert.Equal(t, uint32(2), drv.destroyed[0].GiID)
	})
}

// 身份验证必须读取加速器在其锁中的数据，而不是从该进程打开的快照中读取。此时，该快照可能已经是整个分
// 配周期的旧数据了；此外，非带内 nvidia-smi mig -dgi 以及 NVML 的 ID 重用机制可能会让不同的——可能处
// 于运行中的——实例出现在所记录的 ID 对应的时间段内。与过时的视图对比后，该标记仍然“匹配”，因此正在运
// 行的 Pod 的 MIG 设备将在该情况下被销毁。
func TestReclaim_IDReusedAfterThePassSnapshotIsNotDestroyed(t *testing.T) {
	redirectLogicalSliceDirs(t)
	drv := newFakeMigDriver()
	seedMarkedInstance(t, drv, "pod-dead", testGPUUUID0, 1)

	// reclaimMaxMisses-th 这一轮是进行销毁的轮次。其自身的快照编号为 reclaimMaxMisses，而加速器锁定下的
	// 重新读取则对应之后的轮次——因此在此处替换实例，会严格落在这两个数值之间。
	drv.listHook = func(d *fakeMigDriver, call int) {
		if call != reclaimMaxMisses+1 {
			return
		}
		d.live[testGPUUUID0] = []migInstance{{
			GiID: 1, CiID: 1, ComputeSlices: 1,
			Placement: migPlacement{Start: 0, Length: 2}, UUID: "MIG-reused",
		}}
	}

	r := newReclaimer(drv, deviceplugin.OperatorPodsDir, logr.Discard(), noClaims)
	for i := 0; i < reclaimMaxMisses; i++ {
		r.reconcile(nil)
	}

	assert.Empty(t, drv.destroyed,
		"the id now carries somebody else's instance, so nothing may be destroyed under this marker")
	_, err := parseMarker(markerPath("pod-dead", "c", testGPUUUID0))
	require.Error(t, err, "the stale marker is dropped, since it describes an instance that is gone")
}

// 失败的重读操作实际上是对每个加速器的跳过处理，而非对未验证视图的销毁操作：标记文件会保留下来，去抖
// 机制不会清除，下一次处理会再次尝试。
func TestReclaim_FailsClosedWhenTheLockedRereadFails(t *testing.T) {
	redirectLogicalSliceDirs(t)
	drv := newFakeMigDriver()
	seedMarkedInstance(t, drv, "pod-dead", testGPUUUID0, 1)

	// 仅在锁定状态下失败重新读取操作，从而让后续步骤能够决定进行销毁。
	drv.listHook = func(d *fakeMigDriver, call int) {
		d.listErr = nil
		if call == reclaimMaxMisses+1 {
			d.listErr = errors.New("nvml enumeration failed")
		}
	}

	r := newReclaimer(drv, deviceplugin.OperatorPodsDir, logr.Discard(), noClaims)
	for i := 0; i < reclaimMaxMisses; i++ {
		r.reconcile(nil)
	}

	assert.Empty(t, drv.destroyed, "an unreadable card is never destroyed on")
	_, err := parseMarker(markerPath("pod-dead", "c", testGPUUUID0))
	require.NoError(t, err, "the marker is kept, so the next pass can retry")
}

// TestReclaim_RecordsTheDestroyedPartitionIdentity 标记了同一路径的另外一半：在授予操作与容器启动之
// 间消失的分区，要么是被此循环销毁的，要么是容器引擎无法寻址的，只有明确标注其身份被销毁的分区才能区
// 分二者。这两种情况都得到了处理，因为它们针对的是不同的问题——即死去的拥有者记录与未被任何分区声称的
// 分区。
func TestReclaim_RecordsTheDestroyedPartitionIdentity(t *testing.T) {
	cases := []struct {
		name string
		seed func(t *testing.T, drv *fakeMigDriver) string
	}{
		{
			name: "a dead pod's partition",
			seed: func(t *testing.T, drv *fakeMigDriver) string {
				t.Helper()
				inst := migInstance{
					GiID: 3, CiID: 4, ComputeSlices: 1,
					Placement: migPlacement{Start: 0, Length: 2}, UUID: "MIG-dead-owner",
				}
				drv.seedLive(testGPUUUID0, inst)
				require.NoError(t, writeMarker(markerPath("pod-gone", "c", testGPUUUID0), migMarker{
					PodUID: "pod-gone", Container: "c", Card: testGPUUUID0, Profile: "1g.10gb",
					GiID: inst.GiID, CiID: inst.CiID, MigUUID: inst.UUID,
					ComputeSlices: 1, Start: 0, Length: 2,
				}))
				return inst.UUID
			},
		},
		{
			name: "a marker-less partition on a drained card",
			seed: func(_ *testing.T, drv *fakeMigDriver) string {
				inst := migInstance{
					GiID: 9, CiID: 9, ComputeSlices: 1,
					Placement: migPlacement{Start: 4, Length: 2}, UUID: "MIG-no-owner",
				}
				drv.seedLive(testGPUUUID0, inst)
				return inst.UUID
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			redirectLogicalSliceDirs(t)
			drv := newFakeMigDriver()
			want := c.seed(t, drv)

			var lines []string
			logger := funcr.New(func(prefix, args string) {
				lines = append(lines, prefix+args)
			}, funcr.Options{})
			r := newReclaimer(drv, deviceplugin.OperatorPodsDir, logger, noClaims)

			for i := 0; i < reclaimMaxMisses; i++ {
				r.reconcile(nil)
			}
			require.Len(t, drv.destroyed, 1, "the partition is destroyed")

			joined := strings.Join(lines, "\n")
			assert.Contains(t, joined, want, "the destroyed partition identity is recorded")
			assert.Contains(t, joined, testGPUUUID0, "the accelerator it was destroyed on is recorded")
		})
	}
}

// TestReclaim_SameSlotRebuildNotDestroyedUnderStaleMarker 描述了上述 id-reuse 保护机制无法识别的一种
// 情况。MIG-设备 UUID 是基于名称的——由父加速器及实例自身的身份衍生而来——因此，在相同的放置区间中，一
// 个分区被销毁后另一个分区创建时，会保留相同的标识符，且插槽选择是确定性的（按最低空闲状态排序），这
// 使得重新获取刚释放的插槽成为常见现象，而非异常情况。随后，前序设备的标记会在身份校验涉及的每个字段
// 上与后续设备匹配，无论加速器在锁定状态下被读取多少次：后续设备会完全复现整个身份。
//
// TestReclaim_StaleMarkerGiIdReuseNotDestroyed 体现了有意的设计差异：它给被重新使用的 ID 分配不同的
// 槽位和不同的 UUID，从而在承载信息的环节进行身份验证。而这一项则在没有承载信息的环节进行验证。
//
// 时间测量的是实际时间。前任的记录已经经历了去抖过程，而后继的标记在销毁前会经过一次处理——在该节点上，
// 耗时仅为三分之一秒。这两种情况仅在于后继是否已经达到了当时设定的活Pod-UID集合。通常情况并非如此：
// 该集合是在会话开始时被记录的，由信息源生成，因此通过它来决定所有权会经过一个过时的过滤器，从而只能
// 由标记单独决定所有权。TestReclaim_TwoDeadRecordsOnOneGiIdStillEndInADestroy是替代保护机制的实现声
// 明，属于断言而非争论。两个声称拥有同一GPU实例ID的记录都被另一个记录替代，因此仅检查“是否还有其他记
// 录拥有此ID”的保护机制可能会同时消除两个记录并销毁它们，导致分区被留在一个从未被释放的加速器上——孤
// 儿清理仅在被释放的节点上运行，因此后续没有东西可以收集它。
//
// 由于决策在加速器锁定状态下从磁盘重新获取，因此会收敛：丢弃第一个记录对后续处理过程可见，随后该过程
// 成为唯一的拥有者，并在常规路径上被销毁。第二种情况则让另一个Pod的活跃分区继续存在于同一加速器中，
// 因为在这种状态下，泄漏问题会成为永久性问题。
func TestReclaim_TwoDeadRecordsOnOneGiIdStillEndInADestroy(t *testing.T) {
	const sharedUUID = "MIG-contested"
	slot := migPlacement{Start: 0, Length: 2}
	claim := func(podUID string) migMarker {
		return migMarker{
			PodUID: podUID, Container: "c", Card: testGPUUUID0, Profile: "1g.10gb",
			GiID: 1, CiID: 1, MigUUID: sharedUUID,
			ComputeSlices: 1, Start: slot.Start, Length: slot.Length,
		}
	}

	for _, c := range []struct {
		name        string
		alsoLivePod bool // 第二个容器在相同的加速器上保持自己的分区
	}{
		{name: "the contested accelerator holds nothing else"},
		{name: "another pod's live partition keeps the accelerator from draining", alsoLivePod: true},
	} {
		t.Run(c.name, func(t *testing.T) {
			redirectLogicalSliceDirs(t)
			drv := newFakeMigDriver()
			drv.seedLive(testGPUUUID0, migInstance{
				GiID: 1, CiID: 1, ComputeSlices: 1, Placement: slot, UUID: sharedUUID,
			})
			require.NoError(t, writeMarker(markerPath("pod-a", "c", testGPUUUID0), claim("pod-a")))
			require.NoError(t, writeMarker(markerPath("pod-b", "c", testGPUUUID0), claim("pod-b")))

			var live []string
			if c.alsoLivePod {
				other := migMarker{
					PodUID: "pod-live", Container: "c", Card: testGPUUUID0, Profile: "1g.10gb",
					GiID: 2, CiID: 2, MigUUID: "MIG-live", ComputeSlices: 1, Start: 2, Length: 2,
				}
				drv.seedLive(testGPUUUID0, migInstance{
					GiID: 2, CiID: 2, ComputeSlices: 1,
					Placement: migPlacement{Start: 2, Length: 2}, UUID: "MIG-live",
				})
				require.NoError(t, writeMarker(markerPath("pod-live", "c", testGPUUUID0), other))
				live = []string{"pod-live"}
			}

			r := newReclaimer(drv, deviceplugin.OperatorPodsDir, logr.Discard(), noClaims)
			for i := 0; i < reclaimMaxMisses*2+2; i++ {
				r.reconcile(live)
			}

			assert.Contains(t, drv.destroyed, migInstance{
				GiID: 1, CiID: 1, ComputeSlices: 1, Placement: slot, UUID: sharedUUID,
			}, "the contested partition must not survive both records being dropped")
			for _, p := range []string{"pod-a", "pod-b"} {
				_, err := parseMarker(markerPath(p, "c", testGPUUUID0))
				assert.Error(t, err, p+"'s record is retired")
			}
			if c.alsoLivePod {
				assert.NotContains(t, drv.destroyed, migInstance{
					GiID: 2, CiID: 2, ComputeSlices: 1,
					Placement: migPlacement{Start: 2, Length: 2}, UUID: "MIG-live",
				}, "the live pod's own partition is untouched")
			}
		})
	}
}

// TestReclaim_CorruptMarkerHoldsTheDestroyItMightOwn 断言：超序保护机制会尊重损坏列表，以及标记扫描
// 返回的、可解析的条目。损坏文件的内容无法读取，因此它所声称的 GPU 实例无法被恢复，也不存在针对单个
// 实例的校验机制可以识别它；只有持有加速器时，才符合那种该声称的优先级高于正在被回收的记录的情况。
//
// 第三种情况是防止保护机制在另一个方向上变得无用：Pod 自身的损坏标记不属于任何其他人，如果将其计入，
// 那么只要该 Pod 的不良文件存在，其分区就永远无法被回收。
func TestReclaim_CorruptMarkerHoldsTheDestroyItMightOwn(t *testing.T) {
	cases := []struct {
		name string
		// corruptPod 拥有 corrupt 文件；corruptCard 是名为该名称的加速器。
		corruptPod  string
		corruptCard string
		wantDestroy bool
	}{
		{
			name:        "another pod's corrupt marker on this card holds the destroy",
			corruptPod:  "pod-other",
			corruptCard: testGPUUUID0,
			wantDestroy: false,
		},
		{
			name:        "a corrupt marker naming no accelerator holds this card too",
			corruptPod:  "pod-other",
			corruptCard: "",
			wantDestroy: false,
		},
		{
			name:        "the reclaimed pod's own corrupt marker does not hold its own destroy",
			corruptPod:  "pod-dead",
			corruptCard: testGPUUUID0,
			wantDestroy: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			redirectLogicalSliceDirs(t)
			drv := newFakeMigDriver()
			slot := migPlacement{Start: 0, Length: 2}
			drv.seedLive(testGPUUUID0, migInstance{
				GiID: 1, CiID: 1, ComputeSlices: 1, Placement: slot, UUID: "MIG-held",
			})
			require.NoError(t, writeMarker(markerPath("pod-dead", "c", testGPUUUID0), migMarker{
				PodUID: "pod-dead", Container: "c", Card: testGPUUUID0, Profile: "1g.10gb",
				GiID: 1, CiID: 1, MigUUID: "MIG-held",
				ComputeSlices: 1, Start: slot.Start, Length: slot.Length,
			}))
			// 第二个容器目录，因此损坏的文件不会覆盖上方的记录。
			writeCorruptMarker(t, deviceplugin.PodWorkDir(c.corruptPod, "c2"), markerFileName(c.corruptCard))

			// 回收容器必须处于运行状态，才能确保在去抖时间过后，损坏的文件仍然存在：该循环会移除已消失的容器中的
			// 损坏标记文件，因此处于死亡状态的请求方文件会在应被保留的数据被销毁之前就消失。
			var live []string
			if c.corruptPod != "pod-dead" {
				live = []string{c.corruptPod}
			}

			r := newReclaimer(drv, deviceplugin.OperatorPodsDir, logr.Discard(), noClaims)
			for i := 0; i < reclaimMaxMisses+1; i++ {
				r.reconcile(live)
			}

			if c.wantDestroy {
				assert.NotEmpty(t, drv.destroyed, "the pod's own corrupt file must not shield its partitions")
				return
			}
			assert.Empty(t, drv.destroyed, "a partition a corrupt claim may own is never destroyed")
			_, err := parseMarker(markerPath("pod-dead", "c", testGPUUUID0))
			require.NoError(t, err, "the record is kept so a later pass can find the partition again")
		})
	}
}

func TestReclaim_SameSlotRebuildNotDestroyedUnderStaleMarker(t *testing.T) {
	const sameUUID = "MIG-6deb82ec-name-based"
	slot := migPlacement{Start: 0, Length: 2}
	marker := func(podUID string) migMarker {
		return migMarker{
			PodUID: podUID, Container: "c", Card: testGPUUUID0, Profile: "1g.10gb",
			GiID: 1, CiID: 1, MigUUID: sameUUID,
			ComputeSlices: 1, Start: slot.Start, Length: slot.Length,
		}
	}

	cases := []struct {
		name string
		// “live”是指针对前序记录进行操作的Pod UID集合。
		live []string
	}{
		{
			name: "the successor is in the pass's live set",
			live: []string{"pod-new"},
		},
		{
			name: "the successor has not reached the pass's live set yet",
			live: nil,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			redirectLogicalSliceDirs(t)
			drv := newFakeMigDriver()
			drv.seedLive(testGPUUUID0, migInstance{
				GiID: 1, CiID: 1, ComputeSlices: 1, Placement: slot, UUID: sameUUID,
			})
			// 前任系统的记录会在其 Pod 消失时继续进行去抖操作。
			require.NoError(t, writeMarker(markerPath("pod-dead", "c", testGPUUUID0), marker("pod-dead")))
			r := newReclaimer(drv, deviceplugin.OperatorPodsDir, logr.Discard(), noClaims)
			for i := 0; i < reclaimMaxMisses-1; i++ {
				r.reconcile(nil)
			}
			require.Empty(t, drv.destroyed, "nothing is reclaimed before the debounce")

			// 继任者占据前任记录仍指定的插槽，并在加速器锁中写入自己的标记文件——即已经确定的记录。
			require.NoError(t, writeMarker(markerPath("pod-new", "c", testGPUUUID0), marker("pod-new")))

			r.reconcile(c.live)

			assert.Empty(t, drv.destroyed, "a partition another pod's marker claims is never destroyed")
			_, err := parseMarker(markerPath("pod-new", "c", testGPUUUID0))
			require.NoError(t, err, "the successor's marker is intact")
			_, err = parseMarker(markerPath("pod-dead", "c", testGPUUUID0))
			require.Error(t, err, "the superseded marker is dropped, so the decision is not retaken every pass")
		})
	}
}
