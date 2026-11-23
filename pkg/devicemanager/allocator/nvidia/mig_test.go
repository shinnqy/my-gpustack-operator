package nvidia

import (
	"context"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/go-logr/logr/funcr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/deviceplugin"
	"gpustack.ai/gpustack/pkg/utils/json"
)

// fakeMigDriver 是一个内存中的 migDriver：它记录 create/destroy 调用操作，并保存每个加速器对应的可能
// 放置集合以及实时实例列表，因此 marker/slot-pick/reuse 核心功能可以在没有 NVML 的情况下进行测试。该
// 实现具有并发安全性（reclaim/actuator 模块持有每个加速器的锁，但不同加速器可以并行运行，且不涉及 Go
// 映射）。
type fakeMigDriver struct {
	mu       sync.Mutex
	possible map[string][]migPlacement
	live     map[string][]migInstance

	nextGiID    uint32
	createCalls int
	createErr   error
	destroyed   []migInstance
	// inUseGiIDs 中的 GI 模拟仍在使用的实例，DestroyInstance 对它们返回 errInstanceInUse。
	inUseGiIDs map[uint32]bool
	// processesByGiID 表示每个分区所响应的计算进程数量，processesErr 则会导致查询本身失败——即无法被查询
	// 到的分区。
	processesByGiID map[uint32]int
	processesErr    error
	listErr         error

	// listCalls 和 cardListCalls 分别统计了节点范围内以及每个加速器上的枚举结果，而 listHook 则在每次枚
	// 举操作之前使用这些合并后的计数结果进行执行。它们共同使得案例能够改变加速器在枚举过程中的实时数据集，
	// 以及在加速器锁定状态下所获取的数据集——这是模拟带外销毁并允许ID复用的唯一方式——同时让程序能够证明在
	// 锁定状态下读取数据所涉及的成本属于加速器而非节点。
	listCalls     int
	cardListCalls int
	listHook      func(drv *fakeMigDriver, call int)
}

func newFakeMigDriver() *fakeMigDriver {
	return &fakeMigDriver{
		possible: make(map[string][]migPlacement),
		live:     make(map[string][]migInstance),
	}
}

func (f *fakeMigDriver) CardState(cardUUID, _ string, _, _ int32) (migCardState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return migCardState{
		Possible: append([]migPlacement(nil), f.possible[cardUUID]...),
		Live:     append([]migInstance(nil), f.live[cardUUID]...),
	}, nil
}

func (f *fakeMigDriver) CreateInstance(cardUUID, _ string, computeSlices, _ int32, slot migPlacement) (migInstance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createCalls++
	if f.createErr != nil {
		return migInstance{}, f.createErr
	}
	f.nextGiID++
	inst := migInstance{
		GiID:          f.nextGiID,
		CiID:          f.nextGiID,
		ComputeSlices: computeSlices,
		Placement:     slot,
		UUID:          fmt.Sprintf("MIG-%s-%d", cardUUID, f.nextGiID),
	}
	f.live[cardUUID] = append(f.live[cardUUID], inst)
	return inst, nil
}

func (f *fakeMigDriver) DestroyInstance(cardUUID string, inst migInstance) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.inUseGiIDs[inst.GiID] {
		return fmt.Errorf("card %s: destroy gpu instance %d: %w", cardUUID, inst.GiID, errInstanceInUse)
	}
	f.destroyed = append(f.destroyed, inst)
	kept := f.live[cardUUID][:0]
	for _, l := range f.live[cardUUID] {
		if l.GiID != inst.GiID {
			kept = append(kept, l)
		}
	}
	f.live[cardUUID] = kept
	return nil
}

// InstanceProcesses 表示每个分区对应多少个计算进程（即回收进程检查环节）。非标准分区则没有进程，这是
// 正常情况。
func (f *fakeMigDriver) InstanceProcesses(_ string, inst migInstance) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.processesErr != nil {
		return 0, f.processesErr
	}
	return f.processesByGiID[inst.GiID], nil
}

// ListInstances 返回所有加速器上的所有已播种活跃实例（用于回收孤立的 GC 问题）。
func (f *fakeMigDriver) ListInstances() ([]migLiveInstance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listCalls++
	if f.listHook != nil {
		f.listHook(f, f.listCalls+f.cardListCalls)
	}
	if f.listErr != nil {
		return nil, f.listErr
	}
	var out []migLiveInstance
	for card, insts := range f.live {
		for _, inst := range insts {
			out = append(out, migLiveInstance{Card: card, Inst: inst})
		}
	}
	return out, nil
}

// CardInstances 返回了加速器中的某个已播种的实时实例（即验证-重新读取接缝），因此持有该加速器锁定的
// 调用者不会支付整个节点范围内的操作费用。
func (f *fakeMigDriver) CardInstances(cardUUID string) ([]migInstance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cardListCalls++
	if f.listHook != nil {
		f.listHook(f, f.listCalls+f.cardListCalls)
	}
	if f.listErr != nil {
		return nil, f.listErr
	}
	return append([]migInstance(nil), f.live[cardUUID]...), nil
}

// seedLive 将实时实例添加到加速器（一种非带内/可复用分区）中。
func (f *fakeMigDriver) seedLive(cardUUID string, inst migInstance) {
	f.live[cardUUID] = append(f.live[cardUUID], inst)
}

// evenSlots 返回 8 个切片加速器上 2 个切片配置下的内存切片放置位置 ([0,2),[2,2),[4,2),[6,2)) — 这是
// 测试中使用的 1g.10gb 合法集合。
func evenSlots() []migPlacement {
	return []migPlacement{{0, 2}, {2, 2}, {4, 2}, {6, 2}}
}

func TestPickPlacement(t *testing.T) {
	cases := []struct {
		name     string
		possible []migPlacement
		occupied []migPlacement
		want     migPlacement
		wantOK   bool
	}{
		{
			name:     "empty accelerator picks lowest",
			possible: evenSlots(),
			want:     migPlacement{0, 2},
			wantOK:   true,
		},
		{
			name:     "lowest free skips the overlapping slot",
			possible: evenSlots(),
			occupied: []migPlacement{{0, 2}},
			want:     migPlacement{2, 2},
			wantOK:   true,
		},
		{
			name:     "a wide occupant blocks several slots",
			possible: evenSlots(),
			occupied: []migPlacement{{0, 4}}, // 第 0-3 个切片上的 3G/4G 分区
			want:     migPlacement{4, 2},
			wantOK:   true,
		},
		{
			name:     "full accelerator yields nothing",
			possible: evenSlots(),
			occupied: []migPlacement{{0, 8}},
			wantOK:   false,
		},
		{
			name:     "unsorted possible still returns the lowest",
			possible: []migPlacement{{4, 2}, {0, 2}, {6, 2}, {2, 2}},
			occupied: []migPlacement{{0, 2}},
			want:     migPlacement{2, 2},
			wantOK:   true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := pickPlacement(c.possible, c.occupied)
			assert.Equal(t, c.wantOK, ok)
			if c.wantOK {
				assert.Equal(t, c.want, got)
			}
		})
	}
}

func TestReserveMigInstance(t *testing.T) {
	const profile = "1g.10gb"

	t.Run("creates at the lowest free placement", func(t *testing.T) {
		redirectLogicalSliceDirs(t)
		drv := newFakeMigDriver()
		drv.possible[testGPUUUID0] = evenSlots()

		inst, outcome, err := reserveMigInstance(
			drv, deviceplugin.OperatorPodsDir, "pod-a", "c", testGPUUUID0, profile, 1, 2,
		)
		require.NoError(t, err)
		assert.Equal(t, migCreated, outcome)
		assert.Equal(t, migPlacement{0, 2}, inst.Placement)
		assert.Equal(t, 1, drv.createCalls)

		// 标记文件进行往返操作：它用于标识创建的实例，以便回收机制可以销毁它。
		m, err := parseMarker(markerPath("pod-a", "c", testGPUUUID0))
		require.NoError(t, err)
		assert.Equal(t, profile, m.Profile)
		assert.Equal(t, inst.GiID, m.GiID)
		assert.Equal(t, inst.UUID, m.MigUUID)
	})

	t.Run("second accelerator of a new pod picks the next free slot", func(t *testing.T) {
		redirectLogicalSliceDirs(t)
		drv := newFakeMigDriver()
		drv.possible[testGPUUUID0] = evenSlots()
		// 之前的 Pod 已经拥有 slot 0 的配额（其标记文件属于 GI 100）。
		drv.seedLive(testGPUUUID0, migInstance{GiID: 100, ComputeSlices: 1, Placement: migPlacement{0, 2}, UUID: "MIG-x"})
		require.NoError(t, writeMarker(markerPath("pod-old", "c", testGPUUUID0), migMarker{
			PodUID: "pod-old", Container: "c", Card: testGPUUUID0, Profile: profile,
			GiID: 100, CiID: 100, MigUUID: "MIG-x", ComputeSlices: 1, Start: 0, Length: 2,
		}))

		inst, outcome, err := reserveMigInstance(
			drv, deviceplugin.OperatorPodsDir, "pod-b", "c", testGPUUUID0, profile, 1, 2,
		)
		require.NoError(t, err)
		assert.Equal(t, migCreated, outcome)
		assert.Equal(t, migPlacement{2, 2}, inst.Placement, "slot 0 is occupied by pod-old")
	})

	t.Run("binds a reusable unbound instance instead of creating", func(t *testing.T) {
		redirectLogicalSliceDirs(t)
		drv := newFakeMigDriver()
		drv.possible[testGPUUUID0] = evenSlots()
		// 请求几何形状的未绑定实例（没有标记引用它）。
		drv.seedLive(testGPUUUID0, migInstance{GiID: 7, ComputeSlices: 1, Placement: migPlacement{4, 2}, UUID: "MIG-reuse"})

		inst, outcome, err := reserveMigInstance(
			drv, deviceplugin.OperatorPodsDir, "pod-c", "c", testGPUUUID0, profile, 1, 2,
		)
		require.NoError(t, err)
		assert.Equal(t, migBound, outcome, "an unbound instance is reused, not created")
		assert.Equal(t, uint32(7), inst.GiID)
		assert.Equal(t, "MIG-reuse", inst.UUID)
		assert.Equal(t, 0, drv.createCalls)
	})

	t.Run("full accelerator fails without creating", func(t *testing.T) {
		redirectLogicalSliceDirs(t)
		drv := newFakeMigDriver()
		drv.possible[testGPUUUID0] = evenSlots()
		drv.seedLive(testGPUUUID0, migInstance{GiID: 1, ComputeSlices: 7, Placement: migPlacement{0, 8}, UUID: "MIG-whole"})

		_, _, err := reserveMigInstance(
			drv, deviceplugin.OperatorPodsDir, "pod-d", "c", testGPUUUID0, profile, 1, 2,
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no free placement")
		assert.Equal(t, 0, drv.createCalls)
	})

	t.Run("rebinds its own live instance on a retry", func(t *testing.T) {
		redirectLogicalSliceDirs(t)
		drv := newFakeMigDriver()
		drv.possible[testGPUUUID0] = evenSlots()
		drv.seedLive(testGPUUUID0, migInstance{GiID: 5, CiID: 5, ComputeSlices: 1, Placement: migPlacement{0, 2}, UUID: "MIG-self"})
		require.NoError(t, writeMarker(markerPath("pod-e", "c", testGPUUUID0), migMarker{
			PodUID: "pod-e", Container: "c", Card: testGPUUUID0, Profile: profile,
			GiID: 5, CiID: 5, MigUUID: "MIG-self", ComputeSlices: 1, Start: 0, Length: 2,
		}))

		inst, outcome, err := reserveMigInstance(
			drv, deviceplugin.OperatorPodsDir, "pod-e", "c", testGPUUUID0, profile, 1, 2,
		)
		require.NoError(t, err)
		assert.Equal(t, migRebound, outcome, "a retry rebinds its own partition, not a new one")
		assert.Equal(t, "MIG-self", inst.UUID)
		assert.Equal(t, 0, drv.createCalls)
	})

	t.Run("fails closed when its gpu-instance id was reused by another partition", func(t *testing.T) {
		redirectLogicalSliceDirs(t)
		drv := newFakeMigDriver()
		drv.possible[testGPUUUID0] = evenSlots()
		// 当前的实时 GI 5 与记录的标记文件属于不同的分区（不同的 MIG UUID）。
		drv.seedLive(testGPUUUID0, migInstance{GiID: 5, CiID: 5, ComputeSlices: 1, Placement: migPlacement{0, 2}, UUID: "MIG-new"})
		require.NoError(t, writeMarker(markerPath("pod-f", "c", testGPUUUID0), migMarker{
			PodUID: "pod-f", Container: "c", Card: testGPUUUID0, Profile: profile,
			GiID: 5, CiID: 5, MigUUID: "MIG-old", ComputeSlices: 1, Start: 0, Length: 2,
		}))

		_, _, err := reserveMigInstance(
			drv, deviceplugin.OperatorPodsDir, "pod-f", "c", testGPUUUID0, profile, 1, 2,
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "id reused")
		assert.Equal(t, 0, drv.createCalls)
	})
}

// TestReserveMigInstance_SkipsEmptyUUIDInstance 断言：没有 MIG-设备 UUID 的活跃 GPU 实例（即没有计算
// 实例的孤儿实例，属于崩溃后留下的残骸）不会被重新使用——分配会创建一个新的、可用的分区，而不是采用那
// 个损坏的分区，并注入空的 NVIDIA_VISIBLE_DEVICES。
func TestReserveMigInstance_SkipsEmptyUUIDInstance(t *testing.T) {
	const profile = "1g.10gb"
	redirectLogicalSliceDirs(t)
	drv := newFakeMigDriver()
	drv.possible[testGPUUUID0] = evenSlots()
	// 在 slot 0 位置有一个没有 UUID、无 CI、无标记的孤儿对象，没有对象拥有者。
	drv.seedLive(testGPUUUID0, migInstance{GiID: 9, ComputeSlices: 1, Placement: migPlacement{0, 2}, UUID: ""})

	inst, outcome, err := reserveMigInstance(
		drv, deviceplugin.OperatorPodsDir, "pod-a", "c", testGPUUUID0, profile, 1, 2,
	)
	require.NoError(t, err)
	assert.Equal(t, migCreated, outcome, "a UUID-less orphan is not adopted")
	assert.Equal(t, 1, drv.createCalls)
	assert.NotEqual(t, uint32(9), inst.GiID)
	assert.Equal(t, migPlacement{2, 2}, inst.Placement, "the orphan's slot 0 is treated as occupied")
	assert.NotEmpty(t, inst.UUID)
}

// writeCorruptMarker 在 dir 下按 fileName 写入一份无法解析的标记，模拟节点异常关机后文件名保留、内容
// 却不完整的情况。
func writeCorruptMarker(t *testing.T, dir, fileName string) string {
	t.Helper()
	require.NoError(t, os.MkdirAll(dir, 0o777))
	path := filepath.Join(dir, fileName)
	require.NoError(t, os.WriteFile(path, []byte(`{"podUID":"pod-crash","card":"GPU-aaa`), 0o644))
	return path
}

// TestReserveMigInstance_CorruptMarkerFailsAdoptionClosed 断言：损坏的所有权标记在加速器上无法被采用，
// 其 FILE NAME 名称仅在此处：记录表明该加速器上剩余的资源未被拥有，因此采用该资源可能让另一个 Pod 占
// 用同一分区，而另一个 Pod 仍持有该资源。缺失记录未涉及的内容均不受影响——在同一加速器上的空闲 slots
// 中重新创建（占用来自驱动程序，而非标记），或在兄弟加速器上采用该资源。损坏的名称在任何地方都会导致
// 无法采用加速器，因为未知范围本身也是未知的。
func TestReserveMigInstance_CorruptMarkerFailsAdoptionClosed(t *testing.T) {
	const profile = "1g.10gb"
	// leftoverGiID/leftoverSlot 描述了未标记、几何匹配的剩余资源，即一个健康的扫描会采用的资源（无论是因
	// 崩溃而产生的还是非带内工具留下的）。
	const leftoverGiID = uint32(7)
	leftoverSlot := migPlacement{0, 2}

	cases := []struct {
		name string
		// corruptFile 是死 Pod 工作目录下无法解析的标记文件名称。
		corruptFile string
		// 当设置时，mismatchedPod会拥有一个完整且可解析的标记文件，但会记录一个加速器，该加速器自身的文件名
		// 无法被识别——否则设置所有权后就无法证明其存在。
		mismatchedPod string
		// seedLeftover 会在所请求的加速器上初始化可采用的剩余资源。
		seedLeftover  bool
		wantOutcome   migReserveOutcome
		wantCreates   int
		wantPlacement migPlacement
	}{
		{
			name:          "refuses adopting a leftover on the accelerator the corrupt marker names",
			corruptFile:   markerFileName(testGPUUUID0),
			seedLeftover:  true,
			wantOutcome:   migCreated,
			wantCreates:   1,
			wantPlacement: migPlacement{2, 2}, // 未采用的剩余部分仍占据 slot 0 位置
		},
		{
			name:          "a fresh create in a free slot on that same accelerator still succeeds",
			corruptFile:   markerFileName(testGPUUUID0),
			wantOutcome:   migCreated,
			wantCreates:   1,
			wantPlacement: migPlacement{0, 2},
		},
		{
			name:          "adoption on a sibling accelerator is unaffected",
			corruptFile:   markerFileName(testGPUUUID1),
			seedLeftover:  true,
			wantOutcome:   migBound,
			wantCreates:   0,
			wantPlacement: leftoverSlot,
		},
		{
			// 记录中解析并包含所有必需的字段，但加速器却使用其自身的文件名，而非其他文件名：仅由记录中的加速器确
			// 定的所有权集合进行分组后，剩余部分将被视为无所有权状态，并被分配给第二个Pod，导致两个Pod共享一个
			// MIG分区。因此，它应被视为属于该加速器的不可读取所有权。
			name:          "refuses adopting a leftover while a marker's record disagrees with its file name",
			mismatchedPod: "pod-mismatch",
			seedLeftover:  true,
			wantOutcome:   migCreated,
			wantCreates:   1,
			wantPlacement: migPlacement{2, 2},
		},
		{
			name:          "a corrupt name yielding no accelerator refuses adoption everywhere",
			corruptFile:   markerFileName(""), // nvidia-mig-.json: 一个用于标识无卡设备的标记文件
			seedLeftover:  true,
			wantOutcome:   migCreated,
			wantCreates:   1,
			wantPlacement: migPlacement{2, 2},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			redirectLogicalSliceDirs(t)
			drv := newFakeMigDriver()
			drv.possible[testGPUUUID0] = evenSlots()
			if c.seedLeftover {
				drv.seedLive(testGPUUUID0, migInstance{
					GiID: leftoverGiID, CiID: leftoverGiID, ComputeSlices: 1,
					Placement: leftoverSlot, UUID: "MIG-reuse",
				})
			}
			if c.corruptFile != "" {
				writeCorruptMarker(t, deviceplugin.PodWorkDir("pod-crash", "c"), c.corruptFile)
			}
			if c.mismatchedPod != "" {
				// 写在所请求的加速器自身路径上，但记录下兄弟加速器。
				require.NoError(t, writeMarker(markerPath(c.mismatchedPod, "c", testGPUUUID0), migMarker{
					PodUID: c.mismatchedPod, Container: "c", Card: testGPUUUID1, Profile: profile,
					GiID: leftoverGiID, CiID: leftoverGiID, MigUUID: "MIG-reuse",
					ComputeSlices: 1, Start: leftoverSlot.Start, Length: leftoverSlot.Length,
				}))
			}

			inst, outcome, err := reserveMigInstance(
				drv, deviceplugin.OperatorPodsDir, "pod-new", "c", testGPUUUID0, profile, 1, 2,
			)
			require.NoError(t, err, "a corrupt marker never fails the allocation outright")
			assert.Equal(t, c.wantOutcome, outcome)
			assert.Equal(t, c.wantCreates, drv.createCalls)
			assert.Equal(t, c.wantPlacement, inst.Placement)
			assert.NotEmpty(t, inst.UUID)
			if c.wantOutcome == migBound {
				assert.Equal(t, leftoverGiID, inst.GiID, "the sibling card's leftover is adopted")
			} else if c.seedLeftover {
				assert.NotEqual(t, leftoverGiID, inst.GiID, "the leftover is not adopted")
			}
		})
	}
}

func TestReserveMigInstance_IdempotentRetry(t *testing.T) {
	const profile = "1g.10gb"
	redirectLogicalSliceDirs(t)
	drv := newFakeMigDriver()
	drv.possible[testGPUUUID0] = evenSlots()

	first, outcome, err := reserveMigInstance(
		drv, deviceplugin.OperatorPodsDir, "pod-a", "c", testGPUUUID0, profile, 1, 2,
	)
	require.NoError(t, err)
	require.Equal(t, migCreated, outcome)

	// kubelet Allocate重试机制会重新绑定同一实例，而非重复创建。
	second, outcome, err := reserveMigInstance(
		drv, deviceplugin.OperatorPodsDir, "pod-a", "c", testGPUUUID0, profile, 1, 2,
	)
	require.NoError(t, err)
	assert.Equal(t, migRebound, outcome)
	assert.Equal(t, first.GiID, second.GiID)
	assert.Equal(t, 1, drv.createCalls, "no second create on retry")
}

func TestReserveMigInstance_SelfMarkerStale(t *testing.T) {
	const profile = "1g.10gb"
	redirectLogicalSliceDirs(t)
	drv := newFakeMigDriver()
	drv.possible[testGPUUUID0] = evenSlots()
	// 一个其 GPU 实例已从运行集合中移除的标记文件（离线销毁）：会以失败关闭的方式处理，而非重新绑定缺失
	// 的分区。
	require.NoError(t, writeMarker(markerPath("pod-a", "c", testGPUUUID0), migMarker{
		PodUID: "pod-a", Container: "c", Card: testGPUUUID0, Profile: profile,
		GiID: 42, CiID: 42, MigUUID: "MIG-gone", ComputeSlices: 1, Start: 0, Length: 2,
	}))

	_, _, err := reserveMigInstance(
		drv, deviceplugin.OperatorPodsDir, "pod-a", "c", testGPUUUID0, profile, 1, 2,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing gpu instance")
}

func TestReserveMigInstance_ProfileMismatch(t *testing.T) {
	redirectLogicalSliceDirs(t)
	drv := newFakeMigDriver()
	drv.possible[testGPUUUID0] = evenSlots()
	require.NoError(t, writeMarker(markerPath("pod-a", "c", testGPUUUID0), migMarker{
		PodUID: "pod-a", Container: "c", Card: testGPUUUID0, Profile: "2g.20gb",
		GiID: 1, CiID: 1, MigUUID: "MIG-other", ComputeSlices: 2, Start: 0, Length: 4,
	}))

	_, _, err := reserveMigInstance(
		drv, deviceplugin.OperatorPodsDir, "pod-a", "c", testGPUUUID0, "1g.10gb", 1, 2,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mismatches request")
}

// TestReserveMigInstance_ConcurrentSameCard 断言：由每个加速器锁机制序列化后的、同时使用同一加速器的
// 并发预订，会转化为不同的不重叠时段，且不会出现重复创建的情况，而同级加速器则可以并行进行。
func TestReserveMigInstance_ConcurrentSameCard(t *testing.T) {
	const profile = "1g.10gb"
	redirectLogicalSliceDirs(t)
	drv := newFakeMigDriver()
	drv.possible[testGPUUUID0] = evenSlots()
	drv.possible[testGPUUUID1] = evenSlots()

	const n = 4
	var wg sync.WaitGroup
	starts := make([]int32, n)
	sibling := make(chan migPlacement, 1)

	// 一个预留资源在兄弟加速器上竞争；它不能被正在争抢的资源阻塞。
	wg.Add(1)
	go func() {
		defer wg.Done()
		unlock := lockCard(testGPUUUID1)
		defer unlock()
		inst, _, err := reserveMigInstance(
			drv, deviceplugin.OperatorPodsDir, "pod-sib", "c", testGPUUUID1, profile, 1, 2,
		)
		require.NoError(t, err)
		sibling <- inst.Placement
	}()

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			unlock := lockCard(testGPUUUID0)
			defer unlock()
			inst, outcome, err := reserveMigInstance(
				drv, deviceplugin.OperatorPodsDir, fmt.Sprintf("pod-%d", i), "c", testGPUUUID0, profile, 1, 2,
			)
			require.NoError(t, err)
			require.Equal(t, migCreated, outcome)
			starts[i] = inst.Placement.Start
		}(i)
	}
	wg.Wait()

	assert.Equal(t, n+1, drv.createCalls, "one create per distinct pod (n on the card + 1 sibling), no double-create")
	got := append([]int32(nil), starts...)
	slices.Sort(got)
	assert.Equal(t, []int32{0, 2, 4, 6}, got, "distinct non-overlapping slots")

	select {
	case p := <-sibling:
		assert.Equal(t, int32(0), p.Start, "sibling card allocates independently")
	default:
		t.Fatal("sibling card reservation did not complete")
	}
}

// migDevices 构建了一个支持 MIG 的装置：其中一组加速器的每个都携带给定的物理切片规格几何结构（计算/
// 内存切片），因此 profileGeometry 可以解析它。
func migDevices(profile string, computeSlices, memorySlices int32, uuids ...string) *workercore.Devices {
	accels := make([]workercore.Accelerator, len(uuids))
	for i, u := range uuids {
		accels[i] = workercore.Accelerator{
			ID:    u,
			Index: uint32(i),
			Status: workercore.AcceleratorStatus{
				PhysicalSliced: workercore.AcceleratorPhysicalSliced{
					Profiles: []workercore.AcceleratorPhysicalSlicedProfile{{
						Name:          profile,
						ComputeSlices: computeSlices,
						MemorySlices:  memorySlices,
						Count:         4,
					}},
					Count: 4,
				},
			},
		}
	}
	return &workercore.Devices{
		Spec: workercore.DevicesSpec{
			Groups: []workercore.DevicesGroup{{
				ID:           "h100",
				Manufacturer: Manufacturer,
				Accelerators: accels,
			}},
		},
	}
}

func TestActuatePhysicalSliced(t *testing.T) {
	const profile = "1g.10gb"
	redirectLogicalSliceDirs(t)
	drv := newFakeMigDriver()
	drv.possible[testGPUUUID0] = evenSlots()
	drv.possible[testGPUUUID1] = evenSlots()
	s := &server{mig: drv}

	devs := migDevices(profile, 1, 2, testGPUUUID0, testGPUUUID1)
	pod := &core.Pod{ObjectMeta: meta.ObjectMeta{Name: "p", UID: types.UID("pod-1")}}
	ctr := &core.Container{Name: "c"}
	allocated := map[deviceplugin.Resource]int32{
		{Group: "h100", Device: testGPUUUID0}: 200000,
		{Group: "h100", Device: testGPUUUID1}: 200000,
	}

	out, err := s.ActuatePhysicalSliced(context.Background(), pod, ctr, devs, allocated, profile)
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Equal(t, profile, out.Profile)

	// 每个加速器记录一个放置（账本的占用来源）。
	require.Len(t, out.Placements, 2)
	for _, res := range []deviceplugin.Resource{
		{Group: "h100", Device: testGPUUUID0},
		{Group: "h100", Device: testGPUUUID1},
	} {
		require.Len(t, out.Placements[res], 1)
		assert.Equal(t, int32(0), out.Placements[res][0].Start)
		assert.Equal(t, int32(2), out.Placements[res][0].Length)
	}

	// 该响应仅注入 MIG UUID 值（不包含 libvgpu / CUDA_DEVICE_* 逻辑切片环境）。
	require.NotNil(t, out.Response)
	vis := out.Response.Envs["NVIDIA_VISIBLE_DEVICES"]
	assert.Contains(t, vis, "MIG-"+testGPUUUID0)
	assert.Contains(t, vis, "MIG-"+testGPUUUID1)
	assert.NotContains(t, out.Response.Envs, "CUDA_DEVICE_MEMORY_LIMIT_0")

	// 每个加速器都会生成一个标记文件。
	for _, card := range []string{testGPUUUID0, testGPUUUID1} {
		_, err := parseMarker(markerPath("pod-1", "c", card))
		require.NoError(t, err, "marker for card %s", card)
	}
}

func TestActuatePhysicalSliced_RollbackDestroysCreated(t *testing.T) {
	const profile = "1g.10gb"
	redirectLogicalSliceDirs(t)
	drv := newFakeMigDriver()
	drv.possible[testGPUUUID0] = evenSlots()
	s := &server{mig: drv}

	devs := migDevices(profile, 1, 2, testGPUUUID0)
	pod := &core.Pod{ObjectMeta: meta.ObjectMeta{Name: "p", UID: types.UID("pod-1")}}
	ctr := &core.Container{Name: "c"}
	allocated := map[deviceplugin.Resource]int32{{Group: "h100", Device: testGPUUUID0}: 200000}

	out, err := s.ActuatePhysicalSliced(context.Background(), pod, ctr, devs, allocated, profile)
	require.NoError(t, err)
	require.Len(t, drv.destroyed, 0)

	out.Rollback()
	require.Len(t, drv.destroyed, 1, "rollback destroys the created instance")
	_, statErr := parseMarker(markerPath("pod-1", "c", testGPUUUID0))
	require.Error(t, statErr, "rollback removes the marker")
}

// TestActuatePhysicalSliced_RollbackReusedNotDestroyed 表示对分配的回滚操作，该操作采用了原有的无绑
// 定实例，仅会移除所有权标记文件（将其返回到无绑定池中），且不会破坏该操作所创建的硬件。
func TestActuatePhysicalSliced_RollbackReusedNotDestroyed(t *testing.T) {
	const profile = "1g.10gb"
	redirectLogicalSliceDirs(t)
	drv := newFakeMigDriver()
	drv.possible[testGPUUUID0] = evenSlots()
	// 可重用的未绑定实例（具有 UUID，无所有权标记文件）。
	drv.seedLive(testGPUUUID0, migInstance{GiID: 5, ComputeSlices: 1, Placement: migPlacement{0, 2}, UUID: "MIG-pre"})
	s := &server{mig: drv}

	devs := migDevices(profile, 1, 2, testGPUUUID0)
	pod := &core.Pod{ObjectMeta: meta.ObjectMeta{Name: "p", UID: types.UID("pod-1")}}
	ctr := &core.Container{Name: "c"}
	allocated := map[deviceplugin.Resource]int32{{Group: "h100", Device: testGPUUUID0}: 200000}

	out, err := s.ActuatePhysicalSliced(context.Background(), pod, ctr, devs, allocated, profile)
	require.NoError(t, err)
	assert.Equal(t, 0, drv.createCalls, "the pre-existing instance is reused")

	out.Rollback()
	assert.Len(t, drv.destroyed, 0, "rollback must not destroy an adopted instance")
	_, statErr := parseMarker(markerPath("pod-1", "c", testGPUUUID0))
	require.Error(t, statErr, "rollback drops the ownership marker")
}

func TestActuatePhysicalSliced_UnknownProfileFails(t *testing.T) {
	redirectLogicalSliceDirs(t)
	drv := newFakeMigDriver()
	drv.possible[testGPUUUID0] = evenSlots()
	s := &server{mig: drv}

	devs := migDevices("1g.10gb", 1, 2, testGPUUUID0)
	pod := &core.Pod{ObjectMeta: meta.ObjectMeta{Name: "p", UID: types.UID("pod-1")}}
	ctr := &core.Container{Name: "c"}
	allocated := map[deviceplugin.Resource]int32{{Group: "h100", Device: testGPUUUID0}: 200000}

	_, err := s.ActuatePhysicalSliced(context.Background(), pod, ctr, devs, allocated, "3g.20gb")
	require.Error(t, err)
	assert.Equal(t, 0, drv.createCalls, "an unknown profile geometry never reaches create")
}

// 该记录仅为写入者所保存，同时存储该记录的目录也保持可遍历状态，这两者很容易混淆：它们相距两行，较宽
// 的那个用于承载其他分配器放置在同一位置的逻辑切片对象。请同时固定两者。
func TestWriteMarkerModes(t *testing.T) {
	redirectLogicalSliceDirs(t)
	m := migMarker{
		PodUID: "pod-a", Container: "c", Card: testGPUUUID0, Profile: "1g.5gb",
		GiID: 1, CiID: 0, MigUUID: "MIG-0", ComputeSlices: 1, Start: 0, Length: 1,
	}
	path := markerPath(m.PodUID, m.Container, m.Card)
	require.NoError(t, writeMarker(path, m))

	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm(),
		"nothing outside this process reads the record, so nothing outside it may")

	dirInfo, err := os.Stat(filepath.Dir(path))
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o777), dirInfo.Mode().Perm(),
		"the per-container work directory is shared with artifacts a container reads as its own user")
}

// 标记文件的 JSON 标签采用磁盘存储格式：在加速器词汇表重命名之前编写的标记文件会存在于运行中的节点上，
// 而移动的标签会导致所有此类文件无法被读取，从而破坏重试、可见性、采用和回收功能。该包中的其他测试都
// 通过相同的结构进行写入和读取操作，因此通过协调的标签重命名，所有测试都将保持正常状态。但这一测试不
// 同：它直接使用重命名前的文档，并确保精确的网络键能够重新输出。
func TestMigMarker_LegacyJSONRoundTrip(t *testing.T) {
	// legacyKeys 是完整的磁盘上密钥集合，按字面拼写而非从结构体派生，因此重命名标签在此会失败，不会被镜
	// 像到预期中。
	legacyKeys := []string{
		"podUID", "container", "card", "profile",
		"giID", "ciID", "migUUID", "computeSlices", "start", "length",
	}

	cases := []struct {
		name string
		doc  string
		want migMarker
	}{
		{
			name: "a complete pre-rename record",
			doc: `{"podUID":"pod-a","container":"c0","card":"GPU-aaaa0000-0000-0000-0000-000000000000",` +
				`"profile":"1g.10gb","giID":3,"ciID":1,"migUUID":"MIG-legacy-0",` +
				`"computeSlices":1,"start":2,"length":2}`,
			want: migMarker{
				PodUID: "pod-a", Container: "c0", Card: "GPU-aaaa0000-0000-0000-0000-000000000000",
				Profile: "1g.10gb", GiID: 3, CiID: 1, MigUUID: "MIG-legacy-0",
				ComputeSlices: 1, Start: 2, Length: 2,
			},
		},
		{
			name: "the zero-valued numeric fields of a slot-0 record",
			doc: `{"podUID":"pod-b","container":"c1","card":"GPU-bbbb0000-0000-0000-0000-000000000000",` +
				`"profile":"7g.80gb","giID":0,"ciID":0,"migUUID":"MIG-legacy-1",` +
				`"computeSlices":7,"start":0,"length":8}`,
			want: migMarker{
				PodUID: "pod-b", Container: "c1", Card: "GPU-bbbb0000-0000-0000-0000-000000000000",
				Profile: "7g.80gb", GiID: 0, CiID: 0, MigUUID: "MIG-legacy-1",
				ComputeSlices: 7, Start: 0, Length: 8,
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var got migMarker
			require.NoError(t, json.Unmarshal([]byte(c.doc), &got),
				"a pre-rename marker must still parse")
			assert.Equal(t, c.want, got, "every identity a pre-rename marker records must survive")

			out, err := json.Marshal(got)
			require.NoError(t, err)

			var keyed map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(out, &keyed))
			assert.ElementsMatch(t, legacyKeys, slices.Collect(maps.Keys(keyed)),
				"the marshalled record must carry exactly the legacy keys")
			assert.NotContains(t, keyed, "accelerator",
				"the vocabulary rename must not have reached the on-disk format")
		})
	}
}

// TestActuatePhysicalSliced_RecordsTheGrantedPartitionIdentity 标识了分配操作将容器分配到哪个分区。
// 该标识作为环境值被传递到容器中，不会出现在其他地方；因此当容器引擎拒绝解析该标识时，便无法得知究竟
// 是什么被授予、由哪个加速器处理、或是是否刚刚创建或采用——而这两种可能的错误情况——即分配后分区被销毁
// 的情况，以及引擎无法处理的情形，正是通过这一标识被明确区分的。
func TestActuatePhysicalSliced_RecordsTheGrantedPartitionIdentity(t *testing.T) {
	const profile = "1g.10gb"
	redirectLogicalSliceDirs(t)
	drv := newFakeMigDriver()
	drv.possible[testGPUUUID0] = evenSlots()

	var lines []string
	logger := funcr.New(func(prefix, args string) {
		lines = append(lines, prefix+args)
	}, funcr.Options{})
	s := &server{ResourceServer: deviceplugin.ResourceServer{Logger: logger}, mig: drv}

	devs := migDevices(profile, 1, 2, testGPUUUID0)
	pod := &core.Pod{ObjectMeta: meta.ObjectMeta{Name: "p", Namespace: "ns", UID: types.UID("pod-1")}}
	ctr := &core.Container{Name: "c"}
	allocated := map[deviceplugin.Resource]int32{{Group: "h100", Device: testGPUUUID0}: 200000}

	out, err := s.ActuatePhysicalSliced(context.Background(), pod, ctr, devs, allocated, profile)
	require.NoError(t, err)
	granted := out.IDs[deviceplugin.Resource{Group: "h100", Device: testGPUUUID0}]
	require.NotEmpty(t, granted)

	joined := strings.Join(lines, "\n")
	assert.Contains(t, joined, granted, "the granted partition identity is recorded")
	assert.Contains(t, joined, testGPUUUID0, "the accelerator it was carved on is recorded")
	assert.Contains(t, joined, "created", "how the partition was obtained is recorded")
}
